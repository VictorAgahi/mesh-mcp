//! Python brokers (9.0 T8c) through the real dispatch (`PolyglotIndexer`) and the graph. The
//! golden truth of the fixture is `tests/golden/events/py-brokers.expected.yaml`; these tests pin
//! the idioms the fixture cannot show on its own line (scopes, shadowing, constants, defaults,
//! the caps and the hostile shapes).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use mesh_core::{Broker, ContractGraph};
use mesh_parsers::languages::{PolyglotIndexer, MAX_PY_BROKER_SITES_PER_FILE};
use mesh_parsers::FileIndex;
use std::path::Path;

fn extract(code: &str) -> FileIndex {
    let fi = PolyglotIndexer::extract(Path::new("src/svc/app.py"), code, 0);
    assert!(!fi.parse_failed, "must parse");
    fi
}

/// `(role, broker slug, topic)` of every producer and consumer, sorted. A site without a broker
/// reads `?`.
fn sites(fi: &FileIndex) -> Vec<(char, String, String)> {
    let slug = |brokers: &[(usize, Broker)], i: usize| {
        brokers
            .iter()
            .find(|(j, _)| *j == i)
            .map_or("?", |(_, b)| b.slug())
            .to_string()
    };
    let mut out: Vec<_> = fi
        .producers
        .iter()
        .enumerate()
        .map(|(i, (_, t))| ('P', slug(&fi.producer_brokers, i), t.to_string()))
        .chain(
            fi.consumers
                .iter()
                .enumerate()
                .map(|(i, (_, t))| ('C', slug(&fi.consumer_brokers, i), t.to_string())),
        )
        .collect();
    out.sort();
    out
}

fn s(role: char, broker: &str, topic: &str) -> (char, String, String) {
    (role, broker.to_string(), topic.to_string())
}

fn read(code: &str) -> Vec<(char, String, String)> {
    sites(&extract(code))
}

// ---- RabbitMQ ---------------------------------------------------------------------------

#[test]
fn pika_publish_keys_on_the_routing_key_and_consume_on_the_queue() {
    let code = r#"
import pika

Q = "orders"
connection = pika.BlockingConnection(pika.ConnectionParameters("h"))
channel = connection.channel()

def run(cb, name):
    channel.queue_declare(queue="declared.only")
    channel.basic_publish(exchange="", routing_key=Q, body=b"x")
    channel.basic_publish("ex", "rk.positional", b"x")
    channel.basic_publish(exchange="ex", routing_key="rk.*", body=b"x")
    channel.basic_publish(exchange="", routing_key=name, body=b"x")
    channel.basic_consume(queue="q.kw", on_message_callback=cb)
    channel.basic_consume("q.pos", cb)
    channel.basic_get(queue=Q)
    channel.basic_consume(queue=name, on_message_callback=cb)
"#;
    assert_eq!(
        read(code),
        vec![
            s('C', "rabbitmq", "orders"),
            s('C', "rabbitmq", "q.kw"),
            s('C', "rabbitmq", "q.pos"),
            s('P', "rabbitmq", "orders"),
            s('P', "rabbitmq", "rk.positional"),
        ]
    );
}

#[test]
fn pika_through_a_with_target_a_self_attribute_and_an_annotated_parameter() {
    let code = r#"
import pika
from pika.adapters.blocking_connection import BlockingChannel

def a():
    with pika.BlockingConnection(params) as conn:
        ch = conn.channel()
        ch.basic_publish(exchange="", routing_key="with.target", body=b"x")

class W:
    queue = "w.queue"
    def __init__(self):
        self.conn = pika.BlockingConnection(params)
        self.ch = self.conn.channel()
    def push(self):
        self.ch.basic_publish(exchange="", routing_key=self.queue, body=b"x")

def b(ch: BlockingChannel):
    ch.basic_consume(queue="annotated", on_message_callback=None)

def c(ch):
    ch.basic_consume(queue="unannotated.param", on_message_callback=None)
"#;
    assert_eq!(
        read(code),
        vec![
            s('C', "rabbitmq", "annotated"),
            s('P', "rabbitmq", "w.queue"),
            s('P', "rabbitmq", "with.target"),
        ]
    );
}

#[test]
fn aio_pika_queues_exchanges_and_the_default_exchange() {
    let code = r#"
import aio_pika

async def main(url, handle):
    connection = await aio_pika.connect_robust(url)
    channel = await connection.channel()
    queue = await channel.declare_queue("inv.todo", durable=True)
    await queue.consume(handle)
    async with queue.iterator() as it:
        pass
    other = await channel.get_queue(name="inv.other")
    await other.consume(handle)
    await channel.default_exchange.publish(aio_pika.Message(b"x"), routing_key="inv.done")
    ex = await channel.declare_exchange("events")
    await ex.publish(aio_pika.Message(b"y"), "inv.paid")
    unnamed = await channel.declare_queue(url)
    await unnamed.consume(handle)
"#;
    assert_eq!(
        read(code),
        vec![
            s('C', "rabbitmq", "inv.other"),
            s('C', "rabbitmq", "inv.todo"),
            s('C', "rabbitmq", "inv.todo"),
            s('P', "rabbitmq", "inv.done"),
            s('P', "rabbitmq", "inv.paid"),
        ]
    );
}

#[test]
fn kombu_producers_and_consumers() {
    let code = r#"
from kombu import Connection, Consumer, Exchange, Queue, Producer

conn = Connection("amqp://h")
q1 = Queue("kq.one", Exchange("x"))
q2 = Queue(name="kq.two")

def a(payload, cb):
    with conn.Producer() as p:
        p.publish(payload, routing_key="kr.one")
    Producer(conn).publish(payload, routing_key="kr.two")
    Consumer(conn, queues=[q1, q2], callbacks=[cb])
    conn.Consumer(queues=Queue("kq.three"))
    Consumer(conn, queues=[unknown_queue])
"#;
    assert_eq!(
        read(code),
        vec![
            s('C', "rabbitmq", "kq.one"),
            s('C', "rabbitmq", "kq.three"),
            s('C', "rabbitmq", "kq.two"),
            s('P', "rabbitmq", "kr.one"),
            s('P', "rabbitmq", "kr.two"),
        ]
    );
}

// ---- Redis, NATS ------------------------------------------------------------------------

#[test]
fn redis_publish_subscribe_psubscribe_and_keyword_channels() {
    let code = r#"
import redis
import redis.asyncio as aredis
from redis import Redis as R

r = redis.Redis()
a = aredis.from_url("redis://h")
b = R()
ps = r.pubsub()

async def go(name, handler):
    r.publish("ch.one", "x")
    await a.publish("ch.async", "x")
    b.publish(channel="ch.kw", message="x")
    r.publish("ch.*", "x")
    r.publish(name, "x")
    ps.subscribe("s.one", "s.two")
    ps.psubscribe("p.*")
    ps.subscribe(s_kw=handler)
    r.pubsub().subscribe("chained")
    async with a.pubsub() as aps:
        await aps.subscribe("async.sub")
"#;
    assert_eq!(
        read(code),
        vec![
            s('C', "redis", "async.sub"),
            s('C', "redis", "chained"),
            s('C', "redis", "p.*"),
            s('C', "redis", "s.one"),
            s('C', "redis", "s.two"),
            s('C', "redis", "s_kw"),
            s('P', "redis", "ch.async"),
            s('P', "redis", "ch.kw"),
            s('P', "redis", "ch.one"),
        ]
    );
}

#[test]
fn nats_client_and_jetstream() {
    let code = r#"
import nats
from nats.aio.client import Client as NATS

async def main(cb):
    nc = await nats.connect("nats://h")
    other = NATS()
    js = nc.jetstream()
    await nc.publish("n.one", b"x")
    await nc.publish(subject="n.kw", payload=b"x")
    await nc.publish("n.>", b"x")
    await nc.request("n.rpc", b"x")
    await nc.subscribe("n.sub.*", cb=cb)
    await other.subscribe("n.other", cb=cb)
    await js.publish("j.one", b"x")
    await js.subscribe("j.sub")
    await js.pull_subscribe("j.pull", durable="d")
"#;
    assert_eq!(
        read(code),
        vec![
            s('C', "nats", "j.pull"),
            s('C', "nats", "j.sub"),
            s('C', "nats", "n.other"),
            s('C', "nats", "n.sub.*"),
            s('P', "nats", "j.one"),
            s('P', "nats", "n.kw"),
            s('P', "nats", "n.one"),
            s('P', "nats", "n.rpc"),
        ]
    );
}

// ---- AWS, Pub/Sub -----------------------------------------------------------------------

#[test]
fn sqs_and_sns_name_the_last_segment_of_the_url_or_arn() {
    let code = r#"
import boto3

URL = "https://sqs.eu-west-1.amazonaws.com/123456789012/orders"
sqs = boto3.client("sqs")
sns = boto3.client(service_name="sns")
s3 = boto3.client("s3")
session = boto3.Session()
other = session.client("sqs")

def a(body, dynamic):
    sqs.send_message(QueueUrl=URL, MessageBody=body)
    sqs.receive_message(QueueUrl=URL)
    sqs.send_message_batch(QueueUrl="https://sqs.x/1/batch", Entries=[])
    sns.publish(TopicArn="arn:aws:sns:eu-west-1:123456789012:events", Message=body)
    s3.send_message(QueueUrl=URL)
    sqs.send_message(QueueUrl=dynamic, MessageBody=body)
    queue = other.get_queue_url(QueueName="by.name")["QueueUrl"]
    other.send_message(QueueUrl=queue, MessageBody=body)
"#;
    assert_eq!(
        read(code),
        vec![
            s('C', "sqs", "orders"),
            s('P', "sns", "events"),
            s('P', "sqs", "batch"),
            s('P', "sqs", "by.name"),
            s('P', "sqs", "orders"),
        ]
    );
}

#[test]
fn pubsub_topic_and_subscription_paths() {
    let code = r#"
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1 import PublisherClient

pub = PublisherClient()
sub = pubsub_v1.SubscriberClient()
tp = pub.topic_path("p", "alerts")
sp = sub.subscription_path("p", "alerts-sub")

def a(data, cb, dyn):
    pub.publish(tp, data)
    pub.publish("projects/p/topics/literal", data)
    pub.publish(topic=dyn, data=data)
    sub.subscribe(sp, callback=cb)
    sub.subscribe(subscription="projects/p/subscriptions/lit-sub", callback=cb)
"#;
    assert_eq!(
        read(code),
        vec![
            s('C', "pubsub", "alerts-sub"),
            s('C', "pubsub", "lit-sub"),
            s('P', "pubsub", "alerts"),
            s('P', "pubsub", "literal"),
        ]
    );
}

// ---- Kafka: the gaps and the tag ---------------------------------------------------------

#[test]
fn kafka_gaps_are_filled_and_bound_clients_carry_their_broker() {
    let code = r#"
from confluent_kafka import Consumer, Producer
from kafka import KafkaConsumer, KafkaProducer
import aiokafka

c1 = KafkaConsumer("k.one", "k.two", bootstrap_servers="b")
c2 = Consumer({"group.id": "g"})
p1 = Producer({})
p2 = KafkaProducer()
ac = aiokafka.AIOKafkaConsumer("k.async")

def run(injected):
    c2.subscribe(topics=["k.kw"])
    c2.subscribe(["k.list"])
    p1.produce("k.produced", b"x")
    p2.send("k.sent", b"x")
    injected.produce("k.unbound", b"x")
"#;
    assert_eq!(
        read(code),
        vec![
            s('C', "kafka", "k.async"),
            s('C', "kafka", "k.kw"),
            s('C', "kafka", "k.list"),
            s('C', "kafka", "k.one"),
            s('C', "kafka", "k.two"),
            s('P', "?", "k.unbound"),
            s('P', "kafka", "k.produced"),
            s('P', "kafka", "k.sent"),
        ]
    );
}

#[test]
fn a_call_of_another_broker_is_not_a_kafka_topic() {
    // The shape-based reader takes any `.publish("literal")` as Kafka; a call the broker pass
    // recognises is read once, under its own broker.
    let code = "import redis\nr = redis.Redis()\ndef a():\n    r.publish(\"only.redis\", \"x\")\n";
    assert_eq!(read(code), vec![s('P', "redis", "only.redis")]);
}

#[test]
fn a_class_the_file_declares_is_nobodys_producer() {
    let code = r#"
class Bus:
    def publish(self, topic, message): pass
    def subscribe(self, topic): pass

bus = Bus()

def a():
    bus.publish("local.topic", 1)
    bus.subscribe("local.topic")
    Bus().publish("local.inline", 1)
"#;
    assert!(read(code).is_empty(), "{:?}", read(code));
}

// ---- gating -----------------------------------------------------------------------------

/// Each snippet would register `fp.q` if its receiver were read as the library client.
#[test]
fn a_hostile_import_or_binding_does_not_make_a_client() {
    let cases: [(&str, &str); 14] = [
        (
            "an import in a comment",
            "# import redis\nr = redis.Redis()\nr.publish(\"fp.q\", 1)\n",
        ),
        (
            "an import in a docstring",
            "\"\"\"\nimport redis\n\"\"\"\nr = redis.Redis()\nr.publish(\"fp.q\", 1)\n",
        ),
        (
            "a same-named class of another module",
            "from acme.cache import Redis\nr = Redis()\nr.publish(\"fp.q\", 1)\n",
        ),
        (
            "a local class of the same name",
            "from redis import Redis\nclass Redis:\n    def publish(self, c, m): pass\nr = Redis()\nr.publish(\"fp.q\", 1)\n",
        ),
        (
            "a parameter of the same name as the module",
            "import redis\ndef f(redis):\n    r = redis.Redis()\n    r.publish(\"fp.q\", 1)\n",
        ),
        (
            "a parameter hiding the client",
            "import redis\nr = redis.Redis()\ndef f(r):\n    r.publish(\"fp.q\", 1)\n",
        ),
        (
            "a comprehension variable",
            "import redis\nr = redis.Redis()\nx = [r.publish(\"fp.q\", 1) for r in sinks]\n",
        ),
        (
            "a lambda parameter",
            "import redis\nr = redis.Redis()\nf = lambda r: r.publish(\"fp.q\", 1)\n",
        ),
        (
            "a loop variable",
            "import redis\nr = redis.Redis()\ndef f(sinks):\n    for r in sinks:\n        r.publish(\"fp.q\", 1)\n",
        ),
        (
            "an except variable",
            "import redis\nr = redis.Redis()\ndef f():\n    try:\n        pass\n    except Exception as r:\n        r.publish(\"fp.q\", 1)\n",
        ),
        (
            "a with target of another call",
            "import redis\nr = redis.Redis()\ndef f(open_sink):\n    with open_sink() as r:\n        r.publish(\"fp.q\", 1)\n",
        ),
        (
            "a rebinding to something else",
            "import redis\nr = redis.Redis()\nr = make_sink()\nr.publish(\"fp.q\", 1)\n",
        ),
        (
            "a relative import",
            "from . import redis\nr = redis.Redis()\nr.publish(\"fp.q\", 1)\n",
        ),
        (
            "a method of the client class reached through another module",
            "import acme\nr = acme.redis.Redis()\nr.publish(\"fp.q\", 1)\n",
        ),
    ];
    for (name, code) in cases {
        let got = read(code);
        assert!(
            got.iter().all(|(_, b, _)| b == "?"),
            "{name}: a broker was assigned: {got:?}"
        );
        assert!(!got.iter().any(|(_, b, _)| b == "redis"), "{name}: {got:?}");
    }
    // The real thing still works next to the same shapes, with aliases of every import form.
    for real in [
        "import redis\nr = redis.Redis()\nr.publish(\"ok.q\", 1)\n",
        "import redis as rd\nr = rd.Redis()\nr.publish(\"ok.q\", 1)\n",
        "from redis import Redis\nr = Redis()\nr.publish(\"ok.q\", 1)\n",
        "from redis import Redis as Client\nr = Client()\nr.publish(\"ok.q\", 1)\n",
        "import redis.asyncio\nr = redis.asyncio.Redis()\nr.publish(\"ok.q\", 1)\n",
    ] {
        assert_eq!(read(real), vec![s('P', "redis", "ok.q")], "{real}");
    }
}

#[test]
fn a_client_name_declared_twice_with_different_meanings_is_ambiguous() {
    let code = "import redis\nr = redis.Redis()\nr = None\ndef f():\n    r.publish(\"amb.q\", 1)\n";
    assert!(!read(code).iter().any(|(_, b, _)| b == "redis"));
    // Declared twice with the same meaning is not ambiguous.
    let twice = "import redis\nr = redis.Redis()\nr = redis.Redis()\nr.publish(\"twice.q\", 1)\n";
    assert_eq!(read(twice), vec![s('P', "redis", "twice.q")]);
}

#[test]
fn a_class_scope_name_is_not_visible_from_a_method() {
    let code = r#"
import redis

class C:
    r = redis.Redis()
    r.publish("class.body", 1)

    def m(self):
        r.publish("class.method", 1)
"#;
    // The method's `r` is nobody's client: only the shape-based reader (no broker) sees it.
    let redis: Vec<_> = read(code)
        .into_iter()
        .filter(|(_, b, _)| b != "?")
        .collect();
    assert_eq!(redis, vec![s('P', "redis", "class.body")]);
}

// ---- names ------------------------------------------------------------------------------

/// Canary placeholder defaults and credential-shaped names: none of them may become a channel
/// name (T6, commandment 5). The key-shaped literals are built at runtime.
#[test]
fn a_placeholder_default_is_never_a_channel_name() {
    let secret_like = format!("sk_{}_{}", "live", "ABCDEF0123456789");
    let code = format!(
        r#"
import os
import redis
import pika

r = redis.Redis()
channel = pika.BlockingConnection(p).channel()
QUEUE = os.getenv("APP_QUEUE", "hunter2-prod-CANARYX")
OTHER = os.environ.get("APP_OTHER", "Bearer_CANARYB")
PLAIN = os.getenv("APP_PLAIN", "plain.default")

def run():
    r.publish(QUEUE, "x")
    r.publish(OTHER, "x")
    r.publish(PLAIN, "x")
    r.publish(os.getenv("APP_INLINE", "inline-CANARYI"), "x")
    r.publish(os.environ["APP_SUBSCRIPT"], "x")
    channel.basic_publish(exchange="", routing_key=os.getenv("APP_RK", "rk-CANARYR"), body=b"x")
    r.publish("{secret_like}", "x")
"#
    );
    let fi = extract(&code);
    let dump = format!("{fi:?}");
    for canary in [
        "hunter2-prod-CANARYX",
        "Bearer_CANARYB",
        "plain.default",
        "inline-CANARYI",
        "rk-CANARYR",
        secret_like.as_str(),
    ] {
        assert!(
            !dump.contains(canary),
            "{canary} reached the file index: {dump}"
        );
    }
    assert_eq!(
        sites(&fi),
        vec![
            s('P', "rabbitmq", "${APP_RK}"),
            s('P', "redis", "${APP_INLINE}"),
            s('P', "redis", "${APP_OTHER}"),
            s('P', "redis", "${APP_PLAIN}"),
            s('P', "redis", "${APP_QUEUE}"),
            s('P', "redis", "${APP_SUBSCRIPT}"),
        ]
    );
}

#[test]
fn only_a_shown_name_is_read() {
    let code = r#"
import redis

r = redis.Redis()
TOPIC_PREFIX = "pre"
NAMES = ["a", "b"]

def run(name, cfg):
    r.publish(f"events.{name}", 1)
    r.publish("events." + name, 1)
    r.publish("events.%s" % name, 1)
    r.publish("events.{}".format(name), 1)
    r.publish(name, 1)
    r.publish(cfg.channel, 1)
    r.publish(NAMES[0], 1)
    r.publish(get_channel(), 1)
    r.publish("with space", 1)
    r.publish("", 1)
    r.publish(b"bytes.chan", 1)
    r.publish("esc\\aped", 1)
    r.publish("ok.one" "ok.two", 1)
    r.publish(*args)
"#;
    assert!(read(code).is_empty(), "{:?}", read(code));
}

#[test]
fn a_reassigned_constant_is_not_a_name() {
    let code = "import redis\nr = redis.Redis()\nQ = \"one\"\nQ = \"two\"\nr.publish(Q, 1)\nR = \"same\"\nR = \"same\"\nr.publish(R, 1)\n";
    assert_eq!(read(code), vec![s('P', "redis", "same")]);
}

#[test]
fn a_producer_never_publishes_to_a_pattern() {
    let code = r#"
import nats
import redis
import pika

async def a(cb):
    nc = await nats.connect("n")
    r = redis.Redis()
    ch = pika.BlockingConnection(p).channel()
    await nc.publish("n.>", b"x")
    await nc.publish("n.*", b"x")
    await nc.publish("n.ok", b"x")
    r.publish("r.*", 1)
    ch.basic_publish(exchange="", routing_key="x.#", body=b"x")
    ch.basic_publish(exchange="", routing_key="x.ok", body=b"x")
    await nc.subscribe("c.>", cb=cb)
    r.pubsub().psubscribe("c.*")
    ch.basic_consume(queue="c.#", on_message_callback=cb)
"#;
    assert_eq!(
        read(code),
        vec![
            s('C', "nats", "c.>"),
            s('C', "rabbitmq", "c.#"),
            s('C', "redis", "c.*"),
            s('P', "nats", "n.ok"),
            s('P', "rabbitmq", "x.ok"),
        ]
    );
}

#[test]
fn a_test_file_path_is_left_to_the_scorer_but_the_code_is_read_the_same() {
    let fi = PolyglotIndexer::extract(
        Path::new("tests/test_it.py"),
        "import redis\nr = redis.Redis()\nr.publish(\"t.q\", 1)\n",
        0,
    );
    assert_eq!(sites(&fi), vec![s('P', "redis", "t.q")]);
}

// ---- graph ------------------------------------------------------------------------------

#[test]
fn one_name_on_two_brokers_is_two_channels() {
    let mut graph = ContractGraph::new();
    for (file, code) in [
        (
            "svc/a.py",
            "import pika\nch = pika.BlockingConnection(p).channel()\ndef a(cb):\n    ch.basic_consume(queue=\"orders\", on_message_callback=cb)\n",
        ),
        (
            "svc/b.py",
            "import boto3\nsqs = boto3.client(\"sqs\")\ndef b():\n    sqs.send_message(QueueUrl=\"https://sqs.x/1/orders\", MessageBody=\"x\")\n",
        ),
    ] {
        PolyglotIndexer::index_file(Path::new(file), code, 0, &mut graph);
    }
    graph.reconcile_edges();
    let hubs: Vec<String> = graph
        .all_nodes()
        .filter(|n| n.package == "event-bus" && n.name.ends_with(":orders"))
        .map(|n| n.name.to_string())
        .collect();
    assert!(hubs.contains(&"rabbitmq:orders".to_string()), "{hubs:?}");
    assert!(hubs.contains(&"sqs:orders".to_string()), "{hubs:?}");
}

// ---- bounds -----------------------------------------------------------------------------

#[test]
fn a_file_past_the_site_cap_is_cut_and_flagged_never_silently() {
    let mut code = String::from("import redis\nr = redis.Redis()\n");
    for i in 0..MAX_PY_BROKER_SITES_PER_FILE + 50 {
        code.push_str(&format!("def m{i}():\n    r.publish(\"q{i}\", 1)\n"));
    }
    let fi = extract(&code);
    assert!(fi.py_brokers_capped);
    assert_eq!(fi.producers.len(), MAX_PY_BROKER_SITES_PER_FILE);
    assert!(sites(&fi).contains(&s('P', "redis", "q0")));

    let small = extract("import redis\nr = redis.Redis()\nr.publish(\"q\", 1)\n");
    assert!(!small.py_brokers_capped);
}

/// The hostile shapes of the ticket: 4000 defs and 3000 nested brackets / lambdas, read on a
/// 2 MB stack (the size of a Rayon worker thread's stack here). Nothing recurses on the nesting.
#[test]
fn hostile_files_cost_a_bounded_walk_on_a_small_stack() {
    let defs = {
        let mut code = String::from("import redis\nr = redis.Redis()\n");
        for i in 0..4000 {
            code.push_str(&format!("def m{i}(o):\n    r.publish(\"q{i}\", o)\n"));
        }
        code
    };
    let brackets = {
        let depth = 3000;
        format!(
            "import redis\nr = redis.Redis()\nx = {}1{}\nr.publish(\"q\", 1)\n",
            "[".repeat(depth),
            "]".repeat(depth)
        )
    };
    let lambdas = {
        let depth = 3000;
        format!(
            "import redis\nr = redis.Redis()\nf = {}1\nr.publish(\"q\", f)\n",
            "lambda x: ".repeat(depth)
        )
    };
    // A 50 000-link attribute chain, one link per line (each line is short, the tree is deep).
    let attrs = {
        let chain = "\n.c".repeat(50_000);
        format!(
            "import redis\nr = redis.Redis()\nx = (r{chain}\n.publish(\"q\", 1))\ny = (redis{chain}\n.Redis())\nz = (r{chain}\n.pubsub().subscribe(\"s\"))\n"
        )
    };
    let handle = std::thread::Builder::new()
        .stack_size(2 * 1024 * 1024)
        .spawn(move || {
            for code in [&defs, &brackets, &lambdas, &attrs] {
                // The lexical guard may reject the file outright; either way no overflow.
                let fi = PolyglotIndexer::extract(Path::new("src/svc/app.py"), code, 0);
                assert!(fi.producers.len() <= MAX_PY_BROKER_SITES_PER_FILE + 4000);
            }
        })
        .unwrap();
    handle.join().unwrap();
}

// ---- 9.0 T8c fix round 1 ------------------------------------------------------------------

/// `X0 = "exp.q"` then `depth` copies of `Xk = Xk-1` for each of `levels` levels: a name resolved
/// per path costs `depth ^ levels`.
fn chain_of_copies(levels: usize, depth: usize) -> String {
    let mut code = String::from("import redis\nr = redis.Redis()\nX0 = \"exp.q\"\n");
    for k in 1..=levels {
        for _ in 0..depth {
            code.push_str(&format!("X{k} = X{}\n", k - 1));
        }
    }
    code.push_str(&format!("r.publish(X{levels}, 1)\n"));
    code
}

#[test]
fn a_chain_of_name_copies_is_resolved_once_not_once_per_path() {
    let started = std::time::Instant::now();
    for (levels, depth) in [(4, 50), (6, 30), (7, 50)] {
        let fi = extract(&chain_of_copies(levels, depth));
        assert_eq!(
            sites(&fi),
            vec![s('P', "redis", "exp.q")],
            "{levels}x{depth}"
        );
        assert!(!fi.py_brokers_capped, "{levels}x{depth}");
    }
    // The same shape for a receiver: a client copied through a long chain.
    let mut client = String::from("import redis\nC0 = redis.Redis()\n");
    for k in 1..=6 {
        for _ in 0..40 {
            client.push_str(&format!("C{k} = C{}\n", k - 1));
        }
    }
    client.push_str("C6.publish(\"copied.client\", 1)\n");
    assert_eq!(read(&client), vec![s('P', "redis", "copied.client")]);
    assert!(
        started.elapsed() < std::time::Duration::from_secs(10),
        "name resolution is not bounded: {:?}",
        started.elapsed()
    );
}

#[test]
fn a_cycle_of_name_copies_reads_as_nothing_and_stays_bounded() {
    let mut code = String::from("import redis\nr = redis.Redis()\n");
    for _ in 0..40 {
        for level in 0..6 {
            code.push_str(&format!("A{level} = A{}\n", (level + 1) % 6));
        }
    }
    code.push_str("r.publish(A0, 1)\n");
    let started = std::time::Instant::now();
    let fi = extract(&code);
    assert!(sites(&fi).is_empty(), "{:?}", sites(&fi));
    assert!(
        started.elapsed() < std::time::Duration::from_secs(10),
        "{:?}",
        started.elapsed()
    );
}
