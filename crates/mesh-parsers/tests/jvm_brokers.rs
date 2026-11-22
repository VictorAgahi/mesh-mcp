//! JVM brokers in Java (9.0 T8a) through the real dispatch (`PolyglotIndexer`) and the graph.
//! The golden truth of the fixture is `tests/golden/events/jvm-brokers.expected.yaml`; these
//! tests pin the idioms the fixture cannot show on its own line (a request built in a local,
//! a bean topic, constants, shadowing, the caps and the hostile shapes).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use mesh_core::{Broker, ContractGraph};
use mesh_parsers::languages::{PolyglotIndexer, MAX_JVM_BROKER_SITES_PER_FILE};
use mesh_parsers::FileIndex;
use std::path::Path;

fn extract(code: &str) -> FileIndex {
    let fi = PolyglotIndexer::extract(Path::new("src/main/java/p/C.java"), code, 0);
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

/// A class `C` with `fields` and one method body `body`, importing `imports`.
fn class(imports: &[&str], fields: &str, params: &str, body: &str) -> String {
    let mut out = String::from("package p;\n");
    for i in imports {
        out.push_str(&format!("import {i};\n"));
    }
    out.push_str(&format!(
        "public class C {{\n{fields}\n  void run({params}) {{\n{body}\n  }}\n}}\n"
    ));
    out
}

const RABBIT: &str = "org.springframework.amqp.rabbit.core.RabbitTemplate";

// ---- RabbitMQ ---------------------------------------------------------------------------

#[test]
fn rabbit_send_forms_key_on_the_routing_key_or_the_default_exchange_queue() {
    let fi = extract(&class(
        &[RABBIT],
        "  private RabbitTemplate rabbit;",
        "Object m",
        r#"    rabbit.convertAndSend("ex", "rk.a", m);
    rabbit.convertAndSend("queue.b", m);
    rabbit.convertAndSend("queue.c", m, msg -> msg);
    rabbit.convertAndSend("ex", "rk.d", m, null);
    this.rabbit.send("ex", "rk.e", m);
    rabbit.convertAndSend(m);"#,
    ));
    assert_eq!(
        sites(&fi),
        vec![
            s('P', "rabbitmq", "queue.b"),
            s('P', "rabbitmq", "queue.c"),
            s('P', "rabbitmq", "rk.a"),
            s('P', "rabbitmq", "rk.d"),
            s('P', "rabbitmq", "rk.e"),
        ]
    );
}

#[test]
fn rabbit_listener_forms() {
    let code = r##"package p;
import org.springframework.amqp.rabbit.annotation.*;
public class C {
    @RabbitListener(queues = {"a", "b"})
    void one(String m) {}
    @RabbitListener(queuesToDeclare = @Queue("c"))
    void two(String m) {}
    @RabbitListener(bindings = @QueueBinding(value = @Queue("d"), exchange = @Exchange("x"), key = {"k1", "k2"}))
    void three(String m) {}
    @RabbitListener(bindings = @QueueBinding(value = @Queue(name = "e"), exchange = @Exchange("x")))
    void four(String m) {}
    @RabbitListener(bindings = @QueueBinding(value = @Queue("f"), exchange = @Exchange("x"), key = "order.#"))
    void hash(String m) {}
    @RabbitListener(queues = "#{spel}")
    void spel(String m) {}
    @RabbitListener(id = "no-queue")
    void none(String m) {}
}
"##;
    assert_eq!(
        sites(&extract(code)),
        vec![
            s('C', "rabbitmq", "a"),
            s('C', "rabbitmq", "b"),
            s('C', "rabbitmq", "c"),
            s('C', "rabbitmq", "e"),
            s('C', "rabbitmq", "k1"),
            s('C', "rabbitmq", "k2"),
        ]
    );
}

#[test]
fn a_class_level_rabbit_listener_is_one_consumer_on_the_class() {
    let code = r#"package p;
import org.springframework.amqp.rabbit.annotation.*;
@RabbitListener(queues = "shipments")
public class C {
    @RabbitHandler void a(String m) {}
    @RabbitHandler void b(Integer m) {}
}
"#;
    let fi = extract(code);
    assert_eq!(sites(&fi), vec![s('C', "rabbitmq", "shipments")]);
    assert_eq!(fi.nodes[fi.consumers[0].0].name, "C");
}

#[test]
fn constants_of_the_file_resolve_and_the_rest_do_not() {
    let code = r#"package p;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
public class C {
    static final String ORDERS = "orders.q";
    private static String mutable = "mutable.q";
    @RabbitListener(queues = ORDERS) void a(String m) {}
    @RabbitListener(queues = C.ORDERS) void b(String m) {}
    @RabbitListener(queues = mutable) void c(String m) {}
    @RabbitListener(queues = Other.ORDERS) void d(String m) {}
    @RabbitListener(queues = ORDERS + ".x") void e(String m) {}
}
"#;
    assert_eq!(
        sites(&extract(code)),
        vec![
            s('C', "rabbitmq", "orders.q"),
            s('C', "rabbitmq", "orders.q")
        ]
    );
}

#[test]
fn a_constant_declared_twice_with_different_values_is_ambiguous() {
    let code = r#"package p;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
public class C {
    static class A { static final String Q = "a.q"; }
    static class B { static final String Q = "b.q"; }
    @RabbitListener(queues = Q) void a(String m) {}
}
"#;
    assert!(sites(&extract(code)).is_empty());
}

#[test]
fn value_placeholders_resolve_like_kafka_and_a_local_hides_the_field() {
    let code = class(
        &[RABBIT, "org.springframework.beans.factory.annotation.Value"],
        "  @Value(\"${app.q}\") private String queue;\n  private RabbitTemplate rabbit;",
        "String other",
        "    rabbit.convertAndSend(queue, other);\n    rabbit.convertAndSend(this.queue, other);\n    String queue = other;\n    rabbit.convertAndSend(queue, other);",
    );
    // A bare `queue` is the local anywhere in the method (as for `@KafkaListener` producers);
    // only `this.queue` is the field.
    assert_eq!(sites(&extract(&code)), vec![s('P', "rabbitmq", "${app.q}")]);
}

// ---- JMS --------------------------------------------------------------------------------

#[test]
fn jms_listener_template_and_destination_objects() {
    let code = r#"package p;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.core.JmsTemplate;
import org.apache.activemq.command.ActiveMQTopic;
public class C {
    private JmsTemplate jms;
    @JmsListener(destination = "in.q") void on(String m) {}
    void out(Object m) {
        jms.convertAndSend("out.q", m);
        jms.send(new ActiveMQTopic("t.news"), s -> null);
        jms.receive("poll.q");
        jms.convertAndSend(m);
    }
}
"#;
    assert_eq!(
        sites(&extract(code)),
        vec![
            s('C', "jms", "in.q"),
            s('C', "jms", "poll.q"),
            s('P', "jms", "out.q"),
            s('P', "jms", "t.news"),
        ]
    );
}

// ---- SQS / SNS --------------------------------------------------------------------------

#[test]
fn sqs_and_sns_name_the_last_segment_of_a_url_or_arn() {
    let code = r#"package p;
import io.awspring.cloud.sqs.annotation.SqsListener;
import io.awspring.cloud.sqs.operations.SqsTemplate;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sns.SnsClient;
public class C {
    private SqsTemplate tpl;
    private SqsClient sqs;
    private SnsClient sns;
    @SqsListener("https://sqs.eu-west-1.amazonaws.com/1/listened") void a(String m) {}
    @SqsListener(queueNames = {"q1", "q2"}) void b(String m) {}
    void out(String m) {
        tpl.send("t.q", m);
        tpl.send(to -> to.queue("lam.q").payload(m));
        sqs.sendMessage(r -> r.queueUrl("https://sqs/1/sent"));
        sqs.receiveMessage(r -> r.queueUrl("https://sqs/1/polled"));
        sns.publish(r -> r.topicArn("arn:aws:sns:eu-west-1:1:topic-a").message(m));
        PublishRequest req = PublishRequest.builder().topicArn("arn:aws:sns:eu-west-1:1:topic-b").message(m).build();
        sns.publish(req);
    }
}
"#;
    assert_eq!(
        sites(&extract(code)),
        vec![
            s('C', "sqs", "listened"),
            s('C', "sqs", "polled"),
            s('C', "sqs", "q1"),
            s('C', "sqs", "q2"),
            s('P', "sns", "topic-a"),
            s('P', "sns", "topic-b"),
            s('P', "sqs", "lam.q"),
            s('P', "sqs", "sent"),
            s('P', "sqs", "t.q"),
        ]
    );
}

#[test]
fn an_aws_request_built_elsewhere_names_nothing() {
    let code = class(
        &["software.amazon.awssdk.services.sns.SnsClient"],
        "  private SnsClient sns;",
        "PublishRequest prepared",
        "    sns.publish(prepared);\n    sns.publish(build());",
    );
    assert!(sites(&extract(&code)).is_empty());
}

// ---- Redis ------------------------------------------------------------------------------

#[test]
fn redis_channels_topics_and_a_bean_topic() {
    let code = r#"package p;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.*;
public class C {
    private StringRedisTemplate redis;
    private final ChannelTopic fixed = new ChannelTopic("fixed.ch");
    ChannelTopic bean() { return new ChannelTopic("bean.ch"); }
    void out(String m) {
        redis.convertAndSend("pub.ch", m);
        redis.convertAndSend(fixed.getTopic(), m);
        redis.convertAndSend(bean().getTopic(), m);
        redis.convertAndSend(m);
    }
    void in(RedisMessageListenerContainer container, Object l) {
        container.addMessageListener(l, new ChannelTopic("a.ch"));
        container.addMessageListener(l, new PatternTopic("p.*"));
        container.addMessageListener(l, bean());
        container.addMessageListener(l, java.util.List.of(new ChannelTopic("x.ch"), new ChannelTopic("y.ch")));
        container.addMessageListener(l, unknown());
    }
}
"#;
    assert_eq!(
        sites(&extract(code)),
        vec![
            s('C', "redis", "a.ch"),
            s('C', "redis", "bean.ch"),
            s('C', "redis", "p.*"),
            s('C', "redis", "x.ch"),
            s('C', "redis", "y.ch"),
            s('P', "redis", "bean.ch"),
            s('P', "redis", "fixed.ch"),
            s('P', "redis", "pub.ch"),
        ]
    );
}

// ---- NATS and Pub/Sub -------------------------------------------------------------------

#[test]
fn nats_publish_subscribe_and_dispatchers() {
    let code = r#"package p;
import io.nats.client.*;
public class C {
    private Connection nc;
    private JetStream js;
    void go(byte[] d) throws Exception {
        nc.publish("a.pub", d);
        js.publish("b.js", d);
        nc.request("c.req", d);
        nc.publish(NatsMessage.builder().subject("d.msg").data(d).build());
        nc.subscribe("e.sub");
        Dispatcher dp = nc.createDispatcher(m -> {});
        dp.subscribe("f.disp", m -> {});
        nc.createDispatcher(m -> {}).subscribe("g.chain");
        nc.publish(subjectFor(d), d);
    }
}
"#;
    assert_eq!(
        sites(&extract(code)),
        vec![
            s('C', "nats", "e.sub"),
            s('C', "nats", "f.disp"),
            s('C', "nats", "g.chain"),
            s('P', "nats", "a.pub"),
            s('P', "nats", "b.js"),
            s('P', "nats", "c.req"),
            s('P', "nats", "d.msg"),
        ]
    );
}

#[test]
fn pubsub_template_publishes_a_topic() {
    let code = class(
        &["com.google.cloud.spring.pubsub.core.PubSubTemplate"],
        "  private PubSubTemplate pubsub;",
        "String m",
        "    pubsub.publish(\"events-topic\", m);\n    pubsub.publish(m);",
    );
    assert_eq!(
        sites(&extract(&code)),
        vec![s('P', "pubsub", "events-topic")]
    );
}

// ---- what is not a site -----------------------------------------------------------------

#[test]
fn an_unrelated_class_a_log_line_and_a_missing_import_are_not_sites() {
    // Same method names on a class that is not a client, in a file that imports the library.
    let unrelated = class(
        &[RABBIT],
        "  private Bridge bridge;\n  private RabbitTemplate rabbit;",
        "String q",
        "    bridge.convertAndSend(\"x.q\", q);\n    log.info(\"rabbit.convertAndSend(\\\"y.q\\\", q)\");\n    rabbit.convertAndSend(q, q);",
    );
    assert!(sites(&extract(&unrelated)).is_empty());

    // The right type name, but the file imports no AMQP library (a local class of that name).
    let ungated = class(
        &[],
        "  private RabbitTemplate rabbit;",
        "String m",
        "    rabbit.convertAndSend(\"z.q\", m);",
    );
    assert!(sites(&extract(&ungated)).is_empty());

    // A NATS `Connection` of another library.
    let sql = class(
        &["java.sql.Connection"],
        "  private Connection db;",
        "String m",
        "    db.publish(\"w.q\", m);",
    );
    assert!(sites(&extract(&sql)).is_empty());
}

#[test]
fn a_name_means_the_declaration_of_its_own_scope() {
    // Two classes name their field `client`; only the one typed with the library's class is a
    // broker client, and each call reads its own class's field.
    let code = r#"package p;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
public class C {
    static class A { RabbitTemplate client; void go(Object m) { client.convertAndSend("a.q", m); } }
    static class B { Bridge client; void go(Object m) { client.convertAndSend("b.q", m); } }
}
"#;
    assert_eq!(sites(&extract(code)), vec![s('P', "rabbitmq", "a.q")]);
}

#[test]
fn kafka_sites_are_untouched() {
    let code = r#"package p;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
public class C {
    private KafkaTemplate<String, String> kafkaTemplate;
    private RabbitTemplate rabbit;
    @KafkaListener(topics = "k.in") void on(String m) {}
    void out(String m) {
        kafkaTemplate.send("k.out", m);
        rabbit.convertAndSend("r.out", m);
    }
}
"#;
    assert_eq!(
        sites(&extract(code)),
        vec![
            s('C', "kafka", "k.in"),
            s('P', "kafka", "k.out"),
            s('P', "rabbitmq", "r.out"),
        ]
    );
}

// ---- graph: broker-qualified hubs -------------------------------------------------------

#[test]
fn a_name_on_rabbitmq_and_jms_is_two_channels() {
    let mut graph = ContractGraph::new();
    for (file, code) in [
        (
            "src/main/java/p/R.java",
            "package p;\nimport org.springframework.amqp.rabbit.annotation.RabbitListener;\npublic class R {\n @RabbitListener(queues = \"orders\") void on(String m) {}\n}\n",
        ),
        (
            "src/main/java/p/J.java",
            "package p;\nimport org.springframework.jms.core.JmsTemplate;\npublic class J {\n JmsTemplate jms;\n void out(String m) { jms.convertAndSend(\"orders\", m); }\n}\n",
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
    assert!(hubs.contains(&"jms:orders".to_string()), "{hubs:?}");
}

// ---- bounds -----------------------------------------------------------------------------

#[test]
fn a_file_past_the_site_cap_is_cut_and_flagged_never_silently() {
    let mut code = String::from(
        "package p;\nimport org.springframework.amqp.rabbit.core.RabbitTemplate;\npublic class C {\n RabbitTemplate rabbit;\n",
    );
    for i in 0..MAX_JVM_BROKER_SITES_PER_FILE + 50 {
        code.push_str(&format!(
            " void m{i}(Object o) {{ rabbit.convertAndSend(\"q{i}\", o); }}\n"
        ));
    }
    code.push_str("}\n");
    let fi = extract(&code);
    assert!(fi.jvm_brokers_capped);
    assert_eq!(fi.producers.len(), MAX_JVM_BROKER_SITES_PER_FILE);
    // The first sites in source order are the ones kept.
    assert!(sites(&fi).contains(&s('P', "rabbitmq", "q0")));

    let small = extract(&class(
        &[RABBIT],
        "  RabbitTemplate rabbit;",
        "Object o",
        "    rabbit.convertAndSend(\"q\", o);",
    ));
    assert!(!small.jvm_brokers_capped);
}

/// The hostile shapes of the ticket: 4000 methods and 3000 nested generics, read on a 2 MB
/// stack (the size of a Rayon worker thread's stack here). Nothing recurses on the nesting.
#[test]
fn hostile_files_cost_a_bounded_walk_on_a_small_stack() {
    let methods = {
        let mut code = String::from(
            "package p;\nimport org.springframework.amqp.rabbit.core.RabbitTemplate;\npublic class C {\n RabbitTemplate rabbit;\n",
        );
        for i in 0..4000 {
            code.push_str(&format!(
                " void m{i}(Object o) {{ rabbit.convertAndSend(\"q{i}\", o); }}\n"
            ));
        }
        code.push_str("}\n");
        code
    };
    let generics = {
        let depth = 3000;
        format!(
            "package p;\nimport org.springframework.amqp.rabbit.core.RabbitTemplate;\npublic class C {{\n {}x{} field;\n RabbitTemplate rabbit;\n void m(Object o) {{ rabbit.convertAndSend(\"q\", o); }}\n}}\n",
            "M<".repeat(depth),
            ">".repeat(depth)
        )
    };
    let chain = format!(
        "package p;\nimport org.springframework.amqp.rabbit.core.RabbitTemplate;\npublic class C {{\n static final String Q = \"q\";\n RabbitTemplate rabbit;\n void m(Object o) {{ rabbit.convertAndSend(Q{}, o); }}\n}}\n",
        ".getTopic()".repeat(3000)
    );
    let handle = std::thread::Builder::new()
        .stack_size(2 * 1024 * 1024)
        .spawn(move || {
            for code in [&methods, &generics, &chain] {
                // The lexical guard may reject the file outright; either way no overflow.
                let fi = PolyglotIndexer::extract(Path::new("src/main/java/p/C.java"), code, 0);
                assert!(fi.producers.len() <= MAX_JVM_BROKER_SITES_PER_FILE);
            }
        })
        .unwrap();
    handle.join().unwrap();
}

// ---- 9.0 T8a review round 1 -------------------------------------------------------------

/// Canary placeholder defaults: none of them may become a channel name (T6, commandment 5).
const CANARIES: [&str; 6] = [
    "rpw-DEFPW6",
    "jpw-DEFPW7",
    "hunter2-prod",
    "credpw-DEFPW2",
    "tok_DEFPW3_x",
    "P@ssw0rd!",
];

#[test]
fn a_placeholder_default_is_never_a_channel_name() {
    let code = r##"package p;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.beans.factory.annotation.Value;
import io.awspring.cloud.sqs.annotation.SqsListener;
public class C {
    @Value("${app.absent2:credpw-DEFPW2}") private String b;
    @Value("${app.absent3:tok_DEFPW3_x}") private String d;
    @Value("${app.present:fallback.topic}") private String c;
    private static final String K = "${k.const:hunter2-prod}";
    private RabbitTemplate rabbit;
    void m() { rabbit.convertAndSend(b, "x"); rabbit.convertAndSend(d, "x"); rabbit.convertAndSend(c, "x"); rabbit.convertAndSend(K, "x"); }
    @RabbitListener(queues = "${app.absent6:rpw-DEFPW6}") public void r(String m) {}
    @JmsListener(destination = "${app.absent7:jpw-DEFPW7}") public void j(String m) {}
    @RabbitListener(queues = "${app.absent10:hunter2-prod}") public void h(String m) {}
    @RabbitListener(queues = "${app.absent9:P@ssw0rd!}") public void p(String m) {}
    @SqsListener("${app.absent11:hunter2-prod}") public void q(String m) {}
}
"##;
    let fi = extract(code);
    let dump = format!("{fi:?}");
    for canary in CANARIES {
        assert!(
            !dump.contains(canary),
            "{canary} reached the file index: {dump}"
        );
    }
    let got = sites(&fi);
    // Every site stays a visible `${key}` for the configuration to resolve.
    assert!(
        got.contains(&s('C', "rabbitmq", "${app.absent6}")),
        "{got:?}"
    );
    assert!(got.contains(&s('C', "jms", "${app.absent7}")), "{got:?}");
    assert!(
        got.contains(&s('P', "rabbitmq", "${app.present}")),
        "{got:?}"
    );
    assert!(got.contains(&s('P', "rabbitmq", "${k.const}")), "{got:?}");
    assert!(got.iter().all(|(_, _, t)| t.starts_with("${")), "{got:?}");
}

#[test]
fn a_hostile_import_or_class_does_not_make_a_client() {
    // Each file would register `fp.q` if its receiver were read as the Spring client.
    let cases: [(&str, String); 8] = [
        (
            "an import in a comment",
            "package p;\n// import org.springframework.amqp.rabbit.core.RabbitTemplate;\npublic class C { RabbitTemplate r; void a() { r.convertAndSend(\"fp.q\", \"x\"); } }\n".into(),
        ),
        (
            "an import in a block comment",
            "package p;\n/* import org.springframework.amqp.rabbit.core.RabbitTemplate; */\npublic class C { RabbitTemplate r; void a() { r.convertAndSend(\"fp.q\", \"x\"); } }\n".into(),
        ),
        (
            "a same-named class of another library",
            "package p;\nimport com.acme.legacy.RabbitTemplate;\nimport org.springframework.amqp.core.Message;\npublic class C { RabbitTemplate r; void a() { r.convertAndSend(\"fp.q\", \"x\"); } }\n".into(),
        ),
        (
            "a nested class of the same name",
            "package p;\nimport org.springframework.amqp.core.Message;\npublic class C { static class RabbitTemplate { void convertAndSend(String a, Object b) {} } RabbitTemplate r = new RabbitTemplate(); void a() { r.convertAndSend(\"fp.q\", \"x\"); } }\n".into(),
        ),
        (
            "a for-each variable",
            "package p;\nimport org.springframework.amqp.rabbit.core.RabbitTemplate;\nimport java.util.List;\npublic class C { RabbitTemplate r; interface Sink { void convertAndSend(String a, Object b); } void a(List<Sink> sinks) { for (Sink r : sinks) { r.convertAndSend(\"fp.q\", null); } } }\n".into(),
        ),
        (
            "a lambda parameter",
            "package p;\nimport org.springframework.amqp.rabbit.core.RabbitTemplate;\nimport java.util.List;\npublic class C { RabbitTemplate r; interface Sink { void convertAndSend(String a, Object b); } void a(List<Sink> sinks) { sinks.forEach(r -> r.convertAndSend(\"fp.q\", null)); } }\n".into(),
        ),
        (
            "a local of another type",
            "package p;\nimport org.springframework.amqp.rabbit.core.RabbitTemplate;\npublic class C { RabbitTemplate r; interface Sink { void convertAndSend(String a, Object b); } void a(Sink s) { Sink r = s; r.convertAndSend(\"fp.q\", null); } }\n".into(),
        ),
        (
            "a catch variable",
            "package p;\nimport org.springframework.amqp.rabbit.core.RabbitTemplate;\npublic class C { RabbitTemplate r; void a() { try { } catch (Sink r) { r.convertAndSend(\"fp.q\", null); } } interface Sink { void convertAndSend(String a, Object b); } }\n".into(),
        ),
    ];
    for (name, code) in cases {
        let got = sites(&extract(&code));
        assert!(got.is_empty(), "{name}: {got:?}");
    }
    // The real thing still works next to the same shapes.
    let real = "package p;\nimport org.springframework.amqp.rabbit.core.*;\npublic class C { RabbitTemplate r; void a() { r.convertAndSend(\"ok.q\", \"x\"); } }\n";
    assert_eq!(sites(&extract(real)), vec![s('P', "rabbitmq", "ok.q")]);
    let qualified = "package p;\npublic class C { org.springframework.amqp.rabbit.core.RabbitTemplate r; void a() { r.convertAndSend(\"ok.q\", \"x\"); } }\n";
    assert_eq!(sites(&extract(qualified)), vec![s('P', "rabbitmq", "ok.q")]);
}

#[test]
fn a_listener_annotation_needs_a_real_import_too() {
    let hidden = "package p;\n// import org.springframework.amqp.rabbit.annotation.RabbitListener;\npublic class C { @RabbitListener(queues = \"fp.q\") void a(String m) {} }\n";
    assert!(sites(&extract(hidden)).is_empty());
    let own = "package p;\nimport com.acme.RabbitListener;\nimport org.springframework.amqp.core.Message;\npublic class C { @RabbitListener(queues = \"fp.q\") void a(String m) {} }\n";
    assert!(sites(&extract(own)).is_empty());
    let real = "package p;\nimport org.springframework.amqp.rabbit.annotation.RabbitListener;\npublic class C { @RabbitListener(queues = \"ok.q\") void a(String m) {} }\n";
    assert_eq!(sites(&extract(real)), vec![s('C', "rabbitmq", "ok.q")]);
}

#[test]
fn nats_types_need_a_real_nats_import() {
    let comment = "package p;\n// import io.nats.client.Connection;\npublic class C { Connection nc; void a() { nc.publish(\"fp.s\", null); nc.subscribe(\"fp.t\"); } }\n";
    assert!(sites(&extract(comment)).is_empty());
    let sql = "package p;\nimport java.sql.Connection;\nimport io.nats.client.Message;\npublic class C { Connection db; void a() { db.publish(\"fp.s\", null); } }\n";
    assert!(sites(&extract(sql)).is_empty());
    let real = "package p;\nimport io.nats.client.Connection;\npublic class C { Connection nc; void a() { nc.publish(\"ok.s\", null); } }\n";
    assert_eq!(sites(&extract(real)), vec![s('P', "nats", "ok.s")]);
}

#[test]
fn an_ambiguous_three_argument_send_names_no_channel() {
    let code = class(
        &[
            RABBIT,
            "org.springframework.amqp.core.MessagePostProcessor",
            "com.acme.Order",
        ],
        "  private RabbitTemplate rabbit;\n  private MessagePostProcessor pp;",
        "Order order, MessagePostProcessor given, Mystery mystery",
        r#"    rabbit.convertAndSend("q.pp", "body", pp);
    rabbit.convertAndSend("q.given", "body", given);
    rabbit.convertAndSend("q.new", "body", new MessagePostProcessor() { });
    rabbit.convertAndSend("ex", "rk.order", order);
    rabbit.convertAndSend("ex", "rk.literal", "payload");
    rabbit.convertAndSend("q.unknown", "body", mystery);
    rabbit.convertAndSend("q.undeclared", "body", inheritedField);
    rabbit.convertAndSend("ex", "rk.four", order, pp);"#,
    );
    assert_eq!(
        sites(&extract(&code)),
        vec![
            // `mystery` is declared with a type that is not named like a post-processor: read as
            // the message, so `body` is taken as the routing key (a documented residual).
            s('P', "rabbitmq", "body"),
            s('P', "rabbitmq", "q.given"),
            s('P', "rabbitmq", "q.new"),
            s('P', "rabbitmq", "q.pp"),
            s('P', "rabbitmq", "rk.four"),
            s('P', "rabbitmq", "rk.literal"),
            s('P', "rabbitmq", "rk.order"),
        ]
    );
}

#[test]
fn a_producer_never_publishes_to_a_pattern() {
    let code = r#"package p;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import io.nats.client.Connection;
public class C {
    RabbitTemplate rabbit; StringRedisTemplate redis; Connection nats;
    void a(Object m) {
        rabbit.convertAndSend("ex", "x.#", m);
        rabbit.convertAndSend("x.*", m);
        redis.convertAndSend("r.*", m);
        nats.publish("n.>", null);
        nats.publish("n.ok", null);
    }
    @RabbitListener(queues = "c.*") void on(String m) {}
}
"#;
    assert_eq!(
        sites(&extract(code)),
        vec![s('C', "rabbitmq", "c.*"), s('P', "nats", "n.ok")]
    );
}
