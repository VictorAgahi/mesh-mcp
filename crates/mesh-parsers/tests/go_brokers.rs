//! Go brokers (9.0 T8b) through the real dispatch (`PolyglotIndexer`). The golden truth of the
//! fixture is `tests/golden/events/go-brokers.expected.yaml`; these tests pin the idioms the
//! fixture cannot show on its own line (a request built in a local, a queue declared and read by
//! its `Name`, struct fields, shadowing, the caps and the hostile shapes).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use mesh_core::Broker;
use mesh_parsers::languages::{PolyglotIndexer, MAX_GO_BROKER_SITES_PER_FILE};
use mesh_parsers::FileIndex;
use std::path::Path;

fn extract(code: &str) -> FileIndex {
    let fi = PolyglotIndexer::extract(Path::new("svc/x.go"), code, 0);
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

const AMQP: &str = "amqp \"github.com/rabbitmq/amqp091-go\"";
const NATS: &str = "\"github.com/nats-io/nats.go\"";
const REDIS9: &str = "\"github.com/redis/go-redis/v9\"";
const SQS: &str = "\"github.com/aws/aws-sdk-go-v2/service/sqs\"";
const SNS: &str = "\"github.com/aws/aws-sdk-go-v2/service/sns\"";
const AWS: &str = "\"github.com/aws/aws-sdk-go-v2/aws\"";
const PUBSUB: &str = "\"cloud.google.com/go/pubsub\"";

/// A Go file importing `imports` with `body` as its declarations.
fn file(imports: &[&str], body: &str) -> String {
    let mut out = String::from("package svc\n\nimport (\n");
    for i in imports {
        out.push_str(&format!("\t{i}\n"));
    }
    out.push_str(")\n\n");
    out.push_str(body);
    out.push('\n');
    out
}

// ---- RabbitMQ ---------------------------------------------------------------------------

#[test]
fn rabbit_publish_forms_key_on_the_routing_key_the_queue_or_the_fanout_exchange() {
    let fi = extract(&file(
        &[AMQP],
        r#"type Pub struct { ch *amqp.Channel }

func (p *Pub) Send() {
	p.ch.PublishWithContext(nil, "ex", "orders.created", false, false, amqp.Publishing{})
	p.ch.Publish("", "queue.b", false, false, amqp.Publishing{})
	p.ch.Publish("logs", "", false, false, amqp.Publishing{})
	p.ch.PublishWithDeferredConfirm("ex", "rk.d", false, false, amqp.Publishing{})
}"#,
    ));
    assert_eq!(
        sites(&fi),
        vec![
            s('P', "rabbitmq", "logs"),
            s('P', "rabbitmq", "orders.created"),
            s('P', "rabbitmq", "queue.b"),
            s('P', "rabbitmq", "rk.d"),
        ]
    );
}

#[test]
fn a_declared_queue_is_read_by_its_name_and_a_binding_keys_the_consumer() {
    let fi = extract(&file(
        &[AMQP],
        r#"func run(conn *amqp.Connection) {
	ch, _ := conn.Channel()
	q, _ := ch.QueueDeclare("hello", false, false, false, false, nil)
	ch.Consume(q.Name, "", true, false, false, false, nil)
	ch.Consume("tail", "", true, false, false, false, nil)
	b, _ := ch.QueueDeclare("bound", false, false, false, false, nil)
	ch.QueueBind(b.Name, "bound.key", "ex", false, nil)
	ch.Consume(b.Name, "", true, false, false, false, nil)
	f, _ := ch.QueueDeclare("", false, false, true, false, nil)
	ch.QueueBind(f.Name, "", "logs", false, nil)
	ch.Consume(f.Name, "", true, false, false, false, nil)
	ch.Get("polled", true)
}"#,
    ));
    assert_eq!(
        sites(&fi),
        vec![
            s('C', "rabbitmq", "bound.key"),
            s('C', "rabbitmq", "hello"),
            s('C', "rabbitmq", "polled"),
            s('C', "rabbitmq", "tail"),
        ]
    );
}

#[test]
fn streadway_amqp_and_the_default_package_name_of_amqp091() {
    let streadway = file(
        &["\"github.com/streadway/amqp\""],
        "func a(ch *amqp.Channel) { ch.Publish(\"\", \"old.q\", false, false, amqp.Publishing{}) }",
    );
    assert_eq!(
        sites(&extract(&streadway)),
        vec![s('P', "rabbitmq", "old.q")]
    );
    let bare = file(
        &["\"github.com/rabbitmq/amqp091-go\""],
        "func a(ch *amqp091.Channel) { ch.Publish(\"\", \"new.q\", false, false, amqp091.Publishing{}) }",
    );
    assert_eq!(sites(&extract(&bare)), vec![s('P', "rabbitmq", "new.q")]);
}

// ---- NATS -------------------------------------------------------------------------------

#[test]
fn nats_publish_request_subscribe_and_messages() {
    let fi = extract(&file(
        &[NATS],
        r#"func run() {
	nc, _ := nats.Connect("nats://x")
	nc.Publish("a.pub", nil)
	nc.Request("a.req", nil, 0)
	nc.PublishMsg(&nats.Msg{Subject: "a.msg"})
	m := nats.NewMsg("a.new")
	nc.PublishMsg(m)
	nc.Subscribe("b.sub", func(m *nats.Msg) {})
	nc.QueueSubscribe("b.queue", "workers", func(m *nats.Msg) {})
	nc.ChanSubscribe("b.chan", nil)
	nc.SubscribeSync("b.wild.>")
	js, _ := nc.JetStream()
	js.Publish("c.js", nil)
	js.Subscribe("c.sub", func(m *nats.Msg) {})
	js.PullSubscribe("c.pull", "dur")
}"#,
    ));
    assert_eq!(
        sites(&fi),
        vec![
            s('C', "nats", "b.chan"),
            s('C', "nats", "b.queue"),
            s('C', "nats", "b.sub"),
            s('C', "nats", "b.wild.>"),
            s('C', "nats", "c.pull"),
            s('C', "nats", "c.sub"),
            s('P', "nats", "a.msg"),
            s('P', "nats", "a.new"),
            s('P', "nats", "a.pub"),
            s('P', "nats", "a.req"),
            s('P', "nats", "c.js"),
        ]
    );
}

#[test]
fn the_new_jetstream_package_takes_the_subject_after_the_context() {
    let fi = extract(&file(
        &[NATS, "\"github.com/nats-io/nats.go/jetstream\""],
        r#"func run(ctx context.Context) {
	nc, _ := nats.Connect("nats://x")
	js, _ := jetstream.New(nc)
	js.Publish(ctx, "d.pub", nil)
	js.PublishMsg(ctx, &nats.Msg{Subject: "d.msg"})
	js.PublishAsync("d.async", nil)
}"#,
    ));
    assert_eq!(
        sites(&fi),
        vec![
            s('P', "nats", "d.async"),
            s('P', "nats", "d.msg"),
            s('P', "nats", "d.pub"),
        ]
    );
}

// ---- Redis ------------------------------------------------------------------------------

#[test]
fn redis_publish_and_subscribe_with_and_without_a_context() {
    let v9 = file(
        &[REDIS9],
        r#"func run(ctx context.Context) {
	rdb := redis.NewClient(&redis.Options{})
	rdb.Publish(ctx, "r.pub", "x")
	ps := rdb.Subscribe(ctx, "r.a", "r.b")
	ps.Subscribe(ctx, "r.c")
	rdb.PSubscribe(ctx, "r.pattern.*")
}"#,
    );
    assert_eq!(
        sites(&extract(&v9)),
        vec![
            s('C', "redis", "r.a"),
            s('C', "redis", "r.b"),
            s('C', "redis", "r.c"),
            s('C', "redis", "r.pattern.*"),
            s('P', "redis", "r.pub"),
        ]
    );
    let v6 = file(
        &["\"github.com/go-redis/redis\""],
        r#"func run() {
	rdb := redis.NewClient(&redis.Options{})
	rdb.Publish("old.pub", "x")
	rdb.Subscribe("old.sub")
}"#,
    );
    assert_eq!(
        sites(&extract(&v6)),
        vec![s('C', "redis", "old.sub"), s('P', "redis", "old.pub")]
    );
}

// ---- SQS / SNS --------------------------------------------------------------------------

#[test]
fn sqs_and_sns_name_the_last_segment_of_a_url_or_arn() {
    let fi = extract(&file(
        &[SQS, SNS, AWS],
        r#"const queueURL = "https://sqs.eu-west-1.amazonaws.com/123456789012/orders-q"

func run(ctx context.Context, q *sqs.Client, n *sns.Client) {
	q.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: aws.String(queueURL), MessageBody: aws.String("x")})
	q.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(queueURL)})
	in := &sqs.SendMessageInput{QueueUrl: aws.String("https://sqs.x/1/built-q")}
	q.SendMessage(ctx, in)
	n.Publish(ctx, &sns.PublishInput{TopicArn: aws.String("arn:aws:sns:eu-west-1:123456789012:order-events")})
}"#,
    ));
    assert_eq!(
        sites(&fi),
        vec![
            s('C', "sqs", "orders-q"),
            s('P', "sns", "order-events"),
            s('P', "sqs", "built-q"),
            s('P', "sqs", "orders-q"),
        ]
    );
}

#[test]
fn a_queue_url_from_get_queue_url_is_the_name_the_file_asked_for() {
    let fi = extract(&file(
        &[SQS, AWS],
        r#"func run(ctx context.Context) {
	cfg := aws.Config{}
	client := sqs.NewFromConfig(cfg)
	out, _ := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String("by-name")})
	client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: out.QueueUrl})
}"#,
    ));
    assert_eq!(sites(&fi), vec![s('P', "sqs", "by-name")]);
}

// ---- Pub/Sub ----------------------------------------------------------------------------

#[test]
fn pubsub_topics_publish_and_a_subscription_is_read_only_through_its_topic() {
    let fi = extract(&file(
        &[PUBSUB],
        r#"func run(ctx context.Context) {
	client, _ := pubsub.NewClient(ctx, "proj")
	client.Topic("orders").Publish(ctx, &pubsub.Message{})
	t := client.Topic("invoices")
	t.Publish(ctx, &pubsub.Message{})
	client.Subscription("orders-sub").Receive(ctx, nil)
	sub, _ := client.CreateSubscription(ctx, "bound-sub", pubsub.SubscriptionConfig{Topic: t})
	sub.Receive(ctx, nil)
}"#,
    ));
    assert_eq!(
        sites(&fi),
        vec![
            s('C', "pubsub", "invoices"),
            s('P', "pubsub", "invoices"),
            s('P', "pubsub", "orders"),
        ]
    );
}

// ---- Names ------------------------------------------------------------------------------

#[test]
fn names_constants_concatenations_and_configuration_reads() {
    let fi = extract(&file(
        &[NATS, "\"os\"", "\"github.com/spf13/viper\""],
        r#"const Prefix = "orders"
const Created = Prefix + ".created"
var Fixed = "fixed.subject"
var FromEnv = os.Getenv("ORDERS_SUBJECT")

func run(param string) {
	nc, _ := nats.Connect("nats://x")
	nc.Publish(Created, nil)
	nc.Publish(Fixed, nil)
	nc.Publish(FromEnv, nil)
	nc.Publish(viper.GetString("nats.subject"), nil)
	nc.Publish(param, nil)
	nc.Publish(Prefix+".inline", nil)
	nc.Publish(fmt.Sprintf("x.%s", param), nil)
	local := "local.subject"
	nc.Publish(local, nil)
}"#,
    ));
    assert_eq!(
        sites(&fi),
        vec![
            s('P', "nats", "${ORDERS_SUBJECT}"),
            s('P', "nats", "${nats.subject}"),
            s('P', "nats", "fixed.subject"),
            s('P', "nats", "local.subject"),
            s('P', "nats", "orders.created"),
            s('P', "nats", "orders.inline"),
        ]
    );
}

/// Canary defaults: none of them may become a channel name (T6, commandment 5). Built at run
/// time so no source line of this repository looks like a credential.
fn canary(shape: &str) -> String {
    format!("{shape}-CANARYX")
}

#[test]
fn a_default_a_variable_falls_back_to_is_never_a_channel_name() {
    let pw = canary("hunter2-prod");
    let tok = canary("tok_live");
    let code = file(
        &[NATS, "\"os\""],
        &format!(
            r#"var Subject = "{pw}"

func init() {{
	if v := os.Getenv("SUBJECT"); v != "" {{
		Subject = v
	}}
}}

func run() {{
	nc, _ := nats.Connect("nats://x")
	nc.Publish(Subject, nil)
	topic := "{tok}"
	if v := os.Getenv("TOPIC"); v != "" {{
		topic = v
	}}
	nc.Subscribe(topic, nil)
	env := os.Getenv("KEYED")
	if env == "" {{
		env = "{pw}"
	}}
	nc.Publish(env, nil)
}}"#
        ),
    );
    let fi = extract(&code);
    let dump = format!("{fi:?}");
    for c in [&pw, &tok] {
        assert!(
            !dump.contains(c.as_str()),
            "{c} reached the file index: {dump}"
        );
    }
    // The variable that holds a configuration key stays that key; the literals overridden by an
    // assignment are not names.
    assert_eq!(sites(&fi), vec![s('P', "nats", "${KEYED}")]);
}

#[test]
fn a_producer_never_publishes_to_a_pattern() {
    let fi = extract(&file(
        &[AMQP, NATS, REDIS9],
        r#"func run(ctx context.Context, ch *amqp.Channel, rdb *redis.Client) {
	nc, _ := nats.Connect("nats://x")
	nc.Publish("n.>", nil)
	nc.Publish("n.*", nil)
	nc.Publish("n.ok", nil)
	nc.Subscribe("n.>", nil)
	ch.Publish("ex", "x.#", false, false, amqp.Publishing{})
	ch.Publish("ex", "x.*", false, false, amqp.Publishing{})
	rdb.Publish(ctx, "r.*", "x")
}"#,
    ));
    assert_eq!(
        sites(&fi),
        vec![s('C', "nats", "n.>"), s('P', "nats", "n.ok")]
    );
}

// ---- Names that must never reach the graph (9.0 T8b review round 1) ---------------------------

#[test]
fn a_query_string_or_fragment_of_a_queue_url_or_arn_is_never_part_of_the_name() {
    let tok = canary("qs-token");
    let code = file(
        &[SQS, SNS, AWS],
        &format!(
            r#"func run(ctx context.Context, q *sqs.Client, n *sns.Client) {{
	q.SendMessage(ctx, &sqs.SendMessageInput{{QueueUrl: aws.String("https://sqs.eu-west-1.amazonaws.com/123456789012/orders?token={tok}")}})
	q.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{{QueueUrl: aws.String("https://sqs.x/1/inbox#{tok}")}})
	n.Publish(ctx, &sns.PublishInput{{TopicArn: aws.String("arn:aws:sns:eu-west-1:123456789012:events?sig={tok}")}})
	q.SendMessage(ctx, &sqs.SendMessageInput{{QueueUrl: aws.String("https://sqs.x/1/?token={tok}")}})
}}"#
        ),
    );
    let fi = extract(&code);
    let dump = format!("{fi:?}");
    assert!(!dump.contains(&tok), "{tok} reached the file index: {dump}");
    assert_eq!(
        sites(&fi),
        vec![
            s('C', "sqs", "inbox"),
            s('P', "sns", "events"),
            s('P', "sqs", "orders"),
        ]
    );
}

#[test]
fn a_literal_that_is_no_name_is_never_a_channel_name() {
    let pw = canary("hunter2-prod");
    let url_pw = canary("hunter2-prod");
    let bearer = canary("Bearer_x");
    let tok = canary("tok_live");
    let random = "a3f9c1e7b2d4";
    let code = file(
        &[NATS, AMQP],
        &format!(
            r#"var defaultSubj = "{pw}"
const amqpURL = "amqp://u:{url_pw}@h/"
const apiKey = "orders.created"

func run(nc *nats.Conn, ch *amqp.Channel) {{
	nc.Publish(defaultSubj, nil)
	nc.Publish("{bearer}", nil)
	nc.Subscribe("{tok}", nil)
	nc.Subscribe("{random}", nil)
	nc.Publish(apiKey, nil)
	ch.Publish("", amqpURL, false, false, amqp.Publishing{{}})
	ch.Publish("", "amqp://u:{url_pw}@h/", false, false, amqp.Publishing{{}})
	nc.Publish("${{k.absent:{pw}}}", nil)
}}"#
        ),
    );
    let fi = extract(&code);
    let dump = format!("{fi:?}");
    for c in [&pw, &url_pw, &bearer, &tok] {
        assert!(
            !dump.contains(c.as_str()),
            "{c} reached the file index: {dump}"
        );
    }
    // Only the placeholder survives, without its default.
    assert_eq!(sites(&fi), vec![s('P', "nats", "${k.absent}")]);
}

#[test]
fn legitimate_names_stay_names() {
    let names = [
        "orders.created",
        "password-reset-jobs",
        "queue-v2",
        "user_signed_up",
        "orders2024",
        "audit.events",
        "devices.>",
        "metrics.*",
        "mail-invoices",
    ];
    let mut body = String::from("func run(nc *nats.Conn) {\n");
    for n in names {
        body.push_str(&format!("\tnc.Subscribe(\"{n}\", nil)\n"));
    }
    body.push_str("}\n");
    let got = sites(&extract(&file(&[NATS], &body)));
    let mut want: Vec<_> = names.iter().map(|n| s('C', "nats", n)).collect();
    want.sort();
    assert_eq!(got, want);
}

// ---- Gating -----------------------------------------------------------------------------

#[test]
fn an_unrelated_type_a_log_line_and_a_missing_import_are_not_sites() {
    let cases: [(&str, String); 8] = [
        (
            "an import in a comment",
            "package svc\n// import \"github.com/nats-io/nats.go\"\nfunc a(nc *nats.Conn) { nc.Publish(\"fp.s\", nil) }\n".into(),
        ),
        (
            "an import in a block comment",
            "package svc\n/* import amqp \"github.com/rabbitmq/amqp091-go\" */\nfunc a(ch *amqp.Channel) { ch.Publish(\"\", \"fp.q\", false, false, amqp.Publishing{}) }\n".into(),
        ),
        (
            "the path only in a string",
            "package svc\nvar doc = \"github.com/nats-io/nats.go\"\nfunc a(nc *nats.Conn) { nc.Publish(\"fp.s\", nil) }\n".into(),
        ),
        (
            "a same-named type of another library",
            file(
                &["\"github.com/acme/legacy/nats\""],
                "func a(nc *nats.Conn) { nc.Publish(\"fp.s\", nil) }",
            ),
        ),
        (
            "a local type of the same name",
            file(
                &[NATS],
                "type Conn struct{}\nfunc (c *Conn) Publish(s string, b []byte) {}\nfunc a(nc *Conn) { nc.Publish(\"fp.s\", nil) }",
            ),
        ),
        (
            "a local type shadowing the package name",
            file(
                &[NATS],
                "func a(nats *Bus) { nc := nats.Connect(\"x\"); nc.Publish(\"fp.s\", nil) }\ntype Bus struct{}",
            ),
        ),
        (
            "a range variable of another type",
            file(
                &[NATS],
                "type Sink interface { Publish(s string, b []byte) }\nfunc a(nc *nats.Conn, sinks []Sink) { for _, nc := range sinks { nc.Publish(\"fp.s\", nil) } }",
            ),
        ),
        (
            "a closure parameter",
            file(
                &[NATS],
                "type Sink interface { Publish(s string, b []byte) }\nfunc a(nc *nats.Conn, each func(func(nc Sink))) { each(func(nc Sink) { nc.Publish(\"fp.s\", nil) }) }",
            ),
        ),
    ];
    for (name, code) in cases {
        let got = sites(&extract(&code));
        assert!(got.is_empty(), "{name}: {got:?}");
    }
    // The real thing still works next to the same shapes.
    let real = file(
        &[NATS],
        "func a(nc *nats.Conn) { nc.Publish(\"ok.s\", nil) }",
    );
    assert_eq!(sites(&extract(&real)), vec![s('P', "nats", "ok.s")]);
}

#[test]
fn a_local_of_another_type_hides_a_field_and_a_package_level_client() {
    let code = file(
        &[NATS],
        r#"var shared *nats.Conn

type Sink struct{}
func (Sink) Publish(s string, b []byte) {}

func a() {
	shared := Sink{}
	shared.Publish("fp.s", nil)
}

func b() {
	shared.Publish("ok.s", nil)
}"#,
    );
    assert_eq!(sites(&extract(&code)), vec![s('P', "nats", "ok.s")]);
}

#[test]
fn a_variable_assigned_after_a_declaration_scope_is_not_confused_with_an_outer_one() {
    // `nc` of the inner block is declared after the first call: the first call is on the outer.
    let code = file(
        &[NATS],
        r#"type Sink struct{}
func (Sink) Publish(s string, b []byte) {}

func a(nc *nats.Conn) {
	nc.Publish("outer.ok", nil)
	{
		nc := Sink{}
		nc.Publish("inner.fp", nil)
	}
	nc.Publish("outer.again", nil)
}"#,
    );
    assert_eq!(
        sites(&extract(&code)),
        vec![s('P', "nats", "outer.again"), s('P', "nats", "outer.ok")]
    );
}

#[test]
fn a_call_on_a_function_result_follows_the_declared_result_type() {
    let code = file(
        &[NATS],
        r#"func connect() (*nats.Conn, error) { return nats.Connect("nats://x") }
func plain() int { return 1 }

func a() {
	nc, _ := connect()
	nc.Publish("via.func", nil)
	n := plain()
	n.Publish("fp.s")
}"#,
    );
    assert_eq!(sites(&extract(&code)), vec![s('P', "nats", "via.func")]);
}

// ---- Kafka and the other brokers do not bleed ----------------------------------------------

#[test]
fn kafka_sites_are_untouched_and_another_brokers_subscribe_is_not_kafka() {
    let code = file(
        &["\"github.com/segmentio/kafka-go\"", NATS, REDIS9],
        r#"func run(ctx context.Context) {
	w := &kafka.Writer{Topic: "orders"}
	_ = w
	nc, _ := nats.Connect("nats://x")
	nc.Subscribe("orders", func(m *nats.Msg) {})
	rdb := redis.NewClient(&redis.Options{})
	rdb.Subscribe(ctx, "orders")
}"#,
    );
    assert_eq!(
        sites(&extract(&code)),
        vec![
            s('C', "nats", "orders"),
            s('C', "redis", "orders"),
            s('P', "kafka", "orders"),
        ]
    );
}

#[test]
fn an_unclaimed_subscribe_keeps_its_kafka_reading() {
    // No library client is shown: the older heuristics of the Go extractor are unchanged.
    let code = "package svc\nfunc a(c Consumer) { c.SubscribeTopics([]string{\"orders\"}, nil) }\n";
    assert_eq!(sites(&extract(code)), vec![s('C', "kafka", "orders")]);
}

// ---- Kafka variants the Go extractor did not read ---------------------------------------------

#[test]
fn sarama_consumer_groups_and_franz_go_are_kafka() {
    let code = file(
        &[
            "\"github.com/IBM/sarama\"",
            "\"github.com/twmb/franz-go/pkg/kgo\"",
        ],
        r#"func run(ctx context.Context, h sarama.ConsumerGroupHandler) {
	group, _ := sarama.NewConsumerGroup([]string{"b:9092"}, "g", nil)
	group.Consume(ctx, []string{"k.one", "k.two"}, h)
	cl, _ := kgo.NewClient(kgo.ConsumeTopics("k.three"), kgo.DefaultProduceTopic("k.four"))
	cl.Produce(ctx, &kgo.Record{Topic: "k.five", Value: nil}, nil)
}"#,
    );
    assert_eq!(
        sites(&extract(&code)),
        vec![
            s('C', "kafka", "k.one"),
            s('C', "kafka", "k.three"),
            s('C', "kafka", "k.two"),
            s('P', "kafka", "k.five"),
            s('P', "kafka", "k.four"),
        ]
    );
    // A `Consume` of another type, and a `Record` of another package, are not Kafka.
    let other = file(
        &["\"github.com/IBM/sarama\""],
        "type Group struct{}\nfunc (Group) Consume(ctx context.Context, t []string, h any) {}\nfunc a(ctx context.Context, g Group) { g.Consume(ctx, []string{\"fp.t\"}, nil); _ = &log.Record{Topic: \"fp.r\"} }",
    );
    assert!(sites(&extract(&other)).is_empty());
}

// ---- Kafka-shaped calls of receivers that are not clients of the new libraries (round 1) -------

const KAFKA_GO: &str = "kafka \"github.com/segmentio/kafka-go\"";
const CONFLUENT: &str = "\"github.com/confluentinc/confluent-kafka-go/kafka\"";

#[test]
fn a_wrapper_of_a_kafka_client_keeps_its_kafka_reading_and_a_foreign_type_stays_hidden() {
    let code = file(
        &[KAFKA_GO, NATS],
        r#"type Consumer struct{ r *kafka.Reader }

func (c *Consumer) Subscribe(topic string) error { return nil }

type Bus struct{}

func (Bus) Subscribe(topic string) {}

type NatsFeed struct{ nc *nats.Conn }

func (NatsFeed) Subscribe(topic string) {}

func Wrap(nc *nats.Conn, r *kafka.Reader) {
	c := &Consumer{r: r}
	c.Subscribe("wrap.local.sub")
	nc.Publish("wrap.nats", nil)
	b := Bus{}
	b.Subscribe("bus.hidden")
	f := NatsFeed{nc: nc}
	f.Subscribe("feed.unknown")
}"#,
    );
    assert_eq!(
        sites(&extract(&code)),
        vec![
            // A wrapper of a nats connection is not a Kafka consumer: no known broker.
            s('C', "?", "feed.unknown"),
            s('C', "kafka", "wrap.local.sub"),
            s('P', "nats", "wrap.nats"),
        ]
    );
}

#[test]
fn an_untyped_receiver_in_a_file_of_the_new_libraries_has_no_known_broker() {
    let code = file(
        &[NATS, CONFLUENT],
        r#"type Sink interface{ Subscribe(string) }

func run(nc *nats.Conn, s Sink, other any, c *kafka.Consumer) {
	nc.Subscribe("typed.nats", nil)
	s.Subscribe("untyped.iface")
	c.SubscribeTopics([]string{"typed.kafka"}, nil)
	p, _ := kafka.NewProducer(nil)
	p.Produce(&kafka.Message{}, nil)
}"#,
    );
    assert_eq!(
        sites(&extract(&code)),
        vec![
            s('C', "?", "untyped.iface"),
            s('C', "kafka", "typed.kafka"),
            s('C', "nats", "typed.nats"),
        ]
    );
    // Without a library of the new set the file reads as before: Kafka.
    let plain = file(
        &[CONFLUENT],
        "type Sink interface{ Subscribe(string) }\nfunc run(s Sink) { s.Subscribe(\"untyped.iface\") }",
    );
    assert_eq!(
        sites(&extract(&plain)),
        vec![s('C', "kafka", "untyped.iface")]
    );
}

#[test]
fn calls_past_the_site_cap_are_not_claimed_so_no_edge_base_had_is_lost() {
    let n = MAX_GO_BROKER_SITES_PER_FILE + 1000;
    let mut body = String::from("func run(nc *nats.Conn) {\n");
    for i in 0..n {
        body.push_str(&format!("\tnc.Subscribe(\"s.{i}\", nil)\n"));
    }
    body.push_str("}\n");
    let fi = extract(&file(&[NATS], &body));
    assert!(fi.go_brokers_capped);
    // Base read all of them (as Kafka); the head keeps every edge: the NATS ones the pass could
    // register, the rest left to the older reading, without a broker.
    assert_eq!(fi.consumers.len(), n);
    let nats = sites(&fi).iter().filter(|(_, b, _)| b == "nats").count();
    assert_eq!(nats, MAX_GO_BROKER_SITES_PER_FILE);
    assert!(sites(&fi).iter().all(|(_, b, _)| b != "kafka"));
}

// ---- Bounds -----------------------------------------------------------------------------

#[test]
fn a_file_past_the_site_cap_is_cut_and_flagged_never_silently() {
    let mut body = String::from("func run(nc *nats.Conn) {\n");
    for i in 0..(MAX_GO_BROKER_SITES_PER_FILE + 50) {
        body.push_str(&format!("\tnc.Publish(\"s.{i}\", nil)\n"));
    }
    body.push_str("}\n");
    let fi = extract(&file(&[NATS], &body));
    assert!(fi.go_brokers_capped);
    assert_eq!(fi.producers.len(), MAX_GO_BROKER_SITES_PER_FILE);
    let small = extract(&file(
        &[NATS],
        "func run(nc *nats.Conn) { nc.Publish(\"a\", nil) }",
    ));
    assert!(!small.go_brokers_capped);
}

#[test]
fn hostile_files_cost_a_bounded_walk_on_a_small_stack() {
    let handle = std::thread::Builder::new()
        .stack_size(2 * 1024 * 1024)
        .spawn(|| {
            // 4000 functions, each with a client call.
            let mut many = String::new();
            for i in 0..4000 {
                many.push_str(&format!(
                    "func f{i}(nc *nats.Conn) {{ nc.Publish(\"s.{i}\", nil) }}\n"
                ));
            }
            let fi = extract(&file(&[NATS], &many));
            assert!(fi.producers.len() <= MAX_GO_BROKER_SITES_PER_FILE);
            // 3000 nested composite literals and parentheses: the guard may reject the file
            // outright; either way no overflow.
            for open in ["[]any{", "(", "map[string]any{\"k\": "] {
                let close = if open == "(" { ")" } else { "}" };
                let deep = format!(
                    "func g(nc *nats.Conn) {{ nc.Publish(\"x\", {}1{}) }}\n",
                    open.repeat(3000),
                    close.repeat(3000)
                );
                let code = file(&[NATS], &deep);
                let fi = PolyglotIndexer::extract(Path::new("svc/x.go"), &code, 0);
                assert!(fi.producers.len() <= MAX_GO_BROKER_SITES_PER_FILE);
            }
            // A chain of 3000 selector calls and a 3000-deep chain of locals.
            let mut chain = String::from("func h(nc *nats.Conn) {\n\tx0 := nc\n");
            for i in 1..3000 {
                chain.push_str(&format!("\tx{i} := x{}\n", i - 1));
            }
            chain.push_str("\tx2999.Publish(\"chain.s\", nil)\n}\n");
            let fi = PolyglotIndexer::extract(Path::new("svc/x.go"), &file(&[NATS], &chain), 0);
            // Past the resolution depth the receiver is unknown: nothing is registered.
            assert!(fi.producers.len() <= 1);
        })
        .unwrap();
    handle.join().unwrap();
}
