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
fn a_name_declared_with_two_types_is_read_as_nothing() {
    let code = r#"package p;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
public class C {
    static class A { RabbitTemplate client; void go(Object m) { client.convertAndSend("a.q", m); } }
    static class B { Bridge client; void go(Object m) { client.convertAndSend("b.q", m); } }
}
"#;
    assert!(sites(&extract(code)).is_empty());
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
