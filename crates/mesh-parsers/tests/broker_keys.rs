//! Broker-qualified topic keys (9.0 T7) through `PolyglotIndexer`: which broker each language
//! extractor records, how it survives the per-file cache record, and what the topic pass makes of
//! it on the `examples/eda/multi-broker` fixture (the golden truth is
//! `tests/golden/events/multi-broker.expected.yaml`, written by hand from the sources).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use mesh_core::{Broker, ContractGraph, EdgeKind, NodeId};
use mesh_parsers::languages::PolyglotIndexer;
use mesh_parsers::FileIndex;
use std::path::{Path, PathBuf};

fn extract(file: &str, code: &str) -> FileIndex {
    let fi = PolyglotIndexer::extract(Path::new(file), code, 0);
    assert!(!fi.parse_failed, "{file} must parse");
    fi
}

/// `(role, topic, broker)` of every producer and consumer of `fi`, sorted.
fn sites(fi: &FileIndex) -> Vec<(char, String, Option<Broker>)> {
    let broker_of = |list: &[(usize, Broker)], pair: usize| {
        list.iter().find(|(i, _)| *i == pair).map(|(_, b)| *b)
    };
    let mut out: Vec<_> = fi
        .producers
        .iter()
        .enumerate()
        .map(|(p, (_, t))| ('P', t.to_string(), broker_of(&fi.producer_brokers, p)))
        .chain(
            fi.consumers
                .iter()
                .enumerate()
                .map(|(p, (_, t))| ('C', t.to_string(), broker_of(&fi.consumer_brokers, p))),
        )
        .collect();
    out.sort();
    out
}

#[test]
fn every_js_messaging_family_records_its_broker() {
    let fi = extract(
        "svc/all.ts",
        "import { Kafka } from 'kafkajs';\n\
         import Redis from 'ioredis';\n\
         import amqp from 'amqplib';\n\
         import { PublishCommand } from '@aws-sdk/client-sns';\n\
         import { ReceiveMessageCommand } from '@aws-sdk/client-sqs';\n\
         const producer = new Kafka({ brokers: [] }).producer();\n\
         const redis = new Redis();\n\
         export async function f(ch: amqp.Channel) {\n\
           await producer.send({ topic: 'orders', messages: [] });\n\
           await redis.xadd('orders', '*', 'k', 'v');\n\
           ch.sendToQueue('orders', Buffer.from('x'));\n\
           new PublishCommand({ TopicArn: 'arn:aws:sns:eu-west-1:1:orders' });\n\
           new ReceiveMessageCommand({ QueueUrl: 'https://sqs.eu-west-1.amazonaws.com/1/orders' });\n\
         }\n",
    );
    assert_eq!(
        sites(&fi),
        vec![
            ('C', "orders".into(), Some(Broker::Sqs)),
            ('P', "orders".into(), Some(Broker::Kafka)),
            ('P', "orders".into(), Some(Broker::RabbitMq)),
            ('P', "orders".into(), Some(Broker::Redis)),
            ('P', "orders".into(), Some(Broker::Sns)),
        ]
    );
}

#[test]
fn a_nestjs_handler_and_client_have_no_statically_known_broker() {
    let fi = extract(
        "svc/legacy.ts",
        "import { Controller } from '@nestjs/common';\n\
         import { EventPattern } from '@nestjs/microservices';\n\
         @Controller()\n\
         export class Legacy {\n\
           @EventPattern('orders')\n\
           handle() {}\n\
           ship() { this.client.emit('orders', {}); }\n\
         }\n",
    );
    let got = sites(&fi);
    assert!(got.iter().any(|s| s.0 == 'C' && s.1 == "orders"), "{got:?}");
    assert!(got.iter().all(|s| s.2.is_none()), "{got:?}");
}

#[test]
fn the_kafka_only_languages_tag_their_sites_kafka() {
    let go = extract(
        "ledger/pub.go",
        "package main\n\nimport \"github.com/segmentio/kafka-go\"\n\n\
         func New() *kafka.Writer {\n\treturn &kafka.Writer{Topic: \"orders\"}\n}\n",
    );
    assert_eq!(
        sites(&go),
        vec![('P', "orders".into(), Some(Broker::Kafka))]
    );
    let java = extract(
        "src/Listener.java",
        "class Listener {\n  @KafkaListener(topics = \"orders\")\n  void on(String m) {}\n}\n",
    );
    assert_eq!(
        sites(&java),
        vec![('C', "orders".into(), Some(Broker::Kafka))]
    );
}

#[test]
fn the_file_record_round_trips_with_its_brokers_and_reads_an_older_one() {
    let fi = extract(
        "svc/kafka.ts",
        "import { Kafka } from 'kafkajs';\n\
         const producer = new Kafka({ brokers: [] }).producer();\n\
         export async function f() { await producer.send({ topic: 'orders', messages: [] }); }\n",
    );
    assert_eq!(fi.producer_brokers.len(), 1);
    let json = serde_json::to_string(&fi).expect("serialize");
    assert!(json.contains("\"kafka\""), "{json}");
    let back: FileIndex = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(sites(&back), sites(&fi));

    // A record written before the broker lists existed (schema 15) reads as "unknown".
    let mut value: serde_json::Value = serde_json::from_str(&json).expect("value");
    let object = value.as_object_mut().expect("object");
    object.remove("producer_brokers");
    object.remove("consumer_brokers");
    let old: FileIndex = serde_json::from_value(value).expect("old record");
    assert_eq!(old.producers.len(), 1);
    assert!(old.producer_brokers.is_empty());
}

#[test]
fn merging_two_records_keeps_each_brokers_pair_index() {
    let mut a = extract(
        "svc/a.ts",
        "import { Kafka } from 'kafkajs';\n\
         const producer = new Kafka({ brokers: [] }).producer();\n\
         export async function f() { await producer.send({ topic: 'one', messages: [] }); }\n",
    );
    let b = extract(
        "svc/b.ts",
        "import Redis from 'ioredis';\nconst r = new Redis();\n\
         export async function g() { await r.xadd('two', '*', 'k', 'v'); }\n",
    );
    a.merge(b);
    assert_eq!(
        sites(&a),
        vec![
            ('P', "one".into(), Some(Broker::Kafka)),
            ('P', "two".into(), Some(Broker::Redis)),
        ]
    );
}

#[test]
fn tagging_never_overrides_a_known_broker() {
    let mut fi = FileIndex::default();
    fi.producers.push((0, "a".into()));
    fi.producers.push((0, "b".into()));
    fi.producer_brokers.push((1, Broker::Redis));
    fi.tag_brokers(Broker::Kafka);
    let mut got = fi.producer_brokers.clone();
    got.sort();
    assert_eq!(got, vec![(0, Broker::Kafka), (1, Broker::Redis)]);
}

// ---- the multi-broker fixture ---------------------------------------------------------

fn fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/eda/multi-broker")
}

fn index_fixture() -> ContractGraph {
    let mut files = Vec::new();
    for dir in ["src", "ledger"] {
        for entry in std::fs::read_dir(fixture().join(dir)).expect("fixture dir") {
            let path = entry.expect("entry").path();
            if path.extension().is_some_and(|x| x == "ts" || x == "go") {
                files.push(path);
            }
        }
    }
    files.sort();
    let mut graph = ContractGraph::new();
    for f in files {
        let content = std::fs::read_to_string(&f).expect("read");
        PolyglotIndexer::index_file(&f, &content, 0, &mut graph);
    }
    graph.reconcile_edges();
    graph
}

fn hub_names(graph: &ContractGraph) -> Vec<String> {
    let mut v: Vec<String> = graph
        .all_nodes()
        .filter(|n| n.package == "event-bus")
        .map(|n| n.name.to_string())
        .collect();
    v.sort();
    v
}

/// File name of the site on the other end of every topic edge of `hub`, with its role.
fn ends(graph: &ContractGraph, hub: &str) -> Vec<(char, String)> {
    let hub_id: NodeId = graph
        .all_nodes()
        .find(|n| n.package == "event-bus" && n.name == hub)
        .map(|n| n.id)
        .expect("hub");
    let file = |id: NodeId| {
        graph
            .get_node(id)
            .and_then(|n| n.file_path.file_name())
            .map(|f| f.to_string_lossy().to_string())
            .unwrap_or_default()
    };
    let mut out: Vec<(char, String)> = graph
        .all_edges()
        .iter()
        .filter_map(|e| match e.kind {
            EdgeKind::Produces if e.to == hub_id => Some(('P', file(e.from))),
            EdgeKind::Consumes if e.from == hub_id => Some(('C', file(e.to))),
            _ => None,
        })
        .collect();
    out.sort();
    out
}

#[test]
fn the_fixture_keeps_six_orders_channels_apart() {
    let graph = index_fixture();
    assert_eq!(
        hub_names(&graph),
        vec![
            "events.*",
            "events.created",
            "invoices",
            "kafka:orders",
            "nats:orders",
            "rabbitmq:orders",
            "redis:orders",
            "shipments",
            "sns:orders",
            "sqs:orders",
        ]
    );
    let pc = |role: char, file: &str| (role, file.to_string());
    // Kafka crosses languages: TypeScript and Go share one channel.
    assert_eq!(
        ends(&graph, "kafka:orders"),
        vec![
            pc('C', "consumer.go"),
            pc('C', "kafka.ts"),
            pc('C', "legacy.ts"),
            pc('P', "kafka.ts"),
            pc('P', "publisher.go"),
        ]
    );
    // `legacy.ts` is a NestJS handler of unknown broker: it is on every channel of `orders`.
    assert_eq!(
        ends(&graph, "rabbitmq:orders"),
        vec![
            pc('C', "legacy.ts"),
            pc('C', "rabbit.ts"),
            pc('P', "rabbit.ts")
        ]
    );
    assert_eq!(
        ends(&graph, "sns:orders"),
        vec![pc('C', "legacy.ts"), pc('P', "sns.ts")]
    );
    assert_eq!(
        ends(&graph, "sqs:orders"),
        vec![pc('C', "legacy.ts"), pc('C', "sqs.ts")]
    );
    // A NATS wildcard does not hear a Redis stream of a matching subject.
    assert_eq!(ends(&graph, "events.*"), vec![pc('C', "nats.ts")]);
    assert_eq!(ends(&graph, "events.created"), vec![pc('P', "redis.ts")]);
    // A name used on one broker only keeps its plain key, across languages.
    assert_eq!(
        ends(&graph, "invoices"),
        vec![pc('C', "consumer.go"), pc('P', "billing.ts")]
    );
}

#[test]
fn the_fixture_is_deterministic_across_runs() {
    let a = index_fixture().canonical_lines();
    let b = index_fixture().canonical_lines();
    assert_eq!(a, b);
}
