//! 9.0 T6: a topic read from configuration is recorded as a `${key}` placeholder and
//! resolved when the file index is folded; nothing is guessed for an unknown key.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use mesh_core::{ConfigEntry, ConfigValues, ContractGraph, EdgeConfidence, EdgeKind};
use mesh_parsers::{FileIndex, PolyglotIndexer};
use std::path::Path;

fn index(file: &str, code: &str) -> FileIndex {
    let fi = PolyglotIndexer::extract(Path::new(file), code, 0);
    assert!(!fi.parse_failed, "{file} must parse");
    fi
}

/// Sorted `(role, topic)` of a file index.
fn recorded(fi: &FileIndex) -> Vec<(char, String)> {
    let mut out: Vec<(char, String)> = fi
        .producers
        .iter()
        .map(|(_, t)| ('P', t.to_string()))
        .chain(fi.consumers.iter().map(|(_, t)| ('C', t.to_string())))
        .collect();
    out.sort();
    out
}

fn p(t: &str) -> (char, String) {
    ('P', t.to_string())
}

fn c(t: &str) -> (char, String) {
    ('C', t.to_string())
}

#[test]
fn typescript_env_and_config_service_reads_become_placeholders() {
    let fi = index(
        "svc/a.ts",
        "import { Kafka } from 'kafkajs';\n\
         const k = new Kafka({ brokers: ['b'] });\n\
         const producer = k.producer();\n\
         const consumer = k.consumer({ groupId: 'g' });\n\
         const TOPIC = process.env.KAFKA_TOPIC;\n\
         const Cfg = { out: process.env['OUT_TOPIC'] };\n\
         export async function run(configService: any) {\n\
           await producer.send({ topic: TOPIC, messages: [] });\n\
           await producer.send({ topic: Cfg.out, messages: [] });\n\
           await consumer.subscribe({ topic: configService.get<string>('IN_TOPIC') });\n\
           await consumer.subscribe({ topic: this.config.getOrThrow('IN2') });\n\
           await consumer.subscribe({ topic: process.env.WITH_DEFAULT ?? 'dflt' });\n\
           await consumer.subscribe({ topic: process.env.WITH_OR || 'dflt2' });\n\
           await consumer.subscribe({ topic: `${process.env.PREFIX}.suffix` });\n\
         }\n",
    );
    assert_eq!(
        recorded(&fi),
        vec![
            c("${IN2}"),
            c("${IN_TOPIC}"),
            c("${PREFIX}.suffix"),
            c("${WITH_DEFAULT:dflt}"),
            c("${WITH_OR:dflt2}"),
            p("${KAFKA_TOPIC}"),
            p("${OUT_TOPIC}"),
        ]
    );
}

#[test]
fn typescript_reads_that_are_not_config_stay_unrecorded() {
    // A parameter, a random call, a `get` on a non-config receiver, a computed key.
    let fi = index(
        "svc/b.ts",
        "import { Kafka } from 'kafkajs';\n\
         const k = new Kafka({ brokers: ['b'] });\n\
         const producer = k.producer();\n\
         export async function run(topic: string, cache: any, key: string) {\n\
           await producer.send({ topic, messages: [] });\n\
           await producer.send({ topic: compute(), messages: [] });\n\
           await producer.send({ topic: cache.get('T'), messages: [] });\n\
           await producer.send({ topic: process.env[key], messages: [] });\n\
         }\n",
    );
    assert!(recorded(&fi).is_empty(), "{:?}", recorded(&fi));
}

#[test]
fn http_routes_never_read_config_placeholders() {
    // `Names::config` is for messaging sites only: a route prefix from `process.env` is
    // not a path.
    let fi = index(
        "svc/c.ts",
        "import { Controller, Get } from '@nestjs/common';\n\
         @Controller(process.env.PREFIX)\n\
         export class C { @Get('x') x() {} }\n",
    );
    assert!(
        fi.nodes.iter().all(|n| !n.name.contains("${") && !n.signature.as_deref().unwrap_or("").contains("${")),
        "{:?}",
        fi.nodes.iter().map(|n| (&n.name, &n.signature)).collect::<Vec<_>>()
    );
}

#[test]
fn go_getenv_and_viper_reads_become_placeholders_never_the_key_itself() {
    let fi = index(
        "svc/main.go",
        "package main\n\
         import (\n\
         \t\"os\"\n\
         \tkafka \"github.com/segmentio/kafka-go\"\n\
         )\n\
         func main() {\n\
         \ttopic := os.Getenv(\"LOCAL_TOPIC\")\n\
         \tw := kafka.NewWriter(kafka.WriterConfig{Topic: os.Getenv(\"PAYMENTS_TOPIC\")})\n\
         \tr := kafka.NewReader(kafka.ReaderConfig{Topic: viper.GetString(\"app.in\")})\n\
         \tx := kafka.NewReader(kafka.ReaderConfig{Topic: topic})\n\
         \t_, _, _ = w, r, x\n\
         }\n",
    );
    let got = recorded(&fi);
    assert!(got.contains(&p("${PAYMENTS_TOPIC}")), "{got:?}");
    assert!(got.contains(&c("${app.in}")), "{got:?}");
    assert!(got.contains(&c("${LOCAL_TOPIC}")), "{got:?}");
    assert!(
        !got.iter()
            .any(|(_, t)| t == "PAYMENTS_TOPIC" || t == "app.in" || t == "LOCAL_TOPIC"),
        "the env key is never a topic: {got:?}"
    );
}

#[test]
fn python_environ_reads_become_placeholders() {
    let fi = index(
        "svc/app.py",
        "import os\n\
         from confluent_kafka import Producer\n\
         TOPIC = os.environ.get('KAFKA_TOPIC', 'orders')\n\
         def send():\n\
         \x20   p = Producer({})\n\
         \x20   p.produce(TOPIC, b'x')\n\
         \x20   p.produce(os.environ['OTHER'], b'y')\n\
         \x20   p.produce(os.getenv('THIRD'), b'z')\n",
    );
    assert_eq!(
        recorded(&fi),
        vec![p("${KAFKA_TOPIC:orders}"), p("${OTHER}"), p("${THIRD}")]
    );
}

#[test]
fn java_value_injected_fields_and_listener_placeholders_are_recorded() {
    let fi = index(
        "src/Svc.java",
        "package a;\n\
         public class Svc {\n\
         \x20 @Value(\"${app.topics.orders}\") private String ordersTopic;\n\
         \x20 @Value(\"${app.topics.dflt:fallback}\") private final String dfltTopic;\n\
         \x20 @KafkaListener(topics = \"${app.topics.in:in.default}\")\n\
         \x20 public void on(String m) {}\n\
         \x20 public void go() { kafkaTemplate.send(ordersTopic, \"x\"); }\n\
         \x20 public void go2() { kafkaTemplate.send(this.dfltTopic, \"y\"); }\n\
         \x20 public void go3() { kafkaTemplate.send(other, \"z\"); }\n\
         }\n",
    );
    assert_eq!(
        recorded(&fi),
        vec![
            c("${app.topics.in:in.default}"),
            p("${app.topics.dflt:fallback}"),
            p("${app.topics.orders}"),
        ]
    );
}

fn entry(key: &str, value: &str, weak: bool) -> ConfigEntry {
    ConfigEntry {
        key: key.into(),
        value: value.into(),
        weak,
    }
}

/// `(kind, confidence, hub name)` of every topic edge of the graph, sorted.
fn edges(g: &ContractGraph) -> Vec<(EdgeKind, EdgeConfidence, String)> {
    let mut out: Vec<_> = g
        .all_edges()
        .iter()
        .filter(|e| matches!(e.kind, EdgeKind::Produces | EdgeKind::Consumes))
        .map(|e| {
            let hub = if e.kind == EdgeKind::Produces {
                e.to
            } else {
                e.from
            };
            (
                e.kind,
                e.confidence,
                g.get_node(hub).unwrap().name.to_string(),
            )
        })
        .collect();
    out.sort_by(|a, b| (format!("{:?}", a.0), &a.2).cmp(&(format!("{:?}", b.0), &b.2)));
    out
}

#[test]
fn folding_resolves_placeholders_against_the_configuration() {
    let code = "package a;\npublic class Svc {\n  @KafkaListener(topics = \"${t}\")\n  public void on(String m) {}\n  @KafkaListener(topics = \"${missing}\")\n  public void off(String m) {}\n  @KafkaListener(topics = \"${missing:fb}\")\n  public void dflt(String m) {}\n}\n";
    let build = |entries: &[ConfigEntry]| {
        let mut config = ConfigValues::default();
        config.set_file(Path::new("application.yml"), entries.to_vec());
        config.rebuild();
        let mut g = ContractGraph::new();
        PolyglotIndexer::extract(Path::new("src/Svc.java"), code, 0).apply_with(&mut g, &config);
        g.reconcile_edges();
        edges(&g)
    };
    // One value: exact. No value and no default: the placeholder, heuristic, never a guess.
    // A default alone: weak.
    assert_eq!(
        build(&[entry("t", "orders", false)]),
        vec![
            (
                EdgeKind::Consumes,
                EdgeConfidence::Heuristic,
                "${missing}".to_string()
            ),
            (
                EdgeKind::Consumes,
                EdgeConfidence::Heuristic,
                "fb".to_string()
            ),
            (
                EdgeKind::Consumes,
                EdgeConfidence::Exact,
                "orders".to_string()
            ),
        ]
    );
    // Two values: both, heuristic. A template value alone: heuristic.
    let two = build(&[entry("t", "a", false), entry("t", "b", false)]);
    assert!(two.contains(&(
        EdgeKind::Consumes,
        EdgeConfidence::Heuristic,
        "a".to_string()
    )));
    assert!(two.contains(&(
        EdgeKind::Consumes,
        EdgeConfidence::Heuristic,
        "b".to_string()
    )));
    let weak = build(&[entry("t", "tmpl", true)]);
    assert!(weak.contains(&(
        EdgeKind::Consumes,
        EdgeConfidence::Heuristic,
        "tmpl".to_string()
    )));
}
