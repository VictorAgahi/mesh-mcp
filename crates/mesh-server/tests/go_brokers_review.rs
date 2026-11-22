#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! 9.0 T8b, through the whole stack (index, graph, tools):
//! - a default a Go variable falls back to is never a channel name, in the graph or in any tool
//!   answer (T6, commandment 5);
//! - a same-file re-emit never crosses from one broker to another;
//! - a `Subscribe("x")` of a NATS or Redis client is not also a Kafka consumer of `x`.

use mesh_core::{AppState, AuditLogger, BackgroundRescanEngine, Config};
use mesh_server::tools::ToolRegistry;
use serde_json::{json, Value};
use std::path::Path;
use std::sync::Arc;

fn write(root: &Path, rel: &str, text: &str) {
    let p = root.join(rel);
    std::fs::create_dir_all(p.parent().unwrap()).unwrap();
    std::fs::write(p, text).unwrap();
}

fn state_for(root: &Path) -> Arc<AppState> {
    let cfg =
        Config::load_from_str("[workspace]\nname = \"t\"\nversion = \"0\"\nroots = [\".\"]\n")
            .expect("config");
    let audit = Arc::new(AuditLogger::new_in_memory().expect("audit"));
    let rescan = Arc::new(BackgroundRescanEngine::new().expect("rescan"));
    let state = Arc::new(AppState::new(cfg, vec![root.to_path_buf()], audit, rescan));
    let snap = mesh_server::WorkspaceIndexer::build_snapshot(
        &state.config,
        &state.allowed_roots,
        None,
        None,
        None,
    );
    state.install_snapshot(snap);
    state
}

async fn text(state: &Arc<AppState>, tool: &str, args: Value) -> String {
    match ToolRegistry::call_tool(tool, args, state.clone()).await {
        Ok(v) => v["content"][0]["text"].as_str().unwrap_or("").to_string(),
        Err(e) => format!("{e:?}"),
    }
}

/// Everything an agent or a graph consumer can read.
async fn everything(state: &Arc<AppState>, targets: &[&str]) -> String {
    let snap = state.snapshot();
    let mut out = format!(
        "{:?}\n{:?}\n{:?}\n",
        snap.contract_graph.all_nodes().collect::<Vec<_>>(),
        snap.contract_graph.all_edges(),
        snap.config_values
    );
    drop(snap);
    out += &text(state, "visualize_mesh", json!({})).await;
    out += &text(state, "visualize_mesh", json!({"format": "mermaid"})).await;
    for t in targets {
        out += &text(state, "analyze_impact", json!({"target": t, "depth": 3})).await;
        out += &text(state, "trace_flow", json!({"entry": t})).await;
        out += &text(state, "smart_search", json!({"query": t})).await;
        out += &text(state, "find_dependents", json!({"target": t})).await;
    }
    out
}

/// Canaries are built at run time: no source line of this repository looks like a credential.
fn canaries() -> [String; 3] {
    [
        format!("{}-prod-CANARYX", "hunter2"),
        format!("Bearer_{}", "CANARYB"),
        format!("{}_pw_CANARYC", "tok"),
    ]
}

fn defaults_file() -> String {
    let [pw, bearer, tok] = canaries();
    format!(
        r#"package p

import (
	"os"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/nats-io/nats.go"
)

var Subject = "{pw}"

func init() {{
	if v := os.Getenv("SUBJECT"); v != "" {{
		Subject = v
	}}
}}

func Run(nc *nats.Conn, ch *amqp.Channel) {{
	nc.Publish(Subject, nil)
	topic := os.Getenv("PRESENT_TOPIC")
	if topic == "" {{
		topic = "{bearer}"
	}}
	nc.Publish(topic, nil)
	absent := os.Getenv("ABSENT_TOPIC")
	if absent == "" {{
		absent = "{tok}"
	}}
	nc.Subscribe(absent, nil)
	ch.Publish("", os.Getenv("ABSENT_QUEUE"), false, false, amqp.Publishing{{}})
}}
"#
    )
}

#[tokio::test]
async fn a_default_never_reaches_the_graph_or_a_tool_answer() {
    let tmp = tempfile::tempdir().unwrap();
    let root = dunce::canonicalize(tmp.path()).unwrap();
    write(&root, "svc/defaults.go", &defaults_file());
    write(&root, ".env.example", "PRESENT_TOPIC=plain.topic.value\n");
    let state = state_for(&root);
    let all = everything(
        &state,
        &[
            "${PRESENT_TOPIC}",
            "${ABSENT_TOPIC}",
            "${ABSENT_QUEUE}",
            "${SUBJECT}",
            "plain.topic.value",
        ],
    )
    .await;
    for canary in canaries() {
        assert!(!all.contains(&canary), "{canary} leaked:\n{all}");
    }
    // The key the configuration resolves is resolved; the others stay visible and counted.
    assert!(all.contains("plain.topic.value"), "{all}");
    let snap = state.snapshot();
    assert!(
        snap.health.config_topics_unresolved >= 2,
        "{:?}",
        snap.health
    );
}

const KAFKA_AND_RABBIT: &str = r#"package p

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/segmentio/kafka-go"
)

func Emit() *kafka.Writer { return &kafka.Writer{Topic: "orders"} }

func Listen(ch *amqp.Channel) { ch.Consume("orders", "c", true, false, false, false, nil) }
"#;

const NATS_AND_REDIS: &str = r#"package p

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

func Reader() *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{Topic: "audit.redis"})
}

func Emit(ctx context.Context, rdb *redis.Client, nc *nats.Conn) {
	rdb.Publish(ctx, "audit.redis", "x")
	nc.Subscribe("orders", nil)
}
"#;

#[tokio::test]
async fn a_same_file_reemit_never_crosses_from_one_broker_to_another() {
    let tmp = tempfile::tempdir().unwrap();
    let root = dunce::canonicalize(tmp.path()).unwrap();
    write(&root, "svc/mix.go", KAFKA_AND_RABBIT);
    write(&root, "svc/two.go", NATS_AND_REDIS);
    let state = state_for(&root);
    for tool in ["analyze_impact", "trace_flow"] {
        let args = |t: &str| {
            if tool == "analyze_impact" {
                json!({"target": t, "depth": 3, "limit": 200})
            } else {
                json!({"entry": t})
            }
        };
        // RabbitMQ `orders` is consumed in a file that also produces `orders` on Kafka.
        let rabbit = text(&state, tool, args("rabbitmq:orders")).await;
        assert!(!rabbit.contains("mix.go:8"), "{tool}: {rabbit}");
        assert!(!rabbit.contains("kafka:orders"), "{tool}: {rabbit}");
        // A Kafka reader never re-emits to the Redis producer of its own file.
        let kafka = text(&state, tool, args("kafka:audit.redis")).await;
        assert!(!kafka.contains("two.go:16"), "{tool}: {kafka}");
        assert!(!kafka.contains("redis:audit.redis"), "{tool}: {kafka}");
    }
    // The NATS `Subscribe("orders")` is a NATS consumer and nothing else.
    let snap = state.snapshot();
    let hubs: Vec<String> = snap
        .contract_graph
        .all_nodes()
        .map(|n| n.name.to_string())
        .collect();
    assert!(hubs.iter().any(|n| n == "nats:orders"), "{hubs:?}");
    assert!(hubs.iter().any(|n| n == "kafka:orders"), "{hubs:?}");
    assert!(hubs.iter().any(|n| n == "rabbitmq:orders"), "{hubs:?}");
}

/// Round 1: a query string of a queue URL, a credential-shaped literal and a URL with userinfo
/// reach neither the graph, a tool answer nor the parse cache (database, `-wal`, `-shm`).
#[tokio::test]
async fn a_secret_shaped_name_reaches_no_graph_tool_or_cache_file() {
    let qs = format!("{}-CANARYQ", "qstoken");
    let pw = format!("{}-prod-CANARYV", "hunter2");
    let url_pw = format!("{}-prod-CANARYPW", "hunter2");
    let code = format!(
        r#"package p

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/nats-io/nats.go"
)

var defaultSubj = "{pw}"

const amqpURL = "amqp://u:{url_pw}@h/"

func Run(ctx context.Context, nc *nats.Conn, ch *amqp.Channel, q *sqs.Client, n *sns.Client) {{
	nc.Publish(defaultSubj, nil)
	ch.Publish("", amqpURL, false, false, amqp.Publishing{{}})
	q.SendMessage(ctx, &sqs.SendMessageInput{{QueueUrl: aws.String("https://sqs.eu-west-1.amazonaws.com/1/jobs?token={qs}")}})
	n.Publish(ctx, &sns.PublishInput{{TopicArn: aws.String("arn:aws:sns:eu-west-1:1:alerts?sig={qs}")}})
}}
"#
    );
    let tmp = tempfile::tempdir().unwrap();
    let root = dunce::canonicalize(tmp.path()).unwrap();
    write(&root, "svc/secrets.go", &code);
    let db = tmp.path().join("cache").join("index-cache.db");
    let cache = mesh_core::PersistentIndexCache::open(db.clone(), 1 << 30).unwrap();
    let cfg =
        Config::load_from_str("[workspace]\nname = \"t\"\nversion = \"0\"\nroots = [\".\"]\n")
            .unwrap();
    let audit = Arc::new(AuditLogger::new_in_memory().unwrap());
    let rescan = Arc::new(BackgroundRescanEngine::new().unwrap());
    let state = Arc::new(AppState::new(cfg, vec![root.clone()], audit, rescan));
    let snap = mesh_server::WorkspaceIndexer::build_snapshot(
        &state.config,
        &state.allowed_roots,
        None,
        None,
        Some(&cache),
    );
    state.install_snapshot(snap);
    // `smart_search` shows the file's own declarations (source text, the constants of the file
    // included): it is left out, the graph and the other tools are what a name can reach.
    let mut all = {
        let snap = state.snapshot();
        format!(
            "{:?}\n{:?}\n",
            snap.contract_graph.all_nodes().collect::<Vec<_>>(),
            snap.contract_graph.all_edges()
        )
    };
    all += &text(&state, "visualize_mesh", json!({})).await;
    all += &text(&state, "visualize_mesh", json!({"format": "mermaid"})).await;
    for t in ["jobs", "alerts", "sqs:jobs", "sns:alerts"] {
        all += &text(&state, "analyze_impact", json!({"target": t, "depth": 3})).await;
        all += &text(&state, "trace_flow", json!({"entry": t})).await;
        all += &text(&state, "find_dependents", json!({"target": t})).await;
    }
    // The names the file really shows are there; the secrets are not.
    assert!(all.contains("jobs") && all.contains("alerts"), "{all}");
    assert!(db.exists(), "the cache was written");
    for canary in [&qs, &pw, &url_pw] {
        assert!(!all.contains(canary.as_str()), "{canary} leaked:\n{all}");
        for suffix in ["", "-wal", "-shm"] {
            let mut path = db.as_os_str().to_os_string();
            path.push(suffix);
            if let Ok(bytes) = std::fs::read(std::path::PathBuf::from(path)) {
                let hay = String::from_utf8_lossy(&bytes);
                assert!(
                    !hay.contains(canary.as_str()),
                    "{canary} in cache file {suffix}"
                );
            }
        }
    }
}
