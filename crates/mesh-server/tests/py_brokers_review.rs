#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! 9.0 T8c through the whole stack (index, graph, tools):
//! - a placeholder default or a credential-shaped name of a Python broker site is never a channel
//!   name, in the graph or in any tool answer (T6, commandment 5);
//! - a same-file re-emit never crosses from one broker to another.

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
        out += &text(state, "trace_flow", json!({"target": t})).await;
        out += &text(state, "smart_search", json!({"query": t})).await;
        out += &text(state, "find_dependents", json!({"target": t})).await;
    }
    out
}

/// Credential-shaped values built at runtime so no key-shaped literal sits in the source.
fn secret_like() -> String {
    format!("sk_{}_{}", "live", "ABCDEF0123456789")
}

#[tokio::test]
async fn a_default_or_a_credential_never_reaches_the_graph_or_a_tool_answer() {
    let tmp = tempfile::tempdir().unwrap();
    let root = dunce::canonicalize(tmp.path()).unwrap();
    let code = format!(
        r#"import os
import pika
import redis

r = redis.Redis()
channel = pika.BlockingConnection(None).channel()
QUEUE = os.getenv("APP_QUEUE", "hunter2-prod-CANARYX")
OTHER = os.environ.get("APP_OTHER", "Bearer_CANARYB")

def run():
    r.publish(QUEUE, "x")
    r.publish(OTHER, "x")
    r.publish(os.getenv("APP_INLINE", "inline-CANARYI"), "x")
    r.publish("{secret}", "x")
    channel.basic_consume(queue="{secret}", on_message_callback=None)
    channel.basic_publish(exchange="", routing_key=os.getenv("APP_RK", "rk-CANARYR"), body=b"x")
"#,
        secret = secret_like()
    );
    write(&root, "src/svc/defaults.py", &code);
    let state = state_for(&root);
    let all = everything(
        &state,
        &["${APP_QUEUE}", "${APP_OTHER}", "${APP_INLINE}", "${APP_RK}"],
    )
    .await;
    for canary in [
        "hunter2-prod-CANARYX",
        "Bearer_CANARYB",
        "inline-CANARYI",
        "rk-CANARYR",
        secret_like().as_str(),
    ] {
        assert!(!all.contains(canary), "{canary} leaked:\n{all}");
    }
    // The keys are visible and counted as unresolved, not guessed.
    assert!(all.contains("APP_QUEUE"), "{all}");
    let snap = state.snapshot();
    assert!(
        snap.health.config_topics_unresolved >= 4,
        "{:?}",
        snap.health
    );
}

const KAFKA_AND_RABBIT: &str = r#"import pika
from confluent_kafka import Producer

producer = Producer({})
channel = pika.BlockingConnection(None).channel()

def out():
    producer.produce("orders", b"x")

def inbound(cb):
    channel.basic_consume(queue="orders", on_message_callback=cb)
"#;

const REDIS_AND_KAFKA: &str = r#"import redis
from confluent_kafka import Consumer

r = redis.Redis()
consumer = Consumer({})

def out():
    r.publish("audit.redis", "x")

def inbound():
    consumer.subscribe(["audit.redis"])
"#;

#[tokio::test]
async fn a_same_file_reemit_never_crosses_from_one_broker_to_another() {
    let tmp = tempfile::tempdir().unwrap();
    let root = dunce::canonicalize(tmp.path()).unwrap();
    write(&root, "src/svc/mix.py", KAFKA_AND_RABBIT);
    write(&root, "src/svc/two.py", REDIS_AND_KAFKA);
    let state = state_for(&root);
    for tool in ["analyze_impact", "trace_flow"] {
        let args = |t: &str| {
            if tool == "analyze_impact" {
                json!({"target": t, "depth": 3, "limit": 200})
            } else {
                json!({"target": t})
            }
        };
        // The RabbitMQ consumer of `orders` sits in the file that produces it on Kafka.
        let rabbit = text(&state, tool, args("rabbitmq:orders")).await;
        assert!(
            rabbit.contains("mix.py:11"),
            "{tool}: not a vacuous answer: {rabbit}"
        );
        assert!(!rabbit.contains("mix.py:8"), "{tool}: {rabbit}");
        assert!(!rabbit.contains("kafka:orders"), "{tool}: {rabbit}");
        // A Kafka consumer never re-emits to the Redis producer of its own file.
        let kafka = text(&state, tool, args("kafka:audit.redis")).await;
        assert!(
            kafka.contains("two.py:10"),
            "{tool}: not a vacuous answer: {kafka}"
        );
        assert!(!kafka.contains("two.py:8"), "{tool}: {kafka}");
        assert!(!kafka.contains("redis:audit.redis"), "{tool}: {kafka}");
    }
}

#[tokio::test]
async fn a_credential_shaped_name_reaches_no_graph_tool_answer_or_cache_file() {
    let token = secret_like();
    let canaries = [
        "Bearer_CANARYB".to_string(),
        "hunter2-prod-CANARYL".to_string(),
        token.clone(),
    ];
    let tmp = tempfile::tempdir().unwrap();
    let root = dunce::canonicalize(tmp.path().join("ws")).unwrap_or_else(|_| {
        std::fs::create_dir_all(tmp.path().join("ws")).unwrap();
        dunce::canonicalize(tmp.path().join("ws")).unwrap()
    });
    let code = format!(
        r#"import pika

ch = pika.BlockingConnection(None).channel()

def run():
    ch.basic_publish(exchange="", routing_key="Bearer_CANARYB", body=b"x")
    ch.basic_publish(exchange="", routing_key="hunter2-prod-CANARYL", body=b"x")
    ch.basic_publish(exchange="", routing_key="{token}", body=b"x")
    ch.basic_publish(exchange="", routing_key="orders.created", body=b"x")
"#
    );
    write(&root, "src/svc/secrets.py", &code);
    let cache_dir = tmp.path().join("cache");
    std::fs::create_dir_all(&cache_dir).unwrap();
    let db = cache_dir.join("index-cache.db");
    {
        let cache = mesh_core::PersistentIndexCache::open(db.clone(), 64 * 1024 * 1024).unwrap();
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
        let all = everything(&state, &["orders.created"]).await;
        assert!(
            all.contains("orders.created"),
            "a real name is kept:\n{all}"
        );
        for canary in &canaries {
            assert!(!all.contains(canary.as_str()), "{canary} leaked:\n{all}");
        }
    }
    // The cache files (database, write-ahead log, shared memory) hold none of them either.
    let mut checked = 0;
    for entry in std::fs::read_dir(&cache_dir).unwrap() {
        let bytes = std::fs::read(entry.unwrap().path()).unwrap();
        checked += 1;
        for canary in &canaries {
            assert!(
                !bytes.windows(canary.len()).any(|w| w == canary.as_bytes()),
                "{canary} reached a cache file"
            );
        }
    }
    assert!(
        checked >= 1,
        "the cache wrote nothing: the check is vacuous"
    );
}
