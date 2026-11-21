#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! 9.0 T7: broker-qualified topic keys through the tools, on `examples/eda/multi-broker`.

use mesh_core::{expand_roots, AppState, AuditLogger, BackgroundRescanEngine, Config};
use mesh_server::tools::ToolRegistry;
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;

fn state_for(rel: &str) -> Arc<AppState> {
    state_for_dir(
        &Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(rel),
    )
}

fn state_for_dir(path: &Path) -> Arc<AppState> {
    let dir = dunce::canonicalize(path).expect("canonicalize fixture");
    let config = Config::load_from_file(&dir.join("mesh-mcp.toml")).expect("fixture config");
    let roots = expand_roots(
        &config.workspace.roots,
        &dir,
        &config.workspace.workspace_root,
    )
    .expect("expand roots");
    let audit = Arc::new(AuditLogger::new_in_memory().expect("audit"));
    let rescan = Arc::new(BackgroundRescanEngine::new().expect("rescan"));
    let state = Arc::new(AppState::new(config, roots, audit, rescan));
    let snapshot = mesh_server::WorkspaceIndexer::build_snapshot(
        &state.config,
        &state.allowed_roots,
        None,
        None,
        None,
    );
    state.install_snapshot(snapshot);
    state
}

async fn call(state: &Arc<AppState>, tool: &str, args: Value) -> String {
    let res = ToolRegistry::call_tool(tool, args, state.clone())
        .await
        .expect("tool call");
    assert_eq!(res["isError"], json!(false), "{res}");
    res["content"][0]["text"]
        .as_str()
        .expect("text")
        .to_string()
}

const FIXTURE: &str = "examples/eda/multi-broker";

/// Every `path:line` of a Markdown answer (the scorer's own reading of an answer).
fn locations(text: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for l in text.lines() {
        for piece in l.split('`') {
            let looks = piece.rsplit_once(':').is_some_and(|(p, n)| {
                !p.is_empty() && n.chars().all(|c| c.is_ascii_digit()) && !n.is_empty()
            });
            if looks && (piece.starts_with("src/") || piece.starts_with("ledger/")) {
                out.insert(piece.to_string());
            }
        }
    }
    out
}

#[tokio::test]
async fn a_bare_name_lists_every_broker_and_a_qualified_one_lists_its_own() {
    let state = state_for(FIXTURE);
    let all = call(
        &state,
        "analyze_impact",
        json!({"target": "orders", "limit": 200}),
    )
    .await;
    for hub in [
        "kafka:orders",
        "rabbitmq:orders",
        "sns:orders",
        "sqs:orders",
        "redis:orders",
        "nats:orders",
    ] {
        assert!(all.contains(&format!("`{hub}`")), "{hub}\n{all}");
    }
    let kafka = call(
        &state,
        "analyze_impact",
        json!({"target": "kafka:orders", "limit": 200}),
    )
    .await;
    let seen = locations(&kafka);
    for want in [
        "src/kafka.ts:9",
        "src/kafka.ts:12",
        "ledger/publisher.go:5",
        "ledger/consumer.go:5",
    ] {
        assert!(seen.contains(want), "{want} in {seen:?}\n{kafka}");
    }
    for other in [
        "src/rabbit.ts",
        "src/sns.ts",
        "src/sqs.ts",
        "src/redis.ts",
        "src/nats.ts",
    ] {
        assert!(
            !seen.iter().any(|l| l.starts_with(other)),
            "{other} leaked into the Kafka channel: {seen:?}\n{kafka}"
        );
    }
    assert!(!kafka.contains("`rabbitmq:orders`"), "{kafka}");
}

#[tokio::test]
async fn a_trace_from_one_broker_never_crosses_into_another() {
    let state = state_for(FIXTURE);
    let rabbit = call(&state, "trace_flow", json!({"target": "rabbitmq:orders"})).await;
    let seen = locations(&rabbit);
    assert!(
        seen.contains("src/rabbit.ts:10") && seen.contains("src/fulfilment.ts:4"),
        "{rabbit}"
    );
    assert!(
        !seen
            .iter()
            .any(|l| l.starts_with("src/kafka.ts") || l.starts_with("src/billing.ts")),
        "{rabbit}"
    );
    let kafka = call(&state, "trace_flow", json!({"target": "kafka:orders"})).await;
    let seen = locations(&kafka);
    // Kafka reaches billing, `invoices` and, on the same broker, the Go ledger.
    assert!(
        seen.contains("src/billing.ts:6") && seen.contains("ledger/consumer.go:9"),
        "{kafka}"
    );
    assert!(
        !seen
            .iter()
            .any(|l| l.starts_with("src/rabbit.ts") || l.starts_with("src/fulfilment.ts")),
        "{kafka}"
    );
}

#[tokio::test]
async fn visualize_mesh_draws_one_topic_per_broker() {
    let state = state_for(FIXTURE);
    let mesh = call(&state, "visualize_mesh", json!({})).await;
    for hub in [
        "kafka:orders",
        "rabbitmq:orders",
        "sns:orders",
        "sqs:orders",
        "redis:orders",
        "nats:orders",
        "invoices",
        "shipments",
    ] {
        assert!(
            mesh.contains(&format!("{hub}<br/>1 topic")),
            "{hub}\n{mesh}"
        );
    }
}

/// A cache hit must read the brokers back: the warm snapshot has the same channels as the
/// cold one, and the same fingerprint.
#[test]
fn a_warm_cache_keeps_the_channels_apart() {
    let dir = dunce::canonicalize(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(FIXTURE),
    )
    .expect("fixture");
    let config = Config::load_from_file(&dir.join("mesh-mcp.toml")).expect("config");
    let roots = expand_roots(
        &config.workspace.roots,
        &dir,
        &config.workspace.workspace_root,
    )
    .expect("roots");
    let cache = mesh_core::PersistentIndexCache::in_memory().expect("cache");
    let build =
        || mesh_server::WorkspaceIndexer::build_snapshot(&config, &roots, None, None, Some(&cache));
    let hubs = |s: &mesh_core::state::MeshSnapshot| {
        let mut v: Vec<String> = s
            .contract_graph
            .all_nodes()
            .filter(|n| n.package == "event-bus")
            .map(|n| n.name.to_string())
            .collect();
        v.sort();
        v
    };
    let cold = build();
    let warm = build();
    assert_eq!(cold.fingerprint(), warm.fingerprint());
    assert_eq!(hubs(&cold), hubs(&warm));
    assert!(
        hubs(&warm).contains(&"rabbitmq:orders".to_string()),
        "{:?}",
        hubs(&warm)
    );
    assert_eq!(
        cold.contract_graph.canonical_lines(),
        warm.contract_graph.canonical_lines()
    );
}

#[tokio::test]
async fn trace_steps_name_the_channel_they_consume() {
    let state = state_for(FIXTURE);
    let kafka = call(&state, "trace_flow", json!({"target": "kafka:orders"})).await;
    assert!(
        kafka.contains("consume `kafka:orders` at `ledger/consumer.go:5`"),
        "{kafka}"
    );
    assert!(
        kafka.contains("consume `kafka:orders` at `src/kafka.ts:12`"),
        "{kafka}"
    );
    assert!(!kafka.contains("consume `orders`"), "{kafka}");
    let all = call(&state, "trace_flow", json!({"target": "orders"})).await;
    assert!(
        all.contains("consume `nats:orders` at `src/nats.ts:8`"),
        "{all}"
    );
    assert!(
        all.contains("produce `nats:orders` at `src/nats.ts:5`"),
        "{all}"
    );
    assert!(!all.contains("consume `orders`"), "{all}");
}

/// A Kafka topic literally named `rabbitmq:orders` while RabbitMQ also uses `orders`: the
/// literal key and the real RabbitMQ channel are both listed, and the answer says so.
#[tokio::test]
async fn a_key_spelled_like_a_hub_does_not_hide_the_real_channel() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path();
    std::fs::write(
        root.join("mesh-mcp.toml"),
        "[workspace]\nname = \"clash\"\nversion = \"1.0.0\"\nroots = [\".\"]\n\n[engines.contracts]\nenabled = true\n",
    )
    .expect("config");
    let write = |name: &str, body: &str| std::fs::write(root.join(name), body).expect("write");
    write(
        "literal.ts",
        "import { Kafka } from 'kafkajs';\nconst producer = new Kafka({ brokers: [] }).producer();\nexport async function odd() {\n  await producer.send({ topic: 'rabbitmq:orders', messages: [] });\n}\n",
    );
    write(
        "kafka.ts",
        "import { Kafka } from 'kafkajs';\nconst producer = new Kafka({ brokers: [] }).producer();\nexport async function ship() {\n  await producer.send({ topic: 'orders', messages: [] });\n}\n",
    );
    write(
        "rabbit.ts",
        "import amqp from 'amqplib';\nexport async function enqueue(ch: amqp.Channel) {\n  ch.sendToQueue('orders', Buffer.from('x'));\n}\nexport async function listen(ch: amqp.Channel) {\n  await ch.consume('orders', (m) => {});\n}\n",
    );
    let state = state_for_dir(root);
    let matrix = call(
        &state,
        "analyze_impact",
        json!({"target": "rabbitmq:orders", "limit": 200}),
    )
    .await;
    assert!(matrix.contains("**Ambiguous target**"), "{matrix}");
    let seen: BTreeSet<String> = matrix
        .lines()
        .filter(|l| {
            l.starts_with("| ") && (l.contains("| producer |") || l.contains("| consumer |"))
        })
        .filter_map(|l| {
            l.split('`')
                .find(|c| c.contains(':') && c.ends_with(|ch: char| ch.is_ascii_digit()))
                .map(str::to_string)
        })
        .collect();
    // the literal producer (literal.ts), the RabbitMQ producer and consumer, not the Kafka one
    assert!(
        seen.iter().any(|l| l.starts_with("literal.ts")),
        "{seen:?}\n{matrix}"
    );
    assert!(
        seen.iter().any(|l| l.starts_with("rabbit.ts:3")),
        "{seen:?}\n{matrix}"
    );
    assert!(
        seen.iter()
            .any(|l| l.starts_with("rabbit.ts:6") || l.starts_with("rabbit.ts:5")),
        "{seen:?}\n{matrix}"
    );
    assert!(
        !seen.iter().any(|l| l.starts_with("kafka.ts")),
        "{seen:?}\n{matrix}"
    );
    assert!(!matrix.contains("No consumer"), "{matrix}");
    let trace = call(&state, "trace_flow", json!({"target": "rabbitmq:orders"})).await;
    assert!(trace.contains("**Ambiguous target**"), "{trace}");
    assert!(trace.contains("rabbit.ts"), "{trace}");
    // The bare name is not ambiguous and lists the channels apart.
    let bare = call(
        &state,
        "analyze_impact",
        json!({"target": "orders", "limit": 200}),
    )
    .await;
    assert!(!bare.contains("**Ambiguous target**"), "{bare}");
    assert!(
        bare.contains("`kafka:orders`") && bare.contains("`rabbitmq:orders`"),
        "{bare}"
    );
}
