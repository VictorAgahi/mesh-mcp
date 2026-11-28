#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! 9.0 T17: the producer, consumer and site rows of an `analyze_impact` table carry the broker
//! of their site as the same `broker:name` qualifier the topic rows use, on the fixture corpora.

use mesh_core::{expand_roots, AppState, AuditLogger, BackgroundRescanEngine, Config};
use mesh_server::tools::ToolRegistry;
use serde_json::{json, Value};
use std::path::Path;
use std::sync::Arc;

fn state_for(rel: &str) -> Arc<AppState> {
    let dir = dunce::canonicalize(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(rel),
    )
    .expect("canonicalize fixture");
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

async fn impact(state: &Arc<AppState>, args: Value) -> String {
    let res = ToolRegistry::call_tool("analyze_impact", args, state.clone())
        .await
        .expect("tool call");
    assert_eq!(res["isError"], json!(false), "{res}");
    res["content"][0]["text"]
        .as_str()
        .expect("text")
        .to_string()
}

/// `(role, element, location, confidence)` of every table row, cells without their backticks.
fn rows(text: &str) -> Vec<(String, String, String, String)> {
    text.lines()
        .filter(|l| l.starts_with("| ") && !l.starts_with("| #") && !l.starts_with("|---"))
        .map(|l| {
            let cells: Vec<String> = l
                .trim_matches('|')
                .split('|')
                .map(|c| c.trim().trim_matches('`').to_string())
                .collect();
            (
                cells[2].clone(),
                cells[3].clone(),
                cells[5].clone(),
                cells[6].clone(),
            )
        })
        .collect()
}

/// The element of the row of `role` at `location`.
fn element(text: &str, role: &str, location: &str) -> String {
    let all = rows(text);
    let found: Vec<_> = all
        .iter()
        .filter(|(r, _, loc, _)| r == role && loc == location)
        .collect();
    assert_eq!(found.len(), 1, "{role} {location} in {all:?}\n{text}");
    found[0].1.clone()
}

#[tokio::test]
async fn csharp_producer_and_consumer_rows_keep_their_broker() {
    let state = state_for("examples/eda/cs-brokers");
    let t = impact(&state, json!({"target": "orders", "limit": 100})).await;
    assert_eq!(
        element(&t, "producer", "src/Brk/ServiceBusSdk.cs:19"),
        "servicebus:orders"
    );
    assert_eq!(
        element(&t, "consumer", "src/Brk/MassTransitSetup.cs:16"),
        "rabbitmq:orders"
    );
    assert_eq!(
        element(&t, "producer", "src/Brk/KafkaOrders.cs:11"),
        "kafka:Send"
    );
}

#[tokio::test]
async fn one_name_on_six_brokers_gives_one_qualified_row_per_site() {
    let state = state_for("examples/eda/multi-broker");
    let t = impact(&state, json!({"target": "orders", "limit": 200})).await;
    for (role, loc, want) in [
        ("producer", "src/kafka.ts:9", "kafka:orders"),
        ("producer", "src/rabbit.ts:5", "rabbitmq:orders"),
        ("producer", "src/sns.ts:6", "sns:orders"),
        ("producer", "src/nats.ts:5", "nats:orders"),
        ("producer", "src/redis.ts:6", "redis:orders"),
        ("producer", "ledger/publisher.go:5", "kafka:NewOrderWriter"),
        ("consumer", "src/sqs.ts:5", "sqs:pollOrders"),
        ("consumer", "src/rabbit.ts:8", "rabbitmq:startFulfilment"),
        ("consumer", "src/kafka.ts:12", "kafka:startBilling"),
        ("consumer", "ledger/consumer.go:5", "kafka:NewOrderReader"),
    ] {
        assert_eq!(element(&t, role, loc), want, "{role} {loc}");
    }
    // The same name on two brokers stays two rows: no producer row merged with another broker's.
    let producers: Vec<_> = rows(&t)
        .into_iter()
        .filter(|(r, e, _, _)| r == "producer" && e.ends_with("orders"))
        .map(|(_, e, _, _)| e)
        .collect();
    assert_eq!(
        producers,
        [
            "kafka:orders",
            "nats:orders",
            "rabbitmq:orders",
            "redis:orders",
            "sns:orders"
        ]
    );
}

/// A NestJS `@EventPattern` is read file by file: its broker is not statically known, so its
/// rows stay bare and `heuristic` (it is on every channel of the name).
#[tokio::test]
async fn a_site_of_unknown_broker_stays_bare_and_heuristic() {
    let state = state_for("examples/eda/multi-broker");
    let t = impact(&state, json!({"target": "orders", "limit": 200})).await;
    let legacy: Vec<_> = rows(&t)
        .into_iter()
        .filter(|(_, _, loc, _)| loc == "src/legacy.ts:7")
        .collect();
    assert_eq!(legacy.len(), 2, "{legacy:?}");
    for (role, element, _, confidence) in legacy {
        assert_eq!(element, "orders", "{role}");
        assert_eq!(confidence, "heuristic", "{role}");
    }
}

/// A qualified target lists one broker's channel: every qualified row is that broker's.
#[tokio::test]
async fn a_qualified_target_lists_only_its_own_brokers_qualified_rows() {
    let state = state_for("examples/eda/multi-broker");
    let t = impact(&state, json!({"target": "rabbitmq:orders", "limit": 200})).await;
    assert_eq!(
        element(&t, "producer", "src/rabbit.ts:5"),
        "rabbitmq:orders"
    );
    for (role, element, loc, _) in rows(&t) {
        assert!(
            !element.starts_with("kafka:")
                && !element.starts_with("sns:")
                && !element.starts_with("sqs:")
                && !element.starts_with("redis:")
                && !element.starts_with("nats:"),
            "{role} {element} {loc}\n{t}"
        );
    }
}
