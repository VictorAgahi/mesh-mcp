#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! 9.0 T3: `trace_flow` on the committed fixtures, and its agreement with
//! `analyze_impact` past depth 1 (both run the chain engine of `contracts::flow`).

use mesh_core::{expand_roots, AppState, AuditLogger, BackgroundRescanEngine, Config};
use mesh_server::tools::ToolRegistry;
use serde_json::{json, Value};
use std::collections::BTreeSet;
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

/// `(role, root, path:line)` of the producer / consumer rows of an impact matrix.
fn matrix_sites(text: &str) -> BTreeSet<(String, String)> {
    text.lines()
        .filter(|l| l.starts_with("| ") && !l.starts_with("| #"))
        .filter_map(|l| {
            let cells: Vec<&str> = l.split(" | ").collect();
            let role = cells.get(2)?.trim();
            let loc = cells.get(5)?.trim().trim_matches('`');
            (role == "producer" || role == "consumer").then(|| (role.to_string(), loc.to_string()))
        })
        .collect()
}

/// `(role, path:line)` of the consume / produce steps of a trace, plus the
/// consumers its "Depth limit reached" note names (reached, not expanded).
fn trace_sites(text: &str) -> BTreeSet<(String, String)> {
    let mut out: BTreeSet<(String, String)> = text
        .lines()
        .filter(|l| l.starts_with("**Depth limit reached**"))
        .flat_map(|l| l.split("(`").skip(1).filter_map(|x| x.split('`').next()))
        .map(|loc| ("consumer".to_string(), loc.to_string()))
        .collect();
    out.extend(text.lines().filter_map(|l| {
        let (_, rest) = l.split_once(". ")?;
        let role = if rest.starts_with("consume ") {
            "consumer"
        } else if rest.starts_with("produce ") {
            "producer"
        } else {
            return None;
        };
        let at = rest.split(" at `").nth(1)?.split('`').next()?;
        Some((role.to_string(), at.to_string()))
    }));
    out
}

/// analyze_impact at `depth` and trace_flow at `event_depth = depth - 1` list
/// the same producer and consumer sites, both minus the entry topic's own
/// producers (direct rows of the matrix; a forward trace starts at the consumers).
async fn assert_agree(rel: &str, topic: &str, depth: u8) {
    let state = state_for(rel);
    let matrix = call(
        &state,
        "analyze_impact",
        json!({ "target": topic, "depth": depth, "limit": 200 }),
    )
    .await;
    let trace = call(
        &state,
        "trace_flow",
        json!({ "target": topic, "event_depth": depth - 1, "fan_out": 50 }),
    )
    .await;
    let direct = call(
        &state,
        "analyze_impact",
        json!({ "target": topic, "limit": 200 }),
    )
    .await;
    let upstream: BTreeSet<(String, String)> = matrix_sites(&direct)
        .into_iter()
        .filter(|(r, _)| r == "producer")
        .collect();
    let m: BTreeSet<_> = matrix_sites(&matrix)
        .difference(&upstream)
        .cloned()
        .collect();
    // A consumer node that carries its own Produces edge (a handler-anchored
    // publish) is a consumer row only in the matrix, as in 8.x.
    let mut t = trace_sites(&trace);
    let consumers: BTreeSet<String> = t
        .iter()
        .filter(|(r, _)| r == "consumer")
        .map(|(_, l)| l.clone())
        .collect();
    t.retain(|(r, l)| r == "consumer" || !consumers.contains(l));
    // A producer of the entry topic reached again (a cycle) is a direct row.
    let t: BTreeSet<_> = t.difference(&upstream).cloned().collect();
    assert_eq!(m, t, "{rel} {topic} depth {depth}\n{matrix}\n{trace}");
}

#[tokio::test]
async fn analyze_impact_agrees_with_trace_flow_on_the_fixtures() {
    for depth in [2, 3, 4] {
        assert_agree("examples/eda/layered-ts", "order.requested", depth).await;
        assert_agree("examples/eda/layered-ts", "stock.reserved", depth).await;
        assert_agree("examples/polyglot-shop", "order-created-topic", depth).await;
        assert_agree("examples/unresolved-flow", "chain:0", depth).await;
        assert_agree("examples/unresolved-flow", "cycle:a", depth).await;
        assert_agree("examples/eda/layered-go", "orders.placed", depth).await;
    }
}

#[tokio::test]
async fn layered_ts_forward_and_reverse_chains() {
    let state = state_for("examples/eda/layered-ts");
    let forward = call(
        &state,
        "trace_flow",
        json!({ "target": "POST /orders", "event_depth": 5 }),
    )
    .await;
    // Route -> order service -> inventory -> publisher -> stock.reserved -> shipping -> ...
    let order = [
        "src/order.controller.ts:9",
        "src/order.service.ts:8",
        "src/inventory.service.ts:8",
        "src/stock.publisher.ts:8",
        "src/stock.publisher.ts:9",
        "src/shipping.consumer.ts:10",
        "src/shipment.publisher.ts:9",
        "src/notifiers.ts:15",
    ];
    let mut pos = 0;
    for loc in order {
        let at = forward[pos..]
            .find(loc)
            .unwrap_or_else(|| panic!("{loc} missing or out of order\n{forward}"));
        pos += at;
    }
    assert!(forward.contains("[injected_field, ambiguous]"), "{forward}");
    let reverse = call(
        &state,
        "trace_flow",
        json!({ "target": "stock.reserved", "direction": "reverse" }),
    )
    .await;
    assert!(
        reverse.contains("called by `OrderService.place`"),
        "{reverse}"
    );
    assert!(reverse.contains("route `POST /orders`"), "{reverse}");
    assert!(reverse.contains("-> Chain starts here."), "{reverse}");
}

#[tokio::test]
async fn trace_flow_output_is_deterministic_across_indexes() {
    let args = json!({ "target": "order.requested", "event_depth": 5 });
    let a = call(
        &state_for("examples/eda/layered-ts"),
        "trace_flow",
        args.clone(),
    )
    .await;
    let b = call(&state_for("examples/eda/layered-ts"), "trace_flow", args).await;
    assert_eq!(a, b);
}

#[tokio::test]
async fn trace_flow_rejects_unknown_fields_and_says_no_entry() {
    let state = state_for("examples/eda/layered-ts");
    let res = ToolRegistry::call_tool(
        "trace_flow",
        json!({ "target": "x", "depth": 3 }),
        state.clone(),
    )
    .await
    .expect("tool call");
    assert_eq!(res["isError"], json!(true), "{res}");
    let none = call(
        &state,
        "trace_flow",
        json!({ "target": "NoSuchHandler.run" }),
    )
    .await;
    assert!(none.contains("No entry matched"), "{none}");
}

/// Go (T2b): producers and consumers sit on the `kafka.Writer` / `ReaderConfig`
/// construction line inside `NewT`; the chain starts from the methods of `T`.
#[tokio::test]
async fn layered_go_chain_through_constructor_anchored_sites() {
    let state = state_for("examples/eda/layered-go");
    let text = call(&state, "trace_flow", json!({ "target": "orders.placed" })).await;
    let mut pos = 0;
    for loc in [
        "consumer.go:14",
        "`OrderConsumer.Run`",
        "service.go:14",
        "publisher.go:22",
        "publisher.go:18",
        "billing.go:38",
        "billing.go:13",
    ] {
        pos += text[pos..]
            .find(loc)
            .unwrap_or_else(|| panic!("{loc} missing or out of order\n{text}"));
    }
}
