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

// ---- Fix round 1 (review of c3adbda) ----

fn temp_workspace(files: &[(&str, &str)]) -> (tempfile::TempDir, Arc<AppState>) {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("mesh-mcp.toml"),
        "[workspace]\nname = \"t\"\nversion = \"1.0.0\"\nroots = [\".\"]\n\n[engines.contracts]\nenabled = true\n",
    )
    .expect("config");
    for (name, body) in files {
        std::fs::write(dir.path().join(name), body).expect("file");
    }
    let root = dunce::canonicalize(dir.path()).expect("canonicalize");
    let rel = root.to_string_lossy().to_string();
    let state = state_for_abs(&rel);
    (dir, state)
}

fn state_for_abs(dir: &str) -> Arc<AppState> {
    let dir = Path::new(dir);
    let config = Config::load_from_file(&dir.join("mesh-mcp.toml")).expect("config");
    let roots = expand_roots(
        &config.workspace.roots,
        dir,
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

/// Go constructor anchoring is an inference: `Flush` is never called by `Run`,
/// yet a site in `NewConsumer` is anchored on every field-using method of the
/// struct. A chain through such a root must not read `exact`.
#[tokio::test]
async fn go_constructor_anchored_chain_is_heuristic() {
    let consumer = r#"package main

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader *kafka.Reader
	pub    *Publisher
}

func NewConsumer(brokers []string, pub *Publisher) *Consumer {
	r := kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: "topic.in", GroupID: "g"})
	return &Consumer{reader: r, pub: pub}
}

func (c *Consumer) Run(ctx context.Context) error {
	for {
		if _, err := c.reader.ReadMessage(ctx); err != nil {
			return err
		}
	}
}

func (c *Consumer) Flush(ctx context.Context) error {
	return c.pub.Publish(ctx, "x")
}
"#;
    let publisher = r#"package main

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type Publisher struct {
	writer *kafka.Writer
}

func NewPublisher(addr string) *Publisher {
	return &Publisher{writer: &kafka.Writer{Addr: kafka.TCP(addr), Topic: "topic.out"}}
}

func (p *Publisher) Publish(ctx context.Context, id string) error {
	return p.writer.WriteMessages(ctx, kafka.Message{Key: []byte(id)})
}
"#;
    let (_dir, state) = temp_workspace(&[
        ("go.mod", "module example.com/t\n\ngo 1.22\n"),
        ("consumer.go", consumer),
        ("publisher.go", publisher),
    ]);
    let text = call(&state, "trace_flow", json!({ "target": "topic.in" })).await;
    let flush_hop = text
        .split("\n### Hop ")
        .find(|h| h.contains("Consumer.Flush"))
        .unwrap_or_else(|| panic!("the anchored chain is still shown\n{text}"));
    assert!(flush_hop.contains("chain heuristic"), "{flush_hop}");
    assert!(!flush_hop.contains("; chain exact"), "{flush_hop}");
}

#[tokio::test]
async fn bounds_have_maxima_are_clamped_and_fail_cleanly() {
    let tools = ToolRegistry::list_tools();
    let schema = tools
        .as_array()
        .and_then(|t| t.iter().find(|t| t["name"] == "trace_flow"))
        .map(|t| t["inputSchema"]["properties"].clone())
        .expect("trace_flow listed");
    assert_eq!(
        schema["event_depth"]["maximum"].as_f64(),
        Some(5.0),
        "{schema}"
    );
    assert_eq!(
        schema["call_depth"]["maximum"].as_f64(),
        Some(12.0),
        "{schema}"
    );
    assert_eq!(
        schema["fan_out"]["maximum"].as_f64(),
        Some(50.0),
        "{schema}"
    );
    let state = state_for("examples/eda/layered-ts");
    let clamped = call(
        &state,
        "trace_flow",
        json!({ "target": "order.requested", "event_depth": 13, "call_depth": 300, "fan_out": 51 }),
    )
    .await;
    assert!(
        clamped.contains("Bounds (clamped): event_depth 5, call_depth 12, fan_out 50."),
        "{clamped}"
    );
    for bad in [json!(-1), json!(2.5), json!("3")] {
        let res = ToolRegistry::call_tool(
            "trace_flow",
            json!({ "target": "order.requested", "event_depth": bad }),
            state.clone(),
        )
        .await
        .expect("tool call");
        let text = res["content"][0]["text"].as_str().unwrap_or_default();
        assert_eq!(res["isError"], json!(true), "{bad}: {text}");
        assert!(
            text.contains("`event_depth` must be a whole number from 1 to 5"),
            "{text}"
        );
        assert!(!text.contains("u8") && !text.contains("u32"), "{text}");
    }
}
