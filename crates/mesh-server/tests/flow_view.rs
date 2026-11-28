#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! 9.0 T13: the event-centred flow view of `visualize_mesh`.

use mesh_core::{expand_roots, AppState, AuditLogger, BackgroundRescanEngine, Config};
use mesh_server::tools::ToolRegistry;
use serde_json::{json, Value};
use std::path::Path;
use std::sync::Arc;

fn state_for_abs(dir: &Path) -> Arc<AppState> {
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

fn state_for(rel: &str) -> Arc<AppState> {
    let dir = dunce::canonicalize(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(rel),
    )
    .expect("canonicalize fixture");
    state_for_abs(&dir)
}

async fn call(state: &Arc<AppState>, args: Value) -> String {
    let res = ToolRegistry::call_tool("visualize_mesh", args, state.clone())
        .await
        .expect("tool call");
    assert_eq!(res["isError"], json!(false), "{res}");
    res["content"][0]["text"]
        .as_str()
        .expect("text")
        .to_string()
}

fn temp_workspace(files: &[(&str, &str)]) -> (tempfile::TempDir, Arc<AppState>) {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("mesh-mcp.toml"),
        "[workspace]\nname = \"t\"\nversion = \"1.0.0\"\nroots = [\".\"]\n\n[engines.contracts]\nenabled = true\n",
    )
    .expect("config");
    for (name, body) in files {
        let path = dir.path().join(name);
        std::fs::create_dir_all(path.parent().expect("parent")).expect("dirs");
        std::fs::write(path, body).expect("file");
    }
    let root = dunce::canonicalize(dir.path()).expect("canonicalize");
    let state = state_for_abs(&root);
    (dir, state)
}

async fn call_err(state: &Arc<AppState>, args: Value) -> String {
    let res = ToolRegistry::call_tool("visualize_mesh", args, state.clone())
        .await
        .expect("tool call");
    assert_eq!(res["isError"], json!(true), "{res}");
    res["content"][0]["text"]
        .as_str()
        .expect("text")
        .to_string()
}

/// The text between the mermaid fences.
fn mermaid_block(text: &str) -> &str {
    let start = text.find("```mermaid\n").expect("mermaid fence") + "```mermaid\n".len();
    let end = start + text[start..].find("```").expect("closing fence");
    &text[start..end]
}

/// Mermaid-syntax sanity: every statement is a node, an edge, a class or a style; quotes are
/// balanced and hold no syntax characters (only the `<br/>` we emit); brackets outside quotes
/// are balanced; every id is `n<digits>` and every edge joins two defined nodes.
fn assert_mermaid_sane(text: &str) {
    let block = mermaid_block(text);
    let mut lines = block.lines();
    assert_eq!(lines.next(), Some("flowchart TD"), "{block}");
    let mut defined: Vec<String> = Vec::new();
    let mut edges: Vec<(String, String)> = Vec::new();
    for line in lines {
        let line = line.trim();
        if line.starts_with("classDef ") || line.starts_with("linkStyle ") {
            continue;
        }
        if let Some(rest) = line.strip_prefix("class ") {
            let id = rest.split(' ').next().expect("id");
            assert!(is_id(id), "{line}");
            continue;
        }
        let parts: Vec<&str> = line.split('"').collect();
        assert!(parts.len() % 2 == 1, "unbalanced quotes: {line}");
        let mut outside = String::new();
        for (k, part) in parts.iter().enumerate() {
            if k % 2 == 1 {
                // Our own markup: a line break and numeric entities (`#40;`).
                let mut inner = part.replace("<br/>", " ");
                while let Some(at) = inner.find('#') {
                    let end = inner[at..].find(';').expect("entity end") + at;
                    assert!(
                        inner[at + 1..end].chars().all(|c| c.is_ascii_digit()),
                        "{line}"
                    );
                    inner.replace_range(at..=end, "_");
                }
                assert!(
                    !inner.contains([
                        '"', '[', ']', '(', ')', '{', '}', '|', ';', '<', '>', '&', '`', '\\'
                    ]),
                    "raw syntax character in label: {line}"
                );
                assert!(inner.is_ascii(), "non-ascii in label: {line}");
            } else {
                outside.push_str(part);
            }
        }
        let mut depth = 0i32;
        for c in outside.chars() {
            match c {
                '(' | '[' | '{' => depth += 1,
                ')' | ']' | '}' => depth -= 1,
                _ => {}
            }
            assert!(depth >= 0, "closing before opening: {line}");
        }
        assert_eq!(depth, 0, "unbalanced brackets: {line}");
        let first = outside
            .trim()
            .split(|c: char| !c.is_ascii_alphanumeric())
            .next()
            .expect("first token");
        assert!(is_id(first), "bad node id in {line}");
        if outside.contains("-->") || outside.contains("-.->") {
            let from = first.to_string();
            let to = outside
                .trim()
                .rsplit([' ', '|'])
                .next()
                .expect("target")
                .to_string();
            assert!(is_id(&to), "bad edge target in {line}");
            edges.push((from, to));
        } else {
            defined.push(first.to_string());
        }
    }
    for (from, to) in &edges {
        assert!(defined.contains(from), "edge from undefined {from}");
        assert!(defined.contains(to), "edge to undefined {to}");
    }
    let mut sorted = defined.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(sorted.len(), defined.len(), "a node is defined twice");
}

fn is_id(s: &str) -> bool {
    s.len() > 1 && s.starts_with('n') && s[1..].chars().all(|c| c.is_ascii_digit())
}

fn has_line(text: &str, line: &str) -> bool {
    text.lines().any(|l| l.trim() == line)
}

#[tokio::test]
async fn layered_ts_event_flow_up_and_down() {
    let state = state_for("examples/eda/layered-ts");
    let text = call(&state, json!({ "event": "stock.reserved" })).await;
    assert_mermaid_sane(&text);
    assert!(
        text.starts_with("## Event flow view for `stock.reserved`\n"),
        "{text}"
    );
    assert!(text.contains("flowchart TD"), "{text}");
    assert!(!text.contains("flowchart LR"), "{text}");
    for line in [
        r#"n0(["?:stock.reserved"]):::centre"#,
        r#"n1["StockPublisher.publishReserved<br/>src/stock.publisher.ts:9"]:::site"#,
        r#"n4[/"POST /orders<br/>src/order.controller.ts:9"/]:::origin"#,
        r#"n5["ShippingConsumer.onStockReserved<br/>src/shipping.consumer.ts:10"]:::site"#,
        r#"n2 -->|"3 calls"| n1"#,
        r#"n4 -->|"3 calls"| n1"#,
        r#"n0 --> n5"#,
        r#"n5 -->|"2 calls"| n6"#,
        // An ambiguous call hop: dashed, labelled `?`, red.
        r#"n8 -.->|"? 2 calls"| n9"#,
        r#"linkStyle 8 stroke:#c92a2a,color:#c92a2a"#,
    ] {
        assert!(has_line(&text, line), "missing `{line}` in\n{text}");
    }
    // The table names every node in full, with its place.
    assert!(
        text.contains("| n8 | consumer | `NotificationConsumer.onShipmentScheduled` | `src/notification.consumer.ts:10` |"),
        "{text}"
    );
    assert!(text.contains("Nodes drawn: 13 of 13."), "{text}");
}

#[tokio::test]
async fn depth_limit_is_a_stub_and_a_note_not_an_end() {
    let state = state_for("examples/eda/layered-ts");
    let text = call(
        &state,
        json!({ "event": "order.requested", "hops_down": 1, "direction": "downstream" }),
    )
    .await;
    assert_mermaid_sane(&text);
    assert!(text.contains("hops_up -, hops_down 1"), "{text}");
    assert!(
        has_line(
            &text,
            r#"n4[["1 consumer#40;s#41; not expanded: depth limit #40;1#41;"]]:::bound"#
        ),
        "{text}"
    );
    assert!(has_line(&text, "n3 -.-> n4"), "{text}");
    assert!(
        text.contains("**Depth limit reached** (`hops_down` 1): 1 consumer(s) past the last event drawn were not expanded; raise `hops_down` (max 5)."),
        "{text}"
    );
    // The same walk, one level deeper, reaches the next event and has no depth stub there.
    let deeper = call(
        &state,
        json!({ "event": "order.requested", "hops_down": 2, "direction": "downstream" }),
    )
    .await;
    assert!(deeper.contains("shipment.scheduled"), "{deeper}");
    assert!(
        deeper.contains("1 consumer#40;s#41; not expanded"),
        "{deeper}"
    );
}

#[tokio::test]
async fn detail_shows_the_calls_as_nodes_and_collapsed_does_not() {
    let state = state_for("examples/eda/layered-ts");
    let args = |detail: bool| json!({ "event": "order.requested", "direction": "downstream", "hops_down": 1, "detail": detail });
    let collapsed = call(&state, args(false)).await;
    let detailed = call(&state, args(true)).await;
    assert_mermaid_sane(&detailed);
    assert!(
        has_line(&collapsed, r#"n1 -->|"3 calls"| n2"#),
        "{collapsed}"
    );
    assert!(!collapsed.contains("OrderService.place"), "{collapsed}");
    for name in ["OrderService.place", "InventoryService.reserve"] {
        assert!(
            detailed.contains(&format!("| call | `{name}` |")),
            "{name} in\n{detailed}"
        );
    }
    assert!(!detailed.contains("3 calls"), "{detailed}");
    assert!(detailed.contains("detail on"), "{detailed}");
}

#[tokio::test]
async fn cycle_is_drawn_once_and_marked_and_unresolved_topics_are_hexagons() {
    let state = state_for("examples/eda/flow-view");
    let text = call(
        &state,
        json!({ "event": "invoice.requested", "hops_down": 3 }),
    )
    .await;
    assert_mermaid_sane(&text);
    // The ledger consumer publishes the centre again: one centre node, an edge back to it, marked.
    assert_eq!(
        text.matches("(\"?:invoice.requested\")").count(),
        0,
        "{text}"
    );
    assert_eq!(
        text.matches(r#"(["?:invoice.requested"])"#).count(),
        1,
        "{text}"
    );
    assert!(has_line(&text, "class n0 revisit"), "{text}");
    assert!(has_line(&text, "n3 --> n0"), "{text}");
    // The runtime-built topic is a distinct 'not statically resolved' node, never a dead end.
    assert!(
        has_line(
            &text,
            r#"n10(["?:$#123;invoice_dead_topic#125;"]):::unresolved"#
        ),
        "{text}"
    );
    assert!(
        has_line(
            &text,
            r#"n12{{"not statically resolved: no consumer found"}}:::unresolved"#
        ),
        "{text}"
    );
    assert!(has_line(&text, "n10 -.-> n12"), "{text}");
    // A consumer that publishes nothing is a chain end, said so.
    assert!(
        has_line(
            &text,
            r#"n11{{"not statically resolved: no publish found"}}:::unresolved"#
        ),
        "{text}"
    );
    assert!(text.contains("**Chain end** at a dashed hexagon"), "{text}");
}

#[tokio::test]
async fn hostile_paths_and_names_are_escaped_and_the_table_keeps_full_names() {
    let state = state_for("examples/eda/flow-view");
    let text = call(&state, json!({ "event": "invoice.requested" })).await;
    assert_mermaid_sane(&text);
    for line in [
        r#"n1["InvoiceController.retry<br/>src/#40;billing#41;/#91;id#93;/invoice.controller.ts:10"]:::site"#,
        r#"n2[/"POST /invoices/#123;id#125;/retry<br/>src/#40;billing#41;/#91;id#93;/invoice.controller.ts:9"/]:::origin"#,
        r#"n3["LedgerConsumer.post<br/>src/ledger#59;v2/ledger.consumer.ts:10"]:::site"#,
        r#"n6["InvoicePublisher.publishIssued<br/>src/#40;billing#41;/#91;id#93;/#233;t#233;/invoice.publisher.ts:9"]:::site"#,
    ] {
        assert!(has_line(&text, line), "missing `{line}` in\n{text}");
    }
    // The table is not escaped: the real path, accent included.
    assert!(
        text.contains("| `src/(billing)/[id]/été/invoice.publisher.ts:9` |"),
        "{text}"
    );
    assert!(
        text.contains("| entry | `POST /invoices/{id}/retry` |"),
        "{text}"
    );
}

#[tokio::test]
async fn long_names_are_cut_in_the_diagram_and_whole_in_the_table() {
    let long = "very.long.event.name.that.keeps.going.on.and.on.until.it.is.cut";
    let src = format!(
        "import {{ Controller }} from '@nestjs/common';\nimport {{ EventPattern }} from '@nestjs/microservices';\n@Controller()\nexport class C {{\n  @EventPattern('{long}')\n  async h(p: any) {{ console.log(p); }}\n}}\n"
    );
    let (_d, state) = temp_workspace(&[("c.ts", &src)]);
    let text = call(&state, json!({ "event": long })).await;
    assert_mermaid_sane(&text);
    assert!(
        has_line(
            &text,
            r#"n0(["?:very.long.event.name.that.keeps.goi..."]):::centre"#
        ),
        "{text}"
    );
    assert!(
        text.contains(&format!("| centre event | `?:{long}` |")),
        "{text}"
    );
}

fn many_consumers(n: usize) -> Vec<(String, String)> {
    (0..n)
        .map(|i| {
            (
                format!("c{i:02}.ts"),
                format!(
                    "import {{ Controller }} from '@nestjs/common';\nimport {{ EventPattern }} from '@nestjs/microservices';\n@Controller()\nexport class Consumer{i:02} {{\n  @EventPattern('cap.event')\n  async handle(p: any) {{ console.log(p); }}\n}}\n"
                ),
            )
        })
        .collect()
}

fn temp_from(files: &[(String, String)]) -> (tempfile::TempDir, Arc<AppState>) {
    let refs: Vec<(&str, &str)> = files
        .iter()
        .map(|(a, b)| (a.as_str(), b.as_str()))
        .collect();
    temp_workspace(&refs)
}

#[tokio::test]
async fn node_cap_counts_what_it_cuts_and_marks_where() {
    let (_d, state) = temp_from(&many_consumers(12));
    let text = call(
        &state,
        json!({ "event": "cap.event", "direction": "downstream", "limit": 4 }),
    )
    .await;
    assert_mermaid_sane(&text);
    assert!(text.contains("Nodes drawn: 4 of 13."), "{text}");
    assert!(
        text.contains("**9 nodes not shown (cap 4)**, use hops/limit/scope"),
        "{text}"
    );
    assert!(
        has_line(&text, r#"n16[["+9 not shown #40;cap#41;"]]:::bound"#),
        "{text}"
    );
    assert!(has_line(&text, "n0 -.-> n16"), "{text}");
    // Under the cap nothing is claimed cut.
    let all = call(
        &state,
        json!({ "event": "cap.event", "direction": "downstream" }),
    )
    .await;
    assert!(!all.contains("not shown (cap"), "{all}");
    assert!(all.contains("Nodes drawn: 13 of 13."), "{all}");
}

#[tokio::test]
async fn fan_out_cap_is_reported_as_more_not_followed() {
    let (_d, state) = temp_from(&many_consumers(12));
    let text = call(
        &state,
        json!({ "event": "cap.event", "direction": "downstream", "fan_out": 3 }),
    )
    .await;
    assert_mermaid_sane(&text);
    assert!(
        text.contains("+9 more consumer site#40;s#41; not followed #40;fan_out 3#41;"),
        "{text}"
    );
    assert!(
        text.contains("**Fan-out cap**: 9 consumer/producer site(s) were not followed"),
        "{text}"
    );
    assert!(text.contains("Nodes drawn: 4 of 4."), "{text}");
}

#[tokio::test]
async fn output_stays_under_the_48_kb_cap_when_the_cap_is_high() {
    let (_d, state) = temp_from(&many_consumers(200));
    let text = call(
        &state,
        json!({ "event": "cap.event", "direction": "downstream", "limit": 200, "fan_out": 50 }),
    )
    .await;
    assert!(text.len() <= 48 * 1024, "{} bytes", text.len());
    assert_mermaid_sane(&text);
    // 50 consumers followed at most (fan_out), and the rest said so.
    assert!(
        text.contains("+150 more consumer site#40;s#41; not followed #40;fan_out 50#41;"),
        "{text}"
    );
}

#[tokio::test]
async fn unknown_event_says_so() {
    let state = state_for("examples/eda/layered-ts");
    let text = call(&state, json!({ "event": "no.such.event" })).await;
    assert!(
        text.contains(
            "*No event matched `no.such.event` across 1 indexed workspace roots (0 matches)."
        ),
        "{text}"
    );
    assert!(!text.contains("```mermaid"), "{text}");
}

#[tokio::test]
async fn brokers_never_join() {
    let state = state_for("examples/eda/multi-broker");
    let kafka = call(&state, json!({ "event": "kafka:orders" })).await;
    assert_mermaid_sane(&kafka);
    assert!(
        kafka.contains(r#"n0(["kafka:orders"]):::centre"#),
        "{kafka}"
    );
    assert!(kafka.contains("src/kafka.ts:12"), "{kafka}");
    let kafka_view = &kafka[..kafka.find("*Legend").expect("legend")];
    for other in [
        "src/rabbit.ts",
        "src/redis.ts",
        "src/sns.ts",
        "src/sqs.ts",
        "src/nats.ts",
        "rabbitmq:orders",
    ] {
        assert!(
            !kafka_view.contains(other),
            "{other} joined kafka:orders\n{kafka}"
        );
    }
    let rabbit = call(&state, json!({ "event": "rabbitmq:orders" })).await;
    assert_mermaid_sane(&rabbit);
    assert!(rabbit.contains("src/rabbit.ts:8"), "{rabbit}");
    let rabbit_view = &rabbit[..rabbit.find("*Legend").expect("legend")];
    for other in ["src/kafka.ts", "src/billing.ts", "ledger/", "kafka:orders"] {
        assert!(
            !rabbit_view.contains(other),
            "{other} joined rabbitmq:orders\n{rabbit}"
        );
    }
    // The bare name is every broker's channel, drawn apart, each qualified.
    let bare = call(&state, json!({ "event": "orders" })).await;
    assert_mermaid_sane(&bare);
    for centre in [
        "kafka:orders",
        "rabbitmq:orders",
        "redis:orders",
        "sqs:orders",
    ] {
        assert!(
            bare.contains(&format!("([\"{centre}\"]):::centre")),
            "{centre} missing\n{bare}"
        );
    }
}

#[tokio::test]
async fn credential_shaped_names_never_reach_the_diagram() {
    // Built at runtime: no key-shaped literal in the source.
    let token = format!("{}{}", "tok_", "a1b2c3d4e5f6a7b8");
    let handler = format!("{}{}", "H3a9f1c7e2b4d6a8c0e1", "f3a5");
    let src = format!(
        "import {{ Controller, Injectable }} from '@nestjs/common';\nimport {{ EventPattern, ClientKafka }} from '@nestjs/microservices';\n@Injectable()\nexport class Svc {{\n  constructor(private readonly client: ClientKafka) {{}}\n  go(p: any) {{ this.client.emit('ok.done', p); }}\n}}\n@Controller()\nexport class Other {{\n  constructor(private readonly svc: Svc) {{}}\n  @EventPattern('ok.event')\n  async {handler}(p: any) {{ this.svc.go(p); }}\n  @EventPattern('{token}')\n  async t(p: any) {{ console.log(p); }}\n}}\n"
    );
    let (_d, state) = temp_workspace(&[("c.ts", &src)]);
    let ok = call(&state, json!({ "event": "ok.event" })).await;
    assert!(
        ok.contains("[REDACTED_SECRET: USE_ENV_OR_LOCAL_FALLBACK]"),
        "{ok}"
    );
    assert!(ok.contains("ok.done"), "{ok}");
    let asked = call(&state, json!({ "event": token })).await;
    for text in [&ok, &asked] {
        let lower = text.to_ascii_lowercase();
        for canary in [&token, &handler] {
            assert!(
                !lower.contains(&canary.to_ascii_lowercase()),
                "{canary} leaked\n{text}"
            );
        }
    }
}

#[tokio::test]
async fn output_is_deterministic_across_indexes() {
    let args = json!({ "event": "order.requested", "hops_up": 3, "hops_down": 5, "detail": true });
    let a = call(&state_for("examples/eda/layered-ts"), args.clone()).await;
    let b = call(&state_for("examples/eda/layered-ts"), args).await;
    assert_eq!(a, b);
}

#[tokio::test]
async fn arguments_are_validated_and_hops_are_clamped() {
    let state = state_for("examples/eda/layered-ts");
    assert!(call_err(&state, json!({ "event": "x", "bogus": 1 }))
        .await
        .contains("bogus"));
    assert!(call_err(
        &state,
        json!({ "event": "order.requested", "format": "json" })
    )
    .await
    .contains("The event view is Mermaid only"));
    assert!(call_err(
        &state,
        json!({ "event": "order.requested", "service": "s" })
    )
    .await
    .contains("`event` and `service` are two different views"));
    assert!(call_err(
        &state,
        json!({ "event": "order.requested", "direction": "sideways" })
    )
    .await
    .contains("Unknown direction 'sideways'"));
    assert!(call_err(&state, json!({ "event": "  " }))
        .await
        .contains("`event` must not be empty"));
    let clamped = call(
        &state,
        json!({ "event": "order.requested", "hops_up": 99, "hops_down": 0 }),
    )
    .await;
    assert!(clamped.contains("hops_up 5, hops_down 1,"), "{clamped}");
    // Without `event` the tool is the service view, unchanged.
    let service_view = call(&state, json!({})).await;
    assert!(
        service_view.starts_with("## Polyglot Architecture Mesh Topology"),
        "{service_view}"
    );
}

#[tokio::test]
async fn test_files_are_left_out_unless_asked() {
    let (_d, state) = temp_workspace(&[
        (
            "a.ts",
            "import { Controller } from '@nestjs/common';\nimport { EventPattern } from '@nestjs/microservices';\n@Controller()\nexport class A {\n  @EventPattern('t.event')\n  async h(p: any) { console.log(p); }\n}\n",
        ),
        (
            "a.spec.ts",
            "import { Controller } from '@nestjs/common';\nimport { EventPattern } from '@nestjs/microservices';\n@Controller()\nexport class ATest {\n  @EventPattern('t.event')\n  async h(p: any) { console.log(p); }\n}\n",
        ),
    ]);
    let hidden = call(
        &state,
        json!({ "event": "t.event", "direction": "downstream" }),
    )
    .await;
    assert!(hidden.contains("a.ts:6"), "{hidden}");
    assert!(!hidden.contains("a.spec.ts"), "{hidden}");
    assert!(
        hidden.contains(
            "*Test-file sites are left out (`include_tests: false`): 1 site(s) not drawn.*"
        ),
        "{hidden}"
    );
    let shown = call(
        &state,
        json!({ "event": "t.event", "direction": "downstream", "include_tests": true }),
    )
    .await;
    assert!(shown.contains("a.spec.ts:6"), "{shown}");
    assert!(!shown.contains("Test-file sites are left out"), "{shown}");
}

#[test]
fn tool_schema_carries_the_negative_constraints_and_bounds() {
    let schema = serde_json::to_string(&schemars::schema_for!(
        mesh_server::tools::visualize_mesh::VisualizeMeshArgs
    ))
    .expect("schema");
    for needle in [
        "\"additionalProperties\":false",
        "DO NOT combine with `service`",
        "DO NOT raise it to see everything",
        "hops_up",
        "hops_down",
    ] {
        assert!(schema.contains(needle), "{needle} missing in {schema}");
    }
}

fn listener(class: &str, topic: &str, emit: Option<&str>) -> String {
    let publish = emit.map_or(String::new(), |t| format!("this.client.emit('{t}', p);"));
    format!(
        "import {{ Controller }} from '@nestjs/common';\nimport {{ ClientKafka, EventPattern }} from '@nestjs/microservices';\n@Controller()\nexport class {class} {{\n  constructor(private readonly client: ClientKafka) {{}}\n  @EventPattern('{topic}')\n  async handle(p: any) {{ {publish} }}\n}}\n"
    )
}

#[tokio::test]
async fn a_name_that_matched_several_events_by_substring_says_so() {
    let (_d, state) = temp_workspace(&[
        ("a.ts", &listener("A", "ord.created", None)),
        ("b.ts", &listener("B", "ord.shipped", None)),
    ]);
    let text = call(&state, json!({ "event": "ord", "direction": "downstream" })).await;
    assert_mermaid_sane(&text);
    assert!(
        text.contains("**`ord` matched 2 events by substring**"),
        "{text}"
    );
    assert!(has_line(&text, r#"n0(["?:ord"]):::centre"#), "{text}");
    // An exact key is one event, no such note.
    let exact = call(
        &state,
        json!({ "event": "ord.created", "direction": "downstream" }),
    )
    .await;
    assert!(!exact.contains("by substring"), "{exact}");
    assert!(
        has_line(&exact, r#"n0(["?:ord.created"]):::centre"#),
        "{exact}"
    );
}

#[tokio::test]
async fn call_depth_limit_is_a_stub_and_a_note_not_a_chain_end() {
    // consumer -> s0 -> s1 -> ... -> s8 -> publish: nine calls, the walk is cut at six.
    let mut files: Vec<(String, String)> = Vec::new();
    let chain = 9;
    files.push((
        "consumer.ts".to_string(),
        format!(
            "import {{ Controller }} from '@nestjs/common';\nimport {{ EventPattern }} from '@nestjs/microservices';\nimport {{ S0 }} from './s0';\n@Controller()\nexport class Entry {{\n  constructor(private readonly s0: S0) {{}}\n  @EventPattern('deep.in')\n  async handle(p: any) {{ this.s0.run(p); }}\n}}\n"
        ),
    ));
    for i in 0..chain {
        let body = if i + 1 < chain {
            format!("this.next.run(p);")
        } else {
            "this.client.emit('deep.out', p);".to_string()
        };
        let (field, import) = if i + 1 < chain {
            (
                format!("private readonly next: S{}", i + 1),
                format!("import {{ S{} }} from './s{}';\n", i + 1, i + 1),
            )
        } else {
            (
                "private readonly client: ClientKafka".to_string(),
                "import { ClientKafka } from '@nestjs/microservices';\n".to_string(),
            )
        };
        files.push((
            format!("s{i}.ts"),
            format!(
                "import {{ Injectable }} from '@nestjs/common';\n{import}@Injectable()\nexport class S{i} {{\n  constructor({field}) {{}}\n  run(p: any) {{ {body} }}\n}}\n"
            ),
        ));
    }
    let (_d, state) = temp_from(&files);
    let text = call(
        &state,
        json!({ "event": "deep.in", "direction": "downstream" }),
    )
    .await;
    assert_mermaid_sane(&text);
    assert!(text.contains("call depth limit #40;6#41;"), "{text}");
    assert!(
        text.contains(
            "**Call depth limit reached** (down, call_depth 6) at 1 callable(s) with further calls"
        ),
        "{text}"
    );
    assert!(!text.contains("deep.out"), "{text}");
}

#[tokio::test]
async fn a_long_place_is_cut_at_its_head_so_the_line_stays() {
    let dir = "an_extremely_long_directory_name_for_a_service_module";
    let (_d, state) = temp_workspace(&[(
        &format!("{dir}/{dir}2/consumer.ts"),
        &listener("C", "place.event", None),
    )]);
    let text = call(
        &state,
        json!({ "event": "place.event", "direction": "downstream" }),
    )
    .await;
    assert_mermaid_sane(&text);
    assert!(
        text.contains(
            "<br/>...y_long_directory_name_for_a_service_module2/consumer.ts:7\"]:::site"
        ),
        "{text}"
    );
    assert!(
        text.contains(&format!("`{dir}/{dir}2/consumer.ts:7`")),
        "{text}"
    );
}

#[tokio::test]
async fn the_node_cap_shrinks_until_the_output_fits_the_budget() {
    // 50 consumers of one event, each publishing its own topic that one more consumer hears:
    // 201 nodes with long names, more than 44 KB of diagram and table.
    let mut files: Vec<(String, String)> = Vec::new();
    for i in 0..50 {
        let long = format!("a_rather_long_consumer_class_name_for_shard_{i:02}");
        files.push((
            format!("modules/{long}/in.ts"),
            listener(
                &format!("{}In", long.replace('_', "")),
                "wide.event",
                Some(&format!("wide.out.{i:02}")),
            ),
        ));
        files.push((
            format!("modules/{long}/out.ts"),
            listener(
                &format!("{}Out", long.replace('_', "")),
                &format!("wide.out.{i:02}"),
                None,
            ),
        ));
    }
    let (_d, state) = temp_from(&files);
    let text = call(
        &state,
        json!({ "event": "wide.event", "direction": "downstream", "limit": 200, "fan_out": 50 }),
    )
    .await;
    assert!(text.len() <= 48 * 1024, "{} bytes", text.len());
    assert_mermaid_sane(&text);
    assert!(text.contains("nodes not shown (cap "), "{}", &text[..600]);
    assert!(!text.contains("limit 200,"), "{}", &text[..300]);
}
