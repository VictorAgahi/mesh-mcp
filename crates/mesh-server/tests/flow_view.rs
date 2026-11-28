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
    let text = res["content"][0]["text"]
        .as_str()
        .expect("text")
        .to_string();
    // Manual check against the real Mermaid parser: `MM_DUMP=/path cargo test ...` appends
    // every answer to that file; never set in CI.
    if let Ok(path) = std::env::var("MM_DUMP") {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .expect("dump file");
        writeln!(f, "{text}").expect("dump");
    }
    text
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
    // Class names: ours all carry the `mm_` prefix, so none can be a Mermaid keyword (`call`
    // made every diagram a parse error in Mermaid 11); each used class has a `classDef`.
    const RESERVED: [&str; 17] = [
        "call",
        "click",
        "default",
        "end",
        "graph",
        "subgraph",
        "class",
        "style",
        "linkStyle",
        "classDef",
        "flowchart",
        "direction",
        "href",
        "interpolate",
        "node",
        "edge",
        "state",
    ];
    let mut class_defs: Vec<&str> = Vec::new();
    let mut class_uses: Vec<&str> = Vec::new();
    for line in block.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("classDef ") {
            class_defs.push(rest.split(' ').next().expect("class name"));
        } else if let Some(rest) = line.strip_prefix("class ") {
            class_uses.push(rest.split(' ').nth(1).expect("class name"));
        } else if let Some((_, rest)) = line.split_once(":::") {
            class_uses.push(rest.trim());
        }
    }
    for name in class_defs.iter().chain(&class_uses) {
        assert!(
            !RESERVED.contains(name),
            "Mermaid keyword as class name: {name}"
        );
        assert!(
            name.starts_with("mm_"),
            "class name without the mm_ prefix: {name}"
        );
    }
    for name in &class_uses {
        assert!(
            class_defs.contains(name),
            "class {name} used without classDef"
        );
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

/// The id of the node whose table row contains `needle` (`| n3 | consumer | ... |`).
fn id_of(text: &str, needle: &str) -> String {
    text.lines()
        .find(|l| l.starts_with("| n") && l.contains(needle))
        .and_then(|l| l.split(' ').nth(1))
        .unwrap_or_else(|| panic!("no table row with {needle} in\n{text}"))
        .to_string()
}

/// Whether the diagram has an arrow (solid or dashed, labelled or not) from `from` to `to`.
fn has_edge(text: &str, from: &str, to: &str) -> bool {
    mermaid_block(text).lines().any(|l| {
        let l = l.trim();
        (l.starts_with(&format!("{from} -->")) || l.starts_with(&format!("{from} -.->")))
            && l.ends_with(&format!(" {to}"))
    })
}

/// `Some("-->")` (solid) or `Some("-.->")` (dashed) for the arrow from `from` to `to`.
fn edge_style(text: &str, from: &str, to: &str) -> Option<&'static str> {
    mermaid_block(text).lines().find_map(|l| {
        let l = l.trim();
        if !l.ends_with(&format!(" {to}")) {
            return None;
        }
        [("-->", "-->"), ("-.->", "-.->")]
            .into_iter()
            .find(|(arrow, _)| l.starts_with(&format!("{from} {arrow}")))
            .map(|(_, a)| a)
    })
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
        r#"n0(["?:stock.reserved"]):::mm_centre"#,
        r#"n1["StockPublisher.publishReserved<br/>src/stock.publisher.ts:9"]:::mm_site"#,
        r#"n4[/"POST /orders<br/>src/order.controller.ts:9"/]:::mm_origin"#,
        r#"n5["ShippingConsumer.onStockReserved<br/>src/shipping.consumer.ts:10"]:::mm_site"#,
        r#"n2 -->|"3 calls"| n1"#,
        r#"n4 -->|"3 calls"| n1"#,
        r#"n0 --> n5"#,
        r#"n5 -->|"2 calls"| n6"#,
        // An ambiguous call hop: dashed, labelled `?`, red.
        r#"n8 -.->|"? 2 calls"| n9"#,
        r#"linkStyle 8 stroke:#c92a2a,color:#c92a2a"#,
        // Past the ambiguous call hop every arrow is at most ambiguous (weakest chain), before
        // it nothing is.
        r#"n7 --> n8"#,
        r#"n9 -.->|"?"| n10"#,
        r#"n11 -.->|"?"| n12"#,
        r#"linkStyle 9 stroke:#c92a2a,color:#c92a2a"#,
        r#"linkStyle 11 stroke:#c92a2a,color:#c92a2a"#,
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
            r#"n4[["1 consumer#40;s#41; not expanded: depth limit #40;1#41;"]]:::mm_bound"#
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
    assert!(has_line(&text, "class n0 mm_cycle"), "{text}");
    assert!(has_line(&text, "n3 --> n0"), "{text}");
    // The runtime-built topic is a distinct 'not statically resolved' node, never a dead end.
    assert!(
        has_line(
            &text,
            r#"n10(["?:$#123;invoice_dead_topic#125;"]):::mm_unres"#
        ),
        "{text}"
    );
    assert!(
        has_line(
            &text,
            r#"n12{{"not statically resolved: no consumer found"}}:::mm_unres"#
        ),
        "{text}"
    );
    assert!(has_line(&text, "n10 -.-> n12"), "{text}");
    // A consumer that publishes nothing is a chain end, said so.
    assert!(
        has_line(
            &text,
            r#"n11{{"not statically resolved: no publish found"}}:::mm_unres"#
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
        r#"n1["InvoiceController.retry<br/>src/#40;billing#41;/#91;id#93;/invoice.controller.ts:10"]:::mm_site"#,
        r#"n2[/"POST /invoices/#123;id#125;/retry<br/>src/#40;billing#41;/#91;id#93;/invoice.controller.ts:9"/]:::mm_origin"#,
        r#"n3["LedgerConsumer.post<br/>src/ledger#59;v2/ledger.consumer.ts:10"]:::mm_site"#,
        r#"n6["InvoicePublisher.publishIssued<br/>src/#40;billing#41;/#91;id#93;/#233;t#233;/invoice.publisher.ts:9"]:::mm_site"#,
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
            r#"n0(["?:very.long.event.name.that.keeps.goi..."]):::mm_centre"#
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
        has_line(&text, r#"n16[["+9 not shown #40;cap#41;"]]:::mm_bound"#),
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
        kafka.contains(r#"n0(["kafka:orders"]):::mm_centre"#),
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
    let kafka_centre = id_of(&bare, "| centre event | `kafka:orders` |");
    let rabbit_centre = id_of(&bare, "| centre event | `rabbitmq:orders` |");
    let kafka_consumer = id_of(&bare, "`src/kafka.ts:12`");
    let rabbit_consumer = id_of(&bare, "`src/rabbit.ts:8`");
    assert!(has_edge(&bare, &kafka_centre, &kafka_consumer), "{bare}");
    assert!(has_edge(&bare, &rabbit_centre, &rabbit_consumer), "{bare}");
    assert!(!has_edge(&bare, &rabbit_centre, &kafka_consumer), "{bare}");
    assert!(!has_edge(&bare, &kafka_centre, &rabbit_consumer), "{bare}");
    for centre in [
        "kafka:orders",
        "rabbitmq:orders",
        "redis:orders",
        "sqs:orders",
    ] {
        assert!(
            bare.contains(&format!("([\"{centre}\"]):::mm_centre")),
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
    // The centre has no producer: said so, not left bare.
    assert!(
        ok.contains("not statically resolved: no producer found"),
        "{ok}"
    );
    let asked = call(&state, json!({ "event": token })).await;
    // A credential-shaped name that matches nothing is not echoed back either.
    let unknown = format!("{}{}", "tok_", "ffeeddccbbaa9988");
    let unmatched = call(&state, json!({ "event": unknown })).await;
    assert!(
        unmatched.contains("No event matched `[REDACTED_SECRET: USE_ENV_OR_LOCAL_FALLBACK]`"),
        "{unmatched}"
    );
    assert!(
        !unmatched.to_ascii_lowercase().contains(&unknown),
        "{unmatched}"
    );
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
async fn a_name_that_matches_several_different_events_draws_nothing_and_lists_them() {
    let (_d, state) = temp_workspace(&[
        ("a.ts", &listener("A", "order.requested", None)),
        ("b.ts", &listener("B", "order.shipped", None)),
        ("c.ts", &listener("C", "orders.archive", None)),
    ]);
    let text = call(&state, json!({ "event": "order" })).await;
    assert!(!text.contains("```mermaid"), "{text}");
    for line in [
        "**ambiguous event: 3 events match `order`** by substring, so nothing is drawn. Pass the exact key, for example `visualize_mesh(event: \"order.requested\")`.",
        "| `order.requested` | unknown | 0 | 1 |",
        "| `order.shipped` | unknown | 0 | 1 |",
        "| `orders.archive` | unknown | 0 | 1 |",
    ] {
        assert!(text.contains(line), "missing `{line}` in\n{text}");
    }
    // An exact key is one event: drawn, not merged with its neighbours.
    let exact = call(
        &state,
        json!({ "event": "order.requested", "direction": "downstream" }),
    )
    .await;
    assert_mermaid_sane(&exact);
    assert!(
        has_line(&exact, r#"n0(["?:order.requested"]):::mm_centre"#),
        "{exact}"
    );
    assert!(!exact.contains("order.shipped"), "{exact}");
}

#[tokio::test]
async fn an_ambiguous_list_is_capped_and_says_how_many_more() {
    let files: Vec<(String, String)> = (0..25)
        .map(|i| {
            (
                format!("e{i:02}.ts"),
                listener(&format!("E{i:02}"), &format!("many.evt{i:02}"), None),
            )
        })
        .collect();
    let (_d, state) = temp_from(&files);
    let text = call(&state, json!({ "event": "many" })).await;
    assert!(
        text.contains("**ambiguous event: 25 events match `many`**"),
        "{text}"
    );
    assert!(
        text.contains("| `many.evt19` | unknown | 0 | 1 |"),
        "{text}"
    );
    assert!(!text.contains("many.evt20"), "{text}");
    assert!(
        text.contains("*5 more not listed (the first 20 by key)"),
        "{text}"
    );
}

#[tokio::test]
async fn call_depth_limit_is_a_stub_and_a_note_not_a_chain_end() {
    // consumer -> s0 -> s1 -> ... -> s8 -> publish: nine calls, the walk is cut at six.
    let mut files: Vec<(String, String)> = Vec::new();
    let chain = 9;
    files.push((
        "consumer.ts".to_string(),
        "import { Controller } from '@nestjs/common';\nimport { EventPattern } from '@nestjs/microservices';\nimport { S0 } from './s0';\n@Controller()\nexport class Entry {\n  constructor(private readonly s0: S0) {}\n  @EventPattern('deep.in')\n  async handle(p: any) { this.s0.run(p); }\n}\n"
            .to_string(),
    ));
    for i in 0..chain {
        let body = if i + 1 < chain {
            "this.next.run(p);".to_string()
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
            "<br/>...y_long_directory_name_for_a_service_module2/consumer.ts:7\"]:::mm_site"
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

#[tokio::test]
async fn a_single_broker_workspace_still_qualifies_every_event() {
    let src = "import { Kafka } from 'kafkajs';\nconst kafka = new Kafka({ clientId: 's', brokers: ['localhost:9092'] });\nconst producer = kafka.producer();\nconst consumer = kafka.consumer({ groupId: 'g' });\nexport async function put(): Promise<void> {\n  await producer.send({ topic: 'solo.topic', messages: [] });\n}\nexport async function get(): Promise<void> {\n  await consumer.subscribe({ topic: 'solo.topic' });\n  await put2();\n}\nasync function put2(): Promise<void> {\n  await producer.send({ topic: 'solo.next', messages: [] });\n}\n";
    let (_d, state) = temp_workspace(&[("k.ts", src)]);
    let text = call(&state, json!({ "event": "solo.topic" })).await;
    assert_mermaid_sane(&text);
    assert!(
        has_line(&text, r#"n0(["kafka:solo.topic"]):::mm_centre"#),
        "{text}"
    );
    assert!(
        text.contains(r#"(["kafka:solo.next"]):::mm_event"#),
        "{text}"
    );
    assert!(!mermaid_block(&text).contains("?:"), "{text}");
}

#[tokio::test]
async fn credential_shaped_route_paths_are_redacted_and_braces_are_not() {
    let segment = format!("{}{}", "H3a9f1c7e2b4d6a8c0e1", "f3a5");
    let src = format!(
        "import {{ Controller, Injectable, Post }} from '@nestjs/common';\nimport {{ ClientKafka }} from '@nestjs/microservices';\n@Injectable()\nexport class Svc {{\n  constructor(private readonly client: ClientKafka) {{}}\n  pub(p: any) {{ this.client.emit('route.event', p); }}\n}}\n@Controller('x')\nexport class Gate {{\n  constructor(private readonly svc: Svc) {{}}\n  @Post('{segment}')\n  go(p: any) {{ this.svc.pub(p); }}\n  @Post('{{id}}/retry')\n  retry(p: any) {{ this.svc.pub(p); }}\n}}\n"
    );
    let (_d, state) = temp_workspace(&[("g.ts", &src)]);
    let text = call(
        &state,
        json!({ "event": "route.event", "direction": "upstream" }),
    )
    .await;
    assert_mermaid_sane(&text);
    assert!(
        !text
            .to_ascii_lowercase()
            .contains(&segment.to_ascii_lowercase()),
        "{text}"
    );
    assert!(
        text.contains("| entry | `POST [REDACTED_SECRET: USE_ENV_OR_LOCAL_FALLBACK]` |"),
        "{text}"
    );
    assert!(text.contains("| entry | `POST /x/{id}/retry` |"), "{text}");
}

/// Measurement, not an assertion: a synthetic 2,000-topic workspace (a chain of consumers that
/// each publish the next topic, plus a hub with 300 consumers). Run with
/// `cargo test --release -p mesh-server --test flow_view -- --ignored --nocapture`.
#[tokio::test]
#[ignore]
async fn measure_two_thousand_topics() {
    let mut files: Vec<(String, String)> = Vec::new();
    for i in 0..2000 {
        files.push((
            format!("modules/m{i:04}/c.ts"),
            listener(
                &format!("Chain{i:04}"),
                &format!("topic.{i:04}"),
                Some(&format!("topic.{:04}", i + 1)),
            ),
        ));
    }
    for i in 0..300 {
        files.push((
            format!("hub/h{i:03}.ts"),
            listener(&format!("Hub{i:03}"), "hub.event", None),
        ));
    }
    let t = std::time::Instant::now();
    let (_d, state) = temp_from(&files);
    println!("index (2,300 files, 2,300 topics): {:?}", t.elapsed());
    for (label, args) in [
        ("chain centre, defaults", json!({ "event": "topic.1000" })),
        (
            "chain centre, hops 5/5, detail",
            json!({ "event": "topic.1000", "hops_up": 5, "hops_down": 5, "detail": true }),
        ),
        (
            "hub, limit 200, fan_out 50",
            json!({ "event": "hub.event", "limit": 200, "fan_out": 50 }),
        ),
        ("hub, defaults", json!({ "event": "hub.event" })),
    ] {
        let t = std::time::Instant::now();
        let text = call(&state, args).await;
        println!("{label}: {:?}, {} bytes", t.elapsed(), text.len());
    }
}

#[tokio::test]
async fn a_chain_that_crosses_brokers_keeps_them_apart() {
    let state = state_for("examples/eda/flow-view-crossbroker");
    // Kafka consumer -> bridge -> RabbitMQ publish: the RabbitMQ event is an event of its own.
    let kafka = call(&state, json!({ "event": "kafka:orders" })).await;
    assert_mermaid_sane(&kafka);
    for line in [
        r#"n0(["kafka:orders"]):::mm_centre"#,
        r#"n4(["rabbitmq:orders"]):::mm_event"#,
        r#"n2 -->|"1 call"| n3"#,
        "n3 --> n4",
        "n4 --> n5",
    ] {
        assert!(has_line(&kafka, line), "missing `{line}` in\n{kafka}");
    }
    assert_eq!(
        edge_style(&kafka, "n3", "n0"),
        None,
        "rabbit producer joined the kafka centre\n{kafka}"
    );
    assert_eq!(
        edge_style(&kafka, "n0", "n5"),
        None,
        "rabbit consumer joined the kafka centre\n{kafka}"
    );
    let rabbit = call(&state, json!({ "event": "rabbitmq:orders" })).await;
    assert_mermaid_sane(&rabbit);
    for line in [
        r#"n0(["rabbitmq:orders"]):::mm_centre"#,
        r#"n3(["kafka:orders"]):::mm_event"#,
        "n3 --> n2",
        "n1 --> n0",
        "n0 --> n5",
    ] {
        assert!(has_line(&rabbit, line), "missing `{line}` in\n{rabbit}");
    }
    assert_eq!(
        edge_style(&rabbit, "n0", "n2"),
        None,
        "kafka consumer joined the rabbit centre\n{rabbit}"
    );
    assert_eq!(edge_style(&rabbit, "n1", "n3"), None, "{rabbit}");
    // Both brokers drawn at once: one centre each, the crossing publish lands on the RabbitMQ one.
    let bare = call(&state, json!({ "event": "orders" })).await;
    assert_mermaid_sane(&bare);
    for line in [
        "n2 --> n0",
        "n3 --> n1",
        "n0 --> n4",
        "n1 --> n5",
        "class n1 mm_cycle",
    ] {
        assert!(has_line(&bare, line), "missing `{line}` in\n{bare}");
    }
    assert_eq!(edge_style(&bare, "n0", "n5"), None, "{bare}");
    assert_eq!(edge_style(&bare, "n1", "n4"), None, "{bare}");
}

#[tokio::test]
async fn confidence_is_the_weakest_so_far_along_the_chain() {
    let a = "import { Controller } from '@nestjs/common';\nimport { EventPattern, ClientKafka } from '@nestjs/microservices';\n@Controller()\nexport class A {\n  constructor(private readonly client: ClientKafka) {}\n  @EventPattern('a.evt')\n  onA(p: any) { console.log(p); }\n  pubB(p: any) { this.client.emit('b.evt', p); }\n}\n";
    let b = "import { Controller } from '@nestjs/common';\nimport { EventPattern, ClientKafka } from '@nestjs/microservices';\n@Controller()\nexport class B {\n  constructor(private readonly client: ClientKafka) {}\n  @EventPattern('b.evt')\n  onB(p: any) { this.client.emit('c.evt', p); }\n}\n";
    let (_d, state) = temp_workspace(&[("a.ts", a), ("b.ts", b)]);
    let text = call(
        &state,
        json!({ "event": "a.evt", "direction": "downstream" }),
    )
    .await;
    assert_mermaid_sane(&text);
    // a.evt -> onA is exact; onA -> pubB is the same-file rule (heuristic); everything after
    // that hop is at most heuristic, though onB itself is reached by an exact link.
    for line in [
        "n0 --> n1",
        r#"n1 -.->|"same file"| n2"#,
        "n2 -.-> n3",
        "n3 -.-> n4",
        "n4 -.-> n5",
        "n5 -.-> n6",
    ] {
        assert!(has_line(&text, line), "missing `{line}` in\n{text}");
    }
    // The reverse side obeys the same rule, outwards from the centre.
    let up = call(&state, json!({ "event": "c.evt", "direction": "upstream" })).await;
    assert_mermaid_sane(&up);
    assert_eq!(edge_style(&up, "n1", "n0"), Some("-->"), "{up}");
    let b_site = id_of(&up, "`B.onB` | `b.ts:7`");
    assert!(b_site.starts_with('n'), "{up}");
}

#[tokio::test]
async fn an_unknown_broker_site_joins_a_qualified_event_as_a_dashed_guess() {
    let state = state_for("examples/eda/multi-broker");
    let kafka = call(&state, json!({ "event": "kafka:orders" })).await;
    assert_mermaid_sane(&kafka);
    // The NestJS `@EventPattern('orders')` consumer joins every channel of the name; the Go
    // consumer is known to be Kafka and is solid.
    assert!(
        has_line(
            &kafka,
            r#"n8["consume orders<br/>src/legacy.ts:7"]:::mm_site"#
        ),
        "{kafka}"
    );
    assert_eq!(edge_style(&kafka, "n0", "n8"), Some("-.->"), "{kafka}");
    assert_eq!(edge_style(&kafka, "n0", "n4"), Some("-->"), "{kafka}");
    assert!(
        kafka.contains("its arrows to a broker-qualified event are dashed"),
        "{kafka}"
    );
    // Bare name: the unknown site hangs off EVERY broker's centre, always dashed.
    let bare = call(
        &state,
        json!({ "event": "orders", "direction": "downstream" }),
    )
    .await;
    assert_mermaid_sane(&bare);
    let legacy = id_of(&bare, "`src/legacy.ts:7`");
    for broker in ["kafka", "nats", "rabbitmq", "redis", "sns", "sqs"] {
        let centre = id_of(&bare, &format!("| centre event | `{broker}:orders` |"));
        assert_eq!(
            edge_style(&bare, &centre, &legacy),
            Some("-.->"),
            "{broker}\n{bare}"
        );
    }
}

#[tokio::test]
async fn the_table_names_a_long_node_in_full_up_to_160_characters_and_says_when_it_cut() {
    let name = format!("{}abcdefg", "abcdefghij.".repeat(16));
    assert_eq!(name.len(), 183);
    let (_d, state) = temp_workspace(&[("c.ts", &listener("C", &name, None))]);
    let text = call(&state, json!({ "event": name })).await;
    assert_mermaid_sane(&text);
    let shown = format!("?:{name}");
    let head: String = shown.chars().take(157).collect();
    assert!(
        text.contains(&format!("| centre event | `{head}... (cut, 185 chars)` |")),
        "{text}"
    );
    // The diagram label is cut much shorter, on its own rule.
    assert!(
        text.contains(r#"(["?:abcdefghij.abcdefghij.abcdefghij.ab..."])"#),
        "{text}"
    );
}

#[tokio::test]
async fn over_long_and_non_ascii_names_are_printed_as_the_marker_not_echoed() {
    let state = state_for("examples/eda/layered-ts");
    let long = "x".repeat(300);
    for name in [long.as_str(), "na\u{ef}ve.evt"] {
        let text = call(&state, json!({ "event": name })).await;
        assert!(
            text.contains("No event matched `[REDACTED_SECRET: USE_ENV_OR_LOCAL_FALLBACK]`"),
            "{text}"
        );
        assert!(!text.contains(name), "{text}");
    }
}
