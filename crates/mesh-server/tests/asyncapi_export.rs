#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! 9.0 T12: the `export_asyncapi` tool (AsyncAPI 3 export of the observed event graph, and its
//! declared-versus-code diff) on the hand-annotated fixture `examples/eda/asyncapi-declared`
//! (`tests/golden/events/asyncapi.report.yaml`): the exported document's structure, its truth,
//! determinism, paging, scope, the diff rows, secrets canaries and the documentation counts.

use mesh_core::{expand_roots, AppState, AuditLogger, BackgroundRescanEngine, Config};
use mesh_server::tools::ToolRegistry;
use mesh_server::WorkspaceIndexer;
use serde::Deserialize;
use serde_json::{json, Value as Json};
use serde_yaml::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

fn repo(rel: &str) -> PathBuf {
    dunce::canonicalize(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(rel),
    )
    .expect("canonicalize")
}

fn state_in(dir: &Path) -> Arc<AppState> {
    let config = Config::load_from_file(&dir.join("mesh-mcp.toml")).expect("fixture config");
    let roots = expand_roots(
        &config.workspace.roots,
        dir,
        &config.workspace.workspace_root,
    )
    .expect("expand roots");
    let audit = Arc::new(AuditLogger::new_in_memory().expect("audit"));
    let rescan = Arc::new(BackgroundRescanEngine::new().expect("rescan"));
    let state = Arc::new(AppState::new(config, roots, audit, rescan));
    let snapshot =
        WorkspaceIndexer::build_snapshot(&state.config, &state.allowed_roots, None, None, None);
    state.install_snapshot(snapshot);
    state
}

/// An absolute scope inside the fixture (the jail resolves a relative one against the workspace
/// root, which a single-root `.` fixture does not have).
fn scope_of(rel: &str) -> String {
    repo("examples/eda/asyncapi-declared")
        .join(rel)
        .to_string_lossy()
        .into_owned()
}

fn fixture() -> Arc<AppState> {
    state_in(&repo("examples/eda/asyncapi-declared"))
}

/// The tool's raw `CallToolResult`.
async fn call(state: &Arc<AppState>, args: Json) -> Json {
    ToolRegistry::call_tool("export_asyncapi", args, state.clone())
        .await
        .expect("tool call")
}

async fn text(state: &Arc<AppState>, args: Json) -> String {
    let res = call(state, args).await;
    assert_eq!(res["isError"], json!(false), "{res}");
    res["content"][0]["text"]
        .as_str()
        .expect("text")
        .to_string()
}

/// The document of a page: the fenced block, parsed.
fn document(page: &str) -> Value {
    let start = page.find("```yaml\n").expect("yaml fence") + 8;
    let end = page[start..].find("\n```").expect("closing fence") + start;
    serde_yaml::from_str(&page[start..end]).expect("the block is valid YAML")
}

fn keys(v: &Value) -> BTreeSet<String> {
    v.as_mapping()
        .expect("mapping")
        .keys()
        .map(|k| k.as_str().expect("string key").to_string())
        .collect()
}

/// Every `$ref` of a document (iterative walk).
fn refs(doc: &Value) -> Vec<String> {
    let mut out = Vec::new();
    let mut stack = vec![doc];
    while let Some(v) = stack.pop() {
        match v {
            Value::Mapping(m) => {
                for (k, child) in m {
                    if k.as_str() == Some("$ref") {
                        out.push(child.as_str().expect("$ref is a string").to_string());
                    } else {
                        stack.push(child);
                    }
                }
            }
            Value::Sequence(s) => stack.extend(s),
            _ => {}
        }
    }
    out.sort();
    out
}

fn resolve<'a>(doc: &'a Value, r: &str) -> Option<&'a Value> {
    let mut cur = doc;
    for seg in r.strip_prefix("#/")?.split('/') {
        cur = cur.get(seg)?;
    }
    Some(cur)
}

/// The structural rules of an AsyncAPI 3.0 document this export promises: required keys,
/// string `info.title` / `info.version`, `host` and `protocol` on every server, every `$ref`
/// resolves, each operation names an existing channel and only messages of that channel, each
/// message payload is a schema. Map keys are unique by construction; the operation count is
/// checked against the sites separately.
fn assert_valid_asyncapi3(doc: &Value) {
    assert_eq!(doc["asyncapi"].as_str(), Some("3.0.0"));
    assert!(doc["info"]["title"].is_string() && doc["info"]["version"].is_string());
    for (id, s) in doc["servers"].as_mapping().into_iter().flatten() {
        assert!(
            s["host"].is_string() && s["protocol"].is_string(),
            "server {id:?}"
        );
    }
    for (id, c) in doc["channels"].as_mapping().expect("channels") {
        assert!(c["address"].is_string(), "channel {id:?} has an address");
    }
    for r in refs(doc) {
        assert!(resolve(doc, &r).is_some(), "dangling $ref {r}");
    }
    for (id, op) in doc["operations"].as_mapping().expect("operations") {
        assert!(
            matches!(op["action"].as_str(), Some("send" | "receive")),
            "operation {id:?}"
        );
        let chan = op["channel"]["$ref"].as_str().expect("channel ref");
        assert!(chan.starts_with("#/channels/"), "{chan}");
        for m in op["messages"].as_sequence().into_iter().flatten() {
            let r = m["$ref"].as_str().expect("message ref");
            assert!(
                r.starts_with(&format!("{chan}/messages/")),
                "{r} not a message of {chan}"
            );
        }
    }
    for (name, m) in doc["components"]["messages"]
        .as_mapping()
        .into_iter()
        .flatten()
    {
        let r = m["payload"]["$ref"].as_str().expect("payload ref");
        assert!(r.starts_with("#/components/schemas/"), "{name:?} {r}");
    }
}

#[derive(Deserialize)]
struct Truth {
    export: ExportTruth,
    diff: DiffTruth,
}

#[derive(Deserialize)]
struct ExportTruth {
    channels: Vec<ChannelTruth>,
    not_statically_resolved: Vec<String>,
    must_not_export: Vec<String>,
}

#[derive(Deserialize)]
struct ChannelTruth {
    id: String,
    address: String,
    broker: String,
    sends: Vec<String>,
    receives: Vec<String>,
    schema: Option<String>,
}

#[derive(Deserialize)]
struct DiffTruth {
    declared_not_in_code: Vec<String>,
    in_code_not_declared: Vec<String>,
    payload_disagreement: Vec<PayloadTruth>,
    broker_disagreement: Vec<String>,
    unresolved_topics: usize,
}

#[derive(Deserialize)]
struct PayloadTruth {
    key: String,
    findings: Vec<String>,
}

fn truth() -> Truth {
    let path = repo("tests/golden/events/asyncapi.report.yaml");
    serde_yaml::from_str(&std::fs::read_to_string(path).expect("truth")).expect("parse truth")
}

#[tokio::test]
async fn export_channels_operations_and_sources_match_the_hand_annotated_truth() {
    let page = text(&fixture(), json!({})).await;
    let doc = document(&page);
    assert_valid_asyncapi3(&doc);
    let t = truth().export;
    let expected: BTreeSet<String> = t.channels.iter().map(|c| c.id.clone()).collect();
    assert_eq!(keys(&doc["channels"]), expected);
    let mut sends_total = 0;
    let mut receives_total = 0;
    for c in &t.channels {
        let ch = &doc["channels"][c.id.as_str()];
        assert_eq!(ch["address"].as_str(), Some(c.address.as_str()), "{}", c.id);
        assert_eq!(
            ch["x-mesh-broker"].as_str(),
            Some(c.broker.as_str()),
            "{}",
            c.id
        );
        // A known broker has a server; an unknown one has none and says it is heuristic.
        if c.broker == "unknown" {
            assert!(ch.get("servers").is_none(), "{}", c.id);
            assert_eq!(
                ch["x-mesh-confidence"].as_str(),
                Some("heuristic"),
                "{}",
                c.id
            );
        } else {
            let server = format!("#/servers/{}", c.broker);
            assert_eq!(
                ch["servers"][0]["$ref"].as_str(),
                Some(server.as_str()),
                "{}",
                c.id
            );
        }
        let ops = |action: &str| -> BTreeSet<String> {
            doc["operations"]
                .as_mapping()
                .expect("operations")
                .values()
                .filter(|o| {
                    o["action"].as_str() == Some(action)
                        && o["channel"]["$ref"].as_str() == Some(&format!("#/channels/{}", c.id))
                })
                .map(|o| o["x-mesh-source"].as_str().expect("source").to_string())
                .collect()
        };
        assert_eq!(
            ops("send"),
            c.sends.iter().cloned().collect(),
            "{} sends",
            c.id
        );
        assert_eq!(
            ops("receive"),
            c.receives.iter().cloned().collect(),
            "{} receives",
            c.id
        );
        sends_total += c.sends.len();
        receives_total += c.receives.len();
        if let Some(schema) = &c.schema {
            let (name, loc) = schema.split_once(' ').expect("name loc");
            assert_eq!(
                ch["messages"][name]["$ref"].as_str(),
                Some(format!("#/components/messages/{name}").as_str()),
                "{}",
                c.id
            );
            assert_eq!(
                doc["components"]["schemas"][name]["x-mesh-source"].as_str(),
                Some(loc)
            );
        }
    }
    assert_eq!(
        doc["operations"].as_mapping().expect("operations").len(),
        sends_total + receives_total
    );
    // Declared documents are the declaration, never observed channels.
    for absent in &t.must_not_export {
        assert!(
            !page.contains(&format!("address: {absent}")) && !page.contains(&format!("{absent}:")),
            "{absent} must not be exported"
        );
    }
    // A placeholder topic is not a channel: counted and named under x-mesh.
    let unresolved = &doc["x-mesh"]["not_statically_resolved"];
    assert_eq!(
        unresolved["count"].as_u64(),
        Some(t.not_statically_resolved.len() as u64)
    );
    let names: Vec<&str> = unresolved["topics_listed_on_first_page"]
        .as_sequence()
        .expect("list")
        .iter()
        .map(|e| e["name"].as_str().expect("name"))
        .collect();
    assert_eq!(names, t.not_statically_resolved);
    assert_eq!(doc["x-mesh"]["partial"].as_bool(), Some(false));
    assert!(!page.contains("PARTIAL DOCUMENT"));
}

#[tokio::test]
async fn payload_schemas_come_from_the_t11_types_not_from_a_second_extraction() {
    let doc = document(&text(&fixture(), json!({})).await);
    let schemas = &doc["components"]["schemas"];
    // OrderCreated (src/order.events.ts): orderId string, total number, currency string.
    let order = &schemas["OrderCreated"];
    assert_eq!(order["type"].as_str(), Some("object"));
    assert_eq!(
        order["properties"]["total"]["type"].as_str(),
        Some("number")
    );
    assert_eq!(
        order["properties"]["orderId"]["type"].as_str(),
        Some("string")
    );
    assert_eq!(
        order["required"]
            .as_sequence()
            .expect("required")
            .iter()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>(),
        vec!["currency", "orderId", "total"]
    );
    // PaymentEvent is a Java record: `long amount` is a number.
    assert_eq!(
        schemas["PaymentEvent"]["properties"]["amount"]["type"].as_str(),
        Some("number")
    );
    // An inline object payload has fields and no type name; a field with no written type claims no JSON type.
    let inline = &schemas["order.legacy_Inline1"];
    assert_eq!(
        keys(&inline["properties"]),
        BTreeSet::from(["orderId".to_string()])
    );
    assert!(inline["properties"]["orderId"].get("type").is_none());
}

#[tokio::test]
async fn export_is_deterministic_and_json_and_yaml_say_the_same() {
    let state = fixture();
    let a = text(&state, json!({})).await;
    let b = text(&state, json!({})).await;
    assert_eq!(a, b);
    // A second index of the same tree gives the same bytes.
    assert_eq!(text(&fixture(), json!({})).await, a);
    let as_json = text(&state, json!({"format": "json"})).await;
    let start = as_json.find("```json\n").expect("json fence") + 8;
    let end = as_json[start..].find("\n```").expect("end") + start;
    let from_json: Json = serde_json::from_str(&as_json[start..end]).expect("json");
    let from_yaml: Json = serde_json::to_value(document(&a)).expect("to json");
    assert_eq!(from_json, from_yaml);
}

#[tokio::test]
async fn pages_cover_every_channel_once_and_never_read_as_the_whole_document() {
    let state = fixture();
    let mut seen: Vec<BTreeSet<String>> = Vec::new();
    let mut offset = 0u64;
    let mut pages = 0;
    loop {
        let page = text(&state, json!({"limit": 4, "offset": offset})).await;
        let doc = document(&page);
        assert_valid_asyncapi3(&doc);
        let n = doc["x-mesh"]["channels_in_document"].as_u64().expect("n");
        assert!(n >= 1, "a page always advances");
        seen.push(keys(&doc["channels"]));
        pages += 1;
        match doc["x-mesh"]["next_offset"].as_u64() {
            Some(next) => {
                assert_eq!(next, offset + n);
                assert!(page.contains("PARTIAL DOCUMENT"), "{page}");
                assert!(
                    page.contains(&format!("next page: `offset: {next}`")),
                    "{page}"
                );
                assert_eq!(doc["x-mesh"]["partial"].as_bool(), Some(true));
                offset = next;
            }
            None => {
                // The last page of a paged export is still partial (it is not the first).
                assert_eq!(doc["x-mesh"]["partial"].as_bool(), Some(true));
                break;
            }
        }
    }
    assert_eq!(pages, 2);
    // The unresolved topic is listed on the first page only, counted on every page.
    let second = document(&text(&state, json!({"limit": 4, "offset": 4})).await);
    assert_eq!(
        second["x-mesh"]["not_statically_resolved"]["count"].as_u64(),
        Some(1)
    );
    assert_eq!(
        second["x-mesh"]["not_statically_resolved"]["topics_listed_on_first_page"]
            .as_sequence()
            .map(Vec::len),
        Some(0)
    );
    // Pages follow the address order (email, order.audit.eu, order.created, order.legacy |
    // order.shipped, payments.captured); inside a page the map is sorted by channel id.
    let set = |ids: &[&str]| -> BTreeSet<String> { ids.iter().map(|i| i.to_string()).collect() };
    assert_eq!(
        seen,
        vec![
            set(&[
                "bull_email",
                "order.audit.eu",
                "order.created",
                "order.legacy"
            ]),
            set(&["kafka_payments.captured", "order.shipped"]),
        ]
    );
    let all: BTreeSet<String> = seen.into_iter().flatten().collect();
    let expected: BTreeSet<String> = truth()
        .export
        .channels
        .iter()
        .map(|c| c.id.clone())
        .collect();
    assert_eq!(all, expected);
    // Past the end: said, no document.
    let past = text(&state, json!({"offset": 50})).await;
    assert!(
        past.contains("past the last channel (6 in scope)"),
        "{past}"
    );
    assert!(!past.contains("```"));
}

#[tokio::test]
async fn scope_keeps_the_channels_with_a_site_under_it_and_the_jail_still_holds() {
    let state = fixture();
    let page = text(&state, json!({"scope": scope_of("src/main")})).await;
    let doc = document(&page);
    assert_eq!(
        keys(&doc["channels"]),
        BTreeSet::from(["kafka_payments.captured".to_string()])
    );
    // The placeholder topic of LedgerListener.java is in scope too.
    assert_eq!(
        doc["x-mesh"]["not_statically_resolved"]["count"].as_u64(),
        Some(1)
    );
    let none = text(&state, json!({"scope": scope_of("legacy")})).await;
    assert!(
        none.contains("No channel with a statically resolved topic name is in this scope"),
        "{none}"
    );
    let escape = call(&state, json!({"scope": "../../.."})).await;
    assert_eq!(escape["isError"], json!(true), "{escape}");
    let bad = call(&state, json!({"mode": "diff", "scope": "/etc"})).await;
    assert_eq!(bad["isError"], json!(true), "{bad}");
    let unknown = call(&state, json!({"topic": "order.created"})).await;
    assert_eq!(unknown["isError"], json!(true), "{unknown}");
}

/// The rows of one `### ` section of a diff page: `(key, indented sub-lines)`.
fn diff_rows(page: &str, title: &str) -> Vec<(String, Vec<String>)> {
    let mut out: Vec<(String, Vec<String>)> = Vec::new();
    let mut inside = false;
    for line in page.lines() {
        if let Some(t) = line.strip_prefix("### ") {
            inside = t == title;
        } else if inside {
            if let Some(row) = line.strip_prefix("- `") {
                let key = row.split('`').next().expect("key").to_string();
                out.push((key, Vec::new()));
            } else if let Some(sub) = line.strip_prefix("  - ") {
                out.last_mut().expect("row").1.push(sub.to_string());
            }
        }
    }
    out
}

#[tokio::test]
async fn diff_reports_the_deliberate_disagreements_and_nothing_else() {
    let page = text(&fixture(), json!({"mode": "diff"})).await;
    let t = truth().diff;

    // Header: the two documents read.
    assert!(
        page.contains("| `asyncapi.yaml` | 3.0.0 | 5 | 6 |"),
        "{page}"
    );
    assert!(
        page.contains("| `legacy/asyncapi.v2.yaml` | 2.6.0 | 1 | 1 |"),
        "{page}"
    );

    let declared = diff_rows(&page, "Declared, not in code");
    assert_eq!(declared.len(), t.declared_not_in_code.len());
    assert_eq!(declared[0].0, "order.refunded.v1");
    assert!(
        page.contains("- `order.refunded.v1` [broker unknown] declared at `asyncapi.yaml:25`"),
        "{page}"
    );

    let undeclared = diff_rows(&page, "In code, not declared");
    let got: BTreeMap<String, String> = undeclared
        .iter()
        .map(|(k, sub)| (k.clone(), sub.join(" / ")))
        .collect();
    assert_eq!(got.len(), t.in_code_not_declared.len(), "{got:?}");
    assert!(
        got["payments.captured"].contains("the code sends on this channel"),
        "{got:?}"
    );
    assert!(
        got["email"].contains("the code receives on this channel"),
        "{got:?}"
    );
    assert!(
        got["order.legacy"].contains("1 producer(s), 0 consumer(s)"),
        "{got:?}"
    );

    let payload = diff_rows(&page, "Payload disagreement");
    assert_eq!(payload.len(), t.payload_disagreement.len());
    for want in &t.payload_disagreement {
        let row = payload
            .iter()
            .find(|(k, _)| *k == want.key)
            .expect(&want.key);
        assert_eq!(row.1, want.findings, "{}", want.key);
    }

    let broker = diff_rows(&page, "Broker disagreement");
    assert_eq!(
        broker.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(),
        t.broker_disagreement
    );
    assert!(
        broker[0].1[0].contains("declared server protocol(s) `amqp`; the code uses `kafka`"),
        "{page}"
    );

    // Decoys: the matching channel, the wildcard-heard one and the redis-admitted queue have no
    // row of the classes that would be wrong for them.
    for (class, key) in [
        ("Declared, not in code", "order.shipped"),
        ("In code, not declared", "order.shipped"),
        ("Payload disagreement", "order.shipped"),
        ("Broker disagreement", "order.shipped"),
        ("In code, not declared", "order.audit.eu"),
        ("Declared, not in code", "order.audit.eu"),
        ("Broker disagreement", "email"),
        ("Payload disagreement", "payments.captured"),
    ] {
        assert!(
            diff_rows(&page, class).iter().all(|(k, _)| k != key),
            "{key} wrongly in {class}"
        );
    }
    // The placeholder topic is counted, never compared.
    assert!(
        page.contains(&format!(
            "{} observed topic(s) are named by a `${{KEY}}` placeholder",
            t.unresolved_topics
        )),
        "{page}"
    );
}

#[tokio::test]
async fn diff_paging_advances_per_class_and_scope_filters_declared_files() {
    let state = fixture();
    let p1 = text(&state, json!({"mode": "diff", "limit": 1})).await;
    assert!(
        p1.contains("`email`") && !p1.contains("- `order.legacy`"),
        "{p1}"
    );
    assert!(
        p1.contains("2 more row(s) of this class, `offset: 1`"),
        "{p1}"
    );
    let p3 = text(&state, json!({"mode": "diff", "limit": 1, "offset": 2})).await;
    assert!(
        p3.contains("- `payments.captured` [kafka]") && !p3.contains("Next page"),
        "{p3}"
    );
    // Only the legacy document is under `legacy`: its channel is the only declared one compared as
    // in scope, and the version 3 document's rows are left out.
    let legacy = text(&state, json!({"mode": "diff", "scope": scope_of("legacy")})).await;
    assert!(legacy.contains("`email`"), "{legacy}");
    assert!(!legacy.contains("order.refunded.v1"), "{legacy}");
}

#[tokio::test]
async fn diff_without_a_declared_document_is_not_an_everything_undeclared_report() {
    let state = state_in(&repo("examples/eda/payload-types"));
    let page = text(&state, json!({"mode": "diff"})).await;
    assert!(
        page.contains("No AsyncAPI document is indexed in this workspace"),
        "{page}"
    );
    assert!(!page.contains("In code, not declared"), "{page}");
    // The export of the same workspace still works.
    let export = text(&state, json!({})).await;
    assert!(export.contains("```yaml"), "{export}");
}

fn write(dir: &Path, rel: &str, body: &str) {
    let p = dir.join(rel);
    std::fs::create_dir_all(p.parent().expect("parent")).expect("mkdir");
    std::fs::write(p, body).expect("write");
}

const CONFIG: &str = "[workspace]\nname = \"w\"\nversion = \"1.0.0\"\nroots = [\".\"]\n\n[engines.contracts]\nenabled = true\n";

#[tokio::test]
async fn credential_canaries_in_names_never_reach_either_mode() {
    // Built at runtime: no key-shaped literal in this file.
    let word = ["pass", "word"].concat();
    let token = ["tok", "_", "q7x9z2"].concat();
    let api = ["api", "_", "key"].concat();
    let default_secret = ["hun", "ter2", "-prod"].concat();
    let dir = tempfile::tempdir().expect("tempdir");
    write(dir.path(), "mesh-mcp.toml", CONFIG);
    write(
        dir.path(),
        "package.json",
        "{\"name\":\"w\",\"dependencies\":{\"@nestjs/microservices\":\"^10\"}}",
    );
    write(
        dir.path(),
        "src/a.ts",
        &format!(
            "import {{ Inject }} from '@nestjs/common';\nimport {{ ClientKafka }} from '@nestjs/microservices';\nexport interface {word}Payload {{ {word}: string; {api}: string; id: string }}\nexport class P {{\n  constructor(@Inject('K') private readonly client: ClientKafka) {{}}\n  go(p: {word}Payload) {{ this.client.emit('orders.created', p); }}\n}}\n"
        ),
    );
    write(
        dir.path(),
        "asyncapi.yaml",
        &format!(
            "asyncapi: 3.0.0\nchannels:\n  c:\n    address: 'orders.created'\n    messages:\n      m:\n        payload:\n          type: object\n          required: [id]\n          properties:\n            id: {{type: string}}\n            {word}: {{type: string}}\n  d:\n    address: '{token}'\n  e:\n    address: 'orders.${{TOPIC:{default_secret}}}'\noperations:\n  s:\n    action: send\n    channel:\n      $ref: '#/channels/c'\n"
        ),
    );
    let state = state_in(dir.path());
    for args in [
        json!({}),
        json!({"mode": "diff"}),
        json!({"format": "json"}),
    ] {
        let page = text(&state, args.clone()).await;
        for canary in [&word, &token, &api, &default_secret] {
            assert!(
                !page.to_lowercase().contains(&canary.to_lowercase()),
                "{canary} leaked in {args}: {page}"
            );
        }
    }
    // The diff still says what it hid.
    let diff = text(&state, json!({"mode": "diff"})).await;
    assert!(
        diff.contains("[REDACTED_SECRET: USE_ENV_OR_LOCAL_FALLBACK]"),
        "{diff}"
    );
}

#[tokio::test]
async fn a_big_channel_is_cut_with_a_count_and_every_page_stays_under_the_budget() {
    let dir = tempfile::tempdir().expect("tempdir");
    write(dir.path(), "mesh-mcp.toml", CONFIG);
    write(
        dir.path(),
        "package.json",
        "{\"name\":\"w\",\"dependencies\":{\"@nestjs/microservices\":\"^10\"}}",
    );
    // One topic with 45 producer sites, and 60 more topics.
    let mut ts = String::from("import { Inject } from '@nestjs/common';\nimport { ClientKafka } from '@nestjs/microservices';\nexport class P {\n  constructor(@Inject('K') private readonly client: ClientKafka) {}\n");
    for i in 0..45 {
        ts.push_str(&format!(
            "  m{i}() {{ this.client.emit('hot.topic', {{ n: {i} }}); }}\n"
        ));
    }
    for i in 0..60 {
        ts.push_str(&format!(
            "  t{i}() {{ this.client.emit('topic.number{i:02}.long.name', {{ n: {i} }}); }}\n"
        ));
    }
    ts.push_str("}\n");
    write(dir.path(), "src/p.ts", &ts);
    let state = state_in(dir.path());
    let page = text(&state, json!({"limit": 100})).await;
    let doc = document(&page);
    assert!(page.len() < 40 * 1024, "{}", page.len());
    assert_eq!(
        doc["channels"]["hot.topic"]["x-mesh-operations-not-listed"].as_u64(),
        Some(5)
    );
    assert_eq!(
        doc["operations"]
            .as_mapping()
            .expect("ops")
            .keys()
            .filter(|k| k.as_str().expect("k").contains("hot.topic"))
            .count(),
        40
    );
    // The cap never presents a cut page as complete.
    assert_eq!(doc["x-mesh"]["channels_in_scope"].as_u64(), Some(61));
    assert!(doc["x-mesh"]["channels_in_document"].as_u64().expect("n") < 61);
    assert!(page.contains("PARTIAL DOCUMENT"), "{page}");
    assert_valid_asyncapi3(&doc);
    // The default page size is 20 channels at most.
    let default = document(&text(&state, json!({})).await);
    assert!(
        default["x-mesh"]["channels_in_document"]
            .as_u64()
            .expect("n")
            <= 20
    );
}

#[tokio::test]
async fn the_default_page_holds_twenty_channels_and_points_at_the_next() {
    let dir = tempfile::tempdir().expect("tempdir");
    write(dir.path(), "mesh-mcp.toml", CONFIG);
    write(
        dir.path(),
        "package.json",
        "{\"name\":\"w\",\"dependencies\":{\"@nestjs/microservices\":\"^10\"}}",
    );
    let mut ts = String::from("import { Inject } from '@nestjs/common';\nimport { ClientKafka } from '@nestjs/microservices';\nexport class P {\n  constructor(@Inject('K') private readonly client: ClientKafka) {}\n");
    for i in 0..30 {
        ts.push_str(&format!(
            "  t{i}() {{ this.client.emit('t.n{i:02}', 1); }}\n"
        ));
    }
    ts.push_str("}\n");
    write(dir.path(), "src/p.ts", &ts);
    let state = state_in(dir.path());
    let first = text(&state, json!({})).await;
    let doc = document(&first);
    assert_eq!(doc["x-mesh"]["channels_in_document"].as_u64(), Some(20));
    assert_eq!(doc["x-mesh"]["next_offset"].as_u64(), Some(20));
    assert!(
        first.contains("10 channel(s) are not in this page"),
        "{first}"
    );
    let clamp = document(&text(&state, json!({"limit": 1000})).await);
    assert!(clamp["x-mesh"]["channels_in_document"].as_u64().expect("n") <= 30);
    let zero = document(&text(&state, json!({"limit": 0})).await);
    assert_eq!(
        zero["x-mesh"]["channels_in_document"].as_u64(),
        Some(1),
        "limit 0 is clamped to one, never an empty page"
    );
}

#[tokio::test]
async fn at_most_twenty_declared_documents_are_read_and_the_rest_are_named() {
    let dir = tempfile::tempdir().expect("tempdir");
    write(dir.path(), "mesh-mcp.toml", CONFIG);
    write(dir.path(), "package.json", "{\"name\":\"w\"}");
    for i in 0..22 {
        write(
            dir.path(),
            &format!("specs/s{i:02}/asyncapi.yaml"),
            &format!("asyncapi: 3.0.0\nchannels:\n  c:\n    address: topic.n{i:02}\n"),
        );
    }
    let page = text(&state_in(dir.path()), json!({"mode": "diff", "limit": 100})).await;
    assert!(
        page.contains("20 declared document(s) (20 channel(s)) against 0 observed channel(s)"),
        "{page}"
    );
    assert!(
        page.contains("past the 20 declared documents read per call"),
        "{page}"
    );
    assert_eq!(
        page.matches("past the 20 declared documents").count(),
        2,
        "{page}"
    );
}

#[tokio::test]
async fn a_declared_file_grown_past_the_size_budget_after_indexing_is_refused_before_the_read() {
    let dir = tempfile::tempdir().expect("tempdir");
    write(dir.path(), "mesh-mcp.toml", CONFIG);
    write(dir.path(), "package.json", "{\"name\":\"w\"}");
    write(
        dir.path(),
        "my.asyncapi.yaml",
        "asyncapi: 3.0.0\nchannels:\n  c:\n    address: orders.created\n",
    );
    let state = state_in(dir.path());
    // 384 KB is the budget of a source file; this name is not one of the 1.5 MB schema names.
    let big = format!("asyncapi: 3.0.0\n# {}\n", "x".repeat(400 * 1024));
    write(dir.path(), "my.asyncapi.yaml", &big);
    let page = text(&state, json!({"mode": "diff"})).await;
    assert!(page.contains("oversized"), "{page}");
    assert!(
        page.contains("Every indexed AsyncAPI candidate was skipped"),
        "{page}"
    );
}

#[tokio::test]
async fn test_file_sites_and_declared_channel_nodes_are_not_observed_sites() {
    let dir = tempfile::tempdir().expect("tempdir");
    write(dir.path(), "mesh-mcp.toml", CONFIG);
    write(
        dir.path(),
        "package.json",
        "{\"name\":\"w\",\"dependencies\":{\"@nestjs/microservices\":\"^10\"}}",
    );
    let class = |name: &str, topic: &str| {
        format!("import {{ Inject }} from '@nestjs/common';\nimport {{ ClientKafka }} from '@nestjs/microservices';\nexport class {name} {{\n  constructor(@Inject('K') private readonly client: ClientKafka) {{}}\n  go() {{ this.client.emit('{topic}', {{ id: 1 }}); }}\n}}\n")
    };
    write(dir.path(), "src/a.ts", &class("A", "orders.created"));
    write(
        dir.path(),
        "src/a.spec.ts",
        &class("ASpec", "spec.only.topic"),
    );
    // A declared channel whose id is not its address: its node is the declaration, not code.
    write(
        dir.path(),
        "asyncapi.yaml",
        "asyncapi: 3.0.0\nchannels:\n  declaredOnlyId:\n    address: declared.only.address\n",
    );
    let page = text(&state_in(dir.path()), json!({})).await;
    let doc = document(&page);
    assert_eq!(
        keys(&doc["channels"]),
        BTreeSet::from(["orders.created".to_string()])
    );
    assert!(
        !page.contains("spec.only.topic") && !page.contains("declaredonlyid"),
        "{page}"
    );
    assert!(page.contains("Test-file sites (1) and the channels a declared AsyncAPI file contributes (1) are not counted"), "{page}");
}

#[cfg(unix)]
#[tokio::test]
async fn a_declared_file_replaced_by_a_symlink_is_never_followed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let outside = tempfile::tempdir().expect("outside");
    write(dir.path(), "mesh-mcp.toml", CONFIG);
    write(dir.path(), "package.json", "{\"name\":\"w\"}");
    write(
        dir.path(),
        "asyncapi.yaml",
        "asyncapi: 3.0.0\nchannels:\n  c:\n    address: orders.created\n",
    );
    write(
        outside.path(),
        "other.yaml",
        "asyncapi: 3.0.0\nchannels:\n  c:\n    address: orders.created\n",
    );
    let state = state_in(dir.path());
    std::fs::remove_file(dir.path().join("asyncapi.yaml")).expect("rm");
    std::os::unix::fs::symlink(
        outside.path().join("other.yaml"),
        dir.path().join("asyncapi.yaml"),
    )
    .expect("symlink");
    let page = text(&state, json!({"mode": "diff"})).await;
    assert!(page.contains("a symlink, never followed"), "{page}");
    assert!(
        page.contains("Every indexed AsyncAPI candidate was skipped"),
        "{page}"
    );
}

#[tokio::test]
async fn a_file_that_is_not_asyncapi_is_skipped_and_said() {
    let dir = tempfile::tempdir().expect("tempdir");
    write(dir.path(), "mesh-mcp.toml", CONFIG);
    write(dir.path(), "package.json", "{\"name\":\"w\"}");
    // Indexed (the name says asyncapi) but it is a v1 document: unsupported, not compared.
    write(
        dir.path(),
        "asyncapi.yaml",
        "asyncapi: '1.2.0'\nchannels:\n  c:\n    publish: {}\n",
    );
    let page = text(&state_in(dir.path()), json!({"mode": "diff"})).await;
    assert!(
        page.contains("AsyncAPI version `1.2.0` is not 2.x or 3.x"),
        "{page}"
    );
}

#[test]
fn every_documented_tool_count_says_ten() {
    let root = repo(".");
    let all = ToolRegistry::list_tools();
    assert_eq!(all.as_array().expect("tools").len(), 10);
    for (file, needle) in [
        ("README.md", "ten MCP tools"),
        ("README.md", "## The ten tools"),
        (
            "docs/mcp-tools.md",
            "specifies the ten Model Context Protocol",
        ),
        ("docs/mcp-tools.md", "### Tool 10: `export_asyncapi`"),
        ("SETUP.md", "You should get ten tools back"),
        ("CLAUDE.md", "10 MCP Tools & Schemas"),
        ("docs/governance-rsah.md", "all ten shipped tools"),
        (
            "docs/architecture/04-answering-a-question.md",
            "## The ten tools",
        ),
        ("docs/architecture/01-big-picture.md", "the ten tools"),
        (
            "docs/architecture/07-rust-for-readers.md",
            "Each of the ten tools",
        ),
        ("docs/architecture/README.md", "the ten tools"),
    ] {
        let text = std::fs::read_to_string(root.join(file)).expect(file);
        assert!(text.contains(needle), "{file} must say `{needle}`");
    }
    for file in [
        "README.md",
        "SETUP.md",
        "CLAUDE.md",
        "docs/mcp-tools.md",
        "docs/governance-rsah.md",
        "docs/architecture/04-answering-a-question.md",
        "docs/architecture/01-big-picture.md",
        "docs/architecture/07-rust-for-readers.md",
        "docs/architecture/README.md",
    ] {
        let text = std::fs::read_to_string(root.join(file)).expect(file);
        for stale in [
            "nine tools",
            "nine MCP",
            "the nine",
            "all nine",
            "of the nine",
            "9 MCP Tools",
        ] {
            assert!(!text.contains(stale), "{file} still says `{stale}`");
        }
    }
}
