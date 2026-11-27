#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! 9.0 T11: the payload check of `event_anomalies` (`payload_mismatch` class) on the
//! hand-annotated fixture `examples/eda/payload-types` (`tests/golden/events/payloads.report.yaml`),
//! its determinism (runs, cold and warm index cache), scope, caps said as "not compared", and the
//! secrets canaries.

use mesh_core::{
    expand_roots, AppState, AuditLogger, BackgroundRescanEngine, Config, PersistentIndexCache,
};
use mesh_server::tools::ToolRegistry;
use mesh_server::WorkspaceIndexer;
use serde::Deserialize;
use serde_json::{json, Value};
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

fn state_in(dir: &Path, cache: Option<&PersistentIndexCache>) -> Arc<AppState> {
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
        WorkspaceIndexer::build_snapshot(&state.config, &state.allowed_roots, None, None, cache);
    state.install_snapshot(snapshot);
    state
}

async fn report(state: &Arc<AppState>, args: Value) -> String {
    let res: Value = ToolRegistry::call_tool("event_anomalies", args, state.clone())
        .await
        .expect("tool call");
    assert_eq!(res["isError"], json!(false), "{res}");
    res["content"][0]["text"]
        .as_str()
        .expect("text")
        .to_string()
}

/// One mismatch row as read back from the Markdown.
#[derive(Debug, Default, PartialEq)]
struct Row {
    consumer_only: Vec<String>,
    producer_only: Vec<String>,
    clashes: Vec<String>,
    names_differ: bool,
    notes: Vec<String>,
}

/// `key [broker]` of a row (`` `key` [broker] ``).
fn topic_of(row: &str) -> String {
    let (key, rest) = row
        .strip_prefix('`')
        .and_then(|r| r.split_once('`'))
        .expect("backticked key");
    let broker = rest
        .trim_start()
        .strip_prefix('[')
        .and_then(|r| r.split_once(']'))
        .expect("broker");
    format!("{key} [{}]", broker.0)
}

/// The backticked names of a sub-bullet list (`` `a` (p:1), `b` (p:2) ``).
fn names(list: &str) -> Vec<String> {
    list.split(", ")
        .filter_map(|item| {
            item.strip_prefix('`')
                .and_then(|r| r.split_once('`'))
                .map(|(n, _)| n.to_string())
        })
        .collect()
}

/// The rows of the "Payload mismatches" section, by topic.
fn mismatches(text: &str) -> BTreeMap<String, Row> {
    let mut out: BTreeMap<String, Row> = BTreeMap::new();
    let mut inside = false;
    let mut current: Option<String> = None;
    for line in text.lines() {
        if let Some(title) = line.strip_prefix("### ") {
            inside = title.starts_with("Payload mismatches");
            current = None;
            continue;
        }
        if !inside {
            continue;
        }
        if let Some(row) = line.strip_prefix("- ") {
            let t = topic_of(row);
            out.insert(t.clone(), Row::default());
            current = Some(t);
        } else if let (Some(t), Some(sub)) = (&current, line.strip_prefix("  - ")) {
            let row = out.get_mut(t).expect("row");
            if let Some(l) = sub.strip_prefix("consumer expects, no producer sends: ") {
                row.consumer_only = names(l);
            } else if let Some(l) = sub.strip_prefix("published, no consumer expects: ") {
                row.producer_only = names(l);
            } else if let Some(l) = sub.strip_prefix("type differs: ") {
                row.clashes.extend(names(l.split(' ').next().unwrap_or("")));
            } else if sub == "type names differ between producer and consumer" {
                row.names_differ = true;
            } else {
                row.notes.push(sub.to_string());
            }
        }
    }
    out
}

/// The topics of the "Payloads not compared" footer line.
fn not_compared(text: &str) -> BTreeSet<String> {
    text.lines()
        .find_map(|l| l.strip_prefix("- Payloads not compared: "))
        .map(|l| {
            l.trim_end_matches('.')
                .split(", ")
                .filter(|s| s.starts_with('`'))
                .map(topic_of)
                .collect()
        })
        .unwrap_or_default()
}

fn payload_line(text: &str) -> &str {
    text.lines()
        .find(|l| l.starts_with("- **Payloads**: "))
        .unwrap_or_else(|| panic!("no payload summary:\n{text}"))
}

#[derive(Deserialize)]
struct Golden {
    root: String,
    topics_with_both_ends: usize,
    mismatches: Vec<GoldenRow>,
    matching: Vec<String>,
    not_compared: Vec<String>,
    must_not_flag: Vec<String>,
}

#[derive(Deserialize)]
struct GoldenRow {
    topic: String,
    consumer_only: Vec<String>,
    producer_only: Vec<String>,
    clashes: Vec<String>,
    names_differ: bool,
}

fn golden() -> Golden {
    let text = std::fs::read_to_string(repo("tests/golden/events/payloads.report.yaml"))
        .expect("golden file");
    serde_yaml::from_str(&text).expect("parse golden")
}

#[tokio::test]
async fn the_fixture_matches_the_hand_annotated_truth() {
    let g = golden();
    let state = state_in(&repo(&g.root), None);
    let text = report(&state, json!({ "classes": ["payload_mismatch"], "limit": 100 })).await;
    let got = mismatches(&text);

    let want_topics: BTreeSet<&str> = g.mismatches.iter().map(|r| r.topic.as_str()).collect();
    let got_topics: BTreeSet<&str> = got.keys().map(String::as_str).collect();
    assert_eq!(got_topics, want_topics, "{text}");
    for w in &g.mismatches {
        let row = &got[&w.topic];
        assert_eq!(row.consumer_only, w.consumer_only, "{}: consumer_only", w.topic);
        assert_eq!(row.producer_only, w.producer_only, "{}: producer_only", w.topic);
        assert_eq!(row.clashes, w.clashes, "{}: clashes", w.topic);
        assert_eq!(row.names_differ, w.names_differ, "{}: names_differ", w.topic);
        // Every direction of the fixture's mismatches is compared.
        assert!(row.notes.is_empty(), "{}: {:?}", w.topic, row.notes);
    }
    let want_nc: BTreeSet<String> = g.not_compared.iter().cloned().collect();
    assert_eq!(not_compared(&text), want_nc, "{text}");
    let summary = payload_line(&text);
    assert!(
        summary.contains(&format!(
            "{} topic(s) have live producers and consumers; {} mismatch, {} match (0 of them compared one way only), {} not compared",
            g.topics_with_both_ends,
            g.mismatches.len(),
            g.matching.len(),
            g.not_compared.len()
        )),
        "{summary}"
    );
    assert!(summary.contains("Not compared is not \"no mismatch\""), "{summary}");
    for decoy in &g.must_not_flag {
        assert!(
            !got.keys().any(|t| t.split(" [").next() == Some(decoy.as_str())),
            "{decoy} flagged:\n{text}"
        );
    }
    // The class count of the table agrees with the rows.
    assert!(text.contains("| payload mismatches | 3 |"), "{text}");
    // Both sides of every row give path:line.
    assert!(
        text.contains("producer `OrderCreated` (src/order.events.ts:2) at `src/order.publisher.ts:11` / consumer `BillingOrder` (src/billing.consumer.ts:7) at `src/billing.consumer.ts:17`"),
        "{text}"
    );
    assert!(
        text.contains("`total` is number in the producer (src/order.events.ts:4), string in the consumer (src/billing.consumer.ts:9)"),
        "{text}"
    );
}

#[tokio::test]
async fn the_class_filter_and_the_scope_select_payload_rows() {
    let g = golden();
    let state = state_in(&repo(&g.root), None);
    let other = report(&state, json!({ "classes": ["cycles"] })).await;
    assert!(!other.contains("### Payload mismatches"), "{other}");
    assert!(!other.contains("- **Payloads**"), "{other}");
    // The table still counts the class.
    assert!(other.contains("| payload mismatches | 3 |"), "{other}");

    let scope = repo(&g.root)
        .join("src/main")
        .to_string_lossy()
        .into_owned();
    let java = report(
        &state,
        json!({ "classes": ["payload_mismatch"], "scope": scope }),
    )
    .await;
    let rows: BTreeSet<String> = mismatches(&java).into_keys().collect();
    assert_eq!(
        rows,
        BTreeSet::from(["payments.captured [kafka]".to_string()]),
        "{java}"
    );
}

#[tokio::test]
async fn the_report_is_identical_over_runs_and_index_caches() {
    let dir = repo("examples/eda/payload-types");
    let args = json!({ "classes": ["payload_mismatch"] });
    let plain = report(&state_in(&dir, None), args.clone()).await;
    assert_eq!(plain, report(&state_in(&dir, None), args.clone()).await, "run 2");
    let tmp = tempfile::tempdir().expect("tempdir");
    let cache =
        PersistentIndexCache::open(tmp.path().join("index-cache.db"), u64::MAX).expect("cache");
    for pass in ["cold", "warm"] {
        let got = report(&state_in(&dir, Some(&cache)), args.clone()).await;
        assert_eq!(got, plain, "{pass} cache");
    }
}

/// A workspace of `files` written under a temp dir (a one-root config).
fn workspace(files: &[(&str, String)]) -> (tempfile::TempDir, Arc<AppState>) {
    let tmp = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        tmp.path().join("mesh-mcp.toml"),
        "[workspace]\nname = \"w\"\nversion = \"1.0.0\"\nroots = [\".\"]\n\n[engines.contracts]\nenabled = true\n",
    )
    .expect("config");
    for (name, body) in files {
        let path = tmp.path().join(name);
        std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
        std::fs::write(path, body).expect("write");
    }
    let state = state_in(tmp.path(), None);
    (tmp, state)
}

const NEST: &str = "import { Controller } from '@nestjs/common';\nimport { ClientProxy, EventPattern, Payload } from '@nestjs/microservices';\n";

fn publisher(topic: &str, ty: &str) -> String {
    format!(
        "{NEST}export class P_{ty} {{\n  constructor(private readonly client: ClientProxy) {{}}\n  go(e: {ty}) {{\n    this.client.emit('{topic}', e);\n  }}\n}}\n"
    )
}

fn consumer(topic: &str, ty: &str) -> String {
    format!(
        "{NEST}@Controller()\nexport class C_{ty} {{\n  @EventPattern('{topic}')\n  on(@Payload() e: {ty}) {{\n    return e;\n  }}\n}}\n"
    )
}

#[tokio::test]
async fn a_capped_field_list_is_said_not_compared() {
    let fields: String = (0..70).map(|i| format!("  f{i}: string;\n")).collect();
    let (_tmp, state) = workspace(&[
        ("src/big.ts", format!("export interface Big {{\n{fields}}}\n")),
        (
            "src/small.ts",
            "export interface Small {\n  f0: string;\n  zzz: string;\n}\n".to_string(),
        ),
        ("src/pub.ts", publisher("big.topic", "Big")),
        ("src/sub.ts", consumer("big.topic", "Small")),
    ]);
    let text = report(&state, json!({ "classes": ["payload_mismatch"] })).await;
    let rows = mismatches(&text);
    let row = rows.get("big.topic [broker unknown]").expect(&text);
    // `zzz` may be among the producer's fields past the cap: never claimed missing.
    assert!(row.consumer_only.is_empty(), "{text}");
    assert_eq!(
        row.notes,
        vec![
            "fields a consumer expects: not compared, a field list of the other side is partial (a cap, a spread, an extends)"
                .to_string()
        ],
        "{text}"
    );
    // 64 kept, 1 read by the consumer: 63 published, never expected (6 shown, the rest counted).
    assert_eq!(row.producer_only.len(), 6, "{text}");
    assert!(text.contains("+57 more"), "{text}");
}

#[tokio::test]
async fn too_many_payload_types_on_one_side_is_not_compared() {
    let mut files: Vec<(&str, String)> = Vec::new();
    let names: Vec<String> = (0..9).map(|i| format!("T{i}")).collect();
    let paths: Vec<String> = (0..9).map(|i| format!("src/p{i}.ts")).collect();
    let decls: String = names
        .iter()
        .map(|n| format!("export interface {n} {{ id: string; {n}x: string }}\n"))
        .collect();
    files.push(("src/types.ts", decls));
    for (p, n) in paths.iter().zip(&names) {
        files.push((p.as_str(), publisher("many", n)));
    }
    files.push(("src/sub.ts", consumer("many", "T0")));
    let (_tmp, state) = workspace(&files);
    let text = report(&state, json!({ "classes": ["payload_mismatch"] })).await;
    assert!(mismatches(&text).is_empty(), "{text}");
    assert!(
        payload_line(&text).contains(
            "0 not compared because no producer or no consumer payload is statically typed, 1 not compared because a side has more than 8 payload types"
        ),
        "{text}"
    );
    assert_eq!(
        not_compared(&text),
        BTreeSet::from(["many [broker unknown]".to_string()])
    );
}

#[tokio::test]
async fn a_type_declared_twice_is_ambiguous_not_a_mismatch() {
    let (_tmp, state) = workspace(&[
        ("a/dto.ts", "export interface Ev { id: string }\n".to_string()),
        ("b/dto.ts", "export interface Ev { other: number }\n".to_string()),
        ("src/pub.ts", publisher("dup", "Ev")),
        (
            "src/sub.ts",
            format!(
                "{NEST}interface Mine {{ id: string; extra: string }}\n@Controller()\nexport class S {{\n  @EventPattern('dup')\n  on(@Payload() e: Mine) {{\n    return e;\n  }}\n}}\n"
            ),
        ),
    ]);
    let text = report(&state, json!({ "classes": ["payload_mismatch"] })).await;
    assert!(mismatches(&text).is_empty(), "{text}");
    assert!(
        payload_line(&text).contains("(0 name a type no indexed file declares, 1 a type several files declare)"),
        "{text}"
    );
}

#[tokio::test]
async fn credential_shaped_field_names_are_redacted_and_values_never_print() {
    // Built at runtime: no key-shaped literal in the source.
    let field = ["pass", "word"].concat();
    let canary = ["hunter2", "-prod-", "PAYLOADCANARY"].concat();
    let (tmp, state) = workspace(&[
        (
            "src/pub.ts",
            format!(
                "{NEST}export class P {{\n  constructor(private readonly client: ClientProxy) {{}}\n  go(id: string) {{\n    this.client.emit('login', {{ id, {field}: '{canary}' }});\n  }}\n}}\n"
            ),
        ),
        ("src/sub.ts", consumer("login", "Login")),
        ("src/login.ts", "export interface Login { id: string }\n".to_string()),
    ]);
    let text = report(&state, json!({ "classes": ["payload_mismatch"] })).await;
    let rows = mismatches(&text);
    let row = rows.get("login [broker unknown]").expect(&text);
    assert_eq!(
        row.producer_only,
        vec!["[REDACTED_SECRET: USE_ENV_OR_LOCAL_FALLBACK]".to_string()],
        "{text}"
    );
    assert!(!text.to_lowercase().contains("payloadcanary"), "{text}");
    assert!(!text.contains(&field), "{text}");
    // Nor in the parse cache: a literal's type is kept, never its value.
    let cache_dir = tempfile::tempdir().expect("tempdir");
    let cache = PersistentIndexCache::open(cache_dir.path().join("index-cache.db"), u64::MAX)
        .expect("cache");
    let _ = state_in(tmp.path(), Some(&cache));
    drop(cache);
    for entry in std::fs::read_dir(cache_dir.path()).expect("dir") {
        let bytes = std::fs::read(entry.expect("entry").path()).expect("read");
        let lower = String::from_utf8_lossy(&bytes).to_lowercase();
        assert!(!lower.contains("payloadcanary"), "canary in the cache");
    }
}
