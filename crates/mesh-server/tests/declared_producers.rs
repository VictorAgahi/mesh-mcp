#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! 9.0 T14b review: declared producer calls that could not be read are said so by the tools
//! that report an absence, and a refused credential never reaches the index cache.

use mesh_core::{
    expand_roots, AppState, AuditLogger, BackgroundRescanEngine, Config, PersistentIndexCache,
};
use mesh_server::tools::ToolRegistry;
use mesh_server::WorkspaceIndexer;
use serde_json::{json, Value};
use std::path::Path;
use std::sync::Arc;

const CONFIG: &str = "[workspace]\nname = \"w\"\nversion = \"1.0.0\"\nroots = [\".\"]\n\n\
    [engines.contracts]\nenabled = true\n\n\
    [[engines.contracts.producer_calls]]\nname = \"jobs\"\ncallee = \"JobsOutbox.createJob\"\n\
    topic_keys = [\"type\"]\nqueue_keys = [\"target\"]\n";

const WORKER: &str =
    "import { Worker } from 'bullmq';\nexport const w = new Worker('tbl-one', async () => {});\n";

fn workspace(
    files: &[(&str, String)],
    cache: Option<&PersistentIndexCache>,
) -> (tempfile::TempDir, Arc<AppState>) {
    let tmp = tempfile::tempdir().expect("tempdir");
    std::fs::write(tmp.path().join("mesh-mcp.toml"), CONFIG).expect("config");
    for (name, body) in files {
        let path = tmp.path().join(name);
        std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
        std::fs::write(path, body).expect("write");
    }
    let config = Config::load_from_file(&tmp.path().join("mesh-mcp.toml")).expect("config");
    let roots = expand_roots(
        &config.workspace.roots,
        tmp.path(),
        &config.workspace.workspace_root,
    )
    .expect("roots");
    let audit = Arc::new(AuditLogger::new_in_memory().expect("audit"));
    let rescan = Arc::new(BackgroundRescanEngine::new().expect("rescan"));
    let state = Arc::new(AppState::new(config, roots, audit, rescan));
    let snapshot =
        WorkspaceIndexer::build_snapshot(&state.config, &state.allowed_roots, None, None, cache);
    state.install_snapshot(snapshot);
    (tmp, state)
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

const UNREAD: &str = "declared producer call site(s) could not be read statically";

#[tokio::test]
async fn unread_declared_calls_are_said_by_both_absence_reports_and_only_then() {
    let lookup = "import { JobsOutbox } from './outbox';\nexport const f = (kind: string) => JobsOutbox.createJob({ type: TABLE[kind], target: 'lk-q' });\n".to_string();
    let (_t, state) = workspace(
        &[
            ("src/lookup.ts", lookup),
            ("src/worker.ts", WORKER.to_string()),
        ],
        None,
    );
    let anomalies = call(&state, "event_anomalies", json!({})).await;
    assert!(anomalies.contains("tbl-one"), "{anomalies}");
    assert!(
        anomalies.contains(&format!("1 {UNREAD} (0 file(s) cut by the per-file cap)")),
        "{anomalies}"
    );
    let impact = call(&state, "analyze_impact", json!({ "target": "tbl-one" })).await;
    assert!(
        impact.contains("No producer statically resolved"),
        "{impact}"
    );
    assert!(impact.contains(&format!("1 {UNREAD}")), "{impact}");
    // A topic that has its producer says nothing of it.
    let found = call(&state, "analyze_impact", json!({ "target": "lk-q" })).await;
    assert!(!found.contains(UNREAD), "{found}");

    // Nothing unread: neither report carries the note.
    let (_t2, clean) = workspace(&[("src/worker.ts", WORKER.to_string())], None);
    assert!(!call(&clean, "event_anomalies", json!({}))
        .await
        .contains(UNREAD));
    let impact = call(&clean, "analyze_impact", json!({ "target": "tbl-one" })).await;
    assert!(
        impact.contains("No producer statically resolved"),
        "{impact}"
    );
    assert!(!impact.contains(UNREAD), "{impact}");
}

#[tokio::test]
async fn a_file_cut_by_the_declared_call_cap_is_said_too() {
    let mut body = String::from("import { JobsOutbox } from './outbox';\n");
    for i in 0..2_050 {
        body.push_str(&format!("JobsOutbox.createJob({{ type: 'cap-{i}' }});\n"));
    }
    let (_t, state) = workspace(
        &[("src/many.ts", body), ("src/worker.ts", WORKER.to_string())],
        None,
    );
    let anomalies = call(&state, "event_anomalies", json!({})).await;
    assert!(
        anomalies.contains(&format!("0 {UNREAD} (1 file(s) cut by the per-file cap)")),
        "{anomalies}"
    );
    let impact = call(&state, "analyze_impact", json!({ "target": "tbl-one" })).await;
    assert!(
        impact.contains("(1 file(s) cut by the per-file cap)"),
        "{impact}"
    );
}

fn tree_text(dir: &Path) -> String {
    let mut out = String::new();
    for entry in std::fs::read_dir(dir).expect("dir").flatten() {
        let path = entry.path();
        if path.is_dir() {
            out.push_str(&tree_text(&path));
        } else if let Ok(bytes) = std::fs::read(&path) {
            out.push_str(&String::from_utf8_lossy(&bytes).to_ascii_lowercase());
        }
    }
    out
}

#[tokio::test]
async fn a_credential_placeholder_default_reaches_neither_the_cache_nor_a_tool_answer() {
    // Built at runtime: no key-shaped literal in this file.
    let weak = ["hunter", "2"].concat();
    let weak_prod = ["hunter", "2-prod"].concat();
    // Multi-line declarations: the one-line arrow form echoes its own source line as the node's
    // signature (a pre-existing, shared behaviour, not part of this feature).
    let body = format!(
        "import {{ JobsOutbox }} from './outbox';\n\
         export function a() {{\n  return JobsOutbox.createJob({{ type: process.env.DB_PASSWORD || '{weak}', target: 'pw-q' }});\n}}\n\
         export function b() {{\n  return JobsOutbox.createJob({{ type: process.env.TOPIC_X || '{weak_prod}', target: 'pw-q' }});\n}}\n\
         export function c() {{\n  return JobsOutbox.createJob({{ type: process.env.TOPIC_Y || 'orders', target: 'pw-q' }});\n}}\n"
    );
    let tmp_cache = tempfile::tempdir().expect("cache dir");
    let cache = PersistentIndexCache::open(tmp_cache.path().join("index-cache.db"), u64::MAX)
        .expect("cache");
    let (_t, state) = workspace(&[("src/cases.ts", body)], Some(&cache));
    // A queried canary is echoed in the heading, so the refused names are asked about on their
    // own: the answer must be "matched nothing".
    for target in [weak.as_str(), weak_prod.as_str()] {
        let text = call(&state, "analyze_impact", json!({ "target": target })).await;
        assert!(text.contains("(0 matches)"), "{text}");
    }
    let mut all = String::new();
    for target in ["pw-q", "orders"] {
        all.push_str(&call(&state, "analyze_impact", json!({ "target": target })).await);
    }
    all.push_str(&call(&state, "event_anomalies", json!({})).await);
    let all = all.to_ascii_lowercase();
    assert!(!all.contains(&weak), "tool output: {all}");
    // The plain default still works, and the refused sites are counted, not silent.
    let orders = call(&state, "analyze_impact", json!({ "target": "orders" })).await;
    assert!(orders.contains("src/cases.ts:9"), "{orders}");
    assert!(all.contains(UNREAD), "{all}");
    drop(cache);
    let on_disk = tree_text(tmp_cache.path());
    if let Some(at) = on_disk.find(&weak) {
        panic!(
            "cache db / -wal / -shm holds the default: ...{}...",
            on_disk[at.saturating_sub(200)..(at + 120).min(on_disk.len())]
                .replace(|c: char| !c.is_ascii_graphic() && c != ' ', ".")
        );
    }
    assert!(on_disk.contains("pw-q"), "the cache was written");
}
