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

#[tokio::test]
async fn probe() {
    let state = state_for("examples/eda/flow-view");
    println!("{}", call(&state, json!({ "event": "invoice.requested", "hops_down": 3 })).await);
    println!("{}", call(&state, json!({ "event": "invoice.requested", "detail": true })).await);
}
