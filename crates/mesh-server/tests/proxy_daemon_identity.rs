#![allow(clippy::unwrap_used, clippy::expect_used)]
//! The proxy (`mesh-mcp run`) and the daemon it spawns (`meshd`) must compute
//! the same workspace id for every config layout, or the proxy would wait on a
//! socket the daemon never binds.
//!
//! Both sides call `WorkspaceIndexer::discover_config` then
//! `WorkspaceIndexer::workspace_id_for`. The proxy runs from the user's cwd with
//! the user's `--config` (if any); the daemon is spawned with cwd = the
//! workspace directory and `--config <located file>` (see `spawn_meshd`). This
//! test replays both sides. It changes the process cwd, so it is the only test
//! in this binary and runs its layouts one after another.

use mesh_server::WorkspaceIndexer;
use std::path::{Path, PathBuf};

fn write(path: &Path) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("mkdir");
    }
    std::fs::write(path, "[workspace]\nname = \"w\"\nroots = [\".\"]\n").expect("write config");
}

/// What `mesh-mcp run` computes from `cwd` with `cli_config`, and what the
/// `meshd` it would spawn computes from its own launch (cwd = workspace dir,
/// `--config` = the located file).
fn proxy_and_daemon_ids(cwd: &Path, cli_config: Option<&Path>) -> (String, String, PathBuf) {
    std::env::set_current_dir(cwd).expect("chdir proxy");
    let (_, base) = WorkspaceIndexer::discover_config(cli_config).expect("proxy discover");
    let canonical_base = dunce::canonicalize(&base).unwrap_or(base);
    let proxy_id = WorkspaceIndexer::workspace_id_for(&canonical_base, cli_config);
    let located = WorkspaceIndexer::locate_config(cli_config)
        .map(|p| dunce::canonicalize(&p).unwrap_or(p))
        .expect("a config was located");

    std::env::set_current_dir(&canonical_base).expect("chdir daemon");
    let (_, daemon_base) =
        WorkspaceIndexer::discover_config(Some(&located)).expect("daemon discover");
    let daemon_id = WorkspaceIndexer::workspace_id_for(&daemon_base, Some(&located));
    (proxy_id, daemon_id, canonical_base)
}

#[test]
fn proxy_and_daemon_agree_on_the_id_for_every_config_layout() {
    let original = std::env::current_dir().expect("cwd");
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = dunce::canonicalize(tmp.path()).expect("canonical");

    // Discovered `.agents/mesh-mcp.toml`.
    let agents = root.join("agents-ws");
    write(&agents.join(".agents").join("mesh-mcp.toml"));
    let (proxy, daemon, _) = proxy_and_daemon_ids(&agents, None);
    assert_eq!(proxy, daemon, "discovered .agents/mesh-mcp.toml");

    // Discovered root `mesh-mcp.toml`.
    let rooted = root.join("root-ws");
    write(&rooted.join("mesh-mcp.toml"));
    let (proxy, daemon, _) = proxy_and_daemon_ids(&rooted, None);
    assert_eq!(proxy, daemon, "discovered root mesh-mcp.toml");

    // Explicit relative `--config`, two files in one directory.
    let multi = root.join("multi-ws");
    write(&multi.join("k8s-wt1.toml"));
    write(&multi.join("k8s-wt2.toml"));
    let (proxy1, daemon1, _) = proxy_and_daemon_ids(&multi, Some(Path::new("k8s-wt1.toml")));
    let (proxy2, daemon2, _) = proxy_and_daemon_ids(&multi, Some(Path::new("k8s-wt2.toml")));
    assert_eq!(proxy1, daemon1, "explicit relative --config (wt1)");
    assert_eq!(proxy2, daemon2, "explicit relative --config (wt2)");
    assert_ne!(proxy1, proxy2, "two configs in one directory differ");

    std::env::set_current_dir(original).expect("restore cwd");
}
