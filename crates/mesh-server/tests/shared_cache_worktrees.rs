#![allow(clippy::unwrap_used, clippy::expect_used)]

//! 8.0.0: the persistent parse cache is shared by every worktree of a repository.
//!
//! - a second worktree of the same commit boots on 100 % cache hits and fingerprints
//!   exactly like the first (and like a parse with no cache at all);
//! - a record served to worktree B reports B's paths, never A's;
//! - an edit in B never reaches A through the shared database;
//! - the database lives in `workspaces/<cache_family_id>/`, carries the repository's
//!   marker instead of a per-worktree one, and `doctor --fix` removes a pre-8.0
//!   per-worktree directory of the repository but never the shared one.
//!
//! Git runs with no system or global config; everything that reads `$HOME` runs in a
//! child process with an isolated `HOME`, so the real `~/.cache/mesh-mcp` is never touched.

use mesh_core::{
    cache_family_id, Config, ContractNode, MeshSnapshot, PersistentIndexCache, RepoFamily,
};
use mesh_server::WorkspaceIndexer;
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::process::Command;

const CHILD_ENV: &str = "MESH_TEST_SHARED_CACHE_CHILD";

fn git(dir: &Path, args: &[&str]) -> bool {
    Command::new("git")
        .args([
            "-c",
            "user.email=t@t",
            "-c",
            "user.name=t",
            "-c",
            "init.defaultBranch=main",
            "-c",
            "core.autocrlf=false",
        ])
        .args(args)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env(
            "GIT_CONFIG_GLOBAL",
            if cfg!(windows) { "NUL" } else { "/dev/null" },
        )
        .current_dir(dir)
        .output()
        .is_ok_and(|o| o.status.success())
}

fn write(root: &Path, rel: &str, content: &str) {
    let path = root.join(rel);
    std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
    std::fs::write(path, content).expect("write");
}

/// A polyglot project whose every package comes from a manifest (so no fact depends on
/// the directory a checkout sits in) and which exercises every path-carrying part of a
/// `FileIndex`: node paths of every extractor, alias-rewritten TypeScript imports
/// (`tsconfig` `paths` and `package.json` `imports`), an `index.ts` named after its
/// directory, a single-file component, file-system routes, proto / AsyncAPI specs.
fn write_project(root: &Path) {
    write(
        root,
        "package.json",
        r##"{ "name": "shop", "imports": { "#utils/*": "./src/utils/*" } }"##,
    );
    write(
        root,
        "tsconfig.json",
        r#"{ "compilerOptions": { "baseUrl": ".", "paths": { "@/*": ["src/*"] } } }"#,
    );
    write(
        root,
        "src/services/index.ts",
        "import { User } from '@/services/User';\n\
         import { log } from '#utils/log';\n\
         export class UserService {\n  find() { log('x'); return new User(); }\n}\n",
    );
    write(
        root,
        "src/services/User.ts",
        "export class User {\n  name = 'u';\n}\n",
    );
    write(
        root,
        "src/utils/log.ts",
        "export function log(m: string) { return m; }\n",
    );
    write(
        root,
        "src/components/Card.vue",
        "<template><div/></template>\n<script setup lang=\"ts\">\nimport { User } from '@/services/User';\nconst u = new User();\n</script>\n",
    );
    write(
        root,
        "app/orders/page.tsx",
        "export default function Page() { return null; }\n",
    );
    write(
        root,
        "proto/billing.proto",
        "syntax = \"proto3\";\npackage billing.v1;\nservice Billing {\n  rpc Charge (ChargeRequest) returns (ChargeReply);\n}\nmessage ChargeRequest { string id = 1; }\nmessage ChargeReply { bool ok = 1; }\n",
    );
    write(
        root,
        "specs/asyncapi.yaml",
        "asyncapi: 2.6.0\nchannels:\n  orders.created:\n    subscribe:\n      message: {}\n",
    );
    write(root, "go/go.mod", "module example.com/billing\n\ngo 1.22\n");
    write(
        root,
        "go/main.go",
        "package main\n\ntype Server struct{}\n\nfunc (s *Server) Charge() error { return nil }\n",
    );
    write(
        root,
        "rust/Cargo.toml",
        "[package]\nname = \"engine\"\nversion = \"0.1.0\"\n",
    );
    write(
        root,
        "rust/src/lib.rs",
        "pub struct Engine { pub id: u32 }\nimpl Engine { pub fn run(&self) -> u32 { self.id } }\n",
    );
    write(
        root,
        "py/pyproject.toml",
        "[project]\nname = \"worker\"\nversion = \"0.1.0\"\n",
    );
    write(
        root,
        "py/app.py",
        "import os\n\nclass Worker:\n    def run(self):\n        return os.getcwd()\n",
    );
    write(root, "README.md", "# shop\n\nSome docs.\n");
}

/// `base/main` (a repository with one commit of [`write_project`]) and `base/wt`, a linked
/// worktree of the same commit. `None` when git is unavailable.
fn two_worktrees(base: &Path) -> Option<(PathBuf, PathBuf)> {
    let main = base.join("main");
    std::fs::create_dir_all(&main).expect("mkdir");
    write_project(&main);
    if !(git(&main, &["init", "-q"])
        && git(&main, &["add", "."])
        && git(&main, &["commit", "-qm", "init"])
        && git(&main, &["worktree", "add", "-q", "../wt", "-b", "topic"]))
    {
        return None;
    }
    let wt = dunce::canonicalize(base.join("wt")).expect("canon wt");
    Some((dunce::canonicalize(&main).expect("canon main"), wt))
}

fn config_for(root: &Path) -> Config {
    let mut cfg = Config::load_from_str(&format!(
        "[workspace]\nname = \"shared-cache\"\nroots = [{:?}]\n",
        root.display().to_string()
    ))
    .expect("config");
    cfg.resolve_workspace_root(root);
    cfg
}

fn snapshot(root: &Path, cache: Option<&PersistentIndexCache>) -> MeshSnapshot {
    let config = config_for(root);
    let roots = WorkspaceIndexer::resolve_roots(&config, root);
    WorkspaceIndexer::build_snapshot(&config, &roots, None, None, cache)
}

/// Nodes extracted from a file (the graph's own synthetic nodes, such as a topic's
/// `event-bus`, carry no file path).
fn nodes(snap: &MeshSnapshot) -> Vec<&ContractNode> {
    snap.contract_graph
        .all_nodes()
        .filter(|n| n.file_path.is_absolute())
        .collect()
}

fn has_node(snap: &MeshSnapshot, name: &str) -> bool {
    snap.contract_graph.all_nodes().any(|n| n.name == name)
}

fn open_cache(path: &Path) -> PersistentIndexCache {
    PersistentIndexCache::open(
        path.to_path_buf(),
        PersistentIndexCache::quota_bytes_from_mb(64),
    )
    .expect("open cache")
}

#[test]
fn second_worktree_cold_boot_hits_every_entry_and_reports_its_own_paths() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let base = dunce::canonicalize(tmp.path()).expect("canon");
    let Some((main, wt)) = two_worktrees(&base) else {
        eprintln!("git worktree unavailable; skipping");
        return;
    };
    let db = base.join("cache/index-cache.db");

    // Worktree A boots cold and fills the database.
    let cache_a = open_cache(&db);
    let snap_a = snapshot(&main, Some(&cache_a));
    let stats_a = cache_a.stats();
    assert_eq!(stats_a.hits, 0, "{stats_a:?}");
    assert!(
        stats_a.misses >= 10,
        "every source file is a cache candidate: {stats_a:?}"
    );
    drop(cache_a);

    // Worktree B, another process in real life: a fresh handle on the same file.
    let cache_b = open_cache(&db);
    let snap_b = snapshot(&wt, Some(&cache_b));
    let stats_b = cache_b.stats();
    assert_eq!(
        stats_b.misses, 0,
        "second worktree must not re-parse: {stats_b:?}"
    );
    assert_eq!(stats_b.errors, 0, "{stats_b:?}");
    assert_eq!(stats_b.hits, stats_a.misses, "{stats_a:?} vs {stats_b:?}");

    // Same commit, same snapshot; and the records served to B are what B parses itself.
    assert_eq!(snap_a.fingerprint(), snap_b.fingerprint());
    let fresh_b = snapshot(&wt, None);
    assert_eq!(snap_b.fingerprint(), fresh_b.fingerprint());
    assert_eq!(
        nodes(&snap_b).len(),
        nodes(&fresh_b).len(),
        "cached and fresh node counts"
    );

    // Re-anchored records report B's paths, never A's.
    let b_nodes = nodes(&snap_b);
    assert!(
        b_nodes.len() >= 8,
        "fixture extracts facts: {}",
        b_nodes.len()
    );
    for n in &b_nodes {
        assert!(
            n.file_path.starts_with(&wt),
            "{} ({}) is not under worktree B {}",
            n.file_path.display(),
            n.name,
            wt.display()
        );
        assert!(!n.file_path.starts_with(&main), "{}", n.file_path.display());
    }
    for n in nodes(&snap_a) {
        assert!(n.file_path.starts_with(&main), "{}", n.file_path.display());
    }
    // Alias-rewritten imports and the directory-named `index.ts` survive the round trip.
    assert!(has_node(&snap_b, "UserService"));
    assert!(has_node(&snap_b, "Card"));
    let deps: Vec<String> = snap_b
        .contract_graph
        .find_dependents("User")
        .iter()
        .map(|n| n.file_path.display().to_string())
        .collect();
    assert!(
        deps.iter().any(|p| p.ends_with("index.ts")),
        "aliased import resolves in B: {deps:?}"
    );
}

#[test]
fn an_edit_in_one_worktree_never_reaches_the_other() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let base = dunce::canonicalize(tmp.path()).expect("canon");
    let Some((main, wt)) = two_worktrees(&base) else {
        eprintln!("git worktree unavailable; skipping");
        return;
    };
    let cache = open_cache(&base.join("cache/index-cache.db"));
    let before_a = snapshot(&main, Some(&cache));
    let before_b = snapshot(&wt, Some(&cache));
    assert_eq!(before_a.fingerprint(), before_b.fingerprint());

    write(
        &wt,
        "src/services/User.ts",
        "export class User {\n  name = 'u';\n}\nexport class Admin extends User {}\n",
    );
    let after_b = snapshot(&wt, Some(&cache));
    assert!(has_node(&after_b, "Admin"), "B sees its own edit");
    assert_ne!(after_b.fingerprint(), before_b.fingerprint());
    assert_eq!(after_b.fingerprint(), snapshot(&wt, None).fingerprint());

    // A reloads after B wrote its new record: A's content is unchanged, so is A.
    let after_a = snapshot(&main, Some(&cache));
    assert!(!has_node(&after_a, "Admin"), "B's edit leaked into A");
    assert_eq!(after_a.fingerprint(), before_a.fingerprint());
    for n in nodes(&after_a) {
        assert!(n.file_path.starts_with(&main), "{}", n.file_path.display());
    }

    // And B again, after A's reload: still B's content.
    let again_b = snapshot(&wt, Some(&cache));
    assert!(has_node(&again_b, "Admin"));
    assert_eq!(again_b.fingerprint(), after_b.fingerprint());
}

/// A file whose package can only come from the checkout's own directory name (no manifest
/// declares one) must miss in the other worktree rather than be served the first one's.
#[test]
fn a_package_named_after_the_checkout_directory_is_never_shared() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let base = dunce::canonicalize(tmp.path()).expect("canon");
    let main = base.join("alpha");
    write(
        &main,
        "tool.py",
        "class Tool:\n    def run(self):\n        return 1\n",
    );
    if !(git(&main, &["init", "-q"])
        && git(&main, &["add", "."])
        && git(&main, &["commit", "-qm", "init"])
        && git(&main, &["worktree", "add", "-q", "../beta", "-b", "topic"]))
    {
        eprintln!("git worktree unavailable; skipping");
        return;
    }
    let main = dunce::canonicalize(&main).expect("canon");
    let wt = dunce::canonicalize(base.join("beta")).expect("canon");
    let cache = open_cache(&base.join("cache/index-cache.db"));
    let a = snapshot(&main, Some(&cache));
    let b = snapshot(&wt, Some(&cache));
    let package = |s: &MeshSnapshot| {
        s.contract_graph
            .all_nodes()
            .find(|n| n.name == "Tool")
            .map(|n| n.package.to_string())
    };
    assert_eq!(package(&a).as_deref(), Some("alpha"));
    assert_eq!(
        package(&b).as_deref(),
        Some("beta"),
        "B must not get A's package"
    );
    assert_eq!(cache.stats().hits, 0, "{:?}", cache.stats());
}

fn run_child(test: &str, home: &Path, extra: &[(&str, &Path)]) {
    let exe = std::env::current_exe().expect("test exe");
    let mut cmd = Command::new(exe);
    cmd.args([test, "--exact", "--ignored", "--test-threads=1"])
        .env("HOME", home)
        .env("USERPROFILE", home)
        .env_remove("XDG_RUNTIME_DIR")
        .env_remove("MESH_SOCKET_PATH");
    for (k, v) in extra {
        cmd.env(k, v);
    }
    let out = cmd.output().expect("spawn child");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success() && stdout.contains("1 passed"),
        "child {test} failed:\nstdout:\n{stdout}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// Child half of `both_worktrees_open_the_repository_directory`: opens the cache of each
/// worktree under the isolated `HOME` the parent set.
#[test]
#[ignore = "spawned as a child process by both_worktrees_open_the_repository_directory"]
fn child_open_both_worktrees() {
    let Some(main) = std::env::var_os(CHILD_ENV) else {
        return;
    };
    let main = PathBuf::from(main);
    let wt = main.parent().expect("parent").join("wt");
    let family = RepoFamily::resolve(&main).expect("family");
    let expected = PersistentIndexCache::workspace_db_path(&cache_family_id(&family));
    for (ws, config) in [(&main, None), (&wt, Some(wt.join("mesh-mcp.toml")))] {
        let cache = PersistentIndexCache::open_for_workspace(ws, config.as_deref(), 64)
            .expect("open cache");
        drop(cache);
        assert_eq!(
            PersistentIndexCache::cache_dir_id(ws, config.as_deref()).id(),
            cache_family_id(&family)
        );
    }
    assert!(expected.exists(), "{}", expected.display());
    let dir = expected.parent().expect("dir");
    assert_eq!(
        PersistentIndexCache::read_family_marker(&cache_family_id(&family)).as_deref(),
        Some(family.common_dir.as_path())
    );
    assert!(
        !dir.join("workspace_path").exists(),
        "a shared directory must not name one worktree"
    );
    let entries: Vec<_> = std::fs::read_dir(dir.parent().expect("workspaces"))
        .expect("read workspaces")
        .flatten()
        .collect();
    assert_eq!(entries.len(), 1, "one directory for both worktrees");
}

#[test]
fn both_worktrees_open_the_repository_directory() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let base = dunce::canonicalize(tmp.path()).expect("canon");
    let Some((main, wt)) = two_worktrees(&base) else {
        eprintln!("git worktree unavailable; skipping");
        return;
    };
    write(
        &wt,
        "mesh-mcp.toml",
        "[workspace]\nname = \"w\"\nroots = [\".\"]\n",
    );
    let home = base.join("home");
    std::fs::create_dir_all(&home).expect("home");
    run_child("child_open_both_worktrees", &home, &[(CHILD_ENV, &main)]);
    let _ = wt;
}

fn run_doctor(home: &Path, config: &Path, fix: bool) -> Vec<Value> {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_mesh-mcp"));
    cmd.args(["doctor", "--json", "--config"])
        .arg(config)
        .env("HOME", home)
        .env("USERPROFILE", home)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env_remove("XDG_RUNTIME_DIR")
        .env_remove("MESH_SOCKET_PATH");
    if fix {
        cmd.arg("--fix");
    }
    let out = cmd.output().expect("run doctor");
    assert!(
        out.status.success(),
        "doctor failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).expect("doctor json")
}

/// `doctor --fix`, run from either worktree, never takes the shared directory for a
/// leftover, and removes the pre-8.0 per-worktree directory of the other worktree.
#[test]
fn doctor_keeps_the_shared_directory_and_removes_per_worktree_leftovers() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let base = dunce::canonicalize(tmp.path()).expect("canon");
    let Some((main, wt)) = two_worktrees(&base) else {
        eprintln!("git worktree unavailable; skipping");
        return;
    };
    let config_body = "[workspace]\nname = \"d\"\nversion = \"0\"\nroots = [\".\"]\n";
    write(&main, "mesh-mcp.toml", config_body);
    write(&wt, "mesh-mcp.toml", config_body);
    let home = base.join("home");
    let workspaces = home.join(".cache/mesh-mcp/workspaces");
    let family = RepoFamily::resolve(&main).expect("family");
    let shared = workspaces.join(cache_family_id(&family));
    std::fs::create_dir_all(&shared).expect("mkdir");
    std::fs::write(
        shared.join("family_common_dir"),
        family.common_dir.to_string_lossy().as_bytes(),
    )
    .expect("marker");
    std::fs::write(shared.join("index-cache.db"), b"").expect("db");
    // What 7.x left behind for worktree B, and for an unrelated project.
    let leftover = workspaces.join("0123456789abcdef");
    std::fs::create_dir_all(&leftover).expect("mkdir");
    std::fs::write(
        leftover.join("workspace_path"),
        wt.to_string_lossy().as_bytes(),
    )
    .expect("marker");
    let other = workspaces.join("fedcba9876543210");
    std::fs::create_dir_all(&other).expect("mkdir");
    std::fs::write(
        other.join("workspace_path"),
        base.join("elsewhere").to_string_lossy().as_bytes(),
    )
    .expect("marker");

    let orphans = |checks: &[Value]| -> Vec<String> {
        checks
            .iter()
            .filter(|c| c["name"] == "Orphaned workspace cache")
            .map(|c| c["message"].as_str().unwrap_or("").to_string())
            .collect()
    };
    let checks = run_doctor(&home, &main.join("mesh-mcp.toml"), false);
    let found = orphans(&checks);
    assert_eq!(found.len(), 1, "{found:?}");
    assert!(found[0].contains("0123456789abcdef"), "{found:?}");

    let checks = run_doctor(&home, &main.join("mesh-mcp.toml"), true);
    assert_eq!(orphans(&checks).len(), 1);
    assert!(!leftover.exists(), "per-worktree leftover removed");
    assert!(shared.exists(), "shared directory kept");
    assert!(other.exists(), "another project's cache never touched");

    let checks = run_doctor(&home, &wt.join("mesh-mcp.toml"), true);
    assert!(orphans(&checks).is_empty(), "{checks:?}");
    assert!(
        shared.exists(),
        "shared directory kept from the other worktree too"
    );
    assert!(
        !shared.join("workspace_path").exists(),
        "doctor never names one worktree in the shared directory"
    );
}
