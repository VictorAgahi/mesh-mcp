//! Repository-level identities (8.0.0): which Git repository a directory belongs to, and which
//! view of it (worktree plus config file) a session asks for.
//!
//! This module only computes identities. Nothing here chooses a socket or a cache directory yet:
//! the daemon still keys on [`crate::workspace_id`] (carried as [`WorkspaceIdentity::legacy_id`]).
//! Later changes name the per-repository socket with [`family_id`], the shared parse cache with
//! [`cache_family_id`], and route a connection to a view with [`ViewKey`].

use std::path::{Path, PathBuf};

use crate::audit::AuditLogger;
use crate::watcher::git::resolve_git_dir;

/// The Git repository a directory belongs to. All linked worktrees of one repository share
/// `common_dir`; each has its own `worktree_top`. A submodule is its own repository.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoFamily {
    /// Canonical common Git directory (`<main>/.git`, or `<super>/.git/modules/<name>` for a
    /// submodule): where `refs/` and `objects/` live.
    pub common_dir: PathBuf,
    /// Canonical top of the work tree the queried directory sits in.
    pub worktree_top: PathBuf,
}

impl RepoFamily {
    /// Resolves the family of `dir` by following `.git` (directory or `gitdir:` file) upward.
    /// `None` outside a Git work tree or when the pointer is unreadable.
    pub fn resolve(dir: &Path) -> Option<Self> {
        let git = resolve_git_dir(dir)?;
        let top = git.head_marker.parent()?.parent()?;
        Some(Self {
            common_dir: dunce::canonicalize(&git.common_dir).unwrap_or(git.common_dir),
            worktree_top: dunce::canonicalize(top).unwrap_or_else(|_| top.to_path_buf()),
        })
    }
}

fn hash16(key: &str) -> String {
    let digest = AuditLogger::compute_sha256(key.as_bytes());
    digest[..16].to_string()
}

/// 16 hex chars over the canonical common dir and the binary version. Names the per-repository
/// socket, so a daemon from another version is never found.
pub fn family_id(family: &RepoFamily) -> String {
    hash16(&format!(
        "{}\u{0}{}",
        family.common_dir.display(),
        env!("CARGO_PKG_VERSION")
    ))
}

/// Same hash as [`family_id`] without the version. Names the shared parse-cache directory:
/// record compatibility across versions is handled by `SCHEMA_VERSION` in `index_cache`.
pub fn cache_family_id(family: &RepoFamily) -> String {
    hash16(&family.common_dir.display().to_string())
}

/// One view of a repository: a worktree plus the config file a session uses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewKey {
    /// Canonical worktree top (the canonical base directory outside Git).
    pub worktree_top: PathBuf,
    /// Canonical config file path relative to `worktree_top`, so the same config name in two
    /// worktrees gives the same value. A config outside the worktree keeps its absolute
    /// canonical path. `None` when no config file is in play.
    pub config_rel: Option<PathBuf>,
}

/// Everything a session needs to know about who it is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceIdentity {
    /// `None` outside Git.
    pub family: Option<RepoFamily>,
    pub view: ViewKey,
    /// Exactly [`crate::workspace_id`] for the same inputs: the daemon identity in use today and
    /// the fallback outside Git.
    pub legacy_id: String,
}

impl WorkspaceIdentity {
    pub fn resolve(base_dir: &Path, located_config: Option<&Path>) -> Self {
        let family = RepoFamily::resolve(base_dir);
        let worktree_top = match &family {
            Some(f) => f.worktree_top.clone(),
            None => dunce::canonicalize(base_dir).unwrap_or_else(|_| base_dir.to_path_buf()),
        };
        let config_rel = located_config.map(|c| {
            let canon = dunce::canonicalize(c).unwrap_or_else(|_| c.to_path_buf());
            match canon.strip_prefix(&worktree_top) {
                Ok(rel) => rel.to_path_buf(),
                Err(_) => canon,
            }
        });
        Self {
            family,
            view: ViewKey {
                worktree_top,
                config_rel,
            },
            legacy_id: crate::workspace_id(base_dir, located_config),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn git(dir: &Path, args: &[&str]) -> bool {
        std::process::Command::new("git")
            .args([
                "-c",
                "user.email=t@t",
                "-c",
                "user.name=t",
                "-c",
                "protocol.file.allow=always",
                "-c",
                "init.defaultBranch=main",
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

    fn init_repo(dir: &Path) -> bool {
        std::fs::create_dir_all(dir).expect("mkdir");
        if !git(dir, &["init", "-q"]) {
            return false;
        }
        std::fs::write(dir.join("a.txt"), "a").expect("write");
        git(dir, &["add", "."]) && git(dir, &["commit", "-qm", "init"])
    }

    fn norm(p: &Path) -> String {
        p.to_string_lossy().replace('\\', "/")
    }

    #[test]
    fn worktrees_share_family_but_not_view() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let base = dunce::canonicalize(tmp.path()).expect("canon");
        let main = base.join("main");
        if !init_repo(&main) {
            eprintln!("git unavailable; skipping");
            return;
        }
        if !git(&main, &["worktree", "add", "-q", "../wt", "-b", "topic"]) {
            eprintln!("git worktree unavailable; skipping");
            return;
        }
        let wt = base.join("wt");
        for d in [&main, &wt] {
            std::fs::write(d.join("mesh-mcp.toml"), "").expect("cfg");
        }
        let a = WorkspaceIdentity::resolve(&main, Some(&main.join("mesh-mcp.toml")));
        let b = WorkspaceIdentity::resolve(&wt, Some(&wt.join("mesh-mcp.toml")));
        let fa = a.family.as_ref().expect("fa");
        let fb = b.family.as_ref().expect("fb");
        assert_eq!(family_id(fa), family_id(fb));
        assert_eq!(cache_family_id(fa), cache_family_id(fb));
        assert_eq!(family_id(fa).len(), 16);
        assert_ne!(family_id(fa), cache_family_id(fa));
        assert_ne!(a.view, b.view);
        assert_eq!(a.view.worktree_top, main);
        assert_eq!(b.view.worktree_top, wt);
        assert_eq!(a.view.config_rel, b.view.config_rel);
        assert_eq!(
            a.view.config_rel.as_deref().map(norm).as_deref(),
            Some("mesh-mcp.toml")
        );
        assert_ne!(a.legacy_id, b.legacy_id);
        // A subdirectory resolves to the same top.
        std::fs::create_dir_all(wt.join("sub/dir")).expect("mkdir");
        let c = WorkspaceIdentity::resolve(&wt.join("sub/dir"), None);
        assert_eq!(c.family.as_ref(), Some(fb));
        assert_eq!(c.view.config_rel, None);
    }

    #[test]
    fn unrelated_repos_differ_and_submodule_is_own_family() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let base = dunce::canonicalize(tmp.path()).expect("canon");
        let (r1, r2) = (base.join("r1"), base.join("r2"));
        if !init_repo(&r1) || !init_repo(&r2) {
            eprintln!("git unavailable; skipping");
            return;
        }
        let f1 = RepoFamily::resolve(&r1).expect("f1");
        let f2 = RepoFamily::resolve(&r2).expect("f2");
        assert_ne!(family_id(&f1), family_id(&f2));
        assert_ne!(cache_family_id(&f1), cache_family_id(&f2));

        let url = norm(&r2);
        if !git(&r1, &["submodule", "add", "-q", &url, "sub"]) {
            eprintln!("git submodule add refused; skipping the submodule half");
            return;
        }
        let fs = RepoFamily::resolve(&r1.join("sub")).expect("sub");
        assert_ne!(family_id(&fs), family_id(&f1));
        assert_ne!(cache_family_id(&fs), cache_family_id(&f1));
        assert_eq!(fs.worktree_top, r1.join("sub"));
    }

    #[test]
    fn plain_directory_has_no_family_and_legacy_id_matches() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let base = dunce::canonicalize(tmp.path()).expect("canon");
        // A tempdir may itself sit inside a Git checkout on exotic setups; only assert when not.
        if RepoFamily::resolve(&base).is_some() {
            eprintln!("tempdir inside a git work tree; skipping");
            return;
        }
        let cfg = base.join("c.toml");
        std::fs::write(&cfg, "").expect("cfg");
        let id = WorkspaceIdentity::resolve(&base, Some(&cfg));
        assert!(id.family.is_none());
        assert_eq!(id.legacy_id, crate::workspace_id(&base, Some(&cfg)));
        assert_eq!(id.view.worktree_top, base);
        assert_eq!(
            id.view.config_rel.as_deref().map(norm).as_deref(),
            Some("c.toml")
        );
        let none = WorkspaceIdentity::resolve(&base, None);
        assert_eq!(none.legacy_id, crate::workspace_id(&base, None));
        assert_eq!(none.view.config_rel, None);
    }

    #[test]
    fn config_outside_worktree_keeps_absolute_canonical_path() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let base = dunce::canonicalize(tmp.path()).expect("canon");
        let work = base.join("work");
        let outside = base.join("elsewhere");
        std::fs::create_dir_all(&work).expect("mkdir");
        std::fs::create_dir_all(&outside).expect("mkdir");
        let cfg = outside.join("c.toml");
        std::fs::write(&cfg, "").expect("cfg");
        let id = WorkspaceIdentity::resolve(&work, Some(&cfg));
        assert_eq!(id.view.config_rel.as_deref(), Some(cfg.as_path()));
    }
}
