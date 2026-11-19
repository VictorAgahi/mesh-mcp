//! The views one `meshd` serves (8.0.0).
//!
//! Inside a Git repository one daemon serves every worktree and config file of that
//! repository: each distinct (worktree, config) pair is a *view* with its own
//! [`WorkspaceHost`] (its own graph, watcher and config follower). Outside Git the daemon is
//! still keyed by the old per-directory, per-config id, so it serves exactly one view.
//!
//! A connection names its view in its hello ([`mesh_server::hello`]). [`ViewRegistry::open`]
//! validates it against the daemon's own identity, builds the view on first use (once, however
//! many sessions ask at the same instant: one `OnceCell` per key, no global lock across the
//! build), and returns a [`Session`] bound to that one view for the rest of the connection.
//! A view with no session for the idle timeout is dropped: its watcher and config poller are
//! cancelled and its state is freed. All views share one watcher pool, one reload pool and one
//! audit logger.
//!
//! Isolation (invariant I7): a [`Session`] holds one `Arc<View>` chosen from its own hello and
//! answers every request from that view's host. Nothing maps a request to a view after the
//! hello, so no request of one session can be answered from another view.

use crate::host::{self, WorkspaceHost};
use mesh_core::socket::{DaemonMeta, ViewMeta};
use mesh_core::{family_id, AuditLogger, BackgroundRescanEngine, RepoFamily, ViewKey};
use mesh_core::{AppState, WorkspaceIdentity};
use mesh_server::hello::Hello;
use mesh_server::WorkspaceIndexer;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::OnceCell;
use tokio_util::sync::CancellationToken;

/// Who this daemon is: the repository it serves, or the one directory and config outside Git.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonIdentity {
    /// Names its socket, spawn lock, meta file and log.
    pub daemon_id: String,
    /// [`family_id`] of its repository; `None` outside Git.
    pub family_id: Option<String>,
    /// The per-directory, per-config id of the directory it was started for.
    pub legacy_id: String,
}

impl DaemonIdentity {
    pub fn of(identity: &WorkspaceIdentity) -> Self {
        Self {
            daemon_id: identity.daemon_id(),
            family_id: identity.family.as_ref().map(family_id),
            legacy_id: identity.legacy_id.clone(),
        }
    }
}

/// What a view is keyed by: the [`ViewKey`] (worktree top plus config) and the directory its
/// state is built from. With a config file that directory follows from the config; without
/// one it is the session's own directory, so two sessions in two subdirectories of one
/// worktree with no config file get two views, as two standalone servers would.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RouteKey {
    pub view: ViewKey,
    pub base_dir: PathBuf,
}

/// A hello that passed validation.
#[derive(Debug, Clone)]
pub struct ViewSpec {
    pub key: RouteKey,
    /// Canonical config file, `None` for the built-in default.
    pub config: Option<PathBuf>,
    /// The per-directory id of this view (its config status record for `doctor`).
    pub legacy_id: String,
}

fn canonical(path: &Path, what: &str) -> Result<PathBuf, String> {
    dunce::canonicalize(path)
        .map_err(|e| format!("{what} {} does not resolve: {e}", path.display()))
}

fn same_family(dir: &Path, expected: &str) -> bool {
    RepoFamily::resolve(dir).is_some_and(|f| family_id(&f) == expected)
}

/// Whether canonical `dir` lies inside a Git directory: a `.git` component anywhere (`.git/`,
/// `.git/worktrees/*`, `.git/modules/*`), or under the common directory of the repository it
/// resolves to (a bare-style common dir that is not named `.git`). Git's own files are never a
/// workspace.
fn inside_git_dir(dir: &Path) -> bool {
    dir.components().any(|c| c.as_os_str() == ".git")
        || RepoFamily::resolve(dir).is_some_and(|f| dir.starts_with(&f.common_dir))
}

/// Checks `hello` against the daemon's identity (blocking: canonicalizes and reads `.git`).
///
/// Inside Git the session's directory and the directory its state would be built from must
/// both belong to the daemon's repository (same common Git directory, compared through
/// [`family_id`], which also folds in the binary version). Outside Git the session must name
/// exactly the directory and config the daemon was started for.
pub fn validate(daemon: &DaemonIdentity, hello: &Hello) -> Result<ViewSpec, String> {
    let worktree = canonical(&hello.worktree, "worktree")?;
    if !worktree.is_dir() {
        return Err(format!(
            "worktree {} is not a directory",
            worktree.display()
        ));
    }
    let config = match &hello.config {
        Some(c) => {
            let c = canonical(c, "config")?;
            if !c.is_file() {
                return Err(format!("config {} is not a file", c.display()));
            }
            Some(c)
        }
        None => None,
    };
    let base_dir = match config.as_deref().and_then(Path::parent) {
        Some(parent) => canonical(parent, "config directory")?,
        None => worktree.clone(),
    };
    for (what, dir) in [("worktree", &worktree), ("config directory", &base_dir)] {
        if inside_git_dir(dir) {
            return Err(format!(
                "{what} {} is inside a Git directory, not a work tree",
                dir.display()
            ));
        }
    }
    match &daemon.family_id {
        Some(expected) => {
            if !same_family(&worktree, expected) || !same_family(&base_dir, expected) {
                return Err(format!(
                    "{} is not in the Git repository this meshd serves",
                    worktree.display()
                ));
            }
        }
        None => {
            // Computed on the directory the state is built from, as the daemon computed its
            // own id (a config reached through a symlink names the same daemon either way).
            let asked = WorkspaceIdentity::resolve(&base_dir, config.as_deref());
            if asked.family.is_some()
                || RepoFamily::resolve(&worktree).is_some()
                || asked.legacy_id != daemon.legacy_id
            {
                return Err(format!(
                    "{} (config {}) is not the workspace this meshd serves",
                    worktree.display(),
                    config
                        .as_deref()
                        .map_or_else(|| "none".to_string(), |c| c.display().to_string())
                ));
            }
        }
    }
    // The view is picked by the config's directory; the worktree the session names must be
    // that very directory, so a hello can never claim one worktree and be bound to another's
    // view. `Hello::new` (the shipped proxy) always derives it so.
    if worktree != base_dir {
        return Err(format!(
            "worktree {} does not match the directory of config {} ({})",
            worktree.display(),
            config
                .as_deref()
                .map_or_else(|| "none".to_string(), |c| c.display().to_string()),
            base_dir.display()
        ));
    }
    let identity = WorkspaceIdentity::resolve(&base_dir, config.as_deref());
    Ok(ViewSpec {
        key: RouteKey {
            view: identity.view,
            base_dir,
        },
        config,
        legacy_id: identity.legacy_id,
    })
}

/// What a view needs that is read from disk (blocking).
struct Prepared {
    config: mesh_core::Config,
    base_dir: PathBuf,
    sha: Option<String>,
    roots: Vec<PathBuf>,
}

fn prepare(spec: &ViewSpec) -> Result<Prepared, String> {
    let (config, base_dir, sha) = match &spec.config {
        Some(path) => {
            let (cfg, base, sha) = WorkspaceIndexer::load_config_stamped(path)
                .map_err(|e| format!("config {} cannot be loaded: {e}", path.display()))?;
            (cfg, base, Some(sha))
        }
        None => {
            let cfg = WorkspaceIndexer::default_config_at(&spec.key.base_dir)
                .map_err(|e| format!("the built-in config cannot be loaded: {e}"))?;
            (cfg, spec.key.base_dir.clone(), None)
        }
    };
    // The same rule as `WorkspaceIndexer::guard_workspace`, evaluated on this view's own
    // directory and config instead of the daemon's cwd.
    let pins_roots = spec.config.is_some() && !config.workspace.roots.is_empty();
    mesh_core::root_guard::guard_workspace_root(
        &base_dir,
        pins_roots,
        mesh_core::root_guard::home_dir().as_deref(),
    )
    .map_err(|e| e.to_string())?;
    let roots = WorkspaceIndexer::resolve_roots(&config, &base_dir);
    Ok(Prepared {
        config,
        base_dir,
        sha,
        roots,
    })
}

struct Usage {
    clients: usize,
    idle_since: Instant,
}

/// One view: a host and its own session count.
pub struct View {
    pub key: RouteKey,
    pub config: Option<PathBuf>,
    pub host: Arc<WorkspaceHost>,
    /// Parent of this view's watchers and config poller.
    cancel: CancellationToken,
    usage: Mutex<Usage>,
}

impl View {
    fn usage(&self) -> std::sync::MutexGuard<'_, Usage> {
        self.usage.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn acquire(&self) {
        self.usage().clients += 1;
    }

    fn release(&self) {
        let mut usage = self.usage();
        usage.clients = usage.clients.saturating_sub(1);
        if usage.clients == 0 {
            usage.idle_since = Instant::now();
        }
    }

    pub fn clients(&self) -> usize {
        self.usage().clients
    }

    fn idle_for(&self, now: Instant) -> Option<Duration> {
        let usage = self.usage();
        (usage.clients == 0).then(|| now.saturating_duration_since(usage.idle_since))
    }

    /// Stops this view's background work. Idempotent.
    fn retire(&self) {
        self.cancel.cancel();
        self.host.retire_status();
    }
}

/// A connection bound to one view. Dropping it releases the view.
pub struct Session {
    view: Arc<View>,
}

impl Session {
    pub fn host(&self) -> &Arc<WorkspaceHost> {
        &self.view.host
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.view.release();
    }
}

type Cell = Arc<OnceCell<Arc<View>>>;

/// Every view this daemon serves.
pub struct ViewRegistry {
    identity: DaemonIdentity,
    audit: Arc<AuditLogger>,
    rescan: Arc<BackgroundRescanEngine>,
    reload_pool: Arc<BackgroundRescanEngine>,
    shutdown: CancellationToken,
    idle_timeout: Duration,
    /// Tests: index caches and status records go here instead of the user's cache.
    cache_dir: Option<PathBuf>,
    views: Mutex<HashMap<RouteKey, Cell>>,
    builds: AtomicU64,
    /// `(daemon_id, version)` once the socket is bound: the meta file is never written before.
    bound: Mutex<Option<(String, String)>>,
}

impl ViewRegistry {
    pub fn new(
        identity: DaemonIdentity,
        audit: Arc<AuditLogger>,
        rescan: Arc<BackgroundRescanEngine>,
        shutdown: CancellationToken,
        idle_timeout: Duration,
        cache_dir: Option<PathBuf>,
    ) -> Arc<Self> {
        let reload_pool = WorkspaceHost::reload_pool_for(&rescan);
        Arc::new(Self {
            identity,
            audit,
            rescan,
            reload_pool,
            shutdown,
            idle_timeout,
            cache_dir,
            views: Mutex::new(HashMap::new()),
            builds: AtomicU64::new(0),
            bound: Mutex::new(None),
        })
    }

    fn map(&self) -> std::sync::MutexGuard<'_, HashMap<RouteKey, Cell>> {
        self.views.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Views built since start (a view rebuilt after idling out counts again).
    pub fn builds(&self) -> u64 {
        self.builds.load(Ordering::SeqCst)
    }

    /// Whether any view exists or is being built.
    pub fn has_views(&self) -> bool {
        !self.map().is_empty()
    }

    fn ready_views(&self) -> Vec<Arc<View>> {
        self.map()
            .values()
            .filter_map(|c| c.get().cloned())
            .collect()
    }

    /// Builds the view `spec` describes and starts its background work: the first scan and
    /// the file watcher run on blocking threads, so this returns once the view is registered
    /// and `tools/call` answers "still indexing" until its first snapshot.
    async fn build(&self, spec: &ViewSpec) -> Result<Arc<View>, String> {
        let owned = spec.clone();
        let prepared = tokio::task::spawn_blocking(move || prepare(&owned))
            .await
            .map_err(|e| format!("building the view failed: {e}"))??;
        let state = Arc::new(AppState::new(
            prepared.config,
            prepared.roots,
            Arc::clone(&self.audit),
            Arc::clone(&self.rescan),
        ));
        if let Some(dir) = &self.cache_dir {
            static TAG: AtomicU64 = AtomicU64::new(1_000_000);
            host::seed_index_cache(&state, dir, TAG.fetch_add(1, Ordering::Relaxed));
        }
        let cancel = self.shutdown.child_token();
        let watcher_cancel = cancel.child_token();
        let host = WorkspaceHost::new_view(
            Arc::clone(&state),
            spec.config.clone(),
            prepared.sha,
            &spec.legacy_id,
            Arc::clone(&self.audit),
            Arc::clone(&self.rescan),
            Arc::clone(&self.reload_pool),
            cancel.clone(),
            watcher_cancel.clone(),
            self.cache_dir.clone(),
        );
        host.spawn_idle_poller(host::IDLE_POLL_INTERVAL);
        let builds = self.builds.fetch_add(1, Ordering::SeqCst) + 1;
        tracing::info!(
            target: "meshd::views",
            "Building view {} (config {}); views built since start: {builds}",
            spec.key.view.worktree_top.display(),
            spec.config
                .as_deref()
                .map_or_else(|| "built-in default".to_string(), |c| c.display().to_string())
        );
        let ingest_state = Arc::clone(&state);
        let base_dir = prepared.base_dir;
        let config = spec.config.clone();
        tokio::task::spawn_blocking(move || {
            host::ingest_blocking(&ingest_state, &base_dir, config.as_deref());
        });
        tokio::task::spawn_blocking(move || {
            host::start_watcher_blocking(&state, watcher_cancel);
        });
        if self.bound().is_some() {
            host.publish_status();
        }
        Ok(Arc::new(View {
            key: spec.key.clone(),
            config: spec.config.clone(),
            host,
            cancel,
            usage: Mutex::new(Usage {
                clients: 0,
                idle_since: Instant::now(),
            }),
        }))
    }

    /// The view for `spec`, built once whoever asks first. `attach` counts a session on it
    /// under the map lock, so the idle sweep can never drop a view a session just received.
    async fn get(&self, spec: &ViewSpec, attach: bool) -> Result<Arc<View>, String> {
        loop {
            let cell: Cell = Arc::clone(
                self.map()
                    .entry(spec.key.clone())
                    .or_insert_with(|| Arc::new(OnceCell::new())),
            );
            let view = match cell.get_or_try_init(|| self.build(spec)).await {
                Ok(view) => Arc::clone(view),
                Err(reason) => {
                    let mut map = self.map();
                    if map
                        .get(&spec.key)
                        .is_some_and(|c| Arc::ptr_eq(c, &cell) && !c.initialized())
                    {
                        map.remove(&spec.key);
                    }
                    return Err(reason);
                }
            };
            {
                let map = self.map();
                if map.get(&spec.key).is_some_and(|c| Arc::ptr_eq(c, &cell)) {
                    if attach {
                        view.acquire();
                    }
                    drop(map);
                    self.publish_meta();
                    return Ok(view);
                }
            }
            // The cell left the map between its build and here (the view idled out, or a
            // failed build was forgotten and this caller then built it). No session can reach
            // this view any more: stop it and take a fresh cell.
            view.retire();
        }
    }

    /// Validates `hello` and binds a session to its view.
    pub async fn open(self: &Arc<Self>, hello: Hello) -> Result<Session, String> {
        let identity = self.identity.clone();
        let spec = tokio::task::spawn_blocking(move || validate(&identity, &hello))
            .await
            .map_err(|e| format!("validating the hello failed: {e}"))??;
        let view = self.get(&spec, true).await?;
        Ok(Session { view })
    }

    /// Builds the view `hello` names without attaching a session (the daemon's own startup
    /// view, so its first scan starts before the spawning session connects).
    pub async fn prewarm(&self, hello: Hello) -> Result<(), String> {
        let identity = self.identity.clone();
        let spec = tokio::task::spawn_blocking(move || validate(&identity, &hello))
            .await
            .map_err(|e| format!("validating the startup view failed: {e}"))??;
        self.get(&spec, false).await.map(|_| ())
    }

    /// Drops every view with no session for the idle timeout. Returns how many.
    pub fn sweep(&self) -> usize {
        let now = Instant::now();
        let removed: Vec<Arc<View>> = {
            let mut map = self.map();
            let idle: Vec<RouteKey> = map
                .iter()
                .filter_map(|(key, cell)| {
                    let view = cell.get()?;
                    let idle = view.idle_for(now)?;
                    (idle >= self.idle_timeout).then(|| key.clone())
                })
                .collect();
            idle.iter()
                .filter_map(|key| map.remove(key))
                .filter_map(|cell| cell.get().cloned())
                .collect()
        };
        for view in &removed {
            tracing::info!(
                target: "meshd::views",
                "View {} had no session for {:?}; dropped",
                view.key.view.worktree_top.display(),
                self.idle_timeout
            );
            view.retire();
        }
        if !removed.is_empty() {
            self.publish_meta();
        }
        removed.len()
    }

    /// Runs [`Self::sweep`] every `poll` until shutdown.
    pub fn spawn_sweeper(self: &Arc<Self>, poll: Duration) {
        let registry = Arc::clone(self);
        let cancel = self.shutdown.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = tokio::time::sleep(poll) => {
                        registry.sweep();
                    }
                }
            }
        });
    }

    fn bound(&self) -> Option<(String, String)> {
        self.bound.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }

    /// The socket is bound: from now on the meta file lists the views.
    pub fn on_bound(&self, daemon_id: &str, version: &str) {
        *self.bound.lock().unwrap_or_else(|p| p.into_inner()) =
            Some((daemon_id.to_string(), version.to_string()));
        self.publish_meta();
        for view in self.ready_views() {
            view.host.publish_status();
        }
    }

    /// Daemon shutdown: every view's status record goes (the server removes the meta file).
    pub fn on_shutdown(&self) {
        for view in self.ready_views() {
            view.retire();
        }
    }

    /// Rewrites the meta file with the current views, once bound. Serialized by the `bound`
    /// lock so two rewrites never interleave.
    fn publish_meta(&self) {
        let bound = self.bound.lock().unwrap_or_else(|p| p.into_inner());
        let Some((daemon_id, version)) = bound.as_ref() else {
            return;
        };
        let mut views: Vec<ViewMeta> = self
            .ready_views()
            .iter()
            .map(|v| ViewMeta {
                worktree: v.key.view.worktree_top.clone(),
                config: v.config.clone(),
                clients: v.clients(),
            })
            .collect();
        views.sort_by(|a, b| (&a.worktree, &a.config).cmp(&(&b.worktree, &b.config)));
        mesh_core::socket::write_daemon_meta_record(
            daemon_id,
            &DaemonMeta {
                pid: std::process::id(),
                version: version.clone(),
                views,
                view_builds: self.builds(),
            },
        );
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

    fn unavailable(what: &str) {
        assert!(
            std::env::var_os("CI").is_none(),
            "CI: required git operation failed: {what}"
        );
        eprintln!("{what} unavailable; skipping");
    }

    struct Repo {
        _tmp: tempfile::TempDir,
        main: PathBuf,
        wt: PathBuf,
        other: PathBuf,
    }

    fn repo() -> Option<Repo> {
        let tmp = tempfile::tempdir().expect("tempdir");
        let base = dunce::canonicalize(tmp.path()).expect("canon");
        let (main, wt, other) = (base.join("main"), base.join("wt"), base.join("other"));
        for dir in [&main, &other] {
            std::fs::create_dir_all(dir).expect("mkdir");
            std::fs::write(
                dir.join("mesh-mcp.toml"),
                "[workspace]\nname = \"v\"\nroots = [\".\"]\n",
            )
            .expect("cfg");
            if !(git(dir, &["init", "-q"])
                && git(dir, &["add", "."])
                && git(dir, &["commit", "-qm", "init"]))
            {
                unavailable("git init");
                return None;
            }
        }
        if !git(&main, &["worktree", "add", "-q", "../wt", "-b", "topic"]) {
            unavailable("git worktree add");
            return None;
        }
        Some(Repo {
            _tmp: tmp,
            main,
            wt,
            other,
        })
    }

    fn daemon_for(dir: &Path) -> DaemonIdentity {
        DaemonIdentity::of(&WorkspaceIdentity::resolve(
            dir,
            Some(&dir.join("mesh-mcp.toml")),
        ))
    }

    #[test]
    fn a_worktree_of_the_repository_is_accepted_and_keyed_apart() {
        let Some(r) = repo() else { return };
        let daemon = daemon_for(&r.main);
        let a = validate(
            &daemon,
            &Hello::new(&r.main, Some(&r.main.join("mesh-mcp.toml"))),
        )
        .expect("main");
        let b = validate(
            &daemon,
            &Hello::new(&r.wt, Some(&r.wt.join("mesh-mcp.toml"))),
        )
        .expect("worktree");
        assert_ne!(a.key, b.key);
        assert_eq!(a.key.view.worktree_top, r.main);
        assert_eq!(b.key.view.worktree_top, r.wt);
        // The same hello always lands on the same key.
        let again = validate(
            &daemon,
            &Hello::new(&r.wt, Some(&r.wt.join("mesh-mcp.toml"))),
        )
        .expect("again");
        assert_eq!(again.key, b.key);
        // No config: one view per directory, never shared between two subdirectories.
        std::fs::create_dir_all(r.wt.join("x")).expect("mkdir");
        std::fs::create_dir_all(r.wt.join("y")).expect("mkdir");
        let x = validate(&daemon, &Hello::new(&r.wt.join("x"), None)).expect("x");
        let y = validate(&daemon, &Hello::new(&r.wt.join("y"), None)).expect("y");
        assert_ne!(x.key, y.key);
    }

    #[test]
    fn another_repository_or_a_missing_path_is_refused() {
        let Some(r) = repo() else { return };
        let daemon = daemon_for(&r.main);
        let err = validate(
            &daemon,
            &Hello::new(&r.other, Some(&r.other.join("mesh-mcp.toml"))),
        )
        .expect_err("other repo");
        assert!(err.contains("not in the Git repository"), "{err}");
        // A config of another repository with a worktree of this one is refused too: the
        // state would be built from the other repository's directory.
        let err = validate(
            &daemon,
            &Hello::new(&r.main, Some(&r.other.join("mesh-mcp.toml"))),
        )
        .expect_err("foreign config");
        assert!(err.contains("not in the Git repository"), "{err}");
        assert!(validate(&daemon, &Hello::new(&r.main.join("nope"), None)).is_err());
        // A worktree of this repository paired with the config of another worktree of it:
        // refused, never bound to the config's view.
        let crafted = Hello {
            meshd_hello: 1,
            worktree: r.main.clone(),
            config: Some(r.wt.join("mesh-mcp.toml")),
        };
        let err = validate(&daemon, &crafted).expect_err("mismatched worktree and config");
        assert!(
            err.contains("does not match the directory of config"),
            "{err}"
        );
        assert!(validate(&daemon, &Hello::new(&r.main, Some(&r.main.join("no.toml")))).is_err());
    }

    #[test]
    fn a_directory_inside_the_git_dir_is_refused() {
        let Some(r) = repo() else { return };
        let daemon = daemon_for(&r.main);
        let git_dir = r.main.join(".git");
        let wt_admin = std::fs::read_dir(git_dir.join("worktrees"))
            .expect("worktrees admin dir")
            .flatten()
            .next()
            .expect("one linked worktree")
            .path();
        let modules = git_dir.join("modules").join("m");
        std::fs::create_dir_all(&modules).expect("mkdir");
        for dir in [&git_dir, &wt_admin, &modules] {
            let err = validate(&daemon, &Hello::new(dir, None)).expect_err("git dir");
            assert!(err.contains("inside a Git directory"), "{err}");
        }
        // A config file placed inside `.git/` is refused too.
        std::fs::write(git_dir.join("mesh-mcp.toml"), "[workspace]\nname = \"g\"\n").expect("cfg");
        let hello = Hello {
            meshd_hello: 1,
            worktree: git_dir.clone(),
            config: Some(git_dir.join("mesh-mcp.toml")),
        };
        let err = validate(&daemon, &hello).expect_err("config in .git");
        assert!(err.contains("inside a Git directory"), "{err}");
    }

    #[test]
    fn outside_git_only_the_daemons_own_directory_and_config_pass() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let base = dunce::canonicalize(tmp.path()).expect("canon");
        // An unreadable `.git` pointer makes this directory non-Git whatever encloses it.
        std::fs::write(base.join(".git"), "gitdir: ./nowhere\n").expect("dot git");
        let (a, b) = (base.join("a.toml"), base.join("b.toml"));
        for c in [&a, &b] {
            std::fs::write(c, "[workspace]\nname = \"n\"\nroots = [\".\"]\n").expect("cfg");
        }
        let daemon = DaemonIdentity::of(&WorkspaceIdentity::resolve(&base, Some(&a)));
        assert_eq!(daemon.family_id, None);
        assert_eq!(daemon.daemon_id, daemon.legacy_id);
        validate(&daemon, &Hello::new(&base, Some(&a))).expect("own config");
        let err = validate(&daemon, &Hello::new(&base, Some(&b))).expect_err("other config");
        assert!(err.contains("not the workspace"), "{err}");
        assert!(validate(&daemon, &Hello::new(&base, None)).is_err());
    }
}
