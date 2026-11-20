//! meshd — The MeshMCP Daemon
//!
//! A long-lived background process that:
//!   - Serves one Git repository (8.0.0): one view per worktree and config file, each with
//!     its own graph, watcher and config follower; outside Git, one directory and config
//!   - Shares one watcher pool, one reload pool, one audit logger and the repository's parse
//!     cache between its views
//!   - Multiplexes N client connections via Unix Domain Socket (UDS) or named pipe; each
//!     connection names its view in a hello line before any JSON-RPC frame
//!
//! IDE clients (`mesh-mcp run`) connect as lightweight proxies relaying stdio ↔ UDS.
//! On the first connection, mesh-mcp auto-spawns meshd if it is not already running.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

mod host;
mod idle;
mod sandbox;
mod server;
mod socket;
mod views;

use clap::Parser;
use mesh_core::{AuditLogger, BackgroundRescanEngine};
use mesh_server::hello::Hello;
use mesh_server::WorkspaceIndexer;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser, Debug)]
#[command(
    name = "meshd",
    version,
    about = "MeshMCP background daemon — shared graph + UDS multiplexer"
)]
struct Args {
    /// Path to configuration file
    #[arg(short, long)]
    config: Option<PathBuf>,

    /// Minutes a view may go without a session before it is dropped; the daemon stops with
    /// its last view (0 = never)
    #[arg(long, default_value = "30")]
    idle_timeout_minutes: u64,

    /// Tests: the idle timeout in seconds, overriding `--idle-timeout-minutes`
    #[arg(long, hide = true)]
    idle_timeout_secs: Option<u64>,

    /// Tests: how often the idle watchdog and the view sweep run, in milliseconds
    #[arg(long, hide = true, default_value = "10000")]
    idle_poll_millis: u64,

    /// Seconds to wait for a first client to ever connect before shutting down as an orphaned
    /// daemon (0 = disabled). Always active by default (P2 step 3.3) since `ensure_daemon_running`
    /// auto-spawns `meshd` ad-hoc with no supervisor; set to 0 for manual/interactive debugging
    /// runs where you intend to attach a client more than 60s after starting this by hand.
    #[arg(long, default_value = "60")]
    startup_grace_secs: u64,

    /// Override the UDS socket path (Unix) or named pipe address (Windows)
    #[arg(long)]
    socket: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Tracing to stderr only — stdout is reserved for MCP frames
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .init();

    let args = Args::parse();

    // ── Config loading ────────────────────────────────────────────────────────
    // Resolved before the socket path: the default (no `--socket`) is scoped
    // to this daemon's identity (see below), which needs `base_dir` first.
    let (_config, base_dir, _config_sha) =
        WorkspaceIndexer::discover_config_stamped(args.config.as_deref())?;
    // Same guard as `mesh-mcp run`: a daemon started with cwd = `/` or HOME and
    // no config would index the whole filesystem or home. `mesh-mcp` never
    // spawns one in that case; this covers a hand-started or older-proxy start.
    if let Err(refusal) = WorkspaceIndexer::guard_workspace(args.config.as_deref(), &base_dir) {
        tracing::error!(target: "meshd", "{refusal}");
        return Err(refusal.into());
    }
    // The config file of the startup view (absolute), if one was found.
    let config_path = WorkspaceIndexer::locate_config(args.config.as_deref());
    let canonical_base = dunce::canonicalize(&base_dir).unwrap_or(base_dir);

    // ── Identity (8.0.0) ──────────────────────────────────────────────────────
    // Inside Git this daemon serves the whole repository (every worktree and
    // config, one view each) and is named by the repository's family id; outside
    // Git it serves exactly the directory and config it was started for, under
    // the per-workspace id as before. `mesh-mcp run` computes the same id from
    // the same inputs, so both sides name the same socket.
    let identity = views::DaemonIdentity::of(&WorkspaceIndexer::identity_for(
        &canonical_base,
        args.config.as_deref(),
    ));
    let daemon_id = identity.daemon_id.clone();

    // ── Socket path (Unix) / named pipe address (Windows) ───────────────────────
    // `--socket` (always passed by `mesh-mcp`'s own auto-spawn) overrides either
    // form; the default, used only when meshd is started by hand, is scoped to
    // this daemon's identity and binary version, never one path per machine.
    #[cfg(unix)]
    let sock_path = match args.socket.clone() {
        Some(p) => p,
        None => socket::socket_path_for(&daemon_id),
    };

    // Clean up any stale socket from a previous crashed daemon
    #[cfg(unix)]
    socket::cleanup_stale_socket(&sock_path);

    #[cfg(windows)]
    let pipe_name = match args.socket.clone() {
        Some(p) => p.to_string_lossy().into_owned(),
        None => socket::pipe_name_for(&daemon_id),
    };

    // ── Cancellation token + signal handler ───────────────────────────────────
    let cancel_token = CancellationToken::new();
    let cancel_sig = cancel_token.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!(target: "meshd", "SIGINT received. Initiating graceful shutdown.");
        cancel_sig.cancel();
    });

    // ── Timeouts ──────────────────────────────────────────────────────────────
    let idle_timeout = match (args.idle_timeout_secs, args.idle_timeout_minutes) {
        (Some(secs), _) if secs > 0 => Duration::from_secs(secs),
        (None, minutes) if minutes > 0 => Duration::from_secs(minutes * 60),
        _ => Duration::MAX,
    };
    let idle_poll = Duration::from_millis(args.idle_poll_millis.max(10));

    // ── Views: one shared audit logger, watcher pool and reload pool ─────────
    let audit = Arc::new(AuditLogger::new(None)?);
    let rescan = Arc::new(BackgroundRescanEngine::new()?);
    let registry = views::ViewRegistry::new(
        identity,
        audit,
        rescan,
        cancel_token.clone(),
        idle_timeout,
        None,
    );

    // The startup view is built right away (validation, then its first scan and
    // watcher in the background) so the session that spawned this daemon finds
    // its scan already running. The socket accepts connections immediately:
    // `initialize`/`ping` succeed at once and `tools/call` reports "still
    // indexing" until the view's first snapshot.
    registry
        .prewarm(Hello::new(&canonical_base, config_path.as_deref()))
        .await
        .map_err(|e| {
            tracing::error!(target: "meshd", "Cannot build the startup view: {e}");
            e
        })?;
    registry.spawn_sweeper(idle_poll);

    // ── Idle watchdog ─────────────────────────────────────────────────────────
    // Always spawned (P2 step 3.3): the startup-grace deadline is an
    // unconditional-by-default guard against an orphaned daemon (`mesh-mcp`
    // auto-spawns `meshd` ad-hoc, no launchd/systemd). Each view is dropped after
    // `idle_timeout` with no session; the daemon exits once it has no client and
    // no view left. `idle_timeout_minutes == 0` keeps views (and so the daemon)
    // forever; `--startup-grace-secs 0` disables the orphan guard too.
    let counter = idle::ClientCounter::new();
    let startup_grace = if args.startup_grace_secs > 0 {
        Duration::from_secs(args.startup_grace_secs)
    } else {
        Duration::from_secs(u64::MAX)
    };
    let alive_registry = Arc::clone(&registry);
    idle::spawn_idle_watchdog_with(
        counter.clone(),
        cancel_token.clone(),
        idle_timeout,
        idle_poll,
        startup_grace,
        Some(Arc::new(move || alive_registry.has_views())),
    );
    match (idle_timeout != Duration::MAX, args.startup_grace_secs > 0) {
        (true, true) => tracing::info!(
            target: "meshd",
            "Idle watchdog active: a view is dropped after {idle_timeout:?} with no session, the \
             daemon stops with the last one (startup grace: {}s).",
            args.startup_grace_secs
        ),
        (true, false) => tracing::info!(
            target: "meshd",
            "Idle watchdog active: a view is dropped after {idle_timeout:?} with no session, the \
             daemon stops with the last one (startup-grace orphan guard disabled)."
        ),
        (false, true) => tracing::info!(
            target: "meshd",
            "Idle-after-use shutdown disabled; startup-grace orphan guard still active ({}s).",
            args.startup_grace_secs
        ),
        (false, false) => tracing::warn!(
            target: "meshd",
            "Both idle-after-use shutdown and the startup-grace orphan guard are disabled — this \
             daemon will run until explicitly stopped, even if no client ever connects."
        ),
    }

    // ── IPC server (blocking until cancelled) ─────────────────────────────────
    // The meta file (pid, version, views) is written inside
    // `run_uds_server_then`/`run_named_pipe_server`, only once bind/pipe creation
    // succeeds: a losing side of a bind race must never overwrite the winner's
    // record with its own, already-exiting PID.
    //
    // Plan 4 step 4.10: once the socket is bound, confine the whole process
    // (every thread, via TSYNC) so the kernel refuses any AF_INET/AF_INET6
    // socket — see `sandbox`'s module docs for scope, limits and failure policy.
    #[cfg(unix)]
    server::run_uds_server_then(
        &sock_path,
        Arc::clone(&registry),
        cancel_token,
        counter,
        &daemon_id,
        env!("CARGO_PKG_VERSION"),
        sandbox::apply_after_bind,
    )
    .await
    .map_err(|e| e.to_string())?;

    // No network sandbox on Windows: `apply_after_bind` only enforces the
    // `MESH_DAEMON_SANDBOX=required` policy (refuse to serve) before the pipe
    // is created, so the variable means the same thing on every OS.
    #[cfg(windows)]
    sandbox::apply_after_bind()?;
    #[cfg(windows)]
    server::run_named_pipe_server(
        &pipe_name,
        Arc::clone(&registry),
        cancel_token,
        counter,
        &daemon_id,
        env!("CARGO_PKG_VERSION"),
    )
    .await
    .map_err(|e| e.to_string())?;

    // Clean up socket on exit (Unix only — named pipes are released by the OS
    // once the last handle closes, there is no file to remove on Windows).
    #[cfg(unix)]
    {
        let _ = std::fs::remove_file(&sock_path);
    }
    tracing::info!(target: "meshd", "meshd stopped. Socket removed.");

    Ok(())
}
