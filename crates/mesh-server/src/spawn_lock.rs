//! Cross-process spawn lock for `meshd` (one daemon per workspace).
//!
//! When N `mesh-mcp run` start at once on one workspace, each used to see "no
//! daemon", each spawned `meshd` and each indexed before one of them won the
//! socket (4 sessions on a large repository peaked at 1.41 GB). The first
//! session now takes an exclusive lock next to the socket (`meshd-<id>.lock`)
//! around "check liveness, spawn if absent, wait for the socket"; the others
//! wait for that lock, then find the winner's daemon alive and just connect.
//!
//! The lock is advisory and tied to the open file: `flock` on Unix, an open
//! with no share mode (a second open is a sharing violation) on Windows. The
//! operating system releases it when the holder exits, even by a crash, so a
//! dead session never leaves it stuck.

use std::fs::{File, OpenOptions};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// How long a session waits for another session's spawn to finish before it
/// gives up and falls back to standalone mode.
pub const LOCK_WAIT: Duration = Duration::from_secs(10);

/// How long the lock holder waits for a freshly spawned daemon to bind.
pub const SOCKET_WAIT: Duration = Duration::from_secs(5);

const POLL: Duration = Duration::from_millis(25);

/// Why [`ensure_with_lock`] could not guarantee a daemon.
#[derive(Debug, thiserror::Error)]
pub enum SpawnLockError {
    #[error("spawn lock {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("another session held the meshd spawn lock for more than {0:?}")]
    LockTimeout(Duration),
    #[error("meshd did not bind its endpoint within {0:?}")]
    SocketTimeout(Duration),
    #[error("{0}")]
    Spawn(String),
}

/// What [`ensure_with_lock`] did.
#[derive(Debug, PartialEq, Eq)]
pub enum Ensured {
    /// A daemon was already answering (possibly spawned by another session
    /// while this one waited for the lock).
    AlreadyRunning,
    /// This session spawned the daemon and saw it bind.
    Spawned,
}

/// The lock file that goes with `endpoint` (`meshd-<id>.sock` ->
/// `meshd-<id>.lock`, same directory).
pub fn lock_path_for(endpoint: &Path) -> PathBuf {
    endpoint.with_extension("lock")
}

/// An exclusive hold on the spawn lock; released on drop.
#[derive(Debug)]
pub struct SpawnLock {
    _file: File,
}

impl SpawnLock {
    /// Takes the lock at `path` if nobody holds it. `Ok(None)` means another
    /// holder has it right now.
    pub fn try_acquire(path: &Path) -> std::io::Result<Option<Self>> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        Self::try_acquire_inner(path)
    }

    #[cfg(unix)]
    fn try_acquire_inner(path: &Path) -> std::io::Result<Option<Self>> {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        use std::os::unix::io::AsRawFd;

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(path)?;
        // `mode` only applies when the file is created; tighten a pre-existing one.
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
        // SAFETY: `flock` on a file descriptor this function owns for the whole call.
        let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if rc == 0 {
            return Ok(Some(Self { _file: file }));
        }
        let err = std::io::Error::last_os_error();
        if err.kind() == std::io::ErrorKind::WouldBlock {
            Ok(None)
        } else {
            Err(err)
        }
    }

    #[cfg(windows)]
    fn try_acquire_inner(path: &Path) -> std::io::Result<Option<Self>> {
        use std::os::windows::fs::OpenOptionsExt;

        /// `ERROR_SHARING_VIOLATION`: the file is open elsewhere with no sharing.
        const SHARING_VIOLATION: i32 = 32;
        match OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .share_mode(0)
            .open(path)
        {
            Ok(file) => Ok(Some(Self { _file: file })),
            Err(e) if e.raw_os_error() == Some(SHARING_VIOLATION) => Ok(None),
            Err(e) => Err(e),
        }
    }

    #[cfg(not(any(unix, windows)))]
    fn try_acquire_inner(path: &Path) -> std::io::Result<Option<Self>> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        Ok(Some(Self { _file: file }))
    }

    /// Polls for the lock until `wait` runs out.
    pub async fn acquire(path: &Path, wait: Duration) -> Result<Self, SpawnLockError> {
        let deadline = Instant::now() + wait;
        loop {
            match Self::try_acquire(path) {
                Ok(Some(lock)) => return Ok(lock),
                Ok(None) => {}
                Err(source) => {
                    return Err(SpawnLockError::Io {
                        path: path.to_path_buf(),
                        source,
                    })
                }
            }
            if Instant::now() >= deadline {
                return Err(SpawnLockError::LockTimeout(wait));
            }
            tokio::time::sleep(POLL).await;
        }
    }
}

/// Guarantees a daemon is answering, spawning at most one across all the
/// sessions that call this concurrently on the same `lock_path`.
///
/// `probe` says whether the daemon answers right now; `spawn` starts it (and
/// runs only for the session that finds no daemon while holding the lock).
/// The lock is held from the second liveness check until the spawned daemon
/// answers or `socket_wait` runs out, so every other session either finds the
/// daemon alive on its fast path or waits here for the winner.
pub async fn ensure_with_lock<P, PF, S>(
    lock_path: &Path,
    lock_wait: Duration,
    socket_wait: Duration,
    probe: P,
    spawn: S,
) -> Result<Ensured, SpawnLockError>
where
    P: Fn() -> PF,
    PF: Future<Output = bool>,
    S: FnOnce() -> Result<(), String>,
{
    if probe().await {
        return Ok(Ensured::AlreadyRunning);
    }
    let _lock = SpawnLock::acquire(lock_path, lock_wait).await?;
    // The previous holder may have spawned the daemon while this session waited.
    if probe().await {
        return Ok(Ensured::AlreadyRunning);
    }
    spawn().map_err(SpawnLockError::Spawn)?;
    let deadline = Instant::now() + socket_wait;
    loop {
        tokio::time::sleep(POLL).await;
        if probe().await {
            return Ok(Ensured::Spawned);
        }
        if Instant::now() >= deadline {
            return Err(SpawnLockError::SocketTimeout(socket_wait));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    #[test]
    fn lock_path_sits_next_to_the_socket() {
        assert_eq!(
            lock_path_for(Path::new("/run/mesh/meshd-abc.sock")),
            PathBuf::from("/run/mesh/meshd-abc.lock")
        );
    }

    #[cfg(unix)]
    #[test]
    fn lock_file_is_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("meshd-x.lock");
        let _held = SpawnLock::try_acquire(&path).expect("io").expect("free");
        let mode = std::fs::metadata(&path).expect("meta").permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    #[test]
    fn second_holder_is_refused_until_the_first_drops() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("meshd-x.lock");
        let first = SpawnLock::try_acquire(&path).expect("io");
        assert!(first.is_some());
        assert!(SpawnLock::try_acquire(&path).expect("io").is_none());
        drop(first);
        assert!(SpawnLock::try_acquire(&path).expect("io").is_some());
    }

    #[tokio::test]
    async fn waiting_past_the_budget_reports_a_timeout() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("meshd-x.lock");
        let _held = SpawnLock::try_acquire(&path).expect("io").expect("free");
        let outcome = ensure_with_lock(
            &path,
            Duration::from_millis(100),
            Duration::from_millis(100),
            || async { false },
            || Ok(()),
        )
        .await;
        assert!(matches!(outcome, Err(SpawnLockError::LockTimeout(_))));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn four_concurrent_sessions_spawn_exactly_one_daemon() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("meshd-x.lock");
        let alive = Arc::new(AtomicBool::new(false));
        let spawned = Arc::new(AtomicUsize::new(0));

        let mut sessions = Vec::new();
        for _ in 0..4 {
            let (path, alive, spawned) = (path.clone(), Arc::clone(&alive), Arc::clone(&spawned));
            sessions.push(tokio::spawn(async move {
                let probe_alive = Arc::clone(&alive);
                ensure_with_lock(
                    &path,
                    Duration::from_secs(5),
                    Duration::from_secs(5),
                    move || {
                        let alive = Arc::clone(&probe_alive);
                        async move { alive.load(Ordering::SeqCst) }
                    },
                    move || {
                        spawned.fetch_add(1, Ordering::SeqCst);
                        // The daemon binds a little after the spawn call returns.
                        tokio::spawn(async move {
                            tokio::time::sleep(Duration::from_millis(200)).await;
                            alive.store(true, Ordering::SeqCst);
                        });
                        Ok(())
                    },
                )
                .await
            }));
        }
        let mut results = Vec::new();
        for session in sessions {
            results.push(session.await.expect("join").expect("ensure"));
        }
        assert_eq!(spawned.load(Ordering::SeqCst), 1, "exactly one spawn");
        assert_eq!(
            results.iter().filter(|r| **r == Ensured::Spawned).count(),
            1
        );
        assert_eq!(
            results
                .iter()
                .filter(|r| **r == Ensured::AlreadyRunning)
                .count(),
            3
        );
    }
}
