//! Git plumbing shared by tools that read repository history: a shell-free,
//! timed, output-bounded `git` runner, `base` validation, and base-revision
//! selection (the caller's `base`, else the merge-base with the default branch).

use std::ffi::{OsStr, OsString};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// Per-command budget: every call runs on an agent's synchronous request path.
pub(crate) const GIT_COMMAND_TIMEOUT: Duration = Duration::from_secs(5);

/// Default base chain, in order (plan 4.6b). Never `HEAD~1`.
const DEFAULT_BASE_CHAIN: [&str; 3] = ["origin/HEAD", "origin/main", "main"];

/// Longest `base` accepted: a ref name or an expression like `main~3`, never a blob.
const MAX_BASE_LEN: usize = 256;

/// Git variables that would redirect every command to another repository or
/// object store, inject configuration, or change which objects are read (they
/// are set, for instance, when the server is spawned from a Git hook, where
/// `GIT_QUARANTINE_PATH`/`GIT_ALTERNATE_OBJECT_DIRECTORIES` point at a
/// push's quarantine). The user's own discovery settings
/// (`GIT_CEILING_DIRECTORIES`) are kept.
const REPO_REDIRECT_VARS: [&str; 13] = [
    "GIT_DIR",
    "GIT_WORK_TREE",
    "GIT_INDEX_FILE",
    "GIT_OBJECT_DIRECTORY",
    "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    "GIT_QUARANTINE_PATH",
    "GIT_COMMON_DIR",
    "GIT_NAMESPACE",
    "GIT_PREFIX",
    "GIT_CONFIG_PARAMETERS",
    "GIT_CONFIG_COUNT",
    "GIT_REPLACE_REF_BASE",
    "GIT_NO_REPLACE_OBJECTS",
];

/// Small outputs (a SHA, a path prefix).
pub(crate) const SMALL_OUTPUT: u64 = 64 * 1024;

#[derive(Debug, thiserror::Error)]
pub(crate) enum GitError {
    #[error("`base` {0:?} is invalid: {1}")]
    InvalidBase(String, &'static str),
    #[error(
        "`git` was not found on the PATH; install Git or add it to the PATH of the MeshMCP process"
    )]
    GitNotFound,
    #[error("`{0}` is not inside a usable Git repository{1}")]
    NotARepository(PathBuf, String),
    #[error("Git base `{0}` was not found in the repository of this file")]
    BaseNotFound(String),
    #[error("`git {0}` did not finish within {1:?}")]
    Timeout(String, Duration),
    #[error("`git {0}` failed: {1}")]
    GitFailed(String, String),
}

impl GitError {
    /// A malformed argument is an invalid request; everything else is an
    /// environment failure.
    pub(crate) fn is_invalid_request(&self) -> bool {
        matches!(self, Self::InvalidBase(..))
    }
}

/// Rejects anything `git` could read as an option or a path expression:
/// a `base` is passed as its own argv entry (no shell), and additionally must
/// not start with `-` (option injection), nor contain `:` (`rev:path` would
/// point at another file), whitespace or control characters.
pub(crate) fn validate_base(base: &str) -> Result<(), GitError> {
    let fail = |why| Err(GitError::InvalidBase(base.to_string(), why));
    if base.is_empty() {
        return fail("empty");
    }
    if base.len() > MAX_BASE_LEN {
        return fail("longer than 256 bytes");
    }
    if base.starts_with('-') {
        return fail("must not start with '-'");
    }
    if base.contains(':') {
        return fail("must not contain ':'");
    }
    if base.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return fail("must not contain whitespace or control characters");
    }
    Ok(())
}

pub(crate) struct GitOutput {
    pub(crate) success: bool,
    pub(crate) stdout: Vec<u8>,
    pub(crate) stderr: String,
}

impl GitOutput {
    pub(crate) fn stdout_line(&self) -> String {
        String::from_utf8_lossy(&self.stdout).trim().to_string()
    }
}

/// Runs `git` without a shell, one argv entry per argument, with a timeout and
/// bounded output. `path` is where `git` is looked up (`None`: the process
/// `PATH`); `env` is added to the child's environment (tests use it to isolate
/// `HOME` and the global Git config).
pub(crate) struct GitRunner {
    pub(crate) path: Option<OsString>,
    pub(crate) env: Vec<(OsString, OsString)>,
    pub(crate) timeout: Duration,
}

impl GitRunner {
    pub(crate) fn system() -> Self {
        Self {
            path: std::env::var_os("PATH"),
            env: Vec::new(),
            timeout: GIT_COMMAND_TIMEOUT,
        }
    }

    pub(crate) fn locate(&self) -> Result<PathBuf, GitError> {
        let path = self.path.as_ref().ok_or(GitError::GitNotFound)?;
        let names: &[&str] = if cfg!(windows) {
            &["git.exe", "git.cmd", "git"]
        } else {
            &["git"]
        };
        std::env::split_paths(path)
            .filter(|dir| !dir.as_os_str().is_empty())
            .flat_map(|dir| names.iter().map(move |n| dir.join(n)))
            .find(|candidate| candidate.is_file())
            .ok_or(GitError::GitNotFound)
    }

    pub(crate) fn run(
        &self,
        program: &Path,
        cwd: &Path,
        args: &[&OsStr],
        stdout_limit: u64,
    ) -> Result<GitOutput, GitError> {
        self.run_with_input(program, cwd, args, None, stdout_limit)
    }

    /// [`Self::run`] with `input` written to the child's stdin (then closed),
    /// for `git cat-file --batch`. The same timeout and output bound apply; the
    /// input is written from its own thread, so a child that fills its stdout
    /// pipe before reading all of its stdin cannot deadlock the call.
    pub(crate) fn run_with_input(
        &self,
        program: &Path,
        cwd: &Path,
        args: &[&OsStr],
        input: Option<Vec<u8>>,
        stdout_limit: u64,
    ) -> Result<GitOutput, GitError> {
        let display = args
            .iter()
            .map(|a| a.to_string_lossy())
            .collect::<Vec<_>>()
            .join(" ");
        let mut cmd = Command::new(program);
        // No fsmonitor hook, no pager, no credential prompt, stable messages.
        cmd.args(["-c", "core.fsmonitor=false", "--no-pager"])
            .args(args)
            .current_dir(cwd)
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("GIT_OPTIONAL_LOCKS", "0")
            .env("LC_ALL", "C")
            .stdin(if input.is_some() {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for var in REPO_REDIRECT_VARS {
            cmd.env_remove(var);
        }
        for (k, v) in &self.env {
            cmd.env(k, v);
        }
        let mut child = cmd.spawn().map_err(|e| match e.kind() {
            std::io::ErrorKind::NotFound => GitError::GitNotFound,
            _ => GitError::GitFailed(display.clone(), e.to_string()),
        })?;

        let stdin = child.stdin.take();
        let in_writer = input.map(|bytes| {
            std::thread::spawn(move || {
                if let Some(mut s) = stdin {
                    // A child that exits early closes the pipe: EPIPE is its
                    // answer, read from its status and output, not an error here.
                    let _ = std::io::Write::write_all(&mut s, &bytes);
                }
            })
        });
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();
        let out_reader = std::thread::spawn(move || {
            let mut buf = Vec::new();
            if let Some(s) = stdout {
                // `+ 1` so an over-limit blob is detectable; dropping the pipe
                // afterwards makes `git` exit on EPIPE instead of blocking.
                let _ = s.take(stdout_limit + 1).read_to_end(&mut buf);
            }
            buf
        });
        let err_reader = std::thread::spawn(move || {
            let mut buf = Vec::new();
            if let Some(s) = stderr {
                let _ = s.take(8 * 1024).read_to_end(&mut buf);
            }
            buf
        });

        let deadline = Instant::now() + self.timeout;
        let status = loop {
            match child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) if Instant::now() >= deadline => {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(GitError::Timeout(display, self.timeout));
                }
                Ok(None) => std::thread::sleep(Duration::from_millis(2)),
                Err(e) => return Err(GitError::GitFailed(display, e.to_string())),
            }
        };
        if let Some(writer) = in_writer {
            let _ = writer.join();
        }
        let stdout = out_reader.join().unwrap_or_default();
        let stderr = err_reader.join().unwrap_or_default();
        if stdout.len() as u64 > stdout_limit {
            return Err(GitError::GitFailed(
                display,
                format!("output exceeds {stdout_limit} bytes"),
            ));
        }
        Ok(GitOutput {
            success: status.success(),
            stdout,
            stderr: String::from_utf8_lossy(&stderr).trim().to_string(),
        })
    }
}

/// How the base revision was chosen, for the report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BaseChoice {
    /// The caller's `base`.
    Explicit(String),
    /// `git merge-base HEAD <ref>`.
    MergeBase(&'static str),
    /// No ref of the default chain exists, or the merge-base failed.
    HeadFallback,
}

/// Resolves `rev` to a commit SHA, or `None` when it does not name one.
fn verify_commit(
    git: &GitRunner,
    program: &Path,
    dir: &Path,
    rev: &str,
) -> Result<Option<String>, GitError> {
    let spec = format!("{rev}^{{commit}}");
    let out = git.run(
        program,
        dir,
        &[
            OsStr::new("rev-parse"),
            OsStr::new("--verify"),
            OsStr::new("--quiet"),
            OsStr::new("--end-of-options"),
            OsStr::new(&spec),
        ],
        SMALL_OUTPUT,
    )?;
    Ok(out.success.then(|| out.stdout_line()))
}

/// The path of `file_name` (inside `dir`) relative to the top level of the
/// repository that holds `dir`. `git` runs in `dir`, since each configured root
/// may be its own repository (or none); `--show-prefix` gives the directory
/// relative to the repository top level, so no canonical-path comparison is
/// needed.
pub(crate) fn repo_relative_path(
    git: &GitRunner,
    program: &Path,
    dir: &Path,
    file_name: &OsStr,
) -> Result<String, GitError> {
    let prefix = git.run(
        program,
        dir,
        &[OsStr::new("rev-parse"), OsStr::new("--show-prefix")],
        SMALL_OUTPUT,
    )?;
    if !prefix.success {
        // Keep git's own reason: "not a git repository" and a `safe.directory`
        // ownership refusal need different fixes.
        let reason = if prefix.stderr.is_empty() {
            String::new()
        } else {
            format!(
                " (git: {})",
                prefix.stderr.lines().next().unwrap_or_default()
            )
        };
        return Err(GitError::NotARepository(dir.to_path_buf(), reason));
    }
    Ok(format!(
        "{}{}",
        String::from_utf8_lossy(&prefix.stdout).trim_end_matches(['\n', '\r']),
        file_name.to_string_lossy()
    ))
}

/// Resolves the base revision. An explicit `base` must name a commit. Without
/// one, the first existing ref of `DEFAULT_BASE_CHAIN` decides: its merge-base
/// with `HEAD` is the base; a failed merge-base (unrelated histories) falls
/// back to `HEAD` rather than trying the next ref.
pub(crate) fn resolve_base(
    git: &GitRunner,
    program: &Path,
    dir: &Path,
    base: Option<&str>,
) -> Result<(BaseChoice, String), GitError> {
    match base {
        Some(b) => {
            let sha = verify_commit(git, program, dir, b)?
                .ok_or_else(|| GitError::BaseNotFound(b.to_string()))?;
            Ok((BaseChoice::Explicit(b.to_string()), sha))
        }
        None => {
            let mut chosen = None;
            for candidate in DEFAULT_BASE_CHAIN {
                if verify_commit(git, program, dir, candidate)?.is_none() {
                    continue;
                }
                // The first existing ref decides; a failed merge-base (unrelated
                // histories) falls back to HEAD rather than trying the next ref.
                let mb = git.run(
                    program,
                    dir,
                    &[
                        OsStr::new("merge-base"),
                        OsStr::new("HEAD"),
                        OsStr::new(candidate),
                    ],
                    SMALL_OUTPUT,
                )?;
                if mb.success {
                    chosen = Some((BaseChoice::MergeBase(candidate), mb.stdout_line()));
                }
                break;
            }
            match chosen {
                Some(c) => Ok(c),
                None => {
                    let head = verify_commit(git, program, dir, "HEAD")?.ok_or_else(|| {
                        GitError::BaseNotFound("HEAD (the repository has no commit)".into())
                    })?;
                    Ok((BaseChoice::HeadFallback, head))
                }
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::GitRunner;
    use super::GIT_COMMAND_TIMEOUT;
    use std::ffi::OsString;
    use std::path::{Path, PathBuf};
    use std::process::Command;

    /// A throwaway repository with its own `HOME` and no global/system Git
    /// config, so nothing from the developer's `~/.gitconfig` leaks in.
    pub(crate) struct TempRepo {
        pub(crate) _dir: tempfile::TempDir,
        pub(crate) root: PathBuf,
        pub(crate) home: PathBuf,
    }

    impl TempRepo {
        pub(crate) fn new() -> Self {
            let dir = tempfile::tempdir().expect("tempdir");
            let base = dunce::canonicalize(dir.path()).expect("canon");
            let root = base.join("repo");
            let home = base.join("home");
            std::fs::create_dir_all(&root).expect("mkdir repo");
            std::fs::create_dir_all(&home).expect("mkdir home");
            let repo = Self {
                _dir: dir,
                root,
                home,
            };
            repo.git(&["init", "-q"]);
            repo.git(&["symbolic-ref", "HEAD", "refs/heads/main"]);
            repo.git(&["config", "user.email", "test@example.invalid"]);
            repo.git(&["config", "user.name", "Test"]);
            repo.git(&["config", "commit.gpgsign", "false"]);
            repo.git(&["config", "core.autocrlf", "false"]);
            repo
        }

        pub(crate) fn isolated_env(home: &Path) -> Vec<(OsString, OsString)> {
            vec![
                ("HOME".into(), home.as_os_str().to_owned()),
                ("USERPROFILE".into(), home.as_os_str().to_owned()),
                (
                    "XDG_CONFIG_HOME".into(),
                    home.join(".config").into_os_string(),
                ),
                ("GIT_CONFIG_NOSYSTEM".into(), "1".into()),
                (
                    "GIT_CONFIG_GLOBAL".into(),
                    home.join(".gitconfig").into_os_string(),
                ),
            ]
        }

        pub(crate) fn runner(&self) -> GitRunner {
            GitRunner {
                path: std::env::var_os("PATH"),
                env: Self::isolated_env(&self.home),
                timeout: GIT_COMMAND_TIMEOUT,
            }
        }

        pub(crate) fn git(&self, args: &[&str]) -> String {
            let out = Command::new("git")
                .args(args)
                .current_dir(&self.root)
                .envs(Self::isolated_env(&self.home))
                .env_remove("GIT_DIR")
                .env_remove("GIT_WORK_TREE")
                .output()
                .expect("run git");
            assert!(
                out.status.success(),
                "git {args:?}: {}",
                String::from_utf8_lossy(&out.stderr)
            );
            String::from_utf8_lossy(&out.stdout).trim().to_string()
        }

        pub(crate) fn write(&self, rel: &str, content: &str) -> PathBuf {
            let path = self.root.join(rel);
            std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
            std::fs::write(&path, content).expect("write");
            path
        }

        pub(crate) fn commit(&self, msg: &str) {
            self.git(&["add", "-A"]);
            self.git(&["commit", "-q", "-m", msg]);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{validate_base, GitError};

    #[test]
    fn validate_base_accepts_refs_and_refuses_options_and_paths() {
        for bad in ["--output=/tmp/x", "-p", "main:other.proto", "", "a b"] {
            assert!(
                matches!(validate_base(bad), Err(GitError::InvalidBase(..))),
                "{bad:?} must be refused"
            );
        }
        for good in ["main", "origin/main", "v1.2.0", "HEAD~3", "abc123"] {
            assert!(validate_base(good).is_ok(), "{good:?} must be accepted");
        }
    }
}
