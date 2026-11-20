#![cfg(unix)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! 8.0.0: one `meshd` per Git repository serves one view per worktree.
//!
//! Every test builds real Git repositories and worktrees under a short private directory in
//! `/tmp` (the macOS socket path limit is about 104 bytes), with an isolated `HOME` and Git
//! configuration, and talks to the real `meshd` binary (and, where the proxy matters, the real
//! `mesh-mcp`). No test touches the user's own daemon: the sockets live under the test's own
//! `HOME` or at an explicit path in its directory, and every daemon a test starts is killed
//! when the test ends.

use mesh_server::hello::Hello;
use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

const CONFIG: &str = "[workspace]\nname = \"mv\"\nroots = [\".\"]\n";

/// A Git operation the test needs failed: fatal under CI, a skip on a machine without git.
fn unavailable(what: &str) {
    assert!(
        std::env::var_os("CI").is_none(),
        "CI: required operation failed: {what}"
    );
    eprintln!("{what} unavailable; skipping");
}

struct Env {
    _tmp: tempfile::TempDir,
    dir: PathBuf,
    home: PathBuf,
    main: PathBuf,
    wt: PathBuf,
    other: PathBuf,
}

impl Env {
    fn git(&self, cwd: &Path, args: &[&str]) -> bool {
        Command::new("git")
            .args(["-c", "user.email=t@t", "-c", "user.name=t"])
            .args(["-c", "init.defaultBranch=main"])
            .args(args)
            .env("HOME", &self.home)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .current_dir(cwd)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|s| s.success())
    }

    fn config(&self, worktree: &Path) -> PathBuf {
        worktree.join("mesh-mcp.toml")
    }

    /// The meta file of the daemon serving `worktree`'s repository.
    fn meta(&self, worktree: &Path) -> Option<Value> {
        let id = mesh_core::WorkspaceIdentity::resolve(worktree, Some(&self.config(worktree)))
            .daemon_id();
        let path = self
            .home
            .join(".cache/mesh-mcp/meshd")
            .join(format!("{id}.meta"));
        serde_json::from_str(&std::fs::read_to_string(path).ok()?).ok()
    }
}

/// `main` (class `OnlyMainAlpha`) and its worktree `wt` (class `OnlyTopicBeta`) of one
/// repository, plus `other`, an unrelated repository (class `OnlyOtherGamma`).
fn setup() -> Option<Env> {
    let tmp = tempfile::Builder::new()
        .prefix("mv")
        .tempdir_in("/tmp")
        .expect("tempdir");
    let dir = dunce::canonicalize(tmp.path()).expect("canonical");
    let home = dir.join("h");
    std::fs::create_dir_all(&home).expect("home");
    let env = Env {
        main: dir.join("main"),
        wt: dir.join("wt"),
        other: dir.join("other"),
        home,
        dir,
        _tmp: tmp,
    };
    for (repo, class) in [(&env.main, "OnlyMainAlpha"), (&env.other, "OnlyOtherGamma")] {
        std::fs::create_dir_all(repo.join("src")).expect("mkdir");
        std::fs::write(repo.join("mesh-mcp.toml"), CONFIG).expect("config");
        std::fs::write(
            repo.join("src/a.ts"),
            format!("export class {class} {{}}\n"),
        )
        .expect("source");
        if !(env.git(repo, &["init", "-q"])
            && env.git(repo, &["add", "."])
            && env.git(repo, &["commit", "-qm", "init"]))
        {
            unavailable("git init");
            return None;
        }
    }
    if !env.git(
        &env.main,
        &["worktree", "add", "-q", "../wt", "-b", "topic"],
    ) {
        unavailable("git worktree add");
        return None;
    }
    std::fs::write(env.wt.join("src/a.ts"), "export class OnlyTopicBeta {}\n").expect("edit");
    Some(env)
}

/// A `meshd` started by the test; killed when dropped.
struct Daemon {
    child: Child,
    sock: PathBuf,
    log: PathBuf,
}

impl Daemon {
    fn start(env: &Env, extra: &[&str]) -> Self {
        let sock = env.dir.join("s.sock");
        let log = env.dir.join("meshd.log");
        let child = Command::new(env!("CARGO_BIN_EXE_meshd"))
            .arg("--config")
            .arg(env.config(&env.main))
            .arg("--socket")
            .arg(&sock)
            .args(extra)
            .current_dir(&env.main)
            .env("HOME", &env.home)
            .env("RUST_LOG", "info")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env_remove("XDG_RUNTIME_DIR")
            .env_remove("MESH_SOCKET_PATH")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::from(std::fs::File::create(&log).expect("log")))
            .spawn()
            .expect("spawn meshd");
        let daemon = Self { child, sock, log };
        let deadline = Instant::now() + Duration::from_secs(30);
        while UnixStream::connect(&daemon.sock).is_err() {
            assert!(
                Instant::now() < deadline,
                "meshd never bound its socket:\n{}",
                daemon.log()
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        daemon
    }

    fn log(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }

    fn exited_within(&mut self, limit: Duration) -> bool {
        let deadline = Instant::now() + limit;
        while Instant::now() < deadline {
            if self.child.try_wait().ok().flatten().is_some() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        false
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// One raw connection to a daemon.
struct Client {
    reader: BufReader<UnixStream>,
    writer: UnixStream,
    next_id: u64,
}

impl Client {
    /// Connects and sends `line` as the hello; returns the daemon's reply line too.
    fn raw(sock: &Path, line: &str) -> (Self, String) {
        let writer = UnixStream::connect(sock).expect("connect");
        writer
            .set_read_timeout(Some(Duration::from_secs(60)))
            .expect("timeout");
        let mut reader = BufReader::new(writer.try_clone().expect("clone"));
        let mut writer_half = writer;
        writer_half.write_all(line.as_bytes()).expect("hello");
        let mut reply = String::new();
        reader.read_line(&mut reply).expect("reply");
        (
            Self {
                reader,
                writer: writer_half,
                next_id: 1,
            },
            reply,
        )
    }

    fn view(sock: &Path, worktree: &Path, config: Option<&Path>) -> Result<Self, String> {
        let line = Hello::new(worktree, config).to_line().expect("hello");
        let (client, reply) = Self::raw(sock, &line);
        if reply.trim() == "{\"meshd_hello_ok\":1}" {
            Ok(client)
        } else {
            Err(reply)
        }
    }

    fn request(&mut self, method: &str, params: Value) -> Value {
        let id = self.next_id;
        self.next_id += 1;
        let frame = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
        self.writer
            .write_all(format!("{frame}\n").as_bytes())
            .expect("request");
        let mut line = String::new();
        self.reader.read_line(&mut line).expect("response");
        let value: Value = serde_json::from_str(&line).expect("json response");
        assert_eq!(value["id"], id, "responses stay in order: {line}");
        value
    }

    fn search(&mut self, query: &str) -> String {
        let resp = self.request(
            "tools/call",
            json!({"name": "smart_search", "arguments": {"query": query}}),
        );
        resp["result"]["content"][0]["text"]
            .as_str()
            .unwrap_or_default()
            .to_string()
    }

    /// Waits for the view's first snapshot, then returns the answer to `query`.
    fn search_ready(&mut self, query: &str) -> String {
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            let text = self.search(query);
            if !text.contains("still indexing") {
                return text;
            }
            assert!(Instant::now() < deadline, "view never finished indexing");
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}

/// Asserts `text` is an answer from `own` only: its symbol and directory, never another's.
fn assert_only(text: &str, own: (&str, &Path), others: &[(&str, &Path)]) {
    assert!(
        text.contains(own.0),
        "own symbol {} missing:\n{text}",
        own.0
    );
    for (symbol, dir) in others {
        assert!(!text.contains(symbol), "cross-talk: {symbol} in\n{text}");
        let shown = format!("{}/", dir.display());
        assert!(!text.contains(&shown), "cross-talk: {shown} in\n{text}");
    }
}

fn mesh_mcp() -> Option<PathBuf> {
    let path = Path::new(env!("CARGO_BIN_EXE_meshd")).with_file_name("mesh-mcp");
    if path.exists() {
        Some(path)
    } else {
        unavailable("the mesh-mcp binary next to meshd");
        None
    }
}

/// A `mesh-mcp run` session: stdout lines arrive on a channel.
struct Session {
    child: Child,
    stdin: Option<std::process::ChildStdin>,
    lines: mpsc::Receiver<String>,
    stderr: std::sync::Arc<std::sync::Mutex<String>>,
    next_id: u64,
}

impl Session {
    fn start(env: &Env, cwd: &Path, socket: Option<&Path>) -> Option<Self> {
        let bin = mesh_mcp()?;
        let mut cmd = Command::new(bin);
        cmd.arg("run")
            .current_dir(cwd)
            .env("HOME", &env.home)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("RUST_LOG", "info")
            .env_remove("XDG_RUNTIME_DIR")
            .env_remove("MESH_SOCKET_PATH")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(sock) = socket {
            cmd.env("MESH_SOCKET_PATH", sock);
        }
        let mut child = cmd.spawn().expect("spawn mesh-mcp");
        let stdout = child.stdout.take().expect("stdout");
        let (tx, lines) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if tx.send(line).is_err() {
                    break;
                }
            }
        });
        let stderr = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
        let sink = std::sync::Arc::clone(&stderr);
        let mut err = child.stderr.take().expect("stderr");
        std::thread::spawn(move || {
            let mut buf = String::new();
            let _ = err.read_to_string(&mut buf);
            sink.lock().unwrap().push_str(&buf);
        });
        let stdin = child.stdin.take();
        Some(Self {
            child,
            stdin,
            lines,
            stderr,
            next_id: 1,
        })
    }

    fn request(&mut self, method: &str, params: Value) -> Value {
        let id = self.next_id;
        self.next_id += 1;
        let frame = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
        let stdin = self.stdin.as_mut().expect("stdin open");
        stdin
            .write_all(format!("{frame}\n").as_bytes())
            .expect("write");
        stdin.flush().expect("flush");
        let line = self
            .lines
            .recv_timeout(Duration::from_secs(120))
            .expect("a response on stdout");
        let value: Value = serde_json::from_str(&line).expect("json");
        assert_eq!(value["id"], id, "{line}");
        value
    }

    fn search_ready(&mut self, query: &str) -> String {
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            let resp = self.request(
                "tools/call",
                json!({"name": "smart_search", "arguments": {"query": query}}),
            );
            let text = resp["result"]["content"][0]["text"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            if !text.contains("still indexing") {
                return text;
            }
            assert!(Instant::now() < deadline, "never finished indexing");
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    /// Closes stdin and waits for the session to end; returns its stderr.
    fn finish(mut self) -> String {
        drop(self.stdin.take());
        let deadline = Instant::now() + Duration::from_secs(30);
        while self.child.try_wait().ok().flatten().is_none() {
            if Instant::now() > deadline {
                let _ = self.child.kill();
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        let _ = self.child.wait();
        std::thread::sleep(Duration::from_millis(100));
        self.stderr.lock().unwrap().clone()
    }
}

/// PIDs of `meshd` processes whose command line names `needle` (the test's own directory).
fn meshd_pids(needle: &Path) -> Vec<u32> {
    let out = Command::new("ps")
        .args(["-axo", "pid=,command="])
        .output()
        .expect("ps");
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter(|l| l.contains("meshd") && l.contains(&*needle.to_string_lossy()))
        .filter_map(|l| l.split_whitespace().next()?.parse().ok())
        .collect()
}

/// Kills every `meshd` started for this test's directory (the ones `mesh-mcp` spawned).
struct KillSpawned(PathBuf);

impl Drop for KillSpawned {
    fn drop(&mut self) {
        for pid in meshd_pids(&self.0) {
            let _ = Command::new("kill").arg(pid.to_string()).status();
        }
    }
}

#[test]
fn two_worktrees_share_one_daemon_and_each_session_sees_only_its_own_worktree() {
    let Some(env) = setup() else { return };
    let _cleanup = KillSpawned(env.dir.clone());
    let Some(mut a) = Session::start(&env, &env.main, None) else {
        return;
    };
    let init = a.request("initialize", json!({}));
    assert!(init["result"].is_object(), "{init}");
    let text_a = a.search_ready("Only");
    let Some(mut b) = Session::start(&env, &env.wt, None) else {
        return;
    };
    let init = b.request("initialize", json!({}));
    assert!(init["result"].is_object(), "{init}");
    let text_b = b.search_ready("Only");

    assert_only(
        &text_a,
        ("OnlyMainAlpha", &env.main),
        &[("OnlyTopicBeta", &env.wt)],
    );
    assert_only(
        &text_b,
        ("OnlyTopicBeta", &env.wt),
        &[("OnlyMainAlpha", &env.main)],
    );

    // Exactly one daemon for the two worktrees: one process, one socket.
    let pids = meshd_pids(&env.home);
    assert_eq!(pids.len(), 1, "one meshd for the repository: {pids:?}");
    let sockets: Vec<_> = std::fs::read_dir(env.home.join(".cache/mesh"))
        .expect("socket dir")
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().ends_with(".sock"))
        .collect();
    assert_eq!(sockets.len(), 1, "{sockets:?}");

    // The daemon lists both views, and `doctor` reports them.
    let meta = env.meta(&env.wt).expect("meta");
    let views = meta["views"].as_array().expect("views");
    assert_eq!(views.len(), 2, "{meta}");
    let doctor = Command::new(mesh_mcp().expect("mesh-mcp"))
        .args(["doctor", "--json", "--config"])
        .arg(env.config(&env.wt))
        .current_dir(&env.wt)
        .env("HOME", &env.home)
        .env_remove("XDG_RUNTIME_DIR")
        .env_remove("MESH_SOCKET_PATH")
        .output()
        .expect("doctor");
    let checks: Vec<Value> = serde_json::from_slice(&doctor.stdout).expect("doctor json");
    let views_check = checks
        .iter()
        .find(|c| c["name"] == "Daemon views")
        .unwrap_or_else(|| panic!("no Daemon views check: {checks:?}"));
    let message = views_check["message"].as_str().unwrap_or_default();
    assert!(message.contains("2 views"), "{message}");
    assert!(
        message.contains(&*env.main.to_string_lossy())
            && message.contains(&*env.wt.to_string_lossy()),
        "{message}"
    );

    let err_a = a.finish();
    let err_b = b.finish();
    assert!(!err_a.contains("Falling back"), "{err_a}");
    assert!(!err_b.contains("Falling back"), "{err_b}");
}

#[test]
fn concurrent_sessions_on_two_views_never_cross_talk() {
    let Some(env) = setup() else { return };
    let daemon = Daemon::start(&env, &[]);
    let mut warm_a = Client::view(&daemon.sock, &env.main, Some(&env.config(&env.main))).unwrap();
    let mut warm_b = Client::view(&daemon.sock, &env.wt, Some(&env.config(&env.wt))).unwrap();
    warm_a.search_ready("Only");
    warm_b.search_ready("Only");

    let mut handles = Vec::new();
    for i in 0..8 {
        let on_main = i % 2 == 0;
        let (worktree, own, other) = if on_main {
            (
                env.main.clone(),
                "OnlyMainAlpha",
                ("OnlyTopicBeta", env.wt.clone()),
            )
        } else {
            (
                env.wt.clone(),
                "OnlyTopicBeta",
                ("OnlyMainAlpha", env.main.clone()),
            )
        };
        let sock = daemon.sock.clone();
        handles.push(std::thread::spawn(move || {
            let config = worktree.join("mesh-mcp.toml");
            let mut client = Client::view(&sock, &worktree, Some(&config)).unwrap();
            for _ in 0..40 {
                let text = client.search("Only");
                assert_only(&text, (own, &worktree), &[(other.0, &other.1)]);
                let ping = client.request("ping", json!({}));
                assert!(ping["result"].is_object(), "{ping}");
            }
        }));
    }
    for h in handles {
        h.join().expect("no cross-talk");
    }
    assert!(daemon.log().contains("meshd listening"), "{}", daemon.log());
}

/// Inside Git two config files in one directory are two views of the one daemon, each with its
/// own graph (outside Git they stay two daemons: `server.rs`'s
/// `test_two_configs_in_one_directory_get_two_daemons_and_two_graphs`).
#[test]
fn two_configs_in_one_worktree_are_two_isolated_views() {
    let Some(env) = setup() else { return };
    std::fs::create_dir_all(env.main.join("extra")).expect("mkdir");
    std::fs::write(
        env.main.join("extra/b.ts"),
        "export class OnlyExtraDelta {}\n",
    )
    .expect("source");
    let src_cfg = env.main.join("src.toml");
    let extra_cfg = env.main.join("extra.toml");
    std::fs::write(&src_cfg, "[workspace]\nname = \"s\"\nroots = [\"src\"]\n").expect("cfg");
    std::fs::write(
        &extra_cfg,
        "[workspace]\nname = \"e\"\nroots = [\"extra\"]\n",
    )
    .expect("cfg");
    let daemon = Daemon::start(&env, &[]);
    let mut s = Client::view(&daemon.sock, &env.main, Some(&src_cfg)).unwrap();
    let mut e = Client::view(&daemon.sock, &env.main, Some(&extra_cfg)).unwrap();
    for _ in 0..10 {
        let text_s = s.search_ready("Only");
        let text_e = e.search_ready("Only");
        assert!(
            text_s.contains("OnlyMainAlpha") && !text_s.contains("OnlyExtraDelta"),
            "{text_s}"
        );
        assert!(
            text_e.contains("OnlyExtraDelta") && !text_e.contains("OnlyMainAlpha"),
            "{text_e}"
        );
    }
    let meta = env.meta(&env.main).expect("meta");
    assert_eq!(meta["views"].as_array().map(Vec::len), Some(3), "{meta}");
}

#[test]
fn simultaneous_hellos_on_a_new_view_build_it_once() {
    let Some(env) = setup() else { return };
    let daemon = Daemon::start(&env, &[]);
    // The startup view.
    let deadline = Instant::now() + Duration::from_secs(10);
    let before = loop {
        if let Some(meta) = env.meta(&env.main) {
            break meta["view_builds"].as_u64().expect("builds");
        }
        assert!(Instant::now() < deadline, "no meta:\n{}", daemon.log());
        std::thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(before, 1, "the startup view");

    let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
    let handles: Vec<_> = (0..8)
        .map(|_| {
            let (sock, wt) = (daemon.sock.clone(), env.wt.clone());
            let barrier = std::sync::Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                let config = wt.join("mesh-mcp.toml");
                let mut client = Client::view(&sock, &wt, Some(&config)).unwrap();
                let text = client.search_ready("Only");
                assert!(text.contains("OnlyTopicBeta"), "{text}");
                client
            })
        })
        .collect();
    let clients: Vec<Client> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let meta = env.meta(&env.wt).expect("meta");
    assert_eq!(meta["view_builds"], 2, "one build for 8 sessions: {meta}");
    let views = meta["views"].as_array().expect("views");
    assert_eq!(views.len(), 2, "{meta}");
    let wt_view = views
        .iter()
        .find(|v| v["worktree"] == json!(env.wt))
        .expect("wt view");
    assert_eq!(wt_view["clients"], 8, "{meta}");
    let built = daemon.log().matches("Building view").count();
    assert_eq!(built, 2, "{}", daemon.log());
    drop(clients);
}

#[test]
fn a_hello_for_another_repository_is_refused_and_the_session_falls_back() {
    let Some(env) = setup() else { return };
    let daemon = Daemon::start(&env, &[]);
    let refused = Client::view(&daemon.sock, &env.other, Some(&env.config(&env.other)));
    let reply = refused.err().expect("refused");
    assert!(reply.contains("meshd_hello_error"), "{reply}");
    assert!(reply.contains("not in the Git repository"), "{reply}");

    // A session in `other` pointed at this daemon's socket: refused, so it serves
    // itself in-process, and never this daemon's repository.
    let _cleanup = KillSpawned(env.dir.clone());
    let Some(mut session) = Session::start(&env, &env.other, Some(&daemon.sock)) else {
        return;
    };
    let text = session.search_ready("Only");
    assert_only(
        &text,
        ("OnlyOtherGamma", &env.other),
        &[("OnlyMainAlpha", &env.main), ("OnlyTopicBeta", &env.wt)],
    );
    let stderr = session.finish();
    assert!(stderr.contains("did not accept this session"), "{stderr}");
    assert!(stderr.contains("Falling back to standalone"), "{stderr}");
    // The daemon still serves its own repository.
    let mut own = Client::view(&daemon.sock, &env.main, Some(&env.config(&env.main))).unwrap();
    assert!(own.search_ready("Only").contains("OnlyMainAlpha"));
}

/// Reads what is left on `stream` until EOF, failing if it takes longer than `limit`.
fn rest_until_eof(reader: &mut BufReader<UnixStream>, limit: Duration) -> String {
    // macOS refuses a new timeout (EINVAL) on a socket the peer already shut down; the
    // 60 s one set at connect then still bounds the read.
    let _ = reader.get_ref().set_read_timeout(Some(limit));
    let mut rest = String::new();
    reader
        .read_to_string(&mut rest)
        .expect("EOF before the limit");
    rest
}

#[test]
fn malformed_oversized_slow_and_raw_frame_hellos_are_rejected() {
    let Some(env) = setup() else { return };
    let daemon = Daemon::start(&env, &[]);

    // Not JSON.
    let (mut c, reply) = Client::raw(&daemon.sock, "garbage\n");
    assert!(reply.contains("meshd_hello_error"), "{reply}");
    assert_eq!(rest_until_eof(&mut c.reader, Duration::from_secs(5)), "");

    // A raw JSON-RPC frame without a hello: refused, never answered.
    let (mut c, reply) = Client::raw(
        &daemon.sock,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}\n",
    );
    assert!(reply.contains("meshd_hello_error"), "{reply}");
    assert!(reply.contains("JSON-RPC"), "{reply}");
    assert!(!reply.contains("result"), "{reply}");
    assert_eq!(rest_until_eof(&mut c.reader, Duration::from_secs(5)), "");

    // Oversized: no newline within 8 KiB.
    let started = Instant::now();
    let big = "a".repeat(9000);
    let (mut c, reply) = Client::raw(&daemon.sock, &big);
    assert!(reply.contains("longer than"), "{reply}");
    let _ = rest_until_eof(&mut c.reader, Duration::from_secs(5));
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "{:?}",
        started.elapsed()
    );

    // Slow: nothing at all. Refused once the 5 s hello timeout passes.
    let started = Instant::now();
    let (mut c, reply) = Client::raw(&daemon.sock, "");
    let waited = started.elapsed();
    assert!(reply.contains("meshd_hello_error"), "{reply}");
    assert!(
        waited >= Duration::from_secs(4) && waited < Duration::from_secs(8),
        "{waited:?}"
    );
    assert_eq!(rest_until_eof(&mut c.reader, Duration::from_secs(5)), "");

    // A worktree of the repository paired with another worktree's config (review M1).
    let crafted = format!(
        "{{\"meshd_hello\":1,\"worktree\":{},\"config\":{}}}\n",
        serde_json::to_string(&env.main).unwrap(),
        serde_json::to_string(&env.config(&env.wt)).unwrap()
    );
    let (_c, reply) = Client::raw(&daemon.sock, &crafted);
    assert!(
        reply.contains("does not match the directory of config"),
        "{reply}"
    );

    // A directory inside the Git directory (review M2).
    let (_c, reply) = Client::raw(
        &daemon.sock,
        &Hello::new(&env.main.join(".git"), None).to_line().unwrap(),
    );
    assert!(reply.contains("inside a Git directory"), "{reply}");

    // A config file over the 1 MiB bound (review M6): refused, not read whole.
    let big = env.main.join("big.toml");
    std::fs::write(&big, vec![b'#'; 2 * 1024 * 1024]).unwrap();
    let refused = Client::view(&daemon.sock, &env.main, Some(&big))
        .err()
        .expect("refused");
    assert!(refused.contains("larger than 1024 KiB"), "{refused}");

    // A hello of an unknown version.
    let wt_json = serde_json::to_string(&env.main).unwrap();
    let (_c, reply) = Client::raw(
        &daemon.sock,
        &format!("{{\"meshd_hello\":9,\"worktree\":{wt_json},\"config\":null}}\n"),
    );
    assert!(reply.contains("unsupported hello version"), "{reply}");

    // The daemon is unaffected.
    let mut ok = Client::view(&daemon.sock, &env.main, Some(&env.config(&env.main))).unwrap();
    assert!(ok.search_ready("Only").contains("OnlyMainAlpha"));
}

#[test]
fn an_idle_view_is_dropped_and_rebuilt_and_the_daemon_exits_with_the_last_one() {
    let Some(env) = setup() else { return };
    let mut daemon = Daemon::start(
        &env,
        &["--idle-timeout-secs", "2", "--idle-poll-millis", "200"],
    );
    let mut a = Client::view(&daemon.sock, &env.main, Some(&env.config(&env.main))).unwrap();
    assert!(a.search_ready("Only").contains("OnlyMainAlpha"));
    let mut b = Client::view(&daemon.sock, &env.wt, Some(&env.config(&env.wt))).unwrap();
    assert!(b.search_ready("Only").contains("OnlyTopicBeta"));
    let builds = env.meta(&env.wt).expect("meta")["view_builds"]
        .as_u64()
        .expect("builds");
    drop(b);

    // `wt` has no session: dropped after the timeout, while `main` stays.
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let views = env.meta(&env.main).expect("meta")["views"]
            .as_array()
            .expect("views")
            .clone();
        if views.len() == 1 {
            assert_eq!(views[0]["worktree"], json!(env.main));
            break;
        }
        assert!(Instant::now() < deadline, "wt never idled out: {views:?}");
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(daemon.log().contains("had no session"), "{}", daemon.log());
    // `main` keeps answering through the drop.
    assert!(a.search("Only").contains("OnlyMainAlpha"));

    // The next hello builds it again.
    let mut b = Client::view(&daemon.sock, &env.wt, Some(&env.config(&env.wt))).unwrap();
    assert!(b.search_ready("Only").contains("OnlyTopicBeta"));
    let rebuilt = env.meta(&env.wt).expect("meta")["view_builds"]
        .as_u64()
        .expect("builds");
    assert_eq!(rebuilt, builds + 1);

    // Last sessions gone: the views idle out, then the daemon stops on its own.
    drop(a);
    drop(b);
    assert!(
        daemon.exited_within(Duration::from_secs(20)),
        "meshd did not exit after its last view:\n{}",
        daemon.log()
    );
    assert!(!daemon.sock.exists(), "socket removed on exit");
    assert!(env.meta(&env.main).is_none(), "meta removed on exit");
}
