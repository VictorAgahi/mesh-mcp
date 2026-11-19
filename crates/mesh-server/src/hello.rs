//! The one-line handshake between a `mesh-mcp run` proxy and `meshd` (8.0.0).
//!
//! One `meshd` serves every worktree and config file of a Git repository, one view each. The
//! proxy says which view it wants before any JSON-RPC byte crosses the socket:
//!
//! ```text
//! proxy -> meshd   {"meshd_hello":1,"worktree":"/abs/dir","config":"/abs/mesh-mcp.toml"}\n
//! meshd -> proxy   {"meshd_hello_ok":1}\n            or {"meshd_hello_error":"<reason>"}\n
//! ```
//!
//! The daemon reads the first line under [`MAX_HELLO_BYTES`] and [`HELLO_TIMEOUT`]; anything
//! that is not a valid hello (a raw JSON-RPC frame included) gets an error line and the
//! connection is closed, so a frame is never answered from a view nobody asked for. The proxy
//! forwards no client byte before the ack, and reads the ack one byte at a time so nothing the
//! daemon sends after it is swallowed. This module is shared by the Unix socket and the Windows
//! named pipe transports.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::io::{
    AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt,
};

/// The only hello version this binary speaks.
pub const HELLO_VERSION: u32 = 1;

/// Upper bound on a hello or ack line, newline included.
pub const MAX_HELLO_BYTES: usize = 8 * 1024;

/// How long the daemon waits for the hello line of a new connection.
pub const HELLO_TIMEOUT: Duration = Duration::from_secs(5);

/// How long the proxy waits for the daemon's ack. Building a new view only validates and
/// registers it (indexing runs in the background), so this is generous.
pub const ACK_TIMEOUT: Duration = Duration::from_secs(10);

/// The first line a proxy sends.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Hello {
    pub meshd_hello: u32,
    /// The session's canonical base directory: the directory its config file sits in, or the
    /// directory it runs from without one.
    pub worktree: PathBuf,
    /// The session's canonical config file, `None` for the built-in default.
    pub config: Option<PathBuf>,
}

impl Hello {
    /// The hello of a session whose base directory is `base_dir` and whose located config is
    /// `config`, both canonicalized when they exist. With a config the worktree sent is the
    /// canonical directory of the canonical config (what the daemon builds the view from, and
    /// what it requires the worktree to be), so a config reached through a symlink still
    /// names a consistent pair.
    pub fn new(base_dir: &Path, config: Option<&Path>) -> Self {
        let canon = |p: &Path| dunce::canonicalize(p).unwrap_or_else(|_| p.to_path_buf());
        let config = config.map(canon);
        let worktree = config
            .as_deref()
            .and_then(Path::parent)
            .map_or_else(|| canon(base_dir), canon);
        Self {
            meshd_hello: HELLO_VERSION,
            worktree,
            config,
        }
    }

    /// The hello as one line, newline included.
    pub fn to_line(&self) -> Result<String, HelloError> {
        let mut line =
            serde_json::to_string(self).map_err(|e| HelloError::Malformed(e.to_string()))?;
        line.push('\n');
        Ok(line)
    }
}

/// The daemon's answer to a hello.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Reply {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    meshd_hello_ok: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    meshd_hello_error: Option<String>,
}

/// Why a handshake did not complete.
#[derive(Debug, thiserror::Error)]
pub enum HelloError {
    #[error("the connection closed before the hello")]
    Closed,
    #[error("no complete hello line within {0:?}")]
    Timeout(Duration),
    #[error("hello line longer than {0} bytes")]
    TooLong(usize),
    #[error("not a meshd hello: {0}")]
    Malformed(String),
    #[error("unsupported hello version {0} (this binary speaks {HELLO_VERSION})")]
    Version(u32),
    #[error("meshd refused the session: {0}")]
    Refused(String),
    #[error("i/o error during the hello: {0}")]
    Io(#[from] std::io::Error),
}

/// The ack line for an accepted hello.
pub fn ok_line() -> String {
    "{\"meshd_hello_ok\":1}\n".to_string()
}

/// The error line for a refused hello. Never fails: a reason that cannot be serialized is
/// replaced by a fixed one.
pub fn error_line(reason: &str) -> String {
    let reply = Reply {
        meshd_hello_ok: None,
        meshd_hello_error: Some(reason.to_string()),
    };
    let mut line = serde_json::to_string(&reply)
        .unwrap_or_else(|_| "{\"meshd_hello_error\":\"refused\"}".to_string());
    line.push('\n');
    line
}

/// Parses one hello line (newline optional).
pub fn parse_hello(line: &str) -> Result<Hello, HelloError> {
    let value: serde_json::Value = serde_json::from_str(line.trim())
        .map_err(|e| HelloError::Malformed(format!("not JSON: {e}")))?;
    let Some(object) = value.as_object() else {
        return Err(HelloError::Malformed("not a JSON object".to_string()));
    };
    if object.contains_key("jsonrpc") || object.contains_key("method") {
        return Err(HelloError::Malformed(
            "a JSON-RPC frame arrived before the hello; connect through `mesh-mcp run` of the \
             same version"
                .to_string(),
        ));
    }
    let hello: Hello =
        serde_json::from_value(value).map_err(|e| HelloError::Malformed(e.to_string()))?;
    if hello.meshd_hello != HELLO_VERSION {
        return Err(HelloError::Version(hello.meshd_hello));
    }
    if !hello.worktree.is_absolute() {
        return Err(HelloError::Malformed(
            "`worktree` must be an absolute path".to_string(),
        ));
    }
    if hello.config.as_deref().is_some_and(|c| !c.is_absolute()) {
        return Err(HelloError::Malformed(
            "`config` must be an absolute path".to_string(),
        ));
    }
    Ok(hello)
}

/// Daemon side: reads the first line of a connection from `reader` (at most `max` bytes,
/// within `timeout`) and parses it as a hello. Bytes after the newline stay in `reader`.
pub async fn read_hello<R>(
    reader: &mut R,
    max: usize,
    timeout: Duration,
) -> Result<Hello, HelloError>
where
    R: AsyncBufRead + Unpin,
{
    let mut buf = Vec::new();
    let read = async {
        let limit = u64::try_from(max).unwrap_or(u64::MAX).saturating_add(1);
        (&mut *reader).take(limit).read_until(b'\n', &mut buf).await
    };
    let n = tokio::time::timeout(timeout, read)
        .await
        .map_err(|_| HelloError::Timeout(timeout))??;
    if n == 0 {
        return Err(HelloError::Closed);
    }
    if buf.last() != Some(&b'\n') {
        return Err(if buf.len() > max {
            HelloError::TooLong(max)
        } else {
            HelloError::Malformed("the connection closed in the middle of the hello".to_string())
        });
    }
    let line = std::str::from_utf8(&buf)
        .map_err(|_| HelloError::Malformed("not valid UTF-8".to_string()))?;
    parse_hello(line)
}

/// Proxy side: sends `hello` and waits for the daemon's ack. Reads the ack one byte at a time
/// (it is a few dozen bytes) so not one byte the daemon sends after it is consumed here.
pub async fn client_handshake<R, W>(
    reader: &mut R,
    writer: &mut W,
    hello: &Hello,
    timeout: Duration,
) -> Result<(), HelloError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let line = hello.to_line()?;
    let exchange = async {
        writer.write_all(line.as_bytes()).await?;
        writer.flush().await?;
        let mut ack = Vec::new();
        let mut byte = [0u8; 1];
        loop {
            if reader.read(&mut byte).await? == 0 {
                return Err(HelloError::Closed);
            }
            if byte[0] == b'\n' {
                break;
            }
            ack.push(byte[0]);
            if ack.len() >= MAX_HELLO_BYTES {
                return Err(HelloError::TooLong(MAX_HELLO_BYTES));
            }
        }
        Ok(ack)
    };
    let ack = tokio::time::timeout(timeout, exchange)
        .await
        .map_err(|_| HelloError::Timeout(timeout))??;
    let reply: Reply = serde_json::from_slice(&ack)
        .map_err(|e| HelloError::Malformed(format!("unexpected ack from meshd: {e}")))?;
    match reply {
        Reply {
            meshd_hello_ok: Some(HELLO_VERSION),
            meshd_hello_error: None,
        } => Ok(()),
        Reply {
            meshd_hello_error: Some(reason),
            ..
        } => Err(HelloError::Refused(reason)),
        _ => Err(HelloError::Malformed(
            "unexpected ack from meshd".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::BufReader;

    fn abs(p: &str) -> PathBuf {
        if cfg!(windows) {
            PathBuf::from(format!(
                "C:\\{}",
                p.trim_start_matches('/').replace('/', "\\")
            ))
        } else {
            PathBuf::from(p)
        }
    }

    fn line_for(worktree: &Path, config: Option<&Path>) -> String {
        Hello {
            meshd_hello: 1,
            worktree: worktree.to_path_buf(),
            config: config.map(Path::to_path_buf),
        }
        .to_line()
        .unwrap()
    }

    #[test]
    fn a_valid_hello_round_trips() {
        let wt = abs("/r/wt");
        let cfg = abs("/r/wt/mesh-mcp.toml");
        let line = line_for(&wt, Some(&cfg));
        assert!(line.starts_with("{\"meshd_hello\":1,"), "{line}");
        assert!(line.ends_with('\n'));
        let hello = parse_hello(&line).unwrap();
        assert_eq!(hello.worktree, wt);
        assert_eq!(hello.config.as_deref(), Some(cfg.as_path()));
        let none = parse_hello(&line_for(&wt, None)).unwrap();
        assert_eq!(none.config, None);
    }

    #[test]
    fn anything_but_a_hello_is_refused() {
        let wt = abs("/r");
        let wt_json = serde_json::to_string(&wt).unwrap();
        for bad in [
            "not json".to_string(),
            "[1,2]".to_string(),
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}".to_string(),
            "{\"meshd_hello\":1}".to_string(),
            format!("{{\"meshd_hello\":1,\"worktree\":{wt_json},\"config\":null,\"x\":1}}"),
            "{\"meshd_hello\":1,\"worktree\":\"relative/dir\",\"config\":null}".to_string(),
            format!("{{\"meshd_hello\":1,\"worktree\":{wt_json},\"config\":\"rel.toml\"}}"),
        ] {
            assert!(parse_hello(&bad).is_err(), "accepted: {bad}");
        }
        let v2 = format!("{{\"meshd_hello\":2,\"worktree\":{wt_json},\"config\":null}}");
        assert!(matches!(parse_hello(&v2), Err(HelloError::Version(2))));
        let rpc = parse_hello("{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}");
        assert!(
            matches!(&rpc, Err(HelloError::Malformed(m)) if m.contains("JSON-RPC")),
            "{rpc:?}"
        );
    }

    #[tokio::test]
    async fn the_reader_stops_at_the_newline_and_keeps_what_follows() {
        let line = line_for(&abs("/r"), None);
        let input = format!("{line}{{\"next\":1}}\n");
        let mut reader = BufReader::new(input.as_bytes());
        let hello = read_hello(&mut reader, MAX_HELLO_BYTES, HELLO_TIMEOUT)
            .await
            .unwrap();
        assert_eq!(hello.worktree, abs("/r"));
        let mut rest = String::new();
        reader.read_line(&mut rest).await.unwrap();
        assert_eq!(rest, "{\"next\":1}\n");
    }

    #[tokio::test]
    async fn an_oversized_or_truncated_or_empty_hello_is_refused() {
        let big = vec![b'a'; MAX_HELLO_BYTES + 100];
        let mut reader = BufReader::new(big.as_slice());
        let err = read_hello(&mut reader, MAX_HELLO_BYTES, HELLO_TIMEOUT).await;
        assert!(matches!(err, Err(HelloError::TooLong(_))), "{err:?}");

        let mut reader = BufReader::new(&b"{\"meshd_hello\":1"[..]);
        let err = read_hello(&mut reader, MAX_HELLO_BYTES, HELLO_TIMEOUT).await;
        assert!(matches!(err, Err(HelloError::Malformed(_))), "{err:?}");

        let mut reader = BufReader::new(&b""[..]);
        let err = read_hello(&mut reader, MAX_HELLO_BYTES, HELLO_TIMEOUT).await;
        assert!(matches!(err, Err(HelloError::Closed)), "{err:?}");
    }

    #[tokio::test]
    async fn a_silent_client_times_out() {
        let (_client, server) = tokio::io::duplex(64);
        let mut reader = BufReader::new(server);
        let started = std::time::Instant::now();
        let err = read_hello(&mut reader, MAX_HELLO_BYTES, Duration::from_millis(200)).await;
        assert!(matches!(err, Err(HelloError::Timeout(_))), "{err:?}");
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[tokio::test]
    async fn the_client_handshake_reads_only_the_ack() {
        let (client, server) = tokio::io::duplex(1024);
        let daemon = tokio::spawn(async move {
            let (r, mut w) = tokio::io::split(server);
            let mut reader = BufReader::new(r);
            let hello = read_hello(&mut reader, MAX_HELLO_BYTES, HELLO_TIMEOUT)
                .await
                .unwrap();
            // The ack and a later frame in one write: the handshake must leave the frame.
            w.write_all(format!("{}{{\"after\":1}}\n", ok_line()).as_bytes())
                .await
                .unwrap();
            hello
        });
        let (mut r, mut w) = tokio::io::split(client);
        let hello = Hello::new(&abs("/r"), None);
        client_handshake(&mut r, &mut w, &hello, ACK_TIMEOUT)
            .await
            .unwrap();
        assert_eq!(daemon.await.unwrap().worktree, hello.worktree);
        let mut rest = String::new();
        BufReader::new(r).read_line(&mut rest).await.unwrap();
        assert_eq!(rest, "{\"after\":1}\n");
    }

    #[tokio::test]
    async fn a_refusal_carries_the_reason() {
        let (client, server) = tokio::io::duplex(1024);
        tokio::spawn(async move {
            let (r, mut w) = tokio::io::split(server);
            let mut reader = BufReader::new(r);
            let _ = read_hello(&mut reader, MAX_HELLO_BYTES, HELLO_TIMEOUT).await;
            w.write_all(error_line("other \"repo\"").as_bytes())
                .await
                .unwrap();
        });
        let (mut r, mut w) = tokio::io::split(client);
        let err =
            client_handshake(&mut r, &mut w, &Hello::new(&abs("/r"), None), ACK_TIMEOUT).await;
        assert!(
            matches!(&err, Err(HelloError::Refused(m)) if m == "other \"repo\""),
            "{err:?}"
        );
    }
}
