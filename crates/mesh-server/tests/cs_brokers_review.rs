#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! 9.0 T8d through the whole stack (index, graph, tools):
//! - a connection string or a credential-shaped name of a C# broker site is never a channel name or
//!   a signature, in the graph, in any tool answer or in a cache file (commandment 5);
//! - a same-file re-emit never crosses from one broker to another.

use mesh_core::{AppState, AuditLogger, BackgroundRescanEngine, Config};
use mesh_server::tools::ToolRegistry;
use serde_json::{json, Value};
use std::path::Path;
use std::sync::Arc;

fn write(root: &Path, rel: &str, text: &str) {
    let p = root.join(rel);
    std::fs::create_dir_all(p.parent().unwrap()).unwrap();
    std::fs::write(p, text).unwrap();
}

fn state_for(root: &Path) -> Arc<AppState> {
    let cfg =
        Config::load_from_str("[workspace]\nname = \"t\"\nversion = \"0\"\nroots = [\".\"]\n")
            .expect("config");
    let audit = Arc::new(AuditLogger::new_in_memory().expect("audit"));
    let rescan = Arc::new(BackgroundRescanEngine::new().expect("rescan"));
    let state = Arc::new(AppState::new(cfg, vec![root.to_path_buf()], audit, rescan));
    let snap = mesh_server::WorkspaceIndexer::build_snapshot(
        &state.config,
        &state.allowed_roots,
        None,
        None,
        None,
    );
    state.install_snapshot(snap);
    state
}

async fn text(state: &Arc<AppState>, tool: &str, args: Value) -> String {
    match ToolRegistry::call_tool(tool, args, state.clone()).await {
        Ok(v) => v["content"][0]["text"].as_str().unwrap_or("").to_string(),
        Err(e) => format!("{e:?}"),
    }
}

/// Everything an agent or a graph consumer can read.
async fn everything(state: &Arc<AppState>, targets: &[&str]) -> String {
    let snap = state.snapshot();
    let mut out = format!(
        "{:?}\n{:?}\n{:?}\n",
        snap.contract_graph.all_nodes().collect::<Vec<_>>(),
        snap.contract_graph.all_edges(),
        snap.config_values
    );
    drop(snap);
    out += &text(state, "visualize_mesh", json!({})).await;
    out += &text(state, "visualize_mesh", json!({"format": "mermaid"})).await;
    for t in targets {
        out += &text(state, "analyze_impact", json!({"target": t, "depth": 3})).await;
        out += &text(state, "trace_flow", json!({"target": t})).await;
        out += &text(state, "smart_search", json!({"query": t})).await;
        out += &text(state, "find_dependents", json!({"target": t})).await;
    }
    out
}

/// Credential-shaped values built at runtime so no key-shaped literal sits in the source.
fn secret_like() -> String {
    format!("sk_{}_{}", "live", "ABCDEF0123456789")
}

const MIX: &str = r#"using Confluent.Kafka;
using MassTransit;

public class Mix
{
    private readonly IProducer<string, string> _producer;
    public void Configure(IServiceCollection s)
    {
        s.AddMassTransit(x => x.UsingRabbitMq((c, f) => f.ReceiveEndpoint("orders", e => { })));
    }
    public Task Out() => _producer.ProduceAsync("orders", new Message<string, string>());
}
"#;

#[tokio::test]
async fn a_same_file_reemit_never_crosses_from_one_broker_to_another() {
    let tmp = tempfile::tempdir().unwrap();
    let root = dunce::canonicalize(tmp.path()).unwrap();
    write(&root, "src/Svc/Mix.cs", MIX);
    let state = state_for(&root);
    for tool in ["analyze_impact", "trace_flow"] {
        let args = |t: &str| {
            if tool == "analyze_impact" {
                json!({"target": t, "depth": 3, "limit": 200})
            } else {
                json!({"target": t})
            }
        };
        // The RabbitMQ consumer of `orders` sits in the file that produces it on Kafka.
        let rabbit = text(&state, tool, args("rabbitmq:orders")).await;
        assert!(
            rabbit.contains("Mix.cs:9"),
            "{tool}: not a vacuous answer: {rabbit}"
        );
        assert!(!rabbit.contains("Mix.cs:11"), "{tool}: {rabbit}");
        assert!(!rabbit.contains("kafka:orders"), "{tool}: {rabbit}");
        // `trace_flow` starts from a consumer: the Kafka side has only a producer.
        if tool == "analyze_impact" {
            let kafka = text(&state, tool, args("kafka:orders")).await;
            assert!(
                kafka.contains("Mix.cs:11"),
                "{tool}: not a vacuous answer: {kafka}"
            );
            assert!(!kafka.contains("Mix.cs:9"), "{tool}: {kafka}");
        }
    }
}

#[tokio::test]
async fn a_connection_string_or_credential_shaped_name_reaches_no_graph_answer_signature_or_cache_file(
) {
    let key = ["Shared", "Access", "Key"].concat();
    let conn_canary = ["ZqCanary", "Conn", "9x"].concat();
    let canaries = [
        format!("Endpoint=sb://canary.example.net/;{key}={conn_canary}"),
        conn_canary.clone(),
        "Bearer_CANARYB".to_string(),
        "hunter2-prod-CANARYL".to_string(),
        secret_like(),
        "ConnCanaryAttr".to_string(),
    ];
    let tmp = tempfile::tempdir().unwrap();
    let root = dunce::canonicalize(tmp.path().join("ws")).unwrap_or_else(|_| {
        std::fs::create_dir_all(tmp.path().join("ws")).unwrap();
        dunce::canonicalize(tmp.path().join("ws")).unwrap()
    });
    let code = format!(
        r#"using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Configuration;

public class Svc
{{
    private const string Conn = "Endpoint=sb://canary.example.net/;{key}={conn_canary}";
    private readonly ServiceBusClient _client;

    public void Run(IConfiguration configuration)
    {{
        _client.CreateSender(Conn);
        _client.CreateSender("Endpoint=sb://canary.example.net/;{key}={conn_canary}");
        _client.CreateSender("Bearer_CANARYB");
        _client.CreateSender("hunter2-prod-CANARYL");
        _client.CreateSender("{token}");
        _client.CreateSender(configuration["Bus:{key}"]);
        _client.CreateSender("orders.created");
    }}

    [ServiceBusTrigger("%Bus_Password%", Connection = "Endpoint=sb://canary.example.net/;{key}={conn_canary}")]
    public void Handle(string m) {{ }}

    [ServiceBusTrigger("triggered.ok", Connection = "ConnCanaryAttr")]
    public void Other(string m) {{ }}
}}
"#,
        token = secret_like()
    );
    write(&root, "src/Svc/Secrets.cs", &code);
    let cache_dir = tmp.path().join("cache");
    std::fs::create_dir_all(&cache_dir).unwrap();
    let db = cache_dir.join("index-cache.db");
    {
        let cache = mesh_core::PersistentIndexCache::open(db.clone(), 64 * 1024 * 1024).unwrap();
        let cfg =
            Config::load_from_str("[workspace]\nname = \"t\"\nversion = \"0\"\nroots = [\".\"]\n")
                .unwrap();
        let audit = Arc::new(AuditLogger::new_in_memory().unwrap());
        let rescan = Arc::new(BackgroundRescanEngine::new().unwrap());
        let state = Arc::new(AppState::new(cfg, vec![root.clone()], audit, rescan));
        let snap = mesh_server::WorkspaceIndexer::build_snapshot(
            &state.config,
            &state.allowed_roots,
            None,
            None,
            Some(&cache),
        );
        state.install_snapshot(snap);
        let all = everything(&state, &["orders.created", "triggered.ok"]).await;
        assert!(
            all.contains("orders.created") && all.contains("triggered.ok"),
            "real names are kept:\n{all}"
        );
        for canary in &canaries {
            // `ConnCanaryAttr` is a plain setting name (a name, not a secret) and may show.
            if canary == "ConnCanaryAttr" {
                continue;
            }
            assert!(!all.contains(canary.as_str()), "{canary} leaked:\n{all}");
        }
        // The credential-named key stays out of the answer (no `Bus__SharedAccessKey` placeholder).
        assert!(
            !all.to_ascii_lowercase().contains("sharedaccesskey"),
            "{all}"
        );
    }
    // The cache files (database, write-ahead log, shared memory) hold none of them either.
    let mut checked = 0;
    for entry in std::fs::read_dir(&cache_dir).unwrap() {
        let bytes = std::fs::read(entry.unwrap().path()).unwrap();
        checked += 1;
        for canary in canaries.iter().filter(|c| c.as_str() != "ConnCanaryAttr") {
            assert!(
                !bytes.windows(canary.len()).any(|w| w == canary.as_bytes()),
                "{canary} reached a cache file"
            );
        }
        let lower = String::from_utf8_lossy(&bytes).to_ascii_lowercase();
        assert!(
            !lower.contains("sharedaccesskey"),
            "key name reached a cache file"
        );
    }
    assert!(
        checked >= 1,
        "the cache wrote nothing: the check is vacuous"
    );
}
