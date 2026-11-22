#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! 9.0 T6, commandment 5: no value a configuration file holds reaches the graph or a tool
//! answer unless a source site names it as a topic AND it is a topic-shaped, non-secret value
//! of a file the default policy reads. Each case writes a canary and greps the graph and the
//! tool output for it.

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

/// Everything an agent or a graph consumer can read: the graph (nodes and edges), the config
/// values the indexer kept, and the output of the tools, for the canaries as targets too.
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
    for t in targets {
        out += &text(state, "analyze_impact", json!({"target": t, "depth": 3})).await;
        out += &text(state, "trace_flow", json!({"entry": t})).await;
        out += &text(state, "smart_search", json!({"query": t})).await;
        out += &text(state, "find_dependents", json!({"target": t})).await;
    }
    out
}

const SVC: &str = "import { Kafka } from 'kafkajs';\nconst k = new Kafka({ brokers: ['b'] });\nconst producer = k.producer();\nexport async function run() {\n  await producer.send({ topic: process.env.PRODENV_TOPIC, messages: [] });\n  await producer.send({ topic: process.env.DB_URL_TOPIC, messages: [] });\n  await producer.send({ topic: process.env.STRIPE_KEY, messages: [] });\n  await producer.send({ topic: process.env.SENTRY_DSN, messages: [] });\n  await producer.send({ topic: process.env.DATABASE_URL, messages: [] });\n  await producer.send({ topic: process.env.PG_URL, messages: [] });\n  await producer.send({ topic: process.env.SECRET_TOPIC, messages: [] });\n  await producer.send({ topic: process.env.DOT_TOPIC, messages: [] });\n  await producer.send({ topic: process.env.DIST_TOPIC, messages: [] });\n  await producer.send({ topic: process.env.GOOD_TOPIC, messages: [] });\n}\n";

/// F1: `production.env`, `secrets.env`, `.env.local`, `.env.production` are never read.
/// F2: `.env.distributor`, `.env.examples`, `x.env.example.bak` are not templates.
#[tokio::test]
async fn real_dotenv_files_and_look_alikes_never_reach_the_graph_or_a_tool() {
    let tmp = tempfile::tempdir().unwrap();
    let root = dunce::canonicalize(tmp.path()).unwrap();
    write(&root, "svc.ts", SVC);
    write(
        &root,
        "production.env",
        "PRODENV_TOPIC=leak-prodenv\nDB_URL_TOPIC=postgres://u:leakurl@db/x\n",
    );
    write(&root, "secrets.env", "PRODENV_TOPIC=leak-secretsenv\n");
    write(&root, ".env.local", "PRODENV_TOPIC=leak-local\n");
    write(&root, ".env.production", "PRODENV_TOPIC=leak-envprod\n");
    write(&root, ".env.distributor", "DIST_TOPIC=leak-distributor\n");
    write(&root, ".env.examples", "DIST_TOPIC=leak-examples\n");
    write(&root, "x.env.example.bak", "DIST_TOPIC=leak-bak\n");
    write(
        &root,
        ".env.example.local",
        "DIST_TOPIC=leak-examplelocal\n",
    );
    write(&root, ".env.example", "GOOD_TOPIC=good.template.topic\n");
    let state = state_for(&root);
    let all = everything(&state, &["${prodenv_topic}", "${dist_topic}"]).await;
    for canary in [
        "leak-prodenv",
        "leakurl",
        "leak-secretsenv",
        "leak-local",
        "leak-envprod",
        "leak-distributor",
        "leak-examples",
        "leak-bak",
        "leak-examplelocal",
    ] {
        assert!(!all.contains(canary), "{canary} leaked");
    }
    // The template is read: its value is the one resolved topic (heuristic, a template).
    assert!(all.contains("good.template.topic"));
    // The unresolved sites stay visible as their placeholder.
    assert!(all.contains("${prodenv_topic}"));
}

/// F3: credentials, connection strings and token-looking values in files that ARE read
/// (a template, a properties file, a yaml, a compose file, a Kubernetes manifest).
#[tokio::test]
async fn secrets_in_read_config_files_never_reach_the_graph_or_a_tool() {
    let tmp = tempfile::tempdir().unwrap();
    let root = dunce::canonicalize(tmp.path()).unwrap();
    write(&root, "svc.ts", SVC);
    write(
        &root,
        ".env.example",
        "STRIPE_KEY=sk_live_leakstripe\nSENTRY_DSN=https://leakdsn@o1.ingest.sentry.io/1\n\
         DATABASE_URL=postgres://admin:leakpw@db/app\nSECRET_TOPIC=leak.secret.topic\n\
         GOOD_TOPIC=good.topic\n",
    );
    write(
        &root,
        "docker-compose.yml",
        "services:\n  api:\n    environment:\n      PG_URL: postgres://u:pwcompose@db/x\n",
    );
    write(
        &root,
        "compose2.yml",
        "services:\n  api:\n    environment:\n      - PG_URL=postgres://u:pwlist@db/x\n      - DOT_TOPIC=ghp_abcdefghijklmnopqrstuvwxyz0123\n",
    );
    write(
        &root,
        "k8s/dep.yaml",
        "spec:\n  containers:\n    - name: a\n      env:\n        - name: DATABASE_URL\n          value: \"postgres://k:leakk8s@db/y\"\n        - name: PRODENV_TOPIC\n          value: AKIAIOSFODNN7LEAKKEY\n",
    );
    write(
        &root,
        "application.properties",
        "app.stripe.topic=leak.stripe.prop\ndb.password=leakprop\n",
    );
    let state = state_for(&root);
    let all = everything(
        &state,
        &[
            "${stripe_key}",
            "${database_url}",
            "${sentry_dsn}",
            "${pg_url}",
        ],
    )
    .await;
    for canary in [
        "sk_live_leakstripe",
        "leakdsn",
        "leakpw",
        "leak.secret.topic",
        "pwcompose",
        "pwlist",
        "ghp_abcdefghijklmnop",
        "leakk8s",
        "AKIAIOSFODNN7LEAKKEY",
        "leak.stripe.prop",
        "leakprop",
    ] {
        assert!(!all.contains(canary), "{canary} leaked");
    }
    assert!(all.contains("good.topic"));
}

/// Round 2: a credential written as a placeholder DEFAULT (in a yaml value, a compose variable,
/// a source site or a nested default) never becomes a node name, a topic or a tool answer.
#[tokio::test]
async fn credentials_in_placeholder_defaults_never_reach_the_graph_or_a_tool() {
    let tmp = tempfile::tempdir().unwrap();
    let root = dunce::canonicalize(tmp.path()).unwrap();
    write(
        &root,
        "src/Svc.java",
        "package a;\npublic class Svc {\n\
         \x20 @KafkaListener(topics = \"${app.topics.b}\")\n  public void a(String m) {}\n\
         \x20 @KafkaListener(topics = \"${kafka.dflt:postgres://uR1:pwR1leak@db.internal/app}\")\n  public void b(String m) {}\n\
         \x20 @KafkaListener(topics = \"${n1:${n2:sk_live_SITENESTx0123456789abc}}\")\n  public void c(String m) {}\n\
         \x20 @KafkaListener(topics = \"${app.topics.nested}\")\n  public void d(String m) {}\n\
         \x20 @KafkaListener(topics = \"${plain.dflt:fallback.topic}\")\n  public void e(String m) {}\n}\n",
    );
    write(
        &root,
        "src/main/resources/application.yml",
        "app:\n  topics:\n    b: \"${KEY_B:sk_live_CFGLEAKabcdef0123}\"\n    nested: \"${a:${b:sk_live_NESTEDLEAKx0123456789}}\"\n",
    );
    write(
        &root,
        "docker-compose.yml",
        "services:\n  api:\n    environment:\n      LIVE_TOPIC: \"${LIVE_X:sk_live_DEFAULTLEAK0123abcd}\"\n",
    );
    write(
        &root,
        "svc.ts",
        "import { Kafka } from 'kafkajs';\nconst k = new Kafka({ brokers: ['b'] });\nconst producer = k.producer();\nexport async function run() {\n  await producer.send({ topic: process.env.LIVE_TOPIC, messages: [] });\n  await producer.send({ topic: process.env.TSDEF ?? 'sk_live_TSDEFAULTLEAK0123456789', messages: [] });\n}\n",
    );
    let state = state_for(&root);
    let all = everything(
        &state,
        &[
            "${app.topics.b}",
            "${kafka.dflt}",
            "${live_topic}",
            "${tsdef}",
            "${app.topics.nested}",
            "${n1}",
        ],
    )
    .await;
    for canary in [
        "sk_live_CFGLEAK",
        "uR1",
        "pwR1leak",
        "sk_live_SITENEST",
        "sk_live_NESTEDLEAK",
        "sk_live_DEFAULTLEAK",
        "sk_live_TSDEFAULTLEAK",
        "db.internal",
    ] {
        if let Some(at) = all.find(canary) {
            panic!(
                "{canary} leaked: ...{}...",
                &all[at.saturating_sub(80)..(at + 80).min(all.len())]
            );
        }
    }
    // The sites stay visible under their placeholder; a plain default is still used.
    assert!(all.contains("${app.topics.b}"), "{all}");
    assert!(all.contains("${kafka.dflt}"), "{all}");
    assert!(all.contains("fallback.topic"), "{all}");
    assert!(state.snapshot().health.config_topics_unresolved >= 4);
}

/// Round 2: the parse cache stores the extracted index; it must not carry the credential either.
#[test]
fn the_cached_file_index_carries_no_default_credential() {
    let fi = mesh_parsers::PolyglotIndexer::extract(
        Path::new("src/Svc.java"),
        "package a;\npublic class Svc {\n  @KafkaListener(topics = \"${k:postgres://u:pwcache@h/x}\")\n  public void b(String m) {}\n}\n",
        0,
    );
    let cached = serde_json::to_string(&fi).unwrap();
    assert!(
        !cached.contains("pwcache") && !cached.contains("postgres"),
        "{cached}"
    );
    assert!(cached.contains("${k}"));
}
