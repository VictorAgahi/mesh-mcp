#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! 9.0 T9: the `event_anomalies` tool on the hand-annotated corpora
//! (`tests/golden/events/anomalies.report.yaml`), its determinism (runs, cold and warm index
//! cache), its scope jail, its paging, hostile inputs and the secrets canaries.

use mesh_core::{
    expand_roots, AppState, AuditLogger, BackgroundRescanEngine, Config, PersistentIndexCache,
};
use mesh_server::tools::ToolRegistry;
use mesh_server::WorkspaceIndexer;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

fn repo(rel: &str) -> PathBuf {
    dunce::canonicalize(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(rel),
    )
    .expect("canonicalize")
}

fn state_in(dir: &Path, cache: Option<&PersistentIndexCache>) -> Arc<AppState> {
    let config = Config::load_from_file(&dir.join("mesh-mcp.toml")).expect("fixture config");
    let roots = expand_roots(
        &config.workspace.roots,
        dir,
        &config.workspace.workspace_root,
    )
    .expect("expand roots");
    let audit = Arc::new(AuditLogger::new_in_memory().expect("audit"));
    let rescan = Arc::new(BackgroundRescanEngine::new().expect("rescan"));
    let state = Arc::new(AppState::new(config, roots, audit, rescan));
    let snapshot =
        WorkspaceIndexer::build_snapshot(&state.config, &state.allowed_roots, None, None, cache);
    state.install_snapshot(snapshot);
    state
}

/// The raw `CallToolResult`.
async fn raw(state: &Arc<AppState>, args: Value) -> Value {
    ToolRegistry::call_tool("event_anomalies", args, state.clone())
        .await
        .expect("tool call")
}

async fn report(state: &Arc<AppState>, args: Value) -> String {
    let res = raw(state, args).await;
    assert_eq!(res["isError"], json!(false), "{res}");
    res["content"][0]["text"]
        .as_str()
        .expect("text")
        .to_string()
}

const CLASSES: [(&str, &str); 7] = [
    ("published_never_consumed", "Published, never consumed"),
    ("consumed_never_published", "Consumed, never published"),
    ("unresolved", "Unresolved"),
    ("cycles", "Cycles"),
    ("fan_out_topics", "Fan-out: topics"),
    ("fan_out_producers", "Fan-out: files"),
    ("test_only_consumers", "Test-only consumers"),
];

/// The top-level bullet rows of each class section.
fn sections(text: &str) -> BTreeMap<&'static str, Vec<String>> {
    let mut out: BTreeMap<&'static str, Vec<String>> = BTreeMap::new();
    let mut current: Option<&'static str> = None;
    for line in text.lines() {
        if let Some(title) = line.strip_prefix("### ") {
            current = CLASSES
                .iter()
                .find(|(_, t)| title.starts_with(t))
                .map(|(k, _)| *k);
        } else if let (Some(class), Some(row)) = (current, line.strip_prefix("- ")) {
            out.entry(class).or_default().push(row.to_string());
        }
    }
    out
}

/// `key [broker]` of a topic row (`` `key` [broker] ``).
fn topic_of(row: &str) -> String {
    let key = row
        .strip_prefix('`')
        .and_then(|r| r.split_once('`'))
        .expect("backticked key");
    let broker = key
        .1
        .trim_start()
        .strip_prefix('[')
        .and_then(|r| r.split_once(']'))
        .expect("broker");
    format!("{} [{}]", key.0, broker.0)
}

fn count_in_table(text: &str, class: &str) -> usize {
    text.lines()
        .find_map(|l| {
            let l = l.strip_prefix("| ")?;
            let (name, n) = l.split_once(" | ")?;
            (name == class).then(|| n.trim_end_matches(" |").trim().parse().expect("count"))
        })
        .unwrap_or_else(|| panic!("no `{class}` row in the table:\n{text}"))
}

#[derive(Deserialize)]
struct Golden {
    corpora: Vec<Corpus>,
}

#[derive(Deserialize)]
struct Unresolved {
    topic: String,
    missing: String,
    reason: String,
}

#[derive(Deserialize, Default)]
struct KnownFp {
    #[serde(default)]
    consumed_never_published: Vec<String>,
}

#[derive(Deserialize)]
struct Corpus {
    corpus: String,
    root: String,
    published_never_consumed: Vec<String>,
    consumed_never_published: Vec<String>,
    unresolved: Vec<Unresolved>,
    cycles: Vec<Vec<String>>,
    fan_out_topics: Vec<String>,
    fan_out_producers: Vec<String>,
    test_only_consumers: Vec<String>,
    #[serde(default)]
    known_false_positives: KnownFp,
    #[serde(default)]
    must_not_flag: Vec<String>,
}

fn golden() -> Golden {
    let text = std::fs::read_to_string(repo("tests/golden/events/anomalies.report.yaml"))
        .expect("golden file");
    serde_yaml::from_str(&text).expect("parse golden")
}

fn sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v
}

fn unresolved_row(row: &str) -> String {
    let topic = topic_of(row);
    let missing = if row.contains("consumer(s) at") {
        "producer"
    } else {
        "consumer"
    };
    let reason = if row.contains("placeholder") {
        "placeholder"
    } else if row.contains("wildcard") {
        "wildcard"
    } else {
        assert!(row.contains("dead-letter"), "{row}");
        "dead_letter"
    };
    format!("{topic} | {missing} | {reason}")
}

/// A cycle row as its sorted distinct topics.
fn cycle_topics(row: &str) -> Vec<String> {
    let chain = row.split(" (loop of").next().unwrap_or(row);
    let chain = chain.rsplit_once(' ').map_or(chain, |(c, _)| c); // drop the confidence word
    let set: BTreeSet<String> = chain.split(" -> ").map(topic_of).collect();
    set.into_iter().collect()
}

#[tokio::test]
async fn every_class_matches_the_hand_annotated_truth() {
    for c in golden().corpora {
        let state = state_in(&repo(&c.root), None);
        let text = report(&state, json!({ "limit": 100 })).await;
        let got = sections(&text);
        let rows = |class: &str| got.get(class).cloned().unwrap_or_default();
        let ctx = |class: &str| format!("{} / {class}:\n{text}", c.corpus);

        let mut consumed = c.consumed_never_published.clone();
        consumed.extend(c.known_false_positives.consumed_never_published.clone());
        for (class, want) in [
            (
                "published_never_consumed",
                c.published_never_consumed.clone(),
            ),
            ("consumed_never_published", consumed),
            ("test_only_consumers", c.test_only_consumers.clone()),
            ("fan_out_topics", c.fan_out_topics.clone()),
        ] {
            let have: Vec<String> = rows(class).iter().map(|r| topic_of(r)).collect();
            assert_eq!(sorted(have), sorted(want), "{}", ctx(class));
        }
        let want_unresolved: Vec<String> = c
            .unresolved
            .iter()
            .map(|u| format!("{} | {} | {}", u.topic, u.missing, u.reason))
            .collect();
        let have_unresolved: Vec<String> = rows("unresolved")
            .iter()
            .map(|r| unresolved_row(r))
            .collect();
        assert_eq!(
            sorted(have_unresolved),
            sorted(want_unresolved),
            "{}",
            ctx("unresolved")
        );

        let have_cycles: Vec<Vec<String>> =
            rows("cycles").iter().map(|r| cycle_topics(r)).collect();
        let mut want_cycles: Vec<Vec<String>> =
            c.cycles.iter().map(|c| sorted(c.clone())).collect();
        want_cycles.sort();
        let mut have_cycles = have_cycles;
        have_cycles.sort();
        assert_eq!(have_cycles, want_cycles, "{}", ctx("cycles"));

        let have_files: Vec<String> = rows("fan_out_producers")
            .iter()
            .map(|r| {
                r.strip_prefix('`')
                    .and_then(|r| r.split_once('`'))
                    .expect("file")
                    .0
                    .to_string()
            })
            .collect();
        assert_eq!(
            sorted(have_files),
            sorted(c.fan_out_producers.clone()),
            "{}",
            ctx("fan_out_producers")
        );

        // The decoys are the subject of no row, in any class.
        let subjects: BTreeSet<String> = CLASSES
            .iter()
            .filter(|(k, _)| !matches!(*k, "cycles" | "fan_out_producers"))
            .flat_map(|(k, _)| rows(k))
            .map(|r| {
                topic_of(&r)
                    .rsplit_once(" [")
                    .expect("broker")
                    .0
                    .to_string()
            })
            .chain(
                rows("cycles")
                    .iter()
                    .flat_map(|r| cycle_topics(r))
                    .map(|t| t.rsplit_once(" [").expect("broker").0.to_string()),
            )
            .collect();
        for decoy in &c.must_not_flag {
            assert!(
                !subjects.contains(decoy),
                "{}: decoy {decoy} flagged:\n{text}",
                c.corpus
            );
        }
    }
}

#[tokio::test]
async fn the_table_counts_every_class_even_when_one_is_shown() {
    let state = state_in(&repo("examples/eda/anomalies"), None);
    let all = report(&state, json!({})).await;
    let cycles = report(&state, json!({ "classes": ["cycles"] })).await;
    assert!(cycles.contains("### Cycles") && !cycles.contains("### Published"));
    assert!(!cycles.contains("### Fan-out") && !cycles.contains("### Unresolved"));
    for class in [
        "published, never consumed",
        "cycles",
        "fan-out",
        "test-only consumers",
    ] {
        assert_eq!(
            count_in_table(&cycles, class),
            count_in_table(&all, class),
            "{class}"
        );
    }
}

#[tokio::test]
async fn rows_page_with_offset_and_the_footer_names_the_next_page() {
    let state = state_in(&repo("examples/eda/anomalies"), None);
    let full = sections(&report(&state, json!({ "classes": ["unresolved"] })).await);
    let all = full["unresolved"].clone();
    assert!(all.len() >= 4);
    let first = report(&state, json!({ "classes": ["unresolved"], "limit": 2 })).await;
    assert!(first.contains("Next page: `offset: 2`"), "{first}");
    let second = report(
        &state,
        json!({ "classes": ["unresolved"], "limit": 2, "offset": 2 }),
    )
    .await;
    let mut paged = sections(&first)["unresolved"].clone();
    paged.extend(sections(&second)["unresolved"].clone());
    assert_eq!(paged, all[..4].to_vec());
    let past = report(&state, json!({ "classes": ["unresolved"], "offset": 99 })).await;
    assert!(past.contains("No row at `offset: 99`"), "{past}");
}

#[tokio::test]
async fn a_scope_narrows_the_rows_and_the_jail_refuses_an_escape() {
    let root = repo("examples/eda/anomalies");
    let state = state_in(&root, None);
    let tests_dir = root.join("src/__tests__").to_string_lossy().into_owned();
    let scoped = report(&state, json!({ "scope": tests_dir })).await;
    let got = sections(&scoped);
    assert_eq!(got.len(), 1, "{scoped}");
    assert_eq!(
        topic_of(&got["test_only_consumers"][0]),
        "audit.created [kafka]"
    );
    assert!(scoped.contains(&format!("rows shown for `{tests_dir}`")));
    // A producer outside the scope still counts: `payments` stays published-never-consumed.
    let split_file = root.join("src/split.ts").to_string_lossy().into_owned();
    let split = report(&state, json!({ "scope": split_file })).await;
    assert_eq!(
        topic_of(&sections(&split)["published_never_consumed"][0]),
        "payments [kafka]"
    );

    for bad in ["../..", "/etc", "src/../../../etc"] {
        let res = raw(&state, json!({ "scope": bad })).await;
        assert_eq!(res["isError"], json!(true), "{bad}: {res}");
    }
    let res = raw(&state, json!({ "scope": "src", "unknown": 1 })).await;
    assert_eq!(res["isError"], json!(true), "deny_unknown_fields: {res}");
}

#[tokio::test]
async fn the_report_is_the_same_on_every_run_and_with_a_cold_or_warm_cache() {
    for rel in [
        "examples/eda/anomalies",
        "examples/unresolved-flow",
        "examples/eda/multi-broker",
    ] {
        let dir = repo(rel);
        let args = json!({ "limit": 100 });
        let plain = report(&state_in(&dir, None), args.clone()).await;
        assert_eq!(
            plain,
            report(&state_in(&dir, None), args.clone()).await,
            "{rel}: run 2"
        );
        let tmp = tempfile::tempdir().expect("tempdir");
        let cache =
            PersistentIndexCache::open(tmp.path().join("index-cache.db"), u64::MAX).expect("cache");
        for pass in ["cold", "warm"] {
            let got = report(&state_in(&dir, Some(&cache)), args.clone()).await;
            assert_eq!(got, plain, "{rel}: {pass} cache");
        }
    }
}

/// A workspace of `files` written under a temp dir (a one-root config).
fn workspace(files: &[(&str, String)]) -> (tempfile::TempDir, Arc<AppState>) {
    let tmp = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        tmp.path().join("mesh-mcp.toml"),
        "[workspace]\nname = \"w\"\nversion = \"1.0.0\"\nroots = [\".\"]\n\n[engines.contracts]\nenabled = true\n",
    )
    .expect("config");
    for (name, body) in files {
        let path = tmp.path().join(name);
        std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
        std::fs::write(path, body).expect("write");
    }
    let state = state_in(tmp.path(), None);
    (tmp, state)
}

const KAFKA: &str = "import { Kafka } from 'kafkajs';\nconst kafka = new Kafka({ brokers: ['b:9092'] });\nconst producer = kafka.producer();\nconst consumer = kafka.consumer({ groupId: 'g' });\n";

#[tokio::test]
async fn a_380_kb_file_with_thousands_of_topics_stays_bounded_and_stable() {
    let mut body = String::from(KAFKA);
    body.push_str("export async function emitAll(): Promise<void> {\n");
    let mut i = 0;
    while body.len() < 380 * 1024 - 100 {
        body.push_str(&format!(
            "  await producer.send({{ topic: 'bulk.t{i:05}', messages: [{{ value: 'x' }}] }});\n"
        ));
        i += 1;
    }
    body.push_str("}\n");
    assert!(body.len() <= 380 * 1024, "{}", body.len());
    let (_tmp, state) = workspace(&[("src/bulk.ts", body)]);
    let started = std::time::Instant::now();
    let a = report(&state, json!({})).await;
    assert!(started.elapsed().as_secs() < 30, "{:?}", started.elapsed());
    assert_eq!(a, report(&state, json!({})).await);
    assert!(a.len() <= 48 * 1024, "{} bytes", a.len());
    // Thousands of topics were read, the page shows a bounded number of them and says how to
    // see the rest, and the file is named once as a fan-out producer.
    assert!(
        count_in_table(&a, "published, never consumed") > 1_000,
        "{a}"
    );
    assert!(a.contains("Next page: `offset: 25`"));
    let rows = &sections(&a)["published_never_consumed"];
    assert_eq!(rows.len(), 25);
    // Digit-heavy names print as written: 25 distinct rows, not 25 copies of a redaction.
    let topics: BTreeSet<String> = rows.iter().map(|r| topic_of(r)).collect();
    assert_eq!(topics.len(), 25, "{rows:?}");
    assert!(
        topics.iter().all(|t| t.starts_with("bulk.t0")),
        "{topics:?}"
    );
    assert!(!a.contains("REDACTED"), "{a}");
    assert_eq!(sections(&a)["fan_out_producers"].len(), 1);
}

/// `n` files in a ring: file `i` subscribes to `ring.t<i>` and calls `pub<i+1>` of the next
/// file, which publishes `ring.t<i+1>`.
fn ring_workspace(n: usize) -> (tempfile::TempDir, Arc<AppState>) {
    let files: Vec<(String, String)> = (0..n)
        .map(|i| {
            let next = (i + 1) % n;
            let body = format!(
                "import {{ pub{next} }} from './hop{next}';\n{KAFKA}export async function hop{i}(): Promise<void> {{\n  await consumer.subscribe({{ topic: 'ring.t{i}' }});\n  await pub{next}();\n}}\nexport async function pub{i}(): Promise<void> {{\n  await producer.send({{ topic: 'ring.t{i}', messages: [] }});\n}}\n"
            );
            (format!("src/hop{i}.ts"), body)
        })
        .collect();
    let refs: Vec<(&str, String)> = files.iter().map(|(p, b)| (p.as_str(), b.clone())).collect();
    workspace(&refs)
}

#[tokio::test]
async fn a_ring_of_files_is_one_bounded_cycle_row_at_any_size() {
    for n in [40, 300, 3_000] {
        let (_tmp, state) = ring_workspace(n);
        let text = report(&state, json!({})).await;
        assert!(text.len() <= 48 * 1024, "{n}: {} bytes", text.len());
        assert_eq!(count_in_table(&text, "cycles"), 1, "{n}");
        // The row is there (a row too long for the page used to be dropped, with a next-page
        // pointer that did not move), bounded, and says what it left out.
        let rows = sections(&text);
        assert_eq!(rows["cycles"].len(), 1, "{n}:\n{text}");
        assert!(!text.contains("Next page"), "{n}:\n{text}");
        assert!(
            rows["cycles"][0].contains(&format!("+{} more topics", n - 6)),
            "{n}"
        );
        assert!(text.contains(&format!("+{} more hops", n - 4)), "{n}");
        assert!(
            rows["cycles"][0].len() < 1_600,
            "{n}: {}",
            rows["cycles"][0].len()
        );
    }
}

/// Credential-shaped values are built here, never written as literals: a key-shaped literal in
/// the source is rejected by push protection.
fn canaries() -> [String; 4] {
    [
        format!("{}{}", "sk_live_", "4eC39HqLyjWDarjtT1zdp7dc"),
        format!("{}{}{}", "ghp_", "R8c1qZ0xK3mP7vB2nT5yL9wD4sF6hJ0aG1eX", ""),
        format!(
            "{}://{}:{}@{}/db",
            "postgres", "svc", "hunter22secret", "db.internal"
        ),
        format!("{}{}", "AKIA", "IOSFODNN7EXAMPLE"),
    ]
}

#[tokio::test]
async fn credential_shaped_names_and_defaults_never_reach_the_output() {
    let [token, ghp, url, aws] = canaries();
    let src = format!(
        "{KAFKA}\
export async function leak(): Promise<void> {{\n\
  await producer.send({{ topic: '{token}', messages: [] }});\n\
  await producer.send({{ topic: '{ghp}', messages: [] }});\n\
  await producer.send({{ topic: process.env.AUDIT_TOPIC ?? '{url}', messages: [] }});\n\
  await consumer.subscribe({{ topic: process.env.INBOX_TOPIC || '{aws}' }});\n\
  await producer.send({{ topic: 'orders.created', messages: [] }});\n\
}}\n"
    );
    let (_tmp, state) = workspace(&[("src/leak.ts", src)]);
    let text = report(&state, json!({ "limit": 100 })).await;
    for secret in [
        &token,
        &ghp,
        &url,
        &aws,
        &"hunter22secret".to_string(),
        &"sk_live_".to_string(),
    ] {
        assert!(!text.contains(secret.as_str()), "{secret} leaked:\n{text}");
    }
    // The legitimate topic next to them is reported, and a literal topic that is a token is shown
    // as the redaction marker (the graph keeps it; the report must not print it).
    assert!(text.contains("`orders.created` [kafka]"), "{text}");
    assert!(
        text.contains("[REDACTED_SECRET: USE_ENV_OR_LOCAL_FALLBACK]"),
        "{text}"
    );
}

#[tokio::test]
async fn a_decorator_only_python_task_is_heuristic_and_the_footer_says_why() {
    // A Locust load test: `@task` is read as a Celery consumer, with no broker.
    let locust = "from locust import HttpUser, task\n\nclass Shopper(HttpUser):\n    @task(1)\n    def browse_product(self):\n        self.client.get('/')\n".to_string();
    let (_tmp, state) = workspace(&[("locustfile.py", locust)]);
    let text = report(&state, json!({})).await;
    let rows = sections(&text);
    let row = &rows["consumed_never_published"][0];
    assert!(
        row.starts_with("`browse_product` [broker unknown] heuristic"),
        "{text}"
    );
    assert!(!text.contains("exact"), "{text}");
    assert!(
        text.contains("1 row(s) rest on sites whose broker is unknown"),
        "{text}"
    );
    assert!(text.contains("verify each with `rg`"), "{text}");
    // A report with only known brokers makes no such claim.
    let state = state_in(&repo("examples/eda/anomalies"), None);
    let clean = report(&state, json!({})).await;
    assert!(!clean.contains("rest on sites whose broker"), "{clean}");
}

#[tokio::test]
async fn go_testdata_and_pytest_conftest_are_test_files() {
    let prod = format!("{KAFKA}export async function emit(): Promise<void> {{\n  await producer.send({{ topic: 'catalog.sync', messages: [] }});\n}}\n");
    let listener = format!("{KAFKA}export async function listen(): Promise<void> {{\n  await consumer.subscribe({{ topic: 'catalog.sync' }});\n}}\n");
    let conftest = "from celery import Celery\napp = Celery()\n\n@app.task\ndef conftest_only_task():\n    pass\n".to_string();
    let (_tmp, state) = workspace(&[
        ("src/prod.ts", prod),
        ("pkg/testdata/listener.ts", listener),
        ("svc/conftest.py", conftest),
    ]);
    let text = report(&state, json!({})).await;
    let rows = sections(&text);
    assert_eq!(
        topic_of(&rows["test_only_consumers"][0]),
        "catalog.sync [kafka]",
        "{text}"
    );
    // A test fixture's task is no production consumer: no "consumed, never published" row.
    assert!(!text.contains("conftest_only_task"), "{text}");
    assert!(!rows.contains_key("consumed_never_published"), "{text}");
}

#[tokio::test]
async fn the_tool_is_advertised_with_its_thresholds_and_negative_constraints() {
    let tools = ToolRegistry::list_tools();
    let tool = tools
        .as_array()
        .expect("array")
        .iter()
        .find(|t| t["name"] == "event_anomalies")
        .expect("advertised");
    let description = tool["description"].as_str().expect("description");
    assert!(description.contains(&format!(
        "{} or more consumer sites",
        mesh_core::ANOMALY_FAN_OUT_CONSUMERS
    )));
    assert!(description.contains(&format!(
        "onto {} or more topics",
        mesh_core::ANOMALY_FAN_OUT_TOPICS
    )));
    assert!(description.contains("DO NOT USE") && description.contains("does NOT"));
    assert!(description.contains("STATICALLY RESOLVED"));
    assert_eq!(tool["inputSchema"]["additionalProperties"], json!(false));
}

#[test]
fn the_new_sources_never_write_to_stdout() {
    for rel in [
        "crates/mesh-core/src/contracts/anomalies.rs",
        "crates/mesh-parsers/src/anomalies.rs",
        "crates/mesh-server/src/tools/event_anomalies.rs",
    ] {
        let text = std::fs::read_to_string(repo(rel)).expect("source");
        for (n, line) in text.lines().enumerate() {
            let code = line.split("//").next().unwrap_or("");
            for banned in [
                "println!",
                "print!(",
                "dbg!",
                "eprintln!",
                "todo!",
                "unimplemented!",
            ] {
                assert!(!code.contains(banned), "{rel}:{}: {banned}", n + 1);
            }
        }
    }
}
