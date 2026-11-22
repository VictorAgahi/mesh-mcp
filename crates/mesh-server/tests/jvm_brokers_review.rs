#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! 9.0 T8a review round 1, through the whole stack (index, graph, tools):
//! - a placeholder default of a JVM broker site is never a channel name, in the graph or in any
//!   tool answer (T6, commandment 5);
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
        out += &text(state, "trace_flow", json!({"entry": t})).await;
        out += &text(state, "smart_search", json!({"query": t})).await;
        out += &text(state, "find_dependents", json!({"target": t})).await;
    }
    out
}

const DEFAULTS: &str = r#"package p;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.beans.factory.annotation.Value;
public class Def {
    @Value("${app.absent2:credpw-DEFPW2}") private String b;
    @Value("${app.present:fallback.topic}") private String c;
    private RabbitTemplate rabbit;
    void m2() { rabbit.convertAndSend(b, "x"); }
    void m3() { rabbit.convertAndSend(c, "x"); }
    @RabbitListener(queues = "${app.absent6:rpw-DEFPW6}") public void r(String m) {}
    @JmsListener(destination = "${app.absent7:jpw-DEFPW7}") public void j(String m) {}
    @RabbitListener(queues = "${app.absent10:hunter2-prod}") public void h(String m) {}
    @RabbitListener(queues = "${app.absent9:P@ssw0rd!}") public void p(String m) {}
}
"#;

const CANARIES: [&str; 6] = [
    "rpw-DEFPW6",
    "jpw-DEFPW7",
    "hunter2-prod",
    "credpw-DEFPW2",
    "P@ssw0rd!",
    "fallback.topic",
];

#[tokio::test]
async fn a_placeholder_default_never_reaches_the_graph_or_a_tool_answer() {
    let tmp = tempfile::tempdir().unwrap();
    let root = dunce::canonicalize(tmp.path()).unwrap();
    write(&root, "src/main/java/p/Def.java", DEFAULTS);
    write(
        &root,
        "src/main/resources/application.yml",
        "app:\n  present: plain.topic.value\n",
    );
    let state = state_for(&root);
    let all = everything(
        &state,
        &[
            "${app.absent6}",
            "${app.absent7}",
            "${app.absent10}",
            "${app.absent2}",
            "plain.topic.value",
        ],
    )
    .await;
    for canary in CANARIES {
        assert!(!all.contains(canary), "{canary} leaked:\n{all}");
    }
    // The key that the configuration resolves is resolved; the others stay visible and counted.
    assert!(all.contains("plain.topic.value"), "{all}");
    let snap = state.snapshot();
    assert!(
        snap.health.config_topics_unresolved >= 4,
        "{:?}",
        snap.health
    );
}

const KAFKA_AND_RABBIT: &str = r#"package p;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
public class Mix {
    private KafkaTemplate<String, String> kafkaTemplate;
    void a() { kafkaTemplate.send("orders", "x"); }
    @RabbitListener(queues = "orders") public void r(String m) {}
}
"#;

const JMS_AND_RABBIT: &str = r#"package p;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
public class MixJms {
    private JmsTemplate jms;
    void a() { jms.convertAndSend("orders", "x"); }
    @RabbitListener(queues = "orders") public void r(String m) {}
}
"#;

const KAFKA_AND_REDIS: &str = r#"package p;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.data.redis.core.StringRedisTemplate;
public class Two {
    private StringRedisTemplate redis;
    void a() { redis.convertAndSend("audit.redis", "x"); }
    @KafkaListener(topics = "audit.redis") public void k(String m) {}
    @KafkaListener(topics = "orders") public void k2(String m) {}
}
"#;

#[tokio::test]
async fn a_same_file_reemit_never_crosses_from_one_broker_to_another() {
    let tmp = tempfile::tempdir().unwrap();
    let root = dunce::canonicalize(tmp.path()).unwrap();
    write(&root, "src/main/java/p/Mix.java", KAFKA_AND_RABBIT);
    write(&root, "src/main/java/p/MixJms.java", JMS_AND_RABBIT);
    write(&root, "src/main/java/p/Two.java", KAFKA_AND_REDIS);
    let state = state_for(&root);
    for tool in ["analyze_impact", "trace_flow"] {
        let args = |t: &str| {
            if tool == "analyze_impact" {
                json!({"target": t, "depth": 3, "limit": 200})
            } else {
                json!({"entry": t})
            }
        };
        // RabbitMQ `orders` is consumed in two files that also produce on Kafka / JMS.
        let rabbit = text(&state, tool, args("rabbitmq:orders")).await;
        assert!(!rabbit.contains("Mix.java:6"), "{tool}: {rabbit}");
        assert!(!rabbit.contains("MixJms.java:6"), "{tool}: {rabbit}");
        assert!(!rabbit.contains("kafka:orders"), "{tool}: {rabbit}");
        assert!(!rabbit.contains("jms:orders"), "{tool}: {rabbit}");
        // A Kafka listener never re-emits to the Redis producer of its own file.
        let kafka = text(&state, tool, args("kafka:audit.redis")).await;
        assert!(!kafka.contains("Two.java:6"), "{tool}: {kafka}");
        assert!(!kafka.contains("redis:audit.redis"), "{tool}: {kafka}");
    }
}
