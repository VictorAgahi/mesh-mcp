//! SQL producers and consumers (9.0 T5) through the real dispatch (`PolyglotIndexer`), the graph
//! and the cache record: `.sql` files, and the SQL string literals of TypeScript, Go, Java and Python.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use mesh_core::{ContractGraph, EdgeKind, NodeKind};
use mesh_parsers::{FileIndex, PolyglotIndexer};
use std::path::Path;

fn extract(file: &str, code: &str) -> FileIndex {
    let fi = PolyglotIndexer::extract(Path::new(file), code, 0);
    assert!(!fi.parse_failed, "{file} must parse");
    fi
}

/// `(role, topic, line)` of every producer and consumer, sorted.
fn sites(fi: &FileIndex) -> Vec<(char, String, usize)> {
    let mut out: Vec<(char, String, usize)> = fi
        .producers
        .iter()
        .map(|(i, t)| ('P', t.to_string(), fi.nodes[*i].line_start))
        .chain(
            fi.consumers
                .iter()
                .map(|(i, t)| ('C', t.to_string(), fi.nodes[*i].line_start)),
        )
        .collect();
    out.sort();
    out
}

#[test]
fn a_sql_file_is_classified_by_extension_in_any_case() {
    for name in ["db/001.sql", "db/002.SQL", "db/003.Sql"] {
        let fi = extract(
            name,
            "INSERT INTO outbox (topic, payload) VALUES ('a.b', '{}');\n",
        );
        assert_eq!(sites(&fi), vec![('P', "a.b".to_string(), 1)], "{name}");
        assert_eq!(fi.nodes[0].kind, NodeKind::EventStream);
    }
    // Not a SQL file: no sites from a `.sqlx` extension.
    assert!(extract("db/x.sqlx", "NOTIFY a;\n").nodes.is_empty());
}

#[test]
fn host_languages_read_sql_literals_and_ignore_comments_and_prose() {
    let ts = extract(
        "src/repo.ts",
        "export class R {\n\
           async a() {\n\
             // INSERT INTO outbox (topic) VALUES ('c')\n\
             await db.query(\"INSERT INTO outbox (topic, payload) VALUES ('ts.evt', $1)\", [1]);\n\
             await db.query(\"INSERT INTO outbox (topic, payload) VALUES ($1, $2)\", [t, 1]);\n\
             return 'run INSERT INTO outbox (topic) VALUES (\\'prose\\')';\n\
           }\n\
         }\n",
    );
    assert_eq!(sites(&ts), vec![('P', "ts.evt".to_string(), 4)]);

    let go = extract(
        "stock/stock.go",
        "package stock\n\nfunc A(db DB) {\n\tdb.Exec(\"SELECT pg_notify('go_chan', $1)\")\n\tdb.Exec(\"LISTEN go_in\")\n}\n",
    );
    assert_eq!(
        sites(&go),
        vec![
            ('C', "go_in".to_string(), 5),
            ('P', "go_chan".to_string(), 4)
        ]
    );

    let java = extract(
        "src/main/java/shop/Q.java",
        "package shop;\npublic class Q {\n  void a() {\n    jdbc.update(\"INSERT INTO event_queue (event_type, body) VALUES ('java.evt', ?)\");\n  }\n}\n",
    );
    assert_eq!(sites(&java), vec![('P', "java.evt".to_string(), 4)]);

    let py = extract(
        "app/notifier.py",
        "def run(conn):\n    # LISTEN nope\n    conn.execute(\"LISTEN py_chan\")\n",
    );
    assert_eq!(sites(&py), vec![('C', "py_chan".to_string(), 3)]);
}

#[test]
fn existing_broker_facts_are_unchanged_next_to_a_sql_producer() {
    let fi = extract(
        "src/mixed.ts",
        "import { Kafka } from 'kafkajs';\n\
         const producer = new Kafka({ brokers: [] }).producer();\n\
         export async function a() {\n\
           await producer.send({ topic: 'kafka.evt', messages: [] });\n\
           await db.query(\"INSERT INTO outbox (topic) VALUES ('sql.evt')\");\n\
         }\n",
    );
    assert_eq!(
        sites(&fi),
        vec![
            ('P', "kafka.evt".to_string(), 4),
            ('P', "sql.evt".to_string(), 5)
        ]
    );
}

#[test]
fn a_sql_producer_and_a_listener_meet_on_one_topic_in_the_graph() {
    let mut graph = ContractGraph::new();
    extract(
        "db/001.sql",
        "CREATE FUNCTION f() RETURNS trigger AS $$\nBEGIN\n  PERFORM pg_notify('stock_changed', NEW.sku);\n  RETURN NEW;\nEND;\n$$ LANGUAGE plpgsql;\n",
    )
    .apply(&mut graph);
    extract(
        "app/worker.py",
        "def run(c):\n    c.execute('LISTEN Stock_Changed')\n",
    )
    .apply(&mut graph);
    graph.reconcile_edges();
    let name = |id| {
        graph
            .get_node(id)
            .map(|n| n.name.to_string())
            .unwrap_or_default()
    };
    let produces = graph.all_edges().iter().any(|e| {
        e.kind == EdgeKind::Produces
            && graph
                .get_node(e.from)
                .is_some_and(|n| n.file_path.ends_with("001.sql") && n.line_start == 3)
            && name(e.to) == "stock_changed"
    });
    let consumes = graph.all_edges().iter().any(|e| {
        e.kind == EdgeKind::Consumes
            && name(e.from) == "stock_changed"
            && graph
                .get_node(e.to)
                .is_some_and(|n| n.file_path.ends_with("worker.py"))
    });
    assert!(produces, "001.sql:3 -> stock_changed");
    assert!(consumes, "stock_changed -> worker.py");
}

#[test]
fn extraction_is_deterministic_and_survives_the_cache_round_trip() {
    let code = "INSERT INTO outbox (topic) VALUES ('a'), ('b');\nNOTIFY c;\nLISTEN d;\n";
    let first = extract("db/m.sql", code);
    let again = extract("db/m.sql", code);
    assert_eq!(sites(&first), sites(&again));
    let json = serde_json::to_string(&first).unwrap();
    let back: FileIndex = serde_json::from_str(&json).unwrap();
    assert_eq!(sites(&back), sites(&first));
    assert_eq!(serde_json::to_string(&back).unwrap(), json);
}
