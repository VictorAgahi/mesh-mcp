//! Event anomaly report (`anomalies`): one test per rule (each fails when its rule is
//! switched off), hostile graphs (rings of thousands of topics, thousands of one-sided topics)
//! on a 2 MB stack, determinism.

use super::*;
use std::sync::Arc;

fn node(name: &str, kind: NodeKind, file: &str, line: usize) -> ContractNode {
    ContractNode {
        id: 0,
        name: CompactStr::new(name),
        kind,
        file_path: Arc::from(Path::new(file)),
        line_start: line,
        line_end: line,
        package: CompactStr::new("app"),
        repo_id: 0,
        signature: None,
        docstring: None,
    }
}

/// A producer or consumer site at `file:line` (a fresh node per call).
fn site(g: &mut ContractGraph, file: &str, line: usize) -> NodeId {
    let name = format!("site{}", g.node_count());
    g.add_node(node(&name, NodeKind::ServiceClass, file, line))
}

fn produce(g: &mut ContractGraph, file: &str, line: usize, topic: &str, broker: Option<Broker>) {
    let id = site(g, file, line);
    g.add_producer_on(id, topic, broker);
}

fn consume(g: &mut ContractGraph, file: &str, line: usize, topic: &str, broker: Option<Broker>) {
    let id = site(g, file, line);
    g.add_consumer_on(id, topic, broker);
}

fn is_test(n: &ContractNode) -> bool {
    n.file_path.to_string_lossy().contains(".spec.")
}

fn report(g: &ContractGraph) -> EventAnomalies<'_> {
    g.event_anomalies(&is_test, &|_| true)
}

/// `key [broker]` of each row, in report order.
fn keys(rows: &[TopicEntry<'_>]) -> Vec<String> {
    rows.iter()
        .map(|e| format!("{} [{}]", e.key, e.broker.map_or("?", Broker::slug)))
        .collect()
}

fn unresolved(r: &EventAnomalies<'_>) -> Vec<(String, Missing, UnresolvedReason)> {
    r.unresolved
        .iter()
        .map(|u| (u.topic.key.to_string(), u.missing, u.reason))
        .collect()
}

#[test]
fn one_sided_topics_land_in_their_class() {
    let mut g = ContractGraph::new();
    let k = Some(Broker::Kafka);
    produce(&mut g, "/w/a.ts", 1, "only.out", k);
    consume(&mut g, "/w/b.ts", 1, "only.in", k);
    produce(&mut g, "/w/c.ts", 1, "both", k);
    consume(&mut g, "/w/d.ts", 1, "both", k);
    g.reconcile_edges();
    let r = report(&g);
    assert_eq!(keys(&r.published_never_consumed), ["only.out [kafka]"]);
    assert_eq!(keys(&r.consumed_never_published), ["only.in [kafka]"]);
    assert!(r.unresolved.is_empty() && r.cycles.is_empty() && r.test_only_consumers.is_empty());
}

#[test]
fn a_topic_is_never_joined_across_brokers() {
    let mut g = ContractGraph::new();
    produce(&mut g, "/w/a.ts", 1, "payments", Some(Broker::Kafka));
    consume(&mut g, "/w/b.ts", 1, "payments", Some(Broker::RabbitMq));
    g.reconcile_edges();
    let r = report(&g);
    assert_eq!(keys(&r.published_never_consumed), ["payments [kafka]"]);
    assert_eq!(keys(&r.consumed_never_published), ["payments [rabbitmq]"]);
}

#[test]
fn test_files_are_not_production_sites() {
    let mut g = ContractGraph::new();
    let k = Some(Broker::Kafka);
    // Consumed only by a test: a test-only consumer, and NOT "published, never consumed".
    produce(&mut g, "/w/a.ts", 1, "audit", k);
    consume(&mut g, "/w/a.spec.ts", 1, "audit", k);
    // Published only by a test: consumed, never published (the test producer is named).
    produce(&mut g, "/w/b.spec.ts", 1, "inbox", k);
    consume(&mut g, "/w/b.ts", 1, "inbox", k);
    // Both ends are tests: scaffolding, reported nowhere.
    produce(&mut g, "/w/c.spec.ts", 1, "scaffold", k);
    consume(&mut g, "/w/c.spec.ts", 2, "scaffold", k);
    // Consumed in production and in a test: fine.
    produce(&mut g, "/w/d.ts", 1, "mixed", k);
    consume(&mut g, "/w/d.ts", 2, "mixed", k);
    consume(&mut g, "/w/d.spec.ts", 1, "mixed", k);
    g.reconcile_edges();
    let r = report(&g);
    assert_eq!(keys(&r.test_only_consumers), ["audit [kafka]"]);
    assert_eq!(keys(&r.consumed_never_published), ["inbox [kafka]"]);
    assert!(r.consumed_never_published[0].producers.iter().all(|p| p.test));
    assert!(r.published_never_consumed.is_empty());
    assert!(r.unresolved.is_empty() && r.fan_out_topics.is_empty());
}

#[test]
fn placeholders_wildcards_and_dead_letters_are_unresolved_not_anomalies() {
    let mut g = ContractGraph::new();
    let n = Some(Broker::Nats);
    produce(&mut g, "/w/a.ts", 1, "${out_topic}", Some(Broker::Kafka));
    consume(&mut g, "/w/b.ts", 1, "${in_topic}", Some(Broker::Kafka));
    // A placeholder with both ends is nobody's anomaly.
    produce(&mut g, "/w/c.ts", 1, "${shared}", Some(Broker::Kafka));
    consume(&mut g, "/w/d.ts", 1, "${shared}", Some(Broker::Kafka));
    produce(&mut g, "/w/e.ts", 1, "orders.dlq", Some(Broker::Kafka));
    consume(&mut g, "/w/f.ts", 1, "billing-retry-2", Some(Broker::Kafka));
    consume(&mut g, "/w/g.ts", 1, "ghosts.*", n);
    // A wildcard that hears a concrete subject: neither is reported.
    produce(&mut g, "/w/h.ts", 1, "sensors.temp", n);
    consume(&mut g, "/w/i.ts", 1, "sensors.*", n);
    g.reconcile_edges();
    let r = report(&g);
    assert_eq!(
        unresolved(&r),
        [
            (
                "${in_topic}".to_string(),
                Missing::Producer,
                UnresolvedReason::Placeholder
            ),
            (
                "${out_topic}".to_string(),
                Missing::Consumer,
                UnresolvedReason::Placeholder
            ),
            (
                "billing-retry-2".to_string(),
                Missing::Producer,
                UnresolvedReason::DeadLetter
            ),
            (
                "ghosts.*".to_string(),
                Missing::Producer,
                UnresolvedReason::Wildcard
            ),
            (
                "orders.dlq".to_string(),
                Missing::Consumer,
                UnresolvedReason::DeadLetter
            ),
        ]
    );
    assert!(r.published_never_consumed.is_empty(), "{:?}", keys(&r.published_never_consumed));
    assert!(r.consumed_never_published.is_empty());
    assert_eq!(r.placeholder_topics, 3);
}

#[test]
fn a_heuristic_site_makes_the_row_heuristic() {
    let mut g = ContractGraph::new();
    let id = site(&mut g, "/w/a.ts", 1);
    g.add_producer_on(id, "t", Some(Broker::Kafka));
    g.mark_heuristic_topic_node(id);
    g.reconcile_edges();
    let r = report(&g);
    assert_eq!(r.published_never_consumed[0].confidence, EdgeConfidence::Heuristic);
}

#[test]
fn fan_out_starts_at_the_thresholds() {
    let mut g = ContractGraph::new();
    let k = Some(Broker::Kafka);
    produce(&mut g, "/w/p.ts", 1, "narrow", k);
    produce(&mut g, "/w/p.ts", 2, "wide", k);
    for i in 0..ANOMALY_FAN_OUT_CONSUMERS - 1 {
        consume(&mut g, "/w/c.ts", i + 1, "narrow", k);
    }
    for i in 0..ANOMALY_FAN_OUT_CONSUMERS {
        consume(&mut g, "/w/c.ts", 100 + i, "wide", k);
    }
    // A file publishing onto exactly the threshold of topics, and one onto one fewer.
    for i in 0..ANOMALY_FAN_OUT_TOPICS {
        produce(&mut g, "/w/hub.ts", i + 1, &format!("hub.{i}"), k);
    }
    for i in 0..ANOMALY_FAN_OUT_TOPICS - 1 {
        produce(&mut g, "/w/small.ts", i + 1, &format!("small.{i}"), k);
    }
    g.reconcile_edges();
    let r = report(&g);
    assert_eq!(keys(&r.fan_out_topics), ["wide [kafka]"]);
    let files: Vec<_> = r
        .fan_out_producers
        .iter()
        .map(|p| p.file.to_string_lossy().into_owned())
        .collect();
    assert_eq!(files, ["/w/hub.ts"]);
}

#[test]
fn a_scope_keeps_rows_with_a_site_in_it_but_still_counts_every_site() {
    let mut g = ContractGraph::new();
    let k = Some(Broker::Kafka);
    produce(&mut g, "/w/a/p.ts", 1, "t", k);
    consume(&mut g, "/w/b/c.ts", 1, "t", k);
    produce(&mut g, "/w/a/q.ts", 1, "lonely", k);
    g.reconcile_edges();
    let in_a = |n: &ContractNode| n.file_path.starts_with("/w/a");
    let r = g.event_anomalies(&is_test, &in_a);
    // `t` has a consumer in /w/b: still consumed, whatever the scope.
    assert_eq!(keys(&r.published_never_consumed), ["lonely [kafka]"]);
    let in_b = |n: &ContractNode| n.file_path.starts_with("/w/b");
    assert!(g.event_anomalies(&is_test, &in_b).published_never_consumed.is_empty());
}

/// `n` files `rI.ts`: a function that consumes `t<I>` and publishes `t<I+1>` (the last one
/// publishes `t0` when `close`).
fn ring(n: usize, close: bool) -> ContractGraph {
    let mut g = ContractGraph::new();
    for i in 0..n {
        let path = format!("/w/r{i}.ts");
        g.add_call_facts(
            Arc::from(Path::new(&path)),
            0,
            CompactStr::new("app"),
            CallFacts {
                callables: vec![CallableFact {
                    name: CompactStr::new(format!("f{i}")),
                    owner: None,
                    line_start: 1,
                    line_end: 20,
                }],
                ..CallFacts::default()
            },
        );
        g.add_node(node("decl", NodeKind::ServiceClass, &path, 1));
        let c = g.add_node(node(&format!("c{i}"), NodeKind::KafkaTopic, &path, 2));
        g.add_consumer(c, &format!("t{i}"));
        if close || i + 1 < n {
            let p = g.add_node(node(&format!("p{i}"), NodeKind::KafkaTopic, &path, 3));
            g.add_producer(p, &format!("t{}", (i + 1) % n));
        }
    }
    g.reconcile_edges();
    g
}

#[test]
fn a_cycle_needs_a_way_back() {
    let closed = ring(5, true);
    let r = report(&closed);
    assert_eq!(r.cycles.len(), 1);
    assert_eq!(r.cycles[0].members.len(), 5);
    assert_eq!(r.cycles[0].path.len(), 5);
    assert_eq!(r.cycles[0].confidence, EdgeConfidence::Exact);
    // The same five topics as a line: every topic has both ends but the last, which nobody
    // publishes onto: no cycle.
    let open = ring(5, false);
    assert!(report(&open).cycles.is_empty());
}

#[test]
fn a_retry_is_a_cycle_of_one_topic() {
    let mut g = ContractGraph::new();
    g.add_call_facts(
        Arc::from(Path::new("/w/job.ts")),
        0,
        CompactStr::new("app"),
        CallFacts {
            callables: vec![CallableFact {
                name: CompactStr::new("run"),
                owner: None,
                line_start: 1,
                line_end: 20,
            }],
            ..CallFacts::default()
        },
    );
    g.add_node(node("decl", NodeKind::ServiceClass, "/w/job.ts", 1));
    let c = g.add_node(node("c", NodeKind::KafkaTopic, "/w/job.ts", 2));
    g.add_consumer(c, "jobs");
    let p = g.add_node(node("p", NodeKind::KafkaTopic, "/w/job.ts", 3));
    g.add_producer(p, "jobs");
    g.reconcile_edges();
    let r = report(&g);
    assert_eq!(r.cycles.len(), 1);
    assert_eq!(r.cycles[0].members.len(), 1);
    assert_eq!(r.cycles[0].path.len(), 1);
}

#[test]
fn a_same_file_guess_is_not_a_cycle() {
    // No call facts: the only link between the consumer and the producer is "same file".
    let mut g = ContractGraph::new();
    let k = Some(Broker::Kafka);
    consume(&mut g, "/w/x.ts", 1, "a", k);
    produce(&mut g, "/w/x.ts", 2, "b", k);
    consume(&mut g, "/w/x.ts", 3, "b", k);
    produce(&mut g, "/w/x.ts", 4, "a", k);
    g.reconcile_edges();
    let r = report(&g);
    assert!(r.cycles.is_empty());
    assert!(r.cycle_guess_hops > 0);
}

/// Runs `f` on a thread with a 2 MB stack: a recursive walk over thousands of topics would
/// overflow it.
fn on_small_stack<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    std::thread::Builder::new()
        .stack_size(2 * 1024 * 1024)
        .spawn(f)
        .expect("spawn")
        .join()
        .expect("no stack overflow")
}

#[test]
fn a_ring_of_thousands_of_topics_is_one_cycle_on_a_small_stack() {
    on_small_stack(|| {
        let g = ring(3_000, true);
        let r = report(&g);
        assert_eq!(r.cycles.len(), 1);
        assert_eq!(r.cycles[0].members.len(), 3_000);
        assert_eq!(r.cycles[0].path.len(), 3_000);
        assert!(!r.cycle_search_capped);
        let open = ring(3_000, false);
        assert!(report(&open).cycles.is_empty());
    });
}

#[test]
fn thousands_of_topics_come_out_sorted_and_identical() {
    on_small_stack(|| {
        let mut g = ContractGraph::new();
        // Inserted in a scrambled order: the report must not depend on it.
        for i in (0..5_000).map(|i| (i * 7_919) % 5_000) {
            produce(&mut g, &format!("/w/p{i}.ts"), 1, &format!("t{i:05}"), Some(Broker::Kafka));
        }
        g.reconcile_edges();
        let a = report(&g);
        let b = report(&g);
        assert_eq!(a.published_never_consumed.len(), 5_000);
        let got = keys(&a.published_never_consumed);
        let mut sorted = got.clone();
        sorted.sort();
        assert_eq!(got, sorted);
        assert_eq!(format!("{a:?}"), format!("{b:?}"));
    });
}

#[test]
fn cycle_search_stops_at_the_site_cap_and_says_so() {
    // 4 consumers walked of 5: the cap is a parameter of `topic_hops`, exercised directly.
    let g = ring(5, true);
    let hubs: Vec<NodeId> = g
        .all_nodes()
        .filter(|n| ContractGraph::is_hub_like(n))
        .map(|n| n.id)
        .collect();
    let (_, walked, capped) = g.topic_hops(&hubs, &|_| false, 4);
    assert_eq!((walked, capped), (4, true));
    let (_, walked, capped) = g.topic_hops(&hubs, &|_| false, 5);
    assert_eq!((walked, capped), (5, false));
}

#[test]
fn components_of_a_long_chain_do_not_recurse() {
    on_small_stack(|| {
        let n = 200_000;
        let adj: Vec<Vec<usize>> = (0..n)
            .map(|i| if i + 1 < n { vec![i + 1] } else { vec![] })
            .collect();
        let comps = anomalies::strongly_connected(&adj);
        assert_eq!(comps.len(), n);
        let mut closed = adj.clone();
        closed[n - 1] = vec![0];
        let comps = anomalies::strongly_connected(&closed);
        assert_eq!(comps.len(), 1);
        let all: HashSet<usize> = (0..n).collect();
        assert_eq!(anomalies::shortest_cycle(0, &closed, &all).map(|c| c.len()), Some(n));
    });
}
