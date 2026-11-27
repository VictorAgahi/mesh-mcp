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
    assert!(r.consumed_never_published[0]
        .producers
        .iter()
        .all(|p| p.test));
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
    assert!(
        r.published_never_consumed.is_empty(),
        "{:?}",
        keys(&r.published_never_consumed)
    );
    assert!(r.consumed_never_published.is_empty());
    assert_eq!(r.placeholder_topics, 3);
}

#[test]
fn a_site_of_unknown_broker_is_never_exact() {
    let mut g = ContractGraph::new();
    consume(&mut g, "/w/locustfile.py", 1, "browse_product", None);
    produce(&mut g, "/w/a.ts", 1, "known", Some(Broker::Kafka));
    g.reconcile_edges();
    let r = report(&g);
    assert_eq!(r.consumed_never_published[0].broker, None);
    assert_eq!(
        r.consumed_never_published[0].confidence,
        EdgeConfidence::Heuristic
    );
    assert_eq!(
        r.published_never_consumed[0].confidence,
        EdgeConfidence::Exact
    );
}

#[test]
fn a_producer_of_several_subjects_is_heard_on_each_one_a_wildcard_matches() {
    // One site publishes three subjects; the wildcard hub holds one edge for that site.
    let mut g = ContractGraph::new();
    let n = Some(Broker::Nats);
    let id = site(&mut g, "/w/pub.ts", 1);
    for subject in ["events.created", "events.deleted", "other.thing"] {
        g.add_producer_on(id, subject, n);
    }
    consume(&mut g, "/w/sub.ts", 1, "events.*", n);
    g.reconcile_edges();
    let r = report(&g);
    // `other.thing` matches no wildcard: still a finding. The two `events.` subjects are heard.
    assert_eq!(keys(&r.published_never_consumed), ["other.thing [nats]"]);
    assert!(r.unresolved.is_empty());
    // Another broker's wildcard does not hear them.
    let mut g = ContractGraph::new();
    let id = site(&mut g, "/w/pub.ts", 1);
    g.add_producer_on(id, "events.created", Some(Broker::Redis));
    g.add_producer_on(id, "events.deleted", Some(Broker::Redis));
    consume(&mut g, "/w/sub.ts", 1, "events.*", n);
    g.reconcile_edges();
    assert_eq!(
        keys(&report(&g).published_never_consumed),
        ["events.created [redis]", "events.deleted [redis]"]
    );
}

#[test]
fn dead_letter_names_need_the_whole_token() {
    for yes in [
        "orders.dlq",
        "orders-DLT",
        "x_retry_3",
        "orders.dead-letter",
        "a.dead_letter",
        "a.deadletter",
    ] {
        assert!(anomalies::is_dead_letter(yes), "{yes}");
    }
    for no in [
        "mail.letter",
        "mail.dead",
        "retry.policy",
        "dlqueue",
        "letter",
        "orders",
    ] {
        assert!(!anomalies::is_dead_letter(no), "{no}");
    }
}

#[test]
fn a_heuristic_site_makes_the_row_heuristic() {
    let mut g = ContractGraph::new();
    let id = site(&mut g, "/w/a.ts", 1);
    g.add_producer_on(id, "t", Some(Broker::Kafka));
    g.mark_heuristic_topic_node(id);
    g.reconcile_edges();
    let r = report(&g);
    assert_eq!(
        r.published_never_consumed[0].confidence,
        EdgeConfidence::Heuristic
    );
}

#[test]
fn fan_out_starts_at_the_thresholds() {
    let mut g = ContractGraph::new();
    let k = Some(Broker::Kafka);
    produce(&mut g, "/w/p.ts", 1, "narrow", k);
    produce(&mut g, "/w/p.ts", 2, "wide", k);
    // Literal counts on purpose: the documented thresholds are 6 and 6, and changing the
    // constants must fail this test.
    for i in 0..5 {
        consume(&mut g, "/w/c.ts", i + 1, "narrow", k);
    }
    for i in 0..6 {
        consume(&mut g, "/w/c.ts", 100 + i, "wide", k);
    }
    // A file publishing onto exactly the threshold of topics, and one onto one fewer.
    for i in 0..6 {
        produce(&mut g, "/w/hub.ts", i + 1, &format!("hub.{i}"), k);
    }
    for i in 0..5 {
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
    assert!(g
        .event_anomalies(&is_test, &in_b)
        .published_never_consumed
        .is_empty());
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
    // The ring's sites have no known broker: the loop is never `exact`.
    assert_eq!(r.cycles[0].confidence, EdgeConfidence::Heuristic);
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

#[test]
fn placeholder_topics_are_never_a_cycle_or_a_fan_out() {
    // A ring of `${key}` topics, and one placeholder with more consumers than the threshold.
    let mut g = ContractGraph::new();
    for i in 0..3 {
        let path = format!("/w/ph{i}.ts");
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
        g.add_consumer(c, &format!("${{t{i}}}"));
        let p = g.add_node(node(&format!("p{i}"), NodeKind::KafkaTopic, &path, 3));
        g.add_producer(p, &format!("${{t{}}}", (i + 1) % 3));
    }
    produce(&mut g, "/w/w.ts", 1, "${wide}", None);
    for i in 0..8 {
        consume(&mut g, "/w/w.ts", 10 + i, "${wide}", None);
    }
    g.reconcile_edges();
    let r = report(&g);
    assert!(r.cycles.is_empty(), "{:?}", r.cycles.len());
    assert!(r.fan_out_topics.is_empty());
    assert_eq!(r.placeholder_topics, 4);
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
        assert!(r.cycle_first_unsearched.is_none());
        let open = ring(3_000, false);
        assert!(report(&open).cycles.is_empty());
    });
}

#[test]
fn rows_follow_the_key_not_the_order_hubs_were_created_in() {
    let mut g = ContractGraph::new();
    produce(&mut g, "/w/z.ts", 1, "zeta", Some(Broker::Kafka));
    g.reconcile_edges();
    // A later reconcile creates the hub of `alpha` after `zeta`'s: a higher node id.
    produce(&mut g, "/w/a.ts", 1, "alpha", Some(Broker::Kafka));
    g.reconcile_edges();
    let r = report(&g);
    assert_eq!(
        keys(&r.published_never_consumed),
        ["alpha [kafka]", "zeta [kafka]"]
    );
}

#[test]
fn thousands_of_topics_come_out_sorted_and_identical() {
    on_small_stack(|| {
        let mut g = ContractGraph::new();
        // Inserted in a scrambled order: the report must not depend on it.
        for i in (0..5_000).map(|i| (i * 7_919) % 5_000) {
            produce(
                &mut g,
                &format!("/w/p{i}.ts"),
                1,
                &format!("t{i:05}"),
                Some(Broker::Kafka),
            );
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
    let (_, walked, total, first) = g.topic_hops(&hubs, &|_| false, 4);
    assert_eq!((walked, total, first.is_some()), (4, 5, true));
    let (_, walked, total, first) = g.topic_hops(&hubs, &|_| false, 5);
    assert_eq!((walked, total, first.is_some()), (5, 5, false));
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
        assert_eq!(
            anomalies::shortest_cycle(0, &closed, &all).map(|c| c.len()),
            Some(n)
        );
    });
}
