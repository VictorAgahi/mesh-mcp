//! Chain engine (`flow`): cycles, caps, direction, determinism, stack depth, and
//! agreement with `analyze_impact` past depth 1.

use super::*;
use std::sync::Arc;

/// One generated file: free functions `(name, start, end)`, call sites
/// `(caller index, imported callee, line)` (each callee imported from `./<callee>`),
/// consumers and producers `(line, topic)`.
#[derive(Default, Clone)]
struct F {
    path: String,
    fns: Vec<(String, u32, u32)>,
    calls: Vec<(u32, String, u32)>,
    consumers: Vec<(usize, String)>,
    producers: Vec<(usize, String)>,
}

fn f(name: &str) -> F {
    F {
        path: format!("/w/src/{name}.ts"),
        fns: vec![(name.to_string(), 1, 20)],
        ..F::default()
    }
}

impl F {
    fn calls(mut self, callee: &str, line: u32) -> Self {
        self.calls.push((0, callee.to_string(), line));
        self
    }
    fn consumes(mut self, line: usize, topic: &str) -> Self {
        self.consumers.push((line, topic.to_string()));
        self
    }
    fn produces(mut self, line: usize, topic: &str) -> Self {
        self.producers.push((line, topic.to_string()));
        self
    }
}

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

fn build(files: &[F]) -> ContractGraph {
    let mut g = ContractGraph::new();
    for file in files {
        let facts = CallFacts {
            callables: file
                .fns
                .iter()
                .map(|(n, s, e)| CallableFact {
                    name: CompactStr::new(n),
                    owner: None,
                    line_start: *s,
                    line_end: *e,
                })
                .collect(),
            call_sites: file
                .calls
                .iter()
                .map(|(caller, callee, line)| CallFact {
                    caller: *caller,
                    callee: CompactStr::new(callee),
                    receiver: CallReceiver::ImportedSymbol,
                    line: *line,
                })
                .collect(),
            imports: file
                .calls
                .iter()
                .map(|(_, callee, _)| ImportFact {
                    local: CompactStr::new(callee),
                    exported: CompactStr::new(callee),
                    module: CompactStr::new(format!("./{callee}")),
                })
                .collect(),
            ..CallFacts::default()
        };
        g.add_call_facts(
            Arc::from(Path::new(&file.path)),
            0,
            CompactStr::new("app"),
            facts,
        );
        g.add_node(node("decl", NodeKind::ServiceClass, &file.path, 1));
        for (line, topic) in &file.consumers {
            let id = g.add_node(node(topic, NodeKind::KafkaTopic, &file.path, *line));
            g.add_consumer(id, topic);
        }
        for (line, topic) in &file.producers {
            let id = g.add_node(node(topic, NodeKind::KafkaTopic, &file.path, *line));
            g.add_producer(id, topic);
        }
    }
    g.reconcile_edges();
    g
}

fn trace<'g>(
    g: &'g ContractGraph,
    topic: &'static str,
    direction: FlowDirection,
    bounds: FlowBounds,
) -> FlowTrace<'g> {
    g.trace_flow(FlowEntry::Topic(topic), direction, bounds, &|_| false)
}

fn names(hop: &FlowHop<'_>) -> Vec<String> {
    hop.steps
        .iter()
        .map(|s| match s.kind {
            FlowStepKind::Call => s.node.name.to_string(),
            _ => format!(
                "{:?}:{}",
                s.kind,
                s.topic.as_deref().unwrap_or(s.node.name.as_str())
            ),
        })
        .collect()
}

/// `in` consumed in a, a -> b -> c, c publishes `out`.
fn chain3() -> Vec<F> {
    vec![
        f("a").consumes(2, "in").calls("b", 3),
        f("b").calls("c", 4),
        f("c").produces(5, "out"),
    ]
}

#[test]
fn forward_chain_follows_calls_with_callsite_lines() {
    let g = build(&chain3());
    let t = trace(&g, "in", FlowDirection::Forward, FlowBounds::default());
    assert_eq!(t.hops.len(), 1, "{t:#?}");
    let hop = &t.hops[0];
    assert_eq!(hop.id, "1");
    assert_eq!(names(hop), ["Consume:in", "b", "c", "Produce:out"]);
    assert_eq!(hop.steps[1].call_line, Some(3));
    assert_eq!(hop.steps[2].call_line, Some(4));
    assert!(hop.steps[1].call_file.is_some_and(|p| p.ends_with("a.ts")));
    assert_eq!(hop.confidence, EdgeConfidence::Exact);
    assert!(!hop.fallback);
    assert_eq!(hop.next.len(), 1);
    assert_eq!(hop.next[0].outcome, FlowOutcome::NoCounterpart);
}

#[test]
fn reverse_chain_runs_from_publish_to_entry() {
    let g = build(&chain3());
    let t = trace(&g, "out", FlowDirection::Reverse, FlowBounds::default());
    assert_eq!(t.hops.len(), 1, "{t:#?}");
    assert_eq!(names(&t.hops[0]), ["Produce:out", "b", "a", "Consume:in"]);
    // The caller step carries the call site in the caller's own file.
    let b = &t.hops[0].steps[1];
    assert_eq!(b.call_line, Some(4));
    assert!(b.call_file.is_some_and(|p| p.ends_with("b.ts")));
}

#[test]
fn event_cycle_terminates_and_says_so() {
    // t.a -> ha -> pa publishes t.b -> hb -> pb publishes t.a.
    let files = vec![
        f("ha").consumes(2, "t.a").calls("pa", 3),
        f("pa").produces(4, "t.b"),
        f("hb").consumes(2, "t.b").calls("pb", 3),
        f("pb").produces(4, "t.a"),
    ];
    let g = build(&files);
    let bounds = FlowBounds::clamped(None, Some(MAX_EVENT_DEPTH), None);
    for dir in [FlowDirection::Forward, FlowDirection::Reverse] {
        let t = trace(&g, "t.a", dir, bounds);
        assert_eq!(t.hops.len(), 2, "{dir:?} {t:#?}");
        assert_eq!(t.hops[1].id, "1.1");
        assert_eq!(
            t.hops[1].next[0].outcome,
            FlowOutcome::AlreadyTraced,
            "{dir:?}"
        );
    }
}

#[test]
fn call_cycle_terminates() {
    // a <-> b recursion, b -> c publishes.
    let files = vec![
        f("a").consumes(2, "in").calls("b", 3),
        f("b").calls("a", 4).calls("c", 5),
        f("c").produces(6, "out"),
    ];
    let g = build(&files);
    let t = trace(&g, "in", FlowDirection::Forward, FlowBounds::default());
    assert_eq!(t.hops.len(), 1);
    assert_eq!(names(&t.hops[0]), ["Consume:in", "b", "c", "Produce:out"]);
    let r = trace(&g, "out", FlowDirection::Reverse, FlowBounds::default());
    assert_eq!(names(&r.hops[0]), ["Produce:out", "b", "a", "Consume:in"]);
}

#[test]
fn fan_out_cap_reports_the_rest() {
    let mut hub = f("hub").consumes(2, "in");
    let mut files = Vec::new();
    for k in 0..30 {
        let name = format!("g{k:02}");
        hub = hub.calls(&name, 3);
        files.push(f(&name).produces(4, &format!("out{k:02}")));
    }
    files.push(hub);
    let g = build(&files);
    let t = trace(
        &g,
        "in",
        FlowDirection::Forward,
        FlowBounds::clamped(None, None, Some(20)),
    );
    assert_eq!(t.hops.len(), 20, "{:#?}", t.limits);
    assert!(
        t.limits
            .iter()
            .any(|l| matches!(l, FlowLimit::FanOut(_, 20, 10))),
        "{:?}",
        t.limits
    );
    // The 20 kept are the first in (path, line, name) order.
    assert_eq!(names(&t.hops[0])[1], "g00");
    assert_eq!(names(&t.hops[19])[1], "g19");
}

/// `in` consumed in f0, f0 -> f1 -> ... -> f{n}, f{n} publishes `out`.
fn long_chain(n: usize) -> Vec<F> {
    let mut files: Vec<F> = (0..n)
        .map(|i| f(&format!("f{i}")).calls(&format!("f{}", i + 1), 3))
        .collect();
    files[0] = files[0].clone().consumes(2, "in");
    files.push(f(&format!("f{n}")).produces(4, "out"));
    files
}

#[test]
fn call_depth_cap_stops_and_says_so() {
    let g = build(&long_chain(8));
    let short = trace(
        &g,
        "in",
        FlowDirection::Forward,
        FlowBounds::clamped(Some(6), None, None),
    );
    assert_eq!(short.hops.len(), 1);
    // A bound, not an end: not listed among the chain ends.
    assert_eq!(
        short.hops[0].end,
        Some(FlowEnd::CallDepthLimit),
        "{short:#?}"
    );
    assert!(short
        .limits
        .iter()
        .any(|l| matches!(l, FlowLimit::CallDepth(_))));
    assert!(short.chain_end.is_empty());
    let long = trace(
        &g,
        "in",
        FlowDirection::Forward,
        FlowBounds::clamped(Some(12), None, None),
    );
    assert_eq!(long.hops[0].steps.len(), 10, "{long:#?}");
    assert!(long.limits.is_empty());
    // Caps are applied: 99 is clamped to the maxima.
    let capped = FlowBounds::clamped(Some(99), Some(99), Some(99));
    assert_eq!(
        (capped.call_depth, capped.event_depth, capped.fan_out),
        (MAX_CALL_DEPTH, MAX_EVENT_DEPTH, MAX_FAN_OUT)
    );
}

#[test]
fn event_depth_cap_reports_unexpanded() {
    // in -> h1 publishes e1 -> h2 publishes e2 -> h3 publishes e3.
    let files = vec![
        f("h1").consumes(2, "in").calls("p1", 3),
        f("p1").produces(4, "e1"),
        f("h2").consumes(2, "e1").calls("p2", 3),
        f("p2").produces(4, "e2"),
        f("h3").consumes(2, "e2").calls("p3", 3),
        f("p3").produces(4, "e3"),
    ];
    let g = build(&files);
    let t = trace(
        &g,
        "in",
        FlowDirection::Forward,
        FlowBounds::clamped(None, Some(2), None),
    );
    assert_eq!(t.hops.len(), 2);
    assert_eq!(t.hops[1].next[0].outcome, FlowOutcome::DepthLimit(1));
    assert_eq!(t.unexpanded.len(), 1);
    assert!(t.unexpanded[0].file_path.ends_with("h3.ts"));
}

#[test]
fn same_file_fallback_is_heuristic_when_no_call_path() {
    // No call facts at all: consumer and producer in one file.
    let mut g = ContractGraph::new();
    let c = g.add_node(node("in", NodeKind::KafkaTopic, "/w/x.java", 10));
    g.add_consumer(c, "in");
    let p = g.add_node(node("out", NodeKind::KafkaTopic, "/w/x.java", 14));
    g.add_producer(p, "out");
    g.reconcile_edges();
    let t = trace(&g, "in", FlowDirection::Forward, FlowBounds::default());
    assert_eq!(t.hops.len(), 1);
    assert!(t.hops[0].fallback);
    assert_eq!(t.hops[0].confidence, EdgeConfidence::Heuristic);
    let r = trace(&g, "out", FlowDirection::Reverse, FlowBounds::default());
    assert!(r.hops[0].fallback);
    assert_eq!(names(&r.hops[0]), ["Produce:out", "Consume:in"]);
}

#[test]
fn deterministic_across_builds() {
    let render = || {
        let mut files = long_chain(4);
        files.push(f("side").consumes(2, "in").calls("f3", 3));
        let g = build(&files);
        let a = format!(
            "{:?}",
            trace(&g, "in", FlowDirection::Forward, FlowBounds::default())
        );
        let b = format!(
            "{:?}",
            trace(&g, "out", FlowDirection::Reverse, FlowBounds::default())
        );
        (a, b)
    };
    let first = render();
    for _ in 0..3 {
        assert_eq!(render(), first);
    }
}

#[test]
fn two_megabyte_stack_on_a_deep_graph() {
    let handle = std::thread::Builder::new()
        .stack_size(2 * 1024 * 1024)
        .spawn(|| {
            let g = build(&long_chain(3_000));
            let bounds = FlowBounds::clamped(
                Some(MAX_CALL_DEPTH),
                Some(MAX_EVENT_DEPTH),
                Some(MAX_FAN_OUT),
            );
            let f = trace(&g, "in", FlowDirection::Forward, bounds);
            let r = trace(&g, "out", FlowDirection::Reverse, bounds);
            let m = g.analyze_impact_with_depth("in", ContractGraph::MAX_IMPACT_DEPTH);
            (f.hops.len(), r.hops.len(), m.chain_end.len())
        })
        .expect("spawn");
    let (f, r, ends) = handle.join().expect("no stack overflow");
    // The 3,000-call chain is cut by call_depth both ways: a bound, not a chain end.
    assert_eq!((f, r, ends), (1, 1, 0));
}

/// The nodes `analyze_impact` at `depth` lists past the first hop, and the ones
/// the engine reaches with `event_depth = depth - 1` and no fan-out cap.
fn assert_agrees(g: &ContractGraph, topic: &'static str, depth: usize) {
    let flow = g.analyze_impact_with_depth(topic, depth);
    let bounds = FlowBounds {
        call_depth: DEFAULT_CALL_DEPTH,
        event_depth: depth - 1,
        fan_out: usize::MAX,
    };
    let t = trace(g, topic, FlowDirection::Forward, bounds);
    let ids = |v: &[&ContractNode]| {
        let mut ids: Vec<NodeId> = v.iter().map(|n| n.id).collect();
        ids.sort_unstable();
        ids.dedup();
        ids
    };
    let mut producers: Vec<&ContractNode> = t
        .hops
        .iter()
        .filter_map(|h| {
            let (first, last) = (h.steps.first()?, h.steps.last()?);
            (last.kind == FlowStepKind::Produce && last.node.id != first.node.id)
                .then_some(last.node)
        })
        .collect();
    producers.retain(|p| !flow.upstream_producers.iter().any(|u| u.id == p.id));
    let mut consumers: Vec<&ContractNode> = t.entries.clone();
    consumers.extend(t.reached.iter().copied());
    let mut flow_consumers = flow.downstream_consumers.clone();
    flow_consumers.extend(flow.related_sagas.iter().copied());
    assert_eq!(
        ids(&flow.downstream_producers),
        ids(&producers),
        "producers {topic} depth {depth}"
    );
    assert_eq!(
        ids(&flow_consumers),
        ids(&consumers),
        "consumers {topic} depth {depth}"
    );
    assert_eq!(ids(&flow.unexpanded), ids(&t.unexpanded), "unexpanded");
    assert_eq!(ids(&flow.chain_end), ids(&t.chain_end), "chain end");
}

#[test]
fn analyze_impact_past_depth_one_agrees_with_the_engine() {
    let files = vec![
        f("h1").consumes(2, "in").calls("p1", 3),
        f("p1").produces(4, "e1"),
        f("h2").consumes(2, "e1").calls("p2", 3),
        f("p2").produces(4, "e2"),
        f("h3").consumes(2, "e2"),
    ];
    let g = build(&files);
    for depth in 2..=ContractGraph::MAX_IMPACT_DEPTH {
        assert_agrees(&g, "in", depth);
    }
    // The call-linked producer of the first hop is a matrix row (8.x: chain end at h1).
    let flow = g.analyze_impact_with_depth("in", 3);
    assert!(flow
        .downstream_producers
        .iter()
        .any(|p| p.file_path.ends_with("p1.ts")));
    assert_eq!(flow.call_linked_producers.len(), 2);
    // Depth 1 does not run the engine.
    assert!(g
        .analyze_impact_with_depth("in", 1)
        .downstream_producers
        .is_empty());
}

// ---- Fix round 1 (review of c3adbda) ----

/// One handler `h` consuming `in`, calling `p`, which publishes `n` topics, each
/// consumed by its own handler `c{k}` (so every hop has a child to queue).
fn wide(n: usize) -> Vec<F> {
    let mut p = f("p");
    p.fns[0].2 = 100_000;
    let mut files = vec![f("h").consumes(2, "in").calls("p", 3)];
    for k in 0..n {
        p = p.produces(10 + k, &format!("t.{k}"));
        files.push(f(&format!("c{k}")).consumes(2, &format!("t.{k}")));
    }
    files.push(p);
    files
}

fn unbounded_fan_out() -> FlowBounds {
    FlowBounds {
        call_depth: DEFAULT_CALL_DEPTH,
        event_depth: MAX_EVENT_DEPTH,
        fan_out: usize::MAX,
    }
}

#[test]
fn hop_cap_is_enforced_per_hop_and_flagged() {
    let g = build(&wide(2_100));
    let t = trace(&g, "in", FlowDirection::Forward, unbounded_fan_out());
    assert_eq!(t.hops.len(), MAX_FLOW_HOPS, "hop cap");
    assert!(t.capped);
}

#[test]
fn followed_without_traced_children_says_hop_limit() {
    let g = build(&wide(1_500));
    let t = trace(&g, "in", FlowDirection::Forward, unbounded_fan_out());
    assert!(t.capped);
    for h in &t.hops {
        for nx in &h.next {
            assert!(
                !matches!(&nx.outcome, FlowOutcome::Followed(v) if v.is_empty()),
                "hop {} topic {} followed into nothing",
                h.id,
                nx.topic
            );
        }
    }
    assert!(t
        .hops
        .iter()
        .any(|h| h.next.iter().any(|nx| nx.outcome == FlowOutcome::HopLimit)));
}

#[test]
fn call_depth_cut_is_not_relabelled_as_same_file_fallback() {
    let mut files = long_chain(8);
    // Outside f0 (lines 1-20): only the same-file fallback could link it.
    files[0] = files[0].clone().produces(25, "side");
    let g = build(&files);
    let t = trace(
        &g,
        "in",
        FlowDirection::Forward,
        FlowBounds::clamped(Some(6), None, None),
    );
    assert!(t.hops.iter().all(|h| !h.fallback), "{t:#?}");
    assert_eq!(t.hops.len(), 1);
    assert_eq!(t.hops[0].end, Some(FlowEnd::CallDepthLimit));
}

#[test]
fn fan_out_caps_the_publishes_of_one_entry() {
    let g = build(&wide(100));
    let t = trace(
        &g,
        "in",
        FlowDirection::Forward,
        FlowBounds::clamped(None, Some(1), Some(20)),
    );
    assert_eq!(t.hops.len(), 20);
    assert!(
        t.limits
            .iter()
            .any(|l| matches!(l, FlowLimit::FanOut(_, 20, 80))),
        "{:?}",
        t.limits
    );
}
