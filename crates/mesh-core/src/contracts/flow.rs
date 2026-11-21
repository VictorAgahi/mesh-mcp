//! The chain engine (9.0, ticket T3): end-to-end event flows over the contract
//! graph, one structured hop per "entry -> calls -> publish" segment.
//!
//! A hop starts at an entry site (a consumer, an HTTP route or gRPC handler, a
//! named handler or cron method), walks the derived `Calls` edges from the
//! callable enclosing it (see `calls`) and ends at every publish it reaches. Each
//! topic published continues the chain at that topic's consumers, one event level
//! further. `reverse` runs the same walk backwards: from a publish, through the
//! callers, to the entries that reach it, then to the producers of the topics
//! those entries consume.
//!
//! When no call path reaches a publish (no `Calls` data for the language, a call
//! the resolver could not follow), the legacy rule of `analyze_impact` 8.x is the
//! fallback: producers declared in the entry's own file, labelled `heuristic`.
//!
//! Every walk is iterative (explicit queues) with visited sets: each callable is
//! visited once per call walk, each entry once per trace, each topic once per
//! trace (a cyclic event chain stops at the first repeated topic and says so).
//! Every list is sorted by (path, line, name, id) before it is used, so the
//! output never depends on hash iteration order. `analyze_impact` past depth 1
//! is computed from the same engine ([`ContractGraph::analyze_impact_with_depth`]).

use super::*;
use std::collections::VecDeque;

/// Default and maximum length of one hop's call path (calls between an entry and
/// a publish).
pub const DEFAULT_CALL_DEPTH: usize = 6;
pub const MAX_CALL_DEPTH: usize = 12;
/// Default and maximum number of event levels (hops chained through topics).
pub const DEFAULT_EVENT_DEPTH: usize = 3;
pub const MAX_EVENT_DEPTH: usize = 5;
/// Default and maximum callees (or consumers, or producers) followed per node.
pub const DEFAULT_FAN_OUT: usize = 20;
pub const MAX_FAN_OUT: usize = 50;
/// Most hops one trace builds; past it the trace stops and says so.
pub const MAX_FLOW_HOPS: usize = 2_000;

/// Which way a trace runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FlowDirection {
    /// What does handling the entry trigger (entry -> calls -> publish -> consumers).
    Forward,
    /// What ends up publishing the entry (publish <- callers <- entry <- producers).
    Reverse,
}

/// What the entry of a trace names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlowEntry<'a> {
    /// An event, topic, queue or stream, matched like `analyze_impact`.
    Topic(&'a str),
    /// An event when one matches the name, else a handler symbol (one topic match).
    Auto(&'a str),
    /// HTTP route handlers, already resolved by the caller (`routes::find_routes`).
    Route(Vec<NodeId>),
    /// A handler symbol (`OrderService.place`, `place`).
    Handler(&'a str),
    /// A scheduled method (`ReportScheduler.hourly`): an indexed schedule (`@Cron`,
    /// `@Interval`, `@Timeout`, `@Scheduled`) when one carries the name, else the method
    /// resolved like a handler.
    Cron(&'a str),
}

/// Bounds of one trace. [`FlowBounds::clamped`] applies the documented caps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct FlowBounds {
    pub call_depth: usize,
    pub event_depth: usize,
    pub fan_out: usize,
}

impl Default for FlowBounds {
    fn default() -> Self {
        Self {
            call_depth: DEFAULT_CALL_DEPTH,
            event_depth: DEFAULT_EVENT_DEPTH,
            fan_out: DEFAULT_FAN_OUT,
        }
    }
}

impl FlowBounds {
    /// The defaults for `None`, every value clamped to `1..=max`.
    pub fn clamped(call: Option<usize>, event: Option<usize>, fan_out: Option<usize>) -> Self {
        Self {
            call_depth: call.unwrap_or(DEFAULT_CALL_DEPTH).clamp(1, MAX_CALL_DEPTH),
            event_depth: event
                .unwrap_or(DEFAULT_EVENT_DEPTH)
                .clamp(1, MAX_EVENT_DEPTH),
            fan_out: fan_out.unwrap_or(DEFAULT_FAN_OUT).clamp(1, MAX_FAN_OUT),
        }
    }
}

/// What one step of a hop is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FlowStepKind {
    Consume,
    Route,
    Handler,
    Cron,
    /// A callable on the path: in a forward hop the callee, in a reverse hop the caller.
    Call,
    Produce,
}

/// One step of a hop.
#[derive(Debug, Clone, Serialize)]
pub struct FlowStep<'g> {
    pub kind: FlowStepKind,
    /// The site (consumer, route, producer) or the callable (a `Call` step).
    pub node: &'g ContractNode,
    /// The topic a `Consume` / `Produce` step names.
    pub topic: Option<CompactStr>,
    /// The callable enclosing the first step's site, when it is not a step itself.
    pub within: Option<&'g ContractNode>,
    /// For a `Call` step: the file and line of the call expression (in the caller).
    pub call_line: Option<usize>,
    pub call_file: Option<&'g FilePath>,
    /// For a `Call` step: the resolution rule (`injected_field`, `this`, ...).
    pub via: Option<CompactStr>,
    pub confidence: EdgeConfidence,
}

/// What a hop leads to past its last step, per topic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FlowOutcome {
    /// Continued: the next hops (indices into [`FlowTrace::hops`]).
    Followed(Vec<usize>),
    /// No consumer (forward) or producer (reverse) is statically resolved.
    NoCounterpart,
    /// `event_depth` reached: this many consumers / producers were not expanded.
    DepthLimit(usize),
    /// The topic was already traced in this trace (a cycle or a join).
    AlreadyTraced,
    /// Its counterparts were queued but [`MAX_FLOW_HOPS`] stopped the trace first.
    HopLimit,
}

/// What follows a hop: one entry per topic it publishes (forward) or consumes
/// (reverse). Empty for a terminal hop, see [`FlowHop::end`].
#[derive(Debug, Clone, Serialize)]
pub struct FlowNext {
    pub topic: CompactStr,
    pub outcome: FlowOutcome,
    /// Counterparts left out past `fan_out`.
    pub more: usize,
}

/// Why a hop is the last of its chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FlowEnd {
    /// Forward: the entry reaches no statically resolved publish. Reverse: no
    /// statically resolved entry reaches the publish.
    ChainEnd,
    /// Reverse: the chain starts at a route, handler or cron entry.
    Origin,
    /// The call walk was cut by `call_depth` before reaching a publish (forward)
    /// or an entry (reverse): not an end, a bound.
    CallDepthLimit,
}

/// One hop: an entry, the calls, the publish (forward) or the reverse of it.
#[derive(Debug, Clone, Serialize)]
pub struct FlowHop<'g> {
    /// Hierarchical id: `1`, `1.2`, `1.2.1` (the chain is the path of ids).
    pub id: String,
    /// 1-based event level.
    pub level: usize,
    /// The hop this one continues (index into [`FlowTrace::hops`]).
    pub parent: Option<usize>,
    pub steps: Vec<FlowStep<'g>>,
    /// Weakest step of this hop.
    pub confidence: EdgeConfidence,
    /// Weakest step of the whole chain from the entry to this hop.
    pub chain_confidence: EdgeConfidence,
    /// Linked by the legacy same-file rule, not by a call path.
    pub fallback: bool,
    pub next: Vec<FlowNext>,
    pub end: Option<FlowEnd>,
    /// Index of the parent's `next` entry this hop was reached through.
    #[serde(skip)]
    parent_slot: usize,
}

/// A bound that cut the walk short, reported instead of silently dropped.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FlowLimit {
    /// `call_depth` reached at this callable, with more calls past it.
    CallDepth(FlowSite),
    /// `fan_out` reached at this callable: `(site, followed, left out)`.
    FanOut(FlowSite, usize, usize),
}

/// Where a [`FlowLimit`] was hit.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct FlowSite {
    pub file: FilePath,
    pub line: usize,
    pub name: CompactStr,
    pub repo_id: RepoId,
}

/// A structured chain trace. Hops are in depth-first pre-order: every chain
/// reads top to bottom, parents before children.
#[derive(Debug, Clone, Serialize)]
pub struct FlowTrace<'g> {
    pub target: CompactStr,
    pub entry_kind: &'static str,
    pub direction: FlowDirection,
    pub bounds: FlowBounds,
    /// The entry sites the trace started from (sorted).
    pub entries: Vec<&'g ContractNode>,
    pub hops: Vec<FlowHop<'g>>,
    /// Entries reached past `event_depth`, not expanded (sorted).
    pub unexpanded: Vec<&'g ContractNode>,
    /// Entries expanded that reach nothing (sorted).
    pub chain_end: Vec<&'g ContractNode>,
    /// Entries reached at level >= 2, expanded or not, in discovery order.
    pub reached: Vec<&'g ContractNode>,
    /// Topic nodes crossed past the entry, in discovery order.
    pub topics: Vec<&'g ContractNode>,
    /// Bounds hit (sorted, deduplicated).
    pub limits: Vec<FlowLimit>,
    /// Entries, producers or consumers left out by the caller's `hide` rule.
    pub hidden: usize,
    /// [`MAX_FLOW_HOPS`] was reached.
    pub capped: bool,
    /// Entry sites left out past `fan_out`.
    pub entries_more: usize,
}

/// `(path, line, name, id)`: the one sort order of every list of the engine.
fn node_key(n: &ContractNode) -> (&FilePath, usize, &str, NodeId) {
    (&n.file_path, n.line_start, n.name.as_str(), n.id)
}

fn sort_nodes(v: &mut Vec<&ContractNode>) {
    v.sort_by(|a, b| node_key(a).cmp(&node_key(b)));
    v.dedup_by_key(|n| n.id);
}

fn rank(c: EdgeConfidence) -> u8 {
    match c {
        EdgeConfidence::Exact => 0,
        EdgeConfidence::Heuristic => 1,
        EdgeConfidence::Ambiguous => 2,
    }
}

fn weakest(a: EdgeConfidence, b: EdgeConfidence) -> EdgeConfidence {
    if rank(a) >= rank(b) {
        a
    } else {
        b
    }
}

/// `injected_field L11` -> (`injected_field`, 11).
fn parse_call_meta(meta: Option<&CompactStr>) -> (Option<CompactStr>, Option<usize>) {
    let Some(m) = meta else {
        return (None, None);
    };
    match m.rsplit_once(" L") {
        Some((via, line)) => (Some(CompactStr::new(via)), line.parse().ok()),
        None => (Some(m.clone()), None),
    }
}

/// Adjacency built once per trace from the graph's edges and the calls layer.
struct FlowIndex<'g> {
    calls_out: HashMap<NodeId, Vec<&'g ContractEdge>>,
    calls_in: HashMap<NodeId, Vec<&'g ContractEdge>>,
    /// Callable -> producer nodes it encloses (sorted).
    producers_in: HashMap<NodeId, Vec<&'g ContractNode>>,
    /// Callable -> entry nodes (consumers, routes, gRPC handlers) it encloses (sorted).
    entries_in: HashMap<NodeId, Vec<&'g ContractNode>>,
    /// Producer -> topics (`Produces` edges).
    produces: HashMap<NodeId, Vec<NodeId>>,
    /// Topic -> producers.
    produced_by: HashMap<NodeId, Vec<NodeId>>,
    /// `Consumes` edges in both directions (topic <-> consumer).
    consumes: HashMap<NodeId, Vec<NodeId>>,
    consumer_ids: HashSet<NodeId>,
}

/// One segment found by a call walk, before it becomes a hop.
struct Segment<'g> {
    steps: Vec<FlowStep<'g>>,
    fallback: bool,
}

/// A call walk from one callable: BFS order and the BFS tree.
struct Walk<'g> {
    order: Vec<NodeId>,
    parent: HashMap<NodeId, (NodeId, &'g ContractEdge)>,
    /// `call_depth` stopped the walk somewhere with calls left to follow.
    cut: bool,
}

/// An entry waiting to be expanded.
struct Pending<'g> {
    node: &'g ContractNode,
    level: usize,
    parent: Option<usize>,
    /// Index of the parent's `next` entry (the topic) that led here.
    slot: usize,
    topic: Option<CompactStr>,
}

impl ContractGraph {
    /// Traces the flows of `entry` (see the module doc). `hide` leaves nodes out
    /// (the caller's test-file rule); they are counted in [`FlowTrace::hidden`].
    pub fn trace_flow(
        &self,
        entry: FlowEntry<'_>,
        direction: FlowDirection,
        bounds: FlowBounds,
        hide: &dyn Fn(&ContractNode) -> bool,
    ) -> FlowTrace<'_> {
        let mut entry = entry;
        let mut matched = None;
        if let FlowEntry::Auto(t) | FlowEntry::Topic(t) = entry {
            let flow = self.analyze_impact(t);
            let any = !(flow.topics.is_empty()
                && flow.upstream_producers.is_empty()
                && flow.downstream_consumers.is_empty());
            if matches!(entry, FlowEntry::Auto(_)) && !any {
                entry = FlowEntry::Handler(t);
            } else {
                entry = FlowEntry::Topic(t);
                matched = Some(flow);
            }
        }
        let (target, kind, starts, seen_topics, topic): (String, _, Vec<&ContractNode>, _, _) =
            match (&entry, matched) {
                (FlowEntry::Topic(t), Some(flow)) => {
                    let starts: Vec<&ContractNode> = match direction {
                        FlowDirection::Forward => flow
                            .downstream_consumers
                            .iter()
                            .chain(flow.related_sagas.iter())
                            .copied()
                            .collect(),
                        FlowDirection::Reverse => flow.upstream_producers.clone(),
                    };
                    let seen = flow.topics.iter().map(|n| n.id).collect();
                    (t.to_string(), "topic", starts, seen, Some(flow.key.clone()))
                }
                (FlowEntry::Route(ids), _) => (
                    String::new(),
                    "route",
                    ids.iter().filter_map(|id| self.get_node(*id)).collect(),
                    HashSet::new(),
                    None,
                ),
                (FlowEntry::Handler(name), _) => (
                    name.to_string(),
                    "handler",
                    self.resolve_handler(name),
                    HashSet::new(),
                    None,
                ),
                (FlowEntry::Cron(name), _) => (
                    name.to_string(),
                    "cron",
                    self.resolve_schedule(name),
                    HashSet::new(),
                    None,
                ),
                // Unreachable: a topic entry always carries its match.
                (FlowEntry::Topic(t) | FlowEntry::Auto(t), _) => {
                    (t.to_string(), "topic", Vec::new(), HashSet::new(), None)
                }
            };
        let start_kind = match kind {
            "route" => FlowStepKind::Route,
            "handler" => FlowStepKind::Handler,
            "cron" => FlowStepKind::Cron,
            _ => match direction {
                FlowDirection::Forward => FlowStepKind::Consume,
                FlowDirection::Reverse => FlowStepKind::Produce,
            },
        };
        let mut trace = self.trace_from(
            starts,
            start_kind,
            topic,
            seen_topics,
            direction,
            bounds,
            hide,
        );
        trace.target = CompactStr::new(target);
        trace.entry_kind = kind;
        trace
    }

    /// The scheduled methods named `name` (`Owner.method` or a bare method); a name no
    /// schedule carries falls back to [`Self::resolve_handler`], so a method scheduled by
    /// something the indexer does not see (a runtime registry) still traces as a handler.
    /// Sorted.
    fn resolve_schedule(&self, name: &str) -> Vec<&ContractNode> {
        let name = name.trim();
        let mut out: Vec<&ContractNode> = self
            .nodes
            .values()
            .filter(|n| {
                n.kind == NodeKind::Schedule
                    && (n.name == name
                        || (!name.contains('.') && n.name.rsplit('.').next() == Some(name)))
            })
            .collect();
        if out.is_empty() {
            return self.resolve_handler(name);
        }
        sort_nodes(&mut out);
        out
    }

    /// Callables named `name` (`Owner.method` or a bare method), else declarations
    /// with that exact name. Sorted.
    // shortcut: only materialized callables (on an entry -> publish path) and exact
    // declaration names are matched; a fuzzy symbol search is left to smart_search.
    fn resolve_handler(&self, name: &str) -> Vec<&ContractNode> {
        let name = name.trim();
        let mut out: Vec<&ContractNode> = self
            .callable_nodes()
            .iter()
            .filter(|c| {
                c.name == name || (!name.contains('.') && c.name.rsplit('.').next() == Some(name))
            })
            .collect();
        if out.is_empty() {
            out = self
                .name_to_nodes
                .get(name)
                .map(Vec::as_slice)
                .unwrap_or(&[])
                .iter()
                .filter_map(|id| self.nodes.get(id))
                .filter(|n| n.file_path.as_os_str() != "event-bus")
                .collect();
        }
        sort_nodes(&mut out);
        out
    }

    fn flow_index(&self) -> FlowIndex<'_> {
        let mut calls_out: HashMap<NodeId, Vec<&ContractEdge>> = HashMap::new();
        let mut calls_in: HashMap<NodeId, Vec<&ContractEdge>> = HashMap::new();
        for e in self.call_edges() {
            calls_out.entry(e.from).or_default().push(e);
            calls_in.entry(e.to).or_default().push(e);
        }
        let mut produces: HashMap<NodeId, Vec<NodeId>> = HashMap::new();
        let mut produced_by: HashMap<NodeId, Vec<NodeId>> = HashMap::new();
        let mut consumes: HashMap<NodeId, Vec<NodeId>> = HashMap::new();
        for e in &self.edges {
            match e.kind {
                EdgeKind::Produces => {
                    produces.entry(e.from).or_default().push(e.to);
                    produced_by.entry(e.to).or_default().push(e.from);
                }
                EdgeKind::Consumes => {
                    consumes.entry(e.from).or_default().push(e.to);
                    consumes.entry(e.to).or_default().push(e.from);
                }
                _ => {}
            }
        }
        for v in produces
            .values_mut()
            .chain(produced_by.values_mut())
            .chain(consumes.values_mut())
        {
            v.sort_unstable();
            v.dedup();
        }
        let consumer_ids: HashSet<NodeId> =
            self.topic_consumers.values().flatten().copied().collect();
        let mut producers_in: HashMap<NodeId, Vec<&ContractNode>> = HashMap::new();
        let mut entries_in: HashMap<NodeId, Vec<&ContractNode>> = HashMap::new();
        for path in self.derived_calls.by_file.keys() {
            for id in self.get_nodes_for_file(path) {
                let Some(node) = self.nodes.get(id) else {
                    continue;
                };
                let is_producer = produces.contains_key(id);
                let is_entry = consumer_ids.contains(id) || node.kind.is_entry_site();
                if !(is_producer || is_entry) {
                    continue;
                }
                for c in self.site_callables(node) {
                    if is_producer {
                        producers_in.entry(c.id).or_default().push(node);
                    }
                    if is_entry {
                        entries_in.entry(c.id).or_default().push(node);
                    }
                }
            }
        }
        for v in producers_in.values_mut().chain(entries_in.values_mut()) {
            sort_nodes(v);
        }
        FlowIndex {
            calls_out,
            calls_in,
            producers_in,
            entries_in,
            produces,
            produced_by,
            consumes,
            consumer_ids,
        }
    }

    /// The topic an entry site is shown consuming (producing, in reverse): the name the caller
    /// gave, unless the site is on broker hubs of the query (`kafka:orders`), which are named
    /// so that two channels of one name are told apart. A single-broker workspace has no such
    /// hub and keeps the name as given.
    fn entry_label(
        &self,
        idx: &FlowIndex<'_>,
        site: NodeId,
        direction: FlowDirection,
        queried: &HashSet<NodeId>,
        given: &CompactStr,
    ) -> CompactStr {
        let hubs = match direction {
            FlowDirection::Forward => idx.consumes.get(&site),
            FlowDirection::Reverse => idx.produces.get(&site),
        };
        let mut names: Vec<&str> = hubs
            .map(Vec::as_slice)
            .unwrap_or(&[])
            .iter()
            .filter(|h| queried.contains(h))
            .filter_map(|h| self.get_node(*h))
            .filter(|n| Self::hub_broker(n).is_some())
            .map(|n| n.name.as_str())
            .collect();
        names.sort_unstable();
        names.dedup();
        if names.is_empty() {
            given.clone()
        } else {
            CompactStr::new(names.join(", "))
        }
    }

    /// Display name of a topic node (`order.requested`).
    fn topic_label(&self, id: NodeId) -> CompactStr {
        self.get_node(id)
            .map(|n| {
                let name = n.name.as_str();
                CompactStr::new(
                    name.strip_prefix("produce:")
                        .or_else(|| name.strip_prefix("consume:"))
                        .unwrap_or(name),
                )
            })
            .unwrap_or_default()
    }

    /// BFS over `Calls` edges from `start` (callees forward, callers reverse).
    fn call_walk<'g>(
        &'g self,
        idx: &FlowIndex<'g>,
        starts: &[NodeId],
        direction: FlowDirection,
        bounds: FlowBounds,
        limits: &mut Vec<FlowLimit>,
    ) -> Walk<'g> {
        let adjacency = match direction {
            FlowDirection::Forward => &idx.calls_out,
            FlowDirection::Reverse => &idx.calls_in,
        };
        let mut seen: HashSet<NodeId> = starts.iter().copied().collect();
        let mut parent = HashMap::new();
        let mut order = Vec::new();
        let mut cut = false;
        let mut queue: VecDeque<(NodeId, usize)> = starts.iter().map(|&s| (s, 0)).collect();
        while let Some((id, depth)) = queue.pop_front() {
            order.push(id);
            let mut next: Vec<(&ContractNode, &ContractEdge)> = adjacency
                .get(&id)
                .map(Vec::as_slice)
                .unwrap_or(&[])
                .iter()
                .filter_map(|e| {
                    let other = match direction {
                        FlowDirection::Forward => e.to,
                        FlowDirection::Reverse => e.from,
                    };
                    self.get_node(other).map(|n| (n, *e))
                })
                .collect();
            next.sort_by(|a, b| node_key(a.0).cmp(&node_key(b.0)));
            if next.iter().all(|(n, _)| seen.contains(&n.id)) {
                continue;
            }
            let Some(here) = self.get_node(id) else {
                continue;
            };
            let label = || FlowSite {
                file: here.file_path.clone(),
                line: here.line_start,
                name: here.name.clone(),
                repo_id: here.repo_id,
            };
            if depth >= bounds.call_depth {
                limits.push(FlowLimit::CallDepth(label()));
                cut = true;
                continue;
            }
            if next.len() > bounds.fan_out {
                limits.push(FlowLimit::FanOut(
                    label(),
                    bounds.fan_out,
                    next.len() - bounds.fan_out,
                ));
                next.truncate(bounds.fan_out);
            }
            for (n, e) in next {
                if seen.insert(n.id) {
                    parent.insert(n.id, (id, e));
                    queue.push_back((n.id, depth + 1));
                }
            }
        }
        Walk { order, parent, cut }
    }

    /// The `Call` steps from the walk's root to `to`, root excluded, in walk order,
    /// and that root.
    fn call_steps<'g>(&'g self, walk: &Walk<'g>, to: NodeId) -> (Vec<FlowStep<'g>>, NodeId) {
        let mut steps = Vec::new();
        let mut cur = to;
        while let Some(&(prev, edge)) = walk.parent.get(&cur) {
            let (via, line) = parse_call_meta(edge.metadata.as_ref());
            if let Some(node) = self.get_node(cur) {
                steps.push(FlowStep {
                    kind: FlowStepKind::Call,
                    node,
                    topic: None,
                    within: None,
                    call_line: line,
                    call_file: self.get_node(edge.from).map(|n| &n.file_path),
                    via,
                    confidence: edge.confidence,
                });
            }
            cur = prev;
        }
        steps.reverse();
        (steps, cur)
    }

    /// The first step of a segment whose call walk started at `root`: a root the
    /// site is anchored on only through a Go constructor (not its own enclosing
    /// callable) is an inference, so the step is shown in that root and capped
    /// at `heuristic`; the weakest-hop rule carries it to the chain.
    fn rooted_start<'g>(
        &'g self,
        s: &'g ContractNode,
        start: &FlowStep<'g>,
        root: NodeId,
    ) -> FlowStep<'g> {
        let mut step = start.clone();
        let own = s.kind == NodeKind::Callable && s.id == root
            || self.enclosing_callable(s).is_some_and(|c| c.id == root);
        if !own {
            step.within = self.get_node(root).or(step.within);
            step.confidence = weakest(step.confidence, EdgeConfidence::Heuristic);
        }
        step
    }

    /// The confidence of a site reached in callable `x`: its own, capped at
    /// `heuristic` when `x` does not enclose it (a Go constructor anchoring).
    fn anchored_confidence(&self, site: &ContractNode, x: NodeId) -> EdgeConfidence {
        let own = self.topic_confidence(site.id);
        if self.enclosing_callable(site).is_some_and(|c| c.id == x) {
            own
        } else {
            weakest(own, EdgeConfidence::Heuristic)
        }
    }

    /// Keeps the first `fan_out` segments of one entry, the rest named in `limits`.
    fn cap_segments(
        segs: &mut Vec<Segment<'_>>,
        site: &ContractNode,
        bounds: FlowBounds,
        limits: &mut Vec<FlowLimit>,
    ) {
        if segs.len() > bounds.fan_out {
            limits.push(FlowLimit::FanOut(
                FlowSite {
                    file: site.file_path.clone(),
                    line: site.line_start,
                    name: site.name.clone(),
                    repo_id: site.repo_id,
                },
                bounds.fan_out,
                segs.len() - bounds.fan_out,
            ));
            segs.truncate(bounds.fan_out);
        }
    }

    fn site_step<'g>(
        &'g self,
        kind: FlowStepKind,
        node: &'g ContractNode,
        topic: Option<CompactStr>,
        within: Option<&'g ContractNode>,
        confidence: EdgeConfidence,
    ) -> FlowStep<'g> {
        FlowStep {
            kind,
            node,
            topic,
            within,
            call_line: None,
            call_file: None,
            via: None,
            confidence,
        }
    }

    /// The first topic a producer publishes onto, for its step label.
    fn produced_topic(&self, idx: &FlowIndex<'_>, p: NodeId) -> Option<CompactStr> {
        idx.produces
            .get(&p)
            .and_then(|t| t.first())
            .map(|&t| self.topic_label(t))
    }

    /// Forward segments of the entry `s`: every publish its call walk reaches, or
    /// the same-file producers when it reaches none.
    #[allow(clippy::too_many_arguments)]
    fn forward_segments<'g>(
        &'g self,
        idx: &FlowIndex<'g>,
        s: &'g ContractNode,
        start: FlowStep<'g>,
        bounds: FlowBounds,
        limits: &mut Vec<FlowLimit>,
        hide: &dyn Fn(&ContractNode) -> bool,
        hidden: &mut usize,
    ) -> (Vec<Segment<'g>>, bool) {
        let mut segs = Vec::new();
        let mut cut = false;
        let produce = |p: &'g ContractNode, confidence| {
            self.site_step(
                FlowStepKind::Produce,
                p,
                self.produced_topic(idx, p.id),
                None,
                confidence,
            )
        };
        // The entry node itself publishes (a `produce:` pattern on the handler).
        if idx.produces.contains_key(&s.id) {
            segs.push(Segment {
                steps: vec![start.clone(), produce(s, self.topic_confidence(s.id))],
                fallback: false,
            });
        }
        let mut call_linked = false;
        // A site is anchored on its enclosing callable and, for a Go constructor,
        // on the methods of the struct it builds: the walk starts from all of them.
        let roots: Vec<NodeId> = if s.kind == NodeKind::Callable {
            vec![s.id]
        } else {
            self.site_callables(s).iter().map(|c| c.id).collect()
        };
        if !roots.is_empty() {
            let walk = self.call_walk(idx, &roots, FlowDirection::Forward, bounds, limits);
            cut = walk.cut;
            for &x in &walk.order {
                for &p in idx.producers_in.get(&x).map(Vec::as_slice).unwrap_or(&[]) {
                    if p.id == s.id {
                        continue;
                    }
                    if hide(p) {
                        *hidden += 1;
                        continue;
                    }
                    let (calls, root) = self.call_steps(&walk, x);
                    let mut steps = vec![self.rooted_start(s, &start, root)];
                    steps.extend(calls);
                    steps.push(produce(p, self.anchored_confidence(p, x)));
                    call_linked = true;
                    segs.push(Segment {
                        steps,
                        fallback: false,
                    });
                }
            }
        }
        // The fallback applies when no call path reaches a publish: the entry node
        // publishing itself does not count (a Java handler can carry its own
        // Produces edge on the handler line and a second publish further down).
        // A walk cut by `call_depth` is a bound, not "no call path": no fallback.
        if !call_linked && !cut {
            let mut same_file: Vec<&ContractNode> = self
                .get_nodes_for_file(&s.file_path)
                .iter()
                .filter(|id| **id != s.id && idx.produces.contains_key(id))
                .filter_map(|id| self.nodes.get(id))
                .collect();
            sort_nodes(&mut same_file);
            for p in same_file {
                if hide(p) {
                    *hidden += 1;
                    continue;
                }
                segs.push(Segment {
                    steps: vec![start.clone(), produce(p, EdgeConfidence::Heuristic)],
                    fallback: true,
                });
            }
        }
        Self::cap_segments(&mut segs, s, bounds, limits);
        (segs, cut && !call_linked)
    }

    /// Reverse segments of the producer `p`: every entry whose callable reaches
    /// it, or the consumers of its own file when none does.
    #[allow(clippy::too_many_arguments)]
    fn reverse_segments<'g>(
        &'g self,
        idx: &FlowIndex<'g>,
        p: &'g ContractNode,
        start: FlowStep<'g>,
        bounds: FlowBounds,
        limits: &mut Vec<FlowLimit>,
        hide: &dyn Fn(&ContractNode) -> bool,
        hidden: &mut usize,
    ) -> (Vec<Segment<'g>>, bool) {
        let mut segs = Vec::new();
        let mut cut = false;
        let entry_step = |e: &'g ContractNode, confidence| {
            let (kind, topic) = if idx.consumer_ids.contains(&e.id) {
                (
                    FlowStepKind::Consume,
                    idx.consumes
                        .get(&e.id)
                        .and_then(|t| t.first())
                        .map(|&t| self.topic_label(t)),
                )
            } else if e.kind == NodeKind::HttpEndpoint {
                (FlowStepKind::Route, None)
            } else if e.kind == NodeKind::Schedule {
                (FlowStepKind::Cron, None)
            } else {
                (FlowStepKind::Handler, None)
            };
            self.site_step(kind, e, topic, None, confidence)
        };
        // A site is anchored on its enclosing callable and, for a Go constructor,
        // on the methods of the struct it builds: the walk starts from all of them.
        let roots: Vec<NodeId> = if p.kind == NodeKind::Callable {
            vec![p.id]
        } else {
            self.site_callables(p).iter().map(|c| c.id).collect()
        };
        if !roots.is_empty() {
            let walk = self.call_walk(idx, &roots, FlowDirection::Reverse, bounds, limits);
            cut = walk.cut;
            for &x in &walk.order {
                for &e in idx.entries_in.get(&x).map(Vec::as_slice).unwrap_or(&[]) {
                    if e.id == p.id {
                        continue;
                    }
                    if hide(e) {
                        *hidden += 1;
                        continue;
                    }
                    let (calls, root) = self.call_steps(&walk, x);
                    let mut steps = vec![self.rooted_start(p, &start, root)];
                    steps.extend(calls);
                    steps.push(entry_step(e, self.anchored_confidence(e, x)));
                    segs.push(Segment {
                        steps,
                        fallback: false,
                    });
                }
            }
        }
        let call_linked = !segs.is_empty();
        if !call_linked && !cut {
            let mut same_file: Vec<&ContractNode> = self
                .get_nodes_for_file(&p.file_path)
                .iter()
                .filter(|id| **id != p.id && idx.consumer_ids.contains(id))
                .filter_map(|id| self.nodes.get(id))
                .collect();
            sort_nodes(&mut same_file);
            for e in same_file {
                if hide(e) {
                    *hidden += 1;
                    continue;
                }
                segs.push(Segment {
                    steps: vec![start.clone(), entry_step(e, EdgeConfidence::Heuristic)],
                    fallback: true,
                });
            }
        }
        Self::cap_segments(&mut segs, p, bounds, limits);
        (segs, cut && !call_linked)
    }

    /// The engine: BFS over event levels from `starts`, then the hops reordered
    /// depth-first. `seen_topics` are the topics of the entry itself.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn trace_from<'g>(
        &'g self,
        mut starts: Vec<&'g ContractNode>,
        start_kind: FlowStepKind,
        entry_topic: Option<CompactStr>,
        mut seen_topics: HashSet<NodeId>,
        direction: FlowDirection,
        bounds: FlowBounds,
        hide: &dyn Fn(&ContractNode) -> bool,
    ) -> FlowTrace<'g> {
        let idx = self.flow_index();
        sort_nodes(&mut starts);
        let mut hidden = 0;
        let mut limits = Vec::new();
        let mut seen_entries: HashSet<NodeId> = HashSet::new();
        let mut queue: VecDeque<Pending<'g>> = VecDeque::new();
        let mut entries = Vec::new();
        let mut entries_more = 0;
        for s in starts {
            if hide(s) {
                hidden += 1;
                continue;
            }
            if entries.len() >= bounds.fan_out {
                entries_more += 1;
                continue;
            }
            if seen_entries.insert(s.id) {
                entries.push(s);
                queue.push_back(Pending {
                    node: s,
                    level: 1,
                    parent: None,
                    slot: 0,
                    topic: entry_topic
                        .as_ref()
                        .map(|t| self.entry_label(&idx, s.id, direction, &seen_topics, t)),
                });
            }
        }
        let mut hops: Vec<FlowHop<'g>> = Vec::new();
        let mut unexpanded = Vec::new();
        let mut chain_end = Vec::new();
        let mut reached = Vec::new();
        let mut topics = Vec::new();
        let mut capped = false;
        while let Some(p) = queue.pop_front() {
            if hops.len() >= MAX_FLOW_HOPS {
                capped = true;
                break;
            }
            let kind = if p.level == 1 {
                start_kind
            } else {
                match direction {
                    FlowDirection::Forward => FlowStepKind::Consume,
                    FlowDirection::Reverse => FlowStepKind::Produce,
                }
            };
            let topic = match kind {
                FlowStepKind::Consume | FlowStepKind::Produce => p.topic.clone().or_else(|| {
                    let ts = match direction {
                        FlowDirection::Forward => idx.consumes.get(&p.node.id),
                        FlowDirection::Reverse => idx.produces.get(&p.node.id),
                    };
                    ts.and_then(|t| t.first()).map(|&t| self.topic_label(t))
                }),
                _ => None,
            };
            let within = if p.node.kind == NodeKind::Callable {
                None
            } else {
                self.site_callables(p.node).first().copied()
            };
            let start = self.site_step(
                kind,
                p.node,
                topic,
                within,
                self.topic_confidence(p.node.id),
            );
            let (segs, cut) = match direction {
                FlowDirection::Forward => self.forward_segments(
                    &idx,
                    p.node,
                    start.clone(),
                    bounds,
                    &mut limits,
                    hide,
                    &mut hidden,
                ),
                FlowDirection::Reverse => self.reverse_segments(
                    &idx,
                    p.node,
                    start.clone(),
                    bounds,
                    &mut limits,
                    hide,
                    &mut hidden,
                ),
            };
            let parent_conf = p
                .parent
                .and_then(|i| hops.get(i))
                .map_or(EdgeConfidence::Exact, |h| h.chain_confidence);
            if segs.is_empty() {
                if hops.len() >= MAX_FLOW_HOPS {
                    capped = true;
                    break;
                }
                if !cut {
                    chain_end.push(p.node);
                }
                hops.push(FlowHop {
                    id: String::new(),
                    level: p.level,
                    parent: p.parent,
                    confidence: start.confidence,
                    chain_confidence: weakest(parent_conf, start.confidence),
                    steps: vec![start],
                    fallback: false,
                    next: Vec::new(),
                    end: Some(if cut {
                        FlowEnd::CallDepthLimit
                    } else {
                        FlowEnd::ChainEnd
                    }),
                    parent_slot: p.slot,
                });
                continue;
            }
            for seg in segs {
                if hops.len() >= MAX_FLOW_HOPS {
                    capped = true;
                    break;
                }
                let hop_idx = hops.len();
                let confidence = seg
                    .steps
                    .iter()
                    .fold(EdgeConfidence::Exact, |acc, s| weakest(acc, s.confidence));
                let Some(last) = seg.steps.last() else {
                    continue;
                };
                // Forward: topics the publish goes to. Reverse: topics a consumer
                // entry listens to; a route / handler entry is where the chain starts.
                let (topic_ids, end) = match direction {
                    FlowDirection::Forward => (idx.produces.get(&last.node.id), None),
                    FlowDirection::Reverse if last.kind == FlowStepKind::Consume => {
                        (idx.consumes.get(&last.node.id), None)
                    }
                    FlowDirection::Reverse => (None, Some(FlowEnd::Origin)),
                };
                let mut next = Vec::new();
                for &t in topic_ids.map(Vec::as_slice).unwrap_or(&[]) {
                    let label = self.topic_label(t);
                    if !seen_topics.insert(t) {
                        next.push(FlowNext {
                            topic: label,
                            outcome: FlowOutcome::AlreadyTraced,
                            more: 0,
                        });
                        continue;
                    }
                    if let Some(n) = self.get_node(t) {
                        topics.push(n);
                    }
                    let counterpart = match direction {
                        FlowDirection::Forward => idx.consumes.get(&t),
                        FlowDirection::Reverse => idx.produced_by.get(&t),
                    };
                    let mut others: Vec<&ContractNode> = counterpart
                        .map(Vec::as_slice)
                        .unwrap_or(&[])
                        .iter()
                        .filter(|id| **id != last.node.id)
                        .filter_map(|id| self.get_node(*id))
                        .collect();
                    sort_nodes(&mut others);
                    let before = others.len();
                    others.retain(|n| !hide(n));
                    hidden += before - others.len();
                    if others.is_empty() {
                        next.push(FlowNext {
                            topic: label,
                            outcome: FlowOutcome::NoCounterpart,
                            more: 0,
                        });
                        continue;
                    }
                    if p.level >= bounds.event_depth {
                        let mut n = 0;
                        for o in others {
                            if seen_entries.insert(o.id) {
                                unexpanded.push(o);
                                reached.push(o);
                                n += 1;
                            }
                        }
                        next.push(FlowNext {
                            topic: label,
                            outcome: FlowOutcome::DepthLimit(n),
                            more: 0,
                        });
                        continue;
                    }
                    let more = others.len().saturating_sub(bounds.fan_out);
                    others.truncate(bounds.fan_out);
                    for o in others {
                        if seen_entries.insert(o.id) {
                            reached.push(o);
                            queue.push_back(Pending {
                                node: o,
                                level: p.level + 1,
                                parent: Some(hop_idx),
                                slot: next.len(),
                                topic: Some(label.clone()),
                            });
                        }
                    }
                    next.push(FlowNext {
                        topic: label,
                        outcome: FlowOutcome::Followed(Vec::new()),
                        more,
                    });
                }
                hops.push(FlowHop {
                    id: String::new(),
                    level: p.level,
                    parent: p.parent,
                    steps: seg.steps,
                    confidence,
                    chain_confidence: weakest(parent_conf, confidence),
                    fallback: seg.fallback,
                    next,
                    end,
                    parent_slot: p.slot,
                });
            }
            if capped {
                break;
            }
        }
        let hops = Self::depth_first(hops);
        sort_nodes(&mut unexpanded);
        sort_nodes(&mut chain_end);
        limits.sort();
        limits.dedup();
        FlowTrace {
            target: CompactStr::default(),
            entry_kind: "topic",
            direction,
            bounds,
            entries,
            hops,
            unexpanded,
            chain_end,
            reached,
            topics,
            limits,
            hidden,
            capped,
            entries_more,
        }
    }

    /// Reorders hops (created breadth-first, parents first) depth-first, assigns
    /// the hierarchical ids, remaps `parent` and fills `Followed` with the
    /// children of each hop (iterative, explicit stack).
    fn depth_first(hops: Vec<FlowHop<'_>>) -> Vec<FlowHop<'_>> {
        let n = hops.len();
        let mut children: Vec<Vec<usize>> = vec![Vec::new(); n];
        let mut roots = Vec::new();
        for (i, h) in hops.iter().enumerate() {
            match h.parent {
                Some(p) if p < n => children[p].push(i),
                _ => roots.push(i),
            }
        }
        let mut order: Vec<(usize, String)> = Vec::with_capacity(n);
        let mut stack: Vec<(usize, String)> = roots
            .iter()
            .enumerate()
            .rev()
            .map(|(k, &i)| (i, (k + 1).to_string()))
            .collect();
        while let Some((i, id)) = stack.pop() {
            for (k, &c) in children[i].iter().enumerate().rev() {
                stack.push((c, format!("{id}.{}", k + 1)));
            }
            order.push((i, id));
        }
        let mut new_index = vec![0usize; n];
        for (pos, (i, _)) in order.iter().enumerate() {
            new_index[*i] = pos;
        }
        // The `next` entry (topic) each hop was reached through, read before the move.
        let slot_of: Vec<usize> = hops.iter().map(|h| h.parent_slot).collect();
        let mut slots: Vec<Option<FlowHop<'_>>> = hops.into_iter().map(Some).collect();
        let mut out = Vec::with_capacity(n);
        for (i, id) in order {
            let Some(mut h) = slots.get_mut(i).and_then(Option::take) else {
                continue;
            };
            h.id = id;
            h.parent = h.parent.and_then(|p| new_index.get(p).copied());
            for &c in &children[i] {
                if let Some(FlowNext {
                    outcome: FlowOutcome::Followed(v),
                    ..
                }) = h.next.get_mut(slot_of[c])
                {
                    v.push(new_index[c]);
                }
            }
            // Counterparts queued but never expanded: the hop cap stopped the trace.
            for nx in h.next.iter_mut() {
                if matches!(&nx.outcome, FlowOutcome::Followed(v) if v.is_empty()) {
                    nx.outcome = FlowOutcome::HopLimit;
                }
            }
            out.push(h);
        }
        out
    }
}
