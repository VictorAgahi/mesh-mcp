//! Mermaid rendering of an event-centred flow view (`visualize_mesh` with `event`, 9.0 ticket
//! T13).
//!
//! The chains come from the chain engine (`ContractGraph::trace_flow`, the engine of the
//! `trace_flow` tool): a reverse trace gives the producers upstream of the centred event, a
//! forward trace the consumers downstream. This module only folds those traces into one graph
//! (an event node per topic, a code node per consumer / producer / handler, call hops collapsed
//! into an edge label unless `detail` is set), prunes it to a node cap and prints it as a
//! `flowchart TD`.
//!
//! Nothing is dropped silently: a node cut by the cap leaves a "not shown (cap)" stub on its
//! neighbour, an engine bound (depth, fan-out, hop limit) leaves a stub, and a site or event
//! whose continuation is not statically resolved leaves a dashed "not statically resolved"
//! node, never a bare dead end. Every label is escaped for Mermaid and goes through the shared
//! name gates; the full names are in the table below the diagram.

use crate::anomalies::{shown_key, REDACTED};
use crate::markdown::{MarkdownFormatter, MAX_OUTPUT_BYTES, NON_RESULT_RESERVE_BYTES};
use mesh_core::member_consts::name_is_printable;
use mesh_core::{
    split_qualified_topic_key, Broker, CompactStr, ContractGraph, ContractNode, EdgeConfidence,
    FlowBounds, FlowEnd, FlowLimit, FlowOutcome, FlowStepKind, FlowTrace, NodeId,
};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Write as _;
use std::path::PathBuf;

/// Longest label inside the diagram, in characters; the table below carries the full name.
const LABEL_CHARS: usize = 40;
/// Longest name in the table, in characters.
const TABLE_NAME_CHARS: usize = 160;
/// Most characters of an edge label.
const EDGE_LABEL_CHARS: usize = 24;
/// Rendered payload budget, under the 48 KB MCP cap with room for the notes.
const BUDGET: usize = MAX_OUTPUT_BYTES - NON_RESULT_RESERVE_BYTES - 2 * 1024;

/// Everything one flow view is built from.
pub struct FlowViewInput<'a, 'g> {
    /// The event as the caller wrote it.
    pub event: &'a str,
    /// The topic key the event was matched as, and the matched hubs (`ContractGraph::flow_centres`).
    pub key: &'a str,
    pub hubs: &'a [CompactStr],
    pub graph: &'g ContractGraph,
    /// Reverse trace (producers upstream), when the view goes up.
    pub up: Option<&'a FlowTrace<'g>>,
    /// Forward trace (consumers downstream), when the view goes down.
    pub down: Option<&'a FlowTrace<'g>>,
    pub roots: &'a [PathBuf],
    /// Show every call between a consumer and its publish as a node.
    pub detail: bool,
    /// Most nodes drawn (events and code nodes; the stubs are extra).
    pub limit: usize,
    pub tests_hidden: bool,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Kind {
    Centre,
    Event,
    Consumer,
    Producer,
    Call,
    Origin,
}

impl Kind {
    fn word(self) -> &'static str {
        match self {
            Kind::Centre => "centre event",
            Kind::Event => "event",
            Kind::Consumer => "consumer",
            Kind::Producer => "producer",
            Kind::Call => "call",
            Kind::Origin => "entry",
        }
    }
}

struct VNode {
    kind: Kind,
    /// The name as shown (already through the secret gates).
    name: String,
    place: String,
    /// A topic whose name is a placeholder or a wildcard: not statically resolved.
    dynamic: bool,
    /// Reached again by a cycle or a join: drawn once.
    revisit: bool,
}

struct VEdge {
    from: usize,
    to: usize,
    conf: EdgeConfidence,
    label: String,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum MarkKind {
    /// Not statically resolved: the flow may continue at runtime.
    Unresolved,
    /// A bound (depth, fan-out, hop limit) cut the walk here.
    Bound,
}

struct Marker {
    parent: usize,
    /// Drawn above the parent (an upstream continuation) instead of below.
    upstream: bool,
    kind: MarkKind,
    text: String,
}

#[derive(Default)]
struct Totals {
    depth_up: usize,
    depth_down: usize,
    fan_out_more: usize,
    hop_limit: bool,
}

struct Builder<'a, 'g> {
    input: &'a FlowViewInput<'a, 'g>,
    centres: Vec<String>,
    nodes: Vec<VNode>,
    edges: Vec<VEdge>,
    markers: Vec<Marker>,
    events: HashMap<String, usize>,
    sites: HashMap<NodeId, usize>,
    edge_seen: HashSet<(usize, usize)>,
    marker_seen: HashSet<(usize, bool, String)>,
    totals: Totals,
}

/// Whether `label` (a hub name or a topic key) is the key itself or one of its hubs.
fn bare_key(label: &str) -> &str {
    let label = label.strip_suffix(" (literal)").unwrap_or(label);
    split_qualified_topic_key(label).map_or(label, |(_, bare)| bare)
}

fn weakest(a: EdgeConfidence, b: EdgeConfidence) -> EdgeConfidence {
    let rank = |c: EdgeConfidence| match c {
        EdgeConfidence::Exact => 0,
        EdgeConfidence::Heuristic => 1,
        EdgeConfidence::Ambiguous => 2,
    };
    if rank(a) >= rank(b) {
        a
    } else {
        b
    }
}

/// A code name that is safe to print: the shared name gate, else the redaction marker.
fn shown_site(name: &str) -> String {
    if name_is_printable(name) {
        name.replace(['`', '\n', '\r'], "'")
    } else {
        REDACTED.to_string()
    }
}

impl<'a, 'g> Builder<'a, 'g> {
    fn new(input: &'a FlowViewInput<'a, 'g>) -> Self {
        // A key (`orders`, `kafka:orders`) centres on its hubs, one per broker; a lone hub is
        // its own centre. A target that only matched several events by substring has no single
        // event: the key stands for the group (and the output says so).
        let by_key = input.hubs.iter().any(|h| bare_key(h) == bare_key(input.key));
        let centres: Vec<String> = if by_key || input.hubs.len() == 1 {
            input.hubs.iter().map(ToString::to_string).collect()
        } else {
            vec![input.key.to_string()]
        };
        let mut b = Builder {
            input,
            centres,
            nodes: Vec::new(),
            edges: Vec::new(),
            markers: Vec::new(),
            events: HashMap::new(),
            sites: HashMap::new(),
            edge_seen: HashSet::new(),
            marker_seen: HashSet::new(),
            totals: Totals::default(),
        };
        for c in b.centres.clone() {
            b.event(&c);
        }
        b
    }

    fn is_centre(&self, label: &str) -> bool {
        self.centres.iter().any(|c| c == label)
    }

    /// The node of the event `label` (a hub name), created on first use.
    fn event(&mut self, label: &str) -> usize {
        if let Some(&i) = self.events.get(label) {
            return i;
        }
        let (qualifier, rest) = match split_qualified_topic_key(label) {
            Some((b, bare)) => (b.slug(), bare),
            None => (
                self.input
                    .graph
                    .hub_label_broker(label)
                    .map_or("?", Broker::slug),
                label,
            ),
        };
        let name = format!("{qualifier}:{}", shown_key(rest));
        let kind = if self.is_centre(label) {
            Kind::Centre
        } else {
            Kind::Event
        };
        let i = self.nodes.len();
        self.nodes.push(VNode {
            kind,
            name,
            place: String::new(),
            dynamic: label.contains("${") || label.contains('*'),
            revisit: false,
        });
        self.events.insert(label.to_string(), i);
        i
    }

    /// The events a step's topic label names: a site on several queried hubs is labelled
    /// `kafka:orders, rabbitmq:orders` by the engine, and a site whose hub the engine could not
    /// name carries the key as the caller wrote it (it joins every centre).
    fn events_of(&mut self, label: Option<&str>) -> Vec<usize> {
        let all = |b: &mut Self| -> Vec<usize> {
            let cs = b.centres.clone();
            cs.iter().map(|c| b.event(c)).collect()
        };
        let Some(label) = label else {
            return all(self);
        };
        if self.is_centre(label) {
            return vec![self.event(label)];
        }
        if label.contains(", ") {
            let parts: Vec<&str> = label.split(", ").collect();
            if parts.iter().all(|p| self.is_centre(p)) {
                return parts.into_iter().map(|p| self.event(p)).collect();
            }
        }
        if label == self.input.key || bare_key(label) == bare_key(self.input.key) {
            return all(self);
        }
        vec![self.event(label)]
    }

    fn site(&mut self, node: &ContractNode, kind: Kind) -> usize {
        if let Some(&i) = self.sites.get(&node.id) {
            return i;
        }
        let name = match kind {
            // `VERB /path`: the verb is a word, the path goes through the name gate.
            Kind::Origin => match mesh_core::routes::display_route(node, None)
                .as_deref()
                .and_then(|r| r.split_once(' '))
            {
                // `{id}` is path-parameter syntax, not a secret shape.
                Some((verb, path)) if name_is_printable(&path.replace(['{', '}'], "")) => {
                    format!("{verb} {}", path.replace(['`', '\n', '\r'], "'"))
                }
                Some((verb, _)) => format!("{verb} {REDACTED}"),
                None => shown_site(&node.name),
            },
            // The callable around the site; at module level only the site's own name is
            // known (often the topic), so the role is spelled out.
            _ => match self
                .input
                .graph
                .enclosing_callable(node)
                .filter(|_| node.kind != mesh_core::NodeKind::Callable)
            {
                Some(c) => shown_site(&c.name),
                None => match kind {
                    Kind::Producer => format!("publish {}", shown_site(&node.name)),
                    Kind::Consumer => format!("consume {}", shown_site(&node.name)),
                    _ => shown_site(&node.name),
                },
            },
        };
        let i = self.nodes.len();
        self.nodes.push(VNode {
            kind,
            name,
            place: MarkdownFormatter::location(node, self.input.roots),
            dynamic: false,
            revisit: false,
        });
        self.sites.insert(node.id, i);
        i
    }

    fn edge(&mut self, from: usize, to: usize, conf: EdgeConfidence, label: String) {
        if from != to && self.edge_seen.insert((from, to)) {
            self.edges.push(VEdge {
                from,
                to,
                conf,
                label,
            });
        }
    }

    fn marker(&mut self, parent: usize, upstream: bool, kind: MarkKind, text: String) {
        if self.marker_seen.insert((parent, upstream, text.clone())) {
            self.markers.push(Marker {
                parent,
                upstream,
                kind,
                text,
            });
        }
    }

    /// `next` of a hop: what happens past the topic it publishes (down) or consumes (up).
    fn outcomes(&mut self, hop: &mesh_core::FlowHop<'_>, from: usize, conf: EdgeConfidence, up: bool, bounds: FlowBounds) {
        let counterpart = if up { "producer" } else { "consumer" };
        for nx in &hop.next {
            for ev in self.events_of(Some(&nx.topic)) {
                if up {
                    self.edge(ev, from, conf, String::new());
                } else {
                    self.edge(from, ev, conf, String::new());
                }
                match &nx.outcome {
                    FlowOutcome::Followed(_) => {}
                    FlowOutcome::NoCounterpart => self.marker(
                        ev,
                        up,
                        MarkKind::Unresolved,
                        format!("no {counterpart} found"),
                    ),
                    FlowOutcome::DepthLimit(n) => {
                        if up {
                            self.totals.depth_up += n;
                        } else {
                            self.totals.depth_down += n;
                        }
                        self.marker(
                            ev,
                            up,
                            MarkKind::Bound,
                            format!(
                                "{n} {counterpart}(s) not expanded: depth limit ({})",
                                bounds.event_depth
                            ),
                        );
                    }
                    FlowOutcome::AlreadyTraced => self.nodes[ev].revisit = true,
                    FlowOutcome::HopLimit => {
                        self.totals.hop_limit = true;
                        self.marker(ev, up, MarkKind::Bound, "not traced: hop limit".to_string());
                    }
                }
                if nx.more > 0 {
                    self.totals.fan_out_more += nx.more;
                    self.marker(
                        ev,
                        up,
                        MarkKind::Bound,
                        format!(
                            "+{} more {counterpart}(s) not followed (fan_out {})",
                            nx.more, bounds.fan_out
                        ),
                    );
                }
            }
        }
    }

    fn hop_end_marker(&mut self, hop: &mesh_core::FlowHop<'_>, at: usize, up: bool, bounds: FlowBounds) {
        match hop.end {
            Some(FlowEnd::ChainEnd) => {
                let what = if up { "entry" } else { "publish" };
                self.marker(
                    at,
                    up,
                    MarkKind::Unresolved,
                    format!("no {what} found"),
                );
            }
            Some(FlowEnd::CallDepthLimit) => self.marker(
                at,
                up,
                MarkKind::Bound,
                format!("call depth limit ({})", bounds.call_depth),
            ),
            Some(FlowEnd::Origin) | None => {}
        }
    }

    fn link_label(calls: usize, fallback: bool) -> String {
        if fallback {
            "same file".to_string()
        } else if calls == 0 {
            String::new()
        } else if calls == 1 {
            "1 call".to_string()
        } else {
            format!("{calls} calls")
        }
    }

    /// Folds the forward trace: centre -> consumer -> calls -> producer -> event -> ...
    fn add_down(&mut self, trace: &FlowTrace<'g>) {
        let bounds = trace.bounds;
        for hop in &trace.hops {
            let Some(start) = hop.steps.first() else {
                continue;
            };
            let structural = if hop.fallback {
                EdgeConfidence::Heuristic
            } else {
                EdgeConfidence::Exact
            };
            let first = self.site(start.node, Kind::Consumer);
            for ev in self.events_of(start.topic.as_deref()) {
                self.edge(ev, first, start.confidence, String::new());
            }
            let mut prev = first;
            let mut calls = 0usize;
            let mut calls_conf = EdgeConfidence::Exact;
            let mut produced: Option<(usize, EdgeConfidence)> = None;
            let last_call = hop
                .steps
                .iter()
                .rposition(|s| s.kind == FlowStepKind::Call);
            let owner = hop
                .steps
                .last()
                .and_then(|p| self.input.graph.enclosing_callable(p.node))
                .map(|c| c.id);
            for (k, step) in hop.steps.iter().enumerate().skip(1) {
                match step.kind {
                    // The last call is the callable holding the publish: the producer node
                    // stands for it.
                    FlowStepKind::Call if self.input.detail && Some(k) == last_call && Some(step.node.id) == owner => {}
                    FlowStepKind::Call if self.input.detail => {
                        let n = self.site(step.node, Kind::Call);
                        self.edge(prev, n, step.confidence, String::new());
                        prev = n;
                    }
                    FlowStepKind::Call => {
                        calls += 1;
                        calls_conf = weakest(calls_conf, step.confidence);
                    }
                    FlowStepKind::Produce => {
                        let p = self.site(step.node, Kind::Producer);
                        let (conf, label) = if self.input.detail {
                            (structural, Self::link_label(0, hop.fallback))
                        } else {
                            (
                                weakest(calls_conf, structural),
                                Self::link_label(calls, hop.fallback),
                            )
                        };
                        self.edge(prev, p, conf, label);
                        prev = p;
                        produced = Some((p, step.confidence));
                    }
                    _ => {}
                }
            }
            match produced {
                Some((p, conf)) => self.outcomes(hop, p, conf, false, bounds),
                None => self.hop_end_marker(hop, prev, false, bounds),
            }
        }
        if trace.entries_more > 0 {
            self.totals.fan_out_more += trace.entries_more;
            for c in self.centres() {
                self.marker(
                    c,
                    false,
                    MarkKind::Bound,
                    format!(
                        "+{} more consumer site(s) not followed (fan_out {})",
                        trace.entries_more, bounds.fan_out
                    ),
                );
            }
        }
        if trace.capped {
            self.totals.hop_limit = true;
        }
    }

    /// Folds the reverse trace: entry -> calls -> producer -> centre event, upstream first.
    fn add_up(&mut self, trace: &FlowTrace<'g>) {
        let bounds = trace.bounds;
        for hop in &trace.hops {
            let Some(start) = hop.steps.first() else {
                continue;
            };
            let structural = if hop.fallback {
                EdgeConfidence::Heuristic
            } else {
                EdgeConfidence::Exact
            };
            let producer = self.site(start.node, Kind::Producer);
            for ev in self.events_of(start.topic.as_deref()) {
                self.edge(producer, ev, start.confidence, String::new());
            }
            // Walk order: the producer, the callers outwards, the entry last; drawn the other
            // way round (callers above).
            let mut below = producer;
            let mut calls = 0usize;
            let mut calls_conf = EdgeConfidence::Exact;
            let mut entry: Option<(usize, FlowStepKind, EdgeConfidence)> = None;
            let mut chain: Vec<(usize, EdgeConfidence)> = Vec::new();
            let last_call = hop
                .steps
                .iter()
                .rposition(|s| s.kind == FlowStepKind::Call);
            let owner = hop
                .steps
                .last()
                .and_then(|e| self.input.graph.enclosing_callable(e.node))
                .map(|c| c.id);
            for (k, step) in hop.steps.iter().enumerate().skip(1) {
                match step.kind {
                    // The last caller is the callable holding the entry site: the entry node
                    // stands for it.
                    FlowStepKind::Call if self.input.detail && Some(k) == last_call && Some(step.node.id) == owner => {}
                    FlowStepKind::Call if self.input.detail => {
                        let n = self.site(step.node, Kind::Call);
                        chain.push((n, step.confidence));
                    }
                    FlowStepKind::Call => {
                        calls += 1;
                        calls_conf = weakest(calls_conf, step.confidence);
                    }
                    FlowStepKind::Consume => {
                        let n = self.site(step.node, Kind::Consumer);
                        entry = Some((n, step.kind, step.confidence));
                    }
                    FlowStepKind::Route | FlowStepKind::Handler | FlowStepKind::Cron => {
                        let n = self.site(step.node, Kind::Origin);
                        entry = Some((n, step.kind, step.confidence));
                    }
                    FlowStepKind::Produce => {}
                }
            }
            if let Some((e, kind, conf)) = entry {
                // Detail: entry -> caller_n -> ... -> caller_1 -> producer; a call edge carries
                // the confidence of the call it stands for.
                for (n, c) in chain {
                    self.edge(n, below, c, String::new());
                    below = n;
                }
                let (conf_link, label) = if self.input.detail {
                    (structural, Self::link_label(0, hop.fallback))
                } else {
                    (
                        weakest(calls_conf, structural),
                        Self::link_label(calls, hop.fallback),
                    )
                };
                self.edge(e, below, conf_link, label);
                if kind == FlowStepKind::Consume {
                    self.outcomes(hop, e, conf, true, bounds);
                }
            } else {
                self.hop_end_marker(hop, producer, true, bounds);
            }
        }
        if trace.entries_more > 0 {
            self.totals.fan_out_more += trace.entries_more;
            for c in self.centres() {
                self.marker(
                    c,
                    true,
                    MarkKind::Bound,
                    format!(
                        "+{} more producer site(s) not followed (fan_out {})",
                        trace.entries_more, bounds.fan_out
                    ),
                );
            }
        }
        if trace.capped {
            self.totals.hop_limit = true;
        }
    }

    fn centres(&self) -> Vec<usize> {
        (0..self.nodes.len())
            .filter(|&i| self.nodes[i].kind == Kind::Centre)
            .collect()
    }

    /// A centre with no edge on a side the view goes to reads "no ... statically resolved",
    /// never as a bare end.
    fn mark_empty_centres(&mut self, up: bool, down: bool) {
        for c in self.centres() {
            let has_up = self.edges.iter().any(|e| e.to == c);
            let has_down = self.edges.iter().any(|e| e.from == c);
            if up && !has_up {
                self.marker(
                    c,
                    true,
                    MarkKind::Unresolved,
                    "no producer found".to_string(),
                );
            }
            if down && !has_down {
                self.marker(
                    c,
                    false,
                    MarkKind::Unresolved,
                    "no consumer found".to_string(),
                );
            }
        }
    }
}

/// Mermaid-safe label text: every character that Mermaid, markdown or an HTML label could read
/// as syntax becomes a numeric entity (`#34;`), as does every non-ASCII character; control
/// characters become spaces.
pub fn mermaid_escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '\n' | '\r' | '\t' => out.push(' '),
            c if c.is_control() => {}
            c if c.is_ascii_alphanumeric() || " :-_.,/+=@$?!^'*~%".contains(c) => out.push(c),
            c => {
                let _ = write!(out, "#{};", c as u32);
            }
        }
    }
    out
}

fn clamp(text: &str, max: usize) -> String {
    if text == REDACTED {
        return text.to_string();
    }
    MarkdownFormatter::clamp_chars(text, max)
}

/// The last `max` characters of `text` behind `...`: a place keeps its `:line`.
fn clamp_tail(text: &str, max: usize) -> String {
    let n = text.chars().count();
    if n <= max {
        return text.to_string();
    }
    let tail: String = text.chars().skip(n - max.saturating_sub(3)).collect();
    format!("...{tail}")
}

/// A table cell: backticks and pipes neutralised, bounded, as inline code.
fn cell(text: &str) -> String {
    let t: String = text
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect();
    let t = clamp(&t, TABLE_NAME_CHARS).replace('`', "'").replace('|', "\\|");
    format!("`{t}`")
}

struct Drawn {
    text: String,
    shown: usize,
    total: usize,
    cut: usize,
}

/// Prints the graph with at most `limit` real nodes (BFS order from the centres).
fn draw(g: &Builder<'_, '_>, limit: usize) -> Drawn {
    let n = g.nodes.len();
    // BFS over the undirected edges from the centres: a kept prefix is always connected.
    let mut adj: Vec<Vec<usize>> = vec![Vec::new(); n];
    for e in &g.edges {
        adj[e.from].push(e.to);
        adj[e.to].push(e.from);
    }
    let mut order: Vec<usize> = Vec::with_capacity(n);
    let mut seen = vec![false; n];
    let mut queue: VecDeque<usize> = VecDeque::new();
    for c in g.centres() {
        seen[c] = true;
        queue.push_back(c);
    }
    while let Some(v) = queue.pop_front() {
        order.push(v);
        for &w in &adj[v] {
            if !seen[w] {
                seen[w] = true;
                queue.push_back(w);
            }
        }
    }
    // Nodes no centre reaches cannot exist (every edge hangs off a centre chain); keep them
    // out rather than drawing a floating node.
    let total = order.len();
    let mut keep = vec![false; n];
    for &v in order.iter().take(limit.max(1)) {
        keep[v] = true;
    }
    let shown = keep.iter().filter(|k| **k).count();
    let kept: Vec<usize> = (0..n).filter(|&i| keep[i]).collect();

    // Neighbours cut by the cap, per kept node and side.
    let mut cut_up = vec![0usize; n];
    let mut cut_down = vec![0usize; n];
    for e in &g.edges {
        if keep[e.from] && !keep[e.to] && seen[e.to] {
            cut_down[e.from] += 1;
        }
        if keep[e.to] && !keep[e.from] && seen[e.from] {
            cut_up[e.to] += 1;
        }
    }

    let mut out = String::from("flowchart TD\n");
    let mut revisits: Vec<usize> = Vec::new();
    for &i in &kept {
        let v = &g.nodes[i];
        let mut label = clamp(&v.name, LABEL_CHARS);
        label = mermaid_escape(&label);
        if !v.place.is_empty() {
            let place = mermaid_escape(&clamp_tail(&v.place, LABEL_CHARS + 20));
            label = format!("{label}<br/>{place}");
        }
        let (open, close) = match v.kind {
            Kind::Centre | Kind::Event => ("([", "])"),
            Kind::Origin => ("[/", "/]"),
            _ => ("[", "]"),
        };
        let class = match v.kind {
            Kind::Centre => "centre",
            Kind::Event if v.dynamic => "unresolved",
            Kind::Event => "event",
            Kind::Consumer | Kind::Producer => "site",
            Kind::Call => "call",
            Kind::Origin => "origin",
        };
        let _ = writeln!(out, "  n{i}{open}\"{label}\"{close}:::{class}");
        if v.revisit {
            revisits.push(i);
        }
    }

    let mut link_styles: Vec<(usize, EdgeConfidence)> = Vec::new();
    let mut links = 0usize;
    let mut arrow = |out: &mut String, from: usize, to: usize, conf: EdgeConfidence, label: &str| {
        let arrow = match conf {
            EdgeConfidence::Exact => "-->",
            _ => "-.->",
        };
        let label = match (conf, label.is_empty()) {
            (EdgeConfidence::Ambiguous, true) => "?".to_string(),
            (EdgeConfidence::Ambiguous, false) => format!("? {label}"),
            _ => label.to_string(),
        };
        if label.is_empty() {
            let _ = writeln!(out, "  n{from} {arrow} n{to}");
        } else {
            let label = mermaid_escape(&clamp(&label, EDGE_LABEL_CHARS));
            let _ = writeln!(out, "  n{from} {arrow}|\"{label}\"| n{to}");
        }
        if conf == EdgeConfidence::Ambiguous {
            link_styles.push((links, conf));
        }
        links += 1;
    };
    for e in &g.edges {
        if keep[e.from] && keep[e.to] {
            arrow(&mut out, e.from, e.to, e.conf, &e.label);
        }
    }

    // Stubs and markers hang off kept nodes, so none is a floating node.
    let mut stub_id = n;
    let mut stub = |out: &mut String, parent: usize, upstream: bool, text: &str, class: &str| {
        let id = stub_id;
        stub_id += 1;
        let text = mermaid_escape(&clamp(text, 80));
        let shape = if class == "unresolved" {
            format!("{{{{\"{text}\"}}}}")
        } else {
            format!("[[\"{text}\"]]")
        };
        let _ = writeln!(out, "  n{id}{shape}:::{class}");
        if upstream {
            let _ = writeln!(out, "  n{id} -.-> n{parent}");
        } else {
            let _ = writeln!(out, "  n{parent} -.-> n{id}");
        }
        links += 1;
    };
    for m in &g.markers {
        if keep[m.parent] {
            let class = if m.kind == MarkKind::Unresolved {
                "unresolved"
            } else {
                "bound"
            };
            let text = if m.kind == MarkKind::Unresolved {
                format!("not statically resolved: {}", m.text)
            } else {
                m.text.clone()
            };
            stub(&mut out, m.parent, m.upstream, &text, class);
        }
    }
    for &i in &kept {
        if cut_up[i] > 0 {
            stub(&mut out, i, true, &format!("+{} not shown (cap)", cut_up[i]), "bound");
        }
        if cut_down[i] > 0 {
            stub(&mut out, i, false, &format!("+{} not shown (cap)", cut_down[i]), "bound");
        }
    }

    for i in revisits {
        let _ = writeln!(out, "  class n{i} revisit");
    }
    for (idx, _) in &link_styles {
        let _ = writeln!(out, "  linkStyle {idx} stroke:#c92a2a,color:#c92a2a");
    }
    out.push_str("  classDef centre fill:#fff3bf,stroke:#e67700,stroke-width:4px,color:#212529\n");
    out.push_str("  classDef event fill:#e7f5ff,stroke:#1971c2,color:#212529\n");
    out.push_str("  classDef site fill:#f1f3f5,stroke:#495057,color:#212529\n");
    out.push_str("  classDef call fill:#f8f9fa,stroke:#adb5bd,color:#212529\n");
    out.push_str("  classDef origin fill:#ebfbee,stroke:#2f9e44,color:#212529\n");
    out.push_str("  classDef unresolved fill:#ffffff,stroke:#868e96,stroke-dasharray:4 3,color:#495057\n");
    out.push_str("  classDef bound fill:#fff9db,stroke:#f08c00,stroke-dasharray:2 2,color:#495057\n");
    out.push_str("  classDef revisit stroke-dasharray:6 3,stroke-width:3px\n");

    // Table of the full names.
    let mut table = String::from("| Node | Kind | Name | Location |\n|---|---|---|---|\n");
    for &i in &kept {
        let v = &g.nodes[i];
        let place = if v.place.is_empty() {
            String::new()
        } else {
            cell(&v.place)
        };
        let _ = writeln!(
            table,
            "| n{i} | {} | {} | {place} |",
            v.kind.word(),
            cell(&v.name)
        );
    }
    let text = format!("```mermaid\n{out}```\n\n{table}");
    Drawn {
        text,
        shown,
        total,
        cut: total - shown,
    }
}

/// The legend printed under every diagram.
const LEGEND: &str = "*Legend: a rounded node is an event (broker-qualified: `kafka:orders`; `?:` = broker not statically known), the thick yellow one is the centre; a box is a code site (`name`, `path:line`); a slanted box is where a chain starts (a route, handler or schedule). Solid arrow = exact, dashed arrow = heuristic, dashed red arrow labelled `?` = ambiguous (a chain is as strong as its weakest arrow). A dashed hexagon is NOT STATICALLY RESOLVED (the flow may continue at runtime: a runtime-built topic, a registry, an unindexed root); an orange stub is a bound (depth, fan-out, cap) that cut the walk, not an end. A dashed thick border is an event reached again by a cycle or a join, drawn once.*\n";

/// Renders the flow view: the header, the diagram, the legend, the table of full names and the
/// notes. Output stays under the 48 KB cap by halving the node cap until it fits.
pub fn render_flow_view(input: &FlowViewInput<'_, '_>) -> String {
    let mut g = Builder::new(input);
    if let Some(up) = input.up {
        g.add_up(up);
    }
    if let Some(down) = input.down {
        g.add_down(down);
    }
    g.mark_empty_centres(input.up.is_some(), input.down.is_some());

    let clean_event = clamp(&shown_key(input.event.trim()).replace('`', "'"), 80);
    let mut limit = input.limit.max(1);
    loop {
        let d = draw(&g, limit);
        let mut notes = String::new();
        if d.cut > 0 {
            let _ = writeln!(
                notes,
                "\n**{} node{} not shown (cap {limit})**, use hops/limit/scope: lower `hops_up` / `hops_down`, raise `limit` (max {}), set `direction` to one side, or centre the view on a nearer event. The orange `not shown (cap)` stubs mark where the diagram was cut; it is not the complete picture.",
                d.cut,
                if d.cut == 1 { "" } else { "s" },
                MAX_LIMIT
            );
        }
        let depth_note = |trace: Option<&FlowTrace<'_>>, name: &str, n: usize| -> Option<String> {
            let t = trace?;
            (n > 0).then(|| {
                format!(
                    "\n**Depth limit reached** (`{name}` {}): {n} {}(s) past the last event drawn were not expanded; {}.\n",
                    t.bounds.event_depth,
                    if name == "hops_up" { "producer" } else { "consumer" },
                    if t.bounds.event_depth >= mesh_core::MAX_EVENT_DEPTH {
                        format!("`{name}` is at its maximum ({})", mesh_core::MAX_EVENT_DEPTH)
                    } else {
                        format!("raise `{name}` (max {})", mesh_core::MAX_EVENT_DEPTH)
                    }
                )
            })
        };
        notes.extend(depth_note(input.down, "hops_down", g.totals.depth_down));
        notes.extend(depth_note(input.up, "hops_up", g.totals.depth_up));
        if g.markers.iter().any(|m| m.kind == MarkKind::Unresolved) {
            notes.push_str("\n**Chain end** at a dashed hexagon means the end of the STATICALLY RESOLVED chain (a runtime-built topic, a registry or an unindexed root may continue it), never proof that nothing follows.\n");
        }
        if g.totals.fan_out_more > 0 {
            let _ = writeln!(
                notes,
                "\n**Fan-out cap**: {} consumer/producer site(s) were not followed (`fan_out`, max {}); they are named in the orange stubs.",
                g.totals.fan_out_more,
                mesh_core::MAX_FAN_OUT
            );
        }
        for (name, trace) in [("down", input.down), ("up", input.up)] {
            let Some(t) = trace else { continue };
            let calls = t
                .limits
                .iter()
                .filter(|l| matches!(l, FlowLimit::CallDepth(_)))
                .count();
            if calls > 0 {
                let _ = writeln!(
                    notes,
                    "\n**Call depth limit reached** ({name}, call_depth {}) at {calls} callable(s) with further calls: chains through them may be incomplete.",
                    t.bounds.call_depth
                );
            }
            let fans = t
                .limits
                .iter()
                .filter(|l| matches!(l, FlowLimit::FanOut(..)))
                .count();
            if fans > 0 {
                let _ = writeln!(
                    notes,
                    "\n**Fan-out cap** ({name}, fan_out {}) was hit at {fans} callable(s): further calls were not followed.",
                    t.bounds.fan_out
                );
            }
        }
        if g.totals.hop_limit {
            let _ = writeln!(
                notes,
                "\n**Hop limit reached** ({}): the trace stopped early; the view is incomplete. Narrow with `hops_up` / `hops_down`.",
                mesh_core::MAX_FLOW_HOPS
            );
        }
        let hidden = input.up.map_or(0, |t| t.hidden) + input.down.map_or(0, |t| t.hidden);
        if hidden > 0 || input.tests_hidden {
            let _ = writeln!(
                notes,
                "\n*Test-file sites are left out (`include_tests: false`){}.*",
                if hidden > 0 {
                    format!(": {hidden} site(s) not drawn")
                } else {
                    String::new()
                }
            );
        }
        if g.centres.len() == 1 && input.hubs.len() > 1 && !g.is_centre(input.hubs[0].as_str()) {
            let _ = writeln!(
                notes,
                "\n**`{clean_event}` matched {} events by substring** (the centre stands for all of them). Pass an exact key (for example `{}`) to centre on one.",
                input.hubs.len(),
                input
                    .hubs
                    .first()
                    .map_or_else(String::new, |h| shown_key(h).replace('`', "'"))
            );
        }

        let bounds = input
            .up
            .map(|t| t.bounds)
            .or_else(|| input.down.map(|t| t.bounds))
            .unwrap_or_default();
        let header = format!(
            "## Event flow view for `{clean_event}`\n*Bounds (clamped): hops_up {}, hops_down {}, fan_out {}, call_depth {}, limit {limit}, detail {}. Nodes drawn: {} of {}.*\n\n",
            input.up.map_or("-".to_string(), |t| t.bounds.event_depth.to_string()),
            input.down.map_or("-".to_string(), |t| t.bounds.event_depth.to_string()),
            bounds.fan_out,
            bounds.call_depth,
            if input.detail { "on" } else { "off" },
            d.shown,
            d.total
        );
        let text = format!("{header}{}\n{LEGEND}{notes}", d.text);
        if text.len() <= BUDGET || limit <= 1 {
            return text;
        }
        limit = (limit / 2).max(1);
    }
}

/// Largest `limit` the tool accepts.
pub const MAX_LIMIT: usize = 200;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escape_never_leaves_syntax_characters() {
        let hostile = "a\"b[c](d){e}|f;g#h<i>j&k`l\\m\nn\u{e9}\u{4e2d}";
        let e = mermaid_escape(hostile);
        for bad in ['"', '[', ']', '(', ')', '{', '}', '|', ';', '<', '>', '&', '`', '\\', '\n'] {
            assert!(!e.contains(bad), "{bad:?} left in {e}");
        }
        assert!(e.is_ascii());
        assert_eq!(
            e,
            "a#34;b#91;c#93;#40;d#41;#123;e#125;#124;f#59;g#35;h#60;i#62;j#38;k#96;l#92;m n#233;#20013;"
        );
    }

    #[test]
    fn escape_keeps_plain_names_readable() {
        assert_eq!(mermaid_escape("kafka:order.requested-v2/x_y"), "kafka:order.requested-v2/x_y");
    }

    #[test]
    fn bare_key_strips_the_broker_and_the_literal_marker() {
        assert_eq!(bare_key("kafka:orders"), "orders");
        assert_eq!(bare_key("orders"), "orders");
        assert_eq!(bare_key("rabbitmq:orders (literal)"), "orders");
    }
}
