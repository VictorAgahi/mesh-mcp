//! `reconcile_delta`: [`ContractGraph::reconcile_edges`] restricted to the edges
//! a change can reach.
//!
//! Every mutation between two reconciles (`add_node`, `remove_nodes` through
//! `patch_files`, the fact registrations) is written to a [`ReconcileJournal`].
//! The delta reads it, works out which *units* of each edge family can resolve
//! differently, recomputes those units with the same code the full rebuild
//! runs, and keeps every other unit's edges as they were:
//!
//! - `Imports`, unit = importer. Dirty when one of its facts was added or
//!   removed, or when a target it imports looks up an index key (exact name,
//!   file stem, `package/name`, package) that a changed node sits under.
//! - topic edges, unit = topic key. Dirty when the topic's producer or consumer
//!   facts changed, when an event-bus stream node of that name changed, or, for
//!   a wildcard subscription, when a concrete subject it matches changed.
//!   Hubs left without facts are looked for among the changed nodes and the
//!   nodes named after a changed topic only.
//! - `Implements`, pair (handler, proto method). Recomputed for every handler
//!   that is new or whose resolved `Service.Method` changed (a `@` record of its
//!   own, or a `$` constant one of its decorator arguments names); its
//!   candidate methods come from [`GrpcIndex`], then the full pass's predicate.
//! - `CallsRpc`, unit = caller. Dirty when new, when its served method (a
//!   handler's resolved name) changed, when an inherited call goes through a
//!   class field binding that changed, or when one of its calls looks up a
//!   gRPC service name a changed service node carries.
//!
//! A `.proto` method change (the RPC namespace every handler and call resolves
//! against) rebuilds the `Implements` and `CallsRpc` families whole. The
//! `CallsRpc` family is also rebuilt whole when some caller's call records are
//! not contiguous in `rpc_calls`: its edges follow the records' order, which
//! regrouping kept and recomputed callers reproduces only when every caller's
//! calls form one run. A change
//! touching more than [`DELTA_MAX_CHANGED_PERCENT`] of the nodes, a journal that
//! overflowed or was never started, or an edge added by hand fall back to the
//! full rebuild. `reconcile_edges` stays the oracle: both must end in the same
//! graph, edge order included.

use super::reconcile::RpcLookups;
use super::*;

/// Past this many journaled entries the journal stops recording and the next
/// reconcile is a full one: a change that large is cheaper rebuilt than diffed.
const JOURNAL_CAP: usize = 1 << 18;

/// A change touching more than this share of the nodes (a branch switch, a mass
/// rewrite) is reconciled in full.
pub const DELTA_MAX_CHANGED_PERCENT: usize = 20;

/// Below this many changed nodes the share above is not checked: a small graph
/// is cheap to reconcile either way, and its deltas stay exercised.
const DELTA_MIN_CHANGED_FOR_SHARE: usize = 64;

/// How [`ContractGraph::reconcile_delta`] reconciled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconcileMode {
    /// Only the edges the journaled changes can reach were recomputed. With
    /// `grpc_full`, a `.proto` method changed and the `Implements` / `CallsRpc`
    /// families were rebuilt whole; with `calls_full`, only `CallsRpc` was
    /// (some caller's call records are interleaved with another's).
    Delta { grpc_full: bool, calls_full: bool },
    /// A full [`ContractGraph::reconcile_edges`], for the reason given.
    Full(&'static str),
}

/// Changes to a [`ContractGraph`] since its last reconcile. Recording starts at
/// the first reconcile (a graph being built from scratch is reconciled in full
/// anyway) and stops, for good until the next reconcile, past [`JOURNAL_CAP`]
/// entries or on a mutation it cannot describe.
#[derive(Debug, Clone, Default)]
pub(super) struct ReconcileJournal {
    recording: bool,
    entries: usize,
    /// Nodes added (some may have been removed again since).
    added: BTreeSet<NodeId>,
    /// Nodes removed, as they were.
    removed: Vec<ContractNode>,
    /// `reverse_deps` keys whose importer list changed.
    dep_targets: HashSet<CompactStr>,
    /// Topic keys whose producer or consumer list changed.
    topics: HashSet<CompactStr>,
    /// `rpc_calls` records added or removed.
    rpc_records: Vec<(NodeId, CompactStr)>,
}

impl ReconcileJournal {
    pub(super) fn recording() -> Self {
        Self {
            recording: true,
            ..Self::default()
        }
    }

    /// Stops recording: the next reconcile is a full one.
    pub(super) fn invalidate(&mut self) {
        *self = Self::default();
    }

    fn admit(&mut self) -> bool {
        if !self.recording {
            return false;
        }
        self.entries += 1;
        if self.entries > JOURNAL_CAP {
            self.invalidate();
            return false;
        }
        true
    }

    pub(super) fn node_added(&mut self, id: NodeId) {
        if self.admit() {
            self.added.insert(id);
        }
    }

    pub(super) fn node_removed(&mut self, node: ContractNode) {
        if self.admit() {
            self.removed.push(node);
        }
    }

    pub(super) fn dep_target(&mut self, target: &CompactStr) {
        if self.recording && !self.dep_targets.contains(target) && self.admit() {
            self.dep_targets.insert(target.clone());
        }
    }

    pub(super) fn topic(&mut self, key: &CompactStr) {
        if self.recording && !self.topics.contains(key) && self.admit() {
            self.topics.insert(key.clone());
        }
    }

    pub(super) fn rpc_record(&mut self, id: NodeId, record: &CompactStr) {
        if self.admit() {
            self.rpc_records.push((id, record.clone()));
        }
    }

    /// A topic node's confidence changes only with its own facts, which a new
    /// node's registration journals; marking an older node is not described.
    pub(super) fn heuristic_mark(&mut self, id: NodeId) {
        if self.recording && !self.added.contains(&id) {
            self.invalidate();
        }
    }

    fn changed_nodes(&self) -> usize {
        self.added.len() + self.removed.len()
    }
}

/// The last `.` segment of a gRPC name: what the passes call its bare name.
fn bare_of(name: &str) -> &str {
    name.split('.').next_back().unwrap_or(name)
}

/// ASCII-lowercase with `_` removed: equal for any two names
/// `eq_ignore_ascii_case` or `bare_names_match` calls equal.
fn norm_key(s: &str) -> String {
    s.chars()
        .filter(|c| *c != '_')
        .map(|c| c.to_ascii_lowercase())
        .collect()
}

fn index_id<K: std::hash::Hash + Eq>(map: &mut HashMap<K, BTreeSet<NodeId>>, key: K, id: NodeId) {
    map.entry(key).or_default().insert(id);
}

fn unindex_id<K, Q>(map: &mut HashMap<K, BTreeSet<NodeId>>, key: &Q, id: NodeId)
where
    K: std::hash::Hash + Eq + std::borrow::Borrow<Q>,
    Q: std::hash::Hash + Eq + ?Sized,
{
    if let Some(ids) = map.get_mut(key) {
        ids.remove(&id);
        if ids.is_empty() {
            map.remove(key);
        }
    }
}

/// gRPC declarations by the keys the `Implements` and `CallsRpc` passes look
/// them up by, each bucket in node-id order (the order the full passes scan
/// `nodes` in). Kept current by `add_node` and `remove_nodes`.
#[derive(Debug, Clone, Default)]
pub(super) struct GrpcIndex {
    /// `.proto` methods by bare name, verbatim.
    proto_by_bare: HashMap<CompactStr, BTreeSet<NodeId>>,
    /// `.proto` methods by [`norm_key`] of the bare name.
    proto_by_norm_bare: HashMap<String, BTreeSet<NodeId>>,
    /// `.proto` methods by lowercase full name.
    proto_by_lower_name: HashMap<String, BTreeSet<NodeId>>,
    /// `.proto` methods by lowercase bare name.
    proto_by_lower_bare: HashMap<String, BTreeSet<NodeId>>,
    /// Every `GrpcService` node by lowercase name.
    service_by_lower_name: HashMap<String, BTreeSet<NodeId>>,
    /// Longest bare name ever indexed, in bytes (never lowered: a bound).
    max_bare_len: usize,
}

impl GrpcIndex {
    pub(super) fn insert(&mut self, node: &ContractNode) {
        if ContractGraph::is_proto_method(node) {
            let bare = bare_of(&node.name);
            self.max_bare_len = self.max_bare_len.max(bare.len());
            index_id(&mut self.proto_by_bare, CompactStr::new(bare), node.id);
            index_id(&mut self.proto_by_norm_bare, norm_key(bare), node.id);
            index_id(
                &mut self.proto_by_lower_name,
                node.name.as_str().to_lowercase(),
                node.id,
            );
            index_id(&mut self.proto_by_lower_bare, bare.to_lowercase(), node.id);
        }
        if node.kind == NodeKind::GrpcService {
            index_id(
                &mut self.service_by_lower_name,
                node.name.as_str().to_lowercase(),
                node.id,
            );
        }
    }

    pub(super) fn remove(&mut self, node: &ContractNode) {
        if ContractGraph::is_proto_method(node) {
            let bare = bare_of(&node.name);
            unindex_id(&mut self.proto_by_bare, bare, node.id);
            unindex_id(
                &mut self.proto_by_norm_bare,
                norm_key(bare).as_str(),
                node.id,
            );
            unindex_id(
                &mut self.proto_by_lower_name,
                node.name.as_str().to_lowercase().as_str(),
                node.id,
            );
            unindex_id(
                &mut self.proto_by_lower_bare,
                bare.to_lowercase().as_str(),
                node.id,
            );
        }
        if node.kind == NodeKind::GrpcService {
            unindex_id(
                &mut self.service_by_lower_name,
                node.name.as_str().to_lowercase().as_str(),
                node.id,
            );
        }
    }

    /// Every proto method the `Implements` predicate can pair with a handler
    /// named `name` (PascalCase `pascal`, signature `signature`): a superset,
    /// each candidate is re-checked with the predicate itself.
    fn implements_candidates(
        &self,
        name: &str,
        pascal: &str,
        signature: Option<&str>,
    ) -> BTreeSet<NodeId> {
        let mut out = BTreeSet::new();
        let by_norm = |s: &str, out: &mut BTreeSet<NodeId>| {
            if let Some(ids) = self.proto_by_norm_bare.get(norm_key(s).as_str()) {
                out.extend(ids.iter().copied());
            }
        };
        // `name == fqcn`, `eq_ignore_ascii_case`, `bare_names_match`, PascalCase.
        by_norm(bare_of(name), &mut out);
        by_norm(name, &mut out);
        by_norm(pascal, &mut out);
        // `signature_contains_bare`: the identifier after each prefix (its
        // `bare_names_match` arm), and every prefix of the text after it that a
        // bare name can be (its `starts_with` arm).
        if let Some(sig) = signature {
            for prefix in ["@", "'", "\"", "fn ", "func ", "def "] {
                let mut offset = 0;
                while let Some(pos) = sig[offset..].find(prefix) {
                    let start = offset + pos + prefix.len();
                    let rest = &sig[start..];
                    let ident_len: usize = rest
                        .chars()
                        .take_while(|c| c.is_alphanumeric() || *c == '_')
                        .map(char::len_utf8)
                        .sum();
                    if ident_len > 0 {
                        by_norm(&rest[..ident_len], &mut out);
                    }
                    for (i, c) in rest.char_indices() {
                        let end = i + c.len_utf8();
                        if end > self.max_bare_len {
                            break;
                        }
                        if let Some(ids) = self.proto_by_bare.get(&rest[..end]) {
                            out.extend(ids.iter().copied());
                        }
                    }
                    offset += pos + prefix.len();
                }
            }
        }
        out
    }

    /// The `CallsRpc` full-name bucket of `key`: proto methods, then services.
    fn fqcn_bucket(&self, key: &str) -> Vec<NodeId> {
        self.proto_by_lower_name
            .get(key)
            .into_iter()
            .chain(self.service_by_lower_name.get(key))
            .flatten()
            .copied()
            .collect()
    }

    /// The `CallsRpc` bare-name bucket of `key`.
    fn bare_bucket(&self, key: &str) -> Vec<NodeId> {
        self.proto_by_lower_bare
            .get(key)
            .into_iter()
            .flatten()
            .copied()
            .collect()
    }
}

/// The index keys the journaled node changes touch.
#[derive(Default)]
struct DirtyKeys {
    /// `name_to_nodes` keys.
    names: HashSet<CompactStr>,
    /// `package_to_nodes` keys.
    packages: HashSet<CompactStr>,
    /// `fqcn_to_node` keys, and the text after their last `/`.
    fqcn: HashSet<String>,
    fqcn_tails: HashSet<String>,
    /// Lowercase names of event-bus `EventStream` nodes (topic nodes).
    hub_topics: HashSet<CompactStr>,
    /// Lowercase names of `GrpcService` nodes.
    service_keys: HashSet<String>,
    /// Byte lengths of `service_keys`, to skip most calls without lowercasing.
    service_key_lens: HashSet<usize>,
    /// A `.proto` method was added or removed.
    proto_changed: bool,
}

impl DirtyKeys {
    fn collect(graph: &ContractGraph, journal: &ReconcileJournal) -> Self {
        let mut keys = Self::default();
        let added = journal.added.iter().filter_map(|id| graph.nodes.get(id));
        for n in added.chain(journal.removed.iter()) {
            keys.names.insert(n.name.clone());
            let stream = matches!(
                n.kind,
                NodeKind::KafkaTopic | NodeKind::EventStream | NodeKind::Queue
            );
            if stream {
                keys.names.insert(CompactStr::new(n.name.to_lowercase()));
            }
            if !n.package.is_empty() {
                keys.packages.insert(n.package.clone());
            }
            if n.kind == NodeKind::GrpcMethod
                || n.kind == NodeKind::GrpcService
                || !n.package.is_empty()
            {
                let fqcn = format!("{}/{}", n.package, n.name);
                let tail = fqcn.rsplit('/').next().unwrap_or(&fqcn).to_string();
                keys.fqcn_tails.insert(tail);
                keys.fqcn.insert(fqcn);
            }
            if n.package == "event-bus" && n.kind == NodeKind::EventStream {
                keys.hub_topics
                    .insert(CompactStr::new(n.name.to_lowercase()));
            }
            if n.kind == NodeKind::GrpcService {
                let key = n.name.as_str().to_lowercase();
                keys.service_key_lens.insert(key.len());
                keys.service_keys.insert(key);
            }
            if ContractGraph::is_proto_method(n) {
                keys.proto_changed = true;
            }
        }
        keys
    }

    /// Whether resolving the import target `t` reads an index key a changed
    /// node sits under (see `resolve_import_targets`: `t` itself as a name or a
    /// package, its file stem, its fully-qualified `package/name` and `name`).
    fn touches_import(&self, t: &str) -> bool {
        if self.names.contains(t) || self.packages.contains(t) {
            return true;
        }
        if t.starts_with('.') || t.starts_with('/') {
            let stem = Path::new(t)
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or(t);
            return self.names.contains(stem);
        }
        let Some((pkg, name)) = ContractGraph::split_fully_qualified(t) else {
            return false;
        };
        if self.names.contains(name) {
            return true;
        }
        let tail = name.rsplit('/').next().unwrap_or(name);
        self.fqcn_tails.contains(tail) && self.fqcn.contains(&format!("{pkg}/{name}"))
    }

    /// Whether a recorded call looks up a service name a changed service has.
    fn touches_call(&self, target: &str, method_level: bool) -> bool {
        let hit = |s: &str| {
            if s.is_ascii() && !self.service_key_lens.contains(&s.len()) {
                return false;
            }
            self.service_keys.contains(&s.to_lowercase())
        };
        hit(target)
            || (!method_level
                && ContractGraph::service_suffixed_target(target).is_some_and(|s| hit(&s)))
    }
}

/// `a` and `b`, each sorted by `key`, merged (`a` first on a tie).
fn merge_by_key<K: Ord>(
    a: impl IntoIterator<Item = ContractEdge>,
    b: Vec<ContractEdge>,
    key: impl Fn(&ContractEdge) -> K,
) -> Vec<ContractEdge> {
    let mut out = Vec::with_capacity(b.len());
    let mut a = a.into_iter().peekable();
    let mut b = b.into_iter().peekable();
    loop {
        let take_b = match (a.peek(), b.peek()) {
            (Some(x), Some(y)) => key(y) < key(x),
            (Some(_), None) => false,
            (None, Some(_)) => true,
            (None, None) => break,
        };
        let next = if take_b { b.next() } else { a.next() };
        if let Some(e) = next {
            out.push(e);
        }
    }
    out
}

impl ContractGraph {
    /// [`Self::reconcile_edges`], recomputing only the edges the changes since
    /// the last reconcile can reach (see the module documentation for the rule
    /// of each edge family), or in full when the change is too large or not
    /// journaled. Ends in the same graph as `reconcile_edges` would.
    pub fn reconcile_delta(&mut self) -> ReconcileMode {
        self.reconcile_delta_within(DELTA_MAX_CHANGED_PERCENT)
    }

    /// [`Self::reconcile_delta`] with the changed-node share past which it
    /// reconciles in full.
    pub(super) fn reconcile_delta_within(&mut self, max_changed_percent: usize) -> ReconcileMode {
        let mut journal = std::mem::take(&mut self.journal);
        let full = |graph: &mut Self, reason: &'static str| {
            graph.reconcile_edges();
            ReconcileMode::Full(reason)
        };
        if !journal.recording {
            return full(self, "no change journal");
        }
        let changed = journal.changed_nodes();
        if changed >= DELTA_MIN_CHANGED_FOR_SHARE
            && changed * 100 > self.nodes.len() * max_changed_percent
        {
            return full(self, "changed set too large");
        }
        let reconciled = |e: &ContractEdge| {
            matches!(
                e.kind,
                EdgeKind::Imports
                    | EdgeKind::Produces
                    | EdgeKind::Consumes
                    | EdgeKind::Implements
                    | EdgeKind::CallsRpc
            )
        };
        if !self.edges.iter().all(reconciled) {
            return full(self, "edge not derived by reconcile");
        }

        // Changes made from here on (topic hubs) are the next reconcile's.
        self.journal = ReconcileJournal::recording();
        let dirty = DirtyKeys::collect(self, &journal);

        let mut old_imports = Vec::new();
        let mut old_topics = Vec::new();
        let mut old_implements = Vec::new();
        let mut old_rpc = Vec::new();
        for e in std::mem::take(&mut self.edges) {
            match e.kind {
                EdgeKind::Imports => old_imports.push(e),
                EdgeKind::Produces | EdgeKind::Consumes => old_topics.push(e),
                EdgeKind::Implements => old_implements.push(e),
                _ => old_rpc.push(e),
            }
        }

        let imports = self.imports_delta(old_imports, &journal, &dirty);
        let (topics, removed_hubs) = self.topics_delta(old_topics, &journal, &dirty);
        // A hub the topic pass just removed keeps its `rpc_calls` records. Only
        // the delta gRPC passes skip a record whose owner is gone; the full pass
        // (`rpc_call_edges`) has no owner check. Journaling the hub as a removed
        // node whose records changed makes the delta treat those records as
        // changed, so it ends in the same graph as the full pass.
        if !removed_hubs.is_empty() {
            journal.removed.extend(
                self.journal
                    .removed
                    .iter()
                    .filter(|n| removed_hubs.contains(&n.id))
                    .cloned(),
            );
            journal.rpc_records.extend(
                self.rpc_calls
                    .iter()
                    .filter(|(id, _)| removed_hubs.contains(id))
                    .cloned(),
            );
        }
        let grpc_full = dirty.proto_changed;
        let mut calls_full = false;
        let (implements, rpc) = if grpc_full {
            let proto_methods = self.proto_methods();
            let (implements, resolved) = self.implements_full(&proto_methods);
            self.handler_rpc_names = resolved;
            (implements, self.calls_rpc_full(&proto_methods))
        } else {
            let (implements, renamed) = self.implements_delta(old_implements, &journal);
            let rpc = if self.calls_are_contiguous() {
                self.calls_rpc_delta(old_rpc, &journal, &dirty, renamed, &removed_hubs)
            } else {
                calls_full = true;
                self.calls_rpc_full(&self.proto_methods())
            };
            (implements, rpc)
        };

        let mut edges = imports;
        edges.extend(topics);
        edges.extend(implements);
        edges.extend(rpc);
        self.edges = edges;
        ReconcileMode::Delta {
            grpc_full,
            calls_full,
        }
    }

    /// Whether every caller's call records form one run in `rpc_calls`: then
    /// the full pass's record-order edges are its callers' edge groups in
    /// first-call order, which is how `calls_rpc_delta` assembles them.
    fn calls_are_contiguous(&self) -> bool {
        let mut closed: HashSet<NodeId> = HashSet::new();
        let mut current: Option<NodeId> = None;
        for (caller, recorded) in &self.rpc_calls {
            if !Self::is_rpc_call_record(recorded) || current == Some(*caller) {
                continue;
            }
            if !closed.insert(*caller) {
                return false;
            }
            current = Some(*caller);
        }
        true
    }

    /// `Imports`: re-resolves every importer one of whose targets is dirty.
    fn imports_delta(
        &self,
        old: Vec<ContractEdge>,
        journal: &ReconcileJournal,
        dirty: &DirtyKeys,
    ) -> Vec<ContractEdge> {
        // A removed importer's facts can outlive it (a hub removed by the topic
        // pass keeps them): its edges go, as the full pass skips it.
        let removed: HashSet<NodeId> = journal.removed.iter().map(|n| n.id).collect();
        let mut importers: HashSet<NodeId> = HashSet::new();
        for (target, ids) in &self.reverse_deps {
            if journal.dep_targets.contains(target) || dirty.touches_import(target) {
                importers.extend(ids.iter().copied());
            }
        }
        if importers.is_empty() {
            return old
                .into_iter()
                .filter(|e| !removed.contains(&e.from))
                .collect();
        }
        // Every fact of each dirty importer: two targets resolving to one node
        // give one edge, the first target's.
        let mut facts: Vec<(NodeId, &CompactStr)> = self
            .reverse_deps
            .iter()
            .flat_map(|(target, ids)| {
                ids.iter()
                    .filter(|id| importers.contains(id))
                    .map(move |&id| (id, target))
            })
            .collect();
        facts.sort_unstable_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(b.1)));
        let fresh = self.import_edges(&facts);
        merge_by_key(
            old.into_iter()
                .filter(|e| !importers.contains(&e.from) && !removed.contains(&e.from)),
            fresh,
            |e| e.from,
        )
    }

    /// Topic edges: removes hubs left without facts and recomputes every dirty
    /// topic, keeping the others' edges. Returns the edges and the hubs removed.
    fn topics_delta(
        &mut self,
        old: Vec<ContractEdge>,
        journal: &ReconcileJournal,
        dirty: &DirtyKeys,
    ) -> (Vec<ContractEdge>, HashSet<NodeId>) {
        let mut topics: BTreeSet<CompactStr> = journal.topics.iter().cloned().collect();
        topics.extend(dirty.hub_topics.iter().cloned());
        // A wildcard subscription hears the producers of every concrete subject it
        // matches.
        let concrete: Vec<&CompactStr> = journal
            .topics
            .iter()
            .filter(|k| !is_subject_pattern(k))
            .collect();
        if !concrete.is_empty() {
            let patterns: Vec<CompactStr> = self
                .topic_consumers
                .keys()
                .chain(self.topic_producers.keys())
                .filter(|k| {
                    is_subject_pattern(k)
                        && !topics.contains(*k)
                        && concrete.iter().any(|c| subject_matches(k, c))
                })
                .cloned()
                .collect();
            topics.extend(patterns);
        }

        // A hub's liveness changes with its own topic's facts, so a stale one is
        // a new node or one named after a topic whose facts changed.
        let live = |g: &Self, name: &CompactStr| {
            g.topic_producers.contains_key(name) || g.topic_consumers.contains_key(name)
        };
        let mut stale: HashSet<NodeId> = HashSet::new();
        for id in &journal.added {
            if let Some(n) = self.nodes.get(id) {
                if Self::is_hub_like(n) && !live(self, &n.name) {
                    stale.insert(n.id);
                }
            }
        }
        for key in &journal.topics {
            for id in self.name_to_nodes.get(key).into_iter().flatten() {
                if let Some(n) = self.nodes.get(id) {
                    if n.name == *key && Self::is_hub_like(n) && !live(self, &n.name) {
                        stale.insert(n.id);
                    }
                }
            }
        }
        self.remove_nodes(&stale);
        // A removed node's facts outlive it, and whether a wildcard subscription
        // has a subject consumer reads its consumers' nodes: the topics a removed
        // hub takes part in are dirty.
        if !stale.is_empty() {
            for map in [&self.topic_producers, &self.topic_consumers] {
                for (key, ids) in map {
                    if ids.iter().any(|id| stale.contains(id)) {
                        topics.insert(key.clone());
                    }
                }
            }
        }

        // Old edges grouped by topic node, in topic-key order; dirty topics' and
        // removed hubs' groups dropped.
        let mut groups: Vec<(NodeId, Vec<ContractEdge>)> = Vec::new();
        for e in old {
            let topic = if e.kind == EdgeKind::Produces {
                e.to
            } else {
                e.from
            };
            match groups.last_mut() {
                Some((id, group)) if *id == topic => group.push(e),
                _ => groups.push((topic, vec![e])),
            }
        }
        let kept: Vec<(CompactStr, Vec<ContractEdge>)> = groups
            .into_iter()
            .filter_map(|(id, group)| {
                let key = CompactStr::new(self.nodes.get(&id)?.name.to_lowercase());
                (!topics.contains(&key)).then_some((key, group))
            })
            .collect();

        let mut concrete_producers: Option<Vec<(CompactStr, Vec<NodeId>)>> = None;
        let mut fresh: Vec<(CompactStr, Vec<ContractEdge>)> = Vec::new();
        for key in &topics {
            if !live(self, key) {
                continue;
            }
            let topic_id = self.topic_node_or_create(key);
            let mut out = Vec::new();
            self.topic_unit_edges(key, topic_id, &mut concrete_producers, &mut out);
            fresh.push((key.clone(), out));
        }

        let mut edges = Vec::new();
        let mut kept = kept.into_iter().peekable();
        let mut fresh = fresh.into_iter().peekable();
        loop {
            let take_fresh = match (kept.peek(), fresh.peek()) {
                (Some((k, _)), Some((f, _))) => f < k,
                (Some(_), None) => false,
                (None, Some(_)) => true,
                (None, None) => break,
            };
            let next = if take_fresh {
                fresh.next()
            } else {
                kept.next()
            };
            if let Some((_, group)) = next {
                edges.extend(group);
            }
        }
        (edges, stale)
    }

    /// `Implements`: re-resolves the handlers whose `@GrpcMethod` arguments can
    /// resolve differently and re-pairs every new or renamed handler. Returns
    /// the edges and the handlers whose resolved name changed.
    fn implements_delta(
        &mut self,
        old: Vec<ContractEdge>,
        journal: &ReconcileJournal,
    ) -> (Vec<ContractEdge>, HashSet<NodeId>) {
        let mut resolved = std::mem::take(&mut self.handler_rpc_names);
        let mut renamed: HashSet<NodeId> = HashSet::new();
        for n in &journal.removed {
            if resolved.remove(&n.id).is_some() {
                renamed.insert(n.id);
            }
        }

        // Handler references to re-resolve: changed ones, and those naming a
        // constant whose values changed.
        let mut const_keys: HashSet<&str> = HashSet::new();
        let mut owners: BTreeSet<NodeId> = BTreeSet::new();
        for (id, record) in &journal.rpc_records {
            if let Some(c) = record.strip_prefix(CONST_VALUE_PREFIX) {
                if let Some((key, _)) = c.split_once('=') {
                    const_keys.insert(key);
                }
            } else if record.starts_with(HANDLER_REF_PREFIX) && self.nodes.contains_key(id) {
                owners.insert(*id);
            }
        }
        if !const_keys.is_empty() {
            for (id, record) in &self.rpc_calls {
                let Some(parsed) = record
                    .strip_prefix(HANDLER_REF_PREFIX)
                    .and_then(Self::parse_handler_ref)
                else {
                    continue;
                };
                if const_keys.contains(parsed.service.trim())
                    || const_keys.contains(parsed.method.trim())
                {
                    owners.insert(*id);
                }
            }
        }

        if !owners.is_empty() {
            let mut refs: Vec<(NodeId, &str)> = Vec::new();
            let mut needed: HashSet<&str> = HashSet::new();
            for (id, record) in &self.rpc_calls {
                let Some(reference) = record.strip_prefix(HANDLER_REF_PREFIX) else {
                    continue;
                };
                if !owners.contains(id) {
                    continue;
                }
                refs.push((*id, reference));
                if let Some(parsed) = Self::parse_handler_ref(reference) {
                    needed.insert(parsed.service.trim());
                    needed.insert(parsed.method.trim());
                }
            }
            let mut consts: HashMap<&str, Vec<(RepoId, &str)>> = HashMap::new();
            for (id, record) in &self.rpc_calls {
                let Some(c) = record.strip_prefix(CONST_VALUE_PREFIX) else {
                    continue;
                };
                if let (Some(node), Some((key, value))) = (self.nodes.get(id), c.split_once('=')) {
                    if needed.contains(key) {
                        consts.entry(key).or_default().push((node.repo_id, value));
                    }
                }
            }
            let before: HashMap<NodeId, Option<CompactStr>> =
                owners.iter().map(|id| (*id, resolved.remove(id))).collect();
            let declared = |name: &str| {
                self.name_to_nodes
                    .get(name)
                    .into_iter()
                    .flatten()
                    .copied()
                    .filter(|id| self.nodes.get(id).is_some_and(Self::is_proto_method))
            };
            for (id, reference) in refs {
                if let Some(name) = self.resolve_handler_ref(id, reference, &consts, declared) {
                    resolved.insert(id, name);
                }
            }
            for (id, was) in before {
                if resolved.get(&id) != was.as_ref() {
                    renamed.insert(id);
                }
            }
        }

        let handlers: BTreeSet<NodeId> = journal
            .added
            .iter()
            .chain(renamed.iter())
            .copied()
            .filter(|id| self.nodes.get(id).is_some_and(Self::is_grpc_handler))
            .collect();
        let mut fresh = Vec::new();
        for h in handlers {
            let Some(node) = self.nodes.get(&h) else {
                continue;
            };
            let name = resolved
                .get(&h)
                .map_or(node.name.as_str(), CompactStr::as_str);
            let pascal = crate::types::to_pascal_case(name);
            let signature = node.signature.as_deref();
            for p in self
                .grpc_index
                .implements_candidates(name, &pascal, signature)
            {
                let Some(proto) = self.nodes.get(&p) else {
                    continue;
                };
                let method_fqcn = &proto.name;
                let bare = bare_of(method_fqcn);
                if let Some(confidence) =
                    Self::implements_confidence(name, &pascal, signature, method_fqcn, bare)
                {
                    fresh.push(ContractEdge {
                        from: h,
                        to: p,
                        kind: EdgeKind::Implements,
                        metadata: Some(method_fqcn.clone()),
                        confidence,
                    });
                }
            }
        }
        fresh.sort_unstable_by_key(|e| (e.to, e.from));
        let edges = merge_by_key(
            old.into_iter().filter(|e| !renamed.contains(&e.from)),
            fresh,
            |e| (e.to, e.from),
        );
        self.handler_rpc_names = resolved;
        (edges, renamed)
    }

    /// `CallsRpc`: re-resolves every dirty caller's calls, keeping the others'.
    fn calls_rpc_delta(
        &self,
        old: Vec<ContractEdge>,
        journal: &ReconcileJournal,
        dirty: &DirtyKeys,
        renamed: HashSet<NodeId>,
        removed_hubs: &HashSet<NodeId>,
    ) -> Vec<ContractEdge> {
        let removed: HashMap<NodeId, &ContractNode> =
            journal.removed.iter().map(|n| (n.id, n)).collect();
        let mut bindings: HashSet<(&str, &str)> = HashSet::new();
        let mut callers = renamed;
        callers.extend(journal.added.iter().copied());
        // A caller's package is read from its node: one the topic pass just
        // removed resolves its calls without it.
        callers.extend(removed_hubs.iter().copied());
        for (id, record) in &journal.rpc_records {
            if let Some(binding) = record.strip_prefix(CLIENT_FIELD_BINDING_PREFIX) {
                let class = self.nodes.get(id).or_else(|| removed.get(id).copied());
                if let (Some(class), Some((field, _))) = (class, binding.split_once('=')) {
                    bindings.insert((class.name.as_str(), field));
                }
            } else if self.nodes.contains_key(id) {
                callers.insert(*id);
            }
        }
        let service_changed = !dirty.service_keys.is_empty();
        let mut class_fields = service_changed.then(|| self.class_fields());
        for (caller, record) in &self.rpc_calls {
            if callers.contains(caller) {
                continue;
            }
            let through_binding = record
                .strip_prefix(RPC_INHERITED_METHOD_PREFIX)
                .and_then(|t| t.split_once('#'))
                .and_then(|(base, rest)| rest.split_once('.').map(|(field, _)| (base, field)))
                .is_some_and(|key| bindings.contains(&key));
            let hit = through_binding
                || class_fields.as_ref().is_some_and(|fields| {
                    self.rpc_call_target(*caller, record, fields).is_some_and(
                        |(t, method_level, _, inherited)| {
                            dirty.touches_call(inherited.as_deref().unwrap_or(t), method_level)
                        },
                    )
                });
            if hit {
                callers.insert(*caller);
            }
        }

        let calls: Vec<&(NodeId, CompactStr)> = self
            .rpc_calls
            .iter()
            .filter(|(caller, _)| callers.contains(caller))
            .collect();
        if calls.is_empty() && old.iter().all(|e| !callers.contains(&e.from)) {
            return old;
        }
        let class_fields = match class_fields.take() {
            Some(fields) => fields,
            None if calls
                .iter()
                .any(|(_, r)| r.starts_with(RPC_INHERITED_METHOD_PREFIX)) =>
            {
                self.class_fields()
            }
            None => HashMap::new(),
        };
        // The lookup buckets of exactly the names those calls resolve against.
        let mut fqcn_keys: HashSet<String> = HashSet::new();
        let mut bare_keys: HashSet<String> = HashSet::new();
        for (caller, record) in &calls {
            let Some((t, method_level, _, inherited)) =
                self.rpc_call_target(*caller, record, &class_fields)
            else {
                continue;
            };
            let t = inherited.as_deref().unwrap_or(t);
            fqcn_keys.insert(t.to_lowercase());
            if !method_level {
                bare_keys.insert(bare_of(t).to_lowercase());
                if let Some(s) = Self::service_suffixed_target(t) {
                    bare_keys.insert(bare_of(&s).to_lowercase());
                    fqcn_keys.insert(s.to_lowercase());
                }
            }
        }
        let buckets = |keys: HashSet<String>, bucket: &dyn Fn(&str) -> Vec<NodeId>| {
            keys.into_iter()
                .filter_map(|k| {
                    let ids = bucket(&k);
                    (!ids.is_empty()).then_some((k, ids))
                })
                .collect::<HashMap<String, Vec<NodeId>>>()
        };
        let by_fqcn = buckets(fqcn_keys, &|k| self.grpc_index.fqcn_bucket(k));
        let by_bare = buckets(bare_keys, &|k| self.grpc_index.bare_bucket(k));
        let lookups = RpcLookups {
            by_fqcn: self.group_by_package(by_fqcn),
            by_bare: self.group_by_package(by_bare),
            class_fields,
            handler_methods: self.handler_served_methods_where(|id| callers.contains(&id)),
        };
        let fresh = self.rpc_call_edges(calls.into_iter(), &lookups);

        // Grouped by caller, callers in first-call order: the full pass's record
        // order, since every caller's calls are one run (`calls_are_contiguous`).
        let mut groups: HashMap<NodeId, Vec<ContractEdge>> = HashMap::new();
        for e in old
            .into_iter()
            .filter(|e| !callers.contains(&e.from))
            .chain(fresh)
        {
            groups.entry(e.from).or_default().push(e);
        }
        let mut edges = Vec::new();
        for (caller, recorded) in &self.rpc_calls {
            if !Self::is_rpc_call_record(recorded) {
                continue;
            }
            if let Some(group) = groups.remove(caller) {
                edges.extend(group);
            }
        }
        edges
    }
}
