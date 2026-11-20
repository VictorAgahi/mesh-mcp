use crate::types::CompactStr;
use crate::types::{
    ContractEdge, ContractNode, EdgeConfidence, EdgeKind, FilePath, NodeId, NodeKind, RepoId,
};
use serde::Serialize;
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};

mod delta;
mod index;
mod query;
mod reconcile;
mod render;
mod resolve;

pub use delta::ReconcileMode;

#[cfg(test)]
mod delta_tests;
#[cfg(test)]
mod handler_ref_tests;
#[cfg(test)]
mod tests;

/// The wildcard tokens of a subject key: NATS (`.`-separated) has `*` for one token and a final
/// `>` for one or more; MQTT (`/`-separated) has `+` for one level and a final `#` for zero or
/// more. The separator picks the dialect, so `cache.+` or `src/*/x` stay literal keys.
fn subject_dialect(key: &str) -> (char, &'static str, &'static str) {
    if key.contains('/') {
        ('/', "+", "#")
    } else {
        ('.', "*", ">")
    }
}

/// Whether `key` is a wildcard subject.
pub fn is_subject_pattern(key: &str) -> bool {
    let (sep, one, rest) = subject_dialect(key);
    let mut tokens = key.split(sep).peekable();
    while let Some(t) = tokens.next() {
        if t == one || (t == rest && tokens.peek().is_none()) {
            return true;
        }
    }
    false
}

/// Whether the concrete subject `topic` is matched by the wildcard subject `pattern`. Both are
/// already lowercase. A topic with an empty token matches nothing.
pub fn subject_matches(pattern: &str, topic: &str) -> bool {
    let (sep, one, rest) = subject_dialect(pattern);
    if topic.split(sep).any(str::is_empty) {
        return false;
    }
    let mut p = pattern.split(sep).peekable();
    let mut t = topic.split(sep).peekable();
    while let Some(tok) = p.next() {
        let last = p.peek().is_none();
        if tok == rest && last {
            // MQTT `#` also matches its parent level; NATS `>` needs at least one token.
            return rest == "#" || t.peek().is_some();
        }
        match t.next() {
            Some(x) if tok == one || tok == x => {}
            _ => return false,
        }
    }
    t.peek().is_none()
}

/// Most producers one wildcard subscription is linked to.
const MAX_WILDCARD_PRODUCERS: usize = 256;

/// Query results borrow from the graph: the snapshot guard held by the caller
/// keeps them alive, and cloning every `ContractNode` (with its `PathBuf`) per
/// request is what the formatter never needed.
/// The registry key of an event / topic / queue name: lowercase, and for a
/// *code reference* to an enum or constant member — `Streams.EVENT_CREATED`,
/// `EventMessagingType.EVENT_CREATED`, `typeof Topics.EVENT_CREATED` — just the
/// member (`event_created`), so a producer and a consumer that name the same
/// member through different enums or type expressions share one topic.
/// Conservative on purpose: only a dotted path of identifiers whose first
/// segment starts with an uppercase letter and whose last is CONSTANT_CASE is
/// reduced; a broker literal such as `orders.created` is kept whole.
pub fn normalize_topic_key(raw: &str) -> String {
    let trimmed = raw
        .trim()
        .trim_matches(|c| c == '\'' || c == '"' || c == '`')
        .trim();
    let expr = trimmed.strip_prefix("typeof ").map_or(trimmed, str::trim);
    let is_ident = |seg: &str| {
        let mut chars = seg.chars();
        chars
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_' || c == '$')
            && chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
    };
    let segments: Vec<&str> = expr.split('.').collect();
    let is_member_ref = segments.len() >= 2
        && segments.iter().all(|s| is_ident(s))
        && segments[0].starts_with(|c: char| c.is_ascii_uppercase())
        && segments.last().is_some_and(|last| {
            last.chars().any(|c| c.is_ascii_alphabetic())
                && last
                    .chars()
                    .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
        });
    let key = if is_member_ref {
        segments.last().copied().unwrap_or(expr)
    } else {
        expr
    };
    key.to_lowercase()
}

/// Prefix of a recorded RPC call target naming one method (`=UserService.signUp`):
/// resolved by exact `Service.Method` only, with the resolution's own confidence.
pub const RPC_EXACT_METHOD_PREFIX: char = '=';
/// Same as [`RPC_EXACT_METHOD_PREFIX`], but the service was inferred from a
/// field name rather than read from a client binding: heuristic confidence.
pub const RPC_INFERRED_METHOD_PREFIX: char = '~';
/// A method-level call through a field the calling class does not bind itself
/// but inherits (`^BaseEventGrpcController#commandService.createEvent`):
/// resolved against the base class's [`CLIENT_FIELD_BINDING_PREFIX`] records,
/// falling back to the field-name inference of [`RPC_INFERRED_METHOD_PREFIX`].
pub const RPC_INHERITED_METHOD_PREFIX: char = '^';
/// Not a call: records that the class it is attached to binds a client field
/// (`#commandService=EventCommandService`), so subclasses in other files can
/// resolve calls through it. Never becomes an edge by itself.
pub const CLIENT_FIELD_BINDING_PREFIX: char = '#';
/// Not a call: a string constant a top-level declaration carries
/// (`$USER_COMMAND_METHODS.ADD_BADGE_TO_USER=AddBadgeToUser`, `$USER_SERVICE_NAME=UserService`),
/// so a `@GrpcMethod(...)` argument spelled as an enum member, an `as const`
/// object key or an imported constant can be resolved to its literal across
/// files ([`HANDLER_REF_PREFIX`]). Never becomes an edge by itself.
pub const CONST_VALUE_PREFIX: char = '$';
/// Not a call: the two raw `@GrpcMethod(service, method)` arguments of a server
/// handler (`@'UserService'|USER_COMMAND_METHODS.ADD_BADGE_TO_USER`; literal
/// arguments are kept quoted, a non-literal that starts like a quote is
/// prefixed with `?`, a missing service is empty). Resolved against [`CONST_VALUE_PREFIX`]
/// records when the graph is reconciled. Never becomes an edge by itself.
pub const HANDLER_REF_PREFIX: char = '@';

/// A parsed [`HANDLER_REF_PREFIX`] record.
struct HandlerRef<'a> {
    service: &'a str,
    method: &'a str,
    /// `false` when the decorator had no method argument.
    method_written: bool,
}

/// `userService` / `userClient` -> `UserService`; anything else -> `None`.
/// The service a client field is assumed to talk to when nothing binds it.
pub fn service_from_client_field_name(field: &str) -> Option<String> {
    let stem = field
        .strip_suffix("Client")
        .or_else(|| field.strip_suffix("Service"))?;
    if stem.is_empty() || !stem.starts_with(|c: char| c.is_ascii_lowercase()) {
        return None;
    }
    let mut chars = stem.chars();
    let first = chars.next()?.to_ascii_uppercase();
    Some(format!("{first}{}Service", chars.as_str()))
}

/// Lowercase alphanumerics only: `AddBadgeToUser`, `addBadgeToUser` and
/// `ADD_BADGE_TO_USER` fold to the same key.
pub fn fold_rpc_name(name: &str) -> String {
    name.chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .map(|c| c.to_ascii_lowercase())
        .collect()
}

/// How a [`ContractGraph::recorded_rpc_callers`] row calls the RPC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordedCaller {
    /// A recorded call of the method itself.
    Method,
    /// A client constructed for the method's service, with no method-level
    /// call recorded in its file and no client field bound in it (a Go
    /// `NewXClient`, a Python stub): it may call the method.
    ServiceClient,
    /// A class that binds a client field to the method's service
    /// (`#field=Service`) while neither it nor a direct subclass calling
    /// through the field has a resolved call of the method: it holds a client,
    /// no call of the method was found (a subclass two levels down is not
    /// seen: its record names only its own parent).
    BoundClient,
}

/// One recorded RPC call resolved the way `reconcile_edges` resolves it
/// (client binding, inherited field, field-name inference), without the RPC
/// having to be declared: see [`RpcRecordResolver`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedRpcRecord {
    /// `[pkg.]Service.method` as the call names it for a method-level call,
    /// `[pkg.]Service` for a service-level client.
    pub target: String,
    pub method_level: bool,
    /// [`EdgeConfidence::Exact`] when the service was read from a client
    /// binding, [`EdgeConfidence::Heuristic`] when inferred from a field name.
    pub confidence: EdgeConfidence,
    /// `Service.Method` as a `.proto` of the graph declares it, when exactly
    /// one declared RPC folds to the same service and method.
    pub declared: Option<CompactStr>,
}

/// Resolves recorded RPC call records (`=`, `~`, `^` and plain service
/// records) against one graph's client field bindings: built once by
/// [`ContractGraph::rpc_record_resolver`], then asked per record.
pub struct RpcRecordResolver<'g> {
    class_fields: reconcile::ClassFields<'g>,
    /// (folded bare service, folded method) -> declared `Service.Method`s.
    declared: HashMap<(String, String), Vec<CompactStr>>,
    /// Every `$` string constant: path -> (repository, value).
    consts: HashMap<&'g str, Vec<(RepoId, &'g str)>>,
}

/// How [`ContractGraph::find_dependents_matched`] found its result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DependentsMatch {
    /// Files importing `target` (or a subpath of it) — structural.
    Import,
    /// `target` is a declared symbol: files importing its declaring package.
    DeclaringPackage,
    /// Callers linked to `target` by a `CallsRpc` / `Implements` edge.
    GraphEdge,
    /// Last resort: import strings merely *containing* `target` — a heuristic.
    Substring,
    /// Nothing matched.
    None,
}

#[derive(Debug, Clone, Serialize)]
pub struct GrpcTrace<'g> {
    pub target: CompactStr,
    pub proto_definition: Option<&'g ContractNode>,
    /// Each stub paired with the confidence of the match that linked it to
    /// the proto definition (bare-name scan vs. an exact `CallsRpc` edge).
    pub client_stubs: Vec<(&'g ContractNode, EdgeConfidence)>,
    /// Each handler paired with the confidence of the match that linked it
    /// to the proto definition (bare-name scan vs. an exact `Implements` edge).
    pub server_handlers: Vec<(&'g ContractNode, EdgeConfidence)>,
    /// Handlers (by node id) declared with enum members / constants whose
    /// literals resolved to exactly one RPC: the proto RPC name to show in
    /// place of the raw decorator expression.
    pub handler_rpc_names: HashMap<NodeId, CompactStr>,
    /// The decorator arguments of those handlers as written
    /// (`USER_SERVICE_NAME, USER_COMMAND_METHODS.ADD_BADGE_TO_USER`), for the
    /// "declared as" text. Absent for a handler without a recorded decorator.
    pub handler_declared_as: HashMap<NodeId, String>,
    /// When the target is a method: callers linked to the method's *service*
    /// (a client constructed for it, `getService<UserServiceClient>(…)`)
    /// without the called method being resolved, in files with no resolved
    /// call. They may or may not call this method; the formatter says so
    /// instead of presenting `client_stubs` as complete.
    pub service_level_clients: Vec<(&'g ContractNode, EdgeConfidence)>,
    /// `.proto` services/methods whose name merely contains the target
    /// (`CreateEventNode` for `CreateEvent`), left out because a symbol
    /// matched exactly. Listed so the answer says what it did not trace.
    pub related_rpcs: Vec<&'g ContractNode>,
    /// Number of source files in the index searched during this trace.
    pub files_scanned: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct ImpactFlow<'g> {
    pub target: CompactStr,
    /// The normalized topic key the target was matched as: `event.created`
    /// is tried as `event_created` when nothing matches it as written.
    pub key: CompactStr,
    pub upstream_producers: Vec<&'g ContractNode>,
    pub topics: Vec<&'g ContractNode>,
    pub downstream_consumers: Vec<&'g ContractNode>,
    pub related_sagas: Vec<&'g ContractNode>,
    /// Hops past the first: producers declared in the file of a consumer
    /// reached on the previous hop (a handler that re-emits), through which
    /// the walk continued. Linked by file, not by a call edge.
    pub downstream_producers: Vec<&'g ContractNode>,
    /// Consumers (or sagas) the walk reached but did not expand because the
    /// depth limit was hit: a higher `depth` would follow them. Nothing is
    /// claimed about what lies past them. Sorted by location, then id.
    pub unexpanded: Vec<&'g ContractNode>,
    /// Consumers (or sagas) the walk did expand (`depth >= 2`) and found no
    /// resolved onward publish for: a real end of the statically resolved
    /// chain, or a hop whose publish name is resolved at runtime. Sorted by
    /// location.
    pub chain_end: Vec<&'g ContractNode>,
    /// Number of source files in the index searched during this flow trace.
    pub files_scanned: usize,
}

/// Whether an impacted element lives in the service that owns the changed
/// contract (plan 4 step 4.6a). The graph has no "service" field, so the
/// service is the root (`repo_id`): see [`ContractGraph::impact_matrix`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ImpactScope {
    /// Another root than the owner's, or no root at all.
    External,
    /// The owner's root.
    Internal,
}

impl ImpactScope {
    pub fn label(self) -> &'static str {
        match self {
            ImpactScope::External => "EXTERNAL",
            ImpactScope::Internal => "INTERNAL",
        }
    }
}

/// How an impacted element relates to the changed contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ImpactRole {
    /// Implements the gRPC method/service.
    Handler,
    /// Calls the gRPC method, or binds a client to its service.
    Client,
    /// Produces onto a matched topic.
    Producer,
    /// A matched topic / stream / queue.
    Topic,
    /// Consumes a matched topic (directly or through `depth` hops).
    Consumer,
    /// A saga reached by the matched events.
    Saga,
}

impl ImpactRole {
    pub fn label(self) -> &'static str {
        match self {
            ImpactRole::Handler => "handler",
            ImpactRole::Client => "client",
            ImpactRole::Producer => "producer",
            ImpactRole::Topic => "topic",
            ImpactRole::Consumer => "consumer",
            ImpactRole::Saga => "saga",
        }
    }
}

/// One row of an [`ImpactMatrix`].
#[derive(Debug, Clone, Serialize)]
pub struct ImpactRow<'g> {
    pub node: &'g ContractNode,
    pub role: ImpactRole,
    pub scope: ImpactScope,
    /// Confidence of the edge that links `node` to `via` (gRPC rows) or of
    /// the topic match (async rows).
    pub confidence: EdgeConfidence,
    /// Contract node the row is linked through: the proto method it
    /// implements/calls, or its service for a service-level client. `None` for
    /// a direct name match with no edge and for async rows.
    pub via: Option<&'g ContractNode>,
}

/// Impact matrix of a change to a gRPC method/service or an async event
/// (plan 4 step 4.6a). `rows` are deduplicated and totally ordered.
#[derive(Debug, Clone, Serialize)]
pub struct ImpactMatrix<'g> {
    pub target: CompactStr,
    /// The `.proto` declarations the target resolved to.
    pub contracts: Vec<&'g ContractNode>,
    /// Roots that own the changed contract(s), sorted. Empty when no owner
    /// could be determined, in which case every row is `External`.
    pub owner_roots: Vec<RepoId>,
    pub rows: Vec<ImpactRow<'g>>,
    /// An event matched (topics or consumers) but none of its producers was
    /// resolved — a SQL trigger, a runtime-built topic name, a config-driven
    /// publisher. Said explicitly rather than shown as an empty column.
    pub no_producer_resolved: bool,
    /// Producers or topics matched but no consumer was resolved.
    pub no_consumer_resolved: bool,
    /// The walk continued past the first hop through producers found in a
    /// consumer's file ([`ImpactFlow::downstream_producers`]).
    pub file_linked_hops: bool,
    /// Hops requested (clamped), for the depth-limit note.
    pub depth: usize,
    /// See [`ImpactFlow::unexpanded`], without nodes in test files.
    pub unexpanded: Vec<&'g ContractNode>,
    /// See [`ImpactFlow::chain_end`], without nodes in test files.
    pub chain_end: Vec<&'g ContractNode>,
    /// The event is indexed but every node of it is hidden (test files), so the
    /// notes cannot say "indexed" about anything the rows show.
    pub event_only_in_tests: bool,
    /// Rows left out because `hide` judged their node a test file.
    pub hidden_rows: usize,
}

/// In-memory graph of polyglot architecture contracts and dependencies.
#[derive(Debug, Clone, Default)]
pub struct ContractGraph {
    // `BTreeMap`, not `HashMap`: std's `HashMap` uses a per-process-random `SipHash`
    // seed (`RandomState`), so iterating `.values()` — done below to resolve an
    // ambiguous target (multiple proto methods sharing a bare name, multiple RPC
    // call candidates, `analyze_grpc`'s anchor scan) — visits nodes in a different
    // order on every process run even for byte-identical input. `BTreeMap` iterates
    // in sorted `NodeId` order, which is itself deterministic (ids are assigned
    // sequentially while folding files in the indexer's now-sorted crawl order), so
    // "first candidate encountered" becomes reproducible instead of a coin flip
    // seeded at process start (idempotence invariant I1).
    nodes: BTreeMap<NodeId, ContractNode>,
    edges: Vec<ContractEdge>,

    // O(1) in-memory indices
    name_to_nodes: HashMap<CompactStr, Vec<NodeId>>,
    package_to_nodes: HashMap<CompactStr, Vec<NodeId>>,
    file_to_nodes: HashMap<FilePath, Vec<NodeId>>,
    fqcn_to_node: HashMap<CompactStr, Vec<NodeId>>,
    reverse_deps: HashMap<CompactStr, Vec<NodeId>>,
    topic_producers: HashMap<CompactStr, Vec<NodeId>>,
    topic_consumers: HashMap<CompactStr, Vec<NodeId>>,
    rpc_calls: Vec<(NodeId, CompactStr)>,
    /// Producer / consumer nodes registered by a heuristic source (declarative
    /// patterns by default): their topic edges are `heuristic`, not `exact`.
    heuristic_topic_nodes: HashSet<NodeId>,
    /// Handlers whose `@GrpcMethod(...)` arguments resolved to exactly one RPC
    /// of a declared proto service (`Service.Method`). Derived by
    /// `reconcile_edges`, never stored incrementally.
    handler_rpc_names: HashMap<NodeId, CompactStr>,

    /// What changed since the last reconcile (nodes added and removed, fact
    /// keys touched), read by `reconcile_delta` to recompute only the edges
    /// those changes can reach. Not part of the graph's content.
    journal: delta::ReconcileJournal,
    /// gRPC declarations by the names the `Implements` / `CallsRpc` passes
    /// look them up by, kept current by `add_node` / `remove_nodes`, so a
    /// delta finds a changed handler's or call's candidates without a scan.
    grpc_index: delta::GrpcIndex,

    next_node_id: u32,
}

impl ContractGraph {
    /// Hard ceiling on `analyze_impact_with_depth`'s hop count. Bounds the BFS
    /// walk independent of cycle detection — a caller passing an inflated depth
    /// on a very connected mesh should not turn "impact analysis" into "flood
    /// the payload budget", mirroring the other bounded-traversal caps in this
    /// crate (AST nesting depth, query cursor step limit).
    pub const MAX_IMPACT_DEPTH: usize = 5;

    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    #[inline]
    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    #[inline]
    pub fn file_count(&self) -> usize {
        self.file_to_nodes.len()
    }

    pub fn get_node(&self, id: NodeId) -> Option<&ContractNode> {
        self.nodes.get(&id)
    }

    #[inline]
    pub fn all_nodes(&self) -> impl Iterator<Item = &ContractNode> {
        self.nodes.values()
    }

    #[inline]
    pub fn all_edges(&self) -> &[ContractEdge] {
        &self.edges
    }

    pub fn get_nodes_for_file(&self, path: &Path) -> &[NodeId] {
        self.file_to_nodes
            .get(path)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }
}
