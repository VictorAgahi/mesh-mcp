mod call_support;
pub mod cpp;
pub mod csharp;
pub mod go;
mod go_calls;
mod inproc_java;
pub mod java;
mod java_calls;
mod js_frameworks;
pub mod kotlin;
pub mod php;
pub mod proto;
mod py_calls;
pub mod python;
pub mod ruby;
pub mod rust_lang;
pub mod scala;
mod sfc;
mod spec_shape;
pub mod sql;
pub mod swift;
mod ts_calls;
pub mod ts_config;
pub mod typescript;

pub use ts_config::TsAliasIndex;

use crate::decapitate::LanguageKind;
use crate::guard::{AstGuard, ParseOutcome};
use mesh_core::{
    Broker, CallFacts, CompactStr, ConfigValues, ContractGraph, ContractNode, ContractsConfig,
    CustomPatternConfig, FilePath, NodeKind, PatternKind, RepoId,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

/// Default TS decorator that marks a method as a gRPC handler when
/// `[engines.contracts.grpc] controller_annotations` is not configured.
const DEFAULT_CONTROLLER_ANNOTATION: &str = "@GrpcMethod";

/// Resolved extraction-time knobs pulled from `[engines.contracts.*]`. Bundled
/// so `PolyglotIndexer` call sites only thread one value through the pool
/// instead of four independent config lookups per file.
#[derive(Debug, Clone)]
pub struct ExtractConfig {
    /// Directories (matched as a path substring) that scope where `.proto`
    /// files are extracted from. Empty means unrestricted (legacy behaviour:
    /// every `.proto` file, wherever it lives).
    pub proto_dirs: Vec<String>,
    /// TS decorators that mark a method as a gRPC handler. Empty falls back
    /// to the historical hardcoded `@GrpcMethod`.
    pub controller_annotations: Vec<String>,
    /// When true (default), a proto RPC node is named `Service.Method`
    /// (canonical). When false, it is projected as the bare method name.
    pub canonical_fqcn_projection: bool,
    /// Path patterns (substring/suffix match) that scope which files are
    /// treated as OpenAPI specs. Empty falls back to filename/content sniffing.
    pub openapi_spec_files: Vec<String>,
    /// Same as `openapi_spec_files`, for AsyncAPI.
    pub asyncapi_spec_files: Vec<String>,
    /// When true (default), AsyncAPI extraction also infers topics from a
    /// non-standard top-level `topics:` string list, not just `channels`.
    pub infer_string_topics: bool,
    /// `tsconfig.json` / `jsconfig.json` `paths` and `package.json` `imports`
    /// aliases of the workspace; disabled (rewrites nothing) by default.
    pub ts_aliases: TsAliasIndex,
}

impl Default for ExtractConfig {
    fn default() -> Self {
        Self {
            proto_dirs: Vec::new(),
            controller_annotations: vec![DEFAULT_CONTROLLER_ANNOTATION.to_string()],
            canonical_fqcn_projection: true,
            openapi_spec_files: Vec::new(),
            asyncapi_spec_files: Vec::new(),
            infer_string_topics: true,
            ts_aliases: TsAliasIndex::default(),
        }
    }
}

impl ExtractConfig {
    /// Builds the extraction config from `[engines.contracts]`. Missing
    /// sub-tables (`grpc`/`openapi`/`asyncapi`) fall back to their defaults.
    pub fn from_contracts(contracts: &ContractsConfig) -> Self {
        let default = Self::default();
        let (proto_dirs, controller_annotations, canonical_fqcn_projection) = match &contracts.grpc
        {
            Some(g) => {
                let annotations = if g.controller_annotations.is_empty() {
                    default.controller_annotations.clone()
                } else {
                    g.controller_annotations.clone()
                };
                (
                    g.proto_dirs.clone(),
                    annotations,
                    g.canonical_fqcn_projection,
                )
            }
            None => (
                default.proto_dirs.clone(),
                default.controller_annotations.clone(),
                default.canonical_fqcn_projection,
            ),
        };
        let openapi_spec_files = contracts
            .openapi
            .as_ref()
            .map(|o| o.spec_files.clone())
            .unwrap_or_default();
        let (asyncapi_spec_files, infer_string_topics) = match &contracts.asyncapi {
            Some(a) => (a.spec_files.clone(), a.infer_string_topics),
            None => (Vec::new(), default.infer_string_topics),
        };

        Self {
            proto_dirs,
            controller_annotations,
            canonical_fqcn_projection,
            openapi_spec_files,
            asyncapi_spec_files,
            infer_string_topics,
            ts_aliases: default.ts_aliases,
        }
    }

    /// Whether `path` falls under one of `proto_dirs` (or `proto_dirs` is
    /// empty, i.e. unrestricted).
    pub fn allows_proto_path(&self, path_str: &str) -> bool {
        if self.proto_dirs.is_empty() {
            return true;
        }
        let normalized = path_str.replace('\\', "/");
        self.proto_dirs.iter().any(|d| {
            let clean = mesh_core::strip_workspace_root_prefix(d);
            let needle = clean.trim_matches('/').replace('\\', "/");
            !needle.is_empty() && normalized.contains(needle.as_str())
        })
    }

    /// Whether `path` matches one of `openapi_spec_files`.
    pub fn allows_openapi_spec(&self, path_str: &str) -> bool {
        if self.openapi_spec_files.is_empty() {
            return false;
        }
        let lower = path_str.to_lowercase().replace('\\', "/");
        self.openapi_spec_files
            .iter()
            .any(|spec| spec_file_matches(&lower, spec))
    }
}

/// Matches `path_str` (already lowercased) against a configured spec-file
/// pattern by suffix or substring, mirroring `CompiledPattern::matches_path`.
fn spec_file_matches(path_str: &str, pattern: &str) -> bool {
    let clean = mesh_core::strip_workspace_root_prefix(pattern);
    let needle = clean
        .trim_start_matches("./")
        .trim_start_matches('*')
        .to_lowercase();
    !needle.is_empty()
        && (path_str.ends_with(needle.as_str()) || path_str.contains(needle.as_str()))
}

/// Everything extracted from one file, expressed against *local* node indices.
///
/// Extraction is decoupled from graph mutation so it can run on the Rayon pool
/// (`ContractGraph` is `&mut` and single-writer). `apply` assigns real `NodeId`s
/// by inserting into the graph sequentially.
///
/// `Serialize`/`Deserialize`: this is exactly the unit `PersistentIndexCache` (P2 step 3.2)
/// persists per content hash — pre-`NodeId`-numbering, so a cache hit skips tree-sitter
/// entirely without touching global node numbering (see `mesh_core::index_cache`'s module
/// doc).
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct FileIndex {
    pub nodes: Vec<ContractNode>,
    /// (local node index, imported target)
    pub dependencies: Vec<(usize, CompactStr)>,
    /// (local node index, topic)
    pub producers: Vec<(usize, CompactStr)>,
    /// (local node index, topic)
    pub consumers: Vec<(usize, CompactStr)>,
    /// (local node index, target rpc)
    pub rpc_calls: Vec<(usize, CompactStr)>,
    /// Local indices of producer / consumer nodes whose topic link is only a
    /// heuristic (declarative patterns default to it). Their topic edges are
    /// reported `heuristic`, not `exact`.
    #[serde(default)]
    pub heuristic_topic_nodes: Vec<usize>,
    /// (index into `producers`, broker): the producers whose broker is statically known (9.0,
    /// ticket T7). A producer with no entry has an unknown broker and joins every broker's
    /// channel of its topic (`mesh_core::Broker`). Indices past the end are ignored.
    #[serde(default)]
    pub producer_brokers: Vec<(usize, Broker)>,
    /// Same for `consumers`.
    #[serde(default)]
    pub consumer_brokers: Vec<(usize, Broker)>,
    /// (local node index, broker): the nodes that are themselves a broker interaction (a
    /// TypeScript call site named after its topic). Display only: a query for one broker's
    /// channel leaves the topic rows of another broker out.
    #[serde(default)]
    pub node_brokers: Vec<(usize, Broker)>,
    /// Set when this file's tree-sitter parse itself failed or exceeded its budget
    /// (`AstGuard::ParseOutcome::ParseFailed`) — as opposed to `nodes` legitimately
    /// being empty (a blank file, one with no top-level declarations, ...). Callers
    /// must not treat this file as "indexed with zero facts": `WorkspaceIndexer` gives
    /// it one sequential retry outside the contended parallel pool on a full build,
    /// and on an incremental reload keeps its last known-good facts and retries on the
    /// next change, rather than wiping them (idempotence invariant I6).
    pub parse_failed: bool,
    /// Call facts (9.0 calls layer): callables, call sites, injections, imports and
    /// type heritage, language-agnostic (`mesh_core::CallFacts`). Empty for a
    /// language without a call extractor.
    #[serde(default)]
    pub calls: CallFacts,
    /// A Java, Go or Python file that carries no call facts because of its path (test code,
    /// generated code, a `testing/`, `fixtures/` or `gen/` directory): counted in
    /// `IndexHealth`, so a chain through such a file is not silently absent.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub calls_skipped_path: bool,
    /// SQL outbox inserts and `pg_notify` calls of this file whose key is not a static literal
    /// (no event is registered for them): counted in `IndexHealth::sql_sites_unresolved`.
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub sql_unresolved: u32,
    /// A SQL scan cap (tokens, sites, `VALUES` rows, literals) dropped events of this file:
    /// counted in `IndexHealth::files_sql_capped`.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub sql_capped: bool,
    /// The per-file cap on in-process bus sites and schedules (`MAX_INPROC_SITES_PER_FILE`) cut
    /// events of this file: counted in `IndexHealth::files_inproc_capped`.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub inproc_capped: bool,
}

fn is_zero_u32(n: &u32) -> bool {
    *n == 0
}

impl FileIndex {
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Appends `other` after `self`, shifting its local indices.
    pub fn merge(&mut self, other: FileIndex) {
        let offset = self.nodes.len();
        self.nodes.extend(other.nodes);
        self.dependencies
            .extend(other.dependencies.into_iter().map(|(i, t)| (i + offset, t)));
        let (produced, consumed) = (self.producers.len(), self.consumers.len());
        self.producer_brokers.extend(
            other
                .producer_brokers
                .into_iter()
                .map(|(i, b)| (i + produced, b)),
        );
        self.consumer_brokers.extend(
            other
                .consumer_brokers
                .into_iter()
                .map(|(i, b)| (i + consumed, b)),
        );
        self.producers
            .extend(other.producers.into_iter().map(|(i, t)| (i + offset, t)));
        self.consumers
            .extend(other.consumers.into_iter().map(|(i, t)| (i + offset, t)));
        self.rpc_calls
            .extend(other.rpc_calls.into_iter().map(|(i, t)| (i + offset, t)));
        self.heuristic_topic_nodes
            .extend(other.heuristic_topic_nodes.into_iter().map(|i| i + offset));
        self.node_brokers
            .extend(other.node_brokers.into_iter().map(|(i, b)| (i + offset, b)));
        self.parse_failed |= other.parse_failed;
        self.calls.merge(other.calls);
        self.calls_skipped_path |= other.calls_skipped_path;
        self.sql_unresolved = self.sql_unresolved.saturating_add(other.sql_unresolved);
        self.sql_capped |= other.sql_capped;
        self.inproc_capped |= other.inproc_capped;
    }

    /// Inserts the extracted nodes and relations into `graph`. A `${key}` topic is resolved
    /// against nothing (only its own `${key:default}` fallback applies).
    pub fn apply(self, graph: &mut ContractGraph) {
        self.apply_with(graph, &ConfigValues::default());
    }

    /// Whether a producer or consumer topic of this file is a `${key}` config placeholder.
    pub fn has_config_topic(&self) -> bool {
        self.producers
            .iter()
            .chain(&self.consumers)
            .any(|(_, topic)| topic.contains("${"))
    }

    /// [`Self::apply`] with the workspace's configuration values: a `${key}` topic is
    /// registered under each topic the configuration resolves it to (more than one when
    /// profiles disagree), and the site is linked `heuristic` unless exactly one strong
    /// value says so. A placeholder nothing resolves stays registered as the `${key}` topic
    /// itself, `heuristic`: visible, never guessed.
    pub fn apply_with(self, graph: &mut ContractGraph, config: &ConfigValues) {
        // Call facts are keyed by the file their nodes live in: a file with call
        // facts always has the declaration nodes they were read from.
        if let Some(first) = self.nodes.first() {
            if !self.calls.is_empty() {
                graph.add_call_facts(
                    first.file_path.clone(),
                    first.repo_id,
                    first.package.clone(),
                    self.calls,
                );
            }
        }
        // A call-site node named after its topic shows the resolved name when the
        // configuration gives exactly one.
        let mut nodes = self.nodes;
        for n in nodes.iter_mut().filter(|n| n.name.contains("${")) {
            if let Some(r) = config.resolve(&n.name).filter(|r| r.topics.len() == 1) {
                n.name = CompactStr::new(&r.topics[0].0);
            }
        }
        let ids: Vec<_> = nodes.into_iter().map(|n| graph.add_node(n)).collect();
        for (i, target) in self.dependencies {
            graph.add_dependency(ids[i], &target);
        }
        let producer_brokers: HashMap<usize, Broker> = self.producer_brokers.into_iter().collect();
        let mut weak_nodes: BTreeSet<usize> = BTreeSet::new();
        for (pair, (i, topic)) in self.producers.into_iter().enumerate() {
            let broker = producer_brokers.get(&pair).copied();
            for topic in Self::config_topics(&topic, config, i, &mut weak_nodes) {
                graph.add_producer_on(ids[i], &topic, broker);
            }
        }
        let consumer_brokers: HashMap<usize, Broker> = self.consumer_brokers.into_iter().collect();
        for (pair, (i, topic)) in self.consumers.into_iter().enumerate() {
            let broker = consumer_brokers.get(&pair).copied();
            for topic in Self::config_topics(&topic, config, i, &mut weak_nodes) {
                graph.add_consumer_on(ids[i], &topic, broker);
            }
        }
        for i in weak_nodes {
            graph.mark_heuristic_topic_node(ids[i]);
        }
        for (i, target) in self.rpc_calls {
            graph.add_rpc_call(ids[i], &target);
        }
        for i in self.heuristic_topic_nodes {
            graph.mark_heuristic_topic_node(ids[i]);
        }
        for (i, broker) in self.node_brokers {
            if let Some(id) = ids.get(i) {
                graph.set_node_broker(*id, broker);
            }
        }
    }

    /// The topics a recorded topic stands for: itself when it holds no placeholder, else what
    /// `config` resolves it to. A weak or unresolved link adds node `i` to `weak`.
    fn config_topics(
        topic: &CompactStr,
        config: &ConfigValues,
        i: usize,
        weak: &mut BTreeSet<usize>,
    ) -> Vec<CompactStr> {
        if !topic.contains("${") {
            return vec![topic.clone()];
        }
        match config.resolve(topic) {
            Some(resolved) if !resolved.topics.is_empty() => resolved
                .topics
                .into_iter()
                .map(|(t, is_weak)| {
                    if is_weak {
                        weak.insert(i);
                    }
                    CompactStr::new(t)
                })
                .collect(),
            _ => {
                weak.insert(i);
                vec![topic.clone()]
            }
        }
    }

    /// Records `broker` for every producer and consumer registered so far that has none: the
    /// extractors of a language whose only detected broker is one (the Kafka-only Java, Go,
    /// Kotlin, C# and Rust extractors) call this once, after extraction.
    pub fn tag_brokers(&mut self, broker: Broker) {
        let tagged: HashSet<usize> = self.producer_brokers.iter().map(|(i, _)| *i).collect();
        self.producer_brokers.extend(
            (0..self.producers.len())
                .filter(|i| !tagged.contains(i))
                .map(|i| (i, broker)),
        );
        let tagged: HashSet<usize> = self.consumer_brokers.iter().map(|(i, _)| *i).collect();
        self.consumer_brokers.extend(
            (0..self.consumers.len())
                .filter(|i| !tagged.contains(i))
                .map(|i| (i, broker)),
        );
    }

    /// Whether any node carries `package`, the package [`PathContext::package_of`] infers
    /// from the file's directories. A node whose declared package merely equals it counts
    /// too: the record is then keyed by the inferred package, which can only cost a miss.
    pub fn uses_package(&self, package: &str) -> bool {
        self.nodes.iter().any(|n| n.package == package)
    }

    /// Rewrites every node's `file_path` under `root` to its root-relative form (components
    /// joined with `/`), the shape the shared parse cache stores (8.0.0): two checkouts of
    /// one repository, or one checkout moved, then read the same record. Returns the indices
    /// of the rewritten nodes, which [`Self::reanchor`] needs to tell them from a node whose
    /// path was relative to begin with (the empty placeholder path some extractors use).
    ///
    /// `None`, with `self` left as it was, when a node carries an absolute path outside
    /// `root` (or one that is not valid UTF-8): such a record cannot be re-anchored on
    /// another checkout, so it must not be cached at all.
    ///
    /// `file_path` is the only path-typed field of a `FileIndex`. Every other string
    /// (names, packages, signatures, docstrings, dependency / topic / RPC targets) is
    /// either read from the file's bytes or derived from the absolute path through a
    /// decision [`PathContext`] folds into the cache key instead.
    pub fn relativize(&mut self, root: &Path) -> Option<Vec<usize>> {
        let mut rewritten: Vec<(usize, FilePath)> = Vec::new();
        let mut last: Option<(FilePath, FilePath)> = None;
        for (i, node) in self.nodes.iter().enumerate() {
            if !node.file_path.is_absolute() {
                continue;
            }
            if let Some((abs, rel)) = &last {
                if Arc::ptr_eq(abs, &node.file_path) || **abs == *node.file_path {
                    rewritten.push((i, Arc::clone(rel)));
                    continue;
                }
            }
            let rel = root_relative(&node.file_path, root)?;
            let rel: FilePath = Arc::from(Path::new(&rel));
            rewritten.push((i, Arc::clone(&rel)));
            last = Some((Arc::clone(&node.file_path), rel));
        }
        let indices = rewritten.iter().map(|(i, _)| *i).collect();
        for (i, rel) in rewritten {
            self.nodes[i].file_path = rel;
        }
        Some(indices)
    }

    /// Inverse of [`Self::relativize`] on `root`, which may be another checkout's: the
    /// nodes at `anchored` get `root` joined with their `/`-separated relative path. One
    /// `Arc` is shared by every node of the same path, as extraction itself produces.
    pub fn reanchor(&mut self, root: &Path, anchored: &[usize]) {
        let mut last: Option<(FilePath, FilePath)> = None;
        for &i in anchored {
            let Some(node) = self.nodes.get_mut(i) else {
                continue;
            };
            if let Some((rel, abs)) = &last {
                if **rel == *node.file_path {
                    node.file_path = Arc::clone(abs);
                    continue;
                }
            }
            let mut abs = root.to_path_buf();
            if let Some(text) = node.file_path.to_str() {
                abs.extend(text.split('/').filter(|s| !s.is_empty()));
            }
            let abs: FilePath = Arc::from(abs.as_path());
            last = Some((Arc::clone(&node.file_path), Arc::clone(&abs)));
            node.file_path = abs;
        }
    }
}

/// `path` relative to `root`, components joined with `/` whatever the platform
/// separator; `None` when `path` is not strictly under `root` or a component is not UTF-8.
pub fn root_relative(path: &Path, root: &Path) -> Option<String> {
    if path == root {
        return None;
    }
    let rel = path.strip_prefix(root).ok()?;
    let mut parts: Vec<&str> = Vec::new();
    for c in rel.components() {
        match c {
            std::path::Component::Normal(s) => parts.push(s.to_str()?),
            std::path::Component::CurDir => {}
            _ => return None,
        }
    }
    Some(parts.join("/"))
}

/// What extraction reads from a file's absolute path beyond its root-relative path. The
/// shared parse cache (8.0.0) is keyed by the root-relative path, so each of these
/// decisions is folded into the key: a checkout where one comes out differently misses
/// instead of reading a record extracted under another answer.
///
/// Every extractor decision that looks at the absolute path must be covered here. Today:
/// - the package inferred from the directories above the file ([`Self::package_of`],
///   [`mesh_core::detect_service_package`] with no AST package: manifests and directory
///   names, possibly above the root, so the checkout's own directory name when nothing else
///   is found), used as `ContractNode.package` whenever a file declares none (or declares
///   Go's `main`). Only a record that uses it is keyed by it
///   ([`FileIndex::uses_package`]): a declared package must not make every key of a
///   manifest-less project differ between worktrees;
/// - for an `index.*` file, the name of its parent directory (its default export is named
///   after it, and for a file at the root that directory is the checkout itself);
/// - substring tests over the whole lowercased path: `proto_dirs`, `openapi_spec_files`,
///   `asyncapi_spec_files`, and the `openapi` / `asyncapi` / `routes` filename sniffs.
///
/// The last two are always in the key ([`Self::append_key_input`]).
///
/// One instance per scan: the package walk is memoised per directory (it stats manifests and
/// would otherwise cost a cache hit far more than the lookup itself).
#[derive(Debug, Default)]
pub struct PathContext {
    packages: std::sync::Mutex<std::collections::HashMap<std::path::PathBuf, CompactStr>>,
}

impl PathContext {
    /// The package [`mesh_core::detect_service_package`] infers for `file_path` from its
    /// directories alone (a function of the parent directory, memoised per scan).
    pub fn package_of(&self, file_path: &Path) -> CompactStr {
        let dir = file_path.parent().unwrap_or(Path::new(""));
        let cached = self
            .packages
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(dir)
            .cloned();
        if let Some(p) = cached {
            return p;
        }
        let p = mesh_core::detect_service_package(file_path, None);
        self.packages
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(dir.to_path_buf(), p.clone());
        p
    }

    /// Appends to `out` the path-derived inputs every key of `file_path` carries: the
    /// parent directory name of an `index.*` file and the path substring tests. Lock-free,
    /// one allocation (the lowercased path).
    pub fn append_key_input(&self, cfg: &ExtractConfig, file_path: &Path, out: &mut Vec<u8>) {
        // Only an `index.*` file is named after its directory (`file_symbol_name`).
        out.extend_from_slice(b"\0dir=");
        if file_path.file_stem().and_then(|s| s.to_str()) == Some("index") {
            if let Some(name) = file_path
                .parent()
                .and_then(|d| d.file_name())
                .and_then(|n| n.to_str())
            {
                out.extend_from_slice(name.as_bytes());
            }
        }
        let path_str = file_path.to_string_lossy();
        // Lowercased exactly as `extract_yaml_contracts` and the Ruby extractor do.
        let lower = path_str.to_lowercase();
        let flags = [
            cfg.allows_proto_path(&path_str),
            cfg.allows_openapi_spec(&path_str),
            cfg.openapi_spec_files
                .iter()
                .any(|f| spec_file_matches(&lower, f)),
            cfg.asyncapi_spec_files
                .iter()
                .any(|f| spec_file_matches(&lower, f)),
            lower.contains("openapi"),
            lower.contains("asyncapi"),
            lower.contains("routes"),
        ];
        out.extend_from_slice(b"\0flags=");
        out.extend(flags.iter().map(|&b| if b { b'1' } else { b'0' }));
    }
}

/// A [`FileIndex`] as the shared parse cache stores it: node paths root-relative, with the
/// indices of the nodes [`FileIndex::relativize`] rewrote.
#[derive(Debug, Serialize, Deserialize)]
pub struct RootRelativeIndex {
    pub index: FileIndex,
    pub anchored: Vec<usize>,
}

/// A `CustomPatternConfig` with its regex compiled once, instead of once per file.
#[derive(Debug, Clone)]
pub struct CompiledPattern {
    pub kind: PatternKind,
    /// Normalised `file_pattern` (leading `*`/`.` stripped), matched by suffix or substring.
    file_needle: Option<String>,
    regex: regex::Regex,
    target_group: usize,
    consumer_group: Option<usize>,
    /// `confidence = "exact"` was set; otherwise topic edges are heuristic.
    exact: bool,
}

impl CompiledPattern {
    pub fn compile(cfg: &CustomPatternConfig) -> Result<Self, regex::Error> {
        Ok(Self {
            kind: cfg.kind,
            file_needle: cfg.file_needle(),
            regex: regex::Regex::new(&cfg.regex)?,
            target_group: cfg.target_group,
            consumer_group: cfg.consumer_group,
            exact: cfg.is_exact(),
        })
    }

    /// Compiles every pattern, logging and skipping invalid ones rather than
    /// silently ignoring them on every file like the per-file compile did.
    pub fn compile_all(cfgs: &[CustomPatternConfig]) -> Vec<Self> {
        cfgs.iter()
            .filter_map(|cfg| match Self::compile(cfg) {
                Ok(p) => Some(p),
                Err(e) => {
                    tracing::warn!(
                        target: "mesh::indexer",
                        "Skipping custom pattern '{}': invalid regex: {e}",
                        cfg.name
                    );
                    None
                }
            })
            .collect()
    }

    #[inline]
    fn matches_path(&self, path_str: &str) -> bool {
        match &self.file_needle {
            Some(needle) => {
                path_str.ends_with(needle.as_str()) || path_str.contains(needle.as_str())
            }
            None => true,
        }
    }
}

/// Most in-process bus sites (NestJS EventEmitter2 and CQRS, Spring application events) and
/// schedules one file registers; the rest are dropped and the file is counted in
/// `IndexHealth::files_inproc_capped` (never silent).
pub const MAX_INPROC_SITES_PER_FILE: usize = 2_000;

pub struct PolyglotIndexer;

impl PolyglotIndexer {
    /// TypeScript event facts, which 7.0.0 created as nodes but never registered
    /// as producers or consumers (so `analyze_impact` could not link them):
    /// - a method decorated `@EventPattern` / `@MessagePattern` (a `KafkaTopic`
    ///   node named after its pattern) consumes that pattern;
    /// - a call-site producer (`producer.send`, NestJS `client.emit` / `client.send`,
    ///   BullMQ `new Queue`, `redis.publish`) is the call-site node itself, at the line
    ///   of the call. Up to 8.0.1 it was attributed to the smallest declaration
    ///   enclosing it, so it reported the method's declaration line, or the class's
    ///   when the method was itself an `@EventPattern` handler (8.0.1 defects N2 /
    ///   N9). The enclosing method is now found by line containment in the calls
    ///   layer (`ContractGraph::enclosing_callable`);
    /// - a call-site consumer (`consumer.subscribe`, `new Worker`) is still attributed
    ///   to the smallest declaration enclosing it: the handler a subscription runs is
    ///   what a consumer row names, and a topic-named node would say nothing more;
    /// - a decorator fact (`@Processor('q')`, `@RabbitSubscribe`) spans the
    ///   declaration it decorates and stays on it, as does a producer call that a
    ///   one-line declaration spans exactly (same lines, the declaration's name).
    fn wire_ts_events(nodes: &[ContractNode], out: &mut FileIndex) {
        for (i, node) in nodes.iter().enumerate() {
            let signature = node.signature.as_deref().unwrap_or("");
            let produces = match js_frameworks::signature_role(signature) {
                Some(js_frameworks::Role::Produce) => true,
                Some(js_frameworks::Role::Consume) => false,
                // `@EventPattern('x')` / `@MessagePattern('x')` handler method.
                None if node.kind == NodeKind::KafkaTopic => {
                    out.consumers.push((i, node.name.clone()));
                    continue;
                }
                None => continue,
            };
            // A saga's `ofType(Event)` is its own consumer: the field it sits in holds no
            // declaration of its own to attribute it to, and the class would say line 7 for a
            // site on line 11.
            let owner = if signature == js_frameworks::SAGA_CONSUME_SIGNATURE {
                i
            } else {
                nodes
                    .iter()
                    .enumerate()
                    .filter(|(j, n)| {
                        *j != i
                            && n.line_start <= node.line_start
                            && n.line_end >= node.line_end
                            && (!produces
                                || (n.line_start, n.line_end) == (node.line_start, node.line_end))
                            && !matches!(
                                n.kind,
                                NodeKind::KafkaTopic | NodeKind::Queue | NodeKind::EventStream
                            )
                    })
                    .min_by_key(|(_, n)| n.line_end - n.line_start)
                    .map_or(i, |(j, _)| j)
            };
            let broker = js_frameworks::signature_broker(signature);
            if let Some(b) = broker {
                out.node_brokers.push((i, b));
            }
            if produces {
                if let Some(b) = broker {
                    out.producer_brokers.push((out.producers.len(), b));
                }
                out.producers.push((owner, node.name.clone()));
            } else {
                if let Some(b) = broker {
                    out.consumer_brokers.push((out.consumers.len(), b));
                }
                out.consumers.push((owner, node.name.clone()));
            }
        }
    }

    /// Drops the in-process bus sites and schedules of a file past
    /// [`MAX_INPROC_SITES_PER_FILE`], in node order (the order of the source), so a hostile
    /// file of thousands of listeners costs a bounded wiring pass. `true` when any was
    /// dropped: the file is then counted in `IndexHealth::files_inproc_capped`.
    pub(crate) fn cap_inproc_nodes(nodes: &mut Vec<ContractNode>) -> bool {
        let is_inproc = |n: &ContractNode| {
            n.kind == NodeKind::Schedule
                || n.signature.as_deref().is_some_and(|s| {
                    s.starts_with("nestjs-event-emitter") || s.starts_with("nestjs-cqrs")
                })
        };
        let mut seen = 0usize;
        let before = nodes.len();
        nodes.retain(|n| {
            if !is_inproc(n) {
                return true;
            }
            seen += 1;
            seen <= MAX_INPROC_SITES_PER_FILE
        });
        nodes.len() != before
    }

    /// Whether `nodes[i]` sits inside no other node of the same file (a class,
    /// not one of its methods). Of two nodes spanning the exact same lines, the
    /// first one is the outer.
    fn is_outermost(nodes: &[ContractNode], i: usize) -> bool {
        let n = &nodes[i];
        !nodes.iter().enumerate().any(|(j, m)| {
            j != i
                && m.line_start <= n.line_start
                && m.line_end >= n.line_end
                && ((m.line_start, m.line_end) != (n.line_start, n.line_end) || j < i)
        })
    }

    /// Ingests and indexes a source file into the ContractGraph.
    pub fn index_file(file_path: &Path, content: &str, repo_id: RepoId, graph: &mut ContractGraph) {
        Self::extract(file_path, content, repo_id).apply(graph);
    }

    /// Parses a source file into a graph-independent `FileIndex`. Safe to call from any thread.
    /// Uses the default `ExtractConfig` — legacy behaviour for callers that don't
    /// thread `[engines.contracts]` through (tests, ad-hoc CLI usage).
    pub fn extract(file_path: &Path, content: &str, repo_id: RepoId) -> FileIndex {
        Self::extract_with_config(file_path, content, repo_id, &ExtractConfig::default())
    }

    /// The path a file-system router is read from: the file's path below its workspace root,
    /// so a checkout under `/app` or `~/api` does not make `app` / `api` framework folders;
    /// `None` for an absolute path under no root (any ancestor would be a guess). A workspace
    /// root that is itself the router folder (`web/app` registered as a root) has no marker
    /// left in the relative path: register the project root.
    fn routing_path(cfg: &ExtractConfig, file_path: &Path) -> Option<std::path::PathBuf> {
        match cfg.ts_aliases.relative_to_root(file_path) {
            Some(relative) => Some(relative),
            None if file_path.is_absolute() => None,
            None => Some(file_path.to_path_buf()),
        }
    }

    /// Same as [`Self::extract`], but honours `[engines.contracts.grpc]` /
    /// `.openapi` / `.asyncapi` extraction knobs. Safe to call from any thread.
    pub fn extract_with_config(
        file_path: &Path,
        content: &str,
        repo_id: RepoId,
        cfg: &ExtractConfig,
    ) -> FileIndex {
        Self::extract_with_budget(
            file_path,
            content,
            repo_id,
            cfg,
            AstGuard::INDEX_PARSE_TIMEOUT_MICROS,
        )
    }

    /// [`Self::extract_with_config`] with the tree-sitter parse budget made explicit:
    /// indexing passes `AstGuard::INDEX_PARSE_TIMEOUT_MICROS` (through
    /// `extract_with_config`), an extraction on an agent's synchronous request path
    /// (`diff_mesh`) passes `AstGuard::QUERY_PARSE_TIMEOUT_MICROS`. CLAUDE.md
    /// commandment 2: the two budgets are never one constant.
    pub fn extract_with_budget(
        file_path: &Path,
        content: &str,
        repo_id: RepoId,
        cfg: &ExtractConfig,
        timeout_micros: u64,
    ) -> FileIndex {
        // A single-file component is its script blocks, read with the JS / TS grammar;
        // the component itself is a declaration named after its file.
        if sfc::is_sfc(file_path) {
            return Self::extract_component(file_path, content, repo_id, cfg, timeout_micros);
        }
        // `.sql`: outbox inserts, NOTIFY and LISTEN (no grammar, a bounded scanner).
        if sql::is_sql_path(file_path) {
            return sql::extract_file(file_path, content, repo_id);
        }
        let lang_kind = LanguageKind::from_path(&file_path.to_string_lossy());
        Self::extract_as(file_path, content, repo_id, cfg, lang_kind, timeout_micros)
    }

    /// `.vue` / `.svelte` / `.astro`: the script is extracted (line numbers are the file's),
    /// then the component is declared, so that an import of it names a symbol.
    fn extract_component(
        file_path: &Path,
        content: &str,
        repo_id: RepoId,
        cfg: &ExtractConfig,
        timeout_micros: u64,
    ) -> FileIndex {
        let mut out = match sfc::script_view(file_path, content) {
            // Same lexical guard as any parsed source, on the script alone.
            Some(view)
                if AstGuard::max_nesting_depth(view.script.as_bytes())
                    <= AstGuard::MAX_NESTING_DEPTH
                    && view
                        .script
                        .lines()
                        // Blanked template / style lines are runs of spaces: only the
                        // script's own width counts.
                        .all(|l| l.trim_end().len() <= AstGuard::MAX_LINE_LEN_BYTES) =>
            {
                Self::extract_as(
                    file_path,
                    &view.script,
                    repo_id,
                    cfg,
                    view.kind,
                    timeout_micros,
                )
            }
            _ => FileIndex::default(),
        };
        let name =
            typescript::TypeScriptExtractor::file_symbol_name(&std::sync::Arc::from(file_path));
        // SvelteKit's `+page` / `+layout` / `+error` are route files, not named components.
        let special = file_path
            .file_stem()
            .and_then(|s| s.to_str())
            .is_some_and(|s| s.starts_with('+'));
        if !special && !out.nodes.iter().any(|n| n.name == name.as_str()) {
            // The component spans its file; its imports are its dependencies.
            let lines = content.lines().count().max(1);
            let component = ContractNode {
                id: 0,
                name: CompactStr::new(&name),
                kind: NodeKind::ServiceClass,
                file_path: std::sync::Arc::from(file_path),
                line_start: 1,
                line_end: lines,
                package: mesh_core::detect_service_package(file_path, None),
                repo_id,
                signature: Some(CompactStr::new(format!("component {name}"))),
                docstring: None,
            };
            let imports: std::collections::BTreeSet<CompactStr> =
                out.dependencies.iter().map(|(_, d)| d.clone()).collect();
            if out.nodes.len() == 1 && out.nodes[0].kind == NodeKind::Module {
                // Only the imports-only Module node exists: the component replaces it.
                out.nodes.clear();
                out.dependencies.clear();
            }
            let idx = out.nodes.len();
            out.nodes.push(component);
            for d in imports {
                if !out.dependencies.iter().any(|(i, t)| *i == idx && *t == d) {
                    out.dependencies.push((idx, d));
                }
            }
        }
        // SvelteKit's `+page.svelte` and Astro's `src/pages/**/*.astro` are pages by location.
        if let Some(page) = Self::routing_path(cfg, file_path)
            .and_then(|routing_path| js_frameworks::sfc_route(&routing_path))
        {
            let label = page.label();
            out.nodes.push(ContractNode {
                id: 0,
                name: CompactStr::new(&label),
                kind: NodeKind::HttpEndpoint,
                file_path: std::sync::Arc::from(file_path),
                line_start: 1,
                line_end: content.lines().count().max(1),
                package: mesh_core::detect_service_package(file_path, None),
                repo_id,
                signature: Some(CompactStr::new(label)),
                docstring: None,
            });
        }
        out
    }

    /// Whether a Java, Go or Python file carries call facts: not test code (a chain
    /// entered from a test is not an architecture flow, and tests only cost memory;
    /// judged below the workspace root, as for TypeScript) and not generated code
    /// (stubs, `zz_generated` files: thousands of callables nothing hand-written calls
    /// through).
    fn wants_call_facts(cfg: &ExtractConfig, file_path: &Path) -> bool {
        let rel = Self::routing_path(cfg, file_path);
        let judged = rel.as_deref().unwrap_or(file_path);
        let name = file_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default();
        let in_test = match rel.as_deref() {
            Some(rel) => mesh_core::is_test_path(rel),
            None => file_path
                .file_name()
                .is_some_and(|n| mesh_core::is_test_path(Path::new(n))),
        };
        !in_test && !mesh_core::is_generated_path(judged) && !name.starts_with("zz_generated")
    }

    fn extract_as(
        file_path: &Path,
        content: &str,
        repo_id: RepoId,
        cfg: &ExtractConfig,
        lang_kind: LanguageKind,
        timeout_micros: u64,
    ) -> FileIndex {
        let path_str = file_path.to_string_lossy();
        let mut out = FileIndex::default();

        // Parses `content` once for `lang_kind` at the caller's budget
        // (`timeout_micros`: `AstGuard::INDEX_PARSE_TIMEOUT_MICROS` when indexing) and
        // marks `out.parse_failed` on a
        // real failure — as opposed to `AstGuard::with_parser`'s bare `Option`, which
        // made "the parse failed" and "this language has no grammar" indistinguishable
        // from "it parsed and produced nothing" at every one of these call sites.
        macro_rules! parsed {
            ($f:expr) => {
                match AstGuard::parse_with(lang_kind, content, timeout_micros, $f) {
                    ParseOutcome::Parsed(r) => Some(r),
                    ParseOutcome::ParseFailed => {
                        out.parse_failed = true;
                        None
                    }
                    ParseOutcome::NoGrammar => None,
                }
            };
        }

        match lang_kind {
            LanguageKind::Protobuf => {
                if cfg.allows_proto_path(&path_str) {
                    if let Some((nodes, relations)) = parsed!(|tree| {
                        proto::ProtoExtractor::extract_with_relations(
                            file_path,
                            content,
                            repo_id,
                            cfg.canonical_fqcn_projection,
                            tree,
                        )
                    }) {
                        out.nodes = nodes;
                        out.dependencies = relations.dependencies;
                    }
                }
            }

            LanguageKind::Java => {
                let with_calls = Self::wants_call_facts(cfg, file_path);
                out.calls_skipped_path = !with_calls;
                if let Some((((nodes, dependencies, producers, rpc_calls), calls), inproc)) =
                    parsed!(|tree| {
                        (
                            (
                                java::JavaExtractor::extract_relations(
                                    file_path, content, repo_id, tree,
                                ),
                                if with_calls {
                                    java_calls::extract(tree, content.as_bytes())
                                } else {
                                    CallFacts::default()
                                },
                            ),
                            inproc_java::scan(tree, content),
                        )
                    })
                {
                    out.calls = calls;
                    for (i, node) in nodes.iter().enumerate() {
                        if node.kind == NodeKind::KafkaTopic {
                            out.consumers.push((i, node.name.clone()));
                        }
                    }
                    out.nodes = nodes;
                    out.dependencies = dependencies;
                    out.producers = producers;
                    out.rpc_calls = rpc_calls;
                    // Spring application events and schedules, tagged `InProc` before the
                    // Kafka default below reaches them.
                    inproc_java::apply(inproc, &mut out);
                    // Spring Kafka and `kafka-clients` are the only other brokers detected.
                    out.tag_brokers(Broker::Kafka);
                }
            }
            LanguageKind::Go => {
                let with_calls = Self::wants_call_facts(cfg, file_path);
                out.calls_skipped_path = !with_calls;
                if let Some(((nodes, relations), calls)) = parsed!(|tree| {
                    (
                        go::GoExtractor::extract_with_relations(file_path, content, repo_id, tree),
                        if with_calls {
                            go_calls::extract(tree, content.as_bytes())
                        } else {
                            CallFacts::default()
                        },
                    )
                }) {
                    out.calls = calls;
                    out.nodes = nodes;
                    out.dependencies = relations.dependencies;
                    out.producers = relations.producers;
                    out.consumers = relations.consumers;
                    out.rpc_calls = relations.rpc_calls;
                    // kafka-go, sarama and confluent-kafka-go are the only brokers detected.
                    out.tag_brokers(Broker::Kafka);
                }
            }
            LanguageKind::Python => {
                let with_calls = Self::wants_call_facts(cfg, file_path);
                out.calls_skipped_path = !with_calls;
                if let Some(((nodes, relations), calls)) = parsed!(|tree| {
                    (
                        python::PythonExtractor::extract_with_relations(
                            file_path, content, repo_id, tree,
                        ),
                        if with_calls {
                            py_calls::extract(tree, content.as_bytes())
                        } else {
                            CallFacts::default()
                        },
                    )
                }) {
                    out.calls = calls;
                    out.nodes = nodes;
                    out.dependencies = relations.dependencies;
                    out.producers = relations.producers;
                    out.consumers = relations.consumers;
                    out.rpc_calls = relations.rpc_calls;
                }
            }
            LanguageKind::TypeScript | LanguageKind::Tsx | LanguageKind::JavaScript => {
                // One parse under `kind`'s grammar, extracted from unless
                // `defer_if_erroneous` and the tree holds syntax errors (`None`).
                let run = |kind: LanguageKind, defer_if_erroneous: bool| {
                    AstGuard::parse_with(kind, content, timeout_micros, |tree| {
                        if defer_if_erroneous && tree.root_node().has_error() {
                            return None;
                        }
                        let mut imports = Vec::new();
                        let mut rpc_calls = Vec::new();
                        let mut nodes = typescript::TypeScriptExtractor::extract_with_config(
                            file_path,
                            content,
                            repo_id,
                            tree,
                            &mut imports,
                            &mut rpc_calls,
                            &cfg.controller_annotations,
                        );
                        // The path a file-system router is read from is the one below
                        // the workspace root, not the whole checkout path.
                        if let Some(routing_path) = Self::routing_path(cfg, file_path) {
                            nodes.extend(typescript::TypeScriptExtractor::file_route_nodes(
                                &routing_path,
                                file_path,
                                content,
                                repo_id,
                                tree,
                            ));
                        }
                        // Test files carry no call facts: a chain entered from a spec is
                        // not an architecture flow, and they only cost memory.
                        // Judged below the workspace root: a checkout under `~/tests/` is
                        // not test code.
                        let in_test = match Self::routing_path(cfg, file_path) {
                            Some(rel) => mesh_core::is_test_path(&rel),
                            None => file_path
                                .file_name()
                                .is_some_and(|n| mesh_core::is_test_path(Path::new(n))),
                        };
                        let calls = if in_test {
                            CallFacts::default()
                        } else {
                            ts_calls::extract(tree, content.as_bytes(), &|module| {
                                cfg.ts_aliases.rewrite(file_path, module)
                            })
                        };
                        Some((nodes, imports, rpc_calls, calls))
                    })
                };
                // Plain JavaScript is parsed by its own grammar. A `.js` file
                // that carries Flow / Babel annotations does not parse
                // cleanly there (its classes and types become error nodes),
                // but does under TSX: extract from the TSX tree when it is
                // clean, else from the JavaScript one. A TSX parse failure is
                // a failure of the file (counted and retried sequentially),
                // never a silent fall back to the JavaScript result, so the
                // outcome cannot depend on CPU contention.
                let mut outcome = run(lang_kind, lang_kind == LanguageKind::JavaScript);
                if let ParseOutcome::Parsed(None) = outcome {
                    outcome = match run(LanguageKind::Tsx, true) {
                        ParseOutcome::Parsed(None) => run(lang_kind, false),
                        other => other,
                    };
                }
                let (nodes, mut imports, rpc_calls, calls) = match outcome {
                    ParseOutcome::Parsed(Some(extracted)) => extracted,
                    ParseOutcome::Parsed(None) | ParseOutcome::NoGrammar => Default::default(),
                    ParseOutcome::ParseFailed => {
                        out.parse_failed = true;
                        Default::default()
                    }
                };
                // An aliased module (`@/services/User`, `#utils/log`) is the
                // relative path this file would have written for it.
                for (symbol, module) in &mut imports {
                    if symbol.is_empty() {
                        if let Some(relative) = cfg.ts_aliases.rewrite(file_path, module) {
                            *module = relative;
                        }
                    }
                }
                out.rpc_calls = rpc_calls;
                out.calls = calls;
                let mut nodes = nodes;
                out.inproc_capped = Self::cap_inproc_nodes(&mut nodes);
                Self::wire_ts_events(&nodes, &mut out);
                // A file with imports and no declaration (a barrel of
                // `export * from …`, a spec of `describe` calls) had nothing
                // to attach them to, so `find_dependents` never listed it:
                // Volontariapp 7.0.6, 34 of 57 importing specs and the
                // `bridge` barrel missing. One `Module` node spans the file.
                if nodes.is_empty() && !imports.is_empty() {
                    let name = file_path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or("module");
                    nodes.push(ContractNode {
                        id: 0,
                        name: CompactStr::new(name),
                        kind: NodeKind::Module,
                        file_path: std::sync::Arc::from(file_path),
                        line_start: 1,
                        line_end: content.lines().count().max(1),
                        package: mesh_core::detect_service_package(file_path, None),
                        repo_id,
                        signature: Some(CompactStr::new("module (imports only)")),
                        docstring: None,
                    });
                }

                if !imports.is_empty() {
                    // Imports are file-level facts: they go to the file's
                    // outermost declarations (classes, interfaces, functions),
                    // never to each method inside them — 7.0.0 gave every
                    // method of a file its own copy of every module import,
                    // so `find_dependents` listed `constructor`, `getLogger`, …
                    // once per method and once per import line.
                    let content_lines: Vec<&str> = content.lines().collect();
                    let mut seen: HashSet<(usize, &str)> = HashSet::new();
                    for (i, node) in nodes.iter().enumerate() {
                        if !Self::is_outermost(&nodes, i) {
                            continue;
                        }
                        let body = content_lines
                            .get(node.line_start.saturating_sub(1)..node.line_end)
                            .unwrap_or(&[]);

                        for (symbol, imported) in &imports {
                            if imported.is_empty() {
                                continue;
                            }
                            // A module (empty `symbol`) is a dependency of the
                            // whole file; a named symbol only where it is used.
                            let is_used = symbol.is_empty()
                                || body.iter().any(|l| l.contains(imported.as_str()));
                            if is_used && seen.insert((i, imported.as_str())) {
                                out.dependencies.push((i, CompactStr::new(imported)));
                            }
                        }
                    }
                }
                out.nodes = nodes;
            }
            LanguageKind::Rust => {
                if let Some(index) = parsed!(|tree| {
                    rust_lang::RustExtractor::extract_index(file_path, content, repo_id, tree)
                }) {
                    out.merge(index);
                    // `rdkafka` is the only broker detected.
                    out.tag_brokers(Broker::Kafka);
                }
            }
            LanguageKind::Cpp => {
                if let Some(index) = parsed!(|tree| {
                    cpp::CppExtractor::extract_file_index(file_path, content, repo_id, tree)
                }) {
                    out.merge(index);
                }
            }
            LanguageKind::Kotlin => {
                if let Some((nodes, relations)) = parsed!(|tree| {
                    kotlin::KotlinExtractor::extract_with_relations(
                        file_path, content, repo_id, tree,
                    )
                }) {
                    for (i, node) in nodes.iter().enumerate() {
                        if node.kind == NodeKind::KafkaTopic {
                            out.consumers.push((i, node.name.clone()));
                        }
                    }
                    for (i, topic) in relations.producers {
                        out.producers.push((i, topic));
                    }
                    for (i, topic) in relations.consumers {
                        out.consumers.push((i, topic));
                    }
                    out.nodes = nodes;
                    out.tag_brokers(Broker::Kafka);
                }
            }
            LanguageKind::CSharp => {
                if let Some((nodes, relations)) = parsed!(|tree| {
                    csharp::CSharpExtractor::extract_with_relations(
                        file_path, content, repo_id, tree,
                    )
                }) {
                    for (i, topic) in relations.producers {
                        out.producers.push((i, topic));
                    }
                    for (i, topic) in relations.consumers {
                        out.consumers.push((i, topic));
                    }
                    out.nodes = nodes;
                    // Confluent.Kafka is the only broker detected.
                    out.tag_brokers(Broker::Kafka);
                }
            }
            LanguageKind::Ruby => {
                if let Some(nodes) = parsed!(|tree| {
                    ruby::RubyExtractor::extract(file_path, content, repo_id, tree)
                }) {
                    out.nodes = nodes;
                }
            }
            LanguageKind::Php => {
                if let Some(nodes) = parsed!(|tree| {
                    php::PhpExtractor::extract(file_path, content, repo_id, tree)
                }) {
                    out.nodes = nodes;
                }
            }
            LanguageKind::Swift => {
                if let Some(nodes) = parsed!(|tree| {
                    swift::SwiftExtractor::extract(file_path, content, repo_id, tree)
                }) {
                    out.nodes = nodes;
                }
            }
            LanguageKind::Scala => {
                if let Some(nodes) = parsed!(|tree| {
                    scala::ScalaExtractor::extract(file_path, content, repo_id, tree)
                }) {
                    out.nodes = nodes;
                }
            }
            LanguageKind::Yaml => {
                Self::extract_yaml_contracts(file_path, content, repo_id, &mut out, cfg);
            }
            LanguageKind::Unknown => {}
        }

        // SQL held in string literals (outbox inserts, NOTIFY, LISTEN) of the host languages.
        let host = match lang_kind {
            LanguageKind::TypeScript | LanguageKind::Tsx | LanguageKind::JavaScript => {
                Some(sql::Host::Ts)
            }
            LanguageKind::Go => Some(sql::Host::Go),
            LanguageKind::Java => Some(sql::Host::Java),
            LanguageKind::Python => Some(sql::Host::Python),
            _ => None,
        };
        if let Some(host) = host.filter(|_| !out.parse_failed) {
            sql::add_embedded(&mut out, file_path, content, repo_id, host);
        }

        out
    }

    /// Evaluates user-defined declarative patterns from TOML configuration.
    /// Compiles the regexes on every call — prefer `extract_custom_patterns` with
    /// `CompiledPattern::compile_all` done once per run.
    pub fn apply_custom_patterns(
        file_path: &Path,
        content: &str,
        repo_id: RepoId,
        patterns: &[CustomPatternConfig],
        graph: &mut ContractGraph,
    ) {
        if patterns.is_empty() {
            return;
        }
        let compiled = CompiledPattern::compile_all(patterns);
        Self::extract_custom_patterns(file_path, content, repo_id, &compiled).apply(graph);
    }

    /// Runs precompiled declarative patterns over `content`. Safe to call from any thread.
    pub fn extract_custom_patterns(
        file_path: &Path,
        content: &str,
        repo_id: RepoId,
        patterns: &[CompiledPattern],
    ) -> FileIndex {
        let mut out = FileIndex::default();
        if patterns.is_empty() {
            return out;
        }

        let path_str = file_path.to_string_lossy();
        let mut package_name: Option<CompactStr> = None;
        let interned: mesh_core::FilePath = std::sync::Arc::from(file_path);

        for pat in patterns {
            if !pat.matches_path(&path_str) {
                continue;
            }

            // `captures_iter` yields ascending offsets, so the 1-based line of each
            // match is tracked incrementally instead of re-counting from byte 0.
            let mut line_cursor = LineCursor::default();
            for caps in pat.regex.captures_iter(content) {
                let line = caps
                    .get(0)
                    .map_or(1, |m| line_cursor.line_at(content, m.start()));
                let target = caps
                    .get(pat.target_group)
                    .map(|m| m.as_str().trim())
                    .unwrap_or("");
                if target.is_empty() {
                    continue;
                }

                let consumer_name = pat
                    .consumer_group
                    .and_then(|idx| caps.get(idx))
                    .map(|m| m.as_str().trim())
                    .unwrap_or(target);

                let (node_name, kind, signature) = match pat.kind {
                    PatternKind::TopicProducer => (
                        format!("produce:{target}"),
                        NodeKind::EventStream,
                        format!("Producer of {target}"),
                    ),
                    PatternKind::TopicConsumer => (
                        if consumer_name != target {
                            consumer_name.to_string()
                        } else {
                            format!("consume:{target}")
                        },
                        NodeKind::PostProcessor,
                        format!("Consumer of {target}"),
                    ),
                    PatternKind::Saga => (
                        target.to_string(),
                        NodeKind::Saga,
                        format!("Saga: {target}"),
                    ),
                    PatternKind::Rpc => (
                        format!("rpc:{target}"),
                        NodeKind::GrpcMethod,
                        format!("RPC call: {target}"),
                    ),
                };

                let package = package_name
                    .get_or_insert_with(|| mesh_core::detect_service_package(file_path, None))
                    .clone();

                let idx = out.nodes.len();
                out.nodes.push(ContractNode {
                    id: 0,
                    name: CompactStr::new(node_name),
                    kind,
                    file_path: interned.clone(),
                    line_start: line,
                    line_end: line,
                    package,
                    repo_id,
                    signature: Some(CompactStr::new(signature)),
                    docstring: None,
                });

                match pat.kind {
                    PatternKind::TopicProducer => {
                        out.producers.push((idx, CompactStr::new(target)));
                        if !pat.exact {
                            out.heuristic_topic_nodes.push(idx);
                        }
                    }
                    PatternKind::TopicConsumer => {
                        out.consumers.push((idx, CompactStr::new(target)));
                        if !pat.exact {
                            out.heuristic_topic_nodes.push(idx);
                        }
                    }
                    PatternKind::Rpc => out.rpc_calls.push((idx, CompactStr::new(target))),
                    PatternKind::Saga => {}
                }
            }
        }

        out
    }

    fn extract_yaml_contracts(
        file_path: &Path,
        content: &str,
        repo_id: RepoId,
        out: &mut FileIndex,
        cfg: &ExtractConfig,
    ) {
        let path_str = file_path.to_string_lossy().to_lowercase();
        let interned: mesh_core::FilePath = std::sync::Arc::from(file_path);

        // Check for AsyncAPI spec. `asyncapi_spec_files`, when configured, scopes
        // detection to those files exactly instead of the filename/content sniff.
        let is_asyncapi = if cfg.asyncapi_spec_files.is_empty() {
            path_str.contains("asyncapi") || content.contains("asyncapi:")
        } else {
            cfg.asyncapi_spec_files
                .iter()
                .any(|f| spec_file_matches(&path_str, f))
        };
        if is_asyncapi {
            if let Ok(spec) = spec_shape::from_yaml::<spec_shape::AsyncApiShape>(content) {
                let lines = YamlLines::new(content);
                let mut seek = lines.seeker(lines.key_line("channels", 1, usize::MAX));
                for name_str in spec.channels.0.iter().map(String::as_str) {
                    let line = seek.key(name_str).unwrap_or(1);
                    let node = ContractNode {
                        id: 0,
                        name: CompactStr::new(name_str),
                        kind: NodeKind::EventStream,
                        file_path: interned.clone(),
                        line_start: line,
                        line_end: line,
                        package: CompactStr::new("asyncapi"),
                        repo_id,
                        signature: Some(CompactStr::new(format!("channel {name_str}"))),
                        docstring: None,
                    };
                    out.producers
                        .push((out.nodes.len(), CompactStr::new(name_str)));
                    out.nodes.push(node);
                }

                // `infer_string_topics`: beyond the structured `channels` mapping,
                // also pick up a non-standard top-level `topics: [..]` string list.
                if cfg.infer_string_topics {
                    let mut seek = lines.seeker(lines.key_line("topics", 1, usize::MAX));
                    for name_str in spec.topics.0.iter().map(String::as_str) {
                        let line = seek.item(name_str).unwrap_or(1);
                        let node = ContractNode {
                            id: 0,
                            name: CompactStr::new(name_str),
                            kind: NodeKind::EventStream,
                            file_path: interned.clone(),
                            line_start: line,
                            line_end: line,
                            package: CompactStr::new("asyncapi"),
                            repo_id,
                            signature: Some(CompactStr::new(format!("inferred topic {name_str}"))),
                            docstring: None,
                        };
                        out.producers
                            .push((out.nodes.len(), CompactStr::new(name_str)));
                        out.nodes.push(node);
                    }
                }
            }
        }

        // Check for OpenAPI spec. `spec_files`, when configured, scopes detection
        // to those files exactly instead of the filename/content sniff.
        let is_openapi = if cfg.openapi_spec_files.is_empty() {
            path_str.contains("openapi")
                || content.contains("openapi:")
                || content.contains("swagger:")
        } else {
            cfg.openapi_spec_files
                .iter()
                .any(|f| spec_file_matches(&path_str, f))
        };
        if is_openapi {
            if let Ok(spec) = spec_shape::from_yaml::<spec_shape::OpenApiShape>(content) {
                let lines = YamlLines::new(content);
                // Pass 1: each path's line, in document order (one forward scan).
                let mut seek = lines.seeker(lines.key_line("paths", 1, usize::MAX));
                let path_lines: Vec<Option<usize>> =
                    spec.paths.0.iter().map(|(p, _)| seek.key(p)).collect();
                // Pass 2: a method is searched only between its path's line and the
                // next located path's line, so every line is scanned O(1) times.
                for (i, (p_str, methods)) in spec.paths.0.iter().enumerate() {
                    let path_line = path_lines[i];
                    let next_path = path_lines[i + 1..].iter().flatten().next().copied();
                    // An unlocated path's methods are not searched (they would
                    // match some other path's `get:`); they take line 1, as before.
                    let mut method_seek = path_line
                        .map(|l| lines.seeker_until(Some(l), next_path.unwrap_or(usize::MAX)));
                    for m_str in methods {
                        let ep_name = format!("{} {}", m_str.to_uppercase(), p_str);
                        let line = method_seek
                            .as_mut()
                            .and_then(|s| s.key(m_str))
                            .or(path_line)
                            .unwrap_or(1);
                        let node = ContractNode {
                            id: 0,
                            name: CompactStr::new(&ep_name),
                            kind: NodeKind::HttpEndpoint,
                            file_path: interned.clone(),
                            line_start: line,
                            line_end: line,
                            package: CompactStr::new("openapi"),
                            repo_id,
                            signature: Some(CompactStr::new(&ep_name)),
                            docstring: None,
                        };
                        out.nodes.push(node);
                    }
                }
            }
        }
    }
}

/// Incremental byte-offset -> 1-based line lookup for ascending offsets.
#[derive(Default)]
struct LineCursor {
    offset: usize,
    line: usize,
}

impl LineCursor {
    fn line_at(&mut self, content: &str, offset: usize) -> usize {
        if offset < self.offset {
            *self = Self::default();
        }
        let end = offset.min(content.len());
        let start = self.offset.min(end);
        self.line += content.as_bytes()[start..end]
            .iter()
            .filter(|b| **b == b'\n')
            .count();
        self.offset = end;
        self.line + 1
    }
}

/// A spec's lines, split once. `serde_yaml` keeps no source positions, so
/// contract nodes read from a parsed spec recover their declaration line
/// textually; a key that cannot be found (flow style, anchors) yields `None`, in
/// which case callers fall back to line 1 and `smart_search` treats the anchor
/// as unverified.
struct YamlLines<'a> {
    lines: Vec<&'a str>,
}

impl<'a> YamlLines<'a> {
    fn new(content: &'a str) -> Self {
        Self {
            lines: content.lines().collect(),
        }
    }

    /// 1-based line of the first `key:` mapping entry (bare, `"key"` or `'key'`)
    /// in lines `[from, until)`.
    fn key_line(&self, key: &str, from: usize, until: usize) -> Option<usize> {
        self.find(from, until, |l| {
            let t = l.trim_start().trim_start_matches("- ");
            let rest = t
                .strip_prefix(key)
                .or_else(|| t.strip_prefix('"')?.strip_prefix(key)?.strip_prefix('"'))
                .or_else(|| t.strip_prefix('\'')?.strip_prefix(key)?.strip_prefix('\''));
            rest.is_some_and(|r| r.starts_with(':'))
        })
    }

    /// 1-based line of the first `- item` sequence entry in lines `[from, until)`.
    fn item_line(&self, item: &str, from: usize, until: usize) -> Option<usize> {
        self.find(from, until, |l| {
            l.trim_start()
                .strip_prefix('-')
                .map(|r| r.trim().trim_matches(|c| c == '"' || c == '\''))
                == Some(item)
        })
    }

    fn find(&self, from: usize, until: usize, pred: impl Fn(&str) -> bool) -> Option<usize> {
        let start = from.saturating_sub(1).min(self.lines.len());
        let end = until.saturating_sub(1).clamp(start, self.lines.len());
        self.lines[start..end]
            .iter()
            .position(|l| pred(l))
            .map(|i| start + i + 1)
    }

    /// A forward-only cursor over entries after `section` (the section key's
    /// line), or from line 1 when the section key itself was not located.
    fn seeker(&self, section: Option<usize>) -> Seeker<'_, 'a> {
        self.seeker_until(section, usize::MAX)
    }

    fn seeker_until(&self, section: Option<usize>, until: usize) -> Seeker<'_, 'a> {
        Seeker {
            lines: self,
            cursor: section.map_or(1, |l| l + 1),
            until,
            misses_left: Seeker::MAX_MISSES,
        }
    }
}

/// Locates a section's entries in document order. Each lookup resumes after the
/// previous hit; a miss leaves the cursor where it was, so one unlocatable entry
/// (an escaped or multi-line key) does not cost the entries after it their lines.
/// A miss scans to the end of the range, so after [`Seeker::MAX_MISSES`] of them
/// the seeker gives up and every later entry falls back to line 1: that bounds a
/// section to `MAX_MISSES + 1` passes over its lines. Re-searching from the top
/// on every entry was O(entries × lines) — quadratic on a large spec, worse for
/// flow-style YAML where every lookup misses and scanned to EOF.
struct Seeker<'l, 'a> {
    lines: &'l YamlLines<'a>,
    cursor: usize,
    until: usize,
    misses_left: u8,
}

impl Seeker<'_, '_> {
    const MAX_MISSES: u8 = 8;

    fn key(&mut self, key: &str) -> Option<usize> {
        self.advance(|lines, from, until| lines.key_line(key, from, until))
    }

    fn item(&mut self, item: &str) -> Option<usize> {
        self.advance(|lines, from, until| lines.item_line(item, from, until))
    }

    fn advance(
        &mut self,
        find: impl FnOnce(&YamlLines<'_>, usize, usize) -> Option<usize>,
    ) -> Option<usize> {
        if self.misses_left == 0 {
            return None;
        }
        let found = find(self.lines, self.cursor, self.until);
        match found {
            Some(line) => self.cursor = line + 1,
            None => self.misses_left -= 1,
        }
        found
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(name: &str, path: &Path) -> ContractNode {
        ContractNode {
            id: 0,
            name: CompactStr::new(name),
            kind: NodeKind::ServiceClass,
            file_path: Arc::from(path),
            line_start: 1,
            line_end: 2,
            package: CompactStr::new("pkg"),
            repo_id: 3,
            signature: Some(CompactStr::new("sig")),
            docstring: Some(CompactStr::new("doc")),
        }
    }

    /// The shared parse cache's record shape (8.0.0): every node path under the root is
    /// stored root-relative and `/`-separated, the record names no checkout directory, and
    /// re-anchoring on another root gives that root's paths; a relative placeholder path is
    /// left alone both ways, and every non-path field is carried through unchanged.
    #[test]
    fn relativize_then_reanchor_moves_a_record_between_checkouts() {
        let root_a = std::env::temp_dir().join("ckout-a").join("repo");
        let root_b = std::env::temp_dir().join("other").join("ckout-b");
        let file_a = root_a.join("src").join("svc").join("a.ts");
        let mut index = FileIndex {
            nodes: vec![
                node("A", &file_a),
                node("B", &file_a),
                node("placeholder", Path::new("")),
            ],
            dependencies: vec![(0, CompactStr::new("../lib/x"))],
            producers: vec![(1, CompactStr::new("orders.created"))],
            consumers: vec![(1, CompactStr::new("orders.paid"))],
            rpc_calls: vec![(0, CompactStr::new("Billing.Charge"))],
            heuristic_topic_nodes: vec![1],
            producer_brokers: vec![(0, Broker::Kafka)],
            consumer_brokers: Vec::new(),
            node_brokers: Vec::new(),
            parse_failed: false,
            calls: CallFacts::default(),
            calls_skipped_path: false,
            sql_unresolved: 0,
            sql_capped: false,
            inproc_capped: false,
        };
        let before = format!("{index:?}");
        let anchored = index.relativize(&root_a).expect("all under root");
        assert_eq!(anchored, vec![0, 1]);
        assert_eq!(index.nodes[0].file_path.to_str(), Some("src/svc/a.ts"));
        assert_eq!(index.nodes[2].file_path.as_os_str(), "");

        let json = serde_json::to_string(&RootRelativeIndex {
            index: index.clone(),
            anchored: anchored.clone(),
        })
        .expect("serialize");
        assert!(!json.contains("ckout-a"), "{json}");
        let back: RootRelativeIndex = serde_json::from_str(&json).expect("deserialize");

        let mut on_a = back.index.clone();
        on_a.reanchor(&root_a, &back.anchored);
        assert_eq!(format!("{on_a:?}"), before, "round trip on the same root");

        let mut on_b = back.index;
        on_b.reanchor(&root_b, &back.anchored);
        let file_b = root_b.join("src").join("svc").join("a.ts");
        assert_eq!(&*on_b.nodes[0].file_path, file_b.as_path());
        assert_eq!(&*on_b.nodes[1].file_path, file_b.as_path());
        assert!(Arc::ptr_eq(
            &on_b.nodes[0].file_path,
            &on_b.nodes[1].file_path
        ));
        assert_eq!(on_b.nodes[2].file_path.as_os_str(), "");
        assert_eq!(
            format!("{on_b:?}"),
            before.replace(
                &format!("{:?}", file_a.as_path()),
                &format!("{:?}", file_b.as_path())
            )
        );
    }

    /// A node outside the root has no form another checkout could re-anchor: the record is
    /// refused (not cached) and left as it was.
    #[test]
    fn relativize_refuses_a_node_outside_the_root() {
        let root = std::env::temp_dir().join("ckout").join("repo");
        let mut index = FileIndex {
            nodes: vec![
                node("in", &root.join("a.rs")),
                node("out", &std::env::temp_dir().join("elsewhere.rs")),
            ],
            ..FileIndex::default()
        };
        let before = format!("{index:?}");
        assert!(index.relativize(&root).is_none());
        assert_eq!(format!("{index:?}"), before);
        assert_eq!(root_relative(&root, &root), None);
        assert_eq!(
            root_relative(&root.join("x").join("y.rs"), &root).as_deref(),
            Some("x/y.rs")
        );
    }

    /// The parent directory's name (for `index.*`) and the path substring tests are part of
    /// every key: same answers, same bytes. The inferred package comes from the manifest in
    /// both checkouts, and from the checkout's own directory name without one.
    #[test]
    fn path_context_folds_every_path_derived_decision() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let base = dunce::canonicalize(tmp.path()).expect("canon");
        let (a, b) = (base.join("alpha"), base.join("beta"));
        for root in [&a, &b] {
            std::fs::create_dir_all(root.join("svc/routes")).expect("mkdir");
            std::fs::write(root.join("svc/package.json"), r#"{ "name": "orders" }"#)
                .expect("manifest");
        }
        let cfg = ExtractConfig {
            proto_dirs: vec!["contracts".to_string()],
            ..ExtractConfig::default()
        };
        let ctx = PathContext::default();
        let key_input = |p: &Path| {
            let mut out = Vec::new();
            ctx.append_key_input(&cfg, p, &mut out);
            String::from_utf8(out).expect("utf8")
        };
        let in_a = key_input(&a.join("svc/routes/index.ts"));
        assert_eq!(in_a, key_input(&b.join("svc/routes/index.ts")));
        assert!(in_a.contains("dir=routes"), "{in_a}");
        // Only an `index.*` file depends on its directory name.
        assert!(key_input(&a.join("svc/a.ts")).contains("dir=\0"));
        // At the root, an `index.ts` is named after the checkout itself.
        assert_ne!(
            key_input(&a.join("index.ts")),
            key_input(&b.join("index.ts"))
        );
        assert_eq!(key_input(&a.join("tool.ts")), key_input(&b.join("tool.ts")));
        // A substring test over the absolute path is captured.
        let c = base.join("contracts-ws");
        assert_ne!(
            key_input(&c.join("svc/a.proto")),
            key_input(&a.join("svc/a.proto"))
        );

        // The inferred package: from the manifest in both checkouts; the checkout
        // directory itself without one.
        assert_eq!(ctx.package_of(&a.join("svc/routes/x.ts")), "orders");
        assert_eq!(ctx.package_of(&b.join("svc/routes/x.ts")), "orders");
        assert_eq!(ctx.package_of(&a.join("tool.py")), "alpha");
        assert_eq!(ctx.package_of(&b.join("tool.py")), "beta");
    }

    /// A record uses the inferred package when one of its nodes carries it; a file whose
    /// nodes all declare another package does not, so its first key can be shared.
    #[test]
    fn uses_package_spots_a_node_carrying_the_inferred_package() {
        let mut declared = node("A", Path::new("/r/a.go"));
        declared.package = CompactStr::new("billing");
        let mut inferred = node("B", Path::new("/r/a.go"));
        inferred.package = CompactStr::new("alpha");
        let only_declared = FileIndex {
            nodes: vec![declared.clone()],
            ..FileIndex::default()
        };
        assert!(!only_declared.uses_package("alpha"));
        let mixed = FileIndex {
            nodes: vec![declared, inferred],
            ..FileIndex::default()
        };
        assert!(mixed.uses_package("alpha"));
        assert!(!FileIndex::default().uses_package("alpha"));
    }

    /// 7.0.1 (Volontariapp report, point 2 and 5): a module import belongs to
    /// the file's outermost declarations, once, whatever the number of import
    /// lines; a bare package (`rxjs`) is a module, not a symbol to find in the
    /// body; a subpath import counts for its package; a 1-2 character target
    /// never falls back to substring matching.
    #[test]
    fn ts_imports_attach_once_to_top_level_declarations() {
        let mut graph = ContractGraph::new();
        let ts = r#"import { fromEvent } from 'rxjs';
import { JobOutbox } from '@scope/messaging';
import { OutboxStatus } from '@scope/messaging';
import { testing } from '@scope/messaging/testing';

export class JobOutboxFailedPostProcessor {
  constructor(private readonly outbox: JobOutbox) {}
  getLogger() { return null; }
  handle() { return fromEvent(null, 'x'); }
}
"#;
        PolyglotIndexer::index_file(Path::new("svc/src/job.processor.ts"), ts, 0, &mut graph);
        graph.reconcile_edges();

        for target in ["rxjs", "@scope/messaging"] {
            let names: Vec<&str> = graph
                .find_dependents(target)
                .iter()
                .map(|n| n.name.as_str())
                .collect();
            assert_eq!(names, ["JobOutboxFailedPostProcessor"], "{target}");
        }
        let (_, how) = graph.find_dependents_matched("@scope/messaging");
        assert_eq!(how, mesh_core::DependentsMatch::Import);
        // Only through the `/testing` subpath import.
        assert_eq!(graph.find_dependents("@scope/messaging/testing").len(), 1);
        assert!(graph.find_dependents("sc").is_empty());
        let (sub, how) = graph.find_dependents_matched("messag");
        assert_eq!(how, mesh_core::DependentsMatch::Substring);
        assert_eq!(sub.len(), 1);
    }

    /// 7.0.1 (report point 6): TypeScript event facts reach the topic registry
    /// through the real pipeline — producer and consumer meet on one topic.
    #[test]
    fn ts_events_link_producer_and_consumer_methods() {
        let mut graph = ContractGraph::new();
        PolyglotIndexer::index_file(
            Path::new("orders/src/orders.service.ts"),
            r#"
import { ClientProxy } from '@nestjs/microservices';
export class OrdersService {
  place() { return this.client.emit('order.placed', {}); }
  async publish() { await producer.send({ topic: 'order.shipped', messages: [] }); }
}
"#,
            0,
            &mut graph,
        );
        PolyglotIndexer::index_file(
            Path::new("billing/src/billing.controller.ts"),
            r#"
export class BillingController {
  @EventPattern('order.placed')
  onPlaced(data: unknown) {}
  async start() { await consumer.subscribe({ topic: 'order.shipped' }); }
}
"#,
            1,
            &mut graph,
        );
        graph.reconcile_edges();
        // 9.0: a call-site producer / consumer is the call itself (its line), not the
        // method enclosing it; an `@EventPattern` handler is still the method.
        for (topic, producer_line, consumer_line) in
            [("order.placed", 4, 4), ("order.shipped", 5, 5)]
        {
            let flow = graph.analyze_impact(topic);
            let producers: Vec<usize> = flow
                .upstream_producers
                .iter()
                .map(|n| n.line_start)
                .collect();
            let consumers: Vec<usize> = flow
                .downstream_consumers
                .iter()
                .map(|n| n.line_start)
                .collect();
            assert_eq!(producers, vec![producer_line], "{topic}");
            assert_eq!(consumers, vec![consumer_line], "{topic}");
        }
    }

    /// 7.0.1 (report point 3): three spellings of one enum member — a type
    /// argument, a `typeof` and a stream constant — are one topic, and the
    /// exact topic wins over one merely containing its name.
    #[test]
    fn custom_pattern_topic_spellings_share_one_key() {
        let patterns = CompiledPattern::compile_all(&[
            CustomPatternConfig {
                name: "producer".into(),
                kind: PatternKind::TopicProducer,
                file_pattern: Some("*.ts".into()),
                regex: r"createEvent<([A-Za-z0-9_.]+)>".into(),
                target_group: 1,
                consumer_group: None,
                confidence: None,
            },
            CustomPatternConfig {
                name: "stream".into(),
                kind: PatternKind::TopicProducer,
                file_pattern: Some("*.ts".into()),
                regex: r"targetServices:\s*\[\s*(\w+\.\w+)".into(),
                target_group: 1,
                consumer_group: None,
                confidence: None,
            },
            CustomPatternConfig {
                name: "options".into(),
                kind: PatternKind::TopicConsumer,
                file_pattern: Some("options.ts".into()),
                regex: r"streamName:\s*get\w*StreamName\((\w+\.\w+)\)".into(),
                target_group: 1,
                consumer_group: None,
                confidence: None,
            },
            CustomPatternConfig {
                name: "consumer".into(),
                kind: PatternKind::TopicConsumer,
                file_pattern: Some("*.ts".into()),
                regex: r"class\s+(\w+)\s+extends\s+BatchPostProcessor<([^>]+)>".into(),
                target_group: 2,
                consumer_group: Some(1),
                confidence: None,
            },
        ]);
        let mut graph = ContractGraph::new();
        for (path, code) in [
            ("a/repo.ts", "this.outbox.createEvent<EventEventMessagingType.EVENT_CREATED>(e);\n"),
            ("a/pusher.ts", "const o = { targetServices: [Streams.EVENT_CREATED] };\nconst f = { targetServices: [Streams.WS_EVENT_CREATED_FEEDBACK] };\n"),
            ("b/pp.ts", "export class EventCreatedPostProcessor extends BatchPostProcessor<typeof EventMessagingType.EVENT_CREATED> {}\n"),
            ("c/feedback.options.ts", "export const o = { streamName: getEventStreamName(Streams.WS_EVENT_CREATED_FEEDBACK) };\n"),
        ] {
            PolyglotIndexer::extract_custom_patterns(Path::new(path), code, 0, &patterns)
                .apply(&mut graph);
        }
        graph.reconcile_edges();
        let flow = graph.analyze_impact("EVENT_CREATED");
        let hubs: Vec<&str> = flow
            .topics
            .iter()
            .filter(|n| n.package == "event-bus")
            .map(|n| n.name.as_str())
            .collect();
        assert_eq!(hubs, ["event_created"], "{hubs:?}");
        let producers: Vec<&str> = flow
            .upstream_producers
            .iter()
            .map(|n| n.name.as_str())
            .collect();
        assert_eq!(producers.len(), 2, "{producers:?}");
        assert!(
            producers.iter().all(|p| !p.contains("FEEDBACK")),
            "{producers:?}"
        );
        let consumers: Vec<&str> = flow
            .downstream_consumers
            .iter()
            .map(|n| n.name.as_str())
            .collect();
        assert!(
            consumers.contains(&"EventCreatedPostProcessor"),
            "{consumers:?}"
        );
        assert!(
            consumers.iter().all(|c| !c.contains("FEEDBACK")),
            "{consumers:?}"
        );
        // Same answer whichever spelling is asked.
        let again = graph.analyze_impact("Streams.EVENT_CREATED");
        assert_eq!(again.upstream_producers.len(), 2);
        assert_eq!(
            again.downstream_consumers.len(),
            flow.downstream_consumers.len()
        );
    }

    #[test]
    fn test_polyglot_indexer_cpp() {
        let mut graph = ContractGraph::new();
        let cpp = "namespace billing {\nclass Invoice {};\nvoid HandleCharge(int amount) {}\n}\n";
        PolyglotIndexer::index_file(
            Path::new("services/billing/invoice.hpp"),
            cpp,
            0,
            &mut graph,
        );
        assert_eq!(graph.node_count(), 2);
        assert!(graph.all_nodes().any(|n| n.name == "Invoice"));
        assert!(graph
            .all_nodes()
            .any(|n| n.name == "HandleCharge" && n.kind == NodeKind::HttpEndpoint));
    }

    /// Wiring check for items 2/3/5: the per-language extractors gained
    /// relations-aware entry points (`extract_relations`/`extract_with_relations`/
    /// `extract_index`/`extract_file_index`), but `PolyglotIndexer::extract`'s
    /// dispatch is what the real indexing pipeline (`WorkspaceIndexer`) actually
    /// calls. These tests go through `index_file` (which calls the real dispatch),
    /// not the per-language extractor directly, so a regression here means the
    /// feature is unreachable in production even if the language's own unit tests
    /// pass.
    #[test]
    fn test_polyglot_indexer_wires_java_import_dependency() {
        let mut graph = ContractGraph::new();
        PolyglotIndexer::index_file(
            Path::new("services/billing/Widget.java"),
            "package billing;\npublic class Widget {}\n",
            0,
            &mut graph,
        );
        PolyglotIndexer::index_file(
            Path::new("services/orders/Consumer.java"),
            "package orders;\nimport billing.Widget;\npublic class Consumer {\n    void use(Widget w) {}\n}\n",
            0,
            &mut graph,
        );
        assert!(
            !graph.find_dependents("Widget").is_empty(),
            "Java import dependency must reach find_dependents through the real PolyglotIndexer dispatch"
        );
    }

    #[test]
    fn test_polyglot_indexer_wires_go_import_dependency() {
        // Go dependencies are keyed by the raw import path string (Go imports a
        // whole package, not a specific symbol) — mirrors go.rs's own
        // `find_dependents_links_consumer_via_go_import` test.
        let mut graph = ContractGraph::new();
        PolyglotIndexer::index_file(
            Path::new("utils/helper.go"),
            "package utils\n\nfunc Helper() string {\n    return \"ok\"\n}\n",
            0,
            &mut graph,
        );
        PolyglotIndexer::index_file(
            Path::new("main.go"),
            "package main\n\nimport (\n    \"myapp/utils\"\n)\n\nfunc Run() {\n    utils.Helper()\n}\n",
            0,
            &mut graph,
        );
        assert!(
            graph
                .find_dependents("myapp/utils")
                .iter()
                .any(|n| n.name == "Run"),
            "Go import dependency must reach find_dependents through the real PolyglotIndexer dispatch"
        );
    }

    #[test]
    fn test_polyglot_indexer_wires_rust_use_dependency() {
        let mut graph = ContractGraph::new();
        PolyglotIndexer::index_file(
            Path::new("services/billing/widget.rs"),
            "pub struct Widget;\n",
            0,
            &mut graph,
        );
        PolyglotIndexer::index_file(
            Path::new("services/orders/consumer.rs"),
            "use billing::Widget;\n\nfn use_it(_w: Widget) {}\n",
            0,
            &mut graph,
        );
        assert!(
            !graph.find_dependents("Widget").is_empty(),
            "Rust use dependency must reach find_dependents through the real PolyglotIndexer dispatch"
        );
    }

    #[test]
    fn test_polyglot_indexer_wires_cpp_include_dependency() {
        // C++ dependencies are keyed by the #include's file stem, matched against
        // literal usage in the consumer's body — mirrors cpp.rs's own
        // `test_cpp_include_dependency_resolves_via_find_dependents` test, so the
        // header's stem ("Shape") must match the symbol name used in the body.
        let mut graph = ContractGraph::new();
        PolyglotIndexer::index_file(
            Path::new("Shape.hpp"),
            "class Shape {\npublic:\n    virtual ~Shape() = default;\n    virtual double Area() const = 0;\n};\n",
            0,
            &mut graph,
        );
        PolyglotIndexer::index_file(
            Path::new("circle.cpp"),
            "#include \"Shape.hpp\"\n#include <vector>\n\nclass Circle : public Shape {\npublic:\n    double Area() const override { return 3.14; }\n};\n",
            0,
            &mut graph,
        );
        assert!(
            graph
                .find_dependents("Shape")
                .iter()
                .any(|n| n.name == "Circle"),
            "C++ #include dependency must reach find_dependents through the real PolyglotIndexer dispatch"
        );
    }

    #[test]
    fn test_polyglot_indexer_proto() {
        let mut graph = ContractGraph::new();
        let proto = "syntax = \"proto3\"; package test.v1; service TestService { rpc DoTest (Req) returns (Resp); }";
        PolyglotIndexer::index_file(Path::new("test.proto"), proto, 0, &mut graph);
        assert_eq!(graph.node_count(), 2);
    }

    #[test]
    fn test_polyglot_indexer_asyncapi() {
        let mut graph = ContractGraph::new();
        let yaml = r#"
asyncapi: 2.6.0
channels:
  billing.events:
    description: Billing event topic
"#;
        PolyglotIndexer::index_file(Path::new("asyncapi.yaml"), yaml, 0, &mut graph);
        assert_eq!(graph.node_count(), 1);
        let impact = graph.analyze_impact("billing.events");
        assert!(!impact.topics.is_empty());
    }

    /// Line recovery for spec entries: one unlocatable key (an escaped quote
    /// the text search cannot match) must not cost the keys after it their
    /// lines, and an OpenAPI method is only searched inside its own path.
    #[test]
    fn spec_line_recovery_survives_a_miss_and_is_path_bounded() {
        let asyncapi = "asyncapi: 2.6.0\nchannels:\n  'it''s.escaped':\n    description: x\n  orders.created:\n    description: y\n";
        let idx = PolyglotIndexer::extract_with_config(
            Path::new("asyncapi.yaml"),
            asyncapi,
            0,
            &ExtractConfig::default(),
        );
        let line_of = |name: &str| {
            idx.nodes
                .iter()
                .find(|n| n.name.as_str() == name)
                .map(|n| n.line_start)
        };
        assert_eq!(line_of("it's.escaped"), Some(1), "unlocatable key");
        assert_eq!(line_of("orders.created"), Some(5), "key after a miss");

        let openapi = "openapi: 3.0.0\npaths:\n  /a: {get: {}}\n  /b:\n    post: {}\n    get: {}\n";
        let idx = PolyglotIndexer::extract_with_config(
            Path::new("openapi.yaml"),
            openapi,
            0,
            &ExtractConfig::default(),
        );
        let line_of = |name: &str| {
            idx.nodes
                .iter()
                .find(|n| n.name.as_str() == name)
                .map(|n| n.line_start)
        };
        assert_eq!(
            line_of("GET /a"),
            Some(3),
            "flow-style method: its path's line, not /b's get"
        );
        assert_eq!(line_of("POST /b"), Some(5));
        assert_eq!(line_of("GET /b"), Some(6));
    }

    /// Flow-style YAML misses every key; the seeker stops searching after
    /// `MAX_MISSES` instead of rescanning the file once per entry.
    #[test]
    fn seeker_gives_up_after_its_miss_budget() {
        let content = (0..50).map(|i| format!("k{i}: v\n")).collect::<String>();
        let lines = YamlLines::new(&content);
        let mut seek = lines.seeker(None);
        for _ in 0..Seeker::MAX_MISSES {
            assert_eq!(seek.key("absent"), None);
        }
        assert_eq!(seek.key("k3"), None, "budget spent: no more searching");
        let mut fresh = lines.seeker(None);
        assert_eq!(fresh.key("absent"), None);
        assert_eq!(fresh.key("k3"), Some(4), "a miss keeps the cursor");
        assert_eq!(fresh.key("k1"), None, "forward-only");
    }

    #[test]
    fn test_declarative_custom_patterns() {
        let mut graph = ContractGraph::new();
        let patterns = vec![
            CustomPatternConfig {
                name: "outbox_producer".to_string(),
                kind: PatternKind::TopicProducer,
                file_pattern: Some("*.ts".to_string()),
                regex: r#"createEvent<([^>]+)>"#.to_string(),
                target_group: 1,
                consumer_group: None,
                confidence: None,
            },
            CustomPatternConfig {
                name: "post_processor_consumer".to_string(),
                kind: PatternKind::TopicConsumer,
                file_pattern: Some("*.ts".to_string()),
                regex: r#"class\s+(\w+)\s+extends\s+\w*PostProcessor<([^>]+)>"#.to_string(),
                target_group: 2,
                consumer_group: Some(1),
                confidence: None,
            },
        ];

        let ts_producer = "await EventQueueEntity.createEvent<UserCreatedEvent>(event);";
        let ts_consumer = "export class UserCreatedPostProcessor extends SinglePostProcessor<UserCreatedEvent> {}";

        PolyglotIndexer::apply_custom_patterns(
            Path::new("services/user/producer.ts"),
            ts_producer,
            1,
            &patterns,
            &mut graph,
        );
        PolyglotIndexer::apply_custom_patterns(
            Path::new("services/user/consumer.ts"),
            ts_consumer,
            1,
            &patterns,
            &mut graph,
        );

        let impact = graph.analyze_impact("UserCreatedEvent");
        assert_eq!(impact.upstream_producers.len(), 1);
        assert_eq!(impact.downstream_consumers.len(), 1);
        assert_eq!(
            impact.downstream_consumers[0].name.as_str(),
            "UserCreatedPostProcessor"
        );
    }

    #[test]
    fn test_proto_dirs_scopes_extraction() {
        let proto = "syntax = \"proto3\"; package test.v1; service TestService { rpc DoTest (Req) returns (Resp); }";

        // Unrestricted (default): a `.proto` file anywhere is extracted.
        let default_cfg = ExtractConfig::default();
        let unrestricted = PolyglotIndexer::extract_with_config(
            Path::new("services/other/test.proto"),
            proto,
            0,
            &default_cfg,
        );
        assert_eq!(unrestricted.nodes.len(), 2);

        // `proto_dirs` configured: files outside every configured dir are not
        // extracted as protobuf at all.
        let scoped_cfg = ExtractConfig {
            proto_dirs: vec!["proto-registry".to_string()],
            ..ExtractConfig::default()
        };
        let outside = PolyglotIndexer::extract_with_config(
            Path::new("services/other/test.proto"),
            proto,
            0,
            &scoped_cfg,
        );
        assert!(outside.nodes.is_empty());

        let inside = PolyglotIndexer::extract_with_config(
            Path::new("proto-registry/test.proto"),
            proto,
            0,
            &scoped_cfg,
        );
        assert_eq!(inside.nodes.len(), 2);
    }

    #[test]
    fn test_openapi_asyncapi_spec_files_scopes_detection() {
        let openapi_yaml = r#"
paths:
  /users:
    get:
      summary: list users
"#;
        // File neither named nor containing an "openapi"/"swagger" sniff hint,
        // so the default (unconfigured) sniffing misses it entirely.
        let default_cfg = ExtractConfig::default();
        let missed = PolyglotIndexer::extract_with_config(
            Path::new("services/billing/api-contract.yaml"),
            openapi_yaml,
            0,
            &default_cfg,
        );
        assert!(missed.nodes.is_empty());

        // `spec_files` configured to name this exact file: now it is scoped in,
        // regardless of filename/content sniffing.
        let scoped_cfg = ExtractConfig {
            openapi_spec_files: vec!["api-contract.yaml".to_string()],
            ..ExtractConfig::default()
        };
        let scoped = PolyglotIndexer::extract_with_config(
            Path::new("services/billing/api-contract.yaml"),
            openapi_yaml,
            0,
            &scoped_cfg,
        );
        assert_eq!(scoped.nodes.len(), 1);
        assert!(scoped.nodes.iter().any(|n| n.name == "GET /users"));
    }

    #[test]
    fn test_infer_string_topics_toggle() {
        let yaml = r#"
asyncapi: 2.6.0
channels:
  billing.events:
    description: Billing event topic
topics:
  - legacy.orders.created
"#;
        // Default: `infer_string_topics = true` picks up both the structured
        // `channels` entry and the non-standard `topics:` string list.
        let default_cfg = ExtractConfig::default();
        let with_inference =
            PolyglotIndexer::extract_with_config(Path::new("asyncapi.yaml"), yaml, 0, &default_cfg);
        assert_eq!(with_inference.nodes.len(), 2);
        assert!(with_inference
            .nodes
            .iter()
            .any(|n| n.name == "legacy.orders.created"));

        // Disabled: only the structured `channels` entry is extracted.
        let disabled_cfg = ExtractConfig {
            infer_string_topics: false,
            ..ExtractConfig::default()
        };
        let without_inference = PolyglotIndexer::extract_with_config(
            Path::new("asyncapi.yaml"),
            yaml,
            0,
            &disabled_cfg,
        );
        assert_eq!(without_inference.nodes.len(), 1);
        assert!(!without_inference
            .nodes
            .iter()
            .any(|n| n.name == "legacy.orders.created"));
    }

    #[test]
    fn test_extract_config_from_contracts() {
        use mesh_core::config::{AsyncApiConfig, ContractsConfig, GrpcConfig, OpenApiConfig};

        let contracts = ContractsConfig {
            enabled: true,
            grpc: Some(GrpcConfig {
                proto_dirs: vec!["proto-registry".to_string()],
                controller_annotations: vec!["@RpcHandler".to_string()],
                canonical_fqcn_projection: false,
            }),
            spring: None,
            openapi: Some(OpenApiConfig {
                enabled: true,
                spec_files: vec!["openapi.yaml".to_string()],
            }),
            asyncapi: Some(AsyncApiConfig {
                enabled: true,
                spec_files: vec!["asyncapi.yaml".to_string()],
                infer_string_topics: false,
            }),
            cpp: None,
            patterns: Vec::new(),
        };

        let cfg = ExtractConfig::from_contracts(&contracts);
        assert_eq!(cfg.proto_dirs, vec!["proto-registry".to_string()]);
        assert_eq!(cfg.controller_annotations, vec!["@RpcHandler".to_string()]);
        assert!(!cfg.canonical_fqcn_projection);
        assert_eq!(cfg.openapi_spec_files, vec!["openapi.yaml".to_string()]);
        assert_eq!(cfg.asyncapi_spec_files, vec!["asyncapi.yaml".to_string()]);
        assert!(!cfg.infer_string_topics);
    }

    /// 7.1.8: declarative topic patterns default to heuristic edges (a regex can
    /// match an emission routed through a mapping table); `confidence = "exact"`
    /// opts a pattern back in, per pattern.
    #[test]
    fn declarative_topic_patterns_default_to_heuristic_confidence() {
        use mesh_core::{EdgeConfidence, EdgeKind, PatternConfidence};
        let mk = |name: &str, kind, regex: &str, confidence| CustomPatternConfig {
            name: name.into(),
            kind,
            file_pattern: None,
            regex: regex.into(),
            target_group: 1,
            consumer_group: None,
            confidence,
        };
        let patterns = CompiledPattern::compile_all(&[
            mk(
                "p-default",
                PatternKind::TopicProducer,
                r#"emit\("([^"]+)"\)"#,
                None,
            ),
            mk(
                "p-exact",
                PatternKind::TopicProducer,
                r#"literal\("([^"]+)"\)"#,
                Some(PatternConfidence::Exact),
            ),
            mk(
                "c-default",
                PatternKind::TopicConsumer,
                r#"on\("([^"]+)"\)"#,
                None,
            ),
        ]);
        let mut graph = ContractGraph::new();
        let src = "emit(\"t.mapped\")\nliteral(\"t.direct\")\non(\"t.mapped\")\non(\"t.direct\")\n";
        PolyglotIndexer::extract_custom_patterns(Path::new("x.ts"), src, 0, &patterns)
            .apply(&mut graph);
        graph.reconcile_edges();
        let conf = |topic: &str, kind: EdgeKind| -> Vec<EdgeConfidence> {
            let mut v: Vec<_> = graph
                .all_edges()
                .iter()
                .filter(|e| e.kind == kind && e.metadata.as_deref() == Some(topic))
                .map(|e| e.confidence)
                .collect();
            v.sort_by_key(|c| *c as u8);
            v
        };
        assert_eq!(
            conf("t.mapped", EdgeKind::Produces),
            [EdgeConfidence::Heuristic]
        );
        assert_eq!(
            conf("t.direct", EdgeKind::Produces),
            [EdgeConfidence::Exact]
        );
        // The impact matrix reports the same confidence per producer row.
        let producer_row = |topic: &str| {
            graph
                .impact_matrix(topic, 1)
                .rows
                .iter()
                .find(|r| r.role == mesh_core::ImpactRole::Producer)
                .map(|r| r.confidence)
        };
        assert_eq!(producer_row("t.mapped"), Some(EdgeConfidence::Heuristic));
        assert_eq!(producer_row("t.direct"), Some(EdgeConfidence::Exact));
        // The default-confidence consumer stays heuristic on both topics.
        assert_eq!(
            conf("t.mapped", EdgeKind::Consumes),
            [EdgeConfidence::Heuristic]
        );
        assert_eq!(
            conf("t.direct", EdgeKind::Consumes),
            [EdgeConfidence::Heuristic]
        );
    }

    /// Pattern / AsyncAPI / OpenAPI nodes carry their real declaration line, not
    /// a placeholder `1` (smart_search anchors snippets on it).
    #[test]
    fn declarative_nodes_record_real_lines() {
        let patterns = CompiledPattern::compile_all(&[CustomPatternConfig {
            name: "producer".to_string(),
            kind: PatternKind::TopicProducer,
            file_pattern: None,
            regex: r#"send\("([^"]+)"\)"#.to_string(),
            target_group: 1,
            consumer_group: None,
            confidence: None,
        }]);
        let src = "a\nb\nsend(\"t.one\")\nc\n\nsend(\"t.two\")\n";
        let out = PolyglotIndexer::extract_custom_patterns(Path::new("x.java"), src, 0, &patterns);
        let lines: Vec<(String, usize)> = out
            .nodes
            .iter()
            .map(|n| (n.name.to_string(), n.line_start))
            .collect();
        assert_eq!(
            lines,
            vec![
                ("produce:t.one".to_string(), 3),
                ("produce:t.two".to_string(), 6)
            ]
        );

        let asyncapi = "asyncapi: 2.6.0\ninfo:\n  title: x\nchannels:\n  billing.events:\n    description: d\n  'orders.created':\n    description: e\n";
        let out = PolyglotIndexer::extract(Path::new("asyncapi.yaml"), asyncapi, 0);
        let lines: Vec<(String, usize)> = out
            .nodes
            .iter()
            .map(|n| (n.name.to_string(), n.line_start))
            .collect();
        assert_eq!(
            lines,
            vec![
                ("billing.events".to_string(), 5),
                ("orders.created".to_string(), 7)
            ]
        );

        let openapi = "openapi: 3.0.0\npaths:\n  /orders:\n    get:\n      summary: list\n    post:\n      summary: create\n  /users:\n    get:\n      summary: u\n";
        let out = PolyglotIndexer::extract(Path::new("openapi.yaml"), openapi, 0);
        let mut lines: Vec<(String, usize)> = out
            .nodes
            .iter()
            .map(|n| (n.name.to_string(), n.line_start))
            .collect();
        lines.sort();
        assert_eq!(
            lines,
            vec![
                ("GET /orders".to_string(), 4),
                ("GET /users".to_string(), 9),
                ("POST /orders".to_string(), 6)
            ]
        );
    }
}
