//! The calls layer (9.0): `Callable` nodes and `Calls` edges between an entry
//! (a consumer, an HTTP route) and a producer, resolved without a type checker
//! from per-file call facts.
//!
//! Raw facts ([`CallFacts`], one record per file, language-agnostic) are stored by
//! [`ContractGraph::add_call_facts`] and dropped with their file by `patch_files`.
//! After every reconcile, full or delta, [`ContractGraph::derive_calls`] rebuilds
//! the whole layer from the current facts: it is never journaled, so it cannot
//! drift from a full rebuild, and its cost is proportional to the candidate files.
//!
//! Derivation, in order:
//! 1. File-level pruning: a file takes part only when it lies on an import path
//!    (plus implementation links, see below) from an entry file to a producer
//!    file: forward BFS from the entry files intersected with backward BFS from
//!    the producer files, over bitsets indexed by file.
//! 2. Resolution of the candidate files' call sites (rules of the v9 design note):
//!    `this.m()` through the class and its base classes; `this.dep.m()` through the
//!    declared type of an injected field, an interface or abstract class resolving
//!    to every implementation (one candidate `exact`, several `ambiguous`); an
//!    imported function through its import binding (re-exports followed); a
//!    same-module function. Anything else gives no edge: no global bare-name guess.
//! 3. Callable-level pruning: only the callables on a call path from an entry
//!    callable (the one enclosing a consumer or route node, by line containment)
//!    to a producer callable (enclosing a producer node) are materialized, with the
//!    `Calls` edges between them.
//!
//! Every walk is iterative with a visited set (cyclic injection, recursive calls).
//! Callable ids are `CALLABLE_ID_BASE + k` in sorted (path, line, name) order and
//! the layer is kept apart from the reconciled `nodes` / `edges`, so no existing
//! query, index or delta rule sees it unless it asks for it.

use super::*;
use serde::Deserialize;
use std::collections::VecDeque;

/// First id of the derived `Callable` nodes. Real nodes are numbered from 0 by
/// `add_node` and never get near it.
pub const CALLABLE_ID_BASE: NodeId = 1 << 31;

/// Most call sites one derive resolves, over the whole graph. Past it the rest are
/// skipped and counted in [`CallStats::sites_skipped`], never silently.
pub const MAX_DERIVED_CALL_SITES: usize = 2_000_000;

/// Longest chain of base classes / re-exports followed for one lookup.
const MAX_TYPE_HOPS: usize = 16;

/// Extensions a relative module specifier may omit.
const SCRIPT_EXTENSIONS: &[&str] = &["ts", "tsx", "mts", "cts", "js", "jsx", "mjs", "cjs"];

/// A method or function declared in a file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallableFact {
    /// Bare name (`place`, `handleOrder`).
    pub name: CompactStr,
    /// Enclosing class / struct / module type, `None` for a free function.
    #[serde(default)]
    pub owner: Option<CompactStr>,
    /// 1-based, inclusive.
    pub line_start: u32,
    pub line_end: u32,
}

/// How a call site names its callee.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CallReceiver {
    /// `this.m()` / `self.m()`.
    This,
    /// `this.field.m()`: the field's declared type is in [`CallFacts::injections`].
    InjectedField(CompactStr),
    /// `f()` where `f` is an imported binding (the callee is the local name).
    ImportedSymbol,
    /// `f()` where `f` is a free function of the same file.
    SameModule,
}

/// One call site, inside a callable of the same file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallFact {
    /// Index into [`CallFacts::callables`].
    pub caller: u32,
    pub callee: CompactStr,
    pub receiver: CallReceiver,
    pub line: u32,
}

/// A field of `class` holding a dependency of type `declared_type` (a constructor
/// parameter property, an `inject(T)` field, a Spring constructor parameter, ...).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InjectionFact {
    pub class: CompactStr,
    pub field: CompactStr,
    /// The bare type name as written (`OrderService`), generics and namespaces stripped.
    pub declared_type: CompactStr,
}

/// One import binding: `local` names `exported` of `module`. A re-export
/// (`export { X } from './x'`) is recorded as an import of `X` too, and
/// `export * from './x'` as `local == exported == "*"`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImportFact {
    pub local: CompactStr,
    pub exported: CompactStr,
    /// The module as written once aliases are applied. Only a relative specifier
    /// (`./x`, `../y/z`) is resolved to a file; a package import resolves to nothing.
    pub module: CompactStr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TypeFactKind {
    Class,
    /// A class that cannot be instantiated itself: calls resolve to its subclasses.
    Abstract,
    Interface,
}

/// A type declared in a file, with its heritage as written (bare names).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypeFact {
    pub name: CompactStr,
    pub kind: TypeFactKind,
    #[serde(default)]
    pub extends: Vec<CompactStr>,
    #[serde(default)]
    pub implements: Vec<CompactStr>,
}

/// Every call fact of one file. Stored in the parse cache with the rest of the
/// file's facts: no path-typed field (the module of an import is source text).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallFacts {
    #[serde(default)]
    pub callables: Vec<CallableFact>,
    #[serde(default)]
    pub call_sites: Vec<CallFact>,
    #[serde(default)]
    pub injections: Vec<InjectionFact>,
    #[serde(default)]
    pub imports: Vec<ImportFact>,
    #[serde(default)]
    pub types: Vec<TypeFact>,
    /// A per-file cap was hit and some facts were dropped (counted in `IndexHealth`).
    #[serde(default)]
    pub capped: bool,
}

impl CallFacts {
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.callables.is_empty() && self.imports.is_empty() && self.types.is_empty()
    }

    /// Appends `other`, shifting its callable indices.
    pub fn merge(&mut self, other: CallFacts) {
        let offset = self.callables.len() as u32;
        self.callables.extend(other.callables);
        self.call_sites
            .extend(other.call_sites.into_iter().map(|mut c| {
                c.caller += offset;
                c
            }));
        self.injections.extend(other.injections);
        self.imports.extend(other.imports);
        self.types.extend(other.types);
        self.capped |= other.capped;
    }
}

/// The stored call facts of one file.
#[derive(Debug, Clone)]
pub(super) struct FileCallFacts {
    pub(super) repo_id: RepoId,
    pub(super) package: CompactStr,
    pub(super) facts: CallFacts,
}

/// Counters of the last derive.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct CallStats {
    /// Files holding call facts.
    pub files_with_facts: usize,
    /// Files on an import path between an entry file and a producer file.
    pub candidate_files: usize,
    /// Call sites of the candidate files that were resolved.
    pub sites_resolved: usize,
    /// Call sites skipped past [`MAX_DERIVED_CALL_SITES`].
    pub sites_skipped: usize,
    /// Resolved callable -> callable pairs before callable-level pruning.
    pub resolved_pairs: usize,
    pub callables: usize,
    pub calls: usize,
}

/// The derived layer.
#[derive(Debug, Clone, Default)]
pub(super) struct DerivedCalls {
    /// `callables[k].id == CALLABLE_ID_BASE + k`, sorted by (path, line, name).
    pub(super) callables: Vec<ContractNode>,
    /// Sorted by (from, to).
    pub(super) edges: Vec<ContractEdge>,
    /// Per file, indices into `callables` (sorted by line).
    pub(super) by_file: HashMap<FilePath, Vec<u32>>,
    pub(super) stats: CallStats,
}

impl ContractGraph {
    /// Stores the call facts of the file at `path` (replacing any earlier ones).
    pub fn add_call_facts(
        &mut self,
        path: FilePath,
        repo_id: RepoId,
        package: CompactStr,
        facts: CallFacts,
    ) {
        self.calls_dirty = true;
        if facts.is_empty() {
            self.call_facts.remove(&path);
            return;
        }
        self.call_facts.insert(
            path,
            FileCallFacts {
                repo_id,
                package,
                facts,
            },
        );
    }

    /// The derived `Callable` nodes, sorted by (path, line, name).
    pub fn callable_nodes(&self) -> &[ContractNode] {
        &self.derived_calls.callables
    }

    /// The derived `Calls` edges, sorted by (from, to).
    pub fn call_edges(&self) -> &[ContractEdge] {
        &self.derived_calls.edges
    }

    pub fn call_stats(&self) -> CallStats {
        self.derived_calls.stats
    }

    /// The smallest materialized `Callable` of `node`'s file whose lines contain
    /// `node.line_start`: how a consumer or producer node is anchored on the
    /// method or function that handles or publishes. `None` when the node is on no
    /// resolved call path (or at module level).
    pub fn enclosing_callable(&self, node: &ContractNode) -> Option<&ContractNode> {
        let ids = self.derived_calls.by_file.get(&node.file_path)?;
        ids.iter()
            .filter_map(|&k| self.derived_calls.callables.get(k as usize))
            .filter(|c| c.line_start <= node.line_start && node.line_start <= c.line_end)
            .min_by_key(|c| (c.line_end - c.line_start, c.line_start, c.id))
    }

    /// Marks the layer stale when node `id` lives in a file with call facts.
    pub(super) fn touch_calls_node(&mut self, id: NodeId) {
        if !self.calls_dirty {
            if let Some(node) = self.nodes.get(&id) {
                self.calls_dirty = self.call_facts.contains_key(&node.file_path);
            }
        }
    }

    /// Rebuilds the calls layer from the stored facts (see the module doc), or keeps
    /// it when nothing it reads changed since the last derive: everything it reads is
    /// a file's call facts and the nodes of files with call facts, and a file's nodes
    /// only change through `patch_files` + re-fold, which replaces its call facts.
    pub fn derive_calls(&mut self) {
        if !self.calls_dirty {
            return;
        }
        self.calls_dirty = false;
        let started = std::time::Instant::now();
        let deriver = Deriver::new(self);
        let indexed = started.elapsed();
        self.derived_calls = deriver.run();
        let s = self.derived_calls.stats;
        tracing::debug!(
            target: "mesh::calls",
            "calls derive: {:.1} ms (index {:.1} ms), {} files with facts, {} candidates, {} sites resolved \
             ({} skipped), {} pairs, {} callables, {} calls",
            started.elapsed().as_secs_f64() * 1000.0,
            indexed.as_secs_f64() * 1000.0,
            s.files_with_facts,
            s.candidate_files,
            s.sites_resolved,
            s.sites_skipped,
            s.resolved_pairs,
            s.callables,
            s.calls
        );
        if s.sites_skipped > 0 {
            tracing::warn!(
                target: "mesh::calls",
                "calls layer: {} call sites skipped past the {MAX_DERIVED_CALL_SITES} cap",
                s.sites_skipped
            );
        }
    }
}

/// `dir` joined with a relative module specifier, `.` and `..` resolved lexically
/// (no file system access), `/`-separated.
fn join_module(dir: &str, module: &str) -> String {
    let mut parts: Vec<&str> = dir.split('/').collect();
    for seg in module.split('/') {
        match seg {
            "" | "." => {}
            ".." => {
                // Never pop the leading empty segment of an absolute path.
                if parts.len() > 1 {
                    parts.pop();
                }
            }
            other => parts.push(other),
        }
    }
    parts.join("/")
}

/// `a/b.service.ts` -> `a/b.service`; `None` for another extension.
fn strip_script_ext_str(s: &str) -> Option<&str> {
    let (base, ext) = s.rsplit_once('.')?;
    (SCRIPT_EXTENSIONS.contains(&ext) && !base.is_empty() && !base.ends_with('/'))
        .then_some(base)
}

/// FxHash (rustc's): the derive runs after every reload over every file's facts, and
/// SipHash was most of its index time. Keys are symbol names and small integers; no
/// map of this module is iterated into an output without sorting first.
#[derive(Default, Clone, Copy)]
struct FxHasher(u64);

impl FxHasher {
    #[inline]
    fn add(&mut self, word: u64) {
        self.0 = (self.0.rotate_left(5) ^ word).wrapping_mul(0x51_7c_c1_b7_27_22_0a_95);
    }
}

impl std::hash::Hasher for FxHasher {
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        let (chunks, rest) = bytes.as_chunks::<8>();
        for c in chunks {
            self.add(u64::from_le_bytes(*c));
        }
        if !rest.is_empty() {
            let mut w = [0u8; 8];
            w[..rest.len()].copy_from_slice(rest);
            self.add(u64::from_le_bytes(w));
        }
    }
    #[inline]
    fn write_u8(&mut self, n: u8) {
        self.add(u64::from(n));
    }
    #[inline]
    fn write_u32(&mut self, n: u32) {
        self.add(u64::from(n));
    }
    #[inline]
    fn write_u64(&mut self, n: u64) {
        self.add(n);
    }
    #[inline]
    fn write_usize(&mut self, n: usize) {
        self.add(n as u64);
    }
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
}

type FastMap<K, V> = HashMap<K, V, std::hash::BuildHasherDefault<FxHasher>>;
type FastSet<K> = HashSet<K, std::hash::BuildHasherDefault<FxHasher>>;

/// Strongest first: an edge keeps the best confidence any of its sites had.
fn confidence_rank(c: EdgeConfidence) -> u8 {
    match c {
        EdgeConfidence::Exact => 0,
        EdgeConfidence::Heuristic => 1,
        EdgeConfidence::Ambiguous => 2,
    }
}

/// A type (or free function) by (file index, name).
type TypeKey<'g> = (usize, &'g str);

/// What an import binding names.
#[derive(Debug, Clone, Copy)]
enum Binding<'g> {
    /// `exported` of a workspace file (a relative specifier).
    File(usize, &'g str),
    /// `exported` of a workspace package (`@scope/name` -> `name`), matched against
    /// the package each file was indexed under: `heuristic`, `ambiguous` on a tie.
    Package(&'g str, &'g str),
}

/// `@scope/name/sub` -> `name`, `name/sub` -> `name`; `None` for a relative or
/// absolute path and a `node:` builtin.
fn package_key(module: &str) -> Option<&str> {
    if module.starts_with('.') || module.starts_with('/') || module.starts_with("node:") {
        return None;
    }
    let mut parts = module.split('/');
    let first = parts.next()?;
    let key = if first.starts_with('@') { parts.next()? } else { first };
    (!key.is_empty()).then_some(key)
}

/// The strongest confidence per target; several targets are each `ambiguous`.
fn finish(found: BTreeMap<usize, EdgeConfidence>) -> Vec<(usize, EdgeConfidence)> {
    let many = found.len() > 1;
    found
        .into_iter()
        .map(|(t, c)| (t, if many { EdgeConfidence::Ambiguous } else { c }))
        .collect()
}

fn keep_strongest(found: &mut BTreeMap<usize, EdgeConfidence>, t: usize, c: EdgeConfidence) {
    let e = found.entry(t).or_insert(c);
    if confidence_rank(c) < confidence_rank(*e) {
        *e = c;
    }
}

struct Deriver<'g> {
    graph: &'g ContractGraph,
    files: Vec<(&'g FilePath, &'g FileCallFacts)>,
    /// Global index of each file's first callable.
    offsets: Vec<usize>,
    /// Per file: import local name -> binding; `export * from` targets apart.
    imports: Vec<FastMap<&'g str, Binding<'g>>>,
    star_exports: Vec<Vec<usize>>,
    types: FastMap<TypeKey<'g>, &'g TypeFact>,
    /// (package, name, is_type) -> non-test files declaring it, sorted.
    package_decls: FastMap<(&'g str, &'g str, bool), Vec<usize>>,
    /// (file, class) -> resolved base classes.
    bases: FastMap<TypeKey<'g>, Vec<TypeKey<'g>>>,
    /// Resolved type -> types extending or implementing it (sorted).
    subtypes: FastMap<TypeKey<'g>, Vec<TypeKey<'g>>>,
    /// (file, owner, name) -> global callable index; free functions have owner "".
    methods: FastMap<(usize, &'g str, &'g str), usize>,
    /// (file, class, field) -> declared type.
    injections: FastMap<(usize, &'g str, &'g str), &'g str>,
}

impl<'g> Deriver<'g> {
    fn new(graph: &'g ContractGraph) -> Self {
        let files: Vec<_> = graph.call_facts.iter().collect();
        let mut offsets = Vec::with_capacity(files.len());
        let mut total = 0;
        for (_, f) in &files {
            offsets.push(total);
            total += f.facts.callables.len();
        }
        let mut d = Self {
            graph,
            files,
            offsets,
            imports: Vec::new(),
            star_exports: Vec::new(),
            types: FastMap::default(),
            package_decls: FastMap::default(),
            bases: FastMap::default(),
            subtypes: FastMap::default(),
            methods: FastMap::default(),
            injections: FastMap::default(),
        };
        d.index();
        d
    }

    fn index(&mut self) {
        // Module path (`/`-separated, no extension) -> file; a file wins over a
        // directory index. Strings, not `Path`s: `Path` hashes component by component.
        let slashed: Vec<Option<String>> = self
            .files
            .iter()
            .map(|(p, _)| p.to_str().map(|s| s.replace('\\', "/")))
            .collect();
        let mut by_module: FastMap<&str, usize> = FastMap::default();
        for (i, s) in slashed.iter().enumerate() {
            if let Some(stem) = s.as_deref().and_then(strip_script_ext_str) {
                by_module.entry(stem).or_insert(i);
            }
        }
        for (i, s) in slashed.iter().enumerate() {
            if let Some(dir) = s.as_deref().and_then(|s| {
                let (dir, file) = s.rsplit_once('/')?;
                let stem = strip_script_ext_str(file)?;
                (stem == "index").then_some(dir)
            }) {
                by_module.entry(dir).or_insert(i);
            }
        }
        let resolve = |importer: usize, module: &str| -> Option<usize> {
            if !module.starts_with('.') {
                return None;
            }
            let (dir, _) = slashed[importer].as_deref()?.rsplit_once('/')?;
            let joined = join_module(dir, module);
            by_module.get(joined.as_str()).copied().or_else(|| {
                strip_script_ext_str(&joined).and_then(|s| by_module.get(s).copied())
            })
        };
        for (i, (path, f)) in self.files.iter().enumerate() {
            let mut map = FastMap::default();
            let mut stars = Vec::new();
            for imp in &f.facts.imports {
                let binding = match resolve(i, &imp.module) {
                    Some(target) => Binding::File(target, imp.exported.as_str()),
                    None => match package_key(&imp.module) {
                        Some(pkg) => Binding::Package(pkg, imp.exported.as_str()),
                        None => continue,
                    },
                };
                match (imp.local == "*", binding) {
                    (true, Binding::File(target, _)) => stars.push(target),
                    (true, Binding::Package(..)) => {}
                    (false, b) => {
                        map.entry(imp.local.as_str()).or_insert(b);
                    }
                }
            }
            stars.sort_unstable();
            stars.dedup();
            self.imports.push(map);
            self.star_exports.push(stars);
            let in_test = crate::test_paths::is_test_path(path);
            for t in &f.facts.types {
                self.types.entry((i, t.name.as_str())).or_insert(t);
                if !in_test {
                    self.package_decls
                        .entry((f.package.as_str(), t.name.as_str(), true))
                        .or_default()
                        .push(i);
                }
            }
            for (k, c) in f.facts.callables.iter().enumerate() {
                let owner = c.owner.as_deref().unwrap_or("");
                self.methods
                    .entry((i, owner, c.name.as_str()))
                    .or_insert(self.offsets[i] + k);
                if owner.is_empty() && !in_test {
                    self.package_decls
                        .entry((f.package.as_str(), c.name.as_str(), false))
                        .or_default()
                        .push(i);
                }
            }
            for inj in &f.facts.injections {
                self.injections
                    .entry((i, inj.class.as_str(), inj.field.as_str()))
                    .or_insert(inj.declared_type.as_str());
            }
        }
        for v in self.package_decls.values_mut() {
            v.sort_unstable();
            v.dedup();
        }
        let mut bases: FastMap<TypeKey<'g>, Vec<TypeKey<'g>>> = FastMap::default();
        let mut subtypes: FastMap<TypeKey<'g>, Vec<TypeKey<'g>>> = FastMap::default();
        for (i, (_, f)) in self.files.iter().enumerate() {
            for t in &f.facts.types {
                let me = (i, t.name.as_str());
                for base in &t.extends {
                    for (b, _) in self.resolve_type(i, base) {
                        bases.entry(me).or_default().push(b);
                        subtypes.entry(b).or_default().push(me);
                    }
                }
                for iface in &t.implements {
                    for (b, _) in self.resolve_type(i, iface) {
                        subtypes.entry(b).or_default().push(me);
                    }
                }
            }
        }
        for v in subtypes.values_mut().chain(bases.values_mut()) {
            v.sort_unstable();
            v.dedup();
        }
        self.bases = bases;
        self.subtypes = subtypes;
    }

    fn declared(&self, f: usize, n: &str, is_type: bool) -> bool {
        if is_type {
            self.types.contains_key(&(f, n))
        } else {
            self.methods.contains_key(&(f, "", n))
        }
    }

    /// The files of `pkg` declaring `name`: one is `heuristic`, several `ambiguous`.
    fn package_candidates(
        &self,
        pkg: &str,
        name: &'g str,
        is_type: bool,
    ) -> Vec<(TypeKey<'g>, EdgeConfidence)> {
        let files = self
            .package_decls
            .get(&(pkg, name, is_type))
            .map_or(&[][..], Vec::as_slice);
        let confidence = if files.len() > 1 {
            EdgeConfidence::Ambiguous
        } else {
            EdgeConfidence::Heuristic
        };
        files.iter().map(|&f| ((f, name), confidence)).collect()
    }

    /// Where `name`, as written in file `file`, is declared: through an import
    /// binding (re-exports of relative modules followed, `exact`; a workspace
    /// package matched by name, see [`Binding::Package`]), else the file itself.
    fn resolve_export(
        &self,
        file: usize,
        name: &'g str,
        is_type: bool,
    ) -> Vec<(TypeKey<'g>, EdgeConfidence)> {
        let start = match self.imports.get(file).and_then(|m| m.get(name)) {
            Some(&Binding::File(f, exported)) => (f, exported),
            Some(&Binding::Package(pkg, exported)) => {
                return self.package_candidates(pkg, exported, is_type)
            }
            None if self.declared(file, name, is_type) => {
                return vec![((file, name), EdgeConfidence::Exact)]
            }
            None => return Vec::new(),
        };
        // Follow re-exports: a barrel imports (or re-exports) the name from another file.
        let mut queue = VecDeque::from([start]);
        let mut seen: FastSet<TypeKey<'g>> = FastSet::default();
        while let Some((f, n)) = queue.pop_front() {
            if seen.len() > MAX_TYPE_HOPS || !seen.insert((f, n)) {
                continue;
            }
            if self.declared(f, n, is_type) {
                return vec![((f, n), EdgeConfidence::Exact)];
            }
            match self.imports.get(f).and_then(|m| m.get(n)) {
                Some(&Binding::File(g, exported)) => queue.push_back((g, exported)),
                Some(&Binding::Package(pkg, exported)) => {
                    return self.package_candidates(pkg, exported, is_type)
                }
                None => {}
            }
            for &g in self.star_exports.get(f).map_or(&[][..], Vec::as_slice) {
                queue.push_back((g, n));
            }
        }
        Vec::new()
    }

    fn resolve_type(&self, file: usize, name: &'g str) -> Vec<(TypeKey<'g>, EdgeConfidence)> {
        self.resolve_export(file, name, true)
    }

    /// Method `m` of class `class` or of its nearest base class declaring it.
    fn find_method(&self, class: TypeKey<'g>, m: &str) -> Option<usize> {
        let mut stack = vec![class];
        let mut seen = FastSet::default();
        while let Some(c) = stack.pop() {
            if seen.len() > MAX_TYPE_HOPS || !seen.insert(c) {
                continue;
            }
            if let Some(&idx) = self.methods.get(&(c.0, c.1, m)) {
                return Some(idx);
            }
            if let Some(bs) = self.bases.get(&c) {
                // Reverse so the first base is looked at first.
                stack.extend(bs.iter().rev().copied());
            }
        }
        None
    }

    /// The type a field of `class` is injected as, from the class or a base class.
    fn field_type(&self, class: TypeKey<'g>, field: &str) -> Option<(usize, &'g str)> {
        let mut stack = vec![class];
        let mut seen = FastSet::default();
        while let Some(c) = stack.pop() {
            if seen.len() > MAX_TYPE_HOPS || !seen.insert(c) {
                continue;
            }
            if let Some(&t) = self.injections.get(&(c.0, c.1, field)) {
                return Some((c.0, t));
            }
            if let Some(bs) = self.bases.get(&c) {
                stack.extend(bs.iter().rev().copied());
            }
        }
        None
    }

    /// Callables `m` a call on a value of type `t` can reach: the class's own
    /// method, or for an interface / abstract class every implementation's.
    fn dispatch(&self, t: TypeKey<'g>, m: &str) -> Vec<usize> {
        let kind = self.types.get(&t).map(|f| f.kind);
        if kind == Some(TypeFactKind::Class) {
            return self.find_method(t, m).into_iter().collect();
        }
        let mut out = BTreeSet::new();
        let mut queue = VecDeque::from([t]);
        let mut seen = FastSet::default();
        while let Some(c) = queue.pop_front() {
            if !seen.insert(c) {
                continue;
            }
            let concrete = self.types.get(&c).map(|f| f.kind) == Some(TypeFactKind::Class);
            if concrete {
                if let Some(idx) = self.find_method(c, m) {
                    out.insert(idx);
                }
            }
            if let Some(subs) = self.subtypes.get(&c) {
                queue.extend(subs.iter().copied());
            }
        }
        if out.is_empty() && kind == Some(TypeFactKind::Abstract) {
            out.extend(self.find_method(t, m));
        }
        out.into_iter().collect()
    }

    /// The targets of one call site, with their confidence and the rule used.
    fn resolve_site(
        &self,
        file: usize,
        owner: Option<&'g str>,
        site: &'g CallFact,
    ) -> (Vec<(usize, EdgeConfidence)>, &'static str) {
        let callee = site.callee.as_str();
        let mut found = BTreeMap::new();
        let via = match &site.receiver {
            CallReceiver::This => {
                if let Some(t) = owner.and_then(|o| self.find_method((file, o), callee)) {
                    found.insert(t, EdgeConfidence::Exact);
                }
                "this"
            }
            CallReceiver::InjectedField(field) => {
                if let Some((decl_file, t)) = owner.and_then(|o| self.field_type((file, o), field)) {
                    for (ty, c) in self.resolve_type(decl_file, t) {
                        let impls = self.dispatch(ty, callee);
                        let c = if impls.len() > 1 { EdgeConfidence::Ambiguous } else { c };
                        for target in impls {
                            keep_strongest(&mut found, target, c);
                        }
                    }
                }
                "injected_field"
            }
            CallReceiver::ImportedSymbol => {
                if self.imports[file].contains_key(callee) {
                    for ((f2, name), c) in self.resolve_export(file, callee, false) {
                        if let Some(&t) = self.methods.get(&(f2, "", name)) {
                            keep_strongest(&mut found, t, c);
                        }
                    }
                }
                "imported_function"
            }
            CallReceiver::SameModule => {
                if let Some(&t) = self.methods.get(&(file, "", callee)) {
                    found.insert(t, EdgeConfidence::Exact);
                }
                "same_module"
            }
        };
        (finish(found), via)
    }

    /// Global callable index -> (file, callable).
    fn callable(&self, global: usize) -> (usize, &'g CallableFact) {
        let file = self.offsets.partition_point(|&o| o <= global) - 1;
        let (_, f) = self.files[file];
        (file, &f.facts.callables[global - self.offsets[file]])
    }

    /// Smallest callable of `file` containing `line`.
    fn enclosing(&self, file: usize, line: usize) -> Option<usize> {
        let (_, f) = self.files[file];
        f.facts
            .callables
            .iter()
            .enumerate()
            .filter(|(_, c)| (c.line_start as usize) <= line && line <= c.line_end as usize)
            .min_by_key(|(k, c)| (c.line_end - c.line_start, *k))
            .map(|(k, _)| self.offsets[file] + k)
    }

    fn run(self) -> DerivedCalls {
        let n = self.files.len();
        let mut stats = CallStats {
            files_with_facts: n,
            ..CallStats::default()
        };
        if n == 0 {
            return DerivedCalls::default();
        }

        // Entry and producer nodes of each file.
        let producer_ids: FastSet<NodeId> =
            self.graph.topic_producers.values().flatten().copied().collect();
        let consumer_ids: FastSet<NodeId> =
            self.graph.topic_consumers.values().flatten().copied().collect();
        let mut entry_lines: Vec<Vec<usize>> = vec![Vec::new(); n];
        let mut producer_lines: Vec<Vec<usize>> = vec![Vec::new(); n];
        for (i, (path, _)) in self.files.iter().enumerate() {
            for id in self.graph.get_nodes_for_file(path) {
                let Some(node) = self.graph.nodes.get(id) else {
                    continue;
                };
                if producer_ids.contains(id) {
                    producer_lines[i].push(node.line_start);
                }
                // Entries: consumers, HTTP routes and inbound gRPC handlers.
                if consumer_ids.contains(id)
                    || matches!(node.kind, NodeKind::HttpEndpoint | NodeKind::GrpcMethod)
                {
                    entry_lines[i].push(node.line_start);
                }
            }
        }

        // 1. File-level pruning over imports and implementation links.
        let mut adj: Vec<Vec<usize>> = vec![Vec::new(); n];
        for (i, map) in self.imports.iter().enumerate() {
            for b in map.values() {
                match *b {
                    Binding::File(f, _) => adj[i].push(f),
                    Binding::Package(pkg, name) => {
                        for is_type in [true, false] {
                            adj[i].extend(
                                self.package_candidates(pkg, name, is_type)
                                    .into_iter()
                                    .map(|((f, _), _)| f),
                            );
                        }
                    }
                }
            }
            adj[i].extend(self.star_exports[i].iter().copied());
        }
        for (base, subs) in &self.subtypes {
            adj[base.0].extend(subs.iter().map(|s| s.0));
        }
        let mut radj: Vec<Vec<usize>> = vec![Vec::new(); n];
        for (i, out) in adj.iter_mut().enumerate() {
            out.sort_unstable();
            out.dedup();
            for &j in out.iter() {
                radj[j].push(i);
            }
        }
        let fwd_files = bfs(
            (0..n).filter(|&i| !entry_lines[i].is_empty()),
            &adj,
            n,
        );
        let bwd_files = bfs(
            (0..n).filter(|&i| !producer_lines[i].is_empty()),
            &radj,
            n,
        );
        let candidate: Vec<bool> = (0..n).map(|i| fwd_files[i] && bwd_files[i]).collect();
        stats.candidate_files = candidate.iter().filter(|&&c| c).count();

        // 2. Resolution of the candidate files' call sites.
        let mut pairs: BTreeMap<(usize, usize), (u32, EdgeConfidence, &'static str)> =
            BTreeMap::new();
        let mut budget = MAX_DERIVED_CALL_SITES;
        for (i, (_, f)) in self.files.iter().enumerate() {
            if !candidate[i] {
                continue;
            }
            for site in &f.facts.call_sites {
                if budget == 0 {
                    stats.sites_skipped += 1;
                    continue;
                }
                budget -= 1;
                stats.sites_resolved += 1;
                let Some(caller_fact) = f.facts.callables.get(site.caller as usize) else {
                    continue;
                };
                let caller = self.offsets[i] + site.caller as usize;
                let (targets, via) = self.resolve_site(i, caller_fact.owner.as_deref(), site);
                for (t, confidence) in targets {
                    let entry = pairs.entry((caller, t)).or_insert((site.line, confidence, via));
                    if (confidence_rank(confidence), site.line)
                        < (confidence_rank(entry.1), entry.0)
                    {
                        *entry = (site.line, confidence, via);
                    }
                }
            }
        }
        stats.resolved_pairs = pairs.len();

        // 3. Callable-level pruning: entry -> ... -> producer.
        let mut out_edges: HashMap<usize, Vec<usize>> = HashMap::new();
        let mut in_edges: HashMap<usize, Vec<usize>> = HashMap::new();
        for &(a, b) in pairs.keys() {
            out_edges.entry(a).or_default().push(b);
            in_edges.entry(b).or_default().push(a);
        }
        let mut entries = BTreeSet::new();
        let mut producers = BTreeSet::new();
        for i in (0..n).filter(|&i| candidate[i]) {
            entries.extend(entry_lines[i].iter().filter_map(|&l| self.enclosing(i, l)));
            producers.extend(producer_lines[i].iter().filter_map(|&l| self.enclosing(i, l)));
        }
        let fwd = reach(entries, &out_edges);
        let bwd = reach(producers, &in_edges);
        let kept: BTreeSet<usize> = fwd.intersection(&bwd).copied().collect();

        // Materialize, ids in (path, line, name) order.
        let mut order: Vec<(usize, &'g CallableFact, usize)> = kept
            .iter()
            .map(|&g| {
                let (file, c) = self.callable(g);
                (file, c, g)
            })
            .collect();
        order.sort_by(|a, b| {
            (a.0, a.1.line_start, &a.1.owner, &a.1.name, a.2)
                .cmp(&(b.0, b.1.line_start, &b.1.owner, &b.1.name, b.2))
        });
        let mut id_of: HashMap<usize, NodeId> = HashMap::new();
        let mut callables = Vec::with_capacity(order.len());
        let mut by_file: HashMap<FilePath, Vec<u32>> = HashMap::new();
        for (k, (file, c, g)) in order.into_iter().enumerate() {
            let (path, f) = self.files[file];
            let id = CALLABLE_ID_BASE + k as NodeId;
            id_of.insert(g, id);
            by_file.entry(path.clone()).or_default().push(k as u32);
            let name = match &c.owner {
                Some(o) => CompactStr::new(format!("{o}.{}", c.name)),
                None => c.name.clone(),
            };
            callables.push(ContractNode {
                id,
                name,
                kind: NodeKind::Callable,
                file_path: path.clone(),
                line_start: c.line_start as usize,
                line_end: c.line_end as usize,
                package: f.package.clone(),
                repo_id: f.repo_id,
                signature: None,
                docstring: None,
            });
        }
        let mut edges = Vec::new();
        for (&(a, b), &(line, confidence, via)) in &pairs {
            if let (Some(&from), Some(&to)) = (id_of.get(&a), id_of.get(&b)) {
                edges.push(ContractEdge {
                    from,
                    to,
                    kind: EdgeKind::Calls,
                    metadata: Some(CompactStr::new(format!("{via} L{line}"))),
                    confidence,
                });
            }
        }
        edges.sort_by_key(|e| (e.from, e.to));
        stats.callables = callables.len();
        stats.calls = edges.len();
        DerivedCalls {
            callables,
            edges,
            by_file,
            stats,
        }
    }
}

/// Files reachable from `starts` over `adj` (iterative).
fn bfs(starts: impl Iterator<Item = usize>, adj: &[Vec<usize>], n: usize) -> Vec<bool> {
    let mut seen = vec![false; n];
    let mut queue: VecDeque<usize> = VecDeque::new();
    for s in starts {
        if !seen[s] {
            seen[s] = true;
            queue.push_back(s);
        }
    }
    while let Some(i) = queue.pop_front() {
        for &j in &adj[i] {
            if !seen[j] {
                seen[j] = true;
                queue.push_back(j);
            }
        }
    }
    seen
}

/// Callables reachable from `starts` over `adj`, the starts included (iterative).
fn reach(starts: BTreeSet<usize>, adj: &HashMap<usize, Vec<usize>>) -> BTreeSet<usize> {
    let mut seen = starts.clone();
    let mut stack: Vec<usize> = starts.into_iter().collect();
    while let Some(i) = stack.pop() {
        for &j in adj.get(&i).map_or(&[][..], Vec::as_slice) {
            if seen.insert(j) {
                stack.push(j);
            }
        }
    }
    seen
}
