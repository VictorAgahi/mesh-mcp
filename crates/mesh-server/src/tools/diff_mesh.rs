//! `diff_mesh`: what the working tree changes in the mesh compared with a Git
//! base, at the contract level (declarations, RPCs, routes, topics, imports),
//! and what the live graph says depends on what was removed or changed.

use crate::git::{self, BaseChoice, GitError, GitRunner};
use crate::indexer::{ContentExtractor, WorkspaceIndexer};
use crate::protocol::RequestMeta;
use crate::tools::analyze_grpc::{compare_wire, render_report, WireClass};
use crate::tools::find_dependents::is_indexable_source;
use crate::tools::smart_search::SearchScope;
use crate::tools::{McpTool, ToolError, ToolOutput};
use mesh_core::{
    qualified_topic_key, AppState, Broker, CompactStr, ContractGraph, ContractNode,
    DependentsMatch, EdgeConfidence, ExcludeMatcher, FilesystemCrawler, NodeKind, RecordedCaller,
    RepoId, RpcRecordResolver, ValidatedScope,
};
use mesh_parsers::{AstGuard, FileIndex};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ffi::OsStr;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DiffMeshArgs {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        description = "Git revision the working tree is compared against (ex: 'main', 'origin/main', 'v1.4.0', a commit SHA), resolved in each root's own repository. Omit to use the merge-base of HEAD with the first existing of origin/HEAD, origin/main, main (falling back to HEAD). Uncommitted edits and untracked files always count. NOT a file path; must not start with '-' or contain ':'."
    )]
    pub base: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        with = "Option<String>",
        description = "Repository or directory to restrict the diff to, resolved within the configured roots. Omit it (or pass \".\" or \"*\") for every configured root. DO NOT pass a path outside the roots: it is rejected."
    )]
    pub scope: Option<CompactStr>,

    #[serde(default)]
    // Accepted for W3C trace propagation, hidden from `tools/list`.
    #[schemars(skip)]
    pub _meta: Option<RequestMeta>,
}

pub struct DiffMeshTool;

impl McpTool for DiffMeshTool {
    const NAME: &'static str = "diff_mesh";
    const DESCRIPTION: &'static str = "Contract-level diff of the working tree (uncommitted edits and untracked files included) against a Git base, per configured root: declarations added, removed or with a changed signature, gRPC services and RPCs, HTTP routes, event producers and consumers per topic, imports and RPC calls; then, from the live graph, the callers and dependents of every removed or changed symbol and RPC (path:line, heuristic or ambiguous rows marked) and the .proto wire-format check of every changed .proto. This is the tool for 'what does my branch change in the mesh / what will this change break'. Body-only edits are counted, never listed. The time budget is about 20 s, checked between files: files, .proto and elements it does not reach are counted and named, never dropped. DO NOT USE to trace one known symbol, RPC or topic (use find_dependents, analyze_grpc or analyze_impact). DO NOT pass a file path or a `--option` as `base`.";
    type Args = DiffMeshArgs;

    fn meta(args: &Self::Args) -> Option<&RequestMeta> {
        args._meta.as_ref()
    }

    fn subject(args: &Self::Args) -> Option<&str> {
        args.scope.as_deref()
    }

    fn truncation_hint(args: &Self::Args, state: &AppState) -> Option<String> {
        let mut roots: Vec<String> = state
            .allowed_roots
            .iter()
            .map(|r| format!("`{}`", r.display()))
            .collect();
        roots.sort();
        roots.truncate(8);
        Some(format!(
            "Narrow `scope` (now `{}`) to one root or one sub-directory, for example one of {}.",
            args.scope.as_deref().unwrap_or("*"),
            roots.join(", ")
        ))
    }

    fn run(args: &Self::Args, state: &AppState) -> Result<ToolOutput, ToolError> {
        run_with(args, state, &GitRunner::system(), Limits::DEFAULT)
    }
}

// ---------------------------------------------------------------------------
// Bounds
// ---------------------------------------------------------------------------

/// Changed files analysed per call; the rest are counted in a truncation note.
const MAX_CHANGED_FILES: usize = 2_000;
/// Output bound of one `git diff --name-only` / `git ls-files` listing.
const NAME_LIST_LIMIT: u64 = 4 * 1024 * 1024;
/// Total bytes of base contents read through `git cat-file --batch`, all repos.
const BASE_CONTENT_BUDGET: u64 = 32 * 1024 * 1024;
/// The rendered answer stays under the registry's 48 KB cap by this margin
/// (room for the notes the registry prepends).
const OUTPUT_BUDGET: usize = 44 * 1024;
/// Impact rows listed per removed or changed element; the rest are counted.
const MAX_IMPACT_ROWS: usize = 10;
/// Removed or changed elements whose impact is looked up.
const MAX_IMPACT_ITEMS: usize = 60;
/// Contract changes listed per file; the rest are counted.
const MAX_CHANGES_PER_FILE: usize = 40;
/// Longest signature quoted.
const MAX_SIGNATURE_CHARS: usize = 160;
/// Names too generic for a dependents lookup to mean anything.
const GENERIC_NAMES: [&str; 6] = ["constructor", "main", "init", "new", "default", "index"];
/// Wall-clock budget of one call (it runs on an agent's synchronous request
/// path). Once it is spent, the files not yet extracted and the `.proto` files
/// not yet checked are reported as not analysed, never silently dropped.
const CALL_TIME_BUDGET: Duration = Duration::from_secs(20);
/// Room kept, while a section is filled, for the "N more" line that ends it.
const NOTE_RESERVE: usize = 512;

/// The bounds of one call; tests shrink them.
#[derive(Debug, Clone, Copy)]
struct Limits {
    max_files: usize,
    time_budget: Duration,
}

impl Limits {
    const DEFAULT: Self = Self {
        max_files: MAX_CHANGED_FILES,
        time_budget: CALL_TIME_BUDGET,
    };
}

// ---------------------------------------------------------------------------
// Changed files
// ---------------------------------------------------------------------------

/// One configured root (or the part of it `scope` selects) to diff.
struct Target {
    repo_id: RepoId,
    /// Where `git` runs, and the `.` pathspec: the root, or the scope inside it.
    dir: PathBuf,
}

/// A root that was diffed: its base and its changed files.
struct RootDiff {
    root: PathBuf,
    base: BaseChoice,
    base_sha: String,
    toplevel: String,
    shallow: bool,
}

/// A changed path, before contents are read.
struct Changed {
    repo_id: RepoId,
    /// Index into the `RootDiff` list.
    root_diff: usize,
    abs: PathBuf,
    /// Path relative to the repository top level, for `<sha>:<path>`.
    rel_top: String,
    display: String,
}

#[derive(Default)]
struct Counters {
    excluded: usize,
    non_source: usize,
    truncated: usize,
    /// The file cap of this call.
    cap: usize,
    /// Files not extracted because the call's time budget was spent.
    timed_out: Vec<String>,
    /// Changed `.proto` files left out by the file cap: never wire-checked.
    protos_capped: usize,
}

/// The configured roots and their display names (`WorkspaceIndexer::repo_names`:
/// two roots named `api` under different parents stay apart).
struct RootView<'a> {
    roots: &'a [PathBuf],
    names: Vec<String>,
}

impl<'a> RootView<'a> {
    fn new(roots: &'a [PathBuf]) -> Self {
        Self {
            roots,
            names: WorkspaceIndexer::repo_names(roots),
        }
    }

    fn name_of(&self, id: RepoId) -> Option<&str> {
        self.names.get(id as usize).map(String::as_str)
    }

    /// `abs` relative to its most specific root, `/`-separated; prefixed with
    /// the root's display name when several roots are configured.
    fn path(&self, abs: &Path) -> String {
        let Some((id, root)) = WorkspaceIndexer::most_specific_root(abs, self.roots) else {
            return abs.display().to_string();
        };
        let rel = abs
            .strip_prefix(root)
            .map(|r| {
                r.components()
                    .map(|c| c.as_os_str().to_string_lossy())
                    .collect::<Vec<_>>()
                    .join("/")
            })
            .unwrap_or_else(|_| abs.display().to_string());
        match (self.roots.len() > 1, self.name_of(id)) {
            (true, Some(name)) => format!("{name}/{rel}"),
            _ => rel,
        }
    }

    /// A root: its display name when several roots are configured, else its path.
    fn root(&self, root: &Path) -> String {
        let id = self.roots.iter().position(|r| r == root);
        match (self.roots.len() > 1, id.and_then(|i| self.names.get(i))) {
            (true, Some(name)) => name.clone(),
            _ => root.display().to_string(),
        }
    }
}

/// Selects the roots `scope` covers: every root when global, else the root that
/// holds the scope (diffed below the scope only) plus the roots nested in it.
fn select_targets(scope: Option<&str>, state: &AppState) -> Result<Vec<Target>, ToolError> {
    let roots = &state.allowed_roots;
    let all = || {
        roots
            .iter()
            .enumerate()
            .map(|(i, r)| Target {
                repo_id: i as RepoId,
                dir: r.clone(),
            })
            .collect::<Vec<_>>()
    };
    match SearchScope::resolve(scope, state)? {
        SearchScope::Global => Ok(all()),
        SearchScope::Scoped(validated) => {
            let dir = validated.into_path_buf();
            let mut targets = Vec::new();
            for (i, root) in roots.iter().enumerate() {
                if dir.starts_with(root) {
                    // Only the most specific root holding the scope diffs it.
                    if WorkspaceIndexer::most_specific_root(&dir, roots)
                        .is_some_and(|(id, _)| id as usize == i)
                    {
                        targets.push(Target {
                            repo_id: i as RepoId,
                            dir: dir.clone(),
                        });
                    }
                } else if root.starts_with(&dir) {
                    targets.push(Target {
                        repo_id: i as RepoId,
                        dir: root.clone(),
                    });
                }
            }
            Ok(targets)
        }
    }
}

/// Why one root could not be diffed.
struct RootFailure {
    root: PathBuf,
    message: String,
    invalid_request: bool,
}

fn git_args<'a>(args: &'a [&'a str]) -> Vec<&'a OsStr> {
    args.iter().map(OsStr::new).collect()
}

fn is_shallow(git: &GitRunner, program: &Path, dir: &Path) -> bool {
    git.run(
        program,
        dir,
        &git_args(&["rev-parse", "--is-shallow-repository"]),
        git::SMALL_OUTPUT,
    )
    .map(|o| o.success && o.stdout_line() == "true")
    .unwrap_or(false)
}

/// What `list_changes` found for one target.
struct Listing {
    choice: BaseChoice,
    sha: String,
    toplevel: String,
    /// The target directory relative to the top level (`--show-prefix`).
    prefix: String,
    shallow: bool,
    /// Changed and untracked paths, relative to the top level.
    paths: Vec<String>,
}

const SHALLOW_HINT: &str = " This is a shallow clone: the base may not have been fetched (`git fetch --unshallow`, or `git fetch origin <branch>`), or pass a `base` inside the fetched history.";

/// Lists the changed paths of one target: `git diff --name-only` of the base
/// against the working tree, plus untracked files, NUL-separated.
fn list_changes(
    git: &GitRunner,
    program: &Path,
    target: &Target,
    base: Option<&str>,
) -> Result<Listing, RootFailure> {
    let dir = &target.dir;
    let fail = |message: String, invalid_request: bool| RootFailure {
        root: dir.clone(),
        message,
        invalid_request,
    };
    let git_fail = |e: GitError| {
        let invalid = e.is_invalid_request();
        fail(e.to_string(), invalid)
    };
    let top = git
        .run(
            program,
            dir,
            &git_args(&["rev-parse", "--show-toplevel", "--show-prefix"]),
            git::SMALL_OUTPUT,
        )
        .map_err(git_fail)?;
    if !top.success {
        let reason = top.stderr.lines().next().unwrap_or_default().to_string();
        return Err(fail(
            GitError::NotARepository(
                dir.clone(),
                if reason.is_empty() {
                    String::new()
                } else {
                    format!(" (git: {reason})")
                },
            )
            .to_string(),
            false,
        ));
    }
    let text = String::from_utf8_lossy(&top.stdout).into_owned();
    let mut lines = text.lines();
    let toplevel = lines.next().unwrap_or_default().trim().to_string();
    let prefix = lines.next().unwrap_or_default().trim().to_string();
    let shallow = is_shallow(git, program, dir);
    let (choice, sha) = match git::resolve_base(git, program, dir, base) {
        Ok(found) => found,
        Err(GitError::BaseNotFound(b)) => {
            return Err(fail(
                format!(
                    "base `{b}` does not name a commit in this repository; pass an existing branch, tag or SHA as `base`.{}",
                    if shallow { SHALLOW_HINT } else { "" }
                ),
                base.is_some(),
            ))
        }
        Err(e) => return Err(git_fail(e)),
    };
    let diff = git
        .run(
            program,
            dir,
            &git_args(&[
                "-c",
                "diff.relative=false",
                "diff",
                "--name-only",
                "-z",
                "--no-renames",
                "--no-ext-diff",
                "--ignore-submodules=all",
                &sha,
                "--",
                ".",
            ]),
            NAME_LIST_LIMIT,
        )
        .map_err(git_fail)?;
    if !diff.success {
        return Err(fail(format!("`git diff` failed: {}", diff.stderr), false));
    }
    let untracked = git
        .run(
            program,
            dir,
            &git_args(&[
                "ls-files",
                "--others",
                "--exclude-standard",
                "--full-name",
                "-z",
                "--",
                ".",
            ]),
            NAME_LIST_LIMIT,
        )
        .map_err(git_fail)?;
    if !untracked.success {
        return Err(fail(
            format!("`git ls-files` failed: {}", untracked.stderr),
            false,
        ));
    }
    let mut paths: BTreeSet<String> = BTreeSet::new();
    for out in [&diff.stdout, &untracked.stdout] {
        for raw in out.split(|b| *b == 0).filter(|p| !p.is_empty()) {
            paths.insert(String::from_utf8_lossy(raw).into_owned());
        }
    }
    Ok(Listing {
        choice,
        sha,
        toplevel,
        prefix,
        shallow,
        paths: paths.into_iter().collect(),
    })
}

// ---------------------------------------------------------------------------
// Contents
// ---------------------------------------------------------------------------

/// One side of a changed file.
enum Side {
    Absent,
    Skipped(String),
    Content(Vec<u8>),
}

/// Reads the base version of every file of one repository at `sha` with one
/// `git cat-file --batch-check` (types and sizes) and one `git cat-file --batch`
/// (the blobs that fit the guard budget and what is left of `budget`).
fn read_base_contents(
    git: &GitRunner,
    program: &Path,
    dir: &Path,
    sha: &str,
    files: &[(usize, &Changed)],
    budget: &mut u64,
) -> Result<HashMap<usize, Side>, GitError> {
    let mut sides: HashMap<usize, Side> = HashMap::new();
    let mut asked: Vec<(usize, &Changed)> = Vec::new();
    let mut input = Vec::new();
    for &(i, c) in files {
        if c.rel_top.contains('\n') || c.rel_top.contains('\r') {
            sides.insert(i, Side::Skipped("its path contains a line break".into()));
            continue;
        }
        input.extend_from_slice(format!("{sha}:{}\n", c.rel_top).as_bytes());
        asked.push((i, c));
    }
    if asked.is_empty() {
        return Ok(sides);
    }
    let check = git.run_with_input(
        program,
        dir,
        &git_args(&["cat-file", "--batch-check"]),
        Some(input),
        NAME_LIST_LIMIT,
    )?;
    if !check.success {
        return Err(GitError::GitFailed(
            "cat-file --batch-check".into(),
            check.stderr,
        ));
    }
    let listing = String::from_utf8_lossy(&check.stdout).into_owned();
    let mut wanted: Vec<(usize, String, u64)> = Vec::new();
    for (line, &(i, c)) in listing.lines().zip(asked.iter()) {
        if line.ends_with(" missing") || line.ends_with(" ambiguous") {
            sides.insert(i, Side::Absent);
            continue;
        }
        let mut parts = line.rsplitn(3, ' ');
        let (size, kind, oid) = (parts.next(), parts.next(), parts.next());
        let (Some(size), Some(kind), Some(oid)) = (size, kind, oid) else {
            sides.insert(i, Side::Skipped("unexpected `git cat-file` answer".into()));
            continue;
        };
        if kind != "blob" {
            sides.insert(
                i,
                Side::Skipped(format!("a {kind} at the base, not a file")),
            );
            continue;
        }
        let size: u64 = size.parse().unwrap_or(u64::MAX);
        let limit = AstGuard::size_budget(&c.abs);
        if size > limit {
            sides.insert(
                i,
                Side::Skipped(format!(
                    "oversized at the base ({size} bytes, budget {limit})"
                )),
            );
            continue;
        }
        if size > *budget {
            sides.insert(
                i,
                Side::Skipped("base content budget of this call exhausted".into()),
            );
            continue;
        }
        *budget -= size;
        wanted.push((i, oid.to_string(), size));
    }
    if wanted.is_empty() {
        return Ok(sides);
    }
    let mut input = Vec::new();
    let mut expected: u64 = 0;
    for (_, oid, size) in &wanted {
        input.extend_from_slice(oid.as_bytes());
        input.push(b'\n');
        // Header (`<oid> blob <size>\n`), content, trailing newline.
        expected += size + oid.len() as u64 + 32;
    }
    let blobs = git.run_with_input(
        program,
        dir,
        &git_args(&["cat-file", "--batch"]),
        Some(input),
        expected,
    )?;
    if !blobs.success {
        return Err(GitError::GitFailed("cat-file --batch".into(), blobs.stderr));
    }
    let out = &blobs.stdout;
    let mut pos = 0usize;
    for (i, _, size) in wanted {
        let Some(nl) = out[pos.min(out.len())..].iter().position(|b| *b == b'\n') else {
            sides.insert(i, Side::Skipped("truncated `git cat-file` answer".into()));
            continue;
        };
        let start = pos + nl + 1;
        let end = start + size as usize;
        if end > out.len() {
            sides.insert(i, Side::Skipped("truncated `git cat-file` answer".into()));
            pos = out.len();
            continue;
        }
        sides.insert(i, Side::Content(out[start..end].to_vec()));
        pos = end + 1;
    }
    Ok(sides)
}

/// The working-tree version: never through a symlink, always inside the jail,
/// refused before the read when over the guard's size budget.
fn read_current(abs: &Path, state: &AppState) -> Side {
    let meta = match std::fs::symlink_metadata(abs) {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Side::Absent,
        Err(e) => return Side::Skipped(format!("unreadable ({e})")),
    };
    if meta.file_type().is_symlink() {
        return Side::Skipped("a symlink (never followed)".into());
    }
    if !meta.is_file() {
        return Side::Absent;
    }
    if let Err(e) = ValidatedScope::resolve_with_aliases(
        &abs.to_string_lossy(),
        &state.allowed_roots,
        &state.config.workspace.mount_aliases,
        None,
    ) {
        return Side::Skipped(format!("refused by the path jail ({e})"));
    }
    let limit = AstGuard::size_budget(abs);
    if meta.len() > limit {
        return Side::Skipped(format!("oversized ({} bytes, budget {limit})", meta.len()));
    }
    match std::fs::read(abs) {
        Ok(bytes) => Side::Content(bytes),
        Err(e) => Side::Skipped(format!("unreadable ({e})")),
    }
}

// ---------------------------------------------------------------------------
// Contract facts
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum Fact {
    /// A gRPC service or method (`.proto` definition or a server registration).
    Rpc(&'static str, String),
    Route(String),
    /// A declaration: kind label and name.
    Decl(&'static str, String),
    Produces(String),
    Consumes(String),
    Import(String),
    /// A method-level RPC call, resolved as the graph resolves it (binding,
    /// inherited field, field name): the declared `Service.Method` when the
    /// graph declares it, else `Service.method` as the call names it.
    Calls(String),
    /// A call through an inherited field no class binds to one service
    /// (`Base#field.method` with a client-like field name): the raw record and
    /// the service the field's name gives, which is a guess.
    CallsInferred(String, String),
    /// A call through an inherited field some base class binds as a client,
    /// to no single service: the raw record.
    CallsUnresolved(String),
}

impl Fact {
    /// A topic fact: its signatures hold the brokers of its sites (see `facts_of`).
    fn is_topic(&self) -> bool {
        matches!(self, Self::Produces(_) | Self::Consumes(_))
    }

    /// Whether a signature change on both sides is a contract change.
    fn has_signature(&self) -> bool {
        matches!(self, Self::Rpc(..) | Self::Route(_) | Self::Decl(..))
    }

    fn describe(&self) -> String {
        match self {
            Self::Rpc(kind, n) => format!("{kind} `{n}`"),
            Self::Route(n) => format!("route `{n}`"),
            Self::Decl(kind, n) => format!("{kind} `{n}`"),
            Self::Produces(t) => format!("produces topic `{t}`"),
            Self::Consumes(t) => format!("consumes topic `{t}`"),
            Self::Import(t) => format!("import `{t}`"),
            Self::Calls(t) => format!("calls RPC `{t}`"),
            Self::CallsInferred(raw, t) => format!(
                "calls RPC `{t}` (through `{raw}`: service inferred from the field name, no single client binding found)"
            ),
            Self::CallsUnresolved(t) => {
                format!("calls `{t}` (unresolved: the base class binds this field to more than one service)")
            }
        }
    }
}

#[derive(Debug, Clone, Default)]
struct FactVal {
    signatures: BTreeSet<String>,
    line: usize,
    /// The declaration a relation is attached to (a producer's method).
    owner: Option<String>,
    /// The declared package of an RPC (`demo.v1`), which a client may prefix.
    package: Option<String>,
}

fn decl_label(kind: NodeKind) -> Option<&'static str> {
    Some(match kind {
        NodeKind::ServiceClass => "symbol",
        NodeKind::Interface => "interface",
        NodeKind::ProtoMessage => "message",
        NodeKind::Saga => "saga",
        NodeKind::PostProcessor => "post-processor",
        _ => return None,
    })
}

/// Records `fact` at `line` (the earliest wins), with the signature of a
/// declaration or the owner of a relation.
fn add_fact<'f>(
    facts: &'f mut BTreeMap<Fact, FactVal>,
    fact: Fact,
    line: Option<usize>,
    signature: Option<&str>,
    owner: Option<&str>,
) -> &'f mut FactVal {
    let entry = facts.entry(fact).or_insert_with(|| FactVal {
        line: usize::MAX,
        ..FactVal::default()
    });
    if let Some(l) = line {
        entry.line = entry.line.min(l);
    }
    if let Some(sig) = signature {
        entry.signatures.insert(sig.trim().to_string());
    }
    if entry.owner.is_none() {
        entry.owner = owner.map(str::to_string);
    }
    entry
}

/// The contract facts of one extracted file of root `repo`. Its RPC call
/// records are resolved with `resolver` (the live graph's client bindings),
/// so a call through a field inherited from a base class in another file
/// counts like a direct one. An inherited-field record (`^Base#field.method`,
/// emitted for any `this.field.method()` of a subclass) is a call only when
/// the field is a client: bound by the base class, or named like one
/// (`userClient`, `userService`); `this.logger.log()` is not.
fn facts_of(
    index: &FileIndex,
    resolver: &RpcRecordResolver<'_>,
    repo: RepoId,
) -> BTreeMap<Fact, FactVal> {
    let mut facts: BTreeMap<Fact, FactVal> = BTreeMap::new();
    for node in &index.nodes {
        let name = node.name.to_string();
        let is_rpc = matches!(node.kind, NodeKind::GrpcService | NodeKind::GrpcMethod);
        let fact = match node.kind {
            NodeKind::GrpcService => Fact::Rpc("gRPC service", name),
            NodeKind::GrpcMethod => Fact::Rpc("RPC", name),
            NodeKind::HttpEndpoint => Fact::Route(name),
            kind => match decl_label(kind) {
                Some(label) => Fact::Decl(label, name),
                None => continue,
            },
        };
        let entry = add_fact(
            &mut facts,
            fact,
            Some(node.line_start),
            node.signature.as_deref(),
            None,
        );
        if is_rpc && !node.package.is_empty() && entry.package.is_none() {
            entry.package = Some(node.package.to_string());
        }
    }
    let node = |i: usize| index.nodes.get(i);
    // The broker a topic site is statically known to talk to (9.0 T7), by position in
    // `producers` / `consumers`.
    let producer_broker: HashMap<usize, Broker> = index.producer_brokers.iter().copied().collect();
    let consumer_broker: HashMap<usize, Broker> = index.consumer_brokers.iter().copied().collect();
    let relations = index
        .producers
        .iter()
        .enumerate()
        .map(|(p, (i, t))| {
            (
                *i,
                Fact::Produces(t.to_string()),
                producer_broker.get(&p).copied(),
            )
        })
        .chain(index.consumers.iter().enumerate().map(|(p, (i, t))| {
            (
                *i,
                Fact::Consumes(t.to_string()),
                consumer_broker.get(&p).copied(),
            )
        }))
        .chain(
            index
                .dependencies
                .iter()
                .map(|(i, t)| (*i, Fact::Import(t.to_string()), None)),
        )
        .chain(
            index
                .rpc_calls
                .iter()
                .filter_map(|(i, recorded)| {
                    let inherited = recorded
                        .strip_prefix(mesh_core::RPC_INHERITED_METHOD_PREFIX)
                        .and_then(|raw| {
                            let (base, rest) = raw.split_once('#')?;
                            let (field, _) = rest.split_once('.')?;
                            Some((raw, base, field))
                        });
                    let bound = inherited.is_some_and(|(_, base, field)| {
                        resolver.base_binds_client_field(base, field)
                    });
                    match resolver.resolve(Some(repo), recorded) {
                        Some(r) if r.method_level && r.target.contains('.') => {
                            let name = r.declared.map_or(r.target, |d| d.to_string());
                            match inherited {
                                // Only the field's name tells (the resolution's own
                                // rule, `service_from_client_field_name`).
                                Some((raw, _, _)) if r.confidence != EdgeConfidence::Exact => {
                                    Some((*i, Fact::CallsInferred(raw.to_string(), name)))
                                }
                                _ => Some((*i, Fact::Calls(name))),
                            }
                        }
                        // A service-level client is not a call; a binding, constant
                        // or handler reference resolves to nothing.
                        Some(_) => None,
                        // An inherited field that is not a client (not bound, or bound
                        // to a plain type, and not named like one) is not a call.
                        None => inherited
                            .filter(|_| bound)
                            .map(|(raw, _, _)| (*i, Fact::CallsUnresolved(raw.to_string()))),
                    }
                })
                .map(|(i, fact)| (i, fact, None)),
        );
    let topic_kind = |k: NodeKind| {
        matches!(
            k,
            NodeKind::KafkaTopic | NodeKind::EventStream | NodeKind::Queue
        )
    };
    // Declarations only, once per file: a file of thousands of sends is not
    // rescanned per send.
    let decls: Vec<&ContractNode> = index.nodes.iter().filter(|d| !topic_kind(d.kind)).collect();
    for (i, fact, broker) in relations {
        let n = node(i);
        // An import is the file's, not the declaration's that carries it.
        let owner = match fact {
            Fact::Import(_) => None,
            // A call-site producer / consumer (9.0) is named after its topic: its
            // owner is the smallest declaration around the call.
            _ => n.map(|n| {
                if !topic_kind(n.kind) {
                    return n.name.as_str();
                }
                decls
                    .iter()
                    .filter(|d| d.line_start <= n.line_start && n.line_end <= d.line_end)
                    .min_by_key(|d| (d.line_end - d.line_start, d.line_start))
                    .map_or(n.name.as_str(), |d| d.name.as_str())
            }),
        };
        let entry = add_fact(&mut facts, fact, n.map(|n| n.line_start), None, owner);
        // The broker rides in the signatures of a topic fact, so moving a site to another
        // broker is reported as a change while a topic's text stays the one it always was.
        if let Some(b) = broker {
            entry.signatures.insert(b.slug().to_string());
        }
    }
    facts
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChangeKind {
    Added,
    Removed,
    Changed,
}

impl ChangeKind {
    fn sign(self) -> &'static str {
        match self {
            Self::Added => "+",
            Self::Removed => "-",
            Self::Changed => "~",
        }
    }
}

#[derive(Debug, Clone)]
struct Change {
    kind: ChangeKind,
    fact: Fact,
    base: Option<FactVal>,
    current: Option<FactVal>,
}

fn diff_facts(base: &BTreeMap<Fact, FactVal>, current: &BTreeMap<Fact, FactVal>) -> Vec<Change> {
    let mut out = Vec::new();
    for (fact, b) in base {
        match current.get(fact) {
            None => out.push(Change {
                kind: ChangeKind::Removed,
                fact: fact.clone(),
                base: Some(b.clone()),
                current: None,
            }),
            Some(c)
                if c.signatures != b.signatures
                    && (fact.has_signature()
                        || (fact.is_topic()
                            && !b.signatures.is_empty()
                            && !c.signatures.is_empty())) =>
            {
                out.push(Change {
                    kind: ChangeKind::Changed,
                    fact: fact.clone(),
                    base: Some(b.clone()),
                    current: Some(c.clone()),
                })
            }
            Some(_) => {}
        }
    }
    for (fact, c) in current {
        if !base.contains_key(fact) {
            out.push(Change {
                kind: ChangeKind::Added,
                fact: fact.clone(),
                base: None,
                current: Some(c.clone()),
            });
        }
    }
    out
}

// ---------------------------------------------------------------------------
// The run
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileStatus {
    Added,
    Removed,
    Modified,
}

impl FileStatus {
    fn label(self) -> &'static str {
        match self {
            Self::Added => "added",
            Self::Removed => "removed",
            Self::Modified => "modified",
        }
    }
}

struct FileReport {
    display: String,
    abs: PathBuf,
    status: FileStatus,
    changes: Vec<Change>,
    skipped: Option<String>,
}

fn run_with(
    args: &DiffMeshArgs,
    state: &AppState,
    git: &GitRunner,
    limits: Limits,
) -> Result<ToolOutput, ToolError> {
    let deadline = Instant::now() + limits.time_budget;
    let base = args.base.as_deref();
    if let Some(b) = base {
        git::validate_base(b).map_err(|e| (-32602, e.to_string()))?;
    }
    let targets = select_targets(args.scope.as_deref(), state)?;
    if targets.is_empty() {
        return Err((-32602, "`scope` selects no configured root".to_string()));
    }
    let program = git.locate().map_err(|e| (-32603, e.to_string()))?;
    let roots = &state.allowed_roots;
    let view = RootView::new(roots);

    // 1. Changed paths, per target.
    let mut diffs: Vec<RootDiff> = Vec::new();
    let mut failures: Vec<RootFailure> = Vec::new();
    let mut changed: Vec<Changed> = Vec::new();
    let mut seen: HashSet<PathBuf> = HashSet::new();
    let mut counters = Counters {
        cap: limits.max_files,
        ..Counters::default()
    };
    let mut matchers: HashMap<RepoId, ExcludeMatcher> = HashMap::new();
    for target in &targets {
        let Listing {
            choice,
            sha,
            toplevel,
            prefix,
            shallow,
            paths,
        } = match list_changes(git, &program, target, base) {
            Ok(found) => found,
            Err(f) => {
                failures.push(f);
                continue;
            }
        };
        let root_diff = diffs.len();
        diffs.push(RootDiff {
            root: roots
                .get(target.repo_id as usize)
                .cloned()
                .unwrap_or_else(|| target.dir.clone()),
            base: choice,
            base_sha: sha,
            toplevel,
            shallow,
        });
        for rel_top in paths {
            let Some(rel_dir) = rel_top.strip_prefix(prefix.as_str()) else {
                continue;
            };
            let abs = rel_dir
                .split('/')
                .filter(|c| !c.is_empty())
                .fold(target.dir.clone(), |p, c| p.join(c));
            let Some((repo_id, root)) = WorkspaceIndexer::most_specific_root(&abs, roots) else {
                continue;
            };
            if !seen.insert(abs.clone()) {
                continue;
            }
            let matcher = matchers.entry(repo_id).or_insert_with(|| {
                ExcludeMatcher::compile(&WorkspaceIndexer::exclude_patterns_for_root(
                    &state.config.workspace.exclude_patterns,
                    roots,
                    root,
                ))
            });
            if FilesystemCrawler::is_path_excluded(root, &abs, matcher) == Some(true) {
                counters.excluded += 1;
                continue;
            }
            if !is_indexable_source(&abs) {
                counters.non_source += 1;
                continue;
            }
            changed.push(Changed {
                repo_id,
                root_diff,
                display: clip_path(&view.path(&abs)),
                abs,
                rel_top,
            });
        }
    }
    if diffs.is_empty() {
        let code = if failures.iter().all(|f| f.invalid_request) {
            -32602
        } else {
            -32603
        };
        let reasons: Vec<String> = failures
            .iter()
            .map(|f| format!("root `{}`: {}", f.root.display(), f.message))
            .collect();
        return Err((
            code,
            format!(
                "diff_mesh: no root could be compared. {}",
                reasons.join(" ")
            ),
        ));
    }
    changed.sort_by(|a, b| a.display.cmp(&b.display));
    if changed.len() > limits.max_files {
        counters.truncated = changed.len() - limits.max_files;
        counters.protos_capped = changed[limits.max_files..]
            .iter()
            .filter(|c| is_proto(&c.abs))
            .count();
        changed.truncate(limits.max_files);
    }

    // 2. Base contents: one batch per repository and base.
    let mut base_sides: HashMap<usize, Side> = HashMap::new();
    let mut budget = BASE_CONTENT_BUDGET;
    let mut groups: BTreeMap<(String, String), Vec<(usize, &Changed)>> = BTreeMap::new();
    for (i, c) in changed.iter().enumerate() {
        let d = &diffs[c.root_diff];
        groups
            .entry((d.toplevel.clone(), d.base_sha.clone()))
            .or_default()
            .push((i, c));
    }
    for ((_, sha), files) in &groups {
        // Past the deadline nothing more is read; the extraction loop below
        // names every file left as not analysed.
        if Instant::now() >= deadline {
            break;
        }
        let Some(&(_, first)) = files.first() else {
            continue;
        };
        let dir = first
            .abs
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_default();
        // Run where the root is: the file's own directory may be gone.
        let dir = if dir.is_dir() {
            dir
        } else {
            diffs[first.root_diff].root.clone()
        };
        match read_base_contents(git, &program, &dir, sha, files, &mut budget) {
            Ok(sides) => base_sides.extend(sides),
            Err(e) => {
                for &(i, _) in files {
                    base_sides.insert(i, Side::Skipped(format!("base not readable ({e})")));
                }
            }
        }
    }

    // 3-4. Extract both sides, diff the contract facts. Call records resolve
    // against the live graph's client bindings (a base class in another file).
    let snapshot = state.snapshot();
    let resolver = snapshot.contract_graph.rpc_record_resolver();
    let extractor: ContentExtractor =
        WorkspaceIndexer::content_extractor(&state.config, &state.allowed_roots);
    let mut reports: Vec<FileReport> = Vec::new();
    let mut files_accessed: Vec<String> = Vec::new();
    let mut protos: Vec<ProtoInput> = Vec::new();
    // Changed `.proto` files the extraction loop never reached (time budget).
    let mut pending_protos: Vec<String> = Vec::new();
    for (i, c) in changed.iter().enumerate() {
        if Instant::now() >= deadline {
            counters.timed_out = changed[i..].iter().map(|c| c.display.clone()).collect();
            pending_protos.extend(
                changed[i..]
                    .iter()
                    .filter(|c| is_proto(&c.abs))
                    .map(|c| c.display.clone()),
            );
            break;
        }
        let base_side = base_sides.remove(&i).unwrap_or(Side::Absent);
        let current_side = read_current(&c.abs, state);
        let status = match (&base_side, &current_side) {
            (Side::Absent, Side::Absent) => continue,
            (Side::Absent, _) => FileStatus::Added,
            (_, Side::Absent) => FileStatus::Removed,
            _ => FileStatus::Modified,
        };
        if matches!(current_side, Side::Content(_)) {
            files_accessed.push(c.abs.to_string_lossy().into_owned());
        }
        // The wire check reuses the contents read here: no second `git` run.
        if is_proto(&c.abs) && status != FileStatus::Removed {
            let text = |side: &Side| match side {
                Side::Absent => Ok(None),
                Side::Skipped(why) => Err(why.clone()),
                Side::Content(bytes) => Ok(Some(String::from_utf8_lossy(bytes).into_owned())),
            };
            protos.push(ProtoInput {
                display: c.display.clone(),
                rel_top: c.rel_top.clone(),
                root_diff: c.root_diff,
                before: text(&base_side),
                after: text(&current_side),
            });
        }
        let mut skipped = None;
        let mut extract = |side: Side, label: &str| -> BTreeMap<Fact, FactVal> {
            match side {
                Side::Absent => BTreeMap::new(),
                Side::Skipped(why) => {
                    skipped.get_or_insert_with(|| format!("{label}: {why}"));
                    BTreeMap::new()
                }
                Side::Content(bytes) => match extractor.extract(&c.abs, &bytes, c.repo_id) {
                    Ok(index) if index.parse_failed => {
                        skipped.get_or_insert_with(|| format!("{label}: the parse failed"));
                        BTreeMap::new()
                    }
                    Ok(index) => facts_of(&index, &resolver, c.repo_id),
                    Err(why) => {
                        skipped.get_or_insert_with(|| format!("{label}: {why}"));
                        BTreeMap::new()
                    }
                },
            }
        };
        let before = extract(base_side, "base");
        let after = extract(current_side, "working tree");
        let changes = if skipped.is_some() {
            Vec::new()
        } else {
            diff_facts(&before, &after)
        };
        reports.push(FileReport {
            display: c.display.clone(),
            abs: c.abs.clone(),
            status,
            changes,
            skipped,
        });
    }

    // 5. Impact from the live graph, and the wire check of every changed .proto.
    let impact = render_impact(
        &reports,
        &snapshot.contract_graph,
        &resolver,
        &view,
        deadline,
    );
    let mut wire: Vec<WireEntry> = reports
        .iter()
        .filter(|r| is_proto(&r.abs) && r.status == FileStatus::Removed)
        .map(|r| {
            WireEntry::new(
                &r.display,
                WireRank::Removed,
                "file removed: every message and RPC it declared is gone (its RPC removals are listed with the file)".into(),
            )
        })
        .collect();
    const TIME_BUDGET_SPENT: &str = "not checked: the time budget of this call is spent; run `analyze_grpc` with this `.proto` as `target`";
    for display in &pending_protos {
        wire.push(WireEntry::new(
            display,
            WireRank::NotChecked,
            TIME_BUDGET_SPENT.into(),
        ));
    }
    for p in protos {
        if Instant::now() >= deadline {
            wire.push(WireEntry::new(
                &p.display,
                WireRank::NotChecked,
                TIME_BUDGET_SPENT.into(),
            ));
            continue;
        }
        let d = &diffs[p.root_diff];
        match (p.before, p.after) {
            (Err(why), _) => wire.push(WireEntry::new(
                &p.display,
                WireRank::NotChecked,
                format!("not checked: the base version was not read ({why})"),
            )),
            (_, Err(why)) => wire.push(WireEntry::new(
                &p.display,
                WireRank::NotChecked,
                format!("not checked: the working-tree version was not read ({why})"),
            )),
            (Ok(before), Ok(after)) => {
                let report = compare_wire(
                    p.rel_top,
                    d.base.clone(),
                    d.base_sha.clone(),
                    before.as_deref(),
                    after.as_deref().unwrap_or_default(),
                );
                wire.push(WireEntry::report(&p.display, &report));
            }
        }
    }

    // 6. Render.
    let text = render(
        &diffs, &failures, &reports, &counters, &impact, &wire, &view,
    );
    Ok(ToolOutput {
        text,
        files_accessed,
        secrets_redacted: 0,
    })
}

fn is_proto(path: &Path) -> bool {
    path.extension()
        .is_some_and(|e| e.eq_ignore_ascii_case("proto"))
}

/// Both versions of a changed `.proto`, as read for extraction: `Ok(None)` is
/// absent, `Err` the reason it was not read.
struct ProtoInput {
    display: String,
    rel_top: String,
    root_diff: usize,
    before: Result<Option<String>, String>,
    after: Result<Option<String>, String>,
}

/// The order wire results are listed in, most important first: a breaking
/// result is never collapsed before a clean one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum WireRank {
    Breaking,
    /// Not checked (time budget, a version not read) or not compared (a
    /// version that does not parse): nothing is claimed about it.
    NotChecked,
    Removed,
    Clean,
}

/// One changed `.proto`'s wire-format result: its one-line form, and the full
/// report for a breaking result (listed in full while the section has room).
struct WireEntry {
    display: String,
    rank: WireRank,
    result: String,
    full: Option<String>,
    breaks: usize,
    /// A breaking or unparseable result: the file changed its contract even
    /// with no declaration added or removed. A `.proto` merely not checked
    /// (time budget, a version not read) keeps what its facts say.
    contract_change: bool,
}

impl WireEntry {
    fn new(display: &str, rank: WireRank, result: String) -> Self {
        Self {
            display: clip_path(display),
            rank,
            result: clip_line(&result),
            full: None,
            breaks: 0,
            contract_change: false,
        }
    }

    fn report(display: &str, report: &crate::tools::analyze_grpc::WireReport) -> Self {
        let (class, result) = report.summary();
        let rank = match class {
            WireClass::Breaking => WireRank::Breaking,
            WireClass::NotCompared => WireRank::NotChecked,
            WireClass::Clean => WireRank::Clean,
        };
        Self {
            full: (rank == WireRank::Breaking).then(|| render_report(report)),
            breaks: report.breaking_count(),
            contract_change: matches!(class, WireClass::Breaking | WireClass::NotCompared),
            ..Self::new(display, rank, result)
        }
    }

    fn line(&self) -> String {
        format!("- `{}`: {}\n", self.display, self.result)
    }
}

// ---------------------------------------------------------------------------
// Impact
// ---------------------------------------------------------------------------

/// The impact section: one rendered block per looked-up element (the output
/// budget decides how many are shown), and the notes that close it.
struct Impact {
    items: Vec<String>,
    notes: Vec<String>,
    sites: usize,
}

fn push_rows(
    out: &mut String,
    rows: &[(&ContractNode, String)],
    view: &RootView<'_>,
    sites: &mut usize,
) {
    for (node, how) in rows.iter().take(MAX_IMPACT_ROWS) {
        let _ = writeln!(
            out,
            "- `{}` in `{}:{}` _({how})_",
            node.name,
            clip_path(&view.path(&node.file_path)),
            node.line_start
        );
    }
    if rows.len() > MAX_IMPACT_ROWS {
        let _ = writeln!(out, "- … and {} more", rows.len() - MAX_IMPACT_ROWS);
    }
    *sites += rows.len();
}

/// Which graph query an element's impact needs.
enum Lookup<'a> {
    Rpc(&'a str, Option<&'a str>),
    Symbol(&'a str),
    /// A removed site of a topic; its broker when the removed site's was statically known.
    Topic(&'a str, bool, Option<Broker>),
}

/// Looks up the impact of removed and changed elements, most important first:
/// removed RPCs, removed declarations, signature changes, then topics. The item
/// cap and the call's deadline are checked BEFORE each query, so no query runs
/// for an element that would be dropped; the elements they stop are counted.
fn render_impact<'g>(
    reports: &[FileReport],
    graph: &'g ContractGraph,
    resolver: &RpcRecordResolver<'g>,
    view: &RootView<'_>,
    deadline: Instant,
) -> Impact {
    let (mut routes, mut services) = (0usize, 0usize);
    // (priority, file, change, query)
    let mut wanted: Vec<(u8, &FileReport, &Change, Lookup<'_>)> = Vec::new();
    for r in reports {
        for ch in &r.changes {
            if ch.kind == ChangeKind::Added {
                continue;
            }
            let removed = ch.kind == ChangeKind::Removed;
            let package = ch
                .base
                .as_ref()
                .or(ch.current.as_ref())
                .and_then(|v| v.package.as_deref());
            let (priority, lookup) = match &ch.fact {
                Fact::Rpc("RPC", name) => (if removed { 0 } else { 2 }, Lookup::Rpc(name, package)),
                Fact::Rpc(..) => {
                    services += 1;
                    continue;
                }
                Fact::Route(_) => {
                    routes += 1;
                    continue;
                }
                Fact::Decl(_, name)
                    if name.chars().count() >= 3
                        && !GENERIC_NAMES.contains(&name.to_ascii_lowercase().as_str()) =>
                {
                    (if removed { 1 } else { 2 }, Lookup::Symbol(name))
                }
                Fact::Produces(topic) if removed => {
                    (3, Lookup::Topic(topic, true, site_broker(ch.base.as_ref())))
                }
                Fact::Consumes(topic) if removed => (
                    3,
                    Lookup::Topic(topic, false, site_broker(ch.base.as_ref())),
                ),
                _ => continue,
            };
            wanted.push((priority, r, ch, lookup));
        }
    }
    // Stable: within one priority, the file order of the report.
    wanted.sort_by_key(|(p, _, _, _)| *p);

    let mut items: Vec<String> = Vec::new();
    let mut sites = 0usize;
    let (mut capped, mut timed_out) = (0usize, 0usize);
    for (_, r, ch, lookup) in &wanted {
        if items.len() >= MAX_IMPACT_ITEMS {
            capped += 1;
            continue;
        }
        if Instant::now() >= deadline {
            timed_out += 1;
            continue;
        }
        let (rows, empty_note) = match lookup {
            Lookup::Rpc(name, package) => rpc_impact(
                graph,
                resolver,
                name,
                *package,
                &r.abs,
                ch.kind == ChangeKind::Removed,
            ),
            Lookup::Symbol(name) => symbol_impact(graph, name, &r.abs),
            Lookup::Topic(topic, producer, broker) => {
                topic_impact(graph, topic, *producer, *broker, &r.abs)
            }
        };
        let what = if ch.kind == ChangeKind::Removed {
            "removed"
        } else {
            "signature changed"
        };
        let mut item = format!(
            "\n#### {} {what} — `{}`\n",
            clip_line(&ch.fact.describe()),
            r.display
        );
        if rows.is_empty() {
            let _ = writeln!(item, "- {empty_note}");
        } else {
            push_rows(&mut item, &rows, view, &mut sites);
        }
        items.push(item);
    }
    let mut notes = Vec::new();
    if capped > 0 {
        notes.push(format!(
            "\n*Impact not computed for {capped} element(s): cap ({MAX_IMPACT_ITEMS} per call); narrow `scope`.*\n"
        ));
    }
    if timed_out > 0 {
        notes.push(format!(
            "\n*Impact not computed for {timed_out} element(s): time budget ({} s) of this call spent; narrow `scope`.*\n",
            CALL_TIME_BUDGET.as_secs()
        ));
    }
    if routes + services > 0 {
        notes.push(format!(
            "\n*Impact not computed for routes / services: {routes} removed or changed route(s) (HTTP callers are not linked in the graph), {services} removed or changed gRPC service(s) (their callers are listed under each RPC).*\n"
        ));
    }
    Impact {
        items,
        notes,
        sites,
    }
}

type Rows<'g> = (Vec<(&'g ContractNode, String)>, &'static str);

fn rpc_impact<'g>(
    graph: &'g ContractGraph,
    resolver: &RpcRecordResolver<'g>,
    name: &str,
    package: Option<&str>,
    file: &Path,
    removed: bool,
) -> Rows<'g> {
    let Some((service, method)) = name.rsplit_once('.') else {
        return (
            Vec::new(),
            "not a `Service.Method` name: callers not looked up",
        );
    };
    let mut rows: Vec<(&ContractNode, String)> = Vec::new();
    let mut seen: HashSet<mesh_core::NodeId> = HashSet::new();
    for (node, confidence, how) in graph.recorded_rpc_callers(resolver, service, method, package) {
        if seen.insert(node.id) {
            let role = match how {
                RecordedCaller::Method => "caller".to_string(),
                RecordedCaller::BoundClient => format!(
                    "holds a `{service}` client; no call to `{method}` found in it or its direct subclasses"
                ),
                RecordedCaller::ServiceClient => "service-level client, may call".to_string(),
            };
            rows.push((node, format!("{role}, match: {}", confidence.label())));
        }
    }
    // Removed: the handlers still serving it are left with nothing to implement.
    if removed {
        for (node, confidence, service_resolved) in
            graph.handlers_naming_rpc(resolver, service, method, package)
        {
            if seen.insert(node.id) {
                rows.push((
                    node,
                    format!(
                        "orphaned handler: implements a removed RPC{}, match: {}",
                        if service_resolved {
                            ""
                        } else {
                            " (service not resolved)"
                        },
                        confidence.label()
                    ),
                ));
            }
        }
    }
    // Still declared (a changed RPC): the trace adds callers linked by an edge
    // and the server handlers.
    let trace = graph.analyze_grpc(name);
    if trace
        .proto_definition
        .is_some_and(|d| d.name.eq_ignore_ascii_case(name))
    {
        for (node, confidence) in &trace.client_stubs {
            if seen.insert(node.id) {
                rows.push((node, format!("caller, match: {}", confidence.label())));
            }
        }
        for (node, confidence) in &trace.server_handlers {
            if &*node.file_path != file && seen.insert(node.id) {
                rows.push((
                    node,
                    format!("server handler, match: {}", confidence.label()),
                ));
            }
        }
    }
    (
        rows,
        "no recorded call site in any indexed root (a method built at runtime, or a root that is not indexed, is not seen)",
    )
}

fn symbol_impact<'g>(graph: &'g ContractGraph, name: &str, file: &Path) -> Rows<'g> {
    let (deps, how) = graph.find_dependents_matched(name);
    let label = match how {
        DependentsMatch::Import => "imports it, match: exact",
        DependentsMatch::DeclaringPackage => "imports its package, match: heuristic",
        DependentsMatch::GraphEdge => "linked by a call edge, match: exact",
        DependentsMatch::Substring => "import text contains the name, match: heuristic",
        DependentsMatch::None => "",
    };
    let rows = deps
        .into_iter()
        .filter(|n| &*n.file_path != file)
        .map(|n| (n, label.to_string()))
        .collect();
    (
        rows,
        "no dependent in the live graph (a dynamic import, or a root that is not indexed, is not seen)",
    )
}

fn topic_impact<'g>(
    graph: &'g ContractGraph,
    topic: &str,
    producer: bool,
    broker: Option<Broker>,
    file: &Path,
) -> Rows<'g> {
    // The other side on the removed site's own broker; a site of another known broker is not
    // on this channel (an unknown-broker site is, as `heuristic`).
    let target = broker.map_or_else(|| topic.to_string(), |b| qualified_topic_key(b, topic));
    let flow = graph.analyze_impact(&target);
    let (list, role) = if producer {
        (&flow.downstream_consumers, "consumer of the topic")
    } else {
        (&flow.upstream_producers, "producer of the topic")
    };
    let rows = list
        .iter()
        .filter(|n| &*n.file_path != file)
        .map(|n| {
            let confidence = if graph_is_heuristic(graph, n) {
                EdgeConfidence::Heuristic
            } else {
                EdgeConfidence::Exact
            };
            (*n, format!("{role}, match: {}", confidence.label()))
        })
        .collect();
    (
        rows,
        "no other side of this topic in the live graph (a topic name built at runtime is not seen)",
    )
}

/// The one broker a topic fact's sites were on, when the fact records exactly one.
fn site_broker(val: Option<&FactVal>) -> Option<Broker> {
    let mut brokers = val?.signatures.iter().filter_map(|s| Broker::from_slug(s));
    let first = brokers.next()?;
    brokers.next().is_none().then_some(first)
}

/// Topic rows have no per-row confidence in `ImpactFlow`; a pattern-detected
/// node is reported heuristic by the impact matrix, so it is here too.
fn graph_is_heuristic(graph: &ContractGraph, node: &ContractNode) -> bool {
    graph
        .all_edges()
        .iter()
        .any(|e| (e.from == node.id || e.to == node.id) && e.confidence != EdgeConfidence::Exact)
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

fn clip(sig: &str) -> String {
    let flat = sig.replace(['\n', '\r'], " ").replace('`', "'");
    if flat.chars().count() <= MAX_SIGNATURE_CHARS {
        return flat;
    }
    let cut: String = flat.chars().take(MAX_SIGNATURE_CHARS).collect();
    format!("{cut}…")
}

/// Longest prose line (a result, a heading's description) kept as written.
const MAX_LINE_CHARS: usize = 320;
/// Longest path shown; a longer one keeps its end (the file name).
const MAX_PATH_CHARS: usize = 160;

/// `text` on one line, at most [`MAX_LINE_CHARS`]. Its Markdown is kept when it
/// fits; a cut line loses its backticks, so no code span is left open.
fn clip_line(text: &str) -> String {
    let flat = text.replace(['\n', '\r'], " ");
    if flat.chars().count() <= MAX_LINE_CHARS {
        return flat;
    }
    let cut: String = flat.chars().take(MAX_LINE_CHARS).collect();
    format!("{}…", cut.replace('`', "'"))
}

/// A path for display, at most [`MAX_PATH_CHARS`], with no backtick.
fn clip_path(path: &str) -> String {
    let flat = path.replace(['\n', '\r'], " ").replace('`', "'");
    let n = flat.chars().count();
    if n <= MAX_PATH_CHARS {
        return flat;
    }
    let tail: String = flat.chars().skip(n - (MAX_PATH_CHARS - 1)).collect();
    format!("…{tail}")
}

fn signature_of(val: &FactVal) -> String {
    val.signatures
        .iter()
        .map(|s| format!("`{}`", clip(s)))
        .collect::<Vec<_>>()
        .join(", ")
}

fn render_change(ch: &Change) -> String {
    let mut line = format!("- {} {}", ch.kind.sign(), ch.fact.describe());
    let at = |v: &FactVal| (v.line != usize::MAX).then_some(v.line);
    match ch.kind {
        ChangeKind::Added => {
            if let Some(v) = &ch.current {
                if let Some(l) = at(v) {
                    let _ = write!(line, " (L{l})");
                }
                if let Some(o) = &v.owner {
                    let _ = write!(line, " in `{o}`");
                }
                if ch.fact.has_signature() && !v.signatures.is_empty() {
                    let _ = write!(line, ": {}", signature_of(v));
                }
            }
        }
        ChangeKind::Removed => {
            if let Some(v) = &ch.base {
                if let Some(l) = at(v) {
                    let _ = write!(line, " (base L{l})");
                }
                if let Some(o) = &v.owner {
                    let _ = write!(line, " in `{o}`");
                }
                if ch.fact.has_signature() && !v.signatures.is_empty() {
                    let _ = write!(line, ": {}", signature_of(v));
                }
            }
        }
        ChangeKind::Changed => {
            if let (Some(b), Some(c)) = (&ch.base, &ch.current) {
                if let Some(l) = at(c) {
                    let _ = write!(line, " (L{l})");
                }
                let _ = write!(line, ": {} → {}", signature_of(b), signature_of(c));
            }
        }
    }
    line
}

/// Added / removed / changed counts per fact family. An import or a call has no
/// signature, so `diff_facts` never reports it changed; its third slot stays 0 and
/// is not printed. A topic is changed only when its site moved to another broker.
#[derive(Default)]
struct Tally {
    decl: [usize; 3],
    rpc: [usize; 3],
    route: [usize; 3],
    produces: [usize; 3],
    consumes: [usize; 3],
    imports: [usize; 3],
    calls: [usize; 3],
}

fn tally(reports: &[FileReport]) -> Tally {
    let mut t = Tally::default();
    for ch in reports.iter().flat_map(|r| &r.changes) {
        let i = match ch.kind {
            ChangeKind::Added => 0,
            ChangeKind::Removed => 1,
            ChangeKind::Changed => 2,
        };
        let slot = match &ch.fact {
            Fact::Decl(..) => &mut t.decl,
            Fact::Rpc(..) => &mut t.rpc,
            Fact::Route(_) => &mut t.route,
            Fact::Produces(_) => &mut t.produces,
            Fact::Consumes(_) => &mut t.consumes,
            Fact::Import(_) => &mut t.imports,
            Fact::Calls(_) | Fact::CallsInferred(..) | Fact::CallsUnresolved(_) => &mut t.calls,
        };
        slot[i] += 1;
    }
    t
}

fn describe_base(d: &RootDiff) -> String {
    let short = d.base_sha.get(..12).unwrap_or(&d.base_sha);
    match &d.base {
        BaseChoice::Explicit(b) => format!("`{b}` (`{short}`)"),
        BaseChoice::MergeBase(r) => format!("merge-base of `HEAD` and `{r}` (`{short}`)"),
        BaseChoice::HeadFallback => format!(
            "`HEAD` (`{short}`): no `origin/HEAD`, `origin/main` or `main` ref, or no merge-base with it — only uncommitted changes are compared{}",
            if d.shallow { " (shallow clone)" } else { "" }
        ),
    }
}

/// Bytes the wire-format section may take: room for every breaking report a
/// branch usually has, the one-line results after them, and the closing
/// counts. Its result lines are collapsed into counts past it.
const WIRE_BUDGET: usize = 16 * 1024;
/// Roots and failed roots listed in the header; the rest are counted.
const MAX_LISTED_ROOTS: usize = 10;
/// File names listed in one note (time budget, body-only); the rest are counted.
const MAX_LISTED_NAMES: usize = 20;

/// Appends `piece` when `out` stays within `limit` bytes plus `reserve`.
fn push_within(out: &mut String, piece: &str, limit: usize, reserve: usize) -> bool {
    if out.len() + piece.len() + reserve <= limit {
        out.push_str(piece);
        true
    } else {
        false
    }
}

/// `names` as a backticked list of at most [`MAX_LISTED_NAMES`], then a count.
fn listed_names<'a>(names: impl Iterator<Item = &'a String>, total: usize) -> String {
    let listed: Vec<String> = names
        .take(MAX_LISTED_NAMES)
        .map(|n| format!("`{}`", clip_path(n)))
        .collect();
    let more = total.saturating_sub(listed.len());
    format!(
        "{}{}",
        listed.join(", "),
        if more > 0 {
            format!(" and {more} more")
        } else {
            String::new()
        }
    )
}

/// The answer, budgeted section by section within [`OUTPUT_BUDGET`] (44 KB,
/// under the registry's 48 KB cap with room for the notes it prepends):
///
/// 1. the header and summary: bounded by construction (at most
///    [`MAX_LISTED_ROOTS`] roots, [`MAX_LISTED_NAMES`] names per note, clipped
///    paths and messages, counts otherwise); the wire-format breaking COUNT is
///    in the summary, so it is always shown;
/// 2. the wire-format results, within [`WIRE_BUDGET`]: breaking first (full
///    report, else its result line), then not checked / not compared, removed,
///    clean; what does not fit is collapsed into counts;
/// 3. the impact rows, with their closing notes reserved;
/// 4. the per-file sections.
///
/// Each cut section ends with an "N more ... narrow `scope`" line. A final
/// clamp cuts at [`OUTPUT_BUDGET`] should the sections ever overrun it; the
/// sizes above keep it from firing.
fn render(
    diffs: &[RootDiff],
    failures: &[RootFailure],
    reports: &[FileReport],
    counters: &Counters,
    impact: &Impact,
    wire: &[WireEntry],
    view: &RootView<'_>,
) -> String {
    let t = tally(reports);
    // A topic is "changed" only when a site moved to another broker: `~N`, shown when non-zero
    // so a workspace with no move reads as before.
    let moved = |n: usize| {
        if n > 0 {
            format!(" ~{n}")
        } else {
            String::new()
        }
    };
    // A `.proto` whose wire check breaks or could not parse a version changed
    // its contract even with no declaration added or removed (a field
    // renumbered): never "body edits only". Not checked stays where its
    // facts put it (the wire section and the summary say it).
    let wire_of = |r: &FileReport| {
        wire.iter()
            .find(|w| w.contract_change && w.display == r.display)
    };
    let has_contract_change =
        |r: &FileReport| r.skipped.is_none() && (!r.changes.is_empty() || wire_of(r).is_some());
    let with_changes = reports.iter().filter(|r| has_contract_change(r)).count();
    let skipped = reports.iter().filter(|r| r.skipped.is_some()).count();
    let body_only = reports.len() - with_changes - skipped;
    let wire_breaks: usize = wire.iter().map(|w| w.breaks).sum();
    let breaking_protos = wire.iter().filter(|w| w.rank == WireRank::Breaking).count();
    // Every changed `.proto` that was not compared: not reached, not read, not
    // parsed, or left out by the file cap.
    let wire_unchecked = wire
        .iter()
        .filter(|w| w.rank == WireRank::NotChecked)
        .count()
        + counters.protos_capped;

    // 1. Header and summary.
    let mut out = String::from("## Mesh diff: working tree against the Git base\n\n");
    for d in diffs.iter().take(MAX_LISTED_ROOTS) {
        let _ = writeln!(
            out,
            "- **Root** `{}`: base {}",
            clip_path(&view.root(&d.root)),
            describe_base(d)
        );
    }
    if diffs.len() > MAX_LISTED_ROOTS {
        let _ = writeln!(
            out,
            "- … and {} more root(s)",
            diffs.len() - MAX_LISTED_ROOTS
        );
    }
    let _ = writeln!(
        out,
        "\n**Summary**: {} changed source file(s) — {with_changes} with contract changes, {body_only} body-only, {skipped} skipped. Declarations +{} -{} ~{}; RPCs/services +{} -{} ~{}; routes +{} -{} ~{}; producers +{} -{}{}; consumers +{} -{}{}; imports +{} -{}; RPC calls +{} -{}. Wire-format breaking changes: {wire_breaks} in {breaking_protos} `.proto`{}. Impacted call sites / dependents: {}.",
        reports.len(),
        t.decl[0], t.decl[1], t.decl[2],
        t.rpc[0], t.rpc[1], t.rpc[2],
        t.route[0], t.route[1], t.route[2],
        t.produces[0], t.produces[1], moved(t.produces[2]),
        t.consumes[0], t.consumes[1], moved(t.consumes[2]),
        t.imports[0], t.imports[1],
        t.calls[0], t.calls[1],
        if wire_unchecked > 0 {
            format!(" ({wire_unchecked} `.proto` not checked)")
        } else {
            String::new()
        },
        impact.sites,
    );
    if counters.excluded + counters.non_source > 0 {
        let _ = writeln!(
            out,
            "\n*Not analysed: {} file(s) excluded by `exclude_patterns`/ignore files, {} non-source file(s).*",
            counters.excluded, counters.non_source
        );
    }
    if counters.truncated > 0 || !counters.timed_out.is_empty() {
        out.push_str("\n> [!NOTE]\n");
        if counters.truncated > 0 {
            let _ = writeln!(
                out,
                "> {} more changed file(s) were not analysed: at most {} files per call.",
                counters.truncated, counters.cap
            );
        }
        if !counters.timed_out.is_empty() {
            let _ = writeln!(
                out,
                "> {} changed file(s) not analysed: time budget (about {} s, checked between files) of this call spent: {}.",
                counters.timed_out.len(),
                CALL_TIME_BUDGET.as_secs(),
                listed_names(counters.timed_out.iter(), counters.timed_out.len())
            );
        }
        out.push_str("> Narrow `scope` to one root or one sub-directory to cover them.\n");
    }
    for f in failures.iter().take(MAX_LISTED_ROOTS) {
        let _ = writeln!(
            out,
            "\n**Root not compared** `{}`: {}",
            clip_path(&view.root(&f.root)),
            clip(&f.message)
        );
    }
    if failures.len() > MAX_LISTED_ROOTS {
        let _ = writeln!(
            out,
            "\n*… and {} more root(s) not compared.*",
            failures.len() - MAX_LISTED_ROOTS
        );
    }

    // 2. Wire-format results, most important first.
    if !wire.is_empty() {
        let mut ordered: Vec<&WireEntry> = wire.iter().collect();
        ordered.sort_by_key(|w| w.rank);
        let limit = (out.len() + WIRE_BUDGET).min(OUTPUT_BUDGET);
        let _ = writeln!(
            out,
            "\n### Wire-format checks ({} changed `.proto`)",
            wire.len()
        );
        // The four closing count lines below are at most this long each.
        let reserve = 4 * 200;
        // Left out, per rank: breaking, not checked, removed, clean.
        let mut left = [0usize; 4];
        for w in ordered {
            let shown = match (&w.full, w.rank) {
                (Some(full), WireRank::Breaking) if push_within(&mut out, full, limit, reserve) => {
                    true
                }
                _ => push_within(&mut out, &w.line(), limit, reserve),
            };
            if !shown {
                left[w.rank as usize] += 1;
            }
        }
        let [breaking, unchecked, removed, clean] = left;
        if breaking > 0 {
            let _ = writeln!(
                out,
                "\n*{breaking} more breaking `.proto` not listed (counted in the summary); narrow `scope`.*"
            );
        }
        if unchecked > 0 {
            let _ = writeln!(
                out,
                "\n*{unchecked} more `.proto` not checked or not compared, not listed; narrow `scope`.*"
            );
        }
        if removed > 0 {
            let _ = writeln!(out, "\n*{removed} more removed `.proto` not listed.*");
        }
        if clean > 0 {
            let _ = writeln!(
                out,
                "\n*{clean} `.proto` with no wire-format breaking change.*"
            );
        }
    }

    // 3. Impact rows; the closing notes have their room reserved.
    let notes_len: usize = impact.notes.iter().map(String::len).sum();
    if !impact.items.is_empty() || !impact.notes.is_empty() {
        out.push_str("\n### Impact (live graph)\n");
    }
    let mut left_out = 0usize;
    for item in &impact.items {
        if !push_within(&mut out, item, OUTPUT_BUDGET, notes_len + NOTE_RESERVE) {
            left_out += 1;
        }
    }
    if left_out > 0 {
        let _ = writeln!(
            out,
            "\n*{left_out} more impacted element(s) not shown to stay under the 48 KB cap; narrow `scope`.*"
        );
    }
    for note in &impact.notes {
        out.push_str(note);
    }

    // 4. Per-file sections.
    let listed: Vec<&FileReport> = reports
        .iter()
        .filter(|r| has_contract_change(r) || r.skipped.is_some())
        .collect();
    if !listed.is_empty() {
        push_within(
            &mut out,
            "\n### Contract changes per file\n",
            OUTPUT_BUDGET,
            NOTE_RESERVE,
        );
    }
    let mut unlisted = 0usize;
    for r in &listed {
        let mut section = format!("\n#### `{}` ({})\n", r.display, r.status.label());
        if let Some(why) = &r.skipped {
            let _ = writeln!(section, "- skipped: {}", clip_line(why));
        }
        if let Some(w) = wire_of(r) {
            let _ = writeln!(
                section,
                "- wire format: {} (see the wire-format checks)",
                w.result
            );
        }
        for ch in r.changes.iter().take(MAX_CHANGES_PER_FILE) {
            section.push_str(&render_change(ch));
            section.push('\n');
        }
        if r.changes.len() > MAX_CHANGES_PER_FILE {
            let _ = writeln!(
                section,
                "- … and {} more",
                r.changes.len() - MAX_CHANGES_PER_FILE
            );
        }
        if !push_within(&mut out, &section, OUTPUT_BUDGET, NOTE_RESERVE) {
            unlisted += 1;
        }
    }
    if unlisted > 0 {
        let _ = writeln!(
            out,
            "\n*{unlisted} more file section(s) not shown to stay under the 48 KB cap; narrow `scope`.*"
        );
    }
    if body_only > 0 {
        let names = reports
            .iter()
            .filter(|r| !has_contract_change(r) && r.skipped.is_none())
            .map(|r| &r.display);
        let line = format!(
            "\n*No contract-level change (body edits only): {}*\n",
            listed_names(names, body_only)
        );
        if !push_within(&mut out, &line, OUTPUT_BUDGET, 0) {
            let _ = writeln!(out, "\n*{body_only} file(s) with body edits only.*");
        }
    }

    // Final clamp: never fires with the sizes above; it makes the bound hold
    // whatever a future section adds.
    if out.len() > OUTPUT_BUDGET {
        let note = "\n\n*Answer cut at the output budget; narrow `scope`.*\n";
        let mut cut = OUTPUT_BUDGET - note.len();
        while !out.is_char_boundary(cut) {
            cut -= 1;
        }
        if let Some(nl) = out[..cut].rfind('\n') {
            cut = nl;
        }
        out.truncate(cut);
        out.push_str(note);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::git::test_support::TempRepo;
    use crate::git::GIT_COMMAND_TIMEOUT;

    const PROTO: &str = r#"syntax = "proto3";
package demo.v1;

service UserService {
  rpc GetUser(GetUserRequest) returns (User);
  rpc DeleteUser(DeleteUserRequest) returns (Empty);
}

message GetUserRequest {
  string id = 1;
}

message DeleteUserRequest {
  string id = 1;
}

message User {
  string id = 1;
  string email = 2;
}

message Empty {}
"#;

    const GO_SERVER: &str = r#"package main

import (
	"context"

	pb "github.com/demo/gen/go/demo/v1"
	"google.golang.org/grpc"
)

type UserServer struct{}

func (s *UserServer) GetUser(ctx context.Context, req *pb.GetUserRequest) (*pb.User, error) {
	return &pb.User{Id: req.Id}, nil
}

func (s *UserServer) DeleteUser(ctx context.Context, req *pb.DeleteUserRequest) (*pb.Empty, error) {
	return &pb.Empty{}, nil
}

func main() {
	grpcServer := grpc.NewServer()
	pb.RegisterUserServiceServer(grpcServer, &UserServer{})
}
"#;

    const TS_CLIENT: &str = r#"import { Injectable } from '@nestjs/common';
import { ClientGrpc } from '@nestjs/microservices';

export interface UserServiceClient {
  DeleteUser(data: { id: string }): Promise<void>;
}

@Injectable()
export class AccountCleaner {
  private userClient: UserServiceClient;

  constructor(private readonly grpcClient: ClientGrpc) {
    this.userClient = this.grpcClient.getService<UserServiceClient>('UserService');
  }

  async purge(id: string) {
    await this.userClient.DeleteUser({ id });
  }
}
"#;

    const TS_PRODUCER: &str = r#"import { Producer } from 'kafkajs';

export class UserEvents {
  constructor(private readonly producer: Producer) {}

  async deleted(id: string) {
    await this.producer.send({ topic: 'user-deleted', messages: [{ value: id }] });
  }
}
"#;

    const TS_CONSUMER: &str = r#"import { Consumer } from 'kafkajs';

export class AuditConsumer {
  constructor(private readonly consumer: Consumer) {}

  async start() {
    await this.consumer.subscribe({ topic: 'user-deleted' });
  }
}
"#;

    const TS_LIB: &str = r#"export function formatUser(id: string): string {
  return `user:${id}`;
}
"#;

    const TS_VIEW: &str = r#"import { formatUser } from '../lib/format';

export function renderUser(id: string): string {
  return formatUser(id);
}
"#;

    const TS_ARCHIVE: &str = r#"import { Producer } from 'kafkajs';

export class ArchiveEvents {
  constructor(private readonly producer: Producer) {}

  async archived(id: string) {
    await this.producer.send({ topic: 'user-archived', messages: [{ value: id }] });
  }
}
"#;

    fn state_for(root: &Path) -> AppState {
        state_with(root, "")
    }

    /// `extra` is appended to the `[workspace]` table.
    fn state_with(root: &Path, extra: &str) -> AppState {
        let config = mesh_core::Config::load_from_str(&format!(
            "[workspace]\nname = \"t\"\nversion = \"0\"\nroots = [\".\"]\n{extra}"
        ))
        .expect("config");
        let audit = std::sync::Arc::new(mesh_core::AuditLogger::new_in_memory().expect("audit"));
        let rescan = std::sync::Arc::new(mesh_core::BackgroundRescanEngine::new().expect("rescan"));
        AppState::new(config, vec![root.to_path_buf()], audit, rescan)
    }

    /// Indexes the working tree into the live snapshot, as the server does.
    fn indexed_state(root: &Path) -> AppState {
        index(state_for(root))
    }

    fn index(state: AppState) -> AppState {
        let snapshot =
            WorkspaceIndexer::build_snapshot(&state.config, &state.allowed_roots, None, None, None);
        state.install_snapshot(snapshot);
        state
    }

    fn files(max_files: usize) -> Limits {
        Limits {
            max_files,
            ..Limits::DEFAULT
        }
    }

    fn args(base: Option<&str>, scope: Option<&str>) -> DiffMeshArgs {
        DiffMeshArgs {
            base: base.map(str::to_string),
            scope: scope.map(CompactStr::new),
            _meta: None,
        }
    }

    /// The base commit on `main`: a .proto service with two RPCs, a Go server
    /// implementing both, a TS client calling one, a producer and a consumer on
    /// one topic, and an exported function another file imports.
    fn base_repo() -> TempRepo {
        let repo = TempRepo::new();
        repo.write("proto/user.proto", PROTO);
        repo.write("server/main.go", GO_SERVER);
        repo.write("client/cleaner.ts", TS_CLIENT);
        repo.write("events/producer.ts", TS_PRODUCER);
        repo.write("events/consumer.ts", TS_CONSUMER);
        repo.write("lib/format.ts", TS_LIB);
        repo.write("client/view.ts", TS_VIEW);
        repo.write("README.md", "# demo\n");
        repo.commit("base");
        repo
    }

    /// On a branch: a committed RPC removal and signature change, then
    /// uncommitted edits (a field renumbered, a new producer, a binary file).
    fn changed_repo() -> TempRepo {
        let repo = base_repo();
        repo.git(&["checkout", "-q", "-b", "feature"]);
        repo.write(
            "proto/user.proto",
            &PROTO.replace("  rpc DeleteUser(DeleteUserRequest) returns (Empty);\n", ""),
        );
        repo.write(
            "lib/format.ts",
            &TS_LIB.replace(
                "formatUser(id: string): string",
                "formatUser(id: string, upper: boolean): string",
            ),
        );
        repo.commit("drop DeleteUser, widen formatUser");
        // Uncommitted: a wire break, an untracked producer, a binary source
        // file, a body-only edit and a non-source file.
        repo.write(
            "proto/user.proto",
            &PROTO
                .replace("  rpc DeleteUser(DeleteUserRequest) returns (Empty);\n", "")
                .replace("string email = 2;", "string email = 3;"),
        );
        repo.write("events/archive.ts", TS_ARCHIVE);
        repo.write("client/blob.ts", "export const x = 1;\0\0\0\n");
        repo.write(
            "events/consumer.ts",
            &TS_CONSUMER.replace(
                "async start() {",
                "async start() {\n    const ready = true;",
            ),
        );
        repo.write("README.md", "# demo, edited\n");
        repo
    }

    #[test]
    fn reports_contract_changes_impact_and_the_wire_break() {
        let repo = changed_repo();
        let state = indexed_state(&repo.root);
        let out = run_with(&args(None, None), &state, &repo.runner(), Limits::DEFAULT)
            .expect("diff_mesh ok");
        let text = &out.text;

        // Default base: the merge-base with `main`, i.e. the base commit.
        assert!(text.contains("merge-base of `HEAD` and `main`"), "{text}");
        // The removed RPC, from the .proto.
        assert!(
            text.contains("- - RPC `UserService.DeleteUser` (base L6)"),
            "{text}"
        );
        // The changed exported signature.
        assert!(
            text.contains("~ symbol `formatUser`")
                && text.contains("`function formatUser(id: string, upper: boolean): string {`"),
            "{text}"
        );
        // The new producer on a new topic (untracked file).
        assert!(text.contains("#### `events/archive.ts` (added)"), "{text}");
        assert!(
            text.contains("+ produces topic `user-archived` (L7) in `archived`"),
            "{text}"
        );
        // The binary file is skipped with the guard's reason; the body-only edit
        // and the non-source file are counted, not listed as changes.
        assert!(
            text.contains("#### `client/blob.ts` (added)\n- skipped: working tree: binary"),
            "{text}"
        );
        assert!(
            text.contains("No contract-level change (body edits only): `events/consumer.ts`"),
            "{text}"
        );
        assert!(text.contains("1 non-source file(s)"), "{text}");

        // Impact: the caller of the removed RPC, the dependent of the function.
        let impact = text
            .split("### Impact (live graph)")
            .nth(1)
            .expect("impact");
        let rpc = impact
            .split("#### RPC `UserService.DeleteUser` removed")
            .nth(1)
            .expect("rpc impact");
        assert!(
            rpc.contains("- `purge` in `client/cleaner.ts:16` _(caller, match: exact)_"),
            "{text}"
        );
        let func = impact
            .split("#### symbol `formatUser` signature changed — `lib/format.ts`")
            .nth(1)
            .expect("symbol impact");
        assert!(
            func.contains("- `renderUser` in `client/view.ts:3` _(imports it, match: exact)_"),
            "{text}"
        );

        // The wire check of the changed .proto, against the same base.
        assert!(
            text.contains("### Wire-format check: `proto/user.proto`"),
            "{text}"
        );
        assert!(text.contains("`WIRE_FORMAT_BREAKING_CHANGE`"), "{text}");
        assert!(!text.contains("Wire-format breaking changes: 0"), "{text}");
        assert!(text.len() < 48 * 1024);
    }

    /// 9.0 T7: moving a producer to another broker is a contract change even though the topic
    /// name is the same, and a topic that stays on one broker reads as before.
    #[test]
    fn a_producer_moved_to_another_broker_is_a_change() {
        let repo = base_repo();
        repo.git(&["checkout", "-q", "-b", "feature"]);
        repo.write(
            "events/producer.ts",
            r#"import amqp from 'amqplib';

export class UserEvents {
  async deleted(ch: amqp.Channel, id: string) {
    ch.sendToQueue('user-deleted', Buffer.from(id));
  }
}
"#,
        );
        let state = indexed_state(&repo.root);
        let out = run_with(&args(None, None), &state, &repo.runner(), Limits::DEFAULT)
            .expect("diff_mesh ok");
        let text = &out.text;
        assert!(
            text.contains("~ produces topic `user-deleted`")
                && text.contains("`kafka` → `rabbitmq`"),
            "{text}"
        );
        // The summary counts the moved producer, as the detail lists it.
        assert!(
            text.contains("producers +0 -0 ~1; consumers +0 -0;"),
            "{text}"
        );

        // The same file with the same broker reports nothing for the topic.
        repo.write(
            "events/producer.ts",
            &TS_PRODUCER.replace(
                "async deleted(id: string) {",
                "async deleted(id: string) {\n    const t = 1;",
            ),
        );
        let state = indexed_state(&repo.root);
        let out = run_with(&args(None, None), &state, &repo.runner(), Limits::DEFAULT)
            .expect("diff_mesh ok");
        assert!(
            !out.text.contains("produces topic `user-deleted`"),
            "{}",
            out.text
        );
    }

    #[test]
    fn scope_restricts_the_diff_and_an_outside_scope_is_refused() {
        let repo = changed_repo();
        let state = indexed_state(&repo.root);
        let events = repo.root.join("events").to_string_lossy().into_owned();
        let out = run_with(
            &args(Some("main"), Some(&events)),
            &state,
            &repo.runner(),
            Limits::DEFAULT,
        )
        .expect("scoped diff");
        assert!(out.text.contains("events/archive.ts"), "{}", out.text);
        assert!(!out.text.contains("proto/user.proto"), "{}", out.text);
        assert!(!out.text.contains("formatUser"), "{}", out.text);

        let outside = repo.home.to_string_lossy().into_owned();
        let err = run_with(
            &args(None, Some(&outside)),
            &state,
            &repo.runner(),
            Limits::DEFAULT,
        )
        .err()
        .expect("jail");
        assert_eq!(err.0, -32602, "{}", err.1);
    }

    #[test]
    fn file_cap_says_how_many_files_were_left_out() {
        let repo = changed_repo();
        let state = indexed_state(&repo.root);
        let out = run_with(&args(Some("main"), None), &state, &repo.runner(), files(2))
            .expect("capped diff");
        assert!(
            out.text
                .contains("more changed file(s) were not analysed: at most 2 files per call"),
            "{}",
            out.text
        );
        assert!(out.text.contains("Narrow `scope`"), "{}", out.text);
    }

    #[test]
    fn bad_or_unknown_base_is_refused_with_invalid_params() {
        let repo = changed_repo();
        let state = state_for(&repo.root);
        for bad in ["--output=/tmp/x", "main:proto/user.proto", "a b", ""] {
            let err = run_with(&args(Some(bad), None), &state, &repo.runner(), files(10))
                .err()
                .expect("refused");
            assert_eq!(err.0, -32602, "{bad:?}: {}", err.1);
        }
        let err = run_with(
            &args(Some("no-such-branch"), None),
            &state,
            &repo.runner(),
            files(10),
        )
        .err()
        .expect("unknown base");
        assert_eq!(err.0, -32602, "{}", err.1);
        assert!(
            err.1.contains("`no-such-branch` does not name a commit"),
            "{}",
            err.1
        );
        assert!(
            err.1.contains(&repo.root.display().to_string()),
            "{}",
            err.1
        );
    }

    #[test]
    fn a_root_outside_git_is_a_clear_error_naming_it() {
        let dir = tempfile::tempdir().expect("tempdir");
        let base = dunce::canonicalize(dir.path()).expect("canon");
        let root = base.join("plain");
        let home = base.join("home");
        std::fs::create_dir_all(&root).expect("mkdir");
        std::fs::create_dir_all(&home).expect("mkdir");
        std::fs::write(root.join("a.ts"), "export function a() {}\n").expect("write");
        let state = state_for(&root);
        let mut env = TempRepo::isolated_env(&home);
        // Never walk up into whatever repository holds the temp directory.
        env.push((
            "GIT_CEILING_DIRECTORIES".into(),
            base.clone().into_os_string(),
        ));
        let git = GitRunner {
            path: std::env::var_os("PATH"),
            env,
            timeout: GIT_COMMAND_TIMEOUT,
        };
        let err = run_with(&args(None, None), &state, &git, files(10))
            .err()
            .expect("not a repo");
        assert_eq!(err.0, -32603, "{}", err.1);
        assert!(err.1.contains(&root.display().to_string()), "{}", err.1);
        assert!(
            err.1.contains("not inside a usable Git repository"),
            "{}",
            err.1
        );
    }

    #[test]
    fn detached_head_and_renames_are_compared() {
        let repo = base_repo();
        let head = repo.git(&["rev-parse", "HEAD"]);
        repo.git(&["checkout", "-q", "--detach", &head]);
        repo.git(&["mv", "lib/format.ts", "lib/fmt.ts"]);
        let state = indexed_state(&repo.root);
        let out = run_with(&args(Some("main"), None), &state, &repo.runner(), files(10))
            .expect("detached diff");
        // A rename is a removal plus an addition.
        assert!(
            out.text.contains("#### `lib/format.ts` (removed)"),
            "{}",
            out.text
        );
        assert!(
            out.text.contains("#### `lib/fmt.ts` (added)"),
            "{}",
            out.text
        );
        assert!(out.text.contains("- - symbol `formatUser`"), "{}", out.text);
        assert!(out.text.contains("- + symbol `formatUser`"), "{}", out.text);
    }

    const TS_QUALIFIED_CLIENT: &str = r#"import { ClientGrpc } from '@nestjs/microservices';

export class QualifiedCleaner {
  private userClient: UserServiceClient;

  constructor(private readonly grpcClient: ClientGrpc) {
    this.userClient = this.grpcClient.getService<UserServiceClient>('demo.v1.UserService');
  }

  async wipe(id: string) {
    await this.userClient.DeleteUser({ id });
  }
}
"#;

    const GO_CLIENT: &str = r#"package main

import pb "github.com/demo/gen/go/demo/v1"

func Connect(conn any) {
	client := pb.NewUserServiceClient(conn)
	_ = client
}
"#;

    /// A client naming the service with its proto package records
    /// `=demo.v1.UserService.DeleteUser`; Go records only a service-level client.
    /// Removing the RPC lists both, never "no recorded call site".
    #[test]
    fn package_qualified_and_service_level_callers_of_a_removed_rpc_are_listed() {
        let probe = mesh_parsers::PolyglotIndexer::extract(
            Path::new("/probe/qualified.ts"),
            TS_QUALIFIED_CLIENT,
            0,
        );
        assert!(
            probe
                .rpc_calls
                .iter()
                .any(|(_, t)| t == "=demo.v1.UserService.DeleteUser"),
            "{:?}",
            probe.rpc_calls
        );
        let go = mesh_parsers::PolyglotIndexer::extract(Path::new("/probe/c.go"), GO_CLIENT, 0);
        assert!(
            go.rpc_calls.iter().any(|(_, t)| t == "UserService"),
            "{:?}",
            go.rpc_calls
        );

        let repo = TempRepo::new();
        repo.write("proto/user.proto", PROTO);
        repo.write("client/qualified.ts", TS_QUALIFIED_CLIENT);
        repo.write("goclient/main.go", GO_CLIENT);
        repo.commit("base");
        repo.write(
            "proto/user.proto",
            &PROTO.replace("  rpc DeleteUser(DeleteUserRequest) returns (Empty);\n", ""),
        );
        let state = indexed_state(&repo.root);
        let out =
            run_with(&args(Some("main"), None), &state, &repo.runner(), files(10)).expect("diff");
        let rpc = out
            .text
            .split("#### RPC `UserService.DeleteUser` removed")
            .nth(1)
            .expect("rpc impact");
        assert!(
            rpc.contains("- `wipe` in `client/qualified.ts:10` _(caller, match: exact)_"),
            "{}",
            out.text
        );
        assert!(
            rpc.contains(
                "in `goclient/main.go:5` _(service-level client, may call, match: heuristic)_"
            ),
            "{}",
            out.text
        );
        assert!(!rpc.contains("no recorded call site"), "{}", out.text);
    }

    /// Impact rows past the 48 KB cap are cut by `render` itself: the wire
    /// result survives, the cut says so, the whole answer fits.
    #[test]
    fn impact_overflow_keeps_the_wire_result_and_says_what_was_cut() {
        let repo = TempRepo::new();
        let names: Vec<String> = (0..65)
            .map(|i| format!("computeTheQuarterlyRevenueForRegionNumber{i:02}"))
            .collect();
        let lib = |extra: &str| {
            names
                .iter()
                .map(|n| {
                    format!("export function {n}(id: string{extra}): string {{\n  return id;\n}}\n")
                })
                .collect::<String>()
        };
        repo.write("lib/many.ts", &lib(""));
        let imports = names
            .iter()
            .map(|n| format!("  {n},\n"))
            .collect::<String>();
        let calls = names
            .iter()
            .map(|n| format!("  {n}(id);\n"))
            .collect::<String>();
        for i in 0..12 {
            repo.write(
                &format!("consumers/importer_with_a_long_name_{i:02}.ts"),
                &format!(
                    "import {{\n{imports}}} from '../lib/many';\n\nexport function useEverything{i:02}(id: string): string {{\n{calls}  return id;\n}}\n"
                ),
            );
        }
        repo.write("proto/user.proto", PROTO);
        repo.commit("base");
        repo.write("lib/many.ts", &lib(", region: number"));
        repo.write(
            "proto/user.proto",
            &PROTO.replace("string email = 2;", "string email = 3;"),
        );
        let state = indexed_state(&repo.root);
        let out = run_with(
            &args(Some("main"), None),
            &state,
            &repo.runner(),
            files(100),
        )
        .expect("diff");
        let text = &out.text;
        assert!(text.len() <= 48 * 1024, "{} bytes", text.len());
        assert!(
            text.contains("### Wire-format check: `proto/user.proto`"),
            "{text}"
        );
        assert!(text.contains("`WIRE_FORMAT_BREAKING_CHANGE`"), "{text}");
        assert!(
            text.contains(
                "more impacted element(s) not shown to stay under the 48 KB cap; narrow `scope`."
            ),
            "{text}"
        );
        // 65 changed symbols, 60 looked up: the 5 past the cap are never queried,
        // and the answer says so.
        assert!(
            text.contains("Impact not computed for 5 element(s): cap (60 per call)"),
            "{text}"
        );
        // The wire section comes before the impact rows that were cut.
        let wire_at = text.find("### Wire-format check").expect("wire");
        let impact_at = text.find("### Impact (live graph)").expect("impact");
        assert!(wire_at < impact_at, "{text}");
    }

    /// A spent time budget stops the call: the files not extracted are named.
    #[test]
    fn a_spent_time_budget_lists_the_files_not_analysed() {
        let repo = changed_repo();
        let state = indexed_state(&repo.root);
        let limits = Limits {
            max_files: 100,
            time_budget: Duration::ZERO,
        };
        let out =
            run_with(&args(Some("main"), None), &state, &repo.runner(), limits).expect("diff");
        assert!(
            out.text
                .contains("changed file(s) not analysed: time budget"),
            "{}",
            out.text
        );
        assert!(out.text.contains("`proto/user.proto`"), "{}", out.text);
        assert!(out.files_accessed.is_empty(), "{:?}", out.files_accessed);
        // The pending `.proto` is counted: no "0 breaking changes" that reads
        // as checked and safe.
        assert!(
            out.text
                .contains("Wire-format breaking changes: 0 in 0 `.proto` (1 `.proto` not checked)"),
            "{}",
            out.text
        );
        assert!(
            out.text
                .contains("- `proto/user.proto`: not checked: the time budget"),
            "{}",
            out.text
        );
    }

    /// Several hundred changed `.proto` files: the answer stays under 48 KB, the
    /// breaking count is in the summary, the clean results collapse to a count.
    #[test]
    fn hundreds_of_changed_protos_stay_under_the_cap() {
        let proto = |i: usize, email: u32, extra: &str| {
            format!(
                "syntax = \"proto3\";\npackage demo.p{i:03};\n\nmessage User{i:03} {{\n  string id = 1;\n  string email = {email};\n{extra}}}\n"
            )
        };
        let repo = TempRepo::new();
        for i in 0..300 {
            repo.write(
                &format!("protos/a_rather_long_directory_name/user_{i:03}.proto"),
                &proto(i, 2, ""),
            );
        }
        repo.commit("base");
        for i in 0..300 {
            // 40 renumber a field (breaking), the rest add one (compatible).
            let changed = if i < 40 {
                proto(i, 3, "")
            } else {
                proto(i, 2, "  string phone = 4;\n")
            };
            repo.write(
                &format!("protos/a_rather_long_directory_name/user_{i:03}.proto"),
                &changed,
            );
        }
        let state = state_for(&repo.root);
        let out = run_with(
            &args(Some("main"), None),
            &state,
            &repo.runner(),
            Limits::DEFAULT,
        )
        .expect("diff");
        let text = &out.text;
        assert!(text.len() <= 48 * 1024, "{} bytes", text.len());
        assert!(
            text.contains("Wire-format breaking changes: 40 in 40 `.proto`."),
            "{text}"
        );
        assert!(
            text.contains("`.proto` with no wire-format breaking change.*"),
            "{text}"
        );
        assert!(
            text.contains("### Wire-format checks (300 changed `.proto`)"),
            "{text}"
        );
        // Breaking results come before every clean one.
        let first_clean = text
            .find(": no wire-format breaking change")
            .unwrap_or(text.len());
        let last_break = text.rfind("WIRE_FORMAT_BREAKING_CHANGE").unwrap_or(0);
        assert!(last_break < first_clean, "{text}");
    }

    /// A client naming another package's `UserService` is listed, ambiguous.
    #[test]
    fn a_caller_in_another_package_is_ambiguous() {
        let repo = TempRepo::new();
        repo.write("proto/user.proto", PROTO);
        repo.write(
            "client/other.ts",
            &TS_QUALIFIED_CLIENT.replace("'demo.v1.UserService'", "'other.v2.UserService'"),
        );
        repo.commit("base");
        repo.write(
            "proto/user.proto",
            &PROTO.replace("  rpc DeleteUser(DeleteUserRequest) returns (Empty);\n", ""),
        );
        let state = indexed_state(&repo.root);
        let out =
            run_with(&args(Some("main"), None), &state, &repo.runner(), files(10)).expect("diff");
        assert!(
            out.text
                .contains("- `wipe` in `client/other.ts:10` _(caller, match: ambiguous)_"),
            "{}",
            out.text
        );
    }

    /// An excluded file is never read nor reported, even when it is a source
    /// file that changed: its content never reaches the answer.
    #[test]
    fn excluded_secrets_are_neither_read_nor_reported() {
        const SECRET: &str = "sk_live_0123456789abcdefSECRET";
        let repo = TempRepo::new();
        repo.write(".env", "API_KEY=old\n");
        repo.write("certs/server.pem", "-----BEGIN KEY-----\nold\n");
        repo.write("secrets/keys.ts", "export const KEY = 'old';\n");
        repo.write("lib/format.ts", TS_LIB);
        repo.commit("base");
        repo.write(".env", &format!("API_KEY={SECRET}\n"));
        repo.write(
            "certs/server.pem",
            &format!("-----BEGIN KEY-----\n{SECRET}\n"),
        );
        repo.write(
            "secrets/keys.ts",
            &format!("export const KEY = '{SECRET}';\nexport function leak(): string {{ return KEY; }}\n"),
        );
        repo.write(
            "lib/format.ts",
            &TS_LIB.replace("(id: string)", "(id: string, n: number)"),
        );
        let state = index(state_with(
            &repo.root,
            "exclude_patterns = [\".env\", \"*.pem\", \"secrets/**\"]\n",
        ));
        let out =
            run_with(&args(Some("main"), None), &state, &repo.runner(), files(10)).expect("diff");
        assert!(!out.text.contains(SECRET), "{}", out.text);
        for path in [".env", "server.pem", "secrets/keys.ts", "leak"] {
            assert!(!out.text.contains(path), "{path}: {}", out.text);
        }
        assert!(
            out.text
                .contains("3 file(s) excluded by `exclude_patterns`/ignore files"),
            "{}",
            out.text
        );
        assert!(
            out.files_accessed.iter().all(|f| f.ends_with("format.ts")),
            "{:?}",
            out.files_accessed
        );
        assert!(out.text.contains("~ symbol `formatUser`"), "{}", out.text);
    }

    /// Moving a function within its file changes lines, not the contract.
    #[test]
    fn a_function_moved_within_its_file_is_no_change() {
        const FIRST: &str = "export function alpha(id: string): string {\n  return id;\n}\n";
        const SECOND: &str = "export function beta(n: number): number {\n  return n;\n}\n";
        let repo = TempRepo::new();
        repo.write("lib/two.ts", &format!("{FIRST}\n{SECOND}"));
        repo.commit("base");
        repo.write("lib/two.ts", &format!("{SECOND}\n// moved\n{FIRST}"));
        let state = indexed_state(&repo.root);
        let out =
            run_with(&args(Some("main"), None), &state, &repo.runner(), files(10)).expect("diff");
        assert!(
            out.text
                .contains("1 changed source file(s) — 0 with contract changes, 1 body-only"),
            "{}",
            out.text
        );
        assert!(
            out.text
                .contains("No contract-level change (body edits only): `lib/two.ts`"),
            "{}",
            out.text
        );
        assert!(!out.text.contains("#### `lib/two.ts`"), "{}", out.text);
    }

    /// Two roots with one directory name stay apart in every path shown.
    #[test]
    fn roots_sharing_a_directory_name_are_told_apart() {
        let dir = tempfile::tempdir().expect("tempdir");
        let base = dunce::canonicalize(dir.path()).expect("canon");
        let roots = vec![base.join("a").join("api"), base.join("b").join("api")];
        let view = RootView::new(&roots);
        let shown_a = view.path(&roots[0].join("x.ts"));
        let shown_b = view.path(&roots[1].join("x.ts"));
        assert_ne!(shown_a, shown_b);
        assert_eq!(shown_a, "a/api/x.ts");
        assert_eq!(view.root(&roots[1]), "b/api");
    }

    // --- NestJS gateway fixture (the Volontariapp field patterns) ----------

    const NEST_PROTO: &str = r#"syntax = "proto3";
package demo.v1;

service UserService {
  rpc GetPublicUser(GetPublicUserQuery) returns (PublicUser);
  rpc GetUser(GetUserQuery) returns (PublicUser);
}

message GetPublicUserQuery {
  string id = 1;
}

message GetUserQuery {
  string id = 1;
  string pagination = 2;
}

message PublicUser {
  string id = 1;
}
"#;

    /// Holds the client obtained in `onModuleInit`; never calls it itself.
    const NEST_BASE: &str = r#"import { OnModuleInit } from '@nestjs/common';
import type { ClientGrpc } from '@nestjs/microservices';

export abstract class BaseUserGrpcController implements OnModuleInit {
  protected userClient!: UserServiceClient;

  constructor(protected client: ClientGrpc) {}

  onModuleInit() {
    this.userClient = this.client.getService<UserServiceClient>('UserService');
  }
}
"#;

    /// Calls through the field it inherits: the real call site.
    const NEST_QUERY: &str = r#"import { BaseUserGrpcController } from './base-user-grpc.controller';

export class UserQueryController extends BaseUserGrpcController {
  async getPublicUser(id: string) {
    return this.userClient.getPublicUser({ id });
  }
}
"#;

    /// Binds a client of the service, never calls `getPublicUser`.
    const NEST_SEED: &str = r#"import { OnModuleInit } from '@nestjs/common';
import type { ClientGrpc } from '@nestjs/microservices';

export class SystemSeedService implements OnModuleInit {
  private userClient!: UserServiceClient;

  constructor(private readonly client: ClientGrpc) {}

  onModuleInit() {
    this.userClient = this.client.getService<UserServiceClient>('UserService');
  }

  async seed() {
    return 1;
  }
}
"#;

    /// An `onModuleInit` with no client at all.
    const NEST_HEALTH: &str = r#"import { OnModuleInit } from '@nestjs/common';

export class HealthService implements OnModuleInit {
  private ready = false;

  onModuleInit() {
    this.ready = true;
  }
}
"#;

    const NEST_CONSTANTS: &str = r#"export const USER_SERVICE = 'UserService';

export const METHODS = {
  GET_PUBLIC_USER: 'GetPublicUser',
  GET_USER: 'GetUser',
} as const;
"#;

    /// The server side: handlers named through constants.
    const NEST_HANDLER: &str = r#"import { Controller } from '@nestjs/common';
import { GrpcMethod } from '@nestjs/microservices';
import { METHODS, USER_SERVICE } from './constants';

@Controller()
export class UserGrpcController {
  @GrpcMethod(USER_SERVICE, METHODS.GET_PUBLIC_USER)
  async getPublicUser(data: { id: string }) {
    return { id: data.id };
  }

  @GrpcMethod(USER_SERVICE, METHODS.GET_USER)
  async getUser(data: { id: string }) {
    return { id: data.id };
  }
}
"#;

    /// A second subclass calling another RPC through the inherited field.
    const NEST_ADMIN: &str = r#"import { BaseUserGrpcController } from './base-user-grpc.controller';

export class UserAdminController extends BaseUserGrpcController {
  async getUser(id: string) {
    return this.userClient.getUser({ id });
  }
}
"#;

    fn nest_repo() -> TempRepo {
        let repo = TempRepo::new();
        repo.write("proto/user.proto", NEST_PROTO);
        repo.write("gateway/base-user-grpc.controller.ts", NEST_BASE);
        repo.write("gateway/user.query-controller.ts", NEST_QUERY);
        repo.write("gateway/seed.service.ts", NEST_SEED);
        repo.write("gateway/health.service.ts", NEST_HEALTH);
        repo.write("users/constants.ts", NEST_CONSTANTS);
        repo.write("users/user.grpc-controller.ts", NEST_HANDLER);
        repo.commit("base");
        repo
    }

    /// Removing `GetPublicUser`: the subclass call through the inherited
    /// field is the caller (at its own node, exact through the base class's
    /// binding), the base class that only binds the field is not listed, the
    /// bind-only class says it holds a client with no call found, the
    /// unrelated `onModuleInit` is absent, and the handler named through
    /// constants is reported orphaned.
    #[test]
    fn removed_rpc_lists_inherited_callers_bind_only_classes_and_orphaned_handlers() {
        let repo = nest_repo();
        repo.write(
            "proto/user.proto",
            &NEST_PROTO.replace(
                "  rpc GetPublicUser(GetPublicUserQuery) returns (PublicUser);\n",
                "",
            ),
        );
        let state = indexed_state(&repo.root);
        let out =
            run_with(&args(Some("main"), None), &state, &repo.runner(), files(20)).expect("diff");
        let text = &out.text;
        let rpc = text
            .split("#### RPC `UserService.GetPublicUser` removed")
            .nth(1)
            .expect("rpc impact")
            .split("\n### ")
            .next()
            .unwrap_or_default();
        assert!(
            rpc.contains(
                "- `getPublicUser` in `gateway/user.query-controller.ts:4` _(caller, match: exact)_"
            ),
            "{text}"
        );
        assert!(!rpc.contains("base-user-grpc.controller.ts"), "{text}");
        assert!(
            rpc.contains(
                "- `SystemSeedService` in `gateway/seed.service.ts:4` _(holds a `UserService` client; no call to `GetPublicUser` found in it or its direct subclasses, match: heuristic)_"
            ),
            "{text}"
        );
        assert!(!rpc.contains("health.service.ts"), "{text}");
        assert!(!rpc.contains("may call"), "{text}");
        assert!(
            rpc.contains("in `users/user.grpc-controller.ts:")
                && rpc.contains("_(orphaned handler: implements a removed RPC, match: exact)_"),
            "{text}"
        );
        // Only the removed RPC's handler, not its neighbour's.
        assert_eq!(rpc.matches("orphaned handler").count(), 1, "{text}");
        assert!(
            text.contains("Impacted call sites / dependents: 3."),
            "{text}"
        );
    }

    /// A branch adding a call through the inherited field: the per-file RPC
    /// call diff counts it, under its resolved, declared name.
    #[test]
    fn an_added_inherited_call_is_counted_with_its_resolved_name() {
        let repo = nest_repo();
        repo.write("gateway/user.admin-controller.ts", NEST_ADMIN);
        let state = indexed_state(&repo.root);
        let out =
            run_with(&args(Some("main"), None), &state, &repo.runner(), files(20)).expect("diff");
        let text = &out.text;
        assert!(text.contains("RPC calls +1 -0"), "{text}");
        assert!(
            text.contains("- + calls RPC `UserService.GetUser` (L4) in `getUser`"),
            "{text}"
        );
    }

    /// A call through an inherited field no class binds, on a client-like
    /// field name: counted, under the service the name gives, marked as a
    /// guess with the raw record.
    #[test]
    fn an_inherited_call_without_binding_is_marked_inferred() {
        let repo = nest_repo();
        repo.write(
            "gateway/pay.controller.ts",
            "import { Base } from './base';\n\nexport class PayController extends Base {\n  async run() {\n    return this.paymentClient.charge({});\n  }\n}\n",
        );
        let state = indexed_state(&repo.root);
        let out =
            run_with(&args(Some("main"), None), &state, &repo.runner(), files(20)).expect("diff");
        let text = &out.text;
        assert!(text.contains("RPC calls +1 -0"), "{text}");
        assert!(
            text.contains(
                "+ calls RPC `PaymentService.charge` (through `Base#paymentClient.charge`: service inferred from the field name, no single client binding found)"
            ),
            "{text}"
        );
    }

    /// A field two homonym base classes bind to different services: still a
    /// client, listed raw, marked unresolved.
    #[test]
    fn an_ambiguously_bound_inherited_call_is_listed_raw() {
        let repo = nest_repo();
        let base = |service: &str| {
            format!(
                "import type {{ ClientGrpc }} from '@nestjs/microservices';\n\nexport abstract class Base {{\n  protected transport!: {service}Client;\n\n  constructor(protected client: ClientGrpc) {{}}\n\n  onModuleInit() {{\n    this.transport = this.client.getService<{service}Client>('{service}');\n  }}\n}}\n"
            )
        };
        repo.write("gateway/a/base.ts", &base("AlphaService"));
        repo.write("gateway/b/base.ts", &base("BetaService"));
        repo.commit("bases");
        repo.write(
            "gateway/odd.controller.ts",
            "import { Base } from './a/base';\n\nexport class OddController extends Base {\n  async run() {\n    return this.transport.send({});\n  }\n}\n",
        );
        let state = indexed_state(&repo.root);
        let out =
            run_with(&args(Some("main"), None), &state, &repo.runner(), files(20)).expect("diff");
        let text = &out.text;
        assert!(
            text.contains("+ calls `Base#transport.send` (unresolved: the base class binds this field to more than one service)"),
            "{text}"
        );
    }

    /// `this.logger.log()` in a subclass of a base controller is not an RPC
    /// call: the file stays body-only.
    #[test]
    fn a_non_client_inherited_field_call_is_not_an_rpc_call() {
        let repo = nest_repo();
        repo.write(
            "gateway/user.query-controller.ts",
            &NEST_QUERY.replace(
                "    return this.userClient.getPublicUser({ id });",
                "    this.logger.log(id);\n    return this.userClient.getPublicUser({ id });",
            ),
        );
        let state = indexed_state(&repo.root);
        let out =
            run_with(&args(Some("main"), None), &state, &repo.runner(), files(20)).expect("diff");
        let text = &out.text;
        assert!(text.contains("RPC calls +0 -0"), "{text}");
        assert!(
            text.contains("1 changed source file(s) — 0 with contract changes, 1 body-only"),
            "{text}"
        );
        assert!(!text.contains("logger.log"), "{text}");
    }

    /// `AdminService` also declares and serves `GetPublicUser`; removing
    /// `UserService.GetPublicUser` lists neither its handler nor its callers.
    #[test]
    fn a_homonym_rpc_of_another_service_is_not_listed() {
        let repo = nest_repo();
        repo.write(
            "proto/admin.proto",
            "syntax = \"proto3\";\npackage demo.v1;\n\nservice AdminService {\n  rpc GetPublicUser(AdminQuery) returns (AdminUser);\n}\n\nmessage AdminQuery {\n  string id = 1;\n}\n\nmessage AdminUser {\n  string id = 1;\n}\n",
        );
        repo.write(
            "admin/admin.grpc-controller.ts",
            "import { Controller } from '@nestjs/common';\nimport { GrpcMethod } from '@nestjs/microservices';\n\n@Controller()\nexport class AdminGrpcController {\n  @GrpcMethod('AdminService', 'GetPublicUser')\n  async getPublicUser(data: { id: string }) {\n    return { id: data.id };\n  }\n}\n",
        );
        repo.write(
            "gateway/admin.client.ts",
            "import type { ClientGrpc } from '@nestjs/microservices';\n\nexport class AdminGateway {\n  private adminClient!: AdminServiceClient;\n\n  constructor(private readonly client: ClientGrpc) {}\n\n  onModuleInit() {\n    this.adminClient = this.client.getService<AdminServiceClient>('AdminService');\n  }\n\n  async fetch(id: string) {\n    return this.adminClient.getPublicUser({ id });\n  }\n}\n",
        );
        repo.commit("admin");
        repo.write(
            "proto/user.proto",
            &NEST_PROTO.replace(
                "  rpc GetPublicUser(GetPublicUserQuery) returns (PublicUser);\n",
                "",
            ),
        );
        let state = indexed_state(&repo.root);
        let out =
            run_with(&args(Some("main"), None), &state, &repo.runner(), files(20)).expect("diff");
        let text = &out.text;
        let rpc = text
            .split("#### RPC `UserService.GetPublicUser` removed")
            .nth(1)
            .expect("rpc impact")
            .split("\n### ")
            .next()
            .unwrap_or_default();
        assert!(!rpc.contains("admin/admin.grpc-controller.ts"), "{text}");
        assert!(!rpc.contains("gateway/admin.client.ts"), "{text}");
        // The UserService side is still reported.
        assert!(rpc.contains("gateway/user.query-controller.ts:4"), "{text}");
        assert_eq!(rpc.matches("orphaned handler").count(), 1, "{text}");
    }

    /// A renumbered field: the wire result names both numbers, the `.proto` is
    /// a contract change (never "body edits only"), and the summary always
    /// ends with a count of impacted sites.
    #[test]
    fn a_renumbered_field_is_named_and_never_body_only() {
        let repo = nest_repo();
        repo.write(
            "proto/user.proto",
            &NEST_PROTO.replace("string pagination = 2;", "string pagination = 5;"),
        );
        let state = indexed_state(&repo.root);
        let out =
            run_with(&args(Some("main"), None), &state, &repo.runner(), files(20)).expect("diff");
        let text = &out.text;
        assert!(
            text.contains("field `pagination` (`string`) renumbered 2 -> 5"),
            "{text}"
        );
        assert!(
            text.contains("1 changed source file(s) — 1 with contract changes, 0 body-only"),
            "{text}"
        );
        assert!(!text.contains("body edits only"), "{text}");
        assert!(
            text.contains(
                "#### `proto/user.proto` (modified)\n- wire format: 1 `WIRE_FORMAT_BREAKING_CHANGE`"
            ),
            "{text}"
        );
        assert!(
            text.contains("Impacted call sites / dependents: 0."),
            "{text}"
        );
        assert!(!text.contains("dependents: \n"), "{text}");
    }
}
