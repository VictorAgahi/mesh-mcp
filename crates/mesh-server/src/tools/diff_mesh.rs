//! `diff_mesh`: what the working tree changes in the mesh compared with a Git
//! base, at the contract level (declarations, RPCs, routes, topics, imports),
//! and what the live graph says depends on what was removed or changed.

use crate::git::{self, BaseChoice, GitError, GitRunner};
use crate::indexer::{ContentExtractor, WorkspaceIndexer};
use crate::protocol::RequestMeta;
use crate::tools::analyze_grpc::{check_wire_format, render_report};
use crate::tools::find_dependents::is_indexable_source;
use crate::tools::smart_search::SearchScope;
use crate::tools::{McpTool, ToolError, ToolOutput};
use mesh_core::{
    AppState, CompactStr, ContractGraph, ContractNode, DependentsMatch, EdgeConfidence,
    ExcludeMatcher, FilesystemCrawler, NodeKind, RepoId, ValidatedScope,
};
use mesh_parsers::{AstGuard, FileIndex};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ffi::OsStr;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

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
    const DESCRIPTION: &'static str = "Contract-level diff of the working tree (uncommitted edits and untracked files included) against a Git base, per configured root: declarations added, removed or with a changed signature, gRPC services and RPCs, HTTP routes, event producers and consumers per topic, imports and RPC calls; then, from the live graph, the callers and dependents of every removed or changed symbol and RPC (path:line, heuristic or ambiguous rows marked) and the .proto wire-format check of every changed .proto. This is the tool for 'what does my branch change in the mesh / what will this change break'. Body-only edits are counted, never listed. DO NOT USE to trace one known symbol, RPC or topic (use find_dependents, analyze_grpc or analyze_impact). DO NOT pass a file path or a `--option` as `base`.";
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
        run_with(args, state, &GitRunner::system(), MAX_CHANGED_FILES)
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

const SHALLOW_HINT: &str = " This is a shallow clone: the base may not have been fetched (`git fetch --unshallow`, or `git fetch origin <branch>`), or pass a `base` inside the fetched history.";

/// Lists the changed paths of one target: `git diff --name-only` of the base
/// against the working tree, plus untracked files, NUL-separated.
fn list_changes(
    git: &GitRunner,
    program: &Path,
    target: &Target,
    base: Option<&str>,
) -> Result<(BaseChoice, String, String, String, bool, Vec<String>), RootFailure> {
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
    Ok((
        choice,
        sha,
        toplevel,
        prefix,
        shallow,
        paths.into_iter().collect(),
    ))
}

/// `abs` relative to its most specific root, `/`-separated; prefixed with the
/// root's directory name when several roots are configured.
fn display_path(abs: &Path, roots: &[PathBuf]) -> String {
    let Some((_, root)) = WorkspaceIndexer::most_specific_root(abs, roots) else {
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
    if roots.len() > 1 {
        let name = root
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        format!("{name}/{rel}")
    } else {
        rel
    }
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
            sides.insert(i, Side::Skipped(format!("a {kind} at the base, not a file")));
            continue;
        }
        let size: u64 = size.parse().unwrap_or(u64::MAX);
        let limit = AstGuard::size_budget(&c.abs);
        if size > limit {
            sides.insert(
                i,
                Side::Skipped(format!("oversized at the base ({size} bytes, budget {limit})")),
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
    Calls(String),
}

impl Fact {
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
        }
    }
}

#[derive(Debug, Clone, Default)]
struct FactVal {
    signatures: BTreeSet<String>,
    line: usize,
    /// The declaration a relation is attached to (a producer's method).
    owner: Option<String>,
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
fn add_fact(
    facts: &mut BTreeMap<Fact, FactVal>,
    fact: Fact,
    line: Option<usize>,
    signature: Option<&str>,
    owner: Option<&str>,
) {
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
}

fn facts_of(index: &FileIndex) -> BTreeMap<Fact, FactVal> {
    let mut facts: BTreeMap<Fact, FactVal> = BTreeMap::new();
    for node in &index.nodes {
        let name = node.name.to_string();
        let fact = match node.kind {
            NodeKind::GrpcService => Fact::Rpc("gRPC service", name),
            NodeKind::GrpcMethod => Fact::Rpc("RPC", name),
            NodeKind::HttpEndpoint => Fact::Route(name),
            kind => match decl_label(kind) {
                Some(label) => Fact::Decl(label, name),
                None => continue,
            },
        };
        add_fact(
            &mut facts,
            fact,
            Some(node.line_start),
            node.signature.as_deref(),
            None,
        );
    }
    let node = |i: usize| index.nodes.get(i);
    let relations = index
        .producers
        .iter()
        .map(|(i, t)| (*i, Fact::Produces(t.to_string())))
        .chain(
            index
                .consumers
                .iter()
                .map(|(i, t)| (*i, Fact::Consumes(t.to_string()))),
        )
        .chain(
            index
                .dependencies
                .iter()
                .map(|(i, t)| (*i, Fact::Import(t.to_string()))),
        )
        .chain(index.rpc_calls.iter().filter_map(|(i, recorded)| {
            let target = recorded
                .strip_prefix(mesh_core::RPC_EXACT_METHOD_PREFIX)
                .or_else(|| recorded.strip_prefix(mesh_core::RPC_INFERRED_METHOD_PREFIX))?;
            target
                .contains('.')
                .then(|| (*i, Fact::Calls(target.to_string())))
        }));
    for (i, fact) in relations {
        let n = node(i);
        // An import is the file's, not the declaration's that carries it.
        let owner = match fact {
            Fact::Import(_) => None,
            _ => n.map(|n| n.name.as_str()),
        };
        add_fact(&mut facts, fact, n.map(|n| n.line_start), None, owner);
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
            Some(c) if fact.has_signature() && c.signatures != b.signatures => {
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
    max_files: usize,
) -> Result<ToolOutput, ToolError> {
    let base = args.base.as_deref();
    if let Some(b) = base {
        git::validate_base(b).map_err(|e| (-32602, e.to_string()))?;
    }
    let targets = select_targets(args.scope.as_deref(), state)?;
    if targets.is_empty() {
        return Err((
            -32602,
            "`scope` selects no configured root".to_string(),
        ));
    }
    let program = git.locate().map_err(|e| (-32603, e.to_string()))?;
    let roots = &state.allowed_roots;

    // 1. Changed paths, per target.
    let mut diffs: Vec<RootDiff> = Vec::new();
    let mut failures: Vec<RootFailure> = Vec::new();
    let mut changed: Vec<Changed> = Vec::new();
    let mut seen: HashSet<PathBuf> = HashSet::new();
    let mut counters = Counters {
        cap: max_files,
        ..Counters::default()
    };
    let mut matchers: HashMap<RepoId, ExcludeMatcher> = HashMap::new();
    for target in &targets {
        let (choice, sha, toplevel, prefix, shallow, paths) =
            match list_changes(git, &program, target, base) {
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
                display: display_path(&abs, roots),
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
        return Err((code, format!("diff_mesh: no root could be compared. {}", reasons.join(" "))));
    }
    changed.sort_by(|a, b| a.display.cmp(&b.display));
    if changed.len() > max_files {
        counters.truncated = changed.len() - max_files;
        changed.truncate(max_files);
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
        let Some(&(_, first)) = files.first() else {
            continue;
        };
        let dir = first.abs.parent().map(Path::to_path_buf).unwrap_or_default();
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

    // 3-4. Extract both sides, diff the contract facts.
    let extractor: ContentExtractor =
        WorkspaceIndexer::content_extractor(&state.config, &state.allowed_roots);
    let mut reports: Vec<FileReport> = Vec::new();
    let mut files_accessed: Vec<String> = Vec::new();
    for (i, c) in changed.iter().enumerate() {
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
                    Ok(index) => facts_of(&index),
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
    let snapshot = state.snapshot();
    let impact = render_impact(&reports, &snapshot.contract_graph, roots);
    let mut wire = String::new();
    let mut wire_breaks = 0usize;
    for r in reports.iter().filter(|r| is_proto(&r.abs)) {
        if r.status == FileStatus::Removed {
            let _ = writeln!(
                wire,
                "\n### Wire-format check: `{}`\n- **Result**: file removed — every message and RPC it declared is gone (see the RPC removals above)",
                r.display
            );
            continue;
        }
        match check_wire_format(&r.abs, base, git) {
            Ok(report) => {
                wire_breaks += report.breaking_count();
                wire.push_str(&render_report(&report));
            }
            Err(e) => {
                let _ = writeln!(
                    wire,
                    "\n### Wire-format check: `{}`\n*Skipped: {e}*",
                    r.display
                );
            }
        }
    }

    // 6. Render.
    let text = render(
        &diffs,
        &failures,
        &reports,
        &counters,
        &impact,
        &wire,
        wire_breaks,
        roots,
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

// ---------------------------------------------------------------------------
// Impact
// ---------------------------------------------------------------------------

struct Impact {
    text: String,
    sites: usize,
}

fn root_relative(node: &ContractNode, roots: &[PathBuf]) -> String {
    format!("{}:{}", display_path(&node.file_path, roots), node.line_start)
}

fn push_rows(
    out: &mut String,
    rows: &[(&ContractNode, String)],
    roots: &[PathBuf],
    sites: &mut usize,
) {
    for (node, how) in rows.iter().take(MAX_IMPACT_ROWS) {
        let _ = writeln!(
            out,
            "- `{}` in `{}` _({how})_",
            node.name,
            root_relative(node, roots)
        );
    }
    if rows.len() > MAX_IMPACT_ROWS {
        let _ = writeln!(out, "- … and {} more", rows.len() - MAX_IMPACT_ROWS);
    }
    *sites += rows.len();
}

fn render_impact(reports: &[FileReport], graph: &ContractGraph, roots: &[PathBuf]) -> Impact {
    let mut out = String::new();
    let mut sites = 0usize;
    let mut items = 0usize;
    let mut skipped_items = 0usize;
    for r in reports {
        for ch in &r.changes {
            if ch.kind == ChangeKind::Added {
                continue;
            }
            let what = if ch.kind == ChangeKind::Removed {
                "removed"
            } else {
                "signature changed"
            };
            let looked_up = match &ch.fact {
                Fact::Rpc("RPC", name) => Some(rpc_impact(graph, name, &r.abs)),
                Fact::Decl(_, name)
                    if name.chars().count() >= 3
                        && !GENERIC_NAMES.contains(&name.to_ascii_lowercase().as_str()) =>
                {
                    Some(symbol_impact(graph, name, &r.abs))
                }
                Fact::Produces(topic) if ch.kind == ChangeKind::Removed => {
                    Some(topic_impact(graph, topic, true, &r.abs))
                }
                Fact::Consumes(topic) if ch.kind == ChangeKind::Removed => {
                    Some(topic_impact(graph, topic, false, &r.abs))
                }
                _ => None,
            };
            let Some((rows, empty_note)) = looked_up else {
                continue;
            };
            if items >= MAX_IMPACT_ITEMS {
                skipped_items += 1;
                continue;
            }
            items += 1;
            let _ = writeln!(
                out,
                "\n#### {} {what} — `{}`",
                ch.fact.describe(),
                r.display
            );
            if rows.is_empty() {
                let _ = writeln!(out, "- {empty_note}");
            } else {
                push_rows(&mut out, &rows, roots, &mut sites);
            }
        }
    }
    if skipped_items > 0 {
        let _ = writeln!(
            out,
            "\n*{skipped_items} more removed or changed element(s) not looked up (limit {MAX_IMPACT_ITEMS}); narrow `scope`.*"
        );
    }
    Impact { text: out, sites }
}

type Rows<'g> = (Vec<(&'g ContractNode, String)>, &'static str);

fn rpc_impact<'g>(graph: &'g ContractGraph, name: &str, file: &Path) -> Rows<'g> {
    let Some((service, method)) = name.rsplit_once('.') else {
        return (Vec::new(), "not a `Service.Method` name: callers not looked up");
    };
    let mut rows: Vec<(&ContractNode, String)> = Vec::new();
    let mut seen: HashSet<mesh_core::NodeId> = HashSet::new();
    for (node, confidence) in graph.recorded_rpc_callers(service, method) {
        if seen.insert(node.id) {
            rows.push((node, format!("caller, match: {}", confidence.label())));
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
                rows.push((node, format!("server handler, match: {}", confidence.label())));
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

fn topic_impact<'g>(graph: &'g ContractGraph, topic: &str, producer: bool, file: &Path) -> Rows<'g> {
    let flow = graph.analyze_impact(topic);
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

#[derive(Default)]
struct Tally {
    decl: [usize; 3],
    rpc: [usize; 3],
    route: [usize; 3],
    produces: [usize; 2],
    consumes: [usize; 2],
    imports: [usize; 2],
    calls: [usize; 2],
}

fn tally(reports: &[FileReport]) -> Tally {
    let mut t = Tally::default();
    for ch in reports.iter().flat_map(|r| &r.changes) {
        let i = match ch.kind {
            ChangeKind::Added => 0,
            ChangeKind::Removed => 1,
            ChangeKind::Changed => 2,
        };
        let two = i.min(1);
        match &ch.fact {
            Fact::Decl(..) => t.decl[i] += 1,
            Fact::Rpc(..) => t.rpc[i] += 1,
            Fact::Route(_) => t.route[i] += 1,
            Fact::Produces(_) => t.produces[two] += 1,
            Fact::Consumes(_) => t.consumes[two] += 1,
            Fact::Import(_) => t.imports[two] += 1,
            Fact::Calls(_) => t.calls[two] += 1,
        }
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

#[allow(clippy::too_many_arguments)]
fn render(
    diffs: &[RootDiff],
    failures: &[RootFailure],
    reports: &[FileReport],
    counters: &Counters,
    impact: &Impact,
    wire: &str,
    wire_breaks: usize,
    roots: &[PathBuf],
) -> String {
    let t = tally(reports);
    let with_changes = reports.iter().filter(|r| !r.changes.is_empty()).count();
    let skipped = reports.iter().filter(|r| r.skipped.is_some()).count();
    let body_only = reports.len() - with_changes - skipped;
    let mut out = String::from("## Mesh diff: working tree against the Git base\n\n");
    for d in diffs {
        let _ = writeln!(
            out,
            "- **Root** `{}`: base {}",
            display_root(&d.root, roots),
            describe_base(d)
        );
    }
    let _ = writeln!(
        out,
        "\n**Summary**: {} changed source file(s) — {with_changes} with contract changes, {body_only} body-only, {skipped} skipped. Declarations +{} -{} ~{}; RPCs/services +{} -{} ~{}; routes +{} -{} ~{}; producers +{} -{}; consumers +{} -{}; imports +{} -{}; RPC calls +{} -{}. Wire-format breaking changes: {wire_breaks}. Impacted call sites / dependents: {}.",
        reports.len(),
        t.decl[0], t.decl[1], t.decl[2],
        t.rpc[0], t.rpc[1], t.rpc[2],
        t.route[0], t.route[1], t.route[2],
        t.produces[0], t.produces[1],
        t.consumes[0], t.consumes[1],
        t.imports[0], t.imports[1],
        t.calls[0], t.calls[1],
        impact.sites,
    );
    if counters.excluded + counters.non_source > 0 {
        let _ = writeln!(
            out,
            "\n*Not analysed: {} file(s) excluded by `exclude_patterns`/ignore files, {} non-source file(s).*",
            counters.excluded, counters.non_source
        );
    }
    if counters.truncated > 0 {
        let _ = writeln!(
            out,
            "\n> [!NOTE]\n> {} more changed file(s) were not analysed: at most {} files per call. Narrow `scope` to one root or one sub-directory to cover them.",
            counters.truncated, counters.cap
        );
    }
    for f in failures {
        let _ = writeln!(
            out,
            "\n**Root not compared** `{}`: {}",
            display_root(&f.root, roots),
            f.message
        );
    }

    // Impact and wire sections are rendered whole (they are what the diff is
    // for); per-file sections fill what is left of the budget.
    let mut tail = String::new();
    if !impact.text.is_empty() {
        tail.push_str("\n### Impact (live graph)\n");
        tail.push_str(&impact.text);
    }
    tail.push_str(wire);
    let budget = OUTPUT_BUDGET.saturating_sub(out.len() + tail.len() + 512);

    let mut files = String::new();
    let mut unlisted = 0usize;
    let listed: Vec<&FileReport> = reports
        .iter()
        .filter(|r| !r.changes.is_empty() || r.skipped.is_some())
        .collect();
    if !listed.is_empty() {
        files.push_str("\n### Contract changes per file\n");
    }
    for r in &listed {
        let mut section = format!("\n#### `{}` ({})\n", r.display, r.status.label());
        if let Some(why) = &r.skipped {
            let _ = writeln!(section, "- skipped: {why}");
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
        if files.len() + section.len() > budget {
            unlisted += 1;
            continue;
        }
        files.push_str(&section);
    }
    if unlisted > 0 {
        let _ = writeln!(
            files,
            "\n*{unlisted} more file section(s) left out to stay under the 48 KB cap; narrow `scope` to list them.*"
        );
    }
    if body_only > 0 {
        let names: Vec<String> = reports
            .iter()
            .filter(|r| r.changes.is_empty() && r.skipped.is_none())
            .take(20)
            .map(|r| format!("`{}`", r.display))
            .collect();
        let _ = writeln!(
            files,
            "\n*No contract-level change (body edits only): {}{}*",
            names.join(", "),
            if body_only > 20 {
                format!(" and {} more", body_only - 20)
            } else {
                String::new()
            }
        );
    }
    out.push_str(&files);
    out.push_str(&tail);
    out
}

fn display_root(root: &Path, roots: &[PathBuf]) -> String {
    if roots.len() > 1 {
        root.file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| root.display().to_string())
    } else {
        root.display().to_string()
    }
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
        let config = mesh_core::Config::load_from_str(
            "[workspace]\nname = \"t\"\nversion = \"0\"\nroots = [\".\"]\n",
        )
        .expect("config");
        let audit = std::sync::Arc::new(mesh_core::AuditLogger::new_in_memory().expect("audit"));
        let rescan = std::sync::Arc::new(mesh_core::BackgroundRescanEngine::new().expect("rescan"));
        AppState::new(config, vec![root.to_path_buf()], audit, rescan)
    }

    /// Indexes the working tree into the live snapshot, as the server does.
    fn indexed_state(root: &Path) -> AppState {
        let state = state_for(root);
        let snapshot = WorkspaceIndexer::build_snapshot(
            &state.config,
            &state.allowed_roots,
            None,
            None,
            None,
        );
        state.install_snapshot(snapshot);
        state
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
            &PROTO.replace(
                "  rpc DeleteUser(DeleteUserRequest) returns (Empty);\n",
                "",
            ),
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
            &TS_CONSUMER.replace("async start() {", "async start() {\n    const ready = true;"),
        );
        repo.write("README.md", "# demo, edited\n");
        repo
    }

    #[test]
    fn reports_contract_changes_impact_and_the_wire_break() {
        let repo = changed_repo();
        let state = indexed_state(&repo.root);
        let out = run_with(&args(None, None), &state, &repo.runner(), MAX_CHANGED_FILES)
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
            text.contains("+ produces topic `user-archived` (L6) in `archived`"),
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
        let impact = text.split("### Impact (live graph)").nth(1).expect("impact");
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
        assert!(text.contains("### Wire-format check: `proto/user.proto`"), "{text}");
        assert!(text.contains("`WIRE_FORMAT_BREAKING_CHANGE`"), "{text}");
        assert!(!text.contains("Wire-format breaking changes: 0"), "{text}");
        assert!(text.len() < 48 * 1024);
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
            MAX_CHANGED_FILES,
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
            MAX_CHANGED_FILES,
        )
        .err()
        .expect("jail");
        assert_eq!(err.0, -32602, "{}", err.1);
    }

    #[test]
    fn file_cap_says_how_many_files_were_left_out() {
        let repo = changed_repo();
        let state = indexed_state(&repo.root);
        let out = run_with(&args(Some("main"), None), &state, &repo.runner(), 2)
            .expect("capped diff");
        assert!(
            out.text.contains("more changed file(s) were not analysed: at most 2 files per call"),
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
            let err = run_with(&args(Some(bad), None), &state, &repo.runner(), 10)
                .err()
                .expect("refused");
            assert_eq!(err.0, -32602, "{bad:?}: {}", err.1);
        }
        let err = run_with(&args(Some("no-such-branch"), None), &state, &repo.runner(), 10)
            .err()
            .expect("unknown base");
        assert_eq!(err.0, -32602, "{}", err.1);
        assert!(err.1.contains("`no-such-branch` does not name a commit"), "{}", err.1);
        assert!(err.1.contains(&repo.root.display().to_string()), "{}", err.1);
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
        env.push(("GIT_CEILING_DIRECTORIES".into(), base.clone().into_os_string()));
        let git = GitRunner {
            path: std::env::var_os("PATH"),
            env,
            timeout: GIT_COMMAND_TIMEOUT,
        };
        let err = run_with(&args(None, None), &state, &git, 10)
            .err()
            .expect("not a repo");
        assert_eq!(err.0, -32603, "{}", err.1);
        assert!(err.1.contains(&root.display().to_string()), "{}", err.1);
        assert!(err.1.contains("not inside a usable Git repository"), "{}", err.1);
    }

    #[test]
    fn detached_head_and_renames_are_compared() {
        let repo = base_repo();
        let head = repo.git(&["rev-parse", "HEAD"]);
        repo.git(&["checkout", "-q", "--detach", &head]);
        repo.git(&["mv", "lib/format.ts", "lib/fmt.ts"]);
        let state = indexed_state(&repo.root);
        let out = run_with(&args(Some("main"), None), &state, &repo.runner(), 10)
            .expect("detached diff");
        // A rename is a removal plus an addition.
        assert!(out.text.contains("#### `lib/format.ts` (removed)"), "{}", out.text);
        assert!(out.text.contains("#### `lib/fmt.ts` (added)"), "{}", out.text);
        assert!(out.text.contains("- - symbol `formatUser`"), "{}", out.text);
        assert!(out.text.contains("- + symbol `formatUser`"), "{}", out.text);
    }
}
