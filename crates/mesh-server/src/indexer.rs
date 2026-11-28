//! Workspace indexing shared by `mesh-mcp run`, `mesh-mcp graph`, `meshd` and the
//! file watcher. Previously each of those carried its own copy of the scan loop.
//!
//! Extraction (read + guard + tree-sitter) runs in parallel on the QoS-throttled
//! Rayon pool and produces graph-independent [`FileIndex`] fragments; only the
//! final fold into the `ContractGraph` and the single `reconcile_edges` pass are
//! sequential.

use mesh_core::config_values;
use mesh_core::{
    expand_roots, sha256, AppState, BackgroundRescanEngine, CacheEntry, Config, ContractGraph,
    DifferentialVfs, DocIndex, DocSection, ExcludeMatcher, FilesystemCrawler, IndexHealth,
    MeshSnapshot, PersistentIndexCache, PropertyRegistry, PropertySourceMatcher, ReconcileMode,
    RejectReason, RepoId, ValidatedScope,
};
use mesh_parsers::{
    root_relative, AstGuard, CompiledPattern, ExtractConfig, FileIndex, GuardRejection,
    LanguageKind, PathContext, PolyglotIndexer, RootRelativeIndex, TsAliasIndex,
};
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

/// Crawl depth used for every full workspace scan.
pub const SCAN_DEPTH: usize = 10;

/// A file the guards refused for a reason only its bytes decide, with the
/// `(mtime_nanos, size)` it had before it was read.
type StableReject = (PathBuf, (u128, u64));

/// Why `process_file` never produced a fragment for a file — a deliberate,
/// counted policy rejection (see `IndexHealth`), distinct from
/// `FileIndex::parse_failed`, which means the file passed every one of these
/// and still failed inside the parser.
#[derive(Debug, Clone, Copy)]
enum RejectKind {
    /// Over the 384KB/1.5MB size budget — rejected before the file was even read.
    Oversized,
    /// Failed the binary sniff (a null byte in the first 4KB).
    Binary,
    /// A line over the 1024-byte cap; carries the longest line.
    LongLine(usize),
    /// Syntactic nesting over the cap; carries the deepest level.
    DeepNesting(usize),
    /// Could not be read (permissions, vanished mid-scan) or was not valid UTF-8.
    ReadError,
}

/// Guards and extracts in-memory contents the way `process_file` does a file it
/// read from disk: the full lexical guard for tree-sitter inputs, the size budget
/// and binary sniff for everything else, UTF-8, then `extract_with_config` plus
/// the custom patterns. No persistent cache, no docs or properties: contract
/// facts only.
pub(crate) struct ContentExtractor {
    extract_cfg: ExtractConfig,
    patterns: Vec<CompiledPattern>,
}

impl ContentExtractor {
    /// The extracted facts, or why the guards refused the contents.
    pub(crate) fn extract(
        &self,
        path: &Path,
        bytes: &[u8],
        repo_id: RepoId,
    ) -> Result<FileIndex, String> {
        let lang = LanguageKind::from_path(&path.to_string_lossy());
        let budget = AstGuard::size_budget(path);
        let verdict = if mesh_parsers::languages::sql::is_sql_path(path) {
            // A lexer, not a parse: size and binary sniff only (no line-length or nesting rule).
            mesh_parsers::languages::sql::check_content(bytes.len() as u64, bytes)
        } else if lang.language().is_some() {
            AstGuard::check_content(path, bytes)
        } else if bytes.len() as u64 > budget {
            Err(GuardRejection::Oversized {
                size: bytes.len() as u64,
                budget,
            })
        } else if AstGuard::looks_binary(bytes) {
            Err(GuardRejection::Binary)
        } else {
            Ok(())
        };
        verdict.map_err(|r| match r {
            GuardRejection::Oversized { size, budget } => {
                format!("oversized ({size} bytes, budget {budget})")
            }
            GuardRejection::Binary => "binary (a null byte in the first 4 KB)".to_string(),
            GuardRejection::LongLine { len } => format!("a {len}-byte line (limit 1024)"),
            GuardRejection::DeepNesting { depth } => format!("nesting depth {depth} (limit 64)"),
        })?;
        let content = std::str::from_utf8(bytes).map_err(|_| "not valid UTF-8".to_string())?;
        // On an agent's synchronous request path: the query parse budget, never
        // the indexing one (CLAUDE.md commandment 2).
        let mut code = PolyglotIndexer::extract_with_budget(
            path,
            content,
            repo_id,
            &self.extract_cfg,
            AstGuard::QUERY_PARSE_TIMEOUT_MICROS,
        );
        if !self.patterns.is_empty() {
            code.merge(PolyglotIndexer::extract_custom_patterns(
                path,
                content,
                repo_id,
                &self.patterns,
            ));
        }
        Ok(code)
    }
}

/// Output of processing one file on the pool.
struct FileFragment {
    path: PathBuf,
    signature: mesh_core::vfs::FileSignature,
    code: FileIndex,
    docs: Vec<DocSection>,
    props: Option<PropertyRegistry>,
    /// What this file says about `${key}` topics (ticket T6): taken into the snapshot's
    /// `config_values` before any source file is folded.
    config: Option<mesh_core::ConfigFile>,
    /// Set when `code` came from a fresh tree-sitter parse (a persistent-cache miss) rather
    /// than a cache hit — the (key, serialized record) pairs `run_scan_pass` should write
    /// back once the whole parallel pass is done (two when the record depends on the
    /// inferred package, see `CachedRecord`). Empty on a cache hit (nothing changed to
    /// write) or when no cache is configured.
    cache_write: Vec<CacheEntry>,
}

/// What the shared parse cache stores under a key (8.0.0).
///
/// A file's first key leaves out the package inferred from its directories
/// (`PathContext::package_of`): most records do not depend on it (every node declares its
/// package, or the file has none), and in a manifest-less project that value is the
/// checkout's own directory name, which would make every such key differ between
/// worktrees. A record that does use it (a node whose package equals the inferred one,
/// coincidences included) is stored under a second key that also folds the inferred
/// package, and the first key holds `NeedsPackage` to send the lookup there.
#[derive(serde::Deserialize)]
enum CachedRecord {
    Index(Box<RootRelativeIndex>),
    NeedsPackage,
}

/// [`CachedRecord`]'s serialized form, written from a borrow (same variant names, same
/// JSON).
#[derive(serde::Serialize)]
enum CachedRecordRef<'a> {
    Index(&'a RootRelativeIndex),
    NeedsPackage,
}

/// `[engines.contracts.spring]` settings resolved once per scan, mirroring how
/// `doc_index_for`/`compiled_patterns` resolve their own engine's config once instead of per
/// file. Absent `[engines.contracts.spring]` keeps the pre-existing unscoped, redacting,
/// non-resolving behaviour.
#[derive(Clone)]
struct SpringSettings {
    /// `None` means "no scoping" (match every `.properties`/`.yml`/`.yaml` file), which is both
    /// the default and the behaviour when `property_files` is left empty.
    file_matcher: Option<PropertySourceMatcher>,
    resolve_placeholders: bool,
    auto_redact_secrets: bool,
}

impl SpringSettings {
    fn from_config(config: &Config) -> Self {
        match config
            .engines
            .contracts
            .as_ref()
            .and_then(|c| c.spring.as_ref())
        {
            Some(spring) => Self {
                file_matcher: if spring.property_files.is_empty() {
                    None
                } else {
                    Some(PropertySourceMatcher::compile(&spring.property_files))
                },
                resolve_placeholders: spring.resolve_placeholders,
                auto_redact_secrets: spring.auto_redact_secrets,
            },
            None => Self {
                file_matcher: None,
                resolve_placeholders: true,
                auto_redact_secrets: true,
            },
        }
    }

    #[inline]
    fn is_property_source(&self, path: &Path) -> bool {
        match &self.file_matcher {
            Some(matcher) => matcher.is_match(path),
            None => true,
        }
    }
}

/// Per-scan snapshot of which engines are switched on, plus the compiled
/// `[engines.docs] paths` allowlist. Built once per scan from `Config` so
/// `enabled = false` and `paths` actually change what gets indexed instead of
/// being accepted-but-ignored config keys.
struct EngineToggles {
    docs_enabled: bool,
    contracts_enabled: bool,
    /// `None` means `[engines.docs] paths` is empty: no restriction, every `.md`
    /// file under the crawled roots is indexed (the historical behaviour).
    doc_paths: Option<ExcludeMatcher>,
}

/// Everything `process_file` needs beyond the file's own path/repo_id/root,
/// resolved once per scan and bundled to keep the function's argument count
/// down (clippy::too_many_arguments).
struct ScanConfig<'a> {
    patterns: &'a [CompiledPattern],
    doc_template: &'a DocIndex,
    spring: &'a SpringSettings,
    extract_cfg: &'a ExtractConfig,
    toggles: &'a EngineToggles,
    /// Persistent content-hash cache for tree-sitter `FileIndex` fragments (P2 step 3.2).
    /// `None` disables it — every file is parsed fresh, exactly the pre-3.2 behaviour.
    cache: Option<&'a PersistentIndexCache>,
    /// SHA-256 of `extract_cfg`'s `Debug` output, computed once per scan by
    /// `WorkspaceIndexer::config_fingerprint` and folded into every cache key so a config
    /// change (e.g. `proto_dirs`, `infer_string_topics`) invalidates stale entries instead of
    /// serving extraction results computed under different rules.
    config_fingerprint: [u8; 32],
    /// Per-scan memo of what extraction reads from a file's absolute path beyond its
    /// root-relative one (`PathContext::describe`), folded into every cache key.
    path_context: &'a PathContext,
}

/// Files one scan pass parses before it writes their cache records (see `run_scan_pass`).
const SCAN_CHUNK_FILES: usize = 2_048;

/// Label of the pass that restores the call sites of files that came onto a call path.
const CALLS_REFRESH_LABEL: &str = "Calls refresh";
/// The label of the pass that folds again the files whose `${Enum.MEMBER}` topics a reload moved
/// the string constants of (9.0 T14).
const CONST_REFRESH_LABEL: &str = "Constants refresh";

pub struct WorkspaceIndexer;

impl WorkspaceIndexer {
    // ── Configuration discovery ─────────────────────────────────────────────

    /// The config file `discover_config` would load: the first existing among
    /// `explicit`, `.agents/mesh-mcp.toml` and `mesh-mcp.toml` (relative ones
    /// against the current directory), as an absolute path when it can be made
    /// one, so a later change of cwd cannot point it elsewhere.
    pub fn locate_config(explicit: Option<&Path>) -> Option<PathBuf> {
        let candidates = [
            explicit.map(Path::to_path_buf),
            Some(PathBuf::from(".agents/mesh-mcp.toml")),
            Some(PathBuf::from("mesh-mcp.toml")),
        ];
        candidates
            .into_iter()
            .flatten()
            .find(|p| p.exists())
            .map(|p| std::path::absolute(&p).unwrap_or(p))
    }

    /// The daemon identity of the workspace whose directory is `base_dir`
    /// (from [`Self::discover_config`]) when launched with `explicit` as
    /// `--config`: [`mesh_core::workspace_id`] over the canonical directory and
    /// the config file [`Self::locate_config`] finds. The one function both
    /// `mesh-mcp run` and `meshd` call, so the proxy and the daemon it spawns
    /// can never disagree on the socket.
    pub fn workspace_id_for(base_dir: &Path, explicit: Option<&Path>) -> String {
        mesh_core::workspace_id(base_dir, Self::locate_config(explicit).as_deref())
    }

    /// The full identity of the session or daemon whose directory is `base_dir`, launched with
    /// `explicit` as `--config` (8.0.0). Its [`mesh_core::WorkspaceIdentity::daemon_id`] names
    /// the daemon (one per Git repository); its `legacy_id` equals [`Self::workspace_id_for`].
    pub fn identity_for(base_dir: &Path, explicit: Option<&Path>) -> mesh_core::WorkspaceIdentity {
        mesh_core::WorkspaceIdentity::resolve(base_dir, Self::locate_config(explicit).as_deref())
    }

    /// Loads the config file at `path` the way `discover_config` does (skill
    /// paths and workspace root resolved against its directory) and returns it
    /// with that directory.
    pub fn load_config_at(path: &Path) -> Result<(Config, PathBuf), Box<dyn std::error::Error>> {
        let (cfg, base, _sha) = Self::load_config_stamped(path)?;
        Ok((cfg, base))
    }

    /// [`Self::load_config_at`] that also returns the SHA-256 of the bytes the
    /// `Config` was parsed from. The file is read exactly once, so the hash
    /// always describes the very content the returned config came from (a
    /// separate read for the hash could see a newer edit).
    pub fn load_config_stamped(
        path: &Path,
    ) -> Result<(Config, PathBuf, String), Box<dyn std::error::Error>> {
        // Bounded (1 MiB): a config is a few KiB, and `meshd` loads whatever file a hello names.
        let bytes = mesh_core::config_watch::read_config_bounded(path)
            .map_err(|e| mesh_core::ConfigError::IoError(path.to_path_buf(), e))?;
        let digest = mesh_core::config_watch::config_sha256(&bytes);
        let (cfg, base) = Self::parse_config_bytes(path, &bytes)?;
        Ok((cfg, base, digest))
    }

    /// Parses config `bytes` that were read from `path` (which only anchors
    /// relative paths), exactly like [`Self::load_config_at`] does after its read.
    pub fn parse_config_bytes(
        path: &Path,
        bytes: &[u8],
    ) -> Result<(Config, PathBuf), Box<dyn std::error::Error>> {
        let text = std::str::from_utf8(bytes)
            .map_err(|e| format!("config {} is not valid UTF-8: {e}", path.display()))?;
        let mut cfg = Config::load_from_str(text)?;
        let base = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();
        // Skill paths are written relative to the config file; the server is
        // spawned by an IDE with an arbitrary cwd.
        cfg.resolve_skill_paths(&base);
        // Same reason: relative tool scopes anchor on the workspace root,
        // not on whatever CWD the IDE launched us from.
        cfg.resolve_workspace_root(&base);
        Ok((cfg, base))
    }

    /// The one workspace-root guard (`mesh_core::guard_workspace_root`) for
    /// `run`, the daemon spawn, `meshd` and `doctor`: a config file that was
    /// found pins its roots and is always honoured; without one the workspace
    /// is the current directory, which must not be `/`, a drive root or HOME.
    pub fn guard_workspace(
        explicit: Option<&Path>,
        base_dir: &Path,
    ) -> Result<(), mesh_core::root_guard::DegenerateRootError> {
        Self::guard_workspace_with_home(
            explicit,
            base_dir,
            mesh_core::root_guard::home_dir().as_deref(),
        )
    }

    /// [`Self::guard_workspace`] with the home directory given (tests). A config
    /// pins the roots only if it lists at least one: a stray `mesh-mcp.toml`
    /// with an empty `roots` in HOME falls back to its own directory and is
    /// refused like no config at all.
    pub fn guard_workspace_with_home(
        explicit: Option<&Path>,
        base_dir: &Path,
        home: Option<&Path>,
    ) -> Result<(), mesh_core::root_guard::DegenerateRootError> {
        let pins_roots = Self::locate_config(explicit)
            .and_then(|p| Self::load_config_at(&p).ok())
            .is_some_and(|(cfg, _)| !cfg.workspace.roots.is_empty());
        mesh_core::root_guard::guard_workspace_root(base_dir, pins_roots, home)
    }

    /// Loads the first config found among `explicit`, `.agents/mesh-mcp.toml`,
    /// `mesh-mcp.toml`, falling back to a single-root default. Returns the config
    /// and the directory relative roots are resolved against.
    pub fn discover_config(
        explicit: Option<&Path>,
    ) -> Result<(Config, PathBuf), Box<dyn std::error::Error>> {
        let (cfg, base, _digest) = Self::discover_config_stamped(explicit)?;
        Ok((cfg, base))
    }

    /// [`Self::discover_config`] plus the SHA-256 of the config file's bytes
    /// (`None` for the built-in default), taken from the same read.
    pub fn discover_config_stamped(
        explicit: Option<&Path>,
    ) -> Result<(Config, PathBuf, Option<String>), Box<dyn std::error::Error>> {
        match Self::locate_config(explicit) {
            Some(path) => {
                let (cfg, base, digest) = Self::load_config_stamped(&path)?;
                Ok((cfg, base, Some(digest)))
            }
            None => Ok((
                Self::default_config_at(Path::new("."))?,
                PathBuf::from("."),
                None,
            )),
        }
    }

    /// The built-in single-root config used when no config file is found, anchored at
    /// `base_dir` (its one root is `base_dir` itself). `meshd` builds a view of a directory
    /// without a config file with an absolute `base_dir`, never its own cwd.
    pub fn default_config_at(base_dir: &Path) -> Result<Config, Box<dyn std::error::Error>> {
        let default = concat!(
            "[workspace]\nname = \"default-mesh\"\nversion = \"",
            env!("CARGO_PKG_VERSION"),
            "\"\nroots = [\".\"]\n"
        );
        let mut cfg = Config::load_from_str(default)?;
        cfg.resolve_workspace_root(base_dir);
        Ok(cfg)
    }

    /// Expands configured roots; on failure falls back to `base_dir` with a warning.
    pub fn resolve_roots(config: &Config, base_dir: &Path) -> Vec<PathBuf> {
        match expand_roots(
            &config.workspace.roots,
            base_dir,
            &config.workspace.workspace_root,
        ) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(
                    target: "mesh::config",
                    "Failed to expand roots: {e}. Falling back to base directory."
                );
                vec![dunce::canonicalize(base_dir).unwrap_or_else(|_| base_dir.to_path_buf())]
            }
        }
    }

    /// Display names for each root, indexed by `RepoId`. Two roots that share a
    /// directory name (`…/a/api` and `…/b/api`) get the same bare name, which
    /// silently merges their nodes into one service in `visualize_mesh`'s
    /// aggregation (`Aggregate::build` groups purely by this string, step 4.12d
    /// review). Disambiguated with the parent directory segment, deterministically:
    /// every root sharing a name, not just the later ones, so the same input
    /// always renders the same names regardless of scan order.
    pub fn repo_names(roots: &[PathBuf]) -> Vec<String> {
        let bare: Vec<String> = roots
            .iter()
            .map(|r| {
                r.file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_else(|| r.display().to_string())
            })
            .collect();

        let mut counts: HashMap<&str, usize> = HashMap::new();
        for name in &bare {
            *counts.entry(name.as_str()).or_insert(0) += 1;
        }

        roots
            .iter()
            .zip(bare.iter())
            .map(|(root, name)| {
                if counts.get(name.as_str()).copied().unwrap_or(0) <= 1 {
                    return name.clone();
                }
                match root.parent().and_then(|p| p.file_name()) {
                    Some(parent) => format!("{}/{name}", parent.to_string_lossy()),
                    None => name.clone(),
                }
            })
            .collect()
    }

    // ── Full scan ───────────────────────────────────────────────────────────

    /// Scans every root and builds a fresh snapshot. `pool` selects the Rayon pool:
    /// pass `None` (global pool) for the boot scan — the caller is waiting and the
    /// QoS-background engine runs ~5x slower on Apple Silicon E-cores — and the
    /// throttled engine for reloads behind a live IDE. `vfs`, if given, is seeded with
    /// the signatures of every indexed file so the first incremental reload is cheap.
    pub fn build_snapshot(
        config: &Config,
        roots: &[PathBuf],
        pool: Option<&BackgroundRescanEngine>,
        vfs: Option<&mut DifferentialVfs>,
        cache: Option<&PersistentIndexCache>,
    ) -> MeshSnapshot {
        let (files, symlinks) = Self::crawl_all_counted(config, roots);
        let mut snapshot = Self::build_snapshot_from_files(config, roots, &files, pool, vfs, cache);
        snapshot.health.dir_symlinks_not_followed = symlinks;
        snapshot
    }

    /// [`Self::build_snapshot`] over an explicit `(RepoId, path)` list instead of
    /// a fresh crawl. The determinism tests use it to feed the same files in a
    /// different order and check the snapshot fingerprint does not move.
    pub fn build_snapshot_from_files(
        config: &Config,
        roots: &[PathBuf],
        files: &[(RepoId, PathBuf)],
        pool: Option<&BackgroundRescanEngine>,
        vfs: Option<&mut DifferentialVfs>,
        cache: Option<&PersistentIndexCache>,
    ) -> MeshSnapshot {
        let patterns = Self::compiled_patterns(config);
        let doc_template = Self::doc_index_for(config);
        let spring = SpringSettings::from_config(config);
        let extract_cfg = Self::extract_config(config, roots);
        let toggles = Self::engine_toggles(config);
        let config_fingerprint = Self::config_fingerprint(&extract_cfg);
        let path_context = PathContext::default();
        let scan_cfg = ScanConfig {
            patterns: &patterns,
            doc_template: &doc_template,
            spring: &spring,
            extract_cfg: &extract_cfg,
            toggles: &toggles,
            cache,
            config_fingerprint,
            path_context: &path_context,
        };

        let (fragments, health, stable_rejects) =
            Self::run_scan_pass(files, roots, &scan_cfg, pool);

        let mut snapshot = MeshSnapshot {
            doc_index: Self::doc_index_for(config),
            health,
            roots: roots.into(),
            ..MeshSnapshot::default()
        };
        let mut vfs = vfs;
        if let Some(vfs) = vfs.as_deref_mut() {
            Self::note_stable_rejects(vfs, &stable_rejects);
        }
        let mut fragments = fragments;
        Self::fold_config(&mut fragments, &mut snapshot);
        for frag in fragments {
            if let Some(vfs) = vfs.as_deref_mut() {
                vfs.upsert(&frag.path, frag.signature.clone());
            }
            Self::fold(frag, &mut snapshot);
        }
        if spring.resolve_placeholders {
            snapshot.property_registry.resolve_all_placeholders();
        }
        snapshot.contract_graph.reconcile_edges();
        let call_stats = snapshot.contract_graph.call_stats();
        snapshot.health.set_call_stats(&call_stats);
        snapshot
            .health
            .set_wildcards_capped(snapshot.contract_graph.wildcard_hubs_capped());
        snapshot.health.config_topics_unresolved =
            snapshot.contract_graph.unresolved_placeholder_topics();

        tracing::info!(
            target: "mesh::indexer",
            "Workspace scan complete: {} files, {} contract nodes, {} edges, {} doc sections.",
            files.len(),
            snapshot.contract_graph.node_count(),
            snapshot.contract_graph.edge_count(),
            snapshot.doc_index.section_count()
        );
        if !snapshot.health.is_healthy() {
            tracing::warn!(
                target: "mesh::indexer",
                "Index health: {} file(s) failed to parse, {} file(s) unreadable — see `mesh-mcp doctor`.",
                snapshot.health.files_parse_failed,
                snapshot.health.files_read_error
            );
        }
        snapshot
    }

    /// Convenience for the CLI: graph only, using the global Rayon pool.
    /// Delegates to [`Self::build_snapshot`] — one indexing path for the CLI, the
    /// server and the daemon, instead of a second one (previously duplicated here)
    /// that could silently drift out of sync with it.
    pub fn build_graph(
        config: &Config,
        roots: &[PathBuf],
        cache: Option<&PersistentIndexCache>,
    ) -> (ContractGraph, usize) {
        let snapshot = Self::build_snapshot(config, roots, None, None, cache);
        let file_count = snapshot.health.files_scanned;
        (snapshot.contract_graph, file_count)
    }

    /// SHA-256 of `extract_cfg`'s `Debug` representation — a cheap, deterministic fingerprint
    /// (field order and formatting are fixed by the derive) of every knob that changes what
    /// `PolyglotIndexer::extract_with_config` produces for the same bytes. Computed once per
    /// scan, not per file, and folded into every `PersistentIndexCache` key so editing
    /// `[engines.contracts.*]` invalidates stale cache entries instead of silently serving
    /// extraction results computed under different rules.
    fn config_fingerprint(extract_cfg: &ExtractConfig) -> [u8; 32] {
        sha256(format!("{extract_cfg:?}").as_bytes())
    }

    /// Runs `process_file` over `files` (optionally inside `pool`), returning the
    /// resulting fragments plus this pass's `IndexHealth`. A file whose tree-sitter
    /// parse fails on the (possibly contended) parallel pass gets exactly one retry,
    /// alone on the calling thread: the wall-clock budget that used to make this
    /// outcome depend on scheduling is gone (`AstGuard::INDEX_PARSE_TIMEOUT_MICROS`
    /// is 2s), so a transient failure under load is expected to clear on a retry with
    /// nothing else competing for the CPU (idempotence invariants I1 and I6). A
    /// fragment that still has `code.parse_failed == true` after this returns is a
    /// genuine, counted failure — the caller must not fold it as if the file were
    /// simply empty.
    fn run_scan_pass(
        files: &[(RepoId, PathBuf)],
        roots: &[PathBuf],
        scan_cfg: &ScanConfig,
        pool: Option<&BackgroundRescanEngine>,
    ) -> (Vec<FileFragment>, IndexHealth, Vec<StableReject>) {
        let work = |chunk: &[(RepoId, PathBuf)]| {
            chunk
                .par_iter()
                .map(|(repo_id, path)| {
                    let root = roots
                        .get(*repo_id as usize)
                        .map_or(path.as_path(), |r| r.as_path());
                    // The stat a refusal is remembered by is the one from before the
                    // file is read: a file fixed during the pass then differs from it.
                    let stat = std::fs::metadata(path)
                        .ok()
                        .map(|m| DifferentialVfs::stat_of(&m));
                    (
                        *repo_id,
                        path.clone(),
                        Self::process_file(path, *repo_id, root, scan_cfg),
                        stat,
                    )
                })
                .collect::<Vec<_>>()
        };
        // In chunks: each file's serialized cache record is written and dropped after its
        // chunk instead of every record of the workspace being held until the whole
        // pass ends (the peak of a cold build).
        let mut results = Vec::with_capacity(files.len());
        for chunk in files.chunks(SCAN_CHUNK_FILES) {
            let mut part = match pool {
                Some(engine) => engine.install(|| work(chunk)),
                None => work(chunk),
            };
            if let Some(cache) = scan_cfg.cache {
                let writes: Vec<CacheEntry> = part
                    .iter_mut()
                    .filter_map(|(_, _, outcome, _)| outcome.as_mut().ok())
                    .flat_map(|f| std::mem::take(&mut f.cache_write))
                    .collect();
                cache.put_batch(&writes);
            }
            results.extend(part);
        }

        let mut health = IndexHealth::default();
        health.record_scanned(results.len());
        let mut fragments = Vec::with_capacity(results.len());
        let mut needs_retry: Vec<(RepoId, PathBuf)> = Vec::new();
        let mut stable_rejects: Vec<StableReject> = Vec::new();
        for (repo_id, path, outcome, stat) in results {
            match outcome {
                Ok(frag) if frag.code.parse_failed => needs_retry.push((repo_id, path)),
                Ok(frag) => {
                    health.record_indexed();
                    fragments.push(frag);
                }
                Err(reason) => {
                    // A read error may be transient; the other refusals are a
                    // function of the file's bytes alone.
                    if let (false, Some(stat)) = (matches!(reason, RejectKind::ReadError), stat) {
                        stable_rejects.push((path.clone(), stat));
                    }
                    Self::record_rejection(&mut health, reason, path);
                }
            }
        }

        for (repo_id, path) in needs_retry {
            let root = roots
                .get(repo_id as usize)
                .map_or(path.as_path(), |r| r.as_path());
            match Self::process_file(&path, repo_id, root, scan_cfg) {
                Ok(frag) if !frag.code.parse_failed => {
                    health.record_indexed();
                    fragments.push(frag);
                }
                Ok(frag) => {
                    tracing::warn!(
                        target: "mesh::indexer",
                        "{}: tree-sitter parse failed twice (once under load, once retried alone) \
                         — indexed with no facts for this file",
                        path.display()
                    );
                    health.record_indexed();
                    health.record_parse_failed();
                    health.record_rejected_file(
                        path.clone(),
                        RejectReason::ParseFailed,
                        Self::file_size(&path),
                    );
                    fragments.push(frag);
                }
                Err(reason) => Self::record_rejection(&mut health, reason, path),
            }
        }

        for frag in &fragments {
            if frag.code.calls_skipped_path {
                health.record_calls_skipped_path();
            }
            let c = frag.config.as_ref();
            health.record_config(
                c.is_some_and(|c| c.capped),
                c.is_some_and(|c| c.failed),
                c.is_some_and(|c| c.lossy),
            );
            health.record_sql(frag.code.sql_capped, frag.code.sql_unresolved);
            health.record_inproc(frag.code.inproc_capped);
            health.record_const(frag.code.const_capped);
            health.record_declared(frag.code.declared_capped, frag.code.declared_unread);
            health.record_jvm_brokers(frag.code.jvm_brokers_capped);
            health.record_py_brokers(frag.code.py_brokers_capped);
            health.record_go_brokers(frag.code.go_brokers_capped);
            health.record_cs_brokers(frag.code.cs_brokers_capped);
            if frag.code.calls.capped {
                tracing::warn!(
                    target: "mesh::indexer",
                    "{}: call facts capped (calls layer), a call path through it may be cut",
                    frag.path.display()
                );
            }
        }
        if let Some(cache) = scan_cfg.cache {
            let writes: Vec<CacheEntry> = fragments
                .iter_mut()
                .flat_map(|f| std::mem::take(&mut f.cache_write))
                .collect();
            // Also flushes this pass's batched `last_accessed_at` bumps, and enforces the
            // cache quota when more than `QUOTA_CHECK_MIN_WRITES` entries were written.
            cache.put_batch(&writes);
        }

        health.normalize_rejected();
        (fragments, health, stable_rejects)
    }

    /// Counts a rejection and remembers the file by path (plan 4 step 4.1), so a
    /// tool can name it instead of silently returning nothing for it.
    /// A rejected non-source file (image, lockfile, bundle: see
    /// `mesh_core::health::is_non_source`) is only counted as skipped.
    fn record_rejection(health: &mut IndexHealth, reason: RejectKind, path: PathBuf) {
        if mesh_core::health::is_non_source(&path) {
            health.record_skipped_non_source();
            return;
        }
        let mut detail: Option<u64> = None;
        let reason = match reason {
            RejectKind::Oversized => {
                health.record_oversized();
                RejectReason::Oversized
            }
            RejectKind::Binary => {
                health.record_guard_rejected();
                RejectReason::Binary
            }
            RejectKind::LongLine(len) => {
                health.record_guard_rejected();
                detail = Some(len as u64);
                RejectReason::LongLine
            }
            RejectKind::DeepNesting(depth) => {
                health.record_guard_rejected();
                detail = Some(depth as u64);
                RejectReason::DeepNesting
            }
            RejectKind::ReadError => {
                health.record_read_error();
                RejectReason::ReadError
            }
        };
        let size = Self::file_size(&path);
        health.record_rejected_file_detail(path, reason, size, detail);
    }

    /// Remembers the stat of every file the guards refused for a reason that only
    /// its bytes decide, so a later differential reload does not read it again.
    fn note_stable_rejects(vfs: &mut DifferentialVfs, rejects: &[StableReject]) {
        for (path, stat) in rejects {
            vfs.note_rejected(path, *stat);
        }
    }

    /// Size on disk for a rejection record; 0 when the file can no longer be stat'ed.
    fn file_size(path: &Path) -> u64 {
        std::fs::metadata(path).map_or(0, |m| m.len())
    }

    // ── Incremental reload ──────────────────────────────────────────────────

    /// Differential reload driven by the state's VFS: only files whose stat or
    /// content hash changed are re-read and re-indexed; deleted files are purged.
    /// Installs one consistent snapshot at the end.
    ///
    /// Serialized on `state.reload_lock` for its entire body, regardless of caller
    /// (`FileWatcherService::schedule_reload`, a direct test call, ...): two
    /// overlapping reloads would each `snapshot_clone()` from the *same* prior
    /// snapshot and then unconditionally `install_snapshot()` — whichever finishes
    /// last wins outright, silently discarding the other's changes rather than
    /// merging them. The lock is a plain `std::sync::Mutex`, acquired and released
    /// on this same call stack (never held across an `.await` or moved to another
    /// thread), so blocking here is exactly the intended backpressure: a second
    /// caller simply waits its turn and then reloads against then-current disk
    /// state, which already reflects whatever the first pass just did (idempotence
    /// invariant I2).
    pub fn reload(state: &AppState) {
        Self::reload_with(state, Some(&state.rescan), None);
    }

    /// [`Self::reload`] with the pool choice and a forced set made explicit.
    ///
    /// `pool` is the Rayon pool the re-extraction runs on: the QoS-background one
    /// (`Some(&state.rescan)`) for a reload nobody waits on, `None` (the global
    /// pool, normal priority) when a caller is waiting for the result. On a busy
    /// machine background QoS threads are starved, so a reload on the config-swap
    /// path must not use them. `force` names files to re-extract and re-fold even
    /// though their bytes are unchanged (a changed custom pattern governs them).
    pub fn reload_with(
        state: &AppState,
        pool: Option<&BackgroundRescanEngine>,
        force: Option<&(dyn Fn(&Path) -> bool + Sync)>,
    ) {
        let _guard = state
            .reload_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let config = &state.config;
        let roots = &state.allowed_roots;
        let (files, symlinks) = Self::crawl_all_counted(config, roots);

        let mut vfs = state.vfs.lock().unwrap_or_else(|e| e.into_inner());
        // Deleted: tracked by the VFS but no longer on disk. Only a full crawl's
        // complete file surface can answer this by set difference — a targeted,
        // path-driven reload (`reload_paths`) instead knows about a deletion
        // directly, from a specific watcher-reported path that no longer stat()s.
        let present: HashSet<&Path> = files.iter().map(|(_, p)| p.as_path()).collect();
        let deleted: Vec<PathBuf> = vfs
            .tracked_paths()
            .filter(|p| !present.contains(p))
            .map(Path::to_path_buf)
            .collect();
        // The refusal memo follows the crawl too: a deleted file (or one that fell
        // out of the capped rejected list) must not stay in it, or a path recreated
        // with the same mtime and size would be taken for the file that was refused.
        vfs.retain_rejected(|p| present.contains(p));
        drop(vfs);
        // A rejected file never enters the VFS, so the sweep above cannot see it
        // vanish; its entry in the rejected list must still go (plan 4 step 4.1).
        let mut deleted = deleted;
        deleted.extend(
            state
                .snapshot()
                .health
                .rejected
                .iter()
                .filter(|r| !present.contains(r.path.as_path()))
                .map(|r| r.path.clone()),
        );

        Self::apply_incremental(state, "VFS differential", files, deleted, pool, force);
        // A link added or removed changes nothing else a reload would install, yet doctor and
        // the notes must not keep the previous crawl's count.
        if state.snapshot().health.dir_symlinks_not_followed != symlinks {
            let mut snapshot = state.snapshot_clone();
            snapshot.health.dir_symlinks_not_followed = symlinks;
            state.install_snapshot(snapshot);
        }
    }

    /// Targeted reload driven directly by the file watcher's own reported paths —
    /// no `crawl_all` walk of the whole tree to rediscover what might have
    /// changed. Each path is resolved to the most specific containing root
    /// (matching `crawl_all`'s own nested-root attribution) and checked against
    /// that root's exclude patterns/`.gitignore` via
    /// `FilesystemCrawler::is_path_excluded`; a path that check can't cheaply and
    /// correctly resolve (see that function's doc comment — chiefly a nested
    /// ignore file between the root and the file) falls back to a full
    /// `reload()` rather than risk a wrong answer. A `.git/HEAD` or `.git/refs/*`
    /// change (checkout, rebase, branch switch), or a path whose parent
    /// directory is *also* gone (a `rm -rf` on a directory can coalesce into
    /// fewer watcher events than one per contained file), can likewise alter an
    /// arbitrary number of tracked files without each one necessarily producing
    /// its own watcher event, so those also fall back to `reload()`.
    pub fn reload_paths(state: &AppState, changed_paths: &[PathBuf]) {
        if changed_paths.is_empty() {
            return;
        }
        if changed_paths.iter().any(|p| Self::is_git_ref_change(p)) {
            tracing::debug!(
                target: "mesh::watcher",
                "Targeted reload: .git ref change in event set, falling back to full reload."
            );
            return Self::reload(state);
        }
        // A directory stands for a whole subtree (a renamed or moved-in directory, an FSEvents
        // `MustScanSubDirs` hint): its files never produce events of their own, so only a
        // crawl finds them — and forgets the ones under a directory's old name.
        if changed_paths.iter().any(|p| p.is_dir()) {
            tracing::debug!(
                target: "mesh::watcher",
                "Targeted reload: directory in event set, falling back to full reload."
            );
            return Self::reload(state);
        }

        let config = &state.config;
        let roots = &state.allowed_roots;

        let mut seen: HashSet<PathBuf> = HashSet::new();
        let mut candidates: Vec<(RepoId, PathBuf)> = Vec::new();
        let mut deleted: Vec<PathBuf> = Vec::new();
        // Compiling an `ExcludeMatcher` (and, inside `is_path_excluded`, parsing
        // a root's ignore files) is not free; `schedule_reload` deliberately
        // coalesces a whole debounce burst into one call here, so a batch of
        // many paths under the same root must not redo that work per path.
        let mut matcher_cache: HashMap<RepoId, ExcludeMatcher> = HashMap::new();

        for raw in changed_paths {
            // Canonicalize before resolving a root, so a symlink is matched (and,
            // below, indexed) by where it actually points, not by the raw watched
            // path — a symlink created inside a watched root pointing outside
            // every allowed root must never be followed into indexing its
            // target's content (CLAUDE.md Commandment 4: never follow symlinks
            // out of the sandbox). A path that no longer exists can't be
            // canonicalized; a deletion only needs to identify *that* a change
            // happened under some watched root, never a symlink target, so it
            // falls back to matching the raw reported path.
            let canonical = dunce::canonicalize(raw);
            let match_path: &Path = canonical.as_deref().unwrap_or(raw.as_path());
            if !seen.insert(match_path.to_path_buf()) {
                continue;
            }

            let Some((repo_id, root)) = Self::most_specific_root(match_path, roots) else {
                tracing::debug!(
                    target: "mesh::watcher",
                    "Targeted reload: {} (resolved: {}) is outside every allowed root, dropping.",
                    raw.display(),
                    match_path.display()
                );
                continue;
            };
            let matcher = matcher_cache.entry(repo_id).or_insert_with(|| {
                let exclusions = Self::exclude_patterns_for_root(
                    &config.workspace.exclude_patterns,
                    roots,
                    root,
                );
                ExcludeMatcher::compile(&exclusions)
            });
            match FilesystemCrawler::is_path_excluded(root, match_path, matcher) {
                Some(true) => continue,
                None => {
                    tracing::debug!(
                        target: "mesh::watcher",
                        "Targeted reload: could not cheaply resolve exclusion for {}, falling back to full reload.",
                        match_path.display()
                    );
                    return Self::reload(state);
                }
                Some(false) => {}
            }
            if canonical.is_ok() {
                candidates.push((repo_id, match_path.to_path_buf()));
                continue;
            }
            // `raw` doesn't exist (canonicalize failed above). If its parent
            // directory is *also* gone, a whole subtree likely vanished in one
            // `rm -rf` and sibling files under it may never have reported their
            // own deletion event — a plain per-path removal here would leave
            // their VFS/graph entries stale until an unrelated future change
            // happened to touch the same path again. Fall back to a full
            // reload's crawl-vs-VFS set-difference sweep instead of guessing.
            if raw.parent().is_some_and(|p| !p.exists()) {
                tracing::debug!(
                    target: "mesh::watcher",
                    "Targeted reload: {}'s parent directory is also gone, falling back to full reload.",
                    raw.display()
                );
                return Self::reload(state);
            }
            deleted.push(raw.clone());
        }

        if candidates.is_empty() && deleted.is_empty() {
            tracing::debug!(
                target: "mesh::watcher",
                "Targeted reload: no relevant candidates survived exclusion in the watcher event set."
            );
            return;
        }

        // Acquired only now, after every fallback-to-`reload()` branch above has
        // already returned — `reload()` takes this same lock itself, and
        // `std::sync::Mutex` is not reentrant.
        let _guard = state
            .reload_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Self::apply_incremental(
            state,
            "Targeted",
            candidates,
            deleted,
            Some(&state.rescan),
            None,
        );
    }

    /// The `root` in `roots` that most specifically contains `path` (the longest
    /// matching prefix) — the same attribution `crawl_all` gives an overlapping
    /// file via `exclude_patterns_for_root`'s nested-root exclusion, reproduced
    /// here for a single path without crawling anything.
    pub(crate) fn most_specific_root<'a>(
        path: &Path,
        roots: &'a [PathBuf],
    ) -> Option<(RepoId, &'a Path)> {
        roots
            .iter()
            .enumerate()
            .filter(|(_, root)| path.starts_with(root.as_path()))
            .max_by_key(|(_, root)| root.as_os_str().len())
            .map(|(idx, root)| (idx as RepoId, root.as_path()))
    }

    /// `.git/HEAD` or `.git/refs/...` — a branch checkout/rebase/switch can alter
    /// an arbitrary number of tracked files without each one necessarily firing
    /// its own watcher event (e.g. switching to a branch whose only difference
    /// upstream is a ref pointer). `changed_paths` is not a reliable transcript
    /// of what changed on disk in that case.
    fn is_git_ref_change(path: &Path) -> bool {
        let mut components = path.components().map(|c| c.as_os_str());
        while let Some(c) = components.next() {
            if c == ".git" {
                let next = components.next();
                return next == Some(std::ffi::OsStr::new("HEAD"))
                    || next == Some(std::ffi::OsStr::new("refs"));
            }
        }
        false
    }

    /// Shared tail of both `reload` and `reload_paths`: scan whatever candidate
    /// surface the caller resolved (a full crawl's file list, or one path-driven
    /// targeted set), diff it against the VFS, and — if anything really
    /// changed — fold it into a freshly installed snapshot. `label` only
    /// distinguishes the two callers in logs.
    fn apply_incremental(
        state: &AppState,
        label: &str,
        files: Vec<(RepoId, PathBuf)>,
        deleted: Vec<PathBuf>,
        pool: Option<&BackgroundRescanEngine>,
        force: Option<&(dyn Fn(&Path) -> bool + Sync)>,
    ) {
        let config = &state.config;
        let roots = &state.allowed_roots;
        let patterns = Self::compiled_patterns(config);
        let spring = SpringSettings::from_config(config);
        let extract_cfg = Self::extract_config(config, roots);
        let toggles = Self::engine_toggles(config);
        let t_start = std::time::Instant::now();

        let mut vfs = state.vfs.lock().unwrap_or_else(|e| e.into_inner());

        // Candidates: new or stat-changed. The metadata call is parallelized over
        // Rayon threads to maximize OS kernel page-cache stat speed.
        let mut candidates: Vec<(RepoId, PathBuf)> = files
            .par_iter()
            .filter(|(_, p)| match std::fs::metadata(p) {
                Ok(m) => !vfs.is_unchanged_fast(p, &m) && !vfs.is_rejection_unchanged(p, &m),
                Err(_) => false,
            })
            .map(|(r, p)| (*r, p.clone()))
            .collect();

        // `reload_paths` classifies a path as `deleted` from a `stat` taken
        // before `reload_lock` was acquired (its own caller can't hold the lock
        // and still call `reload()` as a fallback without deadlocking — see that
        // function's own comment). A file briefly absent in that window (an
        // editor's atomic save, or a fast delete-then-recreate) can legitimately
        // exist again by now; re-verified here, one last time, while `vfs` is
        // still locked. One that exists again is treated as an ordinary
        // candidate instead of a deletion — removing it from the graph would be
        // wrong, and since it wouldn't be in `candidates` either, permanently
        // wrong until some unrelated future change happened to touch it again.
        let (deleted, resurrected): (Vec<PathBuf>, Vec<PathBuf>) = deleted
            .into_iter()
            .partition(|p| std::fs::metadata(p).is_err());
        for p in resurrected {
            if let Some((repo_id, _)) = Self::most_specific_root(&p, roots) {
                candidates.push((repo_id, p));
            }
        }

        // An alias config edit changes what the scripts it governs import without
        // touching those files: they are re-extracted, and folded in even though their
        // own bytes are unchanged.
        let mut forced = Self::alias_governed_scripts(state, &vfs, roots, &candidates, &deleted);
        // A configuration file edit re-resolves the `${key}` topics of the source files that
        // hold one, though their own bytes did not change (ticket T6).
        forced.extend(Self::config_governed_files(
            state,
            roots,
            &candidates,
            &deleted,
            &forced,
        ));
        if let Some(force) = force {
            let known: HashSet<&Path> = forced.iter().map(|(_, p)| p.as_path()).collect();
            let extra: Vec<(RepoId, PathBuf)> = files
                .iter()
                .filter(|(_, p)| force(p) && !known.contains(p.as_path()))
                .cloned()
                .collect();
            drop(known);
            forced.extend(extra);
        }
        let forced_paths: HashSet<PathBuf> = forced.iter().map(|(_, p)| p.clone()).collect();
        let already: HashSet<PathBuf> = candidates.iter().map(|(_, p)| p.clone()).collect();
        for (repo_id, path) in forced {
            if !already.contains(&path) {
                candidates.push((repo_id, path));
            }
        }

        if candidates.is_empty() && deleted.is_empty() {
            tracing::debug!(target: "mesh::watcher", "{label}: 0 files changed, skipping reload.");
            return;
        }

        tracing::debug!(
            target: "mesh::indexer",
            "{label}: re-extracting {} candidate file(s) ({} forced), {} deleted, on the {} pool.",
            candidates.len(),
            forced_paths.len(),
            deleted.len(),
            if pool.is_some() { "background" } else { "global" }
        );
        let doc_template = state.snapshot().doc_index.clone_settings();
        let path_context = PathContext::default();
        // The workspace's persistent cache, when attached (plan 4 step 4.4): a branch
        // switch back to already-seen content is a lookup instead of a re-parse, and
        // `put_batch` re-checks the cache quota after any reload writing > 100 entries.
        let scan_cfg = ScanConfig {
            patterns: &patterns,
            doc_template: &doc_template,
            spring: &spring,
            extract_cfg: &extract_cfg,
            toggles: &toggles,
            cache: state.index_cache.get(),
            config_fingerprint: Self::config_fingerprint(&extract_cfg),
            path_context: &path_context,
        };
        let (fragments, pass_health, stable_rejects) =
            Self::run_scan_pass(&candidates, roots, &scan_cfg, pool);
        Self::note_stable_rejects(&mut vfs, &stable_rejects);

        // A file whose parse still fails after `run_scan_pass`'s retry keeps its last
        // known-good facts: it is excluded here, *before* the VFS is touched, so its
        // signature is never updated and the next reload (triggered by any change
        // anywhere) sees it as still-changed and retries it again — rather than the
        // pre-1.3 behaviour of wiping its facts to empty and marking it seen
        // (idempotence invariant I6).
        let (still_failed, fragments): (Vec<_>, Vec<_>) =
            fragments.into_iter().partition(|f| f.code.parse_failed);
        if !still_failed.is_empty() {
            tracing::warn!(
                target: "mesh::watcher",
                "{} file(s) still failed to parse after retry; keeping last known-good facts, \
                 will retry on the next reload: {:?}",
                still_failed.len(),
                still_failed
                    .iter()
                    .map(|f| f.path.display().to_string())
                    .collect::<Vec<_>>()
            );
        }

        // Bare `touch`es hash identical and are dropped here.
        let changed: Vec<FileFragment> = fragments
            .into_iter()
            .filter(|f| {
                let content_changed = vfs.upsert(&f.path, f.signature.clone());
                content_changed || forced_paths.contains(&f.path)
            })
            .collect();
        for path in &deleted {
            vfs.remove(path);
        }
        drop(vfs);

        // Rejected-file list: every path this pass reprocessed or deleted takes
        // this pass's outcome (a fixed file drops out, a new rejection appears),
        // even when no fragment changed — a newly oversized file changes nothing
        // else. A rejected file never enters the VFS, so it is reprocessed on
        // every full reload; an unchanged outcome must not reinstall a snapshot.
        let mut rejections = state.snapshot().health.clone();
        let touched: HashSet<&Path> = candidates
            .iter()
            .map(|(_, p)| p.as_path())
            .chain(deleted.iter().map(PathBuf::as_path))
            .collect();
        let rejections_changed = rejections.replace_rejected(&touched, &pass_health.rejected);
        drop(touched);

        if changed.is_empty() && deleted.is_empty() && !rejections_changed {
            return;
        }
        if changed.len() > 200 {
            tracing::info!(
                target: "mesh::watcher",
                "Mass filesystem mutation detected ({} files). Running full differential rebuild.",
                changed.len()
            );
        }

        let t_scanned = std::time::Instant::now();
        let mut snapshot = state.snapshot_clone();
        let t_cloned = std::time::Instant::now();
        snapshot.health.merge(&pass_health);
        snapshot.health.rejected = rejections.rejected;
        snapshot.health.rejected_overflow = rejections.rejected_overflow;
        let stale = changed
            .iter()
            .map(|f| f.path.as_path())
            .chain(deleted.iter().map(PathBuf::as_path));
        snapshot.contract_graph.patch_files(stale);
        // One pass per index for the whole batch, not one per file: a branch
        // switch reloading k of N files was O(k·N) here.
        let stale_files: HashSet<&Path> = changed
            .iter()
            .map(|f| f.path.as_path())
            .chain(deleted.iter().map(PathBuf::as_path))
            .collect();
        snapshot.doc_index.remove_files(&stale_files);
        snapshot.property_registry.remove_files(&stale_files);
        let constants_before = snapshot.member_table.digest();
        snapshot.config_values.remove_files(&stale_files);
        snapshot.member_table.remove_files(&stale_files);
        drop(stale_files);
        let changed_count = changed.len();
        let mut changed = changed;
        Self::fold_config(&mut changed, &mut snapshot);
        let changed_paths: HashSet<PathBuf> = changed.iter().map(|f| f.path.clone()).collect();
        for frag in changed {
            Self::fold(frag, &mut snapshot);
        }
        // An enum or constant this batch added, removed or edited moves how the unchanged files
        // naming it as a topic resolve: they are folded again below, once.
        let const_refresh: Vec<(RepoId, PathBuf)> =
            if snapshot.member_table.digest() == constants_before {
                Vec::new()
            } else {
                snapshot
                    .member_table
                    .ref_files()
                    .filter(|p| !changed_paths.contains(*p))
                    .filter_map(|p| {
                        Self::most_specific_root(p, roots).map(|(r, _)| (r, p.to_path_buf()))
                    })
                    .collect()
            };
        if spring.resolve_placeholders {
            snapshot.property_registry.resolve_all_placeholders();
        }
        let t_folded = std::time::Instant::now();
        // Only the edges this batch can reach are recomputed (`reconcile_delta`
        // falls back to the full rebuild for a large batch). An alias config edit
        // changes how the scripts it governs resolve without those scripts' own
        // nodes saying so, and a changed custom pattern forces the files it applies
        // to; either way `forced_paths` is non-empty and the reload takes the full
        // rebuild.
        let mode = if forced_paths.is_empty() {
            snapshot.contract_graph.reconcile_delta()
        } else {
            snapshot.contract_graph.reconcile_edges();
            ReconcileMode::Full("forced re-extraction (alias config or custom pattern change)")
        };
        let call_stats = snapshot.contract_graph.call_stats();
        snapshot.health.set_call_stats(&call_stats);
        snapshot
            .health
            .set_wildcards_capped(snapshot.contract_graph.wildcard_hubs_capped());
        snapshot.health.config_topics_unresolved =
            snapshot.contract_graph.unresolved_placeholder_topics();
        // Files this batch brought onto an entry -> producer path whose call sites an
        // earlier derive pruned (they lay off every path then): re-extracted below.
        let missing_sites: Vec<(RepoId, PathBuf)> = snapshot
            .contract_graph
            .call_files_missing_sites()
            .iter()
            .filter_map(|p| {
                Self::most_specific_root(p, roots).map(|(repo, _)| (repo, p.to_path_buf()))
            })
            .collect();
        let t_reconciled = std::time::Instant::now();
        let node_count = snapshot.contract_graph.node_count();
        let edge_count = snapshot.contract_graph.edge_count();
        let generation = state.install_snapshot(snapshot);
        let t_installed = std::time::Instant::now();
        let ms = |a: std::time::Instant, b: std::time::Instant| (b - a).as_secs_f64() * 1000.0;
        tracing::debug!(
            target: "mesh::watcher",
            "{label} reload timings: total_ms={:.1} scan_ms={:.1} clone_ms={:.1} patch_fold_ms={:.1} \
             reconcile_ms={:.1} ({mode:?}) install_ms={:.1} nodes={node_count} edges={edge_count}",
            ms(t_start, t_installed),
            ms(t_start, t_scanned),
            ms(t_scanned, t_cloned),
            ms(t_cloned, t_folded),
            ms(t_folded, t_reconciled),
            ms(t_reconciled, t_installed),
        );

        tracing::info!(
            target: "mesh::watcher",
            "{label} reload (gen {generation}): {changed_count} re-indexed, {} removed, {node_count} contract nodes.",
            deleted.len()
        );

        // The files holding an enum-member topic whose constants moved, folded again against the
        // new table. The table does not move in this pass, so it is the last one.
        if !const_refresh.is_empty() && label != CONST_REFRESH_LABEL {
            tracing::debug!(
                target: "mesh::watcher",
                "{} file(s) hold a topic named by a constant that changed: folding them again",
                const_refresh.len()
            );
            {
                let mut vfs = state.vfs.lock().unwrap_or_else(|e| e.into_inner());
                for (_, path) in &const_refresh {
                    vfs.remove(path);
                }
            }
            Self::apply_incremental(
                state,
                CONST_REFRESH_LABEL,
                const_refresh,
                Vec::new(),
                None,
                None,
            );
        }

        // One restoring pass, never a loop: a file that cannot be re-extracted (gone, no
        // longer parses) stays counted in `IndexHealth::call_files_missing_sites`. The files
        // are forgotten by the VFS so the pass sees them as new; nothing else about them
        // changed, so it takes the delta reconcile.
        if !missing_sites.is_empty() && label != CALLS_REFRESH_LABEL {
            tracing::debug!(
                target: "mesh::calls",
                "{} file(s) came onto a call path: restoring their call sites",
                missing_sites.len()
            );
            {
                let mut vfs = state.vfs.lock().unwrap_or_else(|e| e.into_inner());
                for (_, path) in &missing_sites {
                    vfs.remove(path);
                }
            }
            Self::apply_incremental(
                state,
                CALLS_REFRESH_LABEL,
                missing_sites,
                Vec::new(),
                None,
                None,
            );
        }
    }

    /// Whether `path` can hold the value of a `${key}` topic: a yaml / properties file or a
    /// dotenv file.
    fn is_config_source(path: &Path) -> bool {
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        config_values::is_env_template_name(name)
            || matches!(
                path.extension().and_then(|e| e.to_str()),
                Some("yml" | "yaml" | "properties")
            )
    }

    /// The source files holding a `${key}` topic, when a configuration file is among the
    /// `changed` / `deleted` ones and not already `forced`: their resolution may move.
    fn config_governed_files(
        state: &AppState,
        roots: &[PathBuf],
        changed: &[(RepoId, PathBuf)],
        deleted: &[PathBuf],
        forced: &[(RepoId, PathBuf)],
    ) -> Vec<(RepoId, PathBuf)> {
        let touched = changed
            .iter()
            .map(|(_, p)| p.as_path())
            .chain(deleted.iter().map(PathBuf::as_path))
            .any(Self::is_config_source);
        if !touched {
            return Vec::new();
        }
        let known: HashSet<&Path> = forced.iter().map(|(_, p)| p.as_path()).collect();
        let snapshot = state.snapshot();
        snapshot
            .config_values
            .ref_files()
            .filter(|p| !known.contains(p))
            .filter_map(|p| Self::most_specific_root(p, roots).map(|(r, _)| (r, p.to_path_buf())))
            .collect()
    }

    /// Whether `path` is a file that defines import aliases for the scripts under it:
    /// a `tsconfig*.json` / `jsconfig*.json` (also reached through `extends`, from
    /// anywhere under its root) or a `package.json` (`imports`, its own directory).
    fn is_alias_config(path: &Path) -> bool {
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        let ts = (name.starts_with("tsconfig") || name.starts_with("jsconfig"))
            && name.ends_with(".json");
        ts || name == "package.json"
    }

    /// What of an alias config can move an alias: a `tsconfig*` / `jsconfig*` file
    /// whole (it may be extended), a `package.json` only through its `imports` map.
    /// `None` when the file is gone.
    fn alias_digest(path: &Path) -> Option<[u8; 32]> {
        let text = std::fs::read_to_string(path).ok()?;
        let relevant = if path.file_name().is_some_and(|n| n == "package.json") {
            serde_json::from_str::<serde_json::Value>(&text)
                .ok()
                .and_then(|v| v.get("imports").map(|i| i.to_string()))
                .unwrap_or_default()
        } else {
            text
        };
        Some(sha256(relevant.as_bytes()))
    }

    /// The tracked scripts governed by every alias config among `changed` and
    /// `deleted` whose alias-relevant content differs from the last one seen (or was
    /// never seen, or is gone). A `tsconfig*` / `jsconfig*` governs its whole root
    /// (another config may extend it from anywhere); a `package.json` its directory.
    fn alias_governed_scripts(
        state: &AppState,
        vfs: &DifferentialVfs,
        roots: &[PathBuf],
        changed: &[(RepoId, PathBuf)],
        deleted: &[PathBuf],
    ) -> Vec<(RepoId, PathBuf)> {
        let mut digests = state
            .alias_digests
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let mut scopes: Vec<PathBuf> = Vec::new();
        for path in changed
            .iter()
            .map(|(_, p)| p.as_path())
            .chain(deleted.iter().map(PathBuf::as_path))
            .filter(|p| Self::is_alias_config(p))
        {
            let now = Self::alias_digest(path);
            let before = match now {
                Some(d) => digests.insert(path.to_path_buf(), d),
                None => digests.remove(path),
            };
            if before == now && now.is_some() {
                continue;
            }
            let is_package_json = path.file_name().is_some_and(|n| n == "package.json");
            let scope = if is_package_json {
                path.parent().map(Path::to_path_buf)
            } else {
                Self::most_specific_root(path, roots).map(|(_, root)| root.to_path_buf())
            };
            scopes.extend(scope);
        }
        if scopes.is_empty() {
            return Vec::new();
        }
        let is_script = |p: &Path| {
            matches!(
                LanguageKind::from_path(&p.to_string_lossy()),
                LanguageKind::TypeScript | LanguageKind::Tsx | LanguageKind::JavaScript
            ) || matches!(
                p.extension().and_then(|e| e.to_str()),
                Some("vue" | "svelte" | "astro")
            )
        };
        vfs.tracked_paths()
            .filter(|p| is_script(p) && scopes.iter().any(|d| p.starts_with(d)))
            .filter_map(|p| Self::most_specific_root(p, roots).map(|(r, _)| (r, p.to_path_buf())))
            .collect()
    }

    // ── Internals ───────────────────────────────────────────────────────────

    /// Builds the doc index from `[engines.docs]` so aliases, stop words and the
    /// phrase boost configured by the user actually apply. A default-constructed
    /// `DocIndex` silently ignores all of them.
    pub fn doc_index_for(config: &Config) -> DocIndex {
        match config.engines.docs.as_ref() {
            Some(docs) => DocIndex::new(
                docs.aliases.clone(),
                docs.stop_words.clone(),
                docs.exact_phrase_boost,
                docs.sanitize_prompt_injections,
                docs.fuzzy_fallback,
            ),
            None => DocIndex::default(),
        }
    }

    fn compiled_patterns(config: &Config) -> Vec<CompiledPattern> {
        config
            .engines
            .contracts
            .as_ref()
            .map(|c| CompiledPattern::compile_all(&c.patterns))
            .unwrap_or_default()
    }

    /// Builds the extraction knobs from `[engines.contracts.grpc]` /
    /// `.openapi` / `.asyncapi` so `proto_dirs`, `controller_annotations`,
    /// `canonical_fqcn_projection`, `spec_files` and `infer_string_topics`
    /// actually apply instead of being parsed and ignored.
    fn extract_config(config: &Config, roots: &[PathBuf]) -> ExtractConfig {
        let mut extract_cfg = config
            .engines
            .contracts
            .as_ref()
            .map(ExtractConfig::from_contracts)
            .unwrap_or_default();
        extract_cfg.ts_aliases = TsAliasIndex::new(roots);
        extract_cfg
    }

    /// Reads `[engines.docs] enabled`, `[engines.contracts] enabled` and
    /// `[engines.docs] paths` from config. Absent sections default to enabled
    /// (matching each config struct's own `#[serde(default = "default_true")]`),
    /// and an empty `paths` list means "no restriction".
    fn engine_toggles(config: &Config) -> EngineToggles {
        let docs_enabled = config.engines.docs.as_ref().is_none_or(|d| d.enabled);
        let contracts_enabled = config.engines.contracts.as_ref().is_none_or(|c| c.enabled);
        let doc_paths = config
            .engines
            .docs
            .as_ref()
            .filter(|d| !d.paths.is_empty())
            .map(|d| ExcludeMatcher::compile(&d.paths));

        EngineToggles {
            docs_enabled,
            contracts_enabled,
            doc_paths,
        }
    }

    /// Crawls all roots, tagging each file with the `RepoId` of its **most specific**
    /// containing root — the deepest one, when roots nest (e.g. `roots = [".",
    /// "./services/*"]`, the common `[workspace] roots = [..., "./x/*"]` shape `init
    /// --auto` and hand-written configs both produce). `expand_roots` only dedupes
    /// *identical* canonical paths, so an enclosing root and its own subdirectories can
    /// both be configured roots; without this, every file under the more specific root
    /// would be indexed twice, once per root, as two nodes with different `repo_id`s
    /// that `canonical_lines()` cannot tell apart from a real duplicate declaration
    /// (idempotence invariant I4).
    ///
    /// Implemented by excluding each nested root's subtree from every one of its
    /// ancestors' crawls — one file, one crawl, one `RepoId` — rather than crawling
    /// everything and deduplicating after the fact, which would double the I/O on any
    /// workspace using this shape.
    pub fn crawl_all(config: &Config, roots: &[PathBuf]) -> Vec<(RepoId, PathBuf)> {
        Self::crawl_all_counted(config, roots).0
    }

    /// [`Self::crawl_all`] plus the number of directory symlinks no root's crawl followed.
    pub fn crawl_all_counted(
        config: &Config,
        roots: &[PathBuf],
    ) -> (Vec<(RepoId, PathBuf)>, usize) {
        let mut symlinks = 0usize;
        if roots.len() > RepoId::MAX as usize {
            tracing::error!(
                target: "mesh::indexer",
                "Root count ({}) exceeds maximum supported RepoId limit ({})",
                roots.len(),
                RepoId::MAX
            );
        }
        let mut out = Vec::new();
        for (idx, root) in roots.iter().enumerate().take(RepoId::MAX as usize) {
            let repo_id = idx as RepoId;
            match ValidatedScope::resolve_with_aliases(
                &root.to_string_lossy(),
                roots,
                &config.workspace.mount_aliases,
                None,
            ) {
                Ok(scope) => {
                    let exclusions = Self::exclude_patterns_for_root(
                        &config.workspace.exclude_patterns,
                        roots,
                        root,
                    );
                    let matcher = ExcludeMatcher::compile(&exclusions);
                    let report =
                        FilesystemCrawler::crawl_scope_report(&scope, &matcher, Some(SCAN_DEPTH));
                    symlinks += report.symlinks_not_followed;
                    out.extend(report.files.into_iter().map(|f| (repo_id, f)));
                }
                Err(e) => {
                    tracing::warn!(target: "mesh::indexer", "Skipping root {}: {e}", root.display());
                }
            }
        }
        // Deterministic order regardless of `roots`' order or the OS's directory
        // iteration order — `canonical_lines()` doesn't need it, but every other
        // consumer of this list (the differential VFS, `doctor`'s dead-config
        // reporting) benefits from a stable, reviewable file order.
        out.sort_unstable_by(|(_, a), (_, b)| a.cmp(b));
        (out, symlinks)
    }

    /// `exclude_patterns` plus one `<relative>/**` glob per *other* configured root
    /// that is a strict descendant of `root` — so `root`'s own crawl never descends
    /// into a subtree a more specific root already owns. `roots` must already be the
    /// canonicalized, deduplicated list `expand_roots` produces (equal paths never
    /// occur twice, so "strict descendant" is the only overlap `PathBuf` prefixing can
    /// mean here).
    pub(crate) fn exclude_patterns_for_root(
        exclude_patterns: &[String],
        roots: &[PathBuf],
        root: &Path,
    ) -> Vec<String> {
        let mut patterns = exclude_patterns.to_vec();
        for other in roots {
            if other == root || !other.starts_with(root) {
                continue;
            }
            let Ok(rel) = other.strip_prefix(root) else {
                continue;
            };
            if rel.as_os_str().is_empty() {
                continue;
            }
            patterns.push(format!("{}/**", rel.to_string_lossy().replace('\\', "/")));
        }
        patterns
    }

    /// The extraction knobs of a full scan, for callers that extract contents the
    /// scan never reads (a file's version at a Git base, in `diff_mesh`).
    pub(crate) fn content_extractor(config: &Config, roots: &[PathBuf]) -> ContentExtractor {
        ContentExtractor {
            extract_cfg: Self::extract_config(config, roots),
            patterns: Self::compiled_patterns(config),
        }
    }

    /// Reads, guards and extracts one file. Runs on the pool; touches no shared state.
    ///
    /// The `Err` side is a deliberate, logged policy rejection (size/lexical guard,
    /// unreadable, not UTF-8) — every one of these is counted into `IndexHealth` by
    /// the caller, never silently dropped. A successful `Ok(frag)` can still carry
    /// `frag.code.parse_failed == true`: the file passed every guard but its
    /// tree-sitter parse itself failed (see `AstGuard::parse_with`), which the caller
    /// retries sequentially rather than folding as an empty file.
    fn process_file(
        path: &Path,
        repo_id: RepoId,
        root: &Path,
        cfg: &ScanConfig,
    ) -> Result<FileFragment, RejectKind> {
        let ScanConfig {
            patterns,
            doc_template,
            spring,
            toggles,
            ..
        } = *cfg;
        let metadata = std::fs::metadata(path).map_err(|_| RejectKind::ReadError)?;
        // Commandment 2: check the size budget *before* reading, so an oversized file
        // never costs its full read.
        if !AstGuard::within_size_budget(path, &metadata) {
            return Err(RejectKind::Oversized);
        }
        let bytes = std::fs::read(path).map_err(|_| RejectKind::ReadError)?;
        let path_str = path.to_string_lossy();
        let lang = LanguageKind::from_path(&path_str);

        // Tree-sitter inputs get the full lexical guard (null bytes, line length, nesting).
        // `.sql` is read by a linear lexer: size and binary sniff only. Markdown / YAML /
        // properties only get the binary sniff — prose legitimately has >1 KB lines.
        let verdict = if mesh_parsers::languages::sql::is_sql_path(path) {
            mesh_parsers::languages::sql::check_content(metadata.len(), &bytes)
        } else if lang.language().is_some() {
            AstGuard::check_path(path, &metadata, &bytes)
        } else if AstGuard::looks_binary(&bytes) {
            Err(GuardRejection::Binary)
        } else {
            Ok(())
        };
        match verdict {
            Ok(()) => {}
            Err(GuardRejection::Oversized { .. }) => return Err(RejectKind::Oversized),
            Err(GuardRejection::Binary) => return Err(RejectKind::Binary),
            Err(GuardRejection::LongLine { len }) => return Err(RejectKind::LongLine(len)),
            Err(GuardRejection::DeepNesting { depth }) => {
                return Err(RejectKind::DeepNesting(depth))
            }
        }
        // A configuration file that is not UTF-8 is read with the invalid bytes replaced (the
        // lines that held them are lost, counted), instead of losing all its valid lines.
        let lossy_text;
        let (content, config_lossy) = match std::str::from_utf8(&bytes) {
            Ok(c) => (c, false),
            Err(_) if Self::is_config_source(path) => {
                lossy_text = String::from_utf8_lossy(&bytes).into_owned();
                (lossy_text.as_str(), true)
            }
            Err(_) => return Err(RejectKind::ReadError),
        };
        let signature = DifferentialVfs::compute_signature(&metadata, &bytes);

        let mut frag = FileFragment {
            path: path.to_path_buf(),
            signature,
            code: FileIndex::default(),
            docs: Vec::new(),
            props: None,
            config: None,
            cache_write: Vec::new(),
        };
        let file_name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        let template = config_values::is_template_name(file_name);
        if config_values::is_env_template_name(file_name) {
            frag.config = Some(config_values::from_dotenv(content, template));
        }

        let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
        match ext {
            "md" => {
                if toggles.docs_enabled && Self::doc_path_allowed(path, root, &toggles.doc_paths) {
                    frag.docs = doc_template.parse_sections(path, content);
                }
            }
            "properties" => {
                if spring.is_property_source(path) {
                    let mut reg = PropertyRegistry::with_redaction(spring.auto_redact_secrets);
                    reg.ingest_properties_str(content);
                    frag.config = Some(config_values::from_registry(&reg, template, false));
                    frag.props = Some(reg);
                }
            }
            "yml" | "yaml" => {
                let mut props = None;
                let mut yaml_failed = false;
                if spring.is_property_source(path) {
                    let mut reg = PropertyRegistry::with_redaction(spring.auto_redact_secrets);
                    yaml_failed = reg.ingest_yaml_str(content).is_err();
                    props = Some(reg);
                }
                frag.config = Self::yaml_config(content, props.as_ref(), template, yaml_failed);
                frag.props = props;
                if toggles.contracts_enabled {
                    let (code, cache_write) = Self::extract_with_cache(
                        path,
                        root,
                        content,
                        repo_id,
                        &frag.signature.content_hash,
                        cfg,
                    );
                    frag.code = code;
                    frag.cache_write = cache_write;
                }
            }
            _ => {
                if toggles.contracts_enabled {
                    let (code, cache_write) = Self::extract_with_cache(
                        path,
                        root,
                        content,
                        repo_id,
                        &frag.signature.content_hash,
                        cfg,
                    );
                    frag.code = code;
                    frag.cache_write = cache_write;
                }
            }
        }
        if toggles.contracts_enabled && !patterns.is_empty() {
            frag.code.merge(PolyglotIndexer::extract_custom_patterns(
                path, content, repo_id, patterns,
            ));
        }
        if config_lossy {
            frag.config.get_or_insert_with(Default::default).lossy = true;
        }

        Ok(frag)
    }

    /// Tree-sitter extraction with an optional persistent-cache short-circuit (P2 step 3.2).
    /// `content_hash` is the file's already-computed signature hash, so this costs nothing
    /// beyond the lookup itself on either path.
    ///
    /// A cache hit deserializes and returns immediately with no write-back. A miss parses
    /// as before and, unless the parse itself failed (`code.parse_failed`, retried by the
    /// caller — see `run_scan_pass`'s doc comment; caching a failed parse would wrongly
    /// persist "no facts" past the retry), serializes the result for `run_scan_pass` to
    /// batch-write once the whole parallel pass is done.
    ///
    /// The cache is shared by every checkout of a repository (8.0.0), so nothing in a key or
    /// a record names the directory a checkout sits in: the key holds the path relative to
    /// `root`, the record holds node paths relative to `root` (re-anchored on the current
    /// `root` after a hit), and every decision extraction draws from the absolute path is
    /// folded into the key (`PathContext::append_key_input`, plus the inferred package for
    /// a record that uses it, see [`CachedRecord`]). A file not under `root` is never
    /// cached.
    fn extract_with_cache(
        path: &Path,
        root: &Path,
        content: &str,
        repo_id: RepoId,
        content_hash: &[u8; 32],
        cfg: &ScanConfig,
    ) -> (FileIndex, Vec<CacheEntry>) {
        let extract_cfg = cfg.extract_cfg;
        let extract = || PolyglotIndexer::extract_with_config(path, content, repo_id, extract_cfg);
        let (Some(cache), Some(rel_path)) = (cfg.cache, root_relative(path, root)) else {
            return (extract(), Vec::new());
        };
        let fingerprint = Self::file_fingerprint(path, cfg);
        let key = PersistentIndexCache::key_for(&rel_path, content_hash, repo_id, &fingerprint);
        let package_key = |package: &str| {
            let mut input = fingerprint.to_vec();
            input.extend_from_slice(b"\0pkg=");
            input.extend_from_slice(package.as_bytes());
            PersistentIndexCache::key_for(&rel_path, content_hash, repo_id, &sha256(&input))
        };
        let decode = |bytes: Vec<u8>| match serde_json::from_slice::<CachedRecord>(&bytes) {
            Ok(record) => Some(record),
            Err(_) => {
                tracing::warn!(
                    target: "mesh::index_cache",
                    "{}: cached FileIndex failed to deserialize, re-parsing",
                    path.display()
                );
                None
            }
        };
        let mut record = cache.get(&key).and_then(decode);
        if let Some(CachedRecord::NeedsPackage) = record {
            let package = cfg.path_context.package_of(path);
            record = cache.get(&package_key(&package)).and_then(decode);
        }
        if let Some(CachedRecord::Index(cached)) = record {
            let mut code = cached.index;
            code.reanchor(root, &cached.anchored);
            return (code, Vec::new());
        }

        let mut code = extract();
        if code.parse_failed {
            return (code, Vec::new());
        }
        let package = cfg.path_context.package_of(path);
        let uses_package = code.uses_package(&package);
        let Some(anchored) = code.relativize(root) else {
            // A node path outside `root`: no record another checkout could re-anchor.
            return (code, Vec::new());
        };
        let record = RootRelativeIndex {
            index: code,
            anchored,
        };
        let bytes = serde_json::to_vec(&CachedRecordRef::Index(&record));
        let stub = serde_json::to_vec(&CachedRecordRef::NeedsPackage);
        let RootRelativeIndex {
            index: mut code,
            anchored,
        } = record;
        code.reanchor(root, &anchored);
        match (bytes, stub) {
            (Ok(bytes), _) if !uses_package => (code, vec![(key, bytes)]),
            (Ok(bytes), Ok(stub)) => (code, vec![(key, stub), (package_key(&package), bytes)]),
            (Err(e), _) | (_, Err(e)) => {
                tracing::warn!(
                    target: "mesh::index_cache",
                    "{}: failed to serialize FileIndex for caching: {e}",
                    path.display()
                );
                (code, Vec::new())
            }
        }
    }

    /// The extraction-config fingerprint of one file's first cache key: the per-scan config
    /// fingerprint, the `tsconfig` / `package.json` aliases governing the file (so editing
    /// such a config re-extracts the files it governs; written root-relative), and what
    /// extraction reads from the file's absolute path except the inferred package
    /// (`PathContext::append_key_input`; the package goes into the second key, see
    /// [`CachedRecord`]).
    fn file_fingerprint(path: &Path, cfg: &ScanConfig) -> [u8; 32] {
        let mut input = Vec::with_capacity(128);
        input.extend_from_slice(&cfg.config_fingerprint);
        cfg.path_context
            .append_key_input(cfg.extract_cfg, path, &mut input);
        if let Some(aliases) = cfg.extract_cfg.ts_aliases.fingerprint_input(path) {
            input.push(0);
            input.extend_from_slice(aliases.as_bytes());
        }
        sha256(&input)
    }

    /// `paths` is empty (default) → no restriction. Otherwise the file's path,
    /// relative to its own repo root, must match one of the configured globs
    /// (e.g. `"docs/**"`, `"*.md"`). A file outside `root` (shouldn't happen —
    /// the crawl is rooted there) is conservatively allowed.
    fn doc_path_allowed(path: &Path, root: &Path, doc_paths: &Option<ExcludeMatcher>) -> bool {
        let Some(matcher) = doc_paths else {
            return true;
        };
        match path.strip_prefix(root) {
            Ok(rel) => matcher.is_excluded_with_root(rel, Some(root)),
            Err(_) => true,
        }
    }

    /// The `${key}` configuration a yaml file holds: a Spring property file (every key), a
    /// compose `environment:` block or a Kubernetes `env:` list (the variables only).
    /// `None` when it holds nothing of the kind. A multi-document file is read as its first
    /// (default) document only, so what it says is weak.
    fn yaml_config(
        content: &str,
        props: Option<&PropertyRegistry>,
        template: bool,
        failed: bool,
    ) -> Option<mesh_core::ConfigFile> {
        let env_lists = config_values::may_hold_env_list(content);
        let weak = template || config_values::is_multi_document(content);
        let mut file = match props {
            Some(reg) => config_values::from_registry(reg, weak, false),
            None if env_lists => {
                let mut reg = PropertyRegistry::new();
                let throwaway_failed = reg.ingest_yaml_str(content).is_err();
                let mut file = config_values::from_registry(&reg, weak, true);
                file.failed = throwaway_failed;
                file
            }
            None => return None,
        };
        file.failed |= failed;
        if env_lists {
            file.merge(config_values::from_env_lists(content, weak));
        }
        (!file.entries.is_empty() || file.capped || file.failed).then_some(file)
    }

    /// Takes every fragment's configuration into the snapshot, before any source file is
    /// folded: a `${key}` topic resolves against the whole workspace's configuration, not
    /// against the files folded so far.
    fn fold_config(fragments: &mut [FileFragment], snapshot: &mut MeshSnapshot) {
        for frag in fragments.iter_mut() {
            let config = frag.config.take().unwrap_or_default();
            snapshot.config_values.set_file(&frag.path, config.entries);
            // The string constants a source file declares, and the queue sites that name an
            // enum member, resolve each other across files (9.0 T14).
            let repo = frag.code.nodes.first().map_or(0, |n| n.repo_id);
            let package = frag.code.nodes.first().map_or("", |n| n.package.as_str());
            snapshot
                .member_table
                .set_file(&frag.path, repo, package, frag.code.const_records());
            snapshot.member_table.set_sites(
                &frag.path,
                repo,
                frag.code.member_imports.clone(),
                frag.code.queue_members(),
            );
            if config.failed {
                snapshot.config_values.mark_failed(&frag.path);
            }
        }
        snapshot.config_values.rebuild();
        snapshot.member_table.rebuild();
        snapshot.health.config_entries_dropped = snapshot.config_values.dropped_total();
        snapshot.health.const_members_conflicting = snapshot.member_table.conflicts();
    }

    fn fold(mut frag: FileFragment, snapshot: &mut MeshSnapshot) {
        if !frag.docs.is_empty() {
            snapshot.doc_index.extend_sections(frag.docs);
        }
        if let Some(props) = frag.props {
            snapshot.property_registry.merge(props, &frag.path);
        }
        snapshot
            .config_values
            .note_ref_file(&frag.path, frag.code.has_config_topic());
        snapshot
            .member_table
            .note_ref_file(&frag.path, frag.code.has_member_topic());
        // Keyed by the file's path: a file of records or interfaces may hold no graph node.
        if !frag.code.payload.is_empty() {
            let facts = std::mem::take(&mut frag.code.payload);
            snapshot.contract_graph.add_payload_facts(
                std::sync::Arc::from(frag.path.as_path()),
                facts.repo_id,
                facts,
            );
        }
        frag.code.apply_with(
            &mut snapshot.contract_graph,
            &snapshot.config_values,
            &snapshot.member_table,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mesh_core::{AuditLogger, Config};
    use std::sync::Arc;

    fn make_state(root: &Path) -> Arc<AppState> {
        let cfg =
            Config::load_from_str("[workspace]\nname = \"t\"\nversion = \"0\"\nroots = [\".\"]\n")
                .expect("config");
        let audit = Arc::new(AuditLogger::new_in_memory().expect("audit"));
        let rescan = Arc::new(BackgroundRescanEngine::new().expect("rescan"));
        Arc::new(AppState::new(cfg, vec![root.to_path_buf()], audit, rescan))
    }

    /// 9.0 calls layer: a file over the per-file callables cap and one over the
    /// per-file call-site cap are each counted in `IndexHealth` and named by the
    /// summary `doctor` prints, never dropped silently.
    /// `src/loop -> ..` crashed the whole server at startup (stack overflow); now the scan
    /// completes, indexes the file once and reports the link in the health summary doctor prints.
    #[cfg(unix)]
    #[test]
    fn symlink_to_an_ancestor_reaches_index_health_and_survives_reload() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = dunce::canonicalize(tmp.path()).expect("canon");
        std::fs::create_dir_all(root.join("src")).expect("mkdir");
        std::fs::write(root.join("src/a.proto"), "syntax = \"proto3\"; package a;").expect("write");
        std::os::unix::fs::symlink("..", root.join("src/loop")).expect("symlink");
        let state = make_state(&root);
        let snap =
            WorkspaceIndexer::build_snapshot(&state.config, &state.allowed_roots, None, None, None);
        assert_eq!(snap.health.files_scanned, 1);
        assert_eq!(snap.health.dir_symlinks_not_followed, 1);
        let summary = snap.health.render_summary(10);
        assert!(
            summary.contains("1 directory symlink(s) not followed (cycle or outside root"),
            "{summary}"
        );
        state.install_snapshot(snap);
        std::os::unix::fs::symlink("..", root.join("src/loop2")).expect("symlink");
        WorkspaceIndexer::reload(&state);
        assert_eq!(state.snapshot().health.dir_symlinks_not_followed, 2);
    }

    #[test]
    fn per_file_call_fact_caps_reach_index_health_and_the_summary() {
        for (file, code) in [
            ("many_methods.ts", {
                let mut c = String::from("export class Big {\n");
                for k in 0..4100 {
                    c.push_str(&format!("  m{k}() {{}}\n"));
                }
                c + "}\n"
            }),
            ("many_calls.ts", {
                let mut c = String::from("export class Calls {\n  run() {\n");
                for k in 0..8100 {
                    c.push_str(&format!("    this.m{k}();\n"));
                }
                c + "  }\n}\n"
            }),
        ] {
            let tmp = tempfile::tempdir().expect("tempdir");
            let root = dunce::canonicalize(tmp.path()).expect("canon");
            std::fs::write(root.join(file), code).expect("write");
            let state = make_state(&root);
            let snap = WorkspaceIndexer::build_snapshot(
                &state.config,
                &state.allowed_roots,
                None,
                None,
                None,
            );
            assert_eq!(snap.health.files_call_facts_capped, 1, "{file}");
            let summary = snap.health.render_summary(10);
            assert!(
                summary.contains("1 file(s) with call facts capped"),
                "{file}: {summary}"
            );
        }
    }

    /// 9.0 calls layer, memory strategy: a Python file off every entry -> producer path keeps
    /// no call sites after a derive; an edit that brings it onto a path makes the indexer
    /// re-extract it (one restoring pass), so the live graph equals a fresh build and
    /// nothing is left counted as missing.
    #[test]
    fn a_file_brought_onto_a_call_path_gets_its_call_sites_back() {
        let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/eda/layered-py");
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = dunce::canonicalize(tmp.path()).expect("canon");
        let dir = root.join("src/layered_py");
        std::fs::create_dir_all(&dir).expect("mkdir");
        for entry in std::fs::read_dir(fixture.join("src/layered_py")).expect("fixture") {
            let from = entry.expect("entry").path();
            let text = std::fs::read_to_string(&from)
                .expect("read")
                .replace("\r\n", "\n");
            let name = from.file_name().expect("name").to_owned();
            let text = match name.to_str() {
                Some("handler.py") => format!("from layered_py.service import InvoiceService\n{text}")
                    .replace("def __init__(self, service):", "def __init__(self, service: InvoiceService):"),
                Some("service.py") => text
                    .replace(
                        "from layered_py.tasks import generate_report\n",
                        "from layered_py.tasks import generate_report\nfrom layered_py.publisher import InvoicePublisher\n",
                    )
                    .replace(
                        "def __init__(self, publisher):",
                        "def __init__(self, publisher: \"InvoicePublisher\"):",
                    ),
                _ => text,
            };
            std::fs::write(dir.join(name), text).expect("write");
        }
        // The consumer does not import the handler yet: handler, service and publisher
        // lie on no path.
        let consumer = dir.join("consumer.py");
        let annotated = std::fs::read_to_string(&consumer)
            .expect("read")
            .replace("\r\n", "\n");
        let with_import = annotated
            .replace(
                "from confluent_kafka import Consumer\n",
                "from confluent_kafka import Consumer\nfrom layered_py.handler import OrderHandler\n",
            )
            .replace("def __init__(self, handler):", "def __init__(self, handler: OrderHandler):");
        assert_ne!(with_import, annotated, "fixture changed: update the edit");

        let state = make_state(&root);
        let snap = {
            let mut vfs = state.vfs.lock().expect("vfs");
            WorkspaceIndexer::build_snapshot(
                &state.config,
                &state.allowed_roots,
                Some(&state.rescan),
                Some(&mut vfs),
                None,
            )
        };
        state.install_snapshot(snap);
        let names = |state: &AppState| {
            let view = state.snapshot();
            let g = &view.contract_graph;
            let mut v: Vec<(String, String)> = g
                .call_edges()
                .iter()
                .map(|e| {
                    let name = |id| {
                        g.callable_nodes()
                            .iter()
                            .find(|n| n.id == id)
                            .map_or(String::new(), |n| n.name.to_string())
                    };
                    (name(e.from), name(e.to))
                })
                .collect();
            v.sort();
            v
        };
        let before = names(&state);
        assert!(
            !before.iter().any(|e| e.0 == "OrderHandler.handle"),
            "{before:?}"
        );

        std::thread::sleep(std::time::Duration::from_millis(30));
        std::fs::write(&consumer, &with_import).expect("edit");
        WorkspaceIndexer::reload_paths(&state, &[consumer]);
        let after = names(&state);
        assert_eq!(
            after,
            vec![
                (
                    "InvoiceService.issue".to_string(),
                    "InvoicePublisher.announce".to_string()
                ),
                (
                    "OrderConsumer.run".to_string(),
                    "OrderHandler.handle".to_string()
                ),
                (
                    "OrderHandler.handle".to_string(),
                    "InvoiceService.issue".to_string()
                ),
                ("generate_report".to_string(), "build_report".to_string()),
            ]
        );
        let view = state.snapshot();
        assert_eq!(view.health.call_files_missing_sites, 0);
        assert!(view.contract_graph.call_files_missing_sites().is_empty());
    }

    /// Regression (step 4.12d review): two roots sharing a directory name
    /// (`.../a/api`, `.../b/api`) used to render the same display name, silently
    /// merging their nodes into one service in `visualize_mesh`'s aggregation
    /// (`Aggregate::build`/`group_of` group purely by this string). Disambiguated
    /// with the parent segment, symmetrically (both collide, not just the second)
    /// and deterministically (independent of scan order).
    #[test]
    fn repo_names_disambiguates_same_named_roots_by_parent() {
        let roots = vec![
            PathBuf::from("/w/a/api"),
            PathBuf::from("/w/b/api"),
            PathBuf::from("/w/billing"),
        ];
        assert_eq!(
            WorkspaceIndexer::repo_names(&roots),
            vec![
                "a/api".to_string(),
                "b/api".to_string(),
                "billing".to_string(),
            ]
        );

        // No collision: unchanged, bare names.
        let unique = vec![PathBuf::from("/w/a/api"), PathBuf::from("/w/billing")];
        assert_eq!(
            WorkspaceIndexer::repo_names(&unique),
            vec!["api".to_string(), "billing".to_string()]
        );

        // Order-independent: swapping the colliding roots gives the same names,
        // each still paired with its own root by position.
        let swapped = vec![
            PathBuf::from("/w/b/api"),
            PathBuf::from("/w/a/api"),
            PathBuf::from("/w/billing"),
        ];
        assert_eq!(
            WorkspaceIndexer::repo_names(&swapped),
            vec![
                "b/api".to_string(),
                "a/api".to_string(),
                "billing".to_string(),
            ]
        );
    }

    #[test]
    fn full_scan_then_differential_reload_handles_edit_and_delete() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = dunce::canonicalize(tmp.path()).expect("canon");
        std::fs::write(
            root.join("a.proto"),
            "syntax = \"proto3\"; package a; service A { rpc X (R) returns (S); }",
        )
        .expect("write a");
        std::fs::write(
            root.join("b.proto"),
            "syntax = \"proto3\"; package b; service B { rpc Y (R) returns (S); }",
        )
        .expect("write b");
        std::fs::write(root.join("doc.md"), "# Title\nsome text").expect("write md");

        let state = make_state(&root);
        let snap = {
            let mut vfs = state.vfs.lock().expect("vfs");
            WorkspaceIndexer::build_snapshot(
                &state.config,
                &state.allowed_roots,
                Some(&state.rescan),
                Some(&mut vfs),
                None,
            )
        };
        state.install_snapshot(snap);
        assert_eq!(state.snapshot().contract_graph.node_count(), 4);
        assert_eq!(state.snapshot().doc_index.section_count(), 1);
        assert_eq!(state.vfs.lock().expect("vfs").len(), 3);

        // Nothing changed → no new generation.
        WorkspaceIndexer::reload(&state);
        assert_eq!(state.snapshot().generation, 1);

        // Edit one file, delete another.
        std::thread::sleep(std::time::Duration::from_millis(30));
        std::fs::write(
            root.join("a.proto"),
            "syntax = \"proto3\"; package a; service A { rpc X (R) returns (S); rpc Z (R) returns (S); }",
        )
        .expect("rewrite a");
        std::fs::remove_file(root.join("b.proto")).expect("rm b");

        WorkspaceIndexer::reload(&state);
        let view = state.snapshot();
        assert_eq!(view.generation, 2);
        // a: service + 2 rpcs = 3 nodes; b gone.
        assert_eq!(view.contract_graph.node_count(), 3);
        assert!(view.contract_graph.search_symbols("B", None).is_empty());
        assert_eq!(state.vfs.lock().expect("vfs").len(), 2);
    }

    /// A cache-hit `build_snapshot` must produce byte-identical results to a cache-miss one:
    /// this is the whole safety condition step 3.2 relies on (`fold`'s `NodeId` assignment
    /// depends only on crawl order, never on whether a file's `FileIndex` came from a fresh
    /// parse or a deserialized cache entry). Guards against a future change to `FileIndex`'s
    /// serde shape, or to `extract_with_cache`'s plumbing, silently making a cache hit diverge
    /// from a fresh parse.
    #[test]
    fn warm_cache_produces_identical_snapshot_to_cold_parse() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = dunce::canonicalize(tmp.path()).expect("canon");
        std::fs::write(
            root.join("a.proto"),
            "syntax = \"proto3\"; package a; service A { rpc X (R) returns (S); }",
        )
        .expect("write a");
        std::fs::write(
            root.join("b.proto"),
            "syntax = \"proto3\"; package b; service B { rpc Y (R) returns (S); }",
        )
        .expect("write b");
        std::fs::write(root.join("doc.md"), "# Title\nsome text").expect("write md");

        let config =
            Config::load_from_str("[workspace]\nname = \"t\"\nversion = \"0\"\nroots = [\".\"]\n")
                .expect("config");
        let roots = vec![root.clone()];
        let cache = mesh_core::PersistentIndexCache::in_memory().expect("cache");

        let cold = WorkspaceIndexer::build_snapshot(&config, &roots, None, None, Some(&cache));
        assert_eq!(cold.contract_graph.node_count(), 4);
        assert_eq!(cold.doc_index.section_count(), 1);

        // Second build over the exact same files/config, now served entirely from the cache
        // this same `PersistentIndexCache` instance just populated.
        let warm = WorkspaceIndexer::build_snapshot(&config, &roots, None, None, Some(&cache));

        assert_eq!(cold.fingerprint(), warm.fingerprint());
        assert_eq!(warm.contract_graph.node_count(), 4);
        assert_eq!(warm.doc_index.section_count(), 1);
    }

    /// SQL facts (9.0 T5) survive the parse cache: a warm build is byte-identical to the cold one,
    /// a binary `.sql` file is a counted guard rejection, and a `.sql` file with a very long line
    /// or a `-- (` banner (rules that only protect a tree-sitter parse) is still read.
    #[test]
    fn sql_sites_survive_the_parse_cache_and_a_hostile_sql_file_is_counted() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = dunce::canonicalize(tmp.path()).expect("canon");
        std::fs::write(
            root.join("001.sql"),
            "INSERT INTO outbox (topic, payload) VALUES ('order.created', '{}');\nLISTEN order_ready;\n",
        )
        .expect("write sql");
        std::fs::write(
            root.join("repo.py"),
            "def go(c):\n    c.execute(\"SELECT pg_notify('stock_changed', 'x')\")\n",
        )
        .expect("write py");
        std::fs::write(root.join("binary.SQL"), b"NOTIFY hostile;\n\0\0").expect("write binary");
        std::fs::write(
            root.join("long.sql"),
            format!(
                "-- {}\n{}NOTIFY after_long;\n",
                "x".repeat(1_500),
                "-- (\n".repeat(70)
            ),
        )
        .expect("write long");
        let config =
            Config::load_from_str("[workspace]\nname = \"t\"\nversion = \"0\"\nroots = [\".\"]\n")
                .expect("config");
        let roots = vec![root.clone()];
        let cache = mesh_core::PersistentIndexCache::in_memory().expect("cache");

        let cold = WorkspaceIndexer::build_snapshot(&config, &roots, None, None, Some(&cache));
        let warm = WorkspaceIndexer::build_snapshot(&config, &roots, None, None, Some(&cache));
        assert_eq!(cold.fingerprint(), warm.fingerprint());
        let names = |snap: &MeshSnapshot| {
            let mut v: Vec<(String, usize)> = snap
                .contract_graph
                .all_nodes()
                .filter(|n| {
                    n.signature
                        .as_deref()
                        .is_some_and(|s| s.starts_with("sql "))
                })
                .map(|n| (n.name.to_string(), n.line_start))
                .collect();
            v.sort();
            v
        };
        let want = vec![
            ("after_long".to_string(), 72),
            ("order.created".to_string(), 1),
            ("order_ready".to_string(), 2),
            ("stock_changed".to_string(), 2),
        ];
        assert_eq!(names(&cold), want);
        assert_eq!(names(&warm), want);
        assert_eq!(cold.health.files_rejected_guard, 1);
        assert_eq!(cold.health.files_sql_capped, 0);
    }

    /// Caps and unresolved keys are counted, never silent (CLAUDE.md), and the counters survive
    /// the parse cache.
    #[test]
    fn sql_caps_and_unresolved_sites_reach_index_health() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = dunce::canonicalize(tmp.path()).expect("canon");
        std::fs::write(root.join("many.sql"), "NOTIFY c;\n".repeat(4_200)).expect("write many");
        std::fs::write(
            root.join("bind.sql"),
            "INSERT INTO outbox (topic, payload) VALUES ($1, '{}'), ('ok', '{}'), ($2, '{}');\n",
        )
        .expect("write bind");
        std::fs::write(
            root.join("repo.ts"),
            "q(\"INSERT INTO outbox (topic, payload) VALUES ($1, $2)\");\n",
        )
        .expect("write ts");
        let config =
            Config::load_from_str("[workspace]\nname = \"t\"\nversion = \"0\"\nroots = [\".\"]\n")
                .expect("config");
        let roots = vec![root.clone()];
        let cache = mesh_core::PersistentIndexCache::in_memory().expect("cache");
        for pass in ["cold", "warm"] {
            let snap = WorkspaceIndexer::build_snapshot(&config, &roots, None, None, Some(&cache));
            assert_eq!(snap.health.files_sql_capped, 1, "{pass}");
            assert_eq!(snap.health.sql_sites_unresolved, 3, "{pass}");
            assert!(snap.health.sql_truncated(), "{pass}");
            let summary = snap.health.render_summary(5);
            assert!(summary.contains("SQL scan cap"), "{summary}");
            assert!(summary.contains("3 SQL outbox insert"), "{summary}");
        }
    }

    /// The in-process site cap (9.0 T4) is counted, listed by the summary and survives the parse
    /// cache; a file under the cap is not counted.
    #[test]
    fn jvm_broker_site_cap_reaches_index_health_and_survives_the_cache() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = dunce::canonicalize(tmp.path()).expect("canon");
        let mut hostile = String::from(
            "package p;\nimport org.springframework.amqp.rabbit.core.RabbitTemplate;\npublic class Hostile {\n RabbitTemplate rabbit;\n",
        );
        for i in 0..2_100 {
            hostile.push_str(&format!(
                " void m{i}(Object o) {{ rabbit.convertAndSend(\"q{i}\", o); }}\n"
            ));
        }
        hostile.push_str("}\n");
        std::fs::write(root.join("Hostile.java"), hostile).expect("write hostile");
        std::fs::write(
            root.join("Ok.java"),
            "package p;\nimport org.springframework.amqp.rabbit.core.RabbitTemplate;\npublic class Ok {\n RabbitTemplate rabbit;\n void m(Object o) { rabbit.convertAndSend(\"q\", o); }\n}\n",
        )
        .expect("write ok");
        let config =
            Config::load_from_str("[workspace]\nname = \"t\"\nversion = \"0\"\nroots = [\".\"]\n")
                .expect("config");
        let roots = vec![root.clone()];
        let cache = mesh_core::PersistentIndexCache::in_memory().expect("cache");
        let mut prints = Vec::new();
        for pass in ["cold", "warm"] {
            let snap = WorkspaceIndexer::build_snapshot(&config, &roots, None, None, Some(&cache));
            assert_eq!(snap.health.files_jvm_brokers_capped, 1, "{pass}");
            assert!(snap.health.jvm_brokers_truncated(), "{pass}");
            let summary = snap.health.render_summary(5);
            assert!(summary.contains("JVM broker site cap"), "{summary}");
            prints.push(snap.fingerprint());
        }
        assert_eq!(prints[0], prints[1]);
    }

    /// The Go broker site cap (9.0 T8b) is counted, listed by the summary and survives the parse
    /// cache; a file under the cap is not counted.
    #[test]
    fn go_broker_site_cap_reaches_index_health_and_survives_the_cache() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = dunce::canonicalize(tmp.path()).expect("canon");
        let mut hostile = String::from(
            "package p
import \"github.com/nats-io/nats.go\"\nfunc Hostile(nc *nats.Conn) {\n",
        );
        for i in 0..2_100 {
            hostile.push_str(&format!("\tnc.Publish(\"q{i}\", nil)\n"));
        }
        hostile.push_str("}\n");
        std::fs::write(root.join("hostile.go"), hostile).expect("write hostile");
        std::fs::write(
            root.join("ok.go"),
            "package p\nimport \"github.com/nats-io/nats.go\"\nfunc Ok(nc *nats.Conn) { nc.Publish(\"q\", nil) }\n",
        )
        .expect("write ok");
        let config =
            Config::load_from_str("[workspace]\nname = \"t\"\nversion = \"0\"\nroots = [\".\"]\n")
                .expect("config");
        let roots = vec![root.clone()];
        let cache = mesh_core::PersistentIndexCache::in_memory().expect("cache");
        let mut prints = Vec::new();
        for pass in ["cold", "warm"] {
            let snap = WorkspaceIndexer::build_snapshot(&config, &roots, None, None, Some(&cache));
            assert_eq!(snap.health.files_go_brokers_capped, 1, "{pass}");
            assert!(snap.health.go_brokers_truncated(), "{pass}");
            let summary = snap.health.render_summary(5);
            assert!(summary.contains("Go broker site cap"), "{summary}");
            prints.push(snap.fingerprint());
        }
        assert_eq!(prints[0], prints[1]);
    }

    /// The Python broker site cap (9.0 T8c) is counted, listed by the summary and survives the
    /// parse cache; a file under the cap is not counted.
    #[test]
    fn py_broker_site_cap_reaches_index_health_and_survives_the_cache() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = dunce::canonicalize(tmp.path()).expect("canon");
        let mut hostile = String::from("import redis\nr = redis.Redis()\n");
        for i in 0..2_100 {
            hostile.push_str(&format!("def m{i}():\n    r.publish(\"q{i}\", 1)\n"));
        }
        std::fs::write(root.join("hostile.py"), hostile).expect("write hostile");
        std::fs::write(
            root.join("ok.py"),
            "import redis\nr = redis.Redis()\nr.publish(\"q\", 1)\n",
        )
        .expect("write ok");
        let config =
            Config::load_from_str("[workspace]\nname = \"t\"\nversion = \"0\"\nroots = [\".\"]\n")
                .expect("config");
        let roots = vec![root.clone()];
        let cache = mesh_core::PersistentIndexCache::in_memory().expect("cache");
        let mut prints = Vec::new();
        for pass in ["cold", "warm"] {
            let snap = WorkspaceIndexer::build_snapshot(&config, &roots, None, None, Some(&cache));
            assert_eq!(snap.health.files_py_brokers_capped, 1, "{pass}");
            assert!(snap.health.py_brokers_truncated(), "{pass}");
            let summary = snap.health.render_summary(5);
            assert!(summary.contains("Python broker site cap"), "{summary}");
            prints.push(snap.fingerprint());
        }
        assert_eq!(prints[0], prints[1]);
    }

    /// The C# broker site cap (9.0 T8d) is counted, listed by the summary and survives the parse
    /// cache; a file under the cap is not counted.
    #[test]
    fn cs_broker_site_cap_reaches_index_health_and_survives_the_cache() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = dunce::canonicalize(tmp.path()).expect("canon");
        let mut hostile = String::from(
            "using MassTransit;\npublic class H\n{\n    private readonly IPublishEndpoint _bus;\n    public void Go()\n    {\n",
        );
        for i in 0..2_100 {
            hostile.push_str(&format!("        _bus.Publish(new Evt{i}());\n"));
        }
        hostile.push_str("    }\n}\n");
        std::fs::write(root.join("Hostile.cs"), hostile).expect("write hostile");
        std::fs::write(
            root.join("Ok.cs"),
            "using MassTransit;\npublic class Ok { private readonly IPublishEndpoint _b; public void G() { _b.Publish(new E()); } }\n",
        )
        .expect("write ok");
        let config =
            Config::load_from_str("[workspace]\nname = \"t\"\nversion = \"0\"\nroots = [\".\"]\n")
                .expect("config");
        let roots = vec![root.clone()];
        let cache = mesh_core::PersistentIndexCache::in_memory().expect("cache");
        let mut prints = Vec::new();
        for pass in ["cold", "warm"] {
            let snap = WorkspaceIndexer::build_snapshot(&config, &roots, None, None, Some(&cache));
            assert_eq!(snap.health.files_cs_brokers_capped, 1, "{pass}");
            assert!(snap.health.cs_brokers_truncated(), "{pass}");
            let summary = snap.health.render_summary(5);
            assert!(summary.contains("C# broker site cap"), "{summary}");
            prints.push(snap.fingerprint());
        }
        assert_eq!(prints[0], prints[1]);
    }

    #[test]
    fn inproc_site_cap_reaches_index_health_and_survives_the_cache() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = dunce::canonicalize(tmp.path()).expect("canon");
        let mut hostile =
            String::from("import { OnEvent } from '@nestjs/event-emitter';\nexport class L {\n");
        for i in 0..2_100 {
            hostile.push_str(&format!("@OnEvent('e.{i}') h{i}() {{}}\n"));
        }
        hostile.push_str("}\n");
        std::fs::write(root.join("hostile.ts"), hostile).expect("write hostile");
        std::fs::write(
            root.join("ok.ts"),
            "import { OnEvent } from '@nestjs/event-emitter';\nexport class K {\n @OnEvent('a') h() {}\n}\n",
        )
        .expect("write ok");
        let config =
            Config::load_from_str("[workspace]\nname = \"t\"\nversion = \"0\"\nroots = [\".\"]\n")
                .expect("config");
        let roots = vec![root.clone()];
        let cache = mesh_core::PersistentIndexCache::in_memory().expect("cache");
        let mut prints = Vec::new();
        for pass in ["cold", "warm"] {
            let snap = WorkspaceIndexer::build_snapshot(&config, &roots, None, None, Some(&cache));
            assert_eq!(snap.health.files_inproc_capped, 1, "{pass}");
            assert!(snap.health.inproc_truncated(), "{pass}");
            let summary = snap.health.render_summary(5);
            assert!(summary.contains("in-process bus site cap"), "{summary}");
            prints.push(snap.fingerprint());
        }
        assert_eq!(prints[0], prints[1]);
    }

    /// The path-driven `reload_paths` must produce the exact same result as the
    /// crawl-driven `reload` for the same edit+delete, without ever calling
    /// `crawl_all` — proving the watcher's own event paths are enough.
    #[test]
    fn reload_paths_handles_edit_and_delete_without_a_full_crawl() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = dunce::canonicalize(tmp.path()).expect("canon");
        let a_path = root.join("a.proto");
        let b_path = root.join("b.proto");
        std::fs::write(
            &a_path,
            "syntax = \"proto3\"; package a; service A { rpc X (R) returns (S); }",
        )
        .expect("write a");
        std::fs::write(
            &b_path,
            "syntax = \"proto3\"; package b; service B { rpc Y (R) returns (S); }",
        )
        .expect("write b");

        let state = make_state(&root);
        let snap = {
            let mut vfs = state.vfs.lock().expect("vfs");
            WorkspaceIndexer::build_snapshot(
                &state.config,
                &state.allowed_roots,
                Some(&state.rescan),
                Some(&mut vfs),
                None,
            )
        };
        state.install_snapshot(snap);
        assert_eq!(state.snapshot().contract_graph.node_count(), 4);

        std::thread::sleep(std::time::Duration::from_millis(30));
        std::fs::write(
            &a_path,
            "syntax = \"proto3\"; package a; service A { rpc X (R) returns (S); rpc Z (R) returns (S); }",
        )
        .expect("rewrite a");
        std::fs::remove_file(&b_path).expect("rm b");

        // Only the two paths the watcher actually reported change — a directory
        // never crawled at all is proof this didn't fall back to a full scan.
        WorkspaceIndexer::reload_paths(&state, &[a_path, b_path]);
        let view = state.snapshot();
        assert_eq!(view.generation, 2);
        assert_eq!(
            view.contract_graph.node_count(),
            3,
            "a: service + 2 rpcs; b gone"
        );
        assert!(view.contract_graph.search_symbols("B", None).is_empty());
        assert_eq!(
            state.vfs.lock().expect("vfs").len(),
            1,
            "only a.proto remains tracked"
        );
    }

    /// A brand-new file the watcher reports (a `Create` event) must be indexed by
    /// `reload_paths` even though it was never part of any prior crawl or VFS entry.
    #[test]
    fn reload_paths_indexes_a_newly_created_file() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = dunce::canonicalize(tmp.path()).expect("canon");
        let state = make_state(&root);
        state.install_snapshot(MeshSnapshot::default());

        let c_path = root.join("c.proto");
        std::fs::write(
            &c_path,
            "syntax = \"proto3\"; package c; service C { rpc Z (R) returns (S); }",
        )
        .expect("write c");

        WorkspaceIndexer::reload_paths(&state, &[c_path]);
        let view = state.snapshot();
        assert_eq!(view.contract_graph.node_count(), 2, "service + 1 rpc");
    }

    /// `reload_paths` stats a path *before* `reload_lock` is acquired (it must
    /// return, not block, before falling back to `reload()`). If the file is
    /// briefly absent at that moment (an editor's atomic save, or a fast
    /// delete-then-recreate) but exists again by the time `apply_incremental`
    /// actually runs under the lock, it must be re-indexed, not removed from
    /// the graph and left stale.
    #[test]
    fn apply_incremental_reindexes_a_path_that_resurrected_before_the_lock() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = dunce::canonicalize(tmp.path()).expect("canon");
        let a_path = root.join("a.proto");
        std::fs::write(
            &a_path,
            "syntax = \"proto3\"; package a; service A { rpc X (R) returns (S); }",
        )
        .expect("write a");

        let state = make_state(&root);
        let snap = {
            let mut vfs = state.vfs.lock().expect("vfs");
            WorkspaceIndexer::build_snapshot(
                &state.config,
                &state.allowed_roots,
                Some(&state.rescan),
                Some(&mut vfs),
                None,
            )
        };
        state.install_snapshot(snap);
        assert_eq!(state.snapshot().contract_graph.node_count(), 2);

        // Simulate the race directly: `apply_incremental` is handed `a_path` as
        // already-deleted (as `reload_paths` would if its earlier stat had
        // raced a delete), but the file is actually back on disk by the time
        // this runs — exactly as it would be after a real resurrection.
        WorkspaceIndexer::apply_incremental(
            &state,
            "Test",
            Vec::new(),
            vec![a_path],
            Some(&state.rescan),
            None,
        );

        let view = state.snapshot();
        assert_eq!(
            view.contract_graph.node_count(),
            2,
            "a resurrected path must be re-indexed, not silently dropped from the graph"
        );
    }

    /// `reload_paths` must not index a path that a real crawl would have pruned
    /// via `exclude_patterns` — the same exclusion `crawl_all` applies, checked
    /// here for one explicit path instead of by walking the tree.
    #[test]
    fn reload_paths_respects_exclude_patterns() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = dunce::canonicalize(tmp.path()).expect("canon");
        let cfg = Config::load_from_str(
            "[workspace]\nname = \"t\"\nversion = \"0\"\nroots = [\".\"]\nexclude_patterns = [\"secrets/**\"]\n",
        )
        .expect("config");
        let audit = Arc::new(AuditLogger::new_in_memory().expect("audit"));
        let rescan = Arc::new(BackgroundRescanEngine::new().expect("rescan"));
        let state = Arc::new(AppState::new(cfg, vec![root.clone()], audit, rescan));
        state.install_snapshot(MeshSnapshot::default());

        std::fs::create_dir_all(root.join("secrets")).expect("mkdir");
        let secret_path = root.join("secrets").join("leaked.proto");
        std::fs::write(
            &secret_path,
            "syntax = \"proto3\"; package s; service Secret { rpc X (R) returns (S); }",
        )
        .expect("write");

        WorkspaceIndexer::reload_paths(&state, &[secret_path]);
        assert_eq!(
            state.snapshot().contract_graph.node_count(),
            0,
            "an excluded path must not be indexed even when reported directly by the watcher"
        );
    }

    /// A `.git/HEAD` change (branch checkout) can alter files without each one
    /// necessarily producing its own watcher event, so `reload_paths` must fall
    /// back to a full `reload` rather than trust the reported path set alone.
    #[test]
    fn reload_paths_falls_back_to_full_reload_on_git_ref_change() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = dunce::canonicalize(tmp.path()).expect("canon");
        std::fs::write(
            root.join("a.proto"),
            "syntax = \"proto3\"; package a; service A { rpc X (R) returns (S); }",
        )
        .expect("write a");
        let state = make_state(&root);
        state.install_snapshot(MeshSnapshot::default());

        // No .proto path is in the event set at all — only a git ref path — yet
        // the real on-disk file must still be picked up via the full-reload
        // fallback, proving the fallback actually ran rather than silently
        // no-oping on an event set with nothing indexable in it.
        WorkspaceIndexer::reload_paths(&state, &[root.join(".git").join("HEAD")]);
        assert_eq!(state.snapshot().contract_graph.node_count(), 2);
    }

    /// A symlink created inside a watched root pointing at a file *outside*
    /// every allowed root must never have its target indexed — reload_paths
    /// canonicalizes before resolving a root specifically so this can't happen
    /// (CLAUDE.md Commandment 4: never follow a symlink out of the sandbox).
    #[cfg(unix)]
    #[test]
    fn reload_paths_never_indexes_a_symlink_escaping_every_root() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let base = dunce::canonicalize(tmp.path()).expect("canon");
        let root = base.join("workspace");
        std::fs::create_dir_all(&root).expect("mkdir workspace");
        let outside = base.join("outside.proto");
        std::fs::write(
            &outside,
            "syntax = \"proto3\"; package secret; service Leaked { rpc X (R) returns (S); }",
        )
        .expect("write outside");
        let link = root.join("leak.proto");
        std::os::unix::fs::symlink(&outside, &link).expect("symlink");

        let state = make_state(&root);
        state.install_snapshot(MeshSnapshot::default());

        WorkspaceIndexer::reload_paths(&state, &[link]);
        assert_eq!(
            state.snapshot().contract_graph.node_count(),
            0,
            "a symlink resolving outside every allowed root must never be indexed"
        );
    }

    /// A directory-level delete (`rm -rf service/`) can coalesce into fewer
    /// watcher events than one per contained file. `reload_paths` detects this
    /// via the deleted path's parent also being gone and falls back to a full
    /// reload, which correctly sweeps every now-missing tracked file — not just
    /// the one path the watcher happened to report.
    #[test]
    fn reload_paths_falls_back_to_full_reload_when_parent_directory_is_also_gone() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = dunce::canonicalize(tmp.path()).expect("canon");
        std::fs::create_dir_all(root.join("service")).expect("mkdir");
        std::fs::write(
            root.join("service/a.proto"),
            "syntax = \"proto3\"; package a; service A { rpc X (R) returns (S); }",
        )
        .expect("write a");
        std::fs::write(
            root.join("service/b.proto"),
            "syntax = \"proto3\"; package b; service B { rpc Y (R) returns (S); }",
        )
        .expect("write b");

        let state = make_state(&root);
        let snap = {
            let mut vfs = state.vfs.lock().expect("vfs");
            WorkspaceIndexer::build_snapshot(
                &state.config,
                &state.allowed_roots,
                Some(&state.rescan),
                Some(&mut vfs),
                None,
            )
        };
        state.install_snapshot(snap);
        assert_eq!(state.snapshot().contract_graph.node_count(), 4);

        // The whole directory is removed, but only `a.proto`'s deletion is in
        // the reported event set — `b.proto`'s own event was "lost" (exactly
        // what a coalesced directory-level delete can look like).
        std::fs::remove_dir_all(root.join("service")).expect("rm -rf service");
        WorkspaceIndexer::reload_paths(&state, &[root.join("service/a.proto")]);

        let view = state.snapshot();
        assert_eq!(
            view.contract_graph.node_count(),
            0,
            "the full-reload fallback must clean up b.proto's nodes too, not just a.proto's"
        );
    }

    /// `PropertyRegistry` provenance: deleting one of two properties files must remove
    /// exactly its keys on the next incremental reload, leaving the other file's keys intact.
    #[test]
    fn reload_removes_deleted_properties_files_keys_only() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = dunce::canonicalize(tmp.path()).expect("canon");
        std::fs::write(root.join("a.properties"), "app.a.name=a-value\n").expect("write a");
        std::fs::write(root.join("b.properties"), "app.b.name=b-value\n").expect("write b");

        let state = make_state(&root);
        let snap = {
            let mut vfs = state.vfs.lock().expect("vfs");
            WorkspaceIndexer::build_snapshot(
                &state.config,
                &state.allowed_roots,
                Some(&state.rescan),
                Some(&mut vfs),
                None,
            )
        };
        state.install_snapshot(snap);
        assert_eq!(
            state.snapshot().property_registry.get("app.a.name"),
            Some("a-value")
        );
        assert_eq!(
            state.snapshot().property_registry.get("app.b.name"),
            Some("b-value")
        );

        std::fs::remove_file(root.join("b.properties")).expect("rm b");
        WorkspaceIndexer::reload(&state);

        let view = state.snapshot();
        assert_eq!(view.generation, 2);
        assert_eq!(view.property_registry.get("app.a.name"), Some("a-value"));
        assert_eq!(view.property_registry.get("app.b.name"), None);
    }

    /// A file the guards refuse (here a 3 KB line) is not read again by a
    /// differential reload while its stat is unchanged, and is again once it moves.
    /// Reading it again on every reload is what made a config reload on a
    /// workspace with many such files crawl behind the QoS-background pool.
    #[test]
    fn a_guard_rejected_file_is_not_reprocessed_until_it_changes() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = dunce::canonicalize(tmp.path()).expect("canon");
        let long = root.join("bundle.ts");
        std::fs::write(&long, format!("export const a = '{}';\n", "x".repeat(3000)))
            .expect("write");
        std::fs::write(root.join("ok.ts"), "export class Fine {}\n").expect("write ok");

        let state = make_state(&root);
        let snap = {
            let mut vfs = state.vfs.lock().expect("vfs");
            WorkspaceIndexer::build_snapshot(
                &state.config,
                &state.allowed_roots,
                None,
                Some(&mut vfs),
                None,
            )
        };
        state.install_snapshot(snap);
        assert_eq!(state.snapshot().health.rejected.len(), 1);
        {
            let vfs = state.vfs.lock().expect("vfs");
            let meta = std::fs::metadata(&long).expect("meta");
            assert!(vfs.is_rejection_unchanged(&long, &meta));
        }

        // Nothing moved: the reload finds no candidate, the snapshot is untouched.
        WorkspaceIndexer::reload(&state);
        let view = state.snapshot();
        assert_eq!(view.generation, 1);
        assert_eq!(view.health.rejected.len(), 1);
        drop(view);

        // Fixed on disk: it is picked up and indexed, no longer rejected.
        std::fs::write(&long, "export class Bundle {}\n").expect("fix");
        WorkspaceIndexer::reload(&state);
        let view = state.snapshot();
        assert!(
            view.health.rejected.is_empty(),
            "{:?}",
            view.health.rejected
        );
        assert!(view
            .contract_graph
            .all_nodes()
            .any(|n| n.name.as_str() == "Bundle"));
    }

    /// A refused file that is deleted leaves nothing in the refusal memo, so the
    /// same path recreated with the same size and mtime but fixed content is
    /// indexed instead of being taken for the file that was refused.
    #[test]
    fn a_recreated_path_with_the_same_stat_is_not_hidden_by_a_stale_refusal() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = dunce::canonicalize(tmp.path()).expect("canon");
        let path = root.join("bundle.ts");
        let long = format!("export const a = '{}';\n", "x".repeat(3000));
        std::fs::write(&path, &long).expect("write");
        let mtime = std::fs::metadata(&path)
            .and_then(|m| m.modified())
            .expect("mtime");

        let state = make_state(&root);
        let snap = {
            let mut vfs = state.vfs.lock().expect("vfs");
            WorkspaceIndexer::build_snapshot(
                &state.config,
                &state.allowed_roots,
                None,
                Some(&mut vfs),
                None,
            )
        };
        state.install_snapshot(snap);
        assert_eq!(state.vfs.lock().expect("vfs").rejected_len(), 1);

        std::fs::remove_file(&path).expect("rm");
        WorkspaceIndexer::reload(&state);
        assert_eq!(
            state.vfs.lock().expect("vfs").rejected_len(),
            0,
            "memo pruned"
        );

        // Same size, same mtime, but lines short enough to pass the guards.
        let mut fixed = String::from("export class Recreated {}\n");
        while fixed.len() < long.len() {
            fixed.push('\n');
        }
        fixed.truncate(long.len());
        std::fs::write(&path, &fixed).expect("recreate");
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .and_then(|f| f.set_modified(mtime))
            .expect("restore mtime");
        WorkspaceIndexer::reload(&state);
        let view = state.snapshot();
        assert!(
            view.health.rejected.is_empty(),
            "{:?}",
            view.health.rejected
        );
        assert!(view
            .contract_graph
            .all_nodes()
            .any(|n| n.name.as_str() == "Recreated"));
    }

    /// `[engines.contracts.spring]` `property_files` / `auto_redact_secrets` /
    /// `resolve_placeholders` configuration changes scan behaviour.
    #[test]
    fn spring_config_keys_change_scan_behaviour() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = dunce::canonicalize(tmp.path()).expect("canon");
        std::fs::write(
            root.join("application.properties"),
            "app.secret.token=raw_token_value\napp.greeting=${app.name:World}\n",
        )
        .expect("write application.properties");
        std::fs::write(root.join("other.properties"), "other.key=other-value\n")
            .expect("write other.properties");

        let cfg = Config::load_from_str(
            r#"
[workspace]
name = "t"
version = "0"
roots = ["."]

[engines.contracts.spring]
property_files = ["application*.properties"]
auto_redact_secrets = false
resolve_placeholders = true
"#,
        )
        .expect("config");
        let audit = Arc::new(AuditLogger::new_in_memory().expect("audit"));
        let rescan = Arc::new(BackgroundRescanEngine::new().expect("rescan"));
        let state = Arc::new(AppState::new(cfg, vec![root.clone()], audit, rescan));

        let snap = {
            let mut vfs = state.vfs.lock().expect("vfs");
            WorkspaceIndexer::build_snapshot(
                &state.config,
                &state.allowed_roots,
                Some(&state.rescan),
                Some(&mut vfs),
                None,
            )
        };
        state.install_snapshot(snap);
        let view = state.snapshot();

        // property_files scoped to application*.properties: other.properties is not ingested.
        assert_eq!(view.property_registry.get("other.key"), None);
        // auto_redact_secrets = false: a key that would normally be masked stays raw.
        assert_eq!(
            view.property_registry.get("app.secret.token"),
            Some("raw_token_value")
        );
        // resolve_placeholders = true: the ${app.name:World} default is resolved in place.
        assert_eq!(view.property_registry.get("app.greeting"), Some("World"));
    }

    /// `[engines.docs] enabled = false` must stop `.md` files from being indexed
    /// at all, instead of the flag being accepted-but-ignored.
    #[test]
    fn docs_engine_disabled_skips_markdown_indexing() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = dunce::canonicalize(tmp.path()).expect("canon");
        std::fs::write(root.join("doc.md"), "# Title\nsome text").expect("write md");

        let cfg = Config::load_from_str(
            "[workspace]\nname = \"t\"\nversion = \"0\"\nroots = [\".\"]\n\n[engines.docs]\nenabled = false\n",
        )
        .expect("config");
        let snapshot =
            WorkspaceIndexer::build_snapshot(&cfg, std::slice::from_ref(&root), None, None, None);
        assert_eq!(
            snapshot.doc_index.section_count(),
            0,
            "docs engine disabled must index no markdown sections"
        );
    }

    /// `[engines.contracts] enabled = false` must stop symbol/contract extraction,
    /// instead of the flag being accepted-but-ignored.
    #[test]
    fn contracts_engine_disabled_skips_extraction() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = dunce::canonicalize(tmp.path()).expect("canon");
        std::fs::write(
            root.join("a.proto"),
            "syntax = \"proto3\"; package a; service A { rpc X (R) returns (S); }",
        )
        .expect("write a");

        let cfg = Config::load_from_str(
            "[workspace]\nname = \"t\"\nversion = \"0\"\nroots = [\".\"]\n\n[engines.contracts]\nenabled = false\n",
        )
        .expect("config");
        let snapshot =
            WorkspaceIndexer::build_snapshot(&cfg, std::slice::from_ref(&root), None, None, None);
        assert_eq!(
            snapshot.contract_graph.node_count(),
            0,
            "contracts engine disabled must extract no contract nodes"
        );
    }

    /// `[engines.docs] paths` must scope which markdown files get indexed instead
    /// of every `.md` file under `roots` being indexed regardless of the setting.
    #[test]
    fn docs_paths_scopes_markdown_indexing() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = dunce::canonicalize(tmp.path()).expect("canon");
        std::fs::create_dir_all(root.join("docs")).expect("mkdir docs");
        std::fs::write(root.join("docs").join("adr.md"), "# ADR\nsome text").expect("write adr");
        std::fs::write(root.join("README.md"), "# Readme\nother text").expect("write readme");

        let cfg = Config::load_from_str(
            "[workspace]\nname = \"t\"\nversion = \"0\"\nroots = [\".\"]\n\n[engines.docs]\npaths = [\"docs/**\"]\n",
        )
        .expect("config");
        let snapshot =
            WorkspaceIndexer::build_snapshot(&cfg, std::slice::from_ref(&root), None, None, None);
        assert_eq!(
            snapshot.doc_index.section_count(),
            1,
            "only docs/** should be indexed when [engines.docs] paths is set"
        );
    }

    /// `[engines.docs] paths` containing `${workspace_root}/docs/**` must be expanded
    /// properly so markdown files in `docs/` are indexed.
    #[test]
    fn docs_paths_workspace_root_expansion_scopes_indexing() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = dunce::canonicalize(tmp.path()).expect("canon");
        std::fs::create_dir_all(root.join("docs")).expect("mkdir docs");
        std::fs::write(root.join("docs").join("adr.md"), "# ADR\nsome text").expect("write adr");
        std::fs::write(root.join("README.md"), "# Readme\nother text").expect("write readme");

        let cfg = Config::load_from_str(
            "[workspace]\nname = \"t\"\nversion = \"0\"\nroots = [\".\"]\n\n[engines.docs]\npaths = [\"${workspace_root}/docs/**\"]\n",
        )
        .expect("config");
        let snapshot =
            WorkspaceIndexer::build_snapshot(&cfg, std::slice::from_ref(&root), None, None, None);
        assert_eq!(
            snapshot.doc_index.section_count(),
            1,
            "docs/** expanded from ${{workspace_root}}/docs/** should be indexed"
        );
    }

    /// A file under a root nested inside another configured root (the common
    /// `roots = [".", "./services/*"]` shape) is indexed exactly once, attributed to
    /// the more specific (nested) root — not to both.
    #[test]
    fn crawl_all_attributes_overlapping_files_to_the_most_specific_root() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let base = dunce::canonicalize(tmp.path()).expect("canon");
        std::fs::create_dir_all(base.join("services/billing")).expect("mkdir");
        std::fs::write(base.join("services/billing/Widget.java"), "class Widget {}")
            .expect("write");
        std::fs::write(base.join("README.md"), "# root file").expect("write");

        let cfg = Config::load_from_str(
            "[workspace]\nname = \"t\"\nversion = \"0\"\nroots = [\".\", \"./services/*\"]\n",
        )
        .expect("config");
        let roots = vec![base.clone(), base.join("services/billing")];

        let files = WorkspaceIndexer::crawl_all(&cfg, &roots);
        let widget_hits: Vec<RepoId> = files
            .iter()
            .filter(|(_, p)| p.ends_with("Widget.java"))
            .map(|(repo_id, _)| *repo_id)
            .collect();
        assert_eq!(
            widget_hits,
            vec![1],
            "Widget.java must be crawled exactly once, attributed to the more specific \
             root (repo_id 1 = services/billing), got: {widget_hits:?}"
        );

        let readme_hits = files
            .iter()
            .filter(|(_, p)| p.ends_with("README.md"))
            .count();
        assert_eq!(
            readme_hits, 1,
            "README.md (only under the enclosing root) must still be crawled once"
        );
    }

    /// `exclude_patterns_for_root` must add a `<relative>/**` exclusion for every
    /// strict descendant root and nothing for a disjoint or non-nested one.
    #[test]
    fn exclude_patterns_for_root_only_excludes_strict_descendants() {
        let base = PathBuf::from("/ws");
        let roots = vec![
            base.clone(),
            base.join("services/billing"),
            base.join("services/checkout"),
            PathBuf::from("/other/ws"),
        ];
        let patterns =
            WorkspaceIndexer::exclude_patterns_for_root(&["**/*.pem".to_string()], &roots, &base);
        assert_eq!(
            patterns,
            vec![
                "**/*.pem".to_string(),
                "services/billing/**".to_string(),
                "services/checkout/**".to_string(),
            ]
        );

        // The most specific root has no descendants to exclude among its siblings.
        let nested = WorkspaceIndexer::exclude_patterns_for_root(&[], &roots, &roots[1]);
        assert!(nested.is_empty());
    }

    #[test]
    fn tsconfig_aliases_reach_the_snapshot_and_key_the_index_cache() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let base = dunce::canonicalize(tmp.path()).expect("canon");
        std::fs::create_dir_all(base.join("src/services")).expect("mkdir");
        std::fs::create_dir_all(base.join("src/app")).expect("mkdir");
        std::fs::write(
            base.join("tsconfig.json"),
            r#"{ "compilerOptions": { "baseUrl": ".", "paths": { "@/*": ["src/*"] } } }"#,
        )
        .expect("write");
        std::fs::write(
            base.join("src/services/User.ts"),
            "export default class User {}\n",
        )
        .expect("write");
        std::fs::write(
            base.join("src/app/main.ts"),
            "import User from '@/services/User';\nexport class Main { u: User }\n",
        )
        .expect("write");

        let cfg =
            Config::load_from_str("[workspace]\nname = \"t\"\nversion = \"0\"\nroots = [\".\"]\n")
                .expect("config");
        let roots = vec![base.clone()];
        let snapshot = WorkspaceIndexer::build_snapshot(&cfg, &roots, None, None, None);
        let graph = &snapshot.contract_graph;
        let linked = graph
            .all_edges()
            .iter()
            .filter(|e| e.kind == mesh_core::EdgeKind::Imports)
            .any(|e| {
                graph.get_node(e.from).map(|n| n.name.as_str()) == Some("Main")
                    && graph.get_node(e.to).map(|n| n.name.as_str()) == Some("User")
            });
        assert!(linked, "Main -> User through the `@/` alias");

        // The alias map is part of a script's cache key; a config that governs
        // nothing is not.
        let extract_cfg = WorkspaceIndexer::extract_config(&cfg, &roots);
        let main_ts = base.join("src/app/main.ts");
        assert!(extract_cfg.ts_aliases.fingerprint_input(&main_ts).is_some());
        assert!(extract_cfg
            .ts_aliases
            .fingerprint_input(&base.join("tsconfig.json"))
            .is_none());
        // Deterministic across rebuilds of the config.
        assert_eq!(
            WorkspaceIndexer::config_fingerprint(&extract_cfg),
            WorkspaceIndexer::config_fingerprint(&WorkspaceIndexer::extract_config(&cfg, &roots))
        );
    }

    /// A `tsconfig.json` edit reaches the scripts it governs without their bytes
    /// changing: an incremental reload equals a full rebuild (invariant I2).
    #[test]
    fn a_tsconfig_edit_re_resolves_unchanged_scripts_incrementally() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = dunce::canonicalize(tmp.path()).expect("canon");
        for dir in ["src/services", "lib/services", "src/app"] {
            std::fs::create_dir_all(root.join(dir)).expect("mkdir");
        }
        let tsconfig = |target: &str| {
            format!(
                r#"{{ "compilerOptions": {{ "baseUrl": ".", "paths": {{ "@/*": ["{target}/*"] }} }} }}"#
            )
        };
        std::fs::write(root.join("tsconfig.json"), tsconfig("src")).expect("write");
        std::fs::write(
            root.join("src/services/User.ts"),
            "export default class User {}\n",
        )
        .expect("write");
        std::fs::write(
            root.join("lib/services/User.ts"),
            "export default class User {}\n",
        )
        .expect("write");
        std::fs::write(
            root.join("src/app/main.ts"),
            "import User from '@/services/User';\nexport class Main { u: User }\n",
        )
        .expect("write");

        let state = make_state(&root);
        let snap = {
            let mut vfs = state.vfs.lock().expect("vfs");
            WorkspaceIndexer::build_snapshot(
                &state.config,
                &state.allowed_roots,
                Some(&state.rescan),
                Some(&mut vfs),
                None,
            )
        };
        state.install_snapshot(snap);
        let import_target = |state: &AppState| -> Vec<String> {
            let snapshot = state.snapshot();
            let graph = &snapshot.contract_graph;
            let mut out: Vec<String> = graph
                .all_edges()
                .iter()
                .filter(|e| e.kind == mesh_core::EdgeKind::Imports)
                .filter(|e| graph.get_node(e.from).map(|n| n.name.as_str()) == Some("Main"))
                .filter_map(|e| graph.get_node(e.to))
                .map(|n| {
                    n.file_path
                        .strip_prefix(&root)
                        .unwrap_or(&n.file_path)
                        .to_string_lossy()
                        .replace('\\', "/")
                })
                .collect();
            out.sort();
            out
        };
        assert_eq!(import_target(&state), vec!["src/services/User.ts"]);

        std::thread::sleep(std::time::Duration::from_millis(30));
        std::fs::write(root.join("tsconfig.json"), tsconfig("lib")).expect("rewrite");
        WorkspaceIndexer::reload_paths(&state, &[root.join("tsconfig.json")]);
        assert_eq!(import_target(&state), vec!["lib/services/User.ts"]);
    }

    /// Builds a state over `root`, indexes it once, and returns it.
    fn indexed_state(root: &Path) -> Arc<AppState> {
        let state = make_state(root);
        let snap = {
            let mut vfs = state.vfs.lock().expect("vfs");
            WorkspaceIndexer::build_snapshot(
                &state.config,
                &state.allowed_roots,
                Some(&state.rescan),
                Some(&mut vfs),
                None,
            )
        };
        state.install_snapshot(snap);
        state
    }

    /// Invariant I2: after an incremental reload the snapshot equals a full rebuild.
    fn assert_incremental_equals_full(state: &AppState, what: &str) {
        let full =
            WorkspaceIndexer::build_snapshot(&state.config, &state.allowed_roots, None, None, None);
        assert_eq!(
            state.snapshot().fingerprint(),
            full.fingerprint(),
            "{what}: incremental reload differs from a full rebuild"
        );
    }

    fn alias_workspace(root: &Path, package_json: &str) {
        for dir in ["u", "app", "pkgs/a/src"] {
            std::fs::create_dir_all(root.join(dir)).expect("mkdir");
        }
        std::fs::write(root.join("package.json"), package_json).expect("write");
        std::fs::write(root.join("u/User.js"), "export default class User {}\n").expect("write");
        std::fs::write(
            root.join("app/main.js"),
            "import User from '#u/User.js';\nexport class Main { u = new User(); }\n",
        )
        .expect("write");
    }

    #[test]
    fn package_imports_removed_or_shadowed_converge_with_a_full_rebuild() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = dunce::canonicalize(tmp.path()).expect("canon");
        alias_workspace(&root, r##"{ "imports": { "#u/*": "./u/*" } }"##);
        let state = indexed_state(&root);
        assert_incremental_equals_full(&state, "initial");

        // `imports` removed: the alias no longer resolves.
        std::thread::sleep(std::time::Duration::from_millis(30));
        std::fs::write(root.join("package.json"), r#"{ "name": "x" }"#).expect("rewrite");
        WorkspaceIndexer::reload_paths(&state, &[root.join("package.json")]);
        assert_incremental_equals_full(&state, "imports removed");

        // Restored, then shadowed by a nested package.json without `imports`.
        std::fs::write(
            root.join("package.json"),
            r##"{ "imports": { "#u/*": "./u/*" } }"##,
        )
        .expect("rewrite");
        WorkspaceIndexer::reload_paths(&state, &[root.join("package.json")]);
        assert_incremental_equals_full(&state, "imports restored");
        std::fs::write(root.join("app/package.json"), r#"{ "name": "app" }"#).expect("write");
        WorkspaceIndexer::reload_paths(&state, &[root.join("app/package.json")]);
        assert_incremental_equals_full(&state, "nested package.json created");
        std::fs::remove_file(root.join("app/package.json")).expect("rm");
        WorkspaceIndexer::reload_paths(&state, &[root.join("app/package.json")]);
        assert_incremental_equals_full(&state, "nested package.json deleted");
    }

    #[test]
    fn a_package_json_edit_that_moves_no_alias_forces_no_script() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = dunce::canonicalize(tmp.path()).expect("canon");
        alias_workspace(
            &root,
            r##"{ "version": "1", "imports": { "#u/*": "./u/*" } }"##,
        );
        let state = indexed_state(&root);
        let package = |version: &str| {
            format!(r##"{{ "version": "{version}", "imports": {{ "#u/*": "./u/*" }} }}"##)
        };
        let forced = |state: &AppState| {
            let vfs = state.vfs.lock().expect("vfs");
            WorkspaceIndexer::alias_governed_scripts(
                state,
                &vfs,
                &state.allowed_roots,
                &[(0, root.join("package.json"))],
                &[],
            )
            .len()
        };
        // The first sighting of a config is conservative: its previous aliases are unknown.
        std::fs::write(root.join("package.json"), package("2")).expect("rewrite");
        assert_eq!(forced(&state), 2, "both scripts are re-extracted once");
        // Afterwards an edit that leaves `imports` alone forces nothing...
        std::fs::write(root.join("package.json"), package("3")).expect("rewrite");
        assert_eq!(forced(&state), 0);
        // ...and one that changes it forces the scripts under it again.
        std::fs::write(
            root.join("package.json"),
            r##"{ "imports": { "#u/*": "./other/*" } }"##,
        )
        .expect("rewrite");
        assert_eq!(forced(&state), 2);
        std::fs::remove_file(root.join("package.json")).expect("rm");
        assert_eq!(forced(&state), 2, "a vanished config forces them too");
    }

    #[test]
    fn editing_an_extended_base_config_reaches_the_scripts_that_extend_it() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = dunce::canonicalize(tmp.path()).expect("canon");
        for dir in ["pkgs/a/src", "shared", "x/src", "y/src"] {
            std::fs::create_dir_all(root.join(dir)).expect("mkdir");
        }
        let base = |target: &str| {
            format!(
                r#"{{ "compilerOptions": {{ "baseUrl": ".", "paths": {{ "@/*": ["{target}/*"] }} }} }}"#
            )
        };
        std::fs::write(root.join("tsconfig.base.json"), base("x")).expect("write");
        std::fs::write(
            root.join("pkgs/a/tsconfig.json"),
            r#"{ "extends": "../../tsconfig.base.json" }"#,
        )
        .expect("write");
        std::fs::write(root.join("shared/tsconfig.json"), base("y")).expect("write");
        std::fs::write(root.join("pkgs/b.json"), "{}").expect("write");
        std::fs::write(root.join("x/src/Dep.ts"), "export default class Dep {}\n").expect("write");
        std::fs::write(root.join("y/src/Dep.ts"), "export default class Dep {}\n").expect("write");
        std::fs::write(
            root.join("pkgs/a/src/main.ts"),
            "import Dep from '@/src/Dep';\nexport class Main { d: Dep }\n",
        )
        .expect("write");
        let state = indexed_state(&root);
        assert_incremental_equals_full(&state, "initial");

        std::thread::sleep(std::time::Duration::from_millis(30));
        std::fs::write(root.join("tsconfig.base.json"), base("y")).expect("rewrite");
        WorkspaceIndexer::reload_paths(&state, &[root.join("tsconfig.base.json")]);
        assert_incremental_equals_full(&state, "extended base edited");
        // The same through a full reload.
        std::fs::write(root.join("tsconfig.base.json"), base("x")).expect("rewrite");
        WorkspaceIndexer::reload(&state);
        assert_incremental_equals_full(&state, "extended base edited, full reload");
    }
    /// Review fix: a stray `mesh-mcp.toml` in HOME must not switch the guard
    /// off unless it lists roots.
    #[test]
    fn a_config_pins_roots_only_when_it_lists_some() {
        let home = tempfile::tempdir().expect("home");
        let home_dir = dunce::canonicalize(home.path()).expect("canon");
        let cfg = home_dir.join("mesh-mcp.toml");
        let guard = |text: &str| {
            std::fs::write(&cfg, text).expect("write config");
            WorkspaceIndexer::guard_workspace_with_home(Some(&cfg), &home_dir, Some(&home_dir))
        };
        // Empty roots: falls back to HOME itself, refused.
        assert!(guard("[workspace]\nname = \"w\"\nroots = []\n").is_err());
        // Only a name (roots missing, the file does not even load): refused.
        assert!(guard("[workspace]\nname = \"w\"\n").is_err());
        // An explicit root is honoured.
        assert!(guard("[workspace]\nname = \"w\"\nroots = [\"proj\"]\n").is_ok());
    }

    #[test]
    fn the_stamped_loader_hashes_the_bytes_the_config_was_parsed_from() {
        let dir = tempfile::tempdir().expect("dir");
        let p = dir.path().join("mesh-mcp.toml");
        let text = "[workspace]\nname = \"w\"\nroots = [\"a\"]\n";
        std::fs::write(&p, text).expect("write");
        let (cfg, base, digest) = WorkspaceIndexer::load_config_stamped(&p).expect("load");
        assert_eq!(
            digest,
            mesh_core::config_watch::config_sha256(text.as_bytes())
        );
        assert_eq!(cfg.workspace.roots, vec!["a".to_string()]);
        assert_eq!(base, dir.path());
        // A later edit does not change what an earlier load reported.
        std::fs::write(&p, text.replace("\"a\"", "\"b\"")).expect("edit");
        let (_, _, digest2) = WorkspaceIndexer::load_config_stamped(&p).expect("load");
        assert_ne!(digest, digest2);
        // `discover_config_stamped` with an explicit file agrees.
        let (_, _, found) = WorkspaceIndexer::discover_config_stamped(Some(&p)).expect("find");
        assert_eq!(found, Some(digest2));
    }

    // ---- 9.0 T6: config-driven topics --------------------------------------------------------

    /// `(kind, confidence, topic, site file)` of every topic edge, sorted.
    fn topic_rows(view: &MeshSnapshot) -> Vec<String> {
        let g = &view.contract_graph;
        let mut rows: Vec<String> = g
            .all_edges()
            .iter()
            .filter(|e| {
                matches!(
                    e.kind,
                    mesh_core::EdgeKind::Produces | mesh_core::EdgeKind::Consumes
                )
            })
            .filter_map(|e| {
                let (hub, site) = match e.kind {
                    mesh_core::EdgeKind::Produces => (g.get_node(e.to)?, g.get_node(e.from)?),
                    _ => (g.get_node(e.from)?, g.get_node(e.to)?),
                };
                let file = site.file_path.file_name()?.to_string_lossy().into_owned();
                Some(format!(
                    "{:?} {} {} {file}",
                    e.kind,
                    e.confidence.label(),
                    hub.name
                ))
            })
            .collect();
        rows.sort();
        rows
    }

    fn build_and_install(state: &Arc<AppState>) {
        let snap = {
            let mut vfs = state.vfs.lock().expect("vfs");
            WorkspaceIndexer::build_snapshot(
                &state.config,
                &state.allowed_roots,
                None,
                Some(&mut vfs),
                None,
            )
        };
        state.install_snapshot(snap);
    }

    fn write(root: &Path, rel: &str, text: &str) {
        let p = root.join(rel);
        std::fs::create_dir_all(p.parent().expect("parent")).expect("mkdir");
        std::fs::write(p, text).expect("write");
    }

    const SPRING_SVC: &str = "package com.x;\npublic class Svc {\n  @Value(\"${app.topics.out}\") private String outTopic;\n  @KafkaListener(topics = \"${app.topics.in}\")\n  public void on(String m) {}\n  public void go() { kafkaTemplate.send(outTopic, \"x\"); }\n}\n";

    /// A topic read from `application.yml` is resolved, a changed value re-resolves it on the
    /// next reload without the Java file changing, a deleted config leaves it visible as its
    /// placeholder, and every incremental state equals a cold rebuild (fingerprint).
    #[test]
    fn a_config_edit_re_resolves_unchanged_sources_and_converges_with_a_full_rebuild() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = dunce::canonicalize(tmp.path()).expect("canon");
        write(&root, "src/Svc.java", SPRING_SVC);
        write(
            &root,
            "src/main/resources/application.yml",
            "app:\n  topics:\n    out: orders.v1\n    in: orders.v1\n",
        );
        let state = make_state(&root);
        build_and_install(&state);
        let rows = topic_rows(&state.snapshot());
        assert_eq!(
            rows,
            [
                "Consumes exact orders.v1 Svc.java",
                "Produces exact orders.v1 Svc.java"
            ]
        );

        // The graph and the config values equal a cold rebuild's. (`property_registry` is left
        // out on purpose: its last-writer-wins shadowing of a key set by two files is
        // order-dependent, a documented limit that predates T6.)
        let converges = |state: &Arc<AppState>| {
            let inc = state.snapshot();
            let cold = WorkspaceIndexer::build_snapshot(
                &state.config,
                &state.allowed_roots,
                None,
                None,
                None,
            );
            assert_eq!(inc.fingerprint().graph, cold.fingerprint().graph, "graph");
            assert_eq!(
                inc.config_values.canonical_lines_rooted(&inc.roots),
                cold.config_values.canonical_lines_rooted(&cold.roots),
                "config values"
            );
        };

        // Edit the value: the Java file is untouched, its topic moves.
        write(
            &root,
            "src/main/resources/application.yml",
            "app:\n  topics:\n    out: orders.v2\n    in: orders.v1\n",
        );
        WorkspaceIndexer::reload(&state);
        assert_eq!(
            topic_rows(&state.snapshot()),
            [
                "Consumes exact orders.v1 Svc.java",
                "Produces exact orders.v2 Svc.java"
            ]
        );
        converges(&state);

        // A profile file that disagrees: both values, heuristic.
        write(
            &root,
            "src/main/resources/application-prod.yml",
            "app:\n  topics:\n    out: orders.prod\n",
        );
        WorkspaceIndexer::reload(&state);
        assert_eq!(
            topic_rows(&state.snapshot()),
            [
                "Consumes exact orders.v1 Svc.java",
                "Produces heuristic orders.prod Svc.java",
                "Produces heuristic orders.v2 Svc.java"
            ]
        );
        converges(&state);

        // The configuration is deleted: placeholders stay visible, heuristic, counted.
        std::fs::remove_file(root.join("src/main/resources/application.yml")).expect("rm");
        std::fs::remove_file(root.join("src/main/resources/application-prod.yml")).expect("rm");
        WorkspaceIndexer::reload(&state);
        let view = state.snapshot();
        assert_eq!(
            topic_rows(&view),
            [
                "Consumes heuristic ${app.topics.in} Svc.java",
                "Produces heuristic ${app.topics.out} Svc.java"
            ]
        );
        assert_eq!(view.health.config_topics_unresolved, 2);
        drop(view);
        converges(&state);
    }

    /// TypeScript, Go and Python sites read through `.env.example` (the one dotenv file the
    /// default exclusion lets through) and a Kubernetes `env:` list; a real `.env` stays
    /// excluded (commandment 5) and a secret key is never retained.
    #[test]
    fn env_reads_resolve_against_templates_and_manifests_but_never_a_real_dotenv() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = dunce::canonicalize(tmp.path()).expect("canon");
        write(
            &root,
            ".env",
            "KAFKA_TOPIC=from.real.env\nDB_PASSWORD=hunter2\n",
        );
        write(
            &root,
            ".env.example",
            "KAFKA_TOPIC=orders.example\nOTHER_TOPIC=other.example\nAPI_TOKEN=sekret\n",
        );
        write(&root, "k8s/dep.yaml", "spec:\n  containers:\n    - name: a\n      env:\n        - name: OTHER_TOPIC\n          value: other.deployed\n");
        write(&root, "svc.ts", "import { Kafka } from 'kafkajs';\nconst k = new Kafka({ brokers: ['b'] });\nconst producer = k.producer();\nconst consumer = k.consumer({ groupId: 'g' });\nexport async function run(config: any) {\n  await producer.send({ topic: process.env.KAFKA_TOPIC, messages: [] });\n  await consumer.subscribe({ topic: config.get('OTHER_TOPIC') });\n  await consumer.subscribe({ topic: process.env.API_TOKEN });\n}\n");
        let state = make_state(&root);
        build_and_install(&state);
        let view = state.snapshot();
        let rows = topic_rows(&view);
        assert!(
            rows.contains(&"Produces heuristic orders.example svc.ts".to_string()),
            "{rows:?}"
        );
        assert!(
            rows.contains(&"Consumes heuristic other.deployed svc.ts".to_string()),
            "{rows:?}"
        );
        assert!(
            !rows
                .iter()
                .any(|r| r.contains("from.real.env") || r.contains("hunter2")),
            "{rows:?}"
        );
        // The secret key resolves to nothing: its site keeps the placeholder, its value is gone.
        assert!(rows.iter().any(|r| r.contains("${api_token}")), "{rows:?}");
        assert!(!rows.iter().any(|r| r.contains("sekret")), "{rows:?}");
        let dump = format!("{:?}", view.config_values);
        assert!(
            !dump.contains("sekret")
                && !dump.contains("hunter2")
                && !dump.contains("from.real.env")
        );
    }

    /// Hostile configuration: a placeholder cycle, a 10k-key properties file, a deeply nested
    /// yaml and an oversized yaml neither hang nor panic nor invent a topic; the caps are
    /// counted. A config symlink escaping every root is never read.
    #[test]
    fn hostile_configuration_is_bounded_and_counted() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = dunce::canonicalize(tmp.path()).expect("canon");
        write(&root, "src/Svc.java", "package a;\npublic class Svc {\n  @KafkaListener(topics = \"${a}\")\n  public void x(String m) {}\n  @KafkaListener(topics = \"${big.key.7777}\")\n  public void y(String m) {}\n  @KafkaListener(topics = \"${deep}\")\n  public void z(String m) {}\n}\n");
        write(&root, "application.properties", "a=${b}\nb=${a}\n");
        let mut many = String::new();
        for i in 0..10_000 {
            many.push_str(&format!("big.key.{i}=topic.{i}\n"));
        }
        write(&root, "application-many.properties", &many);
        let mut deep = String::new();
        for i in 0..400 {
            deep.push_str(&"  ".repeat(i));
            deep.push_str(&format!("k{i}:\n"));
        }
        write(&root, "application-deep.yml", &deep);
        write(
            &root,
            "application-huge.yml",
            &format!("k: {}\n", "v".repeat(1_100_000)),
        );
        let outside = tempfile::tempdir().expect("outside");
        std::fs::write(
            outside.path().join("application-out.properties"),
            "deep=escaped.topic\n",
        )
        .expect("write");
        #[cfg(unix)]
        std::os::unix::fs::symlink(
            outside.path().join("application-out.properties"),
            root.join("application-link.properties"),
        )
        .expect("symlink");

        let state = make_state(&root);
        build_and_install(&state);
        let view = state.snapshot();
        let rows = topic_rows(&view);
        assert!(
            rows.contains(&"Consumes heuristic ${a} Svc.java".to_string()),
            "cycle stays visible: {rows:?}"
        );
        assert!(
            !rows.iter().any(|r| r.contains("escaped.topic")),
            "{rows:?}"
        );
        assert!(
            rows.contains(&"Consumes heuristic ${deep} Svc.java".to_string()),
            "{rows:?}"
        );
        // 2048 of the 10k keys are kept: the cap is counted, the unreachable key stays visible.
        assert_eq!(view.health.files_config_capped, 1);
        assert!(
            rows.contains(&"Consumes heuristic ${big.key.7777} Svc.java".to_string()),
            "{rows:?}"
        );
        assert!(
            view.health.files_rejected_oversized >= 1,
            "the 1 MB yaml is counted as oversized"
        );
        assert!(view.health.config_truncated());
    }

    const ONE_LISTENER: &str = "package a;\npublic class S {\n  @KafkaListener(topics = \"${t}\")\n  public void on(String m) {}\n}\n";

    /// F6: a base file that does not parse (a duplicate key) must not promote the profile file
    /// next to it to an exact value: it is weak, and the failure is counted.
    #[test]
    fn an_unparsable_base_file_leaves_the_profile_value_weak_and_is_counted() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = dunce::canonicalize(tmp.path()).expect("canon");
        write(&root, "S.java", ONE_LISTENER);
        write(&root, "res/application.yml", "t: orders\nt: orders2\n");
        write(&root, "res/application-prod.yml", "t: orders.prod\n");
        let state = make_state(&root);
        build_and_install(&state);
        let view = state.snapshot();
        assert_eq!(topic_rows(&view), ["Consumes heuristic orders.prod S.java"]);
        assert_eq!(view.health.files_config_parse_failed, 1);
        drop(view);
        // Fixing the base file makes the profile value ambiguous with it, no longer failed.
        write(&root, "res/application.yml", "t: orders\n");
        WorkspaceIndexer::reload(&state);
        let view = state.snapshot();
        assert_eq!(
            topic_rows(&view),
            [
                "Consumes heuristic orders S.java",
                "Consumes heuristic orders.prod S.java"
            ]
        );
    }

    /// F7: a non-UTF-8 properties file keeps every valid line and is counted.
    #[test]
    fn a_non_utf8_config_file_keeps_its_valid_lines_and_is_counted() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = dunce::canonicalize(tmp.path()).expect("canon");
        write(&root, "S.java", ONE_LISTENER);
        std::fs::write(
            root.join("application.properties"),
            b"x=before\nbad=caf\xe9\nt=good.topic\n",
        )
        .expect("write");
        let state = make_state(&root);
        build_and_install(&state);
        let view = state.snapshot();
        assert_eq!(topic_rows(&view), ["Consumes exact good.topic S.java"]);
        assert_eq!(view.health.files_config_lossy, 1);
        assert!(view.health.config_degraded());
    }

    /// F8: a placeholder that stays unresolved while a config file was skipped says so.
    #[test]
    fn an_unresolved_placeholder_names_the_skipped_config_file() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = dunce::canonicalize(tmp.path()).expect("canon");
        write(&root, "S.java", ONE_LISTENER);
        write(
            &root,
            "application.yml",
            &format!("t: orders\nfiller: {}\n", "v".repeat(400_000)),
        );
        let state = make_state(&root);
        build_and_install(&state);
        let view = state.snapshot();
        assert_eq!(topic_rows(&view), ["Consumes heuristic ${t} S.java"]);
        let summary = view.health.render_summary(10);
        assert!(
            summary.contains("1 ${key} topic(s) no configuration file resolves"),
            "{summary}"
        );
        assert!(
            summary.contains("1 configuration file(s) were not indexed"),
            "{summary}"
        );
    }

    /// F4: 20 listeners on one key whose values fan out 8 wide and 8 deep over 64 files stay
    /// unresolved, visible and counted, and the work does not scale with the cross product.
    #[test]
    fn a_wide_chained_config_fan_out_stays_unresolved_visible_and_counted() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = dunce::canonicalize(tmp.path()).expect("canon");
        let mut java = String::from("package a;\npublic class S {\n");
        for i in 0..20 {
            java.push_str(&format!(
                "  @KafkaListener(topics = \"${{k0}}\")\n  public void on{i}(String m) {{}}\n"
            ));
        }
        java.push_str("}\n");
        write(&root, "S.java", &java);
        for f in 0..64 {
            let mut text = String::new();
            for level in 0..8 {
                text.push_str(&format!("k{level}=${{k{}}}.v{}\n", level + 1, f % 8));
            }
            text.push_str("k8=leaf\n");
            write(&root, &format!("res/application-{f}.properties"), &text);
        }
        let state = make_state(&root);
        let started = std::time::Instant::now();
        build_and_install(&state);
        assert!(started.elapsed().as_secs() < 20, "{:?}", started.elapsed());
        let view = state.snapshot();
        let rows = topic_rows(&view);
        assert_eq!(rows.len(), 20, "{rows:?}");
        assert!(
            rows.iter()
                .all(|r| r.starts_with("Consumes heuristic ${k0} S.java")),
            "{rows:?}"
        );
        assert_eq!(view.health.config_topics_unresolved, 1);
    }

    // ---- 9.0 T14: enum-member queue names -----------------------------------------------------

    const QUEUES_TS: &str = "export enum JobQueue {\n  EMAIL = 'email-jobs',\n}\nexport enum LegacyQueue {\n  EMAIL = 'legacy-email-jobs',\n}\n";
    const PRODUCER_TS: &str = "import { InjectQueue } from '@nestjs/bullmq';\nimport { Queue } from 'bullmq';\nimport { JobQueue } from './queues';\nexport class S {\n  constructor(@InjectQueue(JobQueue.EMAIL) private q: Queue) {}\n}\n";
    const CONSUMER_TS: &str = "import { Processor } from '@nestjs/bullmq';\nimport { JobQueue } from './queues';\n@Processor(JobQueue.EMAIL)\nexport class C {}\n";
    const LEGACY_TS: &str = "import { Processor } from '@nestjs/bullmq';\nimport { LegacyQueue } from './queues';\n@Processor(LegacyQueue.EMAIL)\nexport class L {}\n";

    /// An enum member naming a queue resolves to the member's value across files, two enums with
    /// one member name are two queues, a constant edit re-folds the unchanged files naming it,
    /// and every state equals a cold rebuild (graph fingerprint and constants).
    #[test]
    fn an_enum_member_queue_resolves_across_files_and_a_constant_edit_refolds_its_users() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = dunce::canonicalize(tmp.path()).expect("canon");
        write(&root, "src/queues.ts", QUEUES_TS);
        write(&root, "src/producer.ts", PRODUCER_TS);
        write(&root, "src/consumer.ts", CONSUMER_TS);
        write(&root, "src/legacy.ts", LEGACY_TS);
        let state = make_state(&root);
        build_and_install(&state);
        assert_eq!(
            topic_rows(&state.snapshot()),
            [
                "Consumes exact email-jobs consumer.ts",
                "Consumes exact legacy-email-jobs legacy.ts",
                "Produces exact email-jobs producer.ts"
            ]
        );
        let converges = |state: &Arc<AppState>| {
            let inc = state.snapshot();
            let cold = WorkspaceIndexer::build_snapshot(
                &state.config,
                &state.allowed_roots,
                None,
                None,
                None,
            );
            assert_eq!(inc.fingerprint().graph, cold.fingerprint().graph, "graph");
            assert_eq!(
                inc.config_values.canonical_lines_rooted(&inc.roots),
                cold.config_values.canonical_lines_rooted(&cold.roots),
                "constants"
            );
        };
        converges(&state);

        // An agent names the queue by its member, its value or the placeholder an extractor
        // records: one topic, never the other enum's.
        {
            let view = state.snapshot();
            let g = &view.contract_graph;
            for target in ["JobQueue.EMAIL", "email-jobs"] {
                let flow = g.analyze_impact(target);
                assert_eq!(flow.key.as_str(), "email-jobs", "{target}");
                assert_eq!(flow.upstream_producers.len(), 1, "{target}");
                assert_eq!(flow.downstream_consumers.len(), 1, "{target}");
            }
            let legacy = g.analyze_impact("LegacyQueue.EMAIL");
            assert_eq!(legacy.key.as_str(), "legacy-email-jobs");
            assert!(legacy.upstream_producers.is_empty());
            assert_eq!(legacy.downstream_consumers.len(), 1);
        }

        // The value changes in the enum file alone: both unchanged users follow it.
        write(
            &root,
            "src/queues.ts",
            &QUEUES_TS.replace("'email-jobs'", "'mail-jobs'"),
        );
        WorkspaceIndexer::reload(&state);
        assert_eq!(
            topic_rows(&state.snapshot()),
            [
                "Consumes exact legacy-email-jobs legacy.ts",
                "Consumes exact mail-jobs consumer.ts",
                "Produces exact mail-jobs producer.ts"
            ]
        );
        converges(&state);

        // The enum file is deleted: the members keep their QUALIFIED key, `heuristic`, so the
        // two enums do not collapse onto one topic `email`.
        std::fs::remove_file(root.join("src/queues.ts")).expect("rm");
        WorkspaceIndexer::reload(&state);
        let view = state.snapshot();
        assert_eq!(
            topic_rows(&view),
            [
                "Consumes heuristic jobqueue.email consumer.ts",
                "Consumes heuristic legacyqueue.email legacy.ts",
                "Produces heuristic jobqueue.email producer.ts"
            ]
        );
        drop(view);
        converges(&state);

        // The enum comes back: the users resolve again.
        write(&root, "src/queues.ts", QUEUES_TS);
        WorkspaceIndexer::reload(&state);
        assert_eq!(
            topic_rows(&state.snapshot()),
            [
                "Consumes exact email-jobs consumer.ts",
                "Consumes exact legacy-email-jobs legacy.ts",
                "Produces exact email-jobs producer.ts"
            ]
        );
        converges(&state);
    }

    /// A custom pattern that captures a member of an enum some file names a queue by joins the
    /// queue on its value, whichever file it reads; a member of any other enum keeps its member
    /// key (the stream / event-type link of a codebase naming both alike). The registry follows
    /// the sources: it equals a cold rebuild after each edit.
    #[test]
    fn a_pattern_capture_of_a_queue_enum_member_joins_the_queue_by_value() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = dunce::canonicalize(tmp.path()).expect("canon");
        write(&root, "src/queues.ts", QUEUES_TS);
        write(&root, "src/consumer.ts", CONSUMER_TS);
        write(
            &root,
            "src/streams.ts",
            "export enum Streams {\n  EMAIL = 'stream:email',\n}\n",
        );
        write(
            &root,
            "src/outbox.ts",
            "import { JobQueue } from './queues';\nimport { Streams } from './streams';\n\
             export const job = { target: JobQueue.EMAIL, stream: Streams.EMAIL };\n",
        );
        let cfg = Config::load_from_str(
            "[workspace]\nname = \"t\"\nversion = \"0\"\nroots = [\".\"]\n\
             [engines.contracts]\nenabled = true\n\
             [[engines.contracts.patterns]]\nname = \"queue-target\"\nkind = \"topic_producer\"\n\
             confidence = \"exact\"\nfile_pattern = \"outbox.ts\"\nregex = 'target:\\s*(\\w+\\.\\w+)'\n\
             target_group = 1\n\
             [[engines.contracts.patterns]]\nname = \"stream-ref\"\nkind = \"topic_producer\"\n\
             confidence = \"exact\"\nfile_pattern = \"outbox.ts\"\nregex = 'stream:\\s*(\\w+\\.\\w+)'\n\
             target_group = 1\n",
        )
        .expect("config");
        let audit = Arc::new(AuditLogger::new_in_memory().expect("audit"));
        let rescan = Arc::new(BackgroundRescanEngine::new().expect("rescan"));
        let state = Arc::new(AppState::new(cfg, vec![root.clone()], audit, rescan));
        build_and_install(&state);
        // `outbox.ts` names no queue itself: the enum is one because `consumer.ts` names it.
        assert_eq!(
            topic_rows(&state.snapshot()),
            [
                "Consumes exact email-jobs consumer.ts",
                "Produces exact email outbox.ts",
                "Produces exact email-jobs outbox.ts"
            ]
        );
        let converges = |state: &Arc<AppState>| {
            let inc = state.snapshot();
            let cold = WorkspaceIndexer::build_snapshot(
                &state.config,
                &state.allowed_roots,
                None,
                None,
                None,
            );
            assert_eq!(inc.fingerprint().graph, cold.fingerprint().graph, "graph");
            assert_eq!(
                inc.config_values.canonical_lines_rooted(&inc.roots),
                cold.config_values.canonical_lines_rooted(&cold.roots),
                "registry"
            );
        };
        converges(&state);

        // The only queue site goes: the enum is no queue enum any more, the capture falls back to
        // the member key without `outbox.ts` changing.
        std::fs::remove_file(root.join("src/consumer.ts")).expect("rm");
        WorkspaceIndexer::reload(&state);
        assert_eq!(
            topic_rows(&state.snapshot()),
            [
                "Produces exact email outbox.ts",
                "Produces exact email outbox.ts"
            ]
        );
        converges(&state);

        // It comes back.
        write(&root, "src/consumer.ts", CONSUMER_TS);
        WorkspaceIndexer::reload(&state);
        assert_eq!(
            topic_rows(&state.snapshot()),
            [
                "Consumes exact email-jobs consumer.ts",
                "Produces exact email outbox.ts",
                "Produces exact email-jobs outbox.ts"
            ]
        );
        converges(&state);
    }

    /// A call of a declared producer helper (9.0 T14b) is a `heuristic` producer: its `type` is a
    /// topic, its `target` a queue that resolves to the enum member's value like a queue site
    /// does, so a constant edit re-folds the unchanged file, and every state equals a cold rebuild.
    #[test]
    fn a_declared_producer_call_joins_its_queue_by_value_and_follows_a_constant_edit() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = dunce::canonicalize(tmp.path()).expect("canon");
        write(&root, "src/queues.ts", QUEUES_TS);
        write(&root, "src/consumer.ts", CONSUMER_TS);
        write(
            &root,
            "src/jobs.ts",
            "import { JobQueue } from './queues';\nimport { Outbox } from './outbox';\n\
             export const send = () => Outbox.enqueue({\n  type: 'send-welcome',\n  target: JobQueue.EMAIL,\n});\n",
        );
        // An enum no consumer names a queue by: it is a queue only because the declaration says
        // `target` is one.
        write(
            &root,
            "src/batch.ts",
            "import { Outbox } from './outbox';\nimport { BatchQueue } from './batch-queue';\n\
             export const nightly = () => Outbox.enqueue({ type: 'run-batch', target: BatchQueue.NIGHTLY });\n",
        );
        write(
            &root,
            "src/batch-queue.ts",
            "export enum BatchQueue {\n  NIGHTLY = 'nightly-batch',\n}\n",
        );
        let cfg = Config::load_from_str(
            "[workspace]\nname = \"t\"\nversion = \"0\"\nroots = [\".\"]\n\
             [engines.contracts]\nenabled = true\n\
             [[engines.contracts.producer_calls]]\nname = \"outbox\"\ncallee = \"Outbox.enqueue\"\n\
             topic_keys = [\"type\"]\nqueue_keys = [\"target\"]\n",
        )
        .expect("config");
        let audit = Arc::new(AuditLogger::new_in_memory().expect("audit"));
        let rescan = Arc::new(BackgroundRescanEngine::new().expect("rescan"));
        let state = Arc::new(AppState::new(cfg, vec![root.clone()], audit, rescan));
        build_and_install(&state);
        assert_eq!(
            topic_rows(&state.snapshot()),
            [
                "Consumes exact email-jobs consumer.ts",
                "Produces heuristic email-jobs jobs.ts",
                "Produces heuristic nightly-batch batch.ts",
                "Produces heuristic run-batch batch.ts",
                "Produces heuristic send-welcome jobs.ts"
            ]
        );
        let converges = |state: &Arc<AppState>| {
            let cold = WorkspaceIndexer::build_snapshot(
                &state.config,
                &state.allowed_roots,
                None,
                None,
                None,
            );
            assert_eq!(
                state.snapshot().fingerprint().graph,
                cold.fingerprint().graph,
                "graph"
            );
        };
        converges(&state);
        // The unchanged producer file follows the enum's new value.
        write(
            &root,
            "src/queues.ts",
            &QUEUES_TS.replace("email-jobs", "email-v2"),
        );
        WorkspaceIndexer::reload(&state);
        assert_eq!(
            topic_rows(&state.snapshot()),
            [
                "Consumes exact email-v2 consumer.ts",
                "Produces heuristic email-v2 jobs.ts",
                "Produces heuristic nightly-batch batch.ts",
                "Produces heuristic run-batch batch.ts",
                "Produces heuristic send-welcome jobs.ts"
            ]
        );
        converges(&state);
    }

    /// The extraction configuration is part of the parse-cache key: declaring a helper (or
    /// editing one) must not serve the facts of the file cached without it.
    #[test]
    fn a_declared_producer_call_changes_the_config_fingerprint() {
        let base = ExtractConfig::default();
        let declared = ExtractConfig {
            producer_calls: vec![mesh_core::ProducerCallConfig {
                name: "p".to_string(),
                callee: "Outbox.enqueue".to_string(),
                arg: 0,
                topic_keys: vec!["type".to_string()],
                queue_keys: Vec::new(),
            }],
            ..ExtractConfig::default()
        };
        let mut edited = declared.clone();
        edited.producer_calls[0].topic_keys.push("kind".to_string());
        let (a, b, c) = (
            WorkspaceIndexer::config_fingerprint(&base),
            WorkspaceIndexer::config_fingerprint(&declared),
            WorkspaceIndexer::config_fingerprint(&edited),
        );
        assert!(a != b && b != c && a != c);
    }

    fn queue_state(files: &[(&str, &str)]) -> (tempfile::TempDir, PathBuf, Arc<AppState>) {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = dunce::canonicalize(tmp.path()).expect("canon");
        for (rel, text) in files {
            write(&root, rel, text);
        }
        let state = make_state(&root);
        build_and_install(&state);
        (tmp, root, state)
    }

    /// An event or stream naming a member keeps its member-name link with another enum's member
    /// even when a queue site elsewhere names the same enum (Volontariapp: `Streams.X` and
    /// `XEventType.X` are one flow), and a same-named enum of another package stays unresolved by
    /// the first one's queue declaration.
    #[test]
    fn a_non_queue_site_naming_a_member_keeps_its_member_key() {
        let (_t, _r, state) = queue_state(&[
            (
                "src/streams.ts",
                "export enum Streams { USER_CREATED = 'user:created' }\n\
                 export enum UserEventMessagingType { USER_CREATED = 'user.created' }\n",
            ),
            (
                "src/bull.ts",
                "import { Queue } from 'bullmq';\nimport { Streams } from './streams';\n\
                 export const bq = new Queue(Streams.USER_CREATED);\n",
            ),
            (
                "src/kafka.ts",
                "import { EventPattern } from '@nestjs/microservices';\n\
                 import { Streams, UserEventMessagingType } from './streams';\n\
                 export class K {\n  @EventPattern(Streams.USER_CREATED)\n  onStream() {}\n\
                 \x20 @EventPattern(UserEventMessagingType.USER_CREATED)\n  onEvent() {}\n}\n",
            ),
            (
                "pkga/queues.ts",
                "export enum JobQueue { EMAIL = 'a-email' }\n",
            ),
            (
                "pkga/worker.ts",
                "import { Processor } from '@nestjs/bullmq';\nimport { JobQueue } from './queues';\n\
                 @Processor(JobQueue.EMAIL)\nexport class W {}\n",
            ),
            (
                "pkgb/enums.ts",
                "export enum JobQueue { EMAIL = 'b-email' }\n",
            ),
            (
                "pkgb/kafka.ts",
                "import { EventPattern } from '@nestjs/microservices';\nimport { JobQueue } from './enums';\n\
                 export class C {\n  @EventPattern(JobQueue.EMAIL)\n  handle() {}\n}\n",
            ),
        ]);
        let rows = topic_rows(&state.snapshot());
        // The queue site is by value; both Kafka consumers of `user_created` stay one hub.
        assert!(
            rows.contains(&"Produces exact user:created bull.ts".to_string()),
            "{rows:?}"
        );
        assert_eq!(
            rows.iter()
                .filter(|r| r.starts_with("Consumes exact user_created kafka.ts"))
                .count(),
            2,
            "{rows:?}"
        );
        // `pkga` resolves its own declaration; `pkgb`'s @EventPattern is no queue consumer.
        assert!(
            rows.contains(&"Consumes exact a-email worker.ts".to_string()),
            "{rows:?}"
        );
        assert!(
            rows.contains(&"Consumes exact email kafka.ts".to_string()),
            "{rows:?}"
        );
        assert!(!rows.iter().any(|r| r.contains("b-email")), "{rows:?}");
    }

    /// The constants of a test file are never recorded: they neither change what production
    /// resolves nor appear as a topic.
    #[test]
    fn a_test_file_constant_never_resolves_a_production_queue() {
        let (_t, _r, state) = queue_state(&[
            ("pkga/queues.ts", "export enum JobQueue { EMAIL = 'a-email' }\n"),
            (
                "pkga/worker.ts",
                "import { Processor } from '@nestjs/bullmq';\nimport { JobQueue } from './queues';\n\
                 @Processor(JobQueue.EMAIL)\nexport class W {}\n",
            ),
            (
                "src/__tests__/q.spec.ts",
                "export enum JobQueue { EMAIL = 'TEST-ONLY-email' }\n",
            ),
        ]);
        let view = state.snapshot();
        assert_eq!(topic_rows(&view), ["Consumes exact a-email worker.ts"]);
        assert_eq!(view.health.const_members_conflicting, 0);
    }

    /// Two declarations of one member with different values in the package an import names: neither
    /// is chosen, the qualified key is `heuristic`, and it is counted. A site its relative import
    /// binds to one declaration is not a conflict.
    #[test]
    fn a_member_two_declarations_disagree_on_resolves_to_neither_and_is_counted() {
        let (_t, _r, state) = queue_state(&[
            ("shared/package.json", "{\"name\": \"@org/shared\"}"),
            ("shared/one.ts", "export enum JobQueue { EMAIL = 'one-email' }\n"),
            ("shared/two.ts", "export enum JobQueue { EMAIL = 'two-email' }\n"),
            ("a/queues.ts", "export enum JobQueue { EMAIL = 'a-email' }\n"),
            (
                "c/worker.ts",
                "import { Processor } from '@nestjs/bullmq';\nimport { JobQueue } from '@org/shared';\n\
                 @Processor(JobQueue.EMAIL)\nexport class W {}\n",
            ),
            (
                "a/worker.ts",
                "import { Processor } from '@nestjs/bullmq';\nimport { JobQueue } from './queues';\n\
                 @Processor(JobQueue.EMAIL)\nexport class W {}\n",
            ),
        ]);
        let view = state.snapshot();
        assert_eq!(
            topic_rows(&view),
            [
                "Consumes exact a-email worker.ts",
                "Consumes heuristic jobqueue.email worker.ts"
            ]
        );
        assert_eq!(view.health.const_members_conflicting, 1);
        assert!(view.health.const_degraded());
        assert!(view
            .health
            .render_summary(10)
            .contains("a queue site cannot choose between"));
    }

    /// Many files each declaring their own `JobQueue.EMAIL`, every site bound by a relative
    /// import: nothing is ambiguous, nothing is reported.
    #[test]
    fn declarations_that_differ_but_are_all_bound_are_not_a_conflict() {
        let mut files: Vec<(String, String)> = Vec::new();
        for i in 0..20 {
            files.push((
                format!("s{i}/queues.ts"),
                format!("export enum JobQueue {{ EMAIL = 'email-{i}' }}\n"),
            ));
            files.push((
                format!("s{i}/w.ts"),
                "import { Processor } from '@nestjs/bullmq';\nimport { JobQueue } from './queues';\n\
                 @Processor(JobQueue.EMAIL)\nexport class W {}\n"
                    .to_string(),
            ));
        }
        let refs: Vec<(&str, &str)> = files
            .iter()
            .map(|(a, b)| (a.as_str(), b.as_str()))
            .collect();
        let (_t, _r, state) = queue_state(&refs);
        let view = state.snapshot();
        assert_eq!(view.health.const_members_conflicting, 0);
        assert!(!view.health.const_degraded());
        assert_eq!(topic_rows(&view).len(), 20);
    }

    /// A non-relative import binds only to the package it names: never to a homonymous enum of
    /// the importer's own repository (silent wrong answer at `exact`), never to build output.
    #[test]
    fn a_package_import_binds_to_its_package_and_never_to_the_importers_own_enum() {
        let (_t, _r, state) = queue_state(&[
            ("src/queues.ts", "export enum JobQueue { EMAIL = 'in-repo-email' }\n"),
            (
                "src/worker.ts",
                "import { Processor } from '@nestjs/bullmq';\nimport { JobQueue } from 'legacy-queue-lib';\n\
                 @Processor(JobQueue.EMAIL)\nexport class ExtW {}\n",
            ),
            ("packages/queues/package.json", "{\"name\": \"@org/queues\"}"),
            (
                "packages/queues/src/queues.ts",
                "export enum PkgQ { EMAIL = 'pkg-email' }\n",
            ),
            (
                "packages/queues/dist/queues.ts",
                "export enum DistQ { EMAIL = 'dist-email' }\n",
            ),
            (
                "packages/app/src/svc.ts",
                "import { Processor } from '@nestjs/bullmq';\nimport { PkgQ } from '@org/queues';\n\
                 @Processor(PkgQ.EMAIL)\nexport class PkgW {}\n",
            ),
            (
                "packages/app/src/dist.ts",
                "import { Queue } from 'bullmq';\nimport { DistQ } from '@org/queues/dist/queues';\n\
                 export const d = new Queue(DistQ.EMAIL);\n",
            ),
        ]);
        assert_eq!(
            topic_rows(&state.snapshot()),
            [
                "Consumes exact pkg-email svc.ts",
                "Consumes heuristic jobqueue.email worker.ts",
                "Produces heuristic distq.email dist.ts"
            ]
        );
    }

    /// A value that is no name, and a member named like a credential, never enter the index
    /// record, so they cannot reach the parse cache either. (The first source line of a `const`
    /// declaration is a node signature: a known, separate limit.)
    #[test]
    fn refused_constants_never_enter_the_index_record() {
        let code = "export enum Sec {\n  A_BEARER = 'Bearer_CANARYB1',\n  B_HUNTER = 'hunter2-prod-CANARYX',\n  \
                    C_SKLIVE = 'tok_CANARYSKabcdefgh',\n  D_PGURL = 'postgres://u:CANARYPW@h',\n  \
                    API_KEY = 'legit-api-jobs',\n  OK = 'ok-jobs',\n}\n";
        let fi = mesh_parsers::PolyglotIndexer::extract(Path::new("/w/sec.ts"), code, 0);
        let record = serde_json::to_string(&fi).expect("serialize");
        assert!(
            !record.contains("CANARY") && !record.contains("legit-api-jobs"),
            "{record}"
        );
        assert!(fi.const_records().any(|r| r == "Sec.OK=ok-jobs"));
        // A top-level constant too (its declaration line is a node signature, a separate limit,
        // so the constant records are what is checked).
        let bare = mesh_parsers::PolyglotIndexer::extract(
            Path::new("/w/bare.ts"),
            &format!(
                "export const TOK = 'sk_{}_CANARYBARE1234567890abcd';\nexport const URL_C = 'postgres://u:CANARYPW@h/x';\n\
                 export const objs = {{ A: 'Bearer_CANARYOBJ1' }} as const;\n",
                "live"
            ),
            0,
        );
        assert_eq!(
            bare.const_records().count(),
            0,
            "{:?}",
            bare.const_records().collect::<Vec<_>>()
        );
    }

    /// An aliased import and a namespace import bind to the declaring file; a member named like a
    /// credential, and values that are tokens, URLs, `Bearer_...` or password-shaped, are never a
    /// topic name: the site keeps its qualified member key.
    #[test]
    fn aliases_bind_and_secret_looking_constants_never_become_names() {
        let (_t, _r, state) = queue_state(&[
            (
                "src/queues.ts",
                "export enum JobQueue { EMAIL = 'email-jobs', REPORTS = 'report-jobs' }\n\
                 export enum Q {\n  A = 'sk_live_CANARYA1234567890abcdef',\n  \
                 B = 'postgres://admin:CANARY_PW_B@db.internal/x',\n  C = 'hunter2-prod-CANARYC',\n  \
                 D = 'ghp_CANARYDTOKEN0123456789abcdefghij',\n  E = 'jdbc:mysql://h/db?password=CANARY_PW_E',\n  \
                 F = 'Bearer_CANARY_F_eyJhbGciOiJIUzI1NiJ9',\n  PASSWORD = 'plain-name',\n  OK = 'ok-jobs',\n}\n",
            ),
            (
                "src/use.ts",
                "import { Processor } from '@nestjs/bullmq';\nimport { Queue, Worker } from 'bullmq';\n\
                 import { JobQueue as J, Q } from './queues';\nimport * as Qs from './queues';\n\
                 @Processor(J.EMAIL)\nexport class P {}\n\
                 export const n = new Queue(Qs.JobQueue.REPORTS);\n\
                 export const a = new Queue(Q.A);\nexport const b = new Queue(Q.B);\n\
                 export const c = new Worker(Q.C, async () => {});\nexport const d = new Queue(Q.D);\n\
                 export const e = new Queue(Q.E);\nexport const f = new Queue(Q.F);\n\
                 export const p = new Queue(Q.PASSWORD);\nexport const ok = new Queue(Q.OK);\n",
            ),
        ]);
        let view = state.snapshot();
        let rows = topic_rows(&view);
        assert!(
            rows.contains(&"Consumes exact email-jobs use.ts".to_string()),
            "{rows:?}"
        );
        assert!(
            rows.contains(&"Produces exact report-jobs use.ts".to_string()),
            "{rows:?}"
        );
        assert!(
            rows.contains(&"Produces exact ok-jobs use.ts".to_string()),
            "{rows:?}"
        );
        for letter in ["a", "b", "c", "d", "e", "f", "password"] {
            let key = format!("q.{letter}");
            assert!(
                rows.iter()
                    .any(|r| r.contains(&format!("heuristic {key} use.ts"))),
                "{key}: {rows:?}"
            );
        }
        // No canary reaches a topic hub or a site node of the graph.
        let g = &view.contract_graph;
        for e in g.all_edges() {
            for id in [e.from, e.to] {
                if let Some(n) = g.get_node(id) {
                    if n.kind != mesh_core::NodeKind::Module {
                        let name = n.name.to_ascii_lowercase();
                        assert!(
                            !name.contains("canary") && !name.contains("hunter2"),
                            "{}",
                            n.name
                        );
                    }
                }
            }
        }
    }

    /// An enum file past the per-file constant cap is counted in `IndexHealth` and named by the
    /// `doctor` summary, a member past it keeps its member key, and none of it needs more than
    /// the 2 MB stack of an indexing thread.
    #[test]
    fn the_constant_cap_is_counted_and_a_hostile_enum_is_bounded() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = dunce::canonicalize(tmp.path()).expect("canon");
        let mut big = String::from("export enum Big {\n");
        for i in 0..4100 {
            big.push_str(&format!("  M{i} = 'big-{i}',\n"));
        }
        big.push_str("}\nexport enum A { X = B.Y }\nexport enum B { Y = A.X }\n");
        write(&root, "src/big.ts", &big);
        write(
            &root,
            "src/use.ts",
            "import { Processor } from '@nestjs/bullmq';\nimport { Big } from './big';\n\
             @Processor(Big.M0)\nexport class First {}\n\
             @Processor(Big.M4099)\nexport class Last {}\n",
        );
        let state = make_state(&root);
        std::thread::Builder::new()
            .stack_size(2 * 1024 * 1024)
            .spawn({
                let state = Arc::clone(&state);
                move || build_and_install(&state)
            })
            .expect("spawn")
            .join()
            .expect("a hostile enum must not overflow the stack");
        let view = state.snapshot();
        assert_eq!(view.health.files_const_capped, 1);
        assert!(view.health.const_truncated());
        assert!(view
            .health
            .render_summary(10)
            .contains("more string constants or enum imports"));
        // The first member is inside the cap and resolves; the last one is past it and keeps its
        // qualified member key, heuristic, never a guess.
        let rows = topic_rows(&view);
        assert_eq!(
            rows,
            [
                "Consumes exact big-0 use.ts",
                "Consumes heuristic big.m4099 use.ts"
            ]
        );
    }
}
