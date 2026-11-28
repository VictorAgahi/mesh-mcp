use crate::protocol::RequestMeta;
use crate::tools::event_anomalies::coverage_notes;
use crate::tools::smart_search::{starts_with_folded, SearchScope};
use crate::tools::{McpTool, ToolError, ToolOutput};
use mesh_core::{AppState, CompactStr, ValidatedScope};
use mesh_parsers::{
    diff_asyncapi, format_diff, format_export, parse_declared, AstGuard, DeclaredDoc,
    DeclaredParse, DiffView, ExportView, ObservedSet,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AsyncApiMode {
    /// The observed event graph as an AsyncAPI 3.0 document.
    Export,
    /// The declared AsyncAPI document(s) of the workspace against the code.
    Diff,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AsyncApiFormat {
    Yaml,
    Json,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExportAsyncApiArgs {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        description = "`export` (default): the observed event graph as one AsyncAPI 3.0 document. `diff`: the declared AsyncAPI document(s) the index found, compared with the code."
    )]
    pub mode: Option<AsyncApiMode>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        with = "Option<String>",
        description = "Only the channels with a producer or consumer site under this path (a repository or a directory inside one, ex: 'ms-orders' or 'ms-orders/src/billing'); in `diff` also the declared documents under it. Omitted or '.': the whole workspace. The analysis always reads every root. DO NOT pass a topic name or a file glob."
    )]
    pub scope: Option<CompactStr>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(description = "`export` only: the document format, `yaml` (default) or `json`.")]
    pub format: Option<AsyncApiFormat>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        description = "`export`: channels in the page (1-100, default 20; fewer when the document would pass the size budget). `diff`: rows per class (1-100, default 25). DO NOT raise it to see everything; page with `offset`, or narrow with `scope`."
    )]
    pub limit: Option<u32>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        description = "`export`: channels to skip. `diff`: rows to skip in each class. Use the `offset` value the previous page gave."
    )]
    pub offset: Option<u32>,

    #[serde(default)]
    // Accepted for W3C trace propagation, hidden from `tools/list`.
    #[schemars(skip)]
    pub _meta: Option<RequestMeta>,
}

const DEFAULT_EXPORT_LIMIT: u32 = 20;
const DEFAULT_DIFF_LIMIT: u32 = 25;
const MAX_LIMIT: u32 = 100;
/// Declared documents read per call; the rest are named as not read.
const MAX_DECLARED_FILES: usize = 20;

pub struct ExportAsyncApiTool;

impl McpTool for ExportAsyncApiTool {
    const NAME: &'static str = "export_asyncapi";
    const DESCRIPTION: &'static str = "AsyncAPI 3.0 view of the event graph, in two modes. `export` (default): the OBSERVED graph as a valid AsyncAPI 3.0 document in a fenced YAML or JSON block: one channel per topic (address = the engine's normalized topic key, broker-qualified: two brokers sharing a name are two channels), one `send` operation per producer site and one `receive` operation per consumer site, each with path:line (`x-mesh-source`) and a confidence (exact / heuristic / ambiguous), the payload schema when the payload type is statically known (TypeScript / JavaScript and Java, the type behind `event_anomalies` class `payload_mismatch`), a server per known broker (its host is NOT known and is said so; an unknown broker is `heuristic` and gets no server). A topic named by a `${KEY}` placeholder is not exported as a channel: it is counted and listed under `x-mesh.not_statically_resolved` (not statically resolved is not absence). A page holds a bounded number of channels and says `PARTIAL DOCUMENT`, how many channels are not shown and the next `offset`: never read a page as the whole graph. `diff`: the declared AsyncAPI 2.x / 3.x document(s) the index found, compared with the code under the engine's own topic keys (lower case, `{param}` and wildcard subjects matched, `kafka:orders` qualifiers read): declared channels or operations not in code, channels or directions in code and not declared, payload disagreements (declared properties against the code type's fields) and broker disagreements (declared server protocol against the broker the code uses; an unknown broker or a protocol that names no broker makes no claim). Declared files are read from disk at call time, the code from the index. 'Not in code' means not statically resolved in the indexed roots, never proof of absence. Paged with `limit` / `offset`, narrowed with `scope`. DO NOT USE to find who publishes or consumes one named event (use analyze_impact), to trace a flow (use trace_flow), to list anomalies (use event_anomalies), or for gRPC / HTTP contracts (use analyze_grpc). It does NOT write or modify any AsyncAPI file, does NOT export OpenAPI, does NOT see topics built at runtime, does NOT read payload types outside TypeScript / JavaScript and Java, and does NOT follow external `$ref`s of a declared document.";
    type Args = ExportAsyncApiArgs;

    fn meta(args: &Self::Args) -> Option<&RequestMeta> {
        args._meta.as_ref()
    }

    fn truncation_hint(_args: &Self::Args, _state: &AppState) -> Option<String> {
        Some(
            "AsyncAPI output exceeds payload budget. Lower `limit`, page with `offset`, or narrow with `scope` (a repository or directory). A cut document is not a complete one."
                .to_string(),
        )
    }

    fn subject(args: &Self::Args) -> Option<&str> {
        args.scope.as_deref()
    }

    fn run(args: &Self::Args, state: &AppState) -> Result<ToolOutput, ToolError> {
        let raw_scope = args
            .scope
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());
        let scope = SearchScope::resolve(raw_scope, state)?;
        let snapshot = state.snapshot();
        let is_test =
            |n: &mesh_core::ContractNode| mesh_core::is_test_node(n, &state.allowed_roots);
        let in_scope = |n: &mesh_core::ContractNode| match &scope {
            SearchScope::Global => true,
            SearchScope::Scoped(dir) => starts_with_folded(&n.file_path, dir.as_path()),
        };
        let events = snapshot.contract_graph.observed_events(&is_test);
        let observed = ObservedSet::from_core(&events, &state.allowed_roots, &in_scope);
        let mut coverage = coverage_notes(&snapshot.health);
        if snapshot.health.sql_sites_unresolved > 0 {
            coverage.push(format!(
                "{} SQL producer/consumer site(s) whose channel name is not a literal",
                snapshot.health.sql_sites_unresolved
            ));
        }
        let coverage = (!coverage.is_empty()).then(|| coverage.join("; "));
        let mode = args.mode.unwrap_or(AsyncApiMode::Export);
        let offset = args.offset.unwrap_or(0) as usize;

        match mode {
            AsyncApiMode::Export => {
                let view = ExportView {
                    offset,
                    limit: args
                        .limit
                        .unwrap_or(DEFAULT_EXPORT_LIMIT)
                        .clamp(1, MAX_LIMIT) as usize,
                    json: args.format == Some(AsyncApiFormat::Json),
                    scope: raw_scope,
                    coverage,
                    roots: state.allowed_roots.len(),
                };
                Ok(ToolOutput::text(format_export(&observed, &view)))
            }
            AsyncApiMode::Diff => {
                let mut docs: Vec<(DeclaredDoc, bool)> = Vec::new();
                // `skipped`: every file named in the footer. `unread`: the subset that may have
                // declared a channel (cut, unreadable, unsupported, not indexed): while there
                // is one, an observed channel no read document declares is "not compared".
                let mut skipped: Vec<String> = Vec::new();
                let mut unread: Vec<String> = Vec::new();
                let mut accessed: Vec<String> = Vec::new();
                let under = |path: &Path| match &scope {
                    SearchScope::Global => true,
                    SearchScope::Scoped(dir) => starts_with_folded(path, dir.as_path()),
                };
                let label_of = |path: &Path, repo_id: Option<usize>| {
                    let root = match repo_id {
                        Some(i) => state.allowed_roots.get(i),
                        None => state.allowed_roots.iter().find(|r| path.starts_with(r)),
                    };
                    root.and_then(|root| path.strip_prefix(root).ok())
                        .map_or_else(|| path.display().to_string(), slash_path)
                };
                let mut files = snapshot.contract_graph.declared_asyncapi_files();
                let indexed = files.len();
                // The cap counts the documents under the scope first: a scope never loses its own
                // document to out-of-scope ones. What the cap still cuts is `unread`.
                files.sort_by_key(|(path, _)| !under(path));
                for (i, (path, repo_id)) in files.iter().enumerate() {
                    let label = label_of(path, Some(*repo_id as usize));
                    if i >= MAX_DECLARED_FILES {
                        let why = format!(
                            "`{label}` (past the {MAX_DECLARED_FILES} declared documents read per call)"
                        );
                        unread.push(why.clone());
                        skipped.push(why);
                        continue;
                    }
                    accessed.push(label.clone());
                    let content = match read_declared(path, state) {
                        Ok(c) => c,
                        Err(why) => {
                            let why = format!("`{label}` ({why})");
                            unread.push(why.clone());
                            skipped.push(why);
                            continue;
                        }
                    };
                    match parse_declared(&label, &content) {
                        DeclaredParse::Doc(d) => docs.push((d, under(path))),
                        DeclaredParse::NotAsyncApi => skipped.push(format!(
                            "`{label}` (no top-level `asyncapi` version: not an AsyncAPI document)"
                        )),
                        DeclaredParse::Unsupported(v) => {
                            let why = format!(
                                "`{label}` (AsyncAPI version `{}` is not 2.x or 3.x)",
                                v.replace(['`', '\n', '\r'], "'")
                            );
                            unread.push(why.clone());
                            skipped.push(why);
                        }
                        DeclaredParse::Unreadable(e) => {
                            let why = format!(
                                "`{label}` (not readable as YAML: {})",
                                e.replace(['`', '\n', '\r'], "'")
                            );
                            unread.push(why.clone());
                            skipped.push(why);
                        }
                    }
                }
                // A file the indexer rejected (oversized, unreadable, ...) never reached the
                // graph, so it is not in `files`: an AsyncAPI-looking one may declare channels.
                for r in &snapshot.health.rejected {
                    let name = r
                        .path
                        .file_name()
                        .map(|n| n.to_string_lossy().to_ascii_lowercase())
                        .unwrap_or_default();
                    let spec = name.contains("asyncapi")
                        && [".yaml", ".yml", ".json"].iter().any(|e| name.ends_with(e));
                    if spec {
                        let why = format!(
                            "`{}` (not indexed: {})",
                            label_of(&r.path, None),
                            r.describe()
                        );
                        unread.push(why.clone());
                        skipped.push(why);
                    }
                }
                if snapshot.health.rejected_overflow > 0 {
                    unread.push(format!(
                        "{} rejected path(s) are not listed (the index lists a bounded number), one may be an AsyncAPI document",
                        snapshot.health.rejected_overflow
                    ));
                }
                let view = DiffView {
                    limit: args.limit.unwrap_or(DEFAULT_DIFF_LIMIT).clamp(1, MAX_LIMIT) as usize,
                    offset,
                    scope: raw_scope,
                    coverage,
                    roots: state.allowed_roots.len(),
                    skipped_files: skipped,
                };
                let text = if docs.is_empty() {
                    no_declared_document(&view, indexed)
                } else {
                    format_diff(&diff_asyncapi(&observed, &docs, &unread), &view)
                };
                let mut out = ToolOutput::text(text);
                out.files_accessed = accessed;
                Ok(out)
            }
        }
    }
}

/// A relative path with `/` separators on every platform.
fn slash_path(rel: &Path) -> String {
    rel.components()
        .map(|c| c.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

/// The answer when no declared document could be compared: an absent declaration is never
/// "everything is undeclared".
fn no_declared_document(view: &DiffView<'_>, indexed: usize) -> String {
    let mut out = String::from("## AsyncAPI declared-versus-code diff\n");
    if indexed == 0 && view.skipped_files.is_empty() {
        out.push_str("No AsyncAPI document is indexed in this workspace (a YAML or JSON file named like `asyncapi.yaml` or holding an `asyncapi:` key, with at least one channel; `[engines.contracts.asyncapi]` scopes which files count), so there is nothing to compare. This is not a finding about the code: use `mode: export` to produce a document from it.\n");
    } else {
        out.push_str(
            "Every indexed AsyncAPI candidate was skipped, so there is nothing to compare:\n",
        );
        for s in &view.skipped_files {
            out.push_str(&format!("- {s}\n"));
        }
    }
    out
}

/// A declared document's text: never through a symlink, always inside the jail, refused before
/// the read when over the schema size budget (the same rule `diff_mesh` applies to a file).
fn read_declared(abs: &Path, state: &AppState) -> Result<String, String> {
    let meta = std::fs::symlink_metadata(abs).map_err(|e| format!("unreadable: {e}"))?;
    if meta.file_type().is_symlink() {
        return Err("symlinked declared document not read: a symlink is never followed".into());
    }
    if !meta.is_file() {
        return Err("not a regular file".into());
    }
    ValidatedScope::resolve_with_aliases(
        &abs.to_string_lossy(),
        &state.allowed_roots,
        &state.config.workspace.mount_aliases,
        None,
    )
    .map_err(|e| format!("refused by the path jail: {e}"))?;
    let limit = AstGuard::size_budget(abs);
    if meta.len() > limit {
        return Err(format!("oversized: {} bytes, budget {limit}", meta.len()));
    }
    let bytes = std::fs::read(abs).map_err(|e| format!("unreadable: {e}"))?;
    if AstGuard::looks_binary(&bytes) {
        return Err("binary content".into());
    }
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}
