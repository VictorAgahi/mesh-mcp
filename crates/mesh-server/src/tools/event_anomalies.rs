use crate::protocol::RequestMeta;
use crate::tools::smart_search::{starts_with_folded, SearchScope};
use crate::tools::{McpTool, ToolError, ToolOutput};
use mesh_core::{AppState, CompactStr};
use mesh_parsers::{AnomalyShow, AnomalyView, MarkdownFormatter};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// One class of the report, for the `classes` filter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AnomalyClass {
    PublishedNeverConsumed,
    ConsumedNeverPublished,
    Unresolved,
    Cycles,
    FanOut,
    TestOnlyConsumers,
    PayloadMismatch,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EventAnomaliesArgs {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        with = "Option<String>",
        description = "Only report rows that have a producer or consumer site under this path (a repository or a directory inside one, ex: 'ms-orders' or 'ms-orders/src/billing'); omitted or '.': the whole workspace. The analysis always reads every root, so a producer outside the scope still counts as a producer. DO NOT pass a topic name or a file glob."
    )]
    pub scope: Option<CompactStr>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        description = "Classes to show (default: all): published_never_consumed, consumed_never_published, unresolved, cycles, fan_out, test_only_consumers, payload_mismatch. Counts of every class are always given."
    )]
    pub classes: Option<Vec<AnomalyClass>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        description = "Rows shown per class (1-100, default 25). DO NOT raise it to see everything; page with `offset`, or narrow with `scope` and `classes`."
    )]
    pub limit: Option<u32>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        description = "Rows to skip in each class (default 0). Use the `offset` value given in a class's 'Next page' footer."
    )]
    pub offset: Option<u32>,

    #[serde(default)]
    // Accepted for W3C trace propagation, hidden from `tools/list`.
    #[schemars(skip)]
    pub _meta: Option<RequestMeta>,
}

const DEFAULT_LIMIT: u32 = 25;
const MAX_LIMIT: u32 = 100;

pub struct EventAnomaliesTool;

impl McpTool for EventAnomaliesTool {
    const NAME: &'static str = "event_anomalies";
    const DESCRIPTION: &'static str = "Workspace event anomaly report in one call, over the event graph: topics published but never consumed, topics consumed but never published, cycles (event chains that loop back, with the shortest loop and the call-path sites of each hop), abnormal fan-out (a topic with 6 or more consumer sites, a file publishing onto 6 or more topics), and topics whose only consumers are in test files. Every row gives path:line sites, the broker and a confidence (exact / heuristic / ambiguous); two brokers sharing a topic name are two rows. 'No producer' / 'no consumer' means none was STATICALLY RESOLVED in the indexed roots, never proof of absence: a name that is a `${KEY}` placeholder, a wildcard subscription or a dead-letter / retry channel is listed apart as 'unresolved' with the reason, not as an anomaly, and a runtime-built name or an unindexed root can hide the other end of any row. Test files do not count as production sites. Paged per class with `limit`/`offset`; narrowed with `scope` and `classes`. DO NOT USE to trace what one event triggers hop by hop (use trace_flow), to list every producer and consumer of one named event (use analyze_impact), or for gRPC or imports (use analyze_grpc, find_dependents). It does NOT check payload types, and does NOT detect anomalies in code that registers handlers at runtime.";
    type Args = EventAnomaliesArgs;

    fn meta(args: &Self::Args) -> Option<&RequestMeta> {
        args._meta.as_ref()
    }

    fn truncation_hint(_args: &Self::Args, _state: &AppState) -> Option<String> {
        Some(
            "Anomaly report exceeds payload budget. Page with `offset`, lower `limit`, ask for one class with `classes`, or narrow with `scope` (a repository or directory)."
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
        let report = snapshot.contract_graph.event_anomalies(&is_test, &in_scope);

        let show = match args.classes.as_deref() {
            None | Some([]) => AnomalyShow::ALL,
            Some(classes) => {
                let has = |c| classes.contains(&c);
                AnomalyShow {
                    published_never_consumed: has(AnomalyClass::PublishedNeverConsumed),
                    consumed_never_published: has(AnomalyClass::ConsumedNeverPublished),
                    unresolved: has(AnomalyClass::Unresolved),
                    cycles: has(AnomalyClass::Cycles),
                    fan_out: has(AnomalyClass::FanOut),
                    test_only_consumers: has(AnomalyClass::TestOnlyConsumers),
                    payload_mismatch: has(AnomalyClass::PayloadMismatch),
                }
            }
        };
        let h = &snapshot.health;
        let unread = h.files_rejected_oversized
            + h.files_rejected_guard
            + h.files_read_error
            + h.files_parse_failed;
        let mut coverage = Vec::new();
        if unread > 0 {
            coverage.push(format!(
                "{unread} file(s) were not indexed (oversized, rejected by a guard, unreadable or unparsable; see `mesh-mcp doctor`), so a missing producer or consumer may be in one"
            ));
        }
        if h.config_topics_unresolved > 0 {
            coverage.push(format!(
                "{} config-driven topic name(s) no configuration file resolves",
                h.config_topics_unresolved
            ));
        }
        let view = AnomalyView {
            show,
            limit: args.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT) as usize,
            offset: args.offset.unwrap_or(0) as usize,
            scope: raw_scope,
            coverage: (!coverage.is_empty()).then(|| coverage.join("; ")),
            roots: state.allowed_roots.len(),
        };
        let text = MarkdownFormatter::format_event_anomalies(&report, &state.allowed_roots, &view);
        Ok(ToolOutput::text(text))
    }
}
