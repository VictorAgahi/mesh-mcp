use crate::protocol::RequestMeta;
use crate::tools::{McpTool, ToolError, ToolOutput};
use mesh_core::routes::{find_routes, RouteQuery, RouteRank};
use mesh_core::{AppState, CompactStr, FlowBounds, FlowDirection, FlowEntry};
use mesh_parsers::MarkdownFormatter;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// What `target` names. `auto` (the default): a route when written `VERB /path`,
/// else an event when one matches, else a handler symbol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum EntryKind {
    Auto,
    Topic,
    Route,
    Handler,
    Cron,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    Forward,
    Reverse,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TraceFlowArgs {
    #[schemars(
        with = "String",
        description = "Where the flow starts: an event / topic / queue / stream name (ex: 'order.requested', 'EVENT_CREATED'), an HTTP route written 'VERB /path' (ex: 'POST /orders'), or a handler / scheduled method as 'Class.method' (ex: 'OrderService.place'). DO NOT pass a file path or a free-text question."
    )]
    pub target: CompactStr,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        description = "What `target` names: auto (default: a 'VERB /path' is a route, else an indexed event, else a handler), topic, route, handler, cron (a scheduled method, resolved like a handler: schedules themselves are not indexed yet)."
    )]
    pub entry_kind: Option<EntryKind>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        description = "forward (default): what handling `target` triggers, entry -> calls -> publish -> next consumers. reverse: what ends up publishing `target`, publish <- callers <- entry <- the producers of what that entry consumes."
    )]
    pub direction: Option<Direction>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        description = "Event levels to chain through topics (default 3, max 5). Level 1 is the entry's own hop."
    )]
    pub event_depth: Option<u8>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        description = "Longest call path followed inside one hop, entry to publish (default 6, max 12)."
    )]
    pub call_depth: Option<u8>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        description = "Callees, consumers or producers followed per node (default 20, max 50); the rest is reported as '+K more'."
    )]
    pub fan_out: Option<u8>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        description = "Number of hops to skip (default 0). Use the `offset` value given in a previous page's 'More hops' footer. DO NOT raise the bounds to see more of a long answer; page with `offset`."
    )]
    pub offset: Option<u32>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        description = "Follow sites in test files and test-only directories (*.spec.ts, *_test.go, __tests__/, ...). Default false: they are left out and counted."
    )]
    pub include_tests: Option<bool>,

    #[serde(default)]
    #[schemars(skip)]
    pub _meta: Option<RequestMeta>,
}

pub struct TraceFlowTool;

impl McpTool for TraceFlowTool {
    const NAME: &'static str = "trace_flow";
    const DESCRIPTION: &'static str = "End-to-end event flow chains, hop by hop: from an event/topic, an HTTP route (`VERB /path`), a handler or a scheduled method, each hop goes entry site -> every resolved intra-repository call (callee declaration path:line and call-site path:line) -> the publish it reaches, then continues at that topic's consumers (forward), or runs backwards from a publish to the entries that reach it and the producers upstream of them (reverse). Every step has a path:line to check and a confidence (exact / heuristic / ambiguous); a chain is as strong as its weakest hop. Calls are resolved without a type checker (this.m(), injected fields, imports, interface implementations); a hop with no resolved call path falls back on producers declared in the entry's own file, labelled heuristic. 'Chain end' means the end of the STATICALLY RESOLVED chain (a runtime-built topic, a registry, an unindexed root may continue it), never proof that nothing follows; 'Depth limit reached' is a bound, not an end. Bounded by event_depth, call_depth, fan_out; paged by hop with `offset`. DO NOT USE for the flat list of every producer and consumer of an event or a gRPC impact (use analyze_impact), for 'who calls this gRPC method' (use analyze_grpc), or for imports (use find_dependents). It does NOT follow HTTP client calls across repositories, dynamic dispatch through reflection or runtime registries, or calls through local variables.";
    type Args = TraceFlowArgs;

    fn meta(args: &Self::Args) -> Option<&RequestMeta> {
        args._meta.as_ref()
    }

    fn truncation_hint(args: &Self::Args, _state: &AppState) -> Option<String> {
        Some(format!(
            "Flow trace for '{}' exceeds payload budget. Page with `offset`, or lower `event_depth` / `fan_out`.",
            args.target
        ))
    }

    fn subject(args: &Self::Args) -> Option<&str> {
        Some(args.target.as_str())
    }

    fn run(args: &Self::Args, state: &AppState) -> Result<ToolOutput, ToolError> {
        let snapshot = state.snapshot();
        let graph = &snapshot.contract_graph;
        let target = args.target.trim();
        if target.is_empty() {
            return Err((-32602, "`target` must not be empty".to_string()));
        }
        let include_tests = args.include_tests.unwrap_or(false);
        let hide = |n: &mesh_core::ContractNode| {
            !include_tests && mesh_core::is_test_node(n, &state.allowed_roots)
        };
        let direction = match args.direction {
            Some(Direction::Reverse) => FlowDirection::Reverse,
            _ => FlowDirection::Forward,
        };
        let bounds = FlowBounds::clamped(
            args.call_depth.map(usize::from),
            args.event_depth.map(usize::from),
            args.fan_out.map(usize::from),
        );
        let route = RouteQuery::parse_target(target);
        let kind = match args.entry_kind.unwrap_or(EntryKind::Auto) {
            EntryKind::Auto if route.is_some() => EntryKind::Route,
            EntryKind::Auto => {
                let flow = graph.analyze_impact(target);
                if flow.topics.is_empty()
                    && flow.upstream_producers.is_empty()
                    && flow.downstream_consumers.is_empty()
                {
                    EntryKind::Handler
                } else {
                    EntryKind::Topic
                }
            }
            k => k,
        };
        let entry = match kind {
            EntryKind::Route => {
                let Some(route) = route else {
                    return Err((
                        -32602,
                        format!(
                            "`{target}` is not a route: write it 'VERB /path' (ex: 'POST /orders')"
                        ),
                    ));
                };
                let (hits, _) = find_routes(
                    graph,
                    &route,
                    &state.http_prefixes,
                    None,
                    include_tests,
                    &state.allowed_roots,
                    RouteRank::Value,
                );
                FlowEntry::Route(hits.iter().map(|h| h.node.id).collect())
            }
            EntryKind::Handler => FlowEntry::Handler(target),
            EntryKind::Cron => FlowEntry::Cron(target),
            EntryKind::Topic | EntryKind::Auto => FlowEntry::Topic(target),
        };
        let mut trace = graph.trace_flow(entry, direction, bounds, &hide);
        trace.target = CompactStr::new(target);
        let offset = args.offset.unwrap_or(0) as usize;
        Ok(ToolOutput::text(MarkdownFormatter::format_flow_trace(
            &trace,
            &state.allowed_roots,
            offset,
        )))
    }
}
