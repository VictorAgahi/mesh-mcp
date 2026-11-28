use crate::indexer::WorkspaceIndexer;
use crate::protocol::RequestMeta;
use crate::tools::{McpTool, ToolError, ToolOutput};
use mesh_core::{AppState, CompactStr, FlowBounds, FlowDirection, FlowEntry};
use mesh_parsers::topology::{display_name, inline_code_text};
use mesh_parsers::{
    render_flow_view, Aggregate, FlowViewInput, GraphRenderer, TopologyOptions, MAX_FLOW_VIEW_NODES,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct VisualizeMeshArgs {
    #[serde(default = "default_format")]
    #[schemars(
        with = "String",
        description = "Output format: 'mermaid' (default, GitHub-compatible Mermaid Markdown), 'json' (aggregated graph payload) or 'html' (standalone page). Every format renders the per-service aggregated view, never the raw contract graph."
    )]
    pub format: Option<CompactStr>,

    #[serde(default)]
    #[schemars(
        with = "Option<String>",
        description = "Zoom into one service (a name shown in the default view or listed as hidden in its footer): draws that service's contracts and the services they talk to, and, in the mermaid format only, lists its HTTP routes (bounded; json and html do not carry the route list). Omit for the whole-mesh service view."
    )]
    pub service: Option<CompactStr>,

    #[serde(default)]
    #[schemars(
        description = "Most services/topics drawn before the rest fold into one 'other' node (1-200, default 40). The view shrinks further on its own to stay under the 48 KB cap. DO NOT raise it to see everything; zoom with `service` instead."
    )]
    pub max_services: Option<u32>,

    #[serde(default)]
    #[schemars(
        with = "Option<String>",
        description = "Event flow view: centre the diagram on ONE event (an event / topic / queue / stream name, ex: 'order.requested', or its broker-qualified key 'kafka:orders'; a bare name used on several brokers draws each broker's event apart, and a substring match is reported). Returns a Mermaid `flowchart TD` (mermaid format only) with the producers upstream and the consumers downstream, each node with its path:line, solid arrows for exact links and dashed ones for heuristic / ambiguous. Chains end at a dashed 'not statically resolved' node or at a stub that names the bound that cut them, never at a bare dead end. DO NOT combine with `service`; DO NOT pass a file path, a route or a handler (use trace_flow for those)."
    )]
    pub event: Option<CompactStr>,

    #[serde(default)]
    #[schemars(
        range(min = 1, max = 5),
        description = "With `event`: producer levels drawn upstream of the event (default 2, 1 to 5; a larger value is clamped, the header shows the value used)."
    )]
    pub hops_up: Option<u32>,

    #[serde(default)]
    #[schemars(
        range(min = 1, max = 5),
        description = "With `event`: consumer levels drawn downstream of the event (default 2, 1 to 5; a larger value is clamped, the header shows the value used)."
    )]
    pub hops_down: Option<u32>,

    #[serde(default)]
    #[schemars(
        with = "Option<String>",
        description = "With `event`: 'both' (default), 'upstream' (what ends up publishing the event) or 'downstream' (what handling the event triggers)."
    )]
    pub direction: Option<CompactStr>,

    #[serde(default)]
    #[schemars(
        description = "With `event`: draw every call between a consumer and the publish it reaches as a node (default false: the calls fold into the arrow label, '3 calls'). Costs nodes; leave off unless the call path itself is the question."
    )]
    pub detail: Option<bool>,

    #[serde(default)]
    #[schemars(
        range(min = 1, max = 200),
        description = "With `event`: most nodes drawn (events and code nodes, default 60, 1 to 200). What the cap cuts is counted ('N nodes not shown (cap)') and marked on the diagram. DO NOT raise it to see everything; lower `hops_up` / `hops_down` or centre on a nearer event."
    )]
    pub limit: Option<u32>,

    #[serde(default)]
    #[schemars(
        range(min = 1, max = 50),
        description = "With `event`: consumers, producers or callees followed per node (default 20, 1 to 50); the rest is reported as '+K more'."
    )]
    pub fan_out: Option<u32>,

    #[serde(default)]
    #[schemars(
        description = "With `event`: follow sites in test files and test-only directories. Default false: they are left out and counted."
    )]
    pub include_tests: Option<bool>,

    // Accepted for W3C trace propagation, hidden from `tools/list`: the model
    // cannot use it, and it cost every session ~200 schema tokens.
    #[schemars(skip)]
    #[serde(default)]
    pub _meta: Option<RequestMeta>,
}

fn default_format() -> Option<CompactStr> {
    Some(CompactStr::new("mermaid"))
}

const DEFAULT_MAX_SERVICES: u32 = 40;
const DEFAULT_FLOW_NODES: u32 = 60;
const DEFAULT_FLOW_HOPS: u32 = 2;
const MAX_SERVICES_CAP: u32 = 200;
/// Rendered payload budget, under the 48 KB MCP cap with room for the footer.
const RENDER_BUDGET: usize = 44 * 1024;

pub struct VisualizeMeshTool;

impl McpTool for VisualizeMeshTool {
    const NAME: &'static str = "visualize_mesh";
    const DESCRIPTION: &'static str = "Generate a per-service architecture topology of the polyglot mesh (services, gRPC/HTTP contracts, topics, weighted cross-service links) in Mermaid, JSON or HTML. Zoom into one service with `service`: in the mermaid format it then also lists the service's HTTP routes (verb, path, handler, file:line) and its contract counts by kind; json and html do NOT include them. With `event` it instead draws a Mermaid flow view centred on one event: the producers upstream and the consumers downstream (`hops_up` / `hops_down`, default 2 each), with path:line per node, confidence by arrow style, and an explicit marker where a chain is not statically resolved or a bound cut it. DO NOT USE for a targeted question about one symbol (use find_dependents / analyze_grpc), for the step-by-step call path of a chain (use trace_flow), or for a flat list of an event's producers and consumers (use analyze_impact). The event view does NOT follow HTTP client calls across repositories, dynamic dispatch or runtime registries, and is mermaid only.";
    type Args = VisualizeMeshArgs;

    fn meta(args: &Self::Args) -> Option<&RequestMeta> {
        args._meta.as_ref()
    }

    fn subject(args: &Self::Args) -> Option<&str> {
        args.event.as_deref()
    }

    fn returns_document(args: &Self::Args) -> bool {
        args.format
            .as_deref()
            .is_some_and(|f| f.eq_ignore_ascii_case("json") || f.eq_ignore_ascii_case("html"))
    }

    fn truncation_hint(args: &Self::Args, state: &AppState) -> Option<String> {
        if let Some(event) = &args.event {
            return Some(format!(
                "Flow view for '{event}' exceeds payload budget. Lower `hops_up` / `hops_down` or `limit`, or turn `detail` off."
            ));
        }
        let snapshot = state.snapshot();
        let graph = &snapshot.contract_graph;
        Some(format!(
            "Total graph nodes: {}, total edges: {}. Zoom with `service` or lower `max_services`.",
            graph.node_count(),
            graph.edge_count()
        ))
    }

    fn run(args: &Self::Args, state: &AppState) -> Result<ToolOutput, ToolError> {
        let format_choice = args
            .format
            .as_deref()
            .unwrap_or("mermaid")
            .to_ascii_lowercase();
        if !matches!(format_choice.as_str(), "mermaid" | "json" | "html") {
            return Err((
                -32602,
                format!("Unknown format '{format_choice}': use 'mermaid', 'json' or 'html'."),
            ));
        }

        if args.event.is_some() {
            return run_event_view(args, state, &format_choice);
        }
        let snapshot = state.snapshot();
        let graph = &snapshot.contract_graph;
        let workspace_name = &state.config.workspace.name;
        let repo_names = WorkspaceIndexer::repo_names(&state.allowed_roots);

        let mut opts = TopologyOptions {
            max_groups: args
                .max_services
                .unwrap_or(DEFAULT_MAX_SERVICES)
                .clamp(1, MAX_SERVICES_CAP) as usize,
            focus: args.service.as_ref().map(|s| s.to_string()),
            roots: state.allowed_roots.to_vec(),
            ..TopologyOptions::default()
        };

        // The O(contracts + edges) fold runs once; the shrink loop below only
        // re-selects over the (small) aggregated groups and links.
        let aggregate = Aggregate::build(
            graph,
            &repo_names,
            opts.focus.as_deref(),
            &state.allowed_roots,
        )
        .with_http_prefixes(state.http_prefixes.clone());
        if let (Some(asked), None) = (&opts.focus, aggregate.focus()) {
            return Err((
                -32602,
                format!(
                    "No service named '{}'. Call visualize_mesh without `service` to list them.",
                    display_name(asked)
                ),
            ));
        }

        // Shrink until the rendering fits the budget: the same "show the top
        // groups, fold the rest, say how to zoom" contract `smart_search`'s
        // truncation follows, instead of cutting a document in half.
        loop {
            let topology = aggregate.select(&opts);
            let body = match format_choice.as_str() {
                "html" => GraphRenderer::payload_to_html(&topology.to_payload(workspace_name)),
                "json" => serde_json::to_string_pretty(&topology.to_payload(workspace_name))
                    .unwrap_or_else(|_| "{}".to_string()),
                _ => format!(
                    "## Polyglot Architecture Mesh Topology{}\n\n```mermaid\n{}```\n",
                    topology
                        .focus
                        .as_deref()
                        .map(|f| format!(" — service `{}`", inline_code_text(f)))
                        .unwrap_or_default(),
                    topology.to_mermaid(workspace_name)
                ),
            };
            // The footer (folded groups, zoom hint, HTTP routes of a zoomed service)
            // is part of the answer: it counts against the budget for Mermaid.
            let footer = if format_choice == "mermaid" {
                topology.footer()
            } else {
                String::new()
            };
            // The mermaid footer follows the body after one newline.
            let footer_cost = if footer.is_empty() {
                0
            } else {
                footer.len() + 1
            };
            if body.len() + footer_cost <= RENDER_BUDGET {
                let text = match format_choice.as_str() {
                    // The page / document must stay parseable: no appended prose.
                    "html" | "json" => body,
                    _ => format!("{body}\n{footer}"),
                };
                return Ok(ToolOutput::text(text));
            }
            if opts.max_groups <= 1 && opts.max_focus_contracts <= 1 {
                // Names are capped, so this is not expected; but a document cut
                // by the central 48 KB truncation would be unparseable, so say
                // so instead of returning half of one.
                return Err((
                    -32602,
                    format!(
                        "The smallest {format_choice} view is still {} KB, over the 48 KB cap. Use format 'mermaid' or zoom with `service`.",
                        body.len() / 1024
                    ),
                ));
            }
            opts.max_groups = (opts.max_groups / 2).max(1);
            opts.max_focus_contracts = (opts.max_focus_contracts / 2).max(1);
        }
    }
}

/// The event view (9.0 T13): the chains of the `trace_flow` engine, forward and reverse from one
/// event, folded into one Mermaid flowchart.
fn run_event_view(
    args: &VisualizeMeshArgs,
    state: &AppState,
    format_choice: &str,
) -> Result<ToolOutput, ToolError> {
    let event = args.event.as_deref().unwrap_or_default().trim();
    if event.is_empty() {
        return Err((-32602, "`event` must not be empty".to_string()));
    }
    if format_choice != "mermaid" {
        return Err((
            -32602,
            format!("The event view is Mermaid only: omit `format` or use 'mermaid' (got '{format_choice}')."),
        ));
    }
    if args.service.is_some() {
        return Err((
            -32602,
            "`event` and `service` are two different views: pass one of them.".to_string(),
        ));
    }
    let direction = args
        .direction
        .as_deref()
        .unwrap_or("both")
        .to_ascii_lowercase();
    let (up, down) = match direction.as_str() {
        "both" => (true, true),
        "upstream" | "up" => (true, false),
        "downstream" | "down" => (false, true),
        other => {
            return Err((
                -32602,
                format!("Unknown direction '{other}': use 'both', 'upstream' or 'downstream'."),
            ))
        }
    };
    let snapshot = state.snapshot();
    let graph = &snapshot.contract_graph;
    let include_tests = args.include_tests.unwrap_or(false);
    let hide = |n: &mesh_core::ContractNode| {
        !include_tests && mesh_core::is_test_node(n, &state.allowed_roots)
    };
    let centres = graph.flow_centres(event);
    if !centres.matched {
        let shown = mesh_parsers::anomalies_shown_key(event);
        return Ok(ToolOutput::text(format!(
            "## Event flow view for `{shown}`\n\n*No event matched `{shown}` across {} indexed workspace roots (0 matches). Pass an event / topic / queue name or its broker-qualified key (`kafka:orders`); a name built at runtime is not indexed. For a route or a handler use `trace_flow`.*\n",
            state.allowed_roots.len()
        )));
    }
    let bounds = |hops: Option<u32>| {
        FlowBounds::clamped(
            None,
            Some(hops.unwrap_or(DEFAULT_FLOW_HOPS) as usize),
            args.fan_out.map(|v| v as usize),
        )
    };
    let up_trace = up.then(|| {
        graph.trace_flow(
            FlowEntry::Topic(event),
            FlowDirection::Reverse,
            bounds(args.hops_up),
            &hide,
        )
    });
    let down_trace = down.then(|| {
        graph.trace_flow(
            FlowEntry::Topic(event),
            FlowDirection::Forward,
            bounds(args.hops_down),
            &hide,
        )
    });
    let limit = args
        .limit
        .unwrap_or(DEFAULT_FLOW_NODES)
        .clamp(1, MAX_FLOW_VIEW_NODES as u32) as usize;
    let text = render_flow_view(&FlowViewInput {
        event,
        key: &centres.key,
        hubs: &centres.hubs,
        graph,
        up: up_trace.as_ref(),
        down: down_trace.as_ref(),
        roots: &state.allowed_roots,
        detail: args.detail.unwrap_or(false),
        limit,
        tests_hidden: !include_tests,
    });
    Ok(ToolOutput::text(
        crate::tools::analyze_impact::with_literal_clash_note(text, graph, event),
    ))
}
