//! 9.0 calls layer on TypeScript: extraction through `PolyglotIndexer` and resolution
//! on the layered EDA fixtures (`examples/eda`). Expected hops are the ones written by
//! hand in `tests/golden/events/*.expected.yaml`.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use mesh_core::{ContractGraph, EdgeConfidence, NodeKind};
use mesh_parsers::languages::PolyglotIndexer;
use std::path::{Path, PathBuf};

fn fixture_dir(corpus: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../examples/eda")
        .join(corpus)
        .join("src")
}

fn index_dir(dir: &Path) -> ContractGraph {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
        .expect("fixture dir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "ts"))
        .collect();
    files.sort();
    let mut graph = ContractGraph::new();
    for f in files {
        let content = std::fs::read_to_string(&f).expect("read");
        PolyglotIndexer::index_file(&f, &content, 0, &mut graph);
    }
    graph.reconcile_edges();
    graph
}

fn calls(graph: &ContractGraph) -> Vec<(String, String, EdgeConfidence)> {
    graph
        .call_edges()
        .iter()
        .map(|e| {
            let name = |id| graph.get_node(id).map_or(String::new(), |n| n.name.to_string());
            (name(e.from), name(e.to), e.confidence)
        })
        .collect()
}

#[test]
fn layered_ts_resolves_every_golden_hop() {
    let graph = index_dir(&fixture_dir("layered-ts"));
    let mut got = calls(&graph);
    got.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
    let x = EdgeConfidence::Exact;
    let a = EdgeConfidence::Ambiguous;
    let mut want: Vec<(String, String, EdgeConfidence)> = [
        ("OrderConsumer.handle", "OrderService.place", x),
        ("OrderController.create", "OrderService.place", x),
        ("OrderService.place", "InventoryService.reserve", x),
        ("InventoryService.reserve", "StockPublisher.publishReserved", x),
        ("ShippingConsumer.onStockReserved", "ShippingService.schedule", x),
        ("ShippingService.schedule", "ShipmentPublisher.publishScheduled", x),
        ("NotificationConsumer.onShipmentScheduled", "NotificationService.dispatch", x),
        // The Notifier token is bound by a runtime factory: both implementations.
        ("NotificationService.dispatch", "EmailNotifier.send", a),
        ("NotificationService.dispatch", "SmsNotifier.send", a),
        ("ReconcileConsumer.onReconcile", "ReconcileService.run", x),
        ("ReconcileService.run", "LedgerService.post", x),
        // forwardRef cycle.
        ("LedgerService.post", "ReconcileService.confirm", x),
        ("ReconcileService.confirm", "LedgerService.post", x),
    ]
    .iter()
    .map(|(f, t, c)| (f.to_string(), t.to_string(), *c))
    .collect();
    want.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
    assert_eq!(got, want);
    // MetricsService (local EventEmitter, logger) is on no path.
    assert!(graph.callable_nodes().iter().all(|n| !n.name.starts_with("Metrics")));

    // Producers sit on the publish line (8.0.1 defects N2 / N9) and anchor on the
    // enclosing method.
    let producer_lines: Vec<(String, usize, Option<String>)> = graph
        .all_edges()
        .iter()
        .filter(|e| e.kind == mesh_core::EdgeKind::Produces)
        .filter_map(|e| graph.get_node(e.from))
        .map(|n| {
            (
                n.file_path.file_name().map_or(String::new(), |f| f.to_string_lossy().into_owned()),
                n.line_start,
                graph.enclosing_callable(n).map(|c| c.name.to_string()),
            )
        })
        .collect();
    for (file, line, anchor) in [
        ("audit.handler.ts", 11, "AuditHandler.onAudit"),
        ("stock.publisher.ts", 9, "StockPublisher.publishReserved"),
        ("ledger.service.ts", 13, "LedgerService.post"),
    ] {
        assert!(
            producer_lines.contains(&(file.to_string(), line, Some(anchor.to_string()))),
            "{file}:{line} in {producer_lines:?}"
        );
    }
}

#[test]
fn layered_ts_is_deterministic() {
    let a = index_dir(&fixture_dir("layered-ts")).fingerprint();
    let b = index_dir(&fixture_dir("layered-ts")).fingerprint();
    assert_eq!(a, b);
}

#[test]
fn monolith_hops_resolve_once_a_producer_is_known() {
    // EventEmitter2 / CQRS producers are not detected before T4, so nothing is on an
    // entry -> producer path and the layer is empty, by design.
    let mut graph = index_dir(&fixture_dir("nestjs-monolith-bus"));
    assert!(graph.call_edges().is_empty());
    // Declare the two publishes the source makes (`eventEmitter.emit` in
    // OrderEvents.announce, `commandBus.execute` in RefundService.request).
    for (file, line, topic) in [("order.events.ts", 9, "order.created"), ("refund.service.ts", 10, "IssueRefundCommand")] {
        let path = fixture_dir("nestjs-monolith-bus").join(file);
        let id = graph.add_node(mesh_core::ContractNode {
            id: 0,
            name: topic.into(),
            kind: NodeKind::EventStream,
            file_path: std::sync::Arc::from(path.as_path()),
            line_start: line,
            line_end: line,
            package: "nestjs-monolith-bus".into(),
            repo_id: 0,
            signature: None,
            docstring: None,
        });
        graph.add_producer(id, topic);
    }
    graph.reconcile_edges();
    let got = calls(&graph);
    for (from, to) in [
        ("OrderController.create", "OrderService.create"),
        ("OrderService.create", "OrderEvents.announce"),
        ("RefundController.request", "RefundService.request"),
    ] {
        assert!(
            got.iter().any(|(f, t, c)| f == from && t == to && *c == EdgeConfidence::Exact),
            "{from} -> {to} in {got:?}"
        );
    }
}
