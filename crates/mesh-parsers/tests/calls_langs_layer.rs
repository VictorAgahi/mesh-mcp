//! 9.0 calls layer on Java (Spring), Go and Python: extraction through
//! `PolyglotIndexer` and resolution on the layered EDA fixtures (`examples/eda`).
//! Expected hops are the ones written by hand in `tests/golden/events/*.expected.yaml`.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use mesh_core::{ContractGraph, EdgeConfidence};
use mesh_parsers::languages::PolyglotIndexer;
use std::path::{Path, PathBuf};

fn fixture_dir(corpus: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../examples/eda")
        .join(corpus)
}

fn source_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let mut entries: Vec<PathBuf> = std::fs::read_dir(dir)
        .expect("fixture dir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .collect();
    entries.sort();
    for p in entries {
        if p.is_dir() {
            source_files(&p, out);
        } else if p
            .extension()
            .is_some_and(|x| matches!(x.to_str(), Some("java" | "go" | "py")))
        {
            out.push(p);
        }
    }
}

/// Indexes a fixture; `edit` may rewrite a file's text (a fixture variant, never the
/// fixture itself).
fn index(corpus: &str, edit: &dyn Fn(&str, String) -> String) -> ContractGraph {
    let mut files = Vec::new();
    source_files(&fixture_dir(corpus), &mut files);
    let mut graph = ContractGraph::new();
    for f in files {
        let name = f.file_name().unwrap().to_str().unwrap().to_string();
        // CRLF checkouts (Windows) would defeat the textual edits below.
        let text = std::fs::read_to_string(&f)
            .expect("read")
            .replace("\r\n", "\n");
        let content = edit(&name, text);
        PolyglotIndexer::index_file(&f, &content, 0, &mut graph);
    }
    graph.reconcile_edges();
    graph
}

fn calls(graph: &ContractGraph) -> Vec<(String, String, EdgeConfidence)> {
    let name = |id| {
        graph
            .get_node(id)
            .or_else(|| graph.callable_nodes().iter().find(|n| n.id == id))
            .map_or(String::new(), |n| n.name.to_string())
    };
    let mut out: Vec<_> = graph
        .call_edges()
        .iter()
        .map(|e| (name(e.from), name(e.to), e.confidence))
        .collect();
    out.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
    out
}

fn edges(list: &[(&str, &str, EdgeConfidence)]) -> Vec<(String, String, EdgeConfidence)> {
    let mut v: Vec<_> = list
        .iter()
        .map(|(a, b, c)| (a.to_string(), b.to_string(), *c))
        .collect();
    v.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
    v
}

const X: EdgeConfidence = EdgeConfidence::Exact;
const A: EdgeConfidence = EdgeConfidence::Ambiguous;

#[test]
fn layered_go_resolves_the_golden_hops_on_an_entry_to_producer_path() {
    let g = index("layered-go", &|_, c| c);
    // Golden h1, h3-h6. `OrderService.Handle -> DefaultPricer.Price` (h2) is resolved
    // but lies on no entry -> producer path, so callable pruning drops it.
    assert_eq!(
        calls(&g),
        edges(&[
            ("OrderConsumer.Run", "OrderService.Handle", X),
            ("OrderService.Handle", "KafkaPublisher.Publish", A),
            ("OrderService.Handle", "AuditPublisher.Publish", A),
            ("BillingConsumer.Run", "BillingService.Invoice", X),
            ("BillingService.Invoice", "InvoiceWriter.Write", X),
        ])
    );
    assert_eq!(g.call_stats().dispatch_overflow, 0);
}

#[test]
fn layered_py_without_annotations_resolves_only_the_imported_function_hop() {
    // The fixture's `__init__(self, handler)` parameters carry no annotation: no type,
    // no edge (a name-based guess is not made). Golden h4 is an imported function.
    let g = index("layered-py", &|_, c| c);
    assert_eq!(calls(&g), edges(&[("generate_report", "build_report", X)]));
}

#[test]
fn layered_py_with_annotated_parameters_resolves_every_golden_hop() {
    // The same fixture with the annotations a typed codebase would carry (a variant
    // written in memory; the fixture and its golden are untouched).
    let g = index("layered-py", &|name, c| {
        match name {
        "consumer.py" => c
            .replace(
                "from confluent_kafka import Consumer\n",
                "from confluent_kafka import Consumer\nfrom layered_py.handler import OrderHandler\n",
            )
            .replace("def __init__(self, handler):", "def __init__(self, handler: OrderHandler):"),
        "handler.py" => format!("from layered_py.service import InvoiceService\n{c}")
            .replace("def __init__(self, service):", "def __init__(self, service: InvoiceService):"),
        "service.py" => c
            .replace(
                "from layered_py.tasks import generate_report\n",
                "from layered_py.tasks import generate_report\nfrom layered_py.publisher import InvoicePublisher\n",
            )
            .replace(
                "def __init__(self, publisher):",
                "def __init__(self, publisher: \"InvoicePublisher\"):",
            ),
        _ => c,
    }
    });
    assert_eq!(
        calls(&g),
        edges(&[
            ("OrderConsumer.run", "OrderHandler.handle", X),
            ("OrderHandler.handle", "InvoiceService.issue", X),
            ("InvoiceService.issue", "InvoicePublisher.announce", X),
            ("generate_report", "build_report", X),
        ])
    );
}

#[test]
fn spring_monolith_resolves_the_golden_hops_through_events_and_schedules() {
    // Spring application events and `@Scheduled` entries are read since T4: the unmodified
    // fixture resolves the eight golden hops (h7 and h8 start from the cron entry).
    let g = index("spring-monolith", &|_, c| c);
    assert_eq!(
        calls(&g),
        edges(&[
            ("BillingListener.onOrderPlaced", "BillingService.charge", X),
            (
                "BillingService.charge",
                "PaymentPublisher.publishCharged",
                X
            ),
            (
                "BillingService.reconcile",
                "PaymentPublisher.publishReconciled",
                X
            ),
            ("OrderController.create", "OrderService.place", X),
            ("OrderService.place", "OrderEventPublisher.announce", X),
            (
                "PaymentsKafkaListener.onCharged",
                "FulfilmentService.start",
                X
            ),
            ("ReconcileJob.hourly", "BillingService.reconcile", X),
            (
                "ShipmentListener.onFulfilmentStarted",
                "ShipmentService.dispatch",
                X
            ),
        ])
    );
}

/// The Kafka shapes the Java extractor read before in-process events existed
/// (`@KafkaListener`, `KafkaTemplate.send`) still produce the golden hops through the calls
/// layer: the variant swaps the fixture's `@EventListener` / `publishEvent` pairs for them.
#[test]
fn spring_monolith_kafka_variant_resolves_the_same_hops() {
    let g = index("spring-monolith", &|name, c| match name {
        "BillingListener.java" => c
            .replace(
                "import org.springframework.context.event.EventListener;",
                "import org.springframework.kafka.annotation.KafkaListener;",
            )
            .replace(
                "@EventListener",
                "@KafkaListener(topics = \"order.placed\")",
            ),
        "ShipmentListener.java" => c.replace(
            "@TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)",
            "@KafkaListener(topics = \"fulfilment.started\")",
        ),
        "OrderEventPublisher.java" => kafka_publisher(
            c,
            "events.publishEvent(new OrderPlacedEvent(orderId, total));",
            "order.placed",
        ),
        "FulfilmentService.java" => kafka_publisher(
            c,
            "events.publishEvent(new FulfilmentStartedEvent(orderId));",
            "fulfilment.started",
        ),
        _ => c,
    });
    let got = calls(&g);
    for (from, to) in [
        ("OrderController.create", "OrderService.place"),
        ("OrderService.place", "OrderEventPublisher.announce"),
        ("BillingListener.onOrderPlaced", "BillingService.charge"),
        ("BillingService.charge", "PaymentPublisher.publishCharged"),
        ("PaymentsKafkaListener.onCharged", "FulfilmentService.start"),
        (
            "ShipmentListener.onFulfilmentStarted",
            "ShipmentService.dispatch",
        ),
        ("ReconcileJob.hourly", "BillingService.reconcile"),
    ] {
        assert!(
            got.iter().any(|(f, t, c)| f == from && t == to && *c == X),
            "{from} -> {to} in {got:?}"
        );
    }
    // The Kafka producers are Kafka, not in-process: the variant has no `InProc` site left on
    // the two swapped publishers.
    let path =
        fixture_dir("spring-monolith").join("src/main/java/com/shop/OrderEventPublisher.java");
    let fi = PolyglotIndexer::extract(
        &path,
        &kafka_publisher(
            std::fs::read_to_string(&path)
                .expect("read")
                .replace("\r\n", "\n"),
            "events.publishEvent(new OrderPlacedEvent(orderId, total));",
            "order.placed",
        ),
        0,
    );
    assert_eq!(fi.producers.len(), 1);
    assert_eq!(fi.producers[0].1, "order.placed");
    assert_eq!(fi.producer_brokers, vec![(0, mesh_core::Broker::Kafka)]);
}

fn kafka_publisher(c: String, publish: &str, topic: &str) -> String {
    c.replace(
        "import org.springframework.context.ApplicationEventPublisher;",
        "import org.springframework.kafka.core.KafkaTemplate;",
    )
    .replace(
        "ApplicationEventPublisher events",
        "KafkaTemplate<String, String> kafkaTemplate",
    )
    .replace(
        "this.events = events;",
        "this.kafkaTemplate = kafkaTemplate;",
    )
    .replace(
        publish,
        &format!("kafkaTemplate.send(\"{topic}\", orderId);"),
    )
}
