//! In-process buses and schedules (9.0 T4) through the real dispatch (`PolyglotIndexer`), the
//! graph and the chain engine: NestJS EventEmitter2 and CQRS, Spring application events,
//! `@Cron` / `@Scheduled` entries. The golden truth of the fixtures is
//! `tests/golden/events/{nestjs-monolith-bus,spring-monolith}.expected.yaml`.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use mesh_core::{
    Broker, ContractGraph, FlowBounds, FlowDirection, FlowEntry, FlowStepKind, NodeKind,
};
use mesh_parsers::languages::{PolyglotIndexer, MAX_INPROC_SITES_PER_FILE};
use mesh_parsers::FileIndex;
use std::path::Path;

fn extract(file: &str, code: &str) -> FileIndex {
    let fi = PolyglotIndexer::extract(Path::new(file), code, 0);
    assert!(!fi.parse_failed, "{file} must parse");
    fi
}

/// `(role, topic)` of every producer and consumer, sorted.
fn sites(fi: &FileIndex) -> Vec<(char, String)> {
    let mut out: Vec<(char, String)> = fi
        .producers
        .iter()
        .map(|(_, t)| ('P', t.to_string()))
        .chain(fi.consumers.iter().map(|(_, t)| ('C', t.to_string())))
        .collect();
    out.sort();
    out
}

fn s(role: char, name: &str) -> (char, String) {
    (role, name.to_string())
}

fn schedules(fi: &FileIndex) -> Vec<(String, usize, String)> {
    let mut out: Vec<_> = fi
        .nodes
        .iter()
        .filter(|n| n.kind == NodeKind::Schedule)
        .map(|n| {
            (
                n.name.to_string(),
                n.line_start,
                n.signature.as_deref().unwrap_or("").to_string(),
            )
        })
        .collect();
    out.sort();
    out
}

fn all_in_proc(fi: &FileIndex) -> bool {
    (0..fi.producers.len()).all(|i| fi.producer_brokers.contains(&(i, Broker::InProc)))
        && (0..fi.consumers.len()).all(|i| fi.consumer_brokers.contains(&(i, Broker::InProc)))
}

// ---- NestJS EventEmitter2 ---------------------------------------------------------------

#[test]
fn event_emitter_emit_and_on_event_with_a_typed_receiver() {
    let fi = extract(
        "src/orders.ts",
        "import { Injectable } from '@nestjs/common';\n\
         import { EventEmitter2, OnEvent } from '@nestjs/event-emitter';\n\
         import { OrderShipped } from './events';\n\
         const ORDER_PAID = 'order.paid';\n\
         @Injectable()\n\
         export class Orders {\n\
           constructor(private readonly emitter: EventEmitter2) {}\n\
           create() {\n\
             this.emitter.emit('order.created', {});\n\
             this.emitter.emitAsync(ORDER_PAID, {});\n\
             this.emitter.emit(OrderShipped.name, {});\n\
             this.emitter.emit(dynamicName(), {});\n\
           }\n\
           @OnEvent('order.created')\n\
           onCreated() {}\n\
           @OnEvent(['order.a', 'order.b'], { async: true })\n\
           onMany() {}\n\
         }\n",
    );
    assert_eq!(
        sites(&fi),
        vec![
            s('C', "order.a"),
            s('C', "order.b"),
            s('C', "order.created"),
            s('P', "OrderShipped"),
            s('P', "order.created"),
            s('P', "order.paid"),
        ]
    );
    assert!(all_in_proc(&fi));
}

#[test]
fn an_untyped_or_foreign_emit_is_not_the_application_bus() {
    // A socket, a Node EventEmitter and a receiver of an unrelated type: no bus.
    let fi = extract(
        "src/sockets.ts",
        "import { OnEvent } from '@nestjs/event-emitter';\n\
         import { EventEmitter } from 'events';\n\
         const local = new EventEmitter();\n\
         export class Gateway {\n\
           constructor(private readonly socket: Socket, private readonly logger: Logger) {}\n\
           go() {\n\
             this.socket.emit('chat', {});\n\
             local.emit('tick');\n\
             this.logger.emit('x');\n\
           }\n\
         }\n",
    );
    assert_eq!(sites(&fi), Vec::<(char, String)>::new());
    // No import of the library: nothing, even with the right method names.
    let fi = extract(
        "src/plain.ts",
        "export class A { constructor(private readonly emitter: EventEmitter2) {}\n\
         go() { this.emitter.emit('a.b'); } }\n",
    );
    assert_eq!(sites(&fi), Vec::<(char, String)>::new());
}

#[test]
fn eventemitter2_package_and_imperative_listeners() {
    let fi = extract(
        "src/bus.ts",
        "import EventEmitter2 from 'eventemitter2';\n\
         const bus = new EventEmitter2();\n\
         bus.on('job.done', () => {});\n\
         bus.once('job.failed', () => {});\n\
         export const finish = () => bus.emit('job.done');\n",
    );
    assert_eq!(
        sites(&fi),
        vec![s('C', "job.done"), s('C', "job.failed"), s('P', "job.done")]
    );
    assert!(all_in_proc(&fi));
}

// ---- NestJS CQRS ------------------------------------------------------------------------

#[test]
fn cqrs_handlers_and_buses_are_keyed_by_class_name() {
    let handlers = extract(
        "src/handlers.ts",
        "import { CommandHandler, EventsHandler, QueryHandler } from '@nestjs/cqrs';\n\
         @CommandHandler(PayCommand)\n\
         export class PayHandler { execute(c: PayCommand) {} }\n\
         @EventsHandler(PaidEvent, RefundedEvent)\n\
         export class AuditHandler { handle(e: PaidEvent) {} }\n\
         @QueryHandler(GetOrderQuery)\n\
         export class GetOrderHandler { execute(q: GetOrderQuery) {} }\n",
    );
    assert_eq!(
        sites(&handlers),
        vec![
            s('C', "GetOrderQuery"),
            s('C', "PaidEvent"),
            s('C', "PayCommand"),
            s('C', "RefundedEvent"),
        ]
    );
    assert!(all_in_proc(&handlers));

    let callers = extract(
        "src/service.ts",
        "import { Injectable } from '@nestjs/common';\n\
         import { CommandBus, EventBus, QueryBus } from '@nestjs/cqrs';\n\
         @Injectable()\n\
         export class Svc {\n\
           constructor(private readonly commands: CommandBus, private readonly events: EventBus, private readonly queries: QueryBus) {}\n\
           go(cmd: PayCommand) {\n\
             this.commands.execute(new PayCommand(1));\n\
             this.queries.execute(new GetOrderQuery(1));\n\
             this.events.publish(new PaidEvent(1));\n\
             this.events.publishAll([new PaidEvent(2), new RefundedEvent(2)]);\n\
             this.commands.execute(cmd);\n\
           }\n\
         }\n",
    );
    // A variable names no class: `execute(cmd)` registers nothing.
    assert_eq!(
        sites(&callers),
        vec![
            s('P', "GetOrderQuery"),
            s('P', "PaidEvent"),
            s('P', "PaidEvent"),
            s('P', "PayCommand"),
            s('P', "RefundedEvent"),
        ]
    );
    assert!(all_in_proc(&callers));
}

#[test]
fn a_saga_consumes_what_it_listens_for_and_produces_the_commands_it_builds() {
    let fi = extract(
        "src/saga.ts",
        "import { Injectable } from '@nestjs/common';\n\
         import { ICommand, ofType, Saga } from '@nestjs/cqrs';\n\
         import { Observable, Subject } from 'rxjs';\n\
         import { map } from 'rxjs/operators';\n\
         import { PaidEvent, ShipCommand } from './messages';\n\
         @Injectable()\n\
         export class Sagas {\n\
           @Saga()\n\
           ship = (events$: Observable<any>): Observable<ICommand> =>\n\
             events$.pipe(\n\
               ofType(PaidEvent),\n\
               map((e) => new ShipCommand(e.id)),\n\
             );\n\
           other() { return new Subject(); }\n\
         }\n",
    );
    assert_eq!(sites(&fi), vec![s('C', "PaidEvent"), s('P', "ShipCommand")]);
    // The sites are on their own lines, not on the field's first line.
    let lines: Vec<usize> = fi
        .producers
        .iter()
        .map(|(i, _)| fi.nodes[*i].line_start)
        .chain(fi.consumers.iter().map(|(i, _)| fi.nodes[*i].line_start))
        .collect();
    assert!(lines.contains(&11) && lines.contains(&12), "{lines:?}");
    assert!(all_in_proc(&fi));
}

// ---- schedules --------------------------------------------------------------------------

#[test]
fn nest_schedules_are_entries_named_class_dot_method() {
    let fi = extract(
        "src/jobs.ts",
        "import { Injectable } from '@nestjs/common';\n\
         import { Cron, Interval, Timeout } from '@nestjs/schedule';\n\
         @Injectable()\n\
         export class Jobs {\n\
           @Cron('0 * * * *')\n\
           hourly() {}\n\
           @Interval(5000)\n\
           poll() {}\n\
           @Timeout(1000)\n\
           warm() {}\n\
         }\n",
    );
    assert_eq!(
        schedules(&fi),
        vec![
            (
                "Jobs.hourly".to_string(),
                6,
                "@Cron('0 * * * *')".to_string()
            ),
            ("Jobs.poll".to_string(), 8, "@Interval(5000)".to_string()),
            ("Jobs.warm".to_string(), 10, "@Timeout(1000)".to_string()),
        ]
    );
    // A `@Cron` from somewhere else is not a schedule.
    let other = extract(
        "src/other.ts",
        "import { Cron } from './my-cron';\nexport class A {\n  @Cron('x')\n  run() {}\n}\n",
    );
    assert!(schedules(&other).is_empty());
}

// ---- Spring -----------------------------------------------------------------------------

#[test]
fn spring_listeners_publishers_and_schedules() {
    let fi = extract(
        "src/main/java/com/shop/Events.java",
        "package com.shop;\n\
         import org.springframework.context.ApplicationEventPublisher;\n\
         import org.springframework.context.event.EventListener;\n\
         import org.springframework.kafka.annotation.KafkaListener;\n\
         import org.springframework.scheduling.annotation.Scheduled;\n\
         @Component\n\
         public class Events implements ApplicationListener<LegacyEvent> {\n\
           private final ApplicationEventPublisher events;\n\
           @EventListener\n\
           public void onPlaced(OrderPlacedEvent e) {}\n\
           @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)\n\
           public void onPaid(com.shop.PaidEvent e) {}\n\
           @EventListener(classes = {AEvent.class, BEvent.class})\n\
           public void onTwo() {}\n\
           @EventListener\n\
           public void onPayload(PayloadApplicationEvent<Wrapped> e) {}\n\
           @EventListener\n\
           public void everything(Object o) {}\n\
           @Override\n\
           public void onApplicationEvent(LegacyEvent e) {}\n\
           @KafkaListener(topics = \"orders\")\n\
           public void kafka(String m) {}\n\
           public void go(OrderPlacedEvent already) {\n\
             events.publishEvent(new ShippedEvent(1));\n\
             events.publishEvent(already);\n\
           }\n\
           @Scheduled(cron = \"0 0 * * * *\")\n\
           public void hourly() {}\n\
         }\n",
    );
    assert_eq!(
        sites(&fi),
        vec![
            s('C', "AEvent"),
            s('C', "BEvent"),
            s('C', "LegacyEvent"),
            s('C', "OrderPlacedEvent"),
            s('C', "PaidEvent"),
            s('C', "Wrapped"),
            s('C', "orders"),
            s('P', "ShippedEvent"),
        ]
    );
    // The Kafka listener keeps its broker, every application event is in-process.
    let broker_of = |topic: &str| {
        let i = fi.consumers.iter().position(|(_, t)| t == topic).unwrap();
        fi.consumer_brokers.iter().find(|(c, _)| *c == i).unwrap().1
    };
    assert_eq!(broker_of("orders"), Broker::Kafka);
    assert_eq!(broker_of("OrderPlacedEvent"), Broker::InProc);
    let published = fi
        .producers
        .iter()
        .position(|(_, t)| t == "ShippedEvent")
        .unwrap();
    assert!(fi.producer_brokers.contains(&(published, Broker::InProc)));
    assert_eq!(
        schedules(&fi),
        vec![(
            "Events.hourly".to_string(),
            27,
            "@Scheduled(cron = \"0 0 * * * *\")".to_string()
        )]
    );
    // The publish site is its own node, on the line of the call.
    assert_eq!(fi.nodes[fi.producers[0].0].line_start, 24);
}

#[test]
fn publish_event_needs_the_publisher_type_in_the_file() {
    let fi = extract(
        "src/main/java/com/shop/Other.java",
        "package com.shop;\npublic class Other {\n  void go(Foo f) { f.publishEvent(new XEvent()); }\n}\n",
    );
    assert!(sites(&fi).is_empty());
}

// ---- namespaces, caps -------------------------------------------------------------------

#[test]
fn an_in_process_event_never_meets_a_broker_topic_of_the_same_name() {
    let mut graph = ContractGraph::new();
    for (file, code) in [
        (
            "src/bus.ts",
            "import { EventEmitter2 } from '@nestjs/event-emitter';\n\
             export class A { constructor(private readonly e: EventEmitter2) {}\n\
             go() { this.e.emit('orders'); } }\n",
        ),
        (
            "src/worker.ts",
            "import { Kafka } from 'kafkajs';\n\
             const consumer = new Kafka({ brokers: [] }).consumer({ groupId: 'g' });\n\
             export async function run() { await consumer.subscribe({ topic: 'orders' }); }\n",
        ),
    ] {
        PolyglotIndexer::index_file(Path::new(file), code, 0, &mut graph);
    }
    graph.reconcile_edges();
    let hubs: Vec<String> = {
        let mut v: Vec<String> = graph
            .all_nodes()
            .filter(|n| n.package == "event-bus")
            .map(|n| n.name.to_string())
            .collect();
        v.sort();
        v
    };
    assert_eq!(hubs, vec!["inproc:orders", "kafka:orders"]);
}

#[test]
fn a_file_past_the_site_cap_is_cut_and_flagged_never_silently() {
    let mut ts =
        String::from("import { OnEvent } from '@nestjs/event-emitter';\nexport class L {\n");
    for i in 0..MAX_INPROC_SITES_PER_FILE + 50 {
        ts.push_str(&format!("  @OnEvent('e.{i}')\n  h{i}() {{}}\n"));
    }
    ts.push_str("}\n");
    let fi = extract("src/listeners.ts", &ts);
    assert!(fi.inproc_capped);
    assert_eq!(fi.consumers.len(), MAX_INPROC_SITES_PER_FILE);
    // The first sites in source order are the ones kept.
    assert!(sites(&fi).contains(&s('C', "e.0")));

    let mut java = String::from(
        "package p;\nimport org.springframework.context.event.EventListener;\npublic class L {\n",
    );
    for i in 0..MAX_INPROC_SITES_PER_FILE + 50 {
        java.push_str(&format!("  @EventListener\n  void h{i}(E{i} e) {{}}\n"));
    }
    java.push_str("}\n");
    let fi = extract("src/main/java/p/L.java", &java);
    assert!(fi.inproc_capped);
    assert_eq!(fi.consumers.len(), MAX_INPROC_SITES_PER_FILE);

    let small = extract(
        "src/ok.ts",
        "import { OnEvent } from '@nestjs/event-emitter';\nexport class L {\n @OnEvent('a')\n h() {}\n}\n",
    );
    assert!(!small.inproc_capped);
}

// ---- chains -----------------------------------------------------------------------------

fn graph_of(files: &[(&str, &str)]) -> ContractGraph {
    let mut graph = ContractGraph::new();
    for (file, code) in files {
        PolyglotIndexer::index_file(Path::new(file), code, 0, &mut graph);
    }
    graph.reconcile_edges();
    graph
}

/// Two listeners that re-emit each other's event: a cycle through the bus.
const PING_PONG: [(&str, &str); 2] = [
    (
        "src/ping.ts",
        "import { EventEmitter2, OnEvent } from '@nestjs/event-emitter';\n\
         export class Ping {\n\
           constructor(private readonly bus: EventEmitter2) {}\n\
           @OnEvent('ping')\n\
           onPing() { this.bus.emit('pong'); }\n\
         }\n",
    ),
    (
        "src/pong.ts",
        "import { EventEmitter2, OnEvent } from '@nestjs/event-emitter';\n\
         export class Pong {\n\
           constructor(private readonly bus: EventEmitter2) {}\n\
           @OnEvent('pong')\n\
           onPong() { this.bus.emit('ping'); }\n\
         }\n",
    ),
];

#[test]
fn a_cyclic_emit_stops_at_the_first_repeated_event() {
    let graph = graph_of(&PING_PONG);
    let trace = graph.trace_flow(
        FlowEntry::Topic("ping"),
        FlowDirection::Forward,
        FlowBounds::clamped(None, Some(5), None),
        &|_| false,
    );
    // ping -> pong -> (ping already traced): two hops, however deep the bound is.
    assert_eq!(trace.hops.len(), 2, "{:?}", trace.hops.len());
    assert!(!trace.capped);
}

#[test]
fn a_reverse_trace_ends_at_the_schedule_that_starts_the_flow() {
    let graph = graph_of(&[
        (
            "src/job.ts",
            "import { Cron } from '@nestjs/schedule';\n\
             import { Report } from './report';\n\
             export class Job {\n\
               constructor(private readonly report: Report) {}\n\
               @Cron('0 * * * *')\n\
               hourly() { this.report.run(); }\n\
             }\n",
        ),
        (
            "src/report.ts",
            "import { EventEmitter2 } from '@nestjs/event-emitter';\n\
             export class Report {\n\
               constructor(private readonly bus: EventEmitter2) {}\n\
               run() { this.bus.emit('report.done'); }\n\
             }\n",
        ),
    ]);
    let reverse = graph.trace_flow(
        FlowEntry::Topic("report.done"),
        FlowDirection::Reverse,
        FlowBounds::default(),
        &|_| false,
    );
    let kinds: Vec<FlowStepKind> = reverse
        .hops
        .iter()
        .flat_map(|h| h.steps.iter().map(|st| st.kind))
        .collect();
    assert!(kinds.contains(&FlowStepKind::Cron), "{kinds:?}");
    // Forward from the schedule by its name: the emit is reached through the call.
    let forward = graph.trace_flow(
        FlowEntry::Cron("Job.hourly"),
        FlowDirection::Forward,
        FlowBounds::default(),
        &|_| false,
    );
    assert_eq!(forward.entry_kind, "cron");
    let produced: Vec<String> = forward
        .hops
        .iter()
        .flat_map(|h| h.steps.iter())
        .filter(|st| st.kind == FlowStepKind::Produce)
        .filter_map(|st| st.topic.as_ref().map(ToString::to_string))
        .collect();
    assert_eq!(produced, vec!["report.done".to_string()]);
}
