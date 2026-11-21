//! Spring in-process events and schedules (9.0 ticket T4).
//!
//! The "topic" of a Spring application event is the event class, written as in the source
//! (`OrderPlacedEvent`; a qualified name keeps its last segment):
//! - a method annotated `@EventListener` / `@TransactionalEventListener` consumes the class of
//!   its first parameter, or the classes of `classes = ...` / `value = ...` when it names some
//!   (`PayloadApplicationEvent<T>` reads as `T`; `Object` and `ApplicationEvent`, which hear
//!   everything, name no class); `onApplicationEvent(E)` of an `ApplicationListener<E>` is the
//!   same consumer;
//! - `publisher.publishEvent(new E(...))` produces `E`, in a file that names
//!   `ApplicationEventPublisher` or `ApplicationContext`. The argument must be a `new`
//!   expression: a variable names no class this scan can read;
//! - `@Scheduled(...)` makes its method a [`NodeKind::Schedule`] entry named `Class.method`.
//!
//! A consumer is the annotated method's own node; a producer is a node at the call site
//! (like the TypeScript call-site producers), so a chain shows the line of the publish call.
//! All of them are recorded under [`Broker::InProc`], not under the Kafka the Java extractor
//! otherwise assumes. Sites past [`MAX_INPROC_SITES_PER_FILE`] are dropped and the file is
//! counted in `IndexHealth::files_inproc_capped`.

use super::{FileIndex, MAX_INPROC_SITES_PER_FILE};
use mesh_core::{Broker, CompactStr, ContractNode, NodeKind};
use std::collections::HashMap;
use tree_sitter::{Node, Tree};

const PUBLISH_SIGNATURE: &str = "spring-event publishEvent";

/// Event types that hear every event: they name no class a producer could match.
const CATCH_ALL: &[&str] = &["Object", "ApplicationEvent", "PayloadApplicationEvent"];

fn text<'s>(node: Node, source: &'s [u8]) -> &'s str {
    node.utf8_text(source).unwrap_or("")
}

/// The simple class name a type node stands for.
fn simple_type(node: Node, source: &[u8]) -> Option<String> {
    match node.kind() {
        "type_identifier" => Some(text(node, source).to_string()),
        "scoped_type_identifier" => node
            .named_child(node.named_child_count().checked_sub(1)?)
            .and_then(|last| simple_type(last, source)),
        "generic_type" => {
            let base = node.named_child(0).and_then(|b| simple_type(b, source))?;
            if base == "PayloadApplicationEvent" {
                // `PayloadApplicationEvent<OrderPlaced>` is an `OrderPlaced` payload.
                let args = node.named_child(1)?;
                return args.named_child(0).and_then(|a| simple_type(a, source));
            }
            Some(base)
        }
        _ => None,
    }
}

fn event_class(node: Node, source: &[u8]) -> Option<String> {
    simple_type(node, source).filter(|n| !CATCH_ALL.contains(&n.as_str()))
}

/// Last segment of an annotation name (`org.springframework...EventListener`).
fn annotation_name<'s>(annotation: Node, source: &'s [u8]) -> &'s str {
    let name = annotation
        .child_by_field_name("name")
        .map_or("", |n| text(n, source));
    name.rsplit('.').next().unwrap_or(name)
}

/// The classes an event-listener annotation names (`classes = {A.class, B.class}`, `A.class`).
fn annotation_classes(annotation: Node, source: &[u8]) -> Vec<String> {
    let Some(args) = annotation.child_by_field_name("arguments") else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let mut stack = vec![args];
    while let Some(n) = stack.pop() {
        if n.kind() == "class_literal" {
            out.extend(n.named_child(0).and_then(|t| event_class(t, source)));
            continue;
        }
        stack.extend(n.named_children(&mut n.walk()));
    }
    out.sort();
    out.dedup();
    out
}

/// The class of the first parameter of a method.
fn first_parameter_class(method: Node, source: &[u8]) -> Option<String> {
    let params = method.child_by_field_name("parameters")?;
    let first = params
        .named_children(&mut params.walk())
        .find(|p| p.kind() == "formal_parameter")?;
    event_class(first.child_by_field_name("type")?, source)
}

/// The name of the class a node sits in.
fn enclosing_class<'s>(node: Node, source: &'s [u8]) -> Option<&'s str> {
    let mut up = node.parent();
    while let Some(p) = up {
        if matches!(
            p.kind(),
            "class_declaration" | "record_declaration" | "enum_declaration"
        ) {
            return p.child_by_field_name("name").map(|n| text(n, source));
        }
        up = p.parent();
    }
    None
}

/// The annotations of a method, as `(simple name, node)`.
fn annotations<'t>(method: Node<'t>, source: &[u8]) -> Vec<(String, Node<'t>)> {
    let mut out = Vec::new();
    for child in method.children(&mut method.walk()) {
        if child.kind() != "modifiers" {
            continue;
        }
        for a in child.children(&mut child.walk()) {
            if matches!(a.kind(), "marker_annotation" | "annotation") {
                out.push((annotation_name(a, source).to_string(), a));
            }
        }
    }
    out
}

/// The in-process facts of a Java file, read off its tree.
#[derive(Default)]
pub(crate) struct Scan {
    sites: Vec<Site>,
    capped: bool,
}

/// Reads the in-process facts of a Java file. Cheap for a file that names none of the idioms.
pub(crate) fn scan(tree: &Tree, content: &str) -> Scan {
    let publishes = content.contains("publishEvent")
        && (content.contains("ApplicationEventPublisher")
            || content.contains("ApplicationContext"));
    if !(publishes
        || content.contains("EventListener")
        || content.contains("ApplicationListener")
        || content.contains("Scheduled"))
    {
        return Scan::default();
    }
    let source = content.as_bytes();
    let mut found: Vec<Site> = Vec::new();
    let mut cursor = tree.walk();
    'walk: loop {
        let node = cursor.node();
        match node.kind() {
            "method_declaration" => method_sites(node, source, &mut found),
            "method_invocation" if publishes => {
                if let Some(site) = publish_site(node, source) {
                    found.push(site);
                }
            }
            _ => {}
        }
        if found.len() > MAX_INPROC_SITES_PER_FILE {
            break;
        }
        if cursor.goto_first_child() || cursor.goto_next_sibling() {
            continue;
        }
        loop {
            if !cursor.goto_parent() {
                break 'walk;
            }
            if cursor.goto_next_sibling() {
                break;
            }
        }
    }
    let capped = found.len() > MAX_INPROC_SITES_PER_FILE;
    found.truncate(MAX_INPROC_SITES_PER_FILE);
    Scan {
        sites: found,
        capped,
    }
}

/// Registers a [`Scan`] in `out`, whose `nodes` the Java extractor has already filled.
pub(crate) fn apply(scan: Scan, out: &mut FileIndex) {
    out.inproc_capped |= scan.capped;
    let Some(template) = out.nodes.first().cloned() else {
        return;
    };
    // The method nodes the Java extractor made, by (first line, name).
    let methods: HashMap<(usize, String), usize> = out
        .nodes
        .iter()
        .enumerate()
        .map(|(i, n)| ((n.line_start, n.name.to_string()), i))
        .collect();
    for site in scan.sites {
        site.register(&template, &methods, out);
    }
}

/// One in-process fact before it becomes a node.
enum Site {
    /// A listener method (its first line, its name) of an event class.
    Listener {
        method: (usize, String),
        class: String,
    },
    /// A `publishEvent(new E(...))` call (its lines).
    Publish {
        class: String,
        lines: (usize, usize),
    },
    /// A scheduled method: `Class.method`, its lines, the annotation text.
    Schedule {
        name: String,
        lines: (usize, usize),
        annotation: String,
    },
}

fn lines(node: Node) -> (usize, usize) {
    (node.start_position().row + 1, node.end_position().row + 1)
}

fn method_sites(method: Node, source: &[u8], found: &mut Vec<Site>) {
    let Some(name) = method.child_by_field_name("name").map(|n| text(n, source)) else {
        return;
    };
    let first_line = method.start_position().row + 1;
    let mut classes: Vec<String> = Vec::new();
    let mut schedule: Option<Node> = None;
    let mut listens = false;
    for (annotation, node) in annotations(method, source) {
        match annotation.as_str() {
            "EventListener" | "TransactionalEventListener" => {
                listens = true;
                classes.extend(annotation_classes(node, source));
            }
            "Scheduled" => schedule = Some(node),
            _ => {}
        }
    }
    if listens && classes.is_empty() {
        classes.extend(first_parameter_class(method, source));
    }
    if name == "onApplicationEvent" && !listens {
        // `implements ApplicationListener<E>`: the method hears `E`.
        let implements_listener = method
            .parent()
            .and_then(|b| b.parent())
            .and_then(|c| c.child_by_field_name("interfaces"))
            .is_some_and(|i| text(i, source).contains("ApplicationListener"));
        if implements_listener {
            classes.extend(first_parameter_class(method, source));
        }
    }
    classes.sort();
    classes.dedup();
    for class in classes {
        found.push(Site::Listener {
            method: (first_line, name.to_string()),
            class,
        });
    }
    if let Some(annotation) = schedule {
        if let Some(class) = enclosing_class(method, source) {
            let mut at = text(annotation, source).replace(['\n', '\r'], " ");
            let mut end = at.len().min(96);
            while !at.is_char_boundary(end) {
                end -= 1;
            }
            at.truncate(end);
            found.push(Site::Schedule {
                name: format!("{class}.{name}"),
                lines: lines(method),
                annotation: at,
            });
        }
    }
}

/// `x.publishEvent(new E(...))`.
fn publish_site(call: Node, source: &[u8]) -> Option<Site> {
    if text(call.child_by_field_name("name")?, source) != "publishEvent" {
        return None;
    }
    let args = call.child_by_field_name("arguments")?;
    let first = args.named_child(0)?;
    if first.kind() != "object_creation_expression" {
        return None;
    }
    let class = event_class(first.child_by_field_name("type")?, source)?;
    Some(Site::Publish {
        class,
        lines: lines(call),
    })
}

impl Site {
    fn register(
        self,
        template: &ContractNode,
        methods: &HashMap<(usize, String), usize>,
        out: &mut FileIndex,
    ) {
        let node =
            |name: String, kind: NodeKind, lines: (usize, usize), signature: &str| ContractNode {
                id: 0,
                name: CompactStr::new(name),
                kind,
                file_path: template.file_path.clone(),
                line_start: lines.0,
                line_end: lines.1,
                package: template.package.clone(),
                repo_id: template.repo_id,
                signature: Some(CompactStr::new(signature)),
                docstring: None,
            };
        match self {
            Site::Listener { method, class } => {
                let Some(&owner) = methods.get(&method) else {
                    return;
                };
                out.consumer_brokers
                    .push((out.consumers.len(), Broker::InProc));
                out.consumers.push((owner, CompactStr::new(class)));
            }
            Site::Publish { class, lines } => {
                out.nodes.push(node(
                    class.clone(),
                    NodeKind::EventStream,
                    lines,
                    PUBLISH_SIGNATURE,
                ));
                let index = out.nodes.len() - 1;
                out.node_brokers.push((index, Broker::InProc));
                out.producer_brokers
                    .push((out.producers.len(), Broker::InProc));
                out.producers.push((index, CompactStr::new(class)));
            }
            Site::Schedule {
                name,
                lines,
                annotation,
            } => out
                .nodes
                .push(node(name, NodeKind::Schedule, lines, &annotation)),
        }
    }
}
