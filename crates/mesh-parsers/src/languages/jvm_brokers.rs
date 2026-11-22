//! Message brokers other than Kafka in Java (9.0 ticket T8a).
//!
//! The Java extractor reads Spring Kafka. This pass adds the other brokers a JVM service
//! talks to, each recorded under its own [`Broker`] so a queue named like a Kafka topic is a
//! different channel (T7):
//!
//! | broker | consumers | producers |
//! |---|---|---|
//! | `rabbitmq` | `@RabbitListener(queues / queuesToDeclare / bindings)`, on a method or a class (`@RabbitHandler`) | `RabbitTemplate` / `AmqpTemplate` `convertAndSend`, `send`, `convertSendAndReceive` |
//! | `jms` | `@JmsListener(destination)`, `JmsTemplate.receive` | `JmsTemplate` / `JmsMessagingTemplate` `convertAndSend`, `send` |
//! | `sqs` | `@SqsListener`, `SqsClient.receiveMessage` | `SqsTemplate`, `QueueMessagingTemplate`, `SqsClient.sendMessage` |
//! | `sns` | none | `SnsTemplate`, `NotificationMessagingTemplate`, `SnsClient.publish` |
//! | `redis` | `RedisMessageListenerContainer.addMessageListener(l, ChannelTopic / PatternTopic)` | `RedisTemplate.convertAndSend` |
//! | `nats` | `Connection` / `Dispatcher` / `JetStream` `.subscribe` | `.publish`, `.request` |
//! | `pubsub` | none | `PubSubTemplate.publish` |
//!
//! Only what a call site shows is registered: a name is a string literal, a `static final`
//! string constant or a `new ChannelTopic("x")` of the same file, or an `@Value("${key}")`
//! placeholder (resolved against the configuration when the index is folded, like
//! `@KafkaListener`). A variable, a method result or a constant of another file registers
//! nothing: no name is ever guessed from an identifier.
//!
//! A call is read only when its receiver is a field, parameter or local whose declared type
//! is the client class (`rabbitTemplate.convertAndSend(...)` with `RabbitTemplate
//! rabbitTemplate`) in a file that imports that library, so a `convertAndSend` of an
//! unrelated class is never a producer. A name declared twice with different types in the
//! file is ambiguous and read as nothing. A listener registers on its method (the handler), a
//! class-level `@RabbitListener` on its class; a call-site consumer or producer registers a node on the call, like the T4 publishers.
//!
//! Bounds: the walk is iterative; argument and initializer searches visit at most
//! [`MAX_SEARCH_NODES`] nodes; sites past [`MAX_JVM_BROKER_SITES_PER_FILE`] are dropped and
//! the file is counted in `IndexHealth::files_jvm_brokers_capped`.

use super::java::JavaExtractor;
use super::{FileIndex, MAX_JVM_BROKER_SITES_PER_FILE};
use mesh_core::{Broker, CompactStr, ContractNode, NodeKind};
use std::collections::{HashMap, HashSet};
use tree_sitter::{Node, Tree};

/// Nodes one argument / initializer search visits at most.
const MAX_SEARCH_NODES: usize = 256;
/// Declared names the type table remembers; past it the file is counted as capped.
const MAX_DECLARED_NAMES: usize = 16_384;
/// Links of a `x.getTopic()` chain one name resolution follows.
const MAX_RESOLVE_DEPTH: usize = 4;

/// Client classes whose calls this pass reads, by what they talk to.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Fam {
    Rabbit,
    Jms,
    SqsTemplate,
    SnsTemplate,
    SqsClient,
    SnsClient,
    RedisTemplate,
    RedisContainer,
    Nats,
    PubSub,
}

impl Fam {
    fn of_type(name: &str) -> Option<Fam> {
        Some(match name {
            "RabbitTemplate"
            | "AmqpTemplate"
            | "RabbitMessagingTemplate"
            | "RabbitOperations"
            | "AsyncRabbitTemplate" => Fam::Rabbit,
            "JmsTemplate" | "JmsMessagingTemplate" | "JmsOperations" => Fam::Jms,
            "SqsTemplate" | "SqsOperations" | "QueueMessagingTemplate" => Fam::SqsTemplate,
            "SnsTemplate" | "SnsOperations" | "NotificationMessagingTemplate" => Fam::SnsTemplate,
            "SqsClient" | "SqsAsyncClient" | "AmazonSQS" | "AmazonSQSAsync" => Fam::SqsClient,
            "SnsClient" | "SnsAsyncClient" | "AmazonSNS" | "AmazonSNSAsync" => Fam::SnsClient,
            "RedisTemplate"
            | "StringRedisTemplate"
            | "ReactiveRedisTemplate"
            | "ReactiveStringRedisTemplate"
            | "RedisOperations" => Fam::RedisTemplate,
            "RedisMessageListenerContainer" | "ReactiveRedisMessageListenerContainer" => {
                Fam::RedisContainer
            }
            "Connection" | "Dispatcher" | "JetStream" => Fam::Nats,
            "PubSubTemplate" | "PubSubOperations" | "PubSubPublisherTemplate" => Fam::PubSub,
            _ => return None,
        })
    }

    fn broker(self) -> Broker {
        match self {
            Fam::Rabbit => Broker::RabbitMq,
            Fam::Jms => Broker::Jms,
            Fam::SqsTemplate | Fam::SqsClient => Broker::Sqs,
            Fam::SnsTemplate | Fam::SnsClient => Broker::Sns,
            Fam::RedisTemplate | Fam::RedisContainer => Broker::Redis,
            Fam::Nats => Broker::Nats,
            Fam::PubSub => Broker::PubSub,
        }
    }
}

/// A library a file must import for its annotations and clients to count, as substrings of
/// the file (an import, or a fully qualified name).
fn gate(fam: Fam) -> &'static [&'static str] {
    match fam {
        Fam::Rabbit => &["springframework.amqp"],
        Fam::Jms => &["springframework.jms", "jakarta.jms", "javax.jms"],
        Fam::SqsTemplate | Fam::SnsTemplate | Fam::SqsClient | Fam::SnsClient => &[
            "awspring.cloud",
            "springframework.cloud.aws",
            "software.amazon.awssdk",
            "com.amazonaws",
        ],
        Fam::RedisTemplate | Fam::RedisContainer => &["springframework.data.redis"],
        Fam::Nats => &["io.nats.client"],
        Fam::PubSub => &[
            "com.google.cloud.spring.pubsub",
            "springframework.cloud.gcp.pubsub",
        ],
    }
}

fn text<'s>(node: Node, source: &'s [u8]) -> &'s str {
    node.utf8_text(source).unwrap_or("")
}

fn lines(node: Node) -> (usize, usize) {
    (node.start_position().row + 1, node.end_position().row + 1)
}

/// The simple class name a type node stands for (`a.b.C<T>` reads as `C`). Iterative.
fn simple_type(node: Node, source: &[u8]) -> Option<String> {
    let mut node = node;
    loop {
        match node.kind() {
            "type_identifier" => return Some(text(node, source).to_string()),
            "scoped_type_identifier" => {
                node = node.named_child(node.named_child_count().checked_sub(1)?)?;
            }
            "generic_type" => node = node.named_child(0)?,
            _ => return None,
        }
    }
}

/// The content of a plain string literal (no escape, no text block).
fn string_literal(node: Node, source: &[u8]) -> Option<String> {
    if node.kind() != "string_literal" {
        return None;
    }
    let t = text(node, source);
    if t.starts_with("\"\"\"") || t.contains('\\') {
        return None;
    }
    let inner = t.strip_prefix('"')?.strip_suffix('"')?;
    Some(inner.to_string())
}

/// A string usable as a channel name: non-empty, no whitespace, not a SpEL expression.
fn usable_name(s: &str) -> bool {
    !s.is_empty() && !s.contains("#{") && !s.chars().any(char::is_whitespace)
}

/// What a name of the file resolves to.
enum Entry {
    /// A type: `Some(family)` of a broker client, `None` of anything else.
    Typed(Option<Fam>),
    /// A string constant or destination object of the file.
    Value(String),
    /// Declared twice with differing meanings: read as nothing.
    Conflict,
}

/// Per-file tables the call sites are resolved against.
struct Ctx<'a> {
    source: &'a [u8],
    /// Variable name to type family; constant name to value. Separate key spaces.
    vars: HashMap<String, Entry>,
    consts: HashMap<String, Entry>,
    /// Names of the types the file declares.
    classes: HashSet<String>,
    /// `@Value("${key}")` fields (`JavaExtractor::value_placeholder_fields`).
    placeholders: HashMap<String, String>,
    /// Parameters and locals of a method, by node id.
    locals: HashMap<usize, HashSet<String>>,
    /// Bit per [`Fam`] whose library the file imports.
    gates: u16,
    capped: bool,
}

impl<'a> Ctx<'a> {
    fn gated(&self, fam: Fam) -> bool {
        self.gates & (1 << fam as u16) != 0
    }

    fn declare_var(&mut self, name: &str, fam: Option<Fam>) {
        match self.vars.get(name) {
            Some(Entry::Typed(prev)) if *prev == fam => {}
            Some(_) => {
                self.vars.insert(name.to_string(), Entry::Conflict);
            }
            None => {
                if self.vars.len() >= MAX_DECLARED_NAMES {
                    self.capped = true;
                    return;
                }
                self.vars.insert(name.to_string(), Entry::Typed(fam));
            }
        }
    }

    fn declare_const(&mut self, name: &str, value: String) {
        match self.consts.get(name) {
            Some(Entry::Value(prev)) if *prev == value => {}
            Some(_) => {
                self.consts.insert(name.to_string(), Entry::Conflict);
            }
            None => {
                if self.consts.len() >= MAX_DECLARED_NAMES {
                    self.capped = true;
                    return;
                }
                self.consts.insert(name.to_string(), Entry::Value(value));
            }
        }
    }

    fn family_of(&self, name: &str) -> Option<Fam> {
        match self.vars.get(name) {
            Some(Entry::Typed(Some(f))) if self.gated(*f) => Some(*f),
            _ => None,
        }
    }

    /// Whether `name` is a parameter or local of the method around `at` (it then hides a
    /// field of that name).
    fn is_local(&mut self, at: Node, name: &str) -> bool {
        let mut up = at.parent();
        while let Some(p) = up {
            if matches!(p.kind(), "method_declaration" | "constructor_declaration") {
                let source = self.source;
                return self
                    .locals
                    .entry(p.id())
                    .or_insert_with(|| JavaExtractor::local_names(p, source))
                    .contains(name);
            }
            up = p.parent();
        }
        false
    }

    /// The channel name a node stands for, if the file shows it.
    fn resolve(&mut self, node: Node) -> Option<String> {
        self.resolve_at(node, 0)
    }

    /// [`Self::resolve`] at call depth `depth`: `a.getTopic().getTopic()...` is followed at most
    /// [`MAX_RESOLVE_DEPTH`] links, whatever the file nests.
    fn resolve_at(&mut self, node: Node, depth: usize) -> Option<String> {
        if depth > MAX_RESOLVE_DEPTH {
            return None;
        }
        let source = self.source;
        let name = match node.kind() {
            "string_literal" => string_literal(node, source)?,
            "identifier" => {
                let id = text(node, source);
                if self.is_local(node, id) {
                    return None;
                }
                self.named_value(id)?
            }
            // `this.queue` or `Queues.ORDERS` of a class this file declares: its constant.
            "field_access" => {
                let owner = node.child_by_field_name("object")?;
                let declared_here = owner.kind() == "this"
                    || (owner.kind() == "identifier" && self.classes.contains(text(owner, source)));
                if !declared_here {
                    return None;
                }
                let id = text(node.child_by_field_name("field")?, source);
                self.named_value(id)?
            }
            "object_creation_expression" => {
                let ty = simple_type(node.child_by_field_name("type")?, source)?;
                if !is_destination_type(&ty) {
                    return None;
                }
                let args = node.child_by_field_name("arguments")?;
                string_literal(args.named_child(0)?, source)?
            }
            "method_invocation" => {
                let method = text(node.child_by_field_name("name")?, source);
                let args = node.child_by_field_name("arguments")?;
                let object = node.child_by_field_name("object");
                match (object, method) {
                    // `ChannelTopic.of("x")`
                    (Some(o), "of")
                        if o.kind() == "identifier" && is_destination_type(text(o, source)) =>
                    {
                        string_literal(args.named_child(0)?, source)?
                    }
                    // `topic.getTopic()`, `queue.getName()`
                    (Some(o), "getTopic" | "getName" | "getQueueName" | "getTopicName")
                        if args.named_child_count() == 0 =>
                    {
                        self.resolve_at(o, depth + 1)?
                    }
                    // `topic()`: a zero-argument bean method returning a destination.
                    _ if args.named_child_count() == 0
                        && object.is_none_or(|o| o.kind() == "this") =>
                    {
                        match self.consts.get(method) {
                            Some(Entry::Value(v)) => v.clone(),
                            _ => return None,
                        }
                    }
                    _ => return None,
                }
            }
            _ => return None,
        };
        usable_name(&name).then_some(name)
    }

    /// A string constant, or an `@Value` placeholder, named `id`.
    fn named_value(&self, id: &str) -> Option<String> {
        match self.consts.get(id) {
            Some(Entry::Value(v)) => return Some(v.clone()),
            Some(_) => return None,
            None => {}
        }
        self.placeholders.get(id).cloned()
    }
}

fn is_destination_type(name: &str) -> bool {
    matches!(
        name,
        "ChannelTopic" | "PatternTopic" | "ActiveMQQueue" | "ActiveMQTopic"
    )
}

/// Pre-order walk of `root` without recursion; `visit` returns `false` to stop.
fn walk(root: Node, mut visit: impl FnMut(Node) -> bool) {
    let mut cursor = root.walk();
    'walk: loop {
        if !visit(cursor.node()) {
            return;
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
}

/// The nodes of `root`'s subtree, at most `limit`, pre-order.
fn subtree(root: Node, limit: usize) -> Vec<Node> {
    let mut out = Vec::new();
    let mut stack = vec![root];
    while let Some(n) = stack.pop() {
        if out.len() >= limit {
            break;
        }
        out.push(n);
        let mut cursor = n.walk();
        let children: Vec<Node> = n.named_children(&mut cursor).collect();
        stack.extend(children.into_iter().rev());
    }
    out
}

/// The type of a declaration (`var` reads the type of its `new` initializer).
fn declared_type(decl: Node, declarator: Option<Node>, source: &[u8]) -> Option<String> {
    let ty = decl.child_by_field_name("type")?;
    let name = simple_type(ty, source)?;
    if name != "var" {
        return Some(name);
    }
    let init = declarator?.child_by_field_name("value")?;
    if init.kind() == "object_creation_expression" {
        return simple_type(init.child_by_field_name("type")?, source);
    }
    None
}

fn has_modifier(decl: Node, word: &str, source: &[u8]) -> bool {
    let mut cursor = decl.walk();
    let found = decl
        .children(&mut cursor)
        .find(|m| m.kind() == "modifiers")
        .is_some_and(|m| text(m, source).split_whitespace().any(|w| w == word));
    found
}

/// Fills the variable-type and constant tables of the file.
fn declare(root: Node, ctx: &mut Ctx) {
    let source = ctx.source;
    walk(root, |node| {
        match node.kind() {
            "field_declaration" | "local_variable_declaration" => {
                let is_final = has_modifier(node, "final", source);
                let mut cursor = node.walk();
                let declarators: Vec<Node> = node
                    .children_by_field_name("declarator", &mut cursor)
                    .collect();
                for d in declarators {
                    let Some(name) = d.child_by_field_name("name").map(|n| text(n, source)) else {
                        continue;
                    };
                    let ty = declared_type(node, Some(d), source);
                    ctx.declare_var(name, ty.as_deref().and_then(Fam::of_type));
                    let Some(value) = d.child_by_field_name("value") else {
                        continue;
                    };
                    if let Some(lit) = string_literal(value, source) {
                        if is_final && node.kind() == "field_declaration" {
                            ctx.declare_const(name, lit);
                        }
                    } else if value.kind() == "object_creation_expression"
                        && ty.as_deref().is_some_and(is_destination_type)
                    {
                        if let Some(v) = ctx.resolve(value) {
                            ctx.declare_const(name, v);
                        }
                    }
                }
            }
            "class_declaration"
            | "interface_declaration"
            | "enum_declaration"
            | "record_declaration" => {
                if let Some(name) = node.child_by_field_name("name") {
                    if ctx.classes.len() < MAX_DECLARED_NAMES {
                        ctx.classes.insert(text(name, source).to_string());
                    }
                }
            }
            "formal_parameter" => {
                if let Some(name) = node.child_by_field_name("name").map(|n| text(n, source)) {
                    let ty = declared_type(node, None, source);
                    ctx.declare_var(name, ty.as_deref().and_then(Fam::of_type));
                }
            }
            // `@Bean ChannelTopic topic() { return new ChannelTopic("x"); }`
            "method_declaration" => {
                let params_empty = node
                    .child_by_field_name("parameters")
                    .is_some_and(|p| p.named_child_count() == 0);
                let ty = node
                    .child_by_field_name("type")
                    .and_then(|t| simple_type(t, source));
                if let (true, Some(ty), Some(name), Some(body)) = (
                    params_empty,
                    ty,
                    node.child_by_field_name("name").map(|n| text(n, source)),
                    node.child_by_field_name("body"),
                ) {
                    if is_destination_type(&ty) && body.named_child_count() == 1 {
                        if let Some(ret) = body
                            .named_child(0)
                            .filter(|r| r.kind() == "return_statement")
                        {
                            if let Some(v) = ret.named_child(0).and_then(|e| ctx.resolve(e)) {
                                ctx.declare_const(name, v);
                            }
                        }
                    }
                }
            }
            _ => {}
        }
        true
    });
}

/// One broker fact before it becomes a node or a consumer row.
enum Site {
    /// A listener annotation: the declaration it sits on (first line, name), the key.
    Listener {
        owner: (usize, String),
        broker: Broker,
        key: String,
    },
    /// A call: a producer or a consumer node on its lines.
    Call {
        producer: bool,
        broker: Broker,
        key: String,
        lines: (usize, usize),
        via: &'static str,
    },
}

/// The broker facts of a Java file, read off its tree.
#[derive(Default)]
pub(crate) struct Scan {
    sites: Vec<Site>,
    capped: bool,
}

/// Methods a call is read for at all, so the receiver lookup runs on few calls.
const CALL_NAMES: &[&str] = &[
    "convertAndSend",
    "send",
    "sendAsync",
    "sendAndReceive",
    "convertSendAndReceive",
    "convertSendAndReceiveAsType",
    "sendAndReceiveAsType",
    "sendNotification",
    "sendMessage",
    "sendMessageBatch",
    "receiveMessage",
    "publish",
    "publishBatch",
    "request",
    "subscribe",
    "addMessageListener",
    "receive",
    "receiveAndConvert",
];

/// Reads the broker facts of a Java file. Cheap for a file that names none of the libraries.
pub(crate) fn scan(tree: &Tree, content: &str) -> Scan {
    let mut gates: u16 = 0;
    for fam in [
        Fam::Rabbit,
        Fam::Jms,
        Fam::SqsTemplate,
        Fam::SnsTemplate,
        Fam::SqsClient,
        Fam::SnsClient,
        Fam::RedisTemplate,
        Fam::RedisContainer,
        Fam::Nats,
        Fam::PubSub,
    ] {
        if gate(fam).iter().any(|g| content.contains(g)) {
            gates |= 1 << fam as u16;
        }
    }
    if gates == 0 {
        return Scan::default();
    }
    let source = content.as_bytes();
    let mut ctx = Ctx {
        source,
        vars: HashMap::new(),
        consts: HashMap::new(),
        classes: HashSet::new(),
        placeholders: if content.contains("@Value(") {
            JavaExtractor::value_placeholder_fields(content)
        } else {
            HashMap::new()
        },
        locals: HashMap::new(),
        gates,
        capped: false,
    };
    let root = tree.root_node();
    declare(root, &mut ctx);

    let mut found: Vec<Site> = Vec::new();
    walk(root, |node| {
        match node.kind() {
            "annotation" | "marker_annotation" => listener_sites(node, &mut ctx, &mut found),
            "method_invocation" => call_sites(node, &mut ctx, &mut found),
            _ => {}
        }
        found.len() <= MAX_JVM_BROKER_SITES_PER_FILE
    });
    let capped = found.len() > MAX_JVM_BROKER_SITES_PER_FILE || ctx.capped;
    found.truncate(MAX_JVM_BROKER_SITES_PER_FILE);
    Scan {
        sites: found,
        capped,
    }
}

/// The `(key, value)` pairs of an annotation's arguments; a lone positional value has no key.
fn annotation_pairs<'t>(annotation: Node<'t>, source: &[u8]) -> Vec<(Option<String>, Node<'t>)> {
    let Some(args) = annotation.child_by_field_name("arguments") else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let mut cursor = args.walk();
    for c in args.named_children(&mut cursor) {
        if c.kind() == "element_value_pair" {
            if let (Some(k), Some(v)) =
                (c.child_by_field_name("key"), c.child_by_field_name("value"))
            {
                out.push((Some(text(k, source).to_string()), v));
            }
        } else {
            out.push((None, c));
        }
    }
    out
}

/// A value that is one element or an `{a, b}` array of them.
fn elements(value: Node) -> Vec<Node> {
    if value.kind() == "element_value_array_initializer" {
        let mut cursor = value.walk();
        value.named_children(&mut cursor).collect()
    } else {
        vec![value]
    }
}

fn annotation_name<'s>(annotation: Node, source: &'s [u8]) -> &'s str {
    let name = annotation
        .child_by_field_name("name")
        .map_or("", |n| text(n, source));
    name.rsplit('.').next().unwrap_or(name)
}

/// `@Queue("q")` / `@Queue(name = "q")` / `@Queue(value = "q")`.
fn queue_annotation_name(queue: Node, ctx: &mut Ctx) -> Option<String> {
    if queue.kind() != "annotation" || annotation_name(queue, ctx.source) != "Queue" {
        return None;
    }
    for (key, v) in annotation_pairs(queue, ctx.source) {
        if matches!(key.as_deref(), None | Some("value") | Some("name")) {
            return ctx.resolve(v);
        }
    }
    None
}

/// The routing keys of a `@QueueBinding`, or its queue when it names no key.
fn binding_keys(binding: Node, ctx: &mut Ctx) -> Vec<String> {
    if binding.kind() != "annotation" || annotation_name(binding, ctx.source) != "QueueBinding" {
        return Vec::new();
    }
    let mut keys = Vec::new();
    let mut queue = None;
    for (key, v) in annotation_pairs(binding, ctx.source) {
        match key.as_deref() {
            Some("key") => keys.extend(elements(v).into_iter().filter_map(|e| ctx.resolve(e))),
            Some("value") => queue = queue_annotation_name(v, ctx),
            _ => {}
        }
    }
    if keys.is_empty() {
        keys.extend(queue);
    }
    keys
}

/// The channels a `@RabbitListener` reads: its queues, or the routing keys of its bindings.
/// A topic-exchange `#` pattern is not a name this pass keys on.
fn rabbit_keys(listener: Node, ctx: &mut Ctx) -> Vec<String> {
    let mut out = Vec::new();
    for (key, v) in annotation_pairs(listener, ctx.source) {
        match key.as_deref() {
            Some("queues") => out.extend(elements(v).into_iter().filter_map(|e| ctx.resolve(e))),
            Some("queuesToDeclare") => {
                for e in elements(v) {
                    out.extend(queue_annotation_name(e, ctx));
                }
            }
            Some("bindings") => {
                for e in elements(v) {
                    out.extend(binding_keys(e, ctx));
                }
            }
            _ => {}
        }
    }
    out.retain(|k| !k.contains('#'));
    out
}

/// The values of the named parameters (or the lone positional one when `positional`).
fn annotation_values(
    annotation: Node,
    names: &[&str],
    positional: bool,
    ctx: &mut Ctx,
) -> Vec<String> {
    let mut out = Vec::new();
    for (key, v) in annotation_pairs(annotation, ctx.source) {
        let wanted = match key.as_deref() {
            Some(k) => names.contains(&k),
            None => positional,
        };
        if wanted {
            out.extend(elements(v).into_iter().filter_map(|e| ctx.resolve(e)));
        }
    }
    out
}

/// A queue or topic name from an ARN or URL: its last segment (`.../123456789012/orders`).
fn aws_name(s: &str) -> String {
    if s.contains("${") {
        return s.to_string();
    }
    s.rsplit(['/', ':']).next().unwrap_or(s).to_string()
}

/// The declaration a modifiers-bound annotation decorates: `(kind, node)`.
fn decorated(annotation: Node) -> Option<Node> {
    let modifiers = annotation.parent().filter(|p| p.kind() == "modifiers")?;
    modifiers.parent()
}

fn decl_owner(decl: Node, source: &[u8]) -> Option<(usize, String)> {
    let name = text(decl.child_by_field_name("name")?, source);
    Some((decl.start_position().row + 1, name.to_string()))
}

fn listener_sites(annotation: Node, ctx: &mut Ctx, found: &mut Vec<Site>) {
    let source = ctx.source;
    let name = annotation_name(annotation, source);
    let (fam, is_rabbit) = match name {
        "RabbitListener" => (Fam::Rabbit, true),
        "JmsListener" => (Fam::Jms, false),
        "SqsListener" => (Fam::SqsTemplate, false),
        _ => return,
    };
    if !ctx.gated(fam) {
        return;
    }
    let Some(decl) = decorated(annotation) else {
        return;
    };
    let (broker, keys) = match name {
        "RabbitListener" => (Broker::RabbitMq, rabbit_keys(annotation, ctx)),
        "JmsListener" => (
            Broker::Jms,
            annotation_values(annotation, &["destination"], false, ctx),
        ),
        _ => (
            Broker::Sqs,
            annotation_values(annotation, &["value", "queueNames"], true, ctx)
                .iter()
                .map(|k| aws_name(k))
                .collect(),
        ),
    };
    let mut keys = keys;
    keys.sort();
    keys.dedup();
    if keys.is_empty() {
        return;
    }
    let mut owners: Vec<(usize, String)> = Vec::new();
    match decl.kind() {
        "method_declaration" => owners.extend(decl_owner(decl, source)),
        // A class-level `@RabbitListener` (its `@RabbitHandler` methods dispatch by payload
        // type): the class is the one consumer of the queue.
        "class_declaration" if is_rabbit => owners.extend(decl_owner(decl, source)),
        _ => {}
    }
    for owner in owners {
        for key in &keys {
            found.push(Site::Listener {
                owner: owner.clone(),
                broker,
                key: key.clone(),
            });
        }
    }
}

/// The receiver family of a call: a declared client variable, `this.client`, or a NATS
/// `createDispatcher(...)` result.
fn receiver_family(call: Node, ctx: &Ctx) -> Option<Fam> {
    let object = call.child_by_field_name("object")?;
    let source = ctx.source;
    match object.kind() {
        "identifier" => ctx.family_of(text(object, source)),
        "field_access" => {
            let target = object.child_by_field_name("object")?;
            if target.kind() != "this" {
                return None;
            }
            ctx.family_of(text(object.child_by_field_name("field")?, source))
        }
        "method_invocation" => {
            let is_dispatcher = object
                .child_by_field_name("name")
                .is_some_and(|n| text(n, source) == "createDispatcher");
            (is_dispatcher && ctx.gated(Fam::Nats)).then_some(Fam::Nats)
        }
        _ => None,
    }
}

/// The first string passed to a call of one of `names` inside `root`'s subtree
/// (`PublishRequest.builder().topicArn("...")`, `r -> r.queueUrl("...")`).
fn builder_value(root: Node, names: &[&str], ctx: &mut Ctx) -> Option<String> {
    for n in subtree(root, MAX_SEARCH_NODES) {
        if n.kind() != "method_invocation" {
            continue;
        }
        let is_wanted = n
            .child_by_field_name("name")
            .is_some_and(|m| names.contains(&text(m, ctx.source)));
        if !is_wanted {
            continue;
        }
        let arg = n.child_by_field_name("arguments")?.named_child(0)?;
        if let Some(v) = ctx.resolve(arg) {
            return Some(v);
        }
    }
    None
}

/// The initializer of the local named `ident` in the method around `at`.
fn local_initializer<'t>(at: Node<'t>, ident: &str, source: &[u8]) -> Option<Node<'t>> {
    let mut up = at.parent();
    while let Some(p) = up {
        if matches!(p.kind(), "method_declaration" | "constructor_declaration") {
            for n in subtree(p, 2048) {
                if n.kind() == "variable_declarator"
                    && n.child_by_field_name("name")
                        .is_some_and(|x| text(x, source) == ident)
                {
                    return n.child_by_field_name("value");
                }
            }
            return None;
        }
        up = p.parent();
    }
    None
}

/// The channel an AWS SDK request names: a string first argument (SDK v1), or the
/// `topicArn` / `queueUrl` of a request built inline or in a local of the method.
fn aws_request_name(call: Node, args: &[Node], names: &[&str], ctx: &mut Ctx) -> Option<String> {
    if args.len() >= 2 {
        if let Some(v) = ctx.resolve(args[0]) {
            return Some(aws_name(&v));
        }
    }
    let first = *args.first()?;
    let source = ctx.source;
    let v = if first.kind() == "identifier" {
        let init = local_initializer(call, text(first, source), source)?;
        builder_value(init, names, ctx)?
    } else {
        builder_value(first, names, ctx)?
    };
    Some(aws_name(&v))
}

fn call_sites(call: Node, ctx: &mut Ctx, found: &mut Vec<Site>) {
    let source = ctx.source;
    let Some(method) = call.child_by_field_name("name").map(|n| text(n, source)) else {
        return;
    };
    if !CALL_NAMES.contains(&method) {
        return;
    }
    let Some(fam) = receiver_family(call, ctx) else {
        return;
    };
    let Some(arglist) = call.child_by_field_name("arguments") else {
        return;
    };
    let mut cursor = arglist.walk();
    let args: Vec<Node> = arglist.named_children(&mut cursor).collect();
    let is_callback = |n: Node| matches!(n.kind(), "lambda_expression" | "method_reference");

    // (producer, key, via)
    let hit: Option<(bool, String)> = match (fam, method) {
        (
            Fam::Rabbit,
            "convertAndSend"
            | "send"
            | "convertSendAndReceive"
            | "sendAndReceive"
            | "convertSendAndReceiveAsType"
            | "sendAndReceiveAsType",
        ) => {
            // (exchange, routingKey, message[, ...]) or (routingKey, message[, postProcessor]):
            // a trailing lambda marks the second form. The routing key names the channel.
            let routing = if args.len() >= 3 && !is_callback(args[2]) {
                args.get(1)
            } else if args.len() >= 2 && !is_callback(args[1]) {
                args.first()
            } else {
                None
            };
            routing.and_then(|a| ctx.resolve(*a)).map(|k| (true, k))
        }
        (Fam::Jms, "convertAndSend" | "send" | "sendAndReceive" | "convertSendAndReceive")
            if args.len() >= 2 =>
        {
            ctx.resolve(args[0]).map(|k| (true, k))
        }
        (Fam::Jms, "receive" | "receiveAndConvert") if !args.is_empty() => {
            ctx.resolve(args[0]).map(|k| (false, k))
        }
        (Fam::SqsTemplate, "send" | "sendAsync" | "convertAndSend") => {
            if args.len() >= 2 {
                ctx.resolve(args[0]).map(|k| (true, aws_name(&k)))
            } else {
                // `send(to -> to.queue("q").payload(p))`
                args.first()
                    .and_then(|a| builder_value(*a, &["queue"], ctx))
                    .map(|k| (true, aws_name(&k)))
            }
        }
        (Fam::SnsTemplate, "sendNotification" | "convertAndSend" | "send") if args.len() >= 2 => {
            ctx.resolve(args[0]).map(|k| (true, aws_name(&k)))
        }
        (Fam::SqsClient, "sendMessage" | "sendMessageBatch") => {
            aws_request_name(call, &args, &["queueUrl", "withQueueUrl"], ctx).map(|k| (true, k))
        }
        (Fam::SqsClient, "receiveMessage") => {
            aws_request_name(call, &args, &["queueUrl", "withQueueUrl"], ctx).map(|k| (false, k))
        }
        (Fam::SnsClient, "publish" | "publishBatch") => {
            aws_request_name(call, &args, &["topicArn", "withTopicArn"], ctx).map(|k| (true, k))
        }
        (Fam::RedisTemplate, "convertAndSend") if args.len() == 2 => {
            ctx.resolve(args[0]).map(|k| (true, k))
        }
        (Fam::Nats, "publish" | "request") if !args.is_empty() => ctx
            .resolve(args[0])
            // `publish(NatsMessage.builder().subject("x")...build())`
            .or_else(|| builder_value(args[0], &["subject"], ctx))
            .map(|k| (true, k)),
        (Fam::Nats, "subscribe") if !args.is_empty() => ctx.resolve(args[0]).map(|k| (false, k)),
        (Fam::PubSub, "publish") if args.len() >= 2 => ctx.resolve(args[0]).map(|k| (true, k)),
        (Fam::RedisContainer, "addMessageListener") if args.len() == 2 => {
            // One topic, or a collection of `new ChannelTopic("x")`: every one is heard.
            let mut keys: Vec<String> = ctx.resolve(args[1]).into_iter().collect();
            if keys.is_empty() {
                for n in subtree(args[1], MAX_SEARCH_NODES) {
                    if matches!(n.kind(), "object_creation_expression" | "method_invocation") {
                        keys.extend(ctx.resolve(n));
                    }
                }
            }
            keys.sort();
            keys.dedup();
            for key in keys {
                found.push(Site::Call {
                    producer: false,
                    broker: Broker::Redis,
                    key,
                    lines: lines(call),
                    via: "addMessageListener",
                });
            }
            return;
        }
        _ => None,
    };
    let Some((producer, key)) = hit else {
        return;
    };
    found.push(Site::Call {
        producer,
        broker: fam.broker(),
        key,
        lines: lines(call),
        via: static_method_name(method),
    });
}

/// `method` as a `'static` string (it is always one of [`CALL_NAMES`]).
fn static_method_name(method: &str) -> &'static str {
    CALL_NAMES
        .iter()
        .copied()
        .find(|m| *m == method)
        .unwrap_or("call")
}

/// Registers a [`Scan`] in `out`, whose `nodes` the Java extractor has already filled.
pub(crate) fn apply(scan: Scan, out: &mut FileIndex) {
    out.jvm_brokers_capped |= scan.capped;
    let Some(template) = out.nodes.first().cloned() else {
        return;
    };
    // The declaration nodes the Java extractor made, by (first line, name).
    let owners: HashMap<(usize, String), usize> = out
        .nodes
        .iter()
        .enumerate()
        .map(|(i, n)| ((n.line_start, n.name.to_string()), i))
        .collect();
    for site in scan.sites {
        match site {
            Site::Listener { owner, broker, key } => {
                let Some(&idx) = owners.get(&owner) else {
                    continue;
                };
                out.consumer_brokers.push((out.consumers.len(), broker));
                out.consumers.push((idx, CompactStr::new(key)));
            }
            Site::Call {
                producer,
                broker,
                key,
                lines,
                via,
            } => {
                out.nodes.push(ContractNode {
                    id: 0,
                    name: CompactStr::new(&key),
                    kind: NodeKind::EventStream,
                    file_path: template.file_path.clone(),
                    line_start: lines.0,
                    line_end: lines.1,
                    package: template.package.clone(),
                    repo_id: template.repo_id,
                    signature: Some(CompactStr::new(format!(
                        "jvm-broker {} {via}",
                        broker.slug()
                    ))),
                    docstring: None,
                });
                let index = out.nodes.len() - 1;
                out.node_brokers.push((index, broker));
                if producer {
                    out.producer_brokers.push((out.producers.len(), broker));
                    out.producers.push((index, CompactStr::new(key)));
                } else {
                    out.consumer_brokers.push((out.consumers.len(), broker));
                    out.consumers.push((index, CompactStr::new(key)));
                }
            }
        }
    }
}
