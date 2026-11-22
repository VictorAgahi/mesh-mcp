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
/// Parameters of a method, lambda or resource list one lookup looks at.
const MAX_PARAMS: usize = 256;
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

/// A library whose classes a file must import (by a real import declaration, or by writing
/// the fully qualified name) for its annotations and clients to count.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Lib {
    Amqp,
    Jms,
    Aws,
    Redis,
    Nats,
    PubSub,
}

impl Lib {
    const ALL: [Lib; 6] = [
        Lib::Amqp,
        Lib::Jms,
        Lib::Aws,
        Lib::Redis,
        Lib::Nats,
        Lib::PubSub,
    ];

    /// The packages (and their sub-packages) the library's classes live in.
    fn packages(self) -> &'static [&'static str] {
        match self {
            Lib::Amqp => &["org.springframework.amqp"],
            Lib::Jms => &[
                "org.springframework.jms",
                "jakarta.jms",
                "javax.jms",
                "org.apache.activemq",
            ],
            Lib::Aws => &[
                "io.awspring.cloud",
                "org.springframework.cloud.aws",
                "software.amazon.awssdk",
                "com.amazonaws",
            ],
            Lib::Redis => &["org.springframework.data.redis"],
            Lib::Nats => &["io.nats.client"],
            Lib::PubSub => &[
                "com.google.cloud.spring.pubsub",
                "org.springframework.cloud.gcp.pubsub",
                "com.google.cloud.pubsub",
            ],
        }
    }

    /// Whether `package` is one of the library's.
    fn owns(self, package: &str) -> bool {
        self.packages().iter().any(|p| {
            package == *p
                || package
                    .strip_prefix(p)
                    .is_some_and(|rest| rest.starts_with('.'))
        })
    }
}

impl Fam {
    fn lib(self) -> Lib {
        match self {
            Fam::Rabbit => Lib::Amqp,
            Fam::Jms => Lib::Jms,
            Fam::SqsTemplate | Fam::SnsTemplate | Fam::SqsClient | Fam::SnsClient => Lib::Aws,
            Fam::RedisTemplate | Fam::RedisContainer => Lib::Redis,
            Fam::Nats => Lib::Nats,
            Fam::PubSub => Lib::PubSub,
        }
    }
}

/// The destination classes a name may be read from, and the library each belongs to.
fn destination_lib(class: &str) -> Option<Lib> {
    match class {
        "ChannelTopic" | "PatternTopic" => Some(Lib::Redis),
        "ActiveMQQueue" | "ActiveMQTopic" => Some(Lib::Jms),
        _ => None,
    }
}

/// The import declarations of a file (never comments or strings: they come off the tree).
#[derive(Default)]
struct Imports {
    /// Simple class name -> package of a single-type import.
    single: HashMap<String, String>,
    /// Packages of `import a.b.*;`.
    wildcard: Vec<String>,
}

impl Imports {
    fn of(root: Node, source: &[u8]) -> Imports {
        let mut out = Imports::default();
        let mut cursor = root.walk();
        for node in root.named_children(&mut cursor) {
            if node.kind() != "import_declaration" {
                continue;
            }
            let mut words = text(node, source).split_whitespace();
            let _import = words.next();
            let rest: String = words.collect::<Vec<_>>().join("");
            if rest.starts_with("static") {
                continue;
            }
            let name = rest.trim_end_matches(';');
            match name.strip_suffix(".*") {
                Some(pkg) => out.wildcard.push(pkg.to_string()),
                None => {
                    if let Some((pkg, simple)) = name.rsplit_once('.') {
                        out.single.insert(simple.to_string(), pkg.to_string());
                    }
                }
            }
        }
        out
    }

    /// Whether any import is of `lib`.
    fn has(&self, lib: Lib) -> bool {
        self.single.values().any(|p| lib.owns(p)) || self.wildcard.iter().any(|p| lib.owns(p))
    }

    /// Whether the simple name `class` is `lib`'s: imported from one of its packages, or (when
    /// not imported by name) reachable through a wildcard import of one.
    fn is_lib_class(&self, class: &str, lib: Lib) -> bool {
        match self.single.get(class) {
            Some(pkg) => lib.owns(pkg),
            None => self.wildcard.iter().any(|p| lib.owns(p)),
        }
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

/// What a constant name of the file resolves to.
enum Entry {
    /// A string constant or destination object of the file.
    Value(String),
    /// Declared twice with differing meanings: read as nothing.
    Conflict,
}

/// What a declaration says about a variable: the client family of its type, and the simple
/// name of the type.
#[derive(Clone)]
struct Decl {
    fam: Option<Fam>,
    ty: Option<String>,
}

/// Per-file tables the call sites are resolved against.
struct Ctx<'a> {
    source: &'a [u8],
    imports: Imports,
    consts: HashMap<String, Entry>,
    /// Names of the types the file declares: they hide an imported class of the same name.
    classes: HashSet<String>,
    /// `@Value("${key}")` fields (`JavaExtractor::value_placeholder_fields`).
    placeholders: HashMap<String, String>,
    /// Parameters and locals of a method, by node id.
    locals: HashMap<usize, HashSet<String>>,
    /// Local variable declarations of a block, by block node id: name -> (start byte, decl).
    blocks: HashMap<usize, HashMap<String, Vec<(usize, Decl)>>>,
    /// Fields of a class body, by node id.
    members: HashMap<usize, HashMap<String, Decl>>,
    /// Names recorded in `members` and `blocks`, against [`MAX_DECLARED_NAMES`].
    declared: usize,
    capped: bool,
}

impl<'a> Ctx<'a> {
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

    /// The client family a type node stands for: its class is the library's (imported by a
    /// real import declaration, or written fully qualified) and no type of the file hides it.
    fn fam_of_type_node(&self, ty: Node) -> Option<Fam> {
        let source = self.source;
        let mut node = ty;
        while node.kind() == "generic_type" {
            node = node.named_child(0)?;
        }
        match node.kind() {
            "type_identifier" => {
                let name = text(node, source);
                if self.classes.contains(name) {
                    return None;
                }
                let fam = Fam::of_type(name)?;
                self.imports.is_lib_class(name, fam.lib()).then_some(fam)
            }
            "scoped_type_identifier" => {
                let full: String = text(node, source).split_whitespace().collect();
                let (package, simple) = full.rsplit_once('.')?;
                let fam = Fam::of_type(simple)?;
                fam.lib().owns(package).then_some(fam)
            }
            _ => None,
        }
    }

    /// What a declaration (`T x`, `var x = new T()`) says about its variable.
    fn decl_of(&self, decl: Node, declarator: Option<Node>) -> Decl {
        let none = Decl {
            fam: None,
            ty: None,
        };
        let source = self.source;
        let Some(ty) = decl.child_by_field_name("type") else {
            return none;
        };
        let simple = simple_type(ty, source);
        if simple.as_deref() != Some("var") {
            return Decl {
                fam: self.fam_of_type_node(ty),
                ty: simple,
            };
        }
        match declarator
            .and_then(|d| d.child_by_field_name("value"))
            .filter(|v| v.kind() == "object_creation_expression")
            .and_then(|v| v.child_by_field_name("type"))
        {
            Some(t) => Decl {
                fam: self.fam_of_type_node(t),
                ty: simple_type(t, source),
            },
            None => none,
        }
    }

    /// The declaration the variable `name` read at `at` refers to, innermost scope first:
    /// lambda and method parameters, locals declared earlier in an enclosing block, loop,
    /// `try` resource and `catch` variables, then the fields of the enclosing classes
    /// (`this_only`: the nearest class's fields only, for `this.name`). `None` when nothing
    /// in the file declares it.
    fn lookup(&mut self, at: Node, name: &str, this_only: bool) -> Option<Decl> {
        let source = self.source;
        let none = Decl {
            fam: None,
            ty: None,
        };
        let mut child = at;
        let mut cur = at.parent();
        while let Some(p) = cur {
            let kind = p.kind();
            if kind == "class_body" {
                if !self.members.contains_key(&p.id()) {
                    let mut map: HashMap<String, Decl> = HashMap::new();
                    let mut cursor = p.walk();
                    let fields: Vec<Node> = p
                        .named_children(&mut cursor)
                        .filter(|m| m.kind() == "field_declaration")
                        .collect();
                    for f in fields {
                        let mut c = f.walk();
                        let declarators: Vec<Node> =
                            f.children_by_field_name("declarator", &mut c).collect();
                        for d in declarators {
                            if self.declared >= MAX_DECLARED_NAMES {
                                self.capped = true;
                                break;
                            }
                            if let Some(n) = d.child_by_field_name("name") {
                                self.declared += 1;
                                map.insert(text(n, source).to_string(), self.decl_of(f, Some(d)));
                            }
                        }
                    }
                    self.members.insert(p.id(), map);
                }
                if let Some(d) = self.members.get(&p.id()).and_then(|m| m.get(name)) {
                    return Some(d.clone());
                }
                if this_only {
                    return None;
                }
            } else if kind == "record_declaration" {
                if let Some(params) = p.child_by_field_name("parameters") {
                    if let Some(d) = self.param_decl(params, name) {
                        return Some(d);
                    }
                }
                if this_only {
                    return None;
                }
            } else if !this_only {
                match kind {
                    "lambda_expression" => {
                        if let Some(params) = p.child_by_field_name("parameters") {
                            match params.kind() {
                                "identifier" if text(params, source) == name => return Some(none),
                                "inferred_parameters" => {
                                    let mut c = params.walk();
                                    let hit = params
                                        .named_children(&mut c)
                                        .take(MAX_PARAMS)
                                        .any(|i| text(i, source) == name);
                                    if hit {
                                        return Some(none);
                                    }
                                }
                                "formal_parameters" => {
                                    if let Some(d) = self.param_decl(params, name) {
                                        return Some(d);
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                    "method_declaration" | "constructor_declaration" => {
                        if let Some(params) = p.child_by_field_name("parameters") {
                            if let Some(d) = self.param_decl(params, name) {
                                return Some(d);
                            }
                        }
                    }
                    "block" | "constructor_body" | "switch_block_statement_group" => {
                        if !self.blocks.contains_key(&p.id()) {
                            let mut map: HashMap<String, Vec<(usize, Decl)>> = HashMap::new();
                            let mut cursor = p.walk();
                            let locals: Vec<Node> = p
                                .named_children(&mut cursor)
                                .filter(|m| m.kind() == "local_variable_declaration")
                                .collect();
                            for l in locals {
                                let mut c = l.walk();
                                let declarators: Vec<Node> =
                                    l.children_by_field_name("declarator", &mut c).collect();
                                for d in declarators {
                                    if self.declared >= MAX_DECLARED_NAMES {
                                        self.capped = true;
                                        break;
                                    }
                                    if let Some(n) = d.child_by_field_name("name") {
                                        self.declared += 1;
                                        let decl = self.decl_of(l, Some(d));
                                        map.entry(text(n, source).to_string())
                                            .or_default()
                                            .push((l.start_byte(), decl));
                                    }
                                }
                            }
                            self.blocks.insert(p.id(), map);
                        }
                        let before = child.start_byte();
                        let hit = self
                            .blocks
                            .get(&p.id())
                            .and_then(|m| m.get(name))
                            .and_then(|v| v.iter().rev().find(|(at, _)| *at < before));
                        if let Some((_, d)) = hit {
                            return Some(d.clone());
                        }
                    }
                    "enhanced_for_statement" => {
                        let in_body = p
                            .child_by_field_name("body")
                            .is_some_and(|b| b.id() == child.id());
                        if in_body
                            && p.child_by_field_name("name")
                                .is_some_and(|n| text(n, source) == name)
                        {
                            return Some(self.decl_of(p, None));
                        }
                    }
                    "for_statement" => {
                        let mut c = p.walk();
                        let inits: Vec<Node> = p
                            .named_children(&mut c)
                            .filter(|m| m.kind() == "local_variable_declaration")
                            .collect();
                        for l in inits {
                            let mut cc = l.walk();
                            let declarators: Vec<Node> =
                                l.children_by_field_name("declarator", &mut cc).collect();
                            for d in declarators {
                                if d.child_by_field_name("name")
                                    .is_some_and(|n| text(n, source) == name)
                                {
                                    return Some(self.decl_of(l, Some(d)));
                                }
                            }
                        }
                    }
                    "try_with_resources_statement" => {
                        if let Some(specs) = p.child_by_field_name("resources") {
                            let mut c = specs.walk();
                            let resources: Vec<Node> = specs
                                .named_children(&mut c)
                                .filter(|r| r.kind() == "resource")
                                .take(MAX_PARAMS)
                                .collect();
                            for r in resources {
                                if r.child_by_field_name("name")
                                    .is_some_and(|n| text(n, source) == name)
                                {
                                    return Some(self.decl_of(r, None));
                                }
                            }
                        }
                    }
                    "catch_clause" => {
                        let mut c = p.walk();
                        let hit = p
                            .named_children(&mut c)
                            .filter(|x| x.kind() == "catch_formal_parameter")
                            .any(|x| {
                                x.child_by_field_name("name")
                                    .is_some_and(|n| text(n, source) == name)
                            });
                        if hit {
                            return Some(none);
                        }
                    }
                    _ => {}
                }
            }
            child = p;
            cur = p.parent();
        }
        None
    }

    /// The parameter `name` of a `formal_parameters` node.
    fn param_decl(&self, params: Node, name: &str) -> Option<Decl> {
        let mut cursor = params.walk();
        let found = params
            .named_children(&mut cursor)
            .filter(|c| c.kind() == "formal_parameter" || c.kind() == "spread_parameter")
            .take(MAX_PARAMS)
            .find(|c| {
                let own = c.child_by_field_name("name");
                // A spread parameter holds its name in a declarator.
                let own = own.or_else(|| {
                    let mut cc = c.walk();
                    let d = c
                        .named_children(&mut cc)
                        .find(|x| x.kind() == "variable_declarator");
                    d.and_then(|d| d.child_by_field_name("name"))
                });
                own.is_some_and(|n| text(n, self.source) == name)
            });
        found.map(|c| {
            if c.kind() == "formal_parameter" {
                self.decl_of(c, None)
            } else {
                Decl {
                    fam: None,
                    ty: None,
                }
            }
        })
    }

    /// Whether the destination class `class` is the library's (imported, not hidden by a type of
    /// the file).
    fn destination_ok(&self, class: &str) -> bool {
        destination_lib(class)
            .is_some_and(|l| !self.classes.contains(class) && self.imports.is_lib_class(class, l))
    }

    /// Whether an annotation is `lib`'s: imported from one of its packages (or written fully
    /// qualified) and not hidden by an annotation type of the file.
    fn annotation_is_lib(&self, annotation: Node, lib: Lib) -> bool {
        let raw: String = annotation
            .child_by_field_name("name")
            .map_or("", |n| text(n, self.source))
            .split_whitespace()
            .collect();
        match raw.rsplit_once('.') {
            Some((package, _)) => lib.owns(package),
            None => !self.classes.contains(&raw) && self.imports.is_lib_class(&raw, lib),
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
                if !self.destination_ok(&ty) {
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
                        if o.kind() == "identifier" && self.destination_ok(text(o, source)) =>
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
        // A placeholder default is never taken as a name (it may be a secret): `${k:d}` reads
        // as `${k}`, resolved by the configuration or left visibly unresolved.
        let name = if name.contains("${") {
            mesh_core::config_values::strip_placeholder_defaults(&name).into_owned()
        } else {
            name
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
    // The types the file declares first: they hide an imported class of the same name.
    walk(root, |node| {
        if matches!(
            node.kind(),
            "class_declaration"
                | "interface_declaration"
                | "enum_declaration"
                | "annotation_type_declaration"
                | "record_declaration"
        ) {
            if let Some(name) = node.child_by_field_name("name") {
                if ctx.classes.len() < MAX_DECLARED_NAMES {
                    ctx.classes.insert(text(name, source).to_string());
                } else {
                    ctx.capped = true;
                }
            }
        }
        true
    });
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
                    let Some(value) = d.child_by_field_name("value") else {
                        continue;
                    };
                    if let Some(lit) = string_literal(value, source) {
                        if is_final && node.kind() == "field_declaration" {
                            ctx.declare_const(name, lit);
                        }
                    } else if value.kind() == "object_creation_expression"
                        && ty.as_deref().is_some_and(|t| ctx.destination_ok(t))
                    {
                        if let Some(v) = ctx.resolve(value) {
                            ctx.declare_const(name, v);
                        }
                    }
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
                    if ctx.destination_ok(&ty) && body.named_child_count() == 1 {
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
    // A cheap filter before the precise one (the import declarations).
    if !Lib::ALL
        .iter()
        .any(|l| l.packages().iter().any(|p| content.contains(p)))
    {
        return Scan::default();
    }
    let source = content.as_bytes();
    let root = tree.root_node();
    let mut ctx = Ctx {
        source,
        imports: Imports::of(root, source),
        consts: HashMap::new(),
        classes: HashSet::new(),
        placeholders: if content.contains("@Value(") {
            JavaExtractor::value_placeholder_fields(content)
        } else {
            HashMap::new()
        },
        locals: HashMap::new(),
        blocks: HashMap::new(),
        members: HashMap::new(),
        declared: 0,
        capped: false,
    };
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
    let (lib, is_rabbit) = match name {
        "RabbitListener" => (Lib::Amqp, true),
        "JmsListener" => (Lib::Jms, false),
        "SqsListener" => (Lib::Aws, false),
        _ => return,
    };
    if !ctx.annotation_is_lib(annotation, lib) {
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

/// The receiver family of a call: a variable (or `this.field`) declared with a client class,
/// as the nearest declaration in scope says, or a NATS `createDispatcher(...)` result.
fn receiver_family(call: Node, ctx: &mut Ctx) -> Option<Fam> {
    let object = call.child_by_field_name("object")?;
    let source = ctx.source;
    match object.kind() {
        "identifier" => ctx.lookup(call, text(object, source), false)?.fam,
        "field_access" => {
            let target = object.child_by_field_name("object")?;
            if target.kind() != "this" {
                return None;
            }
            let field = text(object.child_by_field_name("field")?, source);
            ctx.lookup(call, field, true)?.fam
        }
        "method_invocation" => {
            let is_dispatcher = object
                .child_by_field_name("name")
                .is_some_and(|n| text(n, source) == "createDispatcher");
            (is_dispatcher && ctx.imports.has(Lib::Nats)).then_some(Fam::Nats)
        }
        _ => None,
    }
}

/// What the argument that follows a message in a `RabbitTemplate` send is.
enum Trailing {
    /// A `MessagePostProcessor` / `CorrelationData`: the call is `(routingKey, message, it)`.
    Extra,
    /// The message itself: the call is `(exchange, routingKey, message)`.
    Message,
    /// Nothing the file shows tells which.
    Unknown,
}

fn trailing_role(arg: Node, ctx: &mut Ctx) -> Trailing {
    let is_extra = |t: &str| t.ends_with("PostProcessor") || t.ends_with("CorrelationData");
    match arg.kind() {
        "lambda_expression" | "method_reference" => Trailing::Extra,
        "object_creation_expression" => {
            match arg
                .child_by_field_name("type")
                .and_then(|t| simple_type(t, ctx.source))
            {
                Some(t) if is_extra(&t) => Trailing::Extra,
                _ => Trailing::Message,
            }
        }
        "identifier" => match ctx.lookup(arg, text(arg, ctx.source), false) {
            Some(Decl { ty: Some(t), .. }) if is_extra(&t) => Trailing::Extra,
            Some(Decl { ty: Some(_), .. }) => Trailing::Message,
            _ => Trailing::Unknown,
        },
        _ => Trailing::Message,
    }
}

/// The routing-key argument of a `RabbitTemplate` send: `(exchange, routingKey, message[, ...])`
/// or `(routingKey, message[, postProcessor])`. A three-argument call is told apart by what its
/// last argument is; when the file does not show it, no key is guessed.
fn rabbit_routing<'t>(args: &[Node<'t>], ctx: &mut Ctx) -> Option<Node<'t>> {
    match args.len() {
        0 | 1 => None,
        2 => match trailing_role(args[1], ctx) {
            Trailing::Extra => None,
            _ => Some(args[0]),
        },
        3 => match trailing_role(args[2], ctx) {
            Trailing::Extra => Some(args[0]),
            Trailing::Message => Some(args[1]),
            Trailing::Unknown => None,
        },
        _ => Some(args[1]),
    }
}

/// Whether a key is a pattern (`orders.*`, `orders.#`, NATS `orders.>`): a consumer may
/// subscribe to one, a producer never publishes to one.
fn is_pattern(key: &str) -> bool {
    key.contains(['*', '#', '>'])
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
        ) => rabbit_routing(&args, ctx)
            .and_then(|a| ctx.resolve(a))
            .map(|k| (true, k)),
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
    if producer && is_pattern(&key) {
        return;
    }
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
    // The Java extractor keeps the first line of a declaration as its signature: a listener
    // annotation or a one-line method holding `${key:default}` would carry the default into the
    // graph. Defaults never become names here, so they do not stay as text either.
    let mut site_lines: HashSet<usize> = HashSet::new();
    for site in &scan.sites {
        match site {
            Site::Listener { owner, .. } => {
                site_lines.insert(owner.0);
            }
            Site::Call { lines, .. } => site_lines.extend(lines.0..=lines.1),
        }
    }
    for node in out
        .nodes
        .iter_mut()
        .filter(|n| site_lines.contains(&n.line_start))
    {
        if let Some(sig) = node.signature.as_deref().filter(|s| s.contains("${")) {
            let stripped = mesh_core::config_values::strip_placeholder_defaults(sig).into_owned();
            node.signature = Some(CompactStr::new(stripped));
        }
    }
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
