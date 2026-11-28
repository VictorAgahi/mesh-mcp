//! Message brokers in C# (9.0 ticket T8d).
//!
//! The C# extractor reads Confluent.Kafka by the shape of the call. This pass adds the buses a
//! .NET service talks to through MassTransit, NServiceBus and the Azure Service Bus SDK, each
//! recorded under its own [`Broker`] where the file shows it:
//!
//! | library | consumers | producers |
//! |---|---|---|
//! | MassTransit | a class implementing `IConsumer<T>` / `IJobConsumer<T>` (key: the message type `T`); `ReceiveEndpoint("queue", ...)` inside `UsingRabbitMq` / `UsingAzureServiceBus` / `UsingAmazonSqs` / `UsingInMemory` (key: the queue) | `Publish<T>(...)` / `Publish(new T {...})` / `Send(...)` on `IPublishEndpoint`, `ISendEndpoint`, `IBus`, `IBusControl`, `ConsumeContext` (key: the message type); `GetSendEndpoint(new Uri("queue:name"))` (key: the queue) |
//! | NServiceBus | a class implementing `IHandleMessages<T>` / `IAmStartedByMessages<T>` (key: `T`) | `Publish` / `Send` / `SendLocal` / `Reply` on `IMessageSession`, `IEndpointInstance`, `IMessageHandlerContext`, `IPipelineContext` (key: the message type) |
//! | Azure Service Bus | `ServiceBusClient.CreateProcessor` / `CreateSessionProcessor` / `CreateReceiver` (key: the queue or topic); `[ServiceBusTrigger("queue")]` | `ServiceBusClient.CreateSender("queue-or-topic")`; `[ServiceBusOutput("queue")]` / `[ServiceBus("queue")]` |
//!
//! Only what the file shows is registered: a message type is the type a generic argument names, or
//! the type of a `new T(...)` / a variable of an explicit (or `var x = new T()`) declared type; a
//! queue name is a string literal, a `const string` / `static readonly string` of the file, an
//! `Environment.GetEnvironmentVariable("K")` or an `IConfiguration` read (`${K}` / `${A__B}`
//! placeholders resolved when the index is folded), or a Functions `%Setting%` (`${Setting}`). A
//! default (`?? "d"`, `GetValue<string>("k", "d")`) is never a name, and neither is a connection
//! string: a literal or constant that is not a plain name registers nothing.
//!
//! A call or a type counts only when the file brings the library in (a parsed `using` directive of
//! one of its namespaces, or the fully qualified name) and no type or alias of the file hides it
//! (`class IConsumer<T>` of the file is not MassTransit's). A receiver is a field, property,
//! parameter or local whose declared type is the client interface.
//!
//! The bus (RabbitMQ, Azure Service Bus, SQS, in-memory) is known only where the same file names it:
//! `UsingRabbitMq(...)` / `UsingAzureServiceBus(...)` / `UsingAmazonSqs(...)` / `UsingInMemory(...)`,
//! `UseTransport<RabbitMQTransport>()` / `new AzureServiceBusTransport(...)`. A consumer class
//! of another file has no broker and joins the channel of every broker that uses the name
//! (`heuristic`, T7). The Azure Service Bus SDK is `servicebus` by construction.
//!
//! Bounds: the walks are iterative; a lookup climbs at most the depth of the tree; sites past
//! [`MAX_CS_BROKER_SITES_PER_FILE`] are dropped and the file is counted in
//! `IndexHealth::files_cs_brokers_capped`.

use super::{FileIndex, MAX_CS_BROKER_SITES_PER_FILE};
use mesh_core::member_consts::{
    constant_is_a_name, identifier_is_credential_named, member_is_credential_named,
    name_is_printable,
};
use mesh_core::{Broker, CompactStr, ContractNode, FilePath, NodeKind, RepoId};
use std::collections::{HashMap, HashSet};
use tree_sitter::{Node, Tree};

/// Names the type / constant tables remember; past it the file is counted as capped.
const MAX_DECLARED_NAMES: usize = 16_384;
/// Parameters or arguments one lookup looks at.
const MAX_PARAMS: usize = 256;
/// Arguments of a call read for a message.
const MAX_MESSAGE_ARGS: usize = 3;
/// Links of a constant chain (`const B = A`) one resolution follows.
const MAX_RESOLVE_DEPTH: usize = 4;

/// The libraries whose types and attributes this pass reads.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Lib {
    MassTransit,
    NServiceBus,
    ServiceBus,
    Functions,
    Config,
}

impl Lib {
    fn namespaces(self) -> &'static [&'static str] {
        match self {
            Lib::MassTransit => &["MassTransit"],
            Lib::NServiceBus => &["NServiceBus"],
            Lib::ServiceBus => &["Azure.Messaging.ServiceBus"],
            Lib::Functions => &[
                "Microsoft.Azure.WebJobs",
                "Microsoft.Azure.Functions.Worker",
            ],
            Lib::Config => &["Microsoft.Extensions.Configuration"],
        }
    }

    /// Whether `ns` is one of the library's namespaces or a sub-namespace.
    fn owns(self, ns: &str) -> bool {
        self.namespaces()
            .iter()
            .any(|p| ns == *p || ns.strip_prefix(p).is_some_and(|rest| rest.starts_with('.')))
    }

    /// The `via` tag of a node of this library.
    fn tag(self) -> &'static str {
        match self {
            Lib::MassTransit => "masstransit",
            Lib::NServiceBus => "nservicebus",
            Lib::ServiceBus | Lib::Functions => "servicebus",
            Lib::Config => "config",
        }
    }
}

/// What a client type lets a call do.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Fam {
    /// `Publish` / `Send` and `GetSendEndpoint` (`IBus`, `IBusControl`, `ConsumeContext`).
    MtBus,
    /// `Publish` / `Send` only (`IPublishEndpoint`, `ISendEndpoint`).
    MtPublish,
    /// `GetSendEndpoint` only (`ISendEndpointProvider`).
    MtProvider,
    /// `Publish` / `Send` / `SendLocal` / `Reply`.
    NsbContext,
    /// `CreateSender` / `CreateProcessor` / `CreateReceiver`.
    AsbClient,
    /// Configuration reads.
    Config,
}

fn fam_of_type(name: &str) -> Option<(Lib, Fam)> {
    Some(match name {
        "IBus" | "IBusControl" | "ConsumeContext" => (Lib::MassTransit, Fam::MtBus),
        "IPublishEndpoint" | "ISendEndpoint" => (Lib::MassTransit, Fam::MtPublish),
        "ISendEndpointProvider" => (Lib::MassTransit, Fam::MtProvider),
        "IMessageSession" | "IEndpointInstance" | "IMessageHandlerContext" | "IPipelineContext" => {
            (Lib::NServiceBus, Fam::NsbContext)
        }
        "ServiceBusClient" => (Lib::ServiceBus, Fam::AsbClient),
        "IConfiguration" | "IConfigurationRoot" | "IConfigurationSection" => {
            (Lib::Config, Fam::Config)
        }
        _ => return None,
    })
}

/// The namespaces a file brings in, and the type names it declares or aliases.
#[derive(Default)]
struct Imports {
    namespaces: Vec<String>,
}

impl Imports {
    fn has(&self, lib: Lib) -> bool {
        self.namespaces.iter().any(|n| lib.owns(n))
    }
}

fn text<'s>(node: Node, source: &'s [u8]) -> &'s str {
    node.utf8_text(source).unwrap_or("")
}

fn lines(node: Node) -> (usize, usize) {
    (node.start_position().row + 1, node.end_position().row + 1)
}

/// The content of a plain string literal: no escape, no interpolation, no raw string.
fn string_value(node: Node, source: &[u8]) -> Option<String> {
    let t = text(node, source);
    match node.kind() {
        "string_literal" => {
            if t.starts_with("\"\"\"") || t.contains('\\') {
                return None;
            }
            Some(t.strip_prefix('"')?.strip_suffix('"')?.to_string())
        }
        "verbatim_string_literal" => {
            let inner = t.strip_prefix("@\"")?.strip_suffix('"')?;
            (!inner.contains('"')).then(|| inner.to_string())
        }
        _ => None,
    }
}

/// The expression of an argument (`name: expr` carries a name before it).
fn arg_expr(arg: Node) -> Option<Node> {
    if arg.kind() != "argument" {
        return Some(arg);
    }
    arg.named_child(arg.named_child_count().checked_sub(1)?)
}

/// The arguments of an invocation, at most [`MAX_PARAMS`].
fn args_of(call: Node) -> Vec<Node> {
    let Some(list) = call.child_by_field_name("arguments") else {
        return Vec::new();
    };
    let mut cursor = list.walk();
    list.named_children(&mut cursor).take(MAX_PARAMS).collect()
}

/// The simple type name a (non-generic) type node stands for: `T`, `ns.T`, `T?`.
fn named_type(ty: Node, source: &[u8]) -> Option<String> {
    let mut node = ty;
    while node.kind() == "nullable_type" {
        node = node
            .child_by_field_name("type")
            .or_else(|| node.named_child(0))?;
    }
    match node.kind() {
        "identifier" => Some(text(node, source).to_string()),
        "qualified_name" => {
            let n = node.child_by_field_name("name")?;
            (n.kind() == "identifier").then(|| text(n, source).to_string())
        }
        _ => None,
    }
}

/// Pre-order walk of `root` without recursion; `visit` returns `false` to stop.
fn walk<'t>(root: Node<'t>, mut visit: impl FnMut(Node<'t>) -> bool) {
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

/// What a declaration says about a variable.
#[derive(Clone, Default)]
struct Decl {
    fam: Option<(Lib, Fam)>,
    /// The message type its declared type names (`OrderCreated`), if it is a plain one.
    ty: Option<String>,
    is_const: bool,
    /// Found in a local scope (a parameter or a local), not a member.
    local: bool,
}

/// What a constant name of the file resolves to.
enum Entry {
    Value(String),
    /// Declared twice with differing meanings, or refused as not being a name.
    Conflict,
}

/// Where a site's broker comes from.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Bk {
    /// Fixed by the client (Azure Service Bus) or by the enclosing `Using...` call.
    Known(Broker),
    /// The `Using...` / `UseTransport` evidence of the file for this library, if unique.
    File(Lib),
    Unknown,
}

/// One broker fact before it becomes a node or a consumer / producer row.
enum Site {
    /// A declaration that is the consumer / producer itself: (first line, name) of its owner.
    Owner {
        owner: (usize, String),
        producer: bool,
        key: String,
        bk: Bk,
    },
    /// A call: a producer or a consumer node on its lines.
    Call {
        producer: bool,
        key: String,
        lines: (usize, usize),
        via: &'static str,
        lib: Lib,
        bk: Bk,
    },
}

/// The broker facts of a C# file, read off its tree.
#[derive(Default)]
pub(crate) struct Scan {
    sites: Vec<Site>,
    capped: bool,
    /// The file imports one of the libraries: its signatures are masked of secret-shaped literals.
    active: bool,
    mt_evidence: Vec<Option<Broker>>,
    nsb_evidence: Vec<Option<Broker>>,
}

struct Ctx<'a> {
    source: &'a [u8],
    imports: Imports,
    /// Types the file declares and `using` aliases: they hide a library type of the same name.
    types: HashSet<String>,
    consts: HashMap<String, Entry>,
    members: HashMap<usize, HashMap<String, Decl>>,
    blocks: HashMap<usize, HashMap<String, Vec<(usize, Decl)>>>,
    declared: usize,
    capped: bool,
}

impl<'a> Ctx<'a> {
    fn declare_const(&mut self, name: &str, value: String) {
        let ok = constant_is_a_name(name, &value) && !identifier_is_credential_named(name);
        match self.consts.get(name) {
            Some(Entry::Value(prev)) if ok && *prev == value => {}
            Some(_) => {
                self.consts.insert(name.to_string(), Entry::Conflict);
            }
            None => {
                if self.consts.len() >= MAX_DECLARED_NAMES {
                    self.capped = true;
                    return;
                }
                let entry = if ok {
                    Entry::Value(value)
                } else {
                    Entry::Conflict
                };
                self.consts.insert(name.to_string(), entry);
            }
        }
    }

    /// The library type a type node stands for: imported by a `using` (or written fully
    /// qualified) and not hidden by a type or alias of the file.
    fn fam_of_type_node(&self, ty: Node) -> Option<(Lib, Fam)> {
        let source = self.source;
        let mut node = ty;
        while node.kind() == "nullable_type" {
            node = node
                .child_by_field_name("type")
                .or_else(|| node.named_child(0))?;
        }
        let (qualifier, name_node) = match node.kind() {
            "qualified_name" => (
                Some(node.child_by_field_name("qualifier")?),
                node.child_by_field_name("name")?,
            ),
            _ => (None, node),
        };
        let name = match name_node.kind() {
            "identifier" => text(name_node, source),
            "generic_name" => text(name_node.named_child(0)?, source),
            _ => return None,
        };
        let (lib, fam) = fam_of_type(name)?;
        match qualifier {
            Some(q) => {
                let q: String = text(q, source).split_whitespace().collect();
                lib.owns(&q).then_some((lib, fam))
            }
            None => (!self.types.contains(name) && self.imports.has(lib)).then_some((lib, fam)),
        }
    }

    /// What a declaration (`T x`, `var x = new T()`, a parameter) says about its variable.
    fn decl_of(&self, type_node: Option<Node>, init: Option<Node>, is_const: bool) -> Decl {
        let source = self.source;
        let mut d = Decl {
            is_const,
            local: true,
            ..Decl::default()
        };
        let Some(ty) = type_node else {
            return d;
        };
        if ty.kind() == "implicit_type" {
            if let Some(new) = init.filter(|i| i.kind() == "object_creation_expression") {
                if let Some(t) = new.child_by_field_name("type") {
                    d.fam = self.fam_of_type_node(t);
                    d.ty = named_type(t, source);
                }
            }
            return d;
        }
        d.fam = self.fam_of_type_node(ty);
        d.ty = named_type(ty, source);
        d
    }

    /// The parameter `name` of a `parameter_list` / lambda parameter node.
    fn param_decl(&self, params: Node, name: &str) -> Option<Decl> {
        let source = self.source;
        if params.kind() == "implicit_parameter" || params.kind() == "identifier" {
            return (text(params, source) == name).then(|| Decl {
                local: true,
                ..Decl::default()
            });
        }
        let mut cursor = params.walk();
        let found = params
            .named_children(&mut cursor)
            .filter(|c| c.kind() == "parameter")
            .take(MAX_PARAMS)
            .find(|c| {
                c.child_by_field_name("name")
                    .is_some_and(|n| text(n, source) == name)
            });
        found.map(|p| self.decl_of(p.child_by_field_name("type"), None, false))
    }

    /// The declaration the variable `name` read at `at` refers to, innermost scope first
    /// (lambda and method parameters, locals declared earlier in an enclosing block, loop, `using`
    /// and `catch` variables, then the members of the enclosing types). `members_only` is the
    /// `this.name` form: the nearest type's members only. `None` when the file declares nothing
    /// of that name; a declaration of an untyped variable is returned as such, so it hides.
    fn lookup(&mut self, at: Node, name: &str, members_only: bool) -> Option<Decl> {
        let source = self.source;
        let mut child = at;
        let mut cur = at.parent();
        let mut past_type = false;
        while let Some(p) = cur {
            match p.kind() {
                "class_declaration"
                | "record_declaration"
                | "struct_declaration"
                | "interface_declaration" => {
                    let mut cursor = p.walk();
                    let ctor_params = p
                        .named_children(&mut cursor)
                        .find(|c| c.kind() == "parameter_list");
                    if let Some(params) = ctor_params {
                        if let Some(mut d) = self.param_decl(params, name) {
                            d.local = false;
                            return Some(d);
                        }
                    }
                    if !self.members.contains_key(&p.id()) {
                        let map = self.members_of(p);
                        self.members.insert(p.id(), map);
                    }
                    if let Some(d) = self.members.get(&p.id()).and_then(|m| m.get(name)) {
                        return Some(d.clone());
                    }
                    if members_only {
                        return None;
                    }
                    past_type = true;
                }
                _ if members_only || past_type => {}
                "lambda_expression" | "anonymous_method_expression" => {
                    if let Some(params) = p.child_by_field_name("parameters") {
                        if let Some(d) = self.param_decl(params, name) {
                            return Some(d);
                        }
                    }
                }
                "method_declaration"
                | "constructor_declaration"
                | "local_function_statement"
                | "operator_declaration"
                | "destructor_declaration" => {
                    if let Some(params) = p.child_by_field_name("parameters") {
                        if let Some(d) = self.param_decl(params, name) {
                            return Some(d);
                        }
                    }
                }
                "block" | "switch_section" | "compilation_unit" => {
                    if !self.blocks.contains_key(&p.id()) {
                        let map = self.block_decls(p);
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
                "for_statement" | "using_statement" | "fixed_statement" => {
                    let mut c = p.walk();
                    let vars: Vec<Node> = p
                        .named_children(&mut c)
                        .filter(|m| m.kind() == "variable_declaration")
                        .collect();
                    for v in vars {
                        if let Some(d) = self.variable_decl(v, name) {
                            return Some(d);
                        }
                    }
                }
                "foreach_statement" => {
                    let in_body = p
                        .child_by_field_name("body")
                        .is_some_and(|b| b.id() == child.id());
                    if in_body
                        && p.child_by_field_name("left")
                            .is_some_and(|n| text(n, source) == name)
                    {
                        return Some(self.decl_of(p.child_by_field_name("type"), None, false));
                    }
                }
                "catch_clause" => {
                    let mut c = p.walk();
                    let hit = p
                        .named_children(&mut c)
                        .find(|x| x.kind() == "catch_declaration")
                        .filter(|x| {
                            x.child_by_field_name("name")
                                .is_some_and(|n| text(n, source) == name)
                        });
                    if let Some(x) = hit {
                        return Some(self.decl_of(x.child_by_field_name("type"), None, false));
                    }
                }
                _ => {}
            }
            child = p;
            cur = p.parent();
        }
        None
    }

    /// The declaration of `name` among the declarators of a `variable_declaration`.
    fn variable_decl(&self, var: Node, name: &str) -> Option<Decl> {
        let source = self.source;
        let mut cursor = var.walk();
        let declarators: Vec<Node> = var
            .named_children(&mut cursor)
            .filter(|c| c.kind() == "variable_declarator")
            .take(MAX_PARAMS)
            .collect();
        for d in declarators {
            if d.child_by_field_name("name")
                .is_some_and(|n| text(n, source) == name)
            {
                let init = (d.named_child_count() >= 2)
                    .then(|| d.named_child(d.named_child_count() - 1))
                    .flatten();
                return Some(self.decl_of(var.child_by_field_name("type"), init, false));
            }
        }
        None
    }

    /// The fields, properties and primary-constructor-free members of a type declaration.
    fn members_of(&mut self, class: Node) -> HashMap<String, Decl> {
        let source = self.source;
        let mut map = HashMap::new();
        let Some(body) = class.child_by_field_name("body") else {
            return map;
        };
        let mut cursor = body.walk();
        let members: Vec<Node> = body.named_children(&mut cursor).collect();
        for m in members {
            match m.kind() {
                "field_declaration" => {
                    let mut c = m.walk();
                    let var = m
                        .named_children(&mut c)
                        .find(|x| x.kind() == "variable_declaration");
                    let Some(var) = var else { continue };
                    let mut cc = var.walk();
                    let declarators: Vec<Node> = var
                        .named_children(&mut cc)
                        .filter(|x| x.kind() == "variable_declarator")
                        .collect();
                    for d in declarators {
                        if self.declared >= MAX_DECLARED_NAMES {
                            self.capped = true;
                            break;
                        }
                        let Some(n) = d.child_by_field_name("name") else {
                            continue;
                        };
                        self.declared += 1;
                        let init = (d.named_child_count() >= 2)
                            .then(|| d.named_child(d.named_child_count() - 1))
                            .flatten();
                        let mut decl = self.decl_of(var.child_by_field_name("type"), init, false);
                        decl.local = false;
                        map.insert(text(n, source).to_string(), decl);
                    }
                }
                "property_declaration" => {
                    if self.declared >= MAX_DECLARED_NAMES {
                        self.capped = true;
                        continue;
                    }
                    if let Some(n) = m.child_by_field_name("name") {
                        self.declared += 1;
                        let mut decl = self.decl_of(m.child_by_field_name("type"), None, false);
                        decl.local = false;
                        map.insert(text(n, source).to_string(), decl);
                    }
                }
                _ => {}
            }
        }
        map
    }

    /// The locals a block (or the top-level statements of a file) declares, by name.
    fn block_decls(&mut self, block: Node) -> HashMap<String, Vec<(usize, Decl)>> {
        let source = self.source;
        let mut map: HashMap<String, Vec<(usize, Decl)>> = HashMap::new();
        let mut cursor = block.walk();
        let stmts: Vec<Node> = block.named_children(&mut cursor).collect();
        for s in stmts {
            let s = if s.kind() == "global_statement" {
                match s.named_child(0) {
                    Some(inner) => inner,
                    None => continue,
                }
            } else {
                s
            };
            if s.kind() != "local_declaration_statement" {
                continue;
            }
            let is_const = {
                let mut c = s.walk();
                let found = s
                    .named_children(&mut c)
                    .any(|m| m.kind() == "modifier" && text(m, source) == "const");
                found
            };
            let mut c = s.walk();
            let Some(var) = s
                .named_children(&mut c)
                .find(|x| x.kind() == "variable_declaration")
            else {
                continue;
            };
            let mut cc = var.walk();
            let declarators: Vec<Node> = var
                .named_children(&mut cc)
                .filter(|x| x.kind() == "variable_declarator")
                .collect();
            for d in declarators {
                if self.declared >= MAX_DECLARED_NAMES {
                    self.capped = true;
                    break;
                }
                let Some(n) = d.child_by_field_name("name") else {
                    continue;
                };
                self.declared += 1;
                let init = (d.named_child_count() >= 2)
                    .then(|| d.named_child(d.named_child_count() - 1))
                    .flatten();
                let decl = self.decl_of(var.child_by_field_name("type"), init, is_const);
                map.entry(text(n, source).to_string())
                    .or_default()
                    .push((s.start_byte(), decl));
            }
        }
        map
    }

    /// Whether `name` is a type parameter of a method or type around `at` (`Publish<T>` inside
    /// `void Send<T>(T m)` names no message).
    fn is_type_param(&self, at: Node, name: &str) -> bool {
        let source = self.source;
        let mut up = at.parent();
        while let Some(p) = up {
            if !matches!(
                p.kind(),
                "method_declaration"
                    | "local_function_statement"
                    | "class_declaration"
                    | "record_declaration"
                    | "struct_declaration"
                    | "interface_declaration"
            ) {
                up = p.parent();
                continue;
            }
            let mut cursor = p.walk();
            let list = p.child_by_field_name("type_parameters").or_else(|| {
                p.named_children(&mut cursor)
                    .find(|c| c.kind() == "type_parameter_list")
            });
            if let Some(list) = list {
                let mut c = list.walk();
                let hit = list
                    .named_children(&mut c)
                    .any(|t| text(t, source).trim() == name);
                if hit {
                    return true;
                }
            }
            up = p.parent();
        }
        false
    }

    /// A message type named by a type node (`OrderCreated`, `Contracts.OrderCreated`).
    fn message_type(&self, at: Node, ty: Node) -> Option<String> {
        let name = named_type(ty, self.source)?;
        (!self.is_type_param(at, &name) && name_is_printable(&name)).then_some(name)
    }

    /// The receiver family of a call: a variable, `this.field`, a `new T(...)`, through `await`,
    /// parentheses and `!`.
    fn receiver_fam(&mut self, recv: Node) -> Option<(Lib, Fam)> {
        let source = self.source;
        let mut r = recv;
        for _ in 0..4 {
            match r.kind() {
                "await_expression" | "parenthesized_expression" | "postfix_unary_expression" => {
                    r = r.named_child(0)?;
                }
                _ => break,
            }
        }
        match r.kind() {
            "identifier" => self.lookup(r, text(r, source), false)?.fam,
            "member_access_expression" => {
                let owner = r.child_by_field_name("expression")?;
                if text(owner, source) != "this" {
                    return None;
                }
                let field = text(r.child_by_field_name("name")?, source);
                self.lookup(r, field, true)?.fam
            }
            "object_creation_expression" => self.fam_of_type_node(r.child_by_field_name("type")?),
            _ => None,
        }
    }

    /// The channel name a node stands for, if the file shows it. `setting`: a Functions
    /// `%Name%` app setting is read as a `${Name}` placeholder.
    fn resolve(&mut self, node: Node, setting: bool) -> Option<String> {
        self.resolve_at(node, setting, 0)
    }

    fn resolve_at(&mut self, node: Node, setting: bool, depth: usize) -> Option<String> {
        if depth > MAX_RESOLVE_DEPTH {
            return None;
        }
        let source = self.source;
        let name = match node.kind() {
            "string_literal" | "verbatim_string_literal" => {
                let v = string_value(node, source)?;
                match v
                    .strip_prefix('%')
                    .and_then(|r| r.strip_suffix('%'))
                    .filter(|_| setting)
                {
                    Some(key) => placeholder(key)?,
                    None => v,
                }
            }
            "parenthesized_expression" | "postfix_unary_expression" => {
                return self.resolve_at(node.named_child(0)?, setting, depth + 1);
            }
            // `x ?? "default"`: the default is never a name.
            "binary_expression" => {
                let op = node
                    .child_by_field_name("operator")
                    .map(|o| text(o, source));
                if op != Some("??") {
                    return None;
                }
                return self.resolve_at(node.child_by_field_name("left")?, setting, depth + 1);
            }
            "identifier" => {
                let id = text(node, source);
                match self.lookup(node, id, false) {
                    Some(d) if d.local && !d.is_const => return None,
                    _ => {}
                }
                self.const_value(id)?
            }
            // `Queues.Orders` of a class the file declares, `this.Orders`.
            "member_access_expression" => {
                let owner = node.child_by_field_name("expression")?;
                let declared_here = text(owner, source) == "this"
                    || (owner.kind() == "identifier" && self.types.contains(text(owner, source)));
                if !declared_here {
                    return None;
                }
                let id = text(node.child_by_field_name("name")?, source);
                self.const_value(id)?
            }
            // `configuration["A:B"]` on an `IConfiguration`.
            "element_access_expression" => {
                let recv = node.child_by_field_name("expression")?;
                if !matches!(self.receiver_fam(recv), Some((Lib::Config, _))) {
                    return None;
                }
                let sub = node.child_by_field_name("subscript")?;
                let arg = sub.named_child(0).and_then(arg_expr)?;
                placeholder(&config_env_name(&string_value(arg, source)?))?
            }
            "invocation_expression" => {
                let func = node.child_by_field_name("function")?;
                let args = args_of(node);
                let first = args.first().copied().and_then(arg_expr)?;
                if func.kind() != "member_access_expression" {
                    return None;
                }
                let method = func.child_by_field_name("name")?;
                let method_name = match method.kind() {
                    "identifier" => text(method, source),
                    "generic_name" => text(method.named_child(0)?, source),
                    _ => return None,
                };
                let recv = func.child_by_field_name("expression")?;
                match method_name {
                    // `Environment.GetEnvironmentVariable("K")`
                    "GetEnvironmentVariable" => {
                        let r = text(recv, source);
                        let is_env = matches!(r, "Environment" | "System.Environment")
                            && !self.types.contains("Environment")
                            && self.lookup(recv, "Environment", false).is_none();
                        if !is_env {
                            return None;
                        }
                        placeholder(&string_value(first, source)?)?
                    }
                    // `configuration.GetValue<string>("A:B"[, "default"])`
                    "GetValue" => {
                        if !matches!(self.receiver_fam(recv), Some((Lib::Config, _))) {
                            return None;
                        }
                        placeholder(&config_env_name(&string_value(first, source)?))?
                    }
                    _ => return None,
                }
            }
            _ => return None,
        };
        usable_key(&name).then_some(name)
    }

    fn const_value(&self, id: &str) -> Option<String> {
        match self.consts.get(id) {
            Some(Entry::Value(v)) => Some(v.clone()),
            _ => None,
        }
    }
}

/// `A:B` of the configuration, spelled as the environment variable that overrides it.
fn config_env_name(key: &str) -> String {
    key.replace(':', "__")
}

/// Whether a configuration key reads like a credential or a connection string.
fn key_is_secret(key: &str) -> bool {
    let lower = key.to_ascii_lowercase();
    lower.contains("connectionstring")
        || lower.contains("connstr")
        || lower.contains("sharedaccess")
        || member_is_credential_named(&key.replace(['-', '.', ':'], "_"))
        || identifier_is_credential_named(key)
}

/// `${key}` for a setting or an environment variable, unless its name is credential-shaped or not
/// a plain identifier.
fn placeholder(key: &str) -> Option<String> {
    let plain = !key.is_empty()
        && key
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'));
    (plain && !key_is_secret(key)).then(|| format!("${{{key}}}"))
}

/// A usable channel name: a `${key}` placeholder, or a plain name (no URL, no credential-shaped
/// token, no wildcard).
fn usable_key(key: &str) -> bool {
    if key.starts_with("${") {
        return true;
    }
    !key.contains(['*', '#', '>']) && constant_is_a_name("", key)
}

/// MassTransit `Using...` calls: the transport each names.
fn mt_transport(name: &str) -> Option<Option<Broker>> {
    let bare = name.strip_prefix("Create").unwrap_or(name);
    Some(match bare {
        "UsingRabbitMq" => Some(Broker::RabbitMq),
        "UsingAzureServiceBus" => Some(Broker::ServiceBus),
        "UsingAmazonSqs" => Some(Broker::Sqs),
        "UsingInMemory" => Some(Broker::InProc),
        // A transport with no broker key of its own.
        "UsingActiveMq" | "UsingGrpc" => None,
        _ => return None,
    })
}

/// NServiceBus transports, by the class (`RabbitMQTransport`).
fn nsb_transport(class: &str) -> Option<Option<Broker>> {
    if !class.ends_with("Transport") {
        return None;
    }
    Some(match class {
        "RabbitMQTransport" => Some(Broker::RabbitMq),
        "AzureServiceBusTransport" => Some(Broker::ServiceBus),
        "SqsTransport" => Some(Broker::Sqs),
        _ => None,
    })
}

/// The one broker a list of evidence agrees on.
fn pick(evidence: &[Option<Broker>]) -> Option<Broker> {
    let first = (*evidence.first()?)?;
    evidence.iter().all(|e| *e == Some(first)).then_some(first)
}

/// The method name and generic type arguments of an invocation's function.
struct Callee<'t> {
    receiver: Option<Node<'t>>,
    name: String,
    type_args: Vec<Node<'t>>,
}

fn callee<'t>(call: Node<'t>, source: &[u8]) -> Option<Callee<'t>> {
    let func = call.child_by_field_name("function")?;
    let (receiver, name_node) = match func.kind() {
        "member_access_expression" => (
            func.child_by_field_name("expression"),
            func.child_by_field_name("name")?,
        ),
        // `_bus?.Publish<T>(...)`: the function is the conditional access.
        "conditional_access_expression" => {
            let binding = func
                .named_child(func.named_child_count().checked_sub(1)?)
                .filter(|b| b.kind() == "member_binding_expression")?;
            (
                func.child_by_field_name("condition"),
                binding.child_by_field_name("name")?,
            )
        }
        "identifier" | "generic_name" => (None, func),
        _ => return None,
    };
    let (name, type_args) = match name_node.kind() {
        "identifier" => (text(name_node, source).to_string(), Vec::new()),
        "generic_name" => {
            let ident = name_node.named_child(0)?;
            let mut cursor = name_node.walk();
            let list = name_node
                .named_children(&mut cursor)
                .find(|c| c.kind() == "type_argument_list");
            let args = list
                .map(|l| {
                    let mut c = l.walk();
                    l.named_children(&mut c).collect()
                })
                .unwrap_or_default();
            (text(ident, source).to_string(), args)
        }
        _ => return None,
    };
    Some(Callee {
        receiver,
        name,
        type_args,
    })
}

/// Reads the broker facts of a C# file. Cheap for a file that names none of the libraries.
pub(crate) fn scan(tree: &Tree, content: &str) -> Scan {
    const MARKERS: [&str; 5] = [
        "MassTransit",
        "NServiceBus",
        "Azure.Messaging.ServiceBus",
        "Microsoft.Azure.WebJobs",
        "Microsoft.Azure.Functions.Worker",
    ];
    if !MARKERS.iter().any(|m| content.contains(m)) {
        return Scan::default();
    }
    let source = content.as_bytes();
    let root = tree.root_node();
    let mut ctx = Ctx {
        source,
        imports: Imports::default(),
        types: HashSet::new(),
        consts: HashMap::new(),
        members: HashMap::new(),
        blocks: HashMap::new(),
        declared: 0,
        capped: false,
    };
    declare(root, &mut ctx);
    // The file names a library (even fully qualified): its signatures are masked, its sites gated.
    let mut scan = Scan {
        active: true,
        ..Scan::default()
    };

    let mut found: Vec<Site> = Vec::new();
    let (mut mt_ev, mut nsb_ev) = (Vec::new(), Vec::new());
    walk(root, |node| {
        match node.kind() {
            "class_declaration" | "record_declaration" | "struct_declaration" => {
                consumer_classes(node, &ctx, &mut found);
            }
            "attribute" => attribute_sites(node, &mut ctx, &mut found),
            "invocation_expression" => {
                call_sites(node, &mut ctx, &mut found, &mut mt_ev, &mut nsb_ev);
            }
            "object_creation_expression" if ctx.imports.has(Lib::NServiceBus) => {
                let class = node
                    .child_by_field_name("type")
                    .and_then(|t| named_type(t, source))
                    .filter(|t| !ctx.types.contains(t));
                if let Some(t) = class {
                    nsb_ev.extend(nsb_transport(&t));
                }
            }
            _ => {}
        }
        found.len() <= MAX_CS_BROKER_SITES_PER_FILE
    });
    scan.capped = found.len() > MAX_CS_BROKER_SITES_PER_FILE || ctx.capped;
    found.truncate(MAX_CS_BROKER_SITES_PER_FILE);
    scan.sites = found;
    scan.mt_evidence = mt_ev;
    scan.nsb_evidence = nsb_ev;
    scan
}

/// Fills the namespace, type and constant tables of the file.
fn declare<'t>(root: Node<'t>, ctx: &mut Ctx) {
    let source = ctx.source;
    let mut pending: Vec<(String, Node<'t>)> = Vec::new();
    walk(root, |node| {
        match node.kind() {
            "using_directive" => {
                let t = text(node, source).trim().trim_end_matches(';');
                let t = t.strip_prefix("global").map_or(t, str::trim_start);
                let rest = t.strip_prefix("using").map_or("", str::trim_start);
                if let Some(alias) = node.child_by_field_name("name") {
                    ctx.types.insert(text(alias, source).to_string());
                } else if !rest.starts_with("static ") && !rest.is_empty() {
                    let ns: String = rest.split_whitespace().collect();
                    if ctx.imports.namespaces.len() < MAX_DECLARED_NAMES {
                        ctx.imports.namespaces.push(ns);
                    }
                }
            }
            "class_declaration"
            | "interface_declaration"
            | "struct_declaration"
            | "record_declaration"
            | "enum_declaration"
            | "delegate_declaration" => {
                if let Some(n) = node.child_by_field_name("name") {
                    if ctx.types.len() < MAX_DECLARED_NAMES {
                        ctx.types.insert(text(n, source).to_string());
                    } else {
                        ctx.capped = true;
                    }
                }
            }
            "field_declaration" | "local_declaration_statement" => {
                let mut c = node.walk();
                let mods: Vec<&str> = node
                    .named_children(&mut c)
                    .filter(|m| m.kind() == "modifier")
                    .map(|m| text(m, source))
                    .collect();
                let constant = mods.contains(&"const")
                    || (mods.contains(&"static") && mods.contains(&"readonly"));
                if constant {
                    let mut c2 = node.walk();
                    let var = node
                        .named_children(&mut c2)
                        .find(|x| x.kind() == "variable_declaration");
                    if let Some(var) = var {
                        let is_string = var.child_by_field_name("type").is_some_and(|t| {
                            matches!(text(t, source), "string" | "String" | "System.String")
                        });
                        if is_string {
                            let mut c3 = var.walk();
                            let declarators: Vec<Node> = var
                                .named_children(&mut c3)
                                .filter(|x| x.kind() == "variable_declarator")
                                .collect();
                            for d in declarators {
                                let Some(n) = d.child_by_field_name("name") else {
                                    continue;
                                };
                                let Some(v) = (d.named_child_count() >= 2)
                                    .then(|| d.named_child(d.named_child_count() - 1))
                                    .flatten()
                                else {
                                    continue;
                                };
                                let name = text(n, source).to_string();
                                match string_value(v, source) {
                                    Some(s) => ctx.declare_const(&name, s),
                                    None => pending.push((name, v)),
                                }
                            }
                        }
                    }
                }
            }
            _ => {}
        }
        true
    });
    // A constant defined by another (`const B = A;`, `Queues.A`): one pass over what is left.
    for (name, node) in pending {
        // A constant that points at something else than a constant is read as nothing.
        let v = match node.kind() {
            "identifier" | "member_access_expression" => ctx.resolve(node, false),
            _ => None,
        };
        match v {
            Some(v) => ctx.declare_const(&name, v),
            None => {
                ctx.consts.insert(name, Entry::Conflict);
            }
        }
    }
}

/// The message type a base type of a consumer class names: `T` of `IConsumer<T>`, or `T` of
/// `IConsumer<Batch<T>>`.
fn consumed_type(arg: Node, ctx: &Ctx) -> Option<String> {
    let source = ctx.source;
    let mut arg = arg;
    if arg.kind() == "generic_name" {
        let outer = text(arg.named_child(0)?, source);
        if outer != "Batch" {
            return None;
        }
        let mut cursor = arg.walk();
        let list = arg
            .named_children(&mut cursor)
            .find(|c| c.kind() == "type_argument_list")?;
        let mut c = list.walk();
        let mut items = list.named_children(&mut c);
        arg = items.next().filter(|_| items.next().is_none())?;
    }
    let name = named_type(arg, source)?;
    (!ctx.is_type_param(arg, &name) && name_is_printable(&name)).then_some(name)
}

/// `IConsumer<T>`, `IHandleMessages<T>` ... in the base list of a class.
fn consumer_classes(class: Node, ctx: &Ctx, found: &mut Vec<Site>) {
    let source = ctx.source;
    let mut cursor = class.walk();
    let Some(base_list) = class
        .named_children(&mut cursor)
        .find(|c| c.kind() == "base_list")
    else {
        return;
    };
    let Some(owner_name) = class.child_by_field_name("name") else {
        return;
    };
    let owner = (
        class.start_position().row + 1,
        text(owner_name, source).to_string(),
    );
    let mut c = base_list.walk();
    let bases: Vec<Node> = base_list.named_children(&mut c).take(MAX_PARAMS).collect();
    for base in bases {
        let ty = base;
        let (qualifier, generic) = match ty.kind() {
            "qualified_name" => (
                ty.child_by_field_name("qualifier"),
                ty.child_by_field_name("name"),
            ),
            _ => (None, Some(ty)),
        };
        let Some(generic) = generic.filter(|g| g.kind() == "generic_name") else {
            continue;
        };
        let Some(ident) = generic.named_child(0) else {
            continue;
        };
        let name = text(ident, source);
        let lib = match name {
            "IConsumer" | "IJobConsumer" => Lib::MassTransit,
            "IHandleMessages" | "IAmStartedByMessages" => Lib::NServiceBus,
            _ => continue,
        };
        let gated = match qualifier {
            Some(q) => {
                let q: String = text(q, source).split_whitespace().collect();
                lib.owns(&q)
            }
            None => !ctx.types.contains(name) && ctx.imports.has(lib),
        };
        if !gated {
            continue;
        }
        let mut cc = generic.walk();
        let Some(list) = generic
            .named_children(&mut cc)
            .find(|x| x.kind() == "type_argument_list")
        else {
            continue;
        };
        let mut c2 = list.walk();
        let args: Vec<Node> = list.named_children(&mut c2).collect();
        // `IConsumer<TKey, TValue>` of Confluent.Kafka has two arguments: never MassTransit's.
        if args.len() != 1 {
            continue;
        }
        let Some(key) = consumed_type(args[0], ctx) else {
            continue;
        };
        found.push(Site::Owner {
            owner: owner.clone(),
            producer: false,
            key,
            bk: Bk::File(lib),
        });
    }
}

/// `[ServiceBusTrigger("queue")]`, `[ServiceBusOutput("queue")]`, `[ServiceBus("queue")]`.
fn attribute_sites(attr: Node, ctx: &mut Ctx, found: &mut Vec<Site>) {
    let source = ctx.source;
    let Some(name_node) = attr.child_by_field_name("name") else {
        return;
    };
    let (qualifier, bare) = match name_node.kind() {
        "qualified_name" => (
            name_node.child_by_field_name("qualifier"),
            name_node.child_by_field_name("name"),
        ),
        _ => (None, Some(name_node)),
    };
    let Some(bare) = bare.filter(|b| b.kind() == "identifier") else {
        return;
    };
    let written = text(bare, source);
    let name = written.strip_suffix("Attribute").unwrap_or(written);
    let producer = match name {
        "ServiceBusTrigger" => false,
        "ServiceBusOutput" | "ServiceBus" => true,
        _ => return,
    };
    let gated = match qualifier {
        Some(q) => {
            let q: String = text(q, source).split_whitespace().collect();
            Lib::Functions.owns(&q)
        }
        None => !ctx.types.contains(written) && ctx.imports.has(Lib::Functions),
    };
    if !gated {
        return;
    }
    // The function the attribute decorates (on the method or on one of its parameters).
    let mut up = attr.parent();
    let method = loop {
        match up {
            Some(p) if p.kind() == "method_declaration" => break p,
            Some(p) if matches!(p.kind(), "class_declaration" | "compilation_unit") => return,
            Some(p) => up = p.parent(),
            None => return,
        }
    };
    let Some(method_name) = method.child_by_field_name("name") else {
        return;
    };
    let Some(list) = attr
        .named_children(&mut attr.walk())
        .find(|c| c.kind() == "attribute_argument_list")
    else {
        return;
    };
    // The first argument is the queue or topic; `Connection = "..."` is an assignment, which
    // names no channel (a setting name is never read).
    let first = list
        .named_children(&mut list.walk())
        .find(|a| a.kind() == "attribute_argument")
        .and_then(|a| a.named_child(a.named_child_count().checked_sub(1)?));
    let Some(first) = first else { return };
    let Some(key) = ctx.resolve(first, true) else {
        return;
    };
    found.push(Site::Owner {
        owner: (
            method.start_position().row + 1,
            text(method_name, source).to_string(),
        ),
        producer,
        key,
        bk: Bk::Known(Broker::ServiceBus),
    });
}

/// The message a `Publish` / `Send` call carries: its generic argument, else the first argument
/// whose type the file shows.
fn message_key(call: Node, callee: &Callee, ctx: &mut Ctx) -> Option<String> {
    let source = ctx.source;
    if let Some(t) = callee.type_args.first() {
        return (callee.type_args.len() == 1)
            .then(|| ctx.message_type(call, *t))
            .flatten();
    }
    for arg in args_of(call).into_iter().take(MAX_MESSAGE_ARGS) {
        let Some(e) = arg_expr(arg) else { continue };
        match e.kind() {
            // A destination (`Send("queue", msg)`) comes before the message.
            "string_literal" | "verbatim_string_literal" | "interpolated_string_expression" => {
                continue;
            }
            "object_creation_expression" => {
                return ctx.message_type(call, e.child_by_field_name("type")?);
            }
            "identifier" => {
                let ty = ctx.lookup(e, text(e, source), false)?.ty?;
                return (!ctx.is_type_param(call, &ty) && name_is_printable(&ty)).then_some(ty);
            }
            _ => return None,
        }
    }
    None
}

/// The queue of `new Uri("queue:name")` / `exchange:` / `topic:`.
fn uri_queue(arg: Node, ctx: &mut Ctx) -> Option<String> {
    let source = ctx.source;
    let e = arg_expr(arg)?;
    if e.kind() != "object_creation_expression" {
        return None;
    }
    let ty = named_type(e.child_by_field_name("type")?, source)?;
    if ty != "Uri" {
        return None;
    }
    let first = args_of(e).first().copied().and_then(arg_expr)?;
    let v = string_value(first, source)?;
    let rest = ["queue:", "exchange:", "topic:"]
        .iter()
        .find_map(|p| v.strip_prefix(p))?;
    let name = rest.split('?').next().unwrap_or(rest);
    usable_key(name).then(|| name.to_string())
}

/// The broker of the nearest `Using...` call around `node` (a `ReceiveEndpoint` is inside one).
fn enclosing_transport(node: Node, source: &[u8]) -> Option<Option<Broker>> {
    let mut up = node.parent();
    while let Some(p) = up {
        if p.kind() == "invocation_expression" {
            if let Some(c) = callee(p, source) {
                if let Some(b) = mt_transport(&c.name) {
                    return Some(b);
                }
            }
        }
        up = p.parent();
    }
    None
}

const CALL_NAMES: &[&str] = &[
    "Publish",
    "Send",
    "SendLocal",
    "Reply",
    "GetSendEndpoint",
    "CreateSender",
    "CreateProcessor",
    "CreateSessionProcessor",
    "CreateReceiver",
    "ReceiveEndpoint",
    "UseTransport",
];

fn call_sites(
    call: Node,
    ctx: &mut Ctx,
    found: &mut Vec<Site>,
    mt_ev: &mut Vec<Option<Broker>>,
    nsb_ev: &mut Vec<Option<Broker>>,
) {
    let source = ctx.source;
    let Some(c) = callee(call, source) else {
        return;
    };
    // The transports the file names (`UsingRabbitMq`, `UseTransport<RabbitMQTransport>`).
    if ctx.imports.has(Lib::MassTransit) {
        if let Some(b) = mt_transport(&c.name) {
            mt_ev.push(b);
        }
    }
    if c.name == "UseTransport" && ctx.imports.has(Lib::NServiceBus) {
        if let Some(t) = c.type_args.first().and_then(|t| named_type(*t, source)) {
            if !ctx.types.contains(&t) {
                nsb_ev.extend(nsb_transport(&t));
            }
        }
    }
    let Some(method) = CALL_NAMES.iter().copied().find(|m| *m == c.name) else {
        return;
    };
    let ls = lines(call);

    // `ReceiveEndpoint("queue", ...)` inside a MassTransit `Using...` configuration.
    if method == "ReceiveEndpoint" {
        if !ctx.imports.has(Lib::MassTransit) {
            return;
        }
        let Some(transport) = enclosing_transport(call, source) else {
            return;
        };
        let Some(first) = args_of(call).first().copied().and_then(arg_expr) else {
            return;
        };
        let Some(key) = ctx.resolve(first, false) else {
            return;
        };
        found.push(Site::Call {
            producer: false,
            key,
            lines: ls,
            via: "ReceiveEndpoint",
            lib: Lib::MassTransit,
            bk: transport.map_or(Bk::Unknown, Bk::Known),
        });
        return;
    }

    let Some(recv) = c.receiver else { return };
    let Some((lib, fam)) = ctx.receiver_fam(recv) else {
        return;
    };
    let hit: Option<(bool, String, Bk)> = match (fam, method) {
        (Fam::MtBus | Fam::MtPublish, "Publish" | "Send") => {
            message_key(call, &c, ctx).map(|k| (true, k, Bk::File(Lib::MassTransit)))
        }
        (Fam::MtBus | Fam::MtProvider, "GetSendEndpoint") => args_of(call)
            .first()
            .copied()
            .and_then(|a| uri_queue(a, ctx))
            .map(|k| (true, k, Bk::File(Lib::MassTransit))),
        (Fam::NsbContext, "Publish" | "Send" | "SendLocal" | "Reply") => {
            message_key(call, &c, ctx).map(|k| (true, k, Bk::File(Lib::NServiceBus)))
        }
        (Fam::AsbClient, "CreateSender") => args_of(call)
            .first()
            .copied()
            .and_then(arg_expr)
            .and_then(|a| ctx.resolve(a, false))
            .map(|k| (true, k, Bk::Known(Broker::ServiceBus))),
        (Fam::AsbClient, "CreateProcessor" | "CreateSessionProcessor" | "CreateReceiver") => {
            args_of(call)
                .first()
                .copied()
                .and_then(arg_expr)
                .and_then(|a| ctx.resolve(a, false))
                .map(|k| (false, k, Bk::Known(Broker::ServiceBus)))
        }
        _ => None,
    };
    let Some((producer, key, bk)) = hit else {
        return;
    };
    found.push(Site::Call {
        producer,
        key,
        lines: ls,
        via: method,
        lib,
        bk,
    });
}

/// Masks the string literals of `sig` that are secret-shaped (a connection string, a URL with
/// credentials, a key): `Connection = "Endpoint=sb://h/;SharedAccessKey=k"` on an attribute line
/// is a node's signature, and a signature is stored and printed.
fn mask_secret_literals(sig: &str) -> Option<String> {
    if !sig.contains('"') {
        return None;
    }
    let mut out = String::with_capacity(sig.len());
    let mut changed = false;
    let mut rest = sig;
    while let Some(start) = rest.find('"') {
        out.push_str(&rest[..=start]);
        let after = &rest[start + 1..];
        let Some(end) = after.find('"') else {
            out.push_str(after);
            rest = "";
            break;
        };
        let lit = &after[..end];
        let lower = lit.to_ascii_lowercase();
        let secret_shaped = !lit.is_empty()
            && !lit.starts_with("${")
            && !constant_is_a_name("", lit)
            && (lit.contains([';', '=', '@'])
                || lit.contains("://")
                || [
                    "key",
                    "secret",
                    "token",
                    "password",
                    "pwd",
                    "sig",
                    "credential",
                ]
                .iter()
                .any(|w| lower.contains(w)));
        if secret_shaped {
            out.push_str("[REDACTED_SECRET: USE_ENV_OR_LOCAL_FALLBACK]");
            changed = true;
        } else {
            out.push_str(lit);
        }
        out.push('"');
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    changed.then_some(out)
}

/// Registers a [`Scan`] in `out`, whose nodes the C# extractor has already filled. Returns the
/// indices of the producers and of the consumers it registered without a broker, which the
/// caller must not tag as Kafka.
pub(crate) fn apply(
    scan: Scan,
    out: &mut FileIndex,
    file_path: &FilePath,
    repo_id: RepoId,
) -> (Vec<usize>, Vec<usize>) {
    out.cs_brokers_capped |= scan.capped;
    let (mut unsure_p, mut unsure_c) = (Vec::new(), Vec::new());
    if scan.active {
        for node in &mut out.nodes {
            if let Some(masked) = node.signature.as_deref().and_then(mask_secret_literals) {
                node.signature = Some(CompactStr::new(masked));
            }
        }
    }
    let template = out.nodes.first().cloned().unwrap_or_else(|| ContractNode {
        id: 0,
        name: CompactStr::new(""),
        kind: NodeKind::EventStream,
        file_path: file_path.clone(),
        line_start: 1,
        line_end: 1,
        package: mesh_core::detect_service_package(file_path, None),
        repo_id,
        signature: None,
        docstring: None,
    });
    let owners: HashMap<(usize, String), usize> = out
        .nodes
        .iter()
        .enumerate()
        .map(|(i, n)| ((n.line_start, n.name.to_string()), i))
        .collect();
    let file_broker = |lib: Lib| match lib {
        Lib::MassTransit => pick(&scan.mt_evidence),
        Lib::NServiceBus => pick(&scan.nsb_evidence),
        _ => None,
    };
    let resolve = |bk: Bk| match bk {
        Bk::Known(b) => Some(b),
        Bk::File(lib) => file_broker(lib),
        Bk::Unknown => None,
    };
    for site in &scan.sites {
        match site {
            Site::Owner {
                owner,
                producer,
                key,
                bk,
            } => {
                let Some(&idx) = owners.get(owner) else {
                    continue;
                };
                let broker = resolve(*bk);
                if *producer {
                    match broker {
                        Some(b) => out.producer_brokers.push((out.producers.len(), b)),
                        None => unsure_p.push(out.producers.len()),
                    }
                    out.producers.push((idx, CompactStr::new(key)));
                } else {
                    match broker {
                        Some(b) => out.consumer_brokers.push((out.consumers.len(), b)),
                        None => unsure_c.push(out.consumers.len()),
                    }
                    out.consumers.push((idx, CompactStr::new(key)));
                }
            }
            Site::Call {
                producer,
                key,
                lines,
                via,
                lib,
                bk,
            } => {
                let broker = resolve(*bk);
                out.nodes.push(ContractNode {
                    id: 0,
                    name: CompactStr::new(key),
                    kind: NodeKind::EventStream,
                    file_path: template.file_path.clone(),
                    line_start: lines.0,
                    line_end: lines.1,
                    package: template.package.clone(),
                    repo_id: template.repo_id,
                    signature: Some(CompactStr::new(format!("cs-broker {} {via}", lib.tag()))),
                    docstring: None,
                });
                let index = out.nodes.len() - 1;
                if let Some(b) = broker {
                    out.node_brokers.push((index, b));
                }
                if *producer {
                    match broker {
                        Some(b) => out.producer_brokers.push((out.producers.len(), b)),
                        None => unsure_p.push(out.producers.len()),
                    }
                    out.producers.push((index, CompactStr::new(key)));
                } else {
                    match broker {
                        Some(b) => out.consumer_brokers.push((out.consumers.len(), b)),
                        None => unsure_c.push(out.consumers.len()),
                    }
                    out.consumers.push((index, CompactStr::new(key)));
                }
            }
        }
    }
    (unsure_p, unsure_c)
}
