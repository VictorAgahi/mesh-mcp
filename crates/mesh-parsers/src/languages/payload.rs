//! Payload facts of TypeScript / JavaScript and Java files (9.0 T11, `mesh_core::PayloadFacts`).
//!
//! Declared types: TypeScript interfaces, object type aliases and classes (fields and
//! constructor parameter properties), Java classes (instance fields) and records (components).
//! Sites, only where the payload type is statically visible:
//! - TypeScript consumers: a method decorated `@EventPattern`, `@MessagePattern`, `@OnEvent`,
//!   `@Process`, `@RabbitSubscribe`, `@RabbitRPC`, `@SqsMessageHandler` (its `@Payload()`
//!   parameter, else its first undecorated one), a BullMQ `process(job: Job<T>)` method,
//!   `new Worker<T>(...)` or `new Worker(q, (job: Job<T>) => ...)`;
//! - TypeScript producers: `x.emit / emitAsync / send / add(topic, payload)` (the second
//!   argument: `new T()`, `v as T`, an object literal, or an identifier whose parameter or local
//!   declaration states its type), `new Queue<T>(...)`, an `@InjectQueue` `Queue<T>`;
//! - Java consumers: a method annotated `@KafkaListener`, `@KafkaHandler`, `@RabbitListener`,
//!   `@RabbitHandler`, `@JmsListener`, `@SqsListener`, `@StreamListener` (its `@Payload`
//!   parameter, else its first one without `@Header(s)`; `ConsumerRecord<K, V>` is `V`,
//!   `Message<T>` is `T`);
//! - Java producers: `send / sendDefault / convertAndSend(topic, ..., payload)` (the last
//!   argument; when it is not typed, the `V` of a `KafkaTemplate<K, V>` field receiver).
//!
//! Only names, declared types and lines are kept, never a value: a literal's type is its JSON
//! family (`string`, `number`, `boolean`). Walks are iterative (`ts_calls::walk`); the
//! identifier lookup is bounded to two steps.

use super::ts_calls::walk;
use mesh_core::{
    CompactStr, PayloadFacts, PayloadField, PayloadImport, PayloadShape, PayloadSite, PayloadType,
    MAX_PAYLOAD_FIELDS_PER_TYPE,
};
use tree_sitter::{Node, Tree};

const TS_CONSUMER_DECORATORS: &[&str] = &[
    "EventPattern",
    "MessagePattern",
    "OnEvent",
    "Process",
    "RabbitSubscribe",
    "RabbitRPC",
    "SqsMessageHandler",
];
const TS_PRODUCER_METHODS: &[&str] = &["emit", "emitAsync", "send", "add"];
const JAVA_LISTENERS: &[&str] = &[
    "KafkaListener",
    "KafkaHandler",
    "RabbitListener",
    "RabbitHandler",
    "JmsListener",
    "SqsListener",
    "StreamListener",
];
const JAVA_PRODUCER_METHODS: &[&str] = &["send", "sendDefault", "convertAndSend"];
/// Steps an identifier is followed through declarations (`const a = b; const b = new T()`).
const LOOKUP_DEPTH: u8 = 2;
/// Longest type text kept.
const TYPE_MAX: usize = 64;

fn text<'s>(n: Node, src: &'s [u8]) -> &'s str {
    n.utf8_text(src).unwrap_or("")
}

fn line(n: Node) -> u32 {
    n.start_position().row as u32 + 1
}

fn end_line(n: Node) -> u32 {
    n.end_position().row as u32 + 1
}

/// A declared type as written, when it is a plain type (no literal, bounded).
fn clean_type(raw: &str) -> Option<CompactStr> {
    let t = raw.trim().trim_start_matches(':').trim();
    let t: String = t.split_whitespace().collect::<Vec<_>>().join(" ");
    let plain = !t.is_empty()
        && t.len() <= TYPE_MAX
        && t.chars()
            .all(|c| c.is_ascii_alphanumeric() || " _$<>,.[]?|&".contains(c));
    plain.then(|| CompactStr::new(t))
}

/// `a.b.Foo<X>` -> `Foo`, when it is an identifier.
fn simple_name(raw: &str) -> Option<CompactStr> {
    let head = raw.split('<').next()?.trim();
    let name = head.rsplit('.').next()?.trim();
    let ident = !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
        && !name.starts_with(|c: char| c.is_ascii_digit());
    ident.then(|| CompactStr::new(name))
}

fn named_children(n: Node) -> Vec<Node> {
    let mut c = n.walk();
    n.named_children(&mut c).collect()
}

fn push_field(fields: &mut Vec<PayloadField>, incomplete: &mut bool, f: PayloadField) {
    if fields.len() >= MAX_PAYLOAD_FIELDS_PER_TYPE {
        *incomplete = true;
    } else {
        fields.push(f);
    }
}

/// The string literal's content, when `n` is one without substitutions.
fn string_literal(n: Node, src: &[u8]) -> Option<CompactStr> {
    match n.kind() {
        "string" | "string_literal" => {
            let inner = named_children(n);
            match inner.as_slice() {
                [frag] if frag.kind() == "string_fragment" => {
                    Some(CompactStr::new(text(*frag, src)))
                }
                [] => Some(CompactStr::new("")),
                _ => None,
            }
        }
        _ => None,
    }
}

// ---------------------------------------------------------------- TypeScript

fn ts_decorator_name<'s>(d: Node, src: &'s [u8]) -> &'s str {
    let Some(inner) = d.named_child(0) else {
        return "";
    };
    let callee = if inner.kind() == "call_expression" {
        inner.child_by_field_name("function").unwrap_or(inner)
    } else {
        inner
    };
    let t = text(callee, src);
    t.rsplit('.').next().unwrap_or(t)
}

/// The decorators written before a class member (siblings in the class body).
fn ts_member_decorators<'t>(n: Node<'t>) -> Vec<Node<'t>> {
    let mut out = Vec::new();
    let mut prev = n.prev_named_sibling();
    while let Some(p) = prev {
        match p.kind() {
            "decorator" => out.push(p),
            "comment" => {}
            _ => break,
        }
        prev = p.prev_named_sibling();
    }
    let mut c = n.walk();
    out.extend(n.children(&mut c).filter(|c| c.kind() == "decorator"));
    out
}

/// A member's name: `id`, `'id'` and `"id"` are the same field; a computed key is `None`.
fn ts_member_name(n: Node, src: &[u8]) -> Option<CompactStr> {
    match n.kind() {
        "property_identifier" | "number" => Some(CompactStr::new(text(n, src))),
        "string" => string_literal(n, src),
        _ => None,
    }
}

fn ts_fields_of_object_type(body: Node, src: &[u8]) -> (Vec<PayloadField>, bool) {
    let mut fields = Vec::new();
    let mut incomplete = false;
    for m in named_children(body) {
        match m.kind() {
            "property_signature" => {
                let Some(name) = m
                    .child_by_field_name("name")
                    .and_then(|n| ts_member_name(n, src))
                else {
                    // A computed key: the field list is not closed.
                    incomplete = true;
                    continue;
                };
                let optional = {
                    let mut c = m.walk();
                    let has = m.children(&mut c).any(|c| c.kind() == "?");
                    has
                };
                let ty = m
                    .child_by_field_name("type")
                    .and_then(|t| clean_type(text(t, src)));
                push_field(
                    &mut fields,
                    &mut incomplete,
                    PayloadField {
                        name,
                        ty,
                        line: line(m),
                        optional,
                    },
                );
            }
            // An index signature or a method: the shape is not a closed list of fields.
            "index_signature" => incomplete = true,
            _ => {}
        }
    }
    (fields, incomplete)
}

fn ts_class_fields(class: Node, src: &[u8]) -> (Vec<PayloadField>, bool) {
    let mut fields = Vec::new();
    let mut incomplete = false;
    let mut c = class.walk();
    if class
        .children(&mut c)
        .any(|ch| ch.kind() == "class_heritage" && text(ch, src).contains("extends"))
    {
        incomplete = true;
    }
    let Some(body) = class.child_by_field_name("body") else {
        return (fields, incomplete);
    };
    for m in named_children(body) {
        match m.kind() {
            "public_field_definition" => {
                let mut c = m.walk();
                let skip = m.children(&mut c).any(|ch| {
                    matches!(ch.kind(), "static")
                        || (ch.kind() == "accessibility_modifier"
                            && matches!(text(ch, src), "private" | "protected"))
                });
                if skip {
                    continue;
                }
                let Some(name) = m
                    .child_by_field_name("name")
                    .and_then(|n| ts_member_name(n, src))
                else {
                    // A computed or `#private` member.
                    continue;
                };
                let mut c = m.walk();
                let optional = m.children(&mut c).any(|ch| ch.kind() == "?");
                let ty = m
                    .child_by_field_name("type")
                    .and_then(|t| clean_type(text(t, src)));
                push_field(
                    &mut fields,
                    &mut incomplete,
                    PayloadField {
                        name,
                        ty,
                        line: line(m),
                        optional,
                    },
                );
            }
            "method_definition" => {
                let is_ctor = m
                    .child_by_field_name("name")
                    .is_some_and(|n| text(n, src) == "constructor");
                let Some(params) = m.child_by_field_name("parameters").filter(|_| is_ctor) else {
                    continue;
                };
                for p in named_children(params) {
                    let mut c = p.walk();
                    let public = p.children(&mut c).any(|ch| {
                        (ch.kind() == "accessibility_modifier" && text(ch, src) == "public")
                            || ch.kind() == "readonly"
                    });
                    let mut c = p.walk();
                    let hidden = p.children(&mut c).any(|ch| {
                        ch.kind() == "accessibility_modifier"
                            && matches!(text(ch, src), "private" | "protected")
                    });
                    if !public || hidden {
                        continue;
                    }
                    let Some(pat) = p
                        .child_by_field_name("pattern")
                        .filter(|n| n.kind() == "identifier")
                    else {
                        continue;
                    };
                    let ty = p
                        .child_by_field_name("type")
                        .and_then(|t| clean_type(text(t, src)));
                    push_field(
                        &mut fields,
                        &mut incomplete,
                        PayloadField {
                            name: CompactStr::new(text(pat, src)),
                            ty,
                            line: line(p),
                            optional: p.kind() == "optional_parameter",
                        },
                    );
                }
            }
            "index_signature" => incomplete = true,
            _ => {}
        }
    }
    (fields, incomplete)
}

/// The payload shape of a written type (a `type_annotation` or the type node itself).
fn ts_type_shape(t: Node, src: &[u8]) -> PayloadShape {
    let t = if t.kind() == "type_annotation" {
        match t.named_child(0) {
            Some(inner) => inner,
            None => return PayloadShape::Untyped,
        }
    } else {
        t
    };
    match t.kind() {
        "type_identifier" | "nested_type_identifier" => {
            simple_name(text(t, src)).map_or(PayloadShape::Untyped, PayloadShape::Named)
        }
        "generic_type" => {
            let name = t
                .child_by_field_name("name")
                .map(|n| text(n, src))
                .unwrap_or("");
            let name = name.rsplit('.').next().unwrap_or(name);
            if name == "Job" {
                // BullMQ `Job<Data, Result, Name>`: the data.
                return t
                    .child_by_field_name("type_arguments")
                    .and_then(|a| a.named_child(0))
                    .map_or(PayloadShape::Untyped, |a| ts_type_shape(a, src));
            }
            simple_name(name).map_or(PayloadShape::Untyped, PayloadShape::Named)
        }
        "object_type" => {
            let (fields, incomplete) = ts_fields_of_object_type(t, src);
            PayloadShape::Inline { fields, incomplete }
        }
        _ => PayloadShape::Untyped,
    }
}

/// The JSON family of a literal value, never the value.
fn ts_literal_family(v: Node) -> Option<CompactStr> {
    let fam = match v.kind() {
        "string" | "template_string" => "string",
        "number" => "number",
        "true" | "false" => "boolean",
        _ => return None,
    };
    Some(CompactStr::new(fam))
}

fn ts_object_literal(obj: Node, src: &[u8]) -> PayloadShape {
    let mut fields = Vec::new();
    let mut incomplete = false;
    for m in named_children(obj) {
        let (name, ty) = match m.kind() {
            "shorthand_property_identifier" => (text(m, src).to_string(), None),
            "pair" => {
                let Some(k) = m.child_by_field_name("key") else {
                    continue;
                };
                let name = match k.kind() {
                    "property_identifier" => text(k, src).to_string(),
                    "string" => match string_literal(k, src) {
                        Some(s) => s.to_string(),
                        None => {
                            incomplete = true;
                            continue;
                        }
                    },
                    // A computed key: the field list is not static.
                    _ => {
                        incomplete = true;
                        continue;
                    }
                };
                (
                    name,
                    m.child_by_field_name("value").and_then(ts_literal_family),
                )
            }
            "spread_element" => {
                incomplete = true;
                continue;
            }
            _ => continue,
        };
        push_field(
            &mut fields,
            &mut incomplete,
            PayloadField {
                name: CompactStr::new(name),
                ty,
                line: line(m),
                optional: false,
            },
        );
    }
    PayloadShape::Inline { fields, incomplete }
}

/// The nearest enclosing function-like node.
fn enclosing_fn<'t>(n: Node<'t>, kinds: &[&str]) -> Option<Node<'t>> {
    let mut p = n.parent();
    while let Some(x) = p {
        if kinds.contains(&x.kind()) {
            return Some(x);
        }
        p = x.parent();
    }
    None
}

const TS_FN_KINDS: &[&str] = &[
    "method_definition",
    "function_declaration",
    "function_expression",
    "arrow_function",
    "generator_function_declaration",
];

/// The payload shape of an expression passed as a payload.
fn ts_expr_shape(e: Node, src: &[u8], depth: u8) -> PayloadShape {
    match e.kind() {
        "new_expression" => e
            .child_by_field_name("constructor")
            .and_then(|c| simple_name(text(c, src)))
            .map_or(PayloadShape::Untyped, PayloadShape::Named),
        "as_expression" | "satisfies_expression" => {
            let kids = named_children(e);
            kids.last()
                .filter(|_| kids.len() >= 2)
                .map_or(PayloadShape::Untyped, |t| ts_type_shape(*t, src))
        }
        "type_assertion" => named_children(e)
            .into_iter()
            .find(|c| c.kind() == "type_arguments")
            .and_then(|a| a.named_child(0))
            .map_or(PayloadShape::Untyped, |t| ts_type_shape(t, src)),
        "object" => ts_object_literal(e, src),
        "parenthesized_expression" => e
            .named_child(0)
            .map_or(PayloadShape::Untyped, |i| ts_expr_shape(i, src, depth)),
        "identifier" if depth > 0 => ts_identifier_shape(e, src, depth - 1),
        _ => PayloadShape::Untyped,
    }
}

/// What the enclosing function says about identifier `id`: a typed parameter, or a local
/// declaration with a type or a value, the first one in source order.
fn ts_identifier_shape(id: Node, src: &[u8], depth: u8) -> PayloadShape {
    let name = text(id, src);
    let mut f = enclosing_fn(id, TS_FN_KINDS);
    while let Some(func) = f {
        if let Some(params) = func.child_by_field_name("parameters") {
            for p in named_children(params) {
                let is_it = p
                    .child_by_field_name("pattern")
                    .is_some_and(|pat| pat.kind() == "identifier" && text(pat, src) == name);
                if is_it {
                    return p
                        .child_by_field_name("type")
                        .map_or(PayloadShape::Untyped, |t| ts_type_shape(t, src));
                }
            }
        }
        if let Some(body) = func.child_by_field_name("body") {
            let mut found: Option<PayloadShape> = None;
            walk(body, &mut |n| {
                if found.is_some()
                    || n.kind() != "variable_declarator"
                    || n.start_byte() > id.start_byte()
                {
                    return;
                }
                let is_it = n
                    .child_by_field_name("name")
                    .is_some_and(|v| v.kind() == "identifier" && text(v, src) == name);
                if !is_it {
                    return;
                }
                found = Some(if let Some(t) = n.child_by_field_name("type") {
                    ts_type_shape(t, src)
                } else if let Some(v) = n.child_by_field_name("value") {
                    ts_expr_shape(v, src, depth)
                } else {
                    PayloadShape::Untyped
                });
            });
            if let Some(s) = found {
                return s;
            }
        }
        f = enclosing_fn(func, TS_FN_KINDS);
    }
    PayloadShape::Untyped
}

fn ts_args(call: Node) -> Vec<Node> {
    call.child_by_field_name("arguments")
        .map(named_children)
        .unwrap_or_default()
        .into_iter()
        .filter(|a| a.kind() != "comment")
        .collect()
}

fn ts_consumer_method(m: Node, src: &[u8], out: &mut PayloadFacts) {
    let decorators = ts_member_decorators(m);
    let decorated = decorators
        .iter()
        .find(|d| TS_CONSUMER_DECORATORS.contains(&ts_decorator_name(**d, src)));
    let name = m
        .child_by_field_name("name")
        .map(|n| text(n, src))
        .unwrap_or("");
    let params = m
        .child_by_field_name("parameters")
        .map(named_children)
        .unwrap_or_default();
    let first_type = params
        .first()
        .and_then(|p| p.child_by_field_name("type"))
        .and_then(|t| t.named_child(0));
    let job_param = first_type.is_some_and(|t| {
        t.kind() == "generic_type"
            && t.child_by_field_name("name")
                .is_some_and(|n| text(n, src) == "Job")
    });
    if decorated.is_none() && !(name == "process" && job_param) {
        return;
    }
    let topic = decorated
        .and_then(|d| d.named_child(0))
        .filter(|c| c.kind() == "call_expression")
        .and_then(|c| ts_args(c).first().copied())
        .and_then(|a| string_literal(a, src));
    let param_decorators = |p: Node| -> Vec<String> {
        let mut c = p.walk();
        p.children(&mut c)
            .filter(|ch| ch.kind() == "decorator")
            .map(|d| ts_decorator_name(d, src).to_string())
            .collect()
    };
    let chosen = params
        .iter()
        .find(|p| param_decorators(**p).iter().any(|d| d == "Payload"))
        .or_else(|| params.iter().find(|p| param_decorators(**p).is_empty()));
    let (shape, at) = match chosen {
        Some(p) => (
            p.child_by_field_name("type")
                .map_or(PayloadShape::Untyped, |t| ts_type_shape(t, src)),
            line(*p),
        ),
        None => (PayloadShape::Untyped, line(m)),
    };
    let start = decorators.iter().map(|d| line(*d)).min().unwrap_or(line(m));
    out.push_site(PayloadSite {
        producer: false,
        start: start.min(line(m)),
        end: end_line(m),
        line: at,
        topic,
        shape,
    });
}

fn ts_new_expression(n: Node, src: &[u8], out: &mut PayloadFacts) {
    let Some(ctor) = n
        .child_by_field_name("constructor")
        .and_then(|c| simple_name(text(c, src)))
    else {
        return;
    };
    let producer = match ctor.as_str() {
        "Queue" => true,
        "Worker" => false,
        _ => return,
    };
    let args = ts_args(n);
    let topic = args.first().and_then(|a| string_literal(*a, src));
    let generic = n
        .child_by_field_name("type_arguments")
        .and_then(|a| a.named_child(0))
        .map(|t| ts_type_shape(t, src));
    let shape = match generic {
        Some(s) => s,
        None if !producer => args
            .get(1)
            .and_then(|f| f.child_by_field_name("parameters"))
            .and_then(|p| p.named_child(0))
            .and_then(|p| p.child_by_field_name("type"))
            .map_or(PayloadShape::Untyped, |t| ts_type_shape(t, src)),
        None => return,
    };
    out.push_site(PayloadSite {
        producer,
        start: line(n),
        end: end_line(n),
        line: line(n),
        topic,
        shape,
    });
}

fn ts_producer_call(n: Node, src: &[u8], out: &mut PayloadFacts) {
    let Some(prop) = n
        .child_by_field_name("function")
        .filter(|f| f.kind() == "member_expression")
        .and_then(|f| f.child_by_field_name("property"))
    else {
        return;
    };
    if !TS_PRODUCER_METHODS.contains(&text(prop, src)) {
        return;
    }
    let args = ts_args(n);
    let [first, payload, ..] = args.as_slice() else {
        return;
    };
    out.push_site(PayloadSite {
        producer: true,
        start: line(n),
        end: end_line(n),
        line: line(*payload),
        topic: string_literal(*first, src),
        shape: ts_expr_shape(*payload, src, LOOKUP_DEPTH),
    });
}

/// `@InjectQueue('q') queue: Queue<T>` (a field or a constructor parameter).
fn ts_inject_queue(n: Node, src: &[u8], out: &mut PayloadFacts) {
    let decorators: Vec<Node> = if n.kind() == "public_field_definition" {
        ts_member_decorators(n)
    } else {
        let mut c = n.walk();
        n.children(&mut c)
            .filter(|ch| ch.kind() == "decorator")
            .collect()
    };
    let Some(d) = decorators
        .iter()
        .find(|d| ts_decorator_name(**d, src) == "InjectQueue")
    else {
        return;
    };
    let Some(t) = n
        .child_by_field_name("type")
        .and_then(|t| t.named_child(0))
        .filter(|t| t.kind() == "generic_type")
    else {
        return;
    };
    let Some(arg) = t
        .child_by_field_name("type_arguments")
        .and_then(|a| a.named_child(0))
    else {
        return;
    };
    let topic = d
        .named_child(0)
        .filter(|c| c.kind() == "call_expression")
        .and_then(|c| ts_args(c).first().copied())
        .and_then(|a| string_literal(a, src));
    out.push_site(PayloadSite {
        producer: true,
        start: line(*d).min(line(n)),
        end: end_line(n),
        line: line(n),
        topic,
        shape: ts_type_shape(arg, src),
    });
}

/// The payload facts of a TypeScript / JavaScript tree.
pub(crate) fn scan_ts(tree: &Tree, src: &[u8]) -> PayloadFacts {
    let mut out = PayloadFacts::default();
    let mut imports: Vec<PayloadImport> = Vec::new();
    walk(tree.root_node(), &mut |n| match n.kind() {
        "interface_declaration" => {
            let (Some(name), Some(body)) =
                (n.child_by_field_name("name"), n.child_by_field_name("body"))
            else {
                return;
            };
            let (fields, mut incomplete) = ts_fields_of_object_type(body, src);
            let mut c = n.walk();
            incomplete |= n
                .children(&mut c)
                .any(|ch| ch.kind() == "extends_type_clause");
            out.push_type(PayloadType {
                name: CompactStr::new(text(name, src)),
                line: line(n),
                fields,
                incomplete,
                opaque: false,
            });
        }
        "type_alias_declaration" => {
            let (Some(name), Some(value)) = (
                n.child_by_field_name("name"),
                n.child_by_field_name("value"),
            ) else {
                return;
            };
            if value.kind() != "object_type" {
                // An intersection, union, mapped or utility type: the name is declared (it
                // shadows a same-named type elsewhere), its fields are not known.
                out.push_type(PayloadType {
                    name: CompactStr::new(text(name, src)),
                    line: line(n),
                    fields: Vec::new(),
                    incomplete: true,
                    opaque: true,
                });
                return;
            }
            let (fields, incomplete) = ts_fields_of_object_type(value, src);
            out.push_type(PayloadType {
                name: CompactStr::new(text(name, src)),
                line: line(n),
                fields,
                incomplete,
                opaque: false,
            });
        }
        "class_declaration" | "abstract_class_declaration" | "class" => {
            let Some(name) = n.child_by_field_name("name") else {
                return;
            };
            let (fields, incomplete) = ts_class_fields(n, src);
            out.push_type(PayloadType {
                name: CompactStr::new(text(name, src)),
                line: line(n),
                fields,
                incomplete,
                opaque: false,
            });
        }
        "import_statement" => ts_import(n, src, &mut imports),
        "method_definition" => ts_consumer_method(n, src, &mut out),
        "new_expression" => ts_new_expression(n, src, &mut out),
        "call_expression" => ts_producer_call(n, src, &mut out),
        "public_field_definition" | "required_parameter" => ts_inject_queue(n, src, &mut out),
        _ => {}
    });
    // Only the imports of names a payload site uses are kept.
    let used: std::collections::BTreeSet<&str> = out
        .sites
        .iter()
        .filter_map(|s| match &s.shape {
            PayloadShape::Named(n) => Some(n.as_str()),
            _ => None,
        })
        .collect();
    imports.retain(|i| used.contains(i.local.as_str()));
    out.imports = imports;
    out
}

/// `import A, { B, C as D } from './m'` and `import type { E } from './m'`; a namespace import
/// (`* as ns`) binds no type name.
fn ts_import(n: Node, src: &[u8], out: &mut Vec<PayloadImport>) {
    let Some(module) = n
        .child_by_field_name("source")
        .and_then(|m| string_literal(m, src))
    else {
        return;
    };
    let Some(clause) = named_children(n)
        .into_iter()
        .find(|c| c.kind() == "import_clause")
    else {
        return;
    };
    let mut push = |local: &str, exported: &str| {
        out.push(PayloadImport {
            local: CompactStr::new(local),
            exported: CompactStr::new(exported),
            module: module.clone(),
        });
    };
    for c in named_children(clause) {
        match c.kind() {
            "identifier" => push(text(c, src), "default"),
            "named_imports" => {
                for spec in named_children(c) {
                    if spec.kind() != "import_specifier" {
                        continue;
                    }
                    let Some(name) = spec.child_by_field_name("name") else {
                        continue;
                    };
                    let local = spec.child_by_field_name("alias").unwrap_or(name);
                    let exported = match name.kind() {
                        "string" => string_literal(name, src).unwrap_or_default(),
                        _ => CompactStr::new(text(name, src)),
                    };
                    push(text(local, src), exported.as_str());
                }
            }
            _ => {}
        }
    }
}

// ---------------------------------------------------------------- Java

fn java_annotations<'t>(n: Node<'t>, src: &[u8]) -> Vec<(Node<'t>, String)> {
    let mut out = Vec::new();
    for c in named_children(n) {
        if c.kind() != "modifiers" {
            continue;
        }
        for a in named_children(c) {
            if matches!(a.kind(), "annotation" | "marker_annotation") {
                let name = a
                    .child_by_field_name("name")
                    .map(|x| text(x, src))
                    .unwrap_or("");
                let name = name.rsplit('.').next().unwrap_or(name).to_string();
                out.push((a, name));
            }
        }
    }
    out
}

fn java_is_static(n: Node, src: &[u8]) -> bool {
    named_children(n)
        .into_iter()
        .any(|c| c.kind() == "modifiers" && text(c, src).split_whitespace().any(|w| w == "static"))
}

/// The payload shape of a Java type node.
fn java_type_shape(t: Node, src: &[u8]) -> PayloadShape {
    match t.kind() {
        "type_identifier" | "scoped_type_identifier" => match simple_name(text(t, src)) {
            Some(n) if !matches!(n.as_str(), "String" | "Object" | "JsonNode" | "Map") => {
                PayloadShape::Named(n)
            }
            _ => PayloadShape::Untyped,
        },
        "generic_type" => {
            let kids = named_children(t);
            let outer = kids
                .first()
                .and_then(|o| simple_name(text(*o, src)))
                .unwrap_or_default();
            let args: Vec<Node> = kids
                .iter()
                .find(|k| k.kind() == "type_arguments")
                .map(|a| named_children(*a))
                .unwrap_or_default();
            match outer.as_str() {
                "ConsumerRecord" | "ProducerRecord" => args
                    .get(1)
                    .map_or(PayloadShape::Untyped, |a| java_type_shape(*a, src)),
                "Message" => args
                    .first()
                    .map_or(PayloadShape::Untyped, |a| java_type_shape(*a, src)),
                "" | "List" | "Collection" | "Set" | "Map" | "Optional" => PayloadShape::Untyped,
                _ => PayloadShape::Named(outer),
            }
        }
        _ => PayloadShape::Untyped,
    }
}

fn java_class_fields(body: Node, src: &[u8]) -> (Vec<PayloadField>, bool) {
    let mut fields = Vec::new();
    let mut incomplete = false;
    for m in named_children(body) {
        if m.kind() != "field_declaration" || java_is_static(m, src) {
            continue;
        }
        let ty = m
            .child_by_field_name("type")
            .and_then(|t| clean_type(text(t, src)));
        let mut c = m.walk();
        for d in m.children_by_field_name("declarator", &mut c) {
            if let Some(name) = d.child_by_field_name("name") {
                push_field(
                    &mut fields,
                    &mut incomplete,
                    PayloadField {
                        name: CompactStr::new(text(name, src)),
                        ty: ty.clone(),
                        line: line(m),
                        optional: false,
                    },
                );
            }
        }
    }
    (fields, incomplete)
}

const JAVA_DECL_KINDS: &[&str] = &[
    "method_declaration",
    "constructor_declaration",
    "lambda_expression",
];

/// The declared type of identifier `name` seen from `at`: a parameter or local of the enclosing
/// method, else a field of the enclosing class.
fn java_declared_type<'t>(at: Node<'t>, name: &str, src: &[u8]) -> Option<Node<'t>> {
    if let Some(m) = enclosing_fn(at, JAVA_DECL_KINDS) {
        if let Some(params) = m.child_by_field_name("parameters") {
            for p in named_children(params) {
                if p.child_by_field_name("name")
                    .is_some_and(|n| text(n, src) == name)
                {
                    return p.child_by_field_name("type");
                }
            }
        }
        if let Some(body) = m.child_by_field_name("body") {
            let mut found = None;
            walk(body, &mut |n| {
                if found.is_some()
                    || n.kind() != "local_variable_declaration"
                    || n.start_byte() > at.start_byte()
                {
                    return;
                }
                let mut c = n.walk();
                let hit = n.children_by_field_name("declarator", &mut c).any(|d| {
                    d.child_by_field_name("name")
                        .is_some_and(|x| text(x, src) == name)
                });
                if hit {
                    found = n.child_by_field_name("type");
                }
            });
            if found.is_some() {
                return found;
            }
        }
    }
    let class = enclosing_fn(
        at,
        &[
            "class_declaration",
            "record_declaration",
            "enum_declaration",
        ],
    )?;
    let body = class.child_by_field_name("body")?;
    for m in named_children(body) {
        if m.kind() != "field_declaration" {
            continue;
        }
        let mut c = m.walk();
        let hit = m.children_by_field_name("declarator", &mut c).any(|d| {
            d.child_by_field_name("name")
                .is_some_and(|x| text(x, src) == name)
        });
        if hit {
            return m.child_by_field_name("type");
        }
    }
    None
}

fn java_expr_shape(e: Node, src: &[u8]) -> PayloadShape {
    match e.kind() {
        "object_creation_expression" => e
            .child_by_field_name("type")
            .map_or(PayloadShape::Untyped, |t| java_type_shape(t, src)),
        "cast_expression" => e
            .child_by_field_name("type")
            .map_or(PayloadShape::Untyped, |t| java_type_shape(t, src)),
        "identifier" => java_declared_type(e, text(e, src), src)
            .map_or(PayloadShape::Untyped, |t| java_type_shape(t, src)),
        "parenthesized_expression" => e
            .named_child(0)
            .map_or(PayloadShape::Untyped, |i| java_expr_shape(i, src)),
        _ => PayloadShape::Untyped,
    }
}

fn java_listener(m: Node, src: &[u8], out: &mut PayloadFacts) {
    let annos = java_annotations(m, src);
    let Some((anno, _)) = annos
        .iter()
        .find(|(_, n)| JAVA_LISTENERS.contains(&n.as_str()))
    else {
        return;
    };
    let params = m
        .child_by_field_name("parameters")
        .map(named_children)
        .unwrap_or_default();
    let names = |p: Node| -> Vec<String> {
        java_annotations(p, src)
            .into_iter()
            .map(|(_, n)| n)
            .collect()
    };
    let chosen = params
        .iter()
        .find(|p| names(**p).iter().any(|n| n == "Payload"))
        .or_else(|| {
            params.iter().find(|p| {
                !names(**p).iter().any(|n| n == "Header" || n == "Headers")
                    && !p.child_by_field_name("type").is_some_and(|t| {
                        matches!(
                            text(t, src),
                            "Acknowledgment" | "Channel" | "Consumer" | "MessageHeaders"
                        )
                    })
            })
        });
    let (shape, at) = match chosen {
        Some(p) => (
            p.child_by_field_name("type")
                .map_or(PayloadShape::Untyped, |t| java_type_shape(t, src)),
            line(*p),
        ),
        None => (PayloadShape::Untyped, line(m)),
    };
    // `topics = "x"` or a single positional literal.
    let topic = anno.child_by_field_name("arguments").and_then(|args| {
        let mut lit = None;
        walk(args, &mut |n| {
            if lit.is_none() && n.kind() == "string_literal" {
                lit = string_literal(n, src);
            }
        });
        lit
    });
    out.push_site(PayloadSite {
        producer: false,
        start: line(m),
        end: end_line(m),
        line: at,
        topic,
        shape,
    });
}

fn java_producer_call(n: Node, src: &[u8], out: &mut PayloadFacts) {
    let Some(name) = n.child_by_field_name("name") else {
        return;
    };
    if !JAVA_PRODUCER_METHODS.contains(&text(name, src)) {
        return;
    }
    let args: Vec<Node> = n
        .child_by_field_name("arguments")
        .map(named_children)
        .unwrap_or_default()
        .into_iter()
        .filter(|a| !matches!(a.kind(), "line_comment" | "block_comment"))
        .collect();
    let (Some(first), Some(payload)) = (args.first(), args.last()) else {
        return;
    };
    if args.len() < 2 {
        return;
    }
    let mut shape = java_expr_shape(*payload, src);
    if shape == PayloadShape::Untyped {
        // `kafkaTemplate.send(topic, x)` with `KafkaTemplate<K, V> kafkaTemplate`: the `V`.
        let receiver = n.child_by_field_name("object").map(|o| {
            if o.kind() == "field_access" {
                o.child_by_field_name("field").unwrap_or(o)
            } else {
                o
            }
        });
        if let Some(r) = receiver.filter(|r| r.kind() == "identifier") {
            if let Some(t) =
                java_declared_type(n, text(r, src), src).filter(|t| t.kind() == "generic_type")
            {
                let kids = named_children(t);
                let is_template = kids
                    .first()
                    .is_some_and(|o| text(*o, src).ends_with("KafkaTemplate"));
                if is_template {
                    if let Some(v) = kids
                        .iter()
                        .find(|k| k.kind() == "type_arguments")
                        .and_then(|a| named_children(*a).get(1).copied())
                    {
                        shape = java_type_shape(v, src);
                    }
                }
            }
        }
    }
    out.push_site(PayloadSite {
        producer: true,
        start: line(n),
        end: end_line(n),
        line: line(*payload),
        topic: string_literal(*first, src),
        shape,
    });
}

/// The payload facts of a Java tree.
pub(crate) fn scan_java(tree: &Tree, src: &[u8]) -> PayloadFacts {
    let mut out = PayloadFacts::default();
    walk(tree.root_node(), &mut |n| match n.kind() {
        "class_declaration" => {
            let (Some(name), Some(body)) =
                (n.child_by_field_name("name"), n.child_by_field_name("body"))
            else {
                return;
            };
            let (fields, mut incomplete) = java_class_fields(body, src);
            incomplete |= n.child_by_field_name("superclass").is_some();
            out.push_type(PayloadType {
                name: CompactStr::new(text(name, src)),
                line: line(n),
                fields,
                incomplete,
                opaque: false,
            });
        }
        "record_declaration" => {
            let Some(name) = n.child_by_field_name("name") else {
                return;
            };
            let mut fields = Vec::new();
            let mut incomplete = false;
            if let Some(params) = n.child_by_field_name("parameters") {
                for p in named_children(params) {
                    if let Some(pn) = p.child_by_field_name("name") {
                        push_field(
                            &mut fields,
                            &mut incomplete,
                            PayloadField {
                                name: CompactStr::new(text(pn, src)),
                                ty: p
                                    .child_by_field_name("type")
                                    .and_then(|t| clean_type(text(t, src))),
                                line: line(p),
                                optional: false,
                            },
                        );
                    }
                }
            }
            out.push_type(PayloadType {
                name: CompactStr::new(text(name, src)),
                line: line(n),
                fields,
                incomplete,
                opaque: false,
            });
        }
        "method_declaration" => java_listener(n, src, &mut out),
        "method_invocation" => java_producer_call(n, src, &mut out),
        _ => {}
    });
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(lang: tree_sitter::Language, code: &str) -> Tree {
        let mut p = tree_sitter::Parser::new();
        p.set_language(&lang).expect("grammar");
        p.parse(code, None).expect("tree")
    }

    fn ts(code: &str) -> PayloadFacts {
        let tree = parse(tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into(), code);
        scan_ts(&tree, code.as_bytes())
    }

    fn java(code: &str) -> PayloadFacts {
        let tree = parse(tree_sitter_java::LANGUAGE.into(), code);
        scan_java(&tree, code.as_bytes())
    }

    fn named(n: &str) -> PayloadShape {
        PayloadShape::Named(CompactStr::new(n))
    }

    /// `(producer, line, topic, shape)` of every site.
    fn sites(f: &PayloadFacts) -> Vec<(bool, u32, Option<&str>, &PayloadShape)> {
        f.sites
            .iter()
            .map(|s| (s.producer, s.line, s.topic.as_deref(), &s.shape))
            .collect()
    }

    fn field_names(t: &PayloadType) -> Vec<(&str, Option<&str>, bool)> {
        t.fields
            .iter()
            .map(|f| (f.name.as_str(), f.ty.as_deref(), f.optional))
            .collect()
    }

    #[test]
    fn ts_declared_types_keep_public_fields_and_their_types() {
        let f = ts("interface A { id: string; n?: number; kind: 'x' }\n\
             interface B extends A { more: string }\n\
             type C = { tags: string[] };\n\
             type D = string;\n\
             class E { a: string; private hidden: string; static s: number; constructor(public readonly b: number, c: string, private d: string) {} }\n");
        let names: Vec<&str> = f.types.iter().map(|t| t.name.as_str()).collect();
        assert_eq!(names, vec!["A", "B", "C", "D", "E"]);
        // A non-object alias is declared, its fields unknown: it shadows a same-named type.
        assert!(f.types[3].opaque && f.types[3].fields.is_empty());
        assert!(!f.types[0].opaque && !f.types[4].opaque);
        // A literal type is never kept as a type: it could hold a value.
        assert_eq!(
            field_names(&f.types[0]),
            vec![
                ("id", Some("string"), false),
                ("n", Some("number"), true),
                ("kind", None, false)
            ]
        );
        assert!(!f.types[0].incomplete);
        assert!(f.types[1].incomplete, "extends: fields of A are not read");
        assert_eq!(
            field_names(&f.types[2]),
            vec![("tags", Some("string[]"), false)]
        );
        assert_eq!(
            field_names(&f.types[4]),
            vec![("a", Some("string"), false), ("b", Some("number"), false)]
        );
        assert!(f.sites.is_empty());
    }

    #[test]
    fn ts_consumer_sites_read_the_payload_parameter() {
        let f = ts("class H {\n\
             @EventPattern('t') on(@Payload() d: A, @Ctx() c: any) {}\n\
             @MessagePattern('m') first(@Ctx() c: any, raw: B) {}\n\
             @OnEvent('o') inline(e: { id: string }) {}\n\
             @Get('x') route(d: A) {}\n\
             plain(d: A) {}\n\
             async process(job: Job<C>) {}\n\
             @EventPattern('u') untyped(e: any) {}\n\
             }\n");
        assert_eq!(
            sites(&f),
            vec![
                (false, 2, Some("t"), &named("A")),
                (false, 3, Some("m"), &named("B")),
                (
                    false,
                    4,
                    Some("o"),
                    &PayloadShape::Inline {
                        fields: vec![PayloadField {
                            name: CompactStr::new("id"),
                            ty: Some(CompactStr::new("string")),
                            line: 4,
                            optional: false
                        }],
                        incomplete: false
                    }
                ),
                (false, 7, None, &named("C")),
                (false, 8, Some("u"), &PayloadShape::Untyped),
            ]
        );
    }

    #[test]
    fn ts_producer_sites_follow_the_payload_expression() {
        let f = ts("function go(p: A, s: string) {\n\
             const v: B = x;\n\
             const w = new C();\n\
             client.emit('a', p);\n\
             client.emit('b', v);\n\
             client.emit('c', w);\n\
             client.emit('d', { id, n: 1, ok: true, s: 'v' });\n\
             client.emit('e', { ...rest, id });\n\
             client.emit('f', JSON.stringify(p));\n\
             queue.add('job', s as D);\n\
             set.add(p);\n\
             client.emit('g', s);\n\
             }\n\
             const q1 = new Queue<E>('q1');\n\
             const q2 = new Queue('q2');\n\
             const w1 = new Worker<F>('w1', async (job) => {});\n\
             const w2 = new Worker('w2', async (job: Job<G>) => {});\n");
        let shapes: Vec<(bool, Option<&str>, String)> = f
            .sites
            .iter()
            .map(|s| {
                let shape = match &s.shape {
                    PayloadShape::Named(n) => n.to_string(),
                    PayloadShape::Untyped => "untyped".to_string(),
                    PayloadShape::Inline { fields, incomplete } => format!(
                        "inline {}{}",
                        fields
                            .iter()
                            .map(|f| format!("{}:{}", f.name, f.ty.as_deref().unwrap_or("?")))
                            .collect::<Vec<_>>()
                            .join(","),
                        if *incomplete { " incomplete" } else { "" }
                    ),
                };
                (s.producer, s.topic.as_deref(), shape)
            })
            .collect();
        assert_eq!(
            shapes,
            vec![
                (true, Some("a"), "A".to_string()),
                (true, Some("b"), "B".to_string()),
                (true, Some("c"), "C".to_string()),
                (
                    true,
                    Some("d"),
                    "inline id:?,n:number,ok:boolean,s:string".to_string()
                ),
                (true, Some("e"), "inline id:? incomplete".to_string()),
                (true, Some("f"), "untyped".to_string()),
                (true, Some("job"), "D".to_string()),
                (true, Some("g"), "untyped".to_string()),
                (true, Some("q1"), "E".to_string()),
                (false, Some("w1"), "F".to_string()),
                (false, Some("w2"), "G".to_string()),
            ]
        );
    }

    #[test]
    fn ts_inject_queue_generic_is_a_producer_payload() {
        let f = ts("class S { constructor(@InjectQueue('mail') private readonly q: Queue<Mail>, @InjectQueue('raw') private readonly r: Queue) {} }\n");
        assert_eq!(sites(&f), vec![(true, 1, Some("mail"), &named("Mail"))]);
    }

    #[test]
    fn java_types_and_sites() {
        let f = java("class P {\n\
             private final KafkaTemplate<String, Ev> t;\n\
             private static int K;\n\
             String a; List<Integer> b;\n\
             @KafkaListener(topics = \"x\") void on(@Header(\"h\") String h, @Payload Ev e) {}\n\
             @KafkaListener(topics = \"y\") void rec(ConsumerRecord<String, Ev> r, Acknowledgment ack) {}\n\
             @RabbitListener(queues = \"z\") void raw(String body) {}\n\
             void notAListener(Ev e) {}\n\
             void go(Ev p, String s) {\n\
               Ev v = new Ev();\n\
               t.send(\"x\", new Ev(1));\n\
               t.send(\"x\", p);\n\
               t.send(\"x\", v);\n\
               t.send(\"x\", build(s));\n\
               rabbit.convertAndSend(\"ex\", \"key\", s);\n\
             }\n\
             }\n\
             record Ev(String id, long n) {}\n");
        let names: Vec<&str> = f.types.iter().map(|t| t.name.as_str()).collect();
        assert_eq!(names, vec!["P", "Ev"]);
        assert_eq!(
            field_names(&f.types[0]),
            vec![
                ("t", Some("KafkaTemplate<String, Ev>"), false),
                ("a", Some("String"), false),
                ("b", Some("List<Integer>"), false)
            ]
        );
        assert_eq!(
            field_names(&f.types[1]),
            vec![("id", Some("String"), false), ("n", Some("long"), false)]
        );
        let ev = named("Ev");
        assert_eq!(
            sites(&f),
            vec![
                (false, 5, Some("x"), &ev),
                (false, 6, Some("y"), &ev),
                (false, 7, Some("z"), &PayloadShape::Untyped),
                (true, 11, Some("x"), &ev),
                (true, 12, Some("x"), &ev),
                (true, 13, Some("x"), &ev),
                // A method result: the `V` of the `KafkaTemplate<String, Ev>` receiver.
                (true, 14, Some("x"), &ev),
                (true, 15, Some("ex"), &PayloadShape::Untyped),
            ]
        );
    }

    #[test]
    fn ts_quoted_keys_are_bare_field_names() {
        let f = ts(
            "interface Q { 'orderId': string; \"total\": number; [k]: string }\n\
             class R { 'a': string; [computed] = 1; }\n",
        );
        assert_eq!(
            field_names(&f.types[0]),
            vec![
                ("orderId", Some("string"), false),
                ("total", Some("number"), false)
            ]
        );
        assert!(f.types[0].incomplete, "a computed key leaves the list open");
        assert_eq!(field_names(&f.types[1]), vec![("a", Some("string"), false)]);
    }

    #[test]
    fn ts_imports_of_payload_type_names_are_kept() {
        let f = ts("import Def, { A, B as Local, Unused } from './m';\n\
             import * as ns from './ns';\n\
             import type { T } from '@pkg/types';\n\
             class H {\n\
             @EventPattern('a') a(@Payload() x: A) {}\n\
             @EventPattern('b') b(@Payload() x: Local) {}\n\
             @EventPattern('c') c(@Payload() x: Def) {}\n\
             @EventPattern('d') d(@Payload() x: T) {}\n\
             }\n");
        let imports: Vec<(&str, &str, &str)> = f
            .imports
            .iter()
            .map(|i| (i.local.as_str(), i.exported.as_str(), i.module.as_str()))
            .collect();
        assert_eq!(
            imports,
            vec![
                ("Def", "default", "./m"),
                ("A", "A", "./m"),
                ("Local", "B", "./m"),
                ("T", "T", "@pkg/types"),
            ]
        );
    }

    #[test]
    fn a_type_past_the_cap_keeps_its_name_only() {
        let decls: String = (0..260)
            .map(|i| format!("interface T{i} {{ a: string }}\n"))
            .collect();
        let f = ts(&decls);
        assert_eq!(f.types.len(), 260);
        assert!(f.capped);
        assert!(!f.types[255].opaque);
        assert!(f.types[256].opaque && f.types[256].fields.is_empty());
        assert_eq!(f.types[259].name.as_str(), "T259");
    }

    #[test]
    fn caps_mark_a_type_incomplete() {
        let fields: String = (0..70).map(|i| format!("f{i}: string; ")).collect();
        let f = ts(&format!("interface Big {{ {fields} }}\n"));
        assert_eq!(f.types[0].fields.len(), 64);
        assert!(f.types[0].incomplete);
    }
}
