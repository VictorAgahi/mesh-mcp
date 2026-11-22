//! Message brokers in Python (9.0 ticket T8c).
//!
//! The Python extractor reads Kafka (`confluent_kafka`, `kafka-python`, `aiokafka`) and Celery by
//! the shape of the call. This pass adds the other brokers a Python service talks to, each
//! recorded under its own [`Broker`] so a queue named like a Kafka topic is a different channel
//! (T7):
//!
//! | broker | consumers | producers |
//! |---|---|---|
//! | `rabbitmq` | pika `basic_consume` / `basic_get` / `consume`; aio-pika `queue.consume` / `iterator`; kombu `Consumer(queues=...)` | pika `basic_publish`; aio-pika `exchange.publish`; kombu `Producer.publish` |
//! | `redis` | redis-py `pubsub.subscribe` / `psubscribe` | `Redis.publish` |
//! | `nats` | nats-py `subscribe`, JetStream `subscribe` / `pull_subscribe` | `publish`, `request` |
//! | `sqs` | boto3 `receive_message(QueueUrl=...)` | `send_message`, `send_message_batch` |
//! | `sns` | none | `publish`, `publish_batch` (`TopicArn=...`) |
//! | `pubsub` | `SubscriberClient.subscribe(subscription)` | `PublisherClient.publish(topic)` |
//! | `kafka` | the gaps of the shape-based reader: `KafkaConsumer("t")`, `subscribe(topics=[...])` | none new |
//!
//! Only what a call site shows is registered: a name is a string literal, a string constant of
//! the file (module, class or `self` attribute assigned once), a keyword argument literal, or a
//! `os.environ` / `os.getenv` read (a `${KEY}` placeholder resolved when the index is folded).
//! A variable, a parameter, a format string or a result of another call registers nothing: no
//! name is ever guessed from an identifier, and a default of `os.getenv("K", "d")` is dropped
//! (the site stays a visible `${K}`).
//!
//! A call is read only when its receiver is a value built from the library: a name assigned from
//! a call on an imported module or class (`pika.BlockingConnection(...).channel()`,
//! `redis.Redis(...)`, `boto3.client("sqs")`), a `with` target, a `self.x` attribute assigned in
//! its class, or a parameter annotated with the library's type, in a file whose import
//! statements (never a comment or a string) bring the library in. A name declared twice with
//! different meanings (a parameter, a loop variable, a lambda parameter, a local class of the
//! same name) is ambiguous and read as nothing, as is a receiver that is a plain parameter.
//!
//! A call this pass recognises is *claimed*: the shape-based Kafka reader skips it when it is a
//! broker other than Kafka (a `redis.publish("x")` is no Kafka topic) and tags what it reads
//! with `kafka` when it is one. A call on an instance of a class the file declares is claimed
//! too, so `bus.publish("x")` of the file's own `Bus` is nobody's producer.
//!
//! Bounds: the walk is iterative; scope tables are built once per scope; evaluating a receiver
//! follows at most [`MAX_EVAL_DEPTH`] links; sites past [`MAX_PY_BROKER_SITES_PER_FILE`] are
//! dropped and the file is counted in `IndexHealth::files_py_brokers_capped`.

use super::python::PythonExtractor;
use super::{FileIndex, MAX_PY_BROKER_SITES_PER_FILE};
use mesh_core::{Broker, CompactStr, ContractNode, NodeKind};
use std::collections::HashMap;
use tree_sitter::{Node, Tree};

/// Bindings a file's scope tables remember; past it the pass stops reading (and says so).
const MAX_BINDINGS: usize = 32_768;
/// Links of an attribute / call chain one receiver evaluation follows.
const MAX_EVAL_DEPTH: usize = 8;
/// Links of a dotted import path one name resolution follows.
const MAX_QUAL_DEPTH: usize = 12;
/// Elements of a `queues=[...]` / `topics=[...]` list one call reads.
const MAX_LIST: usize = 64;

/// What a value is, for the clients this pass reads.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Kind {
    PikaConn,
    PikaChannel,
    AioConn,
    AioChannel,
    AioExchange,
    AioQueue,
    KombuConn,
    KombuProducer,
    KombuQueue,
    RedisClient,
    RedisPubSub,
    NatsClient,
    NatsJs,
    AwsSession,
    SqsClient,
    SnsClient,
    Publisher,
    Subscriber,
    TopicPath,
    SubPath,
    KafkaProducer,
    KafkaConsumer,
    /// An instance of a class the file declares.
    LocalClass,
}

/// A value of a [`Kind`], with the destination name it was made from when it shows one (an
/// aio-pika queue, a Pub/Sub path, a kombu queue).
#[derive(Clone, PartialEq, Eq, Debug)]
struct Value {
    kind: Kind,
    name: Option<String>,
}

fn val(kind: Kind) -> Option<Value> {
    Some(Value { kind, name: None })
}

/// What the shape-based reader must do with a call this pass recognised.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Claim {
    /// A client of `Broker`: Kafka keeps the shape-based reading (tagged), any other broker is
    /// read here only.
    Broker(Broker),
    /// A method of a class the file declares: nobody's producer.
    Foreign,
}

/// One broker fact before it becomes a node.
#[derive(Debug)]
struct Site {
    producer: bool,
    broker: Broker,
    key: String,
    lines: (usize, usize),
    via: &'static str,
}

/// The broker facts of a Python file, read off its tree.
#[derive(Debug, Default)]
pub(crate) struct Scan {
    sites: Vec<Site>,
    claims: HashMap<(usize, usize), Claim>,
    capped: bool,
}

impl Scan {
    /// The claim on `call`, if this pass recognised it.
    pub(crate) fn claim(&self, call: Node) -> Option<Claim> {
        self.claims
            .get(&(call.start_byte(), call.end_byte()))
            .copied()
    }
}

/// What a name is bound to in one scope.
#[derive(Clone, Debug)]
enum Bound<'t> {
    /// An import: the qualified path the name stands for.
    Import(String),
    /// An assignment (or `with` / walrus target): the value expression.
    Expr(Node<'t>),
    /// A parameter or variable annotated with a type.
    Annot(Node<'t>),
    /// A class the file declares.
    Class,
    /// Anything else (a parameter, a loop variable, a function, a pattern target).
    Other,
}

type Table<'t> = HashMap<String, Vec<Bound<'t>>>;

/// What a file must mention at all before the precise scan runs: a library, or one of the method
/// names the shape-based Kafka reader takes (a class of the file with such a method is claimed).
const PREFILTER: &[&str] = &[
    "pika",
    "kombu",
    "redis",
    "nats",
    "boto3",
    "pubsub",
    "kafka",
    ".publish(",
    ".subscribe(",
    ".send(",
    ".produce(",
    ".send_and_wait(",
];

/// Methods a call is read for at all, so the receiver evaluation runs on few calls.
const CALL_NAMES: &[&str] = &[
    "basic_publish",
    "basic_consume",
    "basic_get",
    "consume",
    "iterator",
    "publish",
    "publish_batch",
    "Consumer",
    "subscribe",
    "psubscribe",
    "pull_subscribe",
    "request",
    "send_message",
    "send_message_batch",
    "receive_message",
    "produce",
    "send",
    "send_and_wait",
];

/// Per-file state the scan evaluates receivers and names against.
struct Ctx<'t> {
    source: &'t [u8],
    /// Names bound in each scope, by scope node id.
    tables: HashMap<usize, Table<'t>>,
    /// `self.x` / `cls.x` assignments of each class, by class node id.
    attrs: HashMap<usize, Table<'t>>,
    bindings: usize,
    capped: bool,
}

fn text<'s>(node: Node, source: &'s [u8]) -> &'s str {
    node.utf8_text(source).unwrap_or("")
}

fn is_scope(kind: &str) -> bool {
    matches!(
        kind,
        "module"
            | "function_definition"
            | "class_definition"
            | "lambda"
            | "list_comprehension"
            | "set_comprehension"
            | "dictionary_comprehension"
            | "generator_expression"
    )
}

/// The arguments of a call: positional ones (unusable once a `*args` precedes them) and keywords.
struct Args<'t> {
    pos: Vec<Node<'t>>,
    kw: Vec<(String, Node<'t>)>,
    splat: bool,
}

impl<'t> Args<'t> {
    fn of(call: Node<'t>, source: &[u8]) -> Option<Args<'t>> {
        let list = call.child_by_field_name("arguments")?;
        if list.kind() != "argument_list" {
            return None;
        }
        let mut out = Args {
            pos: Vec::new(),
            kw: Vec::new(),
            splat: false,
        };
        let mut cursor = list.walk();
        for a in list.named_children(&mut cursor) {
            match a.kind() {
                "comment" => {}
                "keyword_argument" => {
                    if let (Some(n), Some(v)) = (
                        a.child_by_field_name("name"),
                        a.child_by_field_name("value"),
                    ) {
                        out.kw.push((text(n, source).to_string(), v));
                    }
                }
                "list_splat" | "dictionary_splat" => out.splat = true,
                _ => {
                    if !out.splat {
                        out.pos.push(a);
                    }
                }
            }
        }
        Some(out)
    }

    /// The argument named `name`, else the positional one at `at` (when given).
    fn get(&self, at: Option<usize>, name: &str) -> Option<Node<'t>> {
        if let Some((_, v)) = self.kw.iter().find(|(k, _)| k == name) {
            return Some(*v);
        }
        at.and_then(|i| self.pos.get(i).copied())
    }
}

impl<'t> Ctx<'t> {
    fn new(source: &'t [u8]) -> Ctx<'t> {
        Ctx {
            source,
            tables: HashMap::new(),
            attrs: HashMap::new(),
            bindings: 0,
            capped: false,
        }
    }

    fn add(&mut self, table: &mut Table<'t>, name: &str, bound: Bound<'t>) {
        if self.bindings >= MAX_BINDINGS {
            self.capped = true;
            return;
        }
        self.bindings += 1;
        table.entry(name.to_string()).or_default().push(bound);
    }

    /// Binds every identifier of an assignment / loop / `with` target pattern to `Other`.
    fn bind_pattern(&mut self, table: &mut Table<'t>, target: Node<'t>) {
        let mut stack = vec![target];
        while let Some(n) = stack.pop() {
            match n.kind() {
                "identifier" => self.add(table, text(n, self.source), Bound::Other),
                "attribute" | "subscript" => {}
                _ => {
                    let mut c = n.walk();
                    stack.extend(n.named_children(&mut c));
                }
            }
        }
    }

    fn bind_params(&mut self, table: &mut Table<'t>, params: Node<'t>) {
        let mut cursor = params.walk();
        let list: Vec<Node> = params.named_children(&mut cursor).collect();
        for p in list {
            match p.kind() {
                "identifier" => self.add(table, text(p, self.source), Bound::Other),
                "typed_parameter" => {
                    let name = p.named_child(0).filter(|n| n.kind() == "identifier");
                    match (name, p.child_by_field_name("type")) {
                        (Some(n), Some(t)) => {
                            self.add(table, text(n, self.source), Bound::Annot(t))
                        }
                        (Some(n), None) => self.add(table, text(n, self.source), Bound::Other),
                        _ => {}
                    }
                }
                "typed_default_parameter" => {
                    if let Some(n) = p.child_by_field_name("name") {
                        let bound = p
                            .child_by_field_name("type")
                            .map_or(Bound::Other, Bound::Annot);
                        self.add(table, text(n, self.source), bound);
                    }
                }
                "default_parameter" => {
                    if let Some(n) = p.child_by_field_name("name") {
                        self.add(table, text(n, self.source), Bound::Other);
                    }
                }
                _ => self.bind_pattern(table, p),
            }
        }
    }

    /// The `import` statements' bindings.
    fn bind_import(&mut self, table: &mut Table<'t>, stmt: Node<'t>) {
        let source = self.source;
        let dotted = |n: Node| -> String { text(n, source).split_whitespace().collect() };
        let mut cursor = stmt.walk();
        if stmt.kind() == "import_statement" {
            let names: Vec<Node> = stmt.children_by_field_name("name", &mut cursor).collect();
            for n in names {
                match n.kind() {
                    "dotted_name" => {
                        let full = dotted(n);
                        let first = full.split('.').next().unwrap_or("").to_string();
                        self.add(table, &first, Bound::Import(first.clone()));
                    }
                    "aliased_import" => {
                        if let (Some(name), Some(alias)) = (
                            n.child_by_field_name("name"),
                            n.child_by_field_name("alias"),
                        ) {
                            self.add(table, text(alias, source), Bound::Import(dotted(name)));
                        }
                    }
                    _ => {}
                }
            }
            return;
        }
        let Some(module) = stmt.child_by_field_name("module_name") else {
            return;
        };
        let module = dotted(module);
        let names: Vec<Node> = stmt.children_by_field_name("name", &mut cursor).collect();
        for n in names {
            let (name, local) = match n.kind() {
                "dotted_name" => (dotted(n), dotted(n)),
                "aliased_import" => match (
                    n.child_by_field_name("name"),
                    n.child_by_field_name("alias"),
                ) {
                    (Some(a), Some(b)) => (dotted(a), text(b, source).to_string()),
                    _ => continue,
                },
                _ => continue,
            };
            // A relative import (`from . import x`) is the project's own: never a library.
            let path = format!("{module}.{name}");
            self.add(table, &local, Bound::Import(path));
        }
    }

    /// The names a scope binds. Iterative; nested scopes bind only their own name here.
    fn build(&mut self, scope: Node<'t>) -> Table<'t> {
        let mut table = Table::new();
        let mut stack: Vec<Node<'t>> = Vec::new();
        match scope.kind() {
            "function_definition" => {
                if let Some(p) = scope.child_by_field_name("parameters") {
                    self.bind_params(&mut table, p);
                }
                stack.extend(scope.child_by_field_name("body"));
            }
            "lambda" => {
                if let Some(p) = scope.child_by_field_name("parameters") {
                    self.bind_params(&mut table, p);
                }
            }
            "list_comprehension"
            | "set_comprehension"
            | "dictionary_comprehension"
            | "generator_expression" => {
                let mut c = scope.walk();
                let clauses: Vec<Node> = scope
                    .named_children(&mut c)
                    .filter(|x| x.kind() == "for_in_clause")
                    .collect();
                for clause in clauses {
                    if let Some(l) = clause.child_by_field_name("left") {
                        self.bind_pattern(&mut table, l);
                    }
                }
            }
            _ => {
                let mut c = scope.walk();
                stack.extend(scope.named_children(&mut c));
            }
        }
        while let Some(n) = stack.pop() {
            match n.kind() {
                "function_definition" => {
                    if let Some(name) = n.child_by_field_name("name") {
                        self.add(&mut table, text(name, self.source), Bound::Other);
                    }
                }
                "class_definition" => {
                    if let Some(name) = n.child_by_field_name("name") {
                        self.add(&mut table, text(name, self.source), Bound::Class);
                    }
                }
                "lambda"
                | "list_comprehension"
                | "set_comprehension"
                | "dictionary_comprehension"
                | "generator_expression" => {}
                "import_statement" | "import_from_statement" => {
                    self.bind_import(&mut table, n);
                }
                "assignment" => {
                    let (left, right) = (
                        n.child_by_field_name("left"),
                        n.child_by_field_name("right"),
                    );
                    let ty = n.child_by_field_name("type");
                    if let Some(l) = left {
                        if l.kind() == "identifier" {
                            let bound = match (right, ty) {
                                (Some(r), _) => Bound::Expr(r),
                                (None, Some(t)) => Bound::Annot(t),
                                (None, None) => Bound::Other,
                            };
                            self.add(&mut table, text(l, self.source), bound);
                        } else {
                            self.bind_pattern(&mut table, l);
                        }
                    }
                    // `a = b = expr`: the inner assignment binds `b`.
                    stack.extend(right.filter(|r| r.kind() == "assignment"));
                }
                "augmented_assignment" => {
                    if let Some(l) = n.child_by_field_name("left") {
                        self.bind_pattern(&mut table, l);
                    }
                }
                "named_expression" => {
                    if let (Some(name), Some(v)) = (
                        n.child_by_field_name("name"),
                        n.child_by_field_name("value"),
                    ) {
                        self.add(&mut table, text(name, self.source), Bound::Expr(v));
                    }
                    let mut c = n.walk();
                    stack.extend(n.named_children(&mut c));
                }
                "for_statement" => {
                    if let Some(l) = n.child_by_field_name("left") {
                        self.bind_pattern(&mut table, l);
                    }
                    let mut c = n.walk();
                    stack.extend(n.named_children(&mut c));
                }
                "with_item" => {
                    if let Some(v) = n.child_by_field_name("value") {
                        if v.kind() == "as_pattern" {
                            let inner = v.named_child(0);
                            let alias = v
                                .child_by_field_name("alias")
                                .and_then(|a| a.named_child(0));
                            match (inner, alias) {
                                (Some(i), Some(a)) if a.kind() == "identifier" => {
                                    self.add(&mut table, text(a, self.source), Bound::Expr(i));
                                }
                                (_, Some(a)) => self.bind_pattern(&mut table, a),
                                _ => {}
                            }
                        }
                    }
                    let mut c = n.walk();
                    stack.extend(n.named_children(&mut c));
                }
                "except_clause" => {
                    if let Some(v) = n.child_by_field_name("value") {
                        if v.kind() == "as_pattern" {
                            if let Some(a) = v
                                .child_by_field_name("alias")
                                .and_then(|a| a.named_child(0))
                            {
                                self.bind_pattern(&mut table, a);
                            }
                        }
                    }
                    let mut c = n.walk();
                    stack.extend(n.named_children(&mut c));
                }
                _ => {
                    let mut c = n.walk();
                    stack.extend(n.named_children(&mut c));
                }
            }
        }
        table
    }

    fn table(&mut self, scope: Node<'t>) -> &Table<'t> {
        if !self.tables.contains_key(&scope.id()) {
            let t = self.build(scope);
            self.tables.insert(scope.id(), t);
        }
        &self.tables[&scope.id()]
    }

    /// What `name` read at `at` is bound to, innermost scope first (a class scope is visible
    /// only from its own body, never from a method). Empty when nothing in the file binds it.
    fn bounds(&mut self, at: Node<'t>, name: &str) -> Vec<Bound<'t>> {
        let mut in_function = false;
        let mut cur = Some(at);
        while let Some(p) = cur {
            let kind = p.kind();
            if is_scope(kind) && !(kind == "class_definition" && in_function) {
                if let Some(b) = self.table(p).get(name) {
                    return b.clone();
                }
            }
            if is_scope(kind) && kind != "class_definition" && kind != "module" {
                in_function = true;
            }
            cur = p.parent();
        }
        Vec::new()
    }

    /// The `self.x` assignments of the class around `at`, with its class-level names.
    fn class_attr(&mut self, at: Node<'t>, attr: &str) -> Vec<Bound<'t>> {
        let mut cur = at.parent();
        let class = loop {
            match cur {
                Some(p) if p.kind() == "class_definition" => break p,
                Some(p) => cur = p.parent(),
                None => return Vec::new(),
            }
        };
        if !self.attrs.contains_key(&class.id()) {
            let mut table = Table::new();
            let mut stack: Vec<Node<'t>> = Vec::new();
            stack.extend(class.child_by_field_name("body"));
            while let Some(n) = stack.pop() {
                if n.kind() == "class_definition" {
                    continue;
                }
                if n.kind() == "assignment" {
                    if let Some(l) = n.child_by_field_name("left") {
                        if l.kind() == "attribute"
                            && l.child_by_field_name("object")
                                .is_some_and(|o| matches!(text(o, self.source), "self" | "cls"))
                        {
                            if let (Some(a), Some(r)) = (
                                l.child_by_field_name("attribute"),
                                n.child_by_field_name("right"),
                            ) {
                                self.add(&mut table, text(a, self.source), Bound::Expr(r));
                            }
                        }
                    }
                }
                let mut c = n.walk();
                stack.extend(n.named_children(&mut c));
            }
            self.attrs.insert(class.id(), table);
        }
        let mut out = self
            .attrs
            .get(&class.id())
            .and_then(|t| t.get(attr))
            .cloned()
            .unwrap_or_default();
        out.extend(self.table(class).get(attr).cloned().unwrap_or_default());
        out
    }

    /// The qualified path an identifier / attribute chain stands for through the imports.
    fn qual(&mut self, node: Node<'t>) -> Option<String> {
        self.qual_at(node, 0)
    }

    /// [`Self::qual`] at chain depth `depth`: a dotted path of a library is a few links long, a
    /// longer chain is read as nothing, whatever the file nests.
    fn qual_at(&mut self, node: Node<'t>, depth: usize) -> Option<String> {
        if depth > MAX_QUAL_DEPTH {
            return None;
        }
        match node.kind() {
            "identifier" => {
                let bounds = self.bounds(node, text(node, self.source));
                let mut path: Option<String> = None;
                for b in &bounds {
                    let Bound::Import(p) = b else {
                        return None;
                    };
                    match &path {
                        Some(prev) if prev != p => return None,
                        _ => path = Some(p.clone()),
                    }
                }
                path
            }
            "attribute" => {
                let object = self.qual_at(node.child_by_field_name("object")?, depth + 1)?;
                let attr = text(node.child_by_field_name("attribute")?, self.source);
                Some(format!("{object}.{attr}"))
            }
            _ => None,
        }
    }

    /// The single value every binding of a name gives, or `None` when they differ.
    fn fold<T: PartialEq>(
        &mut self,
        bounds: Vec<Bound<'t>>,
        depth: usize,
        mut one: impl FnMut(&mut Self, &Bound<'t>, usize) -> Option<T>,
    ) -> Option<T> {
        let mut out: Option<T> = None;
        for b in &bounds {
            let v = one(self, b, depth)?;
            match &out {
                Some(prev) if *prev != v => return None,
                _ => out = Some(v),
            }
        }
        out
    }

    /// What `node` is, when it is one of the clients this pass reads.
    fn eval(&mut self, node: Node<'t>, depth: usize) -> Option<Value> {
        if depth > MAX_EVAL_DEPTH || self.capped {
            return None;
        }
        match node.kind() {
            "parenthesized_expression" | "await" => self.eval(node.named_child(0)?, depth + 1),
            "assignment" => self.eval(node.child_by_field_name("right")?, depth + 1),
            "identifier" => {
                let bounds = self.bounds(node, text(node, self.source));
                self.fold(bounds, depth, Self::eval_bound)
            }
            "attribute" => {
                let object = node.child_by_field_name("object")?;
                let attr = text(node.child_by_field_name("attribute")?, self.source);
                if object.kind() == "identifier"
                    && matches!(text(object, self.source), "self" | "cls")
                {
                    let bounds = self.class_attr(node, attr);
                    return self.fold(bounds, depth, Self::eval_bound);
                }
                let v = self.eval(object, depth + 1)?;
                match (v.kind, attr) {
                    (Kind::AioChannel, "default_exchange") => val(Kind::AioExchange),
                    _ => None,
                }
            }
            "call" => self.eval_call(node, depth),
            "type" => {
                let path = self.qual(node.named_child(0)?)?;
                annotation(&path)
            }
            _ => None,
        }
    }

    fn eval_bound(&mut self, b: &Bound<'t>, depth: usize) -> Option<Value> {
        match b {
            Bound::Expr(e) => self.eval(*e, depth + 1),
            Bound::Annot(t) => self.eval(*t, depth + 1),
            Bound::Class => val(Kind::LocalClass),
            Bound::Import(_) | Bound::Other => None,
        }
    }

    fn eval_call(&mut self, call: Node<'t>, depth: usize) -> Option<Value> {
        let f = call.child_by_field_name("function")?;
        let args = Args::of(call, self.source);
        if f.kind() == "attribute" {
            let object = f.child_by_field_name("object")?;
            let method = text(f.child_by_field_name("attribute")?, self.source);
            if let Some(v) = self.eval(object, depth + 1) {
                return self.method_result(&v, method, args.as_ref());
            }
        }
        if let Some(path) = self.qual(f) {
            return self.factory(&path, args.as_ref());
        }
        if f.kind() == "identifier" {
            let bounds = self.bounds(f, text(f, self.source));
            if !bounds.is_empty() && bounds.iter().all(|b| matches!(b, Bound::Class)) {
                return val(Kind::LocalClass);
            }
        }
        None
    }

    /// What `path(args)` builds, for the constructors and factories of the libraries.
    fn factory(&mut self, path: &str, args: Option<&Args<'t>>) -> Option<Value> {
        let kind = match path {
            "pika.BlockingConnection"
            | "pika.SelectConnection"
            | "pika.adapters.blocking_connection.BlockingConnection"
            | "pika.adapters.select_connection.SelectConnection" => Kind::PikaConn,
            "aio_pika.connect"
            | "aio_pika.connect_robust"
            | "aio_pika.robust_connection.connect_robust"
            | "aio_pika.connection.connect" => Kind::AioConn,
            "kombu.Connection" | "kombu.connection.Connection" => Kind::KombuConn,
            "kombu.Producer" | "kombu.messaging.Producer" => Kind::KombuProducer,
            "kombu.Queue" | "kombu.entity.Queue" => {
                let name = args
                    .and_then(|a| a.get(Some(0), "name"))
                    .and_then(|n| self.name_of(n, 0));
                return Some(Value {
                    kind: Kind::KombuQueue,
                    name,
                });
            }
            "redis.Redis"
            | "redis.StrictRedis"
            | "redis.from_url"
            | "redis.Redis.from_url"
            | "redis.StrictRedis.from_url"
            | "redis.client.Redis"
            | "redis.client.StrictRedis"
            | "redis.asyncio.Redis"
            | "redis.asyncio.StrictRedis"
            | "redis.asyncio.from_url"
            | "redis.asyncio.Redis.from_url"
            | "redis.asyncio.client.Redis"
            | "aioredis.Redis"
            | "aioredis.StrictRedis"
            | "aioredis.from_url"
            | "aioredis.create_redis"
            | "aioredis.create_redis_pool" => Kind::RedisClient,
            "nats.connect" | "nats.NATS" | "nats.aio.client.Client" | "nats.aio.client.NATS" => {
                Kind::NatsClient
            }
            "boto3.Session"
            | "boto3.session.Session"
            | "aioboto3.Session"
            | "aioboto3.session.Session" => Kind::AwsSession,
            "boto3.client" => return self.aws_client(args?),
            "google.cloud.pubsub_v1.PublisherClient"
            | "google.cloud.pubsub_v1.publisher.Client"
            | "google.cloud.pubsub.PublisherClient" => Kind::Publisher,
            "google.cloud.pubsub_v1.SubscriberClient"
            | "google.cloud.pubsub_v1.subscriber.Client"
            | "google.cloud.pubsub.SubscriberClient" => Kind::Subscriber,
            "confluent_kafka.Producer"
            | "confluent_kafka.SerializingProducer"
            | "kafka.KafkaProducer"
            | "kafka.producer.KafkaProducer"
            | "aiokafka.AIOKafkaProducer"
            | "aiokafka.producer.AIOKafkaProducer" => Kind::KafkaProducer,
            "confluent_kafka.Consumer"
            | "confluent_kafka.DeserializingConsumer"
            | "kafka.KafkaConsumer"
            | "kafka.consumer.KafkaConsumer"
            | "aiokafka.AIOKafkaConsumer"
            | "aiokafka.consumer.AIOKafkaConsumer" => Kind::KafkaConsumer,
            _ => return None,
        };
        val(kind)
    }

    /// `boto3.client("sqs")` / `session.client(service_name="sns")`.
    fn aws_client(&mut self, args: &Args<'t>) -> Option<Value> {
        let service = args.get(Some(0), "service_name")?;
        match self.literal(service)?.as_str() {
            "sqs" => val(Kind::SqsClient),
            "sns" => val(Kind::SnsClient),
            _ => None,
        }
    }

    /// What `value.method(args)` returns.
    fn method_result(&mut self, v: &Value, method: &str, args: Option<&Args<'t>>) -> Option<Value> {
        let destination = |ctx: &mut Self, kind: Kind, pos: usize, kw: &str| {
            let name = args
                .and_then(|a| a.get(Some(pos), kw))
                .and_then(|n| ctx.name_of(n, 0));
            Some(Value { kind, name })
        };
        match (v.kind, method) {
            (Kind::PikaConn, "channel") => val(Kind::PikaChannel),
            (Kind::AioConn, "channel") => val(Kind::AioChannel),
            (Kind::AioChannel, "declare_queue" | "get_queue") => {
                destination(self, Kind::AioQueue, 0, "name")
            }
            (Kind::AioChannel, "declare_exchange" | "get_exchange") => val(Kind::AioExchange),
            (Kind::KombuConn, "Producer") => val(Kind::KombuProducer),
            (Kind::RedisClient, "pubsub") => val(Kind::RedisPubSub),
            (Kind::NatsClient, "jetstream") => val(Kind::NatsJs),
            (Kind::AwsSession, "client") => self.aws_client(args?),
            (Kind::Publisher, "topic_path") => destination(self, Kind::TopicPath, 1, "topic"),
            (Kind::Subscriber, "subscription_path") => {
                destination(self, Kind::SubPath, 1, "subscription")
            }
            _ => None,
        }
    }

    /// The content of a plain string literal: no prefix but `r` / `u`, no interpolation, no escape.
    fn literal(&self, node: Node<'t>) -> Option<String> {
        if node.kind() != "string" {
            return None;
        }
        let mut out = String::new();
        let mut cursor = node.walk();
        for c in node.children(&mut cursor) {
            match c.kind() {
                "string_start" => {
                    let start = text(c, self.source);
                    let quote = start.find(['"', '\''])?;
                    if !start[..quote]
                        .chars()
                        .all(|p| matches!(p, 'r' | 'R' | 'u' | 'U'))
                    {
                        return None;
                    }
                }
                "string_content" => out.push_str(text(c, self.source)),
                "string_end" => {}
                _ => return None,
            }
        }
        Some(out)
    }

    /// The channel name a node stands for, if the file shows it (before any name filtering).
    fn name_of(&mut self, node: Node<'t>, depth: usize) -> Option<String> {
        if depth > MAX_EVAL_DEPTH || self.capped {
            return None;
        }
        match node.kind() {
            "string" => self.literal(node),
            "parenthesized_expression" => self.name_of(node.named_child(0)?, depth + 1),
            "identifier" => {
                let bounds = self.bounds(node, text(node, self.source));
                self.fold(bounds, depth, Self::name_of_bound)
            }
            "attribute" => {
                let object = node.child_by_field_name("object")?;
                if object.kind() == "identifier"
                    && matches!(text(object, self.source), "self" | "cls")
                {
                    let attr = text(node.child_by_field_name("attribute")?, self.source);
                    let bounds = self.class_attr(node, attr);
                    return self.fold(bounds, depth, Self::name_of_bound);
                }
                None
            }
            "subscript" | "call" => {
                if let Some(config) = PythonExtractor::config_value(node, self.source) {
                    return Some(config);
                }
                // `sqs.get_queue_url(QueueName="q")["QueueUrl"]`: the queue the request names.
                if node.kind() == "subscript" {
                    return self.created_name(node);
                }
                // A destination value (`publisher.topic_path("p", "orders")`).
                self.eval(node, depth + 1)?.name
            }
            _ => None,
        }
    }

    fn name_of_bound(&mut self, b: &Bound<'t>, depth: usize) -> Option<String> {
        match b {
            Bound::Expr(e) => self.name_of(*e, depth + 1),
            _ => None,
        }
    }

    /// `client.get_queue_url(QueueName=lit)["QueueUrl"]`, `client.create_queue(QueueName=lit)[..]`,
    /// `client.create_topic(Name=lit)["TopicArn"]`.
    fn created_name(&mut self, sub: Node<'t>) -> Option<String> {
        let call = sub.child_by_field_name("value")?;
        if call.kind() != "call" {
            return None;
        }
        let f = call.child_by_field_name("function")?;
        let method = text(f.child_by_field_name("attribute")?, self.source);
        let client = self.eval(f.child_by_field_name("object")?, 1)?;
        let key = match (client.kind, method) {
            (Kind::SqsClient, "get_queue_url" | "create_queue") => "QueueName",
            (Kind::SnsClient, "create_topic") => "Name",
            _ => return None,
        };
        let args = Args::of(call, self.source)?;
        let (_, v) = args.kw.iter().find(|(k, _)| k == key)?;
        self.literal(*v)
    }
}

/// The client a type annotation stands for.
fn annotation(path: &str) -> Option<Value> {
    let kind = match path {
        "pika.adapters.blocking_connection.BlockingChannel" | "pika.channel.Channel" => {
            Kind::PikaChannel
        }
        "pika.BlockingConnection" => Kind::PikaConn,
        "aio_pika.abc.AbstractChannel" | "aio_pika.Channel" | "aio_pika.RobustChannel" => {
            Kind::AioChannel
        }
        "aio_pika.abc.AbstractExchange" | "aio_pika.Exchange" | "aio_pika.RobustExchange" => {
            Kind::AioExchange
        }
        "redis.Redis" | "redis.StrictRedis" | "redis.client.Redis" | "redis.asyncio.Redis" => {
            Kind::RedisClient
        }
        "redis.client.PubSub" | "redis.asyncio.client.PubSub" => Kind::RedisPubSub,
        "nats.aio.client.Client" | "nats.NATS" => Kind::NatsClient,
        "nats.js.JetStreamContext" | "nats.js.client.JetStreamContext" => Kind::NatsJs,
        "mypy_boto3_sqs.SQSClient" | "types_boto3_sqs.SQSClient" => Kind::SqsClient,
        "mypy_boto3_sns.SNSClient" | "types_boto3_sns.SNSClient" => Kind::SnsClient,
        "google.cloud.pubsub_v1.PublisherClient" => Kind::Publisher,
        "google.cloud.pubsub_v1.SubscriberClient" => Kind::Subscriber,
        "confluent_kafka.Producer" | "kafka.KafkaProducer" | "aiokafka.AIOKafkaProducer" => {
            Kind::KafkaProducer
        }
        "confluent_kafka.Consumer" | "kafka.KafkaConsumer" | "aiokafka.AIOKafkaConsumer" => {
            Kind::KafkaConsumer
        }
        _ => return None,
    };
    val(kind)
}

/// A queue or topic name from an ARN, a URL or a Pub/Sub path: its last segment.
fn last_segment(s: &str) -> String {
    if s.contains("${") {
        return s.to_string();
    }
    s.rsplit(['/', ':']).next().unwrap_or(s).to_string()
}

/// Whether a key is a pattern (`orders.*`, `orders.#`, NATS `orders.>`): a consumer may
/// subscribe to one, a producer never publishes to one.
fn is_pattern(key: &str) -> bool {
    key.contains(['*', '#', '>'])
}

/// A usable channel name: a `${key}` placeholder, or a plain name (no whitespace, no URL, no
/// credential-shaped token). A placeholder default is dropped, never taken as the name.
fn finish(key: String) -> Option<String> {
    let key = if key.contains("${") {
        mesh_core::config_values::strip_placeholder_defaults(&key).into_owned()
    } else {
        key
    };
    let ok = if key.starts_with("${") {
        !key.chars().any(char::is_whitespace)
    } else {
        mesh_core::config_values::plain_topic(&key.replace(['#', '>'], "*"))
    };
    ok.then_some(key)
}

struct Out {
    sites: Vec<Site>,
    claims: HashMap<(usize, usize), Claim>,
}

impl<'t> Ctx<'t> {
    fn emit(
        &mut self,
        out: &mut Out,
        call: Node<'t>,
        producer: bool,
        broker: Broker,
        key: Option<String>,
        via: &'static str,
    ) {
        out.claims
            .insert((call.start_byte(), call.end_byte()), Claim::Broker(broker));
        let Some(key) = key.and_then(finish) else {
            return;
        };
        if producer && is_pattern(&key) {
            return;
        }
        // One past the cap is kept to tell "capped" from "exactly full"; the walk goes on so every
        // call this pass recognises stays claimed.
        if out.sites.len() > MAX_PY_BROKER_SITES_PER_FILE {
            return;
        }
        out.sites.push(Site {
            producer,
            broker,
            key,
            lines: (call.start_position().row + 1, call.end_position().row + 1),
            via,
        });
    }

    /// The destination of a Pub/Sub / SQS / SNS call: a path value, or a literal URL / ARN.
    fn destination(&mut self, node: Option<Node<'t>>) -> Option<String> {
        self.name_of(node?, 0).map(|n| last_segment(&n))
    }

    /// Every name of a `queues=` / `topics=` argument: one element, or a list / tuple of them.
    fn names_of_list(&mut self, node: Node<'t>) -> Vec<String> {
        let elements: Vec<Node<'t>> = if matches!(node.kind(), "list" | "tuple" | "set") {
            let mut c = node.walk();
            node.named_children(&mut c).take(MAX_LIST).collect()
        } else {
            vec![node]
        };
        elements
            .into_iter()
            .filter_map(|e| self.name_of(e, 0))
            .collect()
    }

    fn call(&mut self, call: Node<'t>, out: &mut Out) {
        let Some(f) = call.child_by_field_name("function") else {
            return;
        };
        let Some(args) = Args::of(call, self.source) else {
            return;
        };
        let source = self.source;
        let last = match f.kind() {
            "attribute" => f
                .child_by_field_name("attribute")
                .map_or("", |a| text(a, source)),
            "identifier" => text(f, source),
            _ => return,
        };
        // Constructors that name their channels: `KafkaConsumer("a", "b")`, `Consumer(queues=...)`.
        if matches!(last, "KafkaConsumer" | "AIOKafkaConsumer" | "Consumer") {
            if let Some(path) = self.qual(f) {
                self.constructor_site(&path, f, call, &args, out);
                return;
            }
        }
        let Some(method) = CALL_NAMES.iter().copied().find(|m| *m == last) else {
            return;
        };
        if f.kind() != "attribute" {
            return;
        }
        let Some(object) = f.child_by_field_name("object") else {
            return;
        };
        let Some(v) = self.eval(object, 0) else {
            return;
        };
        self.method_site(&v, method, call, &args, out);
    }

    fn constructor_site(
        &mut self,
        path: &str,
        f: Node<'t>,
        call: Node<'t>,
        args: &Args<'t>,
        out: &mut Out,
    ) {
        match path {
            "kafka.KafkaConsumer"
            | "kafka.consumer.KafkaConsumer"
            | "aiokafka.AIOKafkaConsumer"
            | "aiokafka.consumer.AIOKafkaConsumer" => {
                out.claims.insert(
                    (call.start_byte(), call.end_byte()),
                    Claim::Broker(Broker::Kafka),
                );
                // `AIOKafkaConsumer("t")` by its bare name is read by the shape-based reader.
                if f.kind() == "identifier" && last_is(path, "AIOKafkaConsumer") {
                    return;
                }
                if args.splat {
                    return;
                }
                let names: Vec<String> = args
                    .pos
                    .iter()
                    .take(MAX_LIST)
                    .filter_map(|n| self.name_of(*n, 0))
                    .collect();
                for name in names {
                    self.emit(out, call, false, Broker::Kafka, Some(name), "KafkaConsumer");
                }
            }
            "kombu.Consumer" | "kombu.messaging.Consumer" => {
                let queues = args.get(Some(1), "queues");
                self.kombu_consumer(queues, call, out);
            }
            _ => {}
        }
    }

    fn kombu_consumer(&mut self, queues: Option<Node<'t>>, call: Node<'t>, out: &mut Out) {
        let names = queues.map_or_else(Vec::new, |q| {
            let mut found = Vec::new();
            let elements: Vec<Node<'t>> = if matches!(q.kind(), "list" | "tuple") {
                let mut c = q.walk();
                q.named_children(&mut c).take(MAX_LIST).collect()
            } else {
                vec![q]
            };
            for e in elements {
                if let Some(Value {
                    kind: Kind::KombuQueue,
                    name: Some(n),
                }) = self.eval(e, 0)
                {
                    found.push(n);
                }
            }
            found
        });
        out.claims.insert(
            (call.start_byte(), call.end_byte()),
            Claim::Broker(Broker::RabbitMq),
        );
        for name in names {
            self.emit(out, call, false, Broker::RabbitMq, Some(name), "Consumer");
        }
    }

    fn method_site(
        &mut self,
        v: &Value,
        method: &'static str,
        call: Node<'t>,
        args: &Args<'t>,
        out: &mut Out,
    ) {
        let name = |ctx: &mut Self, at: Option<usize>, kw: &str| {
            args.get(at, kw).and_then(|n| ctx.name_of(n, 0))
        };
        match (v.kind, method) {
            (Kind::LocalClass, "produce" | "send" | "send_and_wait" | "publish" | "subscribe") => {
                out.claims
                    .insert((call.start_byte(), call.end_byte()), Claim::Foreign);
            }
            (Kind::PikaChannel, "basic_publish") => {
                let k = name(self, Some(1), "routing_key");
                self.emit(out, call, true, Broker::RabbitMq, k, method);
            }
            (Kind::PikaChannel, "basic_consume" | "basic_get" | "consume") => {
                let k = name(self, Some(0), "queue");
                self.emit(out, call, false, Broker::RabbitMq, k, method);
            }
            (Kind::AioExchange, "publish") => {
                let k = name(self, Some(1), "routing_key");
                self.emit(out, call, true, Broker::RabbitMq, k, method);
            }
            (Kind::AioQueue, "consume" | "iterator") => {
                self.emit(out, call, false, Broker::RabbitMq, v.name.clone(), method);
            }
            (Kind::KombuProducer, "publish") => {
                let k = name(self, Some(1), "routing_key");
                self.emit(out, call, true, Broker::RabbitMq, k, method);
            }
            (Kind::KombuConn, "Consumer") => {
                let queues = args.get(Some(0), "queues");
                self.kombu_consumer(queues, call, out);
            }
            (Kind::RedisClient, "publish") => {
                let k = name(self, Some(0), "channel");
                self.emit(out, call, true, Broker::Redis, k, method);
            }
            (Kind::RedisPubSub, "subscribe" | "psubscribe") => {
                out.claims.insert(
                    (call.start_byte(), call.end_byte()),
                    Claim::Broker(Broker::Redis),
                );
                if args.splat {
                    return;
                }
                // `p.subscribe("a", "b")` and `p.subscribe(a=handler)`: the keyword is the channel.
                let mut names: Vec<String> = args
                    .pos
                    .iter()
                    .take(MAX_LIST)
                    .filter_map(|n| self.name_of(*n, 0))
                    .collect();
                names.extend(args.kw.iter().take(MAX_LIST).map(|(k, _)| k.clone()));
                for k in names {
                    self.emit(out, call, false, Broker::Redis, Some(k), method);
                }
            }
            (Kind::NatsClient | Kind::NatsJs, "publish" | "request") => {
                let k = name(self, Some(0), "subject");
                self.emit(out, call, true, Broker::Nats, k, method);
            }
            (Kind::NatsClient | Kind::NatsJs, "subscribe") => {
                let k = name(self, Some(0), "subject");
                self.emit(out, call, false, Broker::Nats, k, method);
            }
            (Kind::NatsJs, "pull_subscribe") => {
                let k = name(self, Some(0), "subject");
                self.emit(out, call, false, Broker::Nats, k, method);
            }
            (Kind::SqsClient, "send_message" | "send_message_batch") => {
                let k = self.destination(args.get(None, "QueueUrl"));
                self.emit(out, call, true, Broker::Sqs, k, method);
            }
            (Kind::SqsClient, "receive_message") => {
                let k = self.destination(args.get(None, "QueueUrl"));
                self.emit(out, call, false, Broker::Sqs, k, method);
            }
            (Kind::SnsClient, "publish" | "publish_batch") => {
                let k = self.destination(args.get(None, "TopicArn"));
                self.emit(out, call, true, Broker::Sns, k, method);
            }
            (Kind::Publisher, "publish") => {
                let k = self.destination(args.get(Some(0), "topic"));
                self.emit(out, call, true, Broker::PubSub, k, method);
            }
            (Kind::Subscriber, "subscribe") => {
                let k = self.destination(args.get(Some(0), "subscription"));
                self.emit(out, call, false, Broker::PubSub, k, method);
            }
            (Kind::KafkaProducer, "produce" | "send" | "send_and_wait") => {
                out.claims.insert(
                    (call.start_byte(), call.end_byte()),
                    Claim::Broker(Broker::Kafka),
                );
            }
            (Kind::KafkaConsumer, "subscribe") => {
                out.claims.insert(
                    (call.start_byte(), call.end_byte()),
                    Claim::Broker(Broker::Kafka),
                );
                // `subscribe(topics=[...])`: the keyword form the shape-based reader skips.
                if let Some((_, topics)) = args.kw.iter().find(|(k, _)| k == "topics") {
                    for t in self.names_of_list(*topics) {
                        self.emit(out, call, false, Broker::Kafka, Some(t), method);
                    }
                }
            }
            _ => {}
        }
    }
}

fn last_is(path: &str, name: &str) -> bool {
    path.rsplit('.').next() == Some(name)
}

/// Reads the broker facts of a Python file. Cheap for a file that names none of the libraries.
pub(crate) fn scan(tree: &Tree, content: &str) -> Scan {
    if !PREFILTER.iter().any(|p| content.contains(p)) {
        return Scan::default();
    }
    let mut ctx = Ctx::new(content.as_bytes());
    let mut out = Out {
        sites: Vec::new(),
        claims: HashMap::new(),
    };
    let mut cursor = tree.walk();
    'walk: loop {
        let node = cursor.node();
        if node.kind() == "call" {
            ctx.call(node, &mut out);
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
    let capped = out.sites.len() > MAX_PY_BROKER_SITES_PER_FILE || ctx.capped;
    out.sites.truncate(MAX_PY_BROKER_SITES_PER_FILE);
    Scan {
        sites: out.sites,
        claims: out.claims,
        capped,
    }
}

/// Registers a [`Scan`]'s sites in `out`, whose `nodes` the Python extractor has already filled.
pub(crate) fn apply(scan: Scan, out: &mut FileIndex) {
    out.py_brokers_capped |= scan.capped;
    let Some(template) = out.nodes.first().cloned() else {
        return;
    };
    for site in scan.sites {
        out.nodes.push(ContractNode {
            id: 0,
            name: CompactStr::new(&site.key),
            kind: NodeKind::EventStream,
            file_path: template.file_path.clone(),
            line_start: site.lines.0,
            line_end: site.lines.1,
            package: template.package.clone(),
            repo_id: template.repo_id,
            signature: Some(CompactStr::new(format!(
                "py-broker {} {}",
                site.broker.slug(),
                site.via
            ))),
            docstring: None,
        });
        let index = out.nodes.len() - 1;
        out.node_brokers.push((index, site.broker));
        if site.producer {
            out.producer_brokers
                .push((out.producers.len(), site.broker));
            out.producers.push((index, CompactStr::new(&site.key)));
        } else {
            out.consumer_brokers
                .push((out.consumers.len(), site.broker));
            out.consumers.push((index, CompactStr::new(&site.key)));
        }
    }
}
