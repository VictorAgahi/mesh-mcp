//! Message brokers other than Kafka in Go (9.0 ticket T8b).
//!
//! The Go extractor reads kafka-go, sarama and confluent-kafka-go. This pass adds the other
//! brokers a Go service talks to, each recorded under its own [`Broker`] so a queue named like a
//! Kafka topic is a different channel (T7):
//!
//! | broker | consumers | producers |
//! |---|---|---|
//! | `rabbitmq` (`amqp091-go`, `streadway/amqp`) | `Channel.Consume` / `ConsumeWithContext` / `Get` | `Channel.Publish` / `PublishWithContext` (and the deferred-confirm forms) |
//! | `nats` (`nats.go`, `jetstream`) | `Subscribe`, `QueueSubscribe`, `ChanSubscribe`, `SubscribeSync`, `PullSubscribe` (legacy JetStream) | `Publish`, `PublishMsg`, `Request`, `RequestMsg`, JetStream `Publish` |
//! | `redis` (`go-redis`) | `Subscribe`, `PSubscribe`, `SSubscribe` | `Publish`, `SPublish` |
//! | `sqs` (`aws-sdk-go`, `aws-sdk-go-v2`) | `ReceiveMessage` | `SendMessage`, `SendMessageBatch` |
//! | `sns` | none | `Publish`, `PublishBatch` |
//! | `pubsub` (`cloud.google.com/go/pubsub`) | `Subscription.Receive` of a subscription whose topic the file shows | `Topic.Publish` |
//! | `kafka` (sarama, franz-go: the variants `go.rs` does not read) | `ConsumerGroup.Consume(ctx, []string{"t"}, h)`, `kgo.ConsumeTopics("t")` | `kgo.Record{Topic: "t"}`, `kgo.DefaultProduceTopic("t")` |
//!
//! A call is read only when its receiver is a variable, parameter, struct field or call result
//! whose type the file shows to be the library's client (`var ch *amqp.Channel`, `ch, _ :=
//! conn.Channel()`, `p.ch` of `type P struct { ch *amqp.Channel }`) in a file that really imports
//! the library (an `import_spec` of the tree, never a comment or a string), under the package
//! name that import gives. A local type of the same name, a package name hidden by a parameter,
//! a local or a range variable, and an import of another library never make a client. A call on
//! a library client that no name can be read from is still claimed (`claimed`) so the older
//! Kafka heuristics of `go.rs`, which read any `Subscribe("x")`, do not report it as Kafka.
//!
//! A name is read only when the file shows it: a string literal, a constant or a variable of the
//! file that nothing reassigns, a concatenation of those, `os.Getenv("K")` / `viper.GetString("k")`
//! as the `${K}` placeholder the configuration resolves (the default of such a read is never a
//! name: a variable assigned elsewhere is not read), the field of a struct literal
//! (`&sqs.SendMessageInput{QueueUrl: ...}`, `&nats.Msg{Subject: ...}`), the `Name` of a queue the
//! file declared, or `aws.String(x)` of one of those. A parameter, a call result or a name of
//! another package registers nothing: no name is ever guessed from an identifier.
//!
//! Bounds: every walk is iterative; the lookups of a name climb the tree and read a per-scope
//! table built once; type and name resolution follow at most [`MAX_RESOLVE_DEPTH`] links; sites
//! past [`MAX_GO_BROKER_SITES_PER_FILE`] are dropped and the file is counted in
//! `IndexHealth::files_go_brokers_capped`.

use super::{FileIndex, MAX_GO_BROKER_SITES_PER_FILE};
use mesh_core::{Broker, CompactStr, ContractNode, NodeKind};
use std::collections::{HashMap, HashSet};
use tree_sitter::{Node, Tree};

/// Links of a type or name resolution one lookup follows.
const MAX_RESOLVE_DEPTH: u8 = 8;
/// Names the scope tables remember; past it the file is counted as capped.
const MAX_DECLARED_NAMES: usize = 16_384;
/// Arguments, fields and parameters one lookup looks at.
const MAX_ELEMENTS: usize = 256;
/// Calls of interest the pre-pass keeps; past it the file is counted as capped.
const MAX_CALLS: usize = 50_000;
/// Routing keys remembered per queue.
const MAX_BINDS_PER_QUEUE: usize = 32;
/// Longest channel name recorded.
const MAX_NAME_LEN: usize = 512;

/// What an import of the file stands for.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Kind {
    Amqp,
    Nats,
    Jetstream,
    Redis {
        ctx: bool,
    },
    Sqs,
    Sns,
    PubSub,
    /// kafka-go and confluent-kafka-go (read by `go.rs`; typed here only to tell their calls from
    /// the calls of an unknown receiver).
    Kafka,
    /// sarama (`IBM/sarama`, `Shopify/sarama`).
    Sarama,
    /// franz-go (`kgo`).
    Franz,
    /// `aws-sdk-go` `aws` helpers (`aws.String`).
    Aws,
    Os,
    Viper,
}

/// The kind and default package name of an import path, or `None` for any other library.
fn import_kind(path: &str) -> Option<(Kind, &'static str)> {
    match path {
        "github.com/rabbitmq/amqp091-go" => return Some((Kind::Amqp, "amqp091")),
        "github.com/streadway/amqp" => return Some((Kind::Amqp, "amqp")),
        "github.com/nats-io/nats.go" => return Some((Kind::Nats, "nats")),
        "github.com/nats-io/nats.go/jetstream" => return Some((Kind::Jetstream, "jetstream")),
        "github.com/aws/aws-sdk-go-v2/service/sqs" | "github.com/aws/aws-sdk-go/service/sqs" => {
            return Some((Kind::Sqs, "sqs"))
        }
        "github.com/aws/aws-sdk-go/service/sqs/sqsiface" => return Some((Kind::Sqs, "sqsiface")),
        "github.com/aws/aws-sdk-go-v2/service/sns" | "github.com/aws/aws-sdk-go/service/sns" => {
            return Some((Kind::Sns, "sns"))
        }
        "github.com/aws/aws-sdk-go/service/sns/snsiface" => return Some((Kind::Sns, "snsiface")),
        "github.com/aws/aws-sdk-go-v2/aws" | "github.com/aws/aws-sdk-go/aws" => {
            return Some((Kind::Aws, "aws"))
        }
        "cloud.google.com/go/pubsub" | "cloud.google.com/go/pubsub/v2" => {
            return Some((Kind::PubSub, "pubsub"))
        }
        "github.com/segmentio/kafka-go"
        | "github.com/confluentinc/confluent-kafka-go/kafka"
        | "github.com/confluentinc/confluent-kafka-go/v2/kafka" => {
            return Some((Kind::Kafka, "kafka"))
        }
        "github.com/IBM/sarama" | "github.com/Shopify/sarama" => {
            return Some((Kind::Sarama, "sarama"))
        }
        "github.com/twmb/franz-go/pkg/kgo" => return Some((Kind::Franz, "kgo")),
        "os" => return Some((Kind::Os, "os")),
        "github.com/spf13/viper" => return Some((Kind::Viper, "viper")),
        _ => {}
    }
    // go-redis: `.../v9` (and `/v8`) take a context, the older majors do not.
    let (base, version) = match path.rsplit_once("/v") {
        Some((b, v)) if !v.is_empty() && v.bytes().all(|c| c.is_ascii_digit()) => {
            (b, v.parse::<u32>().ok())
        }
        _ => (path, None),
    };
    match base {
        "github.com/redis/go-redis" => Some((Kind::Redis { ctx: true }, "redis")),
        "github.com/go-redis/redis" => Some((
            Kind::Redis {
                ctx: version.is_some_and(|v| v >= 8),
            },
            "redis",
        )),
        _ => None,
    }
}

/// What a variable, field or call result is, as far as a call on it matters.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Ty {
    AmqpConn,
    AmqpChannel,
    /// A queue the file declared (`QueueDeclare("name", ...)`); its `Name` field is the name.
    AmqpQueue(Option<String>),
    NatsConn,
    /// `nats.JetStreamContext`.
    NatsJs,
    /// `jetstream.JetStream` (the context is the first argument).
    NatsJs2,
    Redis {
        ctx: bool,
    },
    RedisPubSub {
        ctx: bool,
    },
    Sqs,
    Sns,
    /// The output of `GetQueueUrl` / `CreateQueue` / `CreateTopic`: `QueueUrl` / `TopicArn` is
    /// the name the file gave.
    AwsResult(Option<String>),
    PsClient,
    /// A Pub/Sub topic and the name it was opened with.
    PsTopic(Option<String>),
    /// A Pub/Sub subscription and the ID it was opened with.
    PsSub(Option<String>),
    SaramaGroup,
    /// A client of kafka-go, confluent-kafka-go, sarama or franz-go.
    Kafka,
    /// A struct declared in the file.
    Local(String),
}

/// Where the type or value of a declared name comes from.
#[derive(Clone, Copy)]
enum Src<'t> {
    Unknown,
    /// A declared type node.
    Type(Node<'t>),
    /// The `idx`-th result of an initializer expression.
    Init(Node<'t>, usize),
}

/// What a declaration says about a name.
#[derive(Clone, Copy)]
struct Binding<'t> {
    src: Src<'t>,
    /// The initializer when it is a single value of this name (for a string constant).
    value: Option<Node<'t>>,
    constant: bool,
}

const UNKNOWN: Binding<'static> = Binding {
    src: Src::Unknown,
    value: None,
    constant: false,
};

/// The declarations of one scope node: name -> (end byte of the declaration, binding), in
/// source order. A parameter or a package-level name is visible everywhere (end byte 0).
type ScopeMap<'t> = HashMap<String, Vec<(usize, Binding<'t>)>>;

/// Methods a call is read for at all, so the receiver lookup runs on few calls.
const CALL_NAMES: &[&str] = &[
    "Publish",
    "PublishWithContext",
    "PublishWithDeferredConfirm",
    "PublishWithDeferredConfirmWithContext",
    "PublishAsync",
    "PublishMsg",
    "PublishMsgAsync",
    "PublishRequest",
    "PublishBatch",
    "PublishBatchWithContext",
    "SPublish",
    "Request",
    "RequestMsg",
    "RequestWithContext",
    "RequestMsgWithContext",
    "BindSendChan",
    "BindRecvChan",
    "BindRecvQueueChan",
    "Consume",
    "ConsumeWithContext",
    "Get",
    "QueueBind",
    "Subscribe",
    "PSubscribe",
    "SSubscribe",
    "QueueSubscribe",
    "ChanSubscribe",
    "ChanQueueSubscribe",
    "SubscribeSync",
    "QueueSubscribeSync",
    "QueueSubscribeSyncWithChan",
    "PullSubscribe",
    "SendMessage",
    "SendMessageWithContext",
    "SendMessageBatch",
    "SendMessageBatchWithContext",
    "ReceiveMessage",
    "ReceiveMessageWithContext",
    "Receive",
    "CreateSubscription",
    "ConsumeTopics",
    "DefaultProduceTopic",
];

/// The methods `go.rs` reads as Kafka calls whatever the receiver.
const LEGACY_KAFKA_CALLS: &[&str] = &[
    "WriteMessages",
    "SendMessage",
    "Produce",
    "ReadMessage",
    "FetchMessage",
    "ConsumePartition",
    "SubscribeTopics",
    "Subscribe",
];

/// One broker fact before it becomes a node.
struct Site {
    producer: bool,
    broker: Broker,
    key: String,
    lines: (usize, usize),
    via: &'static str,
}

/// The broker facts of a Go file, read off its tree.
#[derive(Default)]
pub(crate) struct Scan {
    sites: Vec<Site>,
    /// Start bytes of the calls this pass owns, even when no name could be read: the older
    /// Kafka heuristics of `go.rs` skip them.
    pub(crate) claimed: HashSet<usize>,
    /// Start bytes of the Kafka-shaped calls (`Subscribe("x")`, `Produce(..)`) of a file that
    /// imports one of the other libraries, whose receiver is not shown to be a Kafka client: the
    /// older heuristics still read them, but with an unknown broker, not as Kafka.
    pub(crate) unsure: HashSet<usize>,
    capped: bool,
}

impl Scan {
    /// Room for `n` more sites; a call that does not fit is not claimed (the older heuristics
    /// keep reading it) and the file is counted as capped.
    fn has_room(&mut self, n: usize) -> bool {
        if self.sites.len() + n > MAX_GO_BROKER_SITES_PER_FILE {
            self.capped = true;
            return false;
        }
        true
    }
}

fn text<'s>(node: Node, source: &'s [u8]) -> &'s str {
    node.utf8_text(source).unwrap_or("")
}

fn lines(node: Node) -> (usize, usize) {
    (node.start_position().row + 1, node.end_position().row + 1)
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

/// A string usable as a channel name. The gate is the workspace's one (commandment 5,
/// `mesh_core::member_consts::constant_is_a_name`, the T14 rule): a plain name, not a URL, a
/// token, `Bearer_...`, a password-shaped word nor a random-looking run. A `*` / `>` wildcard is a
/// name character here; a configuration placeholder (`${KEY}`, built by `config_ref`) stands for
/// itself and the rest of the key must still be a name.
fn usable_name(s: &str) -> bool {
    if s.is_empty() || s.len() > MAX_NAME_LEN || s.chars().any(char::is_whitespace) {
        return false;
    }
    let mut plain = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(start) = rest.find("${") {
        plain.push_str(&rest[..start]);
        let Some(len) = rest[start..].find('}') else {
            return false;
        };
        plain.push('x');
        rest = &rest[start + len + 1..];
    }
    plain.push_str(rest);
    mesh_core::member_consts::constant_is_a_name("", &plain.replace('>', "*"))
}

/// Whether a constant or variable may give a name: not named like a credential (`apiKey`,
/// `DB_PASSWORD`). Asks the shared gate with a value that is a name.
fn member_may_name(id: &str) -> bool {
    mesh_core::member_consts::constant_is_a_name(id, "q")
}

/// Whether a key is a pattern (`orders.*`, `orders.>`, `orders.#`): a consumer may subscribe to
/// one, a producer never publishes to one.
fn is_pattern(key: &str) -> bool {
    key.contains(['*', '#', '>'])
}

/// A queue or topic name from an ARN or URL: its last segment (`.../123456789012/orders`), the
/// query string and fragment of a URL cut off first (`.../orders?token=x` is `orders`).
fn aws_name(s: &str) -> String {
    if s.contains("${") {
        return s.to_string();
    }
    let path = s.split(['?', '#']).next().unwrap_or(s);
    path.rsplit(['/', ':']).next().unwrap_or(path).to_string()
}

/// The named children of an argument list (comments left out), bounded.
fn args_of(call: Node) -> Vec<Node> {
    let Some(list) = call.child_by_field_name("arguments") else {
        return Vec::new();
    };
    let mut cursor = list.walk();
    list.named_children(&mut cursor)
        .filter(|a| a.kind() != "comment")
        .take(MAX_ELEMENTS)
        .collect()
}

struct Ctx<'t> {
    source: &'t [u8],
    /// Package name a file import is used under -> what it is.
    imports: HashMap<String, Kind>,
    scopes: HashMap<usize, ScopeMap<'t>>,
    /// Struct types of the file: name -> field -> type node.
    structs: HashMap<String, HashMap<String, Node<'t>>>,
    /// `type X = lib.T` / `type X lib.T`.
    aliases: HashMap<String, Node<'t>>,
    /// Function name -> result node.
    funcs: HashMap<String, Node<'t>>,
    /// (receiver type, method) -> result node.
    methods: HashMap<(String, String), Node<'t>>,
    /// Struct types holding a client of a broker library (`type Feed struct{ r *kafka.Reader }`):
    /// (holds a Kafka client, holds a client of another library). Their methods are the file's
    /// own wrappers, not foreign calls.
    wrappers: HashMap<String, (bool, bool)>,
    /// Every (receiver type, method) the file declares: a call of one is the file's own, not a
    /// library's.
    local_methods: HashSet<(String, String)>,
    /// Names assigned to after their declaration anywhere in the file: a literal they hold is
    /// not a name (it may be a default another assignment overrides).
    assigned: HashSet<String>,
    /// RabbitMQ queue -> the routing keys the file binds it with.
    binds: HashMap<String, Vec<String>>,
    /// Pub/Sub subscription ID -> the topic the file creates it on.
    sub_topics: HashMap<String, String>,
    declared: usize,
    capped: bool,
}

impl<'t> Ctx<'t> {
    // ---- declarations ---------------------------------------------------------------------

    fn note_declared(&mut self) -> bool {
        if self.declared >= MAX_DECLARED_NAMES {
            self.capped = true;
            return false;
        }
        self.declared += 1;
        true
    }

    fn insert(&mut self, map: &mut ScopeMap<'t>, name: &str, end: usize, b: Binding<'t>) {
        if name == "_" || name.is_empty() || !self.note_declared() {
            return;
        }
        map.entry(name.to_string()).or_default().push((end, b));
    }

    /// The names of an `expression_list` / identifier list node.
    fn identifiers(&self, list: Node<'t>) -> Vec<Node<'t>> {
        if list.kind() == "identifier" {
            return vec![list];
        }
        let mut cursor = list.walk();
        list.named_children(&mut cursor)
            .filter(|n| n.kind() == "identifier")
            .take(MAX_ELEMENTS)
            .collect()
    }

    fn expressions(&self, list: Node<'t>) -> Vec<Node<'t>> {
        let mut cursor = list.walk();
        list.named_children(&mut cursor)
            .filter(|n| n.kind() != "comment")
            .take(MAX_ELEMENTS)
            .collect()
    }

    /// `names := values` / `var names = values`: each name gets its value, or the result of the
    /// lone call that returns several.
    fn bind_names(
        &mut self,
        map: &mut ScopeMap<'t>,
        names: &[Node<'t>],
        values: &[Node<'t>],
        declared_type: Option<Node<'t>>,
        constant: bool,
        end: usize,
    ) {
        for (i, n) in names.iter().enumerate() {
            let paired = values.len() == names.len();
            let (src, value) = match declared_type {
                Some(t) => (Src::Type(t), paired.then(|| values[i])),
                None if paired => (Src::Init(values[i], 0), Some(values[i])),
                None if values.len() == 1 => (Src::Init(values[0], i), None),
                None => (Src::Unknown, None),
            };
            let name = text(*n, self.source).to_string();
            self.insert(
                map,
                &name,
                end,
                Binding {
                    src,
                    value,
                    constant,
                },
            );
        }
    }

    fn bind_unknown(&mut self, map: &mut ScopeMap<'t>, names: &[Node<'t>], end: usize) {
        for n in names {
            let name = text(*n, self.source).to_string();
            self.insert(map, &name, end, UNKNOWN);
        }
    }

    fn short_decl(&mut self, map: &mut ScopeMap<'t>, decl: Node<'t>, end: usize) {
        let (Some(left), Some(right)) = (
            decl.child_by_field_name("left"),
            decl.child_by_field_name("right"),
        ) else {
            return;
        };
        let names = self.identifiers(left);
        let values = self.expressions(right);
        self.bind_names(map, &names, &values, None, false, end);
    }

    fn spec(&mut self, map: &mut ScopeMap<'t>, spec: Node<'t>, constant: bool, end: usize) {
        let mut cursor = spec.walk();
        let names: Vec<Node> = spec
            .children_by_field_name("name", &mut cursor)
            .filter(|n| n.kind() == "identifier")
            .take(MAX_ELEMENTS)
            .collect();
        let values = spec
            .child_by_field_name("value")
            .map(|v| self.expressions(v))
            .unwrap_or_default();
        self.bind_names(
            map,
            &names,
            &values,
            spec.child_by_field_name("type"),
            constant,
            end,
        );
    }

    fn declaration(&mut self, map: &mut ScopeMap<'t>, decl: Node<'t>, end: usize) {
        let constant = decl.kind() == "const_declaration";
        let mut specs = Vec::new();
        let mut cursor = decl.walk();
        for c in decl.named_children(&mut cursor).take(MAX_ELEMENTS) {
            match c.kind() {
                "var_spec" | "const_spec" => specs.push(c),
                "var_spec_list" => {
                    let mut inner = c.walk();
                    specs.extend(c.named_children(&mut inner).take(MAX_ELEMENTS));
                }
                _ => {}
            }
        }
        for s in specs {
            if matches!(s.kind(), "var_spec" | "const_spec") {
                let end = if end == 0 { 0 } else { s.end_byte() };
                self.spec(map, s, constant, end);
            }
        }
    }

    fn params(&mut self, map: &mut ScopeMap<'t>, list: Node<'t>) {
        let mut cursor = list.walk();
        let decls: Vec<Node> = list
            .named_children(&mut cursor)
            .filter(|c| {
                matches!(
                    c.kind(),
                    "parameter_declaration" | "variadic_parameter_declaration"
                )
            })
            .take(MAX_ELEMENTS)
            .collect();
        for d in decls {
            let mut c = d.walk();
            let names: Vec<Node> = d
                .children_by_field_name("name", &mut c)
                .filter(|n| n.kind() == "identifier")
                .collect();
            // `...T` is a slice of `T`: the name is declared (it hides) but is no client.
            let variadic = d.kind() == "variadic_parameter_declaration";
            for n in names {
                let name = text(n, self.source).to_string();
                let b = match (variadic, d.child_by_field_name("type")) {
                    (false, Some(t)) => Binding {
                        src: Src::Type(t),
                        value: None,
                        constant: false,
                    },
                    _ => UNKNOWN,
                };
                self.insert(map, &name, 0, b);
            }
        }
    }

    /// The declarations a scope node makes visible to the code it contains.
    fn build_scope(&mut self, p: Node<'t>) -> ScopeMap<'t> {
        let mut map: ScopeMap<'t> = HashMap::new();
        match p.kind() {
            "function_declaration" | "method_declaration" | "func_literal" => {
                for field in ["receiver", "parameters", "result"] {
                    if let Some(list) = p.child_by_field_name(field) {
                        if list.kind() == "parameter_list" {
                            self.params(&mut map, list);
                        }
                    }
                }
            }
            "source_file" | "block" | "expression_case" | "default_case" | "type_case"
            | "communication_case" => {
                let top = p.kind() == "source_file";
                let mut cursor = p.walk();
                let statements: Vec<Node> = p.named_children(&mut cursor).collect();
                for st in statements {
                    // A package-level name is visible from any function of the file.
                    let end = if top { 0 } else { st.end_byte() };
                    match st.kind() {
                        "short_var_declaration" => self.short_decl(&mut map, st, end),
                        "var_declaration" | "const_declaration" => {
                            self.declaration(&mut map, st, end)
                        }
                        _ => {}
                    }
                }
                // `case msg := <-ch:`
                if p.kind() == "communication_case" {
                    if let Some(c) = p.child_by_field_name("communication") {
                        if c.kind() == "receive_statement" {
                            self.receive(&mut map, c);
                        }
                    }
                }
            }
            "for_statement" => {
                let mut cursor = p.walk();
                let clauses: Vec<Node> = p.named_children(&mut cursor).collect();
                for c in clauses {
                    match c.kind() {
                        "for_clause" => {
                            if let Some(init) = c.child_by_field_name("initializer") {
                                if init.kind() == "short_var_declaration" {
                                    self.short_decl(&mut map, init, 0);
                                }
                            }
                        }
                        "range_clause" => {
                            // Only `:=` declares; `for i = range` assigns.
                            let declares = (0..c.child_count())
                                .filter_map(|i| c.child(i))
                                .any(|t| t.kind() == ":=");
                            if let (true, Some(left)) = (declares, c.child_by_field_name("left")) {
                                let names = self.identifiers(left);
                                self.bind_unknown(&mut map, &names, 0);
                            }
                        }
                        _ => {}
                    }
                }
            }
            "if_statement" | "expression_switch_statement" | "type_switch_statement" => {
                if let Some(init) = p.child_by_field_name("initializer") {
                    if init.kind() == "short_var_declaration" {
                        self.short_decl(&mut map, init, init.end_byte());
                    }
                }
                if let Some(alias) = p.child_by_field_name("alias") {
                    let names = self.identifiers(alias);
                    self.bind_unknown(&mut map, &names, 0);
                }
            }
            _ => {}
        }
        map
    }

    fn receive(&mut self, map: &mut ScopeMap<'t>, st: Node<'t>) {
        let declares = (0..st.child_count())
            .filter_map(|i| st.child(i))
            .any(|t| t.kind() == ":=");
        if let (true, Some(left)) = (declares, st.child_by_field_name("left")) {
            let names = self.identifiers(left);
            self.bind_unknown(map, &names, 0);
        }
    }

    /// The package-level types, functions and methods of the file.
    fn declare_top(&mut self, root: Node<'t>) {
        let source = self.source;
        let mut cursor = root.walk();
        let top: Vec<Node> = root.named_children(&mut cursor).collect();
        for d in top {
            match d.kind() {
                "type_declaration" => {
                    let mut c = d.walk();
                    let specs: Vec<Node> = d.named_children(&mut c).collect();
                    for s in specs {
                        self.type_spec(s);
                    }
                }
                "function_declaration" => {
                    if let (Some(name), Some(result)) = (
                        d.child_by_field_name("name"),
                        d.child_by_field_name("result"),
                    ) {
                        self.funcs
                            .entry(text(name, source).to_string())
                            .or_insert(result);
                    }
                }
                "method_declaration" => {
                    let recv = d
                        .child_by_field_name("receiver")
                        .and_then(|r| r.named_child(0))
                        .and_then(|p| p.child_by_field_name("type"))
                        .and_then(|t| self.receiver_type_name(t));
                    if let (Some(recv), Some(name)) = (recv, d.child_by_field_name("name")) {
                        let key = (recv, text(name, source).to_string());
                        if self.local_methods.len() < MAX_DECLARED_NAMES {
                            self.local_methods.insert(key.clone());
                        } else {
                            self.capped = true;
                        }
                        if let Some(result) = d.child_by_field_name("result") {
                            self.methods.entry(key).or_insert(result);
                        }
                    }
                }
                _ => {}
            }
        }
    }

    /// The broker library a field type is drawn from (`*kafka.Reader`, `[]*nats.Conn`).
    fn field_library(&self, t: Node<'t>) -> Option<Kind> {
        let mut node = t;
        for _ in 0..6 {
            match node.kind() {
                "pointer_type" | "parenthesized_type" => node = node.named_child(0)?,
                "slice_type" | "array_type" => node = node.child_by_field_name("element")?,
                "map_type" => node = node.child_by_field_name("value")?,
                "generic_type" => node = node.child_by_field_name("type")?,
                "qualified_type" => {
                    let pkg = text(node.child_by_field_name("package")?, self.source);
                    return match self.imports.get(pkg)? {
                        k @ (Kind::Kafka
                        | Kind::Sarama
                        | Kind::Franz
                        | Kind::Amqp
                        | Kind::Nats
                        | Kind::Jetstream
                        | Kind::Redis { .. }
                        | Kind::Sqs
                        | Kind::Sns
                        | Kind::PubSub) => Some(*k),
                        _ => None,
                    };
                }
                _ => return None,
            }
        }
        None
    }

    /// `*T`, `T[K]`, `*T[K]` -> `T`.
    fn receiver_type_name(&self, t: Node<'t>) -> Option<String> {
        let mut node = t;
        for _ in 0..4 {
            match node.kind() {
                "pointer_type" | "parenthesized_type" => node = node.named_child(0)?,
                "generic_type" => node = node.child_by_field_name("type")?,
                "type_identifier" => return Some(text(node, self.source).to_string()),
                _ => return None,
            }
        }
        None
    }

    fn type_spec(&mut self, spec: Node<'t>) {
        if !matches!(spec.kind(), "type_spec" | "type_alias") {
            return;
        }
        let (Some(name), Some(ty)) = (
            spec.child_by_field_name("name"),
            spec.child_by_field_name("type"),
        ) else {
            return;
        };
        let name = text(name, self.source).to_string();
        match ty.kind() {
            "struct_type" => {
                let mut fields: HashMap<String, Node<'t>> = HashMap::new();
                let mut cursor = ty.walk();
                let lists: Vec<Node> = ty.named_children(&mut cursor).collect();
                for list in lists {
                    let mut c = list.walk();
                    let decls: Vec<Node> = list
                        .named_children(&mut c)
                        .filter(|f| f.kind() == "field_declaration")
                        .take(MAX_ELEMENTS)
                        .collect();
                    for f in decls {
                        let Some(t) = f.child_by_field_name("type") else {
                            continue;
                        };
                        if let Some(kind) = self.field_library(t) {
                            let w = self.wrappers.entry(name.clone()).or_default();
                            if matches!(kind, Kind::Kafka | Kind::Sarama | Kind::Franz) {
                                w.0 = true;
                            } else {
                                w.1 = true;
                            }
                        }
                        let mut cc = f.walk();
                        let names: Vec<Node> = f.children_by_field_name("name", &mut cc).collect();
                        for n in names {
                            if self.note_declared() {
                                fields.insert(text(n, self.source).to_string(), t);
                            }
                        }
                    }
                }
                self.structs.insert(name, fields);
            }
            "interface_type" => {}
            _ => {
                self.aliases.insert(name, ty);
            }
        }
    }

    // ---- scopes ---------------------------------------------------------------------------

    /// The declaration the variable `name` read at `at` refers to, innermost scope first
    /// (parameters of the function and its enclosing closures, locals declared before the use,
    /// loop, `if` / `switch` / `select` variables, then the package level of the file). `None`
    /// when nothing in the file declares it.
    fn lookup(&mut self, at: Node<'t>, name: &str) -> Option<Binding<'t>> {
        let pos = at.start_byte();
        let mut cur = at.parent();
        while let Some(p) = cur {
            if matches!(
                p.kind(),
                "function_declaration"
                    | "method_declaration"
                    | "func_literal"
                    | "source_file"
                    | "block"
                    | "expression_case"
                    | "default_case"
                    | "type_case"
                    | "communication_case"
                    | "for_statement"
                    | "if_statement"
                    | "expression_switch_statement"
                    | "type_switch_statement"
            ) {
                if !self.scopes.contains_key(&p.id()) {
                    let map = self.build_scope(p);
                    self.scopes.insert(p.id(), map);
                }
                let hit = self
                    .scopes
                    .get(&p.id())
                    .and_then(|m| m.get(name))
                    .and_then(|v| v.iter().rev().find(|(end, _)| *end <= pos))
                    .map(|(_, b)| *b);
                if hit.is_some() {
                    return hit;
                }
            }
            cur = p.parent();
        }
        None
    }

    /// The kind of library the package name `node` stands for, when nothing hides it.
    fn package_of(&mut self, node: Node<'t>) -> Option<Kind> {
        if node.kind() != "identifier" {
            return None;
        }
        let id = text(node, self.source);
        let kind = *self.imports.get(id)?;
        self.lookup(node, id).is_none().then_some(kind)
    }

    // ---- types ----------------------------------------------------------------------------

    fn lib_type(kind: Kind, name: &str) -> Option<Ty> {
        Some(match (kind, name) {
            (Kind::Amqp, "Channel") => Ty::AmqpChannel,
            (Kind::Amqp, "Connection") => Ty::AmqpConn,
            (Kind::Nats, "Conn" | "EncodedConn") => Ty::NatsConn,
            (Kind::Nats, "JetStreamContext") => Ty::NatsJs,
            (Kind::Jetstream, "JetStream") => Ty::NatsJs2,
            (
                Kind::Redis { ctx },
                "Client" | "ClusterClient" | "Ring" | "UniversalClient" | "Cmdable" | "Conn"
                | "Pipeliner" | "Pipeline" | "Tx",
            ) => Ty::Redis { ctx },
            (Kind::Redis { ctx }, "PubSub") => Ty::RedisPubSub { ctx },
            (Kind::Sqs, "Client" | "SQS" | "SQSAPI") => Ty::Sqs,
            (Kind::Sns, "Client" | "SNS" | "SNSAPI") => Ty::Sns,
            (Kind::PubSub, "Client") => Ty::PsClient,
            (Kind::PubSub, "Topic" | "Publisher") => Ty::PsTopic(None),
            (Kind::PubSub, "Subscription" | "Subscriber") => Ty::PsSub(None),
            (Kind::Sarama, "ConsumerGroup") => Ty::SaramaGroup,
            (Kind::Kafka | Kind::Sarama | Kind::Franz, _) => Ty::Kafka,
            _ => return None,
        })
    }

    fn ty_of_type_node(&mut self, t: Node<'t>, depth: u8) -> Option<Ty> {
        if depth > MAX_RESOLVE_DEPTH {
            return None;
        }
        match t.kind() {
            "pointer_type" | "parenthesized_type" => {
                self.ty_of_type_node(t.named_child(0)?, depth + 1)
            }
            "generic_type" => self.ty_of_type_node(t.child_by_field_name("type")?, depth + 1),
            "qualified_type" => {
                let pkg = text(t.child_by_field_name("package")?, self.source);
                let name = text(t.child_by_field_name("name")?, self.source);
                let kind = *self.imports.get(pkg)?;
                Self::lib_type(kind, name)
            }
            "type_identifier" => {
                let name = text(t, self.source);
                if let Some(target) = self.aliases.get(name).copied() {
                    return self.ty_of_type_node(target, depth + 1);
                }
                self.structs
                    .contains_key(name)
                    .then(|| Ty::Local(name.to_string()))
            }
            _ => None,
        }
    }

    fn ty_of_binding(&mut self, b: Binding<'t>, depth: u8) -> Option<Ty> {
        match b.src {
            Src::Unknown => None,
            Src::Type(t) => self.ty_of_type_node(t, depth + 1),
            Src::Init(rhs, idx) => self.ty_of(rhs, idx, depth + 1),
        }
    }

    /// The `idx`-th result type of a function's `result` node.
    fn result_ty(&mut self, result: Node<'t>, idx: usize, depth: u8) -> Option<Ty> {
        if result.kind() != "parameter_list" {
            return if idx == 0 {
                self.ty_of_type_node(result, depth + 1)
            } else {
                None
            };
        }
        let mut cursor = result.walk();
        let decls: Vec<Node> = result
            .named_children(&mut cursor)
            .filter(|d| d.kind() == "parameter_declaration")
            .take(MAX_ELEMENTS)
            .collect();
        let mut at = 0;
        for d in decls {
            let mut c = d.walk();
            let count = d.children_by_field_name("name", &mut c).count().max(1);
            if idx < at + count {
                return self.ty_of_type_node(d.child_by_field_name("type")?, depth + 1);
            }
            at += count;
        }
        None
    }

    /// What the expression `e` is, its `idx`-th result when it is a call that returns several.
    fn ty_of(&mut self, e: Node<'t>, idx: usize, depth: u8) -> Option<Ty> {
        if depth > MAX_RESOLVE_DEPTH {
            return None;
        }
        match e.kind() {
            "parenthesized_expression" => self.ty_of(e.named_child(0)?, idx, depth + 1),
            "unary_expression" => {
                let op = e
                    .child_by_field_name("operator")
                    .map(|o| text(o, self.source));
                if matches!(op, Some("&" | "*")) {
                    self.ty_of(e.child_by_field_name("operand")?, idx, depth + 1)
                } else {
                    None
                }
            }
            "identifier" => {
                let id = text(e, self.source);
                let b = self.lookup(e, id)?;
                self.ty_of_binding(b, depth)
            }
            "selector_expression" => {
                let owner = self.ty_of(e.child_by_field_name("operand")?, 0, depth + 1)?;
                let Ty::Local(s) = owner else {
                    return None;
                };
                let field = text(e.child_by_field_name("field")?, self.source);
                let t = self.structs.get(&s)?.get(field).copied()?;
                self.ty_of_type_node(t, depth + 1)
            }
            "composite_literal" => self.ty_of_type_node(e.child_by_field_name("type")?, depth + 1),
            "type_assertion_expression" => {
                self.ty_of_type_node(e.child_by_field_name("type")?, depth + 1)
            }
            "call_expression" => self.call_ty(e, idx, depth),
            _ => None,
        }
    }

    /// What a call returns, for the constructors and accessors of the libraries and the
    /// functions and methods the file declares.
    fn call_ty(&mut self, call: Node<'t>, idx: usize, depth: u8) -> Option<Ty> {
        let func = call.child_by_field_name("function")?;
        let args = args_of(call);
        if func.kind() == "identifier" {
            let name = text(func, self.source);
            if self.lookup(func, name).is_some() {
                return None;
            }
            let result = self.funcs.get(name).copied()?;
            return self.result_ty(result, idx, depth + 1);
        }
        if func.kind() != "selector_expression" {
            return None;
        }
        let operand = func.child_by_field_name("operand")?;
        let method = text(func.child_by_field_name("field")?, self.source);
        if let Some(kind) = self.package_of(operand) {
            if idx != 0 {
                return None;
            }
            return match (kind, method) {
                (Kind::Amqp, "Dial" | "DialConfig" | "DialTLS") => Some(Ty::AmqpConn),
                (Kind::Nats, "Connect" | "NewEncodedConn") => Some(Ty::NatsConn),
                (Kind::Jetstream, "New" | "NewWithAPIOptions" | "NewWithDomain") => {
                    Some(Ty::NatsJs2)
                }
                (
                    Kind::Redis { ctx },
                    "NewClient"
                    | "NewClusterClient"
                    | "NewRing"
                    | "NewUniversalClient"
                    | "NewFailoverClient"
                    | "NewFailoverClusterClient",
                ) => Some(Ty::Redis { ctx }),
                (Kind::Sqs, "New" | "NewFromConfig") => Some(Ty::Sqs),
                (Kind::Sns, "New" | "NewFromConfig") => Some(Ty::Sns),
                (Kind::PubSub, "NewClient" | "NewClientWithConfig") => Some(Ty::PsClient),
                (Kind::Sarama, "NewConsumerGroup" | "NewConsumerGroupFromClient") => {
                    Some(Ty::SaramaGroup)
                }
                (Kind::Kafka | Kind::Sarama, n) if n.starts_with("New") => Some(Ty::Kafka),
                (Kind::Franz, "NewClient") => Some(Ty::Kafka),
                _ => None,
            };
        }
        let owner = self.ty_of(operand, 0, depth + 1)?;
        match (&owner, method) {
            (Ty::Local(s), _) => {
                let result = self
                    .methods
                    .get(&(s.clone(), method.to_string()))
                    .copied()?;
                self.result_ty(result, idx, depth + 1)
            }
            _ if idx != 0 => None,
            (Ty::AmqpConn, "Channel") => Some(Ty::AmqpChannel),
            (Ty::NatsConn, "JetStream") => Some(Ty::NatsJs),
            (Ty::Redis { ctx }, "Subscribe" | "PSubscribe" | "SSubscribe") => {
                Some(Ty::RedisPubSub { ctx: *ctx })
            }
            (Ty::AmqpChannel, "QueueDeclare" | "QueueDeclarePassive") => {
                let name = self.name_of(*args.first()?, 0).filter(|n| !n.is_empty());
                Some(Ty::AmqpQueue(name))
            }
            (Ty::Sqs, "GetQueueUrl" | "CreateQueue") => {
                let name = self.aws_input_field(&args, Kind::Sqs, "QueueName");
                Some(Ty::AwsResult(name))
            }
            (Ty::Sns, "CreateTopic") => {
                let name = self.aws_input_field(&args, Kind::Sns, "Name");
                Some(Ty::AwsResult(name))
            }
            (Ty::PsClient, "Topic" | "Publisher") => {
                Some(Ty::PsTopic(self.name_of(*args.first()?, 0)))
            }
            (Ty::PsClient, "TopicInProject") => Some(Ty::PsTopic(self.name_of(*args.first()?, 0))),
            (Ty::PsClient, "CreateTopic") => Some(Ty::PsTopic(self.name_of(*args.get(1)?, 0))),
            (Ty::PsClient, "Subscription" | "Subscriber") => {
                Some(Ty::PsSub(self.name_of(*args.first()?, 0)))
            }
            (Ty::PsClient, "CreateSubscription") => Some(Ty::PsSub(self.name_of(*args.get(1)?, 0))),
            _ => None,
        }
    }

    // ---- names ----------------------------------------------------------------------------

    /// The content of a plain string literal (no escape).
    fn literal(&self, node: Node<'t>) -> Option<String> {
        let t = text(node, self.source);
        let inner = match node.kind() {
            "interpreted_string_literal" if !t.contains('\\') => {
                t.strip_prefix('"')?.strip_suffix('"')?
            }
            "raw_string_literal" => t.strip_prefix('`')?.strip_suffix('`')?,
            _ => return None,
        };
        // A default of a placeholder written in a literal is never a name (T6).
        Some(if inner.contains("${") {
            mesh_core::config_values::strip_placeholder_defaults(inner).into_owned()
        } else {
            inner.to_string()
        })
    }

    /// The channel name `node` stands for, if the file shows it. May be empty (`""`).
    fn name_of(&mut self, node: Node<'t>, depth: u8) -> Option<String> {
        if depth > MAX_RESOLVE_DEPTH {
            return None;
        }
        match node.kind() {
            "interpreted_string_literal" | "raw_string_literal" => self.literal(node),
            "parenthesized_expression" => self.name_of(node.named_child(0)?, depth + 1),
            "unary_expression" => {
                let op = node
                    .child_by_field_name("operator")
                    .map(|o| text(o, self.source));
                if matches!(op, Some("&" | "*")) {
                    self.name_of(node.child_by_field_name("operand")?, depth + 1)
                } else {
                    None
                }
            }
            "identifier" => {
                let id = text(node, self.source);
                let b = self.lookup(node, id)?;
                let value = b.value?;
                // A constant named like a credential (`apiKey`) is no channel name.
                if !member_may_name(id) {
                    return None;
                }
                let name = self.name_of(value, depth + 1)?;
                // A literal that something assigns to again may be a default the program
                // overrides: not a name. A configuration placeholder stays.
                if !b.constant && self.assigned.contains(id) && !name.starts_with("${") {
                    return None;
                }
                Some(name)
            }
            "binary_expression" => {
                let op = node
                    .child_by_field_name("operator")
                    .map(|o| text(o, self.source));
                if op != Some("+") {
                    return None;
                }
                let left = self.name_of(node.child_by_field_name("left")?, depth + 1)?;
                let right = self.name_of(node.child_by_field_name("right")?, depth + 1)?;
                Some(left + &right)
            }
            "call_expression" => self.name_of_call(node, depth),
            "selector_expression" => {
                let owner = self.ty_of(node.child_by_field_name("operand")?, 0, depth + 1)?;
                let field = text(node.child_by_field_name("field")?, self.source);
                match (owner, field) {
                    (Ty::AmqpQueue(n), "Name") => n,
                    (Ty::AwsResult(n), "QueueUrl" | "TopicArn") => n,
                    _ => None,
                }
            }
            _ => None,
        }
    }

    fn name_of_call(&mut self, call: Node<'t>, depth: u8) -> Option<String> {
        let func = call.child_by_field_name("function")?;
        if func.kind() != "selector_expression" {
            return None;
        }
        let kind = self.package_of(func.child_by_field_name("operand")?)?;
        let method = text(func.child_by_field_name("field")?, self.source);
        let args = args_of(call);
        match (kind, method) {
            // The configuration key, never a default the program falls back to.
            (Kind::Os, "Getenv") | (Kind::Viper, "GetString" | "Get") => {
                let [key] = args.as_slice() else {
                    return None;
                };
                let key = self.literal(*key)?;
                crate::config_ref::placeholder(&key, None)
            }
            (Kind::Aws, "String") => {
                let [arg] = args.as_slice() else {
                    return None;
                };
                self.name_of(*arg, depth + 1)
            }
            _ => None,
        }
    }

    // ---- struct literals ------------------------------------------------------------------

    /// The composite literal an expression is: itself, `&lit`, or a local initialized with one.
    fn composite_of(&mut self, e: Node<'t>, depth: u8) -> Option<Node<'t>> {
        if depth > MAX_RESOLVE_DEPTH {
            return None;
        }
        match e.kind() {
            "composite_literal" => Some(e),
            "parenthesized_expression" => self.composite_of(e.named_child(0)?, depth + 1),
            "unary_expression" => self.composite_of(e.child_by_field_name("operand")?, depth + 1),
            "identifier" => {
                let id = text(e, self.source);
                let b = self.lookup(e, id)?;
                self.composite_of(b.value?, depth + 1)
            }
            _ => None,
        }
    }

    /// The names of the elements of a `[]string{...}` literal.
    fn string_elements(&mut self, lit: Node<'t>) -> Vec<String> {
        let Some(body) = lit.child_by_field_name("body") else {
            return Vec::new();
        };
        let mut cursor = body.walk();
        let elements: Vec<Node> = body
            .named_children(&mut cursor)
            .filter(|e| e.kind() == "literal_element")
            .take(MAX_ELEMENTS)
            .collect();
        elements
            .into_iter()
            .filter_map(|e| e.named_child(0))
            .filter_map(|e| self.name_of(e, 0))
            .collect()
    }

    /// The value of the keyed element `field` of a composite literal.
    fn composite_field(&self, lit: Node<'t>, field: &str) -> Option<Node<'t>> {
        let body = lit.child_by_field_name("body")?;
        let mut cursor = body.walk();
        let found = body
            .named_children(&mut cursor)
            .filter(|el| el.kind() == "keyed_element")
            .take(MAX_ELEMENTS)
            .find(|el| {
                el.child_by_field_name("key")
                    .is_some_and(|k| text(k, self.source) == field)
            })?;
        let value = found.child_by_field_name("value")?;
        if value.kind() == "literal_element" {
            value.named_child(0)
        } else {
            Some(value)
        }
    }

    /// The name of `field` in the SQS / SNS input struct among `args` (any `*sqs.XInput`).
    fn aws_input_field(&mut self, args: &[Node<'t>], kind: Kind, field: &str) -> Option<String> {
        self.aws_input_field_of(args, kind, field, &[])
    }

    fn aws_input_field_of(
        &mut self,
        args: &[Node<'t>],
        kind: Kind,
        field: &str,
        types: &[&str],
    ) -> Option<String> {
        for arg in args.iter().take(4) {
            let Some(lit) = self.composite_of(*arg, 0) else {
                continue;
            };
            let Some(t) = lit.child_by_field_name("type") else {
                continue;
            };
            if t.kind() != "qualified_type" {
                continue;
            }
            let pkg = text(t.child_by_field_name("package")?, self.source);
            let name = text(t.child_by_field_name("name")?, self.source);
            if self.imports.get(pkg) != Some(&kind) || (!types.is_empty() && !types.contains(&name))
            {
                continue;
            }
            if let Some(v) = self.composite_field(lit, field) {
                if let Some(n) = self.name_of(v, 0) {
                    return Some(aws_name(&n));
                }
            }
        }
        None
    }

    /// The subject of a `nats.Msg` argument: `&nats.Msg{Subject: "x"}`, `nats.NewMsg("x")` or a
    /// local initialized with one.
    fn nats_msg_subject(&mut self, msg: Node<'t>, depth: u8) -> Option<String> {
        if depth > MAX_RESOLVE_DEPTH {
            return None;
        }
        match msg.kind() {
            "identifier" => {
                let id = text(msg, self.source);
                let b = self.lookup(msg, id)?;
                self.nats_msg_subject(b.value?, depth + 1)
            }
            "parenthesized_expression" => self.nats_msg_subject(msg.named_child(0)?, depth + 1),
            "unary_expression" => {
                self.nats_msg_subject(msg.child_by_field_name("operand")?, depth + 1)
            }
            "composite_literal" => {
                let t = msg.child_by_field_name("type")?;
                if t.kind() != "qualified_type" {
                    return None;
                }
                let pkg = text(t.child_by_field_name("package")?, self.source);
                let name = text(t.child_by_field_name("name")?, self.source);
                if self.imports.get(pkg) != Some(&Kind::Nats) || name != "Msg" {
                    return None;
                }
                let v = self.composite_field(msg, "Subject")?;
                self.name_of(v, 0)
            }
            "call_expression" => {
                let func = msg.child_by_field_name("function")?;
                if func.kind() != "selector_expression" {
                    return None;
                }
                let kind = self.package_of(func.child_by_field_name("operand")?)?;
                let method = text(func.child_by_field_name("field")?, self.source);
                if (kind, method) != (Kind::Nats, "NewMsg") {
                    return None;
                }
                let args = args_of(msg);
                self.name_of(*args.first()?, 0)
            }
            _ => None,
        }
    }
}

/// `kgo.ConsumeTopics("a", "b")` and `kgo.DefaultProduceTopic("t")`: options of a franz-go client.
fn franz_call<'t>(call: Node<'t>, method: &str, ctx: &mut Ctx<'t>, out: &mut Scan) {
    let args = args_of(call);
    let (producer, names): (bool, Vec<String>) = match method {
        "ConsumeTopics" => (
            false,
            args.iter().filter_map(|a| ctx.name_of(*a, 0)).collect(),
        ),
        "DefaultProduceTopic" => (
            true,
            args.first()
                .and_then(|a| ctx.name_of(*a, 0))
                .into_iter()
                .collect(),
        ),
        _ => return,
    };
    for key in names.into_iter().filter(|k| usable_name(k)) {
        if (producer && is_pattern(&key)) || !out.has_room(1) {
            continue;
        }
        out.sites.push(Site {
            producer,
            broker: Broker::Kafka,
            key,
            lines: lines(call),
            via: if producer {
                "DefaultProduceTopic"
            } else {
                "ConsumeTopics"
            },
        });
    }
}

/// `kgo.Record{Topic: "t"}`: a record of a franz-go client.
fn franz_record<'t>(lit: Node<'t>, ctx: &mut Ctx<'t>, out: &mut Scan) {
    let Some(t) = lit.child_by_field_name("type") else {
        return;
    };
    let Some(pkg) = t.child_by_field_name("package") else {
        return;
    };
    if ctx.imports.get(text(pkg, ctx.source)) != Some(&Kind::Franz) {
        return;
    }
    let Some(key) = ctx
        .composite_field(lit, "Topic")
        .and_then(|v| ctx.name_of(v, 0))
        .filter(|k| usable_name(k) && !is_pattern(k))
    else {
        return;
    };
    if !out.has_room(1) {
        return;
    }
    out.sites.push(Site {
        producer: true,
        broker: Broker::Kafka,
        key,
        lines: lines(lit),
        via: "Record",
    });
}

/// What a call on a client does.
enum Action {
    /// Not a call this pass owns.
    None,
    /// A call this pass owns: the names it shows (possibly none).
    Hits {
        producer: bool,
        broker: Broker,
        keys: Vec<String>,
    },
}

/// Reads the broker facts of a Go file. Cheap for a file that names none of the libraries.
pub(crate) fn scan(tree: &Tree, content: &str) -> Scan {
    const MARKERS: &[&str] = &[
        "amqp091-go",
        "streadway/amqp",
        "nats-io/nats.go",
        "go-redis",
        "aws-sdk-go",
        "cloud.google.com/go/pubsub",
        "sarama",
        "franz-go",
    ];
    // A cheap filter before the precise one (the import declarations).
    if !MARKERS.iter().any(|m| content.contains(m)) {
        return Scan::default();
    }
    let source = content.as_bytes();
    let root = tree.root_node();
    let mut ctx = Ctx {
        source,
        imports: HashMap::new(),
        scopes: HashMap::new(),
        structs: HashMap::new(),
        aliases: HashMap::new(),
        funcs: HashMap::new(),
        methods: HashMap::new(),
        local_methods: HashSet::new(),
        wrappers: HashMap::new(),
        assigned: HashSet::new(),
        binds: HashMap::new(),
        sub_topics: HashMap::new(),
        declared: 0,
        capped: false,
    };
    read_imports(root, &mut ctx);
    if !ctx.imports.values().any(|k| {
        matches!(
            k,
            Kind::Amqp
                | Kind::Nats
                | Kind::Jetstream
                | Kind::Redis { .. }
                | Kind::Sqs
                | Kind::Sns
                | Kind::PubSub
                | Kind::Sarama
                | Kind::Franz
        )
    }) {
        return Scan::default();
    }
    ctx.declare_top(root);

    // One pass: the calls of interest, and the names something assigns to.
    let mut calls: Vec<Node> = Vec::new();
    let mut records: Vec<Node> = Vec::new();
    let mut legacy_calls: Vec<Node> = Vec::new();
    let mut capped = false;
    walk(root, |n| {
        match n.kind() {
            "call_expression" => {
                let named = n
                    .child_by_field_name("function")
                    .filter(|f| f.kind() == "selector_expression")
                    .and_then(|f| f.child_by_field_name("field"))
                    .is_some_and(|f| CALL_NAMES.contains(&text(f, source)));
                let legacy = n
                    .child_by_field_name("function")
                    .filter(|f| f.kind() == "selector_expression")
                    .and_then(|f| f.child_by_field_name("field"))
                    .is_some_and(|f| LEGACY_KAFKA_CALLS.contains(&text(f, source)));
                if legacy && legacy_calls.len() < MAX_CALLS {
                    legacy_calls.push(n);
                }
                if named {
                    if calls.len() >= MAX_CALLS {
                        capped = true;
                    } else {
                        calls.push(n);
                    }
                }
            }
            "composite_literal" => {
                let is_record = n
                    .child_by_field_name("type")
                    .filter(|t| t.kind() == "qualified_type")
                    .and_then(|t| t.child_by_field_name("name"))
                    .is_some_and(|t| text(t, source) == "Record");
                if is_record && records.len() < MAX_CALLS {
                    records.push(n);
                }
            }
            "assignment_statement" => {
                if let Some(left) = n.child_by_field_name("left") {
                    for id in ctx.identifiers(left) {
                        ctx.assigned.insert(text(id, source).to_string());
                    }
                }
            }
            "inc_statement" | "dec_statement" => {
                if let Some(id) = n.named_child(0).filter(|x| x.kind() == "identifier") {
                    ctx.assigned.insert(text(id, source).to_string());
                }
            }
            _ => {}
        }
        true
    });

    // The bindings first (a queue's routing keys, a subscription's topic), then the sites.
    for call in &calls {
        read_binding(*call, &mut ctx);
    }
    let mut out = Scan::default();
    for call in &calls {
        site_calls(*call, &mut ctx, &mut out);
    }
    for lit in &records {
        franz_record(*lit, &mut ctx, &mut out);
    }
    out.capped |= ctx.capped || capped;
    // In a file of the other libraries, a Kafka-shaped call on a receiver that is not shown to be
    // a Kafka client has no known broker.
    let others = ctx.imports.values().any(|k| {
        matches!(
            k,
            Kind::Amqp
                | Kind::Nats
                | Kind::Jetstream
                | Kind::Redis { .. }
                | Kind::Sqs
                | Kind::Sns
                | Kind::PubSub
        )
    });
    if others {
        for call in &legacy_calls {
            if out.claimed.contains(&call.start_byte()) {
                continue;
            }
            let Some(operand) = call
                .child_by_field_name("function")
                .and_then(|f| f.child_by_field_name("operand"))
            else {
                continue;
            };
            let kafka = match ctx.ty_of(operand, 0, 0) {
                Some(Ty::Kafka | Ty::SaramaGroup) => true,
                Some(Ty::Local(s)) => ctx.wrappers.get(&s).is_some_and(|w| w.0),
                _ => false,
            };
            if !kafka {
                out.unsure.insert(call.start_byte());
            }
        }
    }
    out
}

fn read_imports(root: Node, ctx: &mut Ctx) {
    let source = ctx.source;
    let mut cursor = root.walk();
    let decls: Vec<Node> = root
        .named_children(&mut cursor)
        .filter(|d| d.kind() == "import_declaration")
        .collect();
    for d in decls {
        let mut specs: Vec<Node> = Vec::new();
        let mut c = d.walk();
        for child in d.named_children(&mut c) {
            match child.kind() {
                "import_spec" => specs.push(child),
                "import_spec_list" => {
                    let mut inner = child.walk();
                    specs.extend(
                        child
                            .named_children(&mut inner)
                            .filter(|s| s.kind() == "import_spec"),
                    );
                }
                _ => {}
            }
        }
        for s in specs.into_iter().take(MAX_ELEMENTS) {
            let Some(path) = s.child_by_field_name("path") else {
                continue;
            };
            let path = text(path, source).trim_matches(|c| c == '"' || c == '`');
            let Some((kind, default_name)) = import_kind(path) else {
                continue;
            };
            let name = match s.child_by_field_name("name") {
                Some(n) if n.kind() == "package_identifier" => text(n, source),
                // `_` and `.` imports bring no package name this pass can read.
                Some(_) => continue,
                None => default_name,
            };
            ctx.imports.insert(name.to_string(), kind);
        }
    }
}

/// `QueueBind` and `CreateSubscription`: facts the other calls of the file are read against.
fn read_binding<'t>(call: Node<'t>, ctx: &mut Ctx<'t>) {
    let Some(func) = call
        .child_by_field_name("function")
        .filter(|f| f.kind() == "selector_expression")
    else {
        return;
    };
    let Some(method) = func
        .child_by_field_name("field")
        .map(|f| text(f, ctx.source))
    else {
        return;
    };
    if !matches!(method, "QueueBind" | "CreateSubscription") {
        return;
    }
    let Some(operand) = func.child_by_field_name("operand") else {
        return;
    };
    let Some(owner) = ctx.ty_of(operand, 0, 0) else {
        return;
    };
    let args = args_of(call);
    match (owner, method) {
        (Ty::AmqpChannel, "QueueBind") if args.len() >= 3 => {
            let (Some(queue), Some(key), exchange) = (
                ctx.name_of(args[0], 0).filter(|q| !q.is_empty()),
                ctx.name_of(args[1], 0),
                ctx.name_of(args[2], 0),
            ) else {
                return;
            };
            // A fanout binding has no key: the exchange is the channel.
            let bound = if key.is_empty() {
                exchange.filter(|e| !e.is_empty())
            } else {
                Some(key)
            };
            if let Some(b) = bound.filter(|b| usable_name(b) && !b.contains('#')) {
                let keys = ctx.binds.entry(queue).or_default();
                if keys.len() < MAX_BINDS_PER_QUEUE && !keys.contains(&b) {
                    keys.push(b);
                }
            }
        }
        (Ty::PsClient, "CreateSubscription") if args.len() >= 3 => {
            let Some(id) = ctx.name_of(args[1], 0).filter(|i| !i.is_empty()) else {
                return;
            };
            let topic = ctx
                .composite_of(args[2], 0)
                .and_then(|lit| ctx.composite_field(lit, "Topic"))
                .and_then(|v| ctx.ty_of(v, 0, 0));
            if let Some(Ty::PsTopic(Some(t))) = topic {
                ctx.sub_topics.insert(id, t);
            }
        }
        _ => {}
    }
}

fn site_calls<'t>(call: Node<'t>, ctx: &mut Ctx<'t>, out: &mut Scan) {
    let Some(func) = call
        .child_by_field_name("function")
        .filter(|f| f.kind() == "selector_expression")
    else {
        return;
    };
    let (Some(operand), Some(method)) = (
        func.child_by_field_name("operand"),
        func.child_by_field_name("field")
            .map(|f| text(f, ctx.source)),
    ) else {
        return;
    };
    if ctx.package_of(operand) == Some(Kind::Franz) {
        franz_call(call, method, ctx, out);
        return;
    }
    let Some(owner) = ctx.ty_of(operand, 0, 0) else {
        return;
    };
    // A method of a type this file declares is the file's own call, whatever it is named: the
    // Kafka heuristics of `go.rs` must not read a `Subscribe("x")` of it.
    // A wrapper of a broker client (a struct of the file holding one) keeps its older reading:
    // its `Subscribe("x")` is the Kafka consumer it always was.
    if let Ty::Local(s) = &owner {
        if ctx.local_methods.contains(&(s.clone(), method.to_string()))
            && !ctx.wrappers.contains_key(s)
        {
            out.claimed.insert(call.start_byte());
        }
        return;
    }
    let args = args_of(call);
    let Action::Hits {
        producer,
        broker,
        keys,
    } = dispatch(&owner, method, &args, ctx)
    else {
        return;
    };
    let mut keys: Vec<String> = keys.into_iter().filter(|k| usable_name(k)).collect();
    keys.sort();
    keys.dedup();
    keys.retain(|k| {
        !(producer && is_pattern(k)) && !(broker == Broker::RabbitMq && k.contains('#'))
    });
    // A call the cap would cut is not claimed: the older heuristics keep what they read on base.
    if !out.has_room(keys.len()) {
        return;
    }
    out.claimed.insert(call.start_byte());
    let via = CALL_NAMES
        .iter()
        .copied()
        .find(|m| *m == method)
        .unwrap_or("call");
    for key in keys {
        out.sites.push(Site {
            producer,
            broker,
            key,
            lines: lines(call),
            via,
        });
    }
}

fn hits(producer: bool, broker: Broker, keys: Vec<String>) -> Action {
    Action::Hits {
        producer,
        broker,
        keys,
    }
}

/// The table of what a method of a client does.
fn dispatch<'t>(owner: &Ty, method: &str, args: &[Node<'t>], ctx: &mut Ctx<'t>) -> Action {
    let name_at = |ctx: &mut Ctx<'t>, i: usize| -> Vec<String> {
        args.get(i)
            .and_then(|a| ctx.name_of(*a, 0))
            .into_iter()
            .collect()
    };
    match (owner, method) {
        // ---- RabbitMQ
        (Ty::AmqpChannel, "Publish" | "PublishWithDeferredConfirm") => {
            hits(true, Broker::RabbitMq, rabbit_publish(args, 0, ctx))
        }
        (Ty::AmqpChannel, "PublishWithContext" | "PublishWithDeferredConfirmWithContext") => {
            hits(true, Broker::RabbitMq, rabbit_publish(args, 1, ctx))
        }
        (Ty::AmqpChannel, "Consume" | "Get") => {
            hits(false, Broker::RabbitMq, rabbit_consume(args, 0, ctx))
        }
        (Ty::AmqpChannel, "ConsumeWithContext") => {
            hits(false, Broker::RabbitMq, rabbit_consume(args, 1, ctx))
        }
        // ---- NATS
        (Ty::NatsConn, "Publish" | "Request" | "PublishRequest" | "BindSendChan")
        | (Ty::NatsJs, "Publish" | "PublishAsync")
        | (Ty::NatsJs2, "PublishAsync") => hits(true, Broker::Nats, name_at(ctx, 0)),
        (Ty::NatsConn, "RequestWithContext") | (Ty::NatsJs2, "Publish") => {
            hits(true, Broker::Nats, name_at(ctx, 1))
        }
        (Ty::NatsConn, "PublishMsg" | "RequestMsg")
        | (Ty::NatsJs, "PublishMsg" | "PublishMsgAsync")
        | (Ty::NatsJs2, "PublishMsgAsync") => hits(true, Broker::Nats, nats_msg(args, 0, ctx)),
        (Ty::NatsConn, "RequestMsgWithContext") | (Ty::NatsJs2, "PublishMsg") => {
            hits(true, Broker::Nats, nats_msg(args, 1, ctx))
        }
        (
            Ty::NatsConn | Ty::NatsJs,
            "Subscribe"
            | "QueueSubscribe"
            | "ChanSubscribe"
            | "ChanQueueSubscribe"
            | "SubscribeSync"
            | "QueueSubscribeSync"
            | "QueueSubscribeSyncWithChan",
        )
        | (Ty::NatsConn, "BindRecvChan" | "BindRecvQueueChan")
        | (Ty::NatsJs, "PullSubscribe") => hits(false, Broker::Nats, name_at(ctx, 0)),
        // ---- Redis
        (Ty::Redis { ctx: c }, "Publish" | "SPublish") => {
            hits(true, Broker::Redis, name_at(ctx, usize::from(*c)))
        }
        (
            Ty::Redis { ctx: c } | Ty::RedisPubSub { ctx: c },
            "Subscribe" | "PSubscribe" | "SSubscribe",
        ) => {
            let channels: Vec<String> = args
                .iter()
                .skip(usize::from(*c))
                .filter_map(|a| ctx.name_of(*a, 0))
                .collect();
            hits(false, Broker::Redis, channels)
        }
        // ---- SQS / SNS
        (
            Ty::Sqs,
            "SendMessage"
            | "SendMessageWithContext"
            | "SendMessageBatch"
            | "SendMessageBatchWithContext",
        ) => {
            let name = ctx.aws_input_field_of(
                args,
                Kind::Sqs,
                "QueueUrl",
                &["SendMessageInput", "SendMessageBatchInput"],
            );
            hits(true, Broker::Sqs, name.into_iter().collect())
        }
        (Ty::Sqs, "ReceiveMessage" | "ReceiveMessageWithContext") => {
            let name =
                ctx.aws_input_field_of(args, Kind::Sqs, "QueueUrl", &["ReceiveMessageInput"]);
            hits(false, Broker::Sqs, name.into_iter().collect())
        }
        (
            Ty::Sns,
            "Publish" | "PublishWithContext" | "PublishBatch" | "PublishBatchWithContext",
        ) => {
            let name = ctx.aws_input_field_of(
                args,
                Kind::Sns,
                "TopicArn",
                &["PublishInput", "PublishBatchInput"],
            );
            hits(true, Broker::Sns, name.into_iter().collect())
        }
        // ---- Kafka through sarama's consumer group
        (Ty::SaramaGroup, "Consume") => {
            let topics = args
                .get(1)
                .and_then(|a| ctx.composite_of(*a, 0))
                .map(|lit| ctx.string_elements(lit))
                .unwrap_or_default();
            hits(false, Broker::Kafka, topics)
        }
        // ---- Pub/Sub
        (Ty::PsTopic(topic), "Publish") => {
            hits(true, Broker::PubSub, topic.iter().cloned().collect())
        }
        (Ty::PsSub(id), "Receive") => {
            let topic = id.as_ref().and_then(|i| ctx.sub_topics.get(i)).cloned();
            hits(false, Broker::PubSub, topic.into_iter().collect())
        }
        _ => Action::None,
    }
}

/// `(exchange, key, ...)` at offset `off` (1 when a context comes first): the routing key, or
/// the exchange when the key is empty (a fanout publish). The default exchange (`""`) with a key
/// is the queue named by the key.
fn rabbit_publish<'t>(args: &[Node<'t>], off: usize, ctx: &mut Ctx<'t>) -> Vec<String> {
    let (Some(exchange), Some(key)) = (args.get(off), args.get(off + 1)) else {
        return Vec::new();
    };
    match ctx.name_of(*key, 0) {
        Some(k) if !k.is_empty() => vec![k],
        Some(_) => ctx
            .name_of(*exchange, 0)
            .filter(|e| !e.is_empty())
            .into_iter()
            .collect(),
        None => Vec::new(),
    }
}

/// The queue at `args[idx]`, or the routing keys the file binds it with.
fn rabbit_consume<'t>(args: &[Node<'t>], idx: usize, ctx: &mut Ctx<'t>) -> Vec<String> {
    let Some(queue) = args
        .get(idx)
        .and_then(|a| ctx.name_of(*a, 0))
        .filter(|q| !q.is_empty())
    else {
        return Vec::new();
    };
    match ctx.binds.get(&queue) {
        Some(keys) if !keys.is_empty() => keys.clone(),
        _ => vec![queue],
    }
}

fn nats_msg<'t>(args: &[Node<'t>], idx: usize, ctx: &mut Ctx<'t>) -> Vec<String> {
    args.get(idx)
        .and_then(|m| ctx.nats_msg_subject(*m, 0))
        .into_iter()
        .collect()
}

/// Registers a [`Scan`] in `out`, whose `nodes` the Go extractor has already filled.
pub(crate) fn apply(scan: Scan, out: &mut FileIndex) {
    out.go_brokers_capped |= scan.capped;
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
                "go-broker {} {}",
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
