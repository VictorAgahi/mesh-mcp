//! Reads a declared AsyncAPI 2.x / 3.x document into the few facts the diff compares: channels
//! (address, servers, messages with their payload fields) and operations (send or receive).
//!
//! The document is streamed through `mesh_core::yaml_stream` (an alias bomb ingests nothing) and
//! only walked through the keys the diff needs. Local `$ref`s (`#/components/...`) are followed
//! up to [`MAX_REF_HOPS`] hops; an external `$ref` is counted in `unread_refs` and never guessed.
//! Nothing here recurses: the document is read one level at a time.
//!
//! Version 2 words the direction from the API user's side: a `publish` operation is a message
//! the described application *receives*, a `subscribe` one a message it *sends*. Version 3 says
//! `send` / `receive` from the application's side.

use mesh_core::{normalize_topic_key, split_qualified_topic_key, Broker};
use serde_yaml::Value;

/// Channels read per document; past it the rest are counted, not read.
pub const MAX_DECLARED_CHANNELS: usize = 2_000;
const MAX_REF_HOPS: usize = 8;
const MAX_FIELDS: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Action {
    Send,
    Receive,
}

impl Action {
    pub fn word(self) -> &'static str {
        match self {
            Action::Send => "send",
            Action::Receive => "receive",
        }
    }
}

/// A server of the document (`servers.<id>.protocol`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeclaredServer {
    pub id: String,
    pub protocol: String,
}

/// One declared payload property.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeclaredField {
    pub name: String,
    /// `string`, `number` (also `integer`), `boolean` or an array of one (`string[]`); `None`
    /// for an object, an untyped property or a `$ref`.
    pub ty: Option<String>,
    pub required: bool,
}

/// A declared payload schema, as far as it is an object with `properties`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeclaredSchema {
    /// The last segment of the `$ref`, else the schema's `title`.
    pub name: Option<String>,
    pub fields: Vec<DeclaredField>,
    /// The properties are all the schema says: no `oneOf` / `anyOf` / `allOf`, no
    /// `additionalProperties: true`, an object, at most 64 properties, every `$ref` read.
    pub complete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeclaredMessage {
    pub name: String,
    pub schema: Option<DeclaredSchema>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeclaredOp {
    pub id: String,
    pub action: Action,
    pub line: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeclaredChannel {
    /// The channel id (its key under `channels`).
    pub id: String,
    /// The address as written (version 3 `address`, version 2 the channel name).
    pub address: String,
    /// The engine's key for the address: normalized, `{param}` read as a one-token wildcard,
    /// broker qualifier (`kafka:orders`) split off.
    pub key: String,
    /// A broker the address names itself.
    pub broker: Option<Broker>,
    pub line: Option<u32>,
    /// The server ids the channel is on; empty: every server of the document.
    pub servers: Vec<String>,
    pub messages: Vec<DeclaredMessage>,
    pub ops: Vec<DeclaredOp>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeclaredDoc {
    /// The file, as the caller labels it (`path`, relative to its root).
    pub file: String,
    pub version: String,
    pub servers: Vec<DeclaredServer>,
    pub channels: Vec<DeclaredChannel>,
    /// Version 3 channels with no static `address` (dynamic): not compared.
    pub no_address: usize,
    /// External or dangling `$ref`s met: what they point to was not read.
    pub unread_refs: usize,
    /// Channels past [`MAX_DECLARED_CHANNELS`], not read.
    pub channels_cut: usize,
    /// Operations naming a channel the document does not declare.
    pub orphan_ops: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeclaredParse {
    Doc(DeclaredDoc),
    /// A YAML document with no top-level `asyncapi` version.
    NotAsyncApi,
    /// An `asyncapi` version other than 2.x or 3.x.
    Unsupported(String),
    Unreadable(String),
}

fn scalar(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// `#/a/b` -> the node, `~1` and `~0` unescaped.
fn pointer<'a>(root: &'a Value, ptr: &str) -> Option<&'a Value> {
    let mut cur = root;
    for seg in ptr.strip_prefix('/')?.split('/') {
        let seg = seg.replace("~1", "/").replace("~0", "~");
        cur = match cur {
            Value::Mapping(m) => m.get(seg.as_str())?,
            Value::Sequence(s) => s.get(seg.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(cur)
}

/// `v`, or what its local `$ref` chain points to. An external `$ref`, a dangling one or a chain
/// past [`MAX_REF_HOPS`] is counted in `unread` and gives `None`.
fn deref<'a>(root: &'a Value, v: &'a Value, unread: &mut usize) -> Option<&'a Value> {
    let mut cur = v;
    for _ in 0..MAX_REF_HOPS {
        let Some(r) = cur.get("$ref").and_then(Value::as_str) else {
            return Some(cur);
        };
        match r.strip_prefix('#').and_then(|p| pointer(root, p)) {
            Some(next) => cur = next,
            None => {
                *unread += 1;
                return None;
            }
        }
    }
    *unread += 1;
    None
}

fn ref_name(v: &Value) -> Option<String> {
    let r = v.get("$ref")?.as_str()?;
    r.rsplit('/')
        .next()
        .map(|s| s.replace("~1", "/").replace("~0", "~"))
}

/// The JSON family of a property schema (`string`, `number`, `boolean`, arrays of them).
fn family(root: &Value, prop: &Value, unread: &mut usize) -> Option<String> {
    let p = deref(root, prop, unread)?;
    match p.get("type").and_then(Value::as_str)? {
        "string" => Some("string".into()),
        "number" | "integer" => Some("number".into()),
        "boolean" => Some("boolean".into()),
        "array" => {
            let items = p.get("items")?;
            let inner = family(root, items, unread)?;
            Some(format!("{inner}[]"))
        }
        _ => None,
    }
}

fn schema_of(root: &Value, v: &Value, unread: &mut usize) -> Option<DeclaredSchema> {
    let name = ref_name(v);
    let mut obj = deref(root, v, unread)?;
    // `multiFormatSchema`: the schema is under `schema`.
    if let Some(inner) = obj
        .get("schema")
        .filter(|_| obj.get("schemaFormat").is_some())
    {
        obj = deref(root, inner, unread)?;
    }
    let name = name.or_else(|| obj.get("title").and_then(scalar));
    let mut complete = true;
    for k in ["oneOf", "anyOf", "allOf", "not"] {
        complete &= obj.get(k).is_none();
    }
    complete &= obj.get("additionalProperties") != Some(&Value::Bool(true));
    let required: Vec<&str> = obj
        .get("required")
        .and_then(Value::as_sequence)
        .map(|s| s.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    let mut fields = Vec::new();
    match obj.get("properties").and_then(Value::as_mapping) {
        Some(props) => {
            for (k, pv) in props {
                let Some(fname) = k.as_str() else {
                    complete = false;
                    continue;
                };
                if fields.len() >= MAX_FIELDS {
                    complete = false;
                    break;
                }
                fields.push(DeclaredField {
                    name: fname.to_string(),
                    ty: family(root, pv, unread),
                    required: required.contains(&fname),
                });
            }
        }
        None => complete = false,
    }
    Some(DeclaredSchema {
        name,
        fields,
        complete,
    })
}

/// The messages a version 3 channel declares (`messages.<name>`) or a version 2 operation's
/// `message` (one, or a `oneOf` list).
fn message_of(
    root: &Value,
    name: String,
    v: &Value,
    unread: &mut usize,
) -> Option<DeclaredMessage> {
    let name = ref_name(v).unwrap_or(name);
    let msg = deref(root, v, unread)?;
    let schema = msg.get("payload").and_then(|p| schema_of(root, p, unread));
    Some(DeclaredMessage { name, schema })
}

/// The declared key of an address: normalized, `{param}` as a one-token wildcard, a broker
/// qualifier split off.
fn declared_key(address: &str) -> (String, Option<Broker>) {
    let norm = normalize_topic_key(address);
    let (broker, bare) = match split_qualified_topic_key(&norm) {
        Some((b, rest)) => (Some(b), rest.to_string()),
        None => (None, norm),
    };
    let wildcard = if bare.contains('/') { "+" } else { "*" };
    let mut out = String::with_capacity(bare.len());
    let mut chars = bare.chars();
    while let Some(c) = chars.next() {
        if c == '{' {
            // Skip to the closing brace; an unclosed one is kept as written.
            if let Some(end) = chars.clone().position(|c| c == '}') {
                out.push_str(wildcard);
                chars.nth(end);
                continue;
            }
        }
        out.push(c);
    }
    (out, broker)
}

/// 1-based line of the first `key:` entry at or after line index `from`.
fn key_line(lines: &[&str], key: &str, from: usize) -> Option<(usize, u32)> {
    lines.iter().enumerate().skip(from).find_map(|(i, l)| {
        let t = l.trim_start().trim_start_matches("- ");
        let rest = t
            .strip_prefix(key)
            .or_else(|| t.strip_prefix('"')?.strip_prefix(key)?.strip_prefix('"'))
            .or_else(|| t.strip_prefix('\'')?.strip_prefix(key)?.strip_prefix('\''))?;
        rest.starts_with(':').then_some((i, i as u32 + 1))
    })
}

/// Parses one declared document. `file` is only a label carried into the result.
pub fn parse_declared(file: &str, content: &str) -> DeclaredParse {
    let root: Value = match mesh_core::yaml_stream::from_str(content) {
        Ok(v) => v,
        Err(e) => return DeclaredParse::Unreadable(e.to_string()),
    };
    let Some(version) = root.get("asyncapi").and_then(scalar) else {
        return DeclaredParse::NotAsyncApi;
    };
    let major = version.split('.').next().unwrap_or_default();
    if major != "2" && major != "3" {
        return DeclaredParse::Unsupported(version);
    }
    let v3 = major == "3";
    let lines: Vec<&str> = content.lines().collect();
    let mut doc = DeclaredDoc {
        file: file.to_string(),
        version,
        servers: Vec::new(),
        channels: Vec::new(),
        no_address: 0,
        unread_refs: 0,
        channels_cut: 0,
        orphan_ops: 0,
    };
    let mut unread = 0usize;

    if let Some(servers) = root.get("servers").and_then(Value::as_mapping) {
        for (k, s) in servers {
            let (Some(id), Some(protocol)) = (k.as_str(), s.get("protocol").and_then(scalar))
            else {
                continue;
            };
            doc.servers.push(DeclaredServer {
                id: id.to_string(),
                protocol: protocol.to_ascii_lowercase(),
            });
        }
    }

    let section = key_line(&lines, "channels", 0).map_or(0, |(i, _)| i + 1);
    let mut cursor = section;
    if let Some(channels) = root.get("channels").and_then(Value::as_mapping) {
        for (n, (k, ch)) in channels.iter().enumerate() {
            if n >= MAX_DECLARED_CHANNELS {
                doc.channels_cut = channels.len() - n;
                break;
            }
            let Some(id) = k.as_str() else {
                continue;
            };
            let line = key_line(&lines, id, cursor).map(|(i, l)| {
                cursor = i + 1;
                l
            });
            let Some(chan) = deref(&root, ch, &mut unread) else {
                continue;
            };
            let address = if v3 {
                match chan
                    .get("address")
                    .and_then(scalar)
                    .filter(|a| !a.is_empty())
                {
                    Some(a) => a,
                    None => {
                        doc.no_address += 1;
                        continue;
                    }
                }
            } else {
                id.to_string()
            };
            let (key, broker) = declared_key(&address);
            let servers = chan
                .get("servers")
                .and_then(Value::as_sequence)
                .map(|s| {
                    s.iter()
                        .filter_map(|e| ref_name(e).or_else(|| scalar(e)))
                        .collect()
                })
                .unwrap_or_default();
            let mut messages = Vec::new();
            let mut ops = Vec::new();
            if v3 {
                if let Some(ms) = chan.get("messages").and_then(Value::as_mapping) {
                    for (mk, mv) in ms {
                        if let Some(m) = message_of(
                            &root,
                            mk.as_str().unwrap_or_default().to_string(),
                            mv,
                            &mut unread,
                        ) {
                            messages.push(m);
                        }
                    }
                }
            } else {
                for (word, action) in [("publish", Action::Receive), ("subscribe", Action::Send)] {
                    let Some(op) = chan.get(word) else { continue };
                    let op_id = op
                        .get("operationId")
                        .and_then(scalar)
                        .unwrap_or_else(|| format!("{id}.{word}"));
                    ops.push(DeclaredOp {
                        id: op_id,
                        action,
                        line: line
                            .and_then(|l| key_line(&lines, word, l as usize).map(|(_, wl)| wl)),
                    });
                    let Some(msg) = op.get("message") else {
                        continue;
                    };
                    let list: Vec<&Value> = match msg.get("oneOf").and_then(Value::as_sequence) {
                        Some(items) => items.iter().collect(),
                        None => vec![msg],
                    };
                    for m in list {
                        let name = m
                            .get("name")
                            .and_then(scalar)
                            .unwrap_or_else(|| format!("{id}.{word}"));
                        if let Some(m) = message_of(&root, name, m, &mut unread) {
                            if !messages.contains(&m) {
                                messages.push(m);
                            }
                        }
                    }
                }
            }
            doc.channels.push(DeclaredChannel {
                id: id.to_string(),
                address,
                key,
                broker,
                line,
                servers,
                messages,
                ops,
            });
        }
    }

    if v3 {
        let section = key_line(&lines, "operations", 0).map_or(0, |(i, _)| i + 1);
        let mut cursor = section;
        if let Some(ops) = root.get("operations").and_then(Value::as_mapping) {
            for (k, op) in ops {
                let Some(op_id) = k.as_str() else { continue };
                let line = key_line(&lines, op_id, cursor).map(|(i, l)| {
                    cursor = i + 1;
                    l
                });
                let Some(op) = deref(&root, op, &mut unread) else {
                    continue;
                };
                let action = match op.get("action").and_then(Value::as_str) {
                    Some("send") => Action::Send,
                    Some("receive") => Action::Receive,
                    _ => continue,
                };
                let target = op.get("channel").and_then(|c| {
                    c.get("$ref")
                        .and_then(Value::as_str)
                        .and_then(|r| r.strip_prefix("#/channels/"))
                        .map(|s| s.replace("~1", "/").replace("~0", "~"))
                });
                match target.and_then(|id| doc.channels.iter_mut().find(|c| c.id == id)) {
                    Some(ch) => ch.ops.push(DeclaredOp {
                        id: op_id.to_string(),
                        action,
                        line,
                    }),
                    None => doc.orphan_ops += 1,
                }
            }
        }
    }
    doc.unread_refs = unread;
    DeclaredParse::Doc(doc)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The parsed document; anything else is an empty document that fails every assertion.
    fn doc(yaml: &str) -> DeclaredDoc {
        match parse_declared("asyncapi.yaml", yaml) {
            DeclaredParse::Doc(d) => d,
            _ => DeclaredDoc {
                file: String::new(),
                version: "NOT A DOCUMENT".into(),
                servers: Vec::new(),
                channels: Vec::new(),
                no_address: 0,
                unread_refs: 0,
                channels_cut: 0,
                orphan_ops: 0,
            },
        }
    }

    const V3: &str = "\
asyncapi: 3.0.0
servers:
  broker:
    host: kafka.example.com:9092
    protocol: Kafka
channels:
  orderCreated:
    address: Orders.Created
    servers:
      - $ref: '#/servers/broker'
    messages:
      created:
        payload:
          $ref: '#/components/schemas/OrderCreated'
  audit:
    address: 'audit/{region}/log'
  dynamic:
    address: null
operations:
  sendCreated:
    action: send
    channel:
      $ref: '#/channels/orderCreated'
  gone:
    action: receive
    channel:
      $ref: '#/channels/missing'
components:
  schemas:
    OrderCreated:
      type: object
      required: [orderId, total]
      properties:
        orderId: {type: string}
        total: {type: integer}
        tags: {type: array, items: {type: string}}
        meta: {$ref: '#/components/schemas/Meta'}
";

    #[test]
    fn v3_channels_operations_and_payloads() {
        let d = doc(V3);
        assert_eq!(d.version, "3.0.0");
        assert_eq!(d.servers[0].protocol, "kafka");
        assert_eq!(d.no_address, 1);
        assert_eq!(d.orphan_ops, 1);
        assert_eq!(d.channels.len(), 2);
        let c = &d.channels[0];
        assert_eq!(
            (c.id.as_str(), c.key.as_str(), c.line, c.servers.clone()),
            (
                "orderCreated",
                "orders.created",
                Some(7),
                vec!["broker".to_string()]
            )
        );
        assert_eq!(
            c.ops,
            vec![DeclaredOp {
                id: "sendCreated".into(),
                action: Action::Send,
                line: Some(20)
            }]
        );
        let s = c.messages[0].schema.as_ref().expect("schema");
        assert_eq!(s.name.as_deref(), Some("OrderCreated"));
        assert!(s.complete);
        let f: Vec<(&str, Option<&str>, bool)> = s
            .fields
            .iter()
            .map(|f| (f.name.as_str(), f.ty.as_deref(), f.required))
            .collect();
        assert_eq!(
            f,
            vec![
                ("orderId", Some("string"), true),
                ("total", Some("number"), true),
                ("tags", Some("string[]"), false),
                ("meta", None, false),
            ]
        );
        // `{region}` is a one-token wildcard of the `/` dialect.
        assert_eq!(d.channels[1].key, "audit/+/log");
    }

    #[test]
    fn v2_publish_is_receive_and_subscribe_is_send() {
        let d = doc("\
asyncapi: 2.6.0
channels:
  user.signedup:
    publish:
      operationId: onSignedUp
      message:
        payload:
          type: object
          properties:
            id: {type: string}
    subscribe:
      message:
        payload: {$ref: 'other.yaml#/Thing'}
");
        let c = &d.channels[0];
        assert_eq!(c.key, "user.signedup");
        let ops: Vec<(&str, Action)> = c.ops.iter().map(|o| (o.id.as_str(), o.action)).collect();
        assert_eq!(
            ops,
            vec![
                ("onSignedUp", Action::Receive),
                ("user.signedup.subscribe", Action::Send)
            ]
        );
        // The external `$ref` is counted, never followed or guessed.
        assert_eq!(d.unread_refs, 1);
        assert!(c.messages[0].schema.as_ref().expect("schema").complete);
    }

    #[test]
    fn qualified_address_and_incomplete_schemas() {
        let d = doc("\
asyncapi: 3.0.0
channels:
  c:
    address: kafka:Orders
    messages:
      m:
        payload:
          allOf:
            - properties: {a: {type: string}}
");
        assert_eq!(d.channels[0].key, "orders");
        assert_eq!(d.channels[0].broker, Some(Broker::Kafka));
        let s = d.channels[0].messages[0].schema.as_ref().expect("schema");
        assert!(!s.complete);
    }

    #[test]
    fn other_documents_are_said_so() {
        assert_eq!(
            parse_declared("x", "openapi: 3.0.0\npaths: {}\n"),
            DeclaredParse::NotAsyncApi
        );
        assert_eq!(
            parse_declared("x", "asyncapi: 1.2.0\n"),
            DeclaredParse::Unsupported("1.2.0".into())
        );
        assert!(matches!(
            parse_declared("x", "a: [unclosed\n"),
            DeclaredParse::Unreadable(_)
        ));
    }

    #[test]
    fn unclosed_brace_is_kept_and_alias_bomb_is_bounded() {
        assert_eq!(declared_key("a.{b").0, "a.{b");
        let mut bomb = String::from("asyncapi: 3.0.0\nx: &a [1,1,1,1,1,1,1,1,1,1]\n");
        for (i, prev) in ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"]
            .iter()
            .enumerate()
        {
            let name = ["b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m"][i];
            bomb.push_str(&format!("{name}: &{name} [*{prev},*{prev},*{prev},*{prev},*{prev},*{prev},*{prev},*{prev},*{prev},*{prev}]\n"));
        }
        assert!(matches!(
            parse_declared("x", &bomb),
            DeclaredParse::Unreadable(_)
        ));
    }
}
