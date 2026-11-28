//! The observed event graph as an AsyncAPI 3.0 document (`export_asyncapi`, mode `export`).
//!
//! Mapping, stated once (and in `docs/mcp-tools.md`):
//!
//! - a topic hub (per broker) is a **channel**: `address` is the engine's normalized topic key,
//!   the channel id is that key made safe (`kafka_orders_created`, the broker slug first when
//!   the broker is known), a wildcard subscription (`orders.*`) keeps its pattern as address;
//! - a producer site is a `send` **operation**, a consumer site a `receive` one, each with its
//!   `path:line` in `x-mesh-source` and its confidence in `x-mesh-confidence`;
//! - a payload type T11 resolved is a **message** whose payload is a **schema** (properties by
//!   JSON family, `required` for the non-optional fields, declaration `path:line`); a site
//!   whose payload is not statically typed references no message and says so;
//! - a known broker is a **server** (`host` is not statically known: `unresolved.invalid`, the
//!   RFC 6761 reserved name, with `x-mesh-host-known: false`); an unknown broker has no server
//!   and is stated `heuristic`, never guessed;
//! - a topic named by a `${KEY}` placeholder is NOT a channel: it is listed under `x-mesh`
//!   with a count ("not statically resolved" is not absence).
//!
//! Credential-shaped topic or type names never reach the document: a channel whose name fails
//! the shared name gate is counted as unresolved without its name, a type whose name fails it
//! is "redacted", a credential-shaped field is omitted and counted.
//!
//! A page holds `limit` channels at most and fewer when the document would pass the budget
//! (never fewer than one); it is a valid standalone document with `x-mesh.partial: true` and
//! the number of channels not shown, never presented as the whole graph.

use super::{ObsPayload, ObsType, Observed, ObservedSet};
use crate::anomalies::{shown_key, shown_name, REDACTED};
use mesh_core::{payload_json_family, Broker};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

/// Bytes of the document in one page.
const BUDGET: usize = 28 * 1024;
/// Operations kept per direction on one channel; the rest are counted.
const OPS_PER_DIRECTION: usize = 40;
/// Unresolved topics listed in the document.
const UNRESOLVED_LISTED: usize = 25;
const HOST_UNKNOWN: &str = "unresolved.invalid";

/// How one page of the export is shown.
#[derive(Debug, Clone)]
pub struct ExportView<'a> {
    pub offset: usize,
    pub limit: usize,
    /// JSON instead of YAML.
    pub json: bool,
    pub scope: Option<&'a str>,
    pub coverage: Option<String>,
    pub roots: usize,
}

/// `[A-Za-z0-9._-]` only, runs of anything else folded to one `_`.
fn slug(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if c.is_ascii_alphanumeric() || c == '-' || c == '.' {
            out.push(c);
        } else if !out.ends_with('_') {
            out.push('_');
        }
    }
    let t = out.trim_matches(|c| c == '_' || c == '.');
    if t.is_empty() {
        "x".to_string()
    } else {
        t.to_string()
    }
}

/// `base`, or `base_2`, `base_3` while `taken` holds it; inserts the result.
fn unique(taken: &mut BTreeSet<String>, base: &str) -> String {
    let mut name = base.to_string();
    let mut n = 2;
    while !taken.insert(name.clone()) {
        name = format!("{base}_{n}");
        n += 1;
    }
    name
}

fn protocol(b: Broker) -> &'static str {
    match b {
        Broker::RabbitMq => "amqp",
        Broker::PubSub => "googlepubsub",
        other => other.slug(),
    }
}

/// The schema of one payload type, with the number of credential-shaped fields left out.
fn schema_of(t: &ObsType) -> (Value, usize) {
    let mut props = Map::new();
    let mut required: Vec<String> = Vec::new();
    let mut omitted = 0usize;
    for f in &t.fields {
        let name = shown_name(&f.name);
        if name == REDACTED {
            omitted += 1;
            continue;
        }
        let mut prop = Map::new();
        match f.ty.as_deref().and_then(payload_json_family) {
            Some(fam) => match fam.strip_suffix("[]") {
                Some(inner) => {
                    prop.insert("type".into(), json!("array"));
                    prop.insert("items".into(), json!({ "type": inner }));
                }
                None => {
                    prop.insert("type".into(), json!(fam));
                }
            },
            None => {
                // A named object, a date, an enum: no JSON type is claimed. The declared type is
                // kept only when it passes the name gate.
                if let Some(ty) = f.ty.as_deref().map(shown_name).filter(|n| n != REDACTED) {
                    prop.insert("x-mesh-declared-type".into(), json!(ty));
                }
            }
        }
        props.insert(name.clone(), Value::Object(prop));
        if !f.optional {
            required.push(name);
        }
    }
    required.sort();
    let mut schema = Map::new();
    schema.insert("type".into(), json!("object"));
    schema.insert("properties".into(), Value::Object(props));
    if !required.is_empty() {
        schema.insert("required".into(), json!(required));
    }
    schema.insert("x-mesh-source".into(), json!(t.loc.replace('`', "'")));
    if t.incomplete {
        schema.insert("x-mesh-fields-incomplete".into(), json!(true));
    }
    if omitted > 0 {
        schema.insert("x-mesh-fields-redacted".into(), json!(omitted));
    }
    (Value::Object(schema), omitted)
}

struct Row<'a> {
    id: String,
    key: String,
    o: &'a Observed,
    /// Per type of `o`: the component name of its message and schema, `None` when the type's
    /// name is credential-shaped.
    components: Vec<Option<String>>,
}

/// Rows for every printable channel, ids and component names assigned over all of them so a
/// channel's id never depends on the page or the scope.
fn rows(set: &ObservedSet) -> (Vec<Row<'_>>, Vec<&Observed>) {
    let mut ids = BTreeSet::new();
    let mut names = BTreeSet::new();
    let mut by_type: BTreeMap<(String, String), String> = BTreeMap::new();
    let mut redacted: Vec<&Observed> = Vec::new();
    let mut out = Vec::new();
    for o in &set.channels {
        let key = shown_key(&o.key);
        if key == REDACTED {
            redacted.push(o);
            continue;
        }
        let base = match o.broker {
            Some(b) => format!("{}_{}", b.slug(), slug(&key)),
            None => slug(&key),
        };
        let id = unique(&mut ids, &base);
        let mut inline = 0;
        let components = o
            .types
            .iter()
            .map(|t| match &t.name {
                Some(n) if shown_name(n) == REDACTED => None,
                Some(n) => Some(
                    by_type
                        .entry((n.clone(), t.loc.clone()))
                        .or_insert_with(|| unique(&mut names, &slug(n)))
                        .clone(),
                ),
                None => {
                    inline += 1;
                    Some(unique(&mut names, &format!("{id}_Inline{inline}")))
                }
            })
            .collect();
        out.push(Row {
            id,
            key,
            o,
            components,
        });
    }
    (out, redacted)
}

fn page_document(rows: &[&Row<'_>], meta: Value, ops_cap: usize) -> Value {
    let mut servers = Map::new();
    let mut channels = Map::new();
    let mut operations = Map::new();
    let mut messages = Map::new();
    let mut schemas = Map::new();
    let mut op_ids = BTreeSet::new();
    for r in rows {
        let mut chan = Map::new();
        chan.insert("address".into(), json!(r.key));
        if let Some(b) = r.o.broker {
            servers.entry(b.slug().to_string()).or_insert_with(|| {
                json!({
                    "host": HOST_UNKNOWN,
                    "protocol": protocol(b),
                    "description": "Broker read from the client library the code uses; its host is not statically known.",
                    "x-mesh-host-known": false,
                })
            });
            chan.insert(
                "servers".into(),
                json!([{ "$ref": format!("#/servers/{}", b.slug()) }]),
            );
        }
        let mut chan_messages = Map::new();
        for (ti, t) in r.o.types.iter().enumerate() {
            let Some(name) = &r.components[ti] else {
                continue;
            };
            if !schemas.contains_key(name) {
                schemas.insert(name.clone(), schema_of(t).0);
                messages.insert(
                    name.clone(),
                    json!({
                        "name": name,
                        "payload": { "$ref": format!("#/components/schemas/{name}") },
                    }),
                );
            }
        }
        for s in &r.o.sites {
            if let ObsPayload::Typed(ti) = s.payload {
                if let Some(name) = &r.components[ti] {
                    chan_messages.insert(
                        name.clone(),
                        json!({ "$ref": format!("#/components/messages/{name}") }),
                    );
                }
            }
        }
        if !chan_messages.is_empty() {
            chan.insert("messages".into(), Value::Object(chan_messages));
        }
        chan.insert(
            "x-mesh-broker".into(),
            json!(r.o.broker.map_or("unknown", Broker::slug)),
        );
        chan.insert("x-mesh-confidence".into(), json!(r.o.confidence));
        if r.o.broker.is_none() {
            chan.insert(
                "x-mesh-broker-note".into(),
                json!("heuristic: the extractor does not identify the transport; no server is claimed"),
            );
        }
        if r.o.wildcard {
            chan.insert("x-mesh-wildcard".into(), json!(true));
        }
        let untyped =
            r.o.sites
                .iter()
                .filter(|s| !matches!(s.payload, ObsPayload::Typed(_)))
                .count();
        if untyped > 0 {
            chan.insert("x-mesh-sites-without-typed-payload".into(), json!(untyped));
        }
        let mut cut = 0usize;
        for producer in [true, false] {
            let mut kept = 0usize;
            for s in r.o.sites.iter().filter(|s| s.producer == producer) {
                if kept >= ops_cap {
                    cut += 1;
                    continue;
                }
                kept += 1;
                let action = if producer { "send" } else { "receive" };
                let id = unique(&mut op_ids, &format!("{action}_{}_{}", r.id, slug(&s.loc)));
                let mut op = Map::new();
                op.insert("action".into(), json!(action));
                op.insert(
                    "channel".into(),
                    json!({ "$ref": format!("#/channels/{}", r.id) }),
                );
                match s.payload {
                    ObsPayload::Typed(ti) => match &r.components[ti] {
                        Some(name) => {
                            op.insert(
                                "messages".into(),
                                json!([{ "$ref": format!("#/channels/{}/messages/{name}", r.id) }]),
                            );
                        }
                        None => {
                            op.insert("x-mesh-payload".into(), json!("type name redacted"));
                        }
                    },
                    ObsPayload::Untyped => {
                        op.insert("x-mesh-payload".into(), json!("not statically typed"));
                    }
                    ObsPayload::Undeclared => {
                        op.insert(
                            "x-mesh-payload".into(),
                            json!("names a type no indexed file declares"),
                        );
                    }
                    ObsPayload::Ambiguous => {
                        op.insert(
                            "x-mesh-payload".into(),
                            json!("ambiguous: several declarations or two payloads on one node"),
                        );
                    }
                }
                op.insert("x-mesh-source".into(), json!(s.loc.replace('`', "'")));
                op.insert("x-mesh-confidence".into(), json!(s.confidence));
                operations.insert(id, Value::Object(op));
            }
        }
        if cut > 0 {
            chan.insert("x-mesh-operations-not-listed".into(), json!(cut));
        }
        channels.insert(r.id.clone(), Value::Object(chan));
    }
    let mut doc = Map::new();
    doc.insert("asyncapi".into(), json!("3.0.0"));
    doc.insert(
        "info".into(),
        json!({
            "title": "Observed event graph",
            "version": "0.0.0",
            "description": "Statically observed from the indexed workspace by MeshMCP (export_asyncapi). Not an authoritative contract: a topic named at runtime, built from configuration no file resolves, or living in an unindexed root is absent. Channel addresses are the engine's normalized (lower-case) topic keys.",
        }),
    );
    if !servers.is_empty() {
        doc.insert("servers".into(), Value::Object(servers));
    }
    doc.insert("channels".into(), Value::Object(channels));
    doc.insert("operations".into(), Value::Object(operations));
    let mut components = Map::new();
    if !messages.is_empty() {
        components.insert("messages".into(), Value::Object(messages));
        components.insert("schemas".into(), Value::Object(schemas));
    }
    if !components.is_empty() {
        doc.insert("components".into(), Value::Object(components));
    }
    doc.insert("x-mesh".into(), meta);
    Value::Object(doc)
}

fn render(doc: &Value, json: bool) -> String {
    if json {
        serde_json::to_string_pretty(doc).unwrap_or_default()
    } else {
        to_yaml(doc)
    }
}

/// Words a YAML 1.1 parser (PyYAML, Ruby's Psych) reads as a boolean or a null.
const YAML_RESERVED: [&str; 11] = [
    "y", "n", "yes", "no", "on", "off", "true", "false", "null", "none", "nil",
];

/// A string scalar for a YAML document that every YAML 1.1 and 1.2 parser reads back as the same
/// string: plain only when it starts with a letter, holds nothing but letters, digits and
/// `_ - . /`, and is no reserved word; anything else (numbers, `1_000`, `0x1F`, `1e3`, `.5`,
/// `1:30`, dates, `~`, a leading `$`, `#`, `*`, `&`, `!`, `%`, `@`, spaces, colons) is a JSON
/// string, which is a valid YAML double-quoted scalar.
fn yaml_string(s: &str) -> String {
    let plain = s.chars().next().is_some_and(|c| c.is_ascii_alphabetic())
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | '/'))
        && !YAML_RESERVED.contains(&s.to_ascii_lowercase().as_str());
    if plain {
        s.to_string()
    } else {
        serde_json::to_string(s).unwrap_or_else(|_| "\"\"".to_string())
    }
}

fn yaml_scalar(v: &Value) -> Option<String> {
    match v {
        Value::Null => Some("null".to_string()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Number(n) => Some(n.to_string()),
        Value::String(s) => Some(yaml_string(s)),
        Value::Array(a) if a.is_empty() => Some("[]".to_string()),
        Value::Object(m) if m.is_empty() => Some("{}".to_string()),
        _ => None,
    }
}

/// Block-style YAML of `doc` (maps sorted as `serde_json` keeps them), written with an explicit
/// work stack: a map is `key:` then its entries one level in, a sequence is `-` items, a map
/// item of a sequence is `-` then its entries one level in. Every string goes through
/// [`yaml_string`].
fn to_yaml(doc: &Value) -> String {
    let mut out = String::new();
    // (indent, lead text of the line, value)
    let mut stack: Vec<(usize, String, &Value)> = vec![(0, String::new(), doc)];
    while let Some((indent, lead, v)) = stack.pop() {
        let pad = " ".repeat(indent);
        match yaml_scalar(v) {
            Some(text) => {
                if lead.is_empty() {
                    out.push_str(&format!("{pad}{text}\n"));
                } else {
                    out.push_str(&format!("{pad}{lead} {text}\n"));
                }
            }
            None => {
                let child_indent = if lead.is_empty() { indent } else { indent + 2 };
                if !lead.is_empty() {
                    out.push_str(&format!("{pad}{lead}\n"));
                }
                match v {
                    Value::Object(m) => {
                        for (k, child) in m.iter().rev() {
                            stack.push((child_indent, format!("{}:", yaml_string(k)), child));
                        }
                    }
                    Value::Array(a) => {
                        for child in a.iter().rev() {
                            stack.push((child_indent, "-".to_string(), child));
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    out
}

fn fence_for(text: &str) -> String {
    let mut longest = 0usize;
    let mut run = 0usize;
    for c in text.chars() {
        if c == '`' {
            run += 1;
            longest = longest.max(run);
        } else {
            run = 0;
        }
    }
    "`".repeat((longest + 1).max(3))
}

/// One page of the export: Markdown with the AsyncAPI document in a fenced block.
pub fn format_export(set: &ObservedSet, view: &ExportView<'_>) -> String {
    let (all, redacted_channels) = rows(set);
    let redacted_in_scope = redacted_channels.iter().filter(|o| o.in_scope).count();
    let in_scope: Vec<&Row<'_>> = all.iter().filter(|r| r.o.in_scope).collect();
    let total = in_scope.len();
    let scope = view.scope.map_or("the whole workspace".to_string(), |s| {
        format!("`{}`", s.replace(['`', '\n', '\r'], "'"))
    });
    let unresolved: Vec<_> = set.unresolved.iter().filter(|u| u.in_scope).collect();
    let unresolved_count = unresolved.len() + redacted_in_scope;
    let mut out = String::from("## AsyncAPI 3.0 export of the observed event graph\n");
    let producers: usize = in_scope
        .iter()
        .map(|r| r.o.sites.iter().filter(|s| s.producer).count())
        .sum();
    let consumers: usize = in_scope
        .iter()
        .map(|r| r.o.sites.iter().filter(|s| !s.producer).count())
        .sum();
    let _ = writeln!(
        out,
        "*{total} channel(s) in {scope} over {} root(s), sorted by address then broker: {producers} `send` and {consumers} `receive` operation(s). Test-file sites ({}) and the channels a declared AsyncAPI file contributes ({}) are not counted.*\n",
        view.roots, set.test_sites, set.declared_sites
    );

    let start = view.offset.min(total);
    let mut n = view.limit.min(total - start);
    let mut ops_cap = OPS_PER_DIRECTION;
    let (text, shown) = loop {
        let page = &in_scope[start..start + n];
        let not_shown = total - start - n;
        let listed: Vec<Value> = if start == 0 {
            unresolved
                .iter()
                .take(UNRESOLVED_LISTED)
                .map(|u| {
                    json!({
                        "name": shown_key(&u.key),
                        "broker": u.broker.map_or("unknown", Broker::slug),
                        "reason": "name is a ${KEY} placeholder no configuration file resolved",
                        "sources": u.sites.iter().take(3).map(|s| s.replace('`', "'")).collect::<Vec<_>>(),
                    })
                })
                .collect()
        } else {
            Vec::new()
        };
        let mut meta = Map::new();
        meta.insert("generator".into(), json!("MeshMCP export_asyncapi"));
        meta.insert("partial".into(), json!(not_shown > 0 || start > 0));
        meta.insert("channels_in_scope".into(), json!(total));
        meta.insert("channels_in_document".into(), json!(n));
        meta.insert("channels_before_document".into(), json!(start));
        meta.insert("channels_not_shown_after".into(), json!(not_shown));
        if not_shown > 0 {
            meta.insert("next_offset".into(), json!(start + n));
        }
        meta.insert(
            "not_statically_resolved".into(),
            json!({
                "count": unresolved_count,
                "note": "topics whose name is built from configuration no file resolved, or that is not a printable name; they are NOT channels of this document and their absence here proves nothing",
                "topics_listed_on_first_page": listed,
            }),
        );
        let doc = page_document(page, Value::Object(meta), ops_cap);
        let text = render(&doc, view.json);
        if text.len() > BUDGET {
            if n > 1 {
                n -= 1;
                continue;
            }
            // One channel alone is over the budget: its operations are cut (and counted in
            // `x-mesh-operations-not-listed`) until it fits, never fewer than one per direction.
            if ops_cap > 1 {
                ops_cap /= 2;
                continue;
            }
        }
        break (text, n);
    };

    let not_shown = total - start - shown;
    if total == 0 {
        let _ = writeln!(
            out,
            "No channel with a statically resolved topic name is in this scope, so there is no document. That is not proof there are none: a topic named at runtime or in an unindexed root is not seen."
        );
    } else if start >= total {
        let _ = writeln!(
            out,
            "Offset {start} is past the last channel ({total} in scope)."
        );
    } else {
        if not_shown > 0 || start > 0 {
            let _ = writeln!(
                out,
                "> **PARTIAL DOCUMENT**: channels {}-{} of {total}. {} channel(s) are not in this page{}. It is a valid AsyncAPI document of its own channels, never the whole graph.\n",
                start + 1,
                start + shown,
                start + not_shown,
                if not_shown > 0 {
                    format!("; next page: `offset: {}`", start + shown)
                } else {
                    String::new()
                }
            );
        }
        let fence = fence_for(&text);
        let _ = writeln!(
            out,
            "{fence}{}\n{}{fence}\n",
            if view.json { "json" } else { "yaml" },
            if text.ends_with('\n') {
                text
            } else {
                format!("{text}\n")
            }
        );
    }
    if unresolved_count > 0 {
        let _ = writeln!(
            out,
            "- Not statically resolved, not exported as channels: {unresolved_count} topic(s) (a `${{KEY}}` placeholder no configuration resolved, or a name that is not printable){}.",
            if start == 0 && !unresolved.is_empty() { ", listed under `x-mesh.not_statically_resolved`" } else { "" }
        );
    }
    if let Some(c) = &view.coverage {
        let _ = writeln!(out, "- Coverage: {c}.");
    }
    out.push_str("- Brokers come from the client library of each site; a channel with no broker is `heuristic` and has no server. Servers carry no real host.\n");
    out
}

#[cfg(test)]
mod tests {
    use super::super::{ObsField, ObsSite};
    use super::*;

    fn o(key: &str, broker: Option<Broker>, sites: Vec<ObsSite>, types: Vec<ObsType>) -> Observed {
        Observed {
            key: key.into(),
            broker,
            confidence: "exact",
            wildcard: false,
            sites,
            types,
            in_scope: true,
        }
    }

    fn s(producer: bool, loc: &str, p: ObsPayload) -> ObsSite {
        ObsSite {
            producer,
            loc: loc.into(),
            confidence: "exact",
            payload: p,
        }
    }

    fn view(offset: usize, limit: usize) -> ExportView<'static> {
        ExportView {
            offset,
            limit,
            json: true,
            scope: None,
            coverage: None,
            roots: 1,
        }
    }

    fn document(text: &str) -> Value {
        let start = text.find("```json\n").expect("fence") + 8;
        let end = text[start..].find("\n```").expect("end") + start;
        serde_json::from_str(&text[start..end]).expect("json")
    }

    #[test]
    fn ids_are_safe_unique_and_independent_of_the_page() {
        let t = ObsType {
            name: Some("Order".into()),
            loc: "src/o.ts:2".into(),
            fields: vec![
                ObsField {
                    name: "id".into(),
                    ty: Some("string".into()),
                    optional: false,
                },
                ObsField {
                    name: "tags".into(),
                    ty: Some("string[]".into()),
                    optional: true,
                },
                ObsField {
                    name: "at".into(),
                    ty: Some("Date".into()),
                    optional: false,
                },
            ],
            incomplete: false,
        };
        let set = ObservedSet {
            channels: vec![
                o(
                    "a.b",
                    None,
                    vec![s(true, "x/y.ts:1", ObsPayload::Typed(0))],
                    vec![t.clone()],
                ),
                // `a/b` and `a_b` fold to one safe id: the second one is suffixed.
                o(
                    "a/b",
                    None,
                    vec![s(false, "x/y.ts:2", ObsPayload::Untyped)],
                    vec![],
                ),
                o(
                    "a_b",
                    None,
                    vec![s(false, "x/y.ts:4", ObsPayload::Untyped)],
                    vec![],
                ),
                o(
                    "a.b",
                    Some(Broker::Kafka),
                    vec![s(true, "x/z.ts:3", ObsPayload::Undeclared)],
                    vec![],
                ),
            ],
            ..ObservedSet::default()
        };
        let full = document(&format_export(&set, &view(0, 10)));
        let chans: Vec<&String> = full["channels"]
            .as_object()
            .expect("channels")
            .keys()
            .collect();
        assert_eq!(chans, vec!["a.b", "a_b", "a_b_2", "kafka_a.b"]);
        // Page 3 alone: the channel keeps the id it has in the whole document.
        let third = document(&format_export(&set, &view(2, 1)));
        assert_eq!(third["channels"]["a_b_2"]["address"], json!("a_b"));
        // Schema: JSON families, required for non-optional fields, no type claimed for `Date`.
        let schema = &full["components"]["schemas"]["Order"];
        assert_eq!(schema["required"], json!(["at", "id"]));
        assert_eq!(
            schema["properties"]["tags"],
            json!({"type": "array", "items": {"type": "string"}})
        );
        assert_eq!(
            schema["properties"]["at"],
            json!({"x-mesh-declared-type": "Date"})
        );
        assert_eq!(schema["x-mesh-source"], json!("src/o.ts:2"));
    }

    #[test]
    fn credential_shaped_names_never_reach_the_document() {
        let secret = ["pass", "word"].concat();
        // A token-shaped topic name (`tok_...`) fails the shared name gate.
        let canary = ["tok", "_", "a1b2c3"].concat();
        let t = ObsType {
            name: Some(format!("{secret}Payload")),
            loc: "src/o.ts:2".into(),
            fields: vec![
                ObsField {
                    name: secret.clone(),
                    ty: Some("string".into()),
                    optional: false,
                },
                ObsField {
                    name: "id".into(),
                    ty: Some("string".into()),
                    optional: false,
                },
            ],
            incomplete: false,
        };
        let t2 = ObsType {
            name: Some("Safe".into()),
            ..t.clone()
        };
        let set = ObservedSet {
            channels: vec![
                o(
                    "orders",
                    None,
                    vec![s(true, "a.ts:1", ObsPayload::Typed(0))],
                    vec![t],
                ),
                o(
                    "pay",
                    None,
                    vec![s(true, "a.ts:2", ObsPayload::Typed(0))],
                    vec![t2],
                ),
                o(
                    &canary,
                    None,
                    vec![s(true, "a.ts:3", ObsPayload::Untyped)],
                    vec![],
                ),
            ],
            ..ObservedSet::default()
        };
        let text = format_export(&set, &view(0, 10));
        assert!(!text.to_lowercase().contains(&secret), "{text}");
        assert!(!text.contains(&canary), "{text}");
        let d = document(&text);
        assert_eq!(
            d["components"]["schemas"]["Safe"]["x-mesh-fields-redacted"],
            json!(1)
        );
        assert_eq!(d["x-mesh"]["not_statically_resolved"]["count"], json!(1));
    }

    #[test]
    fn untyped_sites_wildcards_and_protocol_names_are_stated() {
        let mut w = o(
            "orders.*",
            Some(Broker::RabbitMq),
            vec![
                s(false, "a.ts:1", ObsPayload::Untyped),
                s(true, "a.ts:2", ObsPayload::Undeclared),
                s(true, "a.ts:3", ObsPayload::Ambiguous),
            ],
            vec![],
        );
        w.wildcard = true;
        let set = ObservedSet {
            channels: vec![w],
            ..ObservedSet::default()
        };
        let d = document(&format_export(&set, &view(0, 10)));
        let ch = &d["channels"]["rabbitmq_orders"];
        assert_eq!(ch["address"], json!("orders.*"));
        assert_eq!(ch["x-mesh-wildcard"], json!(true));
        assert_eq!(ch["x-mesh-sites-without-typed-payload"], json!(3));
        assert!(ch.get("messages").is_none());
        let ops = d["operations"].as_object().expect("ops");
        let payloads: BTreeSet<&str> = ops
            .values()
            .map(|op| op["x-mesh-payload"].as_str().expect("payload note"))
            .collect();
        assert_eq!(
            payloads,
            BTreeSet::from([
                "not statically typed",
                "names a type no indexed file declares",
                "ambiguous: several declarations or two payloads on one node",
            ])
        );
        // RabbitMQ is the `amqp` protocol, Pub/Sub `googlepubsub`; others keep their slug.
        assert_eq!(d["servers"]["rabbitmq"]["protocol"], json!("amqp"));
        assert_eq!(
            d["servers"]["rabbitmq"]["host"],
            json!("unresolved.invalid")
        );
        assert_eq!(d["servers"]["rabbitmq"]["x-mesh-host-known"], json!(false));
        assert_eq!(protocol(Broker::PubSub), "googlepubsub");
        assert_eq!(protocol(Broker::Nats), "nats");
    }

    #[test]
    fn unresolved_topics_are_listed_on_the_first_page_only_and_always_counted() {
        let set = ObservedSet {
            channels: (0..3)
                .map(|i| {
                    o(
                        &format!("t{i}"),
                        None,
                        vec![s(true, "a.ts:1", ObsPayload::Untyped)],
                        vec![],
                    )
                })
                .collect(),
            unresolved: vec![super::super::ObsUnresolved {
                key: "${LATER}".into(),
                broker: None,
                sites: vec!["a.ts:9".into()],
                in_scope: true,
            }],
            ..ObservedSet::default()
        };
        let first = document(&format_export(&set, &view(0, 1)));
        let second = document(&format_export(&set, &view(1, 1)));
        for d in [&first, &second] {
            assert_eq!(d["x-mesh"]["not_statically_resolved"]["count"], json!(1));
        }
        assert_eq!(
            first["x-mesh"]["not_statically_resolved"]["topics_listed_on_first_page"][0]["name"],
            json!("${LATER}")
        );
        assert_eq!(
            second["x-mesh"]["not_statically_resolved"]["topics_listed_on_first_page"],
            json!([])
        );
        // A channel is never listed as unresolved, nor the other way round.
        assert!(!first["channels"]
            .as_object()
            .expect("channels")
            .contains_key("${LATER}"));
    }

    #[test]
    fn every_scalar_a_yaml_1_1_or_1_2_parser_could_misread_is_quoted() {
        let risky = [
            "no",
            "on",
            "yes",
            "off",
            "y",
            "n",
            "Yes",
            "NO",
            "On",
            "OFF",
            "true",
            "False",
            "null",
            "NULL",
            "Null",
            "none",
            "~",
            "1_000",
            "0x1F",
            "0o17",
            "0b11",
            "017",
            "1e3",
            ".5",
            ".inf",
            "1:30",
            "12",
            "-3",
            "+4",
            "2024-01-01",
            "2024-01-01 10:00:00",
            "",
            "a b",
            "a: b",
            "#x",
            "$ref",
            "*a",
            "&a",
            "!t",
            "%d",
            "@a",
            "- x",
            "? x",
            "{a}",
            "[a]",
            "a#b ",
            "é",
        ];
        for name in risky {
            assert_eq!(
                yaml_string(name),
                serde_json::to_string(name).expect("json"),
                "{name:?}"
            );
        }
        for plain in [
            "order.created",
            "kafka_orders",
            "a/b",
            "x-mesh-source",
            "A1",
            "nope",
            "onward",
        ] {
            assert_eq!(yaml_string(plain), plain, "{plain}");
        }
        // A document keyed and valued by those names reads back unchanged, as strings, through
        // serde_yaml and (when one is installed) through Ruby's Psych, a YAML 1.1 parser.
        let mut m = Map::new();
        for name in risky.iter().filter(|n| !n.is_empty()) {
            m.insert((*name).to_string(), json!([*name, {"k": *name}]));
        }
        let doc = Value::Object(m);
        let text = to_yaml(&doc);
        let back: Value = serde_yaml::from_str(&text).expect("valid YAML");
        assert_eq!(back, doc, "{text}");
        let ruby = std::process::Command::new("ruby")
            .args([
                "-ryaml",
                "-rjson",
                "-e",
                "puts JSON.generate(YAML.safe_load(STDIN.read))",
            ])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .spawn();
        if let Ok(mut child) = ruby {
            use std::io::Write;
            child
                .stdin
                .take()
                .expect("stdin")
                .write_all(text.as_bytes())
                .expect("write");
            let out = child.wait_with_output().expect("ruby");
            if out.status.success() {
                let parsed: Value = serde_json::from_slice(&out.stdout).expect("json from ruby");
                assert_eq!(parsed, doc, "Psych read the document differently");
            }
        }
    }

    #[test]
    fn the_scoped_unresolved_count_leaves_out_the_other_scope() {
        let token = ["tok", "_", "a1b2c3"].concat();
        let mut elsewhere = o(
            &token,
            None,
            vec![s(true, "x/a.ts:1", ObsPayload::Untyped)],
            vec![],
        );
        elsewhere.in_scope = false;
        let here = o(
            &token,
            None,
            vec![s(true, "y/a.ts:1", ObsPayload::Untyped)],
            vec![],
        );
        let set = ObservedSet {
            channels: vec![
                elsewhere,
                here,
                o(
                    "ok",
                    None,
                    vec![s(true, "y/b.ts:1", ObsPayload::Untyped)],
                    vec![],
                ),
            ],
            unresolved: vec![
                super::super::ObsUnresolved {
                    key: "${A}".into(),
                    broker: None,
                    sites: vec!["x/c.ts:1".into()],
                    in_scope: false,
                },
                super::super::ObsUnresolved {
                    key: "${B}".into(),
                    broker: None,
                    sites: vec!["y/c.ts:1".into()],
                    in_scope: true,
                },
            ],
            ..ObservedSet::default()
        };
        let d = document(&format_export(&set, &view(0, 10)));
        // One placeholder and one credential-shaped topic are in scope; the other two are not.
        assert_eq!(d["x-mesh"]["not_statically_resolved"]["count"], json!(2));
    }

    #[test]
    fn one_huge_channel_is_cut_to_the_budget_and_says_how_many_operations_are_missing() {
        let sites = (0..40)
            .map(|j| {
                s(
                    true,
                    &format!("src/a/very/long/directory/name/for/the/budget/very/long/directory/name/for/the/budget/very/long/directory/name/for/the/budget/file{j}.ts:{j}"),
                    ObsPayload::Untyped,
                )
            })
            .chain((0..40).map(|j| {
                s(
                    false,
                    &format!("src/b/very/long/directory/name/for/the/budget/very/long/directory/name/for/the/budget/very/long/directory/name/for/the/budget/file{j}.ts:{j}"),
                    ObsPayload::Untyped,
                )
            }))
            .collect();
        let set = ObservedSet {
            channels: vec![o("hot", None, sites, vec![])],
            ..ObservedSet::default()
        };
        let page = format_export(&set, &view(0, 10));
        let start = page.find("```json\n").expect("fence") + 8;
        let end = page[start..].find("\n```").expect("end") + start;
        // The documented bound, literally: the document of a page is at most 28 KB.
        assert!(end - start <= 28 * 1024, "{}", end - start);
        let d = document(&page);
        let ops = d["operations"].as_object().expect("ops").len() as u64;
        let missing = d["channels"]["hot"]["x-mesh-operations-not-listed"]
            .as_u64()
            .expect("count");
        assert!(
            ops >= 2 && missing > 0 && ops + missing == 80,
            "{ops} {missing}"
        );
    }

    #[test]
    fn a_page_never_reads_as_the_whole_document_and_always_advances() {
        let set = ObservedSet {
            channels: (0..30)
                .map(|i| {
                    o(
                        &format!("t{i:02}"),
                        None,
                        vec![s(true, "a.ts:1", ObsPayload::Untyped)],
                        vec![],
                    )
                })
                .collect(),
            ..ObservedSet::default()
        };
        let text = format_export(&set, &view(0, 12));
        assert!(text.contains("PARTIAL DOCUMENT"), "{text}");
        assert!(
            text.contains("18 channel(s) are not in this page"),
            "{text}"
        );
        let d = document(&text);
        assert_eq!(d["x-mesh"]["partial"], json!(true));
        assert_eq!(d["x-mesh"]["next_offset"], json!(12));
        let last = format_export(&set, &view(24, 12));
        let d = document(&last);
        assert_eq!(d["x-mesh"]["channels_in_document"], json!(6));
        assert!(d["x-mesh"].get("next_offset").is_none());
        let whole = format_export(&set, &view(0, 100));
        assert!(!whole.contains("PARTIAL DOCUMENT"));
        assert_eq!(document(&whole)["x-mesh"]["partial"], json!(false));
    }

    #[test]
    fn the_budget_cuts_a_page_to_fewer_channels_never_to_zero() {
        let big = |i: usize| {
            o(
                &format!("topic.{i}"),
                None,
                (0..40)
                    .map(|j| {
                        s(
                            true,
                            &format!("src/very/long/path/number/{i}/file{j}.ts:{j}"),
                            ObsPayload::Untyped,
                        )
                    })
                    .collect(),
                vec![],
            )
        };
        let set = ObservedSet {
            channels: (0..20).map(big).collect(),
            ..ObservedSet::default()
        };
        let text = format_export(&set, &view(0, 20));
        let d = document(&text);
        let shown = d["x-mesh"]["channels_in_document"].as_u64().expect("n");
        assert!((1..20).contains(&shown), "{shown}");
        assert_eq!(d["x-mesh"]["next_offset"], json!(shown));
        assert!(text.len() < 40 * 1024);
    }
}
