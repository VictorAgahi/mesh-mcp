//! Declared-versus-code diff (`export_asyncapi`, mode `diff`): a declared AsyncAPI document
//! against the observed event graph.
//!
//! Four classes of row:
//!
//! - **declared, not in code**: a declared channel no observed topic matches, or a declared
//!   operation (`send` / `receive`) whose direction no observed site of the channel has;
//! - **in code, not declared**: an observed channel no declared channel matches, or a direction
//!   the code has on a channel whose declaration lists operations and not that one;
//! - **payload disagreement**: the declared payload properties against the fields of the code's
//!   type (declared required field missing in code, code field not declared, primitive type
//!   difference), `path:line` on both sides;
//! - **broker disagreement**: the declared servers' protocols admit none of the brokers the
//!   code uses for the channel.
//!
//! Keys are the engine's: `normalize_topic_key` (lower case, enum members reduced), a
//! `{param}` address token read as a one-token wildcard, a broker qualifier (`kafka:orders`)
//! split off, and a wildcard on either side matched with `subject_matches`. A site of an unknown
//! broker is never a broker disagreement (it makes no claim), and a declared server whose
//! protocol maps to no broker (`ws`, `http`) makes no claim either.
//!
//! "Not in code" means not statically resolved in the indexed roots: a name built at runtime, a
//! `${KEY}` placeholder or an unindexed root can hide the other end, and the output says how
//! many topics were not resolved. Everything is sorted; nothing depends on hash order.

use super::declared::{Action, DeclaredChannel, DeclaredDoc, DeclaredSchema};
use super::{ObsPayload, ObservedSet};
use crate::anomalies::{shown_key, shown_name};
use mesh_core::{is_subject_pattern, payload_json_family, subject_matches, Broker};
use std::collections::BTreeSet;
use std::fmt::Write as _;

/// Which class a row belongs to (the sections of the report).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RowClass {
    DeclaredNotInCode,
    InCodeNotDeclared,
    PayloadDisagreement,
    BrokerDisagreement,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RowKind {
    DeclaredChannel,
    DeclaredOperation(Action),
    UndeclaredChannel,
    UndeclaredDirection(Action),
    Payload,
    Broker,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiffRow {
    pub class: RowClass,
    pub kind: RowKind,
    /// The topic key (raw, printed through the name gate).
    pub key: String,
    pub broker: Option<Broker>,
    /// `path:line` of the declaration, when the row is about one.
    pub declared: Option<String>,
    /// `path:line` of the code sites, when there are some.
    pub code: Vec<String>,
    pub detail: Vec<String>,
    /// The declared file or a code site is under the caller's scope.
    pub in_scope: bool,
}

/// One declared document, as summarized in the header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocSummary {
    pub file: String,
    pub version: String,
    pub channels: usize,
    pub operations: usize,
    /// Channels not compared (no static address, past the read cap) and `$ref`s not read.
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DiffReport {
    pub rows: Vec<DiffRow>,
    pub docs: Vec<DocSummary>,
    /// Observed channels read (all scopes).
    pub observed_channels: usize,
    /// Declared channels compared (all scopes).
    pub declared_channels: usize,
    /// Topics that are not channels (`${KEY}` placeholders): not compared.
    pub unresolved_topics: usize,
    /// Channel payloads that were not compared, with the topic.
    pub payload_not_compared: Vec<String>,
}

/// How one page of the report is shown.
#[derive(Debug, Clone)]
pub struct DiffView<'a> {
    /// Rows per class.
    pub limit: usize,
    /// Rows skipped per class.
    pub offset: usize,
    pub scope: Option<&'a str>,
    pub coverage: Option<String>,
    pub roots: usize,
    /// Files that held an AsyncAPI-looking channel but could not be read or are not a
    /// supported AsyncAPI document, with the reason.
    pub skipped_files: Vec<String>,
}

/// What a declared channel's servers admit.
enum Admit {
    /// No claim: no server, or a server whose protocol maps to no broker.
    Any,
    Set(Vec<Broker>),
}

impl Admit {
    fn compatible(&self, b: Option<Broker>) -> bool {
        match (self, b) {
            (Admit::Set(set), Some(b)) => set.contains(&b),
            _ => true,
        }
    }
}

/// The brokers a declared server protocol can be, `None` when it names no broker.
fn protocol_brokers(protocol: &str) -> Option<Vec<Broker>> {
    let p = protocol.to_ascii_lowercase();
    Some(match p.as_str() {
        "kafka-secure" => vec![Broker::Kafka],
        "amqp" | "amqps" | "amqp1.0" => {
            vec![Broker::RabbitMq, Broker::ServiceBus, Broker::EventHubs]
        }
        "mqtts" | "secure-mqtt" => vec![Broker::Mqtt],
        "rediss" => vec![Broker::Redis, Broker::Bull, Broker::BeeQueue],
        "redis" => vec![Broker::Redis, Broker::Bull, Broker::BeeQueue],
        "googlepubsub" => vec![Broker::PubSub],
        "pulsar+ssl" => vec![Broker::Pulsar],
        "stomps" => vec![Broker::Stomp],
        // A protocol that is a broker's own slug (`kafka`, `nats`, `sqs`) is that broker.
        _ => return Broker::from_slug(&p).map(|b| vec![b]),
    })
}

fn admit(doc: &DeclaredDoc, ch: &DeclaredChannel) -> Admit {
    if let Some(b) = ch.broker {
        return Admit::Set(vec![b]);
    }
    let servers: Vec<&str> = if ch.servers.is_empty() {
        doc.servers.iter().map(|s| s.id.as_str()).collect()
    } else {
        ch.servers.iter().map(String::as_str).collect()
    };
    let mut set: BTreeSet<Broker> = BTreeSet::new();
    let mut named = 0;
    for id in servers {
        let Some(s) = doc.servers.iter().find(|s| s.id == id) else {
            continue;
        };
        named += 1;
        match protocol_brokers(&s.protocol) {
            Some(list) => set.extend(list),
            None => return Admit::Any,
        }
    }
    if named == 0 {
        Admit::Any
    } else {
        Admit::Set(set.into_iter().collect())
    }
}

fn keys_match(declared: &str, observed: &str) -> bool {
    declared == observed
        || (is_subject_pattern(declared) && subject_matches(declared, observed))
        || (is_subject_pattern(observed) && subject_matches(observed, declared))
}

fn loc(file: &str, line: Option<u32>) -> String {
    match line {
        Some(l) => format!("{file}:{l}"),
        None => file.to_string(),
    }
}

/// The findings of a declared payload schema against a code type; `None` when the comparison
/// cannot be made (a partial field list on the side a direction is read from).
fn compare_fields(
    declared: &DeclaredSchema,
    code_name: &str,
    code_fields: &[super::ObsField],
    code_incomplete: bool,
) -> Vec<String> {
    let mut out = Vec::new();
    for f in &declared.fields {
        match code_fields.iter().find(|c| c.name == f.name) {
            None if f.required && !code_incomplete => out.push(format!(
                "declared required field `{}` is not in the code type {code_name}",
                shown_name(&f.name)
            )),
            Some(c) => {
                let code_family = c.ty.as_deref().and_then(payload_json_family);
                if let (Some(d), Some(cf)) = (&f.ty, code_family) {
                    if *d != cf {
                        out.push(format!(
                            "field `{}`: declared {d}, code {cf}",
                            shown_name(&f.name)
                        ));
                    }
                }
            }
            None => {}
        }
    }
    if declared.complete && !code_incomplete {
        for c in code_fields {
            if !declared.fields.iter().any(|f| f.name == c.name) {
                out.push(format!(
                    "code type {code_name} has field `{}` the declaration does not list",
                    shown_name(&c.name)
                ));
            }
        }
    }
    out
}

/// The diff of `docs` (each with whether its file is under the caller's scope) against
/// `observed`. No document, no rows: an absent declaration is not "everything undeclared".
pub fn diff_asyncapi(observed: &ObservedSet, docs: &[(DeclaredDoc, bool)]) -> DiffReport {
    let mut rep = DiffReport {
        observed_channels: observed.channels.len(),
        unresolved_topics: observed.unresolved.len(),
        ..DiffReport::default()
    };
    // Per observed channel: the declared channels that match its key.
    let mut declared_by: Vec<Vec<(usize, usize)>> = vec![Vec::new(); observed.channels.len()];

    for (di, (doc, doc_in_scope)) in docs.iter().enumerate() {
        let mut notes = Vec::new();
        if doc.no_address > 0 {
            notes.push(format!(
                "{} channel(s) have no static address (not compared)",
                doc.no_address
            ));
        }
        if doc.channels_cut > 0 {
            notes.push(format!(
                "{} channel(s) past the read cap (not compared)",
                doc.channels_cut
            ));
        }
        if doc.unread_refs > 0 {
            notes.push(format!(
                "{} `$ref`(s) not read (external or dangling): those payloads are not compared",
                doc.unread_refs
            ));
        }
        if doc.orphan_ops > 0 {
            notes.push(format!(
                "{} operation(s) name a channel the document does not declare",
                doc.orphan_ops
            ));
        }
        rep.docs.push(DocSummary {
            file: doc.file.clone(),
            version: doc.version.clone(),
            channels: doc.channels.len(),
            operations: doc.channels.iter().map(|c| c.ops.len()).sum(),
            notes,
        });
        rep.declared_channels += doc.channels.len();

        for (ci, ch) in doc.channels.iter().enumerate() {
            let at = loc(&doc.file, ch.line);
            let key_matches: Vec<usize> = (0..observed.channels.len())
                .filter(|&i| keys_match(&ch.key, &observed.channels[i].key))
                .collect();
            if key_matches.is_empty() {
                rep.rows.push(DiffRow {
                    class: RowClass::DeclaredNotInCode,
                    kind: RowKind::DeclaredChannel,
                    key: ch.key.clone(),
                    broker: ch.broker,
                    declared: Some(at),
                    code: Vec::new(),
                    detail: vec![format!(
                        "channel `{}`: no producer or consumer statically resolved",
                        shown_name(&ch.id)
                    )],
                    in_scope: *doc_in_scope,
                });
                continue;
            }
            for &i in &key_matches {
                declared_by[i].push((di, ci));
            }
            let admits = admit(doc, ch);
            let compat: Vec<usize> = key_matches
                .iter()
                .copied()
                .filter(|&i| admits.compatible(observed.channels[i].broker))
                .collect();
            if compat.is_empty() {
                let brokers: BTreeSet<&str> = key_matches
                    .iter()
                    .filter_map(|&i| observed.channels[i].broker.map(Broker::slug))
                    .collect();
                let protocols: BTreeSet<String> = if ch.servers.is_empty() {
                    doc.servers.iter().map(|s| s.protocol.clone()).collect()
                } else {
                    doc.servers
                        .iter()
                        .filter(|s| ch.servers.contains(&s.id))
                        .map(|s| s.protocol.clone())
                        .collect()
                };
                let declared = match ch.broker {
                    Some(b) => format!("the address names broker `{}`", b.slug()),
                    None => format!(
                        "declared server protocol(s) {}",
                        protocols
                            .iter()
                            .map(|p| format!("`{}`", shown_name(p)))
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                };
                rep.rows.push(DiffRow {
                    class: RowClass::BrokerDisagreement,
                    kind: RowKind::Broker,
                    key: ch.key.clone(),
                    broker: None,
                    declared: Some(at.clone()),
                    code: key_matches
                        .iter()
                        .flat_map(|&i| observed.channels[i].sites.iter().map(|s| s.loc.clone()))
                        .take(3)
                        .collect(),
                    detail: vec![format!(
                        "{declared}; the code uses {}",
                        brokers
                            .iter()
                            .map(|b| format!("`{b}`"))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )],
                    in_scope: *doc_in_scope
                        || key_matches.iter().any(|&i| observed.channels[i].in_scope),
                });
            }
            let m: &[usize] = if compat.is_empty() {
                &key_matches
            } else {
                &compat
            };
            let has = |producer: bool| {
                m.iter().any(|&i| {
                    observed.channels[i]
                        .sites
                        .iter()
                        .any(|s| s.producer == producer)
                })
            };
            for op in &ch.ops {
                let in_code = match op.action {
                    Action::Send => has(true),
                    Action::Receive => has(false),
                };
                if !in_code {
                    rep.rows.push(DiffRow {
                        class: RowClass::DeclaredNotInCode,
                        kind: RowKind::DeclaredOperation(op.action),
                        key: ch.key.clone(),
                        broker: ch.broker,
                        declared: Some(loc(&doc.file, op.line.or(ch.line))),
                        code: m
                            .iter()
                            .flat_map(|&i| observed.channels[i].sites.iter().map(|s| s.loc.clone()))
                            .take(3)
                            .collect(),
                        detail: vec![format!(
                            "operation `{}` ({}): the channel is in code, no {} site statically resolved",
                            shown_name(&op.id),
                            op.action.word(),
                            match op.action {
                                Action::Send => "producer",
                                Action::Receive => "consumer",
                            }
                        )],
                        in_scope: *doc_in_scope,
                    });
                }
            }
            compare_payloads(observed, m, ch, &at, *doc_in_scope, &mut rep);
        }
    }

    for (i, o) in observed.channels.iter().enumerate() {
        let sites: Vec<String> = o.sites.iter().take(3).map(|s| s.loc.clone()).collect();
        if declared_by[i].is_empty() {
            rep.rows.push(DiffRow {
                class: RowClass::InCodeNotDeclared,
                kind: RowKind::UndeclaredChannel,
                key: o.key.clone(),
                broker: o.broker,
                declared: None,
                code: sites,
                detail: vec![format!(
                    "{} producer(s), {} consumer(s), {}",
                    o.sites.iter().filter(|s| s.producer).count(),
                    o.sites.iter().filter(|s| !s.producer).count(),
                    o.confidence
                )],
                in_scope: o.in_scope,
            });
            continue;
        }
        let declared_dirs: BTreeSet<Action> = declared_by[i]
            .iter()
            .flat_map(|&(d, c)| docs[d].0.channels[c].ops.iter().map(|op| op.action))
            .collect();
        if declared_dirs.is_empty() {
            continue;
        }
        for (producer, action) in [(true, Action::Send), (false, Action::Receive)] {
            let here: Vec<String> = o
                .sites
                .iter()
                .filter(|s| s.producer == producer)
                .map(|s| s.loc.clone())
                .collect();
            if here.is_empty() || declared_dirs.contains(&action) {
                continue;
            }
            let (di, ci) = declared_by[i][0];
            rep.rows.push(DiffRow {
                class: RowClass::InCodeNotDeclared,
                kind: RowKind::UndeclaredDirection(action),
                key: o.key.clone(),
                broker: o.broker,
                declared: Some(loc(&docs[di].0.file, docs[di].0.channels[ci].line)),
                code: here.into_iter().take(3).collect(),
                detail: vec![format!(
                    "the code {}s on this channel, the declaration lists no {} operation",
                    action.word(),
                    action.word()
                )],
                in_scope: o.in_scope,
            });
        }
    }

    rep.rows.sort_by(|a, b| {
        (a.class, &a.key, a.broker, a.kind, &a.declared, &a.code).cmp(&(
            b.class,
            &b.key,
            b.broker,
            b.kind,
            &b.declared,
            &b.code,
        ))
    });
    rep.payload_not_compared.sort();
    rep.payload_not_compared.dedup();
    rep
}

/// Compares the declared payload of `ch` with each type the code uses on the channels `m`.
fn compare_payloads(
    observed: &ObservedSet,
    m: &[usize],
    ch: &DeclaredChannel,
    at: &str,
    doc_in_scope: bool,
    rep: &mut DiffReport,
) {
    let with_schema: Vec<&super::declared::DeclaredMessage> = ch
        .messages
        .iter()
        .filter(|msg| msg.schema.is_some())
        .collect();
    if with_schema.is_empty() {
        return;
    }
    let mut compared_any = false;
    let mut typed_any = false;
    for &i in m {
        let o = &observed.channels[i];
        for (ti, t) in o.types.iter().enumerate() {
            typed_any = true;
            let name = t.name.as_deref();
            let msg = with_schema
                .iter()
                .find(|msg| name.is_some_and(|n| msg.name.eq_ignore_ascii_case(n)))
                .copied()
                .or_else(|| (with_schema.len() == 1).then(|| with_schema[0]));
            let Some(schema) = msg.and_then(|m| m.schema.as_ref()) else {
                rep.payload_not_compared.push(format!(
                    "`{}`: {} messages declared, none named like the code type",
                    shown_key(&o.key),
                    with_schema.len()
                ));
                continue;
            };
            compared_any = true;
            let label = name.map_or("(inline payload)".to_string(), |n| {
                format!("`{}`", shown_name(n))
            });
            let findings = compare_fields(schema, &label, &t.fields, t.incomplete);
            if t.incomplete || !schema.complete {
                rep.payload_not_compared.push(format!(
                    "`{}`: a partial field list on the {} side, only the findings that hold anyway are shown",
                    shown_key(&o.key),
                    if t.incomplete { "code" } else { "declared" }
                ));
            }
            if findings.is_empty() {
                continue;
            }
            let sites: Vec<String> = o
                .sites
                .iter()
                .filter(|s| s.payload == ObsPayload::Typed(ti))
                .map(|s| s.loc.clone())
                .take(3)
                .collect();
            let mut code = vec![t.loc.clone()];
            code.extend(sites);
            rep.rows.push(DiffRow {
                class: RowClass::PayloadDisagreement,
                kind: RowKind::Payload,
                key: ch.key.clone(),
                broker: o.broker,
                declared: Some(at.to_string()),
                code,
                detail: findings,
                in_scope: doc_in_scope || o.in_scope,
            });
        }
    }
    if !typed_any || !compared_any {
        rep.payload_not_compared.push(format!(
            "`{}`: no statically typed payload in code",
            shown_key(&ch.key)
        ));
    }
}

const BUDGET: usize = 32 * 1024;
const REDACTED_ROW: &str = "[REDACTED_SECRET: USE_ENV_OR_LOCAL_FALLBACK]";

fn class_title(c: RowClass) -> (&'static str, &'static str) {
    match c {
        RowClass::DeclaredNotInCode => (
            "Declared, not in code",
            "A declared channel or operation with no matching producer or consumer statically resolved in the indexed roots. Not proof of absence: a name built at runtime, a `${KEY}` placeholder or an unindexed root can hide it.",
        ),
        RowClass::InCodeNotDeclared => (
            "In code, not declared",
            "An observed channel no declared channel matches, or a direction (send / receive) the code has on a channel whose declaration lists operations and not that one.",
        ),
        RowClass::PayloadDisagreement => (
            "Payload disagreement",
            "The declared payload properties against the fields of the code's type, where both are statically known.",
        ),
        RowClass::BrokerDisagreement => (
            "Broker disagreement",
            "The declared servers admit none of the brokers the code uses for the channel. A site whose broker is unknown makes no claim.",
        ),
    }
}

fn row_line(r: &DiffRow) -> String {
    let key = shown_key(&r.key);
    let key = if key == REDACTED_ROW {
        key
    } else {
        format!("`{key}`")
    };
    let broker = r.broker.map_or("broker unknown", Broker::slug);
    // A broker row is about a mismatch of brokers: no single broker label fits it.
    let mut s = if r.kind == RowKind::Broker {
        format!("- {key}")
    } else {
        format!("- {key} [{broker}]")
    };
    if let Some(d) = &r.declared {
        let _ = write!(s, " declared at `{d}`");
    }
    if !r.code.is_empty() {
        let _ = write!(
            s,
            "{} in code at {}",
            if r.declared.is_some() { ";" } else { "" },
            r.code
                .iter()
                .map(|c| format!("`{c}`"))
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    let mut text = s;
    for d in &r.detail {
        let _ = write!(text, "\n  - {d}");
    }
    text
}

/// One page of the diff as Markdown.
pub fn format_diff(report: &DiffReport, view: &DiffView<'_>) -> String {
    let scope = view.scope.map_or("the whole workspace".to_string(), |s| {
        format!("`{}`", s.replace(['`', '\n', '\r'], "'"))
    });
    let in_scope: Vec<&DiffRow> = report.rows.iter().filter(|r| r.in_scope).collect();
    let mut out = String::from("## AsyncAPI declared-versus-code diff\n");
    let _ = writeln!(
        out,
        "*{} declared document(s) ({} channel(s)) against {} observed channel(s) over {} root(s); rows shown for {scope}.*\n",
        report.docs.len(),
        report.declared_channels,
        report.observed_channels,
        view.roots
    );
    out.push_str(
        "| Declared document | AsyncAPI | Channels | Operations |\n| :--- | :--- | ---: | ---: |\n",
    );
    for d in &report.docs {
        let _ = writeln!(
            out,
            "| `{}` | {} | {} | {} |",
            d.file.replace(['`', '\n', '\r', '|'], "'"),
            d.version,
            d.channels,
            d.operations
        );
    }
    out.push('\n');
    out.push_str("| Class | Rows |\n| :--- | ---: |\n");
    let classes = [
        RowClass::DeclaredNotInCode,
        RowClass::InCodeNotDeclared,
        RowClass::PayloadDisagreement,
        RowClass::BrokerDisagreement,
    ];
    for c in classes {
        let n = in_scope.iter().filter(|r| r.class == c).count();
        let _ = writeln!(out, "| {} | {n} |", class_title(c).0.to_lowercase());
    }
    out.push('\n');
    for c in classes {
        let rows: Vec<&DiffRow> = in_scope.iter().copied().filter(|r| r.class == c).collect();
        let (title, blurb) = class_title(c);
        let _ = writeln!(out, "### {title}\n*{blurb}*\n");
        if rows.is_empty() {
            out.push_str("None.\n\n");
            continue;
        }
        let start = view.offset.min(rows.len());
        let mut shown = 0usize;
        for r in rows.iter().skip(start).take(view.limit) {
            let line = row_line(r);
            if shown > 0 && out.len() + line.len() > BUDGET {
                break;
            }
            out.push_str(&line);
            out.push('\n');
            shown += 1;
        }
        if shown == 0 {
            let _ = writeln!(
                out,
                "No rows from offset {start} (this class has {}).",
                rows.len()
            );
        } else if start + shown < rows.len() {
            let _ = writeln!(
                out,
                "\n**Next page:** {} more row(s) of this class, `offset: {}`.",
                rows.len() - start - shown,
                start + shown
            );
        }
        out.push('\n');
    }
    out.push_str("---\n");
    for d in &report.docs {
        for n in &d.notes {
            let _ = writeln!(out, "- `{}`: {n}.", d.file.replace(['`', '\n', '\r'], "'"));
        }
    }
    for f in &view.skipped_files {
        let _ = writeln!(out, "- Not compared: {f}.");
    }
    if report.unresolved_topics > 0 {
        let _ = writeln!(
            out,
            "- {} observed topic(s) are named by a `${{KEY}}` placeholder no configuration resolved: they are not channels here and were not compared, so a declared channel reported as not in code may be one of them.",
            report.unresolved_topics
        );
    }
    if !report.payload_not_compared.is_empty() {
        let shown: Vec<&str> = report
            .payload_not_compared
            .iter()
            .take(10)
            .map(String::as_str)
            .collect();
        let _ = writeln!(
            out,
            "- Payloads not compared ({}): {}{}.",
            report.payload_not_compared.len(),
            shown.join("; "),
            if report.payload_not_compared.len() > shown.len() {
                "; and more"
            } else {
                ""
            }
        );
    }
    if let Some(c) = &view.coverage {
        let _ = writeln!(out, "- Coverage: {c}.");
    }
    out.push_str("- The declared documents are read from disk at call time, the code from the index: an edit not yet re-indexed shows on one side only. Observed keys are the engine's normalized (lower-case) keys.\n");
    out
}

#[cfg(test)]
mod tests {
    use super::super::declared::{parse_declared, DeclaredParse};
    use super::super::{ObsField, ObsSite, ObsType, Observed};
    use super::*;

    fn site(producer: bool, loc: &str, payload: ObsPayload) -> ObsSite {
        ObsSite {
            producer,
            loc: loc.into(),
            confidence: "exact",
            payload,
        }
    }

    fn chan(
        key: &str,
        broker: Option<Broker>,
        sites: Vec<ObsSite>,
        types: Vec<ObsType>,
    ) -> Observed {
        Observed {
            key: key.into(),
            broker,
            confidence: "exact",
            wildcard: is_subject_pattern(key),
            sites,
            types,
            in_scope: true,
        }
    }

    fn ty(name: &str, fields: &[(&str, &str, bool)], incomplete: bool) -> ObsType {
        ObsType {
            name: Some(name.into()),
            loc: "src/t.ts:1".into(),
            fields: fields
                .iter()
                .map(|(n, t, o)| ObsField {
                    name: (*n).into(),
                    ty: Some((*t).into()),
                    optional: *o,
                })
                .collect(),
            incomplete,
        }
    }

    fn declared(yaml: &str) -> Vec<(DeclaredDoc, bool)> {
        match parse_declared("asyncapi.yaml", yaml) {
            DeclaredParse::Doc(d) => vec![(d, true)],
            _ => Vec::new(),
        }
    }

    fn rows(r: &DiffReport, class: RowClass) -> Vec<(String, RowKind)> {
        r.rows
            .iter()
            .filter(|x| x.class == class)
            .map(|x| (x.key.clone(), x.kind))
            .collect()
    }

    const DOC: &str = "\
asyncapi: 3.0.0
servers:
  main: {host: h, protocol: amqp}
channels:
  a:
    address: orders.created
    servers: [{$ref: '#/servers/main'}]
    messages:
      m:
        payload:
          type: object
          required: [id, total]
          properties:
            id: {type: string}
            total: {type: number}
            memo: {type: string}
  b:
    address: orders.gone
  w:
    address: 'orders.audit.{region}'
operations:
  sa: {action: send, channel: {$ref: '#/channels/a'}}
  ra: {action: receive, channel: {$ref: '#/channels/a'}}
";

    fn observed() -> ObservedSet {
        ObservedSet {
            channels: vec![
                chan(
                    "orders.audit.eu",
                    Some(Broker::RabbitMq),
                    vec![site(true, "src/p.ts:9", ObsPayload::Untyped)],
                    vec![],
                ),
                chan(
                    "orders.created",
                    Some(Broker::Kafka),
                    vec![site(true, "src/p.ts:3", ObsPayload::Typed(0))],
                    vec![ty(
                        "Created",
                        &[
                            ("id", "string", false),
                            ("total", "string", false),
                            ("extra", "string", false),
                        ],
                        false,
                    )],
                ),
                chan(
                    "orders.legacy",
                    None,
                    vec![site(false, "src/c.ts:4", ObsPayload::Untyped)],
                    vec![],
                ),
            ],
            ..ObservedSet::default()
        }
    }

    #[test]
    fn every_class_on_a_hand_built_pair() {
        let r = diff_asyncapi(&observed(), &declared(DOC));
        assert_eq!(
            rows(&r, RowClass::DeclaredNotInCode),
            vec![
                (
                    "orders.created".into(),
                    RowKind::DeclaredOperation(Action::Receive)
                ),
                ("orders.gone".into(), RowKind::DeclaredChannel),
            ]
        );
        assert_eq!(
            rows(&r, RowClass::InCodeNotDeclared),
            vec![("orders.legacy".into(), RowKind::UndeclaredChannel)]
        );
        assert_eq!(
            rows(&r, RowClass::BrokerDisagreement),
            vec![("orders.created".into(), RowKind::Broker)]
        );
        let p = r
            .rows
            .iter()
            .find(|x| x.class == RowClass::PayloadDisagreement)
            .expect("payload row");
        assert_eq!(
            p.detail,
            vec![
                "field `total`: declared number, code string".to_string(),
                "code type `Created` has field `extra` the declaration does not list".to_string(),
            ]
        );
        // `orders.audit.eu` is heard by the declared `orders.audit.{region}`: no row for it.
        assert!(r.rows.iter().all(|x| x.key != "orders.audit.eu"));
    }

    #[test]
    fn a_partial_code_type_never_claims_missing_fields() {
        let mut o = observed();
        o.channels[1].types = vec![ty("Created", &[("total", "number", false)], true)];
        let r = diff_asyncapi(&o, &declared(DOC));
        assert!(rows(&r, RowClass::PayloadDisagreement).is_empty());
        assert!(r
            .payload_not_compared
            .iter()
            .any(|n| n.contains("partial field list on the code side")));
    }

    #[test]
    fn a_declared_schema_that_is_not_complete_never_claims_undeclared_code_fields() {
        let open = DOC.replace(
            "          type: object\n          required: [id, total]",
            "          additionalProperties: true\n          required: [id, total]",
        );
        let r = diff_asyncapi(&observed(), &declared(&open));
        let p = r
            .rows
            .iter()
            .find(|x| x.class == RowClass::PayloadDisagreement)
            .expect("row");
        // The primitive clash still holds; `extra` and `currency` are not claimed undeclared.
        assert_eq!(
            p.detail,
            vec!["field `total`: declared number, code string".to_string()]
        );
        assert!(r
            .payload_not_compared
            .iter()
            .any(|n| n.contains("partial field list on the declared side")));
    }

    #[test]
    fn a_first_row_past_the_budget_is_still_shown_and_the_next_page_advances() {
        let mut o = ObservedSet::default();
        for i in 0..3 {
            o.channels.push(chan(
                &format!("t{i}"),
                None,
                vec![site(true, "a.ts:1", ObsPayload::Untyped)],
                vec![],
            ));
        }
        let mut r = diff_asyncapi(&o, &declared("asyncapi: 3.0.0\nchannels: {}\n"));
        for row in &mut r.rows {
            row.detail = vec!["x".repeat(40 * 1024)];
        }
        let view = DiffView {
            limit: 25,
            offset: 0,
            scope: None,
            coverage: None,
            roots: 1,
            skipped_files: vec![],
        };
        let p1 = format_diff(&r, &view);
        assert!(
            p1.contains("`t0`") && !p1.contains("`t1`"),
            "one row, then the pointer"
        );
        assert!(p1.contains("2 more row(s) of this class, `offset: 1`"));
        let p2 = format_diff(&r, &DiffView { offset: 1, ..view });
        assert!(p2.contains("`t1`") && p2.contains("`offset: 2`"));
    }

    #[test]
    fn an_unknown_broker_and_an_unmappable_protocol_make_no_claim() {
        let mut o = observed();
        o.channels[1].broker = None;
        let r = diff_asyncapi(&o, &declared(DOC));
        assert!(rows(&r, RowClass::BrokerDisagreement).is_empty());
        let ws = DOC.replace("protocol: amqp", "protocol: ws");
        let r = diff_asyncapi(&observed(), &declared(&ws));
        assert!(rows(&r, RowClass::BrokerDisagreement).is_empty());
    }

    #[test]
    fn a_redis_server_admits_the_queue_libraries_on_redis() {
        let redis = "asyncapi: 2.6.0\nservers:\n  q: {url: r, protocol: redis}\nchannels:\n  jobs:\n    publish:\n      message: {payload: {type: object}}\n";
        let mut o = ObservedSet::default();
        o.channels.push(chan(
            "jobs",
            Some(Broker::Bull),
            vec![site(false, "w.ts:1", ObsPayload::Untyped)],
            vec![],
        ));
        assert!(rows(
            &diff_asyncapi(&o, &declared(redis)),
            RowClass::BrokerDisagreement
        )
        .is_empty());
        o.channels[0].broker = Some(Broker::Kafka);
        assert_eq!(
            rows(
                &diff_asyncapi(&o, &declared(redis)),
                RowClass::BrokerDisagreement
            ),
            vec![("jobs".to_string(), RowKind::Broker)]
        );
    }

    #[test]
    fn two_brokers_one_name_pick_the_declared_one() {
        let mut o = observed();
        o.channels.push(chan(
            "orders.created",
            Some(Broker::RabbitMq),
            vec![site(false, "src/r.ts:2", ObsPayload::Untyped)],
            vec![],
        ));
        let r = diff_asyncapi(&o, &declared(DOC));
        // The RabbitMQ hub is the one the amqp server admits: no broker row, and the receive
        // operation is satisfied by the RabbitMQ consumer, not by the Kafka producer.
        assert!(rows(&r, RowClass::BrokerDisagreement).is_empty());
        assert!(!rows(&r, RowClass::DeclaredNotInCode).contains(&(
            "orders.created".into(),
            RowKind::DeclaredOperation(Action::Receive)
        )));
        assert!(rows(&r, RowClass::DeclaredNotInCode).contains(&(
            "orders.created".into(),
            RowKind::DeclaredOperation(Action::Send)
        )));
    }

    #[test]
    fn an_undeclared_direction_needs_a_declaration_that_lists_operations() {
        let only_channel = "asyncapi: 3.0.0\nchannels:\n  a:\n    address: orders.created\n";
        let r = diff_asyncapi(&observed(), &declared(only_channel));
        assert!(rows(&r, RowClass::InCodeNotDeclared)
            .iter()
            .all(|(_, k)| *k == RowKind::UndeclaredChannel));
        let send_only = "asyncapi: 3.0.0\nchannels:\n  a:\n    address: orders.legacy\noperations:\n  s: {action: send, channel: {$ref: '#/channels/a'}}\n";
        let r = diff_asyncapi(&observed(), &declared(send_only));
        assert!(rows(&r, RowClass::InCodeNotDeclared).contains(&(
            "orders.legacy".into(),
            RowKind::UndeclaredDirection(Action::Receive)
        )));
    }

    #[test]
    fn credential_shaped_names_are_redacted_in_rows() {
        let secret = ["pass", "word"].concat();
        let token = ["tok", "_", "a1b2c3"].concat();
        // A code field, a declared required field and an observed topic, all credential-shaped.
        let o = ObservedSet {
            channels: vec![
                chan(
                    "orders.created",
                    Some(Broker::Kafka),
                    vec![site(true, "src/p.ts:3", ObsPayload::Typed(0))],
                    vec![ty("Created", &[(&secret, "string", false)], false)],
                ),
                chan(
                    &token,
                    None,
                    vec![site(true, "src/q.ts:1", ObsPayload::Untyped)],
                    vec![],
                ),
            ],
            ..ObservedSet::default()
        };
        let declared_secret = format!(
            "asyncapi: 3.0.0\nchannels:\n  a:\n    address: orders.created\n    messages:\n      m:\n        payload:\n          type: object\n          required: [{secret}2]\n          properties:\n            {secret}2: {{type: string}}\n"
        );
        for docs in [declared(DOC), declared(&declared_secret)] {
            let r = diff_asyncapi(&o, &docs);
            let text = format_diff(
                &r,
                &DiffView {
                    limit: 25,
                    offset: 0,
                    scope: None,
                    coverage: None,
                    roots: 1,
                    skipped_files: vec![],
                },
            );
            assert!(!text.contains(&secret), "{text}");
            assert!(!text.contains(&token), "{text}");
            assert!(text.contains("[REDACTED_SECRET"), "{text}");
        }
    }

    #[test]
    fn an_observed_wildcard_subscription_matches_a_declared_concrete_channel() {
        let doc = "asyncapi: 3.0.0\nchannels:\n  a:\n    address: orders.created\noperations:\n  r: {action: receive, channel: {$ref: '#/channels/a'}}\n";
        let o = ObservedSet {
            channels: vec![chan(
                "orders.*",
                Some(Broker::Nats),
                vec![site(false, "w.ts:1", ObsPayload::Untyped)],
                vec![],
            )],
            ..ObservedSet::default()
        };
        let r = diff_asyncapi(&o, &declared(doc));
        assert_eq!(
            r.rows,
            Vec::new(),
            "the wildcard hears the declared channel"
        );
        // A wildcard that does not match leaves both unmatched.
        let other = doc.replace("orders.created", "invoices.created");
        let r = diff_asyncapi(&o, &declared(&other));
        assert_eq!(
            r.rows
                .iter()
                .map(|x| (x.key.as_str(), x.kind))
                .collect::<Vec<_>>(),
            vec![
                ("invoices.created", RowKind::DeclaredChannel),
                ("orders.*", RowKind::UndeclaredChannel),
            ]
        );
    }

    #[test]
    fn paging_always_advances_and_says_what_is_left() {
        let mut o = ObservedSet::default();
        for i in 0..5 {
            o.channels.push(chan(
                &format!("t{i}"),
                None,
                vec![site(true, "a.ts:1", ObsPayload::Untyped)],
                vec![],
            ));
        }
        let r = diff_asyncapi(&o, &declared("asyncapi: 3.0.0\nchannels: {}\n"));
        let view = |offset| DiffView {
            limit: 2,
            offset,
            scope: None,
            coverage: None,
            roots: 1,
            skipped_files: vec![],
        };
        let p1 = format_diff(&r, &view(0));
        assert!(
            p1.contains("`t0`") && p1.contains("`t1`") && !p1.contains("`t2`"),
            "{p1}"
        );
        assert!(
            p1.contains("3 more row(s) of this class, `offset: 2`"),
            "{p1}"
        );
        let p3 = format_diff(&r, &view(4));
        assert!(p3.contains("`t4`") && !p3.contains("Next page"), "{p3}");
    }
}
