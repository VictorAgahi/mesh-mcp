//! AsyncAPI 3 export of the observed event graph and its diff against a declared AsyncAPI
//! document (`export_asyncapi`, 9.0 ticket T12).
//!
//! The graph side is read once into owned [`Observed`] rows (key, broker, sites as `path:line`,
//! the payload types T11 resolved), so the export, the declared-document reader and the diff are
//! pure functions of plain data: every rule is testable with literals, and nothing here holds a
//! borrow of the graph while it renders.
//!
//! Raw values are kept; redaction happens at render (`shown_key` / `shown_name`, the same gates
//! as the anomaly report), so a credential-shaped topic or field name never reaches the output
//! and still never silently merges with another row.

mod declared;
mod diff;
mod export;

use crate::anomalies::{confidence, file_line};
use mesh_core::{Broker, ContractNode, ObservedChannel, ObservedEvents, SitePayload};
use std::path::PathBuf;

pub use declared::{
    parse_declared, Action, DeclaredChannel, DeclaredDoc, DeclaredField, DeclaredMessage,
    DeclaredOp, DeclaredParse, DeclaredSchema, DeclaredServer, MAX_DECLARED_CHANNELS,
};
pub use diff::{diff_asyncapi, format_diff, DiffReport, DiffRow, DiffView, RowClass, RowKind};
pub use export::{format_export, ExportView};

/// One field of an observed payload type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObsField {
    pub name: String,
    /// The declared type as written, when it is a plain type.
    pub ty: Option<String>,
    pub optional: bool,
}

/// A payload type (or inline object) the code uses on a channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObsType {
    /// `None` for an inline object type or literal.
    pub name: Option<String>,
    /// `path:line` of the declaration (or of the inline payload).
    pub loc: String,
    pub fields: Vec<ObsField>,
    /// The field list is partial (a cap, a spread, an `extends`).
    pub incomplete: bool,
}

/// What a site's payload resolved to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObsPayload {
    /// Index into [`Observed::types`].
    Typed(usize),
    Untyped,
    /// The type name is declared by no indexed file.
    Undeclared,
    Ambiguous,
}

/// One producer or consumer site of a channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObsSite {
    pub producer: bool,
    /// `path:line`, relative to the site's root.
    pub loc: String,
    pub confidence: &'static str,
    pub payload: ObsPayload,
}

/// One observed channel: a topic key on a broker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Observed {
    /// The engine's topic key (`normalize_topic_key`: lower case), no broker qualifier.
    pub key: String,
    pub broker: Option<Broker>,
    pub confidence: &'static str,
    pub wildcard: bool,
    /// Producers first, then consumers, each in path order.
    pub sites: Vec<ObsSite>,
    pub types: Vec<ObsType>,
    /// A site of the channel is under the caller's scope.
    pub in_scope: bool,
}

/// A topic that is not a channel (its name is a `${KEY}` no configuration resolved).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObsUnresolved {
    pub key: String,
    pub broker: Option<Broker>,
    pub sites: Vec<String>,
    pub in_scope: bool,
}

/// Everything the export and the diff read from the graph.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ObservedSet {
    pub channels: Vec<Observed>,
    pub unresolved: Vec<ObsUnresolved>,
    pub test_sites: usize,
    pub declared_sites: usize,
}

impl ObservedSet {
    /// Reads the core's observed events into owned rows. `in_scope` keeps a row when one of its
    /// sites passes (the analysis itself always read every site).
    pub fn from_core(
        ev: &ObservedEvents<'_>,
        roots: &[PathBuf],
        in_scope: &dyn Fn(&ContractNode) -> bool,
    ) -> Self {
        let loc = |n: &ContractNode| file_line(&n.file_path, n.repo_id, n.line_start as u32, roots);
        let channels = ev
            .channels
            .iter()
            .map(|c| observed_row(c, roots, in_scope))
            .collect();
        let unresolved = ev
            .unresolved
            .iter()
            .map(|t| ObsUnresolved {
                key: t.key.to_string(),
                broker: t.broker,
                sites: t
                    .producers
                    .iter()
                    .chain(&t.consumers)
                    .map(|s| loc(s.node))
                    .collect(),
                in_scope: t
                    .producers
                    .iter()
                    .chain(&t.consumers)
                    .any(|s| in_scope(s.node)),
            })
            .collect();
        Self {
            channels,
            unresolved,
            test_sites: ev.test_sites,
            declared_sites: ev.declared_sites,
        }
    }
}

fn observed_row(
    c: &ObservedChannel<'_>,
    roots: &[PathBuf],
    in_scope: &dyn Fn(&ContractNode) -> bool,
) -> Observed {
    let loc = |n: &ContractNode| file_line(&n.file_path, n.repo_id, n.line_start as u32, roots);
    let mut types: Vec<ObsType> = Vec::new();
    let mut sites = Vec::new();
    let sides = [
        (true, &c.topic.producers, &c.producer_payloads),
        (false, &c.topic.consumers, &c.consumer_payloads),
    ];
    for (producer, list, payloads) in sides {
        for (site, payload) in list.iter().zip(payloads) {
            let payload = match payload {
                SitePayload::Typed {
                    type_name,
                    file,
                    repo_id,
                    line,
                    fields,
                    incomplete,
                } => {
                    let ty = ObsType {
                        name: type_name.map(str::to_string),
                        loc: file_line(file, *repo_id, *line, roots),
                        fields: fields
                            .iter()
                            .map(|f| ObsField {
                                name: f.name.to_string(),
                                ty: f.ty.as_ref().map(ToString::to_string),
                                optional: f.optional,
                            })
                            .collect(),
                        incomplete: *incomplete,
                    };
                    ObsPayload::Typed(match types.iter().position(|t| *t == ty) {
                        Some(i) => i,
                        None => {
                            types.push(ty);
                            types.len() - 1
                        }
                    })
                }
                SitePayload::Untyped => ObsPayload::Untyped,
                SitePayload::Undeclared => ObsPayload::Undeclared,
                SitePayload::Ambiguous => ObsPayload::Ambiguous,
            };
            sites.push(ObsSite {
                producer,
                loc: loc(site.node),
                confidence: confidence(site.confidence),
                payload,
            });
        }
    }
    Observed {
        key: c.topic.key.to_string(),
        broker: c.topic.broker,
        confidence: confidence(c.topic.confidence),
        wildcard: c.wildcard,
        sites,
        types,
        in_scope: c
            .topic
            .producers
            .iter()
            .chain(&c.topic.consumers)
            .any(|s| in_scope(s.node)),
    }
}
