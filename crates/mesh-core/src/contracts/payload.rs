//! Payload types per topic and the publisher-versus-consumer check (9.0, ticket T11): the events
//! analogue of the `.proto` wire check.
//!
//! The extractors record, per file, the payload a producer or consumer site uses where it is
//! statically visible ([`PayloadSite`]: a type name, an inline object type or literal, or
//! "untyped") and the fields of every class / interface / object type alias / record the file
//! declares ([`PayloadType`]). Only names, field types and lines are kept, never a value, and
//! each list is capped per file and per type (the cap is recorded, never silent).
//!
//! [`ContractGraph::payload_mismatches`] then reads, for every topic hub that has live producers
//! and consumers, the payload of each site, resolves a type name against the declared types
//! (same file, then same repository, then the workspace; two candidates is `ambiguous`, not
//! compared) and reports the fields a consumer expects that no producer sends, the fields
//! published that no consumer expects, and field type clashes between primitive JSON families.
//! A site whose payload is not statically typed is counted, never a mismatch; a direction that
//! a cap or an untyped site leaves incomplete is not compared and is said so.

use super::anomalies::TopicEntry;
use super::flow::node_key;
use super::*;
use serde::Deserialize;
use std::sync::Arc;

/// Fields kept per declared or inline type; past it the type is `incomplete`.
pub const MAX_PAYLOAD_FIELDS_PER_TYPE: usize = 64;
/// Declared types kept per file; past it the file's facts are `capped`.
pub const MAX_PAYLOAD_TYPES_PER_FILE: usize = 256;
/// Payload sites kept per file; past it the file's facts are `capped`.
pub const MAX_PAYLOAD_SITES_PER_FILE: usize = 512;
/// Distinct payload shapes compared per topic side; past it the topic is not compared.
pub const MAX_PAYLOAD_SHAPES_PER_TOPIC: usize = 8;

/// One field of a payload type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PayloadField {
    pub name: CompactStr,
    /// The field's declared type as written (bounded, no literal), `None` when not written or
    /// not a plain type.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ty: Option<CompactStr>,
    pub line: u32,
    /// Declared optional (`name?: T`): a consumer may expect it without a producer sending it.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub optional: bool,
}

/// A class, interface, object type alias or record declared in a file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PayloadType {
    pub name: CompactStr,
    pub line: u32,
    pub fields: Vec<PayloadField>,
    /// The field list is partial: [`MAX_PAYLOAD_FIELDS_PER_TYPE`], a spread, an `extends` or a
    /// super class whose fields are not read here.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub incomplete: bool,
}

/// The payload a site uses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PayloadShape {
    /// A type name (simple name, generics stripped), resolved against the declared types.
    Named(CompactStr),
    /// An inline object type or object literal.
    Inline {
        fields: Vec<PayloadField>,
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        incomplete: bool,
    },
    /// A raw string, bytes, `any`, a value whose type is not written: not statically typed.
    Untyped,
}

/// The payload of one producer or consumer site, matched to its graph node by line overlap.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PayloadSite {
    pub producer: bool,
    /// Lines of the construct (call, handler method) the site's node overlaps.
    pub start: u32,
    pub end: u32,
    /// Line of the payload expression or parameter.
    pub line: u32,
    /// The literal topic argument, when there is one: what tells two sites of one node apart.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic: Option<CompactStr>,
    pub shape: PayloadShape,
}

/// The payload facts of one file.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PayloadFacts {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sites: Vec<PayloadSite>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub types: Vec<PayloadType>,
    /// A per-file cap dropped sites or types.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub capped: bool,
    /// The repository of the file (what "same repository" means when a type name resolves).
    #[serde(default)]
    pub repo_id: RepoId,
}

impl PayloadFacts {
    pub fn is_empty(&self) -> bool {
        self.sites.is_empty() && self.types.is_empty() && !self.capped
    }

    /// Adds a site unless the per-file cap is reached (then marks the file capped).
    pub fn push_site(&mut self, site: PayloadSite) {
        if self.sites.len() >= MAX_PAYLOAD_SITES_PER_FILE {
            self.capped = true;
        } else {
            self.sites.push(site);
        }
    }

    /// Adds a declared type unless the per-file cap is reached (then marks the file capped).
    pub fn push_type(&mut self, ty: PayloadType) {
        if self.types.len() >= MAX_PAYLOAD_TYPES_PER_FILE {
            self.capped = true;
        } else {
            self.types.push(ty);
        }
    }

    /// Appends `other` (a second extractor of the same file).
    pub fn merge(&mut self, other: PayloadFacts) {
        self.capped |= other.capped;
        for s in other.sites {
            self.push_site(s);
        }
        for t in other.types {
            self.push_type(t);
        }
    }
}

/// Stored per file in the graph.
#[derive(Debug)]
pub(super) struct FilePayloads {
    repo_id: RepoId,
    facts: PayloadFacts,
}

/// The resolved payload of one site (or of several sites sharing it).
#[derive(Debug, Clone, Serialize)]
pub struct PayloadSide<'g> {
    /// The first site (sorted) that uses this payload.
    pub site: &'g ContractNode,
    /// Number of live sites of this side that use it.
    pub sites: usize,
    /// The type name, `None` for an inline object type or literal.
    pub type_name: Option<&'g str>,
    /// Where the type is declared (or the inline payload is written).
    pub file: &'g FilePath,
    pub repo_id: RepoId,
    pub line: u32,
    pub fields: &'g [PayloadField],
    pub incomplete: bool,
}

/// A field present on one side only.
#[derive(Debug, Clone, Serialize)]
pub struct FieldGap<'g> {
    /// Index into the entry's `producers` or `consumers`.
    pub side: usize,
    pub field: &'g PayloadField,
}

/// A field both sides use with types of different JSON families.
#[derive(Debug, Clone, Serialize)]
pub struct FieldClash<'g> {
    pub producer: FieldGap<'g>,
    pub consumer: FieldGap<'g>,
}

/// A direction of the comparison that was not made, and why.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NotCompared {
    /// Some site of the other side is not statically typed (or its type is not resolved).
    Untyped,
    /// A field list of the other side is partial (a cap, a spread, an `extends`).
    Incomplete,
}

/// One topic whose producers and consumers disagree on the payload.
#[derive(Debug, Clone, Serialize)]
pub struct PayloadEntry<'g> {
    pub topic: TopicEntry<'g>,
    pub producers: Vec<PayloadSide<'g>>,
    pub consumers: Vec<PayloadSide<'g>>,
    /// Fields a consumer expects (not optional) that no producer sends.
    pub consumer_only: Vec<FieldGap<'g>>,
    /// Fields a producer sends that no consumer expects.
    pub producer_only: Vec<FieldGap<'g>>,
    pub clashes: Vec<FieldClash<'g>>,
    /// Every side is a named type and no producer type name is a consumer type name.
    pub names_differ: bool,
    /// `consumer_only` was not computed (the producers are not all typed and complete).
    pub consumer_only_skipped: Option<NotCompared>,
    /// `producer_only` was not computed.
    pub producer_only_skipped: Option<NotCompared>,
    /// Live sites of the topic whose payload is not statically typed.
    pub untyped_sites: usize,
}

/// A topic with both ends that the payload check could not compare.
#[derive(Debug, Clone, Serialize)]
pub struct PayloadSkipped<'g> {
    pub topic: TopicEntry<'g>,
    pub reason: PayloadSkipReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PayloadSkipReason {
    /// No producer, or no consumer, has a statically typed payload.
    NotTyped,
    /// More than [`MAX_PAYLOAD_SHAPES_PER_TOPIC`] distinct payloads on one side.
    TooManyShapes,
}

/// The payload check of the whole graph.
#[derive(Debug, Clone, Default, Serialize)]
pub struct PayloadReport<'g> {
    pub mismatches: Vec<PayloadEntry<'g>>,
    /// Topics with live producers and consumers that were not compared, with the reason.
    pub skipped: Vec<PayloadSkipped<'g>>,
    /// Topics with live producers and consumers (the population of the check).
    pub topics: usize,
    /// Of them, compared with no finding in each direction that could be compared.
    pub matching: usize,
    /// Of the matching ones, those where a direction was not compared.
    pub matching_partial: usize,
    /// Live sites whose payload is not statically typed (no fact, untyped, ambiguous fact).
    pub untyped_sites: usize,
    /// Live sites whose type name no indexed file declares.
    pub undeclared_sites: usize,
    /// Live sites whose type name several indexed files declare (not compared).
    pub ambiguous_sites: usize,
    /// Files whose payload facts a per-file cap cut.
    pub capped_files: usize,
}

/// What a site's payload resolves to.
enum Resolved<'g> {
    Side {
        type_name: Option<&'g str>,
        file: &'g FilePath,
        repo_id: RepoId,
        line: u32,
        fields: &'g [PayloadField],
        incomplete: bool,
    },
    Untyped,
    Undeclared,
    Ambiguous,
}

/// The JSON family of a declared field type, when it is a primitive (or an array of one): only
/// these are compared, a named object type is not (two services name their DTOs differently).
pub fn json_family(ty: &str) -> Option<String> {
    let t: String = ty.chars().filter(|c| !c.is_whitespace()).collect();
    let t = t
        .trim_end_matches("|null")
        .trim_end_matches("|undefined")
        .trim_end_matches("|null")
        .to_string();
    if let Some(inner) = t.strip_suffix("[]") {
        return json_family(inner).map(|f| format!("{f}[]"));
    }
    for wrapper in ["Array<", "List<", "Set<", "Collection<", "ArrayList<"] {
        if let Some(inner) = t.strip_prefix(wrapper).and_then(|r| r.strip_suffix('>')) {
            return json_family(inner).map(|f| format!("{f}[]"));
        }
    }
    let leaf = t.rsplit('.').next().unwrap_or(&t);
    let family = match leaf {
        "string" | "String" | "CharSequence" => "string",
        "number" | "int" | "Integer" | "long" | "Long" | "short" | "Short" | "double"
        | "Double" | "float" | "Float" | "BigDecimal" | "BigInteger" | "bigint" => "number",
        "boolean" | "Boolean" | "bool" => "boolean",
        _ => return None,
    };
    Some(family.to_string())
}

impl ContractGraph {
    /// Stores the payload facts of the file at `path` (replacing any earlier ones).
    pub fn add_payload_facts(&mut self, path: FilePath, repo_id: RepoId, facts: PayloadFacts) {
        if facts.is_empty() {
            self.payload_facts.remove(&path);
            return;
        }
        self.payload_facts
            .insert(path, Arc::new(FilePayloads { repo_id, facts }));
    }

    /// The payload facts of one file, if any.
    pub fn payload_facts_of(&self, path: &Path) -> Option<&PayloadFacts> {
        self.payload_facts.get(path).map(|f| &f.facts)
    }

    /// The payload check over `entries` (every hub with its sites, test flags set), keeping a
    /// row when one of its sites passes `in_scope`.
    pub(super) fn payload_mismatches<'g>(
        &'g self,
        entries: &[TopicEntry<'g>],
        in_scope: &dyn Fn(&ContractNode) -> bool,
    ) -> PayloadReport<'g> {
        // Declared types by name, sorted by (file, line) so the pick is deterministic.
        let mut declared: HashMap<&'g str, Vec<(&'g FilePath, &'g FilePayloads, &'g PayloadType)>> =
            HashMap::new();
        let mut out = PayloadReport::default();
        for (path, f) in &self.payload_facts {
            out.capped_files += usize::from(f.facts.capped);
            for t in &f.facts.types {
                declared
                    .entry(t.name.as_str())
                    .or_default()
                    .push((path, f, t));
            }
        }

        for e in entries {
            let producers: Vec<&AnomalySite<'g>> = e.producers.iter().filter(|s| !s.test).collect();
            let consumers: Vec<&AnomalySite<'g>> = e.consumers.iter().filter(|s| !s.test).collect();
            if producers.is_empty() || consumers.is_empty() {
                continue;
            }
            if !producers.iter().chain(&consumers).any(|s| in_scope(s.node)) {
                continue;
            }
            out.topics += 1;
            let mut untyped = 0usize;
            let mut sides = |list: &[&AnomalySite<'g>], producer: bool, untyped: &mut usize| {
                let mut v: Vec<PayloadSide<'g>> = Vec::new();
                for s in list {
                    match self.resolve_payload(s.node, producer, &e.key, &declared) {
                        Resolved::Side {
                            type_name,
                            file,
                            repo_id,
                            line,
                            fields,
                            incomplete,
                        } => {
                            if let Some(same) = v.iter_mut().find(|p| {
                                p.file == file && p.line == line && p.type_name == type_name
                            }) {
                                same.sites += 1;
                            } else {
                                v.push(PayloadSide {
                                    site: s.node,
                                    sites: 1,
                                    type_name,
                                    file,
                                    repo_id,
                                    line,
                                    fields,
                                    incomplete,
                                });
                            }
                        }
                        Resolved::Untyped => *untyped += 1,
                        Resolved::Undeclared => {
                            *untyped += 1;
                            out.undeclared_sites += 1;
                        }
                        Resolved::Ambiguous => {
                            *untyped += 1;
                            out.ambiguous_sites += 1;
                        }
                    }
                }
                v.sort_by(|a, b| node_key(a.site).cmp(&node_key(b.site)));
                v
            };
            let mut p_untyped = 0;
            let ps = sides(&producers, true, &mut p_untyped);
            let mut c_untyped = 0;
            let cs = sides(&consumers, false, &mut c_untyped);
            untyped += p_untyped + c_untyped;
            out.untyped_sites += untyped;
            if ps.is_empty() || cs.is_empty() {
                out.skipped.push(PayloadSkipped {
                    topic: e.clone(),
                    reason: PayloadSkipReason::NotTyped,
                });
                continue;
            }
            if ps.len() > MAX_PAYLOAD_SHAPES_PER_TOPIC || cs.len() > MAX_PAYLOAD_SHAPES_PER_TOPIC {
                out.skipped.push(PayloadSkipped {
                    topic: e.clone(),
                    reason: PayloadSkipReason::TooManyShapes,
                });
                continue;
            }

            let skipped_if = |untyped: usize, sides: &[PayloadSide<'_>]| {
                if untyped > 0 {
                    Some(NotCompared::Untyped)
                } else if sides.iter().any(|s| s.incomplete) {
                    Some(NotCompared::Incomplete)
                } else {
                    None
                }
            };
            let consumer_only_skipped = skipped_if(p_untyped, &ps);
            let producer_only_skipped = skipped_if(c_untyped, &cs);
            let names = |sides: &[PayloadSide<'g>]| -> BTreeSet<&'g str> {
                sides
                    .iter()
                    .flat_map(|s| s.fields.iter().map(|f| f.name.as_str()))
                    .collect()
            };
            let (p_names, c_names) = (names(&ps), names(&cs));
            let gaps = |sides: &[PayloadSide<'g>], other: &BTreeSet<&str>, skip_optional: bool| {
                let mut v: Vec<FieldGap<'g>> = Vec::new();
                let mut seen: BTreeSet<&str> = BTreeSet::new();
                for (i, s) in sides.iter().enumerate() {
                    for f in s.fields {
                        if (skip_optional && f.optional) || other.contains(f.name.as_str()) {
                            continue;
                        }
                        if seen.insert(f.name.as_str()) {
                            v.push(FieldGap { side: i, field: f });
                        }
                    }
                }
                v.sort_by(|a, b| a.field.name.cmp(&b.field.name));
                v
            };
            let consumer_only = if consumer_only_skipped.is_none() {
                gaps(&cs, &p_names, true)
            } else {
                Vec::new()
            };
            let producer_only = if producer_only_skipped.is_none() {
                gaps(&ps, &c_names, false)
            } else {
                Vec::new()
            };
            let mut clashes: Vec<FieldClash<'g>> = Vec::new();
            let mut clashed: BTreeSet<&str> = BTreeSet::new();
            for (pi, p) in ps.iter().enumerate() {
                for pf in p.fields {
                    let Some(pfam) = pf.ty.as_deref().and_then(json_family) else {
                        continue;
                    };
                    for (ci, c) in cs.iter().enumerate() {
                        let Some(cf) = c.fields.iter().find(|f| f.name == pf.name) else {
                            continue;
                        };
                        let Some(cfam) = cf.ty.as_deref().and_then(json_family) else {
                            continue;
                        };
                        if pfam != cfam && clashed.insert(pf.name.as_str()) {
                            clashes.push(FieldClash {
                                producer: FieldGap {
                                    side: pi,
                                    field: pf,
                                },
                                consumer: FieldGap {
                                    side: ci,
                                    field: cf,
                                },
                            });
                        }
                    }
                }
            }
            clashes.sort_by(|a, b| a.producer.field.name.cmp(&b.producer.field.name));
            let named = |sides: &[PayloadSide<'g>]| -> Option<BTreeSet<&'g str>> {
                sides.iter().map(|s| s.type_name).collect()
            };
            let names_differ = match (named(&ps), named(&cs)) {
                (Some(a), Some(b)) => a.is_disjoint(&b),
                _ => false,
            };
            if consumer_only.is_empty() && producer_only.is_empty() && clashes.is_empty() {
                out.matching += 1;
                out.matching_partial +=
                    usize::from(consumer_only_skipped.is_some() || producer_only_skipped.is_some());
                continue;
            }
            out.mismatches.push(PayloadEntry {
                topic: e.clone(),
                producers: ps,
                consumers: cs,
                consumer_only,
                producer_only,
                clashes,
                names_differ,
                consumer_only_skipped,
                producer_only_skipped,
                untyped_sites: untyped,
            });
        }
        out
    }

    /// The payload of `node` as a producer (or consumer) of the topic `key`.
    fn resolve_payload<'g>(
        &'g self,
        node: &'g ContractNode,
        producer: bool,
        key: &str,
        declared: &HashMap<&'g str, Vec<(&'g FilePath, &'g FilePayloads, &'g PayloadType)>>,
    ) -> Resolved<'g> {
        let Some(file) = self.payload_facts.get(&node.file_path) else {
            return Resolved::Untyped;
        };
        let (lo, hi) = (
            node.line_start as u32,
            node.line_end.max(node.line_start) as u32,
        );
        let mut hits: Vec<&PayloadSite> = file
            .facts
            .sites
            .iter()
            .filter(|s| s.producer == producer && s.start <= hi && s.end >= lo)
            .collect();
        if hits.len() > 1 {
            let named: Vec<&PayloadSite> = hits
                .iter()
                .copied()
                .filter(|s| {
                    s.topic
                        .as_deref()
                        .is_some_and(|t| t.eq_ignore_ascii_case(key))
                })
                .collect();
            if !named.is_empty() {
                hits = named;
            }
        }
        let Some(first) = hits.first() else {
            return Resolved::Untyped;
        };
        // Two sites of one node with different payloads: which one feeds this topic is unknown.
        if hits.iter().any(|s| s.shape != first.shape) {
            return Resolved::Ambiguous;
        }
        match &first.shape {
            PayloadShape::Untyped => Resolved::Untyped,
            PayloadShape::Inline { fields, incomplete } => Resolved::Side {
                type_name: None,
                file: &node.file_path,
                repo_id: node.repo_id,
                line: first.line,
                fields,
                incomplete: *incomplete,
            },
            PayloadShape::Named(name) => {
                let Some(cands) = declared.get(name.as_str()) else {
                    return Resolved::Undeclared;
                };
                // Same file, then same repository, then the workspace; a tie is ambiguous.
                let pick = |f: &dyn Fn(&(&FilePath, &FilePayloads, &PayloadType)) -> bool| {
                    let v: Vec<_> = cands.iter().filter(|c| f(c)).collect();
                    v
                };
                let same_file = pick(&|c| *c.0 == node.file_path);
                let same_repo = pick(&|c| c.1.repo_id == node.repo_id);
                let chosen = if same_file.len() == 1 {
                    same_file[0]
                } else if same_file.is_empty() && same_repo.len() == 1 {
                    same_repo[0]
                } else if same_file.is_empty() && same_repo.is_empty() && cands.len() == 1 {
                    &cands[0]
                } else {
                    return Resolved::Ambiguous;
                };
                Resolved::Side {
                    type_name: Some(chosen.2.name.as_str()),
                    file: chosen.0,
                    repo_id: chosen.1.repo_id,
                    line: chosen.2.line,
                    fields: &chosen.2.fields,
                    incomplete: chosen.2.incomplete,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_families_compare_primitives_only() {
        assert_eq!(json_family("string").as_deref(), Some("string"));
        assert_eq!(json_family("String").as_deref(), Some("string"));
        assert_eq!(json_family("Long").as_deref(), Some("number"));
        assert_eq!(json_family("number | null").as_deref(), Some("number"));
        assert_eq!(json_family("List<Integer>").as_deref(), Some("number[]"));
        assert_eq!(json_family("string[]").as_deref(), Some("string[]"));
        assert_eq!(json_family("CustomerDto"), None);
        assert_eq!(json_family("Instant"), None);
    }

    #[test]
    fn site_cap_is_recorded() {
        let mut f = PayloadFacts::default();
        for i in 0..(MAX_PAYLOAD_SITES_PER_FILE as u32 + 3) {
            f.push_site(PayloadSite {
                producer: true,
                start: i,
                end: i,
                line: i,
                topic: None,
                shape: PayloadShape::Untyped,
            });
        }
        assert_eq!(f.sites.len(), 512);
        assert!(f.capped);
    }
}
