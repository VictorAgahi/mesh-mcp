//! The observed event graph as the AsyncAPI export and its declared-versus-code diff read it
//! (9.0, ticket T12): one row per topic hub (per broker) with its live producer and consumer
//! sites, each site's payload as the payload check (T11) resolves it, and the topics that cannot
//! be channels.
//!
//! What is left out, and counted so the caller can say so:
//!
//! - test-file sites (not production), and a hub that has none other;
//! - the channel nodes a declared AsyncAPI file contributes (`package == "asyncapi"`): they are
//!   the declaration, not the code, and counting them as producers would make the diff compare a
//!   document with itself;
//! - topics whose name is a `${KEY}` placeholder no configuration resolved: not a channel, listed
//!   as unresolved ("not statically resolved" is not absence).
//!
//! A site whose broker is not statically known keeps `heuristic` at best, like in the anomaly
//! report. Every list is sorted; no hash order reaches a caller.

use super::anomalies::{AnomalySite, TopicEntry};
use super::flow::{node_key, weakest};
use super::payload::SitePayload;
use super::*;

/// Package of the channel nodes a declared AsyncAPI document contributes to the graph.
pub const ASYNCAPI_PACKAGE: &str = "asyncapi";

/// A resolved topic hub with its live sites and their payloads.
#[derive(Debug, Clone, Serialize)]
pub struct ObservedChannel<'g> {
    /// Key, broker, live producers and consumers, weakest confidence.
    pub topic: TopicEntry<'g>,
    /// The payload of each producer, in `topic.producers` order.
    pub producer_payloads: Vec<SitePayload<'g>>,
    /// The payload of each consumer, in `topic.consumers` order.
    pub consumer_payloads: Vec<SitePayload<'g>>,
    /// The key is a wildcard subject (`orders.*`): a subscription pattern, not one topic.
    pub wildcard: bool,
}

/// Every observed channel of the graph.
#[derive(Debug, Clone, Default, Serialize)]
pub struct ObservedEvents<'g> {
    /// Resolved topics with at least one live site, sorted by (key, broker).
    pub channels: Vec<ObservedChannel<'g>>,
    /// Placeholder-named topics (not channels), sorted by key.
    pub unresolved: Vec<TopicEntry<'g>>,
    /// Test-file sites left out.
    pub test_sites: usize,
    /// Declared AsyncAPI channel nodes left out.
    pub declared_sites: usize,
}

impl ContractGraph {
    /// The observed event channels. `is_test` is the caller's test-file rule.
    pub fn observed_events<'g>(
        &'g self,
        is_test: &dyn Fn(&ContractNode) -> bool,
    ) -> ObservedEvents<'g> {
        let mut hubs: Vec<&ContractNode> = self
            .nodes
            .values()
            .filter(|n| Self::is_hub_like(n))
            .collect();
        hubs.sort_by(|a, b| {
            (Self::hub_key(a), Self::hub_broker(a), a.id).cmp(&(
                Self::hub_key(b),
                Self::hub_broker(b),
                b.id,
            ))
        });
        let hub_ids: HashSet<NodeId> = hubs.iter().map(|h| h.id).collect();
        let mut produced: HashMap<NodeId, Vec<(NodeId, EdgeConfidence)>> = HashMap::new();
        let mut consumed: HashMap<NodeId, Vec<(NodeId, EdgeConfidence)>> = HashMap::new();
        for e in &self.edges {
            match e.kind {
                EdgeKind::Produces if hub_ids.contains(&e.to) => {
                    produced
                        .entry(e.to)
                        .or_default()
                        .push((e.from, e.confidence));
                }
                EdgeKind::Consumes if hub_ids.contains(&e.from) => {
                    consumed
                        .entry(e.from)
                        .or_default()
                        .push((e.to, e.confidence));
                }
                _ => {}
            }
        }

        let declared_types = self.declared_payload_types();
        let mut out = ObservedEvents::default();
        for hub in hubs {
            let key = Self::hub_key(hub);
            let broker = self.hub_ref(hub).broker;
            let mut sites = |list: Option<&Vec<(NodeId, EdgeConfidence)>>| -> Vec<AnomalySite<'g>> {
                let mut v: Vec<AnomalySite<'g>> = Vec::new();
                for (id, c) in list.map(Vec::as_slice).unwrap_or(&[]) {
                    let Some(node) = self.nodes.get(id) else {
                        continue;
                    };
                    if node.package == ASYNCAPI_PACKAGE {
                        out.declared_sites += 1;
                        continue;
                    }
                    if is_test(node) {
                        out.test_sites += 1;
                        continue;
                    }
                    v.push(AnomalySite {
                        node,
                        // No broker: the site comes from an extractor that does not identify the
                        // transport, never `exact`.
                        confidence: if broker.is_some() {
                            *c
                        } else {
                            weakest(*c, EdgeConfidence::Heuristic)
                        },
                        test: false,
                    });
                }
                v.sort_by(|a, b| node_key(a.node).cmp(&node_key(b.node)));
                v.dedup_by_key(|s| s.node.id);
                v
            };
            let producers = sites(produced.get(&hub.id));
            let consumers = sites(consumed.get(&hub.id));
            if producers.is_empty() && consumers.is_empty() {
                continue;
            }
            let confidence = producers
                .iter()
                .chain(&consumers)
                .fold(EdgeConfidence::Exact, |acc, s| weakest(acc, s.confidence));
            let wildcard = is_subject_pattern(key);
            let placeholder = key.contains("${");
            let topic = TopicEntry {
                hub,
                key: CompactStr::new(key),
                broker,
                producers,
                consumers,
                confidence,
            };
            if placeholder {
                out.unresolved.push(topic);
                continue;
            }
            let nodes = |sites: &[AnomalySite<'g>]| -> Vec<&'g ContractNode> {
                sites.iter().map(|s| s.node).collect()
            };
            let producer_payloads =
                self.site_payloads(&nodes(&topic.producers), true, key, &declared_types);
            let consumer_payloads =
                self.site_payloads(&nodes(&topic.consumers), false, key, &declared_types);
            out.channels.push(ObservedChannel {
                topic,
                producer_payloads,
                consumer_payloads,
                wildcard,
            });
        }
        out
    }

    /// The files a declared AsyncAPI document was indexed from (the files that contributed
    /// `package == "asyncapi"` channel nodes), with their repository, sorted by path.
    pub fn declared_asyncapi_files(&self) -> Vec<(FilePath, RepoId)> {
        let files: BTreeMap<&Path, (&FilePath, RepoId)> = self
            .nodes
            .values()
            .filter(|n| n.package == ASYNCAPI_PACKAGE && n.kind == NodeKind::EventStream)
            .map(|n| (&*n.file_path, (&n.file_path, n.repo_id)))
            .collect();
        files.into_values().map(|(p, r)| (p.clone(), r)).collect()
    }
}
