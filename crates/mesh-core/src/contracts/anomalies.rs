//! Workspace event anomaly report (9.0, ticket T9): the places where the event graph
//! looks wrong, in one pass over every topic hub.
//!
//! Five classes, plus a sixth that keeps the report honest:
//!
//! - **published, never consumed**: a topic with producers and no consumer;
//! - **consumed, never published**: a topic with consumers and no producer (a producer that
//!   exists only in test files is named as such);
//! - **cycles**: event chains that loop back (a strongly connected set of topics, with the
//!   shortest cycle through its first topic as the witness, each hop with its call path's
//!   consumer and producer sites);
//! - **abnormal fan-out**: a topic with at least [`ANOMALY_FAN_OUT_CONSUMERS`] consumer sites,
//!   and a producer file publishing onto at least [`ANOMALY_FAN_OUT_TOPICS`] topics;
//! - **test-only consumers**: a topic whose every consumer site is in a test file;
//! - **unresolved**: a one-sided topic that cannot be called an anomaly, with the reason: its
//!   name is a `${KEY}` placeholder (config-driven, no file resolved it), a wildcard
//!   subscription (the producers of matching subjects may be built at runtime), or a
//!   dead-letter / retry channel (fed or drained by the framework or by operations).
//!
//! "No producer" always means "none statically resolved in the indexed roots": the name may be
//! built at runtime or the producer may live in an unindexed root, which is why the report
//! carries that caveat and the `unresolved` class instead of an absolute claim.
//!
//! Brokers are never joined: a topic is one hub, and a name used on two known brokers is two
//! hubs with their own rows. A site whose broker is unknown joins every hub of the name,
//! `heuristic` (the topic pass already decided that, the report reads its edges).
//!
//! Every walk is iterative (explicit stacks and queues) with visited sets, every list is
//! sorted before it is used or shown, and no hash order reaches the output.

use super::flow::{node_key, rank, sort_nodes, weakest, TopicHop};
use super::*;
use std::collections::VecDeque;

/// A topic with at least this many (non-test) consumer sites is an abnormal fan-out. Six, because
/// a handful of subscribers (audit, search, notifications, analytics) is common and five more is
/// a topic nobody can reason about at a glance; edit here to tune the report.
pub const ANOMALY_FAN_OUT_CONSUMERS: usize = 6;
/// A producer file publishing onto at least this many distinct (non-wildcard) topics is an
/// abnormal fan-out (a god publisher, an outbox relay).
pub const ANOMALY_FAN_OUT_TOPICS: usize = 6;
/// Most consumer sites the cycle search walks; past it the search stops and says so.
pub const ANOMALY_MAX_CYCLE_SITES: usize = 20_000;

/// Which end of a topic is missing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Missing {
    Producer,
    Consumer,
}

/// Why a one-sided topic is not called an anomaly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UnresolvedReason {
    /// The name is a `${KEY}` placeholder no configuration file resolved.
    Placeholder,
    /// A wildcard subscription: only statically named subjects were matched against it.
    Wildcard,
    /// A dead-letter or retry channel.
    DeadLetter,
}

/// One producer or consumer site of a topic.
#[derive(Debug, Clone, Serialize)]
pub struct AnomalySite<'g> {
    pub node: &'g ContractNode,
    /// Confidence of the edge that links the site to the topic.
    pub confidence: EdgeConfidence,
    /// The site's file is a test file.
    pub test: bool,
}

/// One topic hub with its sites, as a row of the report.
#[derive(Debug, Clone, Serialize)]
pub struct TopicEntry<'g> {
    pub hub: &'g ContractNode,
    /// The topic key, without a broker qualifier.
    pub key: CompactStr,
    /// The broker of the hub; for a plain hub, the name's only known broker.
    pub broker: Option<Broker>,
    pub producers: Vec<AnomalySite<'g>>,
    pub consumers: Vec<AnomalySite<'g>>,
    /// The weakest link among the sites the row is about.
    pub confidence: EdgeConfidence,
}

/// A one-sided topic that is not reported as an anomaly.
#[derive(Debug, Clone, Serialize)]
pub struct UnresolvedEntry<'g> {
    pub topic: TopicEntry<'g>,
    pub missing: Missing,
    pub reason: UnresolvedReason,
}

/// A topic hub with its broker (the hub's, else the name's only known broker).
#[derive(Debug, Clone, Copy, Serialize)]
pub struct HubRef<'g> {
    pub hub: &'g ContractNode,
    pub broker: Option<Broker>,
}

/// One hop of a cycle: the consumer of `from` reaches the producer that publishes onto `to`.
#[derive(Debug, Clone, Serialize)]
pub struct CycleEdge<'g> {
    pub from: HubRef<'g>,
    pub to: HubRef<'g>,
    pub consumer: &'g ContractNode,
    pub producer: &'g ContractNode,
    pub confidence: EdgeConfidence,
}

/// A set of topics whose events loop back (a strongly connected component of the topic graph).
#[derive(Debug, Clone, Serialize)]
pub struct CycleEntry<'g> {
    /// Every topic hub of the loop, sorted by key.
    pub members: Vec<HubRef<'g>>,
    /// The shortest cycle through the first member (a single edge for a self-loop).
    pub path: Vec<CycleEdge<'g>>,
    /// The weakest hop of `path`.
    pub confidence: EdgeConfidence,
}

/// A producer file that publishes onto many topics.
#[derive(Debug, Clone, Serialize)]
pub struct FanOutProducer<'g> {
    pub file: &'g FilePath,
    pub repo_id: RepoId,
    /// The hubs it publishes onto, sorted.
    pub topics: Vec<HubRef<'g>>,
    /// Its producer sites, sorted.
    pub sites: Vec<&'g ContractNode>,
}

/// The report. Every list is sorted; rows outside the caller's scope are already dropped.
#[derive(Debug, Clone, Default, Serialize)]
pub struct EventAnomalies<'g> {
    pub published_never_consumed: Vec<TopicEntry<'g>>,
    pub consumed_never_published: Vec<TopicEntry<'g>>,
    pub unresolved: Vec<UnresolvedEntry<'g>>,
    pub cycles: Vec<CycleEntry<'g>>,
    pub fan_out_topics: Vec<TopicEntry<'g>>,
    pub fan_out_producers: Vec<FanOutProducer<'g>>,
    pub test_only_consumers: Vec<TopicEntry<'g>>,
    /// Topic hubs read.
    pub topics: usize,
    /// Distinct producer and consumer sites behind them.
    pub producer_sites: usize,
    pub consumer_sites: usize,
    /// Hubs named by a `${KEY}` placeholder: never part of the cycle or fan-out search, since a
    /// placeholder can stand for several unrelated names.
    pub placeholder_topics: usize,
    /// Same-file-only hops (no resolved call path) the cycle search did not follow: a cycle is
    /// only reported when every hop of it rests on a call path.
    pub cycle_guess_hops: usize,
    /// Consumer sites the cycle search walked, of `cycle_sites_total`.
    pub cycle_sites_walked: usize,
    pub cycle_sites_total: usize,
    /// The cycle search stopped at [`ANOMALY_MAX_CYCLE_SITES`]: the sites are walked in path
    /// order, so everything from this one on was not searched and the cycle count is a lower
    /// bound.
    pub cycle_first_unsearched: Option<&'g ContractNode>,
    /// Publisher-versus-consumer payload check (9.0 T11), over the topics with live sites on both
    /// ends (placeholder and wildcard hubs left out).
    pub payload: super::payload::PayloadReport<'g>,
}

/// The topic key and broker of a hub node of the graph (`kafka:orders` is `("orders", Kafka)`).
pub fn hub_topic(hub: &ContractNode) -> (&str, Option<Broker>) {
    (ContractGraph::hub_key(hub), ContractGraph::hub_broker(hub))
}

/// Whether `key` is a `${KEY}` placeholder topic (or holds one).
fn is_placeholder(key: &str) -> bool {
    key.contains("${")
}

/// Whether `key` names a dead-letter or retry channel: its last segment, past any trailing
/// numbers (`orders-retry-2`), is one of the usual words.
pub(super) fn is_dead_letter(key: &str) -> bool {
    let lower = key.to_ascii_lowercase();
    let mut tokens = lower
        .split(['.', '-', '_', ':', '/'])
        .filter(|t| !t.is_empty() && !t.bytes().all(|b| b.is_ascii_digit()))
        .rev();
    match tokens.next() {
        Some("dlt" | "dlq" | "retry" | "deadletter") => true,
        // `dead-letter`, `dead_letter`, `dead.letter`: both words, never a bare `letter`.
        Some("letter") => tokens.next() == Some("dead"),
        _ => false,
    }
}

/// The sites of one hub, before they become rows.
struct HubSites<'g> {
    hub: &'g ContractNode,
    key: &'g str,
    producers: Vec<AnomalySite<'g>>,
    consumers: Vec<AnomalySite<'g>>,
}

impl HubSites<'_> {
    fn live(sites: &[AnomalySite<'_>]) -> usize {
        sites.iter().filter(|s| !s.test).count()
    }
}

fn any_in<'a>(
    mut sites: impl Iterator<Item = &'a ContractNode>,
    f: &dyn Fn(&ContractNode) -> bool,
) -> bool {
    sites.any(f)
}

fn weakest_of<'a>(sites: impl Iterator<Item = &'a AnomalySite<'a>>) -> EdgeConfidence {
    sites.fold(EdgeConfidence::Exact, |acc, s| weakest(acc, s.confidence))
}

impl ContractGraph {
    /// `hub` with its broker: the hub's own, else the name's only known broker.
    fn hub_ref<'g>(&self, hub: &'g ContractNode) -> HubRef<'g> {
        let broker = Self::hub_broker(hub).or_else(|| {
            let mask = self.known_brokers(Self::hub_key(hub));
            (mask.count_ones() == 1)
                .then(|| Broker::of_mask(mask).next())
                .flatten()
        });
        HubRef { hub, broker }
    }

    /// The anomaly report of the whole graph. `is_test` is the caller's test-file rule;
    /// `in_scope` keeps a row only when one of its sites passes (the analysis itself always
    /// reads every site: a producer outside the scope still makes a topic consumed).
    pub fn event_anomalies<'g>(
        &'g self,
        is_test: &dyn Fn(&ContractNode) -> bool,
        in_scope: &dyn Fn(&ContractNode) -> bool,
    ) -> EventAnomalies<'g> {
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
        // A concrete subject is heard when a wildcard hub with consumers matches it and one of
        // its producers is linked to that hub. The edge metadata is not read: a producer that
        // publishes several subjects has one edge per wildcard hub, carrying only the first
        // subject, so each subject of the producer is matched against the pattern itself.
        let mut wild_of: HashMap<NodeId, Vec<&ContractNode>> = HashMap::new();
        let mut wild_consumers: HashMap<NodeId, u32> = HashMap::new();
        for h in hubs.iter().filter(|h| is_subject_pattern(Self::hub_key(h))) {
            let Some(cons) = consumed.get(&h.id).filter(|c| !c.is_empty()) else {
                continue;
            };
            // The brokers the pattern's consumers speak to (0: some consumer's broker unknown).
            let mask = cons
                .iter()
                .map(|(id, _)| self.site_mask(false, Self::hub_key(h), *id))
                .try_fold(0u32, |acc, m| (m != 0).then_some(acc | m))
                .unwrap_or(0);
            wild_consumers.insert(h.id, mask);
        }
        for e in &self.edges {
            if e.kind == EdgeKind::Produces && wild_consumers.contains_key(&e.to) {
                if let Some(h) = self.nodes.get(&e.to) {
                    wild_of.entry(e.from).or_default().push(h);
                }
            }
        }
        let capped: HashSet<NodeId> = hubs
            .iter()
            .filter(|h| {
                self.wildcard_capped
                    .contains(&(CompactStr::new(Self::hub_key(h)), Self::hub_broker(h)))
            })
            .map(|h| h.id)
            .collect();
        let compat = |a: u32, b: u32| a == 0 || b == 0 || a & b != 0;
        let mut heard: HashSet<NodeId> = HashSet::new();
        for h in &hubs {
            let key = Self::hub_key(h);
            if is_subject_pattern(key) {
                continue;
            }
            for (pid, _) in produced.get(&h.id).map(Vec::as_slice).unwrap_or(&[]) {
                let pm = self.site_mask(true, key, *pid);
                let linked = wild_of.get(pid).is_some_and(|ws| {
                    ws.iter().any(|w| {
                        subject_matches(Self::hub_key(w), key)
                            && compat(pm, wild_consumers.get(&w.id).copied().unwrap_or(0))
                    })
                });
                // A wildcard hub cut at the producer cap lists only some of its producers.
                let cut = wild_consumers.iter().any(|(wid, wm)| {
                    capped.contains(wid)
                        && compat(pm, *wm)
                        && self
                            .nodes
                            .get(wid)
                            .is_some_and(|w| subject_matches(Self::hub_key(w), key))
                });
                if linked || cut {
                    heard.insert(h.id);
                    break;
                }
            }
        }

        let sites = |list: Option<&Vec<(NodeId, EdgeConfidence)>>| -> Vec<AnomalySite<'g>> {
            let mut v: Vec<AnomalySite<'g>> = list
                .map(Vec::as_slice)
                .unwrap_or(&[])
                .iter()
                .filter_map(|(id, c)| {
                    self.nodes.get(id).map(|node| AnomalySite {
                        node,
                        confidence: *c,
                        test: is_test(node),
                    })
                })
                .collect();
            v.sort_by(|a, b| node_key(a.node).cmp(&node_key(b.node)));
            v.dedup_by_key(|s| s.node.id);
            v
        };
        let all: Vec<HubSites<'g>> = hubs
            .iter()
            .map(|&hub| HubSites {
                hub,
                key: Self::hub_key(hub),
                producers: sites(produced.get(&hub.id)),
                consumers: sites(consumed.get(&hub.id)),
            })
            .collect();

        let entry = |h: &HubSites<'g>,
                     producers: Vec<AnomalySite<'g>>,
                     consumers: Vec<AnomalySite<'g>>,
                     confidence| {
            let broker = self.hub_ref(h.hub).broker;
            TopicEntry {
                hub: h.hub,
                key: CompactStr::new(h.key),
                producers,
                consumers,
                // A site whose broker is unknown comes from an extractor that does not
                // identify the transport (a decorator, a declarative pattern): never `exact`.
                confidence: if broker.is_some() {
                    confidence
                } else {
                    weakest(confidence, EdgeConfidence::Heuristic)
                },
                broker,
            }
        };

        let mut out = EventAnomalies {
            topics: all.len(),
            ..EventAnomalies::default()
        };
        let mut producer_ids: HashSet<NodeId> = HashSet::new();
        let mut consumer_ids: HashSet<NodeId> = HashSet::new();
        for h in &all {
            producer_ids.extend(h.producers.iter().map(|s| s.node.id));
            consumer_ids.extend(h.consumers.iter().map(|s| s.node.id));
        }
        out.producer_sites = producer_ids.len();
        out.consumer_sites = consumer_ids.len();

        for h in &all {
            let placeholder = is_placeholder(h.key);
            out.placeholder_topics += usize::from(placeholder);
            let (pp, pt) = (
                HubSites::live(&h.producers),
                h.producers.len() - HubSites::live(&h.producers),
            );
            let (cp, ct) = (
                HubSites::live(&h.consumers),
                h.consumers.len() - HubSites::live(&h.consumers),
            );
            let wildcard = is_subject_pattern(h.key);
            let is_heard = heard.contains(&h.hub.id);
            let reason = if placeholder {
                Some(UnresolvedReason::Placeholder)
            } else if is_dead_letter(h.key) {
                Some(UnresolvedReason::DeadLetter)
            } else {
                None
            };

            if cp == 0 && ct > 0 {
                // Every consumer is a test while production publishes onto the topic: nothing in
                // production listens (a placeholder name proves nothing either way; a topic both
                // ends of which are test code is scaffolding, not an event).
                if pp > 0 && reason != Some(UnresolvedReason::Placeholder) {
                    let tests: Vec<_> = h.consumers.iter().filter(|s| s.test).cloned().collect();
                    let c = weakest_of(tests.iter());
                    out.test_only_consumers
                        .push(entry(h, h.producers.clone(), tests, c));
                }
            } else if cp == 0 && ct == 0 && pp > 0 && !is_heard {
                let live: Vec<_> = h.producers.iter().filter(|s| !s.test).cloned().collect();
                let c = weakest_of(live.iter());
                let e = entry(h, live, Vec::new(), c);
                match reason {
                    Some(reason) => out.unresolved.push(UnresolvedEntry {
                        topic: e,
                        missing: Missing::Consumer,
                        reason,
                    }),
                    None => out.published_never_consumed.push(e),
                }
            } else if cp > 0 && pp == 0 {
                let live: Vec<_> = h.consumers.iter().filter(|s| !s.test).cloned().collect();
                let c = weakest_of(live.iter());
                // A wildcard with no matching producer at all proves nothing: the subjects it
                // matches may be built at runtime. Producers seen only in tests stay a finding.
                let reason = match (reason, wildcard && pt == 0) {
                    (Some(r), _) => Some(r),
                    (None, true) => Some(UnresolvedReason::Wildcard),
                    (None, false) => None,
                };
                let e = entry(h, h.producers.clone(), live, c);
                match reason {
                    Some(reason) => out.unresolved.push(UnresolvedEntry {
                        topic: e,
                        missing: Missing::Producer,
                        reason,
                    }),
                    None => out.consumed_never_published.push(e),
                }
            }

            if !placeholder && cp >= ANOMALY_FAN_OUT_CONSUMERS {
                let live: Vec<_> = h.consumers.iter().filter(|s| !s.test).cloned().collect();
                let c = weakest_of(live.iter());
                out.fan_out_topics.push(entry(h, Vec::new(), live, c));
            }
        }

        let both: Vec<TopicEntry<'g>> = all
            .iter()
            .filter(|h| {
                !is_placeholder(h.key)
                    && !is_subject_pattern(h.key)
                    && HubSites::live(&h.producers) > 0
                    && HubSites::live(&h.consumers) > 0
            })
            .map(|h| {
                let c = weakest_of(h.producers.iter().chain(&h.consumers).filter(|s| !s.test));
                entry(h, h.producers.clone(), h.consumers.clone(), c)
            })
            .collect();
        out.payload = self.payload_mismatches(&both, in_scope);

        self.fan_out_producers(&all, is_test, &mut out);
        self.cycles(&all, is_test, &mut out);

        let topic_in_scope = |e: &TopicEntry<'_>| {
            any_in(
                e.producers.iter().chain(e.consumers.iter()).map(|s| s.node),
                in_scope,
            )
        };
        out.published_never_consumed.retain(topic_in_scope);
        out.consumed_never_published.retain(topic_in_scope);
        out.fan_out_topics.retain(topic_in_scope);
        out.test_only_consumers.retain(topic_in_scope);
        out.unresolved.retain(|u| topic_in_scope(&u.topic));
        out.fan_out_producers
            .retain(|p| any_in(p.sites.iter().copied(), in_scope));
        out.cycles.retain(|c| {
            any_in(
                c.path.iter().flat_map(|e| [e.consumer, e.producer]),
                in_scope,
            )
        });
        out.fan_out_topics.sort_by(|a, b| {
            (std::cmp::Reverse(a.consumers.len()), &a.key, a.hub.id).cmp(&(
                std::cmp::Reverse(b.consumers.len()),
                &b.key,
                b.hub.id,
            ))
        });
        out
    }

    /// Producer files that publish onto at least [`ANOMALY_FAN_OUT_TOPICS`] topics.
    fn fan_out_producers<'g>(
        &'g self,
        all: &[HubSites<'g>],
        _is_test: &dyn Fn(&ContractNode) -> bool,
        out: &mut EventAnomalies<'g>,
    ) {
        type PerFile<'g> = (
            BTreeMap<NodeId, &'g ContractNode>,
            BTreeMap<NodeId, &'g ContractNode>,
        );
        let mut files: BTreeMap<&'g FilePath, PerFile<'g>> = BTreeMap::new();
        for h in all {
            // A wildcard hub holds producers linked by pattern; they publish onto their own
            // concrete subject, which is another hub.
            if is_placeholder(h.key) || is_subject_pattern(h.key) {
                continue;
            }
            for p in h.producers.iter().filter(|s| !s.test) {
                let slot = files.entry(&p.node.file_path).or_default();
                slot.0.insert(h.hub.id, h.hub);
                slot.1.insert(p.node.id, p.node);
            }
        }
        for (file, (topics, producers)) in files {
            if topics.len() < ANOMALY_FAN_OUT_TOPICS {
                continue;
            }
            let mut sites: Vec<&ContractNode> = producers.into_values().collect();
            sort_nodes(&mut sites);
            let repo_id = sites.first().map_or(0, |n| n.repo_id);
            let mut topics: Vec<&ContractNode> = topics.into_values().collect();
            topics.sort_by(|a, b| {
                (Self::hub_key(a), Self::hub_broker(a), a.id).cmp(&(
                    Self::hub_key(b),
                    Self::hub_broker(b),
                    b.id,
                ))
            });
            let topics = topics.into_iter().map(|h| self.hub_ref(h)).collect();
            out.fan_out_producers.push(FanOutProducer {
                file,
                repo_id,
                topics,
                sites,
            });
        }
        out.fan_out_producers.sort_by(|a, b| {
            (std::cmp::Reverse(a.topics.len()), a.file)
                .cmp(&(std::cmp::Reverse(b.topics.len()), b.file))
        });
    }

    /// Cycles of the topic graph: strongly connected components (iterative Tarjan), each with
    /// the shortest cycle through its first topic as the witness.
    fn cycles<'g>(
        &'g self,
        all: &[HubSites<'g>],
        is_test: &dyn Fn(&ContractNode) -> bool,
        out: &mut EventAnomalies<'g>,
    ) {
        // Placeholder hubs are left out: one `${KEY}` can stand for several unrelated names.
        let eligible: Vec<&HubSites<'g>> = all.iter().filter(|h| !is_placeholder(h.key)).collect();
        let ids: Vec<NodeId> = eligible.iter().map(|h| h.hub.id).collect();
        let slot: HashMap<NodeId, usize> = ids.iter().enumerate().map(|(i, id)| (*id, i)).collect();
        let (hops, walked, total, first_skipped) =
            self.topic_hops(&ids, is_test, ANOMALY_MAX_CYCLE_SITES);
        out.cycle_sites_walked = walked;
        out.cycle_sites_total = total;
        out.cycle_first_unsearched = first_skipped;

        // The best witness of each (from, to) pair: strongest link, then the first sites in
        // path order.
        let mut best: BTreeMap<(usize, usize), &TopicHop<'g>> = BTreeMap::new();
        for hop in &hops {
            let (Some(&a), Some(&b)) = (slot.get(&hop.from), slot.get(&hop.to)) else {
                continue;
            };
            // A guess from the same file is no reason to say a chain loops back.
            if hop.fallback {
                out.cycle_guess_hops += 1;
                continue;
            }
            let order = |h: &TopicHop<'_>| {
                (
                    rank(h.confidence),
                    node_key(h.consumer).0.clone(),
                    h.consumer.line_start,
                    h.consumer.id,
                    node_key(h.producer).0.clone(),
                    h.producer.line_start,
                    h.producer.id,
                )
            };
            match best.get(&(a, b)) {
                Some(cur) if order(cur) <= order(hop) => {}
                _ => {
                    best.insert((a, b), hop);
                }
            }
        }
        let mut adj: Vec<Vec<usize>> = vec![Vec::new(); ids.len()];
        for &(a, b) in best.keys() {
            adj[a].push(b);
        }
        let mut comps = strongly_connected(&adj);
        comps.sort();
        for comp in comps {
            let Some(&start) = comp.first() else {
                continue;
            };
            if comp.len() == 1 && !best.contains_key(&(start, start)) {
                continue;
            }
            let in_comp: HashSet<usize> = comp.iter().copied().collect();
            let Some(order) = shortest_cycle(start, &adj, &in_comp) else {
                continue;
            };
            let mut path = Vec::new();
            for (i, &a) in order.iter().enumerate() {
                let b = order[(i + 1) % order.len()];
                let (Some(hop), Some(from), Some(to)) = (
                    best.get(&(a, b)),
                    self.nodes.get(&ids[a]),
                    self.nodes.get(&ids[b]),
                ) else {
                    continue;
                };
                let (from, to) = (self.hub_ref(from), self.hub_ref(to));
                let unknown = from.broker.is_none() || to.broker.is_none();
                path.push(CycleEdge {
                    from,
                    to,
                    consumer: hop.consumer,
                    producer: hop.producer,
                    confidence: if unknown {
                        weakest(hop.confidence, EdgeConfidence::Heuristic)
                    } else {
                        hop.confidence
                    },
                });
            }
            let confidence = path
                .iter()
                .fold(EdgeConfidence::Exact, |acc, e| weakest(acc, e.confidence));
            out.cycles.push(CycleEntry {
                members: comp
                    .iter()
                    .filter_map(|&i| self.nodes.get(&ids[i]))
                    .map(|h| self.hub_ref(h))
                    .collect(),
                path,
                confidence,
            });
        }
        out.cycles.sort_by(|a, b| {
            (
                rank(a.confidence),
                a.members.first().map(|n| (Self::hub_key(n.hub), n.hub.id)),
            )
                .cmp(&(
                    rank(b.confidence),
                    b.members.first().map(|n| (Self::hub_key(n.hub), n.hub.id)),
                ))
        });
    }
}

/// Strongly connected components of `adj` (Tarjan, with an explicit stack: no recursion, so a
/// chain of any length cannot overflow the call stack). Each component is sorted.
pub(super) fn strongly_connected(adj: &[Vec<usize>]) -> Vec<Vec<usize>> {
    const UNSEEN: usize = usize::MAX;
    let n = adj.len();
    let mut index = vec![UNSEEN; n];
    let mut low = vec![0usize; n];
    let mut on_stack = vec![false; n];
    let mut stack: Vec<usize> = Vec::new();
    let mut comps = Vec::new();
    let mut counter = 0usize;
    for root in 0..n {
        if index[root] != UNSEEN {
            continue;
        }
        index[root] = counter;
        low[root] = counter;
        counter += 1;
        stack.push(root);
        on_stack[root] = true;
        let mut work: Vec<(usize, usize)> = vec![(root, 0)];
        while let Some(&(v, next)) = work.last() {
            if let Some(&w) = adj[v].get(next) {
                if let Some(top) = work.last_mut() {
                    top.1 += 1;
                }
                if index[w] == UNSEEN {
                    index[w] = counter;
                    low[w] = counter;
                    counter += 1;
                    stack.push(w);
                    on_stack[w] = true;
                    work.push((w, 0));
                } else if on_stack[w] {
                    low[v] = low[v].min(index[w]);
                }
                continue;
            }
            work.pop();
            if let Some(&(parent, _)) = work.last() {
                low[parent] = low[parent].min(low[v]);
            }
            if low[v] == index[v] {
                let mut comp = Vec::new();
                while let Some(w) = stack.pop() {
                    on_stack[w] = false;
                    comp.push(w);
                    if w == v {
                        break;
                    }
                }
                comp.sort_unstable();
                comps.push(comp);
            }
        }
    }
    comps
}

/// The shortest cycle through `start` inside `in_comp`, as the nodes of the cycle in order
/// (`start` first; the closing edge goes from the last back to `start`). BFS in sorted order,
/// so ties resolve the same way on every run.
pub(super) fn shortest_cycle(
    start: usize,
    adj: &[Vec<usize>],
    in_comp: &HashSet<usize>,
) -> Option<Vec<usize>> {
    let mut parent: HashMap<usize, usize> = HashMap::new();
    let mut seen: HashSet<usize> = HashSet::from([start]);
    let mut queue: VecDeque<usize> = VecDeque::from([start]);
    while let Some(u) = queue.pop_front() {
        for &v in adj.get(u).map(Vec::as_slice).unwrap_or(&[]) {
            if !in_comp.contains(&v) {
                continue;
            }
            if v == start {
                let mut order = vec![u];
                let mut cur = u;
                while cur != start {
                    cur = *parent.get(&cur)?;
                    order.push(cur);
                }
                order.reverse();
                return Some(order);
            }
            if seen.insert(v) {
                parent.insert(v, u);
                queue.push_back(v);
            }
        }
    }
    None
}
