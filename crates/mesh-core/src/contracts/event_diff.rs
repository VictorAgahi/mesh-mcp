//! Event-level views of one graph that `diff_mesh` compares between a Git base and the working
//! tree (9.0 ticket T10): the producer / consumer sites of a set of files, whether a
//! broker-qualified channel links a producer to a consumer, and the one-hop chains (an entry, the
//! calls, a publish) the chain engine of T3 finds around a set of channels.
//!
//! Every identity here is line-independent (file and enclosing declaration, never a line), so a
//! site that only moved within its file is the same site on both sides, and every collection is
//! sorted, so two runs over the same graphs give the same answer.

use super::flow::FlowIndex;
use super::*;

/// A topic key on one broker: `None` when the site's broker is not statically known (it joins
/// every broker's channel of the key, see `broker`).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventChannel {
    pub key: CompactStr,
    pub broker: Option<Broker>,
}

impl EventChannel {
    /// `kafka:orders`, or the bare key when the broker is not known.
    pub fn label(&self) -> String {
        match self.broker {
            Some(b) => qualified_topic_key(b, &self.key),
            None => self.key.to_string(),
        }
    }

    /// Whether a site whose brokers are `mask` is on this channel: a site of an unknown broker
    /// is on every channel of its key, and a channel of an unknown broker takes every site.
    fn admits(&self, mask: u32) -> bool {
        match self.broker {
            Some(b) => mask == 0 || mask & b.bit() != 0,
            None => true,
        }
    }

    /// One channel per broker of `mask` (one of an unknown broker when `mask` is 0).
    fn of(key: &str, mask: u32) -> Vec<EventChannel> {
        let brokers: Vec<Option<Broker>> = if mask == 0 {
            vec![None]
        } else {
            Broker::of_mask(mask).map(Some).collect()
        };
        brokers
            .into_iter()
            .map(|broker| EventChannel {
                key: CompactStr::new(key),
                broker,
            })
            .collect()
    }
}

/// A producer or consumer site on one channel.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct EventSite {
    pub channel: EventChannel,
    pub producer: bool,
    pub file: FilePath,
    /// The declaration the site belongs to (see
    /// [`ContractGraph::site_owner`]).
    pub owner: CompactStr,
    pub line: usize,
}

/// One end of an [`EventHop`]: the entry (a consumer, route, handler or cron method) or the
/// publish.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct HopEnd {
    /// `consumer`, `route`, `handler`, `cron` or `producer`.
    pub kind: &'static str,
    /// The channel of a consumer or a producer; `None` for a route, handler or cron entry.
    pub channel: Option<EventChannel>,
    pub file: FilePath,
    pub owner: CompactStr,
}

/// One hop of a chain: handling `entry` reaches the publish `publish` (through calls, or by the
/// engine's labelled same-file fallback).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct EventHop {
    pub entry: HopEnd,
    pub publish: HopEnd,
}

/// The hops found around a set of channels.
#[derive(Debug, Clone, Default)]
pub struct EventHops {
    /// Each hop with its weakest confidence over every trace that found it, and whether only
    /// the same-file fallback linked it.
    pub hops: BTreeMap<EventHop, (EdgeConfidence, bool)>,
    /// Channels whose trace was cut by a bound (`call_depth`, `fan_out`, the hop cap): a hop
    /// of theirs may be missing from this side, so it is not compared.
    pub bounded: BTreeSet<EventChannel>,
    /// Channels not traced because the caller's deadline passed.
    pub not_traced: BTreeSet<EventChannel>,
}

fn weaker(a: EdgeConfidence, b: EdgeConfidence) -> EdgeConfidence {
    let rank = |c| match c {
        EdgeConfidence::Exact => 0,
        EdgeConfidence::Heuristic => 1,
        EdgeConfidence::Ambiguous => 2,
    };
    if rank(a) >= rank(b) {
        a
    } else {
        b
    }
}

fn is_topic_kind(k: NodeKind) -> bool {
    matches!(
        k,
        NodeKind::KafkaTopic | NodeKind::EventStream | NodeKind::Queue
    )
}

impl ContractGraph {
    /// The declaration a site belongs to: the site itself when it is a declaration (a handler
    /// method), else the smallest declaration of its file around its line, else `<top level>`
    /// (never the site's own name: a call-site node is named after its topic).
    /// Read from the file's nodes only, never from the derived calls layer (which a pruning
    /// pass may leave out), so both sides of a diff name a site the same way.
    pub fn site_owner(&self, node: &ContractNode) -> CompactStr {
        if !is_topic_kind(node.kind) {
            return node.name.clone();
        }
        self.get_nodes_for_file(&node.file_path)
            .iter()
            .filter_map(|id| self.get_node(*id))
            .filter(|d| {
                !is_topic_kind(d.kind)
                    && d.line_start <= node.line_start
                    && node.line_end <= d.line_end
            })
            .min_by_key(|d| (d.line_end - d.line_start, d.line_start, d.id))
            .map_or_else(|| CompactStr::new("<top level>"), |d| d.name.clone())
    }

    /// The producer and consumer sites declared in `files`, one per channel they are on,
    /// sorted.
    pub fn event_sites_in(&self, files: &[&Path]) -> Vec<EventSite> {
        let ids: HashSet<NodeId> = files
            .iter()
            .flat_map(|f| self.get_nodes_for_file(f).iter().copied())
            .collect();
        let mut out = Vec::new();
        if ids.is_empty() {
            return out;
        }
        for (producer, map) in [
            (true, &self.topic_producers),
            (false, &self.topic_consumers),
        ] {
            for (key, sites) in map {
                for id in sites.iter().filter(|id| ids.contains(id)) {
                    let Some(node) = self.get_node(*id) else {
                        continue;
                    };
                    let owner = self.site_owner(node);
                    for channel in EventChannel::of(key, self.site_mask(producer, key, *id)) {
                        out.push(EventSite {
                            channel,
                            producer,
                            file: node.file_path.clone(),
                            owner: owner.clone(),
                            line: node.line_start,
                        });
                    }
                }
            }
        }
        out.sort();
        out.dedup();
        out
    }

    /// The site masks of `channel`'s producers (`producer`) or consumers, the ones on the
    /// channel only. Consumers include the wildcard subscriptions matching the key.
    fn channel_masks(&self, channel: &EventChannel, producer: bool) -> Vec<u32> {
        let key = channel.key.as_str();
        let map = if producer {
            &self.topic_producers
        } else {
            &self.topic_consumers
        };
        let mut keys: Vec<&str> = vec![key];
        if !producer {
            keys.extend(
                map.keys()
                    .map(CompactStr::as_str)
                    .filter(|k| *k != key && is_subject_pattern(k) && subject_matches(k, key)),
            );
        }
        keys.into_iter()
            .flat_map(|k| {
                map.get(k)
                    .into_iter()
                    .flatten()
                    .map(move |id| self.site_mask(producer, k, *id))
            })
            .filter(|m| channel.admits(*m))
            .collect()
    }

    /// The concrete producer channels a wildcard `pattern` channel (`orders.*`) matches: a
    /// wildcard consumer added or removed changes their links. Empty for a plain key.
    pub fn channels_matching(&self, pattern: &EventChannel) -> Vec<EventChannel> {
        let key = pattern.key.as_str();
        if !is_subject_pattern(key) {
            return Vec::new();
        }
        let mut out: Vec<EventChannel> = self
            .topic_producers
            .iter()
            .filter(|(k, _)| subject_matches(key, k))
            .flat_map(|(k, ids)| {
                ids.iter()
                    .map(move |id| self.site_mask(true, k, *id))
                    .filter(|m| pattern.admits(*m))
                    .flat_map(move |m| EventChannel::of(k, m))
            })
            .filter(|c| {
                c.broker.is_none() || pattern.broker.is_none() || c.broker == pattern.broker
            })
            .collect();
        out.sort();
        out.dedup();
        out
    }

    /// Whether any site, producer or consumer, is on `channel`.
    pub fn channel_has_sites(&self, channel: &EventChannel) -> bool {
        !self.channel_masks(channel, true).is_empty()
            || !self.channel_masks(channel, false).is_empty()
    }

    /// Whether `channel` links a producer to a consumer: one of each on the channel, on no two
    /// different known brokers (the rule of `FlowIndex::crosses_brokers`).
    pub fn channel_linked(&self, channel: &EventChannel) -> bool {
        let consumers = self.channel_masks(channel, false);
        self.channel_masks(channel, true)
            .iter()
            .any(|p| consumers.iter().any(|c| *p == 0 || *c == 0 || p & c != 0))
    }

    /// The channels `node` publishes on (`producer`) or consumes from, through its edges.
    fn site_channels(
        &self,
        idx: &FlowIndex<'_>,
        node: &ContractNode,
        producer: bool,
    ) -> Vec<EventChannel> {
        let hubs = if producer {
            idx.produces.get(&node.id)
        } else {
            idx.consumes.get(&node.id)
        };
        let mut out: Vec<EventChannel> = hubs
            .into_iter()
            .flatten()
            .filter_map(|h| self.get_node(*h))
            .flat_map(|h| {
                let key = Self::hub_key(h);
                EventChannel::of(key, self.site_mask(producer, key, node.id))
            })
            .collect();
        out.sort();
        out.dedup();
        out
    }

    /// The one-hop chains around `channels`: forward from each channel's consumers (what
    /// handling it publishes) and reverse from its producers (which entries reach the publish).
    /// One adjacency for every trace; each trace is bounded like `trace_flow` with
    /// `event_depth` 1 and the maximum `fan_out`. The `deadline` is checked before each channel;
    /// the channels it leaves are listed in [`EventHops::not_traced`].
    pub fn event_hops(&self, channels: &[EventChannel], deadline: std::time::Instant) -> EventHops {
        let idx = self.flow_index();
        let bounds = FlowBounds {
            call_depth: DEFAULT_CALL_DEPTH,
            event_depth: 1,
            fan_out: MAX_FAN_OUT,
        };
        let mut out = EventHops::default();
        for channel in channels {
            if std::time::Instant::now() >= deadline {
                out.not_traced.insert(channel.clone());
                continue;
            }
            for (producer, direction, kind) in [
                (false, FlowDirection::Forward, FlowStepKind::Consume),
                (true, FlowDirection::Reverse, FlowStepKind::Produce),
            ] {
                let map = if producer {
                    &self.topic_producers
                } else {
                    &self.topic_consumers
                };
                let key = channel.key.as_str();
                let starts: Vec<&ContractNode> = map
                    .get(key)
                    .into_iter()
                    .flatten()
                    .filter(|id| channel.admits(self.site_mask(producer, key, **id)))
                    .filter_map(|id| self.get_node(*id))
                    .collect();
                if starts.is_empty() {
                    continue;
                }
                let trace = self.trace_from_in(
                    &idx,
                    starts,
                    kind,
                    None,
                    HashSet::new(),
                    direction,
                    bounds,
                    &|_| false,
                );
                if trace.capped || trace.entries_more > 0 || !trace.limits.is_empty() {
                    out.bounded.insert(channel.clone());
                }
                for hop in &trace.hops {
                    let entry = hop.steps.iter().find(|s| {
                        matches!(
                            s.kind,
                            FlowStepKind::Consume
                                | FlowStepKind::Route
                                | FlowStepKind::Handler
                                | FlowStepKind::Cron
                        )
                    });
                    let publish = hop.steps.iter().find(|s| s.kind == FlowStepKind::Produce);
                    let (Some(entry), Some(publish)) = (entry, publish) else {
                        continue;
                    };
                    let entry_kind = match entry.kind {
                        FlowStepKind::Consume => "consumer",
                        FlowStepKind::Route => "route",
                        FlowStepKind::Handler => "handler",
                        _ => "cron",
                    };
                    let entry_channels: Vec<Option<EventChannel>> =
                        if entry.kind == FlowStepKind::Consume {
                            self.site_channels(&idx, entry.node, false)
                                .into_iter()
                                .map(Some)
                                .collect()
                        } else {
                            vec![None]
                        };
                    let entry_owner = self.site_owner(entry.node);
                    let publish_owner = self.site_owner(publish.node);
                    for ec in &entry_channels {
                        for pc in self.site_channels(&idx, publish.node, true) {
                            let key = EventHop {
                                entry: HopEnd {
                                    kind: entry_kind,
                                    channel: ec.clone(),
                                    file: entry.node.file_path.clone(),
                                    owner: entry_owner.clone(),
                                },
                                publish: HopEnd {
                                    kind: "producer",
                                    channel: Some(pc),
                                    file: publish.node.file_path.clone(),
                                    owner: publish_owner.clone(),
                                },
                            };
                            out.hops
                                .entry(key)
                                .and_modify(|(c, f)| {
                                    *c = weaker(*c, hop.confidence);
                                    *f = *f && hop.fallback;
                                })
                                .or_insert((hop.confidence, hop.fallback));
                        }
                    }
                }
            }
        }
        out
    }
}
