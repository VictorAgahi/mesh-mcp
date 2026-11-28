//! Broker identity of a topic site (9.0 ticket T7).
//!
//! A topic key (`normalize_topic_key`) names a channel, not the broker that carries it: a Kafka
//! topic `orders` and a RabbitMQ queue, an SNS topic or a Redis stream also named `orders` used
//! to collapse into one key and produce chains no message ever takes. Each producer / consumer
//! may now carry the [`Broker`] its client library speaks to; the topic pass keeps one hub per
//! broker when a name is used on two or more known brokers, and one plain hub otherwise (the
//! behaviour before 9.0, so a single-broker workspace reads exactly as it did).
//!
//! The rule, stated once (also in `docs/architecture.md`, "Broker-qualified topic keys"):
//! - same name, same broker: one channel (exact);
//! - same name, two different known brokers: two channels, never linked;
//! - a site whose broker is not statically known (a NestJS `@EventPattern`, a declarative
//!   pattern, a language without broker detection) joins the channel of every broker that
//!   uses the name, reported `heuristic`; when the name has at most one known broker it joins
//!   that channel exactly as before, so nothing changes where nothing was ambiguous.

use super::{CompactStr, ContractGraph, ContractNode, NodeId, RepoId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A message broker or queue system a site is statically known to talk to.
///
/// The declaration order is the order of a name's hubs. `slug` is the lower-case qualifier of
/// a hub name (`kafka:orders`) and of a query target.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Broker {
    Kafka,
    RabbitMq,
    Redis,
    Nats,
    Mqtt,
    Sqs,
    Sns,
    Kinesis,
    EventBridge,
    PubSub,
    Pulsar,
    ServiceBus,
    EventHubs,
    Stomp,
    Bull,
    BeeQueue,
    /// In-process buses (NestJS EventEmitter2 and CQRS, Spring application events): the
    /// "broker" is the application itself, so an in-process event named like a Kafka topic is
    /// never the same channel (9.0 ticket T4).
    InProc,
    /// The JMS API (Spring JMS, ActiveMQ, Artemis, IBM MQ): the provider behind it is not
    /// statically known, so it is one channel space of its own (9.0 ticket T8a). Declared last
    /// so no earlier broker's hub order moves.
    Jms,
}

impl Broker {
    /// Every broker, in declaration order.
    pub const ALL: [Broker; 18] = [
        Broker::Kafka,
        Broker::RabbitMq,
        Broker::Redis,
        Broker::Nats,
        Broker::Mqtt,
        Broker::Sqs,
        Broker::Sns,
        Broker::Kinesis,
        Broker::EventBridge,
        Broker::PubSub,
        Broker::Pulsar,
        Broker::ServiceBus,
        Broker::EventHubs,
        Broker::Stomp,
        Broker::Bull,
        Broker::BeeQueue,
        Broker::InProc,
        Broker::Jms,
    ];

    /// The lower-case qualifier written in a hub name and accepted in a query target.
    pub const fn slug(self) -> &'static str {
        match self {
            Broker::Kafka => "kafka",
            Broker::RabbitMq => "rabbitmq",
            Broker::Redis => "redis",
            Broker::Nats => "nats",
            Broker::Mqtt => "mqtt",
            Broker::Sqs => "sqs",
            Broker::Sns => "sns",
            Broker::Kinesis => "kinesis",
            Broker::EventBridge => "eventbridge",
            Broker::PubSub => "pubsub",
            Broker::Pulsar => "pulsar",
            Broker::ServiceBus => "servicebus",
            Broker::EventHubs => "eventhubs",
            Broker::Stomp => "stomp",
            Broker::Bull => "bull",
            Broker::BeeQueue => "beequeue",
            Broker::InProc => "inproc",
            Broker::Jms => "jms",
        }
    }

    /// The broker named `slug` (exact, lower-case), if any.
    pub fn from_slug(slug: &str) -> Option<Broker> {
        Self::ALL.iter().copied().find(|b| b.slug() == slug)
    }

    /// One bit per broker; a set of brokers is a `u32` mask (0: broker not known).
    pub(crate) const fn bit(self) -> u32 {
        1u32 << (self as u32)
    }

    /// The brokers of `mask`, in declaration order.
    pub(crate) fn of_mask(mask: u32) -> impl Iterator<Item = Broker> {
        Self::ALL.into_iter().filter(move |b| mask & b.bit() != 0)
    }
}

/// The qualified spelling of `name` on `broker` (`kafka:orders`): the name of a broker hub and
/// the form of a query target that selects one broker's channel.
pub fn qualified_topic_key(broker: Broker, name: &str) -> String {
    let mut out = String::with_capacity(broker.slug().len() + 1 + name.len());
    out.push_str(broker.slug());
    out.push(':');
    out.push_str(name);
    out
}

/// Splits `kafka:orders` into `(Kafka, "orders")`. `None` when `key` does not start with a
/// known broker slug and a colon: a Redis key such as `bull:orders` that merely looks
/// qualified is a caller's call (the query layer tries the literal key first).
pub fn split_qualified_topic_key(key: &str) -> Option<(Broker, &str)> {
    let (slug, rest) = key.split_once(':')?;
    let broker = Broker::from_slug(slug)?;
    (!rest.is_empty()).then_some((broker, rest))
}

/// Prefix of the `docstring` of a broker hub (`broker:kafka`): the machine-readable form of
/// the hub's broker, which its name (`kafka:orders`) only repeats for display.
const HUB_TAG_PREFIX: &str = "broker:";
/// Docstring and name suffix of the plain hub of a key that clashes with a broker hub's name.
const LITERAL_TAG: &str = "literal";
const LITERAL_SUFFIX: &str = " (literal)";

impl ContractGraph {
    /// The brokers (a [`Broker::bit`] mask) `key`'s `producer` / consumer `id` is known to talk
    /// to; 0 when not known.
    pub(super) fn site_mask(&self, producer: bool, key: &str, id: NodeId) -> u32 {
        let map = if producer {
            &self.producer_brokers
        } else {
            &self.consumer_brokers
        };
        map.get(key)
            .and_then(|sites| sites.get(&id))
            .copied()
            .unwrap_or(0)
    }

    /// The brokers each site node is known to talk to over every channel it is registered on, as
    /// masks: `(producers, consumers)`. One pass over the tagged sites, for a caller that labels
    /// many rows (the impact matrix).
    pub(super) fn site_masks(&self) -> (HashMap<NodeId, u32>, HashMap<NodeId, u32>) {
        let fold = |map: &HashMap<CompactStr, HashMap<NodeId, u32>>| {
            let mut out: HashMap<NodeId, u32> = HashMap::new();
            for (id, mask) in map.values().flatten() {
                *out.entry(*id).or_default() |= mask;
            }
            out
        };
        (fold(&self.producer_brokers), fold(&self.consumer_brokers))
    }

    /// The broker a site row of the impact matrix is labelled with: the one broker the node is
    /// known to talk to in the row's role (`masks` from [`Self::site_masks`]; the node's own
    /// broker when it is itself a broker site, for a row of another role). `None` for a hub (its
    /// name carries the broker), for a node of unknown broker, and for one on several brokers.
    pub(super) fn row_broker(
        &self,
        node: &ContractNode,
        producer: Option<bool>,
        masks: &(HashMap<NodeId, u32>, HashMap<NodeId, u32>),
    ) -> Option<Broker> {
        if Self::is_hub_like(node) {
            return None;
        }
        let of = |m: &HashMap<NodeId, u32>| m.get(&node.id).copied().unwrap_or(0);
        let mask = match producer {
            Some(true) => of(&masks.0),
            Some(false) => of(&masks.1),
            None => of(&masks.0) | of(&masks.1),
        };
        if mask == 0 {
            return self.node_brokers.get(&node.id).copied();
        }
        (mask.count_ones() == 1)
            .then(|| Broker::of_mask(mask).next())
            .flatten()
    }

    /// Every broker some site of `key` is known to talk to, as a mask.
    pub(super) fn known_brokers(&self, key: &str) -> u32 {
        [&self.producer_brokers, &self.consumer_brokers]
            .into_iter()
            .filter_map(|map| map.get(key))
            .flat_map(HashMap::values)
            .fold(0, |acc, mask| acc | mask)
    }

    /// The hubs of a live `key`, in a fixed order: the one plain hub (`None`) while at most one
    /// broker is known for the name, else one hub per known broker.
    pub(super) fn topic_hubs(&self, key: &str) -> Vec<Option<Broker>> {
        let mask = self.known_brokers(key);
        if mask.count_ones() <= 1 {
            vec![None]
        } else {
            Broker::of_mask(mask).map(Some).collect()
        }
    }

    /// Whether the plain key `key` is spelled like a broker hub (`rabbitmq:orders`) that is live:
    /// a key written so in source (a Kafka topic literally named `rabbitmq:orders`) while
    /// RabbitMQ also uses `orders`. Its plain hub is then named apart (see [`Self::hub_name`]),
    /// so no two hub nodes ever share a name.
    pub(super) fn literal_clash(&self, key: &str) -> bool {
        let Some((broker, bare)) = super::split_qualified_topic_key(key) else {
            return false;
        };
        (self.topic_producers.contains_key(bare) || self.topic_consumers.contains_key(bare))
            && self.topic_hubs(bare).contains(&Some(broker))
    }

    /// The node name of `key`'s hub on `hub`: the key itself for the plain hub (unchanged
    /// since before 9.0), `kafka:orders` for a broker hub, and `rabbitmq:orders (literal)` for
    /// the plain hub of a key that clashes with a broker hub's name.
    pub(super) fn hub_name(&self, key: &str, hub: Option<Broker>) -> CompactStr {
        match hub {
            Some(b) => CompactStr::new(super::qualified_topic_key(b, key)),
            None if self.literal_clash(key) => CompactStr::new(format!("{key}{LITERAL_SUFFIX}")),
            None => CompactStr::new(key),
        }
    }

    /// The `docstring` marking a broker hub or a clashing literal hub.
    pub(super) fn hub_docstring(&self, key: &str, hub: Option<Broker>) -> Option<CompactStr> {
        match hub {
            Some(b) => Some(CompactStr::new(format!("{HUB_TAG_PREFIX}{}", b.slug()))),
            None if self.literal_clash(key) => Some(CompactStr::new(LITERAL_TAG)),
            None => None,
        }
    }

    /// The broker a hub node stands for; `None` for a plain hub and for every other node.
    pub(super) fn hub_broker(n: &ContractNode) -> Option<Broker> {
        if !Self::is_hub_like(n) {
            return None;
        }
        n.docstring
            .as_deref()
            .and_then(|d| d.strip_prefix(HUB_TAG_PREFIX))
            .and_then(Broker::from_slug)
    }

    /// The topic key a hub node stands for (its name without a broker qualifier or the
    /// `(literal)` marker).
    pub(super) fn hub_key(n: &ContractNode) -> &str {
        if let Some(b) = Self::hub_broker(n) {
            return n
                .name
                .strip_prefix(b.slug())
                .and_then(|rest| rest.strip_prefix(':'))
                .unwrap_or(n.name.as_str());
        }
        if Self::is_hub_like(n) && n.docstring.as_deref() == Some(LITERAL_TAG) {
            return n
                .name
                .strip_suffix(LITERAL_SUFFIX)
                .unwrap_or(n.name.as_str());
        }
        n.name.as_str()
    }

    /// Whether a synthetic hub node still has the name its key and broker give it now. A hub
    /// declared by a file keeps its own name and is always current.
    pub(super) fn hub_name_is_current(&self, n: &ContractNode) -> bool {
        n.repo_id != RepoId::MAX
            || n.name
                == self
                    .hub_name(Self::hub_key(n), Self::hub_broker(n))
                    .as_str()
    }

    /// Every hub node of `key`, plain and per broker.
    pub(super) fn hub_nodes_of(&self, key: &str) -> Vec<NodeId> {
        let names = [
            CompactStr::new(key),
            CompactStr::new(format!("{key}{LITERAL_SUFFIX}")),
        ]
        .into_iter()
        .chain(Broker::ALL.map(|b| CompactStr::new(super::qualified_topic_key(b, key))));
        let mut out = Vec::new();
        for name in names {
            for id in self.name_to_nodes.get(&name).into_iter().flatten() {
                if let Some(n) = self.nodes.get(id) {
                    if Self::is_hub_like(n) && Self::hub_key(n) == key {
                        out.push(*id);
                    }
                }
            }
        }
        out
    }

    /// Whether the `is_producer` / consumer site `node` of `key` is excluded from the channel
    /// of `broker`: a site of another known broker. A site of unknown broker is not excluded
    /// (it joins every channel of the name, `heuristic`).
    pub(super) fn site_excluded_from(
        &self,
        is_producer: bool,
        key: &str,
        node: NodeId,
        broker: Broker,
    ) -> bool {
        let mask = self.site_mask(is_producer, key, node);
        mask != 0 && mask & broker.bit() == 0
    }

    /// Whether `node` is of another known broker, or is registered under `key` and every
    /// registration is excluded from the channel of `broker` (see [`Self::site_excluded_from`]).
    /// A node that is no site of the key is never excluded.
    pub(super) fn node_excluded_from(&self, key: &str, node: NodeId, broker: Broker) -> bool {
        if self.node_brokers.get(&node).is_some_and(|b| *b != broker) {
            return true;
        }
        let mut registered = false;
        let mut included = false;
        for (is_producer, sites) in [
            (true, &self.topic_producers),
            (false, &self.topic_consumers),
        ] {
            if sites.get(key).is_some_and(|ids| ids.contains(&node)) {
                registered = true;
                included |= !self.site_excluded_from(is_producer, key, node, broker);
            }
        }
        registered && !included
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::MAX_WILDCARD_PRODUCERS;
    use crate::types::{EdgeConfidence, EdgeKind, NodeKind};
    use std::path::Path;

    #[test]
    fn slugs_round_trip_and_are_unique() {
        let mut seen = std::collections::BTreeSet::new();
        for b in Broker::ALL {
            assert_eq!(Broker::from_slug(b.slug()), Some(b));
            assert!(seen.insert(b.slug()), "duplicate slug {}", b.slug());
            assert_eq!(b.slug(), b.slug().to_lowercase());
        }
        assert_eq!(Broker::from_slug("Kafka"), None);
        assert_eq!(Broker::from_slug(""), None);
    }

    /// The golden checker (`tests/golden/events/check_golden.py`) lists the slugs it accepts
    /// in a qualified key; it must list exactly these.
    #[test]
    fn the_golden_checker_knows_every_slug() {
        let checker = include_str!("../../../../tests/golden/events/check_golden.py");
        for b in Broker::ALL {
            assert!(
                checker.contains(&format!("\"{}\"", b.slug())),
                "check_golden.py lacks {}",
                b.slug()
            );
        }
    }

    #[test]
    fn mask_bits_are_distinct_and_ordered() {
        let mask = Broker::Redis.bit() | Broker::Kafka.bit();
        let got: Vec<Broker> = Broker::of_mask(mask).collect();
        assert_eq!(got, vec![Broker::Kafka, Broker::Redis]);
        assert_eq!(Broker::of_mask(0).count(), 0);
    }

    #[test]
    fn qualified_key_splits_only_on_a_known_slug() {
        assert_eq!(
            split_qualified_topic_key("kafka:orders"),
            Some((Broker::Kafka, "orders"))
        );
        assert_eq!(
            split_qualified_topic_key("rabbitmq:a:b"),
            Some((Broker::RabbitMq, "a:b"))
        );
        assert_eq!(split_qualified_topic_key("orders"), None);
        assert_eq!(split_qualified_topic_key("shop:orders"), None);
        assert_eq!(split_qualified_topic_key("kafka:"), None);
        assert_eq!(qualified_topic_key(Broker::Sns, "orders"), "sns:orders");
    }

    // ---- the topic pass -------------------------------------------------------------

    fn site(g: &mut ContractGraph, name: &str, file: &str) -> NodeId {
        g.add_node(ContractNode {
            id: 0,
            name: CompactStr::new(name),
            kind: NodeKind::ServiceClass,
            file_path: Path::new(file).into(),
            line_start: 1,
            line_end: 1,
            package: CompactStr::new(""),
            repo_id: 0,
            signature: None,
            docstring: None,
        })
    }

    /// Hub names, in node-id (creation) order.
    fn hubs(g: &ContractGraph) -> Vec<String> {
        g.all_nodes()
            .filter(|n| ContractGraph::is_hub_like(n))
            .map(|n| n.name.to_string())
            .collect()
    }

    /// `(hub name, confidence)` of every topic edge touching `id`, sorted.
    fn channels(g: &ContractGraph, id: NodeId) -> Vec<(String, EdgeConfidence)> {
        let name = |n: NodeId| {
            g.get_node(n)
                .map(|n| n.name.to_string())
                .unwrap_or_default()
        };
        let mut out: Vec<_> = g
            .all_edges()
            .iter()
            .filter_map(|e| match e.kind {
                EdgeKind::Produces if e.from == id => Some((name(e.to), e.confidence)),
                EdgeKind::Consumes if e.to == id => Some((name(e.from), e.confidence)),
                _ => None,
            })
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    fn exact(name: &str) -> Vec<(String, EdgeConfidence)> {
        vec![(name.to_string(), EdgeConfidence::Exact)]
    }

    #[test]
    fn the_same_name_on_two_brokers_is_two_channels() {
        let mut g = ContractGraph::new();
        let kafka = site(&mut g, "ship", "a/ship.ts");
        let rabbit = site(&mut g, "recv", "b/recv.ts");
        g.add_producer_on(kafka, "orders", Some(Broker::Kafka));
        g.add_consumer_on(rabbit, "orders", Some(Broker::RabbitMq));
        g.reconcile_edges();
        assert_eq!(hubs(&g), vec!["kafka:orders", "rabbitmq:orders"]);
        assert_eq!(channels(&g, kafka), exact("kafka:orders"));
        assert_eq!(channels(&g, rabbit), exact("rabbitmq:orders"));
    }

    #[test]
    fn the_same_name_on_the_same_broker_is_one_plain_channel() {
        let mut g = ContractGraph::new();
        let p = site(&mut g, "ship", "a/ship.ts");
        let c = site(&mut g, "recv", "b/recv.go");
        g.add_producer_on(p, "Orders", Some(Broker::Kafka));
        g.add_consumer_on(c, "orders", Some(Broker::Kafka));
        g.reconcile_edges();
        // The plain hub keeps the name it always had: no qualifier where nothing was ambiguous.
        assert_eq!(hubs(&g), vec!["orders"]);
        assert_eq!(channels(&g, p), exact("orders"));
        assert_eq!(channels(&g, c), exact("orders"));
    }

    #[test]
    fn a_site_of_unknown_broker_keeps_the_plain_channel_while_one_broker_uses_the_name() {
        let mut g = ContractGraph::new();
        let p = site(&mut g, "ship", "a/ship.ts");
        let c = site(&mut g, "legacy", "b/legacy.ts");
        g.add_producer_on(p, "orders", Some(Broker::Kafka));
        g.add_consumer(c, "orders");
        g.reconcile_edges();
        assert_eq!(hubs(&g), vec!["orders"]);
        assert_eq!(channels(&g, c), exact("orders"));
    }

    #[test]
    fn a_site_of_unknown_broker_joins_every_channel_of_a_shared_name_as_heuristic() {
        let mut g = ContractGraph::new();
        let k = site(&mut g, "k", "a/k.ts");
        let r = site(&mut g, "r", "a/r.ts");
        let legacy = site(&mut g, "legacy", "b/legacy.ts");
        g.add_producer_on(k, "orders", Some(Broker::Kafka));
        g.add_producer_on(r, "orders", Some(Broker::RabbitMq));
        g.add_consumer(legacy, "orders");
        g.reconcile_edges();
        let heuristic = |n: &str| (n.to_string(), EdgeConfidence::Heuristic);
        assert_eq!(
            channels(&g, legacy),
            vec![heuristic("kafka:orders"), heuristic("rabbitmq:orders")]
        );
        // The known producers stay exact on their own channel.
        assert_eq!(channels(&g, k), exact("kafka:orders"));
        assert_eq!(channels(&g, r), exact("rabbitmq:orders"));
    }

    #[test]
    fn sites_that_all_have_an_unknown_broker_share_one_plain_channel() {
        let mut g = ContractGraph::new();
        let p = site(&mut g, "p", "a/p.ts");
        let c = site(&mut g, "c", "b/c.ts");
        g.add_producer(p, "orders");
        g.add_consumer(c, "orders");
        g.reconcile_edges();
        assert_eq!(hubs(&g), vec!["orders"]);
        assert_eq!(channels(&g, p), exact("orders"));
        assert_eq!(channels(&g, c), exact("orders"));
    }

    #[test]
    fn a_site_on_two_brokers_joins_both_channels() {
        let mut g = ContractGraph::new();
        let bridge = site(&mut g, "bridge", "a/bridge.ts");
        g.add_producer_on(bridge, "orders", Some(Broker::Kafka));
        g.add_producer_on(bridge, "orders", Some(Broker::Sns));
        g.reconcile_edges();
        assert_eq!(
            channels(&g, bridge),
            vec![
                ("kafka:orders".to_string(), EdgeConfidence::Exact),
                ("sns:orders".to_string(), EdgeConfidence::Exact)
            ]
        );
    }

    #[test]
    fn a_function_may_consume_on_one_broker_and_produce_on_another() {
        let mut g = ContractGraph::new();
        let relay = site(&mut g, "relay", "a/relay.ts");
        g.add_consumer_on(relay, "orders", Some(Broker::Kafka));
        g.add_producer_on(relay, "orders", Some(Broker::RabbitMq));
        g.reconcile_edges();
        assert_eq!(
            channels(&g, relay),
            vec![
                ("kafka:orders".to_string(), EdgeConfidence::Exact),
                ("rabbitmq:orders".to_string(), EdgeConfidence::Exact)
            ]
        );
        // Not its own cause: nothing on the Kafka hub is a producer.
        let kafka_hub = g
            .all_nodes()
            .find(|n| n.name == "kafka:orders")
            .map(|n| n.id)
            .unwrap_or(0);
        assert!(!g
            .all_edges()
            .iter()
            .any(|e| e.kind == EdgeKind::Produces && e.to == kafka_hub));
    }

    /// The broker of each impact row, by (role, node): `None` is a bare row.
    fn row_brokers(g: &ContractGraph, target: &str) -> Vec<(String, &'static str, Option<Broker>)> {
        g.impact_matrix(target, 1)
            .rows
            .iter()
            .map(|r| (r.node.name.to_string(), r.role.label(), r.broker))
            .collect()
    }

    /// 9.0 T17: a producer and a consumer row carry the broker of their own site, in their own
    /// role, so two sites of one name on two brokers are two rows with two qualifiers.
    #[test]
    fn impact_rows_carry_the_broker_of_their_site_in_their_role() {
        let mut g = ContractGraph::new();
        let k = site(&mut g, "orders", "a/k.ts");
        let r = site(&mut g, "orders", "a/r.ts");
        let relay = site(&mut g, "relay", "a/relay.ts");
        g.add_producer_on(k, "orders", Some(Broker::Kafka));
        g.add_producer_on(r, "orders", Some(Broker::RabbitMq));
        g.add_consumer_on(relay, "orders", Some(Broker::Kafka));
        g.add_producer_on(relay, "orders", Some(Broker::RabbitMq));
        g.reconcile_edges();
        let m = g.impact_matrix("orders", 1);
        let broker_of = |id: NodeId, role: &str| {
            m.rows
                .iter()
                .find(|row| row.node.id == id && row.role.label() == role)
                .map(|row| row.broker)
        };
        assert_eq!(broker_of(k, "producer"), Some(Some(Broker::Kafka)));
        assert_eq!(broker_of(r, "producer"), Some(Some(Broker::RabbitMq)));
        assert_eq!(broker_of(relay, "consumer"), Some(Some(Broker::Kafka)));
        assert_eq!(broker_of(relay, "producer"), Some(Some(Broker::RabbitMq)));
        // Two distinct rows: the two `orders` producers do not merge into one.
        assert_eq!(
            m.rows
                .iter()
                .filter(|row| row.node.name == "orders")
                .count(),
            2
        );
    }

    /// A topic hub's name already carries its broker: the row names no second qualifier. A site
    /// of unknown broker, and one known on two brokers, stay bare.
    #[test]
    fn impact_rows_of_hubs_unknown_and_multi_broker_sites_stay_bare() {
        let mut g = ContractGraph::new();
        let k = site(&mut g, "k", "a/k.ts");
        let r = site(&mut g, "r", "a/r.ts");
        let legacy = site(&mut g, "legacy", "b/legacy.ts");
        let both = site(&mut g, "both", "b/both.ts");
        g.add_producer_on(k, "orders", Some(Broker::Kafka));
        g.add_consumer_on(r, "orders", Some(Broker::RabbitMq));
        g.add_consumer(legacy, "orders");
        g.add_producer_on(both, "orders", Some(Broker::Kafka));
        g.add_producer_on(both, "orders", Some(Broker::RabbitMq));
        g.reconcile_edges();
        let rows = row_brokers(&g, "orders");
        let find = |name: &str, role: &str| {
            rows.iter()
                .find(|(n, ro, _)| n == name && *ro == role)
                .map(|(_, _, b)| *b)
        };
        assert_eq!(find("kafka:orders", "topic"), Some(None));
        assert_eq!(find("rabbitmq:orders", "topic"), Some(None));
        assert_eq!(find("legacy", "consumer"), Some(None));
        assert_eq!(find("both", "producer"), Some(None));
        assert_eq!(find("k", "producer"), Some(Some(Broker::Kafka)));
    }

    #[test]
    fn hubs_follow_the_broker_order_not_the_order_of_registration() {
        let build = |order: &[Broker]| {
            let mut g = ContractGraph::new();
            for (i, b) in order.iter().enumerate() {
                let s = site(&mut g, &format!("s{i}"), &format!("a/s{i}.ts"));
                g.add_producer_on(s, "orders", Some(*b));
            }
            g.reconcile_edges();
            hubs(&g)
        };
        let forward = build(&[Broker::Kafka, Broker::Nats, Broker::Redis]);
        let backward = build(&[Broker::Redis, Broker::Nats, Broker::Kafka]);
        assert_eq!(forward, backward);
        assert_eq!(forward, vec!["kafka:orders", "redis:orders", "nats:orders"]);
    }

    #[test]
    fn a_wildcard_subscription_does_not_hear_another_brokers_subject() {
        let mut g = ContractGraph::new();
        let nats_pub = site(&mut g, "np", "a/np.ts");
        let mqtt_pub = site(&mut g, "mp", "a/mp.ts");
        let nats_sub = site(&mut g, "ns", "b/ns.ts");
        g.add_producer_on(nats_pub, "orders.created", Some(Broker::Nats));
        g.add_producer_on(mqtt_pub, "orders.paid", Some(Broker::Mqtt));
        g.add_consumer_on(nats_sub, "orders.*", Some(Broker::Nats));
        g.reconcile_edges();
        let heard: Vec<NodeId> = g
            .all_edges()
            .iter()
            .filter(|e| e.kind == EdgeKind::Produces && e.confidence == EdgeConfidence::Heuristic)
            .map(|e| e.from)
            .collect();
        assert_eq!(heard, vec![nats_pub]);
    }

    #[test]
    fn the_wildcard_producer_cap_is_unchanged_by_brokers() {
        let mut g = ContractGraph::new();
        let sub = site(&mut g, "sub", "b/sub.ts");
        g.add_consumer_on(sub, "orders.*", Some(Broker::Nats));
        for i in 0..(MAX_WILDCARD_PRODUCERS + 44) {
            let p = site(&mut g, "p", &format!("a/p{i}.ts"));
            g.add_producer_on(p, &format!("orders.n{i:04}"), Some(Broker::Nats));
        }
        g.reconcile_edges();
        let heard = g
            .all_edges()
            .iter()
            .filter(|e| e.kind == EdgeKind::Produces && e.confidence == EdgeConfidence::Heuristic)
            .count();
        assert_eq!(heard, MAX_WILDCARD_PRODUCERS);
    }

    /// The cap is per hub: the same pattern on two brokers has two hubs, each cut at the cap,
    /// and each is counted (`IndexHealth::topic_wildcards_capped`, shown by `doctor`).
    #[test]
    fn the_wildcard_cap_is_per_hub_and_counted() {
        let mut g = ContractGraph::new();
        for broker in [Broker::Nats, Broker::Redis] {
            let sub = site(&mut g, "sub", &format!("b/sub-{}.ts", broker.slug()));
            g.add_consumer_on(sub, "orders.*", Some(broker));
            for i in 0..(MAX_WILDCARD_PRODUCERS + 44) {
                let p = site(&mut g, "p", &format!("a/{}-{i}.ts", broker.slug()));
                g.add_producer_on(p, &format!("orders.{}{i:04}", broker.slug()), Some(broker));
            }
        }
        g.reconcile_edges();
        let heard = g
            .all_edges()
            .iter()
            .filter(|e| e.kind == EdgeKind::Produces && e.confidence == EdgeConfidence::Heuristic)
            .count();
        assert_eq!(heard, 2 * MAX_WILDCARD_PRODUCERS);
        assert_eq!(g.wildcard_hubs_capped(), 2);
        let mut health = crate::health::IndexHealth::default();
        health.set_wildcards_capped(g.wildcard_hubs_capped());
        assert!(health.topics_truncated());
        let text = health.render_summary(10);
        assert!(text.contains("2 wildcard subscription hub(s)"), "{text}");
        // A delta that edits an unrelated key keeps the count; deleting one hub's producers
        // below the cap clears that hub.
        let (delta, _) = {
            let mut d = g.clone();
            let mode = d.reconcile_delta();
            (d, mode)
        };
        assert_eq!(delta.wildcard_hubs_capped(), 2);
        for i in 0..60 {
            g.patch_file(Path::new(&format!("a/nats-{i}.ts")));
        }
        let mut d = g.clone();
        d.reconcile_delta();
        assert_eq!(d.wildcard_hubs_capped(), 1);
        g.reconcile_edges();
        assert_eq!(g.wildcard_hubs_capped(), 1);
        assert_eq!(d.canonical_lines(), g.canonical_lines());
    }

    #[test]
    fn a_qualified_target_selects_one_channel_and_a_bare_one_lists_them_all() {
        let mut g = ContractGraph::new();
        let k = site(&mut g, "k", "a/k.ts");
        let r = site(&mut g, "r", "a/r.ts");
        let ck = site(&mut g, "ck", "b/ck.ts");
        let cr = site(&mut g, "cr", "b/cr.ts");
        let legacy = site(&mut g, "legacy", "b/legacy.ts");
        g.add_producer_on(k, "orders", Some(Broker::Kafka));
        g.add_producer_on(r, "orders", Some(Broker::RabbitMq));
        g.add_consumer_on(ck, "orders", Some(Broker::Kafka));
        g.add_consumer_on(cr, "orders", Some(Broker::RabbitMq));
        g.add_consumer(legacy, "orders");
        g.reconcile_edges();

        let ids = |nodes: &[&ContractNode]| {
            let mut v: Vec<NodeId> = nodes.iter().map(|n| n.id).collect();
            v.sort_unstable();
            v
        };
        let all = g.analyze_impact("orders");
        assert_eq!(ids(&all.upstream_producers), vec![k, r]);
        assert_eq!(ids(&all.downstream_consumers), vec![ck, cr, legacy]);
        assert_eq!(all.topics.len(), 2);

        let kafka = g.analyze_impact("kafka:orders");
        assert_eq!(kafka.key, "orders");
        assert_eq!(ids(&kafka.upstream_producers), vec![k]);
        // The unknown-broker consumer is on every channel of the name.
        assert_eq!(ids(&kafka.downstream_consumers), vec![ck, legacy]);
        assert_eq!(kafka.topics.len(), 1);
        assert_eq!(kafka.topics[0].name, "kafka:orders");

        let rabbit = g.analyze_impact("RabbitMQ:orders");
        assert_eq!(ids(&rabbit.upstream_producers), vec![r]);
        assert_eq!(ids(&rabbit.downstream_consumers), vec![cr, legacy]);
    }

    #[test]
    fn a_key_spelled_like_a_qualified_target_is_read_as_that_key() {
        let mut g = ContractGraph::new();
        let p = site(&mut g, "p", "a/p.ts");
        let q = site(&mut g, "q", "a/q.ts");
        // `kafka:orders` is a Redis key; Kafka does not use `orders` at all: no ambiguity.
        g.add_producer_on(p, "kafka:orders", Some(Broker::Redis));
        g.add_producer_on(q, "orders", Some(Broker::Nats));
        g.reconcile_edges();
        assert_eq!(g.target_literal_clash("kafka:orders"), None);
        let flow = g.analyze_impact("kafka:orders");
        assert_eq!(flow.key, "kafka:orders");
        let ids: Vec<NodeId> = flow.upstream_producers.iter().map(|n| n.id).collect();
        assert_eq!(ids, vec![p]);
    }

    /// A key written like a broker hub (a Kafka topic literally named `rabbitmq:orders`) while
    /// RabbitMQ also uses `orders` must neither share a hub name with the RabbitMQ hub nor hide it.
    fn clash_graph() -> (ContractGraph, NodeId, NodeId, NodeId, NodeId) {
        let mut g = ContractGraph::new();
        let literal = site(&mut g, "literal", "a/literal.ts");
        let kafka = site(&mut g, "kafka", "a/kafka.ts");
        let rabbit_p = site(&mut g, "rabbit_p", "b/rabbit_p.ts");
        let rabbit_c = site(&mut g, "rabbit_c", "b/rabbit_c.ts");
        g.add_producer_on(literal, "rabbitmq:orders", Some(Broker::Kafka));
        g.add_producer_on(kafka, "orders", Some(Broker::Kafka));
        g.add_producer_on(rabbit_p, "orders", Some(Broker::RabbitMq));
        g.add_consumer_on(rabbit_c, "orders", Some(Broker::RabbitMq));
        g.reconcile_edges();
        (g, literal, kafka, rabbit_p, rabbit_c)
    }

    #[test]
    fn a_literal_key_never_shares_its_name_with_a_hub() {
        let (g, literal, _, rabbit_p, rabbit_c) = clash_graph();
        let names = hubs(&g);
        assert_eq!(
            names,
            vec![
                "kafka:orders",
                "rabbitmq:orders",
                "rabbitmq:orders (literal)"
            ]
        );
        let unique: std::collections::BTreeSet<_> = names.iter().collect();
        assert_eq!(unique.len(), names.len());
        assert_eq!(channels(&g, literal), exact("rabbitmq:orders (literal)"));
        assert_eq!(channels(&g, rabbit_p), exact("rabbitmq:orders"));
        assert_eq!(channels(&g, rabbit_c), exact("rabbitmq:orders"));
    }

    #[test]
    fn an_ambiguous_target_lists_both_readings() {
        let (g, literal, kafka, rabbit_p, rabbit_c) = clash_graph();
        assert_eq!(
            g.target_literal_clash("RabbitMQ:orders"),
            Some(CompactStr::new("rabbitmq:orders"))
        );
        let flow = g.analyze_impact("rabbitmq:orders");
        let ids = |nodes: &[&ContractNode]| {
            let mut v: Vec<NodeId> = nodes.iter().map(|n| n.id).collect();
            v.sort_unstable();
            v
        };
        // The literal Kafka producer AND the real RabbitMQ channel, not one hiding the other.
        let mut want = vec![literal, rabbit_p];
        want.sort_unstable();
        assert_eq!(ids(&flow.upstream_producers), want);
        assert_eq!(ids(&flow.downstream_consumers), vec![rabbit_c]);
        assert!(!flow.upstream_producers.iter().any(|n| n.id == kafka));
        let hub_names: Vec<&str> = flow
            .topics
            .iter()
            .filter(|n| n.package == "event-bus")
            .map(|n| n.name.as_str())
            .collect();
        assert!(hub_names.contains(&"rabbitmq:orders"), "{hub_names:?}");
        assert!(
            hub_names.contains(&"rabbitmq:orders (literal)"),
            "{hub_names:?}"
        );
        // The bare name still lists every channel, and a trace from the target starts at the
        // RabbitMQ consumer.
        let trace = g.trace_flow(
            crate::FlowEntry::Topic("rabbitmq:orders"),
            crate::FlowDirection::Forward,
            crate::FlowBounds::clamped(None, None, None),
            &|_| false,
        );
        assert!(trace.entries.iter().any(|n| n.id == rabbit_c));
    }

    #[test]
    fn the_matrix_marks_an_unknown_broker_site_on_a_shared_name_heuristic() {
        use crate::contracts::ImpactRole;
        let mut g = ContractGraph::new();
        let k = site(&mut g, "k", "a/k.ts");
        let r = site(&mut g, "r", "a/r.ts");
        let legacy = site(&mut g, "legacy", "b/legacy.ts");
        g.add_producer_on(k, "orders", Some(Broker::Kafka));
        g.add_producer_on(r, "orders", Some(Broker::RabbitMq));
        g.add_consumer(legacy, "orders");
        g.reconcile_edges();
        let matrix = g.impact_matrix("orders", 1);
        let conf = |id: NodeId, role: ImpactRole| {
            matrix
                .rows
                .iter()
                .find(|row| row.node.id == id && row.role == role)
                .map(|row| row.confidence)
        };
        assert_eq!(conf(k, ImpactRole::Producer), Some(EdgeConfidence::Exact));
        assert_eq!(
            conf(legacy, ImpactRole::Consumer),
            Some(EdgeConfidence::Heuristic)
        );
    }

    #[test]
    fn a_removed_file_returns_the_name_to_one_plain_channel() {
        let mut g = ContractGraph::new();
        let k = site(&mut g, "k", "a/k.ts");
        let r = site(&mut g, "r", "b/r.ts");
        g.add_producer_on(k, "orders", Some(Broker::Kafka));
        g.add_consumer_on(r, "orders", Some(Broker::RabbitMq));
        g.reconcile_edges();
        assert_eq!(hubs(&g).len(), 2);
        g.patch_file(Path::new("b/r.ts"));
        g.reconcile_edges();
        assert_eq!(hubs(&g), vec!["orders"]);
        assert!(g.known_brokers("orders").count_ones() == 1);
        assert!(g.consumer_brokers.is_empty());
    }

    // ---- hostile input ----------------------------------------------------------------

    /// Runs `f` on a thread with the 2 MB stack of the target platforms' smallest worker.
    fn on_small_stack(f: impl FnOnce() + Send + 'static) {
        std::thread::Builder::new()
            .stack_size(2 * 1024 * 1024)
            .spawn(f)
            .expect("spawn")
            .join()
            .expect("no stack overflow, no panic");
    }

    #[test]
    fn hostile_names_do_not_break_the_topic_pass() {
        on_small_stack(|| {
            let mut g = ContractGraph::new();
            let huge = "o".repeat(1 << 20);
            let names = [
                huge.as_str(),
                "",
                "   ",
                "kafka:",
                ":",
                "kafka:kafka:orders",
                "ORDERS",
                "Ünïcode.ÖRDERS",
                "ünïcode.örders",
                "\u{0}\u{1f}",
                "orders\u{1f}kafka",
            ];
            for (i, name) in names.iter().enumerate() {
                let p = site(&mut g, &format!("p{i}"), &format!("a/p{i}.ts"));
                let c = site(&mut g, &format!("c{i}"), &format!("b/c{i}.ts"));
                g.add_producer_on(p, name, Some(Broker::Kafka));
                g.add_consumer_on(c, name, Some(Broker::RabbitMq));
            }
            g.reconcile_edges();
            // Case folding is Unicode-aware and applies to every key alike.
            assert!(hubs(&g).iter().any(|h| h == "kafka:orders"));
            assert!(hubs(&g).iter().any(|h| h == "rabbitmq:ünïcode.örders"));
            assert_eq!(
                hubs(&g)
                    .iter()
                    .filter(|h| h.ends_with("ünïcode.örders"))
                    .count(),
                2
            );
            for target in ["", "kafka:", ":", "kafka:kafka:orders", "rabbitmq:ORDERS"] {
                let _ = g.analyze_impact(target);
            }
        });
    }

    #[test]
    fn ten_thousand_distinct_keys_on_two_brokers_reconcile() {
        on_small_stack(|| {
            let mut g = ContractGraph::new();
            for i in 0..10_000 {
                let p = site(&mut g, "p", &format!("a/p{i}.ts"));
                let c = site(&mut g, "c", &format!("b/c{i}.ts"));
                // Every 3rd key is shared by two brokers, the others by one.
                let consumer_broker = if i % 3 == 0 {
                    Broker::RabbitMq
                } else {
                    Broker::Kafka
                };
                g.add_producer_on(p, &format!("topic.{i}"), Some(Broker::Kafka));
                g.add_consumer_on(c, &format!("topic.{i}"), Some(consumer_broker));
            }
            g.reconcile_edges();
            let hub_count = hubs(&g).len();
            // 3334 shared keys give 2 hubs, 6666 give 1.
            assert_eq!(hub_count, 3334 * 2 + 6666);
        });
    }
}
