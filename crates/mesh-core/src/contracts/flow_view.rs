//! Read-only support for the event-centred flow view (`visualize_mesh` with `event`, 9.0
//! ticket T13). The chains themselves come from the chain engine (`flow`, `trace_flow`); this
//! module only names the centre of the view and the broker of a hub, which the engine keeps
//! private.

use super::*;

/// The event(s) a flow view is centred on.
#[derive(Debug, Clone)]
pub struct FlowCentres {
    /// The topic key the target was matched as (`analyze_impact`'s key).
    pub key: CompactStr,
    /// Names of the matched broker / plain hubs (`kafka:orders`, `orders`), sorted. Empty when
    /// the target names no event.
    pub hubs: Vec<CompactStr>,
    /// Producer and consumer sites of each hub of `hubs` (same order).
    pub hub_counts: Vec<(usize, usize)>,
    /// Whether some event matched the target at all (a hub, a producer or a consumer).
    pub matched: bool,
}

impl ContractGraph {
    /// The centre of a flow view for `target`, matched exactly like `trace_flow` matches a topic
    /// (the same `analyze_impact` resolution: qualified key, bare name, substring fallback).
    pub fn flow_centres(&self, target: &str) -> FlowCentres {
        let flow = self.analyze_impact(target);
        let matched = !(flow.topics.is_empty()
            && flow.upstream_producers.is_empty()
            && flow.downstream_consumers.is_empty()
            && flow.related_sagas.is_empty());
        let mut hub_nodes: Vec<&ContractNode> = flow
            .topics
            .iter()
            .copied()
            .filter(|n| Self::is_hub_like(n))
            .collect();
        hub_nodes.sort_by(|a, b| a.name.cmp(&b.name));
        hub_nodes.dedup_by(|a, b| a.name == b.name);
        let hub_counts = hub_nodes
            .iter()
            .map(|h| {
                let (mut producers, mut consumers) = (0, 0);
                for e in &self.edges {
                    match e.kind {
                        EdgeKind::Produces if e.to == h.id => producers += 1,
                        EdgeKind::Consumes if e.to == h.id || e.from == h.id => consumers += 1,
                        _ => {}
                    }
                }
                (producers, consumers)
            })
            .collect();
        FlowCentres {
            key: flow.key,
            hubs: hub_nodes.iter().map(|n| n.name.clone()).collect(),
            hub_counts,
            matched,
        }
    }

    /// The producer and consumer sites some broker is statically known for. A site that is not
    /// in this set joins every channel of its topic name (the engine's unknown-broker rule).
    pub fn sites_with_known_broker(&self) -> HashSet<NodeId> {
        [&self.producer_brokers, &self.consumer_brokers]
            .into_iter()
            .flat_map(HashMap::values)
            .flat_map(HashMap::iter)
            .filter(|(_, mask)| **mask != 0)
            .map(|(id, _)| *id)
            .collect()
    }

    /// The broker behind the hub named `label` (a `FlowNext::topic` or a step's topic): the
    /// hub's own, else the name's only known broker; `None` when the broker is not known.
    pub fn hub_label_broker(&self, label: &str) -> Option<Broker> {
        let from_hub = self
            .name_to_nodes
            .get(label)
            .map(Vec::as_slice)
            .unwrap_or(&[])
            .iter()
            .filter_map(|id| self.nodes.get(id))
            .filter(|n| Self::is_hub_like(n))
            .find_map(|n| self.hub_ref(n).broker);
        from_hub.or_else(|| {
            let mask = self.known_brokers(label);
            (mask.count_ones() == 1)
                .then(|| Broker::of_mask(mask).next())
                .flatten()
        })
    }
}
