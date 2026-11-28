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
        let mut hubs: Vec<CompactStr> = flow
            .topics
            .iter()
            .filter(|n| Self::is_hub_like(n))
            .map(|n| n.name.clone())
            .collect();
        hubs.sort();
        hubs.dedup();
        FlowCentres {
            key: flow.key,
            hubs,
            matched,
        }
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
