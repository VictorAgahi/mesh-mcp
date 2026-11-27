//! The event section of `diff_mesh` (9.0 ticket T10): per broker-qualified channel, the
//! producers and consumers the branch adds or removes, the channels it renames, the producer ->
//! consumer links it cuts or creates, and the one-hop chains (entry -> calls -> publish) of the
//! T3 chain engine it cuts or creates.
//!
//! Both sides are graphs: the live graph is the working tree; the base graph is a copy of it
//! with every analysed changed file replaced by its base version and reconciled the way a
//! reload is. Only the channels with a site in a changed file (on either side) are compared,
//! so the work is bounded by the branch, not by the workspace.

use super::{clip_line, clip_path, push_within, RootView, NOTE_RESERVE};
use mesh_core::config_values::strip_placeholder_defaults;
use mesh_core::member_consts::constant_is_a_name;
use mesh_core::{
    CompactStr, ConfigValues, ContractGraph, EdgeConfidence, EventChannel, EventHop, EventSite,
    HopEnd, MemberTable,
};
use mesh_parsers::FileIndex;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

/// Channels checked for links and chain hops per call; the rest are counted.
pub(super) const MAX_EVENT_CHANNELS: usize = 200;
/// Bytes the event section may take before the impact rows.
pub(super) const EVENT_BUDGET: usize = 14 * 1024;
/// Site rows listed per channel; the rest are counted.
const MAX_SITE_ROWS: usize = 20;
/// What a channel key that may be a secret is shown as (commandment 5).
const REDACTED: &str = "[REDACTED_SECRET: USE_ENV_OR_LOCAL_FALLBACK]";

/// What the extraction loop gathers for the event section.
#[derive(Default)]
pub(super) struct EventInput {
    /// Some analysed changed file has a producer or consumer on either side.
    pub(super) relevant: bool,
    /// Every changed file analysed on both sides (not skipped).
    pub(super) files: Vec<PathBuf>,
    /// The base version of each of them that exists at the base.
    pub(super) base: Vec<FileIndex>,
}

/// `(producer, file, owner)`: a site of a channel, line-independent.
type SiteId = (bool, Arc<Path>, CompactStr);

struct ChannelDelta<'a> {
    added: Vec<&'a EventSite>,
    removed: Vec<&'a EventSite>,
    /// `Some(true)`: link created, `Some(false)`: link cut.
    link: Option<bool>,
}

struct Rename {
    from: EventChannel,
    to: EventChannel,
    sites: Vec<SiteId>,
    /// Linked at the base under `from` and in the working tree under `to`.
    linked: bool,
}

pub(super) struct EventSection {
    pub(super) head: String,
    pub(super) items: Vec<String>,
    pub(super) notes: Vec<String>,
}

/// The live graph with every file of `input` at its base version, reconciled like a reload.
fn base_graph(
    live: &ContractGraph,
    config: &ConfigValues,
    members: &MemberTable,
    input: EventInput,
) -> (ContractGraph, Vec<PathBuf>) {
    let mut graph = live.clone();
    graph.patch_files(input.files.iter().map(PathBuf::as_path));
    for index in input.base {
        index.apply_with(&mut graph, config, members);
    }
    graph.reconcile_delta();
    (graph, input.files)
}

/// The section, or `None` when no changed file touches an event.
pub(super) fn event_report(
    input: EventInput,
    live: &ContractGraph,
    config: &ConfigValues,
    members: &MemberTable,
    view: &RootView<'_>,
    deadline: Instant,
) -> Option<EventSection> {
    if !input.relevant {
        return None;
    }
    if Instant::now() >= deadline {
        return Some(EventSection {
            head: "\n### Event changes\n\n*Not compared: the time budget of this call is spent; narrow `scope`.*\n".into(),
            items: Vec::new(),
            notes: Vec::new(),
        });
    }
    let (base, files) = base_graph(live, config, members, input);
    let paths: Vec<&Path> = files.iter().map(PathBuf::as_path).collect();
    let base_sites = base.event_sites_in(&paths);
    let head_sites = live.event_sites_in(&paths);
    let id = |s: &EventSite| (s.channel.clone(), s.producer, s.file.clone(), s.owner.clone());
    let bmap: BTreeMap<_, &EventSite> = base_sites.iter().map(|s| (id(s), s)).collect();
    let hmap: BTreeMap<_, &EventSite> = head_sites.iter().map(|s| (id(s), s)).collect();

    let mut channels: BTreeMap<EventChannel, ChannelDelta<'_>> = BTreeMap::new();
    let delta = |channels: &mut BTreeMap<EventChannel, ChannelDelta<'_>>, ch: &EventChannel| {
        if !channels.contains_key(ch) {
            channels.insert(
                ch.clone(),
                ChannelDelta {
                    added: Vec::new(),
                    removed: Vec::new(),
                    link: None,
                },
            );
        }
    };
    for (k, s) in &bmap {
        if !hmap.contains_key(k) {
            delta(&mut channels, &k.0);
            if let Some(d) = channels.get_mut(&k.0) {
                d.removed.push(s);
            }
        }
    }
    for (k, s) in &hmap {
        if !bmap.contains_key(k) {
            delta(&mut channels, &k.0);
            if let Some(d) = channels.get_mut(&k.0) {
                d.added.push(s);
            }
        }
    }

    // Renamed: a channel gone from the whole working tree and a channel new to it, on the same
    // broker, whose sites are the same (same role, file and declaration). Only a one-to-one
    // match is a rename; anything else stays a removal and an addition.
    let sig = |list: &[&EventSite]| -> BTreeSet<SiteId> {
        list.iter()
            .map(|s| (s.producer, s.file.clone(), s.owner.clone()))
            .collect()
    };
    let gone: Vec<(&EventChannel, BTreeSet<SiteId>)> = channels
        .iter()
        .filter(|(ch, d)| {
            d.added.is_empty() && !d.removed.is_empty() && !live.channel_has_sites(ch)
        })
        .map(|(ch, d)| (ch, sig(&d.removed)))
        .collect();
    let new: Vec<(&EventChannel, BTreeSet<SiteId>)> = channels
        .iter()
        .filter(|(ch, d)| {
            d.removed.is_empty() && !d.added.is_empty() && !base.channel_has_sites(ch)
        })
        .map(|(ch, d)| (ch, sig(&d.added)))
        .collect();
    let mut renames: Vec<Rename> = Vec::new();
    for (from, sites) in &gone {
        let matches =
            |a: &EventChannel, b: &EventChannel, x: &BTreeSet<SiteId>, y: &BTreeSet<SiteId>| {
                a.broker == b.broker && x == y
            };
        let to: Vec<&EventChannel> = new
            .iter()
            .filter(|(to, s)| matches(from, to, sites, s))
            .map(|(to, _)| *to)
            .collect();
        let [to] = to.as_slice() else {
            continue;
        };
        let back = gone
            .iter()
            .filter(|(f, s)| matches(f, to, s, sites))
            .count();
        if back != 1 {
            continue;
        }
        renames.push(Rename {
            from: (*from).clone(),
            to: (*to).clone(),
            sites: sites.iter().cloned().collect(),
            linked: base.channel_linked(from) && live.channel_linked(to),
        });
    }
    for r in &renames {
        channels.remove(&r.from);
        channels.remove(&r.to);
    }
    let renamed_to: HashMap<&EventChannel, &EventChannel> =
        renames.iter().map(|r| (&r.from, &r.to)).collect();

    // Links and hops, over every channel with a site in a changed file (capped).
    let touched: BTreeSet<&EventChannel> = base_sites
        .iter()
        .chain(&head_sites)
        .map(|s| &s.channel)
        .collect();
    let traced: Vec<EventChannel> = touched
        .iter()
        .take(MAX_EVENT_CHANNELS)
        .map(|c| (*c).clone())
        .collect();
    let untraced = touched.len() - traced.len();
    let renamed_linked: BTreeSet<&EventChannel> = renames
        .iter()
        .filter(|r| r.linked)
        .flat_map(|r| [&r.from, &r.to])
        .collect();
    for ch in &traced {
        if renamed_linked.contains(ch) {
            continue;
        }
        let (was, is) = (base.channel_linked(ch), live.channel_linked(ch));
        if was != is {
            delta(&mut channels, ch);
            if let Some(d) = channels.get_mut(ch) {
                d.link = Some(is);
            }
        }
    }
    let mut hops_timed_out = false;
    let (mut cut, mut created) = (Vec::new(), Vec::new());
    let mut bounded = 0;
    if Instant::now() >= deadline {
        hops_timed_out = true;
    } else {
        let rename = |c: &mut Option<EventChannel>| {
            if let Some(to) = c.as_ref().and_then(|ch| renamed_to.get(ch)) {
                *c = Some((*to).clone());
            }
        };
        let b = base.event_hops(&traced);
        let h = live.event_hops(&traced);
        bounded = b.bounded.max(h.bounded);
        let b_hops: BTreeMap<EventHop, (EdgeConfidence, bool)> = b
            .hops
            .into_iter()
            .map(|(mut hop, v)| {
                rename(&mut hop.entry.channel);
                rename(&mut hop.publish.channel);
                (hop, v)
            })
            .collect();
        cut = b_hops
            .iter()
            .filter(|(k, _)| !h.hops.contains_key(*k))
            .map(|(k, v)| (k.clone(), *v))
            .collect();
        created = h
            .hops
            .iter()
            .filter(|(k, _)| !b_hops.contains_key(*k))
            .map(|(k, v)| (k.clone(), *v))
            .collect();
    }
    let missing = base.call_files_missing_sites().len();

    // Rendering.
    let count = |producer: bool, added: bool| -> usize {
        channels
            .values()
            .map(|d| {
                let list = if added { &d.added } else { &d.removed };
                list.iter().filter(|s| s.producer == producer).count()
            })
            .sum()
    };
    let with_sites = channels
        .values()
        .filter(|d| !(d.added.is_empty() && d.removed.is_empty()))
        .count();
    let links_cut = channels.values().filter(|d| d.link == Some(false)).count();
    let links_created = channels.values().filter(|d| d.link == Some(true)).count();
    let head = format!(
        "\n### Event changes (broker-qualified channels)\n\n**Events**: {with_sites} channel(s) with site changes; producers +{} -{}; consumers +{} -{}; {} renamed; producer-consumer links cut {links_cut}, created {links_created}; chain hops cut {}, created {}.\n",
        count(true, true),
        count(true, false),
        count(false, true),
        count(false, false),
        renames.len(),
        cut.len(),
        created.len(),
    );
    let mut items = Vec::new();
    for r in &renames {
        let sites: Vec<String> = r
            .sites
            .iter()
            .take(MAX_SITE_ROWS)
            .map(|(p, f, o)| {
                format!(
                    "{} in `{}` (`{}`)",
                    role(*p),
                    clip_line(o),
                    clip_path(&view.path(f))
                )
            })
            .collect();
        let more = r.sites.len().saturating_sub(MAX_SITE_ROWS);
        items.push(format!(
            "\n- renamed {} → {}: the same {} site(s): {}{}{}\n",
            shown(&r.from),
            shown(&r.to),
            r.sites.len(),
            sites.join(", "),
            if more > 0 {
                format!(" and {more} more")
            } else {
                String::new()
            },
            if r.linked {
                "; the producer-consumer link is kept under the new name"
            } else {
                ""
            }
        ));
    }
    for (ch, d) in &channels {
        let mut item = format!("\n#### {}", shown(ch));
        match d.link {
            Some(false) => item.push_str(
                " — link cut: no producer on this channel reaches a consumer any more",
            ),
            Some(true) => item.push_str(" — link created: a producer now reaches a consumer"),
            None => {}
        }
        item.push('\n');
        let rows = d
            .added
            .iter()
            .map(|s| ('+', *s))
            .chain(d.removed.iter().map(|s| ('-', *s)));
        let total = d.added.len() + d.removed.len();
        for (sign, s) in rows.take(MAX_SITE_ROWS) {
            let at = if sign == '+' {
                format!("{}:{}", clip_path(&view.path(&s.file)), s.line)
            } else {
                format!("{}, base L{}", clip_path(&view.path(&s.file)), s.line)
            };
            let _ = writeln!(
                item,
                "- {sign} {} of {} in `{}` (`{at}`)",
                role(s.producer),
                shown(&s.channel),
                clip_line(&s.owner)
            );
        }
        if total > MAX_SITE_ROWS {
            let _ = writeln!(item, "- … and {} more", total - MAX_SITE_ROWS);
        }
        items.push(item);
    }
    for (title, list) in [("cut", &cut), ("created", &created)] {
        if list.is_empty() {
            continue;
        }
        items.push(format!("\n#### Chain hops {title}\n"));
        for (hop, (confidence, fallback)) in list {
            items.push(format!(
                "- {} → publishes {} _({}{})_\n",
                end(&hop.entry, view),
                end(&hop.publish, view),
                confidence.label(),
                if *fallback {
                    ", same-file fallback"
                } else {
                    ""
                }
            ));
        }
    }
    let mut notes = vec![
        "\n*Payload fields: not compared. No extractor exposes the payload type of an event yet.*\n"
            .to_string(),
    ];
    if untraced > 0 {
        notes.push(format!(
            "\n*{untraced} more channel(s) not checked for links and chain hops: cap ({MAX_EVENT_CHANNELS} per call); narrow `scope`.*\n"
        ));
    }
    if hops_timed_out {
        notes.push(
            "\n*Chain hops not compared: the time budget of this call is spent; narrow `scope`.*\n"
                .to_string(),
        );
    }
    if bounded > 0 {
        notes.push(format!(
            "\n*{bounded} trace(s) hit a bound (call_depth, fan_out or the hop cap): a hop past it is not compared; check with `trace_flow`.*\n"
        ));
    }
    if missing > 0 {
        notes.push(format!(
            "\n*{missing} file(s) the live index pruned of call sites lie on a chain at the base: a hop cut through them may be missed.*\n"
        ));
    }
    Some(EventSection { head, items, notes })
}

fn role(producer: bool) -> &'static str {
    if producer {
        "producer"
    } else {
        "consumer"
    }
}

/// One end of a hop, on one line.
fn end(e: &HopEnd, view: &RootView<'_>) -> String {
    let at = clip_path(&view.path(&e.file));
    match &e.channel {
        Some(ch) if e.kind == "producer" => {
            format!("{} in `{}` (`{at}`)", shown(ch), clip_line(&e.owner))
        }
        Some(ch) => format!(
            "{} of {} in `{}` (`{at}`)",
            e.kind,
            shown(ch),
            clip_line(&e.owner)
        ),
        None => format!("{} `{}` (`{at}`)", e.kind, clip_line(&e.owner)),
    }
}

/// A channel as shown, in Markdown: its broker qualifier always (`(broker unknown)` when the
/// site's is not known), placeholder defaults stripped, and a key that may be a credential
/// redacted.
fn shown(ch: &EventChannel) -> String {
    let key = shown_key(&ch.key);
    match ch.broker {
        Some(b) => format!("`{}:{key}`", b.slug()),
        None => format!("`{key}` (broker unknown)"),
    }
}

fn shown_key(key: &str) -> String {
    let stripped = strip_placeholder_defaults(key).into_owned();
    // What is left once every `${k}` is taken out must be a name (the T14 rule).
    let mut outside = stripped.clone();
    while let Some(start) = outside.find("${") {
        let Some(len) = outside[start..].find('}') else {
            break;
        };
        outside.replace_range(start..start + len + 1, "");
    }
    let outside = outside.replace(['#', '>'], "*");
    let bare = outside.trim_matches(|c: char| matches!(c, '.' | '_' | '-' | ':' | '/'));
    if bare.is_empty() || constant_is_a_name("", bare) {
        stripped.replace('`', "'")
    } else {
        REDACTED.to_string()
    }
}

/// The section within [`EVENT_BUDGET`], its notes reserved.
pub(super) fn push_events(out: &mut String, section: &EventSection, output_budget: usize) {
    let limit = (out.len() + EVENT_BUDGET).min(output_budget);
    let notes_len: usize = section.notes.iter().map(String::len).sum();
    out.push_str(&section.head);
    let mut left = 0usize;
    for item in &section.items {
        if !push_within(out, item, limit, notes_len + NOTE_RESERVE) {
            left += 1;
        }
    }
    if left > 0 {
        let _ = writeln!(
            out,
            "\n*{left} more event item(s) not shown to stay under the 48 KB cap; narrow `scope` (one root or one sub-directory).*"
        );
    }
    for note in &section.notes {
        out.push_str(note);
    }
}
