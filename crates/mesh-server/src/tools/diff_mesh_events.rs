//! The event section of `diff_mesh` (9.0 ticket T10): per broker-qualified channel, the
//! producers and consumers the branch adds or removes, the channels it renames, the producer ->
//! consumer links it cuts or creates, and the one-hop chains (entry -> calls -> publish) of the
//! T3 chain engine it cuts or creates.
//!
//! Both sides are graphs: the live graph is the working tree; the base graph is a copy of it
//! with every analysed changed file replaced by its base version and reconciled the way a
//! reload is. Only the channels with a site in a changed file (on either side) are compared, so
//! the traces are bounded by the branch. The base graph is not: it is a full copy of the live
//! graph, built once per call when a changed file has an event site (on kubernetes, about +170 MB
//! of peak memory for the call, freed when it returns).

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
    /// Changed files skipped on either side (a guard, a parse failure): their sites are not
    /// compared.
    pub(super) skipped: usize,
    /// Changed files whose sites a per-file extraction cap cut (either side), for display.
    pub(super) capped: Vec<String>,
    /// A changed file declares string constants that differ between the sides while some
    /// indexed site names its topic through a constant.
    pub(super) constants_changed: bool,
}

impl EventInput {
    /// Records the two extracted sides of one analysed changed file.
    pub(super) fn add(
        &mut self,
        abs: &Path,
        display: &str,
        base: Option<FileIndex>,
        current: Option<&FileIndex>,
        members: &MemberTable,
    ) {
        let has_sites = |i: &FileIndex| !(i.producers.is_empty() && i.consumers.is_empty());
        let capped = |i: &FileIndex| {
            i.inproc_capped
                || i.jvm_brokers_capped
                || i.go_brokers_capped
                || i.py_brokers_capped
                || i.sql_capped
        };
        self.relevant |= base.as_ref().is_some_and(has_sites) || current.is_some_and(has_sites);
        if base.as_ref().is_some_and(capped) || current.is_some_and(capped) {
            self.capped.push(display.to_string());
        }
        let consts = |i: Option<&FileIndex>| -> BTreeSet<String> {
            i.map(|i| i.const_records().map(str::to_string).collect())
                .unwrap_or_default()
        };
        if !self.constants_changed && consts(base.as_ref()) != consts(current) {
            self.constants_changed = members.ref_files().next().is_some();
        }
        self.files.push(abs.to_path_buf());
        self.base.extend(base);
    }
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
        // A skipped file may hold sites: say so rather than show no section.
        return (input.skipped > 0).then(|| EventSection {
            head: format!("\n### Event changes (broker-qualified channels)\n\n*Not compared in {} skipped changed file(s) (see the file sections): their producers and consumers are unknown.*\n", input.skipped),
            items: Vec::new(),
            notes: Vec::new(),
        });
    }
    let (skipped, capped, constants_changed) =
        (input.skipped, input.capped.clone(), input.constants_changed);
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
    let id = |s: &EventSite| {
        (
            s.channel.clone(),
            s.producer,
            s.file.clone(),
            s.owner.clone(),
        )
    };
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
    // A wildcard subscription added or removed changes the links of the channels it matches,
    // whose producers may sit in unchanged files.
    let mut touched: BTreeSet<EventChannel> = base_sites
        .iter()
        .chain(&head_sites)
        .map(|s| s.channel.clone())
        .collect();
    let matched: Vec<EventChannel> = touched
        .iter()
        .flat_map(|c| {
            base.channels_matching(c)
                .into_iter()
                .chain(live.channels_matching(c))
        })
        .collect();
    touched.extend(matched);
    let traced: Vec<EventChannel> = touched.iter().take(MAX_EVENT_CHANNELS).cloned().collect();
    let untraced_set: BTreeSet<EventChannel> =
        touched.iter().skip(MAX_EVENT_CHANNELS).cloned().collect();
    let untraced = untraced_set.len();
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
    let (mut cut, mut created) = (Vec::new(), Vec::new());
    // Hops of a channel whose trace a bound or the deadline cut on either side, or that was
    // not traced (channel cap), are not compared: a missing hop there may be the bound's.
    let mut not_compared = 0usize;
    let rename = |c: &EventChannel| {
        renamed_to
            .get(c)
            .map_or_else(|| c.clone(), |t| (*t).clone())
    };
    let b = base.event_hops(&traced, deadline);
    let h = live.event_hops(&traced, deadline);
    let mut excluded: BTreeSet<EventChannel> = untraced_set;
    for side in [&b, &h] {
        excluded.extend(side.bounded.iter().chain(&side.not_traced).map(rename));
    }
    let bounded = b.bounded.union(&h.bounded).count();
    let not_traced = b.not_traced.union(&h.not_traced).count();
    let b_hops: BTreeMap<EventHop, (EdgeConfidence, bool)> = b
        .hops
        .into_iter()
        .map(|(mut hop, v)| {
            hop.entry.channel = hop.entry.channel.as_ref().map(rename);
            hop.publish.channel = hop.publish.channel.as_ref().map(rename);
            (hop, v)
        })
        .collect();
    let compared = |k: &EventHop| {
        ![&k.entry.channel, &k.publish.channel]
            .into_iter()
            .flatten()
            .any(|c| excluded.contains(c))
    };
    for (k, v) in &b_hops {
        if !h.hops.contains_key(k) {
            if compared(k) {
                cut.push((k.clone(), *v));
            } else {
                not_compared += 1;
            }
        }
    }
    for (k, v) in &h.hops {
        if !b_hops.contains_key(k) {
            if compared(k) {
                created.push((k.clone(), *v));
            } else {
                not_compared += 1;
            }
        }
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
            Some(false) => item
                .push_str(" — link cut: no producer on this channel reaches a consumer any more"),
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
    if not_traced > 0 {
        notes.push(format!(
            "\n*{not_traced} channel(s) not traced for chain hops: the time budget of this call is spent; narrow `scope`.*\n"
        ));
    }
    if bounded > 0 || not_compared > 0 {
        notes.push(format!(
            "\n*Not compared (bound): {not_compared} chain hop(s) that differ between the sides on {bounded} channel(s) whose trace hit a bound (call_depth 6, fan_out 50 or the hop cap), or on a channel not traced; check them with `trace_flow`.*\n"
        ));
    }
    if skipped > 0 {
        notes.push(format!(
            "\n*{skipped} changed file(s) skipped (see the file sections): their sites are not compared, so the counts above are lower bounds.*\n"
        ));
    }
    if !capped.is_empty() {
        notes.push(format!(
            "\n*Per-file site cap hit (for example `MAX_INPROC_SITES_PER_FILE`, 2,000): the sites of {} are truncated, so its counts are too.*\n",
            super::listed_names(capped.iter(), capped.len())
        ));
    }
    if constants_changed {
        notes.push(
            "\n*Topic constants edited: event sites in unchanged files that resolve their topic through them are not compared.*\n"
                .to_string(),
        );
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

/// Also the topic text of the per-file section: a site's raw topic never reaches the output
/// with a placeholder default or a credential-shaped name.
pub(super) fn shown_key(key: &str) -> String {
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

#[cfg(test)]
mod tests {
    use super::super::{run_with, DiffMeshArgs, Limits};
    use crate::git::test_support::TempRepo;
    use crate::indexer::WorkspaceIndexer;
    use mesh_core::AppState;
    use std::path::Path;

    // Hand-written fixture: an in-process (`inproc`) chain
    //   OrderService.create --emit order.created--> InvoiceListener.onOrderCreated
    //   --calls InvoiceService.issue--> emit invoice.issued --> ReportListener.onInvoice
    const ORDER_SERVICE: &str = r#"import { Injectable } from '@nestjs/common';
import { EventEmitter2 } from '@nestjs/event-emitter';

@Injectable()
export class OrderService {
  constructor(private readonly emitter: EventEmitter2) {}

  create(orderId: string) {
    this.emitter.emit('order.created', { orderId });
  }
}
"#;

    const INVOICE_LISTENER: &str = r#"import { Injectable } from '@nestjs/common';
import { OnEvent } from '@nestjs/event-emitter';
import { InvoiceService } from './invoice.service';

@Injectable()
export class InvoiceListener {
  constructor(private readonly invoices: InvoiceService) {}

  @OnEvent('order.created')
  onOrderCreated(order: { orderId: string }) {
    this.invoices.issue(order.orderId);
  }
}
"#;

    const INVOICE_SERVICE: &str = r#"import { Injectable } from '@nestjs/common';
import { EventEmitter2 } from '@nestjs/event-emitter';

@Injectable()
export class InvoiceService {
  constructor(private readonly emitter: EventEmitter2) {}

  issue(orderId: string) {
    this.emitter.emit('invoice.issued', { orderId });
  }
}
"#;

    const REPORT_LISTENER: &str = r#"import { Injectable } from '@nestjs/common';
import { OnEvent } from '@nestjs/event-emitter';

@Injectable()
export class ReportListener {
  @OnEvent('invoice.issued')
  onInvoice(invoice: { orderId: string }) {
    return invoice.orderId;
  }
}
"#;

    fn state(root: &Path) -> AppState {
        let config = mesh_core::Config::load_from_str(
            "[workspace]\nname = \"t\"\nversion = \"0\"\nroots = [\".\"]\n",
        )
        .expect("config");
        let audit = std::sync::Arc::new(mesh_core::AuditLogger::new_in_memory().expect("audit"));
        let rescan = std::sync::Arc::new(mesh_core::BackgroundRescanEngine::new().expect("rescan"));
        let state = AppState::new(config, vec![root.to_path_buf()], audit, rescan);
        let snapshot =
            WorkspaceIndexer::build_snapshot(&state.config, &state.allowed_roots, None, None, None);
        state.install_snapshot(snapshot);
        state
    }

    fn base() -> TempRepo {
        let repo = TempRepo::new();
        repo.write("src/order.service.ts", ORDER_SERVICE);
        repo.write("src/invoice.listener.ts", INVOICE_LISTENER);
        repo.write("src/invoice.service.ts", INVOICE_SERVICE);
        repo.write("src/report.listener.ts", REPORT_LISTENER);
        repo.commit("base");
        repo.git(&["checkout", "-q", "-b", "feature"]);
        repo
    }

    fn diff(repo: &TempRepo) -> String {
        let args = DiffMeshArgs {
            base: Some("main".into()),
            scope: None,
            _meta: None,
        };
        run_with(&args, &state(&repo.root), &repo.runner(), Limits::DEFAULT)
            .expect("diff_mesh ok")
            .text
    }

    fn events(text: &str) -> &str {
        let start = text.find("### Event changes").unwrap_or(text.len());
        let rest = &text[start..];
        let end = rest[3..].find("\n### ").map_or(rest.len(), |e| e + 3);
        &rest[..end]
    }

    fn rm(repo: &TempRepo, rel: &str) {
        std::fs::remove_file(repo.root.join(rel)).expect("rm");
    }

    const NO_CALL: &str = "    this.invoices.issue(order.orderId);\n";

    /// Removed: the only consumer of `invoice.issued` goes, so the channel's link is cut and
    /// the hop into it from `order.created` is kept (its publish is still there).
    #[test]
    fn a_removed_consumer_cuts_the_link_of_its_channel() {
        let repo = base();
        rm(&repo, "src/report.listener.ts");
        let text = diff(&repo);
        let ev = events(&text);
        assert!(
            ev.contains("**Events**: 1 channel(s) with site changes; producers +0 -0; consumers +0 -1; 0 renamed; producer-consumer links cut 1, created 0; chain hops cut 0, created 0."),
            "{ev}"
        );
        assert!(
            ev.contains("#### `inproc:invoice.issued` — link cut: no producer on this channel reaches a consumer any more\n- - consumer of `inproc:invoice.issued` in `onInvoice` (`src/report.listener.ts, base L7`)\n"),
            "{ev}"
        );
        assert!(ev.contains("*Payload fields: not compared."), "{ev}");
    }

    /// Added: a new channel with a new producer and a new consumer is a created link.
    #[test]
    fn added_sites_on_a_new_channel_create_its_link() {
        let repo = base();
        repo.write(
            "src/order.service.ts",
            &ORDER_SERVICE.replace(
                "    this.emitter.emit('order.created', { orderId });\n",
                "    this.emitter.emit('order.created', { orderId });\n    this.emitter.emit('order.audited', { orderId });\n",
            ),
        );
        repo.write(
            "src/audit.listener.ts",
            "import { Injectable } from '@nestjs/common';\nimport { OnEvent } from '@nestjs/event-emitter';\n\n@Injectable()\nexport class AuditListener {\n  @OnEvent('order.audited')\n  onAudit(e: { orderId: string }) {\n    return e.orderId;\n  }\n}\n",
        );
        let text = diff(&repo);
        let ev = events(&text);
        assert!(
            ev.contains("producers +1 -0; consumers +1 -0; 0 renamed; producer-consumer links cut 0, created 1;"),
            "{ev}"
        );
        assert!(
            ev.contains(
                "#### `inproc:order.audited` — link created: a producer now reaches a consumer\n"
            ),
            "{ev}"
        );
        assert!(
            ev.contains(
                "- + consumer of `inproc:order.audited` in `onAudit` (`src/audit.listener.ts:7`)"
            ),
            "{ev}"
        );
        assert!(
            ev.contains(
                "- + producer of `inproc:order.audited` in `create` (`src/order.service.ts:"
            ),
            "{ev}"
        );
        // `order.created` keeps its sites: not listed.
        assert!(!ev.contains("#### `inproc:order.created`"), "{ev}");
    }

    fn renamed_repo() -> TempRepo {
        let repo = base();
        repo.write(
            "src/invoice.service.ts",
            &INVOICE_SERVICE.replace("'invoice.issued'", "'invoice.sent'"),
        );
        repo.write(
            "src/report.listener.ts",
            &REPORT_LISTENER.replace("'invoice.issued'", "'invoice.sent'"),
        );
        repo
    }

    /// Renamed: every site of `invoice.issued` now names `invoice.sent`. One rename line, the
    /// link kept, and the hop from `order.created` is the same hop under the new name.
    #[test]
    fn a_topic_renamed_at_every_site_is_one_rename() {
        let repo = renamed_repo();
        let text = diff(&repo);
        let ev = events(&text);
        assert!(
            ev.contains("**Events**: 0 channel(s) with site changes; producers +0 -0; consumers +0 -0; 1 renamed; producer-consumer links cut 0, created 0; chain hops cut 0, created 0."),
            "{ev}"
        );
        assert!(
            ev.contains("- renamed `inproc:invoice.issued` → `inproc:invoice.sent`: the same 2 site(s): consumer in `onInvoice` (`src/report.listener.ts`), producer in `issue` (`src/invoice.service.ts`); the producer-consumer link is kept under the new name"),
            "{ev}"
        );
        // Deterministic: byte-identical over two runs.
        assert_eq!(text, diff(&repo));
    }

    /// A topic renamed at the producer only is no rename: the old channel loses its producer
    /// (link cut), the new one has no consumer.
    #[test]
    fn a_half_rename_is_a_cut_not_a_rename() {
        let repo = base();
        repo.write(
            "src/invoice.service.ts",
            &INVOICE_SERVICE.replace("'invoice.issued'", "'invoice.sent'"),
        );
        let ev = diff(&repo);
        let ev = events(&ev);
        assert!(
            ev.contains(" 0 renamed; producer-consumer links cut 1, created 0;"),
            "{ev}"
        );
        assert!(
            ev.contains("#### `inproc:invoice.issued` — link cut"),
            "{ev}"
        );
        // The hop now publishes on the new channel.
        assert!(
            ev.contains("#### Chain hops cut\n- consumer of `inproc:order.created` in `onOrderCreated` (`src/invoice.listener.ts`) → publishes `inproc:invoice.issued` in `issue` (`src/invoice.service.ts`) _(exact)_"),
            "{ev}"
        );
        assert!(
            ev.contains("#### Chain hops created\n- consumer of `inproc:order.created` in `onOrderCreated` (`src/invoice.listener.ts`) → publishes `inproc:invoice.sent` in `issue` (`src/invoice.service.ts`) _(exact)_"),
            "{ev}"
        );
    }

    /// Chain cut: the handler no longer calls the service that publishes. No site changes,
    /// the hop `order.created` -> `invoice.issued` is gone.
    #[test]
    fn a_handler_that_stops_calling_the_publisher_cuts_the_hop() {
        let repo = base();
        repo.write(
            "src/invoice.listener.ts",
            &INVOICE_LISTENER.replace(NO_CALL, ""),
        );
        let text = diff(&repo);
        let ev = events(&text);
        assert!(
            ev.contains("**Events**: 0 channel(s) with site changes; producers +0 -0; consumers +0 -0; 0 renamed; producer-consumer links cut 0, created 0; chain hops cut 1, created 0."),
            "{ev}"
        );
        assert!(
            ev.contains("#### Chain hops cut\n- consumer of `inproc:order.created` in `onOrderCreated` (`src/invoice.listener.ts`) → publishes `inproc:invoice.issued` in `issue` (`src/invoice.service.ts`) _(exact)_\n"),
            "{ev}"
        );
    }

    /// Chain created: the reverse of the cut.
    #[test]
    fn a_handler_that_starts_calling_the_publisher_creates_the_hop() {
        let repo = TempRepo::new();
        repo.write("src/order.service.ts", ORDER_SERVICE);
        repo.write(
            "src/invoice.listener.ts",
            &INVOICE_LISTENER.replace(NO_CALL, ""),
        );
        repo.write("src/invoice.service.ts", INVOICE_SERVICE);
        repo.write("src/report.listener.ts", REPORT_LISTENER);
        repo.commit("base");
        repo.git(&["checkout", "-q", "-b", "feature"]);
        repo.write("src/invoice.listener.ts", INVOICE_LISTENER);
        let ev = diff(&repo);
        let ev = events(&ev);
        assert!(ev.contains("chain hops cut 0, created 1."), "{ev}");
        assert!(
            ev.contains("#### Chain hops created\n- consumer of `inproc:order.created` in `onOrderCreated` (`src/invoice.listener.ts`) → publishes `inproc:invoice.issued` in `issue` (`src/invoice.service.ts`) _(exact)_\n"),
            "{ev}"
        );
        assert!(!ev.contains("#### Chain hops cut"), "{ev}");
    }

    /// No cross-broker bleed: a Kafka `orders` producer and an in-process `orders` consumer
    /// are two channels. Removing the in-process consumer is a row of `inproc:orders` only, and
    /// cuts no link (the Kafka producer never reached it).
    #[test]
    fn sites_of_two_brokers_on_one_name_stay_apart() {
        let repo = TempRepo::new();
        repo.write(
            "src/kafka.producer.ts",
            "import { Producer } from 'kafkajs';\n\nexport class OrderEvents {\n  constructor(private readonly producer: Producer) {}\n\n  async placed(id: string) {\n    await this.producer.send({ topic: 'orders', messages: [{ value: id }] });\n  }\n}\n",
        );
        repo.write(
            "src/orders.listener.ts",
            "import { Injectable } from '@nestjs/common';\nimport { OnEvent } from '@nestjs/event-emitter';\n\n@Injectable()\nexport class OrdersListener {\n  @OnEvent('orders')\n  onOrders(e: { id: string }) {\n    return e.id;\n  }\n}\n",
        );
        repo.commit("base");
        repo.git(&["checkout", "-q", "-b", "feature"]);
        rm(&repo, "src/orders.listener.ts");
        let text = diff(&repo);
        let ev = events(&text);
        assert!(
            ev.contains("#### `inproc:orders`\n- - consumer of `inproc:orders` in `onOrders` (`src/orders.listener.ts, base L7`)\n"),
            "{ev}"
        );
        assert!(
            ev.contains("producer-consumer links cut 0, created 0;"),
            "{ev}"
        );
        assert!(!ev.contains("kafka:orders"), "{ev}");
        // Every channel row carries its broker.
        for line in ev
            .lines()
            .filter(|l| l.starts_with("- + ") || l.starts_with("- - "))
        {
            assert!(
                line.contains("`inproc:") || line.contains("`kafka:"),
                "{line}"
            );
        }
    }

    /// A rename is per broker: the Kafka sites of `orders` move to `orders2` while a RabbitMQ
    /// queue keeps the name `orders`. The Kafka channel is gone (the RabbitMQ site is not on it),
    /// so it is a rename, and the RabbitMQ channel is untouched.
    #[test]
    fn a_rename_on_one_broker_ignores_the_same_name_on_another() {
        let kafka_producer = |t: &str| {
            format!("import {{ Producer }} from 'kafkajs';\n\nexport class OrderEvents {{\n  constructor(private readonly producer: Producer) {{}}\n\n  async placed(id: string) {{\n    await this.producer.send({{ topic: '{t}', messages: [{{ value: id }}] }});\n  }}\n}}\n")
        };
        let kafka_consumer = |t: &str| {
            format!("import {{ Consumer }} from 'kafkajs';\n\nexport class OrderConsumer {{\n  constructor(private readonly consumer: Consumer) {{}}\n\n  async start() {{\n    await this.consumer.subscribe({{ topic: '{t}' }});\n  }}\n}}\n")
        };
        let repo = TempRepo::new();
        repo.write("src/kafka.producer.ts", &kafka_producer("orders"));
        repo.write("src/kafka.consumer.ts", &kafka_consumer("orders"));
        repo.write(
            "src/rabbit.ts",
            "import amqp from 'amqplib';\n\nexport class Rabbit {\n  async send(ch: amqp.Channel, id: string) {\n    ch.sendToQueue('orders', Buffer.from(id));\n  }\n}\n",
        );
        repo.commit("base");
        repo.git(&["checkout", "-q", "-b", "feature"]);
        repo.write("src/kafka.producer.ts", &kafka_producer("orders2"));
        repo.write("src/kafka.consumer.ts", &kafka_consumer("orders2"));
        let text = diff(&repo);
        let ev = events(&text);
        assert!(
            ev.contains("- renamed `kafka:orders` → `kafka:orders2`: the same 2 site(s)"),
            "{ev}"
        );
        assert!(!ev.contains("rabbitmq:"), "{ev}");
    }

    /// Secrets: a key-shaped topic name (built at run time, never written in the source) and a
    /// placeholder default are never shown.
    #[test]
    fn secret_shaped_topics_and_placeholder_defaults_never_reach_the_output() {
        let token: String = ["tok", "_", "9f8e7d6c5b4a3f2e1d"].concat();
        let default: String = ["a3f9", "c1e7", "b2d4x9"].concat();
        let repo = base();
        repo.write(
            "src/leak.service.ts",
            &ORDER_SERVICE
                .replace("OrderService", "LeakService")
                .replace("'order.created'", &format!("'{token}'")),
        );
        repo.write(
            "src/main/java/demo/Listener.java",
            &format!("package demo;\n\nimport org.springframework.kafka.annotation.KafkaListener;\n\npublic class Listener {{\n    @KafkaListener(topics = \"${{orders.topic:{default}}}\")\n    public void on(String m) {{}}\n}}\n"),
        );
        let text = diff(&repo);
        assert!(!text.contains(&token), "{text}");
        assert!(!text.contains(&default), "{text}");
        // The listener is seen, its placeholder shown without its default.
        assert!(text.contains("consumes topic `${orders.topic}`"), "{text}");
        let ev = events(&text);
        assert!(ev.contains(super::REDACTED), "{ev}");
    }

    /// Hostile input, on a 2 MB stack: a 380 KB file of thousands of distinct topics and a cyclic
    /// chain (`ping` -> `pong` -> `ping`). The walk ends, the answer stays under the cap and says
    /// what it left out.
    #[test]
    fn thousands_of_topics_and_a_cycle_stay_bounded() {
        let repo = TempRepo::new();
        let cycle = |me: &str, on: &str, emit: &str| {
            format!("import {{ Injectable }} from '@nestjs/common';\nimport {{ OnEvent, EventEmitter2 }} from '@nestjs/event-emitter';\n\n@Injectable()\nexport class {me} {{\n  constructor(private readonly emitter: EventEmitter2) {{}}\n\n  @OnEvent('{on}')\n  handle() {{\n    this.relay();\n  }}\n\n  relay() {{\n    this.emitter.emit('{emit}', {{}});\n  }}\n}}\n")
        };
        repo.write("src/ping.ts", &cycle("Ping", "ping", "pong"));
        repo.write("src/pong.ts", &cycle("Pong", "pong", "ping"));
        repo.commit("base");
        repo.git(&["checkout", "-q", "-b", "feature"]);
        let mut big = String::from("import { Injectable } from '@nestjs/common';\nimport { EventEmitter2 } from '@nestjs/event-emitter';\n\n@Injectable()\nexport class Flood {\n  constructor(private readonly emitter: EventEmitter2) {}\n\n  fire() {\n");
        let mut k = 0;
        while big.len() < 375 * 1024 {
            let _ = std::fmt::Write::write_fmt(
                &mut big,
                format_args!("    this.emitter.emit('flood.topic.number.{k:05}', {{}});\n"),
            );
            k += 1;
        }
        big.push_str("  }\n}\n");
        assert!(big.len() < 384 * 1024);
        repo.write("src/flood.ts", &big);
        // Cut the cycle at one end too.
        repo.write("src/pong.ts", &cycle("Pong", "pong", "pang"));
        let root = repo.root.clone();
        let runner = repo.runner();
        let text = std::thread::Builder::new()
            .stack_size(2 * 1024 * 1024)
            .spawn(move || {
                let args = DiffMeshArgs {
                    base: Some("main".into()),
                    scope: None,
                    _meta: None,
                };
                let state = state(&root);
                let a = run_with(&args, &state, &runner, Limits::DEFAULT)
                    .expect("ok")
                    .text;
                let b = run_with(&args, &state, &runner, Limits::DEFAULT)
                    .expect("ok")
                    .text;
                assert_eq!(a, b);
                a
            })
            .expect("spawn")
            .join()
            .expect("join");
        assert!(text.len() <= 48 * 1024, "{}", text.len());
        let ev = events(&text);
        assert!(ev.contains("more channel(s) not checked for links and chain hops: cap (200 per call); narrow `scope`."), "{ev}");
        assert!(
            ev.contains("more event item(s) not shown to stay under the 48 KB cap; narrow `scope`"),
            "{ev}"
        );
        drop(repo);
    }
}
