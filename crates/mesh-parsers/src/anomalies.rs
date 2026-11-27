//! Markdown rendering of the event anomaly report (`event_anomalies`, 9.0 ticket T9).

use crate::markdown::MarkdownFormatter;
use mesh_core::{
    hub_topic, payload_json_family, AnomalySite, ContractNode, CycleEntry, EdgeConfidence,
    EventAnomalies, FanOutProducer, FieldGap, HubRef, Missing, NotCompared, PayloadEntry,
    PayloadSide, PayloadSkipReason, UnresolvedEntry, UnresolvedReason, ANOMALY_FAN_OUT_CONSUMERS,
    ANOMALY_FAN_OUT_TOPICS, ANOMALY_MAX_CYCLE_SITES, MAX_PAYLOAD_SHAPES_PER_TOPIC,
};
use std::fmt::Write as _;
use std::path::PathBuf;

/// Which classes the caller asked for.
#[derive(Debug, Clone, Copy)]
pub struct AnomalyShow {
    pub published_never_consumed: bool,
    pub consumed_never_published: bool,
    pub unresolved: bool,
    pub cycles: bool,
    pub fan_out: bool,
    pub test_only_consumers: bool,
    pub payload_mismatch: bool,
}

impl AnomalyShow {
    pub const ALL: AnomalyShow = AnomalyShow {
        published_never_consumed: true,
        consumed_never_published: true,
        unresolved: true,
        cycles: true,
        fan_out: true,
        test_only_consumers: true,
        payload_mismatch: true,
    };
}

/// How one page of the report is shown.
#[derive(Debug, Clone)]
pub struct AnomalyView<'a> {
    pub show: AnomalyShow,
    /// Rows per class on this page.
    pub limit: usize,
    /// Rows skipped per class.
    pub offset: usize,
    /// The scope the rows were filtered to, as the caller wrote it.
    pub scope: Option<&'a str>,
    /// What the index did not read (skipped files), when any: a missing producer may be there.
    pub coverage: Option<String>,
    /// Number of workspace roots indexed.
    pub roots: usize,
}

/// Payload budget of one answer past which a section stops adding rows (the first row of a
/// section is always shown); the central 48 KB cap stays a backstop.
const BUDGET: usize = 32 * 1024;
/// Longest row in characters: a row is bounded by construction, this is the last resort.
const ROW_MAX: usize = 1_500;
/// Hops of a cycle written out before `+K more hops`.
const CYCLE_HOPS_SHOWN: usize = 4;
/// Sites named per row before `+K more`.
const SITES_PER_ROW: usize = 3;
/// Topics named per cycle / producer row before `+K more`.
const NAMES_PER_ROW: usize = 6;
const REDACTED: &str = "[REDACTED_SECRET: USE_ENV_OR_LOCAL_FALLBACK]";

/// A topic key that is safe to print: a placeholder default that is not a plain name is
/// dropped, and a key that still is not a printable name (`member_consts::name_is_printable`:
/// the shared name gate without its new-topic randomness rule, so `orders.shard00123` prints;
/// a token, a URL, a weak password) is replaced by the redaction marker (commandment 5,
/// defense in depth: the extractors already refuse most such names).
fn shown_key(key: &str) -> String {
    let masked = mesh_core::config_values::mask_placeholder_defaults(key);
    let probe: String = masked
        .chars()
        .filter(|c| !matches!(c, '$' | '{' | '}'))
        .collect();
    if mesh_core::member_consts::name_is_printable(&probe) {
        masked.replace(['`', '\n', '\r'], "'")
    } else {
        REDACTED.to_string()
    }
}

/// A type or field name that is safe to print (the shared name gate), else the redaction marker.
fn shown_name(name: &str) -> String {
    if mesh_core::member_consts::name_is_printable(name)
        && !mesh_core::member_consts::identifier_is_credential_named(name)
    {
        name.replace(['`', '\n', '\r'], "'")
    } else {
        REDACTED.to_string()
    }
}

/// `path:line` of a line in a file, relative to its root.
fn file_line(
    file: &std::path::Path,
    repo_id: mesh_core::RepoId,
    line: u32,
    roots: &[PathBuf],
) -> String {
    match roots
        .get(repo_id as usize)
        .and_then(|root| file.strip_prefix(root).ok())
    {
        Some(rel) => format!("{}:{line}", MarkdownFormatter::slash_path(rel)),
        None => format!("{}:{line}", file.display()),
    }
}

fn confidence(c: EdgeConfidence) -> &'static str {
    match c {
        EdgeConfidence::Exact => "exact",
        EdgeConfidence::Heuristic => "heuristic",
        EdgeConfidence::Ambiguous => "ambiguous",
    }
}

/// `` `key` `` and, when known, the broker: `` `orders` [kafka] ``.
fn topic_label(key: &str, broker: Option<mesh_core::Broker>) -> String {
    let b = broker.map_or("broker unknown", mesh_core::Broker::slug);
    format!("`{}` [{b}]", shown_key(key))
}

fn hub_label(h: &HubRef<'_>) -> String {
    topic_label(hub_topic(h.hub).0, h.broker)
}

fn site_list(sites: &[&ContractNode], roots: &[PathBuf]) -> String {
    let mut out: Vec<String> = sites
        .iter()
        .take(SITES_PER_ROW)
        .map(|n| format!("`{}`", MarkdownFormatter::location(n, roots)))
        .collect();
    if sites.len() > SITES_PER_ROW {
        out.push(format!("+{} more", sites.len() - SITES_PER_ROW));
    }
    out.join(", ")
}

fn nodes_of<'a>(sites: &'a [AnomalySite<'a>]) -> Vec<&'a ContractNode> {
    sites.iter().map(|s| s.node).collect()
}

fn names_list(hubs: &[HubRef<'_>]) -> String {
    let mut out: Vec<String> = hubs.iter().take(NAMES_PER_ROW).map(hub_label).collect();
    if hubs.len() > NAMES_PER_ROW {
        out.push(format!("+{} more", hubs.len() - NAMES_PER_ROW));
    }
    out.join(", ")
}

impl MarkdownFormatter {
    /// One page of the anomaly report.
    pub fn format_event_anomalies(
        report: &EventAnomalies<'_>,
        roots: &[PathBuf],
        view: &AnomalyView<'_>,
    ) -> String {
        let fan_total = report.fan_out_topics.len() + report.fan_out_producers.len();
        let mut out = String::from("## Event anomalies\n");
        let scope = view.scope.map_or("the whole workspace".to_string(), |s| {
            format!("`{}`", s.replace(['`', '\n', '\r'], "'"))
        });
        let _ = writeln!(
            out,
            "*Read {} topic(s) over {} root(s): {} producer site(s), {} consumer site(s); rows shown for {scope}. Test files do not count as production sites.*\n",
            report.topics, view.roots, report.producer_sites, report.consumer_sites
        );
        out.push_str("| Class | Rows |\n| :--- | ---: |\n");
        for (name, n) in [
            (
                "published, never consumed",
                report.published_never_consumed.len().to_string(),
            ),
            (
                "consumed, never published",
                report.consumed_never_published.len().to_string(),
            ),
            (
                "unresolved (not anomalies)",
                report.unresolved.len().to_string(),
            ),
            (
                "cycles",
                if report.cycle_first_unsearched.is_some() {
                    format!("{}+ (search incomplete)", report.cycles.len())
                } else {
                    report.cycles.len().to_string()
                },
            ),
            ("fan-out", fan_total.to_string()),
            (
                "test-only consumers",
                report.test_only_consumers.len().to_string(),
            ),
            (
                "payload mismatches",
                report.payload.mismatches.len().to_string(),
            ),
        ] {
            let _ = writeln!(out, "| {name} | {n} |");
        }
        out.push('\n');

        if view.show.published_never_consumed {
            Self::anomaly_section(
                &mut out,
                "Published, never consumed",
                "Producers resolved, no consumer resolved in any indexed root.",
                &report.published_never_consumed,
                view,
                |e| {
                    format!(
                        "{} {}: {} producer(s) at {}",
                        topic_label(&e.key, e.broker),
                        confidence(e.confidence),
                        e.producers.len(),
                        site_list(&nodes_of(&e.producers), roots)
                    )
                },
            );
        }
        if view.show.consumed_never_published {
            Self::anomaly_section(
                &mut out,
                "Consumed, never published",
                "Consumers resolved, no producer resolved in any indexed root (a producer seen only in test files is named).",
                &report.consumed_never_published,
                view,
                |e| {
                    let tests = e.producers.iter().filter(|p| p.test).count();
                    let only_tests = if tests > 0 {
                        format!("; published only from {tests} test file site(s)")
                    } else {
                        String::new()
                    };
                    format!(
                        "{} {}: {} consumer(s) at {}{only_tests}",
                        topic_label(&e.key, e.broker),
                        confidence(e.confidence),
                        e.consumers.len(),
                        site_list(&nodes_of(&e.consumers), roots)
                    )
                },
            );
        }
        if view.show.unresolved {
            Self::anomaly_section(
                &mut out,
                "Unresolved (not anomalies)",
                "One-sided topics whose other end may exist: the name is not statically known, or the channel is fed or drained outside the code. Never read these as proof of absence.",
                &report.unresolved,
                view,
                |u: &UnresolvedEntry<'_>| {
                    let (side, sites) = match u.missing {
                        Missing::Producer => ("consumer(s)", &u.topic.consumers),
                        Missing::Consumer => ("producer(s)", &u.topic.producers),
                    };
                    let why = match (u.reason, u.missing) {
                        (UnresolvedReason::Placeholder, _) => {
                            "the name is a `${KEY}` placeholder no configuration file resolved, so it may be the same as another topic or not exist as written"
                        }
                        (UnresolvedReason::Wildcard, _) => {
                            "wildcard subscription: only statically named subjects were matched, the producers of the others may be built at runtime"
                        }
                        (UnresolvedReason::DeadLetter, Missing::Consumer) => {
                            "dead-letter or retry channel: usually drained by operations or by the framework"
                        }
                        (UnresolvedReason::DeadLetter, Missing::Producer) => {
                            "dead-letter or retry channel: usually fed by the framework, not by application code"
                        }
                    };
                    format!(
                        "{} {}: {} {side} at {}; {why}",
                        topic_label(&u.topic.key, u.topic.broker),
                        confidence(u.topic.confidence),
                        sites.len(),
                        site_list(&nodes_of(sites), roots)
                    )
                },
            );
        }
        if view.show.cycles {
            Self::cycle_section(&mut out, report, view, roots);
        }
        if view.show.fan_out {
            Self::anomaly_section(
                &mut out,
                &format!("Fan-out: topics with {ANOMALY_FAN_OUT_CONSUMERS} or more consumer sites"),
                "Many consumers on one topic: a change to its payload reaches all of them.",
                &report.fan_out_topics,
                view,
                |e| {
                    format!(
                        "{} {}: {} consumer(s) at {}",
                        topic_label(&e.key, e.broker),
                        confidence(e.confidence),
                        e.consumers.len(),
                        site_list(&nodes_of(&e.consumers), roots)
                    )
                },
            );
            Self::anomaly_section(
                &mut out,
                &format!("Fan-out: files publishing onto {ANOMALY_FAN_OUT_TOPICS} or more topics"),
                "One file is the producer of many topics (a god publisher, an outbox relay).",
                &report.fan_out_producers,
                view,
                |p: &FanOutProducer<'_>| {
                    let file = p.sites.first().map_or_else(
                        || p.file.display().to_string(),
                        |n| MarkdownFormatter::location(n, roots),
                    );
                    let file = file.rsplit_once(':').map_or(file.as_str(), |(f, _)| f);
                    format!(
                        "`{file}`: {} topic(s) ({}), {} producer site(s)",
                        p.topics.len(),
                        names_list(&p.topics),
                        p.sites.len()
                    )
                },
            );
        }
        if view.show.test_only_consumers {
            Self::anomaly_section(
                &mut out,
                "Test-only consumers",
                "Every consumer site of the topic is in a test file: nothing in production listens.",
                &report.test_only_consumers,
                view,
                |e| {
                    let prod = e.producers.iter().filter(|p| !p.test).count();
                    let side = if e.producers.is_empty() {
                        "no producer resolved".to_string()
                    } else {
                        format!("{prod} production producer(s)")
                    };
                    format!(
                        "{} {}: {} test consumer(s) at {}; {side}",
                        topic_label(&e.key, e.broker),
                        confidence(e.confidence),
                        e.consumers.len(),
                        site_list(&nodes_of(&e.consumers), roots)
                    )
                },
            );
        }

        if view.show.payload_mismatch {
            Self::payload_section(&mut out, report, view, roots);
        }

        out.push_str("### Reading this report\n");
        out.push_str("- \"No consumer\" and \"no producer\" mean none was statically resolved in the indexed roots. A topic name built at runtime, a registry, a generated client or a root that is not indexed can hide the other end: confirm a claim of absence with a search across every root before acting on it.\n");
        out.push_str("- Topics are per broker: two brokers using one name are two rows and are never joined. A site whose broker is unknown joins every channel of its name and is shown `heuristic`.\n");
        out.push_str("- `heuristic` and `ambiguous` rows rest on a name match, a declarative pattern, an unresolved call or a same-file guess: check the `path:line` before trusting them.\n");
        let _ = writeln!(
            out,
            "- Thresholds are constants (`ANOMALY_FAN_OUT_CONSUMERS` = {ANOMALY_FAN_OUT_CONSUMERS}, `ANOMALY_FAN_OUT_TOPICS` = {ANOMALY_FAN_OUT_TOPICS}). Cycles follow resolved call paths from a consumer to a publish, so a language or a call the resolver cannot follow shows no cycle ({} same-file-only hop(s) were not followed); a `${{KEY}}` placeholder topic is never part of one ({} placeholder topic(s) skipped).",
            report.cycle_guess_hops, report.placeholder_topics
        );
        if let Some(first) = report.cycle_first_unsearched {
            let _ = writeln!(
                out,
                "- **Cycle search incomplete**: {} of {} consumer sites were walked ({ANOMALY_MAX_CYCLE_SITES} is the limit), in path order; the sites from `{}` on were not searched, so cycles through them are missing and the cycle count is a lower bound. Narrow with `scope` to search a smaller part.",
                report.cycle_sites_walked,
                report.cycle_sites_total,
                MarkdownFormatter::location(first, roots)
            );
        }
        let unknown = report
            .published_never_consumed
            .iter()
            .chain(&report.consumed_never_published)
            .chain(report.unresolved.iter().map(|u| &u.topic))
            .chain(&report.fan_out_topics)
            .chain(&report.test_only_consumers)
            .filter(|e| e.broker.is_none())
            .count()
            + report
                .cycles
                .iter()
                .filter(|c| c.members.iter().any(|m| m.broker.is_none()))
                .count();
        if unknown > 0 {
            let _ = writeln!(
                out,
                "- {unknown} row(s) rest on sites whose broker is unknown (a decorator-only match such as a Python `@task` or a NestJS `@EventPattern`, a declarative pattern): they are shown `heuristic` and may be false; verify each with `rg` before acting."
            );
        }
        if view.show.payload_mismatch {
            let p = &report.payload;
            let not_typed = p
                .skipped
                .iter()
                .filter(|s| s.reason == PayloadSkipReason::NotTyped)
                .count();
            let too_many = p.skipped.len() - not_typed;
            let _ = writeln!(
                out,
                "- **Payloads**: {} topic(s) have live producers and consumers; {} mismatch, {} match ({} of them compared one way only), {} not compared because no producer or no consumer payload is statically typed, {} not compared because a side has more than {MAX_PAYLOAD_SHAPES_PER_TOPIC} payload types. {} site(s) have a payload that is not statically typed ({} name a type no indexed file declares, {} a type several files declare). Not compared is not \"no mismatch\". Only TypeScript / JavaScript and Java payloads are read; a consumer's expected fields are its declared type's fields, not the fields its code reads.",
                p.topics,
                p.mismatches.len(),
                p.matching,
                p.matching_partial,
                not_typed,
                too_many,
                p.untyped_sites,
                p.undeclared_sites,
                p.ambiguous_sites
            );
            if !p.skipped.is_empty() {
                let mut names: Vec<String> = p
                    .skipped
                    .iter()
                    .take(NAMES_PER_ROW)
                    .map(|s| topic_label(&s.topic.key, s.topic.broker))
                    .collect();
                if p.skipped.len() > NAMES_PER_ROW {
                    names.push(format!("+{} more", p.skipped.len() - NAMES_PER_ROW));
                }
                let _ = writeln!(out, "- Payloads not compared: {}.", names.join(", "));
            }
            if p.capped_files > 0 {
                let _ = writeln!(
                    out,
                    "- **Payload facts incomplete**: {} file(s) hold more payload sites or types than the per-file caps (`MAX_PAYLOAD_SITES_PER_FILE`, `MAX_PAYLOAD_TYPES_PER_FILE`); sites past the cap read as not statically typed.",
                    p.capped_files
                );
            }
        }
        if let Some(note) = &view.coverage {
            let _ = writeln!(out, "- **Coverage**: {note}");
        }
        out
    }

    /// One class: heading, rows `offset..offset + limit` within the payload budget, and the
    /// footer that says how to see the rest.
    fn anomaly_section<T>(
        out: &mut String,
        title: &str,
        blurb: &str,
        rows: &[T],
        view: &AnomalyView<'_>,
        render: impl Fn(&T) -> String,
    ) {
        if rows.is_empty() {
            return;
        }
        let _ = writeln!(out, "### {title} ({})\n*{blurb}*", rows.len());
        let mut shown = 0;
        for row in rows.iter().skip(view.offset).take(view.limit) {
            let mut line = format!("- {}\n", render(row));
            if line.chars().count() > ROW_MAX {
                line = format!("{}\n", Self::clamp_chars(line.trim_end(), ROW_MAX));
            }
            // The first row of a page is always shown, so a next-page pointer always advances.
            if shown > 0 && out.len() + line.len() > BUDGET {
                break;
            }
            out.push_str(&line);
            shown += 1;
        }
        let next = view.offset + shown;
        if shown == 0 {
            let _ = writeln!(
                out,
                "\n*No row at `offset: {}` (only {}).*",
                view.offset,
                rows.len()
            );
        } else if next < rows.len() {
            let _ = writeln!(
                out,
                "\n*Rows {}-{next} of {}. Next page: `offset: {next}`; or narrow with `scope` / `classes`.*",
                view.offset + 1,
                rows.len()
            );
        }
        out.push('\n');
    }

    /// The publisher-versus-consumer payload check (9.0 T11).
    fn payload_section(
        out: &mut String,
        report: &EventAnomalies<'_>,
        view: &AnomalyView<'_>,
        roots: &[PathBuf],
    ) {
        let render = |e: &PayloadEntry<'_>| {
            let side = |s: &PayloadSide<'_>| {
                let name = s.type_name.map_or("inline object".to_string(), |n| {
                    format!("`{}`", shown_name(n))
                });
                let more = if s.sites > 1 {
                    format!(" (+{} more site(s))", s.sites - 1)
                } else {
                    String::new()
                };
                format!(
                    "{name} ({}) at `{}`{more}",
                    file_line(s.file, s.repo_id, s.line, roots),
                    MarkdownFormatter::location(s.site, roots)
                )
            };
            let sides = |v: &[PayloadSide<'_>]| -> String {
                let mut out: Vec<String> = v.iter().take(SITES_PER_ROW).map(side).collect();
                if v.len() > SITES_PER_ROW {
                    out.push(format!("+{} more", v.len() - SITES_PER_ROW));
                }
                out.join("; ")
            };
            let mut text = format!(
                "{} {}: producer {} / consumer {}",
                topic_label(&e.topic.key, e.topic.broker),
                confidence(e.topic.confidence),
                sides(&e.producers),
                sides(&e.consumers)
            );
            let gap_list = |gaps: &[FieldGap<'_>], v: &[PayloadSide<'_>]| -> String {
                let mut out: Vec<String> = gaps
                    .iter()
                    .take(NAMES_PER_ROW)
                    .map(|g| {
                        let at = v.get(g.side).map_or(String::new(), |s| {
                            format!(" ({})", file_line(s.file, s.repo_id, g.field.line, roots))
                        });
                        format!("`{}`{at}", shown_name(&g.field.name))
                    })
                    .collect();
                if gaps.len() > NAMES_PER_ROW {
                    out.push(format!("+{} more", gaps.len() - NAMES_PER_ROW));
                }
                out.join(", ")
            };
            if !e.consumer_only.is_empty() {
                let _ = write!(
                    text,
                    "\n  - consumer expects, no producer sends: {}",
                    gap_list(&e.consumer_only, &e.consumers)
                );
            }
            if !e.producer_only.is_empty() {
                let _ = write!(
                    text,
                    "\n  - published, no consumer expects: {}",
                    gap_list(&e.producer_only, &e.producers)
                );
            }
            for c in e.clashes.iter().take(NAMES_PER_ROW) {
                let fam = |f: &mesh_core::PayloadField| {
                    f.ty.as_deref()
                        .and_then(payload_json_family)
                        .unwrap_or_default()
                };
                let at = |g: &FieldGap<'_>, v: &[PayloadSide<'_>]| {
                    v.get(g.side).map_or(String::new(), |s| {
                        file_line(s.file, s.repo_id, g.field.line, roots)
                    })
                };
                let _ = write!(
                    text,
                    "\n  - type differs: `{}` is {} in the producer ({}), {} in the consumer ({})",
                    shown_name(&c.producer.field.name),
                    fam(c.producer.field),
                    at(&c.producer, &e.producers),
                    fam(c.consumer.field),
                    at(&c.consumer, &e.consumers)
                );
            }
            if e.clashes.len() > NAMES_PER_ROW {
                let _ = write!(
                    text,
                    "\n  - +{} more type differences",
                    e.clashes.len() - NAMES_PER_ROW
                );
            }
            if e.names_differ {
                text.push_str("\n  - type names differ between producer and consumer");
            }
            // The "not compared" notes are what keeps a row honest: kept whole, the findings above
            // them are clamped instead when the row is too long.
            let mut notes = String::new();
            for (side, what, partial) in [
                ("consumer", "expected", e.consumer_only_partial),
                ("producer", "published", e.producer_only_partial),
            ] {
                if partial {
                    let _ = write!(
                        notes,
                        "\n  - partial list above: the {side}'s field list is partial (a cap, a spread, an extends), more {what} fields may be missing"
                    );
                }
            }
            for (what, skipped) in [
                ("fields a consumer expects", e.consumer_only_skipped),
                ("fields published", e.producer_only_skipped),
            ] {
                let why = match skipped {
                    None => continue,
                    Some(NotCompared::Untyped) => {
                        "the other side has a site whose payload is not statically typed"
                    }
                    Some(NotCompared::Incomplete) => {
                        "a field list of the other side is partial (a cap, a spread, an extends)"
                    }
                };
                let _ = write!(notes, "\n  - {what}: not compared, {why}");
            }
            if e.untyped_sites > 0 {
                let _ = write!(
                    notes,
                    "\n  - {} site(s) of this topic have a payload that is not statically typed",
                    e.untyped_sites
                );
            }
            // `- ` and the newline the section adds around a row.
            let room = ROW_MAX.saturating_sub(3 + notes.chars().count());
            if text.chars().count() > room {
                text = Self::clamp_chars(&text, room);
            }
            text.push_str(&notes);
            text
        };
        Self::anomaly_section(
            out,
            "Payload mismatches",
            "Producers and consumers of one topic (per broker) disagree on the payload type: fields a consumer expects that no producer sends, fields published that no consumer expects, primitive field types that differ. Only statically typed payloads are compared.",
            &report.payload.mismatches,
            view,
            render,
        );
    }

    fn cycle_section(
        out: &mut String,
        report: &EventAnomalies<'_>,
        view: &AnomalyView<'_>,
        roots: &[PathBuf],
    ) {
        let render = |c: &CycleEntry<'_>| {
            // Bounded however long the loop: a few topic names, a few hops, the rest counted.
            let mut names: Vec<String> = c
                .path
                .iter()
                .take(NAMES_PER_ROW)
                .map(|e| hub_label(&e.from))
                .collect();
            let hidden = c.path.len().saturating_sub(NAMES_PER_ROW);
            if hidden > 0 {
                names.push(format!("... (+{hidden} more topics)"));
            }
            if let Some(first) = c.path.first() {
                names.push(hub_label(&first.from));
            }
            let mut text = format!("{} {}", names.join(" -> "), confidence(c.confidence));
            if c.members.len() > c.path.len() {
                let _ = write!(
                    text,
                    " (loop of {} topics: {})",
                    c.members.len(),
                    names_list(&c.members)
                );
            }
            for e in c.path.iter().take(CYCLE_HOPS_SHOWN) {
                let _ = write!(
                    text,
                    "\n  - {} -> {}: consumed at `{}`, published at `{}` ({}, resolved call path)",
                    hub_label(&e.from),
                    hub_label(&e.to),
                    MarkdownFormatter::location(e.consumer, roots),
                    MarkdownFormatter::location(e.producer, roots),
                    confidence(e.confidence)
                );
            }
            if c.path.len() > CYCLE_HOPS_SHOWN {
                let _ = write!(text, "\n  - +{} more hops", c.path.len() - CYCLE_HOPS_SHOWN);
            }
            text
        };
        Self::anomaly_section(
            out,
            "Cycles",
            "Event chains that loop back: consuming a topic leads, through resolved calls, to publishing a topic that leads back. Retries and request/reply pairs are legitimate loops; check they terminate.",
            &report.cycles,
            view,
            render,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credential_shaped_keys_never_print() {
        // Built at runtime: a key-shaped literal in source would trip push protection.
        let token = format!("{}{}", "sk_", "live_4eC39HqLyjWDarjtT1zdp7dc");
        let hunter = format!("hunter{}-prod-x", 2);
        let url = format!("postgres://u:{}@h/x", "pw");
        for bad in [&token, &hunter, &url] {
            assert_eq!(shown_key(bad), REDACTED, "{bad}");
        }
        // A default that is not a plain name is dropped; a name-shaped weak secret redacts the key.
        assert_eq!(shown_key(&format!("${{K:{token}}}")), "${K}");
        assert_eq!(shown_key(&format!("${{K:{url}}}")), "${K}");
        assert_eq!(shown_key(&format!("${{K:{hunter}}}")), REDACTED);
        assert_eq!(shown_key("orders.created"), "orders.created");
        assert_eq!(
            shown_key("${ORDERS_TOPIC:orders}"),
            "${ORDERS_TOPIC:orders}"
        );
        assert_eq!(shown_key("orders.*"), "orders.*");
    }

    #[test]
    fn digit_heavy_names_print_and_tokens_do_not() {
        for name in [
            "big.t00005",
            "orders.shard00123",
            "events.p1234567",
            "orders.ab12cd",
            "user.id.v20260101",
        ] {
            assert_eq!(shown_key(name), name);
        }
        let aws = format!("{}{}", "AKIA", "IOSFODNN7EXAMPLE");
        let ghp = format!("{}{}", "ghp_", "R8c1qZ0xK3mP7vB2nT5yL9wD4sF6hJ0aG1eX");
        let run = format!("{}{}", "a1b2c3d4e5", "f6a7b8c9d0e1f2");
        for bad in [&aws, &ghp, &run] {
            assert_eq!(shown_key(bad), REDACTED, "{bad}");
        }
    }

    /// Whatever the rows weigh and however full the page already is, a page shows at least one
    /// row and its next-page pointer moves forward: paging always ends.
    #[test]
    fn a_section_over_budget_still_shows_a_row_and_its_pointer_advances() {
        let rows: Vec<String> = (0..5)
            .map(|i| format!("row{i} {}", "x".repeat(100_000)))
            .collect();
        let mut offset = 0;
        let mut pages = 0;
        loop {
            let view = AnomalyView {
                show: AnomalyShow::ALL,
                limit: 25,
                offset,
                scope: None,
                coverage: None,
                roots: 1,
            };
            let mut out = "y".repeat(BUDGET + 1_000);
            let before = out.len();
            MarkdownFormatter::anomaly_section(&mut out, "T", "b", &rows, &view, |r| r.clone());
            let page = &out[before..];
            assert!(page.contains(&format!("- row{offset} ")), "{offset}");
            assert!(page.len() < ROW_MAX + 400, "{} bytes", page.len());
            pages += 1;
            match page.split("Next page: `offset: ").nth(1) {
                Some(rest) => {
                    let next: usize = rest.split('`').next().unwrap().parse().unwrap();
                    assert!(
                        next > offset,
                        "pointer {next} does not advance from {offset}"
                    );
                    offset = next;
                }
                None => break,
            }
            assert!(pages <= 5);
        }
        assert_eq!(pages, 5);
    }

    #[test]
    fn a_capped_cycle_search_says_incomplete_not_zero() {
        use mesh_core::{CompactStr, NodeKind};
        let node = ContractNode {
            id: 1,
            name: CompactStr::new("site"),
            kind: NodeKind::ServiceClass,
            file_path: std::sync::Arc::from(std::path::Path::new("/w/src/z.ts")),
            line_start: 3,
            line_end: 3,
            package: CompactStr::new("app"),
            repo_id: 0,
            signature: None,
            docstring: None,
        };
        let report = EventAnomalies {
            cycle_sites_walked: 20_000,
            cycle_sites_total: 25_000,
            cycle_first_unsearched: Some(&node),
            ..EventAnomalies::default()
        };
        let view = AnomalyView {
            show: AnomalyShow::ALL,
            limit: 25,
            offset: 0,
            scope: None,
            coverage: None,
            roots: 1,
        };
        let text =
            MarkdownFormatter::format_event_anomalies(&report, &[PathBuf::from("/w")], &view);
        assert!(
            text.contains("| cycles | 0+ (search incomplete) |"),
            "{text}"
        );
        assert!(
            text.contains("20000 of 25000 consumer sites were walked"),
            "{text}"
        );
        assert!(
            text.contains("from `src/z.ts:3` on were not searched"),
            "{text}"
        );
        assert!(!text.contains("of more"), "{text}");
        // Complete search: a plain count and no such note.
        let done = EventAnomalies::default();
        let text = MarkdownFormatter::format_event_anomalies(&done, &[PathBuf::from("/w")], &view);
        assert!(
            text.contains("| cycles | 0 |") && !text.contains("incomplete"),
            "{text}"
        );
    }
}
