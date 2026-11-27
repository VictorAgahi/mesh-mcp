//! Markdown rendering of the event anomaly report (`event_anomalies`, 9.0 ticket T9).

use crate::markdown::MarkdownFormatter;
use mesh_core::{
    hub_topic, AnomalySite, ContractNode, CycleEntry, EdgeConfidence, EventAnomalies,
    FanOutProducer, HubRef, Missing, UnresolvedEntry, UnresolvedReason,
    ANOMALY_FAN_OUT_CONSUMERS, ANOMALY_FAN_OUT_TOPICS, ANOMALY_MAX_CYCLE_SITES,
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
}

impl AnomalyShow {
    pub const ALL: AnomalyShow = AnomalyShow {
        published_never_consumed: true,
        consumed_never_published: true,
        unresolved: true,
        cycles: true,
        fan_out: true,
        test_only_consumers: true,
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

/// Payload budget of one answer; the central 48 KB cap stays a backstop.
const BUDGET: usize = 40 * 1024;
/// Sites named per row before `+K more`.
const SITES_PER_ROW: usize = 3;
/// Topics named per cycle / producer row before `+K more`.
const NAMES_PER_ROW: usize = 6;
const REDACTED: &str = "[REDACTED_SECRET: USE_ENV_OR_LOCAL_FALLBACK]";

/// A topic key that is safe to print: a placeholder default that is not a plain name is
/// dropped, and a key that still is not a plain name (a token, a password, a random-looking
/// run) is replaced by the redaction marker (commandment 5, defense in depth: the extractors
/// already refuse such names).
fn shown_key(key: &str) -> String {
    let masked = mesh_core::config_values::mask_placeholder_defaults(key);
    let probe: String = masked.chars().filter(|c| !matches!(c, '$' | '{' | '}')).collect();
    if mesh_core::member_consts::constant_is_a_name("", &probe) {
        masked.replace(['`', '\n', '\r'], "'")
    } else {
        REDACTED.to_string()
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
            ("published, never consumed", report.published_never_consumed.len()),
            ("consumed, never published", report.consumed_never_published.len()),
            ("unresolved (not anomalies)", report.unresolved.len()),
            ("cycles", report.cycles.len()),
            ("fan-out", fan_total),
            ("test-only consumers", report.test_only_consumers.len()),
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

        out.push_str("### Reading this report\n");
        out.push_str("- \"No consumer\" and \"no producer\" mean none was statically resolved in the indexed roots. A topic name built at runtime, a registry, a generated client or a root that is not indexed can hide the other end: confirm a claim of absence with a search across every root before acting on it.\n");
        out.push_str("- Topics are per broker: two brokers using one name are two rows and are never joined. A site whose broker is unknown joins every channel of its name and is shown `heuristic`.\n");
        out.push_str("- `heuristic` and `ambiguous` rows rest on a name match, a declarative pattern, an unresolved call or a same-file guess: check the `path:line` before trusting them.\n");
        let _ = writeln!(
            out,
            "- Thresholds are constants (`ANOMALY_FAN_OUT_CONSUMERS` = {ANOMALY_FAN_OUT_CONSUMERS}, `ANOMALY_FAN_OUT_TOPICS` = {ANOMALY_FAN_OUT_TOPICS}). Cycles follow resolved call paths from a consumer to a publish, so a language or a call the resolver cannot follow shows no cycle ({} same-file-only hop(s) were not followed); a `${{KEY}}` placeholder topic is never part of one ({} placeholder topic(s) skipped).",
            report.cycle_guess_hops, report.placeholder_topics
        );
        if report.cycle_search_capped {
            let _ = writeln!(
                out,
                "- **Cycle search capped**: {} consumer sites walked of more ({ANOMALY_MAX_CYCLE_SITES} is the limit); cycles through the rest are not listed.",
                report.cycle_sites_walked
            );
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
        let mut next = view.offset;
        for row in rows.iter().skip(view.offset).take(view.limit) {
            let line = format!("- {}\n", render(row));
            if out.len() + line.len() > BUDGET {
                break;
            }
            out.push_str(&line);
            shown += 1;
            next += 1;
        }
        if next < rows.len() {
            let _ = writeln!(
                out,
                "\n*Rows {}-{} of {}. Next page: `offset: {next}`; or narrow with `scope` / `classes`.*",
                view.offset + 1,
                next,
                rows.len()
            );
        } else if shown == 0 {
            let _ = writeln!(out, "\n*No row at `offset: {}` (only {}).*", view.offset, rows.len());
        }
        out.push('\n');
    }

    fn cycle_section(
        out: &mut String,
        report: &EventAnomalies<'_>,
        view: &AnomalyView<'_>,
        roots: &[PathBuf],
    ) {
        let render = |c: &CycleEntry<'_>| {
            let mut names: Vec<String> = c.path.iter().map(|e| hub_label(&e.from)).collect();
            if let Some(first) = names.first().cloned() {
                names.push(first);
            }
            let mut text = format!(
                "{} {}",
                names.join(" -> "),
                confidence(c.confidence)
            );
            if c.members.len() > c.path.len() {
                let _ = write!(
                    text,
                    " (loop of {} topics: {})",
                    c.members.len(),
                    names_list(&c.members)
                );
            }
            for e in &c.path {
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
        assert_eq!(shown_key("${ORDERS_TOPIC:orders}"), "${ORDERS_TOPIC:orders}");
        assert_eq!(shown_key("orders.*"), "orders.*");
    }
}
