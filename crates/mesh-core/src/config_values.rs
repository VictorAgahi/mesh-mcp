//! Configuration values behind `${key}` topic placeholders (9.0, ticket T6).
//!
//! A producer or consumer whose topic is read from configuration is recorded by its
//! extractor as `${key}` or `${key:default}` (`mesh_parsers::config_ref`). This module holds
//! what the workspace's configuration files say about those keys and resolves a placeholder
//! against it when the file index is folded into the graph:
//!
//! - Spring `application*.yml` / `*.properties` (dotted keys), `.env*` files, a compose
//!   `environment:` block, a Kubernetes `env:` list;
//! - one value for a key is `exact`; two different values (profiles, `.env` against
//!   `.env.test`) make every candidate `weak`, and a value that only an example file
//!   (`.env.example`), a multi-document yaml or a `${key:default}` fallback supplies is `weak`
//!   too: the graph links a weak topic `heuristic`;
//! - a placeholder nothing resolves stays a visible `${key}` topic. A name is never guessed.
//!
//! Secrets (commandment 5): only entries whose key is not a secret key and whose value looks
//! like a name (no whitespace, bounded) are kept, and only a value a site actually
//! references ever reaches the graph. Every cap is counted (`IndexHealth`).

use crate::properties::PropertyRegistry;
use crate::types::CompactStr;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};

/// Entries kept per configuration file; more are dropped and the file is counted capped.
pub const MAX_ENTRIES_PER_FILE: usize = 2048;
/// Entries kept in the whole workspace.
pub const MAX_ENTRIES_TOTAL: usize = 50_000;
/// Longest value kept (Kafka's own topic-name limit is 249).
const MAX_VALUE_LEN: usize = 249;
/// Placeholders resolved through other placeholders (`a` -> `${b}` -> ...).
const MAX_RESOLVE_DEPTH: usize = 8;
/// Candidate topics one placeholder may expand to (profiles x nested keys).
const MAX_ALTERNATIVES: usize = 8;
/// Expansion steps one placeholder may take (each expanded text and each candidate counts).
pub const MAX_RESOLVE_STEPS: usize = 256;

/// One `key = value` a configuration file sets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigEntry {
    pub key: CompactStr,
    pub value: CompactStr,
    /// The file is only a template or a default (`.env.example`, a profile document of a
    /// multi-document yaml): the value is not known to be the deployed one.
    pub weak: bool,
}

/// What one configuration file contributes.
#[derive(Debug, Default, Clone)]
pub struct ConfigFile {
    pub entries: Vec<ConfigEntry>,
    /// The per-file cap dropped entries.
    pub capped: bool,
    /// The file did not parse as yaml: it contributes nothing it should have.
    pub failed: bool,
    /// The file is not valid UTF-8 and was read with the invalid bytes replaced.
    pub lossy: bool,
}

impl ConfigFile {
    /// Appends `other`'s entries, within the per-file cap.
    pub fn merge(&mut self, other: ConfigFile) {
        self.capped |= other.capped;
        self.failed |= other.failed;
        self.lossy |= other.lossy;
        for e in other.entries {
            if self.entries.len() >= MAX_ENTRIES_PER_FILE {
                self.capped = true;
                break;
            }
            self.entries.push(e);
        }
    }
}

/// A placeholder resolved: every candidate topic, and whether it is a weak link.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolved {
    pub topics: Vec<(String, bool)>,
}

/// Every configuration file's entries, indexed by key for resolution.
#[derive(Debug, Default, Clone)]
pub struct ConfigValues {
    by_file: BTreeMap<PathBuf, Vec<ConfigEntry>>,
    /// key -> distinct (value, weak). Rebuilt by [`Self::rebuild`].
    index: HashMap<CompactStr, Vec<(CompactStr, bool)>>,
    /// Files that did not parse. A profile file in the same directory is read as weak: the
    /// base file that failed may hold the value that is really active.
    failed: std::collections::BTreeSet<PathBuf>,
    /// Source files whose file index holds a `${...}` topic: re-folded when a config file
    /// changes, since their resolution may move without their own bytes changing.
    ref_files: HashSet<PathBuf>,
    /// Entries dropped by [`MAX_ENTRIES_TOTAL`] at the last rebuild.
    dropped_total: usize,
    memo: Memo,
}

impl ConfigValues {
    pub fn is_empty(&self) -> bool {
        self.by_file.is_empty()
    }

    /// Sets (or, with no entries, clears) what `path` contributes. Call [`Self::rebuild`]
    /// once after a batch.
    pub fn set_file(&mut self, path: &Path, entries: Vec<ConfigEntry>) {
        self.failed.remove(path);
        if entries.is_empty() {
            self.by_file.remove(path);
        } else {
            self.by_file.insert(path.to_path_buf(), entries);
        }
    }

    /// Records that `path` did not parse (call after [`Self::set_file`]).
    pub fn mark_failed(&mut self, path: &Path) {
        self.failed.insert(path.to_path_buf());
    }

    pub fn remove_files(&mut self, paths: &HashSet<&Path>) {
        self.by_file.retain(|p, _| !paths.contains(p.as_path()));
        self.failed.retain(|p| !paths.contains(p.as_path()));
        self.ref_files.retain(|p| !paths.contains(p.as_path()));
    }

    /// Records whether the source file at `path` holds a placeholder topic.
    pub fn note_ref_file(&mut self, path: &Path, has_placeholder: bool) {
        if has_placeholder {
            self.ref_files.insert(path.to_path_buf());
        } else {
            self.ref_files.remove(path);
        }
    }

    pub fn ref_files(&self) -> impl Iterator<Item = &Path> {
        self.ref_files.iter().map(PathBuf::as_path)
    }

    /// Entries dropped by the workspace-wide cap at the last rebuild.
    pub fn dropped_total(&self) -> usize {
        self.dropped_total
    }

    /// Rebuilds the key index from the per-file entries, in path order (deterministic).
    pub fn rebuild(&mut self) {
        self.index.clear();
        self.memo.clear();
        self.dropped_total = 0;
        let mut kept = 0usize;
        let failed_dirs: HashSet<&Path> = self.failed.iter().filter_map(|p| p.parent()).collect();
        for (path, entries) in &self.by_file {
            let sibling_failed = path.parent().is_some_and(|d| failed_dirs.contains(d));
            for e in entries {
                if kept >= MAX_ENTRIES_TOTAL {
                    self.dropped_total += 1;
                    continue;
                }
                kept += 1;
                let weak = e.weak || sibling_failed;
                let slot = self.index.entry(e.key.clone()).or_default();
                match slot.iter_mut().find(|(v, _)| *v == e.value) {
                    // The same value from a strong file is a strong value.
                    Some((_, w)) => *w &= weak,
                    None => slot.push((e.value.clone(), weak)),
                }
            }
        }
        // A template or a default is no evidence against a deployed value: when a strong
        // value exists for a key, the weak ones are dropped.
        for slot in self.index.values_mut() {
            if slot.iter().any(|(_, weak)| !weak) {
                slot.retain(|(_, weak)| !weak);
            }
        }
    }

    /// Canonical lines of the retained entries, for the snapshot fingerprint.
    pub fn canonical_lines_rooted(&self, roots: &[PathBuf]) -> Vec<String> {
        let mut lines: Vec<String> = self
            .by_file
            .iter()
            .flat_map(|(path, entries)| {
                let source = crate::state::root_relative_path(path, roots, None);
                entries.iter().map(move |e| {
                    serde_json::json!(["config", e.key.as_str(), e.value.as_str(), e.weak, source])
                        .to_string()
                })
            })
            .collect();
        lines.extend(self.failed.iter().map(|p| {
            serde_json::json!([
                "config-failed",
                crate::state::root_relative_path(p, roots, None)
            ])
            .to_string()
        }));
        lines.sort_unstable();
        lines
    }

    /// Resolves a topic holding `${key}` / `${key:default}` placeholders. `None` when any
    /// placeholder has neither a configured value nor a default, is cyclic, is nested too
    /// deep, or needs more than [`MAX_RESOLVE_STEPS`] steps / [`MAX_ALTERNATIVES`] candidates:
    /// the caller keeps the placeholder as the (visible) topic. Answers are memoized per
    /// topic until the next [`Self::rebuild`].
    pub fn resolve(&self, topic: &str) -> Option<Resolved> {
        if let Some(hit) = self.memo.get(topic) {
            return hit;
        }
        let (resolved, _) = self.resolve_counted(topic);
        self.memo.put(topic, &resolved);
        resolved
    }

    /// [`Self::resolve`] without the memo, and the work it took (expansion steps).
    pub fn resolve_counted(&self, topic: &str) -> (Option<Resolved>, usize) {
        let mut work = Work::default();
        let mut visiting: Vec<&str> = Vec::new();
        let topics = self.expand(topic, 0, &mut visiting, &mut work);
        let resolved = if work.over {
            None
        } else {
            topics.map(|topics| Resolved { topics })
        };
        (resolved, work.steps)
    }

    fn expand<'a>(
        &'a self,
        text: &'a str,
        depth: usize,
        visiting: &mut Vec<&'a str>,
        work: &mut Work,
    ) -> Option<Vec<(String, bool)>> {
        if depth > MAX_RESOLVE_DEPTH || !work.step() {
            return None;
        }
        let mut acc: Vec<(String, bool)> = vec![(String::new(), false)];
        let mut rest = text;
        while let Some(start) = rest.find("${") {
            for (s, _) in &mut acc {
                s.push_str(&rest[..start]);
            }
            let body = &rest[start + 2..];
            let end = body.find('}')?;
            let inner = &body[..end];
            rest = &body[end + 1..];
            let alternatives = self.expand_key(inner, depth, visiting, work)?;
            if acc.len().saturating_mul(alternatives.len()) > MAX_ALTERNATIVES {
                work.over = true;
                return None;
            }
            acc = acc
                .iter()
                .flat_map(|(prefix, weak)| {
                    alternatives
                        .iter()
                        .map(move |(v, w)| (format!("{prefix}{v}"), *weak || *w))
                })
                .collect();
        }
        for (s, _) in &mut acc {
            s.push_str(rest);
        }
        Some(acc)
    }

    /// The candidates of one `key` / `key:default` placeholder body. The work budget and the
    /// candidate cap are checked inside the loops, so a key with many values chained through
    /// many keys is abandoned as soon as it exceeds them.
    fn expand_key<'a>(
        &'a self,
        inner: &'a str,
        depth: usize,
        visiting: &mut Vec<&'a str>,
        work: &mut Work,
    ) -> Option<Vec<(String, bool)>> {
        let (key, default) = match inner.split_once(':') {
            Some((k, d)) => (k, Some(d)),
            None => (inner, None),
        };
        if inner.contains("${") || visiting.contains(&key) {
            return None; // a nested placeholder or a cycle
        }
        if let Some(values) = self.index.get(key) {
            let ambiguous = values.len() > 1;
            visiting.push(key);
            let mut out: Vec<(String, bool)> = Vec::new();
            for (value, weak) in values {
                let Some(expanded) = self.expand(value, depth + 1, visiting, work) else {
                    if work.over {
                        visiting.pop();
                        return None;
                    }
                    continue;
                };
                for (v, w) in expanded {
                    if !out.iter().any(|(o, _)| *o == v) {
                        if out.len() >= MAX_ALTERNATIVES || !work.step() {
                            work.over = true;
                            visiting.pop();
                            return None;
                        }
                        out.push((v, *weak || w || ambiguous));
                    }
                }
            }
            visiting.pop();
            // Only template values: the source's own default is as good a candidate.
            if let (Some(d), true) = (
                default.filter(|d| !d.is_empty()),
                out.iter().all(|(_, w)| *w),
            ) {
                if !out.iter().any(|(v, _)| v == d) {
                    out.push((d.to_string(), true));
                }
            }
            if !out.is_empty() {
                return Some(out);
            }
        }
        // No (usable) configured value: the source's own default, a weak link.
        default
            .filter(|d| !d.is_empty())
            .map(|d| vec![(d.to_string(), true)])
    }
}

/// Expansion budget of one [`ConfigValues::resolve`] call.
#[derive(Debug, Default)]
struct Work {
    steps: usize,
    /// The budget or the candidate cap was exceeded: abandon the whole expansion.
    over: bool,
}

impl Work {
    /// Counts one step; `false` (and `over`) once the budget is spent.
    fn step(&mut self) -> bool {
        self.steps += 1;
        if self.steps > MAX_RESOLVE_STEPS {
            self.over = true;
        }
        !self.over
    }
}

/// Answers of [`ConfigValues::resolve`] by topic, cleared on every rebuild. Shared between the
/// reader threads of a snapshot (hence the lock); a clone starts empty.
#[derive(Debug, Default)]
struct Memo(std::sync::Mutex<HashMap<String, Option<Resolved>>>);

impl Clone for Memo {
    fn clone(&self) -> Self {
        Self::default()
    }
}

impl Memo {
    /// Most memoized topics.
    const MAX: usize = 4096;

    fn get(&self, topic: &str) -> Option<Option<Resolved>> {
        self.0.lock().ok()?.get(topic).cloned()
    }

    fn put(&self, topic: &str, resolved: &Option<Resolved>) {
        if let Ok(mut m) = self.0.lock() {
            if m.len() < Self::MAX {
                m.insert(topic.to_string(), resolved.clone());
            }
        }
    }

    fn clear(&mut self) {
        if let Ok(m) = self.0.get_mut() {
            m.clear();
        }
    }
}

// ---- extraction of entries from configuration files ------------------------------------------

/// A value worth keeping as a topic candidate (commandment 5: only a topic-shaped value is
/// ever retained, so nothing else a config file holds can reach the graph): a bounded token of
/// ASCII name characters and `${key:default}` placeholder syntax, with no URL scheme, no
/// userinfo (`@`), and nothing that looks like a credential token.
fn name_shaped(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_VALUE_LEN
        && value.chars().all(|c| {
            c.is_ascii_alphanumeric()
                || matches!(
                    c,
                    '.' | '_' | '-' | ':' | '/' | '$' | '{' | '}' | '#' | '*' | '+'
                )
        })
        && !value.contains("://")
        && !looks_like_token(value)
}

/// Well-known credential prefixes, a long hex run, or a long mixed-case / digit run with no
/// separator (a base64-ish key): never a topic name.
fn looks_like_token(value: &str) -> bool {
    const PREFIXES: &[&str] = &[
        "sk_live_",
        "sk_test_",
        "rk_live_",
        "pk_live_",
        "ghp_",
        "gho_",
        "ghu_",
        "ghs_",
        "ghr_",
        "github_pat_",
        "xoxb-",
        "xoxp-",
        "xoxa-",
        "akia",
        "asia",
        "aiza",
        "eyj",
    ];
    let lower = value.to_ascii_lowercase();
    if PREFIXES.iter().any(|p| lower.starts_with(p)) {
        return true;
    }
    let compact = value.len() >= 24
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '=');
    let hex = value.len() >= 32 && value.chars().all(|c| c.is_ascii_hexdigit());
    hex || (compact
        && value.chars().any(|c| c.is_ascii_digit())
        && value.chars().any(|c| c.is_ascii_alphabetic()))
}

/// A key that names a credential, a connection string or a secret: never kept, whatever the
/// value looks like. Stricter than `PropertyRegistry::is_sensitive_key` on purpose.
fn secret_key(key: &str) -> bool {
    let lower = key.to_ascii_lowercase();
    const SUFFIXES: &[&str] = &[
        "_key",
        "-key",
        ".key",
        "_dsn",
        "-dsn",
        ".dsn",
        "_url",
        "-url",
        ".url",
        "_uri",
        "-uri",
        ".uri",
        "_password",
        "_passwd",
        "_pwd",
        "-password",
        ".password",
        "_credentials",
        "-credentials",
        ".credentials",
        "_auth",
        "-auth",
        ".auth",
    ];
    const PARTS: &[&str] = &[
        "dsn",
        "stripe",
        "secret",
        "token",
        "password",
        "passwd",
        "credential",
        "private",
        "passphrase",
        "apikey",
        "api_key",
        "api-key",
        "jwt",
        "cert",
    ];
    lower == "key"
        || SUFFIXES.iter().any(|x| lower.ends_with(x))
        || PARTS.iter().any(|x| lower.contains(x))
}

fn push_entry(
    file: &mut ConfigFile,
    secrets: &PropertyRegistry,
    key: &str,
    value: &str,
    weak: bool,
) {
    if key.is_empty()
        || key.len() > 200
        || !name_shaped(value)
        || secret_key(key)
        || secrets.is_sensitive_key(key)
    {
        return;
    }
    if file.entries.len() >= MAX_ENTRIES_PER_FILE {
        file.capped = true;
        return;
    }
    file.entries.push(ConfigEntry {
        key: CompactStr::new(key),
        value: CompactStr::new(value),
        weak,
    });
}

/// The entries of a Spring-style registry (a `.properties` or a yaml file already flattened
/// to dotted keys), plus the compose `environment:` map entries a yaml file holds
/// (`services.api.environment.KAFKA_TOPIC`, kept under their bare variable name).
///
/// `env_only`: keep the `environment:` variables alone (a yaml file that is not a property
/// source: a compose file, a manifest).
pub fn from_registry(reg: &PropertyRegistry, weak: bool, env_only: bool) -> ConfigFile {
    let secrets = PropertyRegistry::new();
    let mut file = ConfigFile::default();
    let mut raw: Vec<(&str, &str)> = reg.raw_entries().collect();
    raw.sort_unstable();
    for (key, value) in raw {
        if !env_only {
            push_entry(&mut file, &secrets, key, value, weak);
        }
        if let Some((_, name)) = key.rsplit_once(".environment.") {
            push_entry(&mut file, &secrets, name, value, weak);
        }
    }
    file
}

/// Whether a yaml text holds more than one document (`---` profile sections after the first).
pub fn is_multi_document(content: &str) -> bool {
    let mut seen_content = false;
    for line in content.lines() {
        let t = line.trim_end();
        if t == "---" || t.starts_with("--- ") {
            if seen_content {
                return true;
            }
        } else if !t.trim_start().is_empty() && !t.trim_start().starts_with('#') {
            seen_content = true;
        }
    }
    false
}

/// Whether `path` is a file configuration is read from (yaml, properties, a dotenv template).
pub fn is_config_path(path: &Path) -> bool {
    let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
    is_env_template_name(name)
        || matches!(
            path.extension().and_then(|e| e.to_str()),
            Some("yml" | "yaml" | "properties")
        )
}

/// The only dotenv files read: the four template names, exactly. A real dotenv file
/// (`.env`, `.env.local`, `.env.production`, `production.env`, `.env.distributor`) holds
/// secrets and is never read; the default `exclude_patterns` agree for `.env*`, and this
/// check holds for `*.env` and for a workspace that cleared the default.
pub fn is_env_template_name(name: &str) -> bool {
    matches!(
        name,
        ".env.example" | ".env.sample" | ".env.template" | ".env.dist"
    )
}

/// A file that only documents the shape of the real configuration.
pub fn is_template_name(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    ["example", "sample", "template", "dist", "default"]
        .iter()
        .any(|w| lower.contains(w))
}

/// `KEY=value` lines of a dotenv file (`export KEY=value`, quoted values, `# comments`);
/// a shell default `${A:-x}` is read as `${A:x}`.
pub fn from_dotenv(content: &str, weak: bool) -> ConfigFile {
    let secrets = PropertyRegistry::new();
    let mut file = ConfigFile::default();
    for line in content.lines() {
        let line = line.trim();
        let line = line.strip_prefix("export ").unwrap_or(line).trim_start();
        if line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let value = value.trim();
        let value = match value.chars().next() {
            Some(q @ ('"' | '\'')) => value[1..].split(q).next().unwrap_or(""),
            _ => value.split(" #").next().unwrap_or("").trim_end(),
        };
        let value = if value.contains("${") {
            value.replace(":-", ":")
        } else {
            value.to_string()
        };
        push_entry(&mut file, &secrets, key.trim(), &value, weak);
    }
    file
}

/// Whether yaml text may hold an environment list worth scanning for.
pub fn may_hold_env_list(content: &str) -> bool {
    content.contains("environment:") || content.contains("env:")
}

/// The list forms a yaml flattener skips: compose `environment: [ - KEY=value ]` and a
/// Kubernetes container `env: [ - name: KEY \n value: v ]`. A line scanner, not a yaml
/// parser: it reads only those two shapes, only below an `environment:` / `env:` header, and
/// ignores everything else (`valueFrom`, anchors, flow style).
pub fn from_env_lists(content: &str, weak: bool) -> ConfigFile {
    let secrets = PropertyRegistry::new();
    let mut file = ConfigFile::default();
    // Indent of the open `environment:` / `env:` header, if inside one.
    let mut header: Option<usize> = None;
    let mut pending_name: Option<String> = None;
    for line in content.lines() {
        let trimmed = line.trim_start();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let indent = line.len() - trimmed.len();
        if matches!(trimmed.trim_end(), "environment:" | "env:") {
            header = Some(indent);
            pending_name = None;
            continue;
        }
        let Some(h) = header else { continue };
        let is_item = trimmed.starts_with("- ") || trimmed.starts_with("-\t");
        if indent < h || (indent == h && !is_item) {
            header = None;
            pending_name = None;
            continue;
        }
        let body = trimmed.strip_prefix('-').map_or(trimmed, str::trim_start);
        if let Some(name) = body.strip_prefix("name:") {
            pending_name = Some(unquote(name).to_string());
        } else if let Some(value) = body.strip_prefix("value:") {
            if let Some(name) = pending_name.take() {
                push_entry(&mut file, &secrets, &name, unquote(value), weak);
            }
        } else if is_item {
            if let Some((key, value)) = body.split_once('=') {
                push_entry(&mut file, &secrets, key.trim(), unquote(value), weak);
            }
        }
    }
    file
}

fn unquote(s: &str) -> &str {
    let s = s.split(" #").next().unwrap_or("").trim();
    s.trim_matches(|c| c == '"' || c == '\'')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn values(files: &[(&str, ConfigFile)]) -> ConfigValues {
        let mut v = ConfigValues::default();
        for (p, f) in files {
            v.set_file(Path::new(p), f.entries.clone());
        }
        v.rebuild();
        v
    }

    fn props(text: &str) -> ConfigFile {
        let mut reg = PropertyRegistry::new();
        reg.ingest_properties_str(text);
        from_registry(&reg, false, false)
    }

    fn topics(r: Option<Resolved>) -> Vec<(String, bool)> {
        r.map(|r| r.topics).unwrap_or_default()
    }

    #[test]
    fn one_value_is_exact_and_a_default_alone_is_weak() {
        let v = values(&[("a.properties", props("app.topics.orders=orders.v1\n"))]);
        assert_eq!(
            topics(v.resolve("${app.topics.orders}")),
            [("orders.v1".into(), false)]
        );
        assert_eq!(
            topics(v.resolve("${app.topics.orders:other}")),
            [("orders.v1".into(), false)]
        );
        assert_eq!(
            topics(v.resolve("${missing:fallback}")),
            [("fallback".into(), true)]
        );
        assert!(v.resolve("${missing}").is_none());
    }

    #[test]
    fn two_profiles_make_every_candidate_weak() {
        let v = values(&[
            ("application.properties", props("t=orders\n")),
            ("application-prod.properties", props("t=orders.prod\n")),
        ]);
        let mut got = topics(v.resolve("${t}"));
        got.sort();
        assert_eq!(got, [("orders".into(), true), ("orders.prod".into(), true)]);
    }

    #[test]
    fn the_same_value_in_two_files_is_one_exact_value() {
        let v = values(&[
            ("a.properties", props("t=orders\n")),
            ("b.properties", props("t=orders\n")),
        ]);
        assert_eq!(topics(v.resolve("${t}")), [("orders".into(), false)]);
    }

    #[test]
    fn a_template_file_alone_is_weak_and_a_strong_file_wins_the_flag() {
        let weak = from_dotenv("TOPIC=orders\n", true);
        assert_eq!(
            topics(values(&[(".env.example", weak.clone())]).resolve("${TOPIC}")),
            [("orders".into(), true)]
        );
        let strong = from_dotenv("TOPIC=orders\n", false);
        let v = values(&[(".env.example", weak), (".env", strong)]);
        assert_eq!(topics(v.resolve("${TOPIC}")), [("orders".into(), false)]);
        // A template that disagrees with the deployed value is dropped, not an ambiguity.
        let doc = from_dotenv("TOPIC=your-topic-here\n", true);
        let v = values(&[
            (".env.example", doc.clone()),
            (".env", from_dotenv("TOPIC=orders\n", false)),
        ]);
        assert_eq!(topics(v.resolve("${TOPIC}")), [("orders".into(), false)]);
        // A template alone and the source's own default: both candidates, both weak.
        let v = values(&[(".env.example", doc)]);
        let mut got = topics(v.resolve("${TOPIC:dflt}"));
        got.sort();
        assert_eq!(
            got,
            [("dflt".into(), true), ("your-topic-here".into(), true)]
        );
    }

    #[test]
    fn nested_values_resolve_and_cycles_do_not() {
        let v = values(&[(
            "a.properties",
            props("a=${b}\nb=${c}.x\nc=base\nx=${y}\ny=${x}\nself=${self}\n"),
        )]);
        assert_eq!(topics(v.resolve("${a}")), [("base.x".into(), false)]);
        assert!(v.resolve("${x}").is_none(), "a cycle stays unresolved");
        assert!(v.resolve("${self}").is_none());
        assert_eq!(
            topics(v.resolve("${c}.orders")),
            [("base.orders".into(), false)]
        );
    }

    #[test]
    fn depth_and_fan_out_are_bounded() {
        let mut text = String::new();
        for i in 0..20 {
            text.push_str(&format!("k{i}=${{k{}}}\n", i + 1));
        }
        text.push_str("k20=end\n");
        let v = values(&[("a.properties", props(&text))]);
        assert!(v.resolve("${k0}").is_none(), "20 hops exceed the depth cap");
        assert_eq!(topics(v.resolve("${k15}")), [("end".into(), false)]);
    }

    #[test]
    fn secrets_and_prose_are_not_retained() {
        let f =
            props("db.password=hunter2\napi.token=abc\nmessage=hello world\napp.topic=orders\n");
        let keys: Vec<&str> = f.entries.iter().map(|e| e.key.as_str()).collect();
        assert_eq!(keys, ["app.topic"]);
    }

    #[test]
    fn dotenv_lines_are_read_with_quotes_comments_and_shell_defaults() {
        let f = from_dotenv(
            "# c\nexport A=\"x.y\"  # note\nB='z'\nC=${A:-fallback}\nSECRET_KEY=zz\nD=1 # trailing\n",
            false,
        );
        let got: Vec<(&str, &str)> = f
            .entries
            .iter()
            .map(|e| (e.key.as_str(), e.value.as_str()))
            .collect();
        assert_eq!(
            got,
            [("A", "x.y"), ("B", "z"), ("C", "${A:fallback}"), ("D", "1")]
        );
    }

    #[test]
    fn compose_and_kubernetes_env_lists_are_read() {
        let compose = "services:\n  api:\n    environment:\n      - KAFKA_TOPIC=orders\n      - OTHER=1\n    ports:\n      - 80:80\n";
        let f = from_env_lists(compose, false);
        let got: Vec<(&str, &str)> = f
            .entries
            .iter()
            .map(|e| (e.key.as_str(), e.value.as_str()))
            .collect();
        assert_eq!(got, [("KAFKA_TOPIC", "orders"), ("OTHER", "1")]);
        let k8s = "      containers:\n        - name: app\n          env:\n            - name: KAFKA_TOPIC\n              value: \"orders\"\n            - name: FROM\n              valueFrom:\n                secretKeyRef: {}\n          image: x\n";
        let f = from_env_lists(k8s, false);
        let got: Vec<(&str, &str)> = f
            .entries
            .iter()
            .map(|e| (e.key.as_str(), e.value.as_str()))
            .collect();
        assert_eq!(got, [("KAFKA_TOPIC", "orders")]);
    }

    /// F4: 64 files x 8 values per key, chained 8 levels deep, used to expand to the full
    /// cross product before any cap was checked (70 s). The work is now bounded by a step
    /// counter checked inside the loops; the placeholder stays unresolved.
    #[test]
    fn a_wide_chained_fan_out_is_abandoned_within_the_step_budget() {
        let mut v = ConfigValues::default();
        for f in 0..64 {
            let mut text = String::new();
            for level in 0..8 {
                // 8 distinct values per key across the 64 files (f % 8).
                text.push_str(&format!("k{level}=${{k{}}}.v{}\n", level + 1, f % 8));
            }
            text.push_str("k8=leaf\n");
            let mut reg = PropertyRegistry::new();
            reg.ingest_properties_str(&text);
            v.set_file(
                Path::new(&format!("f{f}.properties")),
                from_registry(&reg, false, false).entries,
            );
        }
        v.rebuild();
        let (resolved, steps) = v.resolve_counted("${k0}");
        assert!(resolved.is_none());
        assert!(steps <= MAX_RESOLVE_STEPS + 1, "{steps} steps");
        // 20 sites asking for the same topic cost one expansion (memo), and stay unresolved.
        for _ in 0..20 {
            assert!(v.resolve("${k0}").is_none());
        }
        // A small chain still resolves.
        let mut ok = ConfigValues::default();
        ok.set_file(
            Path::new("a.properties"),
            props("a=${b}\nb=${c}\nc=end\n").entries,
        );
        ok.rebuild();
        let (r, steps) = ok.resolve_counted("${a}");
        assert_eq!(r.unwrap().topics, [("end".to_string(), false)]);
        assert!(steps < 20, "{steps}");
    }

    #[test]
    fn only_the_four_template_names_are_dotenv_files() {
        for yes in [".env.example", ".env.sample", ".env.template", ".env.dist"] {
            assert!(is_env_template_name(yes), "{yes}");
        }
        for no in [
            ".env",
            ".env.local",
            ".env.production",
            "production.env",
            "secrets.env",
            ".env.distributor",
            ".env.examples",
            "x.env.example.bak",
            ".env.example.local",
            "env.example",
            ".ENV.EXAMPLE",
        ] {
            assert!(!is_env_template_name(no), "{no}");
        }
    }

    #[test]
    fn credentials_and_urls_are_never_retained_but_topics_are() {
        let text =
            "STRIPE_KEY=sk_live_leakstripe\nSENTRY_DSN=https://leakdsn@o1.ingest.sentry.io/1\n\
            DATABASE_URL=postgres://admin:leakpw@db/app\nPG_URL=postgres://u:pwcompose@db/x\n\
            DB_PASSWORD=hunter2\nAPI_KEY=abc\nSECRET_TOPIC=leak.secret.topic\nQUOTED=\"a b\"\n\
            WEBHOOK=ghp_abcdefghijklmnopqrstuvwxyz0123456789\nAWS=AKIAIOSFODNN7EXAMPLE\n\
            BLOB=aGVsbG8gd29ybGQgdGhpcyBpcyBhIHNlY3JldA==\nHEX=0123456789abcdef0123456789abcdef\n\
            HOSTED=admin@db.internal\nTOPIC_A=orders.v1\nTOPIC_B=user:deletion:success\n\
            TOPIC_C=sensors/temp\nTOPIC_D=${PREFIX:-pre}.orders\n";
        let f = from_dotenv(text, false);
        let got: Vec<&str> = f.entries.iter().map(|e| e.key.as_str()).collect();
        assert_eq!(got, ["TOPIC_A", "TOPIC_B", "TOPIC_C", "TOPIC_D"]);
        let mut reg = PropertyRegistry::new();
        reg.ingest_properties_str(
            "app.dsn=orders\napp.stripe.topic=orders\napp.auth=orders\napp.db.uri=orders\nok.topic=orders\n",
        );
        let f = from_registry(&reg, false, false);
        let got: Vec<&str> = f.entries.iter().map(|e| e.key.as_str()).collect();
        assert_eq!(got, ["ok.topic"]);
    }

    #[test]
    fn per_file_cap_is_counted() {
        let mut text = String::new();
        for i in 0..(MAX_ENTRIES_PER_FILE + 10) {
            text.push_str(&format!("k{i}=v{i}\n"));
        }
        let f = props(&text);
        assert_eq!(f.entries.len(), MAX_ENTRIES_PER_FILE);
        assert!(f.capped);
    }
}
