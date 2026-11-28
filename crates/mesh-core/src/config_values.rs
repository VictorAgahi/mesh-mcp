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
        // Every candidate, whatever produced it, must be a plain topic name.
        let resolved = if work.over {
            None
        } else {
            topics
                .map(|mut topics| {
                    topics.retain(|(t, _)| plain_topic(t));
                    topics
                })
                .filter(|topics| !topics.is_empty())
                .map(|topics| Resolved { topics })
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
        // A default that is not a plain topic name (a credential, a URL with userinfo, ...)
        // is never used: the placeholder stays unresolved and visible as `${key}`.
        let default = default.filter(|d| plain_topic(d));
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
pub(crate) fn name_shaped(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_VALUE_LEN
        && value.chars().all(|c| {
            c.is_ascii_alphanumeric()
                || matches!(c, '.' | '_' | '-' | ':' | '/' | '$' | '{' | '}' | '*' | '+')
        })
        && !value.contains("://")
        && !looks_like_token(value)
        && defaults_are_plain(value)
}

/// A topic name with no placeholder syntax left: what a resolved topic or a default must be.
pub fn plain_topic(value: &str) -> bool {
    name_shaped(value) && !value.contains(['$', '{', '}'])
}

/// The `(start, end)` of every outermost `${...}` of `text` (`end` is past the closing brace);
/// `None` when one is unterminated.
fn placeholder_spans(text: &str) -> Option<Vec<(usize, usize)>> {
    let mut spans = Vec::new();
    let mut from = 0;
    while let Some(rel) = text[from..].find("${") {
        let start = from + rel;
        let mut depth = 0usize;
        let mut end = None;
        for (i, c) in text[start..].char_indices() {
            match c {
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        end = Some(start + i + 1);
                        break;
                    }
                }
                _ => {}
            }
        }
        let end = end?;
        spans.push((start, end));
        from = end;
    }
    Some(spans)
}

/// Whether every `${key:default}` of `value` has a plain default (a nested placeholder or a
/// credential as a default is not).
fn defaults_are_plain(value: &str) -> bool {
    let Some(spans) = placeholder_spans(value) else {
        return false;
    };
    spans
        .into_iter()
        .all(|(s, e)| match value[s + 2..e - 1].split_once(':') {
            Some((_, d)) => d.is_empty() || plain_topic(d),
            None => true,
        })
}

/// `text` with every placeholder default that is not a plain topic name removed
/// (`${k:postgres://u:pw@h/x}` -> `${k}`, `${a:${b:sk_live_x}}` -> `${a}`): a credential
/// written as a default never becomes a node name, a topic or a cached string. Text with no
/// placeholder, or only plain defaults, is returned as is.
pub fn mask_placeholder_defaults(text: &str) -> std::borrow::Cow<'_, str> {
    rewrite_placeholder_defaults(text, |d| !d.is_empty() && !plain_topic(d))
}

/// Connection-string keys whose value is a credential.
const CONNECTION_SECRET_KEYS: [&str; 8] = [
    "sharedaccesskey",
    "sharedaccesssignature",
    "accountkey",
    "accesskey",
    "secretkey",
    "password",
    "pwd",
    "authkey",
];

/// Byte spans `(value_start, value_end)` of the credential values of `Key=Value` pairs in
/// `text` (`Key = Value` too; a pair ends at `;` or `"`). A value that is empty or only a
/// placeholder (`%s`, `?`, `$1`, `{0}`, `:name`, `@p1`) is no secret.
fn connection_secret_spans(text: &str) -> Vec<(usize, usize)> {
    let lower = text.to_ascii_lowercase();
    let bytes = lower.as_bytes();
    let mut spans = Vec::new();
    for key in CONNECTION_SECRET_KEYS {
        let mut from = 0;
        while let Some(rel) = lower[from..].find(key) {
            let at = from + rel;
            from = at + key.len();
            let mut i = from;
            while i < bytes.len() && bytes[i] == b' ' {
                i += 1;
            }
            if bytes.get(i) != Some(&b'=') || bytes.get(i + 1) == Some(&b'=') {
                continue;
            }
            i += 1;
            while i < bytes.len() && bytes[i] == b' ' {
                i += 1;
            }
            let start = i;
            while i < bytes.len() && bytes[i] != b';' && bytes[i] != b'"' {
                i += 1;
            }
            let value = text[start..i].trim_end();
            let first = value.chars().next();
            let placeholder = matches!(first, Some('%' | '?' | '$' | '{' | ':' | '@' | '<'));
            if value.is_empty() || placeholder {
                continue;
            }
            // Outside a quoted literal a member access or a call is code, not a secret
            // (`Password = settings.Password;`).
            spans.push((start, start + value.len()));
        }
    }
    spans.sort_unstable();
    spans
}

/// `text` with the value of every credential pair of a connection string
/// (`Endpoint=sb://h/;SharedAccessKey=...`, `Server=h;Password = ...`) replaced by the secret
/// mask. It reads a line at a time, so a connection string that a verbatim or raw literal spreads
/// over several lines is masked line by line. A pair with an empty or placeholder value
/// (`password=%s`, `password=?`) and a code assignment (`Password = settings.Password;`) are kept.
/// Text with no such pair is returned as is. (9.0 T8d.)
pub fn mask_connection_strings(text: &str) -> std::borrow::Cow<'_, str> {
    if !text.contains('=') {
        return std::borrow::Cow::Borrowed(text);
    }
    let quote_open: Vec<bool> = {
        let mut open = false;
        text.bytes()
            .map(|b| {
                if b == b'"' {
                    open = !open;
                }
                open
            })
            .collect()
    };
    let kept: Vec<(usize, usize)> = connection_secret_spans(text)
        .into_iter()
        .filter(|&(start, end)| {
            let value = &text[start..end];
            // Unquoted: `x.Password`, `Get(...)`: an expression, not a literal fragment.
            quote_open[start] || !(value.contains(['.', '(', ')']) || value.contains(' '))
        })
        .collect();
    if kept.is_empty() {
        return std::borrow::Cow::Borrowed(text);
    }
    let mut out = String::with_capacity(text.len());
    let mut at = 0;
    for (start, end) in kept {
        if start < at {
            continue;
        }
        out.push_str(&text[at..start]);
        out.push_str("[REDACTED_SECRET: USE_ENV_OR_LOCAL_FALLBACK]");
        at = end;
    }
    out.push_str(&text[at..]);
    std::borrow::Cow::Owned(out)
}

/// `text` with EVERY placeholder default removed (`${k:anything}` -> `${k}`). For a site whose
/// extractor never takes a default as a name (the JVM brokers of 9.0 T8a): a default that merely
/// looks like a topic (`hunter2-prod`) is as likely a secret as a name, so the placeholder
/// stays unresolved unless its key resolves through the configuration.
pub fn strip_placeholder_defaults(text: &str) -> std::borrow::Cow<'_, str> {
    rewrite_placeholder_defaults(text, |_| true)
}

/// [`mask_placeholder_defaults`] with the rule that decides which defaults go.
fn rewrite_placeholder_defaults(
    text: &str,
    drop: impl Fn(&str) -> bool,
) -> std::borrow::Cow<'_, str> {
    if !text.contains("${") {
        return std::borrow::Cow::Borrowed(text);
    }
    let Some(spans) = placeholder_spans(text) else {
        return std::borrow::Cow::Borrowed(text);
    };
    let mut out = String::with_capacity(text.len());
    let mut last = 0;
    let mut changed = false;
    for (s, e) in spans {
        out.push_str(&text[last..s]);
        let inner = &text[s + 2..e - 1];
        match inner.split_once(':') {
            // Only a placeholder-shaped key: `${a ? b : c}` of a template literal is code.
            Some((k, d))
                if drop(d)
                    && !k.is_empty()
                    && k.chars()
                        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-')) =>
            {
                out.push_str("${");
                out.push_str(k);
                out.push('}');
                changed = true;
            }
            _ => out.push_str(&text[s..e]),
        }
        last = e;
    }
    out.push_str(&text[last..]);
    if changed {
        std::borrow::Cow::Owned(out)
    } else {
        std::borrow::Cow::Borrowed(text)
    }
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
    ];
    let lower = value.to_ascii_lowercase();
    if PREFIXES.iter().any(|p| lower.starts_with(p)) {
        return true;
    }
    let upper_run = |min: usize| {
        value.len() >= min
            && value
                .chars()
                .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
    };
    // AWS access key ids, Google API keys, JWTs.
    if ((lower.starts_with("akia") || lower.starts_with("asia")) && upper_run(20))
        || (value.starts_with("AIza") && value.len() >= 35)
        || (value.starts_with("eyJ") && value.len() >= 30)
    {
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
pub(crate) fn secret_key(key: &str) -> bool {
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
        "_pass",
        "-pass",
        ".pass",
        "_passphrase",
        "_pin",
        "_salt",
        "_seed",
        "_signature",
        "_private",
        "_cert",
        "_cookie",
        "_session",
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
    // `pass` as a whole word (`DB_PASS`, `db.pass.value`, `dbPass`), not inside `passenger`.
    let word_pass = key
        .split(|c: char| !c.is_ascii_alphanumeric())
        .flat_map(|w| {
            let mut words: Vec<String> = Vec::new();
            let mut cur = String::new();
            let mut prev_lower = false;
            for c in w.chars() {
                if c.is_ascii_uppercase() && prev_lower {
                    words.push(std::mem::take(&mut cur));
                }
                prev_lower = c.is_ascii_lowercase();
                cur.push(c.to_ascii_lowercase());
            }
            words.push(cur);
            words
        })
        .any(|w| matches!(w.as_str(), "pass" | "pwd" | "pin" | "salt" | "pw"));
    lower == "key"
        || word_pass
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
            // Compose reads shell semantics: `${VAR:-default}` is `${VAR:default}`.
            push_entry(&mut file, &secrets, name, &shell_defaults(value), weak);
        }
    }
    file
}

/// `${A:-x}` (shell / compose) as `${A:x}` (the placeholder syntax of this module).
fn shell_defaults(value: &str) -> std::borrow::Cow<'_, str> {
    if value.contains("${") && value.contains(":-") {
        std::borrow::Cow::Owned(value.replace(":-", ":"))
    } else {
        std::borrow::Cow::Borrowed(value)
    }
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
                push_entry(
                    &mut file,
                    &secrets,
                    &name,
                    &shell_defaults(unquote(value)),
                    weak,
                );
            }
        } else if is_item {
            if let Some((key, value)) = body.split_once('=') {
                push_entry(
                    &mut file,
                    &secrets,
                    key.trim(),
                    &shell_defaults(unquote(value)),
                    weak,
                );
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

    /// 9.0 T8d: a connection string is masked wherever it sits (a literal, a continuation line of
    /// a verbatim literal, spaced pairs); placeholders, code and plain names are not.
    #[test]
    fn connection_strings_are_masked_and_placeholders_code_and_names_are_not() {
        let key = ["Shared", "Access", "Key"].concat();
        let m = "[REDACTED_SECRET: USE_ENV_OR_LOCAL_FALLBACK]";
        let line = format!(
            "[ServiceBusTrigger(\"q\", Connection = \"Endpoint=sb://h/;{key}=zz9\")] // \"orders\""
        );
        assert_eq!(
            mask_connection_strings(&line),
            format!("[ServiceBusTrigger(\"q\", Connection = \"Endpoint=sb://h/;{key}={m}\")] // \"orders\"")
        );
        // A continuation line of a multi-line verbatim literal has no quote on it.
        assert_eq!(
            mask_connection_strings(&format!("    {key}=zz9;")),
            format!("    {key}={m};")
        );
        assert_eq!(
            mask_connection_strings(&format!("    {key} = zz9;")),
            format!("    {key} = {m};")
        );
        let pw = ["Pass", "word"].concat();
        assert_eq!(
            mask_connection_strings(&format!("var c = \"Server=h;{pw} = hunter2;\";")),
            format!("var c = \"Server=h;{pw} = {m};\";")
        );
        // A key that merely ends in a credential key is one too.
        assert_eq!(
            mask_connection_strings("\"A=b;DbPassword=zz9\""),
            format!("\"A=b;DbPassword={m}\"")
        );
        for k in [
            "SharedAccessSignature",
            "AccountKey",
            "AccessKey",
            "SecretKey",
            "Pwd",
        ] {
            let masked = mask_connection_strings(&format!("\"A=b;{k}=zz9\"")).into_owned();
            assert_eq!(masked, format!("\"A=b;{k}={m}\""), "{k}");
        }
        for plain in [
            "var q = \"orders\";",
            "Route(\"api/[controller]\")",
            "x = \"a=b\";",
            "no quote = here",
            "fmt.Sprintf(\"password=%s\", pw)",
            "db.Query(\"... password=?\")",
            "\"password=$1\"",
            "\"password={0}\"",
            "\"password=:name\"",
            "\"pwd=@p1\"",
            "\"password=\"",
            "Password = settings.Password;",
            "Password = Get(x);",
            "msg = \"a Password == b\";",
            "if (a.Password == b) {}",
        ] {
            assert!(
                matches!(
                    mask_connection_strings(plain),
                    std::borrow::Cow::Borrowed(_)
                ),
                "{plain}"
            );
        }
    }
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

    /// Round 2: a credential used as a default (or reached through a value) is never a topic.
    #[test]
    fn credentials_as_placeholder_defaults_are_never_candidates() {
        let mut reg = PropertyRegistry::new();
        reg.ingest_properties_str(
            "a=${KEY_B:sk_live_CFGLEAKabcdef0123}\nn=${x:${y:sk_live_NESTEDx0123456789}}\n\
             u=${U:postgres://u:pw@h/x}\nok=${OK:fine.default}\nh=${H:0123456789abcdef0123456789abcdef}\n",
        );
        let mut v = ConfigValues::default();
        v.set_file(
            Path::new("a.properties"),
            from_registry(&reg, false, false).entries,
        );
        v.rebuild();
        for t in [
            "${a}",
            "${n}",
            "${u}",
            "${h}",
            "${nokey:sk_live_abcdef0123456789}",
            "${nokey:a@b}",
        ] {
            assert!(v.resolve(t).is_none(), "{t}");
        }
        assert_eq!(
            v.resolve("${nokey:plain.dflt}").unwrap().topics,
            [("plain.dflt".to_string(), true)]
        );
        // The stored entries themselves never keep a placeholder with an unsafe default.
        let stored = from_registry(&reg, false, false);
        let keys: Vec<&str> = stored.entries.iter().map(|e| e.key.as_str()).collect();
        assert_eq!(keys, ["ok"]);
    }

    #[test]
    fn masking_removes_unsafe_defaults_only() {
        assert_eq!(
            mask_placeholder_defaults("${k:postgres://u:pw@h/x}"),
            "${k}"
        );
        assert_eq!(mask_placeholder_defaults("${a:${b:sk_live_x}}"), "${a}");
        assert_eq!(
            mask_placeholder_defaults("${k:orders.v1}"),
            "${k:orders.v1}"
        );
        assert_eq!(
            mask_placeholder_defaults("p.${k:sk_live_abcdef0123}.s"),
            "p.${k}.s"
        );
        assert_eq!(mask_placeholder_defaults("${a ? b : c}"), "${a ? b : c}");
        assert_eq!(mask_placeholder_defaults("plain"), "plain");
    }

    /// Round 2 items 4, 5, 6.
    #[test]
    fn shell_defaults_extra_secret_keys_and_hash_values() {
        let compose = "services:\n  a:\n    environment:\n      - T=${X:-orders}\n";
        let f = from_env_lists(compose, false);
        assert_eq!(f.entries[0].value, "${X:orders}");
        let mut reg = PropertyRegistry::new();
        reg.ingest_yaml_str("services:\n  a:\n    environment:\n      T: \"${X:-orders2}\"\n")
            .unwrap();
        let f = from_registry(&reg, false, true);
        assert_eq!(f.entries[0].value, "${X:orders2}");
        let text = "DB_PASS=Tr0ubador3xyz\nPASS=abcdef\nDB_PIN=1234\nX_SALT=ab\nX_SEED=ab\n\
            X_SIGNATURE=ab\nX_PRIVATE=ab\nX_CERT=ab\nX_COOKIE=ab\nX_SESSION=ab\ndbPass=ab\n\
            NOSP_TOPIC=orders#hashLEAKnosp\nPASSENGER_TOPIC=passenger.events\nOK_TOPIC=orders\n";
        let f = from_dotenv(text, false);
        let got: Vec<&str> = f.entries.iter().map(|e| e.key.as_str()).collect();
        assert_eq!(got, ["PASSENGER_TOPIC", "OK_TOPIC"]);
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
