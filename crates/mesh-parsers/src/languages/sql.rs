//! SQL producers and consumers (9.0 T5): events that originate in SQL, not in a broker client call.
//!
//! No tree-sitter SQL grammar is in the workspace, and none is added: this is a bounded lexer
//! (one pass, tokens kept as byte offsets, no regex, no recursion) plus a keyword matcher over the
//! token list. Shapes recognised, each keyed by a STATIC literal only:
//!
//! - `INSERT INTO <outbox table> (<cols>) VALUES (<row>)[, (<row>)...]` and `... SELECT <items>`:
//!   the table is an outbox (its unquoted, lower-cased last name part contains `outbox`, or is
//!   `event_queue` / `event_queues`; a schema prefix and quoted identifiers are read); the key is the
//!   value of the first column named `topic`, `topic_name`, `channel`, `event_type`, `event_name` or
//!   `routing_key` when that value is a single-quoted literal (an `E'..'` prefix and a `::type` cast
//!   are accepted). A bind parameter, an expression or a column list without such a column is "not
//!   statically resolved": counted in [`SqlScan::unresolved`], never given a key;
//! - `NOTIFY channel[, 'payload']` and `pg_notify('channel', ...)`: produce `channel`;
//! - `LISTEN channel`: consume `channel`.
//!
//! Function and trigger bodies need no special case: a dollar-quoted body is scanned as code, so the
//! `INSERT` / `pg_notify` inside a trigger function is the producer, on the line of its literal.
//! An unquoted channel is folded to lower case (Postgres does), a quoted one is kept as written.
//!
//! The same statements are read from the string literals of TypeScript / JavaScript, Go, Java and
//! Python files ([`scan_embedded`]): a literal is scanned only when it BEGINS with `INSERT`,
//! `NOTIFY`, `LISTEN`, `SELECT`, `PERFORM` or `BEGIN`, so prose that merely mentions `INSERT INTO
//! outbox`, and every comment, are ignored. Limits: adjacent literals (`"INSERT ..." + "VALUES ..."`)
//! are not joined, and a regex literal that holds a quote can desynchronise the host lexer for the
//! rest of its line (it ends at the newline).
//!
//! Bounds (CLAUDE.md commandment 2): a `.sql` file goes through `AstGuard::check_content` (384 KB,
//! null-byte sniff, 1,024-byte lines, nesting depth 64) before any scan; the token list is capped, as
//! are the facts per file and the rows per `VALUES`.

use super::FileIndex;
use crate::guard::AstGuard;
use mesh_core::{CompactStr, ContractNode, NodeKind, RepoId};
use std::path::Path;
use std::sync::Arc;

/// Tokens kept per scanned text; a larger text is cut there (and reported through `truncated`).
const MAX_TOKENS: usize = 200_000;
/// Facts kept per file.
const MAX_FACTS: usize = 4_096;
/// Rows of one `VALUES` list that are read.
const MAX_ROWS: usize = 512;
/// Columns of one `INSERT` column list that are read.
const MAX_COLUMNS: usize = 256;
/// Embedded string literals scanned per file.
const MAX_LITERALS: usize = 4_096;
/// Longest topic or channel kept.
const MAX_TOPIC_LEN: usize = 255;

/// Column names that carry the event key of an outbox row, in no particular priority: the first
/// one of the column list wins.
const TOPIC_COLUMNS: &[&str] = &[
    "topic",
    "topic_name",
    "channel",
    "event_type",
    "event_name",
    "routing_key",
];

/// Whether `path` is a SQL file (`.sql`, any case).
pub fn is_sql_path(path: &Path) -> bool {
    path.extension()
        .is_some_and(|e| e.eq_ignore_ascii_case("sql"))
}

/// Which side of the event a fact is on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlRole {
    Produce,
    Consume,
}

/// One statically resolved event site.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlFact {
    pub role: SqlRole,
    pub topic: CompactStr,
    /// 1-based line of the literal that carries the key.
    pub line: usize,
    /// What kind of statement it came from (the node signature).
    pub via: &'static str,
}

/// Result of scanning one text.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SqlScan {
    pub facts: Vec<SqlFact>,
    /// Outbox inserts and `pg_notify` calls whose key is not a static literal.
    pub unresolved: usize,
    /// A bound (tokens, facts) cut the scan short.
    pub truncated: bool,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Kind {
    Word,
    /// `"identifier"`: `start..end` is the inside.
    Quoted,
    /// `'literal'`: `start..end` is the inside.
    Str,
    Punct,
}

#[derive(Clone, Copy, Debug)]
struct Tok {
    kind: Kind,
    start: u32,
    end: u32,
    line: u32,
    /// The character of a `Punct`.
    byte: u8,
}

#[inline]
fn is_ident_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_' || b >= 0x80
}

#[inline]
fn is_ident_part(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b >= 0x80
}

/// Length of a dollar-quote delimiter (`$$`, `$tag$`) starting at `i`, if it is one. `$1` is a
/// parameter, not a delimiter.
fn dollar_tag_len(b: &[u8], i: usize) -> Option<usize> {
    let mut j = i + 1;
    if b.get(j)
        .is_some_and(|&c| c.is_ascii_alphabetic() || c == b'_')
    {
        while j < b.len() && is_ident_part(b[j]) {
            j += 1;
        }
    }
    (j < b.len() && b[j] == b'$').then_some(j + 1 - i)
}

/// Splits `src` into tokens. Comments (`--`, nested `/* */`) are dropped; line numbers start at
/// `first_line`. Returns the tokens and whether the token cap cut the text.
fn lex(src: &str, first_line: usize) -> (Vec<Tok>, bool) {
    let b = src.as_bytes();
    let n = b.len();
    let mut toks: Vec<Tok> = Vec::new();
    let mut line = first_line as u32;
    let mut i = 0usize;
    let push = |toks: &mut Vec<Tok>, kind, start: usize, end: usize, line: u32, byte| {
        toks.push(Tok {
            kind,
            start: start as u32,
            end: end as u32,
            line,
            byte,
        });
    };
    while i < n {
        if toks.len() >= MAX_TOKENS {
            return (toks, true);
        }
        let c = b[i];
        match c {
            b'\n' => {
                line += 1;
                i += 1;
            }
            b' ' | b'\t' | b'\r' | 0x0c => i += 1,
            b'-' if b.get(i + 1) == Some(&b'-') => {
                while i < n && b[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if b.get(i + 1) == Some(&b'*') => {
                let mut depth = 1u32;
                i += 2;
                while i < n && depth > 0 {
                    if b[i] == b'\n' {
                        line += 1;
                        i += 1;
                    } else if b[i] == b'/' && b.get(i + 1) == Some(&b'*') {
                        depth += 1;
                        i += 2;
                    } else if b[i] == b'*' && b.get(i + 1) == Some(&b'/') {
                        depth -= 1;
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
            }
            b'\'' | b'"' => {
                // `E'..'` reads backslash escapes.
                let esc = c == b'\''
                    && i > 0
                    && matches!(b[i - 1], b'e' | b'E')
                    && (i < 2 || !is_ident_part(b[i - 2]));
                let start_line = line;
                let mut j = i + 1;
                while j < n {
                    let d = b[j];
                    if d == b'\n' {
                        line += 1;
                    }
                    if esc && d == b'\\' {
                        if b.get(j + 1) == Some(&b'\n') {
                            line += 1;
                        }
                        j += 2;
                        continue;
                    }
                    if d == c {
                        if b.get(j + 1) == Some(&c) {
                            j += 2;
                            continue;
                        }
                        break;
                    }
                    j += 1;
                }
                let end = j.min(n);
                let kind = if c == b'\'' { Kind::Str } else { Kind::Quoted };
                push(&mut toks, kind, i + 1, end, start_line, 0);
                i = j + 1;
            }
            b'$' => {
                push(&mut toks, Kind::Punct, i, i + 1, line, b'$');
                i += dollar_tag_len(b, i).unwrap_or(1);
            }
            _ if is_ident_start(c) => {
                let start = i;
                while i < n && is_ident_part(b[i]) {
                    i += 1;
                }
                push(&mut toks, Kind::Word, start, i, line, 0);
            }
            _ if c.is_ascii_digit() => {
                let start = i;
                while i < n && is_ident_part(b[i]) {
                    i += 1;
                }
                push(&mut toks, Kind::Word, start, i, line, 0);
            }
            _ => {
                push(&mut toks, Kind::Punct, i, i + 1, line, c);
                i += 1;
            }
        }
    }
    (toks, false)
}

/// Whether `name` (an outbox candidate, already lower-cased) is an event outbox table.
fn is_outbox_table(name: &str) -> bool {
    name.contains("outbox") || name == "event_queue" || name == "event_queues"
}

struct Scanner<'a> {
    src: &'a str,
    t: &'a [Tok],
    out: SqlScan,
}

impl<'a> Scanner<'a> {
    fn text(&self, i: usize) -> &'a str {
        let t = &self.t[i];
        // Token boundaries sit on ASCII bytes or the ends of the text, so the slice is valid.
        self.src.get(t.start as usize..t.end as usize).unwrap_or("")
    }

    fn word_is(&self, i: usize, kw: &str) -> bool {
        self.t
            .get(i)
            .is_some_and(|t| t.kind == Kind::Word && self.text(i).eq_ignore_ascii_case(kw))
    }

    fn punct_is(&self, i: usize, c: u8) -> bool {
        self.t
            .get(i)
            .is_some_and(|t| t.kind == Kind::Punct && t.byte == c)
    }

    fn is_ident(&self, i: usize) -> bool {
        self.t
            .get(i)
            .is_some_and(|t| matches!(t.kind, Kind::Word | Kind::Quoted))
    }

    fn push(&mut self, role: SqlRole, topic: CompactStr, line: u32, via: &'static str) {
        if self.out.facts.len() >= MAX_FACTS {
            self.out.truncated = true;
            return;
        }
        self.out.facts.push(SqlFact {
            role,
            topic,
            line: line as usize,
            via,
        });
    }

    fn run(&mut self) {
        let mut i = 0;
        while i < self.t.len() && !self.out.truncated {
            i = if self.t[i].kind != Kind::Word {
                i + 1
            } else if self.word_is(i, "insert") {
                self.insert(i)
            } else if self.word_is(i, "notify") {
                self.channel(i, SqlRole::Produce, "sql NOTIFY")
            } else if self.word_is(i, "listen") {
                self.channel(i, SqlRole::Consume, "sql LISTEN")
            } else if self.word_is(i, "pg_notify") {
                self.pg_notify(i)
            } else {
                i + 1
            };
        }
    }

    /// `NOTIFY ch` / `LISTEN ch` at the start of a statement. A statement start is the text start,
    /// `;`, a dollar-quote delimiter, or `BEGIN` / `THEN` / `ELSE` / `LOOP` / `DO`: a column or a
    /// type that happens to be named `notify` is not one.
    fn channel(&mut self, i: usize, role: SqlRole, via: &'static str) -> usize {
        let at_start = i == 0
            || self.punct_is(i - 1, b';')
            || self.punct_is(i - 1, b'$')
            || ["begin", "then", "else", "loop", "do"]
                .iter()
                .any(|kw| self.word_is(i - 1, kw));
        let ends = i + 2 >= self.t.len()
            || self.punct_is(i + 2, b';')
            || self.punct_is(i + 2, b',')
            || self.punct_is(i + 2, b'$');
        if !at_start || !self.is_ident(i + 1) || !ends {
            return i + 1;
        }
        let t = self.t[i + 1];
        let name = self.text(i + 1);
        let topic = if t.kind == Kind::Quoted {
            name.replace("\"\"", "\"")
        } else {
            name.to_ascii_lowercase()
        };
        if let Some(topic) = valid_topic(&topic) {
            self.push(role, topic, t.line, via);
        }
        i + 2
    }

    /// `pg_notify('ch', payload)`.
    fn pg_notify(&mut self, i: usize) -> usize {
        if !self.punct_is(i + 1, b'(') {
            return i + 1;
        }
        let (item, end) = self.list_item(i + 2, 0);
        match item.and_then(|r| self.literal(r)) {
            Some((topic, line)) => self.push(SqlRole::Produce, topic, line, "sql pg_notify"),
            None => self.out.unresolved += 1,
        }
        end.max(i + 1)
    }

    /// `INSERT INTO <table> (<cols>) VALUES ... | SELECT ...`, read only for an outbox table.
    fn insert(&mut self, i: usize) -> usize {
        if !self.word_is(i + 1, "into") {
            return i + 1;
        }
        let mut k = i + 2;
        let mut last = None;
        while self.is_ident(k) {
            last = Some(k);
            k += 1;
            if self.punct_is(k, b'.') {
                k += 1;
            } else {
                break;
            }
        }
        let Some(name_tok) = last else {
            return k.max(i + 1);
        };
        if !is_outbox_table(&self.text(name_tok).to_ascii_lowercase()) {
            return k;
        }
        if self.word_is(k, "as") && self.is_ident(k + 1) {
            k += 2;
        }
        if !self.punct_is(k, b'(') {
            self.out.unresolved += 1;
            return k;
        }
        // Column list: plain identifiers only.
        k += 1;
        let mut topic_idx = None;
        let mut idx = 0usize;
        loop {
            if !self.is_ident(k) || idx >= MAX_COLUMNS {
                self.out.unresolved += 1;
                return k;
            }
            let col = self.text(k).to_ascii_lowercase();
            if topic_idx.is_none() && TOPIC_COLUMNS.contains(&col.as_str()) {
                topic_idx = Some(idx);
            }
            idx += 1;
            k += 1;
            if self.punct_is(k, b',') {
                k += 1;
            } else if self.punct_is(k, b')') {
                k += 1;
                break;
            } else {
                self.out.unresolved += 1;
                return k;
            }
        }
        let Some(target) = topic_idx else {
            self.out.unresolved += 1;
            return k;
        };
        // `OVERRIDING SYSTEM VALUE` and the like.
        let mut skipped = 0;
        while skipped < 4
            && self.t.get(k).is_some_and(|t| t.kind == Kind::Word)
            && !self.word_is(k, "values")
            && !self.word_is(k, "select")
        {
            k += 1;
            skipped += 1;
        }
        if self.word_is(k, "values") {
            k += 1;
            let mut rows = 0;
            while self.punct_is(k, b'(') && rows < MAX_ROWS {
                let (item, end) = self.list_item(k + 1, target);
                k = if self.punct_is(end, b')') {
                    end + 1
                } else {
                    end
                };
                rows += 1;
                self.site(item);
                if self.punct_is(k, b',') {
                    k += 1;
                } else {
                    break;
                }
            }
            k
        } else if self.word_is(k, "select") {
            let (item, end) = self.list_item(k + 1, target);
            self.site(item);
            end
        } else {
            self.out.unresolved += 1;
            k
        }
    }

    /// Registers one outbox row key, or counts it unresolved.
    fn site(&mut self, item: Option<(usize, usize)>) {
        match item.and_then(|r| self.literal(r)) {
            Some((topic, line)) => self.push(SqlRole::Produce, topic, line, "sql outbox insert"),
            None => self.out.unresolved += 1,
        }
    }

    /// Finds the `target`-th item of a comma list that starts at token `k` and ends at the first
    /// unmatched `)`, a `;` or (for a `SELECT` list) `FROM` / `WHERE` / `RETURNING` / `ON`. Returns the
    /// item's token range and the index of the terminator. Iterative: nesting is a counter.
    fn list_item(&self, mut k: usize, target: usize) -> (Option<(usize, usize)>, usize) {
        let mut depth = 0usize;
        let mut idx = 0usize;
        let mut item_start = k;
        let mut found = None;
        while k < self.t.len() {
            let t = self.t[k];
            if t.kind == Kind::Punct {
                match t.byte {
                    b'(' => depth += 1,
                    b')' if depth == 0 => break,
                    b')' => depth -= 1,
                    b';' if depth == 0 => break,
                    b',' if depth == 0 => {
                        if idx == target {
                            found = Some((item_start, k));
                        }
                        idx += 1;
                        item_start = k + 1;
                    }
                    _ => {}
                }
            } else if depth == 0
                && t.kind == Kind::Word
                && ["from", "where", "returning", "on"]
                    .iter()
                    .any(|kw| self.word_is(k, kw))
            {
                break;
            }
            k += 1;
        }
        if idx == target && found.is_none() {
            found = Some((item_start, k));
        }
        (found, k)
    }

    /// The static key of an item: one single-quoted literal, optionally `E`-prefixed and followed by
    /// a `::type` cast. Anything else (a parameter, `a || b`, a column) is not resolved.
    fn literal(&self, (mut a, b): (usize, usize)) -> Option<(CompactStr, u32)> {
        if a < b
            && self.word_is(a, "e")
            && self.t.get(a + 1).is_some_and(|t| t.kind == Kind::Str)
            && self.t[a].end == self.t[a + 1].start - 1
        {
            a += 1;
        }
        let t = self.t.get(a).filter(|t| a < b && t.kind == Kind::Str)?;
        let rest_ok = a + 1 == b || (self.punct_is(a + 1, b':') && self.punct_is(a + 2, b':'));
        if !rest_ok {
            return None;
        }
        let topic = valid_topic(&self.text(a).replace("''", "'"))?;
        Some((topic, t.line))
    }
}

/// A key worth registering: non-blank, no control character, bounded.
fn valid_topic(s: &str) -> Option<CompactStr> {
    let s = s.trim();
    (!s.is_empty() && s.len() <= MAX_TOPIC_LEN && !s.chars().any(char::is_control))
        .then(|| CompactStr::new(s))
}

/// Scans SQL text. Facts come in source order, so the result is deterministic.
pub fn scan_sql(src: &str, first_line: usize) -> SqlScan {
    if src.len() as u64 > AstGuard::MAX_FILE_SIZE_BYTES {
        return SqlScan {
            truncated: true,
            ..SqlScan::default()
        };
    }
    let (toks, cut) = lex(src, first_line);
    let mut s = Scanner {
        src,
        t: &toks,
        out: SqlScan::default(),
    };
    s.run();
    s.out.truncated |= cut;
    s.out
}

/// Host language of a file whose string literals are scanned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Host {
    /// TypeScript / JavaScript: `'..'`, `".."`, backtick template with backslash escapes.
    Ts,
    /// Go: `".."` and backtick raw strings, runes read like short strings.
    Go,
    /// Java: `".."` and `"""` text blocks.
    Java,
    /// Python: `'..'`, `".."` and triple-quoted strings, `#` comments.
    Python,
}

impl Host {
    fn slash_comments(self) -> bool {
        self != Self::Python
    }
}

/// Whether the text could hold one of the statements: a substring test that costs one pass.
fn may_hold_sql(src: &str) -> bool {
    let b = src.as_bytes();
    ["outbox", "event_queue", "notify", "listen"]
        .iter()
        .any(|needle| {
            let nb = needle.as_bytes();
            b.windows(nb.len()).any(|w| w.eq_ignore_ascii_case(nb))
        })
}

/// A literal body that begins like one of the statements read.
fn starts_like_sql(body: &str) -> bool {
    let t = body.trim_start();
    let word = t
        .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .next()
        .unwrap_or("");
    ["insert", "notify", "listen", "select", "perform", "begin"]
        .iter()
        .any(|kw| word.eq_ignore_ascii_case(kw))
}

/// Scans the SQL held in the string literals of a host-language file. Comments are skipped by the
/// host lexer; only a literal that begins like a statement is read.
pub fn scan_embedded(src: &str, host: Host) -> SqlScan {
    let mut out = SqlScan::default();
    if !may_hold_sql(src) {
        return out;
    }
    let b = src.as_bytes();
    let n = b.len();
    let mut i = 0usize;
    let mut line = 1usize;
    let mut literals = 0usize;
    while i < n && !out.truncated {
        let c = b[i];
        let triple = |i: usize, q: u8| b.get(i + 1) == Some(&q) && b.get(i + 2) == Some(&q);
        match c {
            b'\n' => {
                line += 1;
                i += 1;
            }
            b'/' if host.slash_comments() && b.get(i + 1) == Some(&b'/') => {
                while i < n && b[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if host.slash_comments() && b.get(i + 1) == Some(&b'*') => {
                i += 2;
                while i < n && !(b[i] == b'*' && b.get(i + 1) == Some(&b'/')) {
                    if b[i] == b'\n' {
                        line += 1;
                    }
                    i += 1;
                }
                i += 2;
            }
            b'#' if host == Host::Python => {
                while i < n && b[i] != b'\n' {
                    i += 1;
                }
            }
            b'"' | b'\'' | b'`' => {
                if c == b'`' && !matches!(host, Host::Ts | Host::Go) {
                    i += 1;
                    continue;
                }
                let multi = match (c, host) {
                    (b'`', _) => true,
                    (b'"', Host::Java | Host::Python) => triple(i, b'"'),
                    (b'\'', Host::Python) => triple(i, b'\''),
                    _ => false,
                };
                let quote_len = if c != b'`' && multi { 3 } else { 1 };
                let escapes = !(c == b'`' && host == Host::Go);
                let body_start = i + quote_len;
                let start_line = line;
                let mut j = body_start;
                while j < n {
                    let d = b[j];
                    if escapes && d == b'\\' {
                        if b.get(j + 1) == Some(&b'\n') {
                            line += 1;
                        }
                        j += 2;
                        continue;
                    }
                    if d == b'\n' {
                        if !multi {
                            break;
                        }
                        line += 1;
                    } else if d == c
                        && (quote_len == 1
                            || (b.get(j + 1) == Some(&c) && b.get(j + 2) == Some(&c)))
                    {
                        break;
                    }
                    j += 1;
                }
                let end = j.min(n);
                if let Some(body) = src.get(body_start..end) {
                    if starts_like_sql(body) && literals < MAX_LITERALS {
                        literals += 1;
                        let found = scan_sql(body, start_line);
                        out.unresolved += found.unresolved;
                        out.truncated |= found.truncated;
                        for fact in found.facts {
                            if out.facts.len() >= MAX_FACTS {
                                out.truncated = true;
                                break;
                            }
                            out.facts.push(fact);
                        }
                    }
                }
                // A multi-line literal's newlines were counted above; a short string stops at its
                // newline, which the main loop counts.
                i = if j < n && b[j] != b'\n' {
                    j + quote_len
                } else {
                    j
                };
            }
            _ => i += 1,
        }
    }
    out
}

/// Registers every fact of `scan` as an `EventStream` node and a producer or consumer on it.
fn register(out: &mut FileIndex, scan: SqlScan, file_path: &Path, repo_id: RepoId) {
    if scan.truncated {
        tracing::debug!(
            target: "mesh::parser",
            "SQL scan of {} hit a bound (tokens, facts or size)",
            file_path.display()
        );
    }
    if scan.facts.is_empty() {
        return;
    }
    let interned: mesh_core::FilePath = Arc::from(file_path);
    let package = mesh_core::detect_service_package(file_path, None);
    for fact in scan.facts {
        let idx = out.nodes.len();
        out.nodes.push(ContractNode {
            id: 0,
            name: fact.topic.clone(),
            kind: NodeKind::EventStream,
            file_path: interned.clone(),
            line_start: fact.line,
            line_end: fact.line,
            package: package.clone(),
            repo_id,
            signature: Some(CompactStr::new(fact.via)),
            docstring: None,
        });
        match fact.role {
            SqlRole::Produce => out.producers.push((idx, fact.topic)),
            SqlRole::Consume => out.consumers.push((idx, fact.topic)),
        }
    }
}

/// Facts of a `.sql` file. The lexical guard runs first; a refused file yields no facts (the
/// indexer already counts that as a rejection before it gets here).
pub fn extract_file(file_path: &Path, content: &str, repo_id: RepoId) -> FileIndex {
    let mut out = FileIndex::default();
    if AstGuard::check_content(file_path, content.as_bytes()).is_err() {
        return out;
    }
    register(&mut out, scan_sql(content, 1), file_path, repo_id);
    out
}

/// Adds the facts held in the string literals of a host-language file to `out`.
pub fn add_embedded(
    out: &mut FileIndex,
    file_path: &Path,
    content: &str,
    repo_id: RepoId,
    host: Host,
) {
    register(out, scan_embedded(content, host), file_path, repo_id);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn produced(scan: &SqlScan) -> Vec<(String, usize)> {
        scan.facts
            .iter()
            .filter(|f| f.role == SqlRole::Produce)
            .map(|f| (f.topic.to_string(), f.line))
            .collect()
    }

    fn consumed(scan: &SqlScan) -> Vec<(String, usize)> {
        scan.facts
            .iter()
            .filter(|f| f.role == SqlRole::Consume)
            .map(|f| (f.topic.to_string(), f.line))
            .collect()
    }

    #[test]
    fn literal_topic_is_a_producer_on_its_own_line() {
        let s = scan_sql(
            "INSERT INTO outbox (topic, payload)\nVALUES ('order.created', '{}');\n",
            1,
        );
        assert_eq!(produced(&s), vec![("order.created".to_string(), 2)]);
        assert_eq!(s.unresolved, 0);
    }

    #[test]
    fn non_literal_topic_is_never_guessed() {
        for sql in [
            "INSERT INTO outbox (topic, payload) VALUES ($1, $2);",
            "INSERT INTO outbox (topic, payload) VALUES (:topic, 1);",
            "INSERT INTO outbox (topic, payload) VALUES (TG_TABLE_NAME || '.changed', 1);",
            "INSERT INTO outbox (payload, topic) VALUES (1, concat('a', 'b'));",
            "INSERT INTO outbox (topic) SELECT t.name FROM topics t;",
            "INSERT INTO outbox (payload) VALUES ('no topic column');",
            "INSERT INTO outbox VALUES ('x', 'y');",
            "SELECT pg_notify(channel_name, 'x');",
        ] {
            let s = scan_sql(sql, 1);
            assert!(s.facts.is_empty(), "{sql}: {:?}", s.facts);
            assert_eq!(s.unresolved, 1, "{sql}");
        }
    }

    #[test]
    fn comments_and_strings_that_mention_insert_are_ignored() {
        let sql = "-- INSERT INTO outbox (topic) VALUES ('c1');\n\
                   /* INSERT INTO outbox (topic) VALUES ('c2'); /* nested NOTIFY c3 */ still */\n\
                   INSERT INTO audit (msg) VALUES ('INSERT INTO outbox (topic) VALUES (''s1'')');\n\
                   SELECT 'NOTIFY s2; LISTEN s3; pg_notify(''s4'', 1)';\n\
                   INSERT INTO notes (body) VALUES ($q$ INSERT INTO outbox (topic) VALUES ('s5') $q$);\n";
        let s = scan_sql(sql, 1);
        // The dollar-quoted text is code to the lexer (a function body is the point), so the s5 line
        // is the only one allowed to register; the comments and quoted strings never do.
        let topics: Vec<_> = s.facts.iter().map(|f| f.topic.as_str()).collect();
        assert!(!topics.contains(&"c1") && !topics.contains(&"c2") && !topics.contains(&"c3"));
        assert!(!topics.contains(&"s1") && !topics.contains(&"s2") && !topics.contains(&"s3"));
        assert!(!topics.contains(&"s4"));
    }

    #[test]
    fn multi_statement_files_and_multi_row_values() {
        let sql = "BEGIN;\n\
                   INSERT INTO outbox (id, topic) VALUES (1, 'a.one'), (2, 'a.two'),\n\
                   (3, 'a.three');\n\
                   NOTIFY chan_a;\n\
                   LISTEN chan_b;\n\
                   COMMIT;\n";
        let s = scan_sql(sql, 1);
        assert_eq!(
            produced(&s),
            vec![
                ("a.one".to_string(), 2),
                ("a.two".to_string(), 2),
                ("a.three".to_string(), 3),
                ("chan_a".to_string(), 4),
            ]
        );
        assert_eq!(consumed(&s), vec![("chan_b".to_string(), 5)]);
    }

    #[test]
    fn quoted_identifiers_schema_qualified_tables_and_case() {
        let sql = "iNsErT   iNtO \"Billing\".\"Invoice_Outbox\" AS o (\"payload\", \"Topic\")\n\
                   vAlUeS ('{}', E'it''s.fine'::text);\n\
                   Notify Foo_Bar;\n\
                   NOTIFY \"Foo.Bar\", 'p';\n\
                   LISTEN Foo_Bar;\n\
                   select Pg_Notify('x.y', null);\n\
                   INSERT INTO app.event_queue (event_type) VALUES ('queued.thing');\n";
        let s = scan_sql(sql, 1);
        assert_eq!(
            produced(&s),
            vec![
                ("it's.fine".to_string(), 2),
                ("foo_bar".to_string(), 3),
                ("Foo.Bar".to_string(), 4),
                ("x.y".to_string(), 6),
                ("queued.thing".to_string(), 7),
            ]
        );
        assert_eq!(consumed(&s), vec![("foo_bar".to_string(), 5)]);
    }

    #[test]
    fn trigger_function_body_produces_on_the_literal_line() {
        let sql = "CREATE FUNCTION f() RETURNS trigger AS $body$\n\
                   BEGIN\n\
                   PERFORM pg_notify('stock_changed', NEW.sku::text);\n\
                   INSERT INTO order_outbox (topic, payload) VALUES ('order.created', to_jsonb(NEW));\n\
                   RETURN NEW;\n\
                   END;\n\
                   $body$ LANGUAGE plpgsql;\n";
        let s = scan_sql(sql, 1);
        assert_eq!(
            produced(&s),
            vec![
                ("stock_changed".to_string(), 3),
                ("order.created".to_string(), 4)
            ]
        );
    }

    #[test]
    fn notify_as_a_column_or_type_is_not_a_channel() {
        let s = scan_sql(
            "ALTER TABLE t ADD COLUMN notify text;\nSELECT notify FROM t;\nCREATE TABLE listen (x int);\n",
            1,
        );
        assert!(s.facts.is_empty());
    }

    #[test]
    fn deterministic_and_bounded() {
        let sql = "INSERT INTO outbox (topic) VALUES ('t');\nLISTEN c;\n".repeat(3_000);
        let a = scan_sql(&sql, 1);
        let b = scan_sql(&sql, 1);
        assert_eq!(a, b);
        assert!(a.facts.len() <= MAX_FACTS);
        assert!(a.truncated);
    }

    #[test]
    fn embedded_literals_in_each_host() {
        let ts = "// INSERT INTO outbox (topic) VALUES ('c')\n\
                  const a = `INSERT INTO outbox (topic, payload)\n VALUES ('ts.topic', $1)`;\n\
                  const b = \"run INSERT INTO outbox (topic) VALUES ('prose')\";\n\
                  const c = 'INSERT INTO outbox (topic) VALUES ($1)';\n";
        let s = scan_embedded(ts, Host::Ts);
        assert_eq!(produced(&s), vec![("ts.topic".to_string(), 3)]);
        assert_eq!(s.unresolved, 1);

        let go = "x := `SELECT pg_notify('go.chan', $1)`\n// LISTEN no\n_, _ = db.Exec(ctx, \"LISTEN go_chan\")\n";
        let s = scan_embedded(go, Host::Go);
        assert_eq!(produced(&s), vec![("go.chan".to_string(), 1)]);
        assert_eq!(consumed(&s), vec![("go_chan".to_string(), 3)]);

        let java = "String q = \"\"\"\n    INSERT INTO event_queue (event_type, body)\n    VALUES ('java.evt', ?)\n    \"\"\";\n";
        let s = scan_embedded(java, Host::Java);
        assert_eq!(produced(&s), vec![("java.evt".to_string(), 3)]);

        let py = "# LISTEN nope\nconn.execute(\"LISTEN py_chan\")\nconn.execute('''\n  NOTIFY py_out\n''')\n";
        let s = scan_embedded(py, Host::Python);
        assert_eq!(consumed(&s), vec![("py_chan".to_string(), 2)]);
        assert_eq!(produced(&s), vec![("py_out".to_string(), 4)]);
    }

    #[test]
    fn extract_file_registers_nodes_producers_and_consumers() {
        let p = Path::new("db/001.sql");
        let idx = extract_file(
            p,
            "INSERT INTO outbox (topic) VALUES ('a');\nLISTEN b;\n",
            0,
        );
        assert_eq!(idx.nodes.len(), 2);
        assert_eq!(idx.producers, vec![(0, CompactStr::new("a"))]);
        assert_eq!(idx.consumers, vec![(1, CompactStr::new("b"))]);
        assert_eq!(idx.nodes[0].kind, NodeKind::EventStream);
        assert_eq!(idx.nodes[1].line_start, 2);
    }

    #[test]
    fn extract_file_cache_round_trip() {
        let idx = extract_file(
            Path::new("m.sql"),
            "SELECT pg_notify('x', '1');\nLISTEN y;\n",
            3,
        );
        let json = serde_json::to_string(&idx).unwrap();
        let back: FileIndex = serde_json::from_str(&json).unwrap();
        assert_eq!(back.producers, idx.producers);
        assert_eq!(back.consumers, idx.consumers);
        assert_eq!(back.nodes.len(), idx.nodes.len());
        assert_eq!(back.nodes[0].line_start, idx.nodes[0].line_start);
        assert_eq!(back.nodes[1].signature, idx.nodes[1].signature);
    }

    #[test]
    fn hostile_inputs_are_bounded() {
        let p = Path::new("x.sql");
        // Oversized, binary and long-line files are refused by the guard.
        assert!(extract_file(p, &"-- x\n".repeat(100_000), 0)
            .nodes
            .is_empty());
        assert!(extract_file(p, "NOTIFY a;\0\n", 0).nodes.is_empty());
        assert!(
            extract_file(p, &format!("NOTIFY a;{}\n", " ".repeat(10_000)), 0)
                .nodes
                .is_empty()
        );
        // Nesting depth 65 is refused.
        let deep = format!("{}1{}", "(".repeat(65), ")".repeat(65));
        assert!(extract_file(p, &format!("NOTIFY a;\n{deep}\n"), 0)
            .nodes
            .is_empty());
        // Unterminated quote, comment and dollar tag end at the end of the text.
        for t in ["'abc", "/* abc", "$tag$ abc", "\"abc", "E'a\\"] {
            let _ = scan_sql(t, 1);
        }
    }

    /// Hostile inputs on a 2 MB stack (the Rayon worker budget): nothing recurses, every probe
    /// ends, and the time of each is printed (`cargo test sql::tests::probes -- --nocapture`).
    #[test]
    fn probes_run_in_two_mb_stack() {
        let handle = std::thread::Builder::new()
            .stack_size(2 * 1024 * 1024)
            .spawn(|| {
                let p = Path::new("probe.sql");
                let stmt = "INSERT INTO outbox (topic, payload) VALUES ('t.one', '{}');\n";
                let big_ok = stmt.repeat(380_000 / stmt.len());
                let over = stmt.repeat(400_000 / stmt.len() + 1);
                let many: String = (0..2_000)
                    .map(|i| {
                        format!("INSERT INTO outbox (topic) VALUES ('t.{i}');\nNOTIFY c{i};\n")
                    })
                    .collect();
                let quotes = format!("{}\nNOTIFY after_quotes;\n", "'".repeat(1_000));
                let nest = format!(
                    "INSERT INTO outbox (topic) VALUES {}'x'{};\n",
                    "(".repeat(60),
                    ")".repeat(60)
                );
                let comments = format!(
                    "{}x{}\nNOTIFY after_comment;\n",
                    "/*".repeat(200),
                    "*/".repeat(200)
                );
                let long_line = format!("NOTIFY a;{}\n", " ".repeat(10_000));
                let binary = "NOTIFY a;\n\0\0\0".to_string();
                let probes: Vec<(&str, String, usize)> = vec![
                    ("380 KB of inserts (under budget)", big_ok, MAX_FACTS),
                    ("400 KB file (over budget)", over, 0),
                    ("2,000 statements", many, MAX_FACTS),
                    ("1,000 quotes then a statement", quotes, 0),
                    ("60-deep parentheses in VALUES", nest, 0),
                    ("200 nested comments", comments, 1),
                    ("10,000-character line", long_line, 0),
                    ("binary file named .sql", binary, 0),
                ];
                for (name, text, want) in probes {
                    let t = std::time::Instant::now();
                    let idx = extract_file(p, &text, 0);
                    let took = t.elapsed();
                    eprintln!("probe {name:<38} facts {:>5} in {took:?}", idx.nodes.len());
                    assert!(took < std::time::Duration::from_secs(3), "{name}: {took:?}");
                    if want == MAX_FACTS {
                        assert!(
                            idx.nodes.len() <= MAX_FACTS && !idx.nodes.is_empty(),
                            "{name}"
                        );
                    } else {
                        assert_eq!(idx.nodes.len(), want, "{name}");
                    }
                }
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn sql_path_is_case_insensitive() {
        assert!(is_sql_path(Path::new("a/B.SQL")));
        assert!(!is_sql_path(Path::new("a/b.sqlx")));
    }
}
