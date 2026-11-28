//! Queue names given as enum members (9.0 ticket T14).
//!
//! A BullMQ queue named `JobQueue.EMAIL` is the topic `email-jobs` when the enum that member
//! comes from says so. This module holds the string constants the workspace's source files
//! declare (`Enum.MEMBER = 'value'`) and answers, for one use site, which declaration it binds
//! to: through the file's own imports (a relative module path, an alias, a namespace import), and
//! otherwise through the same repository. A member two candidate declarations give different
//! values is resolved to neither (`MemberResolution::Ambiguous`), never to one of them.
//!
//! Constants of test files are never recorded, and only a value that looks like a name is kept:
//! a member named like a credential, or a value that is a token, a URL, a password-shaped word
//! or a random-looking run, is dropped, so the site keeps its qualified member key.

use crate::config_values::{name_shaped, plain_topic};
use crate::contracts::is_member_ref;
use crate::test_paths::is_test_path;
use crate::types::{CompactStr, RepoId};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::path::{Component, Path, PathBuf};

/// One `import { exported as local } from 'module'` of a source file (`exported` is `*` for a
/// namespace import).
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ImportBinding {
    pub local: CompactStr,
    pub exported: CompactStr,
    pub module: CompactStr,
}

/// What a member reference resolves to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemberResolution {
    /// The one string constant every candidate declaration agrees on.
    Value(CompactStr),
    /// Candidate declarations that give the member different values: neither is chosen.
    Ambiguous,
    /// No declaration is known (an enum outside the roots, a non-literal value, a test file).
    Unknown,
}

#[derive(Debug, Clone)]
struct FileConsts {
    repo: RepoId,
    package: CompactStr,
    records: Vec<(CompactStr, CompactStr)>,
}

#[derive(Debug, Clone)]
struct FileSites {
    repo: RepoId,
    imports: Vec<ImportBinding>,
    members: Vec<CompactStr>,
}

#[derive(Debug, Clone)]
struct Decl {
    file: PathBuf,
    repo: RepoId,
    package: CompactStr,
    value: CompactStr,
}

/// The string constants of the workspace and the declarations queue sites bind to.
#[derive(Debug, Default, Clone)]
pub struct MemberTable {
    files: BTreeMap<PathBuf, FileConsts>,
    sites: BTreeMap<PathBuf, FileSites>,
    /// `Enum.MEMBER` -> declarations, rebuilt by [`Self::rebuild`].
    index: HashMap<CompactStr, Vec<Decl>>,
    /// `(declaring file, enum)` pairs a queue site binds to.
    queue_decls: HashSet<(PathBuf, CompactStr)>,
    conflicts: usize,
    /// Source files holding a member topic: folded again when the table moves.
    ref_files: HashSet<PathBuf>,
}

/// Words that, as a whole word of a member name (`API_KEY`, `DB_PASSWORD`), mark it as naming a
/// credential.
const CREDENTIAL_WORDS: &[&str] = &[
    "password",
    "passwords",
    "passwd",
    "pwd",
    "pass",
    "secret",
    "secrets",
    "token",
    "tokens",
    "key",
    "apikey",
    "credential",
    "credentials",
    "dsn",
    "jwt",
    "passphrase",
];

/// Whole tokens that make a value a password, whatever surrounds them.
const WEAK_SECRET_TOKENS: &[&str] = &["hunter2", "passw0rd", "p4ssw0rd", "letmein", "changeme"];

/// Values that are a credential word and nothing else.
const BARE_SECRET_VALUES: &[&str] = &[
    "secret",
    "password",
    "passwd",
    "token",
    "apikey",
    "credential",
    "credentials",
    "passphrase",
    "jwt",
];

/// Whether a member is named like a credential (`API_KEY`, `DB_PASSWORD`): whole words of its
/// last segment, so `SECRETARY` is not one.
pub fn member_is_credential_named(member: &str) -> bool {
    let last = member
        .rsplit('.')
        .next()
        .unwrap_or(member)
        .to_ascii_lowercase();
    last.split('_').any(|w| CREDENTIAL_WORDS.contains(&w))
}

/// Words a placeholder or configuration KEY may not hold even inside a longer, separator-less
/// word (`DBPASSWORD`, `APIKEYQUEUE`, `SECRETQ`). Substrings, so they apply to keys only: a topic
/// VALUE such as `password-reset-jobs` or `secretary-jobs` stays a name.
const CREDENTIAL_KEY_PARTS: &[&str] = &[
    "password",
    "passwd",
    "passphrase",
    "secret",
    "token",
    "apikey",
    "credential",
    "accesskey",
    "privatekey",
    "connectionstring",
    "connstr",
    "sharedaccess",
];

/// Ordinary words that hold a [`CREDENTIAL_KEY_PARTS`] entry without naming a credential.
const CREDENTIAL_KEY_WORDS_OK: &[&str] = &["secretary", "secretaries", "secretariat", "tokenizer"];

/// Whether a placeholder or configuration key (`DB_PASSWORD`, `Bus:SharedAccessKey`,
/// `DBPASSWORDQ`) names a credential: [`member_is_credential_named`], the case-split rule of
/// [`identifier_is_credential_named`], and a credential word as a substring of any
/// separator-delimited part (9.0 T8d). Keys only; see [`constant_is_a_name`] for values.
pub fn placeholder_key_is_credential_named(key: &str) -> bool {
    if member_is_credential_named(&key.replace(['-', '.', ':'], "_"))
        || identifier_is_credential_named(key)
    {
        return true;
    }
    key.split(|c: char| !c.is_ascii_alphanumeric())
        .map(str::to_ascii_lowercase)
        .filter(|part| !CREDENTIAL_KEY_WORDS_OK.contains(&part.as_str()))
        .any(|part| CREDENTIAL_KEY_PARTS.iter().any(|w| part.contains(w)))
}

/// Whether a string value may become a topic name (commandment 5): a plain name that is not a
/// token, a URL, `Bearer_...`, a password, nor a random-looking run. `password-reset-jobs`,
/// `private-events` and `orders2024` are names; `hunter2-prod-x`, `tok_...` and `a3f9c1e7b2d4`
/// are not.
fn value_is_a_name(value: &str) -> bool {
    value_is_not_a_secret(value)
        // A run of six or more letters and digits with a digit anywhere but a trailing run of at
        // most four (`orders2024`, `v2`) is random-looking, not a word.
        && !value
            .split(|c: char| !c.is_ascii_alphanumeric())
            .any(|seg| {
                let body = {
                    let trailing = seg.chars().rev().take_while(char::is_ascii_digit).count();
                    if trailing <= 4 {
                        &seg[..seg.len() - trailing]
                    } else {
                        seg
                    }
                };
                seg.len() >= 6
                    && seg.chars().any(|c| c.is_ascii_alphabetic())
                    && body.chars().any(|c| c.is_ascii_digit())
            })
}

/// The part of [`value_is_a_name`] that is about secrets: a plain topic-shaped value that is no
/// token, URL, `Bearer_...` or weak password.
fn value_is_not_a_secret(value: &str) -> bool {
    if !name_shaped(value) || !plain_topic(value) {
        return false;
    }
    let lower = value.to_ascii_lowercase();
    if ["bearer", "basic_", "basic:", "tok_"]
        .iter()
        .any(|p| lower.starts_with(p))
    {
        return false;
    }
    let words: Vec<&str> = lower
        .split(['-', '_', '.', ':', '/'])
        .filter(|w| !w.is_empty())
        .collect();
    !(words.iter().any(|w| WEAK_SECRET_TOKENS.contains(w))
        || (words.len() == 1 && BARE_SECRET_VALUES.contains(&words[0])))
}

/// Whether an existing topic name may be PRINTED by a report (9.0 T9): [`value_is_a_name`]
/// without its random-looking-run rule, which exists to refuse a value as a NEW topic name and
/// would hide shard and id suffixes (`orders.shard00123`, `big.t00005`) from a display. A
/// segment of twenty or more letters and digits holding both is still taken for a token.
pub fn name_is_printable(value: &str) -> bool {
    value_is_not_a_secret(value)
        && !value
            .split(|c: char| !c.is_ascii_alphanumeric())
            .any(|seg| {
                seg.len() >= 20
                    && seg.chars().any(|c| c.is_ascii_alphabetic())
                    && seg.chars().any(|c| c.is_ascii_digit())
            })
}

/// Whether an identifier (a type or field name: `SecretsPayload`, `api_key`, `dbPassword`) holds a
/// credential word once split on case changes, digits and `_` / `-` (9.0 T11). The bare word
/// `key` alone is not one (`routingKey`, `orderKey` are names); `api` + `key` is.
pub fn identifier_is_credential_named(name: &str) -> bool {
    let mut words: Vec<String> = Vec::new();
    let mut cur = String::new();
    let mut prev: Option<char> = None;
    for c in name.chars() {
        let boundary = !c.is_ascii_alphabetic()
            || (c.is_ascii_uppercase() && prev.is_some_and(|p| p.is_ascii_lowercase()));
        if boundary && !cur.is_empty() {
            words.push(std::mem::take(&mut cur));
        }
        if c.is_ascii_alphabetic() {
            cur.push(c.to_ascii_lowercase());
        }
        prev = Some(c);
    }
    if !cur.is_empty() {
        words.push(cur);
    }
    words
        .iter()
        .any(|w| w != "key" && CREDENTIAL_WORDS.contains(&w.as_str()))
        || words.windows(2).any(|p| p[0] == "api" && p[1] == "key")
}

/// Whether a constant may be recorded and may name a topic: the member (or constant) is not
/// named like a credential and the value is a name. Applied where the constants are extracted, so
/// a refused value never reaches the graph or the parse cache.
pub fn constant_is_a_name(member: &str, value: &str) -> bool {
    !member_is_credential_named(member) && value_is_a_name(value)
}

/// Whether `path` is build output (`dist/`, `out/`, `.next/`, `coverage/`, ...): a copy of
/// sources that must not resolve a queue.
fn is_build_output(path: &Path) -> bool {
    path.components().any(|c| match c {
        Component::Normal(n) => n.to_str().is_some_and(|n| {
            matches!(
                n,
                "dist" | "out" | "build" | ".next" | ".nuxt" | ".output" | "coverage" | "target"
            )
        }),
        _ => false,
    })
}

/// The package a non-relative import specifier names, as the graph spells package names (npm
/// scope stripped, subpath dropped): `@org/queues/src/q` is `queues`.
fn specifier_package(module: &str) -> &str {
    let rest = match module.strip_prefix('@') {
        Some(scoped) => scoped.split_once('/').map_or("", |(_, name)| name),
        None => module,
    };
    rest.split('/').next().unwrap_or("")
}

fn strip_script_ext(path: &Path) -> PathBuf {
    match path.extension().and_then(|e| e.to_str()) {
        Some("ts" | "tsx" | "js" | "jsx" | "mjs" | "cjs" | "mts" | "cts") => {
            path.with_extension("")
        }
        _ => path.to_path_buf(),
    }
}

fn lexical(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for c in path.components() {
        match c {
            Component::CurDir => {}
            Component::ParentDir => {
                if !out.pop() {
                    out.push("..");
                }
            }
            other => out.push(other.as_os_str()),
        }
    }
    out
}

/// Whether `decl` is the file `module` names from `importer` (`./queues`, `../a/queues.js`,
/// `./queues/index`).
fn is_module_file(importer: &Path, module: &str, decl: &Path) -> bool {
    let Some(dir) = importer.parent() else {
        return false;
    };
    let target = strip_script_ext(&lexical(&dir.join(module)));
    let file = strip_script_ext(decl);
    file == target
        || (file.file_name().is_some_and(|n| n == "index") && file.parent() == Some(&target))
}

impl MemberTable {
    pub fn is_empty(&self) -> bool {
        self.files.is_empty() && self.sites.is_empty()
    }

    /// Sets (or clears) the constants `path` declares: `Enum.MEMBER=value` records, and the
    /// `package` its nodes belong to. A test file or a build-output file contributes nothing.
    pub fn set_file<'a>(
        &mut self,
        path: &Path,
        repo: RepoId,
        package: &str,
        records: impl Iterator<Item = &'a str>,
    ) {
        let records: Vec<(CompactStr, CompactStr)> = if is_test_path(path) || is_build_output(path)
        {
            Vec::new()
        } else {
            records
                .filter_map(|r| r.split_once('='))
                .filter(|(k, v)| k.len() <= 200 && is_member_ref(k) && constant_is_a_name(k, v))
                .map(|(k, v)| (CompactStr::new(k), CompactStr::new(v)))
                .collect()
        };
        if records.is_empty() {
            self.files.remove(path);
        } else {
            self.files.insert(
                path.to_path_buf(),
                FileConsts {
                    repo,
                    package: CompactStr::new(package),
                    records,
                },
            );
        }
    }

    /// Sets (or clears) the queue sites of `path`: the member paths it names a queue by and the
    /// imports that bind them.
    pub fn set_sites(
        &mut self,
        path: &Path,
        repo: RepoId,
        imports: Vec<ImportBinding>,
        members: Vec<CompactStr>,
    ) {
        if members.is_empty() {
            self.sites.remove(path);
        } else {
            self.sites.insert(
                path.to_path_buf(),
                FileSites {
                    repo,
                    imports,
                    members,
                },
            );
        }
    }

    pub fn remove_files(&mut self, paths: &HashSet<&Path>) {
        self.files.retain(|p, _| !paths.contains(p.as_path()));
        self.sites.retain(|p, _| !paths.contains(p.as_path()));
        self.ref_files.retain(|p| !paths.contains(p.as_path()));
    }

    /// Records whether the source file at `path` holds a member topic.
    pub fn note_ref_file(&mut self, path: &Path, has_member_topic: bool) {
        if has_member_topic {
            self.ref_files.insert(path.to_path_buf());
        } else {
            self.ref_files.remove(path);
        }
    }

    pub fn ref_files(&self) -> impl Iterator<Item = &Path> {
        self.ref_files.iter().map(PathBuf::as_path)
    }

    /// Distinct member paths a queue site could not choose between, because the declarations it
    /// can mean give different values (counted in `IndexHealth`): the site keeps its qualified
    /// member key.
    pub fn conflicts(&self) -> usize {
        self.conflicts
    }

    /// Rebuilds the declaration index and the set of queue declarations (call once after a
    /// batch of [`Self::set_file`] / [`Self::set_sites`]).
    pub fn rebuild(&mut self) {
        self.index.clear();
        for (path, f) in &self.files {
            for (key, value) in &f.records {
                self.index.entry(key.clone()).or_default().push(Decl {
                    file: path.clone(),
                    repo: f.repo,
                    package: f.package.clone(),
                    value: value.clone(),
                });
            }
        }
        let mut queue_decls = HashSet::new();
        let mut conflicting: HashSet<CompactStr> = HashSet::new();
        for (path, s) in &self.sites {
            for member in &s.members {
                let (decls, enum_name) = self.candidates(path, s.repo, &s.imports, member);
                if decls
                    .iter()
                    .map(|d| d.value.as_str())
                    .collect::<HashSet<_>>()
                    .len()
                    > 1
                {
                    conflicting.insert(member.clone());
                }
                if let Some(enum_name) = enum_name {
                    queue_decls.extend(decls.iter().map(|d| (d.file.clone(), enum_name.clone())));
                }
            }
        }
        self.queue_decls = queue_decls;
        self.conflicts = conflicting.len();
    }

    /// The declarations `member` (`Enum.MEMBER`, as written in `importer`) can be, and the
    /// declared enum's name: through the import that binds the enum's local name, else the
    /// importer's own declaration.
    fn candidates(
        &self,
        importer: &Path,
        repo: RepoId,
        imports: &[ImportBinding],
        member: &str,
    ) -> (Vec<&Decl>, Option<CompactStr>) {
        let Some((first, rest)) = member.split_once('.') else {
            return (Vec::new(), None);
        };
        let import = imports.iter().find(|i| i.local == first);
        let (key, module): (String, Option<&str>) = match import {
            None => (member.to_string(), None),
            // `import * as Qs from './queues'; Qs.JobQueue.REPORTS`.
            Some(i) if i.exported == "*" => {
                if !rest.contains('.') {
                    return (Vec::new(), None);
                }
                (rest.to_string(), Some(i.module.as_str()))
            }
            Some(i) if i.exported == "default" => return (Vec::new(), None),
            Some(i) => (format!("{}.{rest}", i.exported), Some(i.module.as_str())),
        };
        let enum_name = key.split('.').next().map(CompactStr::new);
        let Some(decls) = self.index.get(key.as_str()) else {
            return (Vec::new(), enum_name);
        };
        let chosen: Vec<&Decl> = match module {
            None => decls.iter().filter(|d| d.file == importer).collect(),
            Some(m) => {
                let by_path: Vec<&Decl> = if m.starts_with('.') {
                    decls
                        .iter()
                        .filter(|d| is_module_file(importer, m, &d.file))
                        .collect()
                } else {
                    Vec::new()
                };
                if !by_path.is_empty() {
                    by_path
                } else if m.starts_with('.') || m.starts_with('/') {
                    // A relative path that names no file declaring the enum (a barrel that
                    // re-exports it): the declarations of the importer's own repository.
                    decls.iter().filter(|d| d.repo == repo).collect()
                } else {
                    // A package specifier binds only to the declarations of the package it
                    // names (the package.json name the graph already gives every node), never
                    // to a homonymous enum of the importer's own repository.
                    let package = specifier_package(m);
                    let named: Vec<&Decl> = decls
                        .iter()
                        .filter(|d| !package.is_empty() && d.package == package)
                        .collect();
                    let same_repo: Vec<&Decl> =
                        named.iter().copied().filter(|d| d.repo == repo).collect();
                    if same_repo.is_empty() {
                        named
                    } else {
                        same_repo
                    }
                }
            }
        };
        (chosen, enum_name)
    }

    /// What `member`, written in `importer`, names.
    pub fn resolve(
        &self,
        importer: &Path,
        repo: RepoId,
        imports: &[ImportBinding],
        member: &str,
    ) -> MemberResolution {
        let (decls, _) = self.candidates(importer, repo, imports, member);
        let values: HashSet<&str> = decls.iter().map(|d| d.value.as_str()).collect();
        match values.len() {
            0 => MemberResolution::Unknown,
            1 => MemberResolution::Value(CompactStr::new(
                values.into_iter().next().unwrap_or_default(),
            )),
            _ => MemberResolution::Ambiguous,
        }
    }

    /// Whether `member`, written in `importer`, binds to declarations that a queue site names a
    /// queue by (every candidate is a queue declaration, and there is one): what lets a custom
    /// pattern's capture of that member join the queue.
    pub fn is_queue_member(
        &self,
        importer: &Path,
        repo: RepoId,
        imports: &[ImportBinding],
        member: &str,
    ) -> bool {
        let (decls, enum_name) = self.candidates(importer, repo, imports, member);
        let Some(enum_name) = enum_name else {
            return false;
        };
        !decls.is_empty()
            && decls.iter().all(|d| {
                self.queue_decls
                    .contains(&(d.file.clone(), enum_name.clone()))
            })
    }

    /// An order-independent digest of what resolution depends on (call after [`Self::rebuild`]).
    pub fn digest(&self) -> u64 {
        let one = |t: &dyn Fn(&mut std::collections::hash_map::DefaultHasher)| {
            let mut h = std::collections::hash_map::DefaultHasher::new();
            t(&mut h);
            h.finish()
        };
        let decls = self.index.iter().map(|(k, v)| {
            one(&|h| {
                k.hash(h);
                let mut v: Vec<_> = v.iter().map(|d| (&d.file, d.repo, &d.value)).collect();
                v.sort();
                v.hash(h);
            })
        });
        let queues = self.queue_decls.iter().map(|q| one(&|h| q.hash(h)));
        decls
            .chain(queues)
            .fold(self.index.len() as u64, |a, d| a ^ d)
    }

    /// Canonical lines for the snapshot fingerprint.
    pub fn canonical_lines_rooted(&self, roots: &[PathBuf]) -> Vec<String> {
        let rel = |p: &Path| crate::state::root_relative_path(p, roots, None);
        let mut lines: Vec<String> = self
            .files
            .iter()
            .flat_map(|(path, f)| {
                let source = rel(path);
                f.records.iter().map(move |(k, v)| {
                    serde_json::json!(["member-const", k.as_str(), v.as_str(), source]).to_string()
                })
            })
            .collect();
        lines.extend(
            self.queue_decls
                .iter()
                .map(|(p, e)| serde_json::json!(["queue-decl", rel(p), e.as_str()]).to_string()),
        );
        lines.sort_unstable();
        lines
    }
}

#[cfg(test)]
mod tests {

    /// 9.0 T8d: a credential word inside a separator-less key is refused; a plain key and an
    /// ordinary word that holds one are not.
    #[test]
    fn placeholder_keys_with_a_credential_word_inside_are_refused() {
        for key in [
            "DBPASSWORD",
            "APIKEYQUEUE",
            "DBPASSWORDQ",
            "PASSWORDS_Q",
            "SECRETQ",
            "dbpassword",
            "Bus:SharedAccessKey",
            "DbPasswordQueue",
            "A__Token",
            "Db_ConnectionString",
            "PASSWORDS",
        ] {
            assert!(placeholder_key_is_credential_named(key), "{key}");
        }
        for key in [
            "ORDERS_QUEUE",
            "OrdersQueue",
            "SECRETARY_QUEUE",
            "KEYSTONE_Q",
            "Billing__Topic",
            "routingKey",
        ] {
            assert!(!placeholder_key_is_credential_named(key), "{key}");
        }
        assert!(member_is_credential_named("DB_PASSWORDS"));
    }
    use super::*;

    type SiteSpec<'a> = (&'a str, &'a [&'a str], &'a [(&'a str, &'a str, &'a str)]);

    fn table(files: &[(&str, &[&str])], sites: &[SiteSpec]) -> MemberTable {
        let mut t = MemberTable::default();
        for (p, records) in files {
            // The package of a fixture file is its directory's name (`/w/a/q.ts` is `a`).
            let package = Path::new(p)
                .parent()
                .and_then(|d| d.file_name())
                .and_then(|n| n.to_str())
                .unwrap_or("");
            t.set_file(Path::new(p), 0, package, records.iter().copied());
        }
        for (p, members, imports) in sites {
            let imports = imports
                .iter()
                .map(|(l, e, m)| ImportBinding {
                    local: (*l).into(),
                    exported: (*e).into(),
                    module: (*m).into(),
                })
                .collect();
            t.set_sites(
                Path::new(p),
                0,
                imports,
                members.iter().map(|m| CompactStr::new(*m)).collect(),
            );
        }
        t.rebuild();
        t
    }

    fn imp() -> [ImportBinding; 1] {
        [ImportBinding {
            local: "JobQueue".into(),
            exported: "JobQueue".into(),
            module: "./queues".into(),
        }]
    }

    #[test]
    fn a_member_resolves_through_its_import_not_through_its_name() {
        let t = table(
            &[
                ("/w/a/queues.ts", &["JobQueue.EMAIL=a-email"]),
                ("/w/b/queues.ts", &["JobQueue.EMAIL=b-email"]),
            ],
            &[],
        );
        let r = |importer: &str| t.resolve(Path::new(importer), 0, &imp(), "JobQueue.EMAIL");
        assert_eq!(r("/w/a/w.ts"), MemberResolution::Value("a-email".into()));
        assert_eq!(r("/w/b/w.ts"), MemberResolution::Value("b-email".into()));
        // No import binds it, so nothing says which: neither.
        assert_eq!(
            t.resolve(Path::new("/w/c/w.ts"), 0, &[], "JobQueue.EMAIL"),
            MemberResolution::Unknown
        );
        // A package specifier names a package: `@acme/q` is no package of this workspace, so
        // the homonymous `JobQueue` of the importer's own repository is not a guess.
        let pkg = |m: &str| {
            [ImportBinding {
                local: "JobQueue".into(),
                exported: "JobQueue".into(),
                module: m.into(),
            }]
        };
        let by_pkg =
            |importer: &str, m: &str| t.resolve(Path::new(importer), 0, &pkg(m), "JobQueue.EMAIL");
        assert_eq!(by_pkg("/w/c/w.ts", "@acme/q"), MemberResolution::Unknown);
        assert_eq!(
            by_pkg("/w/a/w.ts", "legacy-queue-lib"),
            MemberResolution::Unknown
        );
        // The package it names: `@org/b` (npm scope stripped, subpath dropped) is package `b`.
        assert_eq!(
            by_pkg("/w/a/w.ts", "@org/b/src/queues"),
            MemberResolution::Value("b-email".into())
        );
        assert_eq!(
            by_pkg("/w/c/w.ts", "a"),
            MemberResolution::Value("a-email".into())
        );
    }

    #[test]
    fn a_member_two_declarations_of_one_package_disagree_on_is_ambiguous_and_counted() {
        let t = table(
            &[
                ("/w/a/one.ts", &["JobQueue.EMAIL=one-email"]),
                ("/w/a/two.ts", &["JobQueue.EMAIL=two-email"]),
            ],
            &[(
                "/w/c/w.ts",
                &["JobQueue.EMAIL"],
                &[("JobQueue", "JobQueue", "@org/a")],
            )],
        );
        let imp = [ImportBinding {
            local: "JobQueue".into(),
            exported: "JobQueue".into(),
            module: "@org/a".into(),
        }];
        assert_eq!(
            t.resolve(Path::new("/w/c/w.ts"), 0, &imp, "JobQueue.EMAIL"),
            MemberResolution::Ambiguous
        );
        assert_eq!(t.conflicts(), 1);
        // Two files each with their own enum, every site resolved: no conflict to report.
        let t = table(
            &[
                ("/w/a/queues.ts", &["JobQueue.EMAIL=a-email"]),
                ("/w/b/queues.ts", &["JobQueue.EMAIL=b-email"]),
            ],
            &[
                (
                    "/w/a/w.ts",
                    &["JobQueue.EMAIL"],
                    &[("JobQueue", "JobQueue", "./queues")],
                ),
                (
                    "/w/b/w.ts",
                    &["JobQueue.EMAIL"],
                    &[("JobQueue", "JobQueue", "./queues")],
                ),
            ],
        );
        assert_eq!(t.conflicts(), 0);
    }

    #[test]
    fn build_output_declarations_never_resolve() {
        let mut t = MemberTable::default();
        t.set_file(
            Path::new("/w/packages/queues/dist/queues.ts"),
            0,
            "queues",
            ["DistQ.EMAIL=dist-email"].into_iter(),
        );
        t.set_file(
            Path::new("/w/packages/queues/src/queues.ts"),
            0,
            "queues",
            ["JobQueue.EMAIL=pkg-email"].into_iter(),
        );
        t.rebuild();
        let imp = |local: &str, m: &str| {
            [ImportBinding {
                local: local.into(),
                exported: local.into(),
                module: m.into(),
            }]
        };
        assert_eq!(
            t.resolve(
                Path::new("/w/app/s.ts"),
                0,
                &imp("DistQ", "@org/queues/dist/queues"),
                "DistQ.EMAIL"
            ),
            MemberResolution::Unknown
        );
        assert_eq!(
            t.resolve(
                Path::new("/w/app/s.ts"),
                0,
                &imp("JobQueue", "@org/queues"),
                "JobQueue.EMAIL"
            ),
            MemberResolution::Value("pkg-email".into())
        );
    }

    #[test]
    fn names_and_credentials_as_values() {
        for ok in [
            "email-jobs",
            "user-queue",
            "fallback-user-queue",
            "send_welcome_email",
            "mailbox",
            "queue-v2",
            "invoice-batch-v2024",
            "deadletter-q",
            "password-reset-jobs",
            "token-refresh-jobs",
            "private-events",
            "secretary-jobs",
            "orders2024",
            "tenant-sync",
        ] {
            assert!(constant_is_a_name("Q.ANY", ok), "{ok}");
        }
        for bad in [
            "Bearer_CANARYB1",
            "hunter2-prod-CANARYX",
            concat!("sk_", "live_CANARYSKabcdefgh"),
            "postgres://u:CANARYPW@h",
            "dGhpcyBpcyBhIHNlY3JldA==",
            "tok_CANARYJ123",
            "eyJhbGciOiJIUzI1NiJ9.CANARYK.sig",
            "a3f9c1e7b2d4",
            "password",
            "two words",
            "Bearer_CANARY_F_eyJhbGciOiJIUzI1NiJ9",
        ] {
            assert!(!constant_is_a_name("Q.ANY", bad), "{bad}");
        }
        // Whole words of the member: `API_KEY` is a credential, `SECRETARY` is not.
        assert!(!constant_is_a_name("Sec.API_KEY", "legit-api-jobs"));
        assert!(!constant_is_a_name("Sec.DB_PASSWORD", "db-jobs"));
        assert!(constant_is_a_name("Q.SECRETARY", "secretary-jobs"));
    }

    #[test]
    fn aliases_namespaces_and_index_files_bind() {
        let t = table(
            &[
                ("/w/q/index.ts", &["JobQueue.EMAIL=idx-email"]),
                ("/w/q/other.ts", &["JobQueue.REPORTS=rep"]),
            ],
            &[],
        );
        let alias = [ImportBinding {
            local: "Q".into(),
            exported: "JobQueue".into(),
            module: "./q".into(),
        }];
        assert_eq!(
            t.resolve(Path::new("/w/w.ts"), 0, &alias, "Q.EMAIL"),
            MemberResolution::Value("idx-email".into())
        );
        let ns = [ImportBinding {
            local: "Qs".into(),
            exported: "*".into(),
            module: "./q/other.js".into(),
        }];
        assert_eq!(
            t.resolve(Path::new("/w/w.ts"), 0, &ns, "Qs.JobQueue.REPORTS"),
            MemberResolution::Value("rep".into())
        );
    }

    #[test]
    fn test_files_and_secret_looking_constants_are_not_recorded() {
        let t = table(
            &[
                ("/w/src/__tests__/q.spec.ts", &["JobQueue.EMAIL=TEST-ONLY"]),
                (
                    "/w/q.ts",
                    &[
                        "Q.A=sk_live_CANARYA1234567890abcdef",
                        "Q.B=postgres://admin:CANARY_PW_B@db.internal/x",
                        "Q.C=hunter2-prod-CANARYC",
                        "Q.D=ghp_CANARYDTOKEN0123456789abcdefghij",
                        "Q.E=jdbc:mysql://h/db?password=CANARY_PW_E",
                        "Q.F=Bearer_CANARY_F_eyJhbGciOiJIUzI1NiJ9",
                        "Q.PASSWORD=plain-name",
                        "Q.API_KEY=plain-name",
                        "Q.OK=report-jobs",
                    ],
                ),
            ],
            &[],
        );
        let imp = [ImportBinding {
            local: "Q".into(),
            exported: "Q".into(),
            module: "./q".into(),
        }];
        for m in ["A", "B", "C", "D", "E", "F", "PASSWORD", "API_KEY"] {
            assert_eq!(
                t.resolve(Path::new("/w/w.ts"), 0, &imp, &format!("Q.{m}")),
                MemberResolution::Unknown,
                "{m}"
            );
        }
        assert_eq!(
            t.resolve(Path::new("/w/w.ts"), 0, &imp, "Q.OK"),
            MemberResolution::Value("report-jobs".into())
        );
        let none = [ImportBinding {
            local: "JobQueue".into(),
            exported: "JobQueue".into(),
            module: "./src/__tests__/q.spec".into(),
        }];
        assert_eq!(
            t.resolve(Path::new("/w/w.ts"), 0, &none, "JobQueue.EMAIL"),
            MemberResolution::Unknown
        );
    }

    #[test]
    fn only_a_declaration_a_queue_site_binds_to_is_a_queue_declaration() {
        let t = table(
            &[
                ("/w/a/queues.ts", &["JobQueue.EMAIL=a-email"]),
                ("/w/b/enums.ts", &["JobQueue.EMAIL=b-email"]),
            ],
            &[(
                "/w/a/worker.ts",
                &["JobQueue.EMAIL"],
                &[("JobQueue", "JobQueue", "./queues")],
            )],
        );
        let imp_b = [ImportBinding {
            local: "JobQueue".into(),
            exported: "JobQueue".into(),
            module: "./enums".into(),
        }];
        assert!(t.is_queue_member(Path::new("/w/a/outbox.ts"), 0, &imp(), "JobQueue.EMAIL"));
        assert!(!t.is_queue_member(Path::new("/w/b/k.ts"), 0, &imp_b, "JobQueue.EMAIL"));
        assert!(!t.is_queue_member(Path::new("/w/b/k.ts"), 0, &[], "JobQueue.EMAIL"));
    }

    #[test]
    fn the_digest_moves_with_declarations_and_nothing_else() {
        let one = table(&[("/w/a.ts", &["Q.A=one"])], &[]);
        let same = table(&[("/w/a.ts", &["Q.A=one"])], &[]);
        let other = table(&[("/w/a.ts", &["Q.A=two"])], &[]);
        assert_eq!(one.digest(), same.digest());
        assert_ne!(one.digest(), other.digest());
        assert_ne!(one.digest(), MemberTable::default().digest());
    }
}
