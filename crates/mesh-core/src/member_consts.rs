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

use crate::config_values::{name_shaped, plain_topic, secret_key};
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

/// Whether a constant may become a topic name (commandment 5): the member is not named like a
/// credential and the value is a plain name, not a token, a URL, a password-shaped word or a
/// random-looking run.
pub fn constant_is_a_name(member: &str, value: &str) -> bool {
    let last = member.rsplit('.').next().unwrap_or(member);
    if secret_key(last) || !name_shaped(value) || !plain_topic(value) {
        return false;
    }
    let lower = value.to_ascii_lowercase();
    const WORDS: &[&str] = &[
        "secret",
        "token",
        "password",
        "passwd",
        "credential",
        "apikey",
        "api_key",
        "api-key",
        "bearer",
        "jwt",
        "passphrase",
        "private",
        "dsn",
    ];
    if WORDS.iter().any(|w| lower.contains(w))
        || lower.starts_with("basic_")
        || lower.starts_with("basic:")
    {
        return false;
    }
    // `hunter2`, `eyJhbGci...`, `a1b2c3d4e5`: a run of six or more letters and digits that has
    // both is random-looking, not a word.
    !value
        .split(|c: char| !c.is_ascii_alphanumeric())
        .any(|seg| {
            seg.len() >= 6
                && seg.chars().any(|c| c.is_ascii_digit())
                && seg.chars().any(|c| c.is_ascii_alphabetic())
        })
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

    /// Sets (or clears) the constants `path` declares: `Enum.MEMBER=value` records. A test file
    /// contributes nothing.
    pub fn set_file<'a>(
        &mut self,
        path: &Path,
        repo: RepoId,
        records: impl Iterator<Item = &'a str>,
    ) {
        let records: Vec<(CompactStr, CompactStr)> = if is_test_path(path) {
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
            self.files
                .insert(path.to_path_buf(), FileConsts { repo, records });
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

    /// Distinct member paths declared with different values in different files (counted in
    /// `IndexHealth`): a site that cannot tell which one it imports keeps its member key.
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
                    value: value.clone(),
                });
            }
        }
        self.conflicts = self
            .index
            .values()
            .filter(|decls| {
                decls
                    .iter()
                    .map(|d| d.value.as_str())
                    .collect::<HashSet<_>>()
                    .len()
                    > 1
            })
            .count();
        let mut queue_decls = HashSet::new();
        for (path, s) in &self.sites {
            for member in &s.members {
                let (decls, enum_name) = self.candidates(path, s.repo, &s.imports, member);
                if let Some(enum_name) = enum_name {
                    queue_decls.extend(decls.iter().map(|d| (d.file.clone(), enum_name.clone())));
                }
            }
        }
        self.queue_decls = queue_decls;
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
                } else {
                    // A package, a barrel, an unresolved path: the declarations of the
                    // importer's own repository, else every one there is.
                    let same_repo: Vec<&Decl> = decls.iter().filter(|d| d.repo == repo).collect();
                    if same_repo.is_empty() {
                        decls.iter().collect()
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
    use super::*;

    type SiteSpec<'a> = (&'a str, &'a [&'a str], &'a [(&'a str, &'a str, &'a str)]);

    fn table(files: &[(&str, &[&str])], sites: &[SiteSpec]) -> MemberTable {
        let mut t = MemberTable::default();
        for (p, records) in files {
            t.set_file(Path::new(p), 0, records.iter().copied());
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
        let pkg = [ImportBinding {
            local: "JobQueue".into(),
            exported: "JobQueue".into(),
            module: "@acme/q".into(),
        }];
        assert_eq!(
            t.resolve(Path::new("/w/c/w.ts"), 0, &pkg, "JobQueue.EMAIL"),
            MemberResolution::Ambiguous
        );
        assert_eq!(t.conflicts(), 1);
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
