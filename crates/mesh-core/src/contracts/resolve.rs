use super::*;

impl ContractGraph {
    /// Splits a fully-qualified identifier into `(package, bare_name)`.
    ///
    /// Covers Java-style dotted FQCNs (`com.acme.billing.Invoice`) and
    /// Rust-style `::`-separated paths (`crate::billing::Invoice`). `::` is
    /// checked first since it never appears in a dotted path and a Rust path
    /// segment can itself legitimately contain a `.` (rare, but this keeps
    /// the two syntaxes from being conflated).
    ///
    /// The Rust-style package is normalized from `::` to `.` before being
    /// returned, since `ContractNode::package` is always stored dotted
    /// (`detect_service_package`, proto `package` statements, Java FQCNs).
    /// Without this normalization a `crate::billing::Invoice` import could
    /// never match a node whose package is recorded as `crate.billing`.
    pub(super) fn split_fully_qualified(target_str: &str) -> Option<(Cow<'_, str>, &str)> {
        if let Some(idx) = target_str.rfind("::") {
            let (pkg, name) = (&target_str[..idx], &target_str[idx + 2..]);
            if !pkg.is_empty() && !name.is_empty() {
                return Some((Cow::Owned(pkg.replace("::", ".")), name));
            }
        }
        if let Some((pkg, name)) = target_str.rsplit_once('.') {
            if !pkg.is_empty() && !name.is_empty() {
                return Some((Cow::Borrowed(pkg), name));
            }
        }
        None
    }

    /// `ids.len() == 1`, or exactly one candidate whose `repo_id` matches
    /// `importer_repo` — a legitimate, non-arbitrary tiebreak — returns that one
    /// candidate at `confidence_if_unique`. Otherwise this is a genuine tie the
    /// graph cannot resolve on its own: every candidate (or every same-repo
    /// candidate, when more than one shares the importer's repo) is returned
    /// tagged `Ambiguous`, rather than arbitrarily picking whichever one the
    /// caller's index happened to list first. That pick used to depend on
    /// `HashMap` iteration order and file processing order — a hidden source of
    /// nondeterminism (idempotence invariant I1) as well as a silently wrong
    /// answer for a real homonym (e.g. two proto packages each declaring their
    /// own `AdminService`).
    fn pick_or_ambiguous(
        &self,
        ids: &[NodeId],
        importer_repo: Option<RepoId>,
        confidence_if_unique: EdgeConfidence,
    ) -> Vec<(NodeId, EdgeConfidence)> {
        if let [only] = ids {
            return vec![(*only, confidence_if_unique)];
        }
        let same_repo: Vec<NodeId> = ids
            .iter()
            .copied()
            .filter(|id| self.nodes.get(id).map(|n| n.repo_id) == importer_repo)
            .collect();
        match same_repo.as_slice() {
            [only] => vec![(*only, confidence_if_unique)],
            [] => ids
                .iter()
                .map(|&id| (id, EdgeConfidence::Ambiguous))
                .collect(),
            _ => same_repo
                .into_iter()
                .map(|id| (id, EdgeConfidence::Ambiguous))
                .collect(),
        }
    }

    /// Wraps each candidate list with its ids grouped by declared `package`, for
    /// [`PackageBucket::pick_or_ambiguous`].
    pub(super) fn group_by_package(
        &self,
        buckets: HashMap<String, Vec<NodeId>>,
    ) -> HashMap<String, PackageBucket<'_>> {
        buckets
            .into_iter()
            .map(|(key, ids)| {
                let mut by_package: HashMap<&str, Vec<NodeId>> = HashMap::new();
                if ids.len() > 1 {
                    for &id in &ids {
                        if let Some(n) = self.nodes.get(&id) {
                            by_package.entry(n.package.as_str()).or_default().push(id);
                        }
                    }
                }
                (key, PackageBucket { ids, by_package })
            })
            .collect()
    }

    /// Resolves an `Imports` fact (`importer` imports `target_str`) using the O(1)
    /// indices. Tries each strategy in priority order and returns as soon as one
    /// produces any candidate(s) — one unambiguous winner, or every tied candidate
    /// tagged `Ambiguous` (see [`Self::pick_or_ambiguous`]). An empty result means
    /// no strategy matched at all (e.g. an external package like `@nestjs/common`).
    ///
    /// `memo` carries the per-bucket work shared by distinct targets (see
    /// [`ImportMemo`]), so no bucket is scanned more than once per repo.
    pub(super) fn resolve_import_targets<'g>(
        &'g self,
        importer_repo: Option<RepoId>,
        importer_dir: Option<&Path>,
        target_str: &str,
        memo: &mut ImportMemo<'g>,
    ) -> Vec<(NodeId, EdgeConfidence)> {
        let is_relative_or_absolute_path =
            target_str.starts_with('.') || target_str.starts_with('/');

        // 1. Fully-qualified name (Java `a.b.C`, Rust `a::b::C`, gRPC
        // `package/Name`): an exact package+name pair is unambiguous, so
        // this is tried first and is the only strategy tagged `Exact`.
        if !is_relative_or_absolute_path {
            if let Some((pkg, name)) = Self::split_fully_qualified(target_str) {
                if let Some((bucket_key, ids)) = self.name_to_nodes.get_key_value(name) {
                    let by_package = memo
                        .first_by_package
                        .entry(bucket_key.as_str())
                        .or_insert_with(|| {
                            let mut first = HashMap::new();
                            for id in ids {
                                if let Some(n) = self.nodes.get(id) {
                                    first.entry(n.package.as_str()).or_insert(*id);
                                }
                            }
                            first
                        });
                    if let Some(&id) = by_package.get(pkg.as_ref()) {
                        return vec![(id, EdgeConfidence::Exact)];
                    }
                }
                let fqcn_key = format!("{pkg}/{name}");
                if let Some(ids) = self.fqcn_to_node.get(fqcn_key.as_str()) {
                    return self.pick_or_ambiguous(ids, importer_repo, EdgeConfidence::Exact);
                }
            }
        }

        // A Node.js built-in module name (`fs`, `path`, `os`) resolves to a
        // symbol of the importer's own repository at most — never across
        // repos: `import * as path from 'path'` matched a `const path =
        // require(…)` in another repo, and 7.0.6's mesh drew every backend
        // service importing `nativapp`. A local symbol that happens to be
        // named `events` or `constants` still resolves.
        if Self::is_node_builtin_module(target_str) {
            let Some(ids) = self.name_to_nodes.get(target_str) else {
                return Vec::new();
            };
            let same_repo: Vec<NodeId> = ids
                .iter()
                .copied()
                .filter(|id| self.nodes.get(id).map(|n| n.repo_id) == importer_repo)
                .collect();
            if same_repo.is_empty() {
                return Vec::new();
            }
            return self.pick_or_ambiguous(&same_repo, importer_repo, EdgeConfidence::Heuristic);
        }

        // 2. Exact symbol name anywhere in the mesh — a bare name, so two
        // unrelated types sharing it would collide; heuristic.
        // Prioritize a symbol in the same repository as the importer to avoid cross-repo false links.
        if let Some(ids) = self.name_to_nodes.get(target_str) {
            return self.pick_or_ambiguous(ids, importer_repo, EdgeConfidence::Heuristic);
        }

        let is_qualified = target_str.contains('/') || target_str.contains('.');

        // 3. Relative import: match on file stem, same repo as the importer only
        // (no cross-repo fallback — a relative import can never legitimately
        // resolve outside the importer's own repository).
        if is_relative_or_absolute_path {
            let target_stem = Path::new(target_str)
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or(target_str);
            if let Some((bucket_key, ids)) = self.name_to_nodes.get_key_value(target_stem) {
                // Memoized per (stem, repo): `./a/index`, `../b/index`, ... are
                // distinct targets that all rescan the one `index` bucket.
                let same_repo = memo
                    .same_repo_by_stem
                    .entry((bucket_key.as_str(), importer_repo))
                    .or_insert_with(|| {
                        ids.iter()
                            .copied()
                            .filter(|id| self.nodes.get(id).map(|n| n.repo_id) == importer_repo)
                            .collect()
                    });
                // From a script, the symbol must live in the file the specifier
                // names: `import x from '@/lib/utils'` (rewritten to a relative
                // path) is not a reference to any other `utils` in the repo.
                let in_target_file: Vec<NodeId>;
                let candidates: &[NodeId] = match importer_dir {
                    Some(dir) => {
                        let target = module_key(&dir.join(target_str));
                        let node_keys = &mut memo.node_keys;
                        in_target_file = same_repo
                            .iter()
                            .copied()
                            .filter(|id| {
                                let Some(node) = self.nodes.get(id) else {
                                    return false;
                                };
                                let key = node_keys
                                    .entry(*id)
                                    .or_insert_with(|| module_key(&node.file_path));
                                module_key_matches(key, &target)
                                    && extension_compatible(&node.file_path, target_str)
                            })
                            .collect();
                        &in_target_file
                    }
                    None => same_repo,
                };
                if !candidates.is_empty() {
                    return self.pick_or_ambiguous(
                        candidates,
                        importer_repo,
                        EdgeConfidence::Heuristic,
                    );
                }
            }
        }

        // 4. Qualified package identifier (e.g. '@scope/pkg', 'com.acme.billing')
        // matched against a whole package, not a specific symbol; heuristic.
        // Prioritize package nodes in the importer's repository if available.
        if is_qualified && !is_relative_or_absolute_path {
            if let Some(ids) = self.package_to_nodes.get(target_str) {
                return self.pick_or_ambiguous(ids, importer_repo, EdgeConfidence::Heuristic);
            }
        }

        Vec::new()
    }

    /// `fs`, `node:fs`, `fs/promises`, … — a Node.js built-in module specifier.
    fn is_node_builtin_module(specifier: &str) -> bool {
        const BUILTINS: &[&str] = &[
            "assert",
            "async_hooks",
            "buffer",
            "child_process",
            "cluster",
            "console",
            "constants",
            "crypto",
            "dgram",
            "diagnostics_channel",
            "dns",
            "domain",
            "events",
            "fs",
            "http",
            "http2",
            "https",
            "inspector",
            "module",
            "net",
            "os",
            "path",
            "perf_hooks",
            "process",
            "punycode",
            "querystring",
            "readline",
            "repl",
            "stream",
            "string_decoder",
            "timers",
            "tls",
            "trace_events",
            "tty",
            "url",
            "util",
            "v8",
            "vm",
            "wasi",
            "worker_threads",
            "zlib",
        ];
        if specifier.starts_with("node:") {
            return true;
        }
        let head = specifier.split('/').next().unwrap_or(specifier);
        BUILTINS.contains(&head)
    }

    /// One RPC target resolved against the declared services/methods:
    /// 1. exact FQCN match has top priority and highest confidence;
    /// 2. bare-name fallback: lower confidence.
    ///
    /// At either tier, multiple candidates are disambiguated by caller package;
    /// a genuine tie (no candidate shares the caller's package, or more than
    /// one does) fans out to every tied candidate as `Ambiguous` rather than
    /// picking whichever one the index happened to list first.
    pub(super) fn resolve_rpc_target(
        target_str: &str,
        caller_package: Option<&CompactStr>,
        proto_by_fqcn: &HashMap<String, PackageBucket<'_>>,
        proto_by_bare: &HashMap<String, PackageBucket<'_>>,
    ) -> Vec<(NodeId, EdgeConfidence)> {
        let target_lower = target_str.to_lowercase();
        let target_bare = target_str.split('.').next_back().unwrap_or(target_str);
        let target_bare_lower = target_bare.to_lowercase();
        if let Some(candidates) = proto_by_fqcn.get(&target_lower) {
            match caller_package {
                Some(pkg) => candidates.pick_or_ambiguous(pkg, EdgeConfidence::Exact),
                None => candidates
                    .ids
                    .iter()
                    .map(|&id| (id, EdgeConfidence::Exact))
                    .collect(),
            }
        } else if let Some(candidates) = proto_by_bare.get(&target_bare_lower) {
            match caller_package {
                Some(pkg) => candidates.pick_or_ambiguous(pkg, EdgeConfidence::Heuristic),
                None => candidates
                    .ids
                    .iter()
                    .map(|&id| (id, EdgeConfidence::Ambiguous))
                    .collect(),
            }
        } else {
            Vec::new()
        }
    }

    /// `grpc.health.v1.Health` (or one of its methods) named by its full
    /// proto name — unambiguous whatever the graph declares.
    pub(super) fn is_grpc_health_fqcn(target: &str) -> bool {
        const HEALTH: &str = "grpc.health.v1.health";
        target.len() >= HEALTH.len()
            && target.is_char_boundary(HEALTH.len())
            && target[..HEALTH.len()].eq_ignore_ascii_case(HEALTH)
            && (target.len() == HEALTH.len() || target.as_bytes()[HEALTH.len()] == b'.')
    }

    /// Whether a resolved RPC target node is the standard gRPC health
    /// service: a `Health` service/method declared in the `grpc.health.v1`
    /// proto package, or a `Health` service a language extractor inferred
    /// from code (e.g. Go's `healthpb.RegisterHealthServer(...)`), which with
    /// no `.proto` of its own behind it is the stock health server. A
    /// `service Health` the repo declares in its *own* proto package is a
    /// real service and is kept.
    pub(super) fn is_grpc_health_service(node: &ContractNode) -> bool {
        let service_segment = match node.kind {
            NodeKind::GrpcService => node.name.split('.').next_back(),
            NodeKind::GrpcMethod => node.name.rsplit('.').nth(1),
            _ => None,
        };
        service_segment.is_some_and(|s| s.eq_ignore_ascii_case("health"))
            && (node.package.as_str() == "grpc.health.v1" || !Self::is_proto_file(&node.file_path))
    }

    #[inline]
    pub(super) fn is_proto_file(path: &Path) -> bool {
        path.extension().is_some_and(|e| e == "proto")
    }

    /// The (service, method) each handler serves: the resolved proto RPC when
    /// known, else what the decorator spells (a literal service, and the method
    /// literal or the last segment of the constant's name). The service is
    /// `None` when it is not a literal (the delegation drop then keeps the
    /// call); the method is folded (see [`fold_rpc_name`]).
    pub(super) fn handler_served_methods(&self) -> HashMap<NodeId, (Option<String>, String)> {
        self.handler_served_methods_where(|_| true)
    }

    /// [`Self::handler_served_methods`] restricted to the handlers `keep` accepts.
    pub(super) fn handler_served_methods_where(
        &self,
        keep: impl Fn(NodeId) -> bool,
    ) -> HashMap<NodeId, (Option<String>, String)> {
        let unquote = |part: &str| {
            part.trim_matches(|c| matches!(c, '\'' | '"' | '`' | '?'))
                .to_string()
        };
        let mut out = HashMap::new();
        for (id, recorded) in &self.rpc_calls {
            let Some(reference) = recorded.strip_prefix(HANDLER_REF_PREFIX) else {
                continue;
            };
            if !keep(*id) {
                continue;
            }
            let served = match self.handler_rpc_names.get(id) {
                Some(rpc) => rpc
                    .rsplit_once('.')
                    .map(|(service, method)| (Some(service.to_string()), method.to_string())),
                None => Self::parse_handler_ref(reference).map(|parsed| {
                    let (service, method) = (parsed.service, parsed.method);
                    let literal_service = service
                        .starts_with(['\'', '"', '`'])
                        .then(|| unquote(service))
                        .filter(|s| !s.is_empty())
                        .map(|s| s.rsplit('.').next().unwrap_or(&s).to_string());
                    let method = unquote(method);
                    let method = method.rsplit('.').next().unwrap_or(&method).to_string();
                    (literal_service, method)
                }),
            };
            if let Some((service, method)) = served {
                out.insert(*id, (service, fold_rpc_name(&method)));
            }
        }
        out
    }

    /// The one parser of a [`HANDLER_REF_PREFIX`] record (the text after the
    /// prefix): `service|method`, with a trailing `|~` when the method argument
    /// was not written and `method` is the one NestJS derives from the decorated
    /// method's name.
    pub(super) fn parse_handler_ref(reference: &str) -> Option<HandlerRef<'_>> {
        let (service, rest) = reference.split_once('|')?;
        let mut parts = rest.split('|');
        let method = parts.next()?;
        let method_written = parts.next() != Some("~");
        Some(HandlerRef {
            service,
            method,
            method_written,
        })
    }

    /// The decorator arguments of a [`HANDLER_REF_PREFIX`] record as they were
    /// written: `service, method`, or the service alone when the method was
    /// not written. Truncated to 120 characters. `None` when nothing was written.
    pub(super) fn declared_as(reference: &str) -> Option<String> {
        const MAX: usize = 120;
        let HandlerRef {
            service,
            method,
            method_written,
        } = Self::parse_handler_ref(reference)?;
        let text = match (service.is_empty(), method_written) {
            (true, true) => method.to_string(),
            (true, false) => return None,
            (false, true) => format!("{service}, {method}"),
            (false, false) => service.to_string(),
        };
        if text.is_empty() {
            return None;
        }
        if text.chars().count() > MAX {
            let cut: String = text.chars().take(MAX).collect();
            return Some(format!("{cut}..."));
        }
        Some(text)
    }

    /// Resolves every [`HANDLER_REF_PREFIX`] record to `Service.Method` when
    /// both arguments reduce to a literal (a quoted string, or a constant /
    /// enum member / `as const` key recorded as [`CONST_VALUE_PREFIX`], same
    /// repository first) and that name is declared by exactly one proto RPC.
    /// Anything unresolved or ambiguous is left out, so the handler keeps its
    /// raw name and its heuristic match.
    pub(super) fn resolve_handler_rpc_names(
        &self,
        proto_methods: &[(NodeId, CompactStr)],
    ) -> HashMap<NodeId, CompactStr> {
        let mut out = HashMap::new();
        let mut consts: HashMap<&str, Vec<(RepoId, &str)>> = HashMap::new();
        let mut refs: Vec<(NodeId, &str)> = Vec::new();
        for (id, recorded) in &self.rpc_calls {
            if let Some(c) = recorded.strip_prefix(CONST_VALUE_PREFIX) {
                if let (Some(node), Some((key, value))) = (self.nodes.get(id), c.split_once('=')) {
                    consts.entry(key).or_default().push((node.repo_id, value));
                }
            } else if let Some(r) = recorded.strip_prefix(HANDLER_REF_PREFIX) {
                refs.push((*id, r));
            }
        }
        if refs.is_empty() {
            return out;
        }
        // One pass over the proto methods, not one per handler reference: a
        // reload re-runs this over every handler in the workspace.
        let mut by_name: HashMap<&str, Vec<NodeId>> = HashMap::with_capacity(proto_methods.len());
        for (pid, n) in proto_methods {
            by_name.entry(n.as_str()).or_default().push(*pid);
        }
        for (id, recorded) in refs {
            let declared = |name: &str| by_name.get(name).into_iter().flatten().copied();
            if let Some(name) = self.resolve_handler_ref(id, recorded, &consts, declared) {
                out.insert(id, name);
            }
        }
        out
    }

    /// One [`HANDLER_REF_PREFIX`] record of handler `id` resolved to
    /// `Service.Method` (see [`Self::resolve_handler_rpc_names`]); `declared`
    /// lists the proto RPCs named exactly `Service.Method`.
    pub(super) fn resolve_handler_ref<I: Iterator<Item = NodeId>>(
        &self,
        id: NodeId,
        recorded: &str,
        consts: &HashMap<&str, Vec<(RepoId, &str)>>,
        declared: impl Fn(&str) -> I,
    ) -> Option<CompactStr> {
        let (Some(node), Some(parsed)) = (self.nodes.get(&id), Self::parse_handler_ref(recorded))
        else {
            return None;
        };
        let (service_expr, method_expr) = (parsed.service, parsed.method);
        let (Some(service), Some(method)) = (
            Self::resolve_literal_expr(service_expr, node.repo_id, consts),
            Self::resolve_literal_expr(method_expr, node.repo_id, consts),
        ) else {
            return None;
        };
        // A package-qualified service (`demo.user.UserService`) must be
        // declared in that package.
        let (qualifier, service) = match service.rsplit_once('.') {
            Some((q, s)) => (Some(q), s),
            None => (None, service),
        };
        let name = format!("{service}.{method}");
        // The package qualifier narrows the candidates first; only then must
        // the name be unique. Counting before filtering left a fully-qualified
        // handler unresolved whenever two packages declare the same
        // `Service.Method`.
        let declared_here = declared(name.as_str())
            .filter(|pid| {
                qualifier
                    .is_none_or(|q| self.nodes.get(pid).is_some_and(|p| p.package.as_str() == q))
            })
            .count();
        (declared_here == 1).then(|| CompactStr::new(name))
    }

    /// A quoted literal as-is, else the one string constant `expr` names:
    /// `Enum.MEMBER`, `OBJ.KEY` or `CONST`, spelled exactly as declared. Candidates from the caller's repository win; more
    /// than one distinct value left is ambiguous (`None`).
    pub(super) fn resolve_literal_expr<'a>(
        expr: &'a str,
        repo: RepoId,
        consts: &HashMap<&str, Vec<(RepoId, &'a str)>>,
    ) -> Option<&'a str> {
        let expr = expr.trim();
        let quoted = expr.len() >= 2
            && ["'", "\"", "`"]
                .iter()
                .any(|q| expr.starts_with(q) && expr.ends_with(q));
        if quoted {
            return expr.get(1..expr.len() - 1).filter(|l| !l.is_empty());
        }
        // The exact qualified path only: `Enum.MEMBER` never falls back to an
        // unrelated constant that happens to be called `MEMBER`.
        let candidates = consts.get(expr)?;
        let same_repo: Vec<&str> = candidates
            .iter()
            .filter(|(r, _)| *r == repo)
            .map(|(_, v)| *v)
            .collect();
        let values: BTreeSet<&str> = if same_repo.is_empty() {
            candidates.iter().map(|(_, v)| *v).collect()
        } else {
            same_repo.into_iter().collect()
        };
        let mut it = values.into_iter();
        match (it.next(), it.next()) {
            (Some(only), None) => Some(only),
            _ => None,
        }
    }

    /// The `service` declaration a `.proto` `rpc` node sits in: same file, its
    /// line range enclosing the method's.
    pub(super) fn enclosing_grpc_service(&self, method: &ContractNode) -> Option<NodeId> {
        self.nodes
            .values()
            .filter(|n| {
                n.kind == NodeKind::GrpcService
                    && n.file_path == method.file_path
                    && n.line_start <= method.line_start
                    && n.line_end >= method.line_end
            })
            .min_by_key(|n| (n.line_end - n.line_start, n.id))
            .map(|n| n.id)
    }

    /// How `node` matches an `analyze_grpc` / `impact_matrix` target: `None`
    /// when it does not, else the confidence of the match and whether it is a
    /// symbol-level match (the node *is* the target declaration or one of its
    /// implementations) rather than a package/FQCN substring hit.
    fn grpc_target_match(node: &ContractNode, norm_target: &str) -> Option<(EdgeConfidence, bool)> {
        if !matches!(node.kind, NodeKind::GrpcService | NodeKind::GrpcMethod) {
            return None;
        }
        // Case-sensitive exact-name equality is unambiguous; case-folding
        // or a substring FQCN hit is a name heuristic.
        let exact_name = node.name.as_str() == norm_target
            || Self::fqcn_equals(&node.package, &node.name, norm_target);
        let matches_name = exact_name
            || node.name.eq_ignore_ascii_case(norm_target)
            || Self::bare_names_match(&node.name, norm_target);
        let matches_fqcn = node.package.contains(norm_target)
            || Self::fqcn_contains(&node.package, &node.name, norm_target);
        if !(matches_name || matches_fqcn) {
            return None;
        }
        let confidence = if exact_name {
            EdgeConfidence::Exact
        } else {
            EdgeConfidence::Heuristic
        };
        let is_symbol_match = exact_name
            || node.name.eq_ignore_ascii_case(norm_target)
            || Self::bare_names_match(&node.name, norm_target);
        Some((confidence, is_symbol_match))
    }

    /// `Base#field.method` (an [`RPC_INHERITED_METHOD_PREFIX`] record) ->
    /// (`Service.method`, inferred?). The base class's binding of `field` wins
    /// when it names exactly one service — base classes in the caller's own
    /// repository first, the whole mesh otherwise. Unbound or ambiguous, the
    /// field's name is the only clue left: [`service_from_client_field_name`],
    /// flagged inferred.
    pub(super) fn resolve_inherited_call(
        recorded: &str,
        caller_repo: Option<RepoId>,
        class_fields: &HashMap<(&str, &str), Vec<(RepoId, &str)>>,
    ) -> Option<(String, bool)> {
        let (base, rest) = recorded.split_once('#')?;
        let (field, method) = rest.split_once('.')?;
        if let Some(bindings) = class_fields.get(&(base, field)) {
            let same_repo: BTreeSet<&str> = bindings
                .iter()
                .filter(|(repo, _)| Some(*repo) == caller_repo)
                .map(|(_, service)| *service)
                .collect();
            let services = if same_repo.is_empty() {
                bindings.iter().map(|(_, service)| *service).collect()
            } else {
                same_repo
            };
            let services: Vec<&str> = services.into_iter().collect();
            // `#field=` (no service): the base class declares the field with a
            // plain application type, so it is not a client at all.
            let real: Vec<&str> = services.iter().copied().filter(|s| !s.is_empty()).collect();
            if real.is_empty() && !services.is_empty() {
                return None;
            }
            if let [service] = real[..] {
                return Some((format!("{service}.{method}"), false));
            }
        }
        service_from_client_field_name(field).map(|service| (format!("{service}.{method}"), true))
    }

    /// Every gRPC node matching `norm_target`, then the proto ones left out.
    /// As soon as one node matches as a symbol (exact name, bare name or
    /// FQCN), substring-only matches are dropped: `CreateEvent` must not
    /// anchor on `CreateEventNode`, whose handlers and clients 7.0.6 listed as
    /// `CreateEvent`'s. They come back as "related" so the answer can say what
    /// was left out. With no symbol match at all (a package or partial name),
    /// every substring match stays an anchor, with heuristic confidence.
    pub(super) fn grpc_anchor_matches(
        &self,
        norm_target: &str,
    ) -> (
        Vec<(&ContractNode, EdgeConfidence, bool)>,
        Vec<&ContractNode>,
    ) {
        let matches: Vec<(&ContractNode, EdgeConfidence, bool)> = self
            .nodes
            .values()
            .filter_map(|node| {
                Self::grpc_target_match(node, norm_target).map(|(c, symbol)| (node, c, symbol))
            })
            .collect();
        if !matches.iter().any(|(_, _, symbol)| *symbol) {
            return (matches, Vec::new());
        }
        // A matched service keeps its own methods (`UserService` covers
        // `UserService.SignUp`): they are the target, not a neighbour.
        let services: Vec<&ContractNode> = matches
            .iter()
            .filter(|(n, _, symbol)| *symbol && n.kind == NodeKind::GrpcService)
            .map(|(n, _, _)| *n)
            .collect();
        let (kept, dropped): (Vec<_>, Vec<_>) =
            matches.into_iter().partition(|(node, _, symbol)| {
                *symbol
                    || (node.kind == NodeKind::GrpcMethod
                        && services.iter().any(|s| {
                            s.file_path == node.file_path
                                && node
                                    .name
                                    .strip_prefix(s.name.as_str())
                                    .is_some_and(|rest| rest.starts_with('.'))
                        }))
            });
        let related = dropped
            .into_iter()
            .map(|(node, _, _)| node)
            .filter(|node| Self::is_proto_file(&node.file_path))
            .collect();
        (kept, related)
    }

    /// `target` spells `package` + `.` or `/` + `name`.
    fn fqcn_equals(package: &str, name: &str, target: &str) -> bool {
        !package.is_empty()
            && target
                .strip_prefix(package)
                .and_then(|rest| rest.strip_prefix(['.', '/']))
                == Some(name)
    }

    /// `format!("{package}/{name}").contains(needle)` without the allocation.
    fn fqcn_contains(package: &str, name: &str, needle: &str) -> bool {
        if package.contains(needle) || name.contains(needle) {
            return true;
        }
        // Needle may straddle the '/' separator.
        let Some((head, tail)) = needle.split_once('/') else {
            return false;
        };
        package.ends_with(head) && name.starts_with(tail)
    }

    /// Compares two bare names with normalization (case-insensitive and ignoring `_` underscores).
    /// e.g. "USER_SERVICE_NAME.SIGN_UP" or "SIGN_UP" vs "SignUp" -> true.
    pub fn bare_names_match(a: &str, b: &str) -> bool {
        let a_bare = a.split('.').next_back().unwrap_or(a);
        let b_bare = b.split('.').next_back().unwrap_or(b);

        if a_bare.eq_ignore_ascii_case(b_bare) {
            return true;
        }

        // Allocation-free: `search_symbols` runs this for every in-scope node on
        // every query. `_` is ASCII, so skipping it byte-wise never splits a char.
        let mut a_norm = a_bare.bytes().filter(|b| *b != b'_');
        let mut b_norm = b_bare.bytes().filter(|b| *b != b'_');
        let mut non_empty = false;
        loop {
            match (a_norm.next(), b_norm.next()) {
                (None, None) => return non_empty,
                (Some(x), Some(y)) if x.eq_ignore_ascii_case(&y) => non_empty = true,
                _ => return false,
            }
        }
    }

    /// Allocation-free check whether `sig` contains `bare` prefixed by `@`, `'`, `"`, `fn `, `func `, or `def `.
    pub(super) fn signature_contains_bare(sig: &str, bare: &str) -> bool {
        if bare.is_empty() {
            return false;
        }
        let prefixes = ["@", "'", "\"", "fn ", "func ", "def "];
        for prefix in prefixes {
            let mut offset = 0;
            while let Some(pos) = sig[offset..].find(prefix) {
                let start = offset + pos + prefix.len();
                let rest = &sig[start..];
                if rest.starts_with(bare) {
                    return true;
                }
                let ident: String = rest
                    .chars()
                    .take_while(|c| c.is_alphanumeric() || *c == '_')
                    .collect();
                if !ident.is_empty() && Self::bare_names_match(&ident, bare) {
                    return true;
                }
                offset += pos + prefix.len();
            }
        }
        false
    }
}

/// A candidate list plus its ids grouped by declared package (in list order),
/// so disambiguating by the caller's package is a lookup, not a scan.
pub(super) struct PackageBucket<'g> {
    pub(super) ids: Vec<NodeId>,
    /// Empty when `ids` has a single entry (never consulted then).
    by_package: HashMap<&'g str, Vec<NodeId>>,
}

impl PackageBucket<'_> {
    /// Same shape as [`ContractGraph::pick_or_ambiguous`], grouping by declared
    /// `package` instead of `repo_id`: one candidate, or exactly one sharing the
    /// caller's package, wins at `confidence_if_unique`; otherwise every tied
    /// candidate (the same-package ones if several, else all) is `Ambiguous`.
    pub(super) fn pick_or_ambiguous(
        &self,
        caller_package: &CompactStr,
        confidence_if_unique: EdgeConfidence,
    ) -> Vec<(NodeId, EdgeConfidence)> {
        if let [only] = self.ids.as_slice() {
            return vec![(*only, confidence_if_unique)];
        }
        let same_package = self
            .by_package
            .get(caller_package.as_str())
            .map_or(&[][..], Vec::as_slice);
        match same_package {
            [only] => vec![(*only, confidence_if_unique)],
            [] => self
                .ids
                .iter()
                .map(|&id| (id, EdgeConfidence::Ambiguous))
                .collect(),
            _ => same_package
                .iter()
                .map(|&id| (id, EdgeConfidence::Ambiguous))
                .collect(),
        }
    }
}

/// `(import target, importer repo, importer directory)`: the key of a resolved
/// import fact; the directory is `Some` only for a relative import from a script.
pub(super) type ResolvedKey<'g> = (&'g CompactStr, RepoId, Option<&'g Path>);

/// Source extensions that name a module without being part of its import path
/// (`./user.js` for `user.ts`), longest first.
const MODULE_EXTENSIONS: &[&str] = &[
    ".d.ts", ".d.mts", ".d.cts", ".tsx", ".ts", ".mts", ".cts", ".jsx", ".js", ".mjs", ".cjs",
    ".json", ".vue", ".svelte", ".astro",
];

/// Whether `path` is a JavaScript / TypeScript source file.
pub(super) fn is_script_path(path: &Path) -> bool {
    matches!(
        path.extension().and_then(|e| e.to_str()),
        Some(
            "ts" | "tsx"
                | "mts"
                | "cts"
                | "js"
                | "jsx"
                | "mjs"
                | "cjs"
                | "vue"
                | "svelte"
                | "astro"
        )
    )
}

/// `path` without a module extension, lexically normalised: the identity a
/// relative import path and a declaration's file are compared by.
fn module_key(path: &Path) -> PathBuf {
    let cleaned = path_clean::clean(path);
    let text = cleaned.to_string_lossy();
    for ext in MODULE_EXTENSIONS {
        if let Some(stem) = text.strip_suffix(ext) {
            if !stem.is_empty() && !stem.ends_with(std::path::MAIN_SEPARATOR) {
                return PathBuf::from(stem);
            }
        }
    }
    cleaned
}

/// Whether the file's extension is one the specifier can mean: an explicit `.vue` / `.svelte` /
/// `.astro` names only such a file, and an explicit `.js` / `.ts` / … never names a component
/// (`Foo.vue` and `Foo.ts` side by side are two modules).
fn extension_compatible(file: &Path, specifier: &str) -> bool {
    let component = |e: &str| matches!(e, "vue" | "svelte" | "astro");
    let spec_ext = Path::new(specifier).extension().and_then(|e| e.to_str());
    let file_ext = file.extension().and_then(|e| e.to_str());
    match spec_ext {
        Some(e) if component(e) => file_ext == Some(e),
        Some("js" | "jsx" | "ts" | "tsx" | "mjs" | "cjs" | "mts" | "cts" | "json") => {
            !file_ext.is_some_and(component)
        }
        _ => true,
    }
}

/// Whether a declaration in the file keyed `file` is what the relative import
/// keyed `target` names: the same module path; that directory's `index`
/// (`./services` -> `services/index.ts`); or the directory's file of its own name
/// (`./Button` -> `Button/Button.tsx`, the component-folder convention whose
/// `index.ts` only re-exports it).
fn module_key_matches(file: &Path, target: &Path) -> bool {
    file == target
        || file == target.join("index")
        || target
            .file_name()
            .is_some_and(|name| file == target.join(name))
}

/// Per-`reconcile_edges` memo for [`ContractGraph::resolve_import_targets`],
/// keyed by `name_to_nodes` bucket so that *distinct* targets landing in the same
/// bucket share one scan of it:
///
/// - `first_by_package`: for strategy 1 (`pkg.Name`), the first node of each
///   package in the `Name` bucket, in bucket order — exactly the node the old
///   per-call `find` returned. Scanning per call was O(distinct targets ×
///   bucket): N packages each declaring their own `Constants` and importing
///   `pN.Constants` rescanned an N-long bucket N times.
/// - `same_repo_by_stem`: for strategy 3 (relative imports), the bucket of a
///   file stem filtered to one repo, in bucket order.
#[derive(Default)]
pub(super) struct ImportMemo<'g> {
    first_by_package: HashMap<&'g str, HashMap<&'g str, NodeId>>,
    same_repo_by_stem: HashMap<(&'g str, Option<RepoId>), Vec<NodeId>>,
    /// `module_key` of each declaration's file, computed once per node.
    node_keys: HashMap<NodeId, PathBuf>,
}

/// Lookup index over gRPC handler candidates for `reconcile_edges`' `Implements`
/// pass. Every way the pass's match predicate can succeed maps to a hash key:
///
/// - exact full name (`h.name == fqcn`);
/// - ASCII-lowercased last `.` segment (`eq_ignore_ascii_case`, `bare_names_match`);
/// - that segment with `_` removed (`bare_names_match`'s normalized form);
/// - `pascal_name` verbatim;
/// - for each identifier following a signature prefix (`@`, quotes, `fn `,
///   `func `, `def `): its lowercased and normalized forms; the identifier
///   itself goes into a sorted list, so `signature_contains_bare`'s
///   `rest.starts_with(bare)` test is a binary-searched prefix range (one entry
///   per identifier — indexing every prefix as its own key cost hundreds of MB
///   of short-lived strings on a 40k-handler workspace).
///
/// A `bare` containing a non-identifier character can match a signature in ways
/// no identifier key captures, so it falls back to every handler that has a
/// signature — rare, and still exact.
pub(super) struct HandlerIndex {
    keys: HashMap<String, Vec<usize>>,
    /// `(signature identifier, handler)`, sorted: identifiers sharing a prefix
    /// are contiguous.
    sig_idents: Vec<(String, usize)>,
    with_signature: Vec<usize>,
}

impl HandlerIndex {
    const SIG_PREFIXES: [&'static str; 6] = ["@", "'", "\"", "fn ", "func ", "def "];

    fn is_ident_char(c: char) -> bool {
        c.is_alphanumeric() || c == '_'
    }

    fn last_segment(s: &str) -> &str {
        s.split('.').next_back().unwrap_or(s)
    }

    fn normalized(s: &str) -> String {
        s.bytes()
            .filter(|b| *b != b'_')
            .map(|b| b.to_ascii_lowercase() as char)
            .collect()
    }

    pub(super) fn build<'a>(
        handlers: impl Iterator<Item = (&'a str, &'a str, Option<&'a str>)>,
    ) -> Self {
        let mut keys: HashMap<String, Vec<usize>> = HashMap::new();
        let mut sig_idents: Vec<(String, usize)> = Vec::new();
        let mut with_signature = Vec::new();
        for (i, (name, pascal, signature)) in handlers.enumerate() {
            let mut own: HashSet<String> = HashSet::new();
            own.insert(format!("f:{name}"));
            let last = Self::last_segment(name);
            own.insert(format!("l:{}", last.to_ascii_lowercase()));
            own.insert(format!("n:{}", Self::normalized(last)));
            own.insert(format!("p:{pascal}"));
            if let Some(sig) = signature {
                with_signature.push(i);
                for prefix in Self::SIG_PREFIXES {
                    let mut offset = 0;
                    while let Some(pos) = sig[offset..].find(prefix) {
                        let start = offset + pos + prefix.len();
                        let ident_len: usize = sig[start..]
                            .chars()
                            .take_while(|c| Self::is_ident_char(*c))
                            .map(char::len_utf8)
                            .sum();
                        let ident = &sig[start..start + ident_len];
                        if !ident.is_empty() {
                            own.insert(format!("l:{}", ident.to_ascii_lowercase()));
                            own.insert(format!("n:{}", Self::normalized(ident)));
                            sig_idents.push((ident.to_string(), i));
                        }
                        offset += pos + prefix.len();
                    }
                }
            }
            for key in own {
                keys.entry(key).or_default().push(i);
            }
        }
        sig_idents.sort_unstable();
        sig_idents.dedup();
        Self {
            keys,
            sig_idents,
            with_signature,
        }
    }

    /// Every handler index that *may* match `(fqcn, bare)`, sorted and deduplicated.
    pub(super) fn candidates(&self, fqcn: &str, bare: &str, out: &mut Vec<usize>) {
        let mut push = |key: String| {
            if let Some(ids) = self.keys.get(&key) {
                out.extend_from_slice(ids);
            }
        };
        push(format!("f:{fqcn}"));
        push(format!("l:{}", bare.to_ascii_lowercase()));
        let norm = Self::normalized(bare);
        if !norm.is_empty() {
            push(format!("n:{norm}"));
        }
        push(format!("p:{bare}"));
        if !bare.is_empty() {
            // Identifiers starting with `bare` form one contiguous sorted run.
            let start = self
                .sig_idents
                .partition_point(|(ident, _)| ident.as_str() < bare);
            out.extend(
                self.sig_idents[start..]
                    .iter()
                    .take_while(|(ident, _)| ident.starts_with(bare))
                    .map(|&(_, i)| i),
            );
            if !bare.chars().all(Self::is_ident_char) {
                out.extend_from_slice(&self.with_signature);
            }
        }
        out.sort_unstable();
        out.dedup();
    }
}

/// Allocation-free `haystack.to_lowercase().contains(&needle.to_lowercase())` for ASCII needles.
pub(super) fn contains_ignore_ascii_case(haystack: &str, needle: &str) -> bool {
    if needle.is_empty() {
        return true;
    }
    let (h, n) = (haystack.as_bytes(), needle.as_bytes());
    h.len() >= n.len() && h.windows(n.len()).any(|w| w.eq_ignore_ascii_case(n))
}
