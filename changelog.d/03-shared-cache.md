### Breaking
- Persistent parse cache location: inside a Git repository it now lives at `~/.cache/mesh-mcp/workspaces/<cache_family_id>/index-cache.db`, one database shared by every worktree, every config file and every mesh-mcp version of the repository (marked by a `family_common_dir` file instead of `workspace_path`). Outside Git the per-workspace directory is unchanged. The first boot after upgrading parses everything once.
- Cache schema 12 (was 11): keys hold the file's root-relative path (`/`-separated) instead of its absolute path, plus the path-derived extraction context (package inferred from directories, parent directory name, path substring tests); records store root-relative node paths, re-anchored on the reading checkout. Older databases are dropped and recreated on open. `PersistentIndexCache::key_for` takes the relative path as `&str`.

### Changed
- A second worktree of the same commit, or a checkout moved elsewhere, boots on cache hits and produces the same snapshot fingerprint. A file whose facts depend on the checkout's own directory (a package named after it when no manifest declares one) gets a different key and is parsed, never served another checkout's answer.
- `tsconfig` / `package.json` alias targets enter a file's cache key relative to its workspace root.
- Package inference is now part of the cache key, so renaming a manifest's `name` re-extracts the files it governs instead of serving the old package from the cache.
- `doctor --fix` also removes a pre-8.0 per-worktree cache directory of any worktree of the current repository, and never reports the shared directory as orphaned.
