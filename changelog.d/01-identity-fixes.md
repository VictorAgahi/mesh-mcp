### Fixed
- The daemon identity (socket, log, metadata and parse-cache directory) now includes the canonical config file path when a config file is used. Two config files in one directory (for example `k8s-wt1.toml` and `k8s-wt2.toml` passed with `--config`) used to share one `meshd`, so a session received the other config's graph. Without a config file the identity is unchanged.
- N `mesh-mcp run` starting at once on one workspace no longer each spawn and index their own `meshd`: the spawn is serialized by an exclusive lock file next to the socket (`meshd-<id>.lock`, mode 0600), and the other sessions wait for the winner's daemon. The wait is bounded; on timeout the session falls back to standalone mode with a warning.
- A config edit made while the daemon is idle is now applied before the next request: the daemon checks its config file every 2 seconds instead of only when a request arrives, so the first request after the edit is no longer answered by the old state.

### Changed
- `mesh_core::workspace_id` takes the config file as a second argument (`Option<&Path>`) and stays the single source of truth for the daemon identity; `PersistentIndexCache::open_for_workspace`, `AppState::open_index_cache` and `meshd`'s `ingest_blocking` take it too.
- The wait for a freshly spawned daemon to bind its socket is 5 seconds (was 500 ms), held under the spawn lock.
