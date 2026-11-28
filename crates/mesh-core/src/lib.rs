#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

pub mod audit;
pub mod config;
pub mod config_values;
pub mod config_watch;
pub mod contracts;
pub mod crawler;
pub mod docs;
pub mod governance;
pub mod health;
pub mod identity;
pub mod index_cache;
pub mod member_consts;
pub mod paths;
pub mod properties;
pub mod reload_kind;
pub mod rescan;
pub mod root_guard;
pub mod routes;
pub mod search_cache;
pub mod security;
pub mod socket;
pub mod state;
pub mod test_paths;
pub mod types;
pub mod vfs;
pub mod watcher;
pub mod yaml_stream;

// Re-export common types
pub use audit::{
    normalize_audit_timestamp, parse_timestamp_to_epoch_secs, sanitize_error_message, AuditEntry,
    AuditError, AuditLogger, AuditMetrics, ErrorDetail, IndexCacheTotals,
};
pub use config::{
    expand_roots, strip_workspace_root_prefix, AsyncApiConfig, Config, ConfigError,
    ContractsConfig, CustomPatternConfig, GrpcConfig, OpenApiConfig, PatternConfidence,
    PatternKind, ProducerCallConfig, ReadGovernanceMode, WorkspaceConfig, MAX_PRODUCER_CALLS,
};
pub use config_values::{ConfigEntry, ConfigFile, ConfigValues};
pub use contracts::{
    fold_rpc_name, hub_topic, is_member_ref, normalize_topic_key, payload_json_family,
    qualified_topic_key, service_from_client_field_name, split_qualified_topic_key, AnomalySite,
    Broker, CallFact, CallFacts, CallReceiver, CallStats, CallableFact, ContractGraph, CycleEdge,
    CycleEntry, DependentsMatch, EventAnomalies, EventChannel, EventHop, EventHops, EventSite,
    FanOutProducer, FieldClash, FieldGap, FlowBounds, FlowDirection, FlowEnd, FlowEntry, FlowHop,
    FlowLimit, FlowNext, FlowOutcome, FlowSite, FlowStep, FlowStepKind, FlowTrace, GrpcTrace,
    HopEnd, HubRef, ImpactFlow, ImpactMatrix, ImpactRole, ImpactRow, ImpactScope, ImportFact,
    InjectionFact, Missing, NotCompared, PayloadEntry, PayloadFacts, PayloadField, PayloadImport,
    PayloadReport, PayloadShape, PayloadSide, PayloadSite, PayloadSkipReason, PayloadSkipped,
    PayloadType, ReconcileMode, RecordedCaller, ResolvedRpcRecord, RpcRecordResolver, TopicEntry,
    TypeFact, TypeFactKind, UnresolvedEntry, UnresolvedReason, ANOMALY_FAN_OUT_CONSUMERS,
    ANOMALY_FAN_OUT_TOPICS, ANOMALY_MAX_CYCLE_SITES, CALLABLE_ID_BASE, CLIENT_FIELD_BINDING_PREFIX,
    CONST_VALUE_PREFIX, DEFAULT_CALL_DEPTH, DEFAULT_EVENT_DEPTH, DEFAULT_FAN_OUT,
    HANDLER_REF_PREFIX, MAX_CALL_DEPTH, MAX_DERIVED_CALL_SITES, MAX_EVENT_DEPTH, MAX_FAN_OUT,
    MAX_FLOW_HOPS, MAX_PAYLOAD_FIELDS_PER_TYPE, MAX_PAYLOAD_SHAPES_PER_TOPIC,
    MAX_PAYLOAD_SITES_PER_FILE, MAX_PAYLOAD_TYPES_PER_FILE, MAX_PAYLOAD_TYPE_NAMES_PER_FILE,
    MAX_STRUCTURAL_IMPLS, MAX_STRUCTURAL_PAIRS, MAX_TYPE_HOPS, MAX_WILDCARD_PRODUCERS,
    RPC_EXACT_METHOD_PREFIX, RPC_INFERRED_METHOD_PREFIX, RPC_INHERITED_METHOD_PREFIX,
};
pub use crawler::{CrawlReport, ExcludeMatcher, FilesystemCrawler, WatchPlan};
pub use docs::{DocIndex, DocSection};
pub use governance::{GovernanceEngine, RsahResponse};
pub use health::{IndexHealth, RejectReason, RejectedFile};
pub use identity::{cache_family_id, family_id, RepoFamily, ViewKey, WorkspaceIdentity};
pub use index_cache::{sha256, CacheDirId, CacheEntry, IndexCacheError, PersistentIndexCache};
pub use member_consts::{ImportBinding, MemberResolution, MemberTable};
pub use paths::mesh_cache_dir;
pub use properties::{PropertyRegistry, PropertySourceMatcher};
pub use rescan::BackgroundRescanEngine;
pub use search_cache::{file_stamp, CachedSearch, FileStamp, SearchCache, SearchCacheKey};
pub use security::{SecurityError, ValidatedScope};
pub use socket::{cleanup_stale_socket, socket_path, socket_path_for, workspace_id};
#[cfg(windows)]
pub use socket::{pipe_name, pipe_name_for};
pub use state::{AppState, MeshSnapshot, SnapshotFingerprint};
pub use test_paths::{is_generated_path, is_test_node, is_test_path};
pub use types::{
    detect_service_package, to_pascal_case, CanonicalMethodId, CompactStr, ContractEdge,
    ContractNode, EdgeConfidence, EdgeKind, FilePath, NodeId, NodeKind, PathStr, RepoId, RepoState,
    SymbolName,
};
pub use vfs::DifferentialVfs;
pub use watcher::{FileWatcherService, ReloadFn};
