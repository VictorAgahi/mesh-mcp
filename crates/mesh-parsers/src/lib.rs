#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

pub mod anomalies;
pub mod asyncapi;
pub mod config_ref;
pub mod decapitate;
pub mod graph;
pub mod guard;
pub mod languages;
pub mod markdown;
pub mod topology;

pub use anomalies::{AnomalyShow, AnomalyView};
pub use asyncapi::{
    diff_asyncapi, format_diff, format_export, parse_declared, DeclaredDoc, DeclaredParse,
    DiffView, ExportView, ObservedSet,
};
pub use decapitate::{AstDecapitator, DecapitatedSource, LanguageKind};
pub use graph::{GraphRenderer, WebEdge, WebGraphPayload, WebNode};
pub use guard::{AstGuard, BoundedMatch, GuardRejection, ParserError};
pub use languages::{
    root_relative, CompiledPattern, ExtractConfig, FileIndex, PathContext, PolyglotIndexer,
    RootRelativeIndex, TsAliasIndex,
};
pub use markdown::{
    declaration_label, MarkdownFormatter, RouteImpactEntry, SearchPage, SearchResult,
    MAX_OUTPUT_BYTES, NON_RESULT_RESERVE_BYTES,
};
pub use topology::{Aggregate, Topology, TopologyOptions};
