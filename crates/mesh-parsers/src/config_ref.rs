//! A topic name that is not written at the site but read from configuration
//! (`process.env.X`, `configService.get('X')`, `os.Getenv("X")`, `os.environ["X"]`,
//! Spring `@Value("${a.b:def}")`).
//!
//! Every extractor spells such a name the way Spring does, `${key}` or `${key:default}`,
//! and records it where it would record a literal topic. The workspace's configuration
//! files are read elsewhere (`mesh_core::ConfigValues`); the placeholder is resolved when
//! the file index is folded into the graph, so an edit of a config file can re-resolve it
//! without re-parsing the source. A placeholder nothing resolves stays a visible
//! `${key}` topic: no name is ever guessed.

/// Longest key or default an extractor records.
const MAX_PART_LEN: usize = 160;

/// `${key}` / `${key:default}` for a config key read at a site, or `None` when the key or
/// the default is not a plain name (no placeholder syntax, no whitespace, bounded length):
/// anything else is an expression the extractor did not understand.
pub fn placeholder(key: &str, default: Option<&str>) -> Option<String> {
    if !valid_key(key) {
        return None;
    }
    match default {
        Some(d) if valid_default(d) => Some(format!("${{{key}:{d}}}")),
        // A default that is not a plain topic name (a credential, a URL with userinfo, prose)
        // is dropped, never recorded: the site stays visible as `${key}`.
        _ => Some(format!("${{{key}}}")),
    }
}

fn valid_key(key: &str) -> bool {
    !key.is_empty()
        && key.len() <= MAX_PART_LEN
        && key
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-'))
}

fn valid_default(default: &str) -> bool {
    !default.is_empty()
        && default.len() <= MAX_PART_LEN
        && mesh_core::config_values::plain_topic(default)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_keys_and_defaults_are_accepted() {
        assert_eq!(
            placeholder("KAFKA_TOPIC", None).as_deref(),
            Some("${KAFKA_TOPIC}")
        );
        assert_eq!(
            placeholder("app.topics.orders", Some("orders")).as_deref(),
            Some("${app.topics.orders:orders}")
        );
    }

    #[test]
    fn expressions_are_not_keys() {
        assert!(placeholder("a b", None).is_none());
        assert!(placeholder("${x}", None).is_none());
        assert!(placeholder("", None).is_none());
        // A default that is not a plain topic name is dropped, the key stays.
        for bad in [
            "a b",
            "${y}",
            "sk_live_abcdef0123456789",
            "postgres://u:pw@h/x",
            "u@h",
            "x#y",
        ] {
            assert_eq!(
                placeholder("K", Some(bad)).as_deref(),
                Some("${K}"),
                "{bad}"
            );
        }
        assert!(placeholder(&"k".repeat(200), None).is_none());
    }
}
