//! A native Rust implementation of the Durable Streams HTTP protocol on
//! RivetKit actors.

mod actor;
mod facade;
mod protocol;
mod store;

pub use actor::{ACTOR_NAME, DurableStreamActor, DurableStreamInput, DurableStreamState};
pub use facade::{DurableStreamsConfig, durable_streams_router};
pub use protocol::{MAX_BODY_BYTES, ZERO_OFFSET};

use std::path::PathBuf;
use std::sync::OnceLock;

use include_dir::{Dir, include_dir};
use rivetkit::{ActorConfig, InspectorTabEntry, Registry};
use tempfile::TempDir;

/// Compiled-in inspector bundle so hosts serve custom tabs without shipping files.
static INSPECTOR_BUNDLE: Dir = include_dir!("$CARGO_MANIFEST_DIR/inspector");

/// Registers the durable-stream actor with the supplied RivetKit registry.
pub fn register(registry: &mut Registry) {
    register_with_inspector(registry, None);
}

/// Registers the actor. `inspector_root` overrides the embedded inspector
/// bundle, e.g. for development against a live checkout.
pub fn register_with_inspector(registry: &mut Registry, inspector_root: Option<PathBuf>) {
    let inspector_root = inspector_root.or_else(|| match embedded_inspector_root() {
        Ok(root) => Some(root),
        Err(error) => {
            tracing::warn!(
                ?error,
                "failed to extract the embedded inspector bundle; registering durable streams without custom inspector tabs"
            );
            None
        }
    });
    let inspector_tabs = inspector_root.map(inspector_tabs).unwrap_or_default();
    registry.register_actor_with::<DurableStreamActor>(
        ACTOR_NAME,
        ActorConfig {
            has_database: true,
            has_state: true,
            max_outgoing_message_size: 6 * 1024 * 1024,
            inspector_tabs,
            ..ActorConfig::default()
        },
    );
}

/// The extracted bundle directory, kept alive for the process lifetime.
static EXTRACTED_INSPECTOR_BUNDLE: OnceLock<Result<TempDir, String>> = OnceLock::new();

/// Extracts the embedded bundle once per process into a private temp
/// directory. A fresh randomized directory sidesteps shared-/tmp preplanting
/// and stale caches from other builds; the 27 KiB rewrite per process is
/// cheaper than defending a reusable path.
fn embedded_inspector_root() -> anyhow::Result<PathBuf> {
    let extracted = EXTRACTED_INSPECTOR_BUNDLE.get_or_init(|| {
        let dir = TempDir::with_prefix("rivet-durable-streams-inspector-")
            .map_err(|error| format!("create inspector temp directory: {error}"))?;
        INSPECTOR_BUNDLE
            .extract(dir.path())
            .map_err(|error| format!("extract embedded inspector bundle: {error}"))?;
        Ok(dir)
    });
    match extracted {
        Ok(dir) => Ok(dir.path().to_owned()),
        Err(error) => Err(anyhow::anyhow!("{error}")),
    }
}

fn inspector_tabs(root: PathBuf) -> Vec<InspectorTabEntry> {
    [
        ("stream-overview", "Overview", "tag"),
        ("stream-messages", "Messages", "logs"),
        ("stream-producers", "Producers", "microchip"),
        ("stream-forks", "Forks", "workflow"),
        ("stream-maintenance", "Maintenance", "wrench"),
    ]
    .into_iter()
    .map(|(id, label, icon)| InspectorTabEntry::Custom {
        id: id.to_owned(),
        label: label.to_owned(),
        icon: Some(icon.to_owned()),
        root: root.clone(),
    })
    // Hide every hideable built-in tab; a durable stream only exposes its
    // own custom tabs. These ids are the full BUILTIN_TAB_IDS set. (The
    // "Metadata" tab is not in that set and cannot be hidden via this API.)
    .chain(
        [
            "workflow",
            "database",
            "state",
            "queue",
            "schedules",
            "connections",
            "console",
        ]
        .into_iter()
        .map(|id| InspectorTabEntry::HideBuiltin { id: id.to_owned() }),
    )
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_bundle_extracts_and_matches_the_source_tree() {
        let root = embedded_inspector_root().expect("extract embedded inspector bundle");

        for path in ["index.html", "app.js", "common.js", "styles.css"] {
            let embedded = INSPECTOR_BUNDLE
                .get_file(path)
                .unwrap_or_else(|| panic!("bundle is missing {path}"))
                .contents();
            let extracted = std::fs::read(root.join(path)).expect("read extracted file");
            assert_eq!(embedded, extracted.as_slice(), "{path} differs");
        }
        assert!(root.join("views").is_dir());

        // A second call reuses the same process-lifetime directory.
        let again = embedded_inspector_root().expect("reuse extracted inspector bundle");
        assert_eq!(root, again);
    }

    #[test]
    fn inspector_uses_native_tabs_with_a_shared_bundle() {
        let root = PathBuf::from("/inspector");
        let tabs = inspector_tabs(root.clone());

        // Custom tabs come first and all share the one bundle root.
        let custom_ids = tabs
            .iter()
            .filter(|tab| matches!(tab, InspectorTabEntry::Custom { .. }))
            .map(InspectorTabEntry::id)
            .collect::<Vec<_>>();
        assert_eq!(
            custom_ids,
            [
                "stream-overview",
                "stream-messages",
                "stream-producers",
                "stream-forks",
                "stream-maintenance",
            ]
        );
        assert!(tabs.iter().all(|tab| match tab {
            InspectorTabEntry::Custom { root: tab_root, .. } => tab_root == &root,
            InspectorTabEntry::HideBuiltin { .. } => true,
        }));

        // Every hideable built-in tab is hidden.
        let hidden_ids = tabs
            .iter()
            .filter_map(|tab| match tab {
                InspectorTabEntry::HideBuiltin { id } => Some(id.as_str()),
                InspectorTabEntry::Custom { .. } => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            hidden_ids,
            [
                "workflow",
                "database",
                "state",
                "queue",
                "schedules",
                "connections",
                "console",
            ]
        );
    }
}
