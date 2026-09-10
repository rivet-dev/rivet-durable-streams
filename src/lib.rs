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

use rivetkit::{ActorConfig, InspectorTabEntry, Registry};

/// Registers the durable-stream actor with the supplied RivetKit registry.
pub fn register(registry: &mut Registry) {
    register_with_inspector(registry, None);
}

/// Registers the actor and, when supplied, its read-only inspector bundle.
/// The application owns this path because custom tabs are served from the
/// final binary's filesystem.
pub fn register_with_inspector(registry: &mut Registry, inspector_root: Option<PathBuf>) {
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
