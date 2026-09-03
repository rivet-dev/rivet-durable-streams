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
    let inspector_tabs = inspector_root
        .map(|root| {
            vec![InspectorTabEntry::Custom {
                id: "durable-stream".to_owned(),
                label: "Durable Stream".to_owned(),
                icon: Some("database".to_owned()),
                root,
            }]
        })
        .unwrap_or_default();
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
