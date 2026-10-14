//! Preserve the runtime's effective connection while supporting authenticated
//! management lookups in RivetKit 2.3.16 (which only sets X-Rivet-Token).
use anyhow::{Result, anyhow};
use rivetkit::{Actor, Ctx};
use rivetkit_client::{Client, ClientConfig, EncodingKind, TransportKind};

pub(crate) fn actor_client<A: Actor>(ctx: &Ctx<A>) -> Result<Client> {
    let runtime = ctx.inner();
    let mut config = ClientConfig::new(
        runtime
            .client_endpoint()
            .ok_or_else(|| anyhow!("Missing actor client endpoint"))?,
    )
    .namespace(
        runtime
            .client_namespace()
            .ok_or_else(|| anyhow!("Missing actor client namespace"))?,
    )
    .pool_name(
        runtime
            .client_pool_name()
            .ok_or_else(|| anyhow!("Missing actor client pool"))?,
    )
    .encoding(EncodingKind::Bare)
    .transport(TransportKind::WebSocket)
    .disable_metadata_lookup(true);
    if let Some(token) = runtime.client_token() {
        config = config
            .token(token)
            .header("authorization", format!("Bearer {token}"));
    }
    Ok(Client::new(config))
}
