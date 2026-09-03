use std::net::SocketAddr;

use anyhow::Context;
use clap::Parser;
use rivet_durable_streams::{
    DurableStreamsConfig, durable_streams_router, register_with_inspector,
};
use rivetkit::{Registry, ServeConfig};
use rivetkit_client::{Client, ClientConfig};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    #[arg(long)]
    port: Option<u16>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let port = match args.port {
        Some(port) => port,
        None => std::env::var("RIVET_PORT")
            .ok()
            .map(|value| value.parse().context("parse RIVET_PORT"))
            .transpose()?
            .unwrap_or(8787),
    };
    let serve_config = ServeConfig::from_env();
    let client = Client::new(
        ClientConfig::new(serve_config.endpoint.clone())
            .token_opt(serve_config.token.clone())
            .namespace(serve_config.namespace.clone())
            .pool_name(serve_config.pool_name.clone()),
    );
    let streams_config = DurableStreamsConfig {
        public_origin: std::env::var("PUBLIC_ORIGIN").ok(),
        ..DurableStreamsConfig::default()
    };
    let app =
        axum::Router::new().nest("/v1/stream", durable_streams_router(client, streams_config));

    let mut registry = Registry::new();
    register_with_inspector(
        &mut registry,
        Some(std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("inspector")),
    );
    let shutdown = CancellationToken::new();
    let actor_shutdown = shutdown.clone();
    let mut actors = tokio::spawn(async move {
        registry
            .serve_with_config(serve_config, actor_shutdown)
            .await
    });

    let address: SocketAddr = format!("{}:{port}", args.host)
        .parse()
        .context("parse --host/--port")?;
    let listener = TcpListener::bind(address).await?;
    println!("Durable Streams listening on http://{address}/v1/stream/<path>");

    let server_shutdown = shutdown.clone();
    let mut server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                server_shutdown.cancelled().await;
            })
            .await
    });

    tokio::select! {
        result = &mut actors => {
            shutdown.cancel();
            result.context("actor server task")??;
        }
        result = &mut server => {
            shutdown.cancel();
            result.context("facade server task")??;
        }
        _ = tokio::signal::ctrl_c() => {
            shutdown.cancel();
        }
    }

    if !actors.is_finished() {
        actors.await.context("actor server task")??;
    }
    if !server.is_finished() {
        server.await.context("facade server task")??;
    }
    Ok(())
}
