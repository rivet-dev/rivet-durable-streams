use std::net::SocketAddr;

use anyhow::Context;
use clap::Parser;
use percent_encoding::percent_decode_str;
use rivet_durable_streams::{
    DurableStreamsConfig, durable_streams_router, register_with_inspector,
};
use rivetkit::{Registry, ServeConfig};
use rivetkit_client::{Client, ClientConfig};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

mod serverless_http;

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
        None => std::env::var("PORT")
            .or_else(|_| std::env::var("RIVET_PORT"))
            .ok()
            .map(|value| value.parse().context("parse PORT"))
            .transpose()?
            .unwrap_or(8787),
    };
    let serve_config = normalize_serve_config(ServeConfig::from_env())?;
    let client = Client::new(client_config(&serve_config)?);
    let streams_config = DurableStreamsConfig {
        public_origin: std::env::var("PUBLIC_ORIGIN").ok(),
        ..DurableStreamsConfig::default()
    };
    let mut app =
        axum::Router::new().nest("/v1/stream", durable_streams_router(client, streams_config));

    let mut registry = Registry::new();
    register_with_inspector(
        &mut registry,
        Some(std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("inspector")),
    );
    let serverless = std::env::var("RIVETKIT_RUNTIME_MODE")
        .is_ok_and(|value| value.eq_ignore_ascii_case("serverless"));
    let shutdown = CancellationToken::new();
    let address: SocketAddr = format!("{}:{port}", args.host)
        .parse()
        .context("parse --host/--port")?;
    let listener = TcpListener::bind(address).await?;
    println!("Durable Streams listening on http://{address}/v1/stream/<path>");
    if serverless {
        let runtime = registry.into_serverless_runtime(serve_config).await?;
        app = app.merge(serverless_http::router(runtime.clone()));
        let server_shutdown = shutdown.clone();
        tokio::select! {
            result = axum::serve(listener, app).with_graceful_shutdown(async move {
                server_shutdown.cancelled().await;
            }) => result.context("facade server")?,
            _ = tokio::signal::ctrl_c() => shutdown.cancel(),
        }
        runtime.shutdown().await;
        return Ok(());
    }

    let actor_shutdown = shutdown.clone();
    let mut actors = tokio::spawn(async move {
        registry
            .serve_with_config(serve_config, actor_shutdown)
            .await
    });

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

fn client_config(serve_config: &ServeConfig) -> anyhow::Result<ClientConfig> {
    let token = serve_config.token.clone();
    let authorization = token.as_ref().map(|token| format!("Bearer {token}"));
    let mut config = ClientConfig::new(serve_config.endpoint.clone())
        .token_opt(token)
        .namespace(serve_config.namespace.clone())
        .pool_name(serve_config.pool_name.clone());
    if let Some(authorization) = authorization {
        config = config.header(http::header::AUTHORIZATION.as_str(), authorization);
    }
    Ok(config)
}

fn normalize_serve_config(mut serve_config: ServeConfig) -> anyhow::Result<ServeConfig> {
    let mut endpoint = url::Url::parse(&serve_config.endpoint).context("parse RIVET_ENDPOINT")?;
    if let Some(namespace) = decode_url_auth(endpoint.username()).filter(|value| !value.is_empty())
    {
        serve_config.namespace = namespace;
    }
    if let Some(token) = endpoint.password().and_then(decode_url_auth) {
        serve_config.token = Some(token);
    }
    endpoint
        .set_username("")
        .map_err(|_| anyhow::anyhow!("clear RIVET_ENDPOINT username"))?;
    endpoint
        .set_password(None)
        .map_err(|_| anyhow::anyhow!("clear RIVET_ENDPOINT password"))?;

    serve_config.endpoint = endpoint.to_string();
    Ok(serve_config)
}

fn decode_url_auth(value: &str) -> Option<String> {
    percent_decode_str(value).decode_utf8().ok().map(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_config_extracts_url_auth() {
        let serve_config = normalize_serve_config(ServeConfig {
            endpoint: "https://cloud-ns:sk_cloud%2Dtoken@api.rivet.dev".to_owned(),
            token: Some("dev".to_owned()),
            namespace: "default".to_owned(),
            pool_name: "durable-streams".to_owned(),
            ..ServeConfig::default()
        })
        .unwrap();

        let config = client_config(&serve_config).unwrap();
        assert_eq!(config.endpoint, "https://api.rivet.dev/");
        assert_eq!(config.namespace.as_deref(), Some("cloud-ns"));
        assert_eq!(config.token.as_deref(), Some("sk_cloud-token"));
        assert_eq!(config.pool_name.as_deref(), Some("durable-streams"));
        assert_eq!(
            config
                .headers
                .as_ref()
                .and_then(|headers| headers.get("authorization"))
                .map(String::as_str),
            Some("Bearer sk_cloud-token")
        );
    }
}
