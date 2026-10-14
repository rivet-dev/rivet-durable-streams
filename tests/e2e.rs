use std::time::Duration;

use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Method, StatusCode};
use rivet_durable_streams::{ACTOR_NAME, register};
use rivetkit::{Registry, ServeConfig};
use rivetkit_client::{Client, ClientConfig, GetOrCreateOptions};
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

#[tokio::test]
#[ignore = "requires a matching local Rivet Engine"]
async fn create_append_read_sse_and_delete() -> anyhow::Result<()> {
    let mut registry = Registry::new();
    register(&mut registry);
    let shutdown = CancellationToken::new();
    let _shutdown_guard = shutdown.clone().drop_guard();
    let mut serve = None;
    let mut runtime = None;
    let client = if let Ok(endpoint) = std::env::var("RIVET_TEST_ENDPOINT") {
        let pool = format!("durable-streams-e2e-{}", uuid::Uuid::new_v4());
        let token = std::env::var("RIVET_TOKEN").unwrap_or_else(|_| "dev".into());
        let namespace = std::env::var("RIVET_NAMESPACE").unwrap_or_else(|_| "default".into());
        let config = ServeConfig {
            endpoint: endpoint.clone(),
            token: Some(token.clone()),
            namespace: namespace.clone(),
            pool_name: pool.clone(),
            ..ServeConfig::default()
        };
        serve = Some(tokio::spawn(
            registry.serve_with_config(config, shutdown.clone()),
        ));
        Client::new(
            ClientConfig::new(endpoint)
                .token(&token)
                .header("authorization", format!("Bearer {token}"))
                .namespace(namespace)
                .pool_name(pool),
        )
    } else {
        runtime = Some(rivetkit::test::setup(registry).await?);
        runtime.as_ref().unwrap().client().clone()
    };
    let path = format!("/integration/{}", uuid::Uuid::new_v4());
    let mut hash = Sha256::new();
    hash.update(b"test\0");
    hash.update(path.as_bytes());
    let actor = client.get_or_create(
        ACTOR_NAME,
        vec![format!("{:x}", hash.finalize())],
        GetOrCreateOptions {
            create_with_input: Some(json!({ "path": path, "tenant_scope": "test" })),
            ..GetOrCreateOptions::default()
        },
    )?;

    // The public registry API has no readiness observer. A successful HEAD
    // proves the new pool and actor are ready without retrying mutations.
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Ok(response) = actor
                .fetch("protocol", Method::HEAD, HeaderMap::new(), None)
                .await
                && response.status() == StatusCode::NOT_FOUND
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await?;

    let response = actor
        .fetch(
            "protocol",
            Method::PUT,
            headers(&[("content-type", "application/json")]),
            Some(Bytes::from_static(br#"[{"n":1}]"#)),
        )
        .await?;
    assert_eq!(response.status(), StatusCode::CREATED);
    assert!(response.headers().contains_key("stream-next-offset"));

    let response = actor
        .fetch(
            "protocol",
            Method::POST,
            headers(&[
                ("content-type", "application/json"),
                ("stream-closed", "true"),
            ]),
            Some(Bytes::from_static(br#"{"n":2}"#)),
        )
        .await?;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(response.headers()["stream-closed"], "true");

    let response = actor
        .fetch("protocol?offset=-1", Method::GET, HeaderMap::new(), None)
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["stream-up-to-date"], "true");
    assert_eq!(response.headers()["stream-closed"], "true");
    assert_eq!(response.bytes().await?, br#"[{"n":1},{"n":2}]"#[..]);

    let response = actor
        .fetch(
            "protocol?offset=-1&live=sse",
            Method::GET,
            HeaderMap::new(),
            None,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "text/event-stream");
    let body = response.text().await?;
    assert!(body.contains("event: data\n"));
    assert!(body.contains(r#"data:[{"n":1},{"n":2}]"#));
    let control = body
        .split("\n\n")
        .find(|frame| frame.starts_with("event: control\n"))
        .and_then(|frame| frame.lines().find_map(|line| line.strip_prefix("data:")))
        .expect("SSE control frame");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(control)?["streamClosed"],
        true
    );

    let response = actor
        .fetch("protocol", Method::DELETE, HeaderMap::new(), None)
        .await?;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let response = actor
        .fetch("protocol", Method::HEAD, HeaderMap::new(), None)
        .await?;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    shutdown.cancel();
    if let Some(serve) = serve {
        tokio::time::timeout(Duration::from_secs(30), serve).await???;
    }
    if let Some(runtime) = runtime {
        runtime.shutdown().await;
    }
    Ok(())
}

fn headers(entries: &[(&'static str, &'static str)]) -> HeaderMap {
    entries
        .iter()
        .map(|(name, value)| {
            (
                http::header::HeaderName::from_static(name),
                HeaderValue::from_static(value),
            )
        })
        .collect()
}
