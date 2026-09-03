use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Method, StatusCode};
use rivet_durable_streams::{ACTOR_NAME, register};
use rivetkit::Registry;
use rivetkit_client::GetOrCreateOptions;
use serde_json::json;

#[tokio::test]
#[ignore = "requires a matching local Rivet Engine"]
async fn create_append_read_sse_and_delete() -> anyhow::Result<()> {
    let mut registry = Registry::new();
    register(&mut registry);
    let runtime = rivetkit::test::setup(registry).await?;
    let actor = runtime.client().get_or_create(
        ACTOR_NAME,
        vec![format!("durable-stream-{}", uuid::Uuid::new_v4())],
        GetOrCreateOptions {
            create_with_input: Some(json!({ "path": "/integration", "tenant_scope": "test" })),
            ..GetOrCreateOptions::default()
        },
    )?;

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
    assert!(body.contains("event: control\n"));
    assert!(body.contains(r#"\"streamClosed\":true"#));

    let response = actor
        .fetch("protocol", Method::DELETE, HeaderMap::new(), None)
        .await?;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let response = actor
        .fetch("protocol", Method::HEAD, HeaderMap::new(), None)
        .await?;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    runtime.shutdown().await;
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
