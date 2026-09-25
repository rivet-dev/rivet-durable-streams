use super::*;

#[test]
fn credentials_are_limited_to_the_configured_gateway() {
    let endpoint = "https://namespace:secret@engine.gameinc.io/base";
    let valid =
        url::Url::parse("https://engine.gameinc.io:443/base/gateway/actor-123/__rivet/stream/wake")
            .unwrap();
    let header = gateway_token(&valid, Some(endpoint), Some("test-token"))
        .unwrap()
        .unwrap();
    assert_eq!(header, "test-token");
    assert!(header.is_sensitive());
    for path in [
        "http://engine.gameinc.io/base/gateway/actor-123/health",
        "https://engine.gameinc.io:444/base/gateway/actor-123/health",
        "https://engine.gameinc.io.evil.gameinc.io/base/gateway/actor-123/health",
        "https://other.gameinc.io/base/gateway/actor-123/health",
        "https://engine.gameinc.io/base/auth/webhooks/sign",
        "https://engine.gameinc.io/base/gateway/",
        "https://engine.gameinc.io/base/gateway/actor@other/health",
        "https://engine.gameinc.io/base/gateway/actor%40other/health",
        "https://engine.gameinc.io/gateway/actor-123/health",
    ] {
        assert!(
            gateway_token(
                &url::Url::parse(path).unwrap(),
                Some(endpoint),
                Some("test-token")
            )
            .unwrap()
            .is_none(),
            "unexpected credential on {path}"
        );
    }
}

#[tokio::test]
async fn delivery_auth_is_not_sent_to_other_routes_or_redirects() {
    use axum::{
        Router,
        extract::State,
        http::{HeaderMap, StatusCode},
        routing::post,
    };
    use std::sync::Arc;
    use tokio::sync::Mutex;
    let seen = Arc::new(Mutex::new(Vec::<Option<String>>::new()));
    async fn receive(
        State(seen): State<Arc<Mutex<Vec<Option<String>>>>>,
        headers: HeaderMap,
    ) -> &'static str {
        seen.lock().await.push(
            headers
                .get("x-rivet-token")
                .map(|v| v.to_str().unwrap().to_owned()),
        );
        "{\"done\":false}"
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let app = Router::new()
        .route("/gateway/actor/hook", post(receive))
        .route("/hook", post(receive))
        .route(
            "/gateway/actor/redirect",
            post(|| async { (StatusCode::TEMPORARY_REDIRECT, [("location", "/hook")]) }),
        )
        .with_state(seen.clone());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    for path in ["/gateway/actor/hook", "/hook"] {
        assert!(
            !deliver(
                &format!("{origin}{path}"),
                true,
                "signature".into(),
                "{}".into(),
                Some(&origin),
                Some("process-token")
            )
            .await
            .unwrap()
        );
    }
    assert!(
        deliver(
            &format!("{origin}/gateway/actor/redirect"),
            true,
            "signature".into(),
            "{}".into(),
            Some(&origin),
            Some("process-token")
        )
        .await
        .is_err()
    );
    assert_eq!(*seen.lock().await, vec![Some("process-token".into()), None]);
    server.abort();
}
