use super::*;

#[test]
fn caller_credentials_are_required_and_must_be_unambiguous() {
    let mut headers = HeaderMap::new();
    assert_eq!(credential(&headers), Err(StatusCode::UNAUTHORIZED));
    headers.insert("authorization", "Bearer caller".parse().unwrap());
    assert_eq!(credential(&headers).unwrap(), "caller");
    headers.insert("x-rivet-token", "process".parse().unwrap());
    assert_eq!(credential(&headers), Err(StatusCode::UNAUTHORIZED));
    headers.remove("x-rivet-token");
    headers.insert("authorization", "Basic caller".parse().unwrap());
    assert_eq!(credential(&headers), Err(StatusCode::UNAUTHORIZED));
}

#[tokio::test]
async fn rejected_caller_never_retries_with_process_authority() {
    use axum::{Router, extract::State, routing::put};
    use std::sync::Arc;
    use tokio::sync::Mutex;
    let observed = Arc::new(Mutex::new(Vec::<String>::new()));
    async fn reject(State(seen): State<Arc<Mutex<Vec<String>>>>, headers: HeaderMap) -> StatusCode {
        seen.lock()
            .await
            .push(headers["authorization"].to_str().unwrap().to_owned());
        StatusCode::FORBIDDEN
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let app = Router::new()
        .route("/actors", put(reject))
        .with_state(observed.clone());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let config = rivetkit_client::ClientConfig::new(endpoint)
        .namespace("test")
        .pool_name("services")
        .token("process-secret")
        .header("authorization", "Bearer process-secret");
    let config = DurableStreamsConfig {
        data_client: Some((&config).into()),
        ..Default::default()
    };
    let mut headers = HeaderMap::new();
    headers.insert("authorization", "Bearer caller-token".parse().unwrap());
    let caller = Caller::new(&config, &headers, "/events", &Method::PUT).unwrap();
    assert_eq!(
        caller.resolve("key", true).await,
        Err(StatusCode::FORBIDDEN)
    );
    assert_eq!(*observed.lock().await, vec!["Bearer caller-token"]);
    assert!(matches!(
        Caller::new(&config, &HeaderMap::new(), "/events", &Method::PUT),
        Err(StatusCode::UNAUTHORIZED)
    ));
    server.abort();
}
