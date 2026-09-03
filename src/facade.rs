use std::time::Duration;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::extract::{OriginalUri, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri};
use axum::response::Response;
use axum::routing::any;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use futures_util::StreamExt;
use rivetkit_client::{Client, GetOptions, GetOrCreateOptions};
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::actor::ACTOR_NAME;
use crate::protocol::{BRIDGED_REQUEST_HEADERS, INTERNAL_HEADER_PREFIX, MAX_BODY_BYTES};

const ALLOW_HEADERS: &str = "content-type, authorization, If-None-Match, Stream-Seq, Stream-TTL, Stream-Expires-At, Stream-Closed, Producer-Id, Producer-Epoch, Producer-Seq, Stream-Forked-From, Stream-Fork-Offset, Stream-Fork-Sub-Offset";
const EXPOSE_HEADERS: &str = "Stream-Next-Offset, Stream-Cursor, Stream-Up-To-Date, Stream-Closed, Stream-TTL, Stream-Expires-At, Producer-Epoch, Producer-Seq, Producer-Expected-Seq, Producer-Received-Seq, stream-sse-data-encoding, etag, content-type, content-encoding, location, vary";

#[derive(Clone, Debug)]
pub struct DurableStreamsConfig {
    /// Separates deterministic actor keys between applications or tenants.
    pub tenant_scope: String,
    /// Public origin used to construct the `Location` header on create.
    pub public_origin: Option<String>,
    /// Deadline for collecting a public request body before actor lookup.
    pub body_timeout: Duration,
    /// Deadline for actor resolution and finite responses. SSE responses only
    /// apply this deadline until response headers are received.
    pub actor_timeout: Duration,
}

impl Default for DurableStreamsConfig {
    fn default() -> Self {
        Self {
            tenant_scope: "default".to_owned(),
            public_origin: None,
            body_timeout: Duration::from_secs(15),
            actor_timeout: Duration::from_secs(15),
        }
    }
}

#[derive(Clone)]
struct FacadeState {
    client: Client,
    config: DurableStreamsConfig,
}

/// Returns a router intended to be nested at the application's stream root:
///
/// ```ignore
/// app.nest("/v1/stream", durable_streams_router(client, config))
/// ```
pub fn durable_streams_router(client: Client, config: DurableStreamsConfig) -> Router {
    Router::new()
        .route("/", any(handle_root))
        .route("/{*path}", any(handle))
        .with_state(FacadeState { client, config })
}

async fn handle_root(method: Method) -> Response {
    if method == Method::OPTIONS {
        cors_response(StatusCode::NO_CONTENT, Body::empty())
    } else {
        cors_response(StatusCode::NOT_FOUND, Body::from("Stream not found"))
    }
}

async fn handle(
    State(state): State<FacadeState>,
    OriginalUri(original_uri): OriginalUri,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Response {
    if method == Method::OPTIONS {
        return cors_response(StatusCode::NO_CONTENT, Body::empty());
    }
    if !matches!(
        method,
        Method::GET | Method::HEAD | Method::PUT | Method::POST | Method::DELETE
    ) {
        let mut response = cors_response(StatusCode::METHOD_NOT_ALLOWED, Body::empty());
        response.headers_mut().insert(
            http::header::ALLOW,
            HeaderValue::from_static("GET, POST, PUT, DELETE, HEAD, OPTIONS"),
        );
        return response;
    }

    // Actor identity uses the complete public path. This deliberately matches
    // Stream-Forked-From, whose value is a path relative to this same server.
    let canonical_path = match canonical_stream_path(original_uri.path()) {
        Ok(path) => path,
        Err(message) => {
            return cors_response(StatusCode::BAD_REQUEST, Body::from(message));
        }
    };
    let routed_path = uri.path();
    if routed_path.ends_with("/__ds") || routed_path.contains("/__ds/") {
        return cors_response(StatusCode::NOT_FOUND, Body::from("Stream not found"));
    }

    let body = match tokio::time::timeout(
        state.config.body_timeout,
        to_bytes(body, MAX_BODY_BYTES + 1),
    )
    .await
    {
        Ok(Ok(body)) if body.len() <= MAX_BODY_BYTES => body,
        Ok(Ok(_)) | Ok(Err(_)) => {
            return cors_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                Body::from("Payload too large"),
            );
        }
        Err(_) => {
            return cors_response(
                StatusCode::REQUEST_TIMEOUT,
                Body::from("Request body timed out"),
            );
        }
    };

    let actor_key = stream_actor_key(&state.config.tenant_scope, &canonical_path);
    let actor = if method == Method::PUT {
        state.client.get_or_create(
            ACTOR_NAME,
            vec![actor_key],
            GetOrCreateOptions {
                create_with_input: Some(json!({
                    "path": canonical_path,
                    "tenant_scope": state.config.tenant_scope.clone(),
                })),
                ..GetOrCreateOptions::default()
            },
        )
    } else {
        state
            .client
            .get(ACTOR_NAME, vec![actor_key], GetOptions::default())
    };
    let actor = match actor {
        Ok(actor) => actor,
        Err(error) => {
            return cors_response(
                StatusCode::BAD_GATEWAY,
                Body::from(format!("Actor request failed: {error}")),
            );
        }
    };
    let actor = if method == Method::PUT {
        match tokio::time::timeout(state.config.actor_timeout, actor.resolve_handle()).await {
            Ok(Ok(actor)) => actor,
            Ok(Err(error)) => {
                return cors_response(
                    StatusCode::BAD_GATEWAY,
                    Body::from(format!("Actor resolution failed: {error}")),
                );
            }
            Err(_) => {
                return cors_response(
                    StatusCode::GATEWAY_TIMEOUT,
                    Body::from("Actor resolution timed out"),
                );
            }
        }
    } else {
        match tokio::time::timeout(state.config.actor_timeout, actor.resolve_optional()).await {
            Ok(Ok(Some(actor))) => actor,
            Ok(Ok(None)) => {
                return cors_response(StatusCode::NOT_FOUND, Body::from("Stream not found"));
            }
            Ok(Err(error)) => {
                return cors_response(
                    StatusCode::BAD_GATEWAY,
                    Body::from(format!("Actor resolution failed: {error}")),
                );
            }
            Err(_) => {
                return cors_response(
                    StatusCode::GATEWAY_TIMEOUT,
                    Body::from("Actor resolution timed out"),
                );
            }
        }
    };
    let actor_headers = match bridge_request_headers(&headers) {
        Ok(headers) => headers,
        Err(message) => {
            return cors_response(StatusCode::BAD_REQUEST, Body::from(message));
        }
    };
    let actor_path = match uri.query() {
        Some(query) => format!("protocol?{query}"),
        None => "protocol".to_owned(),
    };
    let actor_response = match tokio::time::timeout(
        state.config.actor_timeout,
        actor.fetch(
            &actor_path,
            method.clone(),
            actor_headers,
            (!body.is_empty()).then_some(body),
        ),
    )
    .await
    {
        Ok(Ok(response)) => response,
        Ok(Err(error)) => {
            return cors_response(
                StatusCode::BAD_GATEWAY,
                Body::from(format!("Actor request failed: {error}")),
            );
        }
        Err(_) => {
            return cors_response(
                StatusCode::GATEWAY_TIMEOUT,
                Body::from("Actor request timed out"),
            );
        }
    };

    let status = actor_response.status();
    let response_headers = actor_response.headers().clone();
    let response_stream = actor_response
        .bytes_stream()
        .map(|result| result.map_err(std::io::Error::other));
    let mut response = Response::builder()
        .status(status)
        .body(Body::from_stream(response_stream))
        .expect("valid actor response");
    for (name, value) in &response_headers {
        if !name.as_str().starts_with("x-rivet-") {
            response.headers_mut().append(name, value.clone());
        }
    }
    apply_cors(response.headers_mut());
    if method == Method::PUT && status.is_success() {
        let location = match &state.config.public_origin {
            Some(origin) => format!("{}{}", origin.trim_end_matches('/'), original_uri.path()),
            None => {
                let scheme = headers
                    .get("x-forwarded-proto")
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.split(',').next())
                    .map(str::trim)
                    .filter(|value| *value == "http" || *value == "https")
                    .unwrap_or("http");
                let host = headers
                    .get(http::header::HOST)
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or("localhost");
                format!("{scheme}://{host}{}", original_uri.path())
            }
        };
        if let Ok(value) = HeaderValue::from_str(&location) {
            response.headers_mut().insert(http::header::LOCATION, value);
        }
    }
    response
}

fn bridge_request_headers(headers: &HeaderMap) -> Result<HeaderMap, String> {
    let mut bridged = HeaderMap::new();
    for name in BRIDGED_REQUEST_HEADERS {
        let header_name = HeaderName::from_static(name);
        if let Some(value) = headers.get(&header_name) {
            let internal_name =
                HeaderName::from_bytes(format!("{INTERNAL_HEADER_PREFIX}{name}").as_bytes())
                    .map_err(|error| error.to_string())?;
            let encoded = URL_SAFE_NO_PAD.encode(value.as_bytes());
            let encoded = HeaderValue::from_str(&encoded).map_err(|error| error.to_string())?;
            bridged.insert(internal_name, encoded);
        }
    }
    Ok(bridged)
}

fn canonical_stream_path(path: &str) -> Result<String, String> {
    let base = url::Url::parse("https://durable-stream.invalid/").expect("valid static URL");
    let canonical = base
        .join(path)
        .map_err(|error| format!("Invalid stream path: {error}"))?;
    let path = canonical.path().to_owned();
    if path == "/" {
        return Err("Stream path must not be empty".to_owned());
    }
    Ok(path)
}

pub(crate) fn stream_actor_key(tenant_scope: &str, path: &str) -> String {
    let mut hash = Sha256::new();
    hash.update(tenant_scope.as_bytes());
    hash.update([0]);
    hash.update(path.as_bytes());
    format!("{:x}", hash.finalize())
}

fn cors_response(status: StatusCode, body: Body) -> Response {
    let mut response = Response::builder()
        .status(status)
        .body(body)
        .expect("valid facade response");
    apply_cors(response.headers_mut());
    response
}

fn apply_cors(headers: &mut HeaderMap) {
    headers.insert(
        http::header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
    headers.insert(
        http::header::ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static("GET, POST, PUT, DELETE, HEAD, OPTIONS"),
    );
    headers.insert(
        http::header::ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static(ALLOW_HEADERS),
    );
    headers.insert(
        http::header::ACCESS_CONTROL_EXPOSE_HEADERS,
        HeaderValue::from_static(EXPOSE_HEADERS),
    );
    headers.insert(
        HeaderName::from_static("x-content-type-options"),
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        HeaderName::from_static("cross-origin-resource-policy"),
        HeaderValue::from_static("cross-origin"),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_are_tenant_and_path_scoped() {
        assert_eq!(stream_actor_key("a", "/x").len(), 64);
        assert_ne!(stream_actor_key("a", "/x"), stream_actor_key("b", "/x"));
        assert_ne!(stream_actor_key("a", "/x"), stream_actor_key("a", "/y"));
    }

    #[test]
    fn canonical_path_normalizes_dot_segments() {
        assert_eq!(canonical_stream_path("/a/../b").unwrap(), "/b");
        assert_eq!(
            canonical_stream_path("/users/__ds/events").unwrap(),
            "/users/__ds/events"
        );
    }

    #[test]
    fn header_bridge_preserves_non_utf8_bytes() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("stream-seq"),
            HeaderValue::from_bytes(&[0x80, b'a']).unwrap(),
        );
        let bridged = bridge_request_headers(&headers).unwrap();
        let encoded = bridged.get("x-rivet-ds-h-stream-seq").unwrap();
        assert_eq!(
            URL_SAFE_NO_PAD.decode(encoded.as_bytes()).unwrap(),
            [0x80, b'a']
        );
    }
}
