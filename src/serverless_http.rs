use std::collections::HashMap;

use axum::Router;
use axum::body::{Body, Bytes, to_bytes};
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::Response;
use axum::routing::any;
use futures_util::stream;
use rivetkit::{CoreServerlessRuntime, ServerlessRequest, ServerlessResponse};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio_util::sync::CancellationToken;

pub fn router(runtime: CoreServerlessRuntime) -> Router {
    Router::new()
        .route("/api/rivet", any(handle))
        .route("/api/rivet/{*path}", any(handle))
        .with_state(runtime)
}

async fn handle(State(runtime): State<CoreServerlessRuntime>, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let body = match to_bytes(body, runtime.max_request_body_bytes()).await {
        Ok(body) => body,
        Err(_) => {
            return into_response(
                runtime.incoming_too_long_response(),
                CancellationToken::new(),
            );
        }
    };
    let path_and_query = parts
        .uri
        .path_and_query()
        .map(|value| value.as_str())
        .unwrap_or("/");
    let cancel_token = CancellationToken::new();
    let request = ServerlessRequest {
        method: parts.method.as_str().to_owned(),
        url: format!("http://internal{path_and_query}"),
        headers: copy_headers(&parts.headers),
        body: body.to_vec(),
        cancel_token: cancel_token.clone(),
    };
    into_response(runtime.handle_request(request).await, cancel_token)
}

fn copy_headers(source: &HeaderMap) -> HashMap<String, String> {
    let mut headers = HashMap::new();
    for (name, value) in source {
        let Ok(value) = value.to_str() else {
            continue;
        };
        headers
            .entry(name.as_str().to_owned())
            .and_modify(|existing: &mut String| {
                existing.push_str(", ");
                existing.push_str(value);
            })
            .or_insert_with(|| value.to_owned());
    }
    headers
}

fn into_response(response: ServerlessResponse, cancel_token: CancellationToken) -> Response {
    let status = StatusCode::from_u16(response.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let mut headers = HeaderMap::new();
    for (name, value) in response.headers {
        if let (Ok(name), Ok(value)) = (
            HeaderName::try_from(name.as_str()),
            HeaderValue::from_str(&value),
        ) {
            headers.append(name, value);
        }
    }
    let stream = stream::unfold(
        ResponseStream {
            receiver: response.body,
            cancel_token,
        },
        |mut state| async move {
            let chunk = state.receiver.recv().await?;
            let chunk = chunk
                .map(Bytes::from)
                .map_err(|error| std::io::Error::other(error.message));
            Some((chunk, state))
        },
    );
    let mut output = Response::new(Body::from_stream(stream));
    *output.status_mut() = status;
    *output.headers_mut() = headers;
    output
}

struct ResponseStream {
    receiver: UnboundedReceiver<std::result::Result<Vec<u8>, rivetkit::ServerlessStreamError>>,
    cancel_token: CancellationToken,
}

impl Drop for ResponseStream {
    fn drop(&mut self) {
        self.cancel_token.cancel();
    }
}
