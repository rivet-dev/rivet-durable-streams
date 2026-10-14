use std::sync::Arc;

use axum::{
    body::Body,
    http::{HeaderMap, Method, StatusCode},
    response::Response,
};
use serde_json::{Value, json};
use tokio::sync::OnceCell;

use super::{Control, Reply, catalog_import::CatalogApi, model::ApiError};

/// Subscription management is closed by default. Embedders may explicitly use
/// their existing authenticated boundary instead of a separate bearer secret.
#[derive(Clone, Default)]
pub struct SubscriptionConfig {
    /// Reconcile actor declarations from this service runtime's Engine namespace.
    pub actor_routing: bool,
    pub management_token: Option<String>,
    pub allow_unauthenticated: bool,
    pub allow_local_http: bool,
    pub catalog_api: Option<CatalogApi>,
}
impl std::fmt::Debug for SubscriptionConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubscriptionConfig")
            .field(
                "management_token_configured",
                &self.management_token.is_some(),
            )
            .field("allow_unauthenticated", &self.allow_unauthenticated)
            .field("allow_local_http", &self.allow_local_http)
            .field("catalog_api_configured", &self.catalog_api.is_some())
            .finish()
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn route(
    client: &rivetkit_client::Client,
    config: &crate::facade::DurableStreamsConfig,
    initialized: &Arc<OnceCell<()>>,
    original_path: &str,
    routed_path: &str,
    method: Method,
    headers: &HeaderMap,
    body: &[u8],
) -> Response {
    let started = std::time::Instant::now();
    match dispatch(
        client,
        config,
        initialized,
        original_path,
        routed_path,
        method,
        headers,
        body,
    )
    .await
    {
        Ok((mut reply, jwks)) => {
            if jwks && let Some(age) = reply.age.as_mut() {
                // Include the entire actor round trip conservatively, so waiting
                // for legacy storage or serialization never resets cache age.
                let elapsed = started.elapsed();
                *age = age
                    .saturating_add(elapsed.as_secs())
                    .saturating_add(u64::from(elapsed.subsec_nanos() > 0));
                if *age >= 300 {
                    return response(
                        ApiError::new(
                            503,
                            "SIGNING_DISCOVERY_STALE",
                            "Engine discovery exceeded its freshness budget",
                        )
                        .into(),
                    );
                }
            }
            let mut r = response(reply);
            if jwks && r.status().is_success() {
                r.headers_mut().insert(
                    http::header::CONTENT_TYPE,
                    http::HeaderValue::from_static("application/jwk-set+json"),
                );
                r.headers_mut().insert(
                    http::header::CACHE_CONTROL,
                    http::HeaderValue::from_static("public, max-age=300"),
                );
            }
            r
        }
        Err(e) => response(e.into()),
    }
}
#[allow(clippy::too_many_arguments)]
async fn dispatch(
    client: &rivetkit_client::Client,
    config: &crate::facade::DurableStreamsConfig,
    initialized: &Arc<OnceCell<()>>,
    original_path: &str,
    routed_path: &str,
    method: Method,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<(Reply, bool), ApiError> {
    let decoded = percent_encoding::percent_decode_str(routed_path)
        .decode_utf8()
        .map_err(|_| ApiError::invalid("Invalid path encoding"))?;
    let path = decoded.trim_start_matches('/');
    let mut parts = path.split('/');
    if parts.next() != Some("__ds") {
        return Err(ApiError::new(404, "NOT_FOUND", "Unknown control route"));
    }
    let kind = parts.next();
    let (id, operation, member) = if kind == Some("jwks.json") && parts.next().is_none() {
        if method != Method::GET {
            return Err(ApiError::new(405, "METHOD_NOT_ALLOWED", "Expected GET"));
        }
        (None, "jwks", None)
    } else if kind == Some("subscriptions") {
        let id = parts
            .next()
            .filter(|s| {
                !s.is_empty()
                    && s.len() <= 256
                    && *s != "."
                    && *s != ".."
                    && !s.chars().any(char::is_control)
            })
            .ok_or_else(|| ApiError::invalid("Invalid subscription id"))?;
        let remainder = parts.collect::<Vec<_>>();
        let (op, member) = match remainder.as_slice() {
            [] => match method {
                Method::PUT => ("put", None),
                Method::GET => ("get", None),
                Method::DELETE => ("delete", None),
                _ => {
                    return Err(ApiError::new(
                        405,
                        "METHOD_NOT_ALLOWED",
                        "Expected PUT, GET or DELETE",
                    ));
                }
            },
            ["streams"] if method == Method::POST => ("add", None),
            ["streams", rest @ ..] if !rest.is_empty() && method == Method::DELETE => {
                ("remove", Some(rest.join("/")))
            }
            [op @ ("claim" | "ack" | "release" | "callback")] if method == Method::POST => {
                (*op, None)
            }
            ["streams" | "claim" | "ack" | "release" | "callback"] => {
                return Err(ApiError::new(
                    405,
                    "METHOD_NOT_ALLOWED",
                    "Unsupported control method",
                ));
            }
            _ => return Err(ApiError::new(404, "NOT_FOUND", "Unknown control route")),
        };
        (Some(id.to_owned()), op, member)
    } else {
        return Err(ApiError::new(404, "NOT_FOUND", "Unknown control route"));
    };
    let bearer = headers
        .get(http::header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "));
    if !matches!(operation, "jwks" | "callback" | "ack" | "release")
        && !config.subscriptions.allow_unauthenticated
    {
        let valid = config
            .subscriptions
            .management_token
            .as_deref()
            .zip(bearer)
            .is_some_and(|(expected, got)| {
                super::security::constant_time_equal(expected.as_bytes(), got.as_bytes())
            });
        if !valid {
            return Err(ApiError::new(
                401,
                "UNAUTHORIZED",
                "Subscription management requires authorization",
            ));
        }
    }
    let body = if body.is_empty() {
        json!({})
    } else {
        serde_json::from_slice::<Value>(body).map_err(|_| ApiError::invalid("Invalid JSON body"))?
    };
    // Derive the mount from Axum's original/routed paths, never from forwarded headers.
    let root = original_path
        .strip_suffix(routed_path)
        .unwrap_or("")
        .trim_end_matches('/')
        .to_owned();
    let origin = config.public_origin.as_deref().ok_or_else(|| {
        ApiError::new(
            503,
            "NOT_CONFIGURED",
            "PUBLIC_ORIGIN is required for subscription URLs",
        )
    })?;
    let parsed = url::Url::parse(origin)
        .map_err(|_| ApiError::new(503, "NOT_CONFIGURED", "Invalid public origin"))?;
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || parsed.path() != "/"
    {
        return Err(ApiError::new(
            503,
            "NOT_CONFIGURED",
            "Public origin must contain only scheme and authority",
        ));
    }
    // Only pattern backfill needs namespace enumeration. Explicit memberships
    // refresh their addressed streams directly, including on later additions.
    let import_catalog = operation == "put"
        && body
            .get("pattern")
            .is_some_and(|pattern| !pattern.is_null());
    let request = Control {
        root: root.clone(),
        public_base: format!("{}{root}", origin.trim_end_matches('/')),
        id,
        operation: operation.to_owned(),
        body,
        token: bearer.map(str::to_owned),
        path: member,
        allow_local: config.subscriptions.allow_local_http,
        deadline: None,
    };
    let actor = super::handle(client, &config.tenant_scope).map_err(gateway)?;
    if import_catalog
        && initialized.get().is_none()
        && let Some(api) = &config.subscriptions.catalog_api
    {
        // Validate/reconfirm atomically before migration. A conflicting PUT
        // must return 409 even when an unrelated stream runner is unavailable.
        let mut confirm = request.clone();
        confirm.operation = "confirm".to_owned();
        let reply = control(&actor, confirm, config.actor_timeout).await?;
        if reply.status != 404 {
            return Ok((reply, false));
        }
        initialized
            .get_or_try_init(|| async {
                super::catalog_import::import(client, api, &config.tenant_scope).await
            })
            .await
            .map_err(|error| {
                tracing::warn!(?error, "Subscription catalog import failed");
                ApiError::new(
                    503,
                    "CATALOG_UNAVAILABLE",
                    "Existing-stream catalog is not ready",
                )
            })?;
    }
    let reply = control(&actor, request, config.actor_timeout).await?;
    Ok((reply, operation == "jwks"))
}

async fn control(
    actor: &rivetkit_client::handle::ActorHandle,
    mut request: Control,
    timeout: std::time::Duration,
) -> Result<Reply, ApiError> {
    let budget_ms = timeout.as_millis().min(i64::MAX as u128) as i64;
    request.deadline = Some(
        super::now_ms()
            .saturating_add(budget_ms)
            .saturating_sub((budget_ms / 10).min(1_000)),
    );
    let value = tokio::time::timeout(
        timeout,
        actor.action(
            super::CONTROL_NAME,
            vec![serde_json::to_value(request).map_err(|e| gateway(e.into()))?],
        ),
    )
    .await
    .map_err(|_| ApiError::new(504, "TIMEOUT", "Subscription request timed out"))?
    .map_err(gateway)?;
    serde_json::from_value(value).map_err(|e| gateway(e.into()))
}
fn gateway(error: anyhow::Error) -> ApiError {
    tracing::warn!(?error, "Subscription actor request failed");
    ApiError::new(502, "UNAVAILABLE", "Subscription actor unavailable")
}
fn response(reply: Reply) -> Response {
    let status = StatusCode::from_u16(reply.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let body = if status == StatusCode::NO_CONTENT {
        Body::empty()
    } else {
        Body::from(serde_json::to_vec(&reply.body).unwrap())
    };
    let mut r = crate::facade::cors_response(status, body);
    r.headers_mut().insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("application/json"),
    );
    r.headers_mut().insert(
        http::header::CACHE_CONTROL,
        http::HeaderValue::from_static("no-store"),
    );
    if let Some(seconds) = reply.retry_after {
        r.headers_mut()
            .insert(http::header::RETRY_AFTER, seconds.into());
    }
    if let Some(age) = reply.age {
        r.headers_mut().insert(http::header::AGE, age.into());
    }
    r
}
