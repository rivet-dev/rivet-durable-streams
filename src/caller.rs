//! Public data requests never borrow the service credential, including during
//! actor resolution. This transport also refuses redirects and ambient proxies.
use crate::{actor::ACTOR_NAME, facade::DurableStreamsConfig};
use axum::http::{HeaderMap, Method, StatusCode};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use bytes::Bytes;
use serde_json::json;

pub(crate) struct Caller {
    http: reqwest::Client,
    endpoint: url::Url,
    namespace: String,
    pool: String,
    token: String,
    path: String,
    tenant: String,
    operation: &'static str,
}

pub(crate) fn credential(headers: &HeaderMap) -> Result<String, StatusCode> {
    let bearer = match headers.get("authorization") {
        Some(value) => {
            let value = value.to_str().map_err(|_| StatusCode::UNAUTHORIZED)?;
            let (scheme, token) = value.split_once(' ').ok_or(StatusCode::UNAUTHORIZED)?;
            if !scheme.eq_ignore_ascii_case("bearer")
                || token.is_empty()
                || token.bytes().any(|b| b.is_ascii_whitespace())
            {
                return Err(StatusCode::UNAUTHORIZED);
            }
            Some(token)
        }
        None => None,
    };
    let rivet = headers
        .get("x-rivet-token")
        .map(|v| v.to_str().map_err(|_| StatusCode::UNAUTHORIZED))
        .transpose()?;
    if headers.get_all("authorization").iter().count() > 1
        || headers.get_all("x-rivet-token").iter().count() > 1
        || matches!((bearer,rivet),(Some(a),Some(b)) if a != b)
    {
        return Err(StatusCode::UNAUTHORIZED);
    }
    bearer
        .or(rivet)
        .filter(|t| !t.is_empty())
        .map(str::to_owned)
        .ok_or(StatusCode::UNAUTHORIZED)
}

impl Caller {
    pub fn new(
        config: &DurableStreamsConfig,
        headers: &HeaderMap,
        path: &str,
        method: &Method,
    ) -> Result<Self, StatusCode> {
        // Authenticate before inspecting service configuration. No anonymous fallback.
        let token = credential(headers)?;
        let connection = config
            .data_client
            .as_ref()
            .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
        let mut endpoint =
            url::Url::parse(&connection.endpoint).map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        if !matches!(endpoint.scheme(), "http" | "https") || endpoint.host_str().is_none() {
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        }
        endpoint
            .set_username("")
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        endpoint
            .set_password(None)
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        endpoint.set_query(None);
        endpoint.set_fragment(None);
        let namespace = connection
            .namespace
            .clone()
            .filter(|s| !s.is_empty())
            .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
        let pool = connection
            .pool_name
            .clone()
            .filter(|s| !s.is_empty())
            .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
        let operation = if headers
            .get("stream-closed")
            .is_some_and(|v| v.to_str().is_ok_and(|s| s.eq_ignore_ascii_case("true")))
            && matches!(*method, Method::POST | Method::PUT)
        {
            "close"
        } else {
            match *method {
                Method::GET | Method::HEAD => "read",
                Method::POST | Method::PUT => "append",
                Method::DELETE => "delete",
                _ => return Err(StatusCode::METHOD_NOT_ALLOWED),
            }
        };
        let http = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(config.actor_timeout)
            .build()
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        Ok(Self {
            http,
            endpoint,
            namespace,
            pool,
            token,
            path: path.into(),
            tenant: config.tenant_scope.clone(),
            operation,
        })
    }

    fn url(&self, path: &str) -> url::Url {
        let mut url = self.endpoint.clone();
        url.set_path(&format!("{}{}", url.path().trim_end_matches('/'), path));
        url
    }

    fn request(&self, method: Method, path: &str) -> reqwest::RequestBuilder {
        self.http
            .request(method, self.url(path))
            .bearer_auth(&self.token)
            .header("x-rivet-token", &self.token)
            .header("x-rivet-stream-path", &self.path)
            .header("x-rivet-stream-tenant", &self.tenant)
            .header("x-rivet-stream-operation", self.operation)
    }

    pub async fn resolve(&self, key: &str, create: bool) -> Result<Option<String>, StatusCode> {
        let request = if create {
            let mut input = Vec::new();
            ciborium::into_writer(
                &json!({"path":self.path,"tenant_scope":self.tenant}),
                &mut input,
            )
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            self.request(Method::PUT,"/actors").query(&[("namespace",self.namespace.as_str())]).json(&json!({"name":ACTOR_NAME,"key":key,"input":STANDARD.encode(input),"runner_name_selector":self.pool,"crash_policy":"sleep"}))
        } else {
            self.request(Method::GET, "/actors").query(&[
                ("namespace", self.namespace.as_str()),
                ("name", ACTOR_NAME),
                ("key", key),
            ])
        };
        let response = request.send().await.map_err(|_| StatusCode::BAD_GATEWAY)?;
        if !response.status().is_success() {
            return Err(response.status());
        }
        let value: serde_json::Value =
            response.json().await.map_err(|_| StatusCode::BAD_GATEWAY)?;
        let actor = if create {
            value.get("actor")
        } else {
            value["actors"].as_array().and_then(|v| v.first())
        };
        let Some(actor) = actor else { return Ok(None) };
        if actor["name"] != ACTOR_NAME
            || actor["runner_name_selector"] != self.pool
            || !actor["destroy_ts"].is_null()
        {
            return Err(StatusCode::FORBIDDEN);
        }
        let id = actor["actor_id"]
            .as_str()
            .filter(|id| {
                !id.is_empty()
                    && id
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            })
            .ok_or(StatusCode::BAD_GATEWAY)?;
        Ok(Some(id.to_owned()))
    }

    pub async fn fetch(
        &self,
        id: &str,
        query: Option<&str>,
        method: Method,
        headers: HeaderMap,
        body: Bytes,
    ) -> Result<reqwest::Response, StatusCode> {
        let mut url = self.url(&format!("/gateway/{id}/request/protocol"));
        url.set_query(query);
        self.http
            .request(method, url)
            .headers(headers)
            .bearer_auth(&self.token)
            .header("x-rivet-token", &self.token)
            .header("x-rivet-stream-path", &self.path)
            .header("x-rivet-stream-tenant", &self.tenant)
            .body(body)
            .send()
            .await
            .map_err(|_| StatusCode::BAD_GATEWAY)
    }
}

#[cfg(test)]
#[path = "../tests/unit/caller_auth.rs"]
mod tests;
