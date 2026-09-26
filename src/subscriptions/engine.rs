//! Engine owns issuance and signing. This adapter only caches public JWT keys.
use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use futures_util::StreamExt;
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use reqwest::{
    Client, RequestBuilder,
    header::{AGE, CACHE_CONTROL},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, OnceCell, RwLock};

use super::catalog_import::CatalogApi;

const MAX_RESPONSE: usize = 256 * 1024;
const MAX_TOKEN: usize = 6144;
const MAX_LIFETIME: u64 = 86400;
const KEY_REFRESH_INTERVAL: Duration = Duration::from_secs(5);
const MAX_BODY_LIMIT: usize = 128 * 1024 * 1024;

/// Messages never include an upstream body, URL, credential, or signed artifact.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EngineError {
    pub status: u16,
    pub code: &'static str,
    pub retry_after: Option<u64>,
}
impl EngineError {
    fn new(status: u16, code: &'static str) -> Self {
        Self {
            status,
            code,
            retry_after: None,
        }
    }
    fn invalid() -> Self {
        Self::new(401, "TOKEN_INVALID")
    }
    fn unavailable() -> Self {
        Self::new(503, "ENGINE_UNAVAILABLE")
    }
    fn response() -> Self {
        Self::new(503, "ENGINE_INVALID_RESPONSE")
    }
}
impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} (HTTP {})", self.code, self.status)
    }
}
impl std::error::Error for EngineError {}
type Result<T> = std::result::Result<T, EngineError>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CapabilityKind {
    Callback,
    Claim,
}
impl CapabilityKind {
    pub fn purpose(self) -> &'static str {
        match self {
            Self::Callback => "durable-streams.callback",
            Self::Claim => "durable-streams.claim",
        }
    }
    fn operations(self) -> &'static [&'static str] {
        match self {
            Self::Callback => &["callback"],
            Self::Claim => &["ack", "release"],
        }
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CapabilityClaims {
    pub root: String,
    pub tenant_scope: String,
    pub subscription_id: String,
    pub wake_id: String,
    pub generation: i64,
    pub operations: Vec<String>,
}

#[derive(Deserialize)]
pub struct IssuedToken {
    pub token: String,
    pub issued_ts: i64,
    pub expires_ts: i64,
}

pub struct VerifiedCapability {
    pub claims: CapabilityClaims,
    pub expires_ts: i64,
}

#[derive(Clone, Deserialize)]
pub struct WebhookMetadata {
    pub namespace_id: String,
    pub alg: String,
    pub kid: String,
    pub jwks_url: String,
    pub generation: u64,
}

#[derive(Deserialize)]
pub struct Signature {
    pub timestamp: String,
    pub kid: String,
    pub signature: String,
}
impl Signature {
    pub fn header(&self) -> String {
        format!(
            "t={},kid={},ed25519={}",
            self.timestamp, self.kid, self.signature
        )
    }
}

/// Preserve `max-age=300` on the DS response and propagate this age in `Age`.
pub struct FreshJwks {
    pub value: Value,
    pub age_secs: u64,
}

#[derive(Clone)]
pub struct EngineClient {
    inner: Arc<Inner>,
}
struct Inner {
    http: Client,
    endpoint: url::Url,
    issuer: String,
    namespace: String,
    token: Option<String>,
    namespace_id: OnceCell<String>,
    jwt_keys: RwLock<KeyCache>,
    jwt_refresh: Mutex<()>,
    webhook_identity: OnceCell<WebhookMetadata>,
    webhook_kids: RwLock<HashSet<String>>,
}
#[derive(Default)]
struct KeyCache {
    keys: HashMap<String, DecodingKey>,
    expires: Option<Instant>,
    last_attempt: Option<Instant>,
    last_error: Option<EngineError>,
}

impl EngineClient {
    /// `issuer` is deployment configuration, never a value learned from a token.
    pub fn new(api: &CatalogApi, issuer: Option<&str>) -> Result<Self> {
        let endpoint = trusted_url(&api.endpoint)?;
        let issuer = trusted_url(issuer.unwrap_or(&api.endpoint))?
            .as_str()
            .trim_end_matches('/')
            .to_owned();
        if issuer.len() > 512 || api.namespace.is_empty() {
            return Err(EngineError::new(503, "ENGINE_CONFIGURATION"));
        }
        let http = Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(35))
            .connect_timeout(Duration::from_secs(5))
            .pool_max_idle_per_host(8)
            .build()
            .map_err(|_| EngineError::unavailable())?;
        Ok(Self {
            inner: Arc::new(Inner {
                http,
                endpoint,
                issuer,
                namespace: api.namespace.clone(),
                token: api.token.clone(),
                namespace_id: OnceCell::new(),
                jwt_keys: RwLock::new(KeyCache::default()),
                jwt_refresh: Mutex::new(()),
                webhook_identity: OnceCell::new(),
                webhook_kids: RwLock::new(HashSet::new()),
            }),
        })
    }

    fn url(&self, path: &str) -> url::Url {
        let mut url = self.inner.endpoint.clone();
        url.set_path(&format!("{}{}", url.path().trim_end_matches('/'), path));
        url
    }
    fn authenticate(&self, request: RequestBuilder) -> Result<RequestBuilder> {
        let token = self
            .inner
            .token
            .as_deref()
            .filter(|v| !v.is_empty())
            .ok_or_else(|| EngineError::new(401, "ENGINE_CREDENTIAL_REQUIRED"))?;
        Ok(request.bearer_auth(token))
    }

    /// Resolve the existing configured namespace once; callers persist this ID to
    /// prevent namespace-name reuse from silently changing subscription authority.
    pub async fn actor_names_page(&self, cursor: Option<&str>) -> Result<Value> {
        let mut request = self.inner.http.get(self.url("/actors/names")).query(&[
            ("namespace", self.inner.namespace.as_str()),
            ("limit", "32"),
        ]);
        if let Some(cursor) = cursor {
            request = request.query(&[("cursor", cursor)]);
        }
        self.json(self.authenticate(request)?).await
    }

    pub async fn route_actor(
        &self,
        name: &str,
        pool: &str,
        id: Option<&str>,
        key: Option<&[String]>,
    ) -> Result<Option<String>> {
        let value: Value = if let Some(id) = id {
            if !identifier(id) {
                return Ok(None);
            }
            match self
                .json(
                    self.authenticate(self.inner.http.get(self.url("/actors")).query(&[
                        ("namespace", self.inner.namespace.as_str()),
                        ("actor_id", id),
                    ]))?,
                )
                .await
            {
                Ok(value) => value,
                // Stream paths are untrusted. An invalid or absent ID must not
                // block routing for unrelated streams behind this job.
                Err(error) if matches!(error.status, 400 | 404) => return Ok(None),
                Err(error) => return Err(error),
            }
        } else {
            let key = key.ok_or_else(EngineError::response)?;
            // Engine actor keys use escaped slash-separated components.
            let encoded = if key.is_empty() {
                "/".to_owned()
            } else {
                key.iter()
                    .map(|v| {
                        if v.is_empty() {
                            "\\0".to_owned()
                        } else {
                            v.replace('\\', "\\\\").replace('/', "\\/")
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("/")
            };
            if encoded.len() > 1024 {
                return Ok(None);
            }
            self.json(self.authenticate(self.inner.http.put(self.url("/actors"))
                .query(&[("namespace", self.inner.namespace.as_str())])
                .json(&json!({"name":name,"key":encoded,"runner_name_selector":pool,"crash_policy":"sleep"})))?).await?
        };
        let actor = if id.is_some() {
            value["actors"].as_array().and_then(|a| a.first())
        } else {
            value.get("actor")
        };
        let Some(actor) = actor else { return Ok(None) };
        if !matches_actor_destination(actor, name, pool, &self.namespace_id().await?, id) {
            return Ok(None);
        }
        Ok(actor["actor_id"]
            .as_str()
            .filter(|id| identifier(id))
            .map(str::to_owned))
    }

    pub fn actor_wake_url(&self, id: &str) -> String {
        self.url(&format!("/gateway/{id}/internal/streams/wake"))
            .to_string()
    }

    pub async fn namespace_id(&self) -> Result<String> {
        self.inner
            .namespace_id
            .get_or_try_init(|| async {
                #[derive(Deserialize)]
                struct Namespace {
                    namespace_id: String,
                    name: String,
                }
                #[derive(Deserialize)]
                struct Namespaces {
                    namespaces: Vec<Namespace>,
                }
                let request = self.authenticate(
                    self.inner
                        .http
                        .get(self.url("/namespaces"))
                        .query(&[("name", self.inner.namespace.as_str())])
                        .timeout(Duration::from_secs(5)),
                )?;
                let response: Namespaces = self.json(request).await?;
                let mut matches = response
                    .namespaces
                    .into_iter()
                    .filter(|n| n.name == self.inner.namespace);
                let namespace = matches
                    .next()
                    .ok_or_else(|| EngineError::new(404, "ENGINE_NAMESPACE_NOT_FOUND"))?;
                if matches.next().is_some() || !identifier(&namespace.namespace_id) {
                    return Err(EngineError::response());
                }
                Ok(namespace.namespace_id)
            })
            .await
            .cloned()
    }

    pub async fn ensure(&self) -> Result<WebhookMetadata> {
        self.inner
            .webhook_identity
            .get_or_try_init(|| async {
                let namespace = self.namespace_id().await?;
                let request = self.authenticate(
                    self.inner
                        .http
                        .post(self.url("/auth/webhooks/ensure"))
                        .timeout(Duration::from_secs(10)),
                )?;
                let metadata = self.checked_metadata(self.json(request).await?, &namespace)?;
                *self.inner.webhook_kids.write().await = HashSet::from([metadata.kid.clone()]);
                Ok(metadata)
            })
            .await
            .cloned()
    }

    pub async fn metadata(&self, namespace: &str) -> Result<WebhookMetadata> {
        if self.ensure().await?.namespace_id != namespace {
            return Err(EngineError::response());
        }
        // Ensure binds this credential before advertising even an existing ring.
        // Metadata still comes from a fresh read, not the cached ensure response.
        self.metadata_read(namespace).await
    }

    async fn metadata_read(&self, namespace: &str) -> Result<WebhookMetadata> {
        let path = namespace_path(namespace)?;
        let request = self
            .inner
            .http
            .get(self.url(&path))
            .timeout(Duration::from_secs(5));
        self.checked_metadata(self.json(request).await?, namespace)
    }

    fn checked_metadata(
        &self,
        metadata: WebhookMetadata,
        namespace: &str,
    ) -> Result<WebhookMetadata> {
        if metadata.namespace_id != namespace
            || metadata.alg != "ed25519"
            || !webhook_kid(&metadata.kid)
            || metadata.generation == 0
            || trusted_url(&metadata.jwks_url).is_err()
        {
            return Err(EngineError::response());
        }
        // Discovery always uses our configured Engine origin, never this advisory URL.
        Ok(metadata)
    }

    /// No private adapter cache: every DS public response needs an authoritative read.
    pub async fn webhook_jwks(&self, namespace: &str) -> Result<FreshJwks> {
        if self.namespace_id().await? != namespace {
            return Err(EngineError::response());
        }
        self.ensure().await?;
        let path = format!("{}/jwks.json", namespace_path(namespace)?);
        let request = || {
            self.inner
                .http
                .get(self.url(&path))
                .header(CACHE_CONTROL, "no-cache")
                .timeout(Duration::from_secs(5))
        };
        // The ensured identity is cached; the public key snapshot never is.
        let reply = response(request()).await?;
        let jwks: JwkSet = decode(&reply.body)?;
        validate_jwks(&jwks, true)?;
        let age_secs = reply.age_secs();
        if age_secs >= 300 {
            return Err(EngineError::unavailable());
        }
        // This bounded set records namespace ownership only; it never serves
        // discovery responses or takes over Engine's rotation/revocation policy.
        *self.inner.webhook_kids.write().await =
            jwks.keys.iter().map(|key| key.kid.clone()).collect();
        Ok(FreshJwks {
            value: serde_json::to_value(jwks).map_err(|_| EngineError::response())?,
            age_secs,
        })
    }

    pub async fn sign(&self, body: Bytes) -> Result<Signature> {
        self.sign_with_limit(body, MAX_BODY_LIMIT).await
    }

    pub async fn sign_with_limit(&self, body: Bytes, limit: usize) -> Result<Signature> {
        if !(1024 * 1024..=128 * 1024 * 1024).contains(&limit) {
            return Err(EngineError::new(503, "ENGINE_CONFIGURATION"));
        }
        if body.len() > limit {
            return Err(EngineError::new(413, "ENGINE_PAYLOAD_TOO_LARGE"));
        }
        let namespace = self.ensure().await?.namespace_id;
        let request = self.authenticate(
            self.inner
                .http
                .post(self.url("/auth/webhooks/sign"))
                .header("content-type", "application/octet-stream")
                .body(body),
        )?;
        let signature: Signature = self.json(request).await?;
        if signature
            .timestamp
            .parse::<u64>()
            .ok()
            .filter(|v| v.to_string() == signature.timestamp)
            .is_none()
            || !webhook_kid(&signature.kid)
            || URL_SAFE_NO_PAD
                .decode(&signature.signature)
                .map_or(true, |v| v.len() != 64)
        {
            return Err(EngineError::response());
        }
        if !self
            .inner
            .webhook_kids
            .read()
            .await
            .contains(&signature.kid)
        {
            // ACL roles are mutable. A new signature kid must belong to the
            // configured namespace, even after an earlier authenticated ensure.
            let fresh = self.webhook_jwks(&namespace).await?;
            if !fresh.value["keys"]
                .as_array()
                .is_some_and(|keys| keys.iter().any(|key| key["kid"] == signature.kid))
            {
                return Err(EngineError::response());
            }
        }
        Ok(signature)
    }

    pub async fn issue(
        &self,
        claims: &CapabilityClaims,
        kind: CapabilityKind,
        namespace: &str,
    ) -> Result<IssuedToken> {
        if !valid_capability(claims, kind) {
            return Err(EngineError::new(400, "ENGINE_INVALID_CAPABILITY"));
        }
        let app = json!({"durable_streams": claims});
        if serde_json::to_vec(&app)
            .map_err(|_| EngineError::response())?
            .len()
            > 3072
        {
            return Err(EngineError::new(413, "ENGINE_CLAIMS_TOO_LARGE"));
        }
        let body = serde_json::to_vec(&json!({"audience":"durable-streams", "purpose":kind.purpose(), "duration":MAX_LIFETIME, "claims":app})).map_err(|_| EngineError::response())?;
        let request = self.authenticate(
            self.inner
                .http
                .post(self.url("/auth/application-tokens"))
                .timeout(Duration::from_secs(10))
                .header("content-type", "application/json")
                .body(body),
        )?;
        let issued: IssuedToken = self.json(request).await?;
        // Verify Engine output before persisting it; this also primes the public-key
        // cache for the usual callback path, without trusting returned token claims.
        let verified = self
            .verify(
                &issued.token,
                namespace,
                kind,
                kind.operations()[0],
                super::now_ms(),
            )
            .await
            .map_err(|error| {
                // A newly issued token can race emergency key publication or
                // expire in transit. Retry the Engine response; it is not a
                // rejected caller credential that should pause delivery.
                if matches!(error.code, "TOKEN_INVALID" | "TOKEN_EXPIRED") {
                    EngineError::response()
                } else {
                    error
                }
            })?;
        if verified.claims != *claims
            || verified.expires_ts != issued.expires_ts
            || issued.issued_ts < 0
            || issued.expires_ts <= issued.issued_ts
            || issued.expires_ts - issued.issued_ts > MAX_LIFETIME as i64 * 1000
        {
            return Err(EngineError::response());
        }
        Ok(issued)
    }

    /// Network/cache work happens before the actor takes its state lock. The
    /// caller must subsequently validate every returned scope against live state.
    pub async fn verify(
        &self,
        token: &str,
        namespace: &str,
        kind: CapabilityKind,
        operation: &str,
        now_ms: i64,
    ) -> Result<VerifiedCapability> {
        if token.len() > MAX_TOKEN || !kind.operations().contains(&operation) {
            return Err(EngineError::invalid());
        }
        let header = strict_header(token)?;
        let key = self.jwt_key(&header.kid).await?;
        verify_with_key(
            token,
            &key,
            &self.inner.issuer,
            namespace,
            kind,
            operation,
            now_ms,
        )
    }

    async fn jwt_key(&self, kid: &str) -> Result<DecodingKey> {
        // Known fresh keys never wait for network work triggered by an attacker
        // sending unknown kids. A separate gate coalesces discovery requests.
        {
            let cache = self.inner.jwt_keys.read().await;
            if cache
                .expires
                .is_some_and(|expires| Instant::now() < expires)
                && let Some(key) = cache.keys.get(kid)
            {
                return Ok(key.clone());
            }
        }
        let _refresh = self.inner.jwt_refresh.lock().await;
        let mut cache = self.inner.jwt_keys.write().await;
        let now = Instant::now();
        if cache.expires.is_some_and(|expires| now < expires)
            && let Some(key) = cache.keys.get(kid)
        {
            return Ok(key.clone());
        }
        if cache
            .last_attempt
            .is_some_and(|last| now.duration_since(last) < KEY_REFRESH_INTERVAL)
            && (!cache.keys.contains_key(kid) || cache.last_error.is_some())
        {
            return Err(cache.last_error.clone().unwrap_or_else(|| {
                if cache.keys.contains_key(kid) {
                    EngineError::unavailable()
                } else {
                    EngineError::invalid()
                }
            }));
        }
        cache.last_attempt = Some(now);
        drop(cache);
        let fetched = self.fetch_jwt_keys().await;
        let mut cache = self.inner.jwt_keys.write().await;
        match fetched {
            Ok((keys, ttl)) => {
                cache.keys = keys;
                cache.expires = Some(Instant::now() + ttl);
                cache.last_error = None;
                cache
                    .keys
                    .get(kid)
                    .cloned()
                    .ok_or_else(EngineError::invalid)
            }
            Err(error) => {
                cache.last_error = Some(error.clone());
                Err(error)
            }
        }
    }

    async fn fetch_jwt_keys(&self) -> Result<(HashMap<String, DecodingKey>, Duration)> {
        let reply = response(
            self.inner
                .http
                .get(self.url("/.well-known/jwks.json"))
                .header(CACHE_CONTROL, "no-cache")
                .timeout(Duration::from_secs(5)),
        )
        .await?;
        let jwks: JwkSet = decode(&reply.body)?;
        validate_jwks(&jwks, false)?;
        let mut keys = HashMap::with_capacity(jwks.keys.len());
        for key in jwks.keys {
            let decoding =
                DecodingKey::from_ed_components(&key.x).map_err(|_| EngineError::response())?;
            keys.insert(key.kid, decoding);
        }
        let age = reply.age();
        if reply
            .max_age
            .is_some_and(|max_age| max_age > 0 && age >= Duration::from_secs(max_age))
        {
            return Err(EngineError::unavailable());
        }
        let ttl = Duration::from_secs(reply.max_age.unwrap_or(0).min(60)).saturating_sub(age);
        Ok((keys, ttl))
    }

    async fn json<T: DeserializeOwned>(&self, request: RequestBuilder) -> Result<T> {
        decode(&response(request).await?.body)
    }
}

fn trusted_url(raw: &str) -> Result<url::Url> {
    let url = url::Url::parse(raw).map_err(|_| EngineError::new(503, "ENGINE_CONFIGURATION"))?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(EngineError::new(503, "ENGINE_CONFIGURATION"));
    }
    Ok(url)
}
fn identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
}

fn matches_actor_destination(
    actor: &Value,
    name: &str,
    pool: &str,
    namespace: &str,
    id: Option<&str>,
) -> bool {
    actor["name"] == name
        && actor["namespace_id"] == namespace
        && actor["destroy_ts"].is_null()
        && actor["runner_name_selector"] == pool
        && id.is_none_or(|id| actor["actor_id"] == id)
}

#[cfg(test)]
#[path = "../../tests/unit/actor_destination.rs"]
mod actor_destination_tests;
fn namespace_path(namespace: &str) -> Result<String> {
    if !identifier(namespace) {
        return Err(EngineError::new(503, "ENGINE_NAMESPACE_INVALID"));
    }
    Ok(format!("/namespaces/{namespace}/auth/webhooks"))
}
fn webhook_kid(kid: &str) -> bool {
    kid.strip_prefix("ds_")
        .is_some_and(|v| URL_SAFE_NO_PAD.decode(v).is_ok_and(|b| b.len() == 32))
}
fn valid_capability(claims: &CapabilityClaims, kind: CapabilityKind) -> bool {
    !claims.root.is_empty()
        && !claims.subscription_id.is_empty()
        && !claims.wake_id.is_empty()
        && claims.generation > 0
        && claims.operations.len() == kind.operations().len()
        && kind
            .operations()
            .iter()
            .all(|op| claims.operations.iter().filter(|v| *v == op).count() == 1)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Header {
    alg: String,
    typ: String,
    kid: String,
}
fn strict_header(token: &str) -> Result<Header> {
    let encoded = token.split('.').next().ok_or_else(EngineError::invalid)?;
    let bytes = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| EngineError::invalid())?;
    let header: Header = serde_json::from_slice(&bytes).map_err(|_| EngineError::invalid())?;
    if header.alg != "EdDSA" || header.typ != "rivet-app+jwt" || !identifier(&header.kid) {
        return Err(EngineError::invalid());
    }
    Ok(header)
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ApplicationClaims {
    rivet_ver: u16,
    rivet_purpose: String,
    iss: String,
    aud: String,
    #[serde(rename = "sub")]
    _subject: Option<String>,
    iat: u64,
    exp: u64,
    jti: String,
    rivet_ns: String,
    app: ApplicationData,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ApplicationData {
    durable_streams: CapabilityClaims,
}
fn verify_with_key(
    token: &str,
    key: &DecodingKey,
    issuer: &str,
    namespace: &str,
    kind: CapabilityKind,
    operation: &str,
    now_ms: i64,
) -> Result<VerifiedCapability> {
    strict_header(token)?;
    let mut validation = Validation::new(Algorithm::EdDSA);
    validation.set_issuer(&[issuer]);
    validation.set_audience(&["durable-streams"]);
    // Validate time below against the actor's supplied clock, without the JWT
    // library's implicit expiry leeway. Signature verification stays mandatory.
    validation.validate_exp = false;
    let claims = jsonwebtoken::decode::<ApplicationClaims>(token, key, &validation)
        .map_err(|_| EngineError::invalid())?
        .claims;
    let now = u64::try_from(now_ms / 1000).map_err(|_| EngineError::invalid())?;
    if claims.rivet_ver != 1
        || claims.rivet_purpose != kind.purpose()
        || claims.iss != issuer
        || claims.aud != "durable-streams"
        || claims.rivet_ns != namespace
        || claims.jti.is_empty()
        || claims.exp <= claims.iat
        || claims.exp - claims.iat > MAX_LIFETIME
        || claims.iat > now.saturating_add(30)
        || !valid_capability(&claims.app.durable_streams, kind)
        || !claims
            .app
            .durable_streams
            .operations
            .iter()
            .any(|op| op == operation)
    {
        return Err(EngineError::invalid());
    }
    if claims.exp <= now {
        return Err(EngineError::new(401, "TOKEN_EXPIRED"));
    }
    let expires_ts = i64::try_from(claims.exp)
        .ok()
        .and_then(|exp| exp.checked_mul(1000))
        .ok_or_else(EngineError::invalid)?;
    Ok(VerifiedCapability {
        claims: claims.app.durable_streams,
        expires_ts,
    })
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct JwkSet {
    keys: Vec<Jwk>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Jwk {
    kty: String,
    crv: String,
    kid: String,
    #[serde(rename = "use")]
    use_: String,
    alg: String,
    x: String,
}
fn validate_jwks(jwks: &JwkSet, webhook: bool) -> Result<()> {
    let mut seen = std::collections::HashSet::new();
    if jwks.keys.len() > 256 {
        return Err(EngineError::response());
    }
    for key in &jwks.keys {
        if key.kty != "OKP"
            || key.crv != "Ed25519"
            || key.use_ != "sig"
            || key.alg != "EdDSA"
            || !identifier(&key.kid)
            || !seen.insert(&key.kid)
            || URL_SAFE_NO_PAD
                .decode(&key.x)
                .map_or(true, |v| v.len() != 32)
        {
            return Err(EngineError::response());
        }
        if webhook {
            let canonical = format!(r#"{{"crv":"Ed25519","kty":"OKP","x":"{}"}}"#, key.x);
            let kid = format!(
                "ds_{}",
                URL_SAFE_NO_PAD.encode(Sha256::digest(canonical.as_bytes()))
            );
            if key.kid != kid {
                return Err(EngineError::response());
            }
        }
    }
    Ok(())
}
struct HttpResponse {
    body: Vec<u8>,
    age: Duration,
    received_at: Instant,
    max_age: Option<u64>,
}
impl HttpResponse {
    fn age(&self) -> Duration {
        self.age.saturating_add(self.received_at.elapsed())
    }
    fn age_secs(&self) -> u64 {
        let age = self.age();
        age.as_secs()
            .saturating_add(u64::from(age.subsec_nanos() > 0))
    }
}
async fn response(request: RequestBuilder) -> Result<HttpResponse> {
    let start = Instant::now();
    let response = request
        .send()
        .await
        .map_err(|_| EngineError::unavailable())?;
    let status = response.status().as_u16();
    if !response.status().is_success() {
        let code = match status {
            400 => "ENGINE_INVALID_REQUEST",
            401 => "ENGINE_UNAUTHORIZED",
            403 => "ENGINE_FORBIDDEN",
            404 => "ENGINE_NOT_FOUND",
            409 => "ENGINE_CONFLICT",
            413 => "ENGINE_PAYLOAD_TOO_LARGE",
            429 => "ENGINE_RATE_LIMITED",
            _ => "ENGINE_UNAVAILABLE",
        };
        let retry_after = response
            .headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok());
        return Err(EngineError {
            status: if matches!(status, 400 | 401 | 403 | 404 | 409 | 413 | 429) {
                status
            } else {
                503
            },
            code,
            retry_after,
        });
    }
    if response
        .content_length()
        .is_some_and(|v| v > MAX_RESPONSE as u64)
    {
        return Err(EngineError::response());
    }
    let age = response
        .headers()
        .get(AGE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);
    let cache_control = response
        .headers()
        .get(CACHE_CONTROL)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let no_cache = cache_control
        .split(',')
        .any(|v| matches!(v.trim(), "no-store" | "no-cache"));
    let max_age = (!no_cache)
        .then(|| {
            cache_control.split(',').find_map(|v| {
                v.trim()
                    .strip_prefix("max-age=")
                    .and_then(|v| v.parse().ok())
            })
        })
        .flatten();
    let mut body = Vec::new();
    let mut chunks = response.bytes_stream();
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk.map_err(|_| EngineError::unavailable())?;
        if body.len() + chunk.len() > MAX_RESPONSE {
            return Err(EngineError::response());
        }
        body.extend_from_slice(&chunk);
    }
    // Preserve precise age for short JWT cache lifetimes, rounding up only
    // when exposing the whole-second Age header to downstream webhook clients.
    let received_at = Instant::now();
    let age = Duration::from_secs(age).saturating_add(received_at.duration_since(start));
    Ok(HttpResponse {
        body,
        age,
        received_at,
        max_age,
    })
}
fn decode<T: DeserializeOwned>(body: &[u8]) -> Result<T> {
    serde_json::from_slice(body).map_err(|_| EngineError::response())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Router,
        extract::{Request, State},
        response::{IntoResponse, Response},
        routing::any,
    };
    use ring::signature::{ED25519, Ed25519KeyPair, KeyPair, UnparsedPublicKey};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    fn key() -> Ed25519KeyPair {
        Ed25519KeyPair::from_seed_unchecked(&[7; 32]).unwrap()
    }
    fn capability(kind: CapabilityKind) -> CapabilityClaims {
        CapabilityClaims {
            root: "https://streams.gameinc.io/v1/stream".into(),
            tenant_scope: "tenant-a".into(),
            subscription_id: "sub-a".into(),
            wake_id: "wake-a".into(),
            generation: 7,
            operations: kind.operations().iter().map(|v| v.to_string()).collect(),
        }
    }
    fn claims(issuer: &str, kind: CapabilityKind) -> Value {
        let now = super::super::now_ms() / 1000;
        json!({"iss":issuer,"aud":"durable-streams","rivet_ver":1,"rivet_purpose":kind.purpose(),"rivet_ns":"namespace-a","iat":now,"exp":now+1800,"jti":"token-a","app":{"durable_streams":capability(kind)}})
    }
    fn token(claims: &Value, header: &Value) -> String {
        let message = format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(header).unwrap()),
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(claims).unwrap())
        );
        format!(
            "{message}.{}",
            URL_SAFE_NO_PAD.encode(key().sign(message.as_bytes()).as_ref())
        )
    }
    fn header() -> Value {
        json!({"typ":"rivet-app+jwt","alg":"EdDSA","kid":"token-key"})
    }
    fn public_key(webhook: bool) -> Value {
        public_key_from(&key(), webhook)
    }
    fn public_key_from(key: &Ed25519KeyPair, webhook: bool) -> Value {
        let x = URL_SAFE_NO_PAD.encode(key.public_key().as_ref());
        let canonical = format!(r#"{{"crv":"Ed25519","kty":"OKP","x":"{x}"}}"#);
        let kid = if webhook {
            format!(
                "ds_{}",
                URL_SAFE_NO_PAD.encode(Sha256::digest(canonical.as_bytes()))
            )
        } else {
            "token-key".into()
        };
        json!({"kty":"OKP","crv":"Ed25519","kid":kid,"alg":"EdDSA","use":"sig","x":x})
    }

    #[test]
    fn strict_application_profile_and_lifecycle() {
        let issuer = "https://engine.gameinc.io";
        let claims = claims(issuer, CapabilityKind::Callback);
        let key =
            DecodingKey::from_ed_components(public_key(false)["x"].as_str().unwrap()).unwrap();
        let check = |claims: &Value, header: &Value, namespace: &str, kind, operation| {
            verify_with_key(
                &token(claims, header),
                &key,
                issuer,
                namespace,
                kind,
                operation,
                super::super::now_ms(),
            )
        };
        assert!(
            check(
                &claims,
                &header(),
                "namespace-a",
                CapabilityKind::Callback,
                "callback"
            )
            .is_ok()
        );
        for (field, value) in [
            ("aud", json!("rivet-api")),
            ("iss", json!("https://untrusted.gameinc.io")),
            ("rivet_ns", json!("namespace-b")),
            ("rivet_ver", json!(2)),
            ("rivet_purpose", json!("durable-streams.claim")),
            ("jti", json!("")),
            ("exp", claims["iat"].clone()),
            ("iat", json!(claims["exp"].as_u64().unwrap() + 60)),
            ("exp", json!(claims["iat"].as_u64().unwrap() + 86401)),
        ] {
            let mut invalid = claims.clone();
            invalid[field] = value;
            assert!(
                check(
                    &invalid,
                    &header(),
                    "namespace-a",
                    CapabilityKind::Callback,
                    "callback"
                )
                .is_err(),
                "{field}"
            );
        }
        for invalid_header in [
            json!({"typ":"rivet-auth+jwt","alg":"EdDSA","kid":"token-key"}),
            json!({"typ":"rivet-app+jwt","alg":"HS256","kid":"token-key"}),
            json!({"typ":"rivet-app+jwt","alg":"EdDSA","kid":"token-key","jku":"https://untrusted.gameinc.io"}),
        ] {
            assert!(
                check(
                    &claims,
                    &invalid_header,
                    "namespace-a",
                    CapabilityKind::Callback,
                    "callback"
                )
                .is_err()
            );
        }
        assert!(
            check(
                &claims,
                &header(),
                "namespace-b",
                CapabilityKind::Callback,
                "callback"
            )
            .is_err()
        );
        assert!(
            check(
                &claims,
                &header(),
                "namespace-a",
                CapabilityKind::Claim,
                "ack"
            )
            .is_err()
        );
        assert!(
            check(
                &claims,
                &header(),
                "namespace-a",
                CapabilityKind::Callback,
                "release"
            )
            .is_err()
        );
        let mut grants = claims.clone();
        grants["rivet_grants"] = json!([]);
        assert!(
            check(
                &grants,
                &header(),
                "namespace-a",
                CapabilityKind::Callback,
                "callback"
            )
            .is_err()
        );
        let mut expired = claims.clone();
        expired["iat"] = json!(1);
        expired["exp"] = json!(2);
        assert_eq!(
            check(
                &expired,
                &header(),
                "namespace-a",
                CapabilityKind::Callback,
                "callback"
            )
            .err()
            .unwrap()
            .code,
            "TOKEN_EXPIRED"
        );
        for operations in [
            json!(["callback", "ack"]),
            json!(["callback", "callback"]),
            json!([]),
        ] {
            let mut invalid = claims.clone();
            invalid["app"]["durable_streams"]["operations"] = operations;
            assert!(
                check(
                    &invalid,
                    &header(),
                    "namespace-a",
                    CapabilityKind::Callback,
                    "callback"
                )
                .is_err()
            );
        }
        let claim = claims_for_claim(issuer);
        assert!(
            check(
                &claim,
                &header(),
                "namespace-a",
                CapabilityKind::Claim,
                "ack"
            )
            .is_ok()
        );
        assert!(
            check(
                &claim,
                &header(),
                "namespace-a",
                CapabilityKind::Claim,
                "release"
            )
            .is_ok()
        );
    }
    fn claims_for_claim(issuer: &str) -> Value {
        claims(issuer, CapabilityKind::Claim)
    }

    #[test]
    fn exact_public_jwk_shape_and_thumbprint_are_enforced() {
        let valid = json!({"keys":[public_key(true)]});
        validate_jwks(&serde_json::from_value(valid.clone()).unwrap(), true).unwrap();
        let mut bad = valid.clone();
        bad["keys"][0]["kid"] = json!("ds_wrong");
        assert!(validate_jwks(&serde_json::from_value(bad).unwrap(), true).is_err());
        let mut bad = valid.clone();
        bad["keys"][0]["d"] = json!("private");
        assert!(serde_json::from_value::<JwkSet>(bad).is_err());
        let duplicate = json!({"keys":[public_key(true),public_key(true)]});
        assert!(validate_jwks(&serde_json::from_value(duplicate).unwrap(), true).is_err());
        assert!(trusted_url("https://secret@engine.gameinc.io").is_err());
        assert!(trusted_url("https://engine.gameinc.io?token=secret").is_err());
        assert!(namespace_path("../../auth").is_err());
    }

    struct TestState {
        issuer: String,
        jwt_fetches: AtomicUsize,
        webhook_fetches: AtomicUsize,
        sign_calls: AtomicUsize,
        expiry_seconds: AtomicUsize,
        jwt_max_age: AtomicUsize,
        issue_status: AtomicUsize,
        rotated_jwt: AtomicBool,
        jwt_status: AtomicUsize,
        sign_status: AtomicUsize,
        webhook_age: AtomicUsize,
        wrong_signing_namespace: AtomicBool,
        rotated_webhook: AtomicBool,
        provisioned: AtomicBool,
        ensures: AtomicUsize,
        response_delay_ms: AtomicUsize,
        last_body: Mutex<Vec<u8>>,
    }
    struct Server {
        api: CatalogApi,
        state: Arc<TestState>,
        task: tokio::task::JoinHandle<()>,
    }
    impl Drop for Server {
        fn drop(&mut self) {
            self.task.abort();
        }
    }
    impl Server {
        async fn start() -> Self {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let state = Arc::new(TestState {
                issuer: endpoint.clone(),
                jwt_fetches: AtomicUsize::new(0),
                webhook_fetches: AtomicUsize::new(0),
                sign_calls: AtomicUsize::new(0),
                expiry_seconds: AtomicUsize::new(29),
                jwt_max_age: AtomicUsize::new(60),
                issue_status: AtomicUsize::new(200),
                rotated_jwt: AtomicBool::new(false),
                jwt_status: AtomicUsize::new(200),
                sign_status: AtomicUsize::new(200),
                webhook_age: AtomicUsize::new(0),
                wrong_signing_namespace: AtomicBool::new(false),
                rotated_webhook: AtomicBool::new(false),
                provisioned: AtomicBool::new(true),
                ensures: AtomicUsize::new(0),
                response_delay_ms: AtomicUsize::new(0),
                last_body: Mutex::new(Vec::new()),
            });
            let app = Router::new().fallback(any(serve)).with_state(state.clone());
            let task = tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            });
            Self {
                api: CatalogApi {
                    endpoint,
                    namespace: "test".into(),
                    token: Some("acl-secret".into()),
                },
                state,
                task,
            }
        }
        fn client(&self) -> EngineClient {
            EngineClient::new(&self.api, None).unwrap()
        }
    }
    async fn serve(State(state): State<Arc<TestState>>, request: Request) -> Response {
        let path = request.uri().path().to_owned();
        tokio::time::sleep(Duration::from_millis(
            state.response_delay_ms.load(Ordering::SeqCst) as u64,
        ))
        .await;
        if path.starts_with("/auth/") || path == "/namespaces" {
            assert_eq!(
                request.headers().get("authorization").unwrap(),
                "Bearer acl-secret"
            );
        }
        let body = axum::body::to_bytes(request.into_body(), MAX_BODY_LIMIT)
            .await
            .unwrap();
        let webhook_key = Ed25519KeyPair::from_seed_unchecked(
            &[if state.rotated_webhook.load(Ordering::SeqCst) {
                8
            } else {
                7
            }; 32],
        )
        .unwrap();
        let webhook_public = public_key_from(&webhook_key, true);
        let metadata = || json!({"namespace_id":"namespace-a","alg":"ed25519","kid":webhook_public["kid"],"jwks_url":format!("{}/namespaces/namespace-a/auth/webhooks/jwks.json",state.issuer),"generation":1});
        let reply = match path.as_str() {
            "/namespaces" => json!({"namespaces":[{"namespace_id":"namespace-a","name":"test"}]}),
            "/auth/webhooks/ensure" => {
                state.ensures.fetch_add(1, Ordering::SeqCst);
                state.provisioned.store(true, Ordering::SeqCst);
                let mut ensured = metadata();
                if state.wrong_signing_namespace.load(Ordering::SeqCst) {
                    ensured["namespace_id"] = json!("namespace-b");
                }
                ensured
            }
            "/namespaces/namespace-a/auth/webhooks" => {
                if !state.provisioned.load(Ordering::SeqCst) {
                    return http::StatusCode::NOT_FOUND.into_response();
                }
                metadata()
            }
            "/.well-known/jwks.json" => {
                state.jwt_fetches.fetch_add(1, Ordering::SeqCst);
                let status = state.jwt_status.load(Ordering::SeqCst);
                if status != 200 {
                    return (
                        http::StatusCode::from_u16(status as u16).unwrap(),
                        "SECRET UPSTREAM ERROR",
                    )
                        .into_response();
                }
                let mut jwk = public_key(false);
                if state.rotated_jwt.load(Ordering::SeqCst) {
                    jwk["kid"] = json!("rotated-key");
                }
                return (
                    [(
                        "cache-control",
                        format!(
                            "public, max-age={}",
                            state.jwt_max_age.load(Ordering::SeqCst)
                        ),
                    )],
                    axum::Json(json!({"keys":[jwk]})),
                )
                    .into_response();
            }
            "/namespaces/namespace-a/auth/webhooks/jwks.json" => {
                state.webhook_fetches.fetch_add(1, Ordering::SeqCst);
                if !state.provisioned.load(Ordering::SeqCst) {
                    return http::StatusCode::BAD_REQUEST.into_response();
                }
                return (
                    [
                        ("cache-control", "no-store".into()),
                        ("age", state.webhook_age.load(Ordering::SeqCst).to_string()),
                    ],
                    axum::Json(json!({"keys":[webhook_public]})),
                )
                    .into_response();
            }
            "/auth/application-tokens" => {
                let status = state.issue_status.load(Ordering::SeqCst);
                if status != 200 {
                    return http::StatusCode::from_u16(status as u16)
                        .unwrap()
                        .into_response();
                }
                let request: Value = serde_json::from_slice(&body).unwrap();
                assert!(request.get("namespace").is_none());
                assert_eq!(request["duration"], 86400);
                let mut claims = claims(&state.issuer, CapabilityKind::Callback);
                claims["aud"] = request["audience"].clone();
                claims["rivet_purpose"] = request["purpose"].clone();
                claims["app"] = request["claims"].clone();
                claims["exp"] = json!(
                    claims["iat"].as_u64().unwrap()
                        + state.expiry_seconds.load(Ordering::SeqCst) as u64
                );
                let mut jwt_header = header();
                if state.rotated_jwt.load(Ordering::SeqCst) {
                    jwt_header["kid"] = json!("rotated-key");
                }
                json!({"token":token(&claims,&jwt_header),"issued_ts":claims["iat"].as_u64().unwrap()*1000,"expires_ts":claims["exp"].as_u64().unwrap()*1000})
            }
            "/auth/webhooks/sign" => {
                state.sign_calls.fetch_add(1, Ordering::SeqCst);
                *state.last_body.lock().await = body.to_vec();
                let status = state.sign_status.load(Ordering::SeqCst);
                if status != 200 {
                    return (
                        http::StatusCode::from_u16(status as u16).unwrap(),
                        [("retry-after", "17")],
                        "SECRET UPSTREAM ERROR",
                    )
                        .into_response();
                }
                let timestamp = (super::super::now_ms() / 1000).to_string();
                let mut message = format!("{timestamp}.").into_bytes();
                message.extend_from_slice(&body);
                let signer = if state.wrong_signing_namespace.load(Ordering::SeqCst) {
                    Ed25519KeyPair::from_seed_unchecked(&[9; 32]).unwrap()
                } else {
                    webhook_key
                };
                json!({"timestamp":timestamp,"kid":public_key_from(&signer,true)["kid"],"signature":URL_SAFE_NO_PAD.encode(signer.sign(&message).as_ref())})
            }
            _ => return http::StatusCode::NOT_FOUND.into_response(),
        };
        axum::Json(reply).into_response()
    }

    #[tokio::test]
    async fn exact_bytes_and_fresh_discovery() {
        let server = Server::start().await;
        let client = server.client();
        assert_eq!(client.namespace_id().await.unwrap(), "namespace-a");
        let metadata = client.ensure().await.unwrap();
        assert_eq!(metadata.kid, public_key(true)["kid"]);
        assert_eq!(
            client.metadata("namespace-a").await.unwrap().kid,
            metadata.kid
        );
        for body in [
            Bytes::new(),
            Bytes::from_static(b"\x00\xff\r\n { \"a\": 1 }\n"),
        ] {
            let signed = client.sign(body.clone()).await.unwrap();
            assert_eq!(*server.state.last_body.lock().await, body);
            let mut message = format!("{}.", signed.timestamp).into_bytes();
            message.extend_from_slice(&body);
            UnparsedPublicKey::new(&ED25519, key().public_key())
                .verify(
                    &message,
                    &URL_SAFE_NO_PAD.decode(&signed.signature).unwrap(),
                )
                .unwrap();
            assert_eq!(
                signed.header(),
                format!(
                    "t={},kid={},ed25519={}",
                    signed.timestamp, metadata.kid, signed.signature
                )
            );
        }
        for _ in 0..2 {
            let jwks = client.webhook_jwks("namespace-a").await.unwrap();
            assert_eq!(jwks.value, json!({"keys":[public_key(true)]}));
            assert!(jwks.age_secs >= 1);
        }
        assert_eq!(server.state.webhook_fetches.load(Ordering::SeqCst), 2);
        server.state.webhook_age.store(300, Ordering::SeqCst);
        assert_eq!(
            client
                .webhook_jwks("namespace-a")
                .await
                .err()
                .unwrap()
                .status,
            503
        );
    }

    #[tokio::test]
    async fn short_expiry_and_bounded_unknown_key_refresh() {
        let server = Server::start().await;
        let client = server.client();
        let issued = client
            .issue(
                &capability(CapabilityKind::Claim),
                CapabilityKind::Claim,
                "namespace-a",
            )
            .await
            .unwrap();
        assert_eq!(issued.expires_ts - issued.issued_ts, 29_000);
        let verified = client
            .verify(
                &issued.token,
                "namespace-a",
                CapabilityKind::Claim,
                "release",
                super::super::now_ms(),
            )
            .await
            .unwrap();
        assert_eq!(verified.expires_ts, issued.expires_ts);
        assert_eq!(server.state.jwt_fetches.load(Ordering::SeqCst), 1);
        // A malicious unknown-kid lookup must not hold up a cached, valid token.
        {
            let _refresh = client.inner.jwt_refresh.lock().await;
            tokio::time::timeout(
                Duration::from_millis(100),
                client.verify(
                    &issued.token,
                    "namespace-a",
                    CapabilityKind::Claim,
                    "ack",
                    super::super::now_ms(),
                ),
            )
            .await
            .unwrap()
            .unwrap();
        }
        let mut unknown_header = header();
        unknown_header["kid"] = json!("unknown-key");
        let unknown = token(
            &claims(&server.api.endpoint, CapabilityKind::Callback),
            &unknown_header,
        );
        client.inner.jwt_keys.write().await.last_attempt =
            Some(Instant::now() - Duration::from_secs(6));
        for _ in 0..10 {
            assert_eq!(
                client
                    .verify(
                        &unknown,
                        "namespace-a",
                        CapabilityKind::Callback,
                        "callback",
                        super::super::now_ms()
                    )
                    .await
                    .err()
                    .unwrap()
                    .code,
                "TOKEN_INVALID"
            );
        }
        assert_eq!(server.state.jwt_fetches.load(Ordering::SeqCst), 2);
        {
            let mut cache = client.inner.jwt_keys.write().await;
            cache.expires = Some(Instant::now() - Duration::from_secs(1));
            cache.last_attempt = Some(Instant::now() - Duration::from_secs(6));
        }
        server.state.jwt_status.store(503, Ordering::SeqCst);
        let error = client
            .verify(
                &issued.token,
                "namespace-a",
                CapabilityKind::Claim,
                "ack",
                super::super::now_ms(),
            )
            .await
            .err()
            .unwrap();
        assert_eq!(error.status, 503);
        assert!(!format!("{error:?}").contains("SECRET"));
    }

    #[tokio::test]
    async fn one_second_key_cache_uses_precise_age_and_refreshes_after_expiry() {
        let server = Server::start().await;
        server.state.jwt_max_age.store(1, Ordering::SeqCst);
        let client = server.client();
        let issued = client
            .issue(
                &capability(CapabilityKind::Claim),
                CapabilityKind::Claim,
                "namespace-a",
            )
            .await
            .unwrap();
        client
            .verify(
                &issued.token,
                "namespace-a",
                CapabilityKind::Claim,
                "ack",
                super::super::now_ms(),
            )
            .await
            .unwrap();
        assert_eq!(server.state.jwt_fetches.load(Ordering::SeqCst), 1);
        let cache = client.inner.jwt_keys.read().await;
        let remaining = cache
            .expires
            .unwrap()
            .saturating_duration_since(Instant::now());
        assert!(!remaining.is_zero() && remaining < Duration::from_secs(1));
        drop(cache);

        client.inner.jwt_keys.write().await.expires = Some(Instant::now());
        client
            .verify(
                &issued.token,
                "namespace-a",
                CapabilityKind::Claim,
                "ack",
                super::super::now_ms(),
            )
            .await
            .unwrap();
        assert_eq!(server.state.jwt_fetches.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn known_keys_refresh_without_reusing_zero_lifetime_snapshots() {
        let server = Server::start().await;
        server.state.jwt_max_age.store(0, Ordering::SeqCst);
        let client = server.client();
        let issued = client
            .issue(
                &capability(CapabilityKind::Claim),
                CapabilityKind::Claim,
                "namespace-a",
            )
            .await
            .unwrap();
        for _ in 0..2 {
            client
                .verify(
                    &issued.token,
                    "namespace-a",
                    CapabilityKind::Claim,
                    "ack",
                    super::super::now_ms(),
                )
                .await
                .unwrap();
        }
        assert_eq!(server.state.jwt_fetches.load(Ordering::SeqCst), 3);

        let mut unknown_header = header();
        unknown_header["kid"] = json!("unknown-key");
        let unknown = token(
            &claims(&server.api.endpoint, CapabilityKind::Claim),
            &unknown_header,
        );
        for _ in 0..2 {
            assert_eq!(
                client
                    .verify(
                        &unknown,
                        "namespace-a",
                        CapabilityKind::Claim,
                        "ack",
                        super::super::now_ms()
                    )
                    .await
                    .err()
                    .unwrap()
                    .status,
                401
            );
        }
        assert_eq!(server.state.jwt_fetches.load(Ordering::SeqCst), 3);

        server.state.jwt_status.store(503, Ordering::SeqCst);
        for _ in 0..2 {
            assert_eq!(
                client
                    .verify(
                        &issued.token,
                        "namespace-a",
                        CapabilityKind::Claim,
                        "ack",
                        super::super::now_ms()
                    )
                    .await
                    .err()
                    .unwrap()
                    .status,
                503
            );
        }
        assert_eq!(server.state.jwt_fetches.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn newly_issued_rotated_key_retries_without_hiding_credential_failures() {
        let server = Server::start().await;
        let client = server.client();
        let claims = capability(CapabilityKind::Callback);
        client
            .issue(&claims, CapabilityKind::Callback, "namespace-a")
            .await
            .unwrap();
        server.state.rotated_jwt.store(true, Ordering::SeqCst);

        let error = client
            .issue(&claims, CapabilityKind::Callback, "namespace-a")
            .await
            .err()
            .unwrap();
        assert_eq!(error.status, 503);
        assert_eq!(error.code, "ENGINE_INVALID_RESPONSE");
        assert_eq!(server.state.jwt_fetches.load(Ordering::SeqCst), 1);

        client.inner.jwt_keys.write().await.last_attempt =
            Some(Instant::now() - Duration::from_secs(6));
        let issued = client
            .issue(&claims, CapabilityKind::Callback, "namespace-a")
            .await
            .unwrap();
        assert_eq!(strict_header(&issued.token).unwrap().kid, "rotated-key");
        assert_eq!(server.state.jwt_fetches.load(Ordering::SeqCst), 2);

        server.state.issue_status.store(401, Ordering::SeqCst);
        let error = client
            .issue(&claims, CapabilityKind::Callback, "namespace-a")
            .await
            .err()
            .unwrap();
        assert_eq!(error.status, 401);
        assert_eq!(error.code, "ENGINE_UNAUTHORIZED");
    }

    #[tokio::test]
    async fn signing_errors_preserve_status_and_retry_without_leaking_body() {
        let server = Server::start().await;
        let client = server.client();
        for status in [400, 401, 403, 404, 409, 413, 429, 503] {
            server.state.sign_status.store(status, Ordering::SeqCst);
            let error = client
                .sign(Bytes::from_static(b"secret payload"))
                .await
                .err()
                .unwrap();
            assert_eq!(usize::from(error.status), status);
            assert_eq!(error.retry_after, Some(17));
            assert!(!format!("{error:?}").contains("SECRET"));
            assert!(!format!("{error:?}").contains("secret payload"));
        }
        let before = server.state.sign_calls.load(Ordering::SeqCst);
        let error = client
            .sign_with_limit(Bytes::from(vec![0; 1024 * 1024 + 1]), 1024 * 1024)
            .await
            .err()
            .unwrap();
        assert_eq!(error.status, 413);
        assert_eq!(server.state.sign_calls.load(Ordering::SeqCst), before);
        let mut oversized = capability(CapabilityKind::Callback);
        oversized.root = "x".repeat(3072);
        assert_eq!(
            client
                .issue(&oversized, CapabilityKind::Callback, "namespace-a")
                .await
                .err()
                .unwrap()
                .status,
            413
        );
    }

    #[tokio::test]
    async fn existing_metadata_and_cached_capabilities_cannot_skip_namespace_binding() {
        let server = Server::start().await;
        server
            .state
            .wrong_signing_namespace
            .store(true, Ordering::SeqCst);
        let client = server.client();
        assert!(client.metadata_read("namespace-a").await.is_ok());
        assert_eq!(
            client.metadata("namespace-a").await.err().unwrap().status,
            503
        );
        assert_eq!(
            client
                .sign(Bytes::from_static(b"persisted callback capability"))
                .await
                .err()
                .unwrap()
                .status,
            503
        );
        assert_eq!(server.state.sign_calls.load(Ordering::SeqCst), 0);

        // Pull-only application operations do not need webhook permission.
        let issued = client
            .issue(
                &capability(CapabilityKind::Claim),
                CapabilityKind::Claim,
                "namespace-a",
            )
            .await
            .unwrap();
        client
            .verify(
                &issued.token,
                "namespace-a",
                CapabilityKind::Claim,
                "ack",
                super::super::now_ms(),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn mutable_signing_scope_cannot_return_another_namespaces_artifact() {
        let server = Server::start().await;
        let client = server.client();
        client.metadata("namespace-a").await.unwrap();
        client.sign(Bytes::from_static(b"first")).await.unwrap();
        client.sign(Bytes::from_static(b"second")).await.unwrap();
        assert_eq!(server.state.ensures.load(Ordering::SeqCst), 1);
        assert_eq!(server.state.webhook_fetches.load(Ordering::SeqCst), 0);

        server
            .state
            .wrong_signing_namespace
            .store(true, Ordering::SeqCst);
        assert_eq!(
            client
                .sign(Bytes::from_static(b"must not deliver"))
                .await
                .err()
                .unwrap()
                .status,
            503
        );
        assert_eq!(server.state.webhook_fetches.load(Ordering::SeqCst), 1);
        assert_eq!(server.state.ensures.load(Ordering::SeqCst), 1);

        // A fresh namespace snapshot replaces prior bindings instead of growing
        // a historical key cache, and stable current keys remain inexpensive.
        assert_eq!(client.inner.webhook_kids.read().await.len(), 1);
        server
            .state
            .wrong_signing_namespace
            .store(false, Ordering::SeqCst);
        client.sign(Bytes::from_static(b"corrected")).await.unwrap();
        assert_eq!(server.state.webhook_fetches.load(Ordering::SeqCst), 1);

        server.state.rotated_webhook.store(true, Ordering::SeqCst);
        let rotated = client.sign(Bytes::from_static(b"rotated")).await.unwrap();
        assert_ne!(rotated.kid, public_key(true)["kid"]);
        client
            .sign(Bytes::from_static(b"same rotated key"))
            .await
            .unwrap();
        assert_eq!(server.state.webhook_fetches.load(Ordering::SeqCst), 2);
        assert_eq!(server.state.ensures.load(Ordering::SeqCst), 1);
        let known = client.inner.webhook_kids.read().await;
        assert_eq!(known.len(), 1);
        assert!(known.contains(&rotated.kid));
    }

    #[tokio::test]
    async fn concurrent_metadata_only_ensures_once_and_timeouts_are_safe() {
        let server = Server::start().await;
        let client = server.client();
        server.state.provisioned.store(false, Ordering::SeqCst);
        let calls = (0..12).map(|_| client.metadata("namespace-a"));
        for result in futures_util::future::join_all(calls).await {
            result.unwrap();
        }
        assert_eq!(server.state.ensures.load(Ordering::SeqCst), 1);
        let server = Server::start().await;
        let client = server.client();
        server.state.provisioned.store(false, Ordering::SeqCst);
        assert_eq!(
            client.webhook_jwks("namespace-a").await.unwrap().value,
            json!({"keys":[public_key(true)]})
        );
        assert_eq!(server.state.ensures.load(Ordering::SeqCst), 1);
        assert_eq!(server.state.webhook_fetches.load(Ordering::SeqCst), 1);
        server.state.response_delay_ms.store(100, Ordering::SeqCst);
        let error = response(
            client
                .inner
                .http
                .get(client.url("/.well-known/jwks.json"))
                .timeout(Duration::from_millis(5)),
        )
        .await
        .err()
        .unwrap();
        assert_eq!(error.status, 503);
        assert_eq!(error.code, "ENGINE_UNAVAILABLE");
    }

    #[tokio::test]
    async fn large_4096_stream_body_remains_byte_exact() {
        let server = Server::start().await;
        let client = server.client();
        // A canonicalized 2048-byte Unicode path expands to over 6000 bytes.
        let path = "%E2%82%AC".repeat(682);
        let streams = (0..4096).map(|i|json!({"path":format!("{path}/{i}"),"acked_offset":"00000000_0000000000000000","tail_offset":"00000000_0000000000000001","has_pending":true})).collect::<Vec<_>>();
        let body = Bytes::from(
            serde_json::to_vec(&json!({"streams":streams,"callback":{"token":"x".repeat(6144)}}))
                .unwrap(),
        );
        assert!(body.len() > 25_000_000 && body.len() < 32 * 1024 * 1024);
        let signature = client.sign(body.clone()).await.unwrap();
        assert_eq!(*server.state.last_body.lock().await, body);
        let mut signed = format!("{}.", signature.timestamp).into_bytes();
        signed.extend_from_slice(&body);
        UnparsedPublicKey::new(&ED25519, key().public_key())
            .verify(
                &signed,
                &URL_SAFE_NO_PAD.decode(signature.signature).unwrap(),
            )
            .unwrap();
    }
}
