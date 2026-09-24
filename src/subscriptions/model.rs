use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::protocol::ZERO_OFFSET;

pub type ApiResult<T> = Result<T, ApiError>;

#[derive(Debug)]
pub struct ApiError {
    pub status: u16,
    pub body: Value,
    pub retry_after: Option<u64>,
}

impl ApiError {
    pub fn new(status: u16, code: &str, message: &str) -> Self {
        Self {
            status,
            retry_after: None,
            body: json!({"error": {"code": code, "message": message}}),
        }
    }
    pub fn invalid(message: &str) -> Self {
        Self::new(400, "INVALID_REQUEST", message)
    }
    pub fn fenced() -> Self {
        Self::new(409, "FENCED", "Wake is no longer current")
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} (HTTP {})",
            self.body["error"]["code"].as_str().unwrap_or("ERROR"),
            self.status
        )
    }
}
impl std::error::Error for ApiError {}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum DeliveryType {
    Webhook,
    PullWake,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Webhook {
    pub url: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(rename = "type")]
    pub kind: DeliveryType,
    #[serde(default)]
    pub pattern: Option<String>,
    #[serde(default)]
    pub streams: Vec<String>,
    #[serde(default)]
    pub webhook: Option<Webhook>,
    #[serde(default)]
    pub wake_stream: Option<String>,
    #[serde(default = "default_lease")]
    pub lease_ttl_ms: i64,
    #[serde(default)]
    pub description: Option<String>,
}
fn default_lease() -> i64 {
    30_000
}

impl Config {
    pub fn normalize(mut self) -> ApiResult<Self> {
        if !(1_000..=600_000).contains(&self.lease_ttl_ms) {
            return Err(ApiError::invalid(
                "lease_ttl_ms must be an integer between 1000 and 600000",
            ));
        }
        if self.streams.len() > 4096 {
            return Err(ApiError::invalid("Too many explicit streams"));
        }
        self.streams = self
            .streams
            .iter()
            .map(|p| normalize_path(p, false))
            .collect::<ApiResult<Vec<_>>>()?;
        self.streams.sort();
        self.streams.dedup();
        self.pattern = self
            .pattern
            .as_deref()
            .map(|p| normalize_path(p, true))
            .transpose()?;
        if self.pattern.is_none() && self.streams.is_empty() {
            return Err(ApiError::invalid(
                "A pattern or explicit streams is required",
            ));
        }
        self.wake_stream = self
            .wake_stream
            .as_deref()
            .map(|p| normalize_path(p, false))
            .transpose()?;
        match self.kind {
            DeliveryType::Webhook if self.webhook.is_none() || self.wake_stream.is_some() => {
                return Err(ApiError::invalid(
                    "webhook delivery requires webhook.url and no wake_stream",
                ));
            }
            DeliveryType::PullWake if self.wake_stream.is_none() || self.webhook.is_some() => {
                return Err(ApiError::invalid(
                    "pull-wake delivery requires wake_stream and no webhook",
                ));
            }
            _ => {}
        }
        if self.description.as_ref().is_some_and(|d| d.len() > 4096) {
            return Err(ApiError::invalid("Description too long"));
        }
        Ok(self)
    }
    pub fn hash(&self) -> String {
        format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(self).expect("config is serializable"))
        )
    }
}

pub fn normalize_path(path: &str, pattern: bool) -> ApiResult<String> {
    let path = path.trim_matches('/');
    if path.is_empty()
        || path.len() > 2048
        || path.contains(['?', '#', '\\'])
        || path.chars().any(char::is_control)
    {
        return Err(ApiError::invalid("Invalid stream-root-relative path"));
    }
    for (index, segment) in path.split('/').enumerate() {
        let decoded = percent_encoding::percent_decode_str(segment)
            .decode_utf8()
            .map_err(|_| ApiError::invalid("Invalid path encoding"))?;
        let bytes = segment.as_bytes();
        if bytes.iter().enumerate().any(|(i, b)| {
            *b == b'%'
                && (i + 2 >= bytes.len()
                    || !bytes[i + 1].is_ascii_hexdigit()
                    || !bytes[i + 2].is_ascii_hexdigit())
        }) || segment.is_empty()
            || decoded == "."
            || decoded == ".."
            || decoded.contains('\\')
            || decoded.chars().any(char::is_control)
            || (index == 0 && (decoded == "__ds" || decoded.starts_with("__ds/")))
            || (segment.contains('*') && (!pattern || (segment != "*" && segment != "**")))
        {
            return Err(ApiError::invalid("Invalid stream-root-relative path"));
        }
    }
    let base = url::Url::parse("https://durable-stream.invalid/").unwrap();
    let canonical = base
        .join(&format!("/{path}"))
        .map_err(|_| ApiError::invalid("Invalid stream path"))?;
    Ok(canonical.path().trim_start_matches('/').to_owned())
}

// Dynamic programming avoids exponential recursion for patterns with repeated **.
pub fn glob_matches(pattern: &str, path: &str) -> bool {
    let parts: Vec<_> = path.split('/').collect();
    let mut matched = vec![false; parts.len() + 1];
    matched[0] = true;
    for segment in pattern.split('/') {
        if segment == "**" {
            for i in 1..matched.len() {
                matched[i] |= matched[i - 1];
            }
        } else {
            for i in (1..matched.len()).rev() {
                matched[i] = matched[i - 1] && (segment == "*" || segment == parts[i - 1]);
            }
            matched[0] = false;
        }
    }
    matched[parts.len()]
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WakeInterval {
    pub before: String,
    pub after: String,
    pub incarnation: String,
    #[serde(default)]
    pub origin: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CatalogEntry {
    pub path: String,
    pub version: i64,
    pub incarnation: String,
    pub tail: String,
    /// Beginning of the new incarnation; inherited fork data is historical.
    pub start: String,
    pub exists: bool,
    #[serde(default)]
    pub wakes: Vec<WakeInterval>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Link {
    #[serde(default)]
    pub version: i64,
    pub explicit: bool,
    pub glob: bool,
    pub incarnation: String,
    pub acked: String,
    pub tail: String,
    #[serde(default)]
    pub synthetic: Vec<WakeInterval>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StreamInfo {
    pub path: String,
    pub link_type: String,
    pub acked_offset: String,
    pub tail_offset: String,
    pub has_pending: bool,
    #[serde(skip)]
    pub incarnation: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub info: StreamInfo,
    pub incarnation: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Wake {
    pub id: String,
    pub generation: i64,
    pub snapshot: Vec<Snapshot>,
    pub holder: Option<String>,
    pub deadline: Option<i64>,
    pub issued_at: i64,
    pub token_expires: i64,
    /// Engine-issued capability, committed before it is exposed to a worker.
    #[serde(default)]
    pub capability: Option<String>,
    #[serde(default)]
    pub delivery_error: Option<Value>,
    pub published: bool,
    pub attempts: u32,
    pub next_attempt_at: Option<i64>,
    /// Durable dispatch reservation prevents overlapping sends after a crash.
    pub dispatch_until: Option<i64>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Subscription {
    pub id: String,
    pub root: String,
    pub public_base: String,
    pub config: Config,
    pub config_hash: String,
    pub created_at: String,
    pub generation: i64,
    pub deleted: bool,
    pub links: BTreeMap<String, Link>,
    pub wake: Option<Wake>,
}

impl Subscription {
    pub fn new(
        id: String,
        root: String,
        public_base: String,
        config: Config,
        generation: i64,
        now: i64,
    ) -> Self {
        Self {
            id,
            root,
            public_base,
            config_hash: config.hash(),
            config,
            created_at: chrono::DateTime::from_timestamp_millis(now)
                .unwrap()
                .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            generation,
            deleted: false,
            links: BTreeMap::new(),
            wake: None,
        }
    }
    pub fn relative<'a>(&self, path: &'a str) -> Option<&'a str> {
        path.strip_prefix(&self.root)?.strip_prefix('/')
    }
    pub fn matches(&self, path: &str) -> bool {
        self.config
            .pattern
            .as_ref()
            .is_some_and(|p| glob_matches(p, path))
    }
    pub fn link(
        &mut self,
        path: &str,
        entry: Option<&CatalogEntry>,
        explicit: bool,
        historical: bool,
    ) {
        let glob = self.matches(path);
        let tail = entry
            .filter(|e| e.exists)
            .map_or(ZERO_OFFSET, |e| e.tail.as_str());
        let ack = if historical {
            tail
        } else {
            entry.map_or(ZERO_OFFSET, |e| e.start.as_str())
        };
        let incarnation = entry.map_or("", |e| e.incarnation.as_str());
        let link = self.links.entry(path.to_owned()).or_insert_with(|| Link {
            version: entry.map_or(0, |e| e.version),
            synthetic: Vec::new(),
            explicit: false,
            glob,
            incarnation: incarnation.to_owned(),
            acked: ack.to_owned(),
            tail: tail.to_owned(),
        });
        link.explicit |= explicit;
        link.glob |= glob;
    }
    pub fn observe(&mut self, entry: &CatalogEntry) -> bool {
        let Some(path) = self.relative(&entry.path).map(str::to_owned) else {
            return false;
        };
        let newly_linked = !self.links.contains_key(&path);
        if newly_linked && self.matches(&path) && entry.exists {
            self.link(&path, Some(entry), false, false);
        }
        let origin = format!("{}/{}", self.root, self.id);
        if let Some(link) = self.links.get_mut(&path) {
            if !newly_linked && entry.version <= link.version {
                return false;
            }
            link.version = entry.version;
            if link.incarnation != entry.incarnation || !entry.exists {
                // An old worker must never acknowledge an offset in another
                // incarnation, even when the opaque strings happen to overlap.
                self.wake = None;
            }
            if link.incarnation != entry.incarnation {
                link.incarnation = entry.incarnation.clone();
                link.acked = entry.start.clone();
                link.synthetic.clear();
            }
            for interval in &entry.wakes {
                if interval.origin == origin
                    && interval.incarnation == entry.incarnation
                    && interval.after > link.acked
                    && !link.synthetic.iter().any(|old| old.after == interval.after)
                {
                    link.synthetic.push(interval.clone());
                }
            }
            link.skip_synthetic();
            link.tail = if entry.exists {
                entry.tail.clone()
            } else {
                link.acked.clone()
            };
            return true;
        }
        false
    }
    pub fn infos(&self) -> Vec<StreamInfo> {
        self.links
            .iter()
            .map(|(path, link)| StreamInfo {
                path: path.clone(),
                link_type: if link.explicit { "explicit" } else { "glob" }.to_owned(),
                acked_offset: link.acked.clone(),
                tail_offset: link.tail.clone(),
                has_pending: link.tail > link.acked,
                incarnation: link.incarnation.clone(),
            })
            .collect()
    }
    pub fn pending(&self) -> bool {
        self.links.values().any(|l| l.tail > l.acked)
    }
    pub fn ensure_wake(&mut self, now: i64) -> bool {
        if self.deleted || self.wake.is_some() || !self.pending() {
            return false;
        }
        self.generation += 1;
        self.wake = Some(Wake {
            id: format!("w_{}", uuid::Uuid::new_v4().simple()),
            generation: self.generation,
            snapshot: self
                .infos()
                .into_iter()
                .map(|info| Snapshot {
                    incarnation: info.incarnation.clone(),
                    info,
                })
                .collect(),
            holder: None,
            // The processing lease starts immediately before delivery, after Engine I/O.
            deadline: None,
            issued_at: now,
            token_expires: now + 86_400_000,
            capability: None,
            delivery_error: None,
            published: false,
            attempts: 0,
            next_attempt_at: Some(now),
            dispatch_until: None,
        });
        true
    }
    /// Explicit management changes retry operator-correctable signing failures.
    pub fn resume_delivery(&mut self, now: i64) -> bool {
        if self
            .wake
            .as_ref()
            .is_some_and(|w| w.delivery_error.is_some() && w.next_attempt_at.is_none())
        {
            // Rebuild the snapshot so membership corrections can reduce its size.
            self.wake = None;
            self.ensure_wake(now);
            return true;
        }
        false
    }
    pub fn expire(&mut self, now: i64) -> bool {
        if self
            .wake
            .as_ref()
            .is_some_and(|w| w.deadline.is_some_and(|d| d <= now) || w.token_expires <= now)
        {
            // Failed delivery retains its persisted retry schedule across generations.
            let delivery_error = self.wake.as_ref().and_then(|w| w.delivery_error.clone());
            let retry = self.wake.as_ref().map(|w| {
                if self.config.kind == DeliveryType::Webhook && w.dispatch_until.is_some() {
                    // An expired in-flight attempt has an unknown result. Fence
                    // it, but charge its retry before the old response arrives.
                    let attempts = w.attempts.saturating_add(1);
                    (attempts, Some(now + super::retry_delay(attempts)))
                } else {
                    (w.attempts, w.next_attempt_at)
                }
            });
            self.wake = None;
            self.ensure_wake(now);
            if let (Some(w), Some((attempts, next))) = (&mut self.wake, retry)
                && attempts > 0
            {
                w.delivery_error = delivery_error;
                w.attempts = attempts;
                w.next_attempt_at = next;
                // A failed delivery does not consume its next processing lease while waiting.
                w.deadline = None;
            }
            return true;
        }
        false
    }
    pub fn finish(&mut self, now: i64, auto_ack: bool) -> bool {
        if auto_ack && let Some(wake) = &self.wake {
            for snap in &wake.snapshot {
                if let Some(link) = self.links.get_mut(&snap.info.path)
                    && link.incarnation == snap.incarnation
                    && snap.info.tail_offset > link.acked
                {
                    link.acked = snap.info.tail_offset.clone();
                }
            }
        }
        for link in self.links.values_mut() {
            link.skip_synthetic();
        }
        self.wake = None;
        self.ensure_wake(now)
    }
    pub fn apply_acks(&mut self, acks: &[Ack]) -> ApiResult<()> {
        let mut offsets = BTreeMap::new();
        for ack in acks {
            let path = normalize_path(&ack.stream, false)?;
            let link = self.links.get(&path).ok_or_else(|| {
                ApiError::new(409, "INVALID_OFFSET", "Unknown subscription stream")
            })?;
            let prior = offsets.get(&path).copied().unwrap_or(&link.acked);
            if !valid_offset(&ack.offset) || ack.offset < *prior || ack.offset > link.tail {
                return Err(ApiError::new(
                    409,
                    "INVALID_OFFSET",
                    "Invalid, regressing, or beyond-tail acknowledgment",
                ));
            }
            offsets.insert(path, &ack.offset);
        }
        for (path, offset) in offsets {
            let link = self.links.get_mut(&path).unwrap();
            link.acked = offset.clone();
            link.skip_synthetic();
        }
        Ok(())
    }
    pub fn due(&self) -> Option<i64> {
        let w = self.wake.as_ref()?;
        [
            w.deadline,
            Some(w.token_expires),
            w.next_attempt_at
                .map(|n| n.max(w.dispatch_until.unwrap_or(0))),
        ]
        .into_iter()
        .flatten()
        .min()
    }
    pub fn public(&self, signing: Value) -> Value {
        json!({"id": self.id, "subscription_id": self.id, "type": self.config.kind,
            "pattern": self.config.pattern, "streams": self.infos().into_iter().map(|s| json!({"path":s.path,"link_type":s.link_type,"acked_offset":s.acked_offset})).collect::<Vec<_>>(),
            "webhook": self.config.webhook.as_ref().map(|w| json!({"url":w.url,"signing":signing})),
            "wake_stream": self.config.wake_stream, "lease_ttl_ms":self.config.lease_ttl_ms,
            "created_at":self.created_at, "status": if self.wake.as_ref().is_some_and(|w| w.attempts > 0) {"failed"} else {"active"},
            "description":self.config.description,
            "delivery_error":self.wake.as_ref().and_then(|w| w.delivery_error.as_ref())})
    }
    pub fn paths(&self) -> BTreeSet<String> {
        self.links
            .keys()
            .map(|p| format!("{}/{p}", self.root))
            .collect()
    }
}

impl Link {
    fn skip_synthetic(&mut self) {
        self.synthetic.sort_by(|a, b| a.before.cmp(&b.before));
        for span in &self.synthetic {
            if span.before <= self.acked && span.after > self.acked {
                self.acked = span.after.clone();
            }
        }
        self.synthetic.retain(|span| span.after > self.acked);
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Ack {
    #[serde(alias = "path")]
    pub stream: String,
    pub offset: String,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Progress {
    pub wake_id: String,
    pub generation: i64,
    #[serde(default)]
    pub acks: Vec<Ack>,
    #[serde(default)]
    pub done: bool,
}

pub fn valid_offset(offset: &str) -> bool {
    offset.len() == ZERO_OFFSET.len()
        && offset.as_bytes()[16] == b'_'
        && offset
            .bytes()
            .enumerate()
            .all(|(i, c)| i == 16 || c.is_ascii_digit())
}
