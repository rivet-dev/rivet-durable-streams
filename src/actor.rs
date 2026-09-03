use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::STANDARD};
use rivetkit::prelude::*;
use rivetkit::{
    Action, ActorHttpResponse, Handles, HttpCallbackClass, Request, Response, ResponseChunk,
    StreamingResponse, action,
};
use rivetkit_client::{GetOrCreateOptions, handle::ActorHandle as ClientActorHandle};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, Semaphore, mpsc, watch};

use crate::protocol::{
    LONG_POLL_TIMEOUT_MS, MAX_BODY_BYTES, MAX_SSE_LIFETIME_MS, MAX_TTL_SECONDS, PRODUCER_EPOCH,
    PRODUCER_EXPECTED_SEQ, PRODUCER_ID, PRODUCER_RECEIVED_SEQ, PRODUCER_SEQ, PRODUCER_STATE_TTL_MS,
    SSE_DATA_ENCODING, STREAM_CLOSED, STREAM_CURSOR, STREAM_EXPIRES_AT, STREAM_FORK_OFFSET,
    STREAM_FORK_SUB_OFFSET, STREAM_FORKED_FROM, STREAM_OFFSET, STREAM_SEQ, STREAM_TTL,
    STREAM_UP_TO_DATE, ZERO_OFFSET, concatenate, encode_sse_data, format_json_messages,
    header_is_true, normalize_content_type, parse_non_negative_safe_integer, process_json_append,
    public_header_bytes, public_header_text, response, response_cursor, sanitize_content_type,
    text_response, validate_offset,
};
use crate::store::{
    self, Meta, ProducerState, ReadBatch, begin_mutation, commit_producer_state, create_meta,
    delete_fork_edge, delete_fork_intent, dequeue_gc_release, enqueue_gc_release, fork_edge_count,
    get_fork_edge_tx, get_fork_intent, get_meta, get_meta_tx, get_producer_state, insert_fork_edge,
    pending_fork_intents, pending_gc_releases, purge, put_fork_intent, read_messages_range,
    set_closed, set_last_seq, set_soft_deleted, touch,
};

use crate::facade::stream_actor_key;
use producer::{
    close_success, closed_by_matches, parse_producer, producer_failure, producer_success,
    validate_producer,
};

mod producer;

pub const ACTOR_NAME: &str = "durableStream";
const MAX_OFFSET_CAP: &str = "9999999999999999_9999999999999999";
const MAINTENANCE_RETRY_MS: i64 = 5_000;

type BoxActionFuture<T> = Pin<Box<dyn Future<Output = Result<T>> + Send>>;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DurableStreamInput {
    pub path: String,
    pub tenant_scope: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DurableStreamState {
    pub path: String,
    pub tenant_scope: String,
}

pub struct DurableStreamActor {
    lifecycle: Mutex<()>,
    changes: watch::Sender<u64>,
    live_readers: Arc<Semaphore>,
    readers: Arc<Semaphore>,
}

#[derive(Clone, Debug)]
struct ProducerHeaders {
    id: String,
    epoch: i64,
    seq: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct ClosedBy {
    producer_id: String,
    epoch: i64,
    seq: i64,
}

enum ProducerValidation {
    Accepted(ProducerState),
    Duplicate { last_seq: i64 },
    StaleEpoch { current_epoch: i64 },
    InvalidEpochSeq,
    SequenceGap { expected: i64, received: i64 },
}

struct ReadSnapshot {
    meta: Meta,
    batch: ReadBatch,
}

struct ForkCreateOptions<'a> {
    parent_path: &'a str,
    fork_offset: Option<&'a str>,
    fork_sub_offset: Option<usize>,
    content_type: Option<&'a str>,
    ttl_seconds: Option<i64>,
    expires_at: Option<&'a str>,
    closed: bool,
    body: &'a [u8],
    now_ms: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ForkAcquire {
    edge_id: String,
    fork_offset: Option<String>,
    content_type_provided: Option<String>,
}

impl Action for ForkAcquire {
    type Output = ForkAcquireResult;
    const NAME: &'static str = "__durableStreamForkAcquire";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ForkAcquireResult {
    ok: bool,
    error: Option<String>,
    fork_offset: Option<String>,
    content_type: Option<String>,
    ttl_seconds: Option<i64>,
    expires_at: Option<String>,
    source_generation: Option<String>,
}

impl ForkAcquireResult {
    fn success(fork_offset: String, meta: &Meta) -> Self {
        Self {
            ok: true,
            error: None,
            fork_offset: Some(fork_offset),
            content_type: meta.content_type.clone(),
            ttl_seconds: meta.ttl_seconds,
            expires_at: meta.expires_at.clone(),
            source_generation: Some(meta.generation.clone()),
        }
    }

    fn failure(error: &str) -> Self {
        Self {
            ok: false,
            error: Some(error.to_owned()),
            fork_offset: None,
            content_type: None,
            ttl_seconds: None,
            expires_at: None,
            source_generation: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ForkRelease {
    edge_id: String,
    source_generation: Option<String>,
}

impl Action for ForkRelease {
    type Output = ();
    const NAME: &'static str = "__durableStreamForkRelease";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReadRange {
    after_offset: Option<String>,
    cap_offset: String,
    limit: Option<usize>,
    byte_budget: Option<usize>,
}

impl Action for ReadRange {
    type Output = ReadBatch;
    const NAME: &'static str = "__durableStreamReadRange";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Maintenance;

impl Action for Maintenance {
    type Output = ();
    const NAME: &'static str = "__durableStreamMaintenance";
}

#[async_trait]
impl Actor for DurableStreamActor {
    type State = DurableStreamState;
    type Input = DurableStreamInput;
    type Actions = (ForkAcquire, ForkRelease, ReadRange, Maintenance);
    type Events = ();
    type Queue = ();
    type ConnParams = ();
    type ConnState = ();
    type Action = action::Raw;

    const HAS_DATABASE: bool = true;
    const CONCURRENT_HTTP_CALLBACKS: bool = true;
    const MAX_CONCURRENT_HTTP_CALLBACKS: usize = 128;
    const MAX_CONCURRENT_LIVE_HTTP_CALLBACK_STARTS: usize = 64;

    fn classify_http_request(request: &Request) -> HttpCallbackClass {
        if request.method() == http::Method::GET
            && query_values(request, "live")
                .first()
                .is_some_and(|value| value == "sse")
        {
            HttpCallbackClass::Live
        } else {
            HttpCallbackClass::Standard
        }
    }

    async fn create_state(_ctx: &Ctx<Self>, input: Self::Input) -> Result<Self::State> {
        Ok(DurableStreamState {
            path: input.path,
            tenant_scope: input.tenant_scope,
        })
    }

    async fn create(ctx: &Ctx<Self>) -> Result<Self> {
        store::ensure_schema(ctx.sql()).await?;
        let (changes, _) = watch::channel(0);
        Ok(Self {
            lifecycle: Mutex::new(()),
            changes,
            live_readers: Arc::new(Semaphore::new(64)),
            readers: Arc::new(Semaphore::new(32)),
        })
    }

    async fn on_fetch_response(
        self: Arc<Self>,
        ctx: Ctx<Self>,
        request: Request,
    ) -> Result<ActorHttpResponse> {
        match *request.method() {
            http::Method::PUT => self.handle_put(&ctx, &request).await.map(Into::into),
            http::Method::POST => self.handle_post(&ctx, &request).await.map(Into::into),
            http::Method::GET => self.handle_get(ctx, request).await,
            http::Method::HEAD => self.handle_head(&ctx).await.map(Into::into),
            http::Method::DELETE => self.handle_delete(&ctx).await.map(Into::into),
            _ => response(
                405,
                [("allow", "GET, POST, PUT, DELETE, HEAD, OPTIONS")],
                Vec::new(),
            )
            .map(Into::into),
        }
    }

    async fn on_start(self: Arc<Self>, ctx: Ctx<Self>) -> Result<()> {
        self.run_maintenance(&ctx).await
    }
}

impl Handles<ForkAcquire> for DurableStreamActor {
    type Future = BoxActionFuture<ForkAcquireResult>;

    fn handle(self: Arc<Self>, ctx: Ctx<Self>, action: ForkAcquire) -> Self::Future {
        Box::pin(async move { self.fork_acquire(&ctx, action).await })
    }
}

impl Handles<ForkRelease> for DurableStreamActor {
    type Future = BoxActionFuture<()>;

    fn handle(self: Arc<Self>, ctx: Ctx<Self>, action: ForkRelease) -> Self::Future {
        Box::pin(async move { self.fork_release(&ctx, action).await })
    }
}

impl Handles<ReadRange> for DurableStreamActor {
    type Future = BoxActionFuture<ReadBatch>;

    fn handle(self: Arc<Self>, ctx: Ctx<Self>, action: ReadRange) -> Self::Future {
        Box::pin(async move {
            let Some(meta) = get_meta(ctx.sql()).await? else {
                return Ok(ReadBatch::default());
            };
            self.read_stitched(
                &ctx,
                &meta,
                action.after_offset.as_deref(),
                Some(&action.cap_offset),
                action.limit.unwrap_or(crate::protocol::MAX_READ_ROWS),
                action
                    .byte_budget
                    .unwrap_or(crate::protocol::MAX_READ_BYTES),
            )
            .await
        })
    }
}

impl Handles<Maintenance> for DurableStreamActor {
    type Future = BoxActionFuture<()>;

    fn handle(self: Arc<Self>, ctx: Ctx<Self>, _action: Maintenance) -> Self::Future {
        Box::pin(async move { self.run_maintenance(&ctx).await })
    }
}

mod fork;
mod lifecycle;
mod maintenance;
mod read;
mod write;

fn query_values(request: &Request, name: &str) -> Vec<String> {
    request
        .uri()
        .query()
        .map(|query| {
            url::form_urlencoded::parse(query.as_bytes())
                .filter(|(key, _)| key == name)
                .map(|(_, value)| value.into_owned())
                .collect()
        })
        .unwrap_or_default()
}

fn is_concrete_offset(value: &str) -> bool {
    let Some((read_seq, byte_offset)) = value.split_once('_') else {
        return false;
    };
    !read_seq.is_empty()
        && !byte_offset.is_empty()
        && read_seq.bytes().all(|byte| byte.is_ascii_digit())
        && byte_offset.bytes().all(|byte| byte.is_ascii_digit())
}

fn parse_ttl(value: &str) -> Result<i64, ()> {
    if value.is_empty()
        || (value.len() > 1 && value.starts_with('0'))
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(());
    }
    let seconds: i64 = value.parse().map_err(|_| ())?;
    (seconds <= MAX_TTL_SECONDS).then_some(seconds).ok_or(())
}

fn metadata_headers(meta: &Meta) -> HashMap<String, String> {
    let mut headers = HashMap::from([(STREAM_OFFSET.to_owned(), meta.current_offset.clone())]);
    if let Some(content_type) = &meta.content_type {
        headers.insert("content-type".to_owned(), content_type.clone());
    }
    if meta.closed {
        headers.insert(STREAM_CLOSED.to_owned(), "true".to_owned());
    }
    if let Some(ttl) = meta.ttl_seconds {
        headers.insert(STREAM_TTL.to_owned(), ttl.to_string());
    }
    if let Some(expires_at) = &meta.expires_at {
        headers.insert(STREAM_EXPIRES_AT.to_owned(), expires_at.clone());
    }
    headers
}

fn make_etag(actor_id: &str, start: &str, end: &str, closed: bool) -> String {
    let suffix = if closed { ":c" } else { "" };
    format!(
        "\"{}:{start}:{end}{suffix}\"",
        STANDARD.encode(actor_id.as_bytes())
    )
}

fn base64_url(value: &[u8]) -> String {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    URL_SAFE_NO_PAD.encode(value)
}

fn decode_base64_url(value: &str) -> Result<Vec<u8>> {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    Ok(URL_SAFE_NO_PAD.decode(value)?)
}

async fn send_finish(tx: &mpsc::Sender<ResponseChunk>, data: Vec<u8>) -> Result<()> {
    let _ = tx.send(ResponseChunk::Data { data, finish: true }).await;
    Ok(())
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn producer(epoch: i64, seq: i64) -> ProducerHeaders {
        ProducerHeaders {
            id: "p".to_owned(),
            epoch,
            seq,
        }
    }

    #[test]
    fn producer_validation_fences_and_deduplicates() {
        assert!(matches!(
            validate_producer(None, &producer(2, 0), 10),
            ProducerValidation::Accepted(_)
        ));
        let state = ProducerState {
            epoch: 2,
            last_seq: 3,
            last_updated: 0,
        };
        assert!(matches!(
            validate_producer(Some(&state), &producer(1, 4), 10),
            ProducerValidation::StaleEpoch { current_epoch: 2 }
        ));
        assert!(matches!(
            validate_producer(Some(&state), &producer(2, 3), 10),
            ProducerValidation::Duplicate { last_seq: 3 }
        ));
        assert!(matches!(
            validate_producer(Some(&state), &producer(2, 5), 10),
            ProducerValidation::SequenceGap {
                expected: 4,
                received: 5
            }
        ));
    }

    #[test]
    fn ttl_parser_is_canonical_and_bounded() {
        assert_eq!(parse_ttl("0"), Ok(0));
        assert_eq!(parse_ttl("10"), Ok(10));
        assert!(parse_ttl("01").is_err());
        assert!(parse_ttl("3153600001").is_err());
    }
}
