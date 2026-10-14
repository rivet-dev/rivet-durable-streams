//! Durable subscription coordinator. One actor owns a tenant's catalog and cursors;
//! stream actors publish a transactional outbox into this owner.
pub(crate) mod catalog_import;
mod db;
mod engine;
pub(crate) mod facade;
pub(crate) mod fault;
mod legacy;
pub(crate) mod model;
mod security;
mod signing;
use signing::*;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use engine::{CapabilityClaims, CapabilityKind, EngineClient, EngineError, VerifiedCapability};
use futures_util::{StreamExt, stream};
use rivetkit::{Action, Handles, action, prelude::*};
use rivetkit_client::{Client, GetOptions, GetOrCreateOptions, handle::ActorHandle};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Mutex, OnceCell};

use db::text;
use model::*;

pub const ACTOR_NAME: &str = "durableStreamSubscriptions";
const TICK_NAME: &str = "__durableStreamSubscriptionTick";
const NOTIFY_NAME: &str = "__durableStreamCatalogChanged";
const CONTROL_NAME: &str = "__durableStreamSubscriptionControl";
const DISPATCH_TIMEOUT: i64 = 60_000;
type BoxFuture<T> = Pin<Box<dyn Future<Output = Result<T>> + Send>>;

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Input {
    pub tenant_scope: String,
}

pub struct SubscriptionActor {
    client: Client,
    engine: EngineClient,
    namespace: OnceCell<String>,
    lock: Mutex<()>,
    pumping: AtomicBool,
    alarm: Mutex<Option<(i64, String)>>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CatalogChanged(pub CatalogEntry);
impl Action for CatalogChanged {
    type Output = ();
    const NAME: &'static str = NOTIFY_NAME;
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CatalogReady {
    pub complete: bool,
}
impl Action for CatalogReady {
    type Output = bool;
    const NAME: &'static str = "__durableStreamCatalogReady";
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Tick;
impl Action for Tick {
    type Output = ();
    const NAME: &'static str = TICK_NAME;
}
#[derive(Clone, Serialize, Deserialize)]
pub struct Control {
    root: String,
    public_base: String,
    id: Option<String>,
    operation: String,
    body: Value,
    token: Option<String>,
    path: Option<String>,
    allow_local: bool,
    #[serde(default)]
    deadline: Option<i64>,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct Reply {
    pub status: u16,
    pub body: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub age: Option<u64>,
}
impl Reply {
    fn ok(status: u16, body: Value) -> Self {
        Self {
            status,
            body,
            retry_after: None,
            age: None,
        }
    }
}
impl From<ApiError> for Reply {
    fn from(e: ApiError) -> Self {
        Self {
            status: e.status,
            body: e.body,
            retry_after: e.retry_after,
            age: None,
        }
    }
}
impl Action for Control {
    type Output = Reply;
    const NAME: &'static str = CONTROL_NAME;
}

pub fn handle(client: &Client, tenant: &str) -> Result<ActorHandle> {
    client.get_or_create(
        ACTOR_NAME,
        vec![crate::facade::stream_actor_key(
            tenant,
            "__ds/subscriptions",
        )],
        GetOrCreateOptions {
            create_with_input: Some(json!({"tenant_scope":tenant})),
            ..Default::default()
        },
    )
}

#[async_trait]
impl Actor for SubscriptionActor {
    type State = Input;
    type Input = Input;
    type Actions = (CatalogChanged, Tick, Control, CatalogReady);
    type Events = ();
    type Queue = ();
    type ConnParams = ();
    type ConnState = ();
    type Action = action::Raw;
    const HAS_DATABASE: bool = true;
    async fn create_state(_: &Ctx<Self>, input: Input) -> Result<Input> {
        Ok(input)
    }
    async fn create(ctx: &Ctx<Self>) -> Result<Self> {
        db::ensure(ctx.sql()).await?;
        ctx.sql().exec("CREATE TABLE IF NOT EXISTS subscription_engine_identity (id INTEGER PRIMARY KEY CHECK(id=1), namespace TEXT NOT NULL)").await?;
        let runtime = ctx.inner();
        let connection = catalog_import::CatalogApi {
            endpoint: runtime
                .client_endpoint()
                .ok_or_else(|| anyhow!("Missing Engine endpoint"))?
                .to_owned(),
            namespace: runtime
                .client_namespace()
                .ok_or_else(|| anyhow!("Missing Engine namespace"))?
                .to_owned(),
            token: runtime.client_token().map(str::to_owned),
        };
        let issuer = std::env::var("DS_ENGINE_ISSUER").ok();
        Ok(Self {
            client: crate::client::actor_client(ctx)?,
            engine: EngineClient::new(&connection, issuer.as_deref())?,
            namespace: OnceCell::new(),
            lock: Mutex::new(()),
            pumping: AtomicBool::new(false),
            alarm: Mutex::new(None),
        })
    }
    async fn on_start(self: Arc<Self>, ctx: Ctx<Self>) -> Result<()> {
        self.kick(&ctx);
        Ok(())
    }
}
impl Handles<CatalogChanged> for SubscriptionActor {
    type Future = BoxFuture<()>;
    fn handle(self: Arc<Self>, ctx: Ctx<Self>, action: CatalogChanged) -> Self::Future {
        Box::pin(async move {
            self.observe(&ctx, action.0).await?;
            self.kick(&ctx);
            Ok(())
        })
    }
}
impl Handles<CatalogReady> for SubscriptionActor {
    type Future = BoxFuture<bool>;
    fn handle(self: Arc<Self>, ctx: Ctx<Self>, action: CatalogReady) -> Self::Future {
        Box::pin(async move {
            let _guard = self.lock.lock().await;
            if action.complete {
                ctx.sql()
                    .execute(
                        "UPDATE subscription_catalog_status SET ready=1 WHERE id=1",
                        None,
                    )
                    .await?;
            }
            let rows = ctx
                .sql()
                .query(
                    "SELECT ready FROM subscription_catalog_status WHERE id=1",
                    None,
                )
                .await?;
            Ok(matches!(
                rows.rows.first().and_then(|r| r.first()),
                Some(rivetkit::ColumnValue::Integer(1))
            ))
        })
    }
}
impl Handles<Tick> for SubscriptionActor {
    type Future = BoxFuture<()>;
    fn handle(self: Arc<Self>, ctx: Ctx<Self>, _: Tick) -> Self::Future {
        Box::pin(async move {
            {
                let mut alarm = self.alarm.lock().await;
                if alarm.as_ref().is_some_and(|(time, _)| *time <= now_ms()) {
                    *alarm = None;
                }
            }
            if let Err(error) = self.fanout_tick(&ctx).await {
                self.arm(&ctx, now_ms() + 1_000).await?;
                return Err(error);
            }
            self.kick(&ctx);
            Ok(())
        })
    }
}
impl Handles<Control> for SubscriptionActor {
    type Future = BoxFuture<Reply>;
    fn handle(self: Arc<Self>, ctx: Ctx<Self>, request: Control) -> Self::Future {
        Box::pin(async move {
            let reply = self.control(&ctx, request).await?;
            self.kick(&ctx);
            Ok(reply)
        })
    }
}

impl SubscriptionActor {
    async fn arm(&self, ctx: &Ctx<Self>, when: i64) -> Result<()> {
        let mut alarm = self.alarm.lock().await;
        if alarm.as_ref().is_some_and(|(at, _)| *at <= when) {
            return Ok(());
        }
        let id = ctx.schedule().at(when, TICK_NAME, &[]).await?;
        if let Some((_, old)) = alarm.replace((when, id)) {
            ctx.schedule().cancel(&old).await?;
        }
        Ok(())
    }
    fn kick(self: &Arc<Self>, ctx: &Ctx<Self>) {
        let this = self.clone();
        let work_ctx = ctx.clone();
        ctx.wait_until(async move {
            if let Err(error) = this.clone().pump(work_ctx.clone()).await {
                tracing::warn!(?error, "Subscription delivery deferred to durable schedule");
                if let Err(error) = this.arm(&work_ctx, now_ms() + 1_000).await {
                    tracing::error!(?error, "Subscription recovery schedule failed");
                }
            }
        });
    }
    async fn observe(&self, ctx: &Ctx<Self>, entry: CatalogEntry) -> Result<()> {
        let _guard = self.lock.lock().await;
        self.arm(ctx, now_ms() + 1_000).await?;
        let tx = db::begin(ctx.sql()).await?;
        db::save_catalog(&tx, &entry).await?;
        // Stream responses wait for this durable catalog update. Fanout runs
        // separately in bounded pump transactions.
        tx.commit().await
    }

    async fn fanout_batch(
        &self,
        tx: &rivetkit::SqliteTransaction,
        path: Option<&str>,
    ) -> Result<()> {
        const BATCH: usize = 64;
        let Some(job) = db::fanout(tx, path).await? else {
            return Ok(());
        };
        let entry = &job.entry;
        // Bind prefix lengths rather than every prefix: a deep path otherwise
        // exceeds SQLite's compound-select limit and duplicates quadratic bytes.
        // Each root still uses the (root,key) index and reads at most one batch.
        let roots = entry
            .path
            .chars()
            .enumerate()
            .filter_map(|(i, c)| (job.discover && entry.exists && c == '/').then_some(i))
            .collect::<Vec<_>>();
        let sql = format!(
            "WITH roots AS (SELECT substr(?1,1,value) AS root FROM json_each(?3)),
             candidates AS (
               SELECT key FROM (SELECT sub_key AS key FROM subscription_links WHERE path=?1 AND sub_key>?2 ORDER BY sub_key LIMIT {BATCH})
               UNION
               SELECT candidate.value FROM roots JOIN json_each((
                 SELECT json_group_array(key) FROM (
                   SELECT key FROM subscriptions WHERE root=roots.root AND key>?2 AND pattern IS NOT NULL ORDER BY key LIMIT {BATCH}
                 )
               )) AS candidate
             )
             SELECT s.key,s.root,s.pattern,EXISTS(SELECT 1 FROM subscription_links l WHERE l.path=?1 AND l.sub_key=s.key),octet_length(s.value)
             FROM subscriptions s JOIN candidates c ON c.key=s.key ORDER BY s.key LIMIT {BATCH}"
        );
        let rows = tx
            .execute(
                sql,
                Some(vec![
                    text(&entry.path),
                    text(&job.cursor),
                    text(serde_json::to_string(&roots)?),
                ]),
            )
            .await?;
        let mut cursor = job.cursor.clone();
        let mut state_bytes = 0_i64;
        let mut limited = false;
        for row in &rows.rows {
            let key = db::string(row, 0)?;
            let root = db::string(row, 1)?;
            let linked = matches!(row.get(3), Some(rivetkit::ColumnValue::Integer(1)));
            let relative = entry
                .path
                .strip_prefix(root)
                .and_then(|p| p.strip_prefix('/'));
            if !linked && !relative.is_some_and(|path|matches!(row.get(2),Some(rivetkit::ColumnValue::Text(pattern)) if glob_matches(pattern,path))) {
                cursor = key.to_owned();
                continue;
            }
            let Some(rivetkit::ColumnValue::Integer(bytes)) = row.get(4) else {
                return Err(anyhow!("Missing subscription state size"));
            };
            // Admit at least one state, then cap aggregate rewrites. Leave the
            // cursor before the deferred row so the next transaction resumes it.
            if state_bytes > 0 && state_bytes.saturating_add(*bytes) > 8 * 1024 * 1024 {
                limited = true;
                break;
            }
            cursor = key.to_owned();
            // Reads and JSON parsing also consume the transaction budget, even
            // when reconciliation has already applied this catalog version.
            state_bytes = state_bytes.saturating_add(*bytes);
            let Some(mut sub) = db::load(tx, key).await? else {
                continue;
            };
            if sub.deleted
                || relative
                    .and_then(|p| sub.links.get(p))
                    .is_some_and(|l| l.version >= entry.version)
            {
                continue;
            }
            sub.observe(entry);
            sub.expire(now_ms());
            sub.ensure_wake(now_ms());
            if relative.is_some_and(|p| sub.links.contains_key(p)) {
                tx.execute(
                    "INSERT OR IGNORE INTO subscription_links(path,sub_key) VALUES(?,?)",
                    Some(vec![text(&entry.path), text(key)]),
                )
                .await?;
            }
            db::save(tx, &sub).await?;
        }
        db::advance_fanout(tx, &job, &cursor, !limited && rows.rows.len() < BATCH).await
    }

    async fn fanout_tick(&self, ctx: &Ctx<Self>) -> Result<()> {
        let _guard = self.lock.lock().await;
        if ctx
            .sql()
            .query("SELECT 1 FROM subscription_fanout LIMIT 1", None)
            .await?
            .rows
            .is_empty()
        {
            return Ok(());
        }
        // Fanout progresses while the delivery pump awaits slow destinations.
        self.arm(ctx, now_ms() + 1_000).await?;
        let tx = db::begin(ctx.sql()).await?;
        self.fanout_batch(&tx, None).await?;
        let more = !tx
            .execute("SELECT 1 FROM subscription_fanout LIMIT 1", None)
            .await?
            .rows
            .is_empty();
        tx.commit().await?;
        if more {
            self.arm(ctx, now_ms() + 10).await?;
        }
        Ok(())
    }

    async fn control(&self, ctx: &Ctx<Self>, request: Control) -> Result<Reply> {
        if request.operation == "jwks" {
            return Ok(match self.public_keys(ctx).await {
                Ok(r) => r,
                Err(e) => e.into(),
            });
        }
        let id = request
            .id
            .as_deref()
            .ok_or_else(|| anyhow!("Missing routed subscription ID"))?;
        let config = if matches!(request.operation.as_str(), "put" | "confirm") {
            let config = match serde_json::from_value::<Config>(request.body.clone()) {
                Ok(c) => match c.normalize() {
                    Ok(c) => c,
                    Err(e) => return Ok(e.into()),
                },
                Err(e) => return Ok(ApiError::invalid(&e.to_string()).into()),
            };
            Some(config)
        } else {
            None
        };
        if let Some(config) = &config {
            let existing = {
                let _guard = self.lock.lock().await;
                self.arm(ctx, now_ms() + 1_000).await?;
                let tx = db::begin(ctx.sql()).await?;
                let mut existing = db::load(&tx, &db::key(&request.root, id)).await?;
                if let Some(sub) = existing.as_mut().filter(|s| !s.deleted) {
                    let mut changed = sub.expire(now_ms());
                    if sub.config_hash == config.hash() {
                        changed |= sub.resume_delivery(now_ms());
                    }
                    if changed {
                        db::save(&tx, sub).await?;
                    }
                    tx.commit().await?;
                } else {
                    tx.rollback().await?;
                }
                existing
            };
            if let Some(sub) = existing.filter(|s| !s.deleted) {
                return Ok(if sub.config_hash == config.hash() {
                    Reply::ok(
                        200,
                        sub.public(
                            match self
                                .signing_metadata(
                                    ctx,
                                    &sub.public_base,
                                    sub.config.webhook.is_some(),
                                )
                                .await
                            {
                                Ok(v) => v,
                                Err(e) => return Ok(e.into()),
                            },
                        ),
                    )
                } else {
                    ApiError::new(
                        409,
                        "SUBSCRIPTION_ALREADY_EXISTS",
                        "Different subscription configuration",
                    )
                    .into()
                });
            }
            if request.operation == "confirm" {
                return Ok(
                    ApiError::new(404, "SUBSCRIPTION_NOT_FOUND", "Subscription not found").into(),
                );
            }
            if let Some(webhook) = &config.webhook
                && let Err(error) = security::destination(&webhook.url, request.allow_local).await
            {
                return Ok(ApiError::new(400, "WEBHOOK_URL_REJECTED", &error.to_string()).into());
            }
        }
        if request.operation == "add"
            && let Err(error) = added_streams(&request.body)
        {
            return Ok(error.into());
        }
        // All external work happens before acquiring the mutation lock.
        let webhook = if let Some(config) = &config {
            config.webhook.is_some()
        } else if request.operation == "get" {
            let _guard = self.lock.lock().await;
            let tx = db::begin(ctx.sql()).await?;
            let webhook = db::load(&tx, &db::key(&request.root, id))
                .await?
                .is_some_and(|s| !s.deleted && s.config.webhook.is_some());
            tx.rollback().await?;
            webhook
        } else {
            false
        };
        let signing = match self
            .signing_metadata(ctx, &request.public_base, webhook)
            .await
        {
            Ok(value) => value,
            Err(e) => return Ok(e.into()),
        };
        let authorization = match self.prepare_authorization(ctx, &request).await {
            Ok(value) => value,
            Err(e) => return Ok(e.into()),
        };
        if matches!(request.operation.as_str(), "put" | "add") {
            self.refresh_membership_catalog(ctx, &request, config.as_ref())
                .await?;
        }
        let _guard = self.lock.lock().await;
        // Arm before the mutation; a surplus schedule is safe, missing recovery isn't.
        self.arm(ctx, now_ms() + 1_000).await?;
        let tx = db::begin(ctx.sql()).await?;
        let key = db::key(&request.root, id);
        let existing = db::load(&tx, &key).await?;
        if let Some(config) = config {
            if let Some(sub) = existing.as_ref().filter(|s| !s.deleted) {
                let reply = if sub.config_hash == config.hash() {
                    Reply::ok(200, sub.public(signing.clone()))
                } else {
                    ApiError::new(
                        409,
                        "SUBSCRIPTION_ALREADY_EXISTS",
                        "Different subscription configuration",
                    )
                    .into()
                };
                tx.rollback().await?;
                return Ok(reply);
            }
            let mut sub = Subscription::new(
                id.to_owned(),
                request.root.clone(),
                request.public_base.clone(),
                config,
                existing.map_or(0, |s| s.generation + 1),
                now_ms(),
            );
            let paths = sub.config.streams.clone();
            self.add_links(&tx, &mut sub, &paths)
                .await
                .map_err(|e| anyhow!("add subscription links: {}", e.body))?;
            if sub.config.pattern.is_some() {
                let mut after = String::new();
                loop {
                    let rows=tx.execute("SELECT path,value FROM subscription_catalog WHERE path>? AND path>=? AND path<? ORDER BY path LIMIT 128",Some(vec![text(&after),text(format!("{}/",sub.root)),text(format!("{}0",sub.root))])).await?;
                    if rows.rows.is_empty() {
                        break;
                    }
                    for row in &rows.rows {
                        after = db::string(row, 0)?.to_owned();
                        let entry: CatalogEntry = serde_json::from_str(db::string(row, 1)?)?;
                        if entry.exists
                            && let Some(path) = sub.relative(&entry.path).map(str::to_owned)
                            && sub.matches(&path)
                        {
                            sub.link(&path, Some(&entry), false, true);
                        }
                    }
                }
            }
            db::save(&tx, &sub).await?;
            db::sync_links(&tx, &sub).await?;
            tx.commit().await?;
            return Ok(Reply::ok(201, sub.public(signing)));
        }
        let capability = matches!(request.operation.as_str(), "callback" | "ack" | "release");
        let Some(mut sub) = existing.filter(|s| !s.deleted) else {
            tx.rollback().await?;
            return Ok(if capability {
                ApiError::fenced()
            } else {
                ApiError::new(404, "SUBSCRIPTION_NOT_FOUND", "Subscription not found")
            }
            .into());
        };
        let mut changed = false;
        if matches!(
            request.operation.as_str(),
            "claim" | "callback" | "ack" | "release"
        ) {
            changed |= self.reconcile_subscription(&tx, &mut sub).await?;
        }
        changed |= sub.expire(now_ms());
        changed |= sub.ensure_wake(now_ms());
        if request.operation == "claim" {
            if changed {
                db::save(&tx, &sub).await?;
            }
            tx.commit().await?;
            drop(_guard);
            return Ok(match self.claim(ctx, sub, &request).await {
                Ok(r) => r,
                Err(e) => e.into(),
            });
        }
        let result = self
            .apply_control(&mut sub, &request, authorization.as_ref(), signing, &tx)
            .await;
        let renew_lease = result.is_ok()
            && matches!(request.operation.as_str(), "callback" | "ack")
            && request.body.get("done").and_then(Value::as_bool) != Some(true);
        let reply = match result {
            Ok(r) => {
                changed |= request.operation != "get";
                r
            }
            Err(e) => e.into(),
        };
        // apply_control validates the entire batch before mutation. Expiry itself
        // persists even when the triggering request is fenced.
        if changed {
            if renew_lease {
                let persisted = db::save_with_lease(&tx, &mut sub)
                    .await
                    .map_err(internal)
                    .and_then(|_| signing::require_live_lease(&sub));
                if let Err(error) = persisted {
                    tx.rollback().await?;
                    return Ok(error.into());
                }
            } else {
                db::save(&tx, &sub).await?;
            }
        }
        if matches!(request.operation.as_str(), "delete" | "add" | "remove") {
            db::sync_links(&tx, &sub).await?;
        }
        tx.commit().await?;
        Ok(reply)
    }

    async fn reconcile_subscription(
        &self,
        tx: &rivetkit::SqliteTransaction,
        sub: &mut Subscription,
    ) -> Result<bool> {
        let mut after = String::new();
        let key = db::key(&sub.root, &sub.id);
        let mut changed = false;
        loop {
            let rows=tx.execute("SELECT c.path,c.value FROM subscription_links l JOIN subscription_catalog c ON c.path=l.path WHERE l.sub_key=? AND l.path>? ORDER BY l.path LIMIT 128",Some(vec![text(&key),text(&after)])).await?;
            if rows.rows.is_empty() {
                break;
            }
            for row in &rows.rows {
                after = db::string(row, 0)?.to_owned();
                let entry: CatalogEntry = serde_json::from_str(db::string(row, 1)?)?;
                changed |= sub.observe(&entry);
            }
        }
        Ok(changed)
    }

    async fn refresh_membership_catalog(
        &self,
        ctx: &Ctx<Self>,
        request: &Control,
        config: Option<&Config>,
    ) -> Result<()> {
        let mut paths = std::collections::BTreeSet::new();
        if let Some(config) = config {
            paths.extend(
                config
                    .streams
                    .iter()
                    .map(|p| format!("{}/{p}", request.root)),
            );
            if let Some(pattern) = &config.pattern {
                let mut after = String::new();
                loop {
                    let rows=ctx.sql().query("SELECT path FROM subscription_catalog WHERE path>? AND path>=? AND path<? ORDER BY path LIMIT 128",
                        Some(vec![text(&after),text(format!("{}/",request.root)),text(format!("{}0",request.root))])).await?;
                    if rows.rows.is_empty() {
                        break;
                    }
                    for row in &rows.rows {
                        after = db::string(row, 0)?.to_owned();
                        if after
                            .strip_prefix(&format!("{}/", request.root))
                            .is_some_and(|p| glob_matches(pattern, p))
                        {
                            paths.insert(after.clone());
                        }
                    }
                }
            }
        } else if let Some(streams) = request.body.get("streams").and_then(Value::as_array) {
            for path in streams.iter().filter_map(Value::as_str) {
                if let Ok(path) = normalize_path(path, false) {
                    paths.insert(format!("{}/{path}", request.root));
                }
            }
        }
        let client = &self.client;
        let tenant = ctx.state().tenant_scope.clone();
        let mut snapshots = stream::iter(paths)
            .map(|path| {
                let client = &client;
                let tenant = &tenant;
                async move {
                    tokio::time::timeout(Duration::from_secs(10), async {
                        let actor = client.get(
                            crate::actor::ACTOR_NAME,
                            vec![crate::facade::stream_actor_key(tenant, &path)],
                            GetOptions::default(),
                        )?;
                        if actor.resolve_optional().await?.is_none() {
                            return Ok::<_, anyhow::Error>(None);
                        }
                        let value = actor
                            .action("__durableStreamSubscriptionSnapshot", vec![])
                            .await?;
                        let entry = value
                            .get("entry")
                            .filter(|v| !v.is_null())
                            .cloned()
                            .map(serde_json::from_value::<CatalogEntry>)
                            .transpose()?;
                        Ok(entry)
                    })
                    .await?
                }
            })
            .buffer_unordered(8);
        while let Some(entry) = snapshots.next().await {
            if let Some(entry) = entry? {
                self.observe(ctx, entry).await?;
            }
        }
        Ok(())
    }

    async fn add_links(
        &self,
        tx: &rivetkit::SqliteTransaction,
        sub: &mut Subscription,
        paths: &[String],
    ) -> ApiResult<()> {
        for batch in paths.chunks(16) {
            let placeholders = vec!["?"; batch.len()].join(",");
            let rows = tx
                .execute(
                    format!(
                        "SELECT path,value FROM subscription_catalog WHERE path IN ({placeholders})"
                    ),
                    Some(
                        batch
                            .iter()
                            .map(|p| text(format!("{}/{p}", sub.root)))
                            .collect(),
                    ),
                )
                .await
                .map_err(internal)?;
            let mut catalog = std::collections::HashMap::new();
            for row in &rows.rows {
                let entry: CatalogEntry =
                    serde_json::from_str(db::string(row, 1).map_err(internal)?)
                        .map_err(|e| internal(e.into()))?;
                catalog.insert(entry.path.clone(), entry);
            }
            for path in batch {
                sub.link(
                    path,
                    catalog.get(&format!("{}/{path}", sub.root)),
                    true,
                    true,
                );
            }
        }
        Ok(())
    }

    async fn apply_control(
        &self,
        sub: &mut Subscription,
        r: &Control,
        authorization: Option<&Authorization>,
        signing: Value,
        tx: &rivetkit::SqliteTransaction,
    ) -> ApiResult<Reply> {
        let now = now_ms();
        match r.operation.as_str() {
            "get" => Ok(Reply::ok(200, sub.public(signing))),
            "delete" => {
                sub.deleted = true;
                sub.wake = None;
                sub.links.clear();
                sub.generation += 1;
                Ok(Reply::ok(204, Value::Null))
            }
            "add" => {
                let paths = added_streams(&r.body)?;
                self.add_links(tx, sub, &paths).await?;
                sub.resume_delivery(now);
                Ok(Reply::ok(204, Value::Null))
            }
            "remove" => {
                let path = normalize_path(r.path.as_deref().unwrap_or(""), false)?;
                if let Some(link) = sub.links.get_mut(&path) {
                    link.explicit = false;
                    if !link.glob {
                        sub.links.remove(&path);
                    }
                }
                sub.resume_delivery(now);
                Ok(Reply::ok(204, Value::Null))
            }
            "callback" | "ack" | "release" => {
                let progress: Progress = serde_json::from_value(r.body.clone())
                    .map_err(|_| ApiError::invalid("Invalid wake progress"))?;
                if (r.operation == "callback") != (sub.config.kind == DeliveryType::Webhook) {
                    return Err(ApiError::invalid("Wrong delivery type"));
                }
                if r.public_base != sub.public_base {
                    return Err(ApiError::new(
                        401,
                        "TOKEN_INVALID",
                        "Capability is for a different public stream root",
                    ));
                }
                match authorization.ok_or_else(|| {
                    ApiError::new(401, "TOKEN_INVALID", "Invalid capability token")
                })? {
                    Authorization::Engine(capability) => {
                        validate_capability(capability, sub, &r.operation, now)?
                    }
                    Authorization::Legacy(keys) => {
                        keys.verify(r.token.as_deref().unwrap_or(""), sub, now)?
                    }
                }
                let w = sub.wake.as_ref().ok_or_else(ApiError::fenced)?;
                if w.id != progress.wake_id
                    || w.generation != progress.generation
                    || (sub.config.kind == DeliveryType::PullWake && w.holder.is_none())
                {
                    return Err(ApiError::fenced());
                }
                if r.operation == "release" {
                    if !progress.acks.is_empty() || progress.done {
                        return Err(ApiError::invalid("release cannot acknowledge"));
                    }
                    sub.finish(now, false);
                    return Ok(Reply::ok(204, Value::Null));
                }
                sub.apply_acks(&progress.acks)?;
                let next = if progress.done {
                    sub.finish(now, false)
                } else {
                    let w = sub.wake.as_mut().unwrap();
                    w.deadline = Some((now + sub.config.lease_ttl_ms).min(w.token_expires));
                    false
                };
                Ok(Reply::ok(200, json!({"ok":true,"next_wake":next})))
            }
            _ => Err(ApiError::new(404, "NOT_FOUND", "Unknown control route")),
        }
    }

    async fn pump(self: Arc<Self>, ctx: Ctx<Self>) -> Result<()> {
        {
            let _guard = self.lock.lock().await;
            if let Some(deadline) = db::cleanup_legacy(ctx.sql(), now_ms()).await? {
                self.arm(&ctx, deadline).await?;
            }
        }
        if self.pumping.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        struct Reset<'a>(&'a AtomicBool);
        impl Drop for Reset<'_> {
            fn drop(&mut self) {
                self.0.store(false, Ordering::Release);
            }
        }
        let _reset = Reset(&self.pumping);
        // Fanout and reservation can each rewrite a maximum-size snapshot.
        // Commit separately and release the mutation lock between those steps.
        self.fanout_tick(&ctx).await?;
        // Always leave a recovery alarm before marking delivery in flight.
        let jobs = {
            let _guard = self.lock.lock().await;
            let due = ctx
                .sql()
                .query(
                    "SELECT key FROM subscriptions WHERE due<=? LIMIT 1",
                    Some(vec![rivetkit::BindParam::Integer(now_ms())]),
                )
                .await?;
            if due.rows.is_empty() {
                let rows = ctx
                    .sql()
                    .query(
                        "SELECT MIN(due) FROM subscriptions WHERE due IS NOT NULL",
                        None,
                    )
                    .await?;
                if let Some(rivetkit::ColumnValue::Integer(at)) =
                    rows.rows.first().and_then(|r| r.first())
                {
                    self.arm(&ctx, (*at).max(now_ms() + 10)).await?;
                }
                return Ok(());
            }
            self.arm(&ctx, now_ms() + 1_000).await?;
            let tx = db::begin(ctx.sql()).await?;
            let rows = tx
                .execute(
                    "SELECT key,octet_length(value) FROM subscriptions WHERE due<=? ORDER BY due LIMIT 8",
                    Some(vec![rivetkit::BindParam::Integer(now_ms())]),
                )
                .await?;
            let mut jobs = Vec::new();
            let mut state_bytes = 0_i64;
            for row in &rows.rows {
                let Some(rivetkit::ColumnValue::Integer(bytes)) = row.get(1) else {
                    return Err(anyhow!("Missing subscription state size"));
                };
                // Bound aggregate rewrites within the transaction budget.
                // Always process one row so a large subscription cannot starve.
                if state_bytes > 0 && state_bytes.saturating_add(*bytes) > 8 * 1024 * 1024 {
                    break;
                }
                state_bytes = state_bytes.saturating_add(*bytes);
                let mut sub = db::load(&tx, db::string(row, 0)?).await?.unwrap();
                sub.expire(now_ms());
                if let Some(w) = sub.wake.as_mut()
                    && w.next_attempt_at.is_some_and(|n| n <= now_ms())
                    && w.dispatch_until.is_none_or(|d| d <= now_ms())
                {
                    w.dispatch_until = Some(now_ms() + DISPATCH_TIMEOUT);
                    jobs.push(sub.clone());
                }
                db::save(&tx, &sub).await?;
            }
            tx.commit().await?;
            jobs
        };
        if !jobs.is_empty() {
            fault::checkpoint("after_wake_persist");
        }
        if !jobs.is_empty() {
            stream::iter(jobs)
                .for_each_concurrent(8, |sub| {
                    let ctx = &ctx;
                    let this = &self;
                    async move {
                        let result = tokio::time::timeout(
                            Duration::from_millis(DISPATCH_TIMEOUT as u64 - 1_000),
                            this.deliver(ctx, &sub),
                        )
                        .await
                        .unwrap_or_else(|_| Err(anyhow!("Delivery attempt timed out")));
                        if let Err(error) = this.delivered(ctx, &sub, result).await {
                            tracing::warn!(?error, "Failed to commit subscription delivery result");
                        }
                    }
                })
                .await;
        }
        let rows = ctx
            .sql()
            .query(
                "SELECT MIN(due) FROM subscriptions WHERE due IS NOT NULL",
                None,
            )
            .await?;
        if let Some(rivetkit::ColumnValue::Integer(due)) = rows.rows.first().and_then(|r| r.first())
        {
            self.arm(&ctx, (*due).max(now_ms() + 10)).await?;
        }
        if !ctx
            .sql()
            .query("SELECT 1 FROM subscription_fanout LIMIT 1", None)
            .await?
            .rows
            .is_empty()
        {
            self.arm(&ctx, now_ms() + 10).await?;
        }
        Ok(())
    }

    async fn deliver(&self, ctx: &Ctx<Self>, sub: &Subscription) -> Result<bool> {
        let w = sub.wake.as_ref().unwrap();
        if let Some(webhook) = &sub.config.webhook {
            let token = self
                .webhook_capability(ctx, sub)
                .await
                .map_err(api_failure)?;
            let body = webhook_body(sub, &token)?;
            let signature = self.engine.sign(body.clone()).await?;
            fault::pause("after_webhook_signing").await;
            // Signing may outlive a lease or race deletion/recreation. Never deliver
            // a result for a wake that is already fenced.
            self.check_dispatch(ctx, sub).await?;
            // Persisted subscriptions cannot grant the development exception: it
            // is a deployment setting and must be consistent across actor runners.
            let local = std::env::var("DS_SUBSCRIPTIONS_ALLOW_LOCAL_HTTP").is_ok_and(|s| s == "1");
            return security::deliver(
                &webhook.url,
                local,
                signature.header(),
                body,
                ctx.inner().client_endpoint(),
                ctx.inner().client_token(),
            )
            .await;
        }
        let target = format!("{}/{}", sub.root, sub.config.wake_stream.as_ref().unwrap());
        let actor = self.client.get(
            crate::actor::ACTOR_NAME,
            vec![crate::facade::stream_actor_key(
                &ctx.state().tenant_scope,
                &target,
            )],
            GetOptions::default(),
        )?;
        let event = json!({"type":"wake","subscription_id":sub.id,"stream":w.snapshot.iter().find(|s|s.info.has_pending).map(|s|&s.info.path),"generation":w.generation,"ts":w.issued_at});
        let value=tokio::time::timeout(Duration::from_secs(10),actor.action("__durableStreamAppendWake",vec![json!({"wake_id":w.id,"event":event,"origin_sub_key":db::key(&sub.root,&sub.id)})])).await??;
        let status = value
            .as_u64()
            .ok_or_else(|| anyhow!("Wake append omitted status"))?;
        if !(200..300).contains(&status) {
            return Err(anyhow!("Wake stream append HTTP {status}"));
        }
        Ok(false)
    }
    async fn delivered(
        &self,
        ctx: &Ctx<Self>,
        sent: &Subscription,
        result: Result<bool>,
    ) -> Result<()> {
        let _guard = self.lock.lock().await;
        let tx = db::begin(ctx.sql()).await?;
        let Some(mut sub) = db::load(&tx, &db::key(&sent.root, &sent.id)).await? else {
            tx.rollback().await?;
            return Ok(());
        };
        sub.expire(now_ms());
        let same = sub
            .wake
            .as_ref()
            .zip(sent.wake.as_ref())
            .is_some_and(|(a, b)| a.id == b.id && a.generation == b.generation);
        if !sub.deleted && same {
            match result {
                Ok(true) => {
                    sub.finish(now_ms(), true);
                }
                Ok(false) => {
                    let w = sub.wake.as_mut().unwrap();
                    w.delivery_error = None;
                    w.published = true;
                    w.attempts = 0;
                    w.next_attempt_at = None;
                    w.dispatch_until = None;
                }
                Err(error) => {
                    let w = sub.wake.as_mut().unwrap();
                    w.attempts = w.attempts.saturating_add(1);
                    let engine = error.downcast_ref::<EngineError>();
                    let api = error.downcast_ref::<ApiError>();
                    let status = engine.map(|e| e.status).or_else(|| api.map(|e| e.status));
                    let code = engine
                        .map(|e| e.code)
                        .or_else(|| api.and_then(|e| e.body["error"]["code"].as_str()))
                        .unwrap_or("WEBHOOK_DELIVERY_FAILED");
                    let retry_after = engine
                        .and_then(|e| e.retry_after)
                        .or_else(|| api.and_then(|e| e.retry_after));
                    let permanent = matches!(status, Some(400 | 401 | 403 | 413));
                    let delay = retry_delay(w.attempts)
                        .max(retry_after.unwrap_or(0).min((i64::MAX / 1000) as u64) as i64 * 1_000);
                    w.next_attempt_at = (!permanent).then(|| now_ms().saturating_add(delay));
                    w.delivery_error =
                        Some(json!({"code":code,"status":status,"retry_at":w.next_attempt_at}));
                    if permanent {
                        w.deadline = None;
                    }
                    w.dispatch_until = None;
                    // Retries retain identity until lease expiry; expiry preserves
                    // next_attempt_at and attempts in the next fenced generation.
                }
            }
        }
        db::save(&tx, &sub).await?;
        tx.commit().await
    }
}
fn added_streams(body: &Value) -> ApiResult<Vec<String>> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Add {
        streams: Vec<String>,
    }
    let add: Add = serde_json::from_value(body.clone())
        .map_err(|_| ApiError::invalid("Expected streams array"))?;
    if add.streams.len() > 4096 {
        return Err(ApiError::invalid("Too many streams"));
    }
    add.streams
        .iter()
        .map(|path| normalize_path(path, false))
        .collect()
}

fn retry_delay(attempt: u32) -> i64 {
    let base = (1_000_i64.saturating_mul(1_i64 << attempt.saturating_sub(1).min(6))).min(60_000);
    let random = uuid::Uuid::new_v4().as_u128() as u64;
    (base * 80 / 100 + (random % (base as u64 * 40 / 100 + 1)) as i64).min(60_000)
}
fn internal(error: anyhow::Error) -> ApiError {
    let error = match error.downcast::<ApiError>() {
        Ok(error) => return error,
        Err(error) => error,
    };
    tracing::error!(?error, "Subscription storage failure");
    ApiError::new(500, "INTERNAL", "Subscription storage failure")
}
pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
fn url_component(s: &str) -> String {
    url::form_urlencoded::byte_serialize(s.as_bytes())
        .collect::<String>()
        .replace('+', "%20")
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod signing_tests;
