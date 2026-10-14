//! Rivet-specific discovery that materializes ordinary protocol subscriptions.
//! The catalog transaction records routing jobs before acknowledging a stream write.
use super::*;
use anyhow::ensure;
use std::collections::{BTreeMap, BTreeSet};
use urlpattern::{UrlPattern, UrlPatternInit, UrlPatternMatchInput};

const REFRESH_MS: i64 = 5_000;

#[derive(Clone, Serialize, Deserialize)]
pub struct Configure {
    pub root: String,
    pub public_base: String,
}
impl Action for Configure {
    type Output = ();
    const NAME: &'static str = "__durableStreamConfigureActorRouting";
}
impl Handles<Configure> for SubscriptionActor {
    type Future = BoxFuture<()>;
    fn handle(self: Arc<Self>, ctx: Ctx<Self>, config: Configure) -> Self::Future {
        Box::pin(async move {
            let url = url::Url::parse(&config.public_base)?;
            ensure!(
                matches!(url.scheme(), "http" | "https")
                    && url.host_str().is_some()
                    && url.username().is_empty()
                    && url.password().is_none()
                    && url.query().is_none()
                    && url.fragment().is_none()
                    && url.path().trim_end_matches('/') == config.root,
                "Invalid actor routing mount"
            );
            {
                let _lock = self.lock.lock().await;
                ctx.sql().execute("INSERT INTO actor_route_mounts(root,public_base) VALUES(?,?) ON CONFLICT(root) DO UPDATE SET public_base=excluded.public_base", Some(vec![text(config.root), text(config.public_base)])).await?;
            }
            *self.routes_refreshed.lock().await = 0;
            self.refresh_actor_routes(&ctx).await?;
            self.kick(&ctx);
            Ok(())
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
enum Destination {
    ActorId {
        #[serde(rename = "actorName")]
        actor_name: String,
        id: String,
    },
    ActorKey {
        #[serde(rename = "actorName")]
        actor_name: String,
        key: Vec<String>,
    },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Rule {
    pool: String,
    name: String,
    stream: String,
    destination: Destination,
}
impl Rule {
    fn actor_name(&self) -> &str {
        match &self.destination {
            Destination::ActorId { actor_name, .. } | Destination::ActorKey { actor_name, .. } => {
                actor_name
            }
        }
    }
    fn key(&self) -> Result<String> {
        Ok(serde_json::to_string(&(
            &self.pool,
            self.actor_name(),
            &self.name,
        ))?)
    }
    fn pattern(&self) -> Result<UrlPattern> {
        ensure!(
            self.stream.starts_with('/') && self.stream.len() <= 2048,
            "Actor stream pattern must be an absolute pathname of at most 2048 bytes"
        );
        Ok(UrlPattern::parse(
            UrlPatternInit {
                pathname: Some(self.stream.clone()),
                ..Default::default()
            },
            Default::default(),
        )?)
    }
    fn validate(&self, actor: &str) -> Result<()> {
        ensure!(
            self.actor_name() == actor && !self.pool.is_empty(),
            "Actor route ownership mismatch"
        );
        ensure!(
            !self.name.is_empty()
                && self.name.len() <= 128
                && self
                    .name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-'),
            "Invalid actor subscription name"
        );
        ensure!(
            matches!(
                (&*self.name, &self.destination),
                ("primary", Destination::ActorId { .. })
            ) || (self.name != "primary"
                && matches!(self.destination, Destination::ActorKey { .. })),
            "Invalid actor route destination"
        );
        let pattern = self.pattern()?;
        let parts = match &self.destination {
            Destination::ActorId { id, .. } => std::slice::from_ref(id),
            Destination::ActorKey { key, .. } => key,
        };
        for part in parts {
            if let Some(capture) = part.strip_prefix(':') {
                ensure!(
                    pattern
                        .pathname
                        .group_name_list
                        .iter()
                        .any(|n| n == capture),
                    "Unknown actor key capture"
                );
            }
        }
        Ok(())
    }
    fn resolve(&self, path: &str) -> Result<Option<Vec<String>>> {
        let Some(result) = self
            .pattern()?
            .exec(UrlPatternMatchInput::Init(UrlPatternInit {
                pathname: Some(format!("/{}", path.trim_start_matches('/'))),
                ..Default::default()
            }))?
        else {
            return Ok(None);
        };
        let parts = match &self.destination {
            Destination::ActorId { id, .. } => std::slice::from_ref(id),
            Destination::ActorKey { key, .. } => key,
        };
        parts
            .iter()
            .map(|part| match part.strip_prefix(':') {
                Some(name) => result
                    .pathname
                    .groups
                    .get(name)
                    .and_then(Clone::clone)
                    .ok_or_else(|| anyhow!("Missing actor key capture")),
                None => Ok(part.clone()),
            })
            .collect::<Result<Vec<_>>>()
            .map(Some)
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Job {
    rule: Rule,
    root: String,
    public_base: String,
    before: String,
    entry: CatalogEntry,
}

pub async fn ensure(db: &rivetkit::SqliteDb) -> Result<()> {
    for sql in [
        "CREATE TABLE IF NOT EXISTS actor_route_backfill(rule_key TEXT PRIMARY KEY,cursor TEXT NOT NULL)",
        "CREATE TABLE IF NOT EXISTS actor_route_destinations(rule TEXT NOT NULL,captures TEXT NOT NULL,actor_id TEXT NOT NULL,PRIMARY KEY(rule,captures))",
        "CREATE INDEX IF NOT EXISTS actor_route_destinations_actor ON actor_route_destinations(actor_id)",
        "CREATE TABLE IF NOT EXISTS actor_route_mounts(root TEXT PRIMARY KEY,public_base TEXT NOT NULL)",
        "CREATE TABLE IF NOT EXISTS actor_routes(key TEXT PRIMARY KEY,value TEXT NOT NULL UNIQUE)",
        "CREATE TABLE IF NOT EXISTS actor_route_jobs(rule_key TEXT NOT NULL,path TEXT NOT NULL,value TEXT NOT NULL,PRIMARY KEY(rule_key,path))",
        "CREATE TABLE IF NOT EXISTS actor_route_links(sub_key TEXT NOT NULL,path TEXT NOT NULL,rule TEXT NOT NULL,PRIMARY KEY(sub_key,path))",
        "CREATE INDEX IF NOT EXISTS actor_route_links_rule ON actor_route_links(rule)",
    ] {
        db.exec(sql).await?;
    }
    Ok(())
}

pub async fn enqueue(tx: &rivetkit::SqliteTransaction, entry: &CatalogEntry) -> Result<()> {
    let old = db::catalog(tx, &entry.path).await?;
    if old.as_ref().is_some_and(|e| e.version >= entry.version) || !entry.exists {
        return Ok(());
    }
    let before = old
        .as_ref()
        .filter(|e| e.incarnation == entry.incarnation)
        .map_or(&entry.start, |e| &e.tail);
    let mounts = tx
        .execute("SELECT root,public_base FROM actor_route_mounts", None)
        .await?;
    if mounts.rows.is_empty() {
        return Ok(());
    }
    let rules = tx
        .execute("SELECT key,value FROM actor_routes", None)
        .await?;
    for mount in &mounts.rows {
        let root = db::string(mount, 0)?;
        let Some(relative) = entry
            .path
            .strip_prefix(root)
            .and_then(|p| p.strip_prefix('/'))
        else {
            continue;
        };
        for row in &rules.rows {
            let rule: Rule = serde_json::from_str(db::string(row, 1)?)?;
            if rule.resolve(relative)?.is_none() {
                continue;
            }
            let key = db::string(row, 0)?;
            let prior = tx
                .execute(
                    "SELECT value FROM actor_route_jobs WHERE rule_key=? AND path=?",
                    Some(vec![text(key), text(&entry.path)]),
                )
                .await?;
            let prior: Option<Job> = prior
                .rows
                .first()
                .map(|r| Ok::<_, anyhow::Error>(serde_json::from_str(db::string(r, 0)?)?))
                .transpose()?;
            let before = prior
                .filter(|p| p.rule == rule && p.entry.incarnation == entry.incarnation)
                .map_or_else(|| before.clone(), |p| p.before);
            let mut snapshot = entry.clone();
            snapshot.wakes.clear();
            let job = Job {
                rule,
                root: root.into(),
                public_base: db::string(mount, 1)?.into(),
                before,
                entry: snapshot,
            };
            tx.execute("INSERT INTO actor_route_jobs(rule_key,path,value) VALUES(?,?,?) ON CONFLICT(rule_key,path) DO UPDATE SET value=excluded.value",Some(vec![text(key),text(&entry.path),text(serde_json::to_string(&job)?)])).await?;
        }
    }
    Ok(())
}

impl SubscriptionActor {
    /// Authorize only integration-owned memberships, against the current catalog
    /// and a destination previously verified by Engine (namespace, name, pool).
    pub(super) async fn authorized_actor_wake(
        &self,
        ctx: &Ctx<Self>,
        sub: &Subscription,
    ) -> Result<Option<String>> {
        let Some(webhook) = &sub.config.webhook else {
            return Ok(None);
        };
        let _lock = self.lock.lock().await;
        let rows = ctx.sql().query("SELECT l.path,l.rule FROM actor_route_links l JOIN actor_routes r ON r.value=l.rule WHERE l.sub_key=?", Some(vec![text(db::key(&sub.root,&sub.id))])).await?;
        if rows.rows.is_empty() {
            return Ok(None);
        }
        ensure!(
            rows.rows.len() == sub.links.len(),
            "Actor wake has unowned stream memberships"
        );
        let mut destination: Option<String> = None;
        for row in &rows.rows {
            let path = db::string(row, 0)?;
            let rule_value = db::string(row, 1)?;
            ensure!(
                sub.links.contains_key(path),
                "Actor wake membership mismatch"
            );
            let rule: Rule = serde_json::from_str(rule_value)?;
            let captures = rule
                .resolve(path)?
                .ok_or_else(|| anyhow!("Actor wake pattern mismatch"))?;
            let rows = ctx
                .sql()
                .query(
                    "SELECT actor_id FROM actor_route_destinations WHERE rule=? AND captures=?",
                    Some(vec![
                        text(rule_value),
                        text(serde_json::to_string(&captures)?),
                    ]),
                )
                .await?;
            let id = if let Some(row) = rows.rows.first() {
                db::string(row, 0)?.to_owned()
            } else {
                // Upgrade pre-cache subscriptions by revalidating their declared
                // destination. Never infer trust from a stored webhook URL.
                let resolved = match &rule.destination {
                    Destination::ActorId { actor_name, .. } => {
                        self.engine
                            .route_actor(actor_name, &rule.pool, Some(&captures[0]), None)
                            .await?
                    }
                    Destination::ActorKey { actor_name, .. } => {
                        self.engine
                            .route_actor(actor_name, &rule.pool, None, Some(&captures))
                            .await?
                    }
                }
                .ok_or_else(|| anyhow!("Actor destination has not been verified"))?;
                ensure!(
                    webhook.url == self.engine.actor_wake_url(&resolved),
                    "Actor wake URL does not match its declared destination"
                );
                ctx.sql().execute("INSERT INTO actor_route_destinations(rule,captures,actor_id) VALUES(?,?,?) ON CONFLICT(rule,captures) DO NOTHING", Some(vec![text(rule_value),text(serde_json::to_string(&captures)?),text(&resolved)])).await?;
                resolved
            };
            ensure!(
                destination.as_deref().is_none_or(|old| old == id),
                "Actor wake mixes destinations"
            );
            ensure!(
                webhook.url == self.engine.actor_wake_url(&id),
                "Actor wake URL does not match its declared destination"
            );
            let expected_sub = if rule.name == "primary" {
                format!("actor-{id}")
            } else {
                format!("actor-{id}-{}", rule.name)
            };
            ensure!(sub.id == expected_sub, "Actor wake handler mismatch");
            destination = Some(id);
        }
        Ok(destination)
    }

    /// A webhook 404 alone can be an application routing error. Confirm absence
    /// with Engine before invalidating the durable destination cache.
    pub(super) async fn reconcile_missing_destination(
        &self,
        ctx: &Ctx<Self>,
        sub: &Subscription,
        id: &str,
    ) -> Result<()> {
        let rows = ctx
            .sql()
            .query(
                "SELECT rule FROM actor_route_links WHERE sub_key=? LIMIT 1",
                Some(vec![text(db::key(&sub.root, &sub.id))]),
            )
            .await?;
        let Some(row) = rows.rows.first() else {
            return Ok(());
        };
        let rule: Rule = serde_json::from_str(db::string(row, 0)?)?;
        if self
            .engine
            .route_actor(rule.actor_name(), &rule.pool, Some(id), None)
            .await?
            .is_some()
        {
            return Ok(());
        }
        let _lock = self.lock.lock().await;
        let tx = db::begin(ctx.sql()).await?;
        let sub_key = db::key(&sub.root, &sub.id);
        let Some(mut current) = db::load(&tx, &sub_key).await? else {
            tx.rollback().await?;
            return Ok(());
        };
        // Do not invalidate a replacement configured during Engine I/O.
        if current.config.webhook != sub.config.webhook {
            tx.rollback().await?;
            return Ok(());
        }
        let rows = tx.execute("SELECT l.path,l.rule FROM actor_route_links l JOIN actor_routes r ON r.value=l.rule WHERE l.sub_key=?",Some(vec![text(&sub_key)])).await?;
        for row in &rows.rows {
            let relative = db::string(row, 0)?;
            let rule: Rule = serde_json::from_str(db::string(row, 1)?)?;
            if matches!(rule.destination, Destination::ActorKey { .. }) {
                let path = format!("{}/{}", sub.root, relative);
                if let Some(entry) = db::catalog(&tx, &path).await? {
                    if let Some(link) = current.links.get(relative) {
                        let job = Job {
                            rule: rule.clone(),
                            root: sub.root.clone(),
                            public_base: sub.public_base.clone(),
                            before: link.acked.clone(),
                            entry,
                        };
                        tx.execute("INSERT INTO actor_route_jobs(rule_key,path,value) VALUES(?,?,?) ON CONFLICT(rule_key,path) DO UPDATE SET value=excluded.value",Some(vec![text(rule.key()?),text(path),text(serde_json::to_string(&job)?)])).await?;
                    }
                }
            }
        }
        tx.execute(
            "DELETE FROM actor_route_destinations WHERE actor_id=?",
            Some(vec![text(id)]),
        )
        .await?;
        tx.execute(
            "DELETE FROM actor_route_links WHERE sub_key=?",
            Some(vec![text(&sub_key)]),
        )
        .await?;
        current.deleted = true;
        current.wake = None;
        current.generation += 1;
        current.links.clear();
        current.config.streams.clear();
        current.config_hash = current.config.hash();
        db::sync_links(&tx, &current).await?;
        db::save(&tx, &current).await?;
        tx.commit().await?;
        self.arm(ctx, now_ms() + 10).await?;
        Ok(())
    }
    pub(super) async fn refresh_actor_routes(&self, ctx: &Ctx<Self>) -> Result<()> {
        let mut refreshed = self.routes_refreshed.lock().await;
        if *refreshed + REFRESH_MS > now_ms() {
            self.arm(ctx, *refreshed + REFRESH_MS).await?;
            return Ok(());
        }
        if ctx
            .sql()
            .query("SELECT root FROM actor_route_mounts LIMIT 1", None)
            .await?
            .rows
            .is_empty()
        {
            return Ok(());
        }
        self.arm(ctx, now_ms() + REFRESH_MS).await?;
        let mut cursor = None;
        let mut seen = BTreeSet::new();
        let mut rules = BTreeMap::new();
        loop {
            let page = self.engine.actor_names_page(cursor.as_deref()).await?;
            let names = page["names"]
                .as_object()
                .ok_or_else(|| anyhow!("Invalid Engine actor names response"))?;
            for (actor, data) in names {
                let Some(sources) = data["metadata"]["streamSubscriptionSources"].as_object()
                else {
                    continue;
                };
                for (pool, source) in sources {
                    // Conflicts remove this source from the adopted catalog.
                    // Aborting refresh would leave its old routes active.
                    if source["conflict"] == true {
                        continue;
                    }
                    let subscriptions = source["subscriptions"]
                        .as_object()
                        .ok_or_else(|| anyhow!("Invalid actor subscriptions"))?;
                    for (name, declaration) in subscriptions {
                        let rule = Rule {
                            pool: pool.clone(),
                            name: name.clone(),
                            stream: declaration["stream"]
                                .as_str()
                                .ok_or_else(|| anyhow!("Invalid stream pattern"))?
                                .into(),
                            destination: serde_json::from_value(
                                declaration["destination"].clone(),
                            )?,
                        };
                        rule.validate(actor)?;
                        rules.insert(rule.key()?, serde_json::to_string(&rule)?);
                        ensure!(
                            rules.len() <= 1024,
                            "Actor route catalog exceeds 1024 rules"
                        );
                    }
                }
            }
            let next = page["pagination"]["cursor"].as_str().map(str::to_owned);
            if names.is_empty() || next.is_none() {
                break;
            }
            ensure!(
                seen.insert(next.clone().unwrap()),
                "Engine actor names cursor did not advance"
            );
            cursor = next;
        }
        let _lock = self.lock.lock().await;
        let tx = db::begin(ctx.sql()).await?;
        // Catalog replacement is atomic. Jobs carry the rule used at acceptance;
        // changed/removed rules are fenced before they can materialize consumers.
        let previous = tx
            .execute("SELECT key,value FROM actor_routes", None)
            .await?;
        let previous = previous
            .rows
            .iter()
            .map(|row| {
                Ok::<_, anyhow::Error>((
                    db::string(row, 0)?.to_owned(),
                    db::string(row, 1)?.to_owned(),
                ))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        for (key, value) in &rules {
            let rule: Rule = serde_json::from_str(value)?;
            if rule.name == "primary" && previous.get(key) != Some(value) {
                tx.execute("INSERT INTO actor_route_backfill(rule_key,cursor) VALUES(?,'') ON CONFLICT(rule_key) DO UPDATE SET cursor=''",Some(vec![text(key)])).await?;
            }
        }
        tx.execute("DELETE FROM actor_routes", None).await?;
        for (key, value) in rules {
            tx.execute(
                "INSERT INTO actor_routes(key,value) VALUES(?,?)",
                Some(vec![text(key), text(value)]),
            )
            .await?;
        }
        tx.execute("DELETE FROM actor_route_destinations WHERE rule NOT IN (SELECT value FROM actor_routes)", None).await?;
        tx.commit().await?;
        *refreshed = now_ms();
        drop(_lock);
        self.arm(ctx, *refreshed + REFRESH_MS).await?;
        Ok(())
    }

    async fn backfill_primary_routes(&self, ctx: &Ctx<Self>) -> Result<()> {
        let _lock = self.lock.lock().await;
        let tx = db::begin(ctx.sql()).await?;
        let jobs=tx.execute("SELECT b.rule_key,b.cursor,r.value FROM actor_route_backfill b LEFT JOIN actor_routes r ON r.key=b.rule_key LIMIT 1",None).await?;
        let Some(job) = jobs.rows.first() else {
            tx.rollback().await?;
            return Ok(());
        };
        let key = db::string(job, 0)?;
        let cursor = db::string(job, 1)?;
        if matches!(job.get(2), Some(rivetkit::ColumnValue::Null)) {
            tx.execute(
                "DELETE FROM actor_route_backfill WHERE rule_key=?",
                Some(vec![text(key)]),
            )
            .await?;
        } else {
            let rule: Rule = serde_json::from_str(db::string(job, 2)?)?;
            let entries=tx.execute("SELECT path,value FROM subscription_catalog WHERE path>? ORDER BY path LIMIT 32",Some(vec![text(cursor)])).await?;
            let mounts = tx
                .execute("SELECT root,public_base FROM actor_route_mounts", None)
                .await?;
            for row in &entries.rows {
                let mut entry: CatalogEntry = serde_json::from_str(db::string(row, 1)?)?;
                if !entry.exists {
                    continue;
                }
                entry.wakes.clear();
                for mount in &mounts.rows {
                    let root = db::string(mount, 0)?;
                    let Some(relative) = entry
                        .path
                        .strip_prefix(root)
                        .and_then(|p| p.strip_prefix('/'))
                    else {
                        continue;
                    };
                    if rule.resolve(relative)?.is_none() {
                        continue;
                    }
                    let job = Job {
                        rule: rule.clone(),
                        root: root.into(),
                        public_base: db::string(mount, 1)?.into(),
                        before: entry.start.clone(),
                        entry: entry.clone(),
                    };
                    tx.execute(
                        "INSERT OR IGNORE INTO actor_route_jobs(rule_key,path,value) VALUES(?,?,?)",
                        Some(vec![
                            text(key),
                            text(&entry.path),
                            text(serde_json::to_string(&job)?),
                        ]),
                    )
                    .await?;
                }
            }
            if let Some(last) = entries.rows.last() {
                tx.execute(
                    "UPDATE actor_route_backfill SET cursor=? WHERE rule_key=?",
                    Some(vec![text(db::string(last, 0)?), text(key)]),
                )
                .await?;
            } else {
                tx.execute(
                    "DELETE FROM actor_route_backfill WHERE rule_key=?",
                    Some(vec![text(key)]),
                )
                .await?;
            }
        }
        tx.commit().await?;
        self.arm(ctx, now_ms() + 10).await?;
        Ok(())
    }

    pub(super) async fn reconcile_actor_routes(&self, ctx: &Ctx<Self>) -> Result<()> {
        // Remove only memberships owned by this integration. Invalidate in-flight
        // snapshots before allowing normal subscription delivery to proceed.
        loop {
            let _lock = self.lock.lock().await;
            let tx = db::begin(ctx.sql()).await?;
            let stale=tx.execute("SELECT l.sub_key,l.path FROM actor_route_links l LEFT JOIN actor_routes r ON r.value=l.rule WHERE r.key IS NULL LIMIT 32",None).await?;
            if stale.rows.is_empty() {
                tx.rollback().await?;
                break;
            }
            for row in &stale.rows {
                let key = db::string(row, 0)?;
                let path = db::string(row, 1)?;
                if let Some(mut sub) = db::load(&tx, key).await? {
                    sub.links.remove(path);
                    sub.config.streams.retain(|p| p != path);
                    sub.config_hash = sub.config.hash();
                    sub.wake = None;
                    sub.generation += 1;
                    sub.ensure_wake(now_ms());
                    db::sync_links(&tx, &sub).await?;
                    db::save(&tx, &sub).await?;
                }
                tx.execute(
                    "DELETE FROM actor_route_links WHERE sub_key=? AND path=?",
                    Some(vec![text(key), text(path)]),
                )
                .await?;
            }
            tx.commit().await?;
        }
        self.backfill_primary_routes(ctx).await?;
        for _ in 0..8 {
            let job = {
                let _lock = self.lock.lock().await;
                let rows = ctx
                    .sql()
                    .query(
                        "SELECT value FROM actor_route_jobs ORDER BY rule_key,path LIMIT 1",
                        None,
                    )
                    .await?;
                rows.rows
                    .first()
                    .map(|r| {
                        Ok::<_, anyhow::Error>(serde_json::from_str::<Job>(db::string(r, 0)?)?)
                    })
                    .transpose()?
            };
            let Some(job) = job else { return Ok(()) };
            let key = job.rule.key()?;
            let rule_value = serde_json::to_string(&job.rule)?;
            let active = ctx
                .sql()
                .query(
                    "SELECT key FROM actor_routes WHERE key=? AND value=?",
                    Some(vec![text(&key), text(&rule_value)]),
                )
                .await?;
            let relative = job
                .entry
                .path
                .strip_prefix(&format!("{}/", job.root))
                .ok_or_else(|| anyhow!("Invalid route job path"))?;
            let resolved = job
                .rule
                .resolve(relative)?
                .ok_or_else(|| anyhow!("Invalid route job pattern"))?;
            let captures = serde_json::to_string(&resolved)?;
            let cached = ctx
                .sql()
                .query(
                    "SELECT actor_id FROM actor_route_destinations WHERE rule=? AND captures=?",
                    Some(vec![text(&rule_value), text(&captures)]),
                )
                .await?;
            let cached = cached
                .rows
                .first()
                .map(|r| db::string(r, 0).map(str::to_owned))
                .transpose()?;
            let actor_id = if active.rows.is_empty() {
                None
            } else if let Some(id) = cached {
                Some(id)
            } else {
                match &job.rule.destination {
                    Destination::ActorId { actor_name, .. } => {
                        self.engine
                            .route_actor(actor_name, &job.rule.pool, Some(&resolved[0]), None)
                            .await?
                    }
                    Destination::ActorKey { actor_name, .. } => {
                        self.engine
                            .route_actor(actor_name, &job.rule.pool, None, Some(&resolved))
                            .await?
                    }
                }
            };
            let _lock = self.lock.lock().await;
            let tx = db::begin(ctx.sql()).await?;
            // Metadata refresh or another append may have happened during Engine I/O.
            let active = tx
                .execute(
                    "SELECT key FROM actor_routes WHERE key=? AND value=?",
                    Some(vec![text(&key), text(&rule_value)]),
                )
                .await?;
            if let Some(actor_id) = actor_id.filter(|_| !active.rows.is_empty()) {
                tx.execute("INSERT INTO actor_route_destinations(rule,captures,actor_id) VALUES(?,?,?) ON CONFLICT(rule,captures) DO UPDATE SET actor_id=excluded.actor_id", Some(vec![text(&rule_value),text(&captures),text(&actor_id)])).await?;
                let id = if job.rule.name == "primary" {
                    format!("actor-{actor_id}")
                } else {
                    format!("actor-{actor_id}-{}", job.rule.name)
                };
                let sub_key = db::key(&job.root, &id);
                let url = self.engine.actor_wake_url(&actor_id);
                let config = Config {
                    kind: DeliveryType::Webhook,
                    pattern: None,
                    streams: vec![],
                    webhook: Some(Webhook { url }),
                    wake_stream: None,
                    lease_ttl_ms: 60_000,
                    description: None,
                };
                let mut sub = db::load(&tx, &sub_key).await?.unwrap_or_else(|| {
                    Subscription::new(
                        id,
                        job.root.clone(),
                        job.public_base.clone(),
                        config.clone(),
                        0,
                        now_ms(),
                    )
                });
                ensure!(
                    sub.config.kind == config.kind
                        && sub.config.webhook == config.webhook
                        && sub.config.pattern.is_none(),
                    "Actor subscription conflicts with an existing consumer"
                );
                for path in sub.links.keys() {
                    ensure!(
                        job.rule.resolve(path)?.as_ref() == Some(&resolved),
                        "Existing consumer has memberships outside the actor route"
                    );
                }
                sub.deleted = false;
                let entry = db::catalog(&tx, &job.entry.path)
                    .await?
                    .unwrap_or(job.entry.clone());
                if !sub.links.contains_key(relative) {
                    sub.link(relative, Some(&entry), true, false);
                    let link = sub.links.get_mut(relative).unwrap();
                    if entry.incarnation == job.entry.incarnation {
                        link.acked = job.before.clone();
                    }
                }
                sub.observe(&entry);
                sub.config.streams = sub.links.keys().cloned().collect();
                sub.config_hash = sub.config.hash();
                sub.ensure_wake(now_ms());
                db::sync_links(&tx, &sub).await?;
                db::save(&tx, &sub).await?;
                tx.execute("INSERT INTO actor_route_links(sub_key,path,rule) VALUES(?,?,?) ON CONFLICT(sub_key,path) DO UPDATE SET rule=excluded.rule",Some(vec![text(&sub_key),text(relative),text(&rule_value)])).await?;
            } else if !active.rows.is_empty()
                && matches!(job.rule.destination, Destination::ActorId { .. })
            {
                // Another subscription may already have evicted this actor's
                // cache. A confirmed missing ID still retires its primary
                // membership, even when there is no cached wake to deliver.
                let sub_key = db::key(&job.root, &format!("actor-{}", resolved[0]));
                let owned = tx
                    .execute(
                        "SELECT path FROM actor_route_links WHERE sub_key=? AND rule=? LIMIT 1",
                        Some(vec![text(&sub_key), text(&rule_value)]),
                    )
                    .await?;
                if !owned.rows.is_empty() {
                    if let Some(mut sub) = db::load(&tx, &sub_key).await? {
                        sub.deleted = true;
                        sub.wake = None;
                        sub.generation += 1;
                        sub.links.clear();
                        sub.config.streams.clear();
                        sub.config_hash = sub.config.hash();
                        tx.execute(
                            "DELETE FROM actor_route_links WHERE sub_key=?",
                            Some(vec![text(&sub_key)]),
                        )
                        .await?;
                        db::sync_links(&tx, &sub).await?;
                        db::save(&tx, &sub).await?;
                    }
                }
            }
            // A later coalesced append is already represented by the latest catalog
            // read above, but retain it if the rule changed while resolution ran.
            tx.execute("DELETE FROM actor_route_jobs WHERE rule_key=? AND path=? AND json_extract(value,'$.rule')=json(?)",Some(vec![text(&key),text(&job.entry.path),text(&rule_value)])).await?;
            tx.commit().await?;
        }
        self.arm(ctx, now_ms() + 10).await?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../tests/unit/actor_routing.rs"]
mod tests;
