use anyhow::{Result, anyhow};
use rivetkit::{BindParam, ColumnValue, SqliteDb, SqliteTransaction};

use super::{
    legacy::{LegacyKeys, TOKEN_RETENTION_MS},
    model::{CatalogEntry, Subscription},
};

pub async fn begin(db: &SqliteDb) -> Result<SqliteTransaction> {
    // Maximum snapshots require hundreds of bounded Engine RPCs; a replacement
    // takes about seven seconds locally. Keep ordinary stream writes at five.
    db.begin_transaction(Some(std::time::Duration::from_secs(10)))
        .await
}

pub async fn ensure(db: &SqliteDb) -> Result<()> {
    for sql in [
        "CREATE TABLE IF NOT EXISTS subscription_keys (id INTEGER PRIMARY KEY CHECK(id=1), value TEXT NOT NULL)",
        "CREATE TABLE IF NOT EXISTS subscription_signing_state (id INTEGER PRIMARY KEY CHECK(id=1), cutover INTEGER NOT NULL)",
        "CREATE TABLE IF NOT EXISTS subscription_catalog (path TEXT PRIMARY KEY, version INTEGER NOT NULL, value TEXT NOT NULL)",
        "CREATE TABLE IF NOT EXISTS subscriptions (key TEXT PRIMARY KEY, root TEXT NOT NULL, pattern TEXT, due INTEGER, value TEXT NOT NULL)",
        "CREATE INDEX IF NOT EXISTS subscriptions_due ON subscriptions(due) WHERE due IS NOT NULL",
        "CREATE INDEX IF NOT EXISTS subscriptions_pattern_keys ON subscriptions(root,key) WHERE pattern IS NOT NULL",
        "CREATE TABLE IF NOT EXISTS subscription_links (path TEXT NOT NULL, sub_key TEXT NOT NULL, PRIMARY KEY(path,sub_key))",
        "CREATE INDEX IF NOT EXISTS subscription_links_sub ON subscription_links(sub_key)",
        "CREATE TABLE IF NOT EXISTS subscription_fanout (path TEXT PRIMARY KEY, version INTEGER NOT NULL, cursor TEXT NOT NULL, discover INTEGER NOT NULL, value TEXT NOT NULL)",
        "CREATE TABLE IF NOT EXISTS subscription_catalog_status (id INTEGER PRIMARY KEY CHECK(id=1), ready INTEGER NOT NULL)",
        "INSERT OR IGNORE INTO subscription_catalog_status(id,ready) VALUES(1,0)",
    ] {
        db.exec(sql).await?;
    }
    let tx = crate::store::begin_mutation(db).await?;
    if tx
        .execute(
            "SELECT cutover FROM subscription_signing_state WHERE id=1",
            None,
        )
        .await?
        .rows
        .is_empty()
    {
        let cutover = super::now_ms();
        let rows = tx
            .execute("SELECT value FROM subscription_keys WHERE id=1", None)
            .await?;
        if let Some(row) = rows.rows.first() {
            let mut latest_expiry = cutover;
            let mut after = String::new();
            loop {
                // Extract only bounded metadata; a subscription's full snapshot
                // can be several MiB and is unnecessary for this one-time scan.
                let page = tx.execute(
                    "SELECT key,json_extract(value,'$.wake.token_expires'),json_extract(value,'$.wake.issued_at') FROM subscriptions WHERE key>? ORDER BY key LIMIT 128",
                    Some(vec![text(&after)]),
                ).await?;
                if page.rows.is_empty() {
                    break;
                }
                for entry in &page.rows {
                    after = string(entry, 0)?.to_owned();
                    if let (Some(ColumnValue::Integer(expiry)), Some(ColumnValue::Integer(issued))) =
                        (entry.get(1), entry.get(2))
                        && *issued < cutover
                    {
                        latest_expiry = latest_expiry
                            .max((*expiry).min(cutover.saturating_add(TOKEN_RETENTION_MS)));
                    }
                }
            }
            let legacy = LegacyKeys::migrate(string(row, 0)?, cutover, latest_expiry)?;
            save_legacy(&tx, &legacy).await?;
        }
        tx.execute(
            "INSERT INTO subscription_signing_state(id,cutover) VALUES(1,?)",
            Some(vec![BindParam::Integer(cutover)]),
        )
        .await?;
    }
    tx.commit().await
}
pub fn text(s: impl Into<String>) -> BindParam {
    BindParam::Text(s.into())
}
pub fn string(row: &[ColumnValue], i: usize) -> Result<&str> {
    match row.get(i) {
        Some(ColumnValue::Text(v)) => Ok(v),
        _ => Err(anyhow!("Expected subscription text column")),
    }
}
pub fn key(root: &str, id: &str) -> String {
    format!("{root}/{id}")
}
pub async fn legacy(db: &SqliteDb) -> Result<Option<LegacyKeys>> {
    let result = db
        .query("SELECT value FROM subscription_keys WHERE id=1", None)
        .await?;
    result
        .rows
        .first()
        .map(|row| Ok(serde_json::from_str(string(row, 0)?)?))
        .transpose()
}

async fn save_legacy(tx: &SqliteTransaction, keys: &LegacyKeys) -> Result<()> {
    if keys.next_deadline().is_none() {
        tx.execute("DELETE FROM subscription_keys WHERE id=1", None)
            .await?;
        return Ok(());
    }
    let value = serde_json::to_string(keys)?;
    let inline = value.len() <= 60 * 1024;
    tx.execute(
        "UPDATE subscription_keys SET value=? WHERE id=1",
        Some(vec![text(if inline { &value } else { "" })]),
    )
    .await?;
    if !inline {
        for chunk in value.as_bytes().chunks(60 * 1024) {
            tx.execute(
                "UPDATE subscription_keys SET value=CAST(value || ? AS TEXT) WHERE id=1",
                Some(vec![BindParam::Blob(chunk.to_vec())]),
            )
            .await?;
        }
    }
    Ok(())
}

/// Call under the coordinator's mutation lock and schedule the returned deadline.
pub async fn cleanup_legacy(db: &SqliteDb, now: i64) -> Result<Option<i64>> {
    let Some(mut keys) = legacy(db).await? else {
        return Ok(None);
    };
    if keys.next_deadline().is_some_and(|deadline| deadline > now) {
        return Ok(keys.next_deadline());
    }
    keys.cleanup(now);
    let tx = crate::store::begin_mutation(db).await?;
    save_legacy(&tx, &keys).await?;
    tx.commit().await?;
    Ok(keys.next_deadline())
}
pub async fn load(tx: &SqliteTransaction, key: &str) -> Result<Option<Subscription>> {
    tx.execute(
        "SELECT value FROM subscriptions WHERE key=?",
        Some(vec![text(key)]),
    )
    .await?
    .rows
    .first()
    .map(|r| Ok(serde_json::from_str(string(r, 0)?)?))
    .transpose()
}
pub async fn save(tx: &SqliteTransaction, sub: &Subscription) -> Result<()> {
    let value = stage(tx, sub).await?;
    write(tx, sub, value, None).await
}

/// Start or renew a lease after uploading the snapshot, not before that work.
pub async fn save_with_lease(tx: &SqliteTransaction, sub: &mut Subscription) -> Result<()> {
    let value = stage(tx, sub).await?;
    let now = super::now_ms();
    let wake = sub
        .wake
        .as_mut()
        .ok_or_else(|| anyhow!("Cannot lease a subscription without a wake"))?;
    if wake.token_expires <= now {
        return Err(anyhow::Error::new(super::model::ApiError::new(
            401,
            "TOKEN_EXPIRED",
            "Subscription capability expired before lease persistence",
        )));
    }
    let deadline = now
        .saturating_add(sub.config.lease_ttl_ms)
        .min(wake.token_expires);
    wake.deadline = Some(deadline);
    write(tx, sub, value, Some(deadline)).await
}

async fn stage(tx: &SqliteTransaction, sub: &Subscription) -> Result<String> {
    let value = serde_json::to_string(sub)?;
    let inline = value.len() <= 60 * 1024;
    if !inline {
        // Stage bounded blobs, then assemble once. Repeated value || chunk
        // copies the entire growing snapshot and becomes quadratic at MiB sizes.
        // Engine limits each statement's decoded binds to 128 KiB; leave room
        // for the part number while halving the serialized RPC round trips.
        // Temporary staging avoids committing a second copy of the snapshot.
        tx.execute(
            "CREATE TEMP TABLE IF NOT EXISTS subscription_value_parts (part INTEGER PRIMARY KEY, value BLOB NOT NULL)",
            None,
        ).await?;
        for (part, chunk) in value.as_bytes().chunks(120 * 1024).enumerate() {
            tx.execute(
                "INSERT INTO temp.subscription_value_parts(part,value) VALUES(?,?)",
                Some(vec![
                    BindParam::Integer(part as i64),
                    BindParam::Blob(chunk.to_vec()),
                ]),
            )
            .await?;
        }
    }
    Ok(value)
}

async fn write(
    tx: &SqliteTransaction,
    sub: &Subscription,
    value: String,
    deadline: Option<i64>,
) -> Result<()> {
    let inline = value.len() <= 60 * 1024;
    let mut params = vec![
        text(key(&sub.root, &sub.id)),
        text(&sub.root),
        sub.config
            .pattern
            .as_ref()
            .filter(|_| !sub.deleted)
            .map_or(BindParam::Null, text),
        sub.due().map_or(BindParam::Null, BindParam::Integer),
    ];
    let value_sql = if inline {
        params.push(text(value));
        "?"
    } else {
        "CAST((SELECT group_concat(value,'') FROM (SELECT value FROM temp.subscription_value_parts ORDER BY part)) AS TEXT)"
    };
    let value_sql = if let Some(deadline) = deadline {
        params.push(BindParam::Integer(deadline));
        format!("json_set({value_sql},'$.wake.deadline',?)")
    } else {
        value_sql.to_owned()
    };
    // Keep the previous snapshot intact until its replacement is assembled.
    // Clearing it first needlessly writes and frees all its pages twice.
    tx.execute(format!("INSERT INTO subscriptions(key,root,pattern,due,value) VALUES(?,?,?,?,{value_sql}) ON CONFLICT(key) DO UPDATE SET pattern=excluded.pattern,due=excluded.due,value=excluded.value"), Some(params)).await?;
    if !inline {
        tx.execute("DELETE FROM temp.subscription_value_parts", None)
            .await?;
    }
    Ok(())
}
pub async fn sync_links(tx: &SqliteTransaction, sub: &Subscription) -> Result<()> {
    let key = key(&sub.root, &sub.id);
    let rows = tx
        .execute(
            "SELECT path FROM subscription_links WHERE sub_key=?",
            Some(vec![text(&key)]),
        )
        .await?;
    let old = rows
        .rows
        .iter()
        .map(|r| string(r, 0).map(str::to_owned))
        .collect::<Result<std::collections::BTreeSet<_>>>()?;
    let new = if sub.deleted {
        Default::default()
    } else {
        sub.paths()
    };
    for paths in link_batches(old.difference(&new), key.len() + 8, 8)? {
        let mut params = vec![text(&key)];
        params.extend(paths.iter().map(|path| text(*path)));
        tx.execute(
            format!(
                "DELETE FROM subscription_links WHERE sub_key=? AND path IN ({})",
                vec!["?"; paths.len()].join(",")
            ),
            Some(params),
        )
        .await?;
    }
    for paths in link_batches(new.difference(&old), 0, key.len() + 16)? {
        let params = paths
            .iter()
            .flat_map(|path| [text(*path), text(&key)])
            .collect();
        tx.execute(
            format!(
                "INSERT INTO subscription_links(path,sub_key) VALUES {}",
                vec!["(?,?)"; paths.len()].join(",")
            ),
            Some(params),
        )
        .await?;
    }
    Ok(())
}

fn link_batches<'a>(
    paths: impl IntoIterator<Item = &'a String>,
    base: usize,
    per_row: usize,
) -> Result<Vec<Vec<&'a String>>> {
    // Keep both encoded binds and SQLite's parameter count bounded. The size
    // allowance includes the string length tags used by the remote transport.
    const BUDGET: usize = 60 * 1024;
    let mut batches = Vec::new();
    let mut batch = Vec::new();
    let mut bytes = base;
    for path in paths {
        let size = path.len() + per_row;
        if base + size > BUDGET {
            return Err(anyhow!("Subscription link exceeds parameter budget"));
        }
        if bytes + size > BUDGET || batch.len() == 256 {
            batches.push(std::mem::take(&mut batch));
            bytes = base;
        }
        bytes += size;
        batch.push(path);
    }
    if !batch.is_empty() {
        batches.push(batch);
    }
    Ok(batches)
}
pub async fn catalog(tx: &SqliteTransaction, path: &str) -> Result<Option<CatalogEntry>> {
    tx.execute(
        "SELECT value FROM subscription_catalog WHERE path=?",
        Some(vec![text(path)]),
    )
    .await?
    .rows
    .first()
    .map(|r| Ok(serde_json::from_str(string(r, 0)?)?))
    .transpose()
}
pub async fn save_catalog(tx: &SqliteTransaction, entry: &CatalogEntry) -> Result<bool> {
    let old = catalog(tx, &entry.path).await?;
    if old.as_ref().is_some_and(|old| old.version >= entry.version) {
        return Ok(false);
    }
    let active = tx
        .execute(
            "SELECT version FROM subscription_fanout WHERE path=?",
            Some(vec![text(&entry.path)]),
        )
        .await?;
    let mut latest = entry.clone();
    if !active.rows.is_empty()
        && let Some(old) = &old
        && old.incarnation == entry.incarnation
    {
        // A newer tail cannot discard synthetic spans before the prior fanout
        // has reached every subscriber. Delivery versions remain immutable.
        let mut offsets = latest
            .wakes
            .iter()
            .map(|s| s.after.clone())
            .collect::<std::collections::HashSet<_>>();
        for span in &old.wakes {
            if offsets.insert(span.after.clone()) {
                latest.wakes.push(span.clone());
            }
        }
    }
    write_catalog(tx, &latest).await?;
    if active.rows.is_empty() {
        let discover = old
            .as_ref()
            .is_none_or(|old| !old.exists || old.incarnation != entry.incarnation);
        save_fanout(tx, &latest, discover).await?;
    }
    Ok(true)
}

async fn write_catalog(tx: &SqliteTransaction, entry: &CatalogEntry) -> Result<()> {
    let value = serde_json::to_string(entry)?;
    let inline = value.len() <= 60 * 1024;
    tx.execute("INSERT INTO subscription_catalog(path,version,value) VALUES(?,?,?) ON CONFLICT(path) DO UPDATE SET version=excluded.version,value=excluded.value",Some(vec![text(&entry.path),BindParam::Integer(entry.version),text(if inline{&value}else{""})])).await?;
    if !inline {
        for chunk in value.as_bytes().chunks(60 * 1024) {
            tx.execute(
                "UPDATE subscription_catalog SET value=CAST(value || ? AS TEXT) WHERE path=?",
                Some(vec![BindParam::Blob(chunk.to_vec()), text(&entry.path)]),
            )
            .await?;
        }
    }
    Ok(())
}

pub struct Fanout {
    pub entry: CatalogEntry,
    pub cursor: String,
    pub discover: bool,
}

pub async fn fanout(tx: &SqliteTransaction, path: Option<&str>) -> Result<Option<Fanout>> {
    let rows = match path {
        Some(path) => {
            tx.execute(
                "SELECT value,cursor,discover FROM subscription_fanout WHERE path=?",
                Some(vec![text(path)]),
            )
            .await?
        }
        None => {
            tx.execute(
                "SELECT value,cursor,discover FROM subscription_fanout ORDER BY rowid LIMIT 1",
                None,
            )
            .await?
        }
    };
    rows.rows
        .first()
        .map(|r| {
            Ok(Fanout {
                entry: serde_json::from_str(string(r, 0)?)?,
                cursor: string(r, 1)?.to_owned(),
                discover: matches!(r.get(2), Some(ColumnValue::Integer(1))),
            })
        })
        .transpose()
}

async fn save_fanout(tx: &SqliteTransaction, entry: &CatalogEntry, discover: bool) -> Result<()> {
    let value = serde_json::to_string(entry)?;
    let inline = value.len() <= 60 * 1024;
    tx.execute("INSERT INTO subscription_fanout(path,version,cursor,discover,value) VALUES(?,?,'',?,?) ON CONFLICT(path) DO UPDATE SET version=excluded.version,cursor='',discover=excluded.discover,value=excluded.value",Some(vec![text(&entry.path),BindParam::Integer(entry.version),BindParam::Integer(i64::from(discover)),text(if inline{&value}else{""})])).await?;
    if !inline {
        for chunk in value.as_bytes().chunks(60 * 1024) {
            tx.execute(
                "UPDATE subscription_fanout SET value=CAST(value || ? AS TEXT) WHERE path=?",
                Some(vec![BindParam::Blob(chunk.to_vec()), text(&entry.path)]),
            )
            .await?;
        }
    }
    Ok(())
}

pub async fn advance_fanout(
    tx: &SqliteTransaction,
    job: &Fanout,
    cursor: &str,
    complete: bool,
) -> Result<()> {
    if !complete {
        tx.execute(
            "UPDATE subscription_fanout SET cursor=? WHERE path=?",
            Some(vec![text(cursor), text(&job.entry.path)]),
        )
        .await?;
        return Ok(());
    }
    let mut latest = catalog(tx, &job.entry.path)
        .await?
        .ok_or_else(|| anyhow!("Fanout catalog entry missing"))?;
    // Delete/reinsert rotates a busy path behind other pending paths.
    tx.execute(
        "DELETE FROM subscription_fanout WHERE path=?",
        Some(vec![text(&job.entry.path)]),
    )
    .await?;
    if latest.version > job.entry.version {
        let discover = !job.entry.exists || job.entry.incarnation != latest.incarnation;
        latest.wakes.retain(|span| {
            span.incarnation != job.entry.incarnation || span.after > job.entry.tail
        });
        write_catalog(tx, &latest).await?;
        save_fanout(tx, &latest, discover).await?;
    } else if !latest.wakes.is_empty() {
        latest.wakes.clear();
        write_catalog(tx, &latest).await?;
    }
    Ok(())
}
