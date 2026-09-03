use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use chrono::DateTime;
use rivetkit::{BindParam, ColumnValue, SqliteBatchStatement, SqliteDb, SqliteTransaction};
use serde::{Deserialize, Serialize};

use crate::protocol::{MAX_READ_ROWS, ZERO_OFFSET, advance_offset};

// Rivet's remote SQLite transport caps the aggregate encoded bind parameters
// for one statement at 128 KiB. Keep enough headroom for protocol overhead and
// the offset parameter while retaining the page-tested 1 MB message limit.
const SQLITE_BIND_CHUNK_BYTES: usize = 64 * 1024;

const SCHEMA_VERSION_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS durable_streams_schema_version (
  singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
  schema_version INTEGER NOT NULL CHECK (schema_version >= 0)
) STRICT
"#;

struct Migration {
    version: i64,
    statements: &'static [&'static str],
}

const MIGRATION_1: &[&str] = &[
    r#"
CREATE TABLE IF NOT EXISTS meta (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  gen TEXT NOT NULL,
  content_type TEXT,
  ttl_seconds INTEGER,
  expires_at TEXT,
  closed INTEGER NOT NULL DEFAULT 0,
  closed_by TEXT,
  current_offset TEXT NOT NULL,
  last_seq TEXT,
  created_at INTEGER NOT NULL,
  last_accessed_at INTEGER NOT NULL,
  forked_from TEXT,
  fork_offset TEXT,
  fork_sub_offset INTEGER,
  fork_edge_id TEXT,
  fork_source_gen TEXT,
  soft_deleted INTEGER NOT NULL DEFAULT 0
)
"#,
    r#"
CREATE TABLE IF NOT EXISTS messages (
  msg_offset TEXT PRIMARY KEY,
  data BLOB NOT NULL,
  ts INTEGER NOT NULL
)
"#,
    r#"
CREATE TABLE IF NOT EXISTS producers (
  producer_id TEXT PRIMARY KEY,
  epoch INTEGER NOT NULL,
  last_seq INTEGER NOT NULL,
  last_updated INTEGER NOT NULL
)
"#,
    "CREATE INDEX IF NOT EXISTS producers_last_updated_idx ON producers(last_updated, producer_id)",
    r#"
CREATE TABLE IF NOT EXISTS fork_edges (
  edge_id TEXT PRIMARY KEY,
  fork_offset TEXT NOT NULL
)
"#,
    r#"
CREATE TABLE IF NOT EXISTS fork_intents (
  edge_id TEXT PRIMARY KEY,
  parent_path TEXT NOT NULL,
  params_key TEXT NOT NULL
)
"#,
    r#"
CREATE TABLE IF NOT EXISTS gc_releases (
  edge_id TEXT PRIMARY KEY,
  parent_path TEXT NOT NULL,
  source_gen TEXT
)
"#,
];

// Entries are immutable once released. Schema changes append a migration with
// the next version instead of modifying or reordering existing entries.
const MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    statements: MIGRATION_1,
}];

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Meta {
    pub generation: String,
    pub content_type: Option<String>,
    pub ttl_seconds: Option<i64>,
    pub expires_at: Option<String>,
    pub closed: bool,
    pub closed_by: Option<String>,
    pub current_offset: String,
    pub last_seq: Option<String>,
    pub created_at: i64,
    pub last_accessed_at: i64,
    pub forked_from: Option<String>,
    pub fork_offset: Option<String>,
    pub fork_sub_offset: Option<i64>,
    pub fork_edge_id: Option<String>,
    pub fork_source_gen: Option<String>,
    pub soft_deleted: bool,
}

impl Meta {
    pub fn is_expired(&self, now_ms: i64) -> bool {
        self.expiry_time().is_some_and(|expiry| now_ms >= expiry)
    }

    pub fn expiry_time(&self) -> Option<i64> {
        let absolute = self.expires_at.as_ref().map(|expires_at| {
            DateTime::parse_from_rfc3339(expires_at)
                .map(|value| value.timestamp_millis())
                .unwrap_or(i64::MIN)
        });
        let sliding = self.ttl_seconds.map(|seconds| {
            self.last_accessed_at
                .saturating_add(seconds.saturating_mul(1_000))
        });
        match (absolute, sliding) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (Some(value), None) | (None, Some(value)) => Some(value),
            (None, None) => None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StoredMessage {
    pub offset: String,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ReadBatch {
    pub messages: Vec<StoredMessage>,
    pub capped: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProducerState {
    pub epoch: i64,
    pub last_seq: i64,
    pub last_updated: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ForkIntent {
    pub edge_id: String,
    pub parent_path: String,
    pub params_key: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GcRelease {
    pub edge_id: String,
    pub parent_path: String,
    pub source_generation: Option<String>,
}

pub async fn ensure_schema(db: &SqliteDb) -> Result<()> {
    db.exec(SCHEMA_VERSION_TABLE)
        .await
        .context("create Durable Streams schema version table")?;

    let current = read_schema_version(db).await?;
    validate_schema_version(current)?;

    for migration in MIGRATIONS
        .iter()
        .filter(|migration| migration.version > current)
    {
        db.execute_batch(migration_batch(migration))
            .await
            .with_context(|| {
                format!(
                    "apply Durable Streams schema migration v{}",
                    migration.version
                )
            })?;
    }
    Ok(())
}

fn supported_schema_version() -> i64 {
    MIGRATIONS.last().map_or(0, |migration| migration.version)
}

fn validate_schema_version(current: i64) -> Result<()> {
    let supported = supported_schema_version();
    if current > supported {
        bail!(
            "Durable Streams schema version {current} is newer than supported version {supported}"
        );
    }
    Ok(())
}

async fn read_schema_version(db: &SqliteDb) -> Result<i64> {
    let result = db
        .query(
            "SELECT schema_version FROM durable_streams_schema_version WHERE singleton = 1",
            None,
        )
        .await
        .context("read Durable Streams schema version")?;
    match result.rows.first().and_then(|row| row.first()) {
        None => Ok(0),
        Some(ColumnValue::Integer(version)) => Ok(*version),
        Some(value) => bail!("Durable Streams schema version was not an integer: {value:?}"),
    }
}

fn migration_batch(migration: &Migration) -> Vec<SqliteBatchStatement> {
    let mut statements = migration
        .statements
        .iter()
        .map(|sql| SqliteBatchStatement {
            sql: (*sql).to_owned(),
            params: None,
        })
        .collect::<Vec<_>>();
    statements.push(SqliteBatchStatement {
        sql:
            "INSERT INTO durable_streams_schema_version (singleton, schema_version) VALUES (1, ?) \
              ON CONFLICT(singleton) DO UPDATE SET schema_version = excluded.schema_version"
                .to_owned(),
        params: Some(vec![BindParam::Integer(migration.version)]),
    });
    statements
}

pub async fn get_meta(db: &SqliteDb) -> Result<Option<Meta>> {
    let result = db.query(meta_select(), None).await?;
    result.rows.first().map(|row| parse_meta(row)).transpose()
}

pub async fn get_meta_tx(tx: &SqliteTransaction) -> Result<Option<Meta>> {
    let result = tx.execute(meta_select(), None).await?;
    result.rows.first().map(|row| parse_meta(row)).transpose()
}

fn meta_select() -> &'static str {
    "SELECT gen, content_type, ttl_seconds, expires_at, closed, closed_by, \
	 current_offset, last_seq, created_at, last_accessed_at, forked_from, \
	 fork_offset, fork_sub_offset, fork_edge_id, fork_source_gen, soft_deleted \
	 FROM meta WHERE id = 1"
}

fn parse_meta(row: &[ColumnValue]) -> Result<Meta> {
    Ok(Meta {
        generation: text(row.first())?,
        content_type: optional_text(row.get(1))?,
        ttl_seconds: optional_int(row.get(2))?,
        expires_at: optional_text(row.get(3))?,
        closed: int(row.get(4))? != 0,
        closed_by: optional_text(row.get(5))?,
        current_offset: text(row.get(6))?,
        last_seq: optional_text(row.get(7))?,
        created_at: int(row.get(8))?,
        last_accessed_at: int(row.get(9))?,
        forked_from: optional_text(row.get(10))?,
        fork_offset: optional_text(row.get(11))?,
        fork_sub_offset: optional_int(row.get(12))?,
        fork_edge_id: optional_text(row.get(13))?,
        fork_source_gen: optional_text(row.get(14))?,
        soft_deleted: int(row.get(15))? != 0,
    })
}

#[allow(clippy::too_many_arguments)]
pub async fn create_meta(
    tx: &SqliteTransaction,
    content_type: Option<&str>,
    ttl_seconds: Option<i64>,
    expires_at: Option<&str>,
    closed: bool,
    now_ms: i64,
    forked_from: Option<&str>,
    fork_offset: Option<&str>,
    fork_sub_offset: Option<i64>,
    fork_edge_id: Option<&str>,
    fork_source_gen: Option<&str>,
) -> Result<()> {
    let initial_offset = if forked_from.is_some() {
        fork_offset.unwrap_or(ZERO_OFFSET)
    } else {
        ZERO_OFFSET
    };
    tx.execute(
        "INSERT INTO meta (id, gen, content_type, ttl_seconds, expires_at, closed, \
		 closed_by, current_offset, last_seq, created_at, last_accessed_at, \
		 forked_from, fork_offset, fork_sub_offset, fork_edge_id, fork_source_gen, \
		 soft_deleted) VALUES (1, ?, ?, ?, ?, ?, NULL, ?, NULL, ?, ?, ?, ?, ?, ?, ?, 0)",
        Some(vec![
            BindParam::Text(uuid::Uuid::new_v4().to_string()),
            bind_optional_text(content_type),
            bind_optional_int(ttl_seconds),
            bind_optional_text(expires_at),
            BindParam::Integer(i64::from(closed)),
            BindParam::Text(initial_offset.to_owned()),
            BindParam::Integer(now_ms),
            BindParam::Integer(now_ms),
            bind_optional_text(forked_from),
            bind_optional_text(fork_offset),
            bind_optional_int(fork_sub_offset),
            bind_optional_text(fork_edge_id),
            bind_optional_text(fork_source_gen),
        ]),
    )
    .await?;
    Ok(())
}

pub async fn append_message(
    tx: &SqliteTransaction,
    current_offset: &str,
    payload: Vec<u8>,
    now_ms: i64,
) -> Result<String> {
    let next = advance_offset(current_offset, payload.len())?;
    tx.execute(
        "INSERT INTO messages (msg_offset, data, ts) VALUES (?, X'', ?)",
        Some(vec![
            BindParam::Text(next.clone()),
            BindParam::Integer(now_ms),
        ]),
    )
    .await?;
    for chunk in payload.chunks(SQLITE_BIND_CHUNK_BYTES) {
        tx.execute(
            "UPDATE messages SET data = CAST(data || ? AS BLOB) WHERE msg_offset = ?",
            Some(vec![
                BindParam::Blob(chunk.to_vec()),
                BindParam::Text(next.clone()),
            ]),
        )
        .await?;
    }
    tx.execute(
        "UPDATE meta SET current_offset = ? WHERE id = 1",
        Some(vec![BindParam::Text(next.clone())]),
    )
    .await?;
    Ok(next)
}

pub async fn touch(tx: &SqliteTransaction, now_ms: i64) -> Result<()> {
    tx.execute(
        "UPDATE meta SET last_accessed_at = ? WHERE id = 1",
        Some(vec![BindParam::Integer(now_ms)]),
    )
    .await?;
    Ok(())
}

pub async fn set_last_seq(tx: &SqliteTransaction, seq: String) -> Result<()> {
    tx.execute(
        "UPDATE meta SET last_seq = ? WHERE id = 1",
        Some(vec![BindParam::Text(seq)]),
    )
    .await?;
    Ok(())
}

pub async fn set_closed(tx: &SqliteTransaction, closed_by: Option<String>) -> Result<()> {
    tx.execute(
        "UPDATE meta SET closed = 1, closed_by = COALESCE(?, closed_by) WHERE id = 1",
        Some(vec![closed_by.map_or(BindParam::Null, BindParam::Text)]),
    )
    .await?;
    Ok(())
}

pub async fn purge(tx: &SqliteTransaction) -> Result<()> {
    for statement in [
        "DELETE FROM meta",
        "DELETE FROM messages",
        "DELETE FROM producers",
        "DELETE FROM fork_edges",
    ] {
        tx.execute(statement, None).await?;
    }
    Ok(())
}

pub async fn set_soft_deleted(tx: &SqliteTransaction) -> Result<()> {
    tx.execute("UPDATE meta SET soft_deleted = 1 WHERE id = 1", None)
        .await?;
    Ok(())
}

pub async fn fork_edge_count(tx: &SqliteTransaction) -> Result<i64> {
    let result = tx.execute("SELECT COUNT(*) FROM fork_edges", None).await?;
    result
        .rows
        .first()
        .map(|row| int(row.first()))
        .transpose()
        .map(|value| value.unwrap_or_default())
}

pub async fn get_fork_edge_tx(tx: &SqliteTransaction, edge_id: &str) -> Result<Option<String>> {
    let result = tx
        .execute(
            "SELECT fork_offset FROM fork_edges WHERE edge_id = ?",
            Some(vec![BindParam::Text(edge_id.to_owned())]),
        )
        .await?;
    result.rows.first().map(|row| text(row.first())).transpose()
}

pub async fn insert_fork_edge(
    tx: &SqliteTransaction,
    edge_id: &str,
    fork_offset: &str,
) -> Result<String> {
    tx.execute(
        "INSERT OR IGNORE INTO fork_edges (edge_id, fork_offset) VALUES (?, ?)",
        Some(vec![
            BindParam::Text(edge_id.to_owned()),
            BindParam::Text(fork_offset.to_owned()),
        ]),
    )
    .await?;
    get_fork_edge_tx(tx, edge_id)
        .await?
        .ok_or_else(|| anyhow!("fork edge was not persisted"))
}

pub async fn delete_fork_edge(tx: &SqliteTransaction, edge_id: &str) -> Result<()> {
    tx.execute(
        "DELETE FROM fork_edges WHERE edge_id = ?",
        Some(vec![BindParam::Text(edge_id.to_owned())]),
    )
    .await?;
    Ok(())
}

pub async fn get_fork_intent(db: &SqliteDb, params_key: &str) -> Result<Option<String>> {
    let result = db
        .query(
            "SELECT edge_id FROM fork_intents WHERE params_key = ?",
            Some(vec![BindParam::Text(params_key.to_owned())]),
        )
        .await?;
    result.rows.first().map(|row| text(row.first())).transpose()
}

pub async fn put_fork_intent(
    tx: &SqliteTransaction,
    edge_id: &str,
    parent_path: &str,
    params_key: &str,
) -> Result<()> {
    tx.execute(
        "INSERT OR IGNORE INTO fork_intents (edge_id, parent_path, params_key) VALUES (?, ?, ?)",
        Some(vec![
            BindParam::Text(edge_id.to_owned()),
            BindParam::Text(parent_path.to_owned()),
            BindParam::Text(params_key.to_owned()),
        ]),
    )
    .await?;
    Ok(())
}

pub async fn delete_fork_intent(tx: &SqliteTransaction, edge_id: &str) -> Result<()> {
    tx.execute(
        "DELETE FROM fork_intents WHERE edge_id = ?",
        Some(vec![BindParam::Text(edge_id.to_owned())]),
    )
    .await?;
    Ok(())
}

pub async fn pending_fork_intents(db: &SqliteDb) -> Result<Vec<ForkIntent>> {
    let result = db
        .query(
            "SELECT edge_id, parent_path, params_key FROM fork_intents ORDER BY edge_id LIMIT 64",
            None,
        )
        .await?;
    result
        .rows
        .iter()
        .map(|row| {
            Ok(ForkIntent {
                edge_id: text(row.first())?,
                parent_path: text(row.get(1))?,
                params_key: text(row.get(2))?,
            })
        })
        .collect()
}

pub async fn enqueue_gc_release(
    tx: &SqliteTransaction,
    edge_id: &str,
    parent_path: &str,
    source_generation: Option<&str>,
) -> Result<()> {
    tx.execute(
        "INSERT OR IGNORE INTO gc_releases (edge_id, parent_path, source_gen) VALUES (?, ?, ?)",
        Some(vec![
            BindParam::Text(edge_id.to_owned()),
            BindParam::Text(parent_path.to_owned()),
            bind_optional_text(source_generation),
        ]),
    )
    .await?;
    Ok(())
}

pub async fn dequeue_gc_release(tx: &SqliteTransaction, edge_id: &str) -> Result<()> {
    tx.execute(
        "DELETE FROM gc_releases WHERE edge_id = ?",
        Some(vec![BindParam::Text(edge_id.to_owned())]),
    )
    .await?;
    Ok(())
}

pub async fn pending_gc_releases(db: &SqliteDb) -> Result<Vec<GcRelease>> {
    let result = db
        .query(
            "SELECT edge_id, parent_path, source_gen FROM gc_releases ORDER BY edge_id LIMIT 64",
            None,
        )
        .await?;
    result
        .rows
        .iter()
        .map(|row| {
            Ok(GcRelease {
                edge_id: text(row.first())?,
                parent_path: text(row.get(1))?,
                source_generation: optional_text(row.get(2))?,
            })
        })
        .collect()
}

pub async fn read_messages_range(
    db: &SqliteDb,
    after_offset: Option<&str>,
    cap_offset: Option<&str>,
    limit: usize,
    byte_budget: usize,
    allow_oversized_first: bool,
) -> Result<ReadBatch> {
    let after = after_offset.filter(|value| *value != "-1").unwrap_or("");
    let mut conditions = vec!["msg_offset > ?"];
    let mut params = vec![BindParam::Text(after.to_owned())];
    if let Some(cap) = cap_offset {
        conditions.push("msg_offset <= ?");
        params.push(BindParam::Text(cap.to_owned()));
    }
    let scan_limit = limit.saturating_add(1).min(MAX_READ_ROWS + 1);
    let lengths = db
		.query(
			format!(
				"SELECT msg_offset, length(data) FROM messages WHERE {} ORDER BY msg_offset ASC LIMIT {scan_limit}",
				conditions.join(" AND ")
			),
			Some(params.clone()),
		)
		.await?;

    let mut selected = 0usize;
    let mut bytes = 0usize;
    let mut last_offset = None;
    for row in &lengths.rows {
        if selected == limit {
            break;
        }
        let length = int(row.get(1))?.max(0) as usize;
        if (selected > 0 || !allow_oversized_first) && bytes.saturating_add(length) > byte_budget {
            break;
        }
        bytes = bytes.saturating_add(length);
        selected += 1;
        last_offset = Some(text(row.first())?);
    }

    let capped = lengths.rows.len() > selected;
    let Some(last_offset) = last_offset else {
        return Ok(ReadBatch {
            messages: Vec::new(),
            capped,
        });
    };
    conditions.push("msg_offset <= ?");
    params.push(BindParam::Text(last_offset));
    let rows = db
        .query(
            format!(
                "SELECT msg_offset, data FROM messages WHERE {} ORDER BY msg_offset ASC",
                conditions.join(" AND ")
            ),
            Some(params),
        )
        .await?;
    let messages = rows
        .rows
        .iter()
        .map(|row| {
            Ok(StoredMessage {
                offset: text(row.first())?,
                data: blob(row.get(1))?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(ReadBatch { messages, capped })
}

pub async fn get_producer_state(
    tx: &SqliteTransaction,
    producer_id: &str,
    expired_before_ms: i64,
) -> Result<Option<ProducerState>> {
    // The addressed producer is expired synchronously. Other rows are swept in
    // a bounded, index-backed batch so a write cannot trigger an unbounded delete.
    tx.execute(
        "DELETE FROM producers WHERE producer_id = ? AND last_updated < ?",
        Some(vec![
            BindParam::Text(producer_id.to_owned()),
            BindParam::Integer(expired_before_ms),
        ]),
    )
    .await?;
    tx.execute(
        "DELETE FROM producers WHERE producer_id IN (SELECT producer_id FROM producers \
		 WHERE last_updated < ? ORDER BY last_updated, producer_id LIMIT 64)",
        Some(vec![BindParam::Integer(expired_before_ms)]),
    )
    .await?;
    let result = tx
        .execute(
            "SELECT epoch, last_seq, last_updated FROM producers WHERE producer_id = ?",
            Some(vec![BindParam::Text(producer_id.to_owned())]),
        )
        .await?;
    result
        .rows
        .first()
        .map(|row| {
            Ok(ProducerState {
                epoch: int(row.first())?,
                last_seq: int(row.get(1))?,
                last_updated: int(row.get(2))?,
            })
        })
        .transpose()
}

pub async fn commit_producer_state(
    tx: &SqliteTransaction,
    producer_id: String,
    state: &ProducerState,
) -> Result<()> {
    tx.execute(
        "INSERT INTO producers (producer_id, epoch, last_seq, last_updated) VALUES (?, ?, ?, ?) \
		 ON CONFLICT(producer_id) DO UPDATE SET epoch = excluded.epoch, \
		 last_seq = excluded.last_seq, last_updated = excluded.last_updated",
        Some(vec![
            BindParam::Text(producer_id),
            BindParam::Integer(state.epoch),
            BindParam::Integer(state.last_seq),
            BindParam::Integer(state.last_updated),
        ]),
    )
    .await?;
    Ok(())
}

pub async fn begin_mutation(db: &SqliteDb) -> Result<SqliteTransaction> {
    db.begin_transaction(Some(Duration::from_secs(5))).await
}

fn bind_optional_text(value: Option<&str>) -> BindParam {
    value.map_or(BindParam::Null, |value| BindParam::Text(value.to_owned()))
}

fn bind_optional_int(value: Option<i64>) -> BindParam {
    value.map_or(BindParam::Null, BindParam::Integer)
}

fn text(value: Option<&ColumnValue>) -> Result<String> {
    match value {
        Some(ColumnValue::Text(value)) => Ok(value.clone()),
        other => Err(anyhow!("expected text column, got {other:?}")),
    }
}

fn optional_text(value: Option<&ColumnValue>) -> Result<Option<String>> {
    match value {
        Some(ColumnValue::Text(value)) => Ok(Some(value.clone())),
        Some(ColumnValue::Null) => Ok(None),
        other => Err(anyhow!("expected optional text column, got {other:?}")),
    }
}

fn int(value: Option<&ColumnValue>) -> Result<i64> {
    match value {
        Some(ColumnValue::Integer(value)) => Ok(*value),
        other => Err(anyhow!("expected integer column, got {other:?}")),
    }
}

fn optional_int(value: Option<&ColumnValue>) -> Result<Option<i64>> {
    match value {
        Some(ColumnValue::Integer(value)) => Ok(Some(*value)),
        Some(ColumnValue::Null) => Ok(None),
        other => Err(anyhow!("expected optional integer column, got {other:?}")),
    }
}

fn blob(value: Option<&ColumnValue>) -> Result<Vec<u8>> {
    match value {
        Some(ColumnValue::Blob(value)) => Ok(value.clone()),
        other => Err(anyhow!("expected blob column, got {other:?}")),
    }
}

#[cfg(test)]
mod schema_tests {
    use super::*;

    #[test]
    fn migrations_are_contiguous() {
        for (index, migration) in MIGRATIONS.iter().enumerate() {
            assert_eq!(migration.version, index as i64 + 1);
            assert!(!migration.statements.is_empty());
        }
        assert_eq!(supported_schema_version(), MIGRATIONS.len() as i64);
    }

    #[test]
    fn future_schema_is_rejected() {
        let error = validate_schema_version(supported_schema_version() + 1).unwrap_err();
        assert!(error.to_string().contains("newer than supported"));
    }
}
