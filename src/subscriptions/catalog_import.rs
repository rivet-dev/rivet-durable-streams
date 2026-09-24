//! Import actors created before the durable stream catalog existed.
use std::{collections::HashMap, time::Duration};

use anyhow::{Context, Result, ensure};
use futures_util::{StreamExt, TryStreamExt, stream};
use rivetkit_client::{Client, GetOptions};
use serde::Deserialize;

use super::model::CatalogEntry;

const PAGE_SIZE: usize = 256;
const MAX_PAGE_SIZE: usize = 8_192;
const MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone)]
pub struct CatalogApi {
    pub endpoint: String,
    pub namespace: String,
    pub token: Option<String>,
}

#[derive(Deserialize)]
struct ActorPage {
    actors: Vec<ListedActor>,
    pagination: Pagination,
}

#[derive(Deserialize)]
struct ListedActor {
    actor_id: String,
    name: String,
    create_ts: i64,
}

#[derive(Deserialize)]
struct Pagination {
    cursor: Option<String>,
}

#[derive(Deserialize)]
struct Snapshot {
    tenant_scope: String,
    entry: Option<CatalogEntry>,
}

pub(crate) async fn import(client: &Client, api: &CatalogApi, tenant: &str) -> Result<()> {
    let mut url = url::Url::parse(&api.endpoint).context("parse catalog Engine endpoint")?;
    ensure!(
        matches!(url.scheme(), "http" | "https")
            && url.username().is_empty()
            && url.password().is_none(),
        "catalog Engine endpoint must be HTTP(S) with credentials configured separately"
    );
    url.set_path(&format!("{}/actors", url.path().trim_end_matches('/')));
    url.set_query(None);
    url.set_fragment(None);
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(30))
        .build()?;
    let manager = super::handle(client, tenant)?;
    let ready = manager
        .action(
            "__durableStreamCatalogReady",
            vec![serde_json::json!({"complete":false})],
        )
        .await?;
    if ready == serde_json::Value::Bool(true) {
        return Ok(());
    }
    let mut pager = Pager::default();

    loop {
        let mut request = http.get(url.clone()).query(&[
            ("namespace", api.namespace.clone()),
            ("name", crate::actor::ACTOR_NAME.to_owned()),
            ("include_destroyed", "false".to_owned()),
            ("limit", pager.limit.to_string()),
        ]);
        if let Some(cursor) = pager.cursor {
            request = request.query(&[("cursor", cursor.to_string())]);
        }
        if let Some(token) = &api.token {
            request = request.bearer_auth(token);
        }
        let response = request
            .send()
            .await
            .context("list existing stream actors")?
            .error_for_status()
            .context("Engine rejected catalog actor listing")?;
        ensure!(
            response
                .content_length()
                .is_none_or(|n| n <= MAX_RESPONSE_BYTES as u64),
            "Engine catalog listing exceeds response size limit"
        );
        let mut body = Vec::new();
        let mut chunks = response.bytes_stream();
        while let Some(chunk) = chunks.try_next().await? {
            ensure!(
                body.len() + chunk.len() <= MAX_RESPONSE_BYTES,
                "Engine catalog listing exceeds response size limit"
            );
            body.extend_from_slice(&chunk);
        }
        let page = serde_json::from_slice(&body).context("decode Engine catalog actor listing")?;
        let (ids, complete) = pager.accept(page)?;
        let manager = &manager;
        stream::iter(ids)
            .map(|id| async move {
                tokio::time::timeout(Duration::from_secs(60), async {
                    let actor =
                        client.get_for_id(crate::actor::ACTOR_NAME, &id, GetOptions::default())?;
                    let value = actor
                        .action("__durableStreamSubscriptionSnapshot", vec![])
                        .await?;
                    let snapshot: Snapshot = serde_json::from_value(value)
                        .context("decode stream subscription snapshot")?;
                    if snapshot.tenant_scope == tenant
                        && let Some(entry) = snapshot.entry
                    {
                        manager
                            .action(
                                "__durableStreamCatalogChanged",
                                vec![serde_json::to_value(entry)?],
                            )
                            .await?;
                    }
                    Ok::<_, anyhow::Error>(())
                })
                .await
                .context("catalog actor import timed out")?
                .with_context(|| format!("import stream actor {id}"))
            })
            .buffer_unordered(8)
            .try_collect::<Vec<_>>()
            .await?;
        if complete {
            manager
                .action(
                    "__durableStreamCatalogReady",
                    vec![serde_json::json!({"complete":true})],
                )
                .await?;
            return Ok(());
        }
    }
}

struct Pager {
    cursor: Option<i64>,
    limit: usize,
    seen: HashMap<String, i64>,
}

impl Default for Pager {
    fn default() -> Self {
        Self {
            cursor: None,
            limit: PAGE_SIZE,
            seen: HashMap::new(),
        }
    }
}

impl Pager {
    fn accept(&mut self, page: ActorPage) -> Result<(Vec<String>, bool)> {
        ensure!(
            page.actors.len() <= self.limit,
            "Engine exceeded catalog page limit"
        );
        ensure!(
            page.actors
                .windows(2)
                .all(|pair| pair[0].create_ts >= pair[1].create_ts),
            "Engine catalog listing is not ordered by creation time"
        );
        ensure!(
            page.actors
                .iter()
                .all(|actor| actor.name == crate::actor::ACTOR_NAME
                    && !actor.actor_id.is_empty()
                    && self.cursor.is_none_or(|cursor| actor.create_ts <= cursor)),
            "Engine returned an actor outside the catalog query"
        );
        let last_ts = page.actors.last().map(|actor| actor.create_ts);
        let returned_cursor = page
            .pagination
            .cursor
            .as_deref()
            .map(str::parse::<i64>)
            .transpose()
            .context("invalid Engine catalog pagination cursor")?;
        ensure!(
            returned_cursor == last_ts,
            "Engine catalog cursor does not match its page"
        );
        let complete = page.actors.len() < self.limit;
        if !complete {
            // Engine 2.3.12 cursors contain only milliseconds, without an actor-ID
            // tiebreaker. Overlap both inclusive and exclusive implementations to
            // avoid dropping actors sharing the last timestamp in a full page.
            let next = last_ts
                .context("missing full catalog page timestamp")?
                .checked_add(1)
                .context("catalog timestamp cannot be paginated")?;
            if self.cursor.is_some_and(|cursor| next >= cursor) {
                ensure!(
                    self.limit < MAX_PAGE_SIZE,
                    "catalog migration cannot advance past a timestamp boundary within {MAX_PAGE_SIZE} actors"
                );
                self.limit = (self.limit * 2).min(MAX_PAGE_SIZE);
            } else {
                self.cursor = Some(next);
                self.limit = PAGE_SIZE;
            }
        }
        let ids = page
            .actors
            .into_iter()
            .filter_map(|actor| {
                self.seen
                    .insert(actor.actor_id.clone(), actor.create_ts)
                    .is_none()
                    .then_some(actor.actor_id)
            })
            .collect();
        if let Some(cursor) = self.cursor {
            // Only the overlapping boundary can reappear. Keep migration memory
            // bounded by a page instead of the total number of existing actors.
            self.seen.retain(|_, timestamp| *timestamp <= cursor);
        }
        Ok((ids, complete))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn page(actors: Vec<ListedActor>) -> ActorPage {
        let cursor = actors.last().map(|actor| actor.create_ts.to_string());
        ActorPage {
            actors,
            pagination: Pagination { cursor },
        }
    }

    fn actor(id: usize, create_ts: i64) -> ListedActor {
        ListedActor {
            actor_id: id.to_string(),
            name: crate::actor::ACTOR_NAME.to_owned(),
            create_ts,
        }
    }

    #[test]
    fn pagination_keeps_all_timestamp_ties_for_both_engine_boundary_rules() {
        let timestamps: Vec<_> = (0..900).map(|i| 100 - i / 300).collect();
        for inclusive in [false, true] {
            let mut pager = Pager::default();
            let mut imported = HashSet::new();
            let mut complete = false;
            for _ in 0..20 {
                let rows = timestamps
                    .iter()
                    .enumerate()
                    .filter(|(_, ts)| {
                        pager.cursor.is_none_or(|cursor| {
                            if inclusive {
                                **ts <= cursor
                            } else {
                                **ts < cursor
                            }
                        })
                    })
                    .take(pager.limit)
                    .map(|(id, ts)| actor(id, *ts))
                    .collect();
                let (ids, done) = pager.accept(page(rows)).unwrap();
                for id in ids {
                    assert!(imported.insert(id), "duplicate import");
                }
                if done {
                    complete = true;
                    break;
                }
            }
            assert!(complete, "pagination did not terminate");
            assert_eq!(imported.len(), timestamps.len());
        }
    }

    #[test]
    fn pagination_handles_empty_and_exact_size_final_pages() {
        let mut pager = Pager::default();
        assert!(pager.accept(page(vec![])).unwrap().1);
        let rows = (0..PAGE_SIZE)
            .map(|id| actor(id, 1000 - id as i64))
            .collect();
        assert!(!pager.accept(page(rows)).unwrap().1);
        let (ids, complete) = pager
            .accept(page(vec![actor(
                PAGE_SIZE - 1,
                1000 - (PAGE_SIZE - 1) as i64,
            )]))
            .unwrap();
        assert!(ids.is_empty());
        assert!(complete);
    }

    #[test]
    fn pagination_fails_instead_of_skipping_an_unbounded_timestamp_group() {
        let mut pager = Pager {
            cursor: Some(101),
            limit: MAX_PAGE_SIZE,
            seen: HashMap::new(),
        };
        let rows = (0..MAX_PAGE_SIZE).map(|id| actor(id, 100)).collect();
        assert!(
            pager
                .accept(page(rows))
                .unwrap_err()
                .to_string()
                .contains("cannot advance")
        );
    }

    #[test]
    fn pagination_rejects_inconsistent_engine_results() {
        let mut pager = Pager::default();
        assert!(pager.accept(page(vec![actor(1, 1), actor(2, 2)])).is_err());
        let mut response = page(vec![actor(1, 1)]);
        response.pagination.cursor = Some("2".into());
        assert!(pager.accept(response).is_err());
    }
}
