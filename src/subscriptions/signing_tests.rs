use super::{model::*, signing::webhook_body};
use serde_json::json;

fn maximum_subscription() -> Subscription {
    let config = serde_json::from_value::<Config>(json!({
        "type":"webhook", "pattern":"events/**",
        "webhook":{"url":"https://receiver.gameinc.io/hook"}
    }))
    .unwrap()
    .normalize()
    .unwrap();
    let mut sub = Subscription::new(
        "quoted\"subscription".into(),
        "/v1/stream".into(),
        "https://streams.gameinc.io/v1/stream".into(),
        config,
        0,
        1000,
    );
    for i in 0..4096 {
        let prefix = format!("events/{i:04}/");
        let remaining = 2048 - prefix.len();
        let raw = format!(
            "{prefix}{}{}",
            "\u{a0}".repeat(remaining / 2),
            "x".repeat(remaining % 2)
        );
        assert_eq!(raw.len(), 2048);
        let path = normalize_path(&raw, false).unwrap();
        assert!(path.len() > 6000);
        sub.links.insert(
            path,
            Link {
                version: 1,
                explicit: false,
                glob: true,
                incarnation: "incarnation".into(),
                acked: crate::protocol::ZERO_OFFSET.into(),
                tail: "0000000000000000_0000000000000001".into(),
                synthetic: vec![],
            },
        );
    }
    assert!(sub.ensure_wake(1000));
    sub
}

#[test]
fn maximum_4096_stream_webhook_snapshot_preserves_every_expanded_path() {
    let sub = maximum_subscription();
    let token = "x".repeat(6144);
    let started = std::time::Instant::now();
    let body = webhook_body(&sub, &token).unwrap();
    let elapsed = started.elapsed();
    assert!(body.len() > 24 * 1024 * 1024);
    assert!(body.len() < 32 * 1024 * 1024);
    let decoded: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(decoded["subscription_id"], sub.id);
    assert_eq!(decoded["callback_token"], token);
    let streams = decoded["streams"].as_array().unwrap();
    assert_eq!(streams.len(), 4096);
    for (stream, expected) in streams.iter().zip(sub.links.keys()) {
        assert_eq!(stream["path"], expected.as_str());
        assert_eq!(stream["tail_offset"], "0000000000000000_0000000000000001");
    }
    let state_bytes = serde_json::to_vec(&sub).unwrap().len();
    eprintln!(
        "4096 maximum paths: webhook={} bytes, state={} bytes, serialization={elapsed:?}",
        body.len(),
        state_bytes
    );
}

mod storage {
    use super::*;
    use anyhow::{Context, Result};
    use async_trait::async_trait;
    use rivetkit::{Action, Handles, prelude::*};
    use serde::{Deserialize, Serialize};
    use sha2::{Digest, Sha256};
    use std::{
        sync::Arc,
        time::{Duration, Instant},
    };

    const NAME: &str = "subscriptionLargeStateTest";
    struct Fixture;
    #[derive(Serialize, Deserialize)]
    struct Ready;
    impl Action for Ready {
        type Output = bool;
        const NAME: &'static str = "ready";
    }
    #[derive(Serialize, Deserialize)]
    struct Persist;
    impl Action for Persist {
        type Output = serde_json::Value;
        const NAME: &'static str = "persist";
    }
    #[async_trait]
    impl Actor for Fixture {
        type State = ();
        type Input = ();
        type Actions = (Ready, Persist);
        type Events = ();
        type Queue = ();
        type ConnParams = ();
        type ConnState = ();
        type Action = rivetkit::action::Raw;
        const HAS_DATABASE: bool = true;
        async fn create_state(_: &Ctx<Self>, _: ()) -> Result<()> {
            Ok(())
        }
        async fn create(ctx: &Ctx<Self>) -> Result<Self> {
            super::super::db::ensure(ctx.sql()).await?;
            Ok(Self)
        }
    }
    impl Handles<Ready> for Fixture {
        type Future = super::super::BoxFuture<bool>;
        fn handle(self: Arc<Self>, _: Ctx<Self>, _: Ready) -> Self::Future {
            Box::pin(async { Ok(true) })
        }
    }
    impl Handles<Persist> for Fixture {
        type Future = super::super::BoxFuture<serde_json::Value>;
        fn handle(self: Arc<Self>, ctx: Ctx<Self>, _: Persist) -> Self::Future {
            Box::pin(async move {
                let mut sub = maximum_subscription();
                let key = super::super::db::key(&sub.root, &sub.id);
                let mut times = Vec::new();
                let mut load_times = Vec::new();
                let mut lease_remaining = Vec::new();
                let mut lease_at_commit = Vec::new();
                // A 4,096-stream snapshot can take over a second to commit on CI.
                sub.config.lease_ttl_ms = 10_000;
                for round in 0..2 {
                    let old_deadline = super::super::now_ms() + 1_000;
                    let wake = sub.wake.as_mut().unwrap();
                    wake.deadline = Some(old_deadline);
                    wake.token_expires = old_deadline + 60_000;
                    wake.next_attempt_at = None;
                    let started = Instant::now();
                    let tx = super::super::db::begin(ctx.sql()).await?;
                    super::super::db::save_with_lease(&tx, &mut sub)
                        .await
                        .with_context(|| format!("save round {round}"))?;
                    tx.commit().await.context("commit snapshot")?;
                    times.push(started.elapsed().as_millis());
                    eprintln!("maximum state save round {round}: {:?}", started.elapsed());
                    let deadline = sub.wake.as_ref().unwrap().deadline.unwrap();
                    lease_at_commit.push(deadline - super::super::now_ms());
                    assert!(old_deadline <= super::super::now_ms());
                    let persisted = ctx.sql().execute(
                        "SELECT due,json_extract(value,'$.wake.deadline') FROM subscriptions WHERE key=?",
                        Some(vec![super::super::db::text(&key)]),
                    ).await?;
                    assert!(
                        matches!(persisted.rows[0][0], rivetkit::ColumnValue::Integer(n) if n == deadline)
                    );
                    assert!(
                        matches!(persisted.rows[0][1], rivetkit::ColumnValue::Integer(n) if n == deadline)
                    );
                    let remaining = deadline - super::super::now_ms();
                    assert!(
                        remaining > 0,
                        "fresh minimum lease expired after commit: {remaining}ms"
                    );
                    lease_remaining.push(remaining);
                    let before = hash(&sub)?;
                    let started = Instant::now();
                    let tx = super::super::db::begin(ctx.sql()).await?;
                    let loaded = super::super::db::load(&tx, &key)
                        .await?
                        .context("Missing saved state")?;
                    let parts = tx
                        .execute("SELECT count(*) FROM subscription_value_parts", None)
                        .await?;
                    assert!(matches!(
                        parts.rows[0][0],
                        rivetkit::ColumnValue::Integer(0)
                    ));
                    tx.rollback().await?;
                    load_times.push(started.elapsed().as_millis());
                    eprintln!("maximum state load round {round}: {:?}", started.elapsed());
                    assert_eq!(before, hash(&loaded)?);
                    if round == 0 {
                        sub.links.first_entry().unwrap().get_mut().acked =
                            "0000000000000000_0000000000000001".into();
                    }
                }
                // Expired capabilities fail without inserting an owned wake.
                let mut expired = sub.clone();
                expired.id = "expired capability".into();
                let wake = expired.wake.as_mut().unwrap();
                wake.token_expires = super::super::now_ms() + 1_000;
                let tx = super::super::db::begin(ctx.sql()).await?;
                let failure = super::super::db::save_with_lease(&tx, &mut expired)
                    .await
                    .unwrap_err();
                let failure = failure.downcast_ref::<ApiError>().context("typed expiry")?;
                assert_eq!(failure.status, 401);
                assert_eq!(failure.body["error"]["code"], "TOKEN_EXPIRED");
                tx.rollback().await?;
                let parts = ctx
                    .sql()
                    .execute("SELECT count(*) FROM temp.subscription_value_parts", None)
                    .await?;
                assert!(matches!(
                    parts.rows[0][0],
                    rivetkit::ColumnValue::Integer(0)
                ));
                let rows = ctx
                    .sql()
                    .execute(
                        "SELECT 1 FROM subscriptions WHERE key=?",
                        Some(vec![super::super::db::text(super::super::db::key(
                            &expired.root,
                            &expired.id,
                        ))]),
                    )
                    .await?;
                assert!(rows.rows.is_empty());
                let first_key = key;
                sub.id = "second maximum subscription".into();
                let relative = sub.links.first_key_value().unwrap().0.clone();
                let entry = CatalogEntry {
                    path: format!("{}/{}", sub.root, relative),
                    version: 2,
                    incarnation: "incarnation".into(),
                    tail: "0000000000000000_0000000000000002".into(),
                    start: crate::protocol::ZERO_OFFSET.into(),
                    exists: true,
                    wakes: vec![],
                };
                let tx = super::super::db::begin(ctx.sql()).await?;
                super::super::db::save(&tx, &sub).await?;
                super::super::db::save_catalog(&tx, &entry).await?;
                tx.commit().await?;
                // This fixture only runs the coordinator's SQL fanout path;
                // neither the placeholder Engine client nor delivery is used.
                let coordinator = super::super::SubscriptionActor {
                    client: crate::client::actor_client(&ctx)?,
                    engine: super::super::EngineClient::new(
                        &super::super::catalog_import::CatalogApi {
                            endpoint: "http://127.0.0.1:1".into(),
                            namespace: "default".into(),
                            token: None,
                        },
                        None,
                    )?,
                    namespace: tokio::sync::OnceCell::new(),
                    lock: tokio::sync::Mutex::new(()),
                    pumping: std::sync::atomic::AtomicBool::new(false),
                    alarm: tokio::sync::Mutex::new(None),
                    routes_refreshed: tokio::sync::Mutex::new(0),
                };
                let mut fanout_times = Vec::new();
                for pass in 0..4 {
                    let round = pass % 2;
                    if pass == 2 {
                        // Replay an already-reconciled catalog entry. Reading
                        // those large states must still consume the batch cap.
                        ctx.sql().execute(
                            "INSERT INTO subscription_fanout(path,version,cursor,discover,value) SELECT path,version,'',1,value FROM subscription_catalog WHERE path=?",
                            Some(vec![super::super::db::text(&entry.path)]),
                        ).await?;
                    }
                    let started = Instant::now();
                    let tx = super::super::db::begin(ctx.sql()).await?;
                    coordinator.fanout_batch(&tx, Some(&entry.path)).await?;
                    tx.commit().await?;
                    fanout_times.push(started.elapsed().as_millis());
                    let tx = super::super::db::begin(ctx.sql()).await?;
                    let job = super::super::db::fanout(&tx, Some(&entry.path)).await?;
                    if round == 0 {
                        assert_eq!(job.unwrap().cursor, first_key);
                    } else {
                        assert!(job.is_none());
                    }
                    let updated = tx
                        .execute(
                            "SELECT count(*) FROM subscriptions WHERE json_extract(value,?)=2",
                            Some(vec![super::super::db::text(format!(
                                "$.links.\"{relative}\".version"
                            ))]),
                        )
                        .await?;
                    let expected = if pass < 2 { round + 1 } else { 2 };
                    assert!(
                        matches!(updated.rows[0][0], rivetkit::ColumnValue::Integer(n) if n == expected)
                    );
                    tx.rollback().await?;
                }
                Ok(
                    json!({"streams":sub.links.len(),"save_ms":times,"load_ms":load_times,"fanout_ms":fanout_times,"lease_remaining_ms":lease_remaining,"lease_at_commit_ms":lease_at_commit}),
                )
            })
        }
    }
    fn hash(sub: &Subscription) -> Result<Vec<u8>> {
        struct Writer(Sha256);
        impl std::io::Write for Writer {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.update(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let mut writer = Writer(Sha256::new());
        serde_json::to_writer(&mut writer, sub)?;
        Ok(writer.0.finalize().to_vec())
    }

    #[tokio::test]
    #[ignore = "requires RIVET_TEST_ENDPOINT and matching Engine credentials"]
    async fn maximum_snapshot_persists_through_real_engine() -> Result<()> {
        let endpoint =
            std::env::var("RIVET_TEST_ENDPOINT").context("RIVET_TEST_ENDPOINT required")?;
        let token = std::env::var("RIVET_TOKEN").context("RIVET_TOKEN required")?;
        let namespace = std::env::var("RIVET_NAMESPACE").unwrap_or_else(|_| "default".into());
        let pool = format!("subscription-state-{}", uuid::Uuid::new_v4());
        let mut registry = rivetkit::Registry::new();
        registry.register_actor_with::<Fixture>(
            NAME,
            rivetkit::ActorConfig {
                has_database: true,
                has_state: true,
                action_timeout: Duration::from_secs(180),
                ..Default::default()
            },
        );
        let shutdown = tokio_util::sync::CancellationToken::new();
        let _guard = shutdown.clone().drop_guard();
        let server = tokio::spawn(registry.serve_with_config(
            rivetkit::ServeConfig {
                endpoint: endpoint.clone(),
                token: Some(token.clone()),
                namespace: namespace.clone(),
                pool_name: pool.clone(),
                ..Default::default()
            },
            shutdown.clone(),
        ));
        let client = rivetkit_client::Client::new(
            rivetkit_client::ClientConfig::new(endpoint)
                .token(&token)
                .header("authorization", format!("Bearer {token}"))
                .namespace(namespace)
                .pool_name(pool),
        );
        let actor = client.get_or_create(
            NAME,
            vec![uuid::Uuid::new_v4().to_string()],
            Default::default(),
        )?;
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if actor.action(Ready::NAME, vec![]).await.is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await?;
        let result = actor.action(Persist::NAME, vec![]).await?;
        assert_eq!(result["streams"], 4096);
        eprintln!("Real Engine maximum subscription persistence: {result}");
        shutdown.cancel();
        server.await??;
        Ok(())
    }
}
