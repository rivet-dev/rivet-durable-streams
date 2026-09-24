use super::*;

use crate::subscriptions::model::CatalogEntry;

impl DurableStreamActor {
    pub(super) fn defer_subscription_flush(self: &Arc<Self>, ctx: &Ctx<Self>) {
        let actor = self.clone();
        let work_ctx = ctx.clone();
        // Wake delivery returns once its append commits. Publishing back to
        // the coordinator runs independently, outside the stream lifecycle lock.
        ctx.wait_until(async move { actor.flush_subscriptions_best_effort(&work_ctx).await });
    }

    pub(super) async fn arm_subscription_recovery(&self, ctx: &Ctx<Self>) -> Result<()> {
        let mut armed = self.subscription_recovery_armed.lock().await;
        if !*armed {
            self.schedule_maintenance_after(ctx, MAINTENANCE_RETRY_MS)
                .await?;
            *armed = true;
        }
        Ok(())
    }

    pub(super) async fn subscription_snapshot(
        &self,
        ctx: &Ctx<Self>,
    ) -> Result<SubscriptionSnapshotResult> {
        let _guard = self.lifecycle.lock().await;
        self.visible_meta_while_locked(ctx, now_ms()).await?;
        let tenant_scope = ctx.state().tenant_scope.clone();
        Ok(SubscriptionSnapshotResult {
            tenant_scope,
            entry: store::subscription_outbox(ctx.sql())
                .await?
                .map(|outbox| catalog_entry(ctx, outbox)),
        })
    }

    pub(super) async fn flush_subscriptions_best_effort(&self, ctx: &Ctx<Self>) {
        if let Err(error) = self.flush_subscriptions(ctx).await {
            // Stream writes already committed. A temporary coordinator failure
            // leaves the durable intent for the pre-armed maintenance event.
            tracing::warn!(
                ?error,
                actor_id = ctx.actor_id(),
                "subscription catalog notification deferred"
            );
            if let Err(error) = self.arm_subscription_recovery(ctx).await {
                tracing::warn!(
                    ?error,
                    actor_id = ctx.actor_id(),
                    "subscription recovery scheduling failed"
                );
            }
        }
    }

    pub(super) async fn flush_subscriptions(&self, ctx: &Ctx<Self>) -> Result<()> {
        let mut retry_after = self.subscription_flush.lock().await;
        if retry_after.is_some_and(|deadline| Instant::now() < deadline) {
            // Queued appends share one failed attempt instead of each waiting
            // for a coordinator timeout during an outage.
            return self.arm_subscription_recovery(ctx).await;
        }
        let result = self.publish_subscription_outbox(ctx).await;
        *retry_after = result
            .is_err()
            .then(|| Instant::now() + Duration::from_millis(MAINTENANCE_RETRY_MS as u64));
        result
    }

    async fn publish_subscription_outbox(&self, ctx: &Ctx<Self>) -> Result<()> {
        let entry = {
            let _guard = self.lifecycle.lock().await;
            let Some(outbox) = store::subscription_outbox(ctx.sql()).await? else {
                return Ok(());
            };
            if !outbox.dirty {
                return Ok(());
            }
            catalog_entry(ctx, outbox)
        };
        self.arm_subscription_recovery(ctx).await?;
        let tenant_scope = ctx.state().tenant_scope.clone();
        let handle = self.client.get_or_create(
            "durableStreamSubscriptions",
            vec![stream_actor_key(&tenant_scope, "__ds/subscriptions")],
            GetOrCreateOptions {
                create_with_input: Some(serde_json::json!({
                    "tenant_scope": tenant_scope,
                })),
                ..GetOrCreateOptions::default()
            },
        )?;
        tokio::time::timeout(
            Duration::from_secs(5),
            handle.action(
                "__durableStreamCatalogChanged",
                vec![serde_json::to_value(&entry)?],
            ),
        )
        .await??;
        // A notification racing a new write must not clear that write's
        // intent. That writer's own flush (or maintenance) publishes it next.
        store::acknowledge_subscription_outbox(ctx.sql(), entry.version).await
    }
}

fn catalog_entry(ctx: &Ctx<DurableStreamActor>, outbox: store::SubscriptionOutbox) -> CatalogEntry {
    CatalogEntry {
        path: ctx.state().path.clone(),
        version: outbox.version,
        incarnation: outbox.incarnation,
        tail: outbox.tail,
        start: outbox.start,
        exists: outbox.exists,
        wakes: outbox.wakes,
    }
}

pub(super) fn wake_request(action: AppendWake) -> Result<Request> {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let headers = [
        ("content-type", "application/json".to_owned()),
        (PRODUCER_ID, format!("__ds_wake_{}", action.wake_id)),
        (PRODUCER_EPOCH, "0".to_owned()),
        (PRODUCER_SEQ, "0".to_owned()),
    ]
    .into_iter()
    .map(|(name, value)| {
        (
            format!("{}{name}", crate::protocol::INTERNAL_HEADER_PREFIX),
            URL_SAFE_NO_PAD.encode(value),
        )
    })
    .collect();
    Request::from_parts(
        "POST",
        "/protocol",
        headers,
        serde_json::to_vec(&action.event)?,
    )
}
