use super::*;

impl DurableStreamActor {
    pub(super) async fn schedule_maintenance_after(
        &self,
        ctx: &Ctx<Self>,
        delay_ms: i64,
    ) -> Result<()> {
        ctx.schedule()
            .after(
                Duration::from_millis(delay_ms.max(0) as u64),
                Maintenance::NAME,
                &[],
            )
            .await?;
        Ok(())
    }

    pub(super) async fn schedule_expiry(&self, ctx: &Ctx<Self>, meta: &Meta) -> Result<()> {
        if let Some(expiry) = meta.expiry_time() {
            ctx.schedule().at(expiry, Maintenance::NAME, &[]).await?;
        }
        Ok(())
    }

    pub(super) async fn run_maintenance(&self, ctx: &Ctx<Self>) -> Result<()> {
        if get_meta(ctx.sql()).await?.is_none() {
            self.reconcile_fork_intents(ctx).await?;
        }
        self.flush_gc_releases(ctx).await?;
        if let Some(meta) = self.visible_meta(ctx, now_ms()).await?
            && !meta.soft_deleted
        {
            self.schedule_expiry(ctx, &meta).await?;
        }
        Ok(())
    }

    pub(super) async fn reconcile_fork_intents(&self, ctx: &Ctx<Self>) -> Result<()> {
        for intent in pending_fork_intents(ctx.sql()).await? {
            let (_, fork_offset): (String, Option<String>) =
                serde_json::from_str(&intent.params_key)?;
            let acquired = self
                .source_handle(ctx, &intent.parent_path)?
                .action(
                    ForkAcquire::NAME,
                    vec![serde_json::to_value(ForkAcquire {
                        edge_id: intent.edge_id.clone(),
                        fork_offset,
                        content_type_provided: None,
                    })?],
                )
                .await
                .and_then(|value| {
                    serde_json::from_value::<ForkAcquireResult>(value).map_err(Into::into)
                });
            let Ok(acquired) = acquired else {
                self.schedule_maintenance_after(ctx, MAINTENANCE_RETRY_MS)
                    .await?;
                continue;
            };
            let tx = begin_mutation(ctx.sql()).await?;
            if acquired.ok {
                enqueue_gc_release(
                    &tx,
                    &intent.edge_id,
                    &intent.parent_path,
                    acquired.source_generation.as_deref(),
                )
                .await?;
            }
            delete_fork_intent(&tx, &intent.edge_id).await?;
            tx.commit().await?;
        }
        self.flush_gc_releases(ctx).await
    }

    pub(super) async fn flush_gc_releases(&self, ctx: &Ctx<Self>) -> Result<()> {
        let pending = pending_gc_releases(ctx.sql()).await?;
        if pending.is_empty() {
            return Ok(());
        }
        let mut retry = false;
        for release in pending {
            let result = self
                .source_handle(ctx, &release.parent_path)?
                .action(
                    ForkRelease::NAME,
                    vec![serde_json::to_value(ForkRelease {
                        edge_id: release.edge_id.clone(),
                        source_generation: release.source_generation,
                    })?],
                )
                .await;
            if result.is_ok() {
                let tx = begin_mutation(ctx.sql()).await?;
                dequeue_gc_release(&tx, &release.edge_id).await?;
                tx.commit().await?;
            } else {
                retry = true;
            }
        }
        if retry {
            self.schedule_maintenance_after(ctx, MAINTENANCE_RETRY_MS)
                .await?;
        }
        Ok(())
    }

    pub(super) async fn purge_stream_tx(
        &self,
        tx: &rivetkit::SqliteTransaction,
        meta: &Meta,
    ) -> Result<()> {
        if let (Some(parent_path), Some(edge_id)) =
            (meta.forked_from.as_deref(), meta.fork_edge_id.as_deref())
        {
            enqueue_gc_release(tx, edge_id, parent_path, meta.fork_source_gen.as_deref()).await?;
        }
        purge(tx).await
    }
}
