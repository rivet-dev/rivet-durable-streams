use super::*;

impl DurableStreamActor {
    pub(super) async fn handle_head(&self, ctx: &Ctx<Self>) -> Result<Response> {
        let Some(meta) = self.visible_meta(ctx, now_ms()).await? else {
            return response(404, [("content-type", "text/plain")], Vec::new());
        };
        if meta.soft_deleted {
            return response(410, [("content-type", "text/plain")], Vec::new());
        }
        let mut headers = metadata_headers(&meta);
        headers.insert("cache-control".to_owned(), "no-store".to_owned());
        headers.insert(
            "etag".to_owned(),
            make_etag(ctx.actor_id(), "-1", &meta.current_offset, meta.closed),
        );
        response(200, headers, Vec::new())
    }

    pub(super) async fn handle_delete(&self, ctx: &Ctx<Self>) -> Result<Response> {
        let _guard = self.lifecycle.lock().await;
        let now = now_ms();
        let Some(meta) = self.visible_meta_while_locked(ctx, now).await? else {
            return text_response(404, "Stream not found", [] as [(&str, &str); 0]);
        };
        if meta.soft_deleted {
            return text_response(410, "Stream is gone", [] as [(&str, &str); 0]);
        }
        self.schedule_maintenance_after(ctx, MAINTENANCE_RETRY_MS)
            .await?;
        let tx = begin_mutation(ctx.sql()).await?;
        let soft_delete = fork_edge_count(&tx).await? > 0;
        if soft_delete {
            set_soft_deleted(&tx).await?;
        } else {
            self.purge_stream_tx(&tx, &meta).await?;
        }
        tx.commit().await?;
        self.notify_change();
        if !soft_delete {
            self.flush_gc_releases(ctx).await?;
        }
        response(204, [] as [(&str, &str); 0], Vec::new())
    }

    pub(super) async fn visible_meta(&self, ctx: &Ctx<Self>, now: i64) -> Result<Option<Meta>> {
        let _guard = self.lifecycle.lock().await;
        self.visible_meta_while_locked(ctx, now).await
    }

    pub(super) async fn visible_meta_while_locked(
        &self,
        ctx: &Ctx<Self>,
        now: i64,
    ) -> Result<Option<Meta>> {
        let Some(meta) = get_meta(ctx.sql()).await? else {
            return Ok(None);
        };
        if !meta.is_expired(now) {
            return Ok(Some(meta));
        }
        self.schedule_maintenance_after(ctx, MAINTENANCE_RETRY_MS)
            .await?;
        let tx = begin_mutation(ctx.sql()).await?;
        let Some(fresh) = get_meta_tx(&tx).await? else {
            tx.rollback().await?;
            return Ok(None);
        };
        if !fresh.is_expired(now) {
            tx.rollback().await?;
            return Ok(Some(fresh));
        }
        if fork_edge_count(&tx).await? > 0 {
            set_soft_deleted(&tx).await?;
            tx.commit().await?;
            self.notify_change();
            return Ok(Some(Meta {
                soft_deleted: true,
                ..fresh
            }));
        }
        self.purge_stream_tx(&tx, &fresh).await?;
        tx.commit().await?;
        self.notify_change();
        self.flush_gc_releases(ctx).await?;
        Ok(None)
    }

    pub(super) async fn touch_access(&self, ctx: &Ctx<Self>, now: i64) -> Result<()> {
        let _guard = self.lifecycle.lock().await;
        let tx = begin_mutation(ctx.sql()).await?;
        touch(&tx, now).await?;
        tx.commit().await
    }

    pub(super) fn notify_change(&self) {
        self.changes
            .send_modify(|version| *version = version.wrapping_add(1));
    }
}
