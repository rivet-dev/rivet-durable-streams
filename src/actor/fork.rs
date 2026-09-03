use super::*;

impl DurableStreamActor {
    pub(super) fn source_handle(&self, ctx: &Ctx<Self>, path: &str) -> Result<ClientActorHandle> {
        let state = ctx.state();
        let key = stream_actor_key(&state.tenant_scope, path);
        ctx.client()?.get_or_create(
            ACTOR_NAME,
            vec![key],
            GetOrCreateOptions {
                create_with_input: Some(serde_json::json!({
                    "path": path,
                    "tenant_scope": state.tenant_scope.clone(),
                })),
                ..GetOrCreateOptions::default()
            },
        )
    }

    pub(super) async fn fork_acquire(
        &self,
        ctx: &Ctx<Self>,
        action: ForkAcquire,
    ) -> Result<ForkAcquireResult> {
        if action.edge_id.is_empty() {
            return Err(anyhow!("fork acquire requires a non-empty edge id"));
        }
        let _guard = self.lifecycle.lock().await;
        let tx = begin_mutation(ctx.sql()).await?;
        if let Some(recorded_offset) = get_fork_edge_tx(&tx, &action.edge_id).await?
            && let Some(meta) = get_meta_tx(&tx).await?
        {
            tx.commit().await?;
            return Ok(ForkAcquireResult::success(recorded_offset, &meta));
        }
        tx.rollback().await?;

        let Some(meta) = self.visible_meta_while_locked(ctx, now_ms()).await? else {
            return Ok(ForkAcquireResult::failure("not_found"));
        };
        if meta.soft_deleted {
            return Ok(ForkAcquireResult::failure("soft_deleted"));
        }
        if action
            .content_type_provided
            .as_deref()
            .is_some_and(|provided| {
                !provided.trim().is_empty()
                    && normalize_content_type(Some(provided))
                        != normalize_content_type(meta.content_type.as_deref())
            })
        {
            return Ok(ForkAcquireResult::failure("content_type_mismatch"));
        }
        let fork_offset = action
            .fork_offset
            .unwrap_or_else(|| meta.current_offset.clone());
        if !is_concrete_offset(&fork_offset)
            || fork_offset.as_str() < ZERO_OFFSET
            || fork_offset > meta.current_offset
        {
            return Ok(ForkAcquireResult::failure("invalid_offset"));
        }

        let tx = begin_mutation(ctx.sql()).await?;
        let recorded_offset = insert_fork_edge(&tx, &action.edge_id, &fork_offset).await?;
        tx.commit().await?;
        Ok(ForkAcquireResult::success(recorded_offset, &meta))
    }

    pub(super) async fn fork_release(&self, ctx: &Ctx<Self>, action: ForkRelease) -> Result<()> {
        if action.edge_id.is_empty() {
            return Err(anyhow!("fork release requires a non-empty edge id"));
        }
        let _guard = self.lifecycle.lock().await;
        let Some(meta) = get_meta(ctx.sql()).await? else {
            return Ok(());
        };
        if action
            .source_generation
            .as_deref()
            .is_some_and(|generation| generation != meta.generation)
        {
            return Ok(());
        }
        if meta.soft_deleted {
            self.schedule_maintenance_after(ctx, MAINTENANCE_RETRY_MS)
                .await?;
        }

        let tx = begin_mutation(ctx.sql()).await?;
        if get_fork_edge_tx(&tx, &action.edge_id).await?.is_none() {
            tx.rollback().await?;
            return Ok(());
        }
        delete_fork_edge(&tx, &action.edge_id).await?;
        let purge_after_release = meta.soft_deleted && fork_edge_count(&tx).await? == 0;
        if purge_after_release {
            self.purge_stream_tx(&tx, &meta).await?;
        }
        tx.commit().await?;
        self.notify_change();
        if purge_after_release {
            self.flush_gc_releases(ctx).await?;
        }
        Ok(())
    }

    pub(super) async fn create_fork(
        &self,
        ctx: &Ctx<Self>,
        options: ForkCreateOptions<'_>,
    ) -> Result<Response> {
        if options.parent_path == ctx.state().path {
            return text_response(404, "Source stream not found", [] as [(&str, &str); 0]);
        }

        let params_key = serde_json::to_string(&(
            options.parent_path,
            options.fork_offset.map(ToOwned::to_owned),
        ))?;
        let edge_id = match get_fork_intent(ctx.sql(), &params_key).await? {
            Some(edge_id) => edge_id,
            None => {
                let edge_id = uuid::Uuid::new_v4().to_string();
                // Arm recovery before persisting the duty. A stray maintenance
                // event is harmless; an acquired reference without one is not.
                self.schedule_maintenance_after(ctx, MAINTENANCE_RETRY_MS)
                    .await?;
                let tx = begin_mutation(ctx.sql()).await?;
                put_fork_intent(&tx, &edge_id, options.parent_path, &params_key).await?;
                tx.commit().await?;
                edge_id
            }
        };

        let acquired = self
            .source_handle(ctx, options.parent_path)?
            .action(
                ForkAcquire::NAME,
                vec![serde_json::to_value(ForkAcquire {
                    edge_id: edge_id.clone(),
                    fork_offset: options.fork_offset.map(ToOwned::to_owned),
                    content_type_provided: options.content_type.map(ToOwned::to_owned),
                })?],
            )
            .await
            .and_then(|value| {
                serde_json::from_value::<ForkAcquireResult>(value).map_err(Into::into)
            })?;
        if !acquired.ok {
            let tx = begin_mutation(ctx.sql()).await?;
            delete_fork_intent(&tx, &edge_id).await?;
            tx.commit().await?;
            return match acquired.error.as_deref() {
                Some("not_found") => {
                    text_response(404, "Source stream not found", [] as [(&str, &str); 0])
                }
                Some("soft_deleted") => text_response(
                    409,
                    "source stream was deleted but still has active forks",
                    [] as [(&str, &str); 0],
                ),
                Some("content_type_mismatch") => text_response(
                    409,
                    "Content type mismatch with source stream",
                    [] as [(&str, &str); 0],
                ),
                Some("invalid_offset") => text_response(
                    400,
                    "Fork offset beyond source stream length",
                    [] as [(&str, &str); 0],
                ),
                _ => Err(anyhow!("source returned an invalid fork acquire result")),
            };
        }

        let fork_offset = acquired
            .fork_offset
            .clone()
            .ok_or_else(|| anyhow!("source omitted acquired fork offset"))?;
        let source_generation = acquired
            .source_generation
            .clone()
            .ok_or_else(|| anyhow!("source omitted generation"))?;
        let resolved_content_type = options
            .content_type
            .map(ToOwned::to_owned)
            .or(acquired.content_type.clone())
            .unwrap_or_else(|| "application/octet-stream".to_owned());
        let is_json = normalize_content_type(Some(&resolved_content_type)) == "application/json";
        let (effective_ttl, effective_expires_at) =
            if options.ttl_seconds.is_none() && options.expires_at.is_none() {
                if acquired.ttl_seconds.is_some() {
                    (acquired.ttl_seconds, None)
                } else {
                    (None, acquired.expires_at.clone())
                }
            } else {
                (
                    options.ttl_seconds,
                    options.expires_at.map(ToOwned::to_owned),
                )
            };

        let mut sub_offset_prefix = None;
        if let Some(sub_offset) = options.fork_sub_offset.filter(|value| *value > 0) {
            let first = self
                .source_handle(ctx, options.parent_path)?
                .action(
                    ReadRange::NAME,
                    vec![serde_json::to_value(ReadRange {
                        after_offset: Some(fork_offset.clone()),
                        cap_offset: MAX_OFFSET_CAP.to_owned(),
                        limit: Some(1),
                        byte_budget: Some(crate::protocol::MAX_READ_BYTES),
                    })?],
                )
                .await
                .and_then(|value| serde_json::from_value::<ReadBatch>(value).map_err(Into::into))?
                .messages
                .into_iter()
                .next();
            let Some(first) = first else {
                self.release_acquired(ctx, &edge_id, options.parent_path, Some(&source_generation))
                    .await?;
                return text_response(400, "Invalid fork sub-offset", [] as [(&str, &str); 0]);
            };
            let prefix = if is_json {
                let mut fragment = first.data;
                if fragment.last() == Some(&b',') {
                    fragment.pop();
                }
                let mut wrapped = Vec::with_capacity(fragment.len() + 2);
                wrapped.push(b'[');
                wrapped.extend(fragment);
                wrapped.push(b']');
                let values = serde_json::from_slice::<Vec<serde_json::Value>>(&wrapped).ok();
                let Some(values) = values.filter(|values| sub_offset <= values.len()) else {
                    self.release_acquired(
                        ctx,
                        &edge_id,
                        options.parent_path,
                        Some(&source_generation),
                    )
                    .await?;
                    return text_response(400, "Invalid fork sub-offset", [] as [(&str, &str); 0]);
                };
                let mut prefix = Vec::new();
                for value in values.into_iter().take(sub_offset) {
                    serde_json::to_writer(&mut prefix, &value)?;
                    prefix.push(b',');
                }
                prefix
            } else if sub_offset <= first.data.len() {
                first.data[..sub_offset].to_vec()
            } else {
                self.release_acquired(ctx, &edge_id, options.parent_path, Some(&source_generation))
                    .await?;
                return text_response(400, "Invalid fork sub-offset", [] as [(&str, &str); 0]);
            };
            if !prefix.is_empty() {
                sub_offset_prefix = Some(prefix);
            }
        }

        let initial_payload = if options.body.is_empty() {
            None
        } else if is_json {
            match process_json_append(options.body, true) {
                Ok(payload) if payload.len() <= MAX_BODY_BYTES => Some(payload),
                Ok(_) => {
                    self.release_acquired(
                        ctx,
                        &edge_id,
                        options.parent_path,
                        Some(&source_generation),
                    )
                    .await?;
                    return text_response(413, "Payload too large", [] as [(&str, &str); 0]);
                }
                Err(error) => {
                    self.release_acquired(
                        ctx,
                        &edge_id,
                        options.parent_path,
                        Some(&source_generation),
                    )
                    .await?;
                    return text_response(400, error.to_string(), [] as [(&str, &str); 0]);
                }
            }
        } else {
            Some(options.body.to_vec())
        };

        let creation = async {
            let tx = begin_mutation(ctx.sql()).await?;
            if get_meta_tx(&tx).await?.is_some() {
                tx.rollback().await?;
                return Err(anyhow!("stream was created during fork acquisition"));
            }
            create_meta(
                &tx,
                Some(&resolved_content_type),
                effective_ttl,
                effective_expires_at.as_deref(),
                options.closed,
                options.now_ms,
                Some(options.parent_path),
                Some(&fork_offset),
                options
                    .fork_sub_offset
                    .filter(|value| *value > 0)
                    .map(|value| value as i64),
                Some(&edge_id),
                Some(&source_generation),
            )
            .await?;
            let mut current_offset = fork_offset.clone();
            if let Some(prefix) = sub_offset_prefix {
                current_offset =
                    store::append_message(&tx, &current_offset, prefix, options.now_ms).await?;
            }
            if let Some(payload) = initial_payload
                && !payload.is_empty()
            {
                current_offset =
                    store::append_message(&tx, &current_offset, payload, options.now_ms).await?;
            }
            delete_fork_intent(&tx, &edge_id).await?;
            tx.commit().await?;
            Ok::<_, anyhow::Error>(current_offset)
        }
        .await;
        let current_offset = match creation {
            Ok(offset) => offset,
            Err(error) => {
                self.release_acquired(ctx, &edge_id, options.parent_path, Some(&source_generation))
                    .await?;
                return Err(error);
            }
        };
        self.notify_change();
        if let Some(meta) = get_meta(ctx.sql()).await? {
            self.schedule_expiry(ctx, &meta).await?;
        }
        let mut headers = vec![
            ("content-type".to_owned(), resolved_content_type),
            (STREAM_OFFSET.to_owned(), current_offset),
            ("location".to_owned(), ctx.state().path.clone()),
        ];
        if options.closed {
            headers.push((STREAM_CLOSED.to_owned(), "true".to_owned()));
        }
        response(201, headers, Vec::new())
    }

    pub(super) async fn release_acquired(
        &self,
        ctx: &Ctx<Self>,
        edge_id: &str,
        parent_path: &str,
        source_generation: Option<&str>,
    ) -> Result<()> {
        self.schedule_maintenance_after(ctx, MAINTENANCE_RETRY_MS)
            .await?;
        let tx = begin_mutation(ctx.sql()).await?;
        enqueue_gc_release(&tx, edge_id, parent_path, source_generation).await?;
        delete_fork_intent(&tx, edge_id).await?;
        tx.commit().await?;
        self.flush_gc_releases(ctx).await
    }
}
