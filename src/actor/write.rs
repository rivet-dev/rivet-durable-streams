use super::*;

impl DurableStreamActor {
    pub(super) async fn handle_put(&self, ctx: &Ctx<Self>, request: &Request) -> Result<Response> {
        if request.body().len() > MAX_BODY_BYTES {
            return text_response(413, "Payload too large", [] as [(&str, &str); 0]);
        }

        let forked_from = public_header_text(request, STREAM_FORKED_FROM)?;
        let fork_offset = public_header_text(request, STREAM_FORK_OFFSET)?;
        let fork_sub_offset_header = public_header_text(request, STREAM_FORK_SUB_OFFSET)?;
        let has_fork = forked_from.is_some();
        if !has_fork && (fork_offset.is_some() || fork_sub_offset_header.is_some()) {
            return text_response(
                400,
                "Stream-Fork-Offset and Stream-Fork-Sub-Offset require Stream-Forked-From",
                [] as [(&str, &str); 0],
            );
        }
        if fork_offset
            .as_deref()
            .is_some_and(|offset| !is_concrete_offset(offset))
        {
            return text_response(
                400,
                "Invalid Stream-Fork-Offset format",
                [] as [(&str, &str); 0],
            );
        }
        let fork_sub_offset = match fork_sub_offset_header.as_deref() {
            Some(value) => match parse_non_negative_safe_integer(value, "fork sub-offset") {
                Ok(value) => Some(value as usize),
                Err(_) => {
                    return text_response(
                        400,
                        "Invalid Stream-Fork-Sub-Offset format",
                        [] as [(&str, &str); 0],
                    );
                }
            },
            None => None,
        };

        let content_type =
            sanitize_content_type(public_header_text(request, "content-type")?, has_fork);
        let ttl_header = public_header_text(request, STREAM_TTL)?;
        let expires_at = public_header_text(request, STREAM_EXPIRES_AT)?;
        if ttl_header.is_some() && expires_at.is_some() {
            return text_response(
                400,
                "Cannot specify both Stream-TTL and Stream-Expires-At",
                [] as [(&str, &str); 0],
            );
        }
        let ttl_seconds = match ttl_header.as_deref() {
            Some(value) => match parse_ttl(value) {
                Ok(value) => Some(value),
                Err(()) => {
                    return text_response(400, "Invalid Stream-TTL value", [] as [(&str, &str); 0]);
                }
            },
            None => None,
        };
        if expires_at
            .as_deref()
            .is_some_and(|value| chrono::DateTime::parse_from_rfc3339(value).is_err())
        {
            return text_response(
                400,
                "Invalid Stream-Expires-At timestamp",
                [] as [(&str, &str); 0],
            );
        }
        let create_closed = header_is_true(request, STREAM_CLOSED)?;
        let now = now_ms();
        let _guard = self.lifecycle.lock().await;
        if let Some(existing) = self.visible_meta_while_locked(ctx, now).await? {
            if existing.soft_deleted {
                return text_response(
                    409,
                    "stream was deleted but still has active forks — path cannot be reused until all forks are removed",
                    [] as [(&str, &str); 0],
                );
            }
            let content_type_matches =
                if has_fork && content_type.is_none() && forked_from == existing.forked_from {
                    true
                } else {
                    normalize_content_type(content_type.as_deref())
                        == normalize_content_type(existing.content_type.as_deref())
                };
            let matches = content_type_matches
                && ttl_seconds == existing.ttl_seconds
                && expires_at == existing.expires_at
                && create_closed == existing.closed
                && forked_from == existing.forked_from
                && fork_offset
                    .as_deref()
                    .is_none_or(|offset| Some(offset) == existing.fork_offset.as_deref())
                && fork_sub_offset.unwrap_or(0)
                    == existing.fork_sub_offset.unwrap_or(0).max(0) as usize;
            if !matches {
                return text_response(
                    409,
                    "Stream already exists with different configuration",
                    [] as [(&str, &str); 0],
                );
            }
            let mut headers = vec![
                (
                    "content-type".to_owned(),
                    existing
                        .content_type
                        .unwrap_or_else(|| "application/octet-stream".to_owned()),
                ),
                (STREAM_OFFSET.to_owned(), existing.current_offset),
            ];
            if existing.closed {
                headers.push((STREAM_CLOSED.to_owned(), "true".to_owned()));
            }
            return response(200, headers, Vec::new());
        }

        if let Some(parent_path) = forked_from.as_deref() {
            return self
                .create_fork(
                    ctx,
                    ForkCreateOptions {
                        parent_path,
                        fork_offset: fork_offset.as_deref(),
                        fork_sub_offset,
                        content_type: content_type.as_deref(),
                        ttl_seconds,
                        expires_at: expires_at.as_deref(),
                        closed: create_closed,
                        body: request.body(),
                        now_ms: now,
                    },
                )
                .await;
        }

        let resolved_content_type = content_type
            .clone()
            .unwrap_or_else(|| "application/octet-stream".to_owned());
        let payload = if request.body().is_empty() {
            Vec::new()
        } else if normalize_content_type(Some(&resolved_content_type)) == "application/json" {
            match process_json_append(request.body(), true) {
                Ok(payload) if payload.len() <= MAX_BODY_BYTES => payload,
                Ok(_) => {
                    return text_response(413, "Payload too large", [] as [(&str, &str); 0]);
                }
                Err(error) => {
                    return text_response(400, error.to_string(), [] as [(&str, &str); 0]);
                }
            }
        } else {
            request.body().clone()
        };

        let tx = begin_mutation(ctx.sql()).await?;
        if get_meta_tx(&tx).await?.is_some() {
            tx.rollback().await?;
            return text_response(409, "Stream already exists", [] as [(&str, &str); 0]);
        }
        create_meta(
            &tx,
            Some(&resolved_content_type),
            ttl_seconds,
            expires_at.as_deref(),
            create_closed,
            now,
            None,
            None,
            None,
            None,
            None,
        )
        .await?;
        let mut current_offset = crate::protocol::ZERO_OFFSET.to_owned();
        if !payload.is_empty() {
            current_offset = store::append_message(&tx, &current_offset, payload, now).await?;
        }
        tx.commit().await?;
        self.notify_change();
        if let Some(meta) = get_meta(ctx.sql()).await? {
            self.schedule_expiry(ctx, &meta).await?;
        }

        let mut headers = vec![
            ("content-type".to_owned(), resolved_content_type),
            (STREAM_OFFSET.to_owned(), current_offset),
            ("location".to_owned(), ctx.state().path.clone()),
        ];
        if create_closed {
            headers.push((STREAM_CLOSED.to_owned(), "true".to_owned()));
        }
        response(201, headers, Vec::new())
    }

    pub(super) async fn handle_post(&self, ctx: &Ctx<Self>, request: &Request) -> Result<Response> {
        if request.body().len() > MAX_BODY_BYTES {
            return text_response(413, "Payload too large", [] as [(&str, &str); 0]);
        }
        let close_stream = header_is_true(request, STREAM_CLOSED)?;
        let producer = match parse_producer(request) {
            Ok(value) => value,
            Err(message) => {
                return text_response(400, message, [] as [(&str, &str); 0]);
            }
        };
        if request.body().is_empty() && close_stream {
            return self.handle_close_only(ctx, producer).await;
        }
        if request.body().is_empty() {
            return text_response(400, "Empty body", [] as [(&str, &str); 0]);
        }
        let Some(content_type) = public_header_text(request, "content-type")? else {
            return text_response(
                400,
                "Content-Type header is required",
                [] as [(&str, &str); 0],
            );
        };
        let seq = public_header_bytes(request, STREAM_SEQ)?;
        let now = now_ms();
        let _guard = self.lifecycle.lock().await;
        let Some(meta) = self.visible_meta_while_locked(ctx, now).await? else {
            return text_response(404, "Stream not found", [] as [(&str, &str); 0]);
        };
        if meta.soft_deleted {
            return text_response(410, "Stream is gone", [] as [(&str, &str); 0]);
        }
        if meta.closed {
            if producer
                .as_ref()
                .is_some_and(|producer| closed_by_matches(&meta, producer))
            {
                return close_success(&meta.current_offset, producer.as_ref());
            }
            return text_response(
                409,
                "Stream is closed",
                [
                    (STREAM_CLOSED, "true"),
                    (STREAM_OFFSET, meta.current_offset.as_str()),
                ],
            );
        }
        if normalize_content_type(Some(&content_type))
            != normalize_content_type(meta.content_type.as_deref())
        {
            return text_response(409, "Content-type mismatch", [] as [(&str, &str); 0]);
        }
        let seq_encoded = seq.as_ref().map(|value| base64_url(value));

        let payload = if normalize_content_type(meta.content_type.as_deref()) == "application/json"
        {
            match process_json_append(request.body(), false) {
                Ok(payload) if payload.len() <= MAX_BODY_BYTES => payload,
                Ok(_) => {
                    return text_response(413, "Payload too large", [] as [(&str, &str); 0]);
                }
                Err(error) => {
                    return text_response(400, error.to_string(), [] as [(&str, &str); 0]);
                }
            }
        } else {
            request.body().clone()
        };

        let tx = begin_mutation(ctx.sql()).await?;
        let Some(fresh_meta) = get_meta_tx(&tx).await? else {
            tx.rollback().await?;
            return text_response(404, "Stream not found", [] as [(&str, &str); 0]);
        };
        let producer_state = if let Some(producer) = &producer {
            let prior =
                get_producer_state(&tx, &producer.id, now.saturating_sub(PRODUCER_STATE_TTL_MS))
                    .await?;
            match validate_producer(prior.as_ref(), producer, now) {
                ProducerValidation::Accepted(state) => Some(state),
                failure => {
                    tx.rollback().await?;
                    return producer_failure(failure, producer, &fresh_meta, false);
                }
            }
        } else {
            None
        };
        // Producer deduplication intentionally precedes Stream-Seq validation:
        // a retry carrying both must resolve as the producer's duplicate 204.
        if let (Some(last), Some(next)) = (&fresh_meta.last_seq, &seq_encoded) {
            let last = decode_base64_url(last)?;
            let next = decode_base64_url(next)?;
            if next <= last {
                tx.rollback().await?;
                return text_response(409, "Sequence conflict", [] as [(&str, &str); 0]);
            }
        }
        let new_offset =
            store::append_message(&tx, &fresh_meta.current_offset, payload, now).await?;
        if let (Some(producer), Some(state)) = (&producer, &producer_state) {
            commit_producer_state(&tx, producer.id.clone(), state).await?;
        }
        if let Some(seq) = seq_encoded {
            set_last_seq(&tx, seq).await?;
        }
        if close_stream {
            let closed_by = producer
                .as_ref()
                .map(|producer| {
                    serde_json::to_string(&ClosedBy {
                        producer_id: producer.id.clone(),
                        epoch: producer.epoch,
                        seq: producer.seq,
                    })
                })
                .transpose()?;
            set_closed(&tx, closed_by).await?;
        }
        touch(&tx, now).await?;
        tx.commit().await?;
        self.notify_change();
        producer_success(&new_offset, producer.as_ref(), close_stream)
    }

    pub(super) async fn handle_close_only(
        &self,
        ctx: &Ctx<Self>,
        producer: Option<ProducerHeaders>,
    ) -> Result<Response> {
        let now = now_ms();
        let _guard = self.lifecycle.lock().await;
        let Some(meta) = self.visible_meta_while_locked(ctx, now).await? else {
            return text_response(404, "Stream not found", [] as [(&str, &str); 0]);
        };
        if meta.soft_deleted {
            return text_response(410, "Stream is gone", [] as [(&str, &str); 0]);
        }
        if producer.is_none() {
            let tx = begin_mutation(ctx.sql()).await?;
            set_closed(&tx, None).await?;
            touch(&tx, now).await?;
            tx.commit().await?;
            self.notify_change();
            return close_success(&meta.current_offset, None);
        }
        let producer = producer.expect("producer checked above");
        if meta.closed {
            if closed_by_matches(&meta, &producer) {
                return close_success(&meta.current_offset, Some(&producer));
            }
            return text_response(
                409,
                "Stream is closed",
                [
                    (STREAM_CLOSED, "true"),
                    (STREAM_OFFSET, meta.current_offset.as_str()),
                ],
            );
        }

        let tx = begin_mutation(ctx.sql()).await?;
        let prior =
            get_producer_state(&tx, &producer.id, now.saturating_sub(PRODUCER_STATE_TTL_MS))
                .await?;
        let state = match validate_producer(prior.as_ref(), &producer, now) {
            ProducerValidation::Accepted(state) => state,
            failure => {
                tx.rollback().await?;
                return producer_failure(failure, &producer, &meta, true);
            }
        };
        commit_producer_state(&tx, producer.id.clone(), &state).await?;
        set_closed(
            &tx,
            Some(serde_json::to_string(&ClosedBy {
                producer_id: producer.id.clone(),
                epoch: producer.epoch,
                seq: producer.seq,
            })?),
        )
        .await?;
        touch(&tx, now).await?;
        tx.commit().await?;
        self.notify_change();
        close_success(&meta.current_offset, Some(&producer))
    }
}
