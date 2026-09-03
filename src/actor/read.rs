use super::*;

impl DurableStreamActor {
    pub(super) fn read_stitched<'a>(
        &'a self,
        ctx: &'a Ctx<Self>,
        meta: &'a Meta,
        after_offset: Option<&'a str>,
        cap_offset: Option<&'a str>,
        limit: usize,
        byte_budget: usize,
    ) -> Pin<Box<dyn Future<Output = Result<ReadBatch>> + Send + 'a>> {
        Box::pin(async move {
            let normalized_after = after_offset.filter(|offset| *offset != "-1");
            let limit = limit.min(crate::protocol::MAX_READ_ROWS);
            let byte_budget = byte_budget.min(crate::protocol::MAX_READ_BYTES);
            let mut messages = Vec::new();
            let mut remaining_bytes = byte_budget;

            if let (Some(parent_path), Some(fork_offset)) =
                (meta.forked_from.as_deref(), meta.fork_offset.as_deref())
                && normalized_after.is_none_or(|offset| offset < fork_offset)
            {
                let inherited_cap = cap_offset
                    .filter(|cap| *cap < fork_offset)
                    .unwrap_or(fork_offset);
                let inherited = self
                    .source_handle(ctx, parent_path)?
                    .action(
                        ReadRange::NAME,
                        vec![serde_json::to_value(ReadRange {
                            after_offset: normalized_after.map(ToOwned::to_owned),
                            cap_offset: inherited_cap.to_owned(),
                            limit: Some(limit),
                            byte_budget: Some(remaining_bytes),
                        })?],
                    )
                    .await
                    .and_then(|value| {
                        serde_json::from_value::<ReadBatch>(value).map_err(Into::into)
                    })?;

                let Some(current) = get_meta(ctx.sql()).await? else {
                    return Ok(ReadBatch::default());
                };
                if current.generation != meta.generation {
                    return self
                        .read_stitched(ctx, &current, after_offset, cap_offset, limit, byte_budget)
                        .await;
                }
                remaining_bytes = remaining_bytes.saturating_sub(
                    inherited
                        .messages
                        .iter()
                        .map(|message| message.data.len())
                        .sum(),
                );
                messages.extend(inherited.messages);
                if inherited.capped
                    || messages.len() >= limit
                    || (remaining_bytes == 0 && !messages.is_empty())
                {
                    return Ok(ReadBatch {
                        messages,
                        capped: true,
                    });
                }
            }

            let own = read_messages_range(
                ctx.sql(),
                normalized_after,
                cap_offset,
                limit.saturating_sub(messages.len()),
                remaining_bytes,
                messages.is_empty(),
            )
            .await?;
            messages.extend(own.messages);
            Ok(ReadBatch {
                messages,
                capped: own.capped,
            })
        })
    }

    pub(super) async fn handle_get(
        self: Arc<Self>,
        ctx: Ctx<Self>,
        request: Request,
    ) -> Result<ActorHttpResponse> {
        let offsets = query_values(&request, "offset");
        if offsets.len() > 1 {
            return text_response(
                400,
                "Multiple offset parameters not allowed",
                [] as [(&str, &str); 0],
            )
            .map(Into::into);
        }
        let offset = offsets.first().map(String::as_str);
        if offset == Some("") {
            return text_response(400, "Empty offset parameter", [] as [(&str, &str); 0])
                .map(Into::into);
        }
        if offset.is_some_and(|value| !validate_offset(value)) {
            return text_response(400, "Invalid offset format", [] as [(&str, &str); 0])
                .map(Into::into);
        }
        let live = query_values(&request, "live").first().cloned();
        if matches!(live.as_deref(), Some("long-poll" | "sse")) && offset.is_none() {
            let mode = if live.as_deref() == Some("sse") {
                "SSE"
            } else {
                "Long-poll"
            };
            return text_response(
                400,
                format!("{mode} requires offset parameter"),
                [] as [(&str, &str); 0],
            )
            .map(Into::into);
        }
        let cursor = query_values(&request, "cursor").first().cloned();
        if live.as_deref() == Some("sse") {
            return self.start_sse(ctx, offset.unwrap_or("-1"), cursor).await;
        }

        let mut changes = self.changes.subscribe();
        let Some(initial_meta) = self.visible_meta(&ctx, now_ms()).await? else {
            return text_response(404, "Stream not found", [] as [(&str, &str); 0]).map(Into::into);
        };
        if initial_meta.soft_deleted {
            return text_response(410, "Stream is gone", [] as [(&str, &str); 0]).map(Into::into);
        }
        if offset == Some("now") && live.as_deref() != Some("long-poll") {
            self.touch_access(&ctx, now_ms()).await?;
            let mut headers = metadata_headers(&initial_meta);
            headers.insert(STREAM_UP_TO_DATE.to_owned(), "true".to_owned());
            headers.insert("cache-control".to_owned(), "no-store".to_owned());
            let body = if normalize_content_type(initial_meta.content_type.as_deref())
                == "application/json"
            {
                b"[]".to_vec()
            } else {
                Vec::new()
            };
            return response(200, headers, body).map(Into::into);
        }

        let effective_offset = if offset == Some("now") {
            Some(initial_meta.current_offset.as_str())
        } else {
            offset
        };
        let mut snapshot = self
            .read_snapshot(
                &ctx,
                effective_offset,
                Some(initial_meta.generation.as_str()),
            )
            .await?
            .ok_or_else(|| anyhow!("stream generation changed during read"))?;
        let caught_up = effective_offset == Some(initial_meta.current_offset.as_str());
        if live.as_deref() == Some("long-poll")
            && caught_up
            && snapshot.batch.messages.is_empty()
            && !snapshot.meta.closed
        {
            let abort = ctx.abort_signal();
            tokio::select! {
                _ = changes.changed() => {}
                _ = tokio::time::sleep(Duration::from_millis(LONG_POLL_TIMEOUT_MS)) => {
                    let mut headers = HashMap::from([
                        (STREAM_OFFSET.to_owned(), initial_meta.current_offset.clone()),
                        (STREAM_UP_TO_DATE.to_owned(), "true".to_owned()),
                        (STREAM_CURSOR.to_owned(), response_cursor(cursor.as_deref())),
                    ]);
                    if get_meta(ctx.sql()).await?.is_some_and(|meta| meta.closed) {
                        headers.insert(STREAM_CLOSED.to_owned(), "true".to_owned());
                    }
                    return response(204, headers, Vec::new()).map(Into::into);
                }
                _ = abort.cancelled() => {
                    return Err(anyhow!("actor stopping"));
                }
            }
            snapshot = match self
                .read_snapshot(
                    &ctx,
                    Some(&initial_meta.current_offset),
                    Some(initial_meta.generation.as_str()),
                )
                .await?
            {
                Some(snapshot) => snapshot,
                None => {
                    return text_response(404, "Stream not found", [] as [(&str, &str); 0])
                        .map(Into::into);
                }
            };
        }
        self.build_read_response(
            ctx.actor_id(),
            &request,
            offset,
            live.as_deref(),
            cursor,
            snapshot,
        )
        .map(Into::into)
    }

    pub(super) fn build_read_response(
        &self,
        actor_id: &str,
        request: &Request,
        offset: Option<&str>,
        live: Option<&str>,
        cursor: Option<String>,
        snapshot: ReadSnapshot,
    ) -> Result<Response> {
        let last = snapshot.batch.messages.last();
        let response_offset = last
            .map(|message| message.offset.clone())
            .unwrap_or_else(|| snapshot.meta.current_offset.clone());
        let up_to_date = !snapshot.batch.capped;
        let at_tail = response_offset == snapshot.meta.current_offset;
        let closed = snapshot.meta.closed && at_tail && up_to_date;
        if live == Some("long-poll") && snapshot.batch.messages.is_empty() && closed {
            return response(
                204,
                [
                    (STREAM_OFFSET.to_owned(), response_offset),
                    (STREAM_UP_TO_DATE.to_owned(), "true".to_owned()),
                    (STREAM_CLOSED.to_owned(), "true".to_owned()),
                ],
                Vec::new(),
            );
        }

        let mut headers = metadata_headers(&snapshot.meta);
        headers.insert(STREAM_OFFSET.to_owned(), response_offset.clone());
        if live == Some("long-poll") {
            headers.insert(STREAM_CURSOR.to_owned(), response_cursor(cursor.as_deref()));
        }
        if up_to_date {
            headers.insert(STREAM_UP_TO_DATE.to_owned(), "true".to_owned());
        }
        if !closed {
            headers.remove(STREAM_CLOSED);
        }
        let etag = make_etag(actor_id, offset.unwrap_or("-1"), &response_offset, closed);
        if public_header_text(request, "if-none-match")?.as_deref() == Some(&etag) {
            return response(304, [("etag", etag)], Vec::new());
        }
        headers.insert("etag".to_owned(), etag);
        if live != Some("long-poll") {
            headers.insert(
                "cache-control".to_owned(),
                "public, max-age=60, stale-while-revalidate=300".to_owned(),
            );
        }
        let fragments = snapshot
            .batch
            .messages
            .into_iter()
            .map(|message| message.data)
            .collect::<Vec<_>>();
        let body = if normalize_content_type(snapshot.meta.content_type.as_deref())
            == "application/json"
        {
            format_json_messages(&fragments)
        } else {
            concatenate(&fragments)
        };
        response(200, headers, body)
    }

    pub(super) async fn start_sse(
        self: Arc<Self>,
        ctx: Ctx<Self>,
        offset: &str,
        cursor: Option<String>,
    ) -> Result<ActorHttpResponse> {
        let Some(meta) = self.visible_meta(&ctx, now_ms()).await? else {
            return text_response(404, "Stream not found", [] as [(&str, &str); 0]).map(Into::into);
        };
        if meta.soft_deleted {
            return text_response(410, "Stream is gone", [] as [(&str, &str); 0]).map(Into::into);
        }
        let initial_offset = if offset == "now" {
            meta.current_offset.clone()
        } else {
            offset.to_owned()
        };
        let permit = match Arc::clone(&self.live_readers).try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                return text_response(429, "Too many live readers", [("retry-after", "1")])
                    .map(Into::into);
            }
        };
        let is_json = normalize_content_type(meta.content_type.as_deref()) == "application/json";
        let is_text = meta
            .content_type
            .as_deref()
            .map(|value| normalize_content_type(Some(value)))
            .is_some_and(|value| value.starts_with("text/"));
        let use_base64 = !is_text && !is_json;
        let (tx, rx) = mpsc::channel(8);
        let actor = Arc::clone(&self);
        let task_ctx = ctx.clone();
        ctx.register_task(async move {
            let _permit = permit;
            if let Err(error) = actor
                .pump_sse(
                    task_ctx,
                    meta.generation,
                    initial_offset,
                    cursor,
                    use_base64,
                    is_json,
                    tx.clone(),
                )
                .await
            {
                let _ = tx.send(ResponseChunk::Error(error.to_string())).await;
            }
        });
        let mut headers = HashMap::from([
            ("content-type".to_owned(), "text/event-stream".to_owned()),
            ("cache-control".to_owned(), "no-cache, no-store".to_owned()),
            ("connection".to_owned(), "keep-alive".to_owned()),
            ("content-encoding".to_owned(), "identity".to_owned()),
        ]);
        if use_base64 {
            headers.insert(SSE_DATA_ENCODING.to_owned(), "base64".to_owned());
        }
        Ok(StreamingResponse::from_parts(200, headers, rx)?.into())
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn pump_sse(
        self: Arc<Self>,
        ctx: Ctx<Self>,
        generation: String,
        mut offset: String,
        cursor: Option<String>,
        use_base64: bool,
        is_json: bool,
        tx: mpsc::Sender<ResponseChunk>,
    ) -> Result<()> {
        let started_at = Instant::now();
        let mut changes = self.changes.subscribe();
        loop {
            let Some(snapshot) = self
                .read_snapshot(
                    &ctx,
                    Some(if offset == "-1" { "-1" } else { &offset }),
                    Some(&generation),
                )
                .await?
            else {
                return send_finish(&tx, Vec::new()).await;
            };
            let mut frame = String::new();
            if !snapshot.batch.messages.is_empty() {
                let fragments = snapshot
                    .batch
                    .messages
                    .iter()
                    .map(|message| message.data.clone())
                    .collect::<Vec<_>>();
                let bytes = if is_json {
                    format_json_messages(&fragments)
                } else {
                    concatenate(&fragments)
                };
                let data = if use_base64 {
                    STANDARD.encode(bytes)
                } else {
                    String::from_utf8_lossy(&bytes).into_owned()
                };
                frame.push_str("event: data\n");
                frame.push_str(&encode_sse_data(&data));
            }
            let control_offset = snapshot
                .batch
                .messages
                .last()
                .map(|message| message.offset.clone())
                .unwrap_or_else(|| snapshot.meta.current_offset.clone());
            let at_tail = !snapshot.batch.capped && control_offset == snapshot.meta.current_offset;
            let closed = snapshot.meta.closed && at_tail;
            let control = if closed {
                serde_json::json!({
                    "streamNextOffset": control_offset,
                    "streamClosed": true,
                })
            } else {
                serde_json::json!({
                    "streamNextOffset": control_offset,
                    "streamCursor": response_cursor(cursor.as_deref()),
                    "upToDate": !snapshot.batch.capped,
                })
            };
            frame.push_str("event: control\n");
            frame.push_str(&encode_sse_data(&serde_json::to_string(&control)?));
            if closed {
                return send_finish(&tx, frame.into_bytes()).await;
            }
            if tx
                .send(ResponseChunk::Data {
                    data: frame.into_bytes(),
                    finish: false,
                })
                .await
                .is_err()
            {
                return Ok(());
            }
            offset = control_offset;
            if snapshot.batch.capped {
                continue;
            }
            if started_at.elapsed() >= Duration::from_millis(MAX_SSE_LIFETIME_MS) {
                return send_finish(&tx, Vec::new()).await;
            }
            let abort = ctx.abort_signal();
            tokio::select! {
                _ = changes.changed() => {}
                _ = tokio::time::sleep(Duration::from_millis(LONG_POLL_TIMEOUT_MS)) => {}
                _ = abort.cancelled() => return Ok(()),
            }
        }
    }

    pub(super) async fn read_snapshot(
        &self,
        ctx: &Ctx<Self>,
        offset: Option<&str>,
        expected_generation: Option<&str>,
    ) -> Result<Option<ReadSnapshot>> {
        let _reader = Arc::clone(&self.readers)
            .try_acquire_owned()
            .map_err(|_| anyhow!("too many concurrent reads"))?;
        let Some(meta) = self.visible_meta(ctx, now_ms()).await? else {
            return Ok(None);
        };
        if expected_generation.is_some_and(|generation| generation != meta.generation) {
            return Ok(None);
        }
        let batch = self
            .read_stitched(
                ctx,
                &meta,
                offset,
                None,
                crate::protocol::MAX_READ_ROWS,
                crate::protocol::MAX_READ_BYTES,
            )
            .await?;
        let Some(fresh) = get_meta(ctx.sql()).await? else {
            return Ok(None);
        };
        if fresh.generation != meta.generation {
            return Ok(None);
        }
        self.touch_access(ctx, now_ms()).await?;
        Ok(Some(ReadSnapshot { meta: fresh, batch }))
    }
}
