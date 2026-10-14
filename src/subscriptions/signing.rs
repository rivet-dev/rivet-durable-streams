//! Engine I/O is deliberately outside the coordinator's mutation lock.
use super::*;

pub(super) enum Authorization {
    Engine(VerifiedCapability),
    Legacy(legacy::LegacyKeys),
}

impl SubscriptionActor {
    pub(super) async fn namespace_id(&self, ctx: &Ctx<Self>) -> ApiResult<String> {
        self.namespace
            .get_or_try_init(|| async {
                let namespace = self.engine.namespace_id().await.map_err(engine_failure)?;
                let _guard = self.lock.lock().await;
                let rows = ctx
                    .sql()
                    .query(
                        "SELECT namespace FROM subscription_engine_identity WHERE id=1",
                        None,
                    )
                    .await
                    .map_err(internal)?;
                if let Some(row) = rows.rows.first() {
                    if db::string(row, 0).map_err(internal)? != namespace {
                        return Err(ApiError::new(
                            503,
                            "ENGINE_NAMESPACE_CHANGED",
                            "Configured Engine namespace does not match subscription storage",
                        ));
                    }
                } else {
                    ctx.sql()
                        .execute(
                            "INSERT INTO subscription_engine_identity(id,namespace) VALUES(1,?)",
                            Some(vec![text(&namespace)]),
                        )
                        .await
                        .map_err(internal)?;
                }
                Ok(namespace)
            })
            .await
            .cloned()
    }

    pub(super) async fn signing_metadata(
        &self,
        ctx: &Ctx<Self>,
        base: &str,
        webhook: bool,
    ) -> ApiResult<Value> {
        if !webhook {
            return Ok(Value::Null);
        }
        let namespace = self.namespace_id(ctx).await?;
        let metadata = self
            .engine
            .metadata(&namespace)
            .await
            .map_err(engine_failure)?;
        Ok(
            json!({"alg":metadata.alg,"kid":metadata.kid,"jwks_url":format!("{base}/__ds/jwks.json")}),
        )
    }

    pub(super) async fn public_keys(&self, ctx: &Ctx<Self>) -> ApiResult<Reply> {
        let namespace = self.namespace_id(ctx).await?;
        let mut fresh = self
            .engine
            .webhook_jwks(&namespace)
            .await
            .map_err(engine_failure)?;
        if fresh.age_secs >= 300 {
            return Err(ApiError::new(
                503,
                "SIGNING_DISCOVERY_STALE",
                "Engine signing discovery exceeded its freshness budget",
            ));
        }
        let old = {
            let _guard = self.lock.lock().await;
            db::legacy(ctx.sql()).await.map_err(internal)?
        };
        if let Some(old) = old {
            let keys = fresh
                .value
                .get_mut("keys")
                .and_then(Value::as_array_mut)
                .ok_or_else(|| {
                    ApiError::new(
                        503,
                        "SIGNING_DISCOVERY_INVALID",
                        "Invalid Engine key discovery",
                    )
                })?;
            for key in old.public_jwks(now_ms()) {
                if !keys.iter().any(|k| k["kid"] == key["kid"]) {
                    keys.push(key);
                }
            }
        }
        let mut reply = Reply::ok(200, fresh.value);
        reply.age = Some(fresh.age_secs);
        Ok(reply)
    }

    pub(super) async fn prepare_authorization(
        &self,
        ctx: &Ctx<Self>,
        request: &Control,
    ) -> ApiResult<Option<Authorization>> {
        if !matches!(request.operation.as_str(), "callback" | "ack" | "release") {
            return Ok(None);
        }
        let token = request.token.as_deref().unwrap_or("");
        // This exact header was the only format emitted by the old implementation.
        // Never fall back to legacy verification after an Engine verification failure.
        if token.starts_with("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.") {
            let _guard = self.lock.lock().await;
            return db::legacy(ctx.sql())
                .await
                .map_err(internal)?
                .map(|keys| Some(Authorization::Legacy(keys)))
                .ok_or_else(|| {
                    ApiError::new(
                        401,
                        "TOKEN_INVALID",
                        "Legacy capability verification has ended",
                    )
                });
        }
        let namespace = self.namespace_id(ctx).await?;
        let kind = if request.operation == "callback" {
            CapabilityKind::Callback
        } else {
            CapabilityKind::Claim
        };
        let verified = self
            .engine
            .verify(token, &namespace, kind, &request.operation, now_ms())
            .await
            .map_err(engine_failure)?;
        if verified.claims.tenant_scope != ctx.state().tenant_scope
            || verified.claims.root != request.public_base
        {
            return Err(ApiError::new(
                401,
                "TOKEN_INVALID",
                "Capability is for a different tenant or stream root",
            ));
        }
        Ok(Some(Authorization::Engine(verified)))
    }

    pub(super) async fn claim(
        &self,
        ctx: &Ctx<Self>,
        snapshot: Subscription,
        request: &Control,
    ) -> ApiResult<Reply> {
        if snapshot.config.kind != DeliveryType::PullWake {
            return Err(ApiError::invalid("Claim requires pull-wake subscription"));
        }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Claim {
            worker: String,
        }
        let claim: Claim = serde_json::from_value(request.body.clone())
            .map_err(|_| ApiError::invalid("worker is required"))?;
        if claim.worker.is_empty() || claim.worker.len() > 256 {
            return Err(ApiError::invalid("Invalid worker"));
        }
        let wake = snapshot
            .wake
            .as_ref()
            .ok_or_else(|| ApiError::new(409, "NO_PENDING_WORK", "No pending work"))?;
        if let Some(holder) = &wake.holder {
            return Ok(already_claimed(holder, wake.generation));
        }
        let deadline = request.deadline.unwrap_or_else(|| now_ms() + 14_000);
        let issued = tokio::time::timeout(claim_budget(deadline)?, async {
            let namespace = self.namespace_id(ctx).await?;
            let claims = capability_claims(&snapshot, &ctx.state().tenant_scope)?;
            self.engine
                .issue(&claims, CapabilityKind::Claim, &namespace)
                .await
                .map_err(engine_failure)
        })
        .await
        .map_err(|_| {
            ApiError::new(
                503,
                "ENGINE_UNAVAILABLE",
                "Engine capability issuance timed out",
            )
        })??;
        fault::pause("after_application_issuance").await;
        let _guard = self.lock.lock().await;
        self.arm(ctx, now_ms() + 1_000).await.map_err(internal)?;
        // A slow issuer or lock wait must not acquire a lease after the caller
        // has already timed out. Bound the commit phase by the same request budget.
        let tx = ctx
            .sql()
            .begin_transaction(Some(claim_budget(deadline)?))
            .await
            .map_err(internal)?;
        let mut sub = db::load(&tx, &db::key(&snapshot.root, &snapshot.id))
            .await
            .map_err(internal)?
            .ok_or_else(ApiError::fenced)?;
        let mut changed = self
            .reconcile_subscription(&tx, &mut sub)
            .await
            .map_err(internal)?;
        changed |= sub.expire(now_ms());
        changed |= sub.ensure_wake(now_ms());
        let mut acquired = false;
        let result = if sub.deleted || !same_wake(&sub, &snapshot) {
            Err(ApiError::fenced())
        } else if let Some(holder) = &sub.wake.as_ref().unwrap().holder {
            Ok(already_claimed(
                holder,
                sub.wake.as_ref().unwrap().generation,
            ))
        } else if issued.expires_ts <= now_ms() {
            Err(ApiError::new(
                401,
                "TOKEN_EXPIRED",
                "Engine capability expired before the claim completed",
            ))
        } else {
            let w = sub.wake.as_mut().unwrap();
            w.holder = Some(claim.worker);
            w.token_expires = issued.expires_ts;
            w.capability = Some(issued.token.clone());
            w.deadline = Some((now_ms() + sub.config.lease_ttl_ms).min(issued.expires_ts));
            changed = true;
            acquired = true;
            Ok(Reply::ok(
                200,
                json!({"wake_id":w.id,"generation":w.generation,"token":issued.token,"streams":sub.infos(),"lease_ttl_ms":sub.config.lease_ttl_ms}),
            ))
        };
        if changed {
            if acquired {
                db::save_with_lease(&tx, &mut sub).await.map_err(internal)?;
            } else {
                db::save(&tx, &sub).await.map_err(internal)?;
            }
        }
        if acquired {
            claim_budget(deadline)?;
            require_live_lease(&sub)?;
        }
        tx.commit().await.map_err(internal)?;
        result
    }

    pub(super) async fn webhook_capability(
        &self,
        ctx: &Ctx<Self>,
        snapshot: &Subscription,
    ) -> ApiResult<String> {
        let wake = snapshot.wake.as_ref().ok_or_else(ApiError::fenced)?;
        if let Some(token) = &wake.capability {
            return Ok(token.clone());
        }
        let namespace = self.namespace_id(ctx).await?;
        let claims = capability_claims(snapshot, &ctx.state().tenant_scope)?;
        let issued = self
            .engine
            .issue(&claims, CapabilityKind::Callback, &namespace)
            .await
            .map_err(engine_failure)?;
        let _guard = self.lock.lock().await;
        let tx = db::begin(ctx.sql()).await.map_err(internal)?;
        let mut sub = db::load(&tx, &db::key(&snapshot.root, &snapshot.id))
            .await
            .map_err(internal)?
            .ok_or_else(ApiError::fenced)?;
        let mut changed = sub.expire(now_ms());
        changed |= sub.ensure_wake(now_ms());
        let result = if sub.deleted || !same_wake(&sub, snapshot) {
            Err(ApiError::fenced())
        } else if issued.expires_ts <= now_ms() {
            Err(ApiError::new(
                401,
                "TOKEN_EXPIRED",
                "Engine capability expired before delivery",
            ))
        } else {
            let w = sub.wake.as_mut().unwrap();
            if let Some(token) = &w.capability {
                Ok(token.clone())
            } else {
                w.token_expires = issued.expires_ts;
                w.capability = Some(issued.token.clone());
                w.deadline = w.deadline.map(|at| at.min(issued.expires_ts));
                changed = true;
                Ok(issued.token)
            }
        };
        if changed {
            db::save(&tx, &sub).await.map_err(internal)?;
        }
        tx.commit().await.map_err(internal)?;
        result
    }

    pub(super) async fn check_dispatch(
        &self,
        ctx: &Ctx<Self>,
        snapshot: &Subscription,
    ) -> Result<()> {
        let _guard = self.lock.lock().await;
        let tx = db::begin(ctx.sql()).await?;
        let mut sub = db::load(&tx, &db::key(&snapshot.root, &snapshot.id))
            .await?
            .ok_or_else(|| anyhow!("Wake fenced"))?;
        let mut changed = self.reconcile_subscription(&tx, &mut sub).await?;
        changed |= sub.expire(now_ms());
        changed |= sub.ensure_wake(now_ms());
        let valid = !sub.deleted && same_wake(&sub, snapshot);
        let mut start_lease = false;
        if valid {
            let w = sub.wake.as_mut().unwrap();
            if w.deadline.is_none() {
                w.deadline = Some((now_ms() + sub.config.lease_ttl_ms).min(w.token_expires));
                changed = true;
                start_lease = true;
            }
        }
        if changed {
            if start_lease {
                db::save_with_lease(&tx, &mut sub).await?;
            } else {
                db::save(&tx, &sub).await?;
            }
        }
        if valid {
            require_live_lease(&sub)?;
        }
        tx.commit().await?;
        if !valid {
            anyhow::bail!("Wake fenced");
        }
        Ok(())
    }
}

pub(super) fn require_live_lease(sub: &Subscription) -> ApiResult<()> {
    let now = now_ms();
    if sub
        .wake
        .as_ref()
        .is_some_and(|wake| wake.token_expires <= now)
    {
        return Err(ApiError::new(
            401,
            "TOKEN_EXPIRED",
            "Capability expired before the operation completed",
        ));
    }
    if sub
        .wake
        .as_ref()
        .is_none_or(|wake| wake.deadline.is_none_or(|at| at <= now))
    {
        return Err(ApiError::new(
            409,
            "LEASE_EXPIRED",
            "Processing lease expired before the operation completed",
        ));
    }
    Ok(())
}

fn claim_budget(deadline: i64) -> ApiResult<Duration> {
    let remaining = deadline.saturating_sub(now_ms());
    if remaining <= 0 {
        return Err(ApiError::new(
            503,
            "CLAIM_DEADLINE_EXCEEDED",
            "Claim request expired before ownership was acquired",
        ));
    }
    Ok(Duration::from_millis(remaining.min(10_000) as u64))
}

fn already_claimed(holder: &str, generation: i64) -> Reply {
    Reply::ok(
        409,
        json!({"error":{"code":"ALREADY_CLAIMED","current_holder":holder,"generation":generation}}),
    )
}
fn capability_claims(sub: &Subscription, tenant: &str) -> ApiResult<CapabilityClaims> {
    let w = sub.wake.as_ref().ok_or_else(ApiError::fenced)?;
    Ok(CapabilityClaims {
        root: sub.public_base.clone(),
        tenant_scope: tenant.to_owned(),
        subscription_id: sub.id.clone(),
        wake_id: w.id.clone(),
        generation: w.generation,
        operations: if sub.config.kind == DeliveryType::Webhook {
            vec!["callback".into()]
        } else {
            vec!["ack".into(), "release".into()]
        },
    })
}
pub(super) fn same_wake(a: &Subscription, b: &Subscription) -> bool {
    a.wake
        .as_ref()
        .zip(b.wake.as_ref())
        .is_some_and(|(a, b)| a.id == b.id && a.generation == b.generation)
}
pub(super) fn validate_capability(
    capability: &VerifiedCapability,
    sub: &Subscription,
    operation: &str,
    now: i64,
) -> ApiResult<()> {
    let c = &capability.claims;
    if capability.expires_ts <= now {
        return Err(ApiError::new(
            401,
            "TOKEN_EXPIRED",
            "Capability token expired",
        ));
    }
    if c.root != sub.public_base
        || c.subscription_id != sub.id
        || !c.operations.iter().any(|v| v == operation)
    {
        return Err(ApiError::new(
            401,
            "TOKEN_INVALID",
            "Capability is for a different subscription or operation",
        ));
    }
    let w = sub.wake.as_ref().ok_or_else(ApiError::fenced)?;
    if sub.deleted || c.wake_id != w.id || c.generation != w.generation {
        return Err(ApiError::fenced());
    }
    Ok(())
}
pub(super) fn engine_failure(error: EngineError) -> ApiError {
    let mut e = ApiError::new(error.status, error.code, "Engine signing request failed");
    e.retry_after = error.retry_after;
    e
}
pub(super) fn api_failure(error: ApiError) -> anyhow::Error {
    anyhow::Error::new(error)
}

/// Bound serialization itself so unbounded glob/add memberships cannot allocate an
/// unbounded webhook. Engine enforces its configured (normally 32 MiB) lower cap.
pub(super) fn webhook_body(sub: &Subscription, token: &str) -> Result<bytes::Bytes> {
    struct Limited(Vec<u8>);
    impl std::io::Write for Limited {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if self.0.len().saturating_add(bytes.len()) > 128 * 1024 * 1024 {
                return Err(std::io::Error::other("Webhook snapshot exceeds 128 MiB"));
            }
            self.0.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    #[derive(Serialize)]
    struct Body<'a> {
        subscription_id: &'a str,
        wake_id: &'a str,
        generation: i64,
        streams: Vec<&'a StreamInfo>,
        callback_url: String,
        callback_token: &'a str,
    }
    let wake = sub
        .wake
        .as_ref()
        .ok_or_else(|| anyhow!("No delivery wake"))?;
    let body = Body {
        subscription_id: &sub.id,
        wake_id: &wake.id,
        generation: wake.generation,
        streams: wake.snapshot.iter().map(|s| &s.info).collect(),
        callback_url: format!(
            "{}/__ds/subscriptions/{}/callback",
            sub.public_base,
            url_component(&sub.id)
        ),
        callback_token: token,
    };
    let mut output = Limited(Vec::new());
    serde_json::to_writer(&mut output, &body).map_err(|_| api_failure(ApiError::new(413, "WEBHOOK_PAYLOAD_TOO_LARGE", "Webhook snapshot exceeds the signing payload limit; reduce membership or increase the Engine limit")))?;
    Ok(output.0.into())
}
