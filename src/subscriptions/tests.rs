use super::*;

fn offset(n: usize) -> String {
    format!("0000000000000000_{n:016}")
}
fn config() -> Config {
    serde_json::from_value::<Config>(json!({"type":"pull-wake","pattern":"events/**","wake_stream":"wake/pool","lease_ttl_ms":1000})).unwrap().normalize().unwrap()
}
fn sub() -> Subscription {
    Subscription::new(
        "sub".into(),
        "/v1/stream".into(),
        "https://gameinc.io/v1/stream".into(),
        config(),
        0,
        1000,
    )
}
fn entry(path: &str, n: usize) -> CatalogEntry {
    CatalogEntry {
        path: format!("/v1/stream/{path}"),
        version: n as i64 + 1,
        incarnation: "inc-1".into(),
        tail: offset(n),
        start: ZERO_OFFSET.into(),
        exists: true,
        wakes: vec![],
    }
}
use crate::protocol::ZERO_OFFSET;

#[test]
fn segment_globs_and_configuration_canonicalization() {
    for (pattern, path, expected) in [
        ("events/*", "events/a", true),
        ("events/*", "events/a/b", false),
        ("events/**", "events", true),
        ("events/**", "events/a/b", true),
        ("**/a/**/b", "a/b", true),
        ("**/a/**/b", "x/a/y/b", true),
        ("**/a/**/b", "x/ab/b", false),
    ] {
        assert_eq!(glob_matches(pattern, path), expected, "{pattern} {path}");
    }
    let mut a = config();
    a.streams = vec!["/events/b/".into(), "events/a".into(), "events/a".into()];
    let mut b = config();
    b.streams = vec!["events/a".into(), "events/b".into()];
    assert_eq!(a.normalize().unwrap().hash(), b.normalize().unwrap().hash());
    for ttl in [0, 999, 600001] {
        let mut c = config();
        c.lease_ttl_ms = ttl;
        assert!(c.normalize().is_err());
    }
}
#[test]
fn backfill_new_put_duplicate_observation_and_explicit_precedence() {
    let mut s = sub();
    let first = entry("events/a", 5);
    s.link("events/a", Some(&first), false, true);
    assert!(!s.pending());
    s.link("events/a", Some(&entry("events/a", 8)), true, true);
    assert_eq!(s.links["events/a"].acked, offset(5));
    assert_eq!(s.infos()[0].link_type, "explicit");
    s.links.get_mut("events/a").unwrap().explicit = false;
    assert_eq!(s.infos()[0].link_type, "glob");
    assert!(s.observe(&entry("events/b", 3)));
    assert!(s.pending());
    assert_eq!(s.links["events/b"].acked, ZERO_OFFSET);
    assert!(s.ensure_wake(1000));
    let wake = s.wake.clone().unwrap();
    assert!(!s.observe(&entry("events/b", 3)));
    assert!(!s.observe(&entry("unmatched/b", 3)));
    assert!(!s.ensure_wake(1000));
    assert_eq!(s.wake.as_ref().unwrap().id, wake.id);
}
#[test]
fn snapshot_completion_never_acks_later_appends_or_recreated_streams() {
    let mut s = sub();
    s.observe(&entry("events/a", 3));
    s.ensure_wake(1000);
    s.observe(&entry("events/a", 7));
    assert!(s.finish(1001, true));
    assert_eq!(s.links["events/a"].acked, offset(3));
    assert_eq!(s.generation, 2);
    let mut recreated = entry("events/a", 2);
    recreated.incarnation = "inc-2".into();
    recreated.version = 99;
    s.observe(&recreated);
    assert!(s.finish(1002, true));
    assert_eq!(s.links["events/a"].acked, ZERO_OFFSET);
}
#[test]
fn acknowledgement_batch_is_atomic_and_duplicate_entries_cannot_regress() {
    let mut s = sub();
    s.observe(&entry("events/a", 10));
    s.observe(&entry("events/b", 10));
    for acks in [
        vec![("events/a", offset(4)), ("unknown", offset(1))],
        vec![("events/a", offset(4)), ("events/b", offset(11))],
        vec![("events/a", offset(4)), ("events/a", offset(3))],
        vec![("events/a", offset(4)), ("events/b", "now".into())],
    ] {
        let acks: Vec<_> = acks
            .into_iter()
            .map(|(stream, offset)| Ack {
                stream: stream.into(),
                offset,
            })
            .collect();
        assert!(s.apply_acks(&acks).is_err());
        assert_eq!(s.links["events/a"].acked, ZERO_OFFSET);
    }
    s.apply_acks(&[Ack {
        stream: "events/a".into(),
        offset: offset(4),
    }])
    .unwrap();
    assert!(
        s.apply_acks(&[Ack {
            stream: "events/a".into(),
            offset: offset(2)
        }])
        .is_err()
    );
}
#[test]
fn persisted_leases_retry_deadlines_and_fencing_survive_roundtrip() {
    let mut s = sub();
    s.config.kind = DeliveryType::Webhook;
    s.config.webhook = Some(Webhook {
        url: "https://gameinc.io/hook".into(),
    });
    s.config.wake_stream = None;
    s.observe(&entry("events/a", 1));
    s.ensure_wake(1000);
    let w = s.wake.as_mut().unwrap();
    w.deadline = Some(2000);
    w.attempts = 5;
    w.next_attempt_at = Some(40_000);
    let old = w.id.clone();
    let serialized = serde_json::to_vec(&s).unwrap();
    let mut restored: Subscription = serde_json::from_slice(&serialized).unwrap();
    assert!(!restored.expire(1999));
    assert_eq!(restored.wake.as_ref().unwrap().id, old);
    assert!(restored.expire(2000));
    let restored: Subscription =
        serde_json::from_slice(&serde_json::to_vec(&restored).unwrap()).unwrap();
    let w = restored.wake.as_ref().unwrap();
    assert_ne!(w.id, old);
    assert_eq!(w.attempts, 5);
    assert_eq!(w.next_attempt_at, Some(40_000));
    assert_eq!(w.deadline, None);
    assert_eq!(restored.due(), Some(40_000));
}
#[test]
fn synthetic_wakes_cannot_recurse_or_skip_application_data() {
    let mut s = sub();
    s.config.pattern = Some("**".into());
    let mut wake = entry("wake/pool", 2);
    wake.wakes = vec![WakeInterval {
        before: ZERO_OFFSET.into(),
        after: offset(2),
        incarnation: wake.incarnation.clone(),
        origin: "/v1/stream/sub".into(),
    }];
    s.observe(&wake);
    assert!(!s.pending());
    assert_eq!(s.links["wake/pool"].acked, offset(2));
    s.observe(&entry("wake/pool", 5));
    assert!(s.pending());
    wake.tail = offset(8);
    wake.version = 9;
    wake.wakes = vec![WakeInterval {
        before: offset(5),
        after: offset(8),
        incarnation: wake.incarnation.clone(),
        origin: "/v1/stream/sub".into(),
    }];
    s.observe(&wake);
    assert_eq!(s.links["wake/pool"].acked, offset(2));
    s.apply_acks(&[Ack {
        stream: "wake/pool".into(),
        offset: offset(5),
    }])
    .unwrap();
    assert_eq!(s.links["wake/pool"].acked, offset(8));
    assert!(!s.pending());
}
#[tokio::test]
async fn destination_policy_covers_ipv4_ipv6_dns_and_local_exception() {
    for ip in [
        "127.0.0.1",
        "10.1.2.3",
        "169.254.169.254",
        "100.64.1.1",
        "0.0.0.0",
        "192.0.0.1",
        "::",
        "::1",
        "::ffff:127.0.0.1",
        "fc00::1",
        "fe80::1",
        "2001:db8::1",
        "2002:a00:1::1",
    ] {
        assert!(!security::public_ip(ip.parse().unwrap()), "{ip}");
    }
    for ip in ["1.1.1.1", "2606:4700:4700::1111"] {
        assert!(security::public_ip(ip.parse().unwrap()));
    }
    assert!(
        security::destination("http://localhost:9911/hook", true)
            .await
            .is_ok()
    );
    assert!(
        security::destination("https://localhost/hook", false)
            .await
            .is_err()
    );
    assert!(
        security::destination("http://10.0.0.1/hook", true)
            .await
            .is_err()
    );
    assert!(
        security::destination("http://[::1]:9911/hook", true)
            .await
            .is_ok()
    );
    assert!(
        security::destination("https://user:pass@gameinc.io/", false)
            .await
            .is_err()
    );
}
#[test]
fn retry_jitter_is_bounded_and_exponential() {
    for (attempt, base) in [(1, 1000), (2, 2000), (6, 32000), (7, 60000), (100, 60000)] {
        for _ in 0..100 {
            let delay = retry_delay(attempt);
            assert!((base * 80 / 100..=base * 120 / 100).contains(&delay));
        }
    }
}

#[test]
fn expired_slow_dispatch_preserves_failure_pacing_in_new_generation() {
    let mut s = sub();
    s.config.kind = DeliveryType::Webhook;
    s.config.webhook = Some(Webhook {
        url: "https://gameinc.io/hook".into(),
    });
    s.observe(&entry("events/a", 1));
    s.ensure_wake(1000);
    s.wake.as_mut().unwrap().deadline = Some(2000);
    s.wake.as_mut().unwrap().dispatch_until = Some(16000);
    let prior = s.wake.as_ref().unwrap().id.clone();
    s.expire(2500);
    let w = s.wake.as_ref().unwrap();
    assert_ne!(w.id, prior);
    assert_eq!(w.attempts, 1);
    assert!((3300..=3700).contains(&w.next_attempt_at.unwrap()));
    assert_eq!(w.deadline, None);
}

#[test]
fn current_catalog_reconciliation_retires_and_replaces_old_incarnation_wake() {
    let mut s = sub();
    s.observe(&entry("events/a", 4));
    s.ensure_wake(1000);
    let prior = s.wake.as_ref().unwrap().id.clone();
    let mut replacement = entry("events/a", 4);
    replacement.version = 9;
    replacement.incarnation = "new".into();
    assert!(s.observe(&replacement));
    assert!(!s.expire(1100));
    assert!(s.ensure_wake(1100));
    let mut s: Subscription = serde_json::from_slice(&serde_json::to_vec(&s).unwrap()).unwrap();
    assert_ne!(s.wake.as_ref().unwrap().id, prior);
    assert_eq!(s.links["events/a"].acked, ZERO_OFFSET);
    // An older queued fanout must not overwrite the reconciled version.
    assert!(!s.observe(&entry("events/a", 2)));
    assert_eq!(s.links["events/a"].incarnation, "new");
}

#[test]
fn expiry_and_catalog_deletion_remain_dirty_without_replacement_work() {
    for deletion in [false, true] {
        let mut s = sub();
        s.observe(&entry("events/a", 4));
        s.ensure_wake(1000);
        s.wake.as_mut().unwrap().deadline = Some(2000);
        if !deletion {
            s.apply_acks(&[Ack {
                stream: "events/a".into(),
                offset: offset(4),
            }])
            .unwrap();
        }
        let mut persisted = serde_json::to_vec(&s).unwrap();
        let mut read: Subscription = serde_json::from_slice(&persisted).unwrap();
        let mut changed = if deletion {
            let mut deleted = entry("events/a", 5);
            deleted.exists = false;
            read.observe(&deleted)
        } else {
            read.expire(2000)
        };
        changed |= read.ensure_wake(2000);
        // Read/claim preflight saves only changed state, including retirement
        // when all work was acknowledged or its stream disappeared.
        if changed {
            persisted = serde_json::to_vec(&read).unwrap();
        }
        let restored: Subscription = serde_json::from_slice(&persisted).unwrap();
        assert!(restored.wake.is_none(), "deletion={deletion}");
        assert!(!restored.pending());
    }
}

#[test]
fn permanent_signing_failure_preserves_pending_data_until_management_retry() {
    let mut s = sub();
    s.config.kind = DeliveryType::Webhook;
    s.config.webhook = Some(Webhook {
        url: "https://worker.gameinc.io/hook".into(),
    });
    s.observe(&entry("events/a", 1));
    s.ensure_wake(1000);
    let w = s.wake.as_mut().unwrap();
    w.attempts = 1;
    w.next_attempt_at = None;
    w.deadline = None;
    w.token_expires = 2000;
    w.delivery_error =
        Some(json!({"code":"ENGINE_PAYLOAD_TOO_LARGE","status":413,"retry_at":null}));
    let mut restored: Subscription =
        serde_json::from_str(&serde_json::to_string(&s).unwrap()).unwrap();
    assert!(restored.expire(2001));
    assert!(restored.infos().iter().any(|s| s.has_pending));
    assert_eq!(restored.wake.as_ref().unwrap().next_attempt_at, None);
    assert_eq!(
        restored.public(Value::Null)["delivery_error"]["status"],
        413
    );
    let old_generation = restored.generation;
    assert!(restored.resume_delivery(2002));
    assert!(!restored.resume_delivery(2002));
    let wake = restored.wake.as_ref().unwrap();
    assert!(wake.generation > old_generation);
    assert_eq!(wake.next_attempt_at, Some(2002));
    assert!(wake.delivery_error.is_none());
    assert!(restored.infos().iter().any(|s| s.has_pending));
}
