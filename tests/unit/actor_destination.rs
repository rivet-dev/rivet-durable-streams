use super::*;

#[test]
fn actor_destination_requires_namespace_type_pool_and_live_identity() {
    let actor = json!({
        "actor_id": "actor-a", "namespace_id": "namespace-a",
        "name": "customer", "runner_name_selector": "app", "destroy_ts": null,
    });
    let matches = |actor: &Value| {
        matches_actor_destination(actor, "customer", "app", "namespace-a", Some("actor-a"))
    };
    assert!(matches(&actor));
    for (field, value) in [
        ("actor_id", json!("actor-b")),
        ("namespace_id", json!("namespace-b")),
        ("name", json!("other")),
        ("runner_name_selector", json!("other-pool")),
        ("destroy_ts", json!(123)),
    ] {
        let mut wrong = actor.clone();
        wrong[field] = value;
        assert!(!matches(&wrong), "accepted wrong {field}");
    }
}
