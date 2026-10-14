use super::*;

fn rule() -> Rule {
    Rule {
        pool: "app".into(),
        name: "orders".into(),
        stream: "/tenants/:tenant/orders/:customer".into(),
        destination: Destination::ActorKey {
            actor_name: "customer".into(),
            key: vec![":tenant".into(), ":customer".into()],
        },
    }
}
#[test]
fn actor_routing_matches_urlpattern_and_preserves_key_components() {
    let route = rule();
    route.validate("customer").unwrap();
    assert_eq!(
        route.resolve("tenants/acme/orders/alice").unwrap(),
        Some(vec!["acme".into(), "alice".into()])
    );
    assert_eq!(
        route.resolve("tenants/acme/orders/alice/extra").unwrap(),
        None
    );
    assert_eq!(
        route
            .resolve("tenants/a%2Fb/orders/alice")
            .unwrap()
            .unwrap()[0],
        "a%2Fb"
    );
    assert!(route.validate("different-actor").is_err());
}
#[test]
fn actor_routing_checks_captures_and_destination_contract() {
    let mut route = rule();
    route.stream = "/orders/:customer".into();
    assert!(route.validate("customer").is_err());
    assert!(serde_json::from_value::<Destination>(json!({"actorId":"123"})).is_err());
    assert!(serde_json::from_value::<Destination>(json!({"actorId":{"id":":actorId"}})).is_err());
    route = rule();
    route.name = "primary".into();
    assert!(route.validate("customer").is_err());
}
