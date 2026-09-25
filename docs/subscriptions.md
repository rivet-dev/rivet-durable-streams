# Durable Streams subscriptions

Subscriptions track processed offsets across streams and notify workers through signed webhooks or an ordinary wake stream. This server implements the upstream draft at [`461b40267aabd644558f9b19dbb9507dd5f691cf`](https://github.com/durable-streams/durable-streams/blob/461b40267aabd644558f9b19dbb9507dd5f691cf/PROTOCOL.md#6-reserved-subscription-apis), checked against upstream on September 24, 2026. The conformance harness pins `@durable-streams/server-conformance-tests` 0.3.7 and Vitest 4.0.18, with subscriptions explicitly enabled.

The [Node implementation](https://github.com/durable-streams/durable-streams/blob/461b40267aabd644558f9b19dbb9507dd5f691cf/packages/server/src/subscription-manager.ts) informed wire compatibility, and [Caddy's crypto implementation](https://github.com/durable-streams/durable-streams/blob/461b40267aabd644558f9b19dbb9507dd5f691cf/packages/caddy-plugin/webhook/crypto.go) provided a second signing reference. Both references keep subscription state in memory; this implementation persists delivery state and recovery intents. Caddy uses an older subscription protocol and is not the reference for pull-wake behavior.

## Configure access and public URLs

Set `PUBLIC_ORIGIN` to the externally reachable origin, for example `https://streams.gameinc.io`. The server adds its stream root (`/v1/stream` in the standalone binary) when constructing callback and JWKS URLs. Forwarded host headers do not control these URLs.

Set `DS_SUBSCRIPTIONS_MANAGEMENT_TOKEN` and send it as `Authorization: Bearer <token>` on subscription management and claim requests. Callback, ack and release requests require their own wake capability token. JWKS is public. Engine credentials authenticate internal actor traffic; they are not public client credentials. Protect ordinary stream operations through your application's existing access-control boundary.

The same `RIVET_ENDPOINT`, `RIVET_NAMESPACE`, and `RIVET_TOKEN` connection supplies Engine signing. Issue a namespace-scoped Engine JWT through `/auth/tokens` with `application_token/create` and `webhook_signing/create`, plus the actor and runner grants needed by your deployment. Add `namespace/read` for namespace lookup and `datacenter/list` for RivetKit routing. The JWT's namespace must match `RIVET_NAMESPACE`. An Engine build containing the purpose-specific application-token and webhook-signing APIs is required; released Engine 2.3.16 does not provide them.

JWT verification trusts the configured Engine endpoint as issuer by default. Set `DS_ENGINE_ISSUER` to the trusted leader's issuer when connecting through a proxy or follower with a different URL. The issuer never comes from unverified token claims.

The library's `SubscriptionConfig` supports the same policy. An embedding application that already authenticates management requests may explicitly enable `allow_unauthenticated` inside that protected router. The standalone equivalent, `DS_SUBSCRIPTIONS_ALLOW_UNAUTHENTICATED=1`, is intended for local tests. Management access is denied by default.

Use `DS_TENANT_SCOPE` to isolate standalone applications sharing an Engine namespace. In library integrations, use `DurableStreamsConfig.tenant_scope`; choose the tenant from authenticated application context, not a caller-controlled header.

Webhooks require HTTPS and public IP addresses. Each attempt resolves and validates DNS, pins the connection to the validated addresses, and disables proxies and redirects. For local receivers only, set `DS_SUBSCRIPTIONS_ALLOW_LOCAL_HTTP=1` on every actor runner and enable the library's `allow_local_http` option when embedding. This permits localhost, `127.0.0.x`, and `::1`; other private destinations remain rejected.

## Create and consume

Create the wake stream before using pull-wake delivery:

```sh
curl -X PUT "$BASE/v1/stream/wake/workers" \
  -H 'Content-Type: application/json' --data '[]'

curl -X PUT "$BASE/v1/stream/__ds/subscriptions/processor" \
  -H "Authorization: Bearer $MANAGEMENT_TOKEN" \
  -H 'Content-Type: application/json' \
  --data '{"type":"pull-wake","pattern":"events/**","wake_stream":"wake/workers"}'
```

Read `wake/workers` normally. Claim through `POST .../__ds/subscriptions/processor/claim` with `{"worker":"worker-1"}`. Read each linked stream starting from its `acked_offset`, then acknowledge the returned `Stream-Next-Offset` verbatim. Do not increment or interpret offsets. Send the claim token to `/ack`, with the returned `wake_id`, `generation`, `acks`, and `done:true` when finished. Omit `done` to heartbeat; `/release` gives up ownership without acknowledging.

For webhooks, replace `type` with `webhook` and `wake_stream` with `"webhook":{"url":"https://worker.gameinc.io/hooks"}`. Verify the exact request bytes using `Webhook-Signature` and the advertised JWKS. Returning `{"done":true}` acknowledges only that delivery's snapshot. For asynchronous work, use the supplied callback URL and bearer capability token.

The signature header is `Webhook-Signature: t=<unix-seconds>,kid=<key-id>,ed25519=<signature>`. Decode the unpadded base64url signature and verify Ed25519 over the timestamp's ASCII bytes, a literal `.` byte, and the unchanged request body. Reject timestamps outside a five-minute freshness window. Select the key using the header's `kid`; subscription metadata can race a rotation.

## Rivet gateway authentication

Webhook delivery to the process's configured Rivet gateway automatically uses
the process's existing Rivet token in `x-rivet-token`. The destination must match
the configured Engine origin (scheme, host and port) and its `/gateway/{actor}/`
path prefix. Other destinations receive no Rivet credential. Delivery still
includes the standard webhook signature; receivers must verify it separately.
Redirects and environment HTTP proxies remain disabled.

Subscription management is privileged: this feature lets a subscription invoke
actors with the service process's gateway permissions. Only trusted operators
should hold the management credential. Before exposing registration to tenants,
enforce destination actor and namespace permissions and ensure the gateway
consumes the token without forwarding it to user handlers or request logs.
Token rotation follows the host runtime's configured Rivet credentials; tokens
are not stored in subscription URLs or configuration records.

## Recovery and lifecycle

Configuration, memberships, cursors, active wakes, retry deadlines, leases and notification intents live in actor SQLite storage. Engine owns webhook private keys and application-token issuance. A committed append is retried into the catalog after a crash. Delivery is at least once; receivers must tolerate repeated wake IDs. Pull-wake publication uses a durable producer identity to deduplicate retries, and claims remain serialized across restarts.

A crash during signing or delivery can defer another attempt until the persisted 60-second dispatch reservation expires. This reservation covers bounded signing and receiver work; restarting the server does not bypass it.

The standalone server imports existing stream actors before the first pattern subscription. Library hosts upgrading an existing deployment should configure `SubscriptionConfig.catalog_api` with the authenticated Engine endpoint, namespace and token. Explicit membership also refreshes the addressed stream directly. All actor runners should run the upgraded implementation before the initial catalog import. Import failures fail registration instead of silently omitting streams.

Webhook keys belong to the immutable Engine namespace and may be shared by multiple DS tenants. Callback and claim application tokens use Engine's separate authentication key ring; DS verifies their signature, issuer, audience, purpose, namespace, tenant, public stream root and live wake identity locally. Keep Engine storage durable and protected. Signing outages leave delivery pending; there is no local signing fallback.

Permanent Engine signing failures (400, 401, 403 or 413) leave the subscription visibly `failed` with `delivery_error` and preserve its pending offsets. They do not retry automatically. After correcting credentials, request configuration or body limits, re-confirm the same subscription configuration with PUT to resume in a new generation; membership additions or removals also resume processing. Temporary failures retain bounded retry backoff.

Engine's webhook body limit is 32 MiB. Snapshots exceeding that limit fail visibly; the server never truncates membership or acknowledges undelivered data. Reduce the subscription's membership before resuming. Rate-limited delivery honors `Retry-After`.

Operators manage webhook keys through Engine's `GET /namespaces/{namespace}/auth/webhook-keys`, `POST .../rotate`, and `POST .../revoke` APIs with separately authorized read/update/delete permissions. A normal rotation stages a public key before activation. DS fetches authoritative webhook keys for its own `__ds/jwks.json` response and preserves its public URL and five-minute cache policy. Emergency revocation cannot recall keys already cached by receivers; refresh once on an unfamiliar `kid`, and reject if a trusted verification key is unavailable.

Upgrades preserve already-issued local capabilities only until their recorded expiry, bounded by 24 hours after cutover. Old webhook public keys remain available for 365 seconds after cutover. Legacy private signing keys are discarded, verification material is removed after these windows, and every newly issued token or signature uses Engine. Upgrade all runners before retiring the previous deployment.

The draft leaves several lifecycle details open. This implementation uses these rules:

- Re-confirmation compares the original normalized creation configuration. Adding or removing memberships does not change that configuration hash or reset progress.
- Existing memberships start at the current tail. New matching streams include their initial PUT data. Forks begin after inherited data, including a copied fork sub-offset prefix; newly appended fork data is pending.
- Deletion or recreation of a linked stream fences its active wake. A new incarnation resets that stream's cursor; offsets from the old incarnation cannot acknowledge replacement data.
- Retries use the original transmitted snapshot. Expired generations cannot process late HTTP responses. When expiration overlaps failed delivery, the next generation inherits the retry count and deadline. The delivery lease begins immediately before contacting the receiver, after Engine signing completes.
- Heartbeats extend the persisted lease. DS requests a 24-hour capability lifetime but honors Engine's actual expiry, which may be shorter because of the caller credential. After expiry the worker must acquire the newly fenced wake. Leases otherwise default to 30 seconds and accept 1–600 seconds.
- Missing, closed or expired wake streams leave a durable retry intent. The server never creates them implicitly.
- A subscription's own generated wake records do not recursively trigger it. Their offsets are skipped only after preceding application data has been acknowledged. Other subscriptions can consume those wake records normally. Cross-subscription feedback remains coalesced behind active wakes; applications should avoid cycles that workers continually acknowledge into new work.

Subscriptions do not pin stream retention. Keep processing within your stream retention policy.

## Validation

`bash scripts/test-conformance.sh` runs the pinned upstream suite, including all six subscription cases, additional HTTP race and signing-security coverage, the Rust end-to-end test, a 4096-stream snapshot storage check, and crash/restart recovery tests. Until an Engine release containing the signing APIs is pinned, supply a reviewed executable and its expected SHA-256:

```sh
CONFORMANCE_ENGINE_BINARY=/path/to/rivet-engine \
CONFORMANCE_ENGINE_SHA256=<expected-sha256> \
CONFORMANCE_KEEP_LOGS=1 bash scripts/test-conformance.sh
```

Alternatively, set `CONFORMANCE_ENGINE_URL` and the expected checksum, or pin a compatible released `CONFORMANCE_ENGINE_VERSION` to verify against its published checksum manifest. CI builds the pinned OSS Engine revision when neither variable is configured. The harness creates an isolated Engine, provisions a namespace-scoped JWT, and uses that same connection for DS, signing tests and recovery. `CONFORMANCE_KEEP_LOGS=1` retains successful logs as well as failures. The streaming gateway remains enabled for the pinned Rust runtime.

Set `CONFORMANCE_LEGACY_BINARY` to a previous DS executable with local subscription signing to additionally exercise a live upgrade: existing callback/claim tokens remain valid, their public webhook keys remain discoverable, and subsequent wakes use Engine application JWTs.

HTTP test files run sequentially so large subscription fixtures do not consume the upstream tests' short TTL timing windows. Each test has a 180-second harness timeout for randomized cases that perform hundreds of real-Engine operations. Upstream assertions, generated cases and concurrency within its test file are unchanged.

The recovery harness `tests/subscription_recovery.py` runs separate server processes against an existing test Engine using `RIVET_TOKEN` and `RIVET_NAMESPACE`. Build with `cargo build --locked --features subscription-fault-injection` before running it. That opt-in feature exposes no HTTP endpoint; environment-selected checkpoints terminate the test process at committed-data and delivery-intent boundaries. Production builds leave it disabled.

The test Engine also uses a 250 ms actor rescheduling base delay and maximum backoff exponent of 1. Engine 2.3.16's default allocation backoff can delay actors for several minutes after repeated runner restarts; these test-only settings let the crash tests finish promptly. Subscription retry deadlines and lease durations retain their normal values. The tests do not establish a recovery-time bound for production Engine defaults.


## Rivet Actor declarations (opt in)

Set `DS_ACTOR_ROUTING=1` in the standalone server or enable
`SubscriptionConfig.actor_routing` when embedding the router. Configure
`public_origin` (`PUBLIC_ORIGIN` in the server). The coordinator uses its resolved
Rivet runtime connection and namespace to read `/actors/names`; the runtime token
needs actor list/create and gateway access as well as the signing permissions above.
This requires an Engine that publishes `streamSubscriptionSources` metadata.

RivetKit publishes a complete subscription set per actor type and pool. Increment
`envoy.version` when changing declarations. DS adopts changes about every five
seconds and materializes ordinary signed webhook subscriptions:

- Primary: `actor-{actorId}`, reading `actors/{actorName}/{actorId}`.
- Named: `actor-{actorId}-{subscriptionName}`, with one checkpoint per source.
- Named URLPattern path captures map to complete actor-key components. Key routes
  get or create the actor in the declaring pool. ID routes only address existing
  actors of the declared type in the configured namespace and pool.
- A matching write is journaled before catalog acceptance. New memberships start
  before that write, including data in the first PUT. Primary declarations also
  reconcile existing streams to recover publication races. Named declarations
  start with writes accepted after metadata adoption; they do not replay history.
- Restarts preserve memberships, acknowledgements, and pending reconciliation.
  Removing a declaration removes only its managed memberships. Delayed metadata
  from an older deployment cannot restore a removed rule.
- Managed memberships cannot be changed through subscription management PUT,
  DELETE, add, or remove operations while owned by the actor integration. Change
  the actor declaration instead. Reads and standard callback acknowledgements work
  normally.

This is a Rivet integration layered over the subscription protocol, not a new
standard delivery mode. The standard signature, callback, heartbeat, and lease
rules are unchanged. Retention is still independent of consumer progress.
