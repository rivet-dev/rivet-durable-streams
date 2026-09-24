# Contributing

## Run it

RivetKit downloads and starts a local Rivet Engine for you:

```sh
RIVETKIT_ENGINE_AUTO_DOWNLOAD=1 cargo run -- --host 0.0.0.0
```

To use an existing engine instead, set `RIVET_ENDPOINT`, `RIVET_NAMESPACE`, and `RIVET_TOKEN`.
Subscriptions additionally require an Engine build with the purpose-specific signing APIs and a scoped ACL credential; see [subscription configuration](docs/subscriptions.md#configure-access-and-public-urls).

## Talk to it

```sh
curl -i -X PUT \
  -H 'content-type: application/json' \
  --data '[{"message":"hello"}]' \
  http://127.0.0.1:8787/v1/stream/demo

curl 'http://127.0.0.1:8787/v1/stream/demo?offset=-1'
```

## Test it

```sh
cargo test --locked
CONFORMANCE_ENGINE_BINARY=/path/to/reviewed/rivet-engine \
CONFORMANCE_ENGINE_SHA256=<expected-sha256> \
bash scripts/test-conformance.sh
```

The conformance script verifies the supplied Engine build, provisions an isolated namespace signing credential, starts the server, and runs the complete official `@durable-streams/server-conformance-tests` 0.3.7 suite, including all six subscription cases. Engine 2.3.16 lacks the required signing APIs. A compatible download can instead use `CONFORMANCE_ENGINE_URL` with its expected checksum; once released, pin `CONFORMANCE_ENGINE_VERSION` to use the published checksum manifest. CI accepts the same repository variables. The isolated npm harness pins Vitest 4.0.18 and its dependency lockfile. Use Node.js 22.12+ or 24+ and npm.

SSE requires Engine's streaming gateway. The script sets `RIVET__FEATURES__GUARD_GATEWAY_V3__MODE=on` and `RIVET__FEATURES__GUARD_GATEWAY_V3__PERCENTAGE=100`; use the same settings for your Engine deployment. The default gateway buffers streaming responses until completion.

The script enables unauthenticated subscription management and localhost HTTP webhooks only for its local test server. Failures retain Engine, server, and conformance logs. Set `CONFORMANCE_KEEP_LOGS=1` to retain passing logs too. Set `CONFORMANCE_LEGACY_BINARY` to a previous DS executable with local signing to also test live capability migration.

To run against an existing local test server, export its `RIVET_ENDPOINT`, `RIVET_NAMESPACE`, `RIVET_TOKEN`, and `CONFORMANCE_ENGINE_ADMIN_TOKEN` first. The added signing tests use the operator credential only to provision test namespaces and exercise key rotation:

```sh
npm --prefix tests/conformance ci
CONFORMANCE_TEST_URL=http://127.0.0.1:17878 npm --prefix tests/conformance test
```
