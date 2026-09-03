# Contributing

## Run it

RivetKit downloads and starts a local Rivet Engine for you:

```sh
RIVETKIT_ENGINE_AUTO_DOWNLOAD=1 cargo run -- --host 0.0.0.0
```

To use an existing engine instead, set `RIVET_ENDPOINT`, `RIVET_NAMESPACE`, and `RIVET_TOKEN`.

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
just test
just test-conformance
```

`test-conformance` builds the pinned Rivet Engine from source, starts it with the server, and runs the official Durable Streams conformance suite against it.
