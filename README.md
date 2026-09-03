# Durable Streams for Rivet Actors

A native implementation of the [Durable Streams protocol](https://github.com/durable-streams/durable-streams).

It supports binary and JSON streams, catch-up reads, long polling, SSE, idempotent producers, TTLs, stream closure, forks, and a read-only inspector.

## Running Locally and Deploying

See the [Durable Streams integration guide](https://rivet.dev/actors/integrations/durable-streams).

## Developing Locally

**Durable Streams Server**

```sh
cargo run -- --host 0.0.0.0
```

**Testing the Streams**

```sh
curl -X PUT -H 'content-type: application/json' --data '[{"message":"hello"}]' http://127.0.0.1:8787/v1/stream/demo
curl 'http://127.0.0.1:8787/v1/stream/demo?offset=-1'
```

## License

Apache 2.0
