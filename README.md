# Durable Streams for Rivet Actors

A native implementation of the [Durable Streams protocol](https://github.com/durable-streams/durable-streams).

It supports binary and JSON streams, catch-up reads, long polling, SSE, idempotent producers, TTLs, stream closure, forks, and a read-only inspector.

## Run locally

See the [Rust quickstart](https://rivet.dev/actors/docs/quickstart/rust/) for reference.

**Running Durable Streams**

```sh
RIVETKIT_ENGINE_AUTO_DOWNLOAD=1 \
cargo run -- --host 0.0.0.0
```

**Testing the Streams**

Durable Streams is available at `http://127.0.0.1:8787/v1/stream/<path>`.

```sh
curl -i -X PUT \
  -H 'content-type: application/json' \
  --data '[{"message":"hello"}]' \
  http://127.0.0.1:8787/v1/stream/demo

curl -i -X POST \
  -H 'content-type: application/json' \
  --data '{"message":"world"}' \
  http://127.0.0.1:8787/v1/stream/demo

curl 'http://127.0.0.1:8787/v1/stream/demo?offset=-1'

curl -N 'http://127.0.0.1:8787/v1/stream/demo?offset=now&live=sse'
```

**Connecting the Client**

Use the official `@durable-streams/client` package:

```ts
import { DurableStream } from "@durable-streams/client"

const stream = await DurableStream.create({
  url: "http://127.0.0.1:8787/v1/stream/demo",
  contentType: "application/json",
})

await stream.append(JSON.stringify({ message: "hello" }))
```

**Viewing the Inspector**

Open `http://127.0.0.1:6420/ui/`.

## Deployment

### Deploy to Rivet Cloud

1. Get a cloud token from the [Rivet dashboard](https://dashboard.rivet.dev/).
2. Deploy the service:

```sh
npx @rivetkit/cli@latest deploy --token cloud_api_xxxxx
```

3. Connect to the deployment URL printed by the command:

```sh
curl -X PUT \
  -H 'content-type: application/json' \
  --data '[{"message":"hello"}]' \
  https://YOUR_DEPLOYMENT_URL/v1/stream/demo
```

### Self-hosted

1. Follow the Rivet guides to [self-host the control plane](https://rivet.dev/actors/self-host/control-plane/) and [run workers](https://rivet.dev/actors/self-host/workers/).
2. Run Durable Streams with your private endpoint:

```sh
RIVET_ENDPOINT=https://YOUR_PRIVATE_ENDPOINT \
cargo run --release -- --host 0.0.0.0
```

3. Connect through the public URL in front of the service:

```sh
curl -X PUT \
  -H 'content-type: application/json' \
  --data '[{"message":"hello"}]' \
  https://YOUR_PUBLIC_URL/v1/stream/demo
```

### Configuring the pool name

Set a pool name to run Durable Streams separately or alongside other actor
types registered by your workers:

```sh
npx @rivetkit/cli@latest deploy --pool durable-streams
```

## License

Apache 2.0
