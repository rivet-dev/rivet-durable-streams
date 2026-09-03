<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset=".github/logo-dark.svg">
    <img alt="Durable Streams and Rivet Actors" src=".github/logo-light.svg" width="280">
  </picture>
</p>

<h1 align="center">Durable Streams for Rivet Actors</h1>

<p align="center">Real-time streams with durable history. Powered by open-source, self-hostable infrastructure.</p>

<p align="center">
  <a href="https://rivet.dev/actors/integrations/durable-streams">Guide</a> ·
  <a href="https://rivet.dev/blog/2026-09-03-durable-streams-now-supports-rivet-actors">Announcement</a> ·
  <a href="https://rivet.dev/discord">Discord</a>
</p>

## What are Durable Streams?

[Durable Streams](https://durablestreams.com) are an open standard for real-time data streaming with durable history. You write data to a stream, and readers can read from any offset in the stream: catch up from the beginning, resume from where they left off, or tail it live.

They are a good fit for:

- **[Agent sessions](https://durablestreams.com/vercel-ai-sdk)**: prompts and responses survive restarts, and clients reconnect without losing a token.
- **[CRDT sync](https://durablestreams.com/yjs)**: a durable, ordered log of updates for collaborative editing with Yjs and similar libraries.
- **[Database sync](https://durablestreams.com/stream-db)**: stream changes to every client and replay from any offset.

This repository is the Rivet implementation of the protocol, powered by [Rivet Actors](https://rivet.dev/actors). It supports binary and JSON streams, catch-up reads, long polling, SSE, idempotent producers, TTLs, stream closure, forks, and a read-only inspector. Existing Durable Streams clients work unchanged.

## Why Rivet

- **Bottomless SQLite**: streams are not capped by a per-object SQLite size limit.
- **SSD-backed with S3-tiered storage**: hot reads and writes are served from local SSDs, and history is tiered to S3 so storage is cheap, durable, and independent of any single machine.
- **Multi-region edge network**: each stream lives in the region closest to the clients reading and writing it.
- **Same code locally and in production**: what runs on your laptop is what runs in production. No mocks and no emulators.
- **Built in Rust**: the server is native Rust for high-performance streams.
- **Open-source and self-hostable**: run it in your own VPC or on-prem, so your data stays with you.

## Getting started

**Start the server**

Already running Rivet Actors with the RivetKit TypeScript SDK? Durable Streams are already available to you on RivetKit 2.3.12 and later. No separate server needed.

Otherwise, start a local Rivet Engine and the Durable Streams server:

```sh
npx @rivet-dev/services dev
```

Your streams are at `http://127.0.0.1:8642/durable-streams/v1/stream/<path>`.

**Use it via curl**

```sh
# Create a stream with an initial record
curl -i -X PUT \
  -H 'content-type: application/json' \
  --data '[{"message":"hello"}]' \
  http://127.0.0.1:8642/durable-streams/v1/stream/demo

# Append to it
curl -i -X POST \
  -H 'content-type: application/json' \
  --data '{"message":"world"}' \
  http://127.0.0.1:8642/durable-streams/v1/stream/demo

# Read from the beginning
curl 'http://127.0.0.1:8642/durable-streams/v1/stream/demo?offset=-1'
```

**Use it via TypeScript**

Install the official client:

```sh
npm install @durable-streams/client
```

```ts
import { DurableStream } from "@durable-streams/client";

const stream = await DurableStream.create({
	url: "http://127.0.0.1:8642/durable-streams/v1/stream/demo",
	contentType: "application/json",
});

await stream.append(JSON.stringify({ message: "hello" }));

const res = await stream.stream<{ message: string }>();
res.subscribeJson(async (batch) => {
	for (const item of batch.items) {
		console.log(item.message);
	}
});
```

## Deploying

Durable Streams runs as a single service on [Rivet Cloud](https://dashboard.rivet.dev) or on your own Rivet Engine. See the [integration guide](https://rivet.dev/actors/integrations/durable-streams) for both.

## Resources

- [Integration guide](https://rivet.dev/actors/integrations/durable-streams): run locally, deploy to Rivet Cloud, or self-host.
- [Announcement post](https://rivet.dev/blog/2026-09-03-durable-streams-now-supports-rivet-actors): why Durable Streams on Rivet, and how it works.
- [Durable Streams protocol](https://durablestreams.com): the full protocol, JSON mode, StreamDB, and the Yjs, TanStack AI, and Vercel AI SDK integrations.
- [Discord](https://rivet.dev/discord): questions and support.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for running the server from source and testing.

## License

Apache 2.0
