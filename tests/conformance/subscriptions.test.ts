import { createPublicKey, randomUUID, verify } from "node:crypto";
import { createServer, request as httpRequest } from "node:http";
import type { AddressInfo } from "node:net";
import { afterEach, describe, expect, test } from "vitest";

const origin = process.env.CONFORMANCE_TEST_URL;
if (!origin) throw new Error("CONFORMANCE_TEST_URL is required");
const root = `${origin}/v1/stream`;
const streams: string[] = [];
const subscriptions: string[] = [];
const receivers: ReturnType<typeof createServer>[] = [];
const prefix = () => `subscription-extra/${randomUUID()}`;
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

type Link = { path: string; acked_offset: string; link_type: string };
type Info = Link & { tail_offset: string; has_pending: boolean };
type Wake = { wake_id: string; generation: number; token: string; streams: Info[] };
type Notification = Wake & { callback_token: string; callback_url: string };
type Config = Record<string, unknown>;

function request(path: string, method = "GET", body?: unknown, token?: string) {
  return fetch(`${root}/${path}`, {
    method,
    headers: {
      ...(body === undefined ? {} : { "content-type": "application/json" }),
      ...(token ? { authorization: `Bearer ${token}` } : {}),
    },
    body: body === undefined ? undefined : JSON.stringify(body),
    signal: AbortSignal.timeout(15_000),
  });
}

async function json<T = any>(response: Response, status = 200): Promise<T> {
  expect(response.status, `Unexpected response from ${response.url}`).toBe(status);
  return response.json();
}

async function engine(path: string, method = "GET", body?: unknown, token = process.env.RIVET_TOKEN, status = 200) {
  const endpoint = process.env.RIVET_ENDPOINT;
  if (!endpoint || !token) throw new Error("Engine signing tests require RIVET_ENDPOINT and RIVET_TOKEN from the conformance harness");
  return json(await fetch(`${endpoint}${path}`, {
    method, headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(15_000),
  }), status);
}

async function poll<T>(read: () => Promise<T | undefined>, timeout = 12_000): Promise<T> {
  const deadline = Date.now() + timeout;
  do {
    const result = await read();
    if (result !== undefined) return result;
    await sleep(40);
  } while (Date.now() < deadline);
  throw new Error("Timed out waiting for subscription state");
}

async function concurrent<T, R>(values: T[], run: (value: T) => Promise<R>): Promise<R[]> {
  const output: R[] = new Array(values.length);
  let next = 0;
  const workers = await Promise.allSettled(Array.from({ length: Math.min(8, values.length) }, async () => {
    for (let index = next++; index < values.length; index = next++) output[index] = await run(values[index]);
  }));
  for (const result of workers) if (result.status === "rejected") throw result.reason;
  return output;
}

async function createStream(path: string, records: unknown[] = []) {
  streams.push(path);
  expect((await request(path, "PUT", records)).status).toBe(201);
  return tail(path);
}

async function tail(path: string) {
  const response = await request(path, "HEAD");
  expect(response.status).toBe(200);
  const offset = response.headers.get("stream-next-offset");
  expect(offset).toBeTruthy();
  return offset!;
}

async function append(path: string, value: unknown) {
  expect((await request(path, "POST", value)).status).toBe(204);
  return tail(path);
}

async function subscribe(config: Config, id = `extra-${randomUUID()}`) {
  subscriptions.push(id);
  await json(await request(`__ds/subscriptions/${id}`, "PUT", config), 201);
  return id;
}

async function pull(paths: string[], extra: Config = {}) {
  const wake = `${prefix()}/wake`;
  await createStream(wake);
  const config = { type: "pull-wake", streams: paths, wake_stream: wake, lease_ttl_ms: 10_000, ...extra };
  const id = await subscribe(config);
  return { id, wake, config };
}

async function readSubscription(id: string): Promise<{ streams: Link[]; status: string }> {
  return json(await request(`__ds/subscriptions/${id}`));
}

async function claim(id: string, worker = "worker"): Promise<Wake> {
  return poll(async () => {
    const response = await request(`__ds/subscriptions/${id}/claim`, "POST", { worker });
    if (response.status === 409) {
      const body = await response.json();
      expect(body.error.code).toBe("NO_PENDING_WORK");
      return undefined;
    }
    return json<Wake>(response);
  });
}

function progress(id: string, wake: Wake, acks: { stream: string; offset: string }[] = [], done = true) {
  return request(`__ds/subscriptions/${id}/ack`, "POST", {
    wake_id: wake.wake_id, generation: wake.generation, acks, done,
  }, wake.token);
}

async function receiver(reply: (index: number, notification: Notification) => unknown | Promise<unknown> = () => ({})) {
  const notifications: Notification[] = [];
  const server = createServer(async (req, res) => {
    try {
      const chunks = [];
      for await (const chunk of req) chunks.push(chunk);
      const body = JSON.parse(Buffer.concat(chunks).toString());
      const index = notifications.push(body) - 1;
      const response: any = await reply(index, body);
      res.writeHead(response?.status ?? 200, { "content-type": "application/json", ...response?.headers });
      res.end(JSON.stringify(response?.body ?? response ?? {}));
    } catch {
      res.writeHead(500).end();
    }
  });
  receivers.push(server);
  await new Promise<void>((resolve) => server.listen(0, "0.0.0.0", resolve));
  return {
    url: `http://127.0.0.1:${(server.address() as AddressInfo).port}/hook`,
    notifications,
    next: (index = 0) => poll(async () => notifications[index]),
  };
}

afterEach(async () => {
  await concurrent(subscriptions.splice(0), (id) => request(`__ds/subscriptions/${id}`, "DELETE").catch(() => undefined));
  await Promise.all(receivers.splice(0).map((server) => new Promise<void>((resolve) => {
    server.closeAllConnections();
    server.close(() => resolve());
  })));
  await concurrent(streams.splice(0), (path) => request(path, "DELETE").catch(() => undefined));
}, 90_000);

describe("Subscription durability and protocol edges", () => {
  test("backfills existing streams at tail, respects single segments, and wakes on initial PUT", async () => {
    const base = prefix();
    const old = `${base}/events/old`;
    const oldTail = await createStream(old, [{ n: 0 }]);
    const { id } = await pull([], { pattern: `${base}/events/*` });
    expect((await readSubscription(id)).streams).toContainEqual({ path: old, link_type: "glob", acked_offset: oldTail });
    const nested = `${base}/events/nested/child`;
    await createStream(nested, [{ n: 1 }]);
    const fresh = `${base}/events/new`;
    const freshTail = await createStream(fresh, [{ n: 2 }]);
    const claimed = await claim(id);
    expect(claimed.streams.find((s) => s.path === old)?.has_pending).toBe(false);
    expect(claimed.streams.find((s) => s.path === fresh)?.has_pending).toBe(true);
    expect((await readSubscription(id)).streams.some((s) => s.path === nested)).toBe(false);
    expect(await json(await progress(id, claimed, [{ stream: fresh, offset: freshTail }]))).toEqual({ ok: true, next_wake: false });
    const nextTail = await append(old, { n: 3 });
    const next = await claim(id);
    expect(next.generation).toBeGreaterThan(claimed.generation);
    await json(await progress(id, next, [{ stream: old, offset: nextTail }]));
  });

  test("double-star matches zero and multiple path segments", async () => {
    const base = `${prefix()}/events`;
    const { id } = await pull([], { pattern: `${base}/**` });
    await createStream(base, [{ n: 1 }]);
    await createStream(`${base}/a/b`, [{ n: 2 }]);
    await poll(async () => {
      const paths = (await readSubscription(id)).streams.map((s) => s.path);
      return paths.includes(base) && paths.includes(`${base}/a/b`) ? true : undefined;
    });
  });

  test("deep valid paths discover pattern subscriptions without database query limits", async () => {
    const base = prefix();
    const source = `${base}/${Array(520).fill("s").join("/")}`;
    expect(Buffer.byteLength(source)).toBeLessThan(2048);
    const { id, wake } = await pull([], { pattern: `${base}/**` });
    const offset = await createStream(source, [{ depth: 520 }]);
    await poll(async () => (await json(await request(wake))).some((event: any) => event.subscription_id === id) ? true : undefined);
    const claimed = await claim(id);
    expect(claimed.streams).toHaveLength(1);
    expect(claimed.streams[0]).toMatchObject({ path: source, link_type: "glob", tail_offset: offset, has_pending: true });
    expect(await json(await request(`${source}?offset=${encodeURIComponent(claimed.streams[0].acked_offset)}`))).toEqual([{ depth: 520 }]);
    expect(await json(await progress(id, claimed, [{ stream: source, offset }]))).toEqual({ ok: true, next_wake: false });
  });

  test("explicit removal preserves glob cursor and reconfirmation preserves membership changes", async () => {
    const base = prefix();
    const source = `${base}/source`;
    await createStream(source);
    const { id, config } = await pull([source, source], { pattern: `${base}/*` });
    const offset = await append(source, { n: 1 });
    const claimed = await claim(id);
    await json(await progress(id, claimed, [{ stream: source, offset }], false));
    expect((await request(`__ds/subscriptions/${id}/streams/${encodeURIComponent(source)}`, "DELETE")).status).toBe(204);
    expect((await readSubscription(id)).streams).toContainEqual({ path: source, link_type: "glob", acked_offset: offset });
    const extra = `${prefix()}/absent`;
    expect((await request(`__ds/subscriptions/${id}/streams`, "POST", { streams: [source, extra] })).status).toBe(204);
    await json(await request(`__ds/subscriptions/${id}`, "PUT", { ...config, streams: [source] }));
    const links = (await readSubscription(id)).streams;
    expect(links.find((s) => s.path === source)?.acked_offset).toBe(offset);
    expect(links.some((s) => s.path === extra && s.link_type === "explicit")).toBe(true);
    expect((await request(`__ds/subscriptions/${id}`, "PUT", { ...config, description: "different" })).status).toBe(409);
  });

  test("ack batches reject atomically and acknowledged offsets resume real reads", async () => {
    const base = prefix();
    const a = `${base}/a`, b = `${base}/b`;
    const beginning = await createStream(a);
    await createStream(b);
    const { id } = await pull([a, b]);
    const first = await append(a, { n: 1 });
    const second = await append(b, { n: 2 });
    const claimed = await claim(id);
    for (const bad of [
      { stream: `${base}/unknown`, offset: first },
      { stream: b, offset: "not-an-offset" },
      { stream: b, offset: "9999999999999999_9999999999999999" },
      { stream: a, offset: beginning },
    ]) {
      const result = await json(await progress(id, claimed, [{ stream: a, offset: first }, bad], false), 409);
      expect(result.error.code).toBe("INVALID_OFFSET");
      expect((await readSubscription(id)).streams.find((s) => s.path === a)?.acked_offset).toBe(beginning);
    }
    await json(await progress(id, claimed, [{ stream: a, offset: first }], false));
    expect((await progress(id, claimed, [{ stream: a, offset: beginning }], false)).status).toBe(409);
    await json(await progress(id, claimed, [{ stream: b, offset: second }]));
    await append(a, { n: 3 });
    expect(await json(await request(`${a}?offset=${encodeURIComponent(first)}`))).toEqual([{ n: 3 }]);
  });

  test("byte-stream acknowledgment resumes at the returned offset without replay or skipped bytes", async () => {
    const source = `${prefix()}/bytes`;
    streams.push(source);
    expect((await fetch(`${root}/${source}`, {
      method: "PUT", headers: { "content-type": "application/octet-stream" }, body: new Uint8Array(),
      signal: AbortSignal.timeout(15_000),
    })).status).toBe(201);
    const { id } = await pull([source]);
    const first = new Uint8Array([0, 255, 10, 195, 169]);
    expect((await fetch(`${root}/${source}`, {
      method: "POST", headers: { "content-type": "application/octet-stream" }, body: first,
      signal: AbortSignal.timeout(15_000),
    })).status).toBe(204);
    const claimed = await claim(id);
    const offset = claimed.streams.find((s) => s.path === source)!.tail_offset;
    expect(new Uint8Array(await (await request(source)).arrayBuffer())).toEqual(first);
    await json(await progress(id, claimed, [{ stream: source, offset }]));
    const second = new Uint8Array([13, 0, 128, 254]);
    expect((await fetch(`${root}/${source}`, {
      method: "POST", headers: { "content-type": "application/octet-stream" }, body: second,
      signal: AbortSignal.timeout(15_000),
    })).status).toBe(204);
    const resumed = await request(`${source}?offset=${encodeURIComponent(offset)}`);
    expect(resumed.status).toBe(200);
    expect(new Uint8Array(await resumed.arrayBuffer())).toEqual(second);
    const current = await claim(id);
    await json(await progress(id, current, [{ stream: source, offset: current.streams[0].tail_offset }]));
  });

  test("new forks start after inherited and sub-offset bytes, then wake on new appends", async () => {
    const source = `${prefix()}/source`, base = `${prefix()}/forks`;
    streams.push(source);
    expect((await fetch(`${root}/${source}`, {
      method: "PUT", headers: { "content-type": "text/plain" }, body: "hello",
      signal: AbortSignal.timeout(15_000),
    })).status).toBe(201);
    const { id } = await pull([], { pattern: `${base}/*` });
    for (const [suffix, inherited, subOffset] of [["full", "hello", undefined], ["partial", "hel", "3"]] as const) {
      const fork = `${base}/${suffix}`;
      streams.push(fork);
      expect((await fetch(`${root}/${fork}`, {
        method: "PUT",
        signal: AbortSignal.timeout(15_000),
        headers: {
          "content-type": "text/plain", "stream-forked-from": new URL(`${root}/${source}`).pathname,
          ...(subOffset === undefined ? {} : {
            "stream-fork-offset": "0000000000000000_0000000000000000", "stream-fork-sub-offset": subOffset,
          }),
        },
      })).status).toBe(201);
      const inheritedTail = await tail(fork);
      await poll(async () => (await readSubscription(id)).streams.find((s) => s.path === fork)?.acked_offset === inheritedTail ? true : undefined);
      expect(await (await request(fork)).text()).toBe(inherited);
      expect((await json(await request(`__ds/subscriptions/${id}/claim`, "POST", { worker: "historical" }), 409)).error.code).toBe("NO_PENDING_WORK");
      expect((await fetch(`${root}/${fork}`, {
        method: "POST", headers: { "content-type": "text/plain" }, body: "!",
        signal: AbortSignal.timeout(15_000),
      })).status).toBe(204);
      const claimed = await claim(id);
      const link = claimed.streams.find((s) => s.path === fork)!;
      expect(link).toMatchObject({ acked_offset: inheritedTail, has_pending: true });
      expect(await (await request(`${fork}?offset=${encodeURIComponent(inheritedTail)}`)).text()).toBe("!");
      expect(await json(await progress(id, claimed, [{ stream: fork, offset: link.tail_offset }]))).toEqual({ ok: true, next_wake: false });
    }
  });

  test("synchronous webhook completion acknowledges only its transmitted snapshot", async () => {
    let finish!: () => void;
    const held = new Promise<void>((resolve) => { finish = resolve; });
    const hook = await receiver(async (index) => {
      if (index === 0) await held;
      return { done: true };
    });
    const source = `${prefix()}/source`;
    await createStream(source);
    const id = await subscribe({ type: "webhook", streams: [source], webhook: { url: hook.url }, lease_ttl_ms: 10_000 });
    const first = await append(source, { n: 1 });
    const wake1 = await hook.next();
    expect(wake1.streams.find((s) => s.path === source)?.tail_offset).toBe(first);
    const second = await append(source, { n: 2 });
    finish();
    const wake2 = await hook.next(1);
    expect(wake2.generation).toBeGreaterThan(wake1.generation);
    expect(wake2.wake_id).not.toBe(wake1.wake_id);
    expect(wake2.streams.find((s) => s.path === source)?.acked_offset).toBe(first);
    expect(wake2.streams.find((s) => s.path === source)?.tail_offset).toBe(second);
    await poll(async () => (await readSubscription(id)).streams.find((s) => s.path === source)?.acked_offset === second ? true : undefined);
  });

  test("late HTTP completion cannot acknowledge an expired wake's replacement", async () => {
    let finish!: () => void;
    const held = new Promise<void>((resolve) => { finish = resolve; });
    const hook = await receiver(async (index) => {
      if (index === 0) { await held; return { done: true }; }
      return {};
    });
    const source = `${prefix()}/source`;
    const beginning = await createStream(source);
    const id = await subscribe({ type: "webhook", streams: [source], webhook: { url: hook.url }, lease_ttl_ms: 1000 });
    const offset = await append(source, { n: 1 });
    const old = await hook.next();
    const current = await hook.next(1);
    expect(current.generation).toBeGreaterThan(old.generation);
    finish();
    await sleep(150);
    expect((await readSubscription(id)).streams.find((s) => s.path === source)?.acked_offset).toBe(beginning);
    const stale = await json(await request(`__ds/subscriptions/${id}/callback`, "POST", {
      wake_id: old.wake_id, generation: old.generation, done: true,
    }, old.callback_token), 409);
    expect(stale.error.code).toBe("FENCED");
    await json(await request(`__ds/subscriptions/${id}/callback`, "POST", {
      wake_id: current.wake_id, generation: current.generation, acks: [{ stream: source, offset }], done: true,
    }, current.callback_token));
  });

  test("concurrent claims have one winner and heartbeats extend ownership", async () => {
    const source = `${prefix()}/source`;
    await createStream(source);
    const lease = 3000;
    const { id, wake } = await pull([source], { lease_ttl_ms: lease });
    await append(source, { n: 1 });
    await poll(async () => (await json(await request(wake))).some((event: any) => event.subscription_id === id) ? true : undefined);
    const attempts = await Promise.all(Array.from({ length: 6 }, (_, i) => request(`__ds/subscriptions/${id}/claim`, "POST", { worker: `worker-${i}` })));
    expect(attempts.filter((r) => r.status === 200)).toHaveLength(1);
    const winner = await json<Wake>(attempts.find((r) => r.status === 200)!);
    for (const loser of attempts.filter((r) => r.status !== 200)) {
      const busy = await json(loser, 409);
      expect(busy.error.code).toBe("ALREADY_CLAIMED");
      expect(busy.error.current_holder).toMatch(/^worker-/);
    }
    // Keep ownership past the original lease without assuming that six actor
    // round trips finish within a fraction of a one-second lease under load.
    const heartbeatUntil = Date.now() + lease + 200;
    do {
      await json(await progress(id, winner, [], false));
      await sleep(300);
    } while (Date.now() < heartbeatUntil);
    expect((await json(await request(`__ds/subscriptions/${id}/claim`, "POST", { worker: "too-early" }), 409)).error.code).toBe("ALREADY_CLAIMED");
    await sleep(lease + 200);
    const replacement = await claim(id, "after-expiry");
    expect(replacement.generation).toBeGreaterThan(winner.generation);
    expect((await json(await progress(id, winner), 409)).error.code).toBe("FENCED");
    expect((await json(await request(`__ds/subscriptions/${id}/release`, "POST", {
      wake_id: winner.wake_id, generation: winner.generation,
    }, winner.token), 409)).error.code).toBe("FENCED");
  });

  test("claim capabilities reject tampering, other subscriptions and deleted incarnations", async () => {
    const source = `${prefix()}/source`;
    await createStream(source);
    const first = await pull([source]);
    const other = await pull([source]);
    await append(source, { n: 1 });
    const old = await claim(first.id);
    const currentOther = await claim(other.id);
    const badToken = `${old.token.slice(0, -8)}${old.token.at(-8) === "A" ? "B" : "A"}${old.token.slice(-7)}`;
    expect((await progress(first.id, { ...old, token: badToken })).status).toBe(401);
    expect((await progress(other.id, { ...currentOther, token: old.token })).status).toBe(401);
    expect((await request(`__ds/subscriptions/${first.id}`, "DELETE")).status).toBe(204);
    expect((await progress(first.id, old)).status).toBe(409);
    await subscribe(first.config, first.id);
    await append(source, { n: 2 });
    const replacement = await claim(first.id);
    expect(replacement.generation).toBeGreaterThan(old.generation);
    expect((await json(await progress(first.id, { ...replacement, token: old.token }), 409)).error.code).toBe("FENCED");
  });

  test("Engine application capabilities enforce signature, purpose, audience, tenant and namespace", async () => {
    const source = `${prefix()}/source`;
    const beginning = await createStream(source);
    const { id } = await pull([source], { lease_ttl_ms: 60_000 });
    const offset = await append(source, { n: 1 });
    const claimed = await claim(id);
    const [headerPart, payloadPart, signaturePart] = claimed.token.split(".");
    const header = JSON.parse(Buffer.from(headerPart, "base64url").toString());
    const payload = JSON.parse(Buffer.from(payloadPart, "base64url").toString());
    expect(header).toMatchObject({ alg: "EdDSA", typ: "rivet-app+jwt" });
    expect(payload).toMatchObject({ rivet_ver: 1, aud: "durable-streams", rivet_purpose: "durable-streams.claim" });
    expect(payload.exp - payload.iat).toBeGreaterThan(0);
    expect(payload.exp - payload.iat).toBeLessThanOrEqual(86_400);
    const ds = payload.app.durable_streams;
    expect(ds).toMatchObject({ root, subscription_id: id, wake_id: claimed.wake_id, generation: claimed.generation, operations: ["ack", "release"] });
    const key = (await engine("/.well-known/jwks.json")).keys.find((key: any) => key.kid === header.kid);
    expect(verify(null, Buffer.from(`${headerPart}.${payloadPart}`), createPublicKey({ key, format: "jwk" }), Buffer.from(signaturePart, "base64url"))).toBe(true);
    expect((await engine("/auth/tokens/inspect", "GET", undefined, claimed.token, 401)).code).toBe("invalid_token");

    const issue = (claims = ds, audience = "durable-streams", purpose = "durable-streams.claim", token = process.env.RIVET_TOKEN) =>
      engine("/auth/application-tokens", "POST", { audience, purpose, duration: 86_400, claims: { durable_streams: claims } }, token);
    for (const [claims, audience, purpose, status] of [
      [{ ...ds, root: `${root}/other` }, "durable-streams", "durable-streams.claim", 401],
      [{ ...ds, tenant_scope: "other-tenant" }, "durable-streams", "durable-streams.claim", 401],
      [ds, "other-audience", "durable-streams.claim", 401],
      [ds, "durable-streams", "durable-streams.callback", 401],
      [{ ...ds, operations: ["callback"] }, "durable-streams", "durable-streams.claim", 401],
      [{ ...ds, generation: claimed.generation + 1 }, "durable-streams", "durable-streams.claim", 409],
    ] as const) {
      const issued = await issue(claims, audience, purpose);
      expect((await progress(id, { ...claimed, token: issued.token })).status).toBe(status);
    }

    const admin = process.env.CONFORMANCE_ENGINE_ADMIN_TOKEN;
    if (!admin) throw new Error("Conformance Engine admin credential is required");
    const credential = async (namespace: string, duration: number) => {
      return engine("/auth/tokens", "POST", {
        namespace, duration,
        grants: [{ resource: "application_token", target: "any", operations: ["create"] }],
      }, admin);
    };
    const otherNamespace = `ds-other-${randomUUID()}`;
    await engine("/namespaces", "POST", { name: otherNamespace, display_name: "Other signing namespace" }, admin);
    const other = await credential(otherNamespace, 60);
    expect((await progress(id, { ...claimed, token: (await issue(ds, undefined, undefined, other.token)).token })).status).toBe(401);
    const shortCaller = await credential(process.env.RIVET_NAMESPACE!, 3);
    const short = await issue(ds, undefined, undefined, shortCaller.token);
    expect(short.expires_ts).toBeLessThanOrEqual(shortCaller.expires_ts);
    expect(short.expires_ts - short.issued_ts).toBeLessThanOrEqual(3000);
    await sleep(Math.max(0, short.expires_ts - Date.now()) + 1100);
    expect((await progress(id, { ...claimed, token: short.token })).status).toBe(401);
    expect((await readSubscription(id)).streams[0].acked_offset).toBe(beginning);
    await json(await progress(id, claimed, [{ stream: source, offset }]));
  });

  test("Engine operator rotation updates DS discovery without changing callback authority", async () => {
    const admin = process.env.CONFORMANCE_ENGINE_ADMIN_TOKEN;
    if (!admin) throw new Error("Conformance Engine admin credential is required");
    const source = `${prefix()}/source`;
    await createStream(source);
    const hook = await receiver();
    const id = await subscribe({ type: "webhook", streams: [source], webhook: { url: hook.url }, lease_ttl_ms: 60_000 });
    const offset = await append(source, { n: 1 });
    const notification = await hook.next();
    const sub = await json(await request(`__ds/subscriptions/${id}`));
    expect(sub.webhook.signing).toMatchObject({ alg: "ed25519", jwks_url: `${root}/__ds/jwks.json` });
    const metadata = await engine("/auth/webhooks/ensure", "POST");
    const management = `/namespaces/${encodeURIComponent(process.env.RIVET_NAMESPACE!)}/auth/webhook-keys`;
    const before = await engine(management, "GET", undefined, admin);
    expect(sub.webhook.signing.kid).toBe(before.active_kid);
    const staged = await engine(`${management}/rotate`, "POST", { expected_generation: before.generation }, admin);
    expect(staged.active_kid).toBe(before.active_kid);
    expect(typeof staged.pending_kid).toBe("string");
    const published = await json(await request("__ds/jwks.json"));
    expect(published.keys.some((key: any) => key.kid === staged.pending_kid)).toBe(true);
    expect(published).toEqual(await engine(`/namespaces/${metadata.namespace_id}/auth/webhooks/jwks.json`));
    const revoked = await engine(`${management}/revoke`, "POST", {
      request_id: randomUUID(), expected_generation: staged.generation, kids: [before.active_kid],
    }, admin);
    const current = await json(await request("__ds/jwks.json"));
    expect(current.keys.some((key: any) => key.kid === revoked.active_kid)).toBe(true);
    expect(current.keys.some((key: any) => key.kid === before.active_kid)).toBe(false);
    await json(await request(`__ds/subscriptions/${id}/callback`, "POST", {
      wake_id: notification.wake_id, generation: notification.generation, done: true, acks: [{ stream: source, offset }],
    }, notification.callback_token));
  });

  test("missing wake streams recover after the application creates them", async () => {
    const base = prefix();
    const source = `${base}/source`, wake = `${base}/wake`;
    const id = await subscribe({ type: "pull-wake", streams: [source], wake_stream: wake });
    await createStream(source, [{ n: 1 }]);
    expect((await request(wake, "HEAD")).status).toBe(404);
    await createStream(wake);
    await poll(async () => {
      const events = await json(await request(wake));
      return events.some((event: any) => event.subscription_id === id) ? true : undefined;
    });
    expect((await claim(id)).streams.find((s) => s.path === source)?.has_pending).toBe(true);
  });

  test("explicit subscriptions recover across stream deletion and recreation", async () => {
    const source = `${prefix()}/source`;
    await createStream(source);
    const { id } = await pull([source]);
    const offset = await append(source, { n: 1 });
    const first = await claim(id);
    await json(await progress(id, first, [{ stream: source, offset }]));
    expect((await request(source, "DELETE")).status).toBe(204);
    const recreated = await createStream(source, [{ n: 2 }]);
    const second = await claim(id);
    expect(second.streams.find((s) => s.path === source)?.has_pending).toBe(true);
    expect(second.generation).toBeGreaterThan(first.generation);
    await json(await progress(id, second, [{ stream: source, offset: recreated }]));
  });

  test.each(["pull-wake", "webhook"])("%s fences active capabilities when a stream is recreated at the same offset", async (type) => {
    const source = `${prefix()}/source`;
    await createStream(source);
    const hook = type === "webhook" ? await receiver() : undefined;
    const pulling = type === "pull-wake" ? await pull([source]) : undefined;
    const id = pulling?.id ?? await subscribe({ type, streams: [source], webhook: { url: hook!.url }, lease_ttl_ms: 10_000 });
    const oldTail = await append(source, { n: 1 });
    const old: Wake | Notification = hook ? await hook.next() : await claim(id);
    expect((await request(source, "DELETE")).status).toBe(204);
    const newTail = await createStream(source, [{ n: 2 }]);
    expect(newTail).toBe(oldTail);
    if (pulling) {
      await poll(async () => (await json(await request(pulling.wake))).some((event: any) => event.subscription_id === id && event.generation > old.generation) ? true : undefined);
    }
    const current = hook ? await hook.next(1) : await claim(id);
    const action = hook ? "callback" : "ack";
    const token = (value: Wake | Notification) => hook ? (value as Notification).callback_token : value.token;
    const stale = await json(await request(`__ds/subscriptions/${id}/${action}`, "POST", {
      wake_id: old.wake_id, generation: old.generation, acks: [{ stream: source, offset: oldTail }], done: true,
    }, token(old)), 409);
    expect(stale.error.code).toBe("FENCED");
    await json(await request(`__ds/subscriptions/${id}/${action}`, "POST", {
      wake_id: current.wake_id, generation: current.generation, acks: [{ stream: source, offset: newTail }], done: true,
    }, token(current)));
  });

  test("large persisted memberships and wake snapshots survive database storage limits", async () => {
    const base = prefix();
    const source = `${base}/source`;
    await createStream(source);
    const absent = Array.from({ length: 512 }, (_, index) => `${base}/missing-${index}-${"x".repeat(240)}`);
    const { id } = await pull(absent);
    expect((await request(`__ds/subscriptions/${id}/streams`, "POST", { streams: [source] })).status).toBe(204);
    const subscription = await readSubscription(id);
    expect(subscription.streams).toHaveLength(513);
    expect(Buffer.byteLength(JSON.stringify(subscription))).toBeGreaterThan(128 * 1024);
    const offset = await append(source, { n: 1 });
    const claimed = await claim(id);
    expect(claimed.streams).toHaveLength(513);
    expect(claimed.streams.find((s) => s.path === source)?.has_pending).toBe(true);
    expect(await json(await progress(id, claimed, [{ stream: source, offset }]))).toEqual({ ok: true, next_wake: false });
  });

  test("Unicode and space paths round-trip through encoded callback URLs and membership", async () => {
    const source = `${prefix()}/café room/雪`;
    const canonical = new URL(`${root}/${source}`).pathname.slice(new URL(root).pathname.length + 1);
    const hook = await receiver();
    const id = await subscribe({ type: "webhook", streams: [source], webhook: { url: hook.url }, lease_ttl_ms: 10_000 }, `extra id ${randomUUID()}`);
    const offset = await createStream(source, [{ n: 1 }]);
    const notification = await hook.next();
    expect(notification.callback_url).toBe(`${root}/__ds/subscriptions/${encodeURIComponent(id)}/callback`);
    expect(notification.streams.find((s) => s.path === canonical)?.tail_offset).toBe(offset);
    expect(await json(await request(canonical))).toEqual([{ n: 1 }]);
    const response = await fetch(notification.callback_url, {
      method: "POST",
      headers: { "content-type": "application/json", authorization: `Bearer ${notification.callback_token}` },
      body: JSON.stringify({ wake_id: notification.wake_id, generation: notification.generation, acks: [{ stream: source, offset }], done: true }),
      signal: AbortSignal.timeout(15_000),
    });
    expect(await json(response)).toEqual({ ok: true, next_wake: false });
    expect((await request(`__ds/subscriptions/${id}/streams/${encodeURIComponent(source)}`, "DELETE")).status).toBe(204);
    expect((await readSubscription(id)).streams).toEqual([]);
  });

  test("encoded slash and literal star paths retain distinct stream identities", async () => {
    const base = prefix();
    const encodedSlash = `${base}/a%2Fb`;
    const nested = `${base}/a/b`;
    const literalStar = `${base}/%2A`;
    await createStream(encodedSlash, [{ kind: "encoded slash" }]);
    await createStream(nested, [{ kind: "nested" }]);
    await createStream(literalStar);
    const { id } = await pull([encodedSlash, nested, literalStar]);
    expect((await readSubscription(id)).streams.map((s) => s.path).sort()).toEqual([encodedSlash, nested, literalStar].sort());
    expect(await json(await request(encodedSlash))).toEqual([{ kind: "encoded slash" }]);
    expect(await json(await request(nested))).toEqual([{ kind: "nested" }]);
    const offset = await append(literalStar, { kind: "literal star" });
    const claimed = await claim(id);
    expect(claimed.streams.find((s) => s.path === literalStar)?.has_pending).toBe(true);
    expect(claimed.streams.filter((s) => s.path !== literalStar).every((s) => !s.has_pending)).toBe(true);
    expect(await json(await progress(id, claimed, [{ stream: literalStar, offset }]))).toEqual({ ok: true, next_wake: false });
    expect((await request(`__ds/subscriptions/${id}/streams/${encodeURIComponent(encodedSlash)}`, "DELETE")).status).toBe(204);
    expect((await readSubscription(id)).streams.map((s) => s.path).sort()).toEqual([nested, literalStar].sort());
    expect((await request(`__ds/subscriptions/${id}/streams/${encodeURIComponent(literalStar)}`, "DELETE")).status).toBe(204);
    expect((await readSubscription(id)).streams.map((s) => s.path)).toEqual([nested]);
  });

  test("overlapping wake patterns suppress only their own notifications", async () => {
    const base = prefix();
    const source = `${base}/source`, wake = `${base}/wake`;
    await createStream(source);
    const wakeBeginning = await createStream(wake);
    const observer = await pull([wake]);
    const id = await subscribe({ type: "pull-wake", pattern: `${base}/**`, wake_stream: wake });
    const offset = await append(source, { n: 1 });
    await poll(async () => (await json(await request(observer.wake))).some((event: any) => event.subscription_id === observer.id) ? true : undefined);
    const claimed = await claim(id);
    expect(await json(await progress(id, claimed, [{ stream: source, offset }]))).toEqual({ ok: true, next_wake: false });
    expect((await json(await request(`__ds/subscriptions/${id}/claim`, "POST", { worker: "already-done" }), 409)).error.code).toBe("NO_PENDING_WORK");
    const observed = await claim(observer.id);
    const link = observed.streams.find((s) => s.path === wake)!;
    expect(link.has_pending).toBe(true);
    expect(link.acked_offset).toBe(wakeBeginning);
    const events = await json(await request(wake));
    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({ type: "wake", subscription_id: id });
    await json(await progress(observer.id, observed, [{ stream: wake, offset: link.tail_offset }]));
    await sleep(300);
    expect(await json(await request(wake))).toEqual(events);
  });

  test("fan-out beyond one page preserves notifications and current acknowledgment tails", async () => {
    const base = prefix();
    const source = `${base}/source`, wake = `${base}/wake`;
    await createStream(source);
    await createStream(wake);
    const ids = Array.from({ length: 70 }, (_, index) => `extra-fanout-${base.split("/").at(-1)}-${String(index).padStart(3, "0")}`);
    await concurrent(ids, (id) => subscribe({ type: "pull-wake", streams: [source], wake_stream: wake }, id));
    await append(source, { n: 1 });
    const currentTail = await append(source, { n: 2 });
    const claimed = await claim(ids[69]);
    expect(claimed.streams.find((s) => s.path === source)?.tail_offset).toBe(currentTail);
    expect(await json(await progress(ids[69], claimed, [{ stream: source, offset: currentTail }]))).toEqual({ ok: true, next_wake: false });
    // This second high-sort subscription is left unclaimed until publication,
    // proving the durable fan-out cursor traversed beyond the first 64 rows.
    await poll(async () => (await json(await request(wake))).some((event: any) => event.subscription_id === ids[68]) ? true : undefined, 30_000);
    const next = await claim(ids[68]);
    expect(await json(await progress(ids[68], next, [{ stream: source, offset: currentTail }]))).toEqual({ ok: true, next_wake: false });
  }, 90_000);

  test("registration racing appends cannot skip a write made after registration returns", async () => {
    const base = prefix();
    const source = `${base}/source`, wake = `${prefix()}/wake`;
    await createStream(source, [{ phase: "historical" }]);
    await createStream(wake);
    const [id] = await Promise.all([
      subscribe({ type: "pull-wake", pattern: `${base}/*`, wake_stream: wake }),
      append(source, { phase: "racing-a" }),
      append(source, { phase: "racing-b" }),
    ]);
    const marker = randomUUID();
    const markerTail = await append(source, { marker });
    let covered = false;
    for (let attempt = 0; attempt < 4; attempt++) {
      const claimed = await claim(id);
      const link = claimed.streams.find((s) => s.path === source)!;
      expect(link.has_pending).toBe(true);
      if (link.tail_offset === markerTail) {
        expect(await json(await request(`${source}?offset=${encodeURIComponent(link.acked_offset)}`))).toContainEqual({ marker });
        covered = true;
      }
      await json(await progress(id, claimed, [{ stream: source, offset: link.tail_offset }]));
      if (covered) break;
    }
    expect(covered).toBe(true);
  });

  test("rejects private IPv4, mapped IPv6, local IPv6 and credential-bearing destinations", async () => {
    for (const url of [
      "http://10.0.0.1/hook", "https://10.0.0.1/hook", "https://169.254.169.254/hook",
      "https://[fc00::1]/hook", "https://[fe80::1]/hook", "https://[::ffff:10.0.0.1]/hook",
      "http://user:password@127.0.0.1/hook",
    ]) {
      const response = await request(`__ds/subscriptions/unsafe-${randomUUID()}`, "PUT", {
        type: "webhook", streams: [`${prefix()}/source`], webhook: { url },
      });
      expect((await json(response, 400)).error.code).toBe("WEBHOOK_URL_REJECTED");
    }
  });

  test("webhook redirects are not followed and failed delivery is exposed", async () => {
    const target = await receiver();
    const redirect = await receiver(() => ({ status: 302, headers: { location: target.url }, body: {} }));
    const source = `${prefix()}/source`;
    await createStream(source);
    const id = await subscribe({ type: "webhook", streams: [source], webhook: { url: redirect.url }, lease_ttl_ms: 10_000 });
    await append(source, { n: 1 });
    await redirect.next();
    await poll(async () => (await readSubscription(id)).status === "failed" ? true : undefined);
    expect(target.notifications).toHaveLength(0);
    const jwksResponse = await request("__ds/jwks.json");
    expect(jwksResponse.headers.get("content-type")).toBe("application/jwk-set+json");
    expect(jwksResponse.headers.get("cache-control")).toBe("public, max-age=300");
    const jwks = await json(jwksResponse);
    for (const key of jwks.keys) {
      expect(Object.keys(key).sort()).toEqual(["alg", "crv", "kid", "kty", "use", "x"]);
    }
  });

  test("reserves only the first __ds segment and enforces control methods", async () => {
    expect((await request("__ds/unknown", "PUT", [{ n: 1 }])).status).toBe(404);
    expect((await request("__ds/unknown")).status).toBe(404);
    const source = `${prefix()}/__ds/ordinary`;
    await createStream(source, [{ n: 1 }]);
    expect(await json(await request(source))).toEqual([{ n: 1 }]);
    const { id } = await pull([source]);
    for (const action of ["claim", "ack", "callback", "release"]) {
      expect((await request(`__ds/subscriptions/${id}/${action}`)).status).toBe(405);
    }
  });

  test("rejects raw dot segments before reserved-route and mount normalization", async () => {
    const target = `${prefix()}/escaped`;
    const mount = new URL(root).pathname;
    // fetch normalizes dot segments before sending; node:http preserves the
    // request target so these exercise the server's routing boundary.
    for (const path of [
      `${mount}/ordinary/../__ds/unknown`,
      `${mount}/ordinary/%2e%2e/__ds/unknown`,
      `${mount}/__ds/../${target}`,
      `${mount}/__ds/.%2e/${target}`,
      `${mount}/./${target}`,
      `${mount}/%2e/${target}`,
      `${mount}/../stream/${target}`,
      `${mount}/%2E%2E/stream/${target}`,
    ]) {
      const status = await new Promise<number>((resolve, reject) => {
        const req = httpRequest(new URL(origin!), {
          method: "PUT", path, headers: { "content-type": "application/json", authorization: `Bearer ${process.env.RIVET_TOKEN}` }, timeout: 15_000,
        }, (res) => {
          res.resume();
          res.once("end", () => resolve(res.statusCode!));
          res.once("error", reject);
        });
        req.once("error", reject);
        req.once("timeout", () => req.destroy(new Error("Raw path request timed out")));
        req.end(JSON.stringify([{ escaped: true }]));
      });
      expect(status, path).toBe(400);
    }
    expect((await request(target, "HEAD")).status).toBe(404);
    expect((await request("__ds/unknown", "HEAD")).status).toBe(404);
    await createStream(target, [{ ordinary: true }]);
    expect(await json(await request(target))).toEqual([{ ordinary: true }]);
  });
});
