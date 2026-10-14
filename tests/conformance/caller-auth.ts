// Exercise the public data boundary with a caller credential. Subscription
// management/callback tests keep their own credentials and negative cases.
const origin = new URL(process.env.CONFORMANCE_TEST_URL!).origin;
const caller = process.env.RIVET_TOKEN;
if (!caller) throw new Error("RIVET_TOKEN is required for authenticated stream conformance");
const fetchWithoutCredentials = globalThis.fetch;
globalThis.fetch = (input, init) => {
  const request = new Request(input, init);
  const url = new URL(request.url);
  if (url.origin === origin && url.pathname.startsWith("/v1/stream/") &&
      !url.pathname.startsWith("/v1/stream/__ds/") && !request.headers.has("authorization")) {
    request.headers.set("authorization", `Bearer ${caller}`);
  }
  return fetchWithoutCredentials(request);
};
