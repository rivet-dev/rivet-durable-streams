import { runConformanceTests } from "@durable-streams/server-conformance-tests";

const baseUrl = process.env.CONFORMANCE_TEST_URL;
if (!baseUrl) throw new Error("CONFORMANCE_TEST_URL is required");

runConformanceTests({ baseUrl, subscriptions: true });
