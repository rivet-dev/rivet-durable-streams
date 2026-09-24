import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    setupFiles: ["./caller-auth.ts"],
    include: ["conformance.test.ts", "subscriptions.test.ts"],
    // Upstream TTL cases have short windows that are sensitive to test contention.
    fileParallelism: false,
    maxConcurrency: 1,
    // Randomized cases perform hundreds of real-Engine storage operations.
    testTimeout: 180_000,
  },
});
