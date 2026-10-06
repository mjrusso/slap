import { defineConfig } from "vitest/config"

export default defineConfig({
  test: {
    include: [`conformance.test.js`],
    benchmark: { include: [`benchmarks.bench.js`] },
    testTimeout: 30_000,
  },
})
