// The official conformance suite, against the server at DURABLE_STREAMS_URL
// (for example `mix slap.server --streams --long-poll-timeout 500`, as the reference
// servers use for this suite). Subscriptions are not enabled, so their tests
// are skipped.
import { runConformanceTests } from "@durable-streams/server-conformance-tests"

const baseUrl = process.env.DURABLE_STREAMS_URL ?? `http://127.0.0.1:4437`

runConformanceTests({ baseUrl })
