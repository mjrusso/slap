// Compares the results of scripts/streams_bench.sh in DIR.
//
//   node summary.js DIR
import { readdirSync, readFileSync, writeFileSync } from "node:fs"
import path from "node:path"

const METRICS = [
  { name: `Latency - Total RTT`, stats: [`p50`, `p99`], smaller: true },
  { name: `Throughput - Small Messages`, stats: [`p50`, `min`], smaller: false },
  { name: `Throughput - Large Messages`, stats: [`p50`, `min`], smaller: false },
]

const dir = process.argv[2]
const runs = readdirSync(dir)
  .filter((file) => file.endsWith(`.json`) && ![`smaller.json`, `bigger.json`].includes(file))
  .sort()
  .map((file) => JSON.parse(readFileSync(path.join(dir, file), `utf-8`)))

const targets = runs.map((run) => run.environment)
const lines = [
  `### Official Durable Streams benchmarks`,
  ``,
  `| Metric | ${targets.join(` | `)} |`,
  `|---|${targets.map(() => `---:`).join(`|`)}|`,
]
const smaller = []
const bigger = []

for (const metric of METRICS) {
  for (const stat of metric.stats) {
    const cells = runs.map((run) => {
      const result = run.results[metric.name]
      ;(metric.smaller ? smaller : bigger).push({
        name: `${run.environment}: ${metric.name} ${stat}`,
        unit: result.unit,
        value: result[stat],
      })
      return `${result[stat].toFixed(metric.smaller ? 2 : 0)} ${result.unit}`
    })
    lines.push(`| ${metric.name} ${stat} | ${cells.join(` | `)} |`)
  }
}

console.log(lines.join(`\n`))
writeFileSync(path.join(dir, `smaller.json`), JSON.stringify(smaller, null, 2))
writeFileSync(path.join(dir, `bigger.json`), JSON.stringify(bigger, null, 2))
