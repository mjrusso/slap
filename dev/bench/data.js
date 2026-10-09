window.BENCHMARK_DATA = {
  "lastUpdate": 1791512840605,
  "repoUrl": "https://github.com/mjrusso/slap",
  "entries": {
    "slap_slatedb (lower is better)": [
      {
        "commit": {
          "author": {
            "email": "mjr@mjrusso.com",
            "name": "Michael Russo",
            "username": "mjrusso"
          },
          "committer": {
            "email": "mjr@mjrusso.com",
            "name": "Michael Russo",
            "username": "mjrusso"
          },
          "distinct": true,
          "id": "589be1fcc1f71e1f63b19699030aa1b14a05d88c",
          "message": "chore(repo): link the benchmark charts and describe the new triggers",
          "timestamp": "2026-10-07T08:06:35-04:00",
          "tree_id": "e3c80da79a13973baef8edd005870ebbb3ad2d50",
          "url": "https://github.com/mjrusso/slap/commit/589be1fcc1f71e1f63b19699030aa1b14a05d88c"
        },
        "date": 1791376928814,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "memory: durable_seq (no runtime hop)",
            "value": 0.11,
            "unit": "us",
            "extra": "p99 0.16 us, 100000 samples"
          },
          {
            "name": "memory: get miss",
            "value": 17.44,
            "unit": "us",
            "extra": "p99 33.32 us, 100000 samples"
          },
          {
            "name": "memory: get hit",
            "value": 22.51,
            "unit": "us",
            "extra": "p99 38.65 us, 100000 samples"
          },
          {
            "name": "memory: put 100 B",
            "value": 25.19,
            "unit": "us",
            "extra": "p99 46.91 us, 100000 samples"
          },
          {
            "name": "memory: write, 100 puts",
            "value": 229.04,
            "unit": "us",
            "extra": "p99 647.96 us, 18755 samples"
          },
          {
            "name": "memory: scan 1,000 rows",
            "value": 6985.74,
            "unit": "us",
            "extra": "p99 7839.24 us, 708 samples"
          },
          {
            "name": "memory: get 1 MiB",
            "value": 21.55,
            "unit": "us",
            "extra": "p99 39.13 us, 1000 samples"
          },
          {
            "name": "memory: put 1 MiB",
            "value": 21.21,
            "unit": "us",
            "extra": "p99 276.9 us, 1000 samples"
          },
          {
            "name": "memory: get 256 KiB",
            "value": 27.3,
            "unit": "us",
            "extra": "p99 39.99 us, 1000 samples"
          },
          {
            "name": "memory: put 256 KiB",
            "value": 23.23,
            "unit": "us",
            "extra": "p99 212.26 us, 1000 samples"
          },
          {
            "name": "memory: get 4 KiB",
            "value": 16.86,
            "unit": "us",
            "extra": "p99 30.63 us, 1000 samples"
          },
          {
            "name": "memory: put 4 KiB",
            "value": 27.72,
            "unit": "us",
            "extra": "p99 50.92 us, 1000 samples"
          },
          {
            "name": "s3: durable put p50",
            "value": 101,
            "unit": "ms"
          },
          {
            "name": "s3: durable put p99",
            "value": 101.8,
            "unit": "ms"
          },
          {
            "name": "s3: durability lag p50",
            "value": 53.5,
            "unit": "ms"
          },
          {
            "name": "s3: durability lag p99",
            "value": 103.8,
            "unit": "ms"
          },
          {
            "name": "s3: object store requests per 1,000 writes",
            "value": 0.2,
            "unit": "requests"
          },
          {
            "name": "s3: get, no cache, p50",
            "value": 2368,
            "unit": "us"
          },
          {
            "name": "s3: get, no cache, p99",
            "value": 3242,
            "unit": "us"
          },
          {
            "name": "s3: get, warm cache, p50",
            "value": 34,
            "unit": "us"
          },
          {
            "name": "s3: get, warm cache, p99",
            "value": 57,
            "unit": "us"
          },
          {
            "name": "s3: reader lag p50",
            "value": 77.7,
            "unit": "ms"
          },
          {
            "name": "s3: reader lag p99",
            "value": 78.4,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: durable put p50",
            "value": 101.1,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: durable put p99",
            "value": 102.2,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: durability lag p50",
            "value": 73.5,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: durability lag p99",
            "value": 123.1,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: object store requests per 1,000 writes",
            "value": 0.3,
            "unit": "requests"
          },
          {
            "name": "s3 +20ms: get, no cache, p50",
            "value": 64393,
            "unit": "us"
          },
          {
            "name": "s3 +20ms: get, no cache, p99",
            "value": 85674,
            "unit": "us"
          },
          {
            "name": "s3 +20ms: get, warm cache, p50",
            "value": 65,
            "unit": "us"
          },
          {
            "name": "s3 +20ms: get, warm cache, p99",
            "value": 85,
            "unit": "us"
          },
          {
            "name": "s3 +20ms: reader lag p50",
            "value": 152.9,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: reader lag p99",
            "value": 204.1,
            "unit": "ms"
          },
          {
            "name": "s3: flush 5 ms: durable put p50",
            "value": 6.4,
            "unit": "ms"
          },
          {
            "name": "s3: flush 5 ms: durable put p99",
            "value": 6.9,
            "unit": "ms"
          },
          {
            "name": "s3: flush 5 ms: PUTs/s under load",
            "value": 164.2,
            "unit": "requests/s"
          },
          {
            "name": "s3: flush 10 ms: durable put p50",
            "value": 10.7,
            "unit": "ms"
          },
          {
            "name": "s3: flush 10 ms: durable put p99",
            "value": 11.9,
            "unit": "ms"
          },
          {
            "name": "s3: flush 10 ms: PUTs/s under load",
            "value": 90.4,
            "unit": "requests/s"
          },
          {
            "name": "s3: flush 20 ms: durable put p50",
            "value": 20.9,
            "unit": "ms"
          },
          {
            "name": "s3: flush 20 ms: durable put p99",
            "value": 22,
            "unit": "ms"
          },
          {
            "name": "s3: flush 20 ms: PUTs/s under load",
            "value": 47.4,
            "unit": "requests/s"
          },
          {
            "name": "s3: flush 100 ms: durable put p50",
            "value": 100.9,
            "unit": "ms"
          },
          {
            "name": "s3: flush 100 ms: durable put p99",
            "value": 102,
            "unit": "ms"
          },
          {
            "name": "s3: flush 100 ms: PUTs/s under load",
            "value": 9.9,
            "unit": "requests/s"
          },
          {
            "name": "s3: 8 shards: open all",
            "value": 168.4,
            "unit": "ms"
          },
          {
            "name": "s3: 8 shards: durable put p50",
            "value": 11.1,
            "unit": "ms"
          },
          {
            "name": "s3: 8 shards: durable put p99",
            "value": 16.7,
            "unit": "ms"
          },
          {
            "name": "s3: 8 shards: PUTs/s",
            "value": 718.1,
            "unit": "requests/s"
          },
          {
            "name": "s3: 16 shards: open all",
            "value": 275.4,
            "unit": "ms"
          },
          {
            "name": "s3: 16 shards: durable put p50",
            "value": 15.3,
            "unit": "ms"
          },
          {
            "name": "s3: 16 shards: durable put p99",
            "value": 34.3,
            "unit": "ms"
          },
          {
            "name": "s3: 16 shards: PUTs/s",
            "value": 1005.4,
            "unit": "requests/s"
          },
          {
            "name": "s3: 64 shards: open all",
            "value": 1248.2,
            "unit": "ms"
          },
          {
            "name": "s3: 64 shards: durable put p50",
            "value": 60.3,
            "unit": "ms"
          },
          {
            "name": "s3: 64 shards: durable put p99",
            "value": 124.8,
            "unit": "ms"
          },
          {
            "name": "s3: 64 shards: PUTs/s",
            "value": 1055.3,
            "unit": "requests/s"
          },
          {
            "name": "s3 +20ms: flush 5 ms: durable put p50",
            "value": 24.1,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: flush 5 ms: durable put p99",
            "value": 29.5,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: flush 5 ms: PUTs/s under load",
            "value": 40.8,
            "unit": "requests/s"
          },
          {
            "name": "s3 +20ms: flush 10 ms: durable put p50",
            "value": 24.2,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: flush 10 ms: durable put p99",
            "value": 34.3,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: flush 10 ms: PUTs/s under load",
            "value": 40.8,
            "unit": "requests/s"
          },
          {
            "name": "s3 +20ms: flush 20 ms: durable put p50",
            "value": 24.2,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: flush 20 ms: durable put p99",
            "value": 44.5,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: flush 20 ms: PUTs/s under load",
            "value": 40.2,
            "unit": "requests/s"
          },
          {
            "name": "s3 +20ms: flush 100 ms: durable put p50",
            "value": 101,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: flush 100 ms: durable put p99",
            "value": 102.2,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: flush 100 ms: PUTs/s under load",
            "value": 9.9,
            "unit": "requests/s"
          },
          {
            "name": "s3 +20ms: 8 shards: open all",
            "value": 542.6,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: 8 shards: durable put p50",
            "value": 24.9,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: 8 shards: durable put p99",
            "value": 48.7,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: 8 shards: PUTs/s",
            "value": 314.2,
            "unit": "requests/s"
          },
          {
            "name": "s3 +20ms: 16 shards: open all",
            "value": 584.2,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: 16 shards: durable put p50",
            "value": 26.9,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: 16 shards: durable put p99",
            "value": 55.5,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: 16 shards: PUTs/s",
            "value": 575.1,
            "unit": "requests/s"
          },
          {
            "name": "s3 +20ms: 64 shards: open all",
            "value": 2432.4,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: 64 shards: durable put p50",
            "value": 67.7,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: 64 shards: durable put p99",
            "value": 145.2,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: 64 shards: PUTs/s",
            "value": 953.6,
            "unit": "requests/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mjr@mjrusso.com",
            "name": "Michael Russo",
            "username": "mjrusso"
          },
          "committer": {
            "email": "mjr@mjrusso.com",
            "name": "Michael Russo",
            "username": "mjrusso"
          },
          "distinct": true,
          "id": "03281318f4bbc491e02108bab4158a48174ab4be",
          "message": "ci: test running slap.server via Mix.install",
          "timestamp": "2026-10-07T21:55:31-04:00",
          "tree_id": "8fb92ca885eab21e75bf5348fe5b62feeb1246b9",
          "url": "https://github.com/mjrusso/slap/commit/03281318f4bbc491e02108bab4158a48174ab4be"
        },
        "date": 1791424967688,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "memory: durable_seq (no runtime hop)",
            "value": 0.12,
            "unit": "us",
            "extra": "p99 0.21 us, 100000 samples"
          },
          {
            "name": "memory: get miss",
            "value": 33.09,
            "unit": "us",
            "extra": "p99 51.92 us, 100000 samples"
          },
          {
            "name": "memory: put 100 B",
            "value": 35.78,
            "unit": "us",
            "extra": "p99 63.6 us, 100000 samples"
          },
          {
            "name": "memory: get hit",
            "value": 37.27,
            "unit": "us",
            "extra": "p99 66.26 us, 100000 samples"
          },
          {
            "name": "memory: write, 100 puts",
            "value": 229.28,
            "unit": "us",
            "extra": "p99 688.52 us, 18673 samples"
          },
          {
            "name": "memory: scan 1,000 rows",
            "value": 6986.81,
            "unit": "us",
            "extra": "p99 8143.78 us, 708 samples"
          },
          {
            "name": "memory: get 1 MiB",
            "value": 32.68,
            "unit": "us",
            "extra": "p99 54.16 us, 1000 samples"
          },
          {
            "name": "memory: put 1 MiB",
            "value": 38.3,
            "unit": "us",
            "extra": "p99 94.53 us, 1000 samples"
          },
          {
            "name": "memory: get 256 KiB",
            "value": 34.57,
            "unit": "us",
            "extra": "p99 54.31 us, 1000 samples"
          },
          {
            "name": "memory: put 256 KiB",
            "value": 36.84,
            "unit": "us",
            "extra": "p99 507.8 us, 1000 samples"
          },
          {
            "name": "memory: put 4 KiB",
            "value": 31.45,
            "unit": "us",
            "extra": "p99 49.59 us, 1000 samples"
          },
          {
            "name": "memory: get 4 KiB",
            "value": 37.75,
            "unit": "us",
            "extra": "p99 44.52 us, 1000 samples"
          },
          {
            "name": "s3: durable put p50",
            "value": 101.1,
            "unit": "ms"
          },
          {
            "name": "s3: durable put p99",
            "value": 102.2,
            "unit": "ms"
          },
          {
            "name": "s3: durability lag p50",
            "value": 53.9,
            "unit": "ms"
          },
          {
            "name": "s3: durability lag p99",
            "value": 103.5,
            "unit": "ms"
          },
          {
            "name": "s3: object store requests per 1,000 writes",
            "value": 0.2,
            "unit": "requests"
          },
          {
            "name": "s3: get, no cache, p50",
            "value": 2434,
            "unit": "us"
          },
          {
            "name": "s3: get, no cache, p99",
            "value": 3345,
            "unit": "us"
          },
          {
            "name": "s3: get, warm cache, p50",
            "value": 97,
            "unit": "us"
          },
          {
            "name": "s3: get, warm cache, p99",
            "value": 132,
            "unit": "us"
          },
          {
            "name": "s3: reader lag p50",
            "value": 101.6,
            "unit": "ms"
          },
          {
            "name": "s3: reader lag p99",
            "value": 102.2,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: durable put p50",
            "value": 100.9,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: durable put p99",
            "value": 102,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: durability lag p50",
            "value": 74.3,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: durability lag p99",
            "value": 124.3,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: object store requests per 1,000 writes",
            "value": 0.3,
            "unit": "requests"
          },
          {
            "name": "s3 +20ms: get, no cache, p50",
            "value": 64887,
            "unit": "us"
          },
          {
            "name": "s3 +20ms: get, no cache, p99",
            "value": 87080,
            "unit": "us"
          },
          {
            "name": "s3 +20ms: get, warm cache, p50",
            "value": 68,
            "unit": "us"
          },
          {
            "name": "s3 +20ms: get, warm cache, p99",
            "value": 110,
            "unit": "us"
          },
          {
            "name": "s3 +20ms: reader lag p50",
            "value": 93.2,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: reader lag p99",
            "value": 143.4,
            "unit": "ms"
          },
          {
            "name": "s3: flush 5 ms: durable put p50",
            "value": 6,
            "unit": "ms"
          },
          {
            "name": "s3: flush 5 ms: durable put p99",
            "value": 7,
            "unit": "ms"
          },
          {
            "name": "s3: flush 5 ms: PUTs/s under load",
            "value": 164.7,
            "unit": "requests/s"
          },
          {
            "name": "s3: flush 10 ms: durable put p50",
            "value": 11.1,
            "unit": "ms"
          },
          {
            "name": "s3: flush 10 ms: durable put p99",
            "value": 11.9,
            "unit": "ms"
          },
          {
            "name": "s3: flush 10 ms: PUTs/s under load",
            "value": 90.3,
            "unit": "requests/s"
          },
          {
            "name": "s3: flush 20 ms: durable put p50",
            "value": 21,
            "unit": "ms"
          },
          {
            "name": "s3: flush 20 ms: durable put p99",
            "value": 22.2,
            "unit": "ms"
          },
          {
            "name": "s3: flush 20 ms: PUTs/s under load",
            "value": 47.4,
            "unit": "requests/s"
          },
          {
            "name": "s3: flush 100 ms: durable put p50",
            "value": 101.1,
            "unit": "ms"
          },
          {
            "name": "s3: flush 100 ms: durable put p99",
            "value": 102.1,
            "unit": "ms"
          },
          {
            "name": "s3: flush 100 ms: PUTs/s under load",
            "value": 9.9,
            "unit": "requests/s"
          },
          {
            "name": "s3: 8 shards: open all",
            "value": 162,
            "unit": "ms"
          },
          {
            "name": "s3: 8 shards: durable put p50",
            "value": 11.1,
            "unit": "ms"
          },
          {
            "name": "s3: 8 shards: durable put p99",
            "value": 16.9,
            "unit": "ms"
          },
          {
            "name": "s3: 8 shards: PUTs/s",
            "value": 716.9,
            "unit": "requests/s"
          },
          {
            "name": "s3: 16 shards: open all",
            "value": 302,
            "unit": "ms"
          },
          {
            "name": "s3: 16 shards: durable put p50",
            "value": 16.2,
            "unit": "ms"
          },
          {
            "name": "s3: 16 shards: durable put p99",
            "value": 34.6,
            "unit": "ms"
          },
          {
            "name": "s3: 16 shards: PUTs/s",
            "value": 959.9,
            "unit": "requests/s"
          },
          {
            "name": "s3: 64 shards: open all",
            "value": 1353.1,
            "unit": "ms"
          },
          {
            "name": "s3: 64 shards: durable put p50",
            "value": 64.6,
            "unit": "ms"
          },
          {
            "name": "s3: 64 shards: durable put p99",
            "value": 136.5,
            "unit": "ms"
          },
          {
            "name": "s3: 64 shards: PUTs/s",
            "value": 991.1,
            "unit": "requests/s"
          },
          {
            "name": "s3 +20ms: flush 5 ms: durable put p50",
            "value": 24.9,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: flush 5 ms: durable put p99",
            "value": 29.9,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: flush 5 ms: PUTs/s under load",
            "value": 40.1,
            "unit": "requests/s"
          },
          {
            "name": "s3 +20ms: flush 10 ms: durable put p50",
            "value": 24.8,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: flush 10 ms: durable put p99",
            "value": 35.1,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: flush 10 ms: PUTs/s under load",
            "value": 39.3,
            "unit": "requests/s"
          },
          {
            "name": "s3 +20ms: flush 20 ms: durable put p50",
            "value": 25.1,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: flush 20 ms: durable put p99",
            "value": 45.3,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: flush 20 ms: PUTs/s under load",
            "value": 38.2,
            "unit": "requests/s"
          },
          {
            "name": "s3 +20ms: flush 100 ms: durable put p50",
            "value": 101.4,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: flush 100 ms: durable put p99",
            "value": 101.9,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: flush 100 ms: PUTs/s under load",
            "value": 9.9,
            "unit": "requests/s"
          },
          {
            "name": "s3 +20ms: 8 shards: open all",
            "value": 557.8,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: 8 shards: durable put p50",
            "value": 25.3,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: 8 shards: durable put p99",
            "value": 50.3,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: 8 shards: PUTs/s",
            "value": 309.4,
            "unit": "requests/s"
          },
          {
            "name": "s3 +20ms: 16 shards: open all",
            "value": 615.1,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: 16 shards: durable put p50",
            "value": 27.7,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: 16 shards: durable put p99",
            "value": 58.1,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: 16 shards: PUTs/s",
            "value": 563.3,
            "unit": "requests/s"
          },
          {
            "name": "s3 +20ms: 64 shards: open all",
            "value": 2440.8,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: 64 shards: durable put p50",
            "value": 72.8,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: 64 shards: durable put p99",
            "value": 155.8,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: 64 shards: PUTs/s",
            "value": 884.8,
            "unit": "requests/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mjr@mjrusso.com",
            "name": "Michael Russo",
            "username": "mjrusso"
          },
          "committer": {
            "email": "mjr@mjrusso.com",
            "name": "Michael Russo",
            "username": "mjrusso"
          },
          "distinct": true,
          "id": "ebb5692be526532a00c728d08bb1ab201c2ce047",
          "message": "chore(repo): prepare slap_slatedb 0.2.0",
          "timestamp": "2026-10-08T22:15:38-04:00",
          "tree_id": "fec69e28e0c17e00380f60d264426046182a8379",
          "url": "https://github.com/mjrusso/slap/commit/ebb5692be526532a00c728d08bb1ab201c2ce047"
        },
        "date": 1791512838139,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "memory: durable_seq (no runtime hop)",
            "value": 0.12,
            "unit": "us",
            "extra": "p99 0.19 us, 100000 samples"
          },
          {
            "name": "memory: put 100 B",
            "value": 36.56,
            "unit": "us",
            "extra": "p99 65.45 us, 100000 samples"
          },
          {
            "name": "memory: get miss",
            "value": 33.77,
            "unit": "us",
            "extra": "p99 72.09 us, 100000 samples"
          },
          {
            "name": "memory: get hit",
            "value": 69.68,
            "unit": "us",
            "extra": "p99 80.48 us, 74800 samples"
          },
          {
            "name": "memory: write, 100 puts",
            "value": 257.98,
            "unit": "us",
            "extra": "p99 774.83 us, 16509 samples"
          },
          {
            "name": "memory: scan 1,000 rows",
            "value": 6050.2,
            "unit": "us",
            "extra": "p99 8638.52 us, 801 samples"
          },
          {
            "name": "memory: get 1 MiB",
            "value": 36.99,
            "unit": "us",
            "extra": "p99 42.32 us, 1000 samples"
          },
          {
            "name": "memory: put 1 MiB",
            "value": 31.93,
            "unit": "us",
            "extra": "p99 811.91 us, 1000 samples"
          },
          {
            "name": "memory: get 256 KiB",
            "value": 33.01,
            "unit": "us",
            "extra": "p99 54.19 us, 1000 samples"
          },
          {
            "name": "memory: put 256 KiB",
            "value": 32.01,
            "unit": "us",
            "extra": "p99 1016.19 us, 1000 samples"
          },
          {
            "name": "memory: get 4 KiB",
            "value": 37.84,
            "unit": "us",
            "extra": "p99 47.1 us, 1000 samples"
          },
          {
            "name": "memory: put 4 KiB",
            "value": 43.84,
            "unit": "us",
            "extra": "p99 66.07 us, 1000 samples"
          },
          {
            "name": "s3: durable put p50",
            "value": 101.1,
            "unit": "ms"
          },
          {
            "name": "s3: durable put p99",
            "value": 102.1,
            "unit": "ms"
          },
          {
            "name": "s3: durability lag p50",
            "value": 54.1,
            "unit": "ms"
          },
          {
            "name": "s3: durability lag p99",
            "value": 104.1,
            "unit": "ms"
          },
          {
            "name": "s3: object store requests per 1,000 writes",
            "value": 0.2,
            "unit": "requests"
          },
          {
            "name": "s3: get, no cache, p50",
            "value": 2506,
            "unit": "us"
          },
          {
            "name": "s3: get, no cache, p99",
            "value": 3403,
            "unit": "us"
          },
          {
            "name": "s3: get, warm cache, p50",
            "value": 106,
            "unit": "us"
          },
          {
            "name": "s3: get, warm cache, p99",
            "value": 131,
            "unit": "us"
          },
          {
            "name": "s3: reader lag p50",
            "value": 32.7,
            "unit": "ms"
          },
          {
            "name": "s3: reader lag p99",
            "value": 34,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: durable put p50",
            "value": 101.1,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: durable put p99",
            "value": 102.4,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: durability lag p50",
            "value": 74.5,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: durability lag p99",
            "value": 124.4,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: object store requests per 1,000 writes",
            "value": 0.4,
            "unit": "requests"
          },
          {
            "name": "s3 +20ms: get, no cache, p50",
            "value": 65051,
            "unit": "us"
          },
          {
            "name": "s3 +20ms: get, no cache, p99",
            "value": 87602,
            "unit": "us"
          },
          {
            "name": "s3 +20ms: get, warm cache, p50",
            "value": 73,
            "unit": "us"
          },
          {
            "name": "s3 +20ms: get, warm cache, p99",
            "value": 131,
            "unit": "us"
          },
          {
            "name": "s3 +20ms: reader lag p50",
            "value": 86.4,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: reader lag p99",
            "value": 140.3,
            "unit": "ms"
          },
          {
            "name": "s3: flush 5 ms: durable put p50",
            "value": 5.8,
            "unit": "ms"
          },
          {
            "name": "s3: flush 5 ms: durable put p99",
            "value": 7,
            "unit": "ms"
          },
          {
            "name": "s3: flush 5 ms: PUTs/s under load",
            "value": 164.8,
            "unit": "requests/s"
          },
          {
            "name": "s3: flush 10 ms: durable put p50",
            "value": 11,
            "unit": "ms"
          },
          {
            "name": "s3: flush 10 ms: durable put p99",
            "value": 12.1,
            "unit": "ms"
          },
          {
            "name": "s3: flush 10 ms: PUTs/s under load",
            "value": 90.2,
            "unit": "requests/s"
          },
          {
            "name": "s3: flush 20 ms: durable put p50",
            "value": 21.1,
            "unit": "ms"
          },
          {
            "name": "s3: flush 20 ms: durable put p99",
            "value": 22.2,
            "unit": "ms"
          },
          {
            "name": "s3: flush 20 ms: PUTs/s under load",
            "value": 47.4,
            "unit": "requests/s"
          },
          {
            "name": "s3: flush 100 ms: durable put p50",
            "value": 101,
            "unit": "ms"
          },
          {
            "name": "s3: flush 100 ms: durable put p99",
            "value": 102.2,
            "unit": "ms"
          },
          {
            "name": "s3: flush 100 ms: PUTs/s under load",
            "value": 9.9,
            "unit": "requests/s"
          },
          {
            "name": "s3: 8 shards: open all",
            "value": 179.8,
            "unit": "ms"
          },
          {
            "name": "s3: 8 shards: durable put p50",
            "value": 11.1,
            "unit": "ms"
          },
          {
            "name": "s3: 8 shards: durable put p99",
            "value": 16.6,
            "unit": "ms"
          },
          {
            "name": "s3: 8 shards: PUTs/s",
            "value": 716.9,
            "unit": "requests/s"
          },
          {
            "name": "s3: 16 shards: open all",
            "value": 304.3,
            "unit": "ms"
          },
          {
            "name": "s3: 16 shards: durable put p50",
            "value": 15.8,
            "unit": "ms"
          },
          {
            "name": "s3: 16 shards: durable put p99",
            "value": 32.2,
            "unit": "ms"
          },
          {
            "name": "s3: 16 shards: PUTs/s",
            "value": 988,
            "unit": "requests/s"
          },
          {
            "name": "s3: 64 shards: open all",
            "value": 1328.8,
            "unit": "ms"
          },
          {
            "name": "s3: 64 shards: durable put p50",
            "value": 65.2,
            "unit": "ms"
          },
          {
            "name": "s3: 64 shards: durable put p99",
            "value": 136.6,
            "unit": "ms"
          },
          {
            "name": "s3: 64 shards: PUTs/s",
            "value": 980,
            "unit": "requests/s"
          },
          {
            "name": "s3 +20ms: flush 5 ms: durable put p50",
            "value": 24.7,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: flush 5 ms: durable put p99",
            "value": 30.1,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: flush 5 ms: PUTs/s under load",
            "value": 39.9,
            "unit": "requests/s"
          },
          {
            "name": "s3 +20ms: flush 10 ms: durable put p50",
            "value": 24.8,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: flush 10 ms: durable put p99",
            "value": 35,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: flush 10 ms: PUTs/s under load",
            "value": 39.5,
            "unit": "requests/s"
          },
          {
            "name": "s3 +20ms: flush 20 ms: durable put p50",
            "value": 24.8,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: flush 20 ms: durable put p99",
            "value": 45.2,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: flush 20 ms: PUTs/s under load",
            "value": 39,
            "unit": "requests/s"
          },
          {
            "name": "s3 +20ms: flush 100 ms: durable put p50",
            "value": 101.2,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: flush 100 ms: durable put p99",
            "value": 102.4,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: flush 100 ms: PUTs/s under load",
            "value": 9.9,
            "unit": "requests/s"
          },
          {
            "name": "s3 +20ms: 8 shards: open all",
            "value": 549.9,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: 8 shards: durable put p50",
            "value": 25.3,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: 8 shards: durable put p99",
            "value": 49.7,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: 8 shards: PUTs/s",
            "value": 308.5,
            "unit": "requests/s"
          },
          {
            "name": "s3 +20ms: 16 shards: open all",
            "value": 607.9,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: 16 shards: durable put p50",
            "value": 27.3,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: 16 shards: durable put p99",
            "value": 55,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: 16 shards: PUTs/s",
            "value": 567.2,
            "unit": "requests/s"
          },
          {
            "name": "s3 +20ms: 64 shards: open all",
            "value": 2448,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: 64 shards: durable put p50",
            "value": 72,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: 64 shards: durable put p99",
            "value": 158.8,
            "unit": "ms"
          },
          {
            "name": "s3 +20ms: 64 shards: PUTs/s",
            "value": 894.3,
            "unit": "requests/s"
          }
        ]
      }
    ],
    "slap_slatedb (higher is better)": [
      {
        "commit": {
          "author": {
            "email": "mjr@mjrusso.com",
            "name": "Michael Russo",
            "username": "mjrusso"
          },
          "committer": {
            "email": "mjr@mjrusso.com",
            "name": "Michael Russo",
            "username": "mjrusso"
          },
          "distinct": true,
          "id": "589be1fcc1f71e1f63b19699030aa1b14a05d88c",
          "message": "chore(repo): link the benchmark charts and describe the new triggers",
          "timestamp": "2026-10-07T08:06:35-04:00",
          "tree_id": "e3c80da79a13973baef8edd005870ebbb3ad2d50",
          "url": "https://github.com/mjrusso/slap/commit/589be1fcc1f71e1f63b19699030aa1b14a05d88c"
        },
        "date": 1791376931118,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "s3: durable puts x64",
            "value": 633,
            "unit": "ops/s"
          },
          {
            "name": "s3: writes x16",
            "value": 145132,
            "unit": "ops/s"
          },
          {
            "name": "s3: scan",
            "value": 662449,
            "unit": "rows/s"
          },
          {
            "name": "s3 +20ms: durable puts x64",
            "value": 632,
            "unit": "ops/s"
          },
          {
            "name": "s3 +20ms: writes x16",
            "value": 123561,
            "unit": "ops/s"
          },
          {
            "name": "s3 +20ms: scan",
            "value": 677048,
            "unit": "rows/s"
          },
          {
            "name": "s3: flush 5 ms: durable puts x64",
            "value": 10450,
            "unit": "ops/s"
          },
          {
            "name": "s3: flush 10 ms: durable puts x64",
            "value": 5785,
            "unit": "ops/s"
          },
          {
            "name": "s3: flush 20 ms: durable puts x64",
            "value": 3031,
            "unit": "ops/s"
          },
          {
            "name": "s3: flush 100 ms: durable puts x64",
            "value": 632,
            "unit": "ops/s"
          },
          {
            "name": "s3: 8 shards: durable puts",
            "value": 2869,
            "unit": "ops/s"
          },
          {
            "name": "s3: 16 shards: durable puts",
            "value": 3877,
            "unit": "ops/s"
          },
          {
            "name": "s3: 64 shards: durable puts",
            "value": 4043,
            "unit": "ops/s"
          },
          {
            "name": "s3 +20ms: flush 5 ms: durable puts x64",
            "value": 1983,
            "unit": "ops/s"
          },
          {
            "name": "s3 +20ms: flush 10 ms: durable puts x64",
            "value": 2099,
            "unit": "ops/s"
          },
          {
            "name": "s3 +20ms: flush 20 ms: durable puts x64",
            "value": 2074,
            "unit": "ops/s"
          },
          {
            "name": "s3 +20ms: flush 100 ms: durable puts x64",
            "value": 633,
            "unit": "ops/s"
          },
          {
            "name": "s3 +20ms: 8 shards: durable puts",
            "value": 1236,
            "unit": "ops/s"
          },
          {
            "name": "s3 +20ms: 16 shards: durable puts",
            "value": 2213,
            "unit": "ops/s"
          },
          {
            "name": "s3 +20ms: 64 shards: durable puts",
            "value": 3570,
            "unit": "ops/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mjr@mjrusso.com",
            "name": "Michael Russo",
            "username": "mjrusso"
          },
          "committer": {
            "email": "mjr@mjrusso.com",
            "name": "Michael Russo",
            "username": "mjrusso"
          },
          "distinct": true,
          "id": "03281318f4bbc491e02108bab4158a48174ab4be",
          "message": "ci: test running slap.server via Mix.install",
          "timestamp": "2026-10-07T21:55:31-04:00",
          "tree_id": "8fb92ca885eab21e75bf5348fe5b62feeb1246b9",
          "url": "https://github.com/mjrusso/slap/commit/03281318f4bbc491e02108bab4158a48174ab4be"
        },
        "date": 1791424969768,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "s3: durable puts x64",
            "value": 632,
            "unit": "ops/s"
          },
          {
            "name": "s3: writes x16",
            "value": 124323,
            "unit": "ops/s"
          },
          {
            "name": "s3: scan",
            "value": 670623,
            "unit": "rows/s"
          },
          {
            "name": "s3 +20ms: durable puts x64",
            "value": 633,
            "unit": "ops/s"
          },
          {
            "name": "s3 +20ms: writes x16",
            "value": 104088,
            "unit": "ops/s"
          },
          {
            "name": "s3 +20ms: scan",
            "value": 662581,
            "unit": "rows/s"
          },
          {
            "name": "s3: flush 5 ms: durable puts x64",
            "value": 10540,
            "unit": "ops/s"
          },
          {
            "name": "s3: flush 10 ms: durable puts x64",
            "value": 5777,
            "unit": "ops/s"
          },
          {
            "name": "s3: flush 20 ms: durable puts x64",
            "value": 3031,
            "unit": "ops/s"
          },
          {
            "name": "s3: flush 100 ms: durable puts x64",
            "value": 633,
            "unit": "ops/s"
          },
          {
            "name": "s3: 8 shards: durable puts",
            "value": 2867,
            "unit": "ops/s"
          },
          {
            "name": "s3: 16 shards: durable puts",
            "value": 3663,
            "unit": "ops/s"
          },
          {
            "name": "s3: 64 shards: durable puts",
            "value": 3769,
            "unit": "ops/s"
          },
          {
            "name": "s3 +20ms: flush 5 ms: durable puts x64",
            "value": 1848,
            "unit": "ops/s"
          },
          {
            "name": "s3 +20ms: flush 10 ms: durable puts x64",
            "value": 1979,
            "unit": "ops/s"
          },
          {
            "name": "s3 +20ms: flush 20 ms: durable puts x64",
            "value": 1924,
            "unit": "ops/s"
          },
          {
            "name": "s3 +20ms: flush 100 ms: durable puts x64",
            "value": 633,
            "unit": "ops/s"
          },
          {
            "name": "s3 +20ms: 8 shards: durable puts",
            "value": 1205,
            "unit": "ops/s"
          },
          {
            "name": "s3 +20ms: 16 shards: durable puts",
            "value": 2158,
            "unit": "ops/s"
          },
          {
            "name": "s3 +20ms: 64 shards: durable puts",
            "value": 3329,
            "unit": "ops/s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "mjr@mjrusso.com",
            "name": "Michael Russo",
            "username": "mjrusso"
          },
          "committer": {
            "email": "mjr@mjrusso.com",
            "name": "Michael Russo",
            "username": "mjrusso"
          },
          "distinct": true,
          "id": "ebb5692be526532a00c728d08bb1ab201c2ce047",
          "message": "chore(repo): prepare slap_slatedb 0.2.0",
          "timestamp": "2026-10-08T22:15:38-04:00",
          "tree_id": "fec69e28e0c17e00380f60d264426046182a8379",
          "url": "https://github.com/mjrusso/slap/commit/ebb5692be526532a00c728d08bb1ab201c2ce047"
        },
        "date": 1791512840358,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "s3: durable puts x64",
            "value": 633,
            "unit": "ops/s"
          },
          {
            "name": "s3: writes x16",
            "value": 133079,
            "unit": "ops/s"
          },
          {
            "name": "s3: scan",
            "value": 607884,
            "unit": "rows/s"
          },
          {
            "name": "s3 +20ms: durable puts x64",
            "value": 632,
            "unit": "ops/s"
          },
          {
            "name": "s3 +20ms: writes x16",
            "value": 109483,
            "unit": "ops/s"
          },
          {
            "name": "s3 +20ms: scan",
            "value": 722178,
            "unit": "rows/s"
          },
          {
            "name": "s3: flush 5 ms: durable puts x64",
            "value": 10524,
            "unit": "ops/s"
          },
          {
            "name": "s3: flush 10 ms: durable puts x64",
            "value": 5776,
            "unit": "ops/s"
          },
          {
            "name": "s3: flush 20 ms: durable puts x64",
            "value": 3031,
            "unit": "ops/s"
          },
          {
            "name": "s3: flush 100 ms: durable puts x64",
            "value": 632,
            "unit": "ops/s"
          },
          {
            "name": "s3: 8 shards: durable puts",
            "value": 2865,
            "unit": "ops/s"
          },
          {
            "name": "s3: 16 shards: durable puts",
            "value": 3786,
            "unit": "ops/s"
          },
          {
            "name": "s3: 64 shards: durable puts",
            "value": 3722,
            "unit": "ops/s"
          },
          {
            "name": "s3 +20ms: flush 5 ms: durable puts x64",
            "value": 1878,
            "unit": "ops/s"
          },
          {
            "name": "s3 +20ms: flush 10 ms: durable puts x64",
            "value": 2072,
            "unit": "ops/s"
          },
          {
            "name": "s3 +20ms: flush 20 ms: durable puts x64",
            "value": 1930,
            "unit": "ops/s"
          },
          {
            "name": "s3 +20ms: flush 100 ms: durable puts x64",
            "value": 633,
            "unit": "ops/s"
          },
          {
            "name": "s3 +20ms: 8 shards: durable puts",
            "value": 1210,
            "unit": "ops/s"
          },
          {
            "name": "s3 +20ms: 16 shards: durable puts",
            "value": 2192,
            "unit": "ops/s"
          },
          {
            "name": "s3 +20ms: 64 shards: durable puts",
            "value": 3333,
            "unit": "ops/s"
          }
        ]
      }
    ]
  }
}