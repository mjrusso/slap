window.BENCHMARK_DATA = {
  "lastUpdate": 1791376931720,
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
      }
    ]
  }
}