window.BENCHMARK_DATA = {
  "lastUpdate": 1791294066665,
  "repoUrl": "https://github.com/mjrusso/slap",
  "entries": {
    "Durable Streams (lower is better)": [
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
          "id": "1523f5805ac5e4b5e9d75d8e4b94fcf0a9ccda8a",
          "message": "docs(repo): expand README",
          "timestamp": "2026-10-06T09:23:31-04:00",
          "tree_id": "496533849009e7a332ce2694537b34e12affaf36",
          "url": "https://github.com/mjrusso/slap/commit/1523f5805ac5e4b5e9d75d8e4b94fcf0a9ccda8a"
        },
        "date": 1791294065847,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "caddy-file: Latency - Total RTT p50",
            "value": 1.4546450000016193,
            "unit": "ms"
          },
          {
            "name": "slap-local: Latency - Total RTT p50",
            "value": 11.330011000000013,
            "unit": "ms"
          },
          {
            "name": "slap-s3: Latency - Total RTT p50",
            "value": 11.280806999999186,
            "unit": "ms"
          },
          {
            "name": "caddy-file: Latency - Total RTT p99",
            "value": 4.833645999999135,
            "unit": "ms"
          },
          {
            "name": "slap-local: Latency - Total RTT p99",
            "value": 12.23192100000233,
            "unit": "ms"
          },
          {
            "name": "slap-s3: Latency - Total RTT p99",
            "value": 14.740398999999343,
            "unit": "ms"
          }
        ]
      }
    ]
  }
}