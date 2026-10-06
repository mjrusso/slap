window.BENCHMARK_DATA = {
  "lastUpdate": 1791294068599,
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
    ],
    "Durable Streams (higher is better)": [
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
        "date": 1791294068076,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "caddy-file: Throughput - Small Messages p50",
            "value": 44104.08829462217,
            "unit": "msg/sec"
          },
          {
            "name": "slap-local: Throughput - Small Messages p50",
            "value": 3226.4775901756184,
            "unit": "msg/sec"
          },
          {
            "name": "slap-s3: Throughput - Small Messages p50",
            "value": 3228.2793969413897,
            "unit": "msg/sec"
          },
          {
            "name": "caddy-file: Throughput - Small Messages min",
            "value": 25910.055057572867,
            "unit": "msg/sec"
          },
          {
            "name": "slap-local: Throughput - Small Messages min",
            "value": 3189.652949555657,
            "unit": "msg/sec"
          },
          {
            "name": "slap-s3: Throughput - Small Messages min",
            "value": 3176.9992068907422,
            "unit": "msg/sec"
          },
          {
            "name": "caddy-file: Throughput - Large Messages p50",
            "value": 296.1123100928919,
            "unit": "msg/sec"
          },
          {
            "name": "slap-local: Throughput - Large Messages p50",
            "value": 159.2194745466288,
            "unit": "msg/sec"
          },
          {
            "name": "slap-s3: Throughput - Large Messages p50",
            "value": 71.27558673107916,
            "unit": "msg/sec"
          },
          {
            "name": "caddy-file: Throughput - Large Messages min",
            "value": 261.3306406381623,
            "unit": "msg/sec"
          },
          {
            "name": "slap-local: Throughput - Large Messages min",
            "value": 134.5041489082527,
            "unit": "msg/sec"
          },
          {
            "name": "slap-s3: Throughput - Large Messages min",
            "value": 64.24738198471599,
            "unit": "msg/sec"
          }
        ]
      }
    ]
  }
}