[
  checks: [source_paths: ["lib"]],
  calls: [
    forbidden: [
      # A snapshot log is stored in Durable Streams, through Slap.Streams.
      {["Slap.SnapshotLog", "Slap.SnapshotLog.*"], ["Slap.SlateDB.*"]}
    ]
  ]
]
