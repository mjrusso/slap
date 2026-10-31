[
  checks: [source_paths: ["lib"]],
  calls: [
    forbidden: [
      # Inspection must not resume initialization, forks or publication.
      {["Slap.SnapshotLog.Introspect", "Slap.SnapshotLog.Introspect.*"],
       [
         "Slap.Streams.append",
         "Slap.Streams.read",
         "Slap.Streams.*_fenced",
         "Slap.SnapshotLog.Store.state",
         "Slap.SnapshotLog.Store.current",
         "Slap.SnapshotLog.Control.read",
         "Slap.SnapshotLog.Control.put",
         "Slap.SnapshotLog.Compaction.*",
         "Slap.SnapshotLog.Fork.*",
         "Slap.SnapshotLog.Initialization.*"
       ]},
      # A snapshot log is stored in Durable Streams, through Slap.Streams.
      {["Slap.SnapshotLog", "Slap.SnapshotLog.*"], ["Slap.SlateDB.*"]}
    ]
  ]
]
