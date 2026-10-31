[
  checks: [source_paths: ["lib"]],
  calls: [
    forbidden: [
      # Inspection must not start application processes or write stored updates.
      {["Slap.Yjs.Introspect", "Slap.Yjs.Reconstruction"],
       [
         "Slap.Yjs.DocServer.*",
         "Slap.Yjs.Store.*",
         "Slap.Streams.append",
         "Slap.SnapshotLog.append",
         "Slap.SnapshotLog.publish"
       ]},
      # Documents are snapshot logs in Durable Streams; Yjs code never uses
      # SlateDB directly.
      {["Slap.Yjs", "Slap.Yjs.*"], ["Slap.SlateDB.*"]}
    ]
  ]
]
