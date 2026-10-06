[
  checks: [source_paths: ["lib"]],
  calls: [
    forbidden: [
      # Documents are snapshot logs in Durable Streams; Yjs code never uses
      # SlateDB directly.
      {["Slap.Yjs", "Slap.Yjs.*"], ["Slap.SlateDB.*"]}
    ]
  ]
]
