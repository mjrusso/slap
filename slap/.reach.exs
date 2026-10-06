[
  checks: [source_paths: ["lib"]],
  calls: [
    forbidden: [
      # The server composes Slap.Streams and Slap.KV; it reads and writes
      # data only through them.
      {["Slap", "Slap.*", "Mix.Tasks.Slap.*"],
       [
         "Slap.SlateDB.open",
         "Slap.SlateDB.close",
         "Slap.SlateDB.write",
         "Slap.SlateDB.put",
         "Slap.SlateDB.delete",
         "Slap.SlateDB.merge",
         "Slap.SlateDB.increment",
         "Slap.SlateDB.begin",
         "Slap.SlateDB.get",
         "Slap.SlateDB.scan"
       ]}
    ]
  ]
]
