[
  checks: [source_paths: ["lib", "dev"]],
  calls: [
    forbidden: [
      # Composition and HTTP layers access data only through service APIs.
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
