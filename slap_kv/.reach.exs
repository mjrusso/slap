[
  checks: [source_paths: ["lib"]],
  calls: [
    forbidden: [
      # Handle lifetime belongs to slap_cluster, including drainage after crashes.
      {["Slap.KV", "Slap.KV.*"], ["Slap.SlateDB.open", "Slap.SlateDB.close"]},
      # Each partition is written by its shard's partition writer only.
      # Shard admission accounts for work and never writes application data.
      {["Slap.KV", "Slap.KV.*"],
       [
         "Slap.SlateDB.write",
         "Slap.SlateDB.put",
         "Slap.SlateDB.delete",
         "Slap.SlateDB.merge",
         "Slap.SlateDB.increment",
         "Slap.SlateDB.begin"
       ], except: ["Slap.KV.PartitionWriter"]},
      # The HTTP layer goes through Slap.KV, never to storage.
      {"Slap.KV.HTTP.*",
       [
         "Slap.SlateDB.*",
         "Slap.KV.PartitionWriter.*",
         "Slap.KV.Read.*",
         "Slap.KV.Keys.*",
         "Slap.KV.Admission.*",
         "Slap.KV.Request.*"
       ]}
    ]
  ]
]
