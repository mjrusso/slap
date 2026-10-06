[
  checks: [source_paths: ["lib"]],
  calls: [
    forbidden: [
      # Databases belong to slap_cluster's placement (Slap.Cluster.ShardDb).
      {["Slap.KV", "Slap.KV.*"], ["Slap.SlateDB.open", "Slap.SlateDB.close"]},
      # Each partition is written by its shard's partition writer only.
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
       ["Slap.SlateDB.*", "Slap.KV.PartitionWriter.*", "Slap.KV.Read.*", "Slap.KV.Keys.*"]}
    ]
  ]
]
