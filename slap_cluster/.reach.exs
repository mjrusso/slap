[
  checks: [source_paths: ["lib"]],
  calls: [
    forbidden: [
      # One SlateDB writer per shard: the handle ShardDb opens on the
      # shard's owner.
      {["Slap.Cluster", "Slap.Cluster.*"], ["Slap.SlateDB.open", "Slap.SlateDB.close"],
       except: ["Slap.Cluster.ShardDb"]},
      # slap_cluster stays generic: its shard children write, it does not.
      {["Slap.Cluster", "Slap.Cluster.*"],
       [
         "Slap.SlateDB.write",
         "Slap.SlateDB.put",
         "Slap.SlateDB.delete",
         "Slap.SlateDB.merge",
         "Slap.SlateDB.increment",
         "Slap.SlateDB.begin"
       ]}
    ]
  ]
]
