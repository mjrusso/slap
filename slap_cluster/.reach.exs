[
  checks: [source_paths: ["lib"]],
  calls: [
    forbidden: [
      # Database owns each shard handle until native calls drain and it closes,
      # including after the cluster tree stops.
      {["Slap.Cluster", "Slap.Cluster.*"], ["Slap.SlateDB.open", "Slap.SlateDB.close"],
       except: ["Slap.Cluster.Database"]},
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
