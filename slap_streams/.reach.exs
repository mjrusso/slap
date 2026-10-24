[
  checks: [source_paths: ["lib"]],
  calls: [
    forbidden: [
      # Databases belong to slap_cluster's placement (Slap.Cluster.ShardDb).
      {["Slap.Streams", "Slap.Streams.*"], ["Slap.SlateDB.open", "Slap.SlateDB.close"]},
      # Stream servers write stream data and authorize retention and deletion.
      # ShardState reserves ids; Deleter applies durable deletion bounds.
      # Group owns seals and transactional creation. Offline Upgrade requires
      # exclusive handles with every cluster node stopped.
      {["Slap.Streams", "Slap.Streams.*"],
       [
         "Slap.SlateDB.write",
         "Slap.SlateDB.put",
         "Slap.SlateDB.delete",
         "Slap.SlateDB.merge",
         "Slap.SlateDB.increment",
         "Slap.SlateDB.begin"
       ],
       except: [
         "Slap.Streams.StreamServer",
         "Slap.Streams.ShardState",
         "Slap.Streams.Jobs.Deleter",
         "Slap.Streams.Group",
         "Slap.Streams.Upgrade"
       ]},
      # A stream is created in a group by its stream server.
      {["Slap.Streams", "Slap.Streams.*"], ["Slap.Streams.Group.create_write"],
       except: ["Slap.Streams.StreamServer"]},
      # Source retention decisions cannot depend on target creation or the protocol.
      {"Slap.Streams.Retention", ["Slap.Streams.Creation.*", "Slap.Streams.Protocol.*"]},
      {"Slap.Streams.Protocol", ["Slap.Streams.Creation.*"]},
      # The HTTP layer goes through Slap.Streams, never to storage.
      {"Slap.Streams.HTTP.*",
       [
         "Slap.SlateDB.*",
         "Slap.Streams.Store.*",
         "Slap.Streams.StreamServer.*",
         "Slap.Streams.Group.*"
       ]}
    ]
  ]
]
