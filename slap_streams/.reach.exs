[
  checks: [source_paths: ["lib"]],
  calls: [
    forbidden: [
      # Databases belong to slap_cluster's placement (Slap.Cluster.ShardDb).
      {["Slap.Streams", "Slap.Streams.*"], ["Slap.SlateDB.open", "Slap.SlateDB.close"]},
      # On a shard's owner, a stream's stream server writes the stream; the
      # shard state reserves stream ids, the deleter removes deleted and
      # trimmed rows, and Group seals groups and creates their streams.
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
         "Slap.Streams.Group"
       ]},
      # A stream is created in a group by its stream server.
      {["Slap.Streams", "Slap.Streams.*"], ["Slap.Streams.Group.create_write"],
       except: ["Slap.Streams.StreamServer"]},
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
