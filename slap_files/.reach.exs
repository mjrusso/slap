[
  checks: [source_paths: ["lib"]],
  calls: [
    forbidden: [
      # Inspection must not write records, open intents, or upload or delete
      # objects.
      {"Slap.Files.Introspect",
       [
         "Slap.Files.put",
         "Slap.Files.delete",
         "Slap.Files.Record.put",
         "Slap.Files.Record.delete",
         "Slap.Files.Body.*",
         "Slap.KV.put",
         "Slap.KV.delete",
         "Slap.KV.batch",
         "Slap.Files.Intent.*",
         "Slap.Files.Sweeper.*",
         "Slap.Files.Object.*",
         "Slap.SlateDB.ObjectStore.put",
         "Slap.SlateDB.ObjectStore.upload",
         "Slap.SlateDB.ObjectStore.delete"
       ]},
      # Records and registrations are Slap.KV rows; files never use a
      # SlateDB database directly.
      {["Slap.Files", "Slap.Files.*"],
       [
         "Slap.SlateDB.open",
         "Slap.SlateDB.close",
         "Slap.SlateDB.write",
         "Slap.SlateDB.put",
         "Slap.SlateDB.delete",
         "Slap.SlateDB.merge",
         "Slap.SlateDB.increment",
         "Slap.SlateDB.begin"
       ]},
      # An object is deleted only by a sweep that took its intent, or by
      # reconciliation (Slap.Files.Object.reconcile/2, run by the sweeper).
      {["Slap.Files", "Slap.Files.*"], ["Slap.SlateDB.ObjectStore.delete"],
       except: ["Slap.Files.Sweeper", "Slap.Files.Object"]},
      {["Slap.Files", "Slap.Files.*"],
       ["Slap.Files.Object.reconcile", "Slap.Files.Object.unregister"],
       except: ["Slap.Files.Sweeper"]},
      # Uploads go through Slap.Files.Body, after the key is registered.
      {["Slap.Files", "Slap.Files.*"], ["Slap.SlateDB.ObjectStore.upload"],
       except: ["Slap.Files.Body"]}
    ]
  ]
]
