[
  checks: [source_paths: ["lib"]],
  calls: [
    forbidden: [
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
