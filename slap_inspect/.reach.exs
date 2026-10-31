[
  checks: [source_paths: ["lib"]],
  calls: [
    forbidden: [
      # Distributed calls must go through Remote's allowlist.
      {["Slap.Inspect.*", "Mix.Tasks.Slap.Inspect"],
       ["erpc.*", "rpc.*", "net_kernel.*", "Node.*"], except: ["Slap.Inspect.Remote"]}
    ]
  ]
]
