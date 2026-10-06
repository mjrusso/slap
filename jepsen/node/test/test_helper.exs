ExUnit.start()
{:ok, _} = Application.ensure_all_started(:slap_files)
Slap.SlateDB.set_log_level(:none)
JepsenNode.Stats.setup()
