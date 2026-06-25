"""Instance log + history maintenance.

Holds the opt-in retention job that prunes old run/event history and the per-run
compute-log dirs Dagster OSS won't purge on its own. Auto-discovered by
`load_from_defs_folder` like everything else under `defs/`.
"""
