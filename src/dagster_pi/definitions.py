"""Project entry point — auto-discovery, plus one deployment-level tuning knob.

``load_from_defs_folder`` recursively discovers every asset / schedule / check /
resource under ``defs/`` (the rule the whole project is built on: you never
register definitions by hand). The one thing it can't express is a setting that
belongs to the *deployment* rather than to any single module, so we merge that
in here:

    in_process_executor — run a job's steps inside the run process instead of
    forking a fresh ~150 MB step subprocess per step (the multiprocess default).
    On a 4-core Pi that removes the largest transient memory spike and a cold
    ``import dagster`` per step. Runs still parallelize at the run level, bounded
    by ``run_queue.max_concurrent_runs`` in ``.dagster_home/dagster.yaml``.

This is the one intentional edit to an otherwise-generated file; everything else
still comes from ``defs/``.
"""

from pathlib import Path

import dagster as dg


@dg.definitions
def defs() -> dg.Definitions:
    return dg.Definitions.merge(
        dg.load_from_defs_folder(path_within_project=Path(__file__).parent),
        dg.Definitions(executor=dg.in_process_executor),
    )
