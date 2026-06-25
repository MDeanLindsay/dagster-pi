"""Opt-in maintenance: prune run + event + compute-log history.

Dagster OSS auto-purges only schedule/sensor *ticks* (the `retention:` block in
`.dagster_home/dagster.yaml`). Run + event history and the per-run compute-log
dirs grow forever otherwise — the slow killer of an always-on deployment. This job
deletes finished runs older than `retention_days`; `delete_run` also removes each
run's event log and compute logs.

`delete_run` removes the run and its event log; the per-run compute-log directory
under `$DAGSTER_HOME/storage/<run_id>` is *not* covered by `delete_run`, so we also
call `compute_log_manager.delete_logs(prefix=[run_id])` — that on-disk tree is the
fastest-growing leftover.

It is destructive, so it ships **opt-in twice over**:

  1. the schedule is created STOPPED — enable it in Deployment -> Schedules; and
  2. the op defaults to `dry_run=True` — it logs what it *would* delete. Set
     `dry_run: false` in the schedule's run config (or a manual run) to actually
     delete.
"""

import datetime as dt

import dagster as dg

# Finished == terminal; in-flight runs are never touched.
_FINISHED = [
    dg.DagsterRunStatus.SUCCESS,
    dg.DagsterRunStatus.FAILURE,
    dg.DagsterRunStatus.CANCELED,
]


class PruneConfig(dg.Config):
    retention_days: int = 90
    dry_run: bool = True


@dg.op
def prune_old_runs(context: dg.OpExecutionContext, config: PruneConfig) -> None:
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=config.retention_days)
    instance = context.instance
    old = instance.get_runs(
        filters=dg.RunsFilter(statuses=_FINISHED, created_before=cutoff)
    )
    for run in old:
        if config.dry_run:
            context.log.info(
                f"[dry-run] would delete run {run.run_id} ({run.job_name})"
            )
        else:
            instance.compute_log_manager.delete_logs(
                prefix=[run.run_id]
            )  # on-disk compute logs
            instance.delete_run(run.run_id)  # run record + event log
    verb = "would delete" if config.dry_run else "deleted"
    context.log.info(
        f"prune: {verb} {len(old)} finished run(s) created before "
        f"{cutoff.date()} (retention_days={config.retention_days})"
    )


@dg.job
def maintenance_job():
    prune_old_runs()


maintenance_schedule = dg.ScheduleDefinition(
    name="daily_maintenance",
    job=maintenance_job,
    cron_schedule="0 4 * * *",  # 04:00 local, daily
    default_status=dg.DefaultScheduleStatus.STOPPED,  # opt-in: enable it in the UI
)
