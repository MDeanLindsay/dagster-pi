# Benchmarks — Dagster on a Pi, measured

A config isn't a project until the claims have numbers behind them. This folder
holds a dependency-free harness ([bench.py](bench.py)) and the measured effect of
the deployment's low-resource tuning. Run it before and after a change; it appends
a JSON record to `results.jsonl` (gitignored) and prints a markdown summary.

## The box under test

Raspberry Pi 5 · 8 GB RAM · 4 cores · root + data + `DAGSTER_HOME` on **NVMe**
(no SD card in the I/O path) · Python 3.12 · Dagster 1.13.x · the three-process
systemd deployment (code-server + webserver + daemon) from the top-level README.

## What `bench.py` measures

| Metric | What it captures | Why it matters on a Pi |
|---|---|---|
| `processes` | Every live deployment process (count + RSS), classified by role | The multiprocess executor's **~150 MB step-worker** forks show up here *during a run* |
| `cold_import_s` | Median `python -c "import dagster_pi.definitions"` in a fresh interpreter | The startup tax every forked process pays before doing any work |
| `state` | run+event history size, compute-log dir count, DuckDB file size | Unbounded instance state is the slow killer of an always-on deployment |

```bash
uv run python benchmarks/bench.py --label before
# ...apply changes, restart services, let it settle...
uv run python benchmarks/bench.py --label after
# run it DURING a backfill to catch the step-worker delta (see below)
```

It only reads `/proc` and file sizes — never the DuckDB file or instance stores —
so it's safe to run against the live service.

## Tier-1 tuning applied

| Change | Where | Effect |
|---|---|---|
| **`in_process_executor`** | [src/dagster_pi/definitions.py](../src/dagster_pi/definitions.py) | Steps run inside the run process instead of forking a ~150 MB `import dagster` step-worker per step. Removes the biggest transient spike + per-step startup tax. |
| **Telemetry off** | [.dagster_home/dagster.yaml](../.dagster_home/dagster.yaml) | No usage stats shipped off the box; one fewer background network path. |
| **Tick retention** | `.dagster_home/dagster.yaml` | Schedule/sensor tick records auto-purge instead of growing forever. |
| **DuckDB caps** | [src/dagster_pi/defs/resources.py](../src/dagster_pi/defs/resources.py) | `memory_limit=2GB`, `threads=2`, NVMe `temp_directory`. One query can no longer grab ~80% of RAM / all 4 cores and starve the webserver + daemon. |

## Numbers

### Baseline (re-measure on your box)

No committed numbers yet: the figures here came from the pre-split instance and
have been cleared. Capture a fresh idle baseline on your own deployment, then run
the before/after executor experiment below to fill in the headline comparison:

```bash
uv run python benchmarks/bench.py --label idle
```

### What moves, and what doesn't

Be honest about the levers: **idle RSS barely changes** — at rest there are no
step-workers, and none of these four changes touch the three resident processes.
The wins are elsewhere:

- **During a run**, the multiprocess executor forks a ~150 MB step-worker per step;
  a concurrent backfill (`max_concurrent_runs: 4`) stacks several of these on top of
  the idle base. `in_process_executor` eliminates those forks — steps run inside each
  run-worker. **To measure it:** backfill the bundled synthetic workload (below) and
  run `bench.py --label during-backfill` before and after the switch; compare the
  `step-worker` line.
- **Startup latency:** the cold import is paid once per *run* now, not once per
  *step*.
- **Blast radius:** the DuckDB cap is a guardrail — its payoff is a heavy query (or
  a buggy one) no longer being able to OOM the box; not a number you see at idle.

### After (reproduce on your Pi)

The edits are on disk but the **running services still hold the old config** — they
pick it up on restart:

```bash
uv run dg check defs                    # confirm defs still load
sudo systemctl restart dagster-code-server dagster-webserver dagster-daemon
uv run python benchmarks/bench.py --label after-idle
```

To capture the *during-backfill* numbers — where the executor actually wins — drive
the bundled **`synthetic_load`** asset ([src/dagster_pi/defs/synthetic_load.py](../src/dagster_pi/defs/synthetic_load.py));
no private data source needed. It's a partitioned, parameterized workload (`rows`,
`mem_mb`, `cpu_spin_seconds`) that fans a backfill out into many runs:

```bash
# fan out every partition (UI: Assets -> synthetic_load -> Materialize -> all),
# or one at a time from the CLI:
uv run dagster asset materialize --select synthetic_load --partition part-000 \
    -m dagster_pi.definitions

# while the backfill runs, snapshot the footprint:
uv run python benchmarks/bench.py --label during-backfill
```

Take that snapshot once on the multiprocess executor and once on `in_process`, then
compare the `step-worker` line — that delta is the headline win. Crank `rows` to
stress the DuckDB caps, or `mem_mb` to prove a systemd `MemoryMax` (Tier 2) bounds a
single run. Data lands in its own `benchmark` schema; `DROP SCHEMA benchmark CASCADE`
cleans up.

## Keeping state bounded (the other half of "retention")

The `retention:` block in `dagster.yaml` only covers **schedule/sensor ticks**.
Run + event history and the per-run compute-log dirs are
**not** auto-pruned in Dagster OSS. Delete old runs — which also clears their event
logs and compute logs — with a periodic sweep over the instance API:

```python
# scratch: delete runs that finished more than 90 days ago
import datetime as dt
from dagster import DagsterInstance, RunsFilter
from dagster._core.storage.dagster_run import FINISHED_STATUSES

cutoff = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=90)).timestamp()
inst = DagsterInstance.get()
for run in inst.get_runs(filters=RunsFilter(statuses=list(FINISHED_STATUSES))):
    rec = inst.get_run_record_by_id(run.run_id)
    if rec and rec.end_time and rec.end_time < cutoff:
        inst.delete_run(run.run_id)   # removes run + event log + compute logs
```

This is destructive (it drops history), so it's documented rather than wired in.
The natural home for it is a small daily maintenance schedule under `defs/` — ask
and it's a 15-line asset.

## Reproducing from scratch

```bash
uv run python benchmarks/bench.py --label my-snapshot   # any label
cat benchmarks/results.jsonl                            # the rolling local log
```
