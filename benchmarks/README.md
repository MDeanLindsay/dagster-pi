# Benchmarks: Dagster on a Pi, measured

A dependency-free `/proc` harness plus the measured effect of the deployment's low-resource tuning. Run it before and after a change; it appends a JSON record under `benchmarks/results/` (gitignored) and prints a markdown summary.

- [bench.py](bench.py): point-in-time snapshot, tiered into services / run-workers / step-workers.

The workload it measures against is the [`synthetic_load`](../src/dagster_pi/defs/benchmarking/synthetic_load.py) asset. (The one-off probe scripts that originally gathered the backfill-peak and copy-on-write numbers below answered their questions — executor choice, eager-loading — and have been removed; the method for reproducing each number with `bench.py` alone is described inline.) The tuning levers they measure are summarized in the [main README](../README.md#tuning-for-low-resources); this folder is where those numbers come from.

## The box under test

Raspberry Pi 5, 8 GB RAM, 4 cores, root + data + `DAGSTER_HOME` on NVMe (no SD card in the I/O path), Python 3.12, Dagster 1.13.x, the three-process systemd deployment from the top-level README.

## What `bench.py` measures

| Metric | Captures | Why it matters on a Pi |
|---|---|---|
| `processes` | every live deployment process (count + PSS + RSS), tiered by parent PID into service / run-worker / step-worker | the multiprocess executor's ~100 MB-PSS step-worker forks show up here *during a run* |
| `cold_import_s` | median `python -c "import dagster_pi.definitions"` in a fresh interpreter | the startup tax every forked process pays before doing any work |
| `state` | run+event history size, compute-log dir count, DuckDB file size | unbounded instance state is the slow killer of an always-on box |

```bash
uv run python benchmarks/bench.py --label before
# apply changes, restart services, let it settle
uv run python benchmarks/bench.py --label after
# run it DURING a backfill to catch the step-worker delta
```

It reads only `/proc` and file sizes, never the DuckDB file or instance stores, so it is safe against the live service.

## Numbers

Measured on the box above with `dagster 1.13.x`. Figures are **PSS** (proportional set size), which, unlike RSS, does not double-count the shared library pages (libpython, libduckdb) every process maps, so per-process numbers sum honestly across the tree. RSS is shown as an upper bound. Re-run on your own box; absolute numbers will differ, the shape should not.

### Idle baseline

Three resident services + the gRPC code location + the supervisor, at rest:

| process | PSS | RSS |
|---|--:|--:|
| webserver | 146 MB | 168 MB |
| code-server-grpc | 140 MB | 162 MB |
| daemon | 121 MB | 144 MB |
| code-server (supervisor) | 78 MB | 100 MB |
| **total (idle)** | **~485 MB** | **~575 MB** |

Cold import of `dagster_pi.definitions`: **0.95 s** (median of 3).

### The headline: in_process vs multiprocess, during a backfill

Same workload both times: an 8-partition `synthetic_load` backfill at `max_concurrent_runs: 4`, each run a 10 s CPU spin + a 200k-row DuckDB write. The peak is the single highest-total-PSS sample while 4 runs execute. To reproduce, launch the backfill (UI → Assets → `synthetic_load` → Materialize → all partitions) and sample it from another shell:

```bash
while true; do uv run python benchmarks/bench.py --label peak; sleep 2; done
# keep the highest-total-PSS record from results.jsonl; then switch
# definitions.py to dg.multiprocess_executor, reload, and repeat
```

> **Reproducing this needs the `duckdb` pool raised first.** The deployment now ships `synthetic_load` in the `duckdb` concurrency pool at limit 1 (the single-writer guard from the [main README](../README.md#duckdb-pool)), which serializes a backfill run-by-run. These numbers predate the pool and assume 4 concurrent writers: `uv run dagster instance concurrency set duckdb 4` before the backfill, `... set duckdb 1` after.

| peak during 4-concurrent backfill | multiprocess | in_process | delta |
|---|--:|--:|--:|
| **total PSS** | 1294 MB | 916 MB | **-378 MB** |
| total RSS | 1793 MB | 1245 MB | -548 MB |
| live processes | 20 | 18 | -2 |
| run-workers | 4 × ~98 MB PSS | 4 × ~101 MB PSS | n/a |
| **step-workers** | 4 × ~100 MB PSS | 0 | **eliminated** |

multiprocess forks one step-worker per step *on top of* the run-worker, so each concurrent run carries ~2x the footprint; `in_process_executor` runs the step inside the run-worker and the step-worker row goes to zero. **The saving scales with `max_concurrent_runs × steps-per-run`**: ~378 MB at 4 single-step runs, more with wider fan-out or multi-step jobs.

What does not move:

- **Idle footprint** is unchanged: at rest there are no workers.
- **Startup tax:** the ~0.95 s cold import is now paid once per *run*, not per *step*.
- **DuckDB cap** is a guardrail, not an idle number. Crank `--rows` to exercise spill, or `mem_mb` to prove a systemd `MemoryMax` bounds a single run.

> **Why PSS, and why parent-PID tiering?** With the gRPC code-server, both run-workers and step-workers are `multiprocessing.spawn` children launched as `python -c "...spawn_main(...)"`, indistinguishable by command line, so `bench.py` tiers them by parent PID (a run-worker's parent is the grpc server; a step-worker's parent is a run-worker). They re-import privately, but the resulting shared-library pages are mapped by every process, so RSS double-counts them and PSS divides them.

Data lands in DuckDB's own `benchmark` schema; `DROP SCHEMA benchmark CASCADE` cleans up.

## Why eager-loading the code server won't help (run-workers spawn, not fork)

A tempting optimization: the gRPC code server runs with `--lazy-load-user-code`. If it instead *eager-loaded* at startup, wouldn't concurrent run-workers (its children) inherit those import pages copy-on-write and cost almost nothing each? We measured first, by dumping each run-worker's `/proc/<pid>/smaps_rollup` at peak concurrency during a backfill (run-workers are the children of the gRPC code-server process, so they are easy to find by parent PID).

**The premise is false: run-workers are `multiprocessing.spawn` children, not forks.** Their command line is `python -c "from multiprocessing.spawn import spawn_main; ..."`, a fresh interpreter that re-imports `dagster` + user code into its *own private heap*. At a 3-concurrent backfill:

| per run-worker (peak of 3) | value |
|---|--:|
| RSS | 138 MB |
| **PSS** | 102 MB |
| **private / anonymous** (own re-imported heap) | 94 MB |
| shared (file-backed libraries) | 44 MB |
| **marginal PSS per concurrent run** | 107 MB (idle 450 to peak 772 MB) |

If workers were forks sharing the parent's imports COW, the split would invert (~90 MB shared / ~10 MB private). It is the opposite (94 MB private) because spawn starts a fresh interpreter, so the parent's resident state cannot transfer. Eager-loading the parent would only enlarge its idle footprint for *zero* per-run benefit. (Forcing `fork`/`forkserver` would enable COW but is unsafe: forking the multi-threaded gRPC server risks deadlock, which is why Dagster spawns.) The ~44 MB workers *do* share is file-backed libraries (libpython, libduckdb), mapped by any process regardless of start method.

So per-run cost is ~107 MB and **unshareable**, and the only levers that cut concurrent-backfill RAM are:

1. **Fewer concurrent workers:** `max_concurrent_runs` 4 to 3, so peak is 3 × ~107 MB, not 4×.
2. **Keep frequent small work off run-workers.** A recurring health check or metrics scrape should not get its own run; a schedule would spawn a fresh ~107 MB worker (and pay the ~0.95 s re-import) every tick. Do it *inside the resident daemon* via a sensor that acts inline and returns a `SkipReason`, for near-zero marginal RAM.

## Keeping state bounded (the other half of "retention")

The `retention:` block in `dagster.yaml` covers **schedule/sensor ticks** only. Run + event history and the per-run compute-log dirs are **not** auto-pruned in Dagster OSS, and `delete_run` clears the run + event log but leaves the on-disk `storage/<run_id>/compute_logs` tree, the fastest-growing leftover.

This is wired in as an **opt-in** job: [`maintenance_job`](../src/dagster_pi/defs/logs/maintenance.py) + the `daily_maintenance` schedule (cron `0 4 * * *`). It deletes finished runs older than `retention_days` *and* their compute-log dirs, and ships safe-by-default: the schedule is created **STOPPED**, and the op defaults to **`dry_run: true`**. Preview what it would prune:

```bash
uv run dagster job execute -j maintenance_job -m dagster_pi.definitions \
  -c <(echo 'ops: {prune_old_runs: {config: {retention_days: 90, dry_run: true}}}')
```

To run it for real on a schedule, enable `daily_maintenance` and set `dry_run: false` (Schedules -> daily_maintenance -> edit config).

## Reproducing from scratch

```bash
uv run python benchmarks/bench.py --label my-snapshot   # any label
cat benchmarks/results/results.jsonl                    # the rolling local log
```
