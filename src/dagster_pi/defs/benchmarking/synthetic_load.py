"""Synthetic, parameterized workload for benchmarking the deployment itself.

This asset exists to make the repo's tuning claims *reproducible* without any
private data source: anyone can backfill it on their own Pi and measure the
effect of a change (multiprocess vs `in_process` executor, a systemd `MemoryMax`,
the DuckDB caps) with `benchmarks/bench.py`.

It is intentionally boring and controllable. Three independent knobs (Dagster run
config, all off-by-default except the DuckDB write) target each resource axis on
its own:

    rows               how many rows to generate + write to DuckDB (I/O + the
                       capped `duckdb` resource).
    mem_mb             transiently hold ~this many MB of RAM (memory pressure;
                       use it to prove a systemd `MemoryMax` bounds a single run
                       instead of taking down the box).
    cpu_spin_seconds   busy-loop one core for N seconds (CPU pressure; use it to
                       watch governor/thermal behavior, or contention with the UI).

Partitions fan a backfill out into many runs — exactly what surfaces the
multiprocess executor's per-step worker forks. Backfill the whole partition set
and snapshot with `bench.py` mid-run to see (or, after switching to
`in_process_executor`, *not* see) the ~150 MB step-workers:

    # one partition:
    uv run dagster asset materialize --select synthetic_load \\
        --partition part-000 -m dagster_pi.definitions
    # whole set: UI -> Assets -> synthetic_load -> Materialize -> all partitions
    # (a backfill of BENCH_PARTITIONS runs); or launch synthetic_load_job.

Nothing here auto-runs: there is no schedule, so it stays idle on a real
deployment until you trigger it. Data lands in its own `benchmark` schema, so
`DROP SCHEMA benchmark CASCADE` removes every trace.

Note: the asset sits in the `duckdb` concurrency pool (limit 1), so a backfill
normally serializes run-by-run. To reproduce the N-concurrent numbers in
benchmarks/README.md, raise the pool first:

    uv run dagster instance concurrency set duckdb 4   # and back to 1 after
"""

import os
import time

import dagster as dg
from dagster_duckdb import DuckDBResource

from dagster_pi.defs.resources import DUCKDB_POOL, spill_watch

# Partition count == max fan-out of a full backfill. Override with BENCH_PARTITIONS.
_N_PARTITIONS = int(os.getenv("BENCH_PARTITIONS", "24"))
synthetic_partitions = dg.StaticPartitionsDefinition(
    [f"part-{i:03d}" for i in range(_N_PARTITIONS)]
)


class SyntheticConfig(dg.Config):
    """Per-run knobs. Defaults do a pure DuckDB write with no extra pressure."""

    rows: int = 200_000
    mem_mb: int = 0
    cpu_spin_seconds: float = 0.0


@dg.asset(
    partitions_def=synthetic_partitions,
    group_name="benchmark",
    kinds={"duckdb", "python"},
    tags={"category": "benchmark"},
    pool=DUCKDB_POOL,
)
def synthetic_load(
    context: dg.AssetExecutionContext,
    config: SyntheticConfig,
    duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    """Generate `rows` synthetic rows for this partition and write them to DuckDB.

    Idempotent: delete-and-replace this partition's rows, so a re-run or
    re-backfill never double-counts. Flat, atomic columns only.
    """
    partition = context.partition_key
    start = time.perf_counter()

    # Optional memory pressure: hold a ballast buffer through the DuckDB work.
    # bytearray(n) is zero-filled, so its pages are resident immediately.
    ballast = bytearray(config.mem_mb * 1024 * 1024) if config.mem_mb > 0 else None

    # Optional CPU pressure: busy-loop one core for the requested duration.
    if config.cpu_spin_seconds > 0:
        deadline = time.perf_counter() + config.cpu_spin_seconds
        spins = 0
        while time.perf_counter() < deadline:
            spins += 1

    # Watch the DuckDB spill dir for the high-water mark: a large `rows` value can
    # push the write past the capped memory_limit and spill to disk, which slows the
    # run instead of failing it. spill_watch logs the peak (and exposes it below).
    with spill_watch(context) as spill, duckdb.get_connection() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS benchmark")
        conn.execute(
            "CREATE TABLE IF NOT EXISTS benchmark.synthetic_load ("
            " partition VARCHAR, row_id BIGINT, bucket INTEGER,"
            " value DOUBLE, created_at TIMESTAMP)"
        )
        # Delete-and-replace this partition (idempotent).
        conn.execute(
            "DELETE FROM benchmark.synthetic_load WHERE partition = ?", [partition]
        )
        # Generate rows server-side in DuckDB so a Python row-build doesn't dominate
        # the timing; this exercises the capped duckdb resource on the write path.
        conn.execute(
            "INSERT INTO benchmark.synthetic_load "
            "SELECT ? AS partition, i AS row_id, (i % 100)::INTEGER AS bucket, "
            "       ((i * 2654435761) % 1000000) / 1000.0 AS value, now() AS created_at "
            "FROM range(CAST(? AS BIGINT)) AS t(i)",
            [partition, config.rows],
        )
        written = conn.execute(
            "SELECT count(*) FROM benchmark.synthetic_load WHERE partition = ?",
            [partition],
        ).fetchone()[0]

    del ballast  # release the memory pressure
    elapsed = round(time.perf_counter() - start, 3)
    context.log.info(
        f"{partition}: wrote {written} rows in {elapsed}s "
        f"(mem_mb={config.mem_mb}, cpu_spin_seconds={config.cpu_spin_seconds})"
    )
    return dg.MaterializeResult(
        metadata={
            "partition": partition,
            "rows": written,
            "elapsed_seconds": elapsed,
            "peak_spill_mb": spill.peak_mb,
            "mem_mb": config.mem_mb,
            "cpu_spin_seconds": config.cpu_spin_seconds,
        }
    )


# A job over just this asset, for a one-click partition backfill from the UI or CLI.
# It inherits the asset's partitions and the deployment's in_process executor.
synthetic_load_job = dg.define_asset_job(
    "synthetic_load_job", selection=[synthetic_load]
)
