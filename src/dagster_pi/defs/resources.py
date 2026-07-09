"""Shared resources, auto-loaded into the project's Definitions.

The DuckDB file lives at <project-root>/.duckdb/pi.duckdb, relative to the
repo root regardless of username or clone location.

Override with the PI_DUCKDB_PATH env var (e.g. in the systemd unit) if it moves.
The connection is lazy, so importing this module never opens the file.

DuckDB allows one read-write process per file, so every asset that opens this
resource must carry ``pool=DUCKDB_POOL``: the instance-level pool (limit 1,
``concurrency:`` in dagster.yaml) serializes those runs at the queue, where a
waiting run costs nothing. Without it, colliding runs sit in dagster-duckdb's
connect-retry loop (~100 s of backoff inside a live ~107 MB run-worker) and fail
outright if the writer holds the lock longer.

DuckDB also defaults to ~80% of RAM and every core. On a shared Pi that lets one
big query starve the webserver + daemon — or blow through the code-server
cgroup's MemoryMax and get the run OOM-killed mid-write instead of spilling. So
we cap the connection, sized for the ONE live instance the pool guarantees:

    memory_limit    working-set cap (default 3GB). Past it DuckDB spills to
                    temp_directory instead of failing. Keeps the lone writer plus
                    its run-worker and the gRPC server under the code-server
                    cgroup's MemoryHigh=4G.
    threads         cores a query may grab (default 4 — a single writer can't
                    oversubscribe, and CPUWeight/Nice keep the UI responsive).
    temp_directory  where DuckDB spills to disk when it hits memory_limit, rather
                    than failing — kept under ~/.duckdb/.tmp

All three are env-overridable; raise memory_limit for a one-off heavy backfill,
keeping it under the cgroup's MemoryHigh=4G minus ~300 MB of process overhead.
"""

import os
import threading
from contextlib import contextmanager

import dagster as dg
from dagster_duckdb import DuckDBResource

_PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
)
DUCKDB_PATH = os.getenv(
    "PI_DUCKDB_PATH", os.path.join(_PROJECT_ROOT, ".duckdb", "pi.duckdb")
)
DUCKDB_MEMORY_LIMIT = os.getenv("PI_DUCKDB_MEMORY_LIMIT", "3GB")
DUCKDB_THREADS = int(os.getenv("PI_DUCKDB_THREADS", "4"))
DUCKDB_TEMP_DIR = os.getenv(
    "PI_DUCKDB_TEMP_DIR",
    os.path.join(_PROJECT_ROOT, ".duckdb", ".tmp"),
)

# Concurrency-pool name for anything that opens the DuckDB file read-write.
# dagster.yaml gives every pool a default limit of 1 with run granularity, so
# tagging an asset `pool=DUCKDB_POOL` is what enforces the single writer.
DUCKDB_POOL = "duckdb"


def _dir_size_bytes(path: str) -> int:
    """Total size of files under `path` (recursive). Best-effort: skips entries
    that vanish mid-walk, which DuckDB's transient spill files routinely do."""
    total = 0
    try:
        entries = os.scandir(path)
    except OSError:
        return 0
    with entries:
        for entry in entries:
            try:
                if entry.is_file(follow_symlinks=False):
                    total += entry.stat(follow_symlinks=False).st_size
                elif entry.is_dir(follow_symlinks=False):
                    total += _dir_size_bytes(entry.path)
            except OSError:
                continue
    return total


class SpillStat:
    """Holder for the spill high-water mark, readable after `spill_watch` exits."""

    peak_bytes: int = 0

    @property
    def peak_mb(self) -> float:
        return round(self.peak_bytes / (1024 * 1024), 1)


@contextmanager
def spill_watch(
    context: dg.AssetExecutionContext,
    temp_dir: str = DUCKDB_TEMP_DIR,
    interval: float = 0.5,
):
    """Sample the DuckDB spill dir on a background thread; log the peak on exit.

    Past `memory_limit` DuckDB spills to `temp_directory` instead of failing, so a
    too-large query doesn't crash — it just gets slower, invisibly. This polls the
    spill dir size and reports the high-water mark, turning that silent slowdown
    into a logged number (and `peak_spill_mb` asset metadata). It only logs when
    spilling actually happened, so ordinary in-memory runs stay quiet.

    Caveat: the spill dir is shared, so a *concurrent* spilling run inflates this
    number. Single-run benchmarks (the intended use) aren't affected. Cheap: one
    daemon thread doing a `scandir` every `interval` seconds.
    """
    stat = SpillStat()
    stop = threading.Event()
    baseline = _dir_size_bytes(temp_dir)

    def sample() -> None:
        while not stop.wait(interval):
            cur = _dir_size_bytes(temp_dir) - baseline
            if cur > stat.peak_bytes:
                stat.peak_bytes = cur

    thread = threading.Thread(target=sample, daemon=True)
    thread.start()
    try:
        yield stat
    finally:
        stop.set()
        thread.join(timeout=interval * 2)
        if stat.peak_bytes > 0:
            context.log.info(
                f"duckdb peak spill: {stat.peak_mb} MB (temp_dir={temp_dir})"
            )


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "duckdb": DuckDBResource(
                database=DUCKDB_PATH,
                connection_config={
                    "memory_limit": DUCKDB_MEMORY_LIMIT,
                    "threads": DUCKDB_THREADS,
                    "temp_directory": DUCKDB_TEMP_DIR,
                },
            )
        },
    )
