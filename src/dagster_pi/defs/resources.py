"""Shared resources, auto-loaded into the project's Definitions.

The DuckDB file lives inside the project (gitignored), on the NVMe SSD tree:
    /home/user/dagster_pi/data/pi.duckdb

Override with the PI_DUCKDB_PATH env var (e.g. in the systemd unit) if it moves.
The connection is lazy, so importing this module never opens the file.

DuckDB defaults to ~80% of RAM and every core. On a shared Pi that lets one big
query starve the webserver + daemon (or OOM the box) during a concurrent run, so
we cap the connection:

    memory_limit    working-set cap (default 2GB, ~25% of the Pi's 8GB)
    threads         cores a query may grab (default 2 of 4 — leaves headroom for
                    the webserver/daemon to stay responsive under a heavy query)
    temp_directory  where DuckDB spills to disk when it hits memory_limit, rather
                    than failing — kept on NVMe under data/ (so it's gitignored
                    and never on the SD card)

All three are env-overridable; raise them for a one-off heavy backfill if needed.
"""

import os

import dagster as dg
from dagster_duckdb import DuckDBResource

DUCKDB_PATH = os.getenv("PI_DUCKDB_PATH", "/home/user/dagster_pi/data/pi.duckdb")
DUCKDB_MEMORY_LIMIT = os.getenv("PI_DUCKDB_MEMORY_LIMIT", "2GB")
DUCKDB_THREADS = int(os.getenv("PI_DUCKDB_THREADS", "2"))
DUCKDB_TEMP_DIR = os.getenv(
    "PI_DUCKDB_TEMP_DIR",
    os.path.join(os.path.dirname(DUCKDB_PATH), ".duckdb_tmp"),
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
