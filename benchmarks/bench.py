#!/usr/bin/env python
"""Measure this Dagster-on-a-Pi deployment's resource footprint.

Stdlib only, Linux ``/proc`` based — no dependencies, and safe to run on the box
while the services are live: it only reads ``/proc`` and file sizes, never the
DuckDB file or the instance stores. Use it to get before/after numbers around a
tuning change (e.g. multiprocess -> in_process executor)::

    uv run python benchmarks/bench.py --label before
    # ... change config, `sudo systemctl restart dagster-*`, let it settle ...
    uv run python benchmarks/bench.py --label after

Each run prints a markdown summary and appends one JSON record to
``benchmarks/results.jsonl`` so a history accumulates.

Metrics
-------
processes      Live deployment processes (count + RSS each + total), classified
               by role. The multiprocess executor's ~150 MB *step-worker*
               subprocesses appear here only while a run is executing, so to
               capture that delta, run this during a backfill.
cold_import_s  Median wall-clock of ``python -c "import <module>"`` in a fresh
               interpreter — the startup tax every forked process pays. The
               multiprocess executor pays it per step; in_process pays it per run.
state          On-disk instance/data growth: run+event history, compute-log dir
               count, and DuckDB file size.

Paths are derived from this file's location (``<project>/benchmarks/bench.py``)
with env overrides (DAGSTER_HOME, PI_DUCKDB_PATH), so the script is portable to
any copy of the template.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
VENV_BIN = PROJECT_ROOT / ".venv" / "bin"
VENV_PY = VENV_BIN / "python"
DAGSTER_HOME = Path(os.getenv("DAGSTER_HOME", str(PROJECT_ROOT / ".dagster_home")))
DUCKDB_PATH = Path(os.getenv("PI_DUCKDB_PATH", str(PROJECT_ROOT / "data" / "pi.duckdb")))
RESULTS = Path(__file__).resolve().parent / "results.jsonl"
PAGE_SIZE = os.sysconf("SC_PAGE_SIZE")

# Substrings -> role, checked in order against each process's full command line.
ROLE_RULES = [
    ("dagster-webserver", "webserver"),
    ("dagster-daemon", "daemon"),
    ("dagster api grpc", "code-server-grpc"),
    ("code-server", "code-server"),
    ("execute_run", "run-worker"),
    ("watch_orphans", "orphan-watcher"),
    ("multiprocessing.spawn", "step-worker"),
    ("multiprocessing.resource_tracker", "mp-tracker"),
]


def _cmdline(pid: str) -> str:
    try:
        raw = Path(f"/proc/{pid}/cmdline").read_bytes()
    except (FileNotFoundError, ProcessLookupError, PermissionError):
        return ""
    return raw.replace(b"\x00", b" ").decode("utf-8", "replace").strip()


def _rss_mb(pid: str) -> float:
    try:
        resident_pages = int(Path(f"/proc/{pid}/statm").read_text().split()[1])
    except (FileNotFoundError, ProcessLookupError, PermissionError, IndexError, ValueError):
        return 0.0
    return resident_pages * PAGE_SIZE / 1024 / 1024


def _classify(cmd: str) -> str:
    for needle, role in ROLE_RULES:
        if needle in cmd:
            return role
    return "other"


def collect_processes() -> list[dict]:
    """Every live process belonging to this deployment (matched by venv path)."""
    venv_marker = str(VENV_BIN)
    procs: list[dict] = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        cmd = _cmdline(entry.name)
        if not cmd:
            continue
        # Match by venv path: every service + forked worker runs <project>/.venv/bin/python,
        # so this catches them all without false-positiving on, say, an avahi hostname
        # that happens to contain "dagster".
        if venv_marker not in cmd:
            continue
        if "bench.py" in cmd:  # don't count ourselves
            continue
        procs.append(
            {"pid": int(entry.name), "role": _classify(cmd), "rss_mb": round(_rss_mb(entry.name), 1)}
        )
    procs.sort(key=lambda p: p["rss_mb"], reverse=True)
    return procs


def cold_import_seconds(module: str, samples: int) -> float | None:
    """Median time to ``import <module>`` in a fresh interpreter, or None."""
    if not VENV_PY.exists():
        return None
    times: list[float] = []
    for _ in range(samples):
        start = time.perf_counter()
        result = subprocess.run([str(VENV_PY), "-c", f"import {module}"], capture_output=True)
        if result.returncode == 0:
            times.append(time.perf_counter() - start)
    return round(statistics.median(times), 3) if times else None


def _dir_size_mb(path: Path) -> float:
    if not path.exists():
        return 0.0
    total = 0
    for p in path.rglob("*"):
        if p.is_file():
            try:
                total += p.stat().st_size
            except OSError:
                pass
    return round(total / 1024 / 1024, 1)


def _compute_log_dirs(storage: Path) -> int:
    if not storage.exists():
        return 0
    return sum(1 for p in storage.iterdir() if (p / "compute_logs").is_dir())


def collect_state() -> dict:
    storage = DAGSTER_HOME / "storage"
    return {
        "history_mb": _dir_size_mb(DAGSTER_HOME / "history"),
        "storage_mb": _dir_size_mb(storage),
        "compute_log_dirs": _compute_log_dirs(storage),
        "duckdb_mb": round(DUCKDB_PATH.stat().st_size / 1024 / 1024, 1) if DUCKDB_PATH.exists() else 0.0,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--label", default="unlabeled", help="tag for this snapshot, e.g. before / after")
    parser.add_argument("--module", default="dagster_pi.definitions", help="module to time a cold import of")
    parser.add_argument("--imports", type=int, default=3, help="cold-import samples; median is reported")
    parser.add_argument("--no-import", action="store_true", help="skip the cold-import timing")
    args = parser.parse_args(argv)

    procs = collect_processes()  # before any cold-import subprocess exists
    by_role: dict[str, dict] = {}
    for p in procs:
        agg = by_role.setdefault(p["role"], {"count": 0, "rss_mb": 0.0})
        agg["count"] += 1
        agg["rss_mb"] = round(agg["rss_mb"] + p["rss_mb"], 1)
    total_rss = round(sum(p["rss_mb"] for p in procs), 1)

    cold = None if args.no_import else cold_import_seconds(args.module, args.imports)
    state = collect_state()

    record = {
        "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "label": args.label,
        "process_count": len(procs),
        "total_rss_mb": total_rss,
        "by_role": by_role,
        "cold_import_s": cold,
        "cold_import_module": args.module,
        "state": state,
    }
    with RESULTS.open("a") as fh:
        fh.write(json.dumps(record) + "\n")

    print(f"\n### Snapshot: {args.label}  ({record['ts']})\n")
    print(f"- **Live processes:** {len(procs)}  |  **total RSS:** {total_rss:.0f} MB")
    for role, agg in sorted(by_role.items(), key=lambda kv: kv[1]["rss_mb"], reverse=True):
        print(f"    - {role:<18} x{agg['count']}  {agg['rss_mb']:.0f} MB")
    if cold is not None:
        print(f"- **Cold import** `{args.module}`: {cold:.3f}s (median of {args.imports})")
    elif not args.no_import:
        print(f"- **Cold import** `{args.module}`: n/a (venv python not found at {VENV_PY})")
    print(
        f"- **State:** history {state['history_mb']:.0f} MB · storage {state['storage_mb']:.0f} MB · "
        f"{state['compute_log_dirs']} compute-log dirs · duckdb {state['duckdb_mb']:.0f} MB"
    )
    print(f"\n(appended to {RESULTS.relative_to(PROJECT_ROOT)})\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
