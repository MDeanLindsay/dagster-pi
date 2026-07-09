#!/usr/bin/env python
"""Measure this Dagster-on-a-Pi deployment's resource footprint.

Stdlib only, Linux ``/proc`` based — no dependencies, and safe to run on the box
while the services are live: it only reads ``/proc`` and file sizes, never the
DuckDB file or the instance stores. Use it to get before/after numbers around a
tuning change (e.g. multiprocess -> in_process executor)::

    uv run python benchmarks/bench.py --label before
    # ... change config, reload the code location, let it settle ...
    uv run python benchmarks/bench.py --label after

Each run prints a markdown summary and appends one JSON record to
``benchmarks/results/results.jsonl`` so a history accumulates.

Process roles & memory accounting
---------------------------------
A run does NOT show up as ``dagster api execute_run``. With the gRPC code-server +
``DefaultRunLauncher``, every run-worker (and, under the multiprocess executor,
every step-worker) is a ``multiprocessing.spawn`` *child* (a fresh interpreter), so
the two are indistinguishable by command line. We tier them by **parent PID** instead:

    code-server-grpc   the ``dagster api grpc`` code location (parent: code-server)
    run-worker         a spawn child whose parent is the grpc server — one per
                       concurrently executing run
    step-worker        a spawn child whose parent is a run-worker — the multiprocess
                       executor's per-step subprocess. **in_process has none.**
    log-capture        the tiny compute-log tee children of a worker

Workers are *spawned* — fresh interpreters that re-import privately, **not** forks —
so they do not share a copy-on-write import heap with the parent (smaps evidence in
benchmarks/README.md). What they *do* share is file-backed library pages (libpython,
libduckdb), mapped by every process, so **RSS double-counts those shared pages**. We
therefore report **PSS** (proportional set size, from ``/proc/<pid>/smaps_rollup``) as
the headline figure: PSS divides shared pages, so it sums across processes without
double-counting — making "total PSS during a backfill" a fair multiprocess-vs-in_process
comparison. RSS is kept alongside as an upper bound.

cold_import_s  Median wall-clock of ``python -c "import <module>"`` in a fresh
               interpreter — the startup tax every forked worker pays.
state          On-disk instance/data growth: run+event history, compute-log dir
               count, and DuckDB file size.
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
DUCKDB_PATH = Path(
    os.getenv("PI_DUCKDB_PATH", str(PROJECT_ROOT / ".duckdb" / "pi.duckdb"))
)
RESULTS = Path(__file__).resolve().parent / "results" / "results.jsonl"
PAGE_SIZE = os.sysconf("SC_PAGE_SIZE")

# Static services, matched on the command line (checked in order).
SERVICE_RULES = [
    ("dagster-webserver", "webserver"),
    ("dagster-daemon", "daemon"),
    ("api grpc", "code-server-grpc"),
    ("code-server start", "code-server"),
    ("resource_tracker", "mp-tracker"),
]
_SPAWN_MARKERS = ("multiprocessing.spawn", "--multiprocessing-fork")


def _read(path: str) -> str:
    try:
        return Path(path).read_text()
    except (FileNotFoundError, ProcessLookupError, PermissionError):
        return ""


def _cmdline(pid: str) -> str:
    try:
        raw = Path(f"/proc/{pid}/cmdline").read_bytes()
    except (FileNotFoundError, ProcessLookupError, PermissionError):
        return ""
    return raw.replace(b"\x00", b" ").decode("utf-8", "replace").strip()


def _ppid(pid: str) -> int:
    # /proc/<pid>/stat: comm (field 2) may contain spaces/parens, so split after the last ')'.
    stat = _read(f"/proc/{pid}/stat")
    rparen = stat.rfind(")")
    if rparen == -1:
        return 0
    fields = stat[rparen + 2 :].split()
    try:
        return int(fields[1])  # ppid is the 2nd field after state
    except (IndexError, ValueError):
        return 0


def _mem_mb(pid: str) -> tuple[float, float]:
    """(rss_mb, pss_mb). PSS from smaps_rollup; falls back to statm RSS if unavailable."""
    rollup = _read(f"/proc/{pid}/smaps_rollup")
    if rollup:
        rss_kb = pss_kb = 0
        for line in rollup.splitlines():
            if line.startswith("Rss:"):
                rss_kb = int(line.split()[1])
            elif line.startswith("Pss:"):
                pss_kb = int(line.split()[1])
        return round(rss_kb / 1024, 1), round(pss_kb / 1024, 1)
    statm = _read(f"/proc/{pid}/statm").split()
    if len(statm) > 1:
        rss = int(statm[1]) * PAGE_SIZE / 1024 / 1024
        return round(rss, 1), -1.0  # PSS unknown
    return 0.0, -1.0


def _service_role(cmd: str) -> str | None:
    for needle, role in SERVICE_RULES:
        if needle in cmd:
            return role
    return None


def collect_processes() -> list[dict]:
    """Every live process belonging to this deployment, classified by PPID tier."""
    venv_marker = str(VENV_BIN)
    raw: list[dict] = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        cmd = _cmdline(entry.name)
        if venv_marker not in cmd or "bench.py" in cmd:
            continue
        rss, pss = _mem_mb(entry.name)
        raw.append(
            {
                "pid": int(entry.name),
                "ppid": _ppid(entry.name),
                "cmd": cmd,
                "rss_mb": rss,
                "pss_mb": pss,
            }
        )

    # Pass 1: static services. Pass 2: spawn-forks tiered by parent.
    role: dict[int, str] = {}
    for p in raw:
        svc = _service_role(p["cmd"])
        if svc:
            role[p["pid"]] = svc
    grpc_pids = {pid for pid, r in role.items() if r == "code-server-grpc"}

    run_worker_pids: set[int] = set()
    for p in raw:
        if p["pid"] in role:
            continue
        if any(m in p["cmd"] for m in _SPAWN_MARKERS) and p["ppid"] in grpc_pids:
            role[p["pid"]] = "run-worker"
            run_worker_pids.add(p["pid"])
    step_worker_pids: set[int] = set()
    for p in raw:
        if p["pid"] in role:
            continue
        if any(m in p["cmd"] for m in _SPAWN_MARKERS) and p["ppid"] in run_worker_pids:
            role[p["pid"]] = "step-worker"
            step_worker_pids.add(p["pid"])
    worker_pids = grpc_pids | run_worker_pids | step_worker_pids
    for p in raw:
        if p["pid"] in role:
            continue
        role[p["pid"]] = "log-capture" if p["ppid"] in worker_pids else "other"

    procs = [
        {
            "pid": p["pid"],
            "role": role[p["pid"]],
            "rss_mb": p["rss_mb"],
            "pss_mb": p["pss_mb"],
        }
        for p in raw
    ]
    procs.sort(key=lambda p: p["rss_mb"], reverse=True)
    return procs


def cold_import_seconds(module: str, samples: int) -> float | None:
    """Median time to ``import <module>`` in a fresh interpreter, or None."""
    if not VENV_PY.exists():
        return None
    times: list[float] = []
    for _ in range(samples):
        start = time.perf_counter()
        result = subprocess.run(
            [str(VENV_PY), "-c", f"import {module}"], capture_output=True
        )
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
        "duckdb_mb": round(DUCKDB_PATH.stat().st_size / 1024 / 1024, 1)
        if DUCKDB_PATH.exists()
        else 0.0,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--label",
        default="unlabeled",
        help="tag for this snapshot, e.g. before / after",
    )
    parser.add_argument(
        "--module",
        default="dagster_pi.definitions",
        help="module to time a cold import of",
    )
    parser.add_argument(
        "--imports", type=int, default=3, help="cold-import samples; median is reported"
    )
    parser.add_argument(
        "--no-import", action="store_true", help="skip the cold-import timing"
    )
    args = parser.parse_args(argv)

    procs = collect_processes()  # before any cold-import subprocess exists
    by_role: dict[str, dict] = {}
    for p in procs:
        agg = by_role.setdefault(p["role"], {"count": 0, "rss_mb": 0.0, "pss_mb": 0.0})
        agg["count"] += 1
        agg["rss_mb"] = round(agg["rss_mb"] + p["rss_mb"], 1)
        if p["pss_mb"] >= 0:
            agg["pss_mb"] = round(agg["pss_mb"] + p["pss_mb"], 1)
    total_rss = round(sum(p["rss_mb"] for p in procs), 1)
    total_pss = round(sum(p["pss_mb"] for p in procs if p["pss_mb"] >= 0), 1)

    cold = None if args.no_import else cold_import_seconds(args.module, args.imports)
    state = collect_state()

    record = {
        "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "label": args.label,
        "process_count": len(procs),
        "total_rss_mb": total_rss,
        "total_pss_mb": total_pss,
        "by_role": by_role,
        "cold_import_s": cold,
        "cold_import_module": args.module,
        "state": state,
    }
    RESULTS.parent.mkdir(parents=True, exist_ok=True)
    with RESULTS.open("a") as fh:
        fh.write(json.dumps(record) + "\n")

    print(f"\n### Snapshot: {args.label}  ({record['ts']})\n")
    print(
        f"- **Live processes:** {len(procs)}  |  **total PSS:** {total_pss:.0f} MB  |  total RSS: {total_rss:.0f} MB"
    )
    for role, agg in sorted(
        by_role.items(), key=lambda kv: kv[1]["pss_mb"], reverse=True
    ):
        print(
            f"    - {role:<16} x{agg['count']}  PSS {agg['pss_mb']:.0f} MB  (RSS {agg['rss_mb']:.0f} MB)"
        )
    if cold is not None:
        print(
            f"- **Cold import** `{args.module}`: {cold:.3f}s (median of {args.imports})"
        )
    elif not args.no_import:
        print(
            f"- **Cold import** `{args.module}`: n/a (venv python not found at {VENV_PY})"
        )
    print(
        f"- **State:** history {state['history_mb']:.0f} MB · storage {state['storage_mb']:.0f} MB · "
        f"{state['compute_log_dirs']} compute-log dirs · duckdb {state['duckdb_mb']:.0f} MB"
    )
    print(f"\n(appended to {RESULTS.relative_to(PROJECT_ROOT)})\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
