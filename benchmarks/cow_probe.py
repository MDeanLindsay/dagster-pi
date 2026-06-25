#!/usr/bin/env python
"""Plan (a): do concurrent run-workers share the code server's imported pages
copy-on-write, or re-import them privately?

This is the measurement behind the "eager-load the code server" idea (#1). That
optimization only pays off if run-workers are **forks** that inherit the parent's
import pages COW -- then preloading the parent means every fork shares those pages
instead of re-importing. If run-workers are **spawns** (a fresh interpreter that
re-imports dagster + user code from scratch), the parent's state is irrelevant and
eager-loading cannot help. So we have to know the start method before investing.

The /proc/<pid>/smaps_rollup breakdown is the definitive tell:

    Shared_Clean / Shared_Dirty   pages shared with parent/siblings -> COW (fork)
    Private_Clean / Private_Dirty  pages unique to this worker      -> re-import (spawn)
    Anonymous                      heap (imported objects/bytecode); private if re-imported

A forked worker that inherited imports shows those pages as *shared* (its PSS for
them is divided among sharers), so PSS << RSS and private/anon stay low. A spawned
worker re-imports into its *own* anonymous heap: high Private_Dirty/Anonymous, PSS
approx RSS. PSS (proportional set size) is the honest per-worker figure.

We submit N synthetic_load runs through the live webserver (the daemon gates them
to run_queue.max_concurrent_runs), keep them co-resident with cpu_spin, and capture
the sample with the most workers. rows is kept tiny so each worker's footprint is
dominated by *imports* (the thing under test), not DuckDB write working-set --
and cpu_spin runs *before* the DuckDB write, so we sample imports-only.

Stdlib only. Targets http://127.0.0.1:3000 (PI_WEBSERVER_URL). Reuses bench.py for
process classification and backfill_probe.py for run submission. Data lands in
DuckDB's `benchmark` schema (`DROP SCHEMA benchmark CASCADE` clears it).
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

BENCH_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BENCH_DIR))
import bench  # noqa: E402  (/proc snapshot + role classification)
from backfill_probe import submit  # noqa: E402  (GraphQL run submission)

RESULTS = BENCH_DIR / "results" / "cow_results.jsonl"
_FIELDS = (
    "Rss",
    "Pss",
    "Shared_Clean",
    "Shared_Dirty",
    "Private_Clean",
    "Private_Dirty",
    "Anonymous",
)


def smaps_detail(pid: int) -> dict:
    """Per-process COW breakdown from smaps_rollup, in MB (0s if unreadable)."""
    out = {k: 0 for k in _FIELDS}
    try:
        text = Path(f"/proc/{pid}/smaps_rollup").read_text()
    except (FileNotFoundError, ProcessLookupError, PermissionError):
        return {k: 0.0 for k in _FIELDS}
    for line in text.splitlines():
        key, _, rest = line.partition(":")
        if key in out:
            out[key] = int(rest.split()[0])  # kB
    return {k: round(v / 1024, 1) for k, v in out.items()}


def snapshot() -> tuple[list[dict], dict | None, float, int]:
    """(run_workers_with_detail, grpc_parent_detail, total_pss_mb, process_count)."""
    procs = bench.collect_processes()
    workers: list[dict] = []
    grpc: dict | None = None
    for p in procs:
        if p["role"] == "run-worker":
            d = smaps_detail(p["pid"])
            d.update(
                pid=p["pid"],
                ppid=bench._ppid(str(p["pid"])),
                cmd=bench._cmdline(str(p["pid"])),
            )
            workers.append(d)
        elif p["role"] == "code-server-grpc":
            grpc = smaps_detail(p["pid"])
            grpc["pid"] = p["pid"]
    total_pss = round(sum(p["pss_mb"] for p in procs if p["pss_mb"] >= 0), 1)
    return workers, grpc, total_pss, len(procs)


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--label", default="a-lazy-baseline")
    ap.add_argument(
        "--partitions",
        type=int,
        default=6,
        help="runs to submit (>= max_concurrent_runs)",
    )
    ap.add_argument(
        "--rows",
        type=int,
        default=10_000,
        help="kept tiny to isolate import footprint from DuckDB working-set",
    )
    ap.add_argument(
        "--cpu-spin",
        type=float,
        default=20.0,
        help="seconds each run spins (before the DuckDB write) to stay co-resident",
    )
    ap.add_argument("--interval", type=float, default=0.3)
    ap.add_argument("--max-seconds", type=float, default=150)
    ap.add_argument("--location", default="dagster_pi")
    ap.add_argument("--repo", default="__repository__")
    args = ap.parse_args()

    _, grpc0, idle_pss, _ = snapshot()
    grpc0_pss = grpc0["Pss"] if grpc0 else float("nan")
    print(
        f"idle: total PSS {idle_pss:.0f} MB | code-server-grpc parent PSS {grpc0_pss:.0f} MB"
    )

    parts = [f"part-{i:03d}" for i in range(args.partitions)]
    print(
        f"submitting {len(parts)} runs (rows={args.rows}, cpu_spin={args.cpu_spin}s) ..."
    )
    for p in parts:
        submit(p, args.rows, args.cpu_spin, args.location, args.repo)
    print("submitted; polling for peak concurrency ...")

    best: dict | None = None
    start = time.time()
    seen = False
    drained_at: float | None = None
    while time.time() - start < args.max_seconds:
        workers, grpc, total_pss, nproc = snapshot()
        n = len(workers)
        if n:
            seen, drained_at = True, None
            if best is None or (n, total_pss) > (best["n"], best["total_pss"]):
                best = {
                    "n": n,
                    "total_pss": total_pss,
                    "nproc": nproc,
                    "workers": workers,
                    "grpc": grpc,
                }
        elif seen and drained_at is None:
            drained_at = time.time()
        if seen and drained_at and time.time() - drained_at > 5:
            break
        time.sleep(args.interval)

    if not best:
        print(
            "NO run-workers observed -- check that runs launched (max_concurrent_runs, repo name)."
        )
        return 1

    ws = best["workers"]
    k = len(ws)
    mean = lambda f: round(sum(f(w) for w in ws) / k, 1)  # noqa: E731
    mean_rss = mean(lambda w: w["Rss"])
    mean_pss = mean(lambda w: w["Pss"])
    mean_shared = mean(lambda w: w["Shared_Clean"] + w["Shared_Dirty"])
    mean_private = mean(lambda w: w["Private_Clean"] + w["Private_Dirty"])
    mean_anon = mean(lambda w: w["Anonymous"])
    marginal = round((best["total_pss"] - idle_pss) / k, 1)
    spawn = mean_private > mean_shared

    print(
        f"\n### {args.label}: peak {best['n']} concurrent run-workers ({best['nproc']} procs)\n"
    )
    print(
        f"- total PSS {best['total_pss']:.0f} MB  (idle {idle_pss:.0f} MB)  ->  marginal {marginal:.0f} MB per concurrent run"
    )
    print(
        f"- run-worker mean: RSS {mean_rss:.0f} | PSS {mean_pss:.0f} | shared {mean_shared:.0f} | private {mean_private:.0f} | anon {mean_anon:.0f} MB"
    )
    for w in ws:
        print(
            f"    pid {w['pid']} ppid {w['ppid']}: RSS {w['Rss']:.0f} PSS {w['Pss']:.0f} "
            f"shared {w['Shared_Clean'] + w['Shared_Dirty']:.0f} privDirty {w['Private_Dirty']:.0f} anon {w['Anonymous']:.0f}"
        )
    verdict = (
        "SPAWN / private re-import  ->  eager-load CANNOT help run-workers"
        if spawn
        else "FORK / COW sharing  ->  eager-load may reduce per-run cost"
    )
    print(
        f"\nverdict: mean private {mean_private:.0f} MB vs shared {mean_shared:.0f} MB  =>  {verdict}"
    )
    print(f"worker cmdline: {ws[0]['cmd'][:240]}")

    RESULTS.parent.mkdir(parents=True, exist_ok=True)
    RESULTS.open("a").write(
        json.dumps(
            {
                "label": args.label,
                "idle_pss": idle_pss,
                "grpc_parent_pss": grpc0_pss,
                "peak_workers": best["n"],
                "peak_total_pss": best["total_pss"],
                "marginal_pss_per_run": marginal,
                "worker_mean_rss": mean_rss,
                "worker_mean_pss": mean_pss,
                "worker_mean_shared": mean_shared,
                "worker_mean_private": mean_private,
                "worker_mean_anon": mean_anon,
                "spawn": spawn,
                "workers": ws,
            }
        )
        + "\n"
    )
    print(f"\n(appended to {RESULTS.relative_to(BENCH_DIR.parent)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
