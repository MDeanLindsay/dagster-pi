#!/usr/bin/env python
"""Drive a synthetic_load backfill and peak-sample the deployment footprint.

This automates the during-backfill half of the benchmark: it submits N partition
runs through the live webserver's GraphQL endpoint (cheap — no per-submission code
server), then polls the process table via ``bench.py`` while the daemon executes
them, recording the single sample with the highest **total PSS** (see bench.py for
why PSS, not RSS, is the honest figure for forked workers).

Run it once per executor to get the headline comparison::

    # 1. with the deployment's in_process_executor (the committed default):
    uv run python benchmarks/backfill_probe.py --label in_process

    # 2. switch src/dagster_pi/definitions.py to dg.multiprocess_executor, then
    #    reload the code location (UI -> Reload, or the GraphQL reload mutation)
    #    and re-run:
    uv run python benchmarks/backfill_probe.py --label multiprocess

    # 3. diff the two records in benchmarks/results/measure_results.jsonl; the step-worker
    #    line is the executor delta.

Stdlib only. Targets http://127.0.0.1:3000 (override with PI_WEBSERVER_URL). Data
lands in DuckDB's ``benchmark`` schema — ``DROP SCHEMA benchmark CASCADE`` clears it.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.request
from pathlib import Path

BENCH_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BENCH_DIR))
import bench  # noqa: E402  (sibling module: the /proc snapshot harness)

WEBSERVER = os.getenv("PI_WEBSERVER_URL", "http://127.0.0.1:3000").rstrip("/")
GRAPHQL = f"{WEBSERVER}/graphql"
RESULTS = BENCH_DIR / "results" / "measure_results.jsonl"
LAUNCH = """
mutation($p: ExecutionParams!) {
  launchPipelineExecution(executionParams: $p) {
    __typename
    ... on LaunchRunSuccess { run { runId } }
    ... on PythonError { message }
    ... on RunConfigValidationInvalid { errors { message } }
  }
}
"""


def _gql(query: str, variables: dict) -> dict:
    body = json.dumps({"query": query, "variables": variables}).encode()
    req = urllib.request.Request(
        GRAPHQL, data=body, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


def submit(
    partition: str, rows: int, cpu_spin: float, location: str, repo: str
) -> None:
    params = {
        "selector": {
            "repositoryLocationName": location,
            "repositoryName": repo,
            "jobName": "synthetic_load_job",
        },
        "runConfigData": {
            "ops": {
                "synthetic_load": {
                    "config": {"rows": rows, "cpu_spin_seconds": cpu_spin}
                }
            }
        },
        "executionMetadata": {
            "tags": [{"key": "dagster/partition", "value": partition}]
        },
        "mode": "default",
    }
    res = _gql(LAUNCH, {"p": params})
    node = res.get("data", {}).get("launchPipelineExecution", {})
    if node.get("__typename") != "LaunchRunSuccess":
        raise SystemExit(f"launch failed for {partition}: {json.dumps(res)[:600]}")


def _by_role(procs: list[dict]) -> dict:
    by: dict[str, dict] = {}
    for p in procs:
        agg = by.setdefault(p["role"], {"count": 0, "rss_mb": 0.0, "pss_mb": 0.0})
        agg["count"] += 1
        agg["rss_mb"] = round(agg["rss_mb"] + p["rss_mb"], 1)
        if p["pss_mb"] >= 0:
            agg["pss_mb"] = round(agg["pss_mb"] + p["pss_mb"], 1)
    return by


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument(
        "--label",
        required=True,
        help="tag for this run, e.g. in_process / multiprocess",
    )
    ap.add_argument(
        "--partitions",
        type=int,
        default=8,
        help="runs to submit (>= max_concurrent_runs)",
    )
    ap.add_argument("--rows", type=int, default=200_000)
    ap.add_argument(
        "--cpu-spin",
        type=float,
        default=10.0,
        help="seconds each run busy-loops (keeps workers co-resident long enough to sample)",
    )
    ap.add_argument("--interval", type=float, default=0.4)
    ap.add_argument("--max-seconds", type=float, default=180)
    ap.add_argument("--location", default="dagster_pi")
    ap.add_argument("--repo", default="__repository__")
    args = ap.parse_args()

    parts = [f"part-{i:03d}" for i in range(args.partitions)]
    print(
        f"submitting {len(parts)} runs (rows={args.rows}, cpu_spin={args.cpu_spin}s) -> {GRAPHQL}"
    )
    for p in parts:
        submit(p, args.rows, args.cpu_spin, args.location, args.repo)
    print("submitted; sampling peak footprint ...")

    peak = {"total_pss_mb": -1.0}
    start = time.time()
    seen = drained_at = None
    samples = 0
    while time.time() - start < args.max_seconds:
        procs = bench.collect_processes()
        by = _by_role(procs)
        total_pss = round(sum(p["pss_mb"] for p in procs if p["pss_mb"] >= 0), 1)
        total_rss = round(sum(p["rss_mb"] for p in procs), 1)
        if total_pss > peak["total_pss_mb"]:
            peak = {
                "total_pss_mb": total_pss,
                "total_rss_mb": total_rss,
                "process_count": len(procs),
                "by_role": by,
            }
        workers = by.get("step-worker", {}).get("count", 0) + by.get(
            "run-worker", {}
        ).get("count", 0)
        if workers:
            seen, drained_at = True, None
        elif seen and drained_at is None:
            drained_at = time.time()
        if seen and drained_at and time.time() - drained_at > 6:
            break
        samples += 1
        time.sleep(args.interval)

    out = {
        "label": args.label,
        "partitions": args.partitions,
        "rows": args.rows,
        "cpu_spin_seconds": args.cpu_spin,
        "samples": samples,
        **peak,
    }
    RESULTS.parent.mkdir(parents=True, exist_ok=True)
    RESULTS.open("a").write(json.dumps(out) + "\n")

    sw = peak["by_role"].get("step-worker", {"count": 0, "pss_mb": 0, "rss_mb": 0})
    rw = peak["by_role"].get("run-worker", {"count": 0, "pss_mb": 0, "rss_mb": 0})
    print(f"\n### {args.label}: peak during {args.partitions}-partition backfill\n")
    print(
        f"- total PSS {peak['total_pss_mb']:.0f} MB (RSS {peak['total_rss_mb']:.0f} MB), {peak['process_count']} processes"
    )
    print(
        f"- run-workers  x{rw['count']}  PSS {rw['pss_mb']:.0f} MB (RSS {rw['rss_mb']:.0f} MB)"
    )
    print(
        f"- step-workers x{sw['count']}  PSS {sw['pss_mb']:.0f} MB (RSS {sw['rss_mb']:.0f} MB)"
    )
    print(f"\n(appended to {RESULTS.relative_to(BENCH_DIR.parent)})\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
