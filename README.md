# Dagster on a Raspberry Pi 5

Self-hosted Dagster as an always-on service on a Pi 5: scheduled ingestion, light transforms, and a real orchestration UI on hardware that costs less than a month of most cloud bills.

This is a from-scratch guide and assumes no prior Dagster. Follow it end to end and you get the same three-process service, tuned for low-resource operation with a benchmark harness behind every tuning claim. Optimization detail lives in [Tuning](#tuning-for-low-resources) at the bottom.

**What you get**
- Three-process split (code-server / webserver / daemon) for in-place code reloads and crash isolation
- DuckDB storage with memory and thread caps plus NVMe spill
- systemd cgroup isolation so a runaway asset can't take down the box
- Opt-in retention job to keep instance state bounded
- A `/proc`-based benchmark harness, safe to run against the live service

## Architecture

Three cooperating processes:

| Process | Port | Role |
|---|---|---|
| Code-server (gRPC) | 4000, loopback | Imports your Python once, serves asset and job defs |
| Webserver (UI + GraphQL) | 3000, `0.0.0.0` | The web UI you open |
| Daemon | none | Schedules, sensors, run queue |

The split lets you reload code in place (**Reload** in the UI) without restarting the UI or losing daemon state, and keeps a bug in your asset code from taking down the web server.

## Install (Pi to running service)

**Prerequisites:** 64-bit Raspberry Pi OS (Lite is fine), run as your normal user, not root. Examples use the account name `user`; if yours differs, replace `user` and `/home/user/...` everywhere. The whole install works on your LAN; add [Tailscale](#remote-access-with-tailscale) for remote access.

### 1. Install `uv`

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
source ~/.bashrc          # put uv on PATH (or open a new shell)
uv --version
```

### 2. Create the project

```bash
cd ~
uvx create-dagster@latest project dagster-pi --uv-sync
cd dagster-pi
```

`--uv-sync` installs dependencies (including `dagster-dg-cli`, version-matched to core) into a `.venv`.

### 3. Promote the webserver to a runtime dependency

`create-dagster` puts `dagster-webserver` in the dev group; the always-on service needs it at runtime:

```bash
uv add dagster-webserver            # promote to a dependency
uv remove --dev dagster-webserver   # drop the scaffold's dev copy
```

`uv` resolves a version aligned with the installed `dagster`. On 64-bit Pi OS this pulls prebuilt `aarch64` wheels, no compiling.

### 4. Set up `DAGSTER_HOME`

Holds instance state (SQLite run and event storage, fine for one Pi). It lives inside the project as `.dagster_home/` (gitignored) to keep the deployment in one directory; point `DAGSTER_HOME` outside the repo if you'd rather separate state from code. The config is optional (omit it for all-defaults); these three blocks are low-resource choices, each explained in [Tuning](#tuning-for-low-resources).

```bash
mkdir -p ~/dagster-pi/.dagster_home
cat > ~/dagster-pi/.dagster_home/dagster.yaml <<'EOF'
# Cap concurrent runs; each run is its own process on 4 cores.
run_queue:
  max_concurrent_runs: 3

# Don't phone home.
telemetry:
  enabled: false

# Auto-purge old tick records so the SQLite stores stay bounded.
retention:
  schedule:
    purge_after_days: 90
  sensor:
    purge_after_days:
      skipped: 7
      failure: 30
      success: -1   # keep forever
EOF
```

### 5. Create the DuckDB data directory

DuckDB won't create intermediate directories, so make it before the first asset run. It holds `pi.duckdb` and the spill dir (`.duckdb/.tmp`); gitignored; override the path with `PI_DUCKDB_PATH`.

```bash
mkdir -p ~/dagster-pi/.duckdb
```

### 6. Create `workspace.yaml`

Points the webserver and daemon at the gRPC code server:

```bash
cat > ~/dagster-pi/workspace.yaml <<'EOF'
load_from:
  - grpc_server:
      host: 127.0.0.1
      port: 4000
      location_name: dagster_pi
EOF
```

### 7. Verify the project loads

```bash
cd ~/dagster-pi
uv run dg check defs
uv run dg list defs
```

A fresh project has no definitions yet; `dg check defs` should still pass.

### 8. Install the three systemd services

One unit per process, each in its own cgroup. What the resource caps do, and which actually take effect on a Pi, is covered in [Resource isolation](#resource-isolation-blast-radius-control); install as-is and tune later.

> **systemd has no inline comments.** A `#` must start its own line or it folds into the value (`MemoryMax=5G  # note` breaks). Keep the comments below on their own lines.

**Code-server** (gRPC). Runs and step-workers live in this cgroup, so its caps bound the real work; lowest CPU priority of the three.

```bash
sudo tee /etc/systemd/system/dagster-code-server.service > /dev/null <<'EOF'
[Unit]
Description=Dagster code location server (gRPC)
After=network-online.target
Wants=network-online.target

[Service]
User=user
WorkingDirectory=/home/user/dagster-pi
Environment=DAGSTER_HOME=/home/user/dagster-pi/.dagster_home

Environment=MALLOC_ARENA_MAX=2
Environment=MALLOC_TRIM_THRESHOLD_=131072
ExecStart=/home/user/dagster-pi/.venv/bin/dagster code-server start -h 127.0.0.1 -p 4000 -m dagster_pi.definitions
Restart=on-failure
RestartSec=5

MemoryMax=5G
MemoryHigh=4G
CPUWeight=100
IOWeight=100
Nice=10
LimitAS=6G

[Install]
WantedBy=multi-user.target
EOF
```

**Webserver** (UI + GraphQL). The one you want responsive under load: top priority, little RAM.

```bash
sudo tee /etc/systemd/system/dagster-webserver.service > /dev/null <<'EOF'
[Unit]
Description=Dagster webserver (UI + GraphQL)
After=network-online.target dagster-code-server.service
Wants=network-online.target dagster-code-server.service

[Service]
User=user
WorkingDirectory=/home/user/dagster-pi
Environment=DAGSTER_HOME=/home/user/dagster-pi/.dagster_home
ExecStart=/home/user/dagster-pi/.venv/bin/dagster-webserver -h 0.0.0.0 -p 3000 -w /home/user/dagster-pi/workspace.yaml
Restart=on-failure
RestartSec=5

MemoryHigh=1G
CPUWeight=800
IOWeight=500
Nice=-5

[Install]
WantedBy=multi-user.target
EOF
```

**Daemon** (schedules, sensors, run queue). Middle priority.

```bash
sudo tee /etc/systemd/system/dagster-daemon.service > /dev/null <<'EOF'
[Unit]
Description=Dagster daemon (schedules, sensors, run queue)
After=network-online.target dagster-code-server.service
Wants=network-online.target dagster-code-server.service

[Service]
User=user
WorkingDirectory=/home/user/dagster-pi
Environment=DAGSTER_HOME=/home/user/dagster-pi/.dagster_home
ExecStart=/home/user/dagster-pi/.venv/bin/dagster-daemon run -w /home/user/dagster-pi/workspace.yaml
Restart=on-failure
RestartSec=5

MemoryHigh=1G
CPUWeight=300
IOWeight=300
Nice=0

[Install]
WantedBy=multi-user.target
EOF
```

### 9. Enable and start

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now dagster-code-server dagster-webserver dagster-daemon
sudo systemctl status dagster-code-server dagster-webserver dagster-daemon --no-pager
```

### 10. Open the UI

```
http://<pi-ip>:3000
```

Reachable from any device that can route to the Pi. Keep port 3000 off the public internet (do not port-forward it on your router). For remote access, use Tailscale.

## Remote access with Tailscale

A private tailnet beats a router port-forward: only your own devices can reach the Pi.

```bash
curl -fsSL https://tailscale.com/install.sh | sh
sudo tailscale up        # authenticate at the printed URL
tailscale ip -4          # the Pi's tailnet address (100.x.y.z)
tailscale status
```

Then open `http://<pi-tailscale-name>:3000` from any device on the tailnet.

**Harden:**
- Close port 3000 on your LAN and router; rely on Tailscale.
- Use tailnet ACLs to limit which devices reach the Pi.
- For TLS, `tailscale serve https / http://localhost:3000` puts the UI behind HTTPS with no certs to manage.

### Updating code

The code server reloads code without restarting the process:

1. **Deployment → Code locations → Reload** in the UI. Usually all you need.
2. Only if a reload misses it (for example, a new installed dependency), restart:
   ```bash
   sudo systemctl restart dagster-code-server dagster-webserver dagster-daemon
   ```

## Tuning for low resources

Stock defaults run fine. The changes below (all already applied in this repo) make the deployment lighter on a Pi, and every claim has a measured number behind it. Full method and committed numbers: [benchmarks/README.md](benchmarks/README.md).

### The levers

| Lever | Where | What it buys | Measured |
|---|---|---|---|
| `in_process_executor` | [definitions.py](src/dagster_pi/definitions.py) | Steps run inside the run process instead of forking a ~100 MB step-worker each. Runs still parallelize at the run level. | saves 378 MB PSS during a 4-concurrent backfill |
| `max_concurrent_runs: 3` | [dagster.yaml](.dagster_home/dagster.yaml) | A backfill peaks at 3 run-workers (~107 MB each, unshareable), not 4, and eases CPU oversubscription. | peak 3 × ~107 MB |
| DuckDB caps | [resources.py](src/dagster_pi/defs/resources.py) | `memory_limit=1.5GB`, `threads=2`, NVMe spill. One query can't grab ~80% of RAM and every core; 3 × 1.5 GB also bounds aggregate DuckDB RAM. | guardrail |
| glibc arena cap | [code-server unit](#8-install-the-three-systemd-services) | `MALLOC_ARENA_MAX=2` curbs malloc-arena fragmentation; run-workers inherit it. | ~7%, code-server tier |
| cgroup isolation | systemd units | Per-process caps so a runaway asset OOMs in its own cgroup, not the box. | see [below](#resource-isolation-blast-radius-control) |
| Telemetry off | [dagster.yaml](.dagster_home/dagster.yaml) | No usage stats leave the box. | n/a |
| Tick retention | [dagster.yaml](.dagster_home/dagster.yaml) | Schedule/sensor ticks auto-purge instead of growing forever. | n/a |


### What moves, and what doesn't

- **Idle is unchanged.** At rest there are no workers, so the executor and concurrency levers do nothing; the three resident services sit at ~485 MB PSS total. The one idle effect is the glibc cap (~7%), kept only on the code-server, where run-worker propagation is proven.

- **The 378 MB saving is during a backfill.** It is the step-worker tier the executor removes, and it scales with `max_concurrent_runs × steps-per-run`.

- **The DuckDB cap is a guardrail, not a footprint cut.** Its payoff: a heavy or buggy query can no longer grab most of RAM and all cores and OOM the box.

### Running a job heavier than the cap

The cap is a spill threshold, not a wall: past `memory_limit` DuckDB spills to the NVMe temp dir and finishes slower, not failed.

- **Raise the caps for a known-heavy one-off** (all three are env-overridable). Keep `caps × max_concurrent_runs` inside RAM:
  ```bash
  PI_DUCKDB_MEMORY_LIMIT=4GB PI_DUCKDB_THREADS=4 \
    uv run dagster job launch -j synthetic_load_job -w workspace.yaml
  ```
- **Spilling is invisible unless you measure it.** The `spill_watch` helper ([resources.py](src/dagster_pi/defs/resources.py)) samples the spill dir and logs the peak (and a `peak_spill_mb` metadata field) only when a run actually spilled. The caps bound DuckDB memory only; Python-side memory (e.g. `fetchall` into a giant list) is bounded by `LimitAS`, so stream results rather than materialize them.

### Resource isolation (blast-radius control)

The `[Service]` caps in [step 8](#8-install-the-three-systemd-services) put each process in its own cgroup so a runaway asset dies without taking the UI or `sshd` with it. Whether each directive is actually enforced on a Pi 5:

| Directive | Role | Enforced here? |
|---|---|---|
| `CPUWeight` / `Nice` | Proportional CPU and scheduler priority; UI (800 / -5) stays ahead of a heavy backfill, code-server (100 / 10) runs at the back. No controller needed. | **Yes** |
| `LimitAS` | Per-process address-space rlimit; a run ballooning past 6 G gets a clean `MemoryError`. No controller needed. | **Yes** (a `mem_mb=7000` run died in 0.1 s, UI stayed up) |
| `MemoryHigh` / `MemoryMax` | Throttle or OOM-kill a runaway run inside the code-server cgroup. Needs the kernel memory cgroup controller. | **No** (controller disabled at boot here) |
| `IOWeight` | Proportional disk I/O, only with a `bfq`-scheduled device. NVMe's default `none` scheduler exposes no `io.weight`. | **No** (harmless on NVMe) |

On this host, CPU priority and `LimitAS` are proven. `MemoryHigh`/`MemoryMax` need the kernel memory cgroup controller, which is off at boot here, so they are inert. The trap worth knowing: `systemctl show` reports the cap as set even when the kernel isn't enforcing it, so confirm the controller is actually loaded with `cat /sys/fs/cgroup/cgroup.controllers` (it must list `memory`; if not, enable it in `/boot/firmware/cmdline.txt` and reboot). Until then, aggregate protection rests on `max_concurrent_runs × DuckDB memory_limit` (3 × 1.5 G = 4.5 G) staying within RAM; on a stock Raspberry Pi OS install the controller is on by default and `MemoryMax` takes over.

Prove the guardrail with the bundled workload; a run that overshoots both caps should fail while the UI stays responsive:

```bash
uv run dagster job launch -j synthetic_load_job -w workspace.yaml \
  --tags '{"dagster/partition":"part-000"}' \
  --config-json '{"ops":{"synthetic_load":{"config":{"rows":1,"mem_mb":7000}}}}'
```

> Controller On: OOM-killed inside the cgroup. Controller Off: The step raises MemoryError (see the run's logs).

Caps take effect only after `sudo systemctl daemon-reload && sudo systemctl restart dagster-code-server dagster-webserver dagster-daemon`.

### Why not eager-load the code server

A tempting next step is to eager-load user code in the gRPC server so run-workers inherit those imports copy-on-write. We measured first, and the premise is false: run-workers are `multiprocessing.spawn` children, not forks. Each starts a fresh interpreter and re-imports `dagster` plus user code into its own ~94 MB private heap, so eager-loading the parent only fattens its idle footprint for zero per-run benefit. (Forking would enable COW, but it risks deadlock in the multi-threaded gRPC server, which is exactly why Dagster spawns.)

Per-run cost is ~107 MB and unshareable. The only two levers that cut concurrent-backfill RAM: **fewer workers** (the 4 to 3 drop) and **keeping small recurring work off run-workers** (run a heartbeat or metrics scrape inline in the resident daemon via a sensor, not a fresh ~107 MB worker per tick). Full breakdown: [benchmarks/README.md](benchmarks/README.md#why-eager-loading-the-code-server-wont-help-run-workers-spawn-not-fork).

### glibc arena cap

The two `Environment=MALLOC_*` lines in the [code-server unit](#8-install-the-three-systemd-services), kept on the code-server only because its spawned run-workers inherit the env block (the measured payoff); a fresh-start A/B showed no reproducible benefit on the webserver or daemon. Verify it is live:

```bash
sudo systemctl daemon-reload && sudo systemctl restart dagster-code-server
systemctl show -p Environment dagster-code-server   # MALLOC_ARENA_MAX=2 should appear
```

### Keeping instance state bounded

Tick retention covers schedule/sensor ticks only. Run and event history and the per-run compute-log dirs are not auto-pruned in Dagster OSS, the slow killer of an always-on box. The opt-in [`maintenance_job`](src/dagster_pi/defs/logs/maintenance.py) plus `daily_maintenance` schedule deletes finished runs older than `retention_days` and their compute-log dirs. It ships safe twice over: the schedule is created **STOPPED**, and the op defaults to **`dry_run: true`** (it logs what it would delete). Enable it in **Deployment → Schedules** and set `dry_run: false` to arm it. Detail: [benchmarks/README.md](benchmarks/README.md#keeping-state-bounded-the-other-half-of-retention).

### Measuring it yourself

[`benchmarks/bench.py`](benchmarks/bench.py) is a dependency-free `/proc` harness, safe against the live service. Snapshot before and after a change; it reports per-process PSS tiered by parent PID (services / run-workers / step-workers), cold-import time, and on-disk state growth:

```bash
uv run python benchmarks/bench.py --label before
# apply a change, reload the code location, let it settle
uv run python benchmarks/bench.py --label after
```

Two probes catch what a snapshot can't: [`backfill_probe.py`](benchmarks/backfill_probe.py) drives and peak-samples a backfill (the source of the 378 MB delta), and [`cow_probe.py`](benchmarks/cow_probe.py) dumps each run-worker's shared-vs-private page split (which disproved eager-loading). Full numbers and method: [benchmarks/README.md](benchmarks/README.md).

> **The harness is optional scaffolding.** The `synthetic_load` workload lives in its own [`defs/benchmarking/`](src/dagster_pi/defs/benchmarking/) package and the probes in [`benchmarks/`](benchmarks/); delete both directories to drop the benchmarking apparatus entirely. Nothing in the deployment (the `duckdb` resource, maintenance, the services) depends on either.

## Development and publishing

Set up to be published as a template:

- **[LICENSE](LICENSE):** MIT.
- **Secret scanning:** [`.pre-commit-config.yaml`](.pre-commit-config.yaml) runs [gitleaks](https://github.com/gitleaks/gitleaks), ruff, and basic file hygiene on every commit. Install once with `uv run pre-commit install`. All config flows through systemd `Environment=` and `os.getenv` defaults, so there is no committed `.env` (it is gitignored).
- **CI:** [`.github/workflows/ci.yml`](.github/workflows/ci.yml) runs `dg check defs`, a `bench.py` smoke test, and a gitleaks scan on every push and PR.

Before committing: `uv run dg check defs`.
