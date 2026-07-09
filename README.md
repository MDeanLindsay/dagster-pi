# Dagster on a Raspberry Pi 5

Self-hosted Dagster as an always-on service on a Pi 5: scheduled ingestion, light transforms, and a real orchestration UI on hardware that costs less than a month of most cloud bills.

A from-scratch guide — no prior Dagster needed. Follow it end to end for a three-process service tuned for low-resource operation, with a benchmark behind every tuning claim ([Tuning](#tuning-for-low-resources)).

- Three-process split (code-server / webserver / daemon) for in-place code reloads and crash isolation
- DuckDB storage with memory and thread caps plus NVMe spill
- systemd cgroup isolation so a runaway asset can't take down the box
- A `/proc`-based benchmark harness, safe to run against the live service

**Run from NVMe or SSD, not a microSD.** This service writes constantly (SQLite instance state, DuckDB); SD cards are slow under random writes and wear out. A Pi 5 with an NVMe HAT is ideal.


## Clean Pi OS Install

One-time host changes, none Dagster-specific. The config edits take effect only on reboot, so make them all, then **reboot once at the end**.

**Update the OS.** Current packages and security fixes, before anything else:

```bash
sudo apt update && sudo apt full-upgrade -y
```

**Uncap the PCIe link.** The NVMe slot defaults to Gen 2; forcing Gen 3 gives full bandwidth for DuckDB spill. Officially unsupported on the Pi 5 but stable on most drives:

```bash
echo 'dtparam=pciex1_gen=3' | sudo tee -a /boot/firmware/config.txt
```

**Update the bootloader EEPROM** (separate from `apt`) for current NVMe-boot support. Staged now, applied on the reboot at the end:

```bash
sudo rpi-eeprom-update -a
```

**Enable the memory cgroup controller.** Pi OS often leaves it off, silently neutering the `MemoryMax`/`MemoryHigh` caps from [step 8](#8-install-the-three-systemd-services) — `systemctl show` still reports them set. Check whether it's already on:

```bash
cat /sys/fs/cgroup/cgroup.controllers      # if this lists 'memory', skip the next command
```

If `memory` is absent, append both parameters to the single line in `/boot/firmware/cmdline.txt` — keep it one line, never add a newline. The `grep` guard makes re-runs idempotent:

```bash
grep -q cgroup_enable=memory /boot/firmware/cmdline.txt || \
  sudo sed -i 's/$/ cgroup_enable=memory cgroup_memory=1/' /boot/firmware/cmdline.txt
```

**Disable Bluetooth.** Unused on a headless Pi; frees a little RAM and one background service:

```bash
echo 'dtoverlay=disable-bt' | sudo tee -a /boot/firmware/config.txt
sudo systemctl disable --now hciuart bluetooth
```

**Reboot once, then verify.** A single reboot applies the PCIe link, the staged EEPROM update, and the cgroup controller:

```bash
sudo reboot
# after it comes back:
cat /sys/fs/cgroup/cgroup.controllers      # now lists 'memory'
```

## Install (Pi to running service)

**Prerequisites:** 64-bit Raspberry Pi OS (Lite is fine), run as your normal user, not root. Examples use the account `user` — replace `user` and `/home/user/...` if yours differs. Works entirely on your LAN; add [Tailscale](#remote-access-with-tailscale) for remote access.

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

### 3. Add project dependencies

The scaffold put `dagster-webserver` in the **dev** group (the Dagster+ cloud assumption). Promote it to a runtime dependency and add DuckDB:

```bash
uv add "dagster-webserver==1.13.*" "dagster-duckdb>=0.29.9" "duckdb>=1.5.3"
uv remove --dev dagster-webserver
```

| Package | Provides |
|---|---|
| `dagster-webserver` | UI + GraphQL. Pulls in `dagster` core, which ships the daemon and code-server CLIs — so all three services come from this one line. |
| `dagster-duckdb` | Dagster's resource / I/O-manager layer for DuckDB. |
| `duckdb` | The engine itself. |

`uv` resolves versions against the installed `dagster` and pulls prebuilt `aarch64` wheels — no compiling.

### 4. Set up `DAGSTER_HOME`

Instance state — runs and events (SQLite) — kept in-project at `.dagster_home/` (point `DAGSTER_HOME` elsewhere to separate state from code). The config is optional; these blocks are low-resource choices, each explained in [Tuning](#tuning-for-low-resources):

```bash
mkdir -p ~/dagster-pi/.dagster_home
cat > ~/dagster-pi/.dagster_home/dagster.yaml <<'EOF'
# Cap concurrent runs (each is its own process on 4 cores), and serialize
# anything in the "duckdb" pool: DuckDB allows one read-write process per
# file, so runs that touch it wait in the queue instead of colliding over
# the file lock. `granularity: run` holds them *before* dequeue, where a
# waiting run costs zero RAM.
concurrency:
  runs:
    max_concurrent_runs: 3
  pools:
    granularity: run
    default_limit: 1

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

DuckDB won't create parent directories, so make it before the first asset run. Holds `pi.duckdb` and the spill dir (`.duckdb/.tmp`); gitignored; override the path with `PI_DUCKDB_PATH`.

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

One unit per process, each in its own cgroup. What the caps do and which actually bite on a Pi: [Resource isolation](#resource-isolation). Install as-is, tune later.

> **systemd has no inline comments** — a `#` must start its own line or it folds into the value (`MemoryMax=5G  # note` breaks). Mind that if you edit these.

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

Reachable from any device that can route to the Pi. Keep port 3000 off the public internet — don't port-forward it; use Tailscale for remote access.

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
| `duckdb` pool, limit 1 | [dagster.yaml](.dagster_home/dagster.yaml) + `pool=` in [resources.py](src/dagster_pi/defs/resources.py) | Serializes every DuckDB-touching run at the queue — no file-lock collisions, and a waiting run costs 0 RAM instead of a live worker spinning in connect retries. Details: [DuckDB pool](#duckdb-pool). | guardrail |
| DuckDB caps | [resources.py](src/dagster_pi/defs/resources.py) | `memory_limit=3GB`, `threads=4`, NVMe spill. One query can't grab ~80% of RAM; sized for the single live instance the pool guarantees, under the cgroup's `MemoryHigh=4G`. | guardrail |
| glibc arena cap | [code-server unit](#8-install-the-three-systemd-services) | `MALLOC_ARENA_MAX=2` curbs malloc-arena fragmentation; run-workers inherit it. | ~7%, code-server tier |
| cgroup isolation | systemd units | Per-process caps so a runaway asset OOMs in its own cgroup, not the box. | see [below](#resource-isolation) |
| Telemetry off | [dagster.yaml](.dagster_home/dagster.yaml) | No usage stats leave the box. | n/a |
| Tick retention | [dagster.yaml](.dagster_home/dagster.yaml) | Schedule/sensor ticks auto-purge instead of growing forever. | n/a |


### What moves, and what doesn't

- **Idle is unchanged.** At rest there are no workers, so the executor and concurrency levers do nothing; the three resident services sit at ~485 MB PSS total. The one idle effect is the glibc cap (~7%), kept only on the code-server, where run-worker propagation is proven.

- **The 378 MB saving is during a backfill.** It is the step-worker tier the executor removes, and it scales with `max_concurrent_runs × steps-per-run`.

### DuckDB pool

DuckDB permits **one read-write process per database file**. The `in_process_executor` serializes steps *within* a run, but `max_concurrent_runs: 3` still lets three runs each open `pi.duckdb` read-write. `dagster-duckdb` papers over the collision with a connect-retry loop (10 attempts, ~100 s of exponential backoff), so overlapping runs appear to work — until one writer holds the lock longer than the backoff window and the colliding run fails. Meanwhile each retrying run is a live ~107 MB worker doing nothing.

The fix is declarative, in two halves:

- Every asset that opens the `duckdb` resource declares `pool=DUCKDB_POOL` ([resources.py](src/dagster_pi/defs/resources.py)).
- `dagster.yaml` gives pools `default_limit: 1` with `granularity: run`, so the run coordinator holds a second DuckDB run **in the queue** — where it costs zero RAM — until the first finishes.

Runs that don't touch DuckDB are unaffected and still parallelize up to `max_concurrent_runs`.

### DuckDB caps

The caps are a guardrail: a heavy or buggy query can't grab most of RAM and every core and OOM the box. The memory cap is a spill threshold, not a wall — past `memory_limit` DuckDB spills to the NVMe temp dir and finishes slower, not failed.

- **Raise the cap for a known-heavy one-off** (all three settings are env-overridable). Just keep the cap under the code-server cgroup's `MemoryHigh=4G` minus ~300 MB of process overhead:

  ```bash
  PI_DUCKDB_MEMORY_LIMIT=3.5GB \
    uv run dagster job launch -j synthetic_load_job -w workspace.yaml
  ```
- **Spilling is invisible unless you measure it.** The `spill_watch` helper ([resources.py](src/dagster_pi/defs/resources.py)) samples the spill dir and logs the peak (and a `peak_spill_mb` metadata field) only when a run actually spilled. The caps bound DuckDB memory only; Python-side memory (e.g. `fetchall` into a giant list) is bounded by `LimitAS`, so stream results rather than materialize them.

### glibc arena cap

The two `Environment=MALLOC_*` lines in the [code-server unit](#8-install-the-three-systemd-services), kept on the code-server only because its spawned run-workers inherit the env block.

### Resource isolation

The `[Service]` caps in [step 8](#8-install-the-three-systemd-services) put each process in its own cgroup so a runaway asset dies without taking the UI or `sshd` with it. Whether each directive is actually enforced on a Pi 5:

| Directive | Role | Enforced here? |
|---|---|---|
| `CPUWeight` / `Nice` | Proportional CPU and scheduler priority; UI (800 / -5) stays ahead of a heavy backfill, code-server (100 / 10) runs at the back. No controller needed. | **Yes** |
| `LimitAS` | Per-process address-space rlimit; a run ballooning past 6 G gets a clean `MemoryError`. No controller needed. | **Yes** (a `mem_mb=7000` run died in 63 ms, UI stayed up) |
| `MemoryHigh` / `MemoryMax` | Throttle or OOM-kill a runaway run inside the code-server cgroup. Needs the kernel memory cgroup controller. | **Yes** (controller enabled in [Clean Pi OS Install](#clean-pi-os-install); `memory.max` reads 5 G, not `max`) |
| `IOWeight` | Proportional disk I/O, only with a `bfq`-scheduled device. NVMe's default `none` scheduler exposes no `io.weight`. | **No** (harmless on NVMe) |

If the controller is off, `MemoryHigh`/`MemoryMax` go inert and protection falls back to the `duckdb` pool times the DuckDB `memory_limit` (one writer × 3 G), still within RAM.

Caps take effect only after `sudo systemctl daemon-reload && sudo systemctl restart dagster-code-server dagster-webserver dagster-daemon`.

### Keeping instance state bounded

Tick retention covers schedule/sensor ticks only. Run and event history and the per-run compute-log dirs are not auto-pruned in Dagster OSS, the slow killer of an always-on box. The opt-in [`maintenance_job`](src/dagster_pi/defs/logs/maintenance.py) plus `daily_maintenance` schedule deletes finished runs older than `retention_days` and their compute-log dirs. It ships safe twice over: the schedule is created **STOPPED**, and the op defaults to **`dry_run: true`** (it logs what it would delete). Enable it in **Deployment → Schedules** and set `dry_run: false` to arm it. Detail: [benchmarks/README.md](benchmarks/README.md#keeping-state-bounded-the-other-half-of-retention).

### Measuring it yourself

[`benchmarks/bench.py`](benchmarks/bench.py) is a dependency-free `/proc` harness, safe against the live service. Snapshot before and after a change; it reports per-process PSS tiered by parent PID (services / run-workers / step-workers), cold-import time, and on-disk state growth:

```bash
uv run python benchmarks/bench.py --label before
# apply a change, reload the code location, let it settle
uv run python benchmarks/bench.py --label after
```

Numbers that only exist *during* load (the 378 MB backfill delta, the per-run-worker cost) come from sampling `bench.py` in a loop while a backfill runs. Full numbers and method: [benchmarks/README.md](benchmarks/README.md).

> **The harness is optional scaffolding.** The `synthetic_load` workload lives in its own [`defs/benchmarking/`](src/dagster_pi/defs/benchmarking/) package and the harness in [`benchmarks/`](benchmarks/); delete both directories to drop the benchmarking apparatus entirely. Nothing in the deployment (the `duckdb` resource, maintenance, the services) depends on either.

## Development and publishing

Set up to be published as a template:

- **[LICENSE](LICENSE):** MIT.
- **Secret scanning:** [`.pre-commit-config.yaml`](.pre-commit-config.yaml) runs [gitleaks](https://github.com/gitleaks/gitleaks), ruff, and basic file hygiene on every commit. Install once with `uv run pre-commit install`. All config flows through systemd `Environment=` and `os.getenv` defaults, so there is no committed `.env` (it is gitignored).
- **CI:** [`.github/workflows/ci.yml`](.github/workflows/ci.yml) runs `dg check defs`, a `bench.py` smoke test, and a gitleaks scan on every push and PR.