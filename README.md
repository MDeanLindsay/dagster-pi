# Dagster on a Raspberry Pi 5

A self-hosted Dagster deployment that runs on a Raspberry Pi 5 as an always-on service. It is built for personal and small-team data work: scheduled ingestion, light transformation, and a real orchestration UI, all on hardware that costs less than a month of most cloud bills and sits on your own network.

This README is a from-scratch guide. It does not assume you have used Dagster before, and it does not depend on anything specific in this repo's own jobs. Follow it and you will have the same three-process service running on your own Pi, ready for you to add your own assets. It is also tuned for low-resource operation and ships a small benchmark harness to measure that tuning — see [Tuning for low resources](#tuning-for-low-resources-and-measuring-it).

## Architecture and why these choices

Dagster runs here as three cooperating processes:

- **Code location server** (gRPC, port 4000): imports your Python once and serves your asset and job definitions. Bound to loopback only.
- **Webserver** (UI + GraphQL, port 3000): the web UI you actually open. Bound to `0.0.0.0` so you can reach it from another machine.
- **Daemon**: runs schedules, sensors, and the run queue. Has no listening port.

Keeping these apart is what lets you reload your code in place (click **Reload** in the UI) without restarting the UI or losing daemon state, and it keeps a bug in your asset code from taking down the web server.

---

## First-time install (Pi to running service)

**Prerequisites:** 64-bit Raspberry Pi OS (Lite is fine). Run these as your normal user, not root. The examples use the account name `user` (paths like `/home/user/...` and `User=user` in the unit files). **If your Pi's username differs, replace `user` with your actual username** everywhere it appears below. You can complete this whole install on your local network; see [Remote access with Tailscale](#remote-access-with-tailscale) afterward to reach the UI from elsewhere.

### 1. Install `uv`

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
source ~/.bashrc          # or open a new shell, so `uv` is on PATH
uv --version
```

### 2. Create the project

```bash
cd ~
uvx create-dagster@latest project dagster_pi --uv-sync
cd dagster_pi
```

This creates `~/dagster_pi/` with a `.venv` and the layout described under [Adding projects, assets, and jobs with `dg`](#adding-projects-assets-and-jobs-with-dg). `--uv-sync` installs dependencies, including `dagster-dg-cli` as a dev dependency, already version-matched to core.

### 3. Promote the webserver to a runtime dependency

`create-dagster` installs `dagster` (which provides the daemon and `dagster code-server`) and puts `dagster-webserver` and `dagster-dg-cli` in the dev dependency group. The always-on service needs the webserver at runtime, so promote it:

```bash
cd ~/dagster_piz
uv add dagster-webserver            # promote to a dependency
uv remove --dev dagster-webserver   # drop the scaffold's dev-group copy

```

`uv` resolves this to a version aligned with the `dagster` that `create-dagster` just installed. There is no need to pin a minor version by hand; the tool keeps the family consistent. On 64-bit Pi OS this fetches prebuilt `aarch64` wheels rather than compiling.

### 4. Set up `DAGSTER_HOME`

```bash
mkdir -p ~/dagster_pi/.dagster_home
cat > ~/dagster_pi/.dagster_home/dagster.yaml <<'EOF'
# Cap concurrent *runs* (each run is its own process on a 4-core Pi).
run_queue:
  max_concurrent_runs: 4

# Don't phone home.
telemetry:
  enabled: false

# Auto-purge old schedule/sensor tick records so the SQLite stores stay bounded.
retention:
  schedule:
    purge_after_days: 90
  sensor:
    purge_after_days:
      skipped: 7
      failure: 30
      success: -1   # -1 = keep forever
EOF
```

`DAGSTER_HOME` holds instance state (SQLite run and event storage by default, which is fine for a single Pi). The three blocks above are deliberate low-resource choices — bounded run concurrency, telemetry off, and tick retention so instance state can't grow without limit ([Tuning for low resources](#tuning-for-low-resources-and-measuring-it) explains each). The file is optional; omit it for all-defaults. Here it lives **inside the project** as `.dagster_home/` (gitignored), which keeps the whole deployment in one directory. If you would rather keep instance state separate from code, point `DAGSTER_HOME` at a path outside the repo instead; nothing else changes.

### 5. Create the DuckDB data directory

DuckDB will not create intermediate directories itself, so create the folder before the first asset run:

```bash
mkdir -p ~/dagster-pi/.duckdb
```

This is where `pi.duckdb` and the spill directory (`.duckdb/.tmp`) will live. The folder is gitignored; override the path at any time with the `PI_DUCKDB_PATH` env var.

### 6. Create `workspace.yaml`

This tells the webserver and daemon where to find the gRPC code server:

```bash
cat > ~/dagster_pi/workspace.yaml <<'EOF'
load_from:
  - grpc_server:
      host: 127.0.0.1
      port: 4000
      location_name: dagster_pi
EOF
```

### 7. Verify the project loads

Do this before wiring up services:

```bash
cd ~/dagster_pi
uv run dg check defs
uv run dg list defs
```

A fresh project has no definitions yet, which is expected. `dg check defs` should pass without errors.

### 8. Install the three systemd services

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
ExecStart=/home/user/dagster-pi/.venv/bin/dagster code-server start -h 127.0.0.1 -p 4000 -m dagster_pi.definitions
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

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

[Install]
WantedBy=multi-user.target
EOF

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

On the same network, visit:

```
http://<pi-ip>:3000
```

The webserver binds `0.0.0.0:3000`, so it is reachable from any device that can route to the Pi. Keep port 3000 off the public internet: do not port-forward it on your router. For remote access, use Tailscale (next section).

### Useful checks

```bash
journalctl -u dagster-webserver -f        # live logs
sudo systemctl restart dagster-code-server   # after a dependency change
```

> **Order of operations:** the code server must be reachable for the webserver and daemon to load the location. The unit ordering above handles startup. If you see "connection refused" on first boot, the other two retry and recover on their own.

---

## Remote access with Tailscale

The webserver binds `0.0.0.0:3000`, which makes it reachable to anything that can route to the Pi. The safe way to use that from outside your home is a private network, not a router port-forward. Tailscale puts the Pi on a personal tailnet so only your own devices can reach it.

### Install and connect

```bash
curl -fsSL https://tailscale.com/install.sh | sh
sudo tailscale up        # follow the printed URL to authenticate
tailscale ip -4          # the Pi's tailnet address (100.x.y.z)
tailscale status         # devices on your tailnet
```

### Open the UI over the tailnet

From any device signed in to the same tailnet:

```
http://<pi-tailscale-name>:3000
```

You can also use the `100.x.y.z` address directly. Enabling MagicDNS in the Tailscale admin console lets `<pi-tailscale-name>` resolve without the IP.

### Lock it down

- Keep port 3000 unexposed on your LAN and router. Rely on Tailscale for access.
- Use tailnet ACLs to limit which devices can reach the Pi.
- For TLS, `tailscale serve https / http://localhost:3000` puts the UI behind an HTTPS endpoint on your tailnet without any extra certificates.

---

## Project Structure

This is the day-to-day workflow: how the project is laid out, and how you add new definitions with the `dg` CLI.

### The project layout

`create-dagster project` produces a standard `src`-layout Python package:

```
dagster_pi/
├── pyproject.toml          # dependencies + [tool.dg] config
├── workspace.yaml          # points the service at the gRPC code server
├── src/
│   └── dagster_pi/
│       ├── __init__.py
│       ├── definitions.py  # entry point (auto-loads everything in defs/)
│       └── defs/           # all your assets, sensors, schedules, resources
│           └── __init__.py
└── tests/
    └── __init__.py
```

### Scaffolding

`dg scaffold` puts files in the right place with boilerplate that is version-matched to core. Run `dg` from inside the project with **`uv run dg`** (it is installed as a project dev dependency). `uvx` is only for the one-time `create-dagster`.

```bash
cd ~/dagster_pi

# Note the .py extension: scaffolds a single file under defs/
uv run dg scaffold defs dagster.asset     ingestion/my_data.py
uv run dg scaffold defs dagster.sensor    sensors/my_sensor.py
uv run dg scaffold defs dagster.schedule  schedules/daily.py
uv run dg scaffold defs dagster.resources resources.py
```

Then implement the logic in the generated file under `src/dagster_pi/defs/`. There is no import or registration step; `definitions.py` picks it up. Validate and inspect:

```bash
uv run dg check defs   # static validation: run before every reload and commit
uv run dg list defs    # confirm it registered
```

`dg check defs` is your fast feedback loop. Run it before clicking **Reload** in the UI or committing.

### Updates

Because the deployment runs a code server (`dagster code-server start`, not a static `dagster api grpc`), it reloads code **without restarting the process**:

1. In the UI: **Deployment → Code locations → Reload**. This re-imports your code in place and is usually all you need.
2. Only if a reload does not pick things up (for example, you added an installed dependency), restart the service:
   ```bash
   sudo systemctl restart dagster-code-server
   sudo systemctl restart dagster-webserver
   sudo systemctl restart
   ```

---

## Benchmark Tuning

The defaults run fine, but a few deliberate changes make the deployment noticeably lighter on a Pi — and this repo *measures* the effect instead of asserting it. All four are already applied here:

| Lever | Where | What it buys you |
|---|---|---|
| `in_process_executor` | [src/dagster_pi/definitions.py](src/dagster_pi/definitions.py) | A run executes its steps in its own process instead of forking a fresh ~150 MB `import dagster` step-worker per step. Removes the biggest transient memory spike during a backfill, and the cold-import tax is paid once per run rather than per step. Runs still parallelize at the run level (bounded by `max_concurrent_runs`). |
| Telemetry off | [.dagster_home/dagster.yaml](.dagster_home/dagster.yaml) | No usage stats leave the box. |
| Tick retention | [.dagster_home/dagster.yaml](.dagster_home/dagster.yaml) | Schedule/sensor tick records auto-purge instead of accumulating forever. |
| DuckDB caps | `src/dagster_pi/defs/resources.py` | `memory_limit`, `threads`, and a spill `temp_directory` on the SSD, so one heavy query can't grab ~80% of RAM and every core and starve the webserver + daemon. All env-overridable for a one-off backfill. |

The executor is the single intentional edit to the generated `definitions.py` — a deployment-level default the `defs/` folder can't express:

```python
@dg.definitions
def defs() -> dg.Definitions:
    return dg.Definitions.merge(
        dg.load_from_defs_folder(path_within_project=Path(__file__).parent),
        dg.Definitions(executor=dg.in_process_executor),
    )
```

### Measuring it

`benchmarks/bench.py` is a dependency-free `/proc` harness — safe to run against the live service (it only reads `/proc` and file sizes). Snapshot before and after a change; it appends a JSON record to `benchmarks/results.jsonl` and prints a markdown summary:

```bash
uv run python benchmarks/bench.py --label before
# ...apply a change, restart services, let it settle...
uv run python benchmarks/bench.py --label after
# run it DURING a backfill to catch the step-worker delta the executor removes
```

It reports the live process footprint (RSS by role), cold-import time, and on-disk state growth. Generate your own idle baseline with the command above; this repo ships no committed numbers until they're re-measured on the current instance.

Be honest about what moves: idle RSS barely changes (none of these touch the three resident processes) — the wins are the during-backfill step-workers the executor eliminates, the per-run (not per-step) startup tax, and the DuckDB cap as an OOM guardrail. See [benchmarks/README.md](benchmarks/README.md) for the full method, the per-lever rationale, and how to prune run/event history (the one growth vector tick retention doesn't cover).