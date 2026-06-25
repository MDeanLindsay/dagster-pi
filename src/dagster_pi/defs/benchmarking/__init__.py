"""Self-contained benchmarking workload.

Groups the parameterized `synthetic_load` asset (and its job) that exercises the
deployment so the README's tuning claims stay reproducible. Kept in its own
package to separate the benchmark machinery from real assets; it is still
auto-discovered by `load_from_defs_folder` like everything else under `defs/`.

It depends on the shared `duckdb` resource and `spill_watch` from
`dagster_pi.defs.resources`, which stay at the `defs/` root as deployment-level
tuning levers rather than benchmark-only details.
"""
