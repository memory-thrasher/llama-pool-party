# ops/ - pool deployment operations

The persist-valuable operations artifacts for the pool deployment (the
group-facing copies; the local test environment keeps its working copies
on the reference machine).

| file | what it is |
|---|---|
| `bring-up.sh` | single cold start: models + pool router + coordinator (REQ-3). Thin wrapper over `run-demo.sh` (the local machinery: scratch dir, symlinks into the read-only models dir, no-download guard, tag mapping, smoke) - `run-demo.sh` itself stays in the local test environment |
| `OPERATIONS.md` | operations runbook: localhost TLS exception, swap/restart procedure, incident records (including the 2026-11-10/11 SSE flush root cause), the pool-link startup gate (D-K5), known weaknesses |
| `fleet-preset.ini` | fleet profile captured live from production 2026-11-08 (record) |
| `swap-fleet.ini` | fleet preset: 27B + gemma (pool-text-md, two members) + Qwen-Drive-4B (pool-image-text-xs), tuned params, class mapping |
| `poolstat.py` | read-only pool state CLI (routers, classes, records via psql) |

Not in this directory (local test environment, machine-specific):
`run-demo.sh`, `swap-to-demo.sh`, `restart-pool.sh`,
`rollback-standalone.sh`, `RESTORE-8000.md`.
