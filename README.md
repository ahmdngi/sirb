# Sirb (سرب)

**v0.1.0** — Lightweight, zero-framework-dependency task orchestration engine.

Manages N worker agents executing tasks concurrently from a thread-safe queue,
routes them by type to registered workers, persists findings on a shared
blackboard with pheromone decay, and checkpoints state to disk for crash recovery.

> **Agnostic by design.** Sirb does not know what a vessel, person, or domain is.
> Workers (`SirbWorker` subclasses) bring domain logic — ShipCrawler, personnel
> OSINT, port authority scrapers — each in their own installable package.

## Quick Start

```bash
pip install git+https://github.com/ahmdngi/sirb.git

# Install a worker
pip install git+https://github.com/ahmdngi/shipcrawler-worker.git

# Run (auto-discovers workers via entry points)
sirb run
```

## Features

### Core Engine
| Feature | Description |
|---------|-------------|
| **Task Queue** | Thread-safe priority queue with state machine (PENDING → CLAIMED → RUNNING → COMPLETED/FAILED). Version-based optimistic concurrency — no locks beyond short `threading.Lock`. |
| **Worker Pool** | Configurable `ThreadPoolExecutor`-based pool. Claims tasks from queue, routes to the correct `SirbWorker.execute()`, handles results. |
| **Router** | Maps `Task.worker` field to a `SirbWorker` by name. Exact match dispatch — no glob, no magic. |
| **Blackboard** | Shared findings store with add/query/decay. Findings have a `weight` that decays over time (pheromone pattern). Stale findings expire naturally. |
| **Aggregator** | Generic assessment generator. Groups findings, counts by severity/type, renders markdown reports. Fully agnostic — works on any target type. |
| **Correlation Engine** | Cross-finding analysis: `group_by_detail_key()`, `group_by_field()`, `risk_tiers()`, `unique_targets()`. No domain-specific logic. |

### Worker Discovery
| Feature | Description |
|---------|-------------|
| **Entry-Point Discovery** | Any pip-installed package with `[project.entry-points.sirb_workers]` is auto-discovered. No manual config required. |
| **Config-Based** | Declare workers in `sirb.yml` — as a simple list or with per-worker config dicts. |
| **Package Auto-Discover** | Scans `sirb.workers.*_worker` modules for `SirbWorker` subclasses. |
| **Filesystem Scan** | Scans custom directories for `*_worker.py` files. |

### Production Hardening
| Feature | Description |
|---------|-------------|
| **Rate Limiting** | Token-bucket throttling per resource. Workers declare `rate_limits()`, Sirb enforces them via `TokenBucketPool`. Per-resource, per-worker buckets. |
| **Task Deduplication** | Same `(worker, params)` content is rejected while the original is still active. Uses SHA256 content hash. Solved by `complete`/`fail`/`cancel` lifecycle. |
| **Health Checks** | WorkerPool tracks consecutive failures per worker. Warns when a worker hits the max-failures threshold. Configurable via `--max-failures`. |
| **Checkpoint / Resume** | JSON checkpoint every N tasks. Resume from any previous run — queue state and blackboard are preserved. |
| **Cron Mode** | `--cron "0 */6 * * *"` installs a system crontab entry for periodic runs. |
| **Webhook Output** | POST assessment JSON to a configured URL on run completion. Configurable via `--webhook` CLI arg or `webhook:` in `sirb.yml`. |

### Intelligence & Reporting
| Feature | Description |
|---------|-------------|
| **Multi-Run Trends** | Persists assessment summaries per run. Compares latest run against previous — shows severity deltas, finding type changes, new targets. Output as markdown. |
| **Trigger Predicates** | Register predicates on the blackboard. When a finding matches, an action fires (e.g., `alert_aggregator`). Predicates match on any finding field. |
| **Assessment Markdown** | Structured report with unique targets, severity distribution, finding types, risk tiers, shared sources, top findings. |
| **Live SSE Dashboard** | `sirb dashboard` starts an HTTP server at `localhost:8100` with a live SSE feed. Shows real-time task progress by polling checkpoint files. Dark-themed HTML UI. |

### Agnosticism (guaranteed)
| Commitment | Evidence |
|------------|----------|
| **No domain-specific code in core/** | Audit clean. Zero references to vessel, MMSI, IMO, Shodan, Equasis, VSAT, shadow fleet, LinkedIn, personnel. |
| **No domain-specific code in cli/** | Audit clean. CLI has no domain knowledge. |
| **Workers are external** | Sirb never imports a worker's Python modules. Workers communicate via subprocess or HTTP. |
| **Generic data model** | `Task`, `Result`, `Finding` — no vessel/domain fields. `target_id` and `target_type` are free-form strings. |

## Architecture

```
                ┌──────────────────────┐
  Config ──────→│    WorkerRegistry     │──→ Register workers (entry points,
    (YAML)      │ discover(config)      │    config, package scan)
                └──────────┬───────────┘
                           │
  discover()  ┌────────────▼───────────┐
  ───────────→│       TaskQueue        │──→ PENDING → CLAIMED → RUNNING
  (workers)   │ thread-safe, dedup     │──→ COMPLETED / FAILED
              │ priority + deps + retry│
              └────────────┬───────────┘
                           │ claims
              ┌────────────▼───────────┐
              │     WorkerPool         │──→ ThreadPoolExecutor × N
              │ TokenBucket throttling │──→ enforces rate_limits()
              │ health checks + staled │──→ max_failures tracking
              ├─── on_complete ────────┤
              │   findings, checkpoint │
              └────────────┬───────────┘
                           │
              ┌────────────▼───────────┐
              │      Blackboard        │──→ Finding store + triggers
              │  pheromone decay (t)   │──→ auto-decay stale findings
              │  trigger predicates    │──→ predicate → action
              └────────────┬───────────┘
                           │
              ┌────────────▼───────────┐
              │   TrendTracker         │──→ save/load summaries per run
              │   CorrelationEngine    │──→ group_by_detail_key()
              │   Aggregator           │──→ render_markdown()
              │   Webhook POST         │──→ assessment JSON → URL
              │   SSE Dashboard        │──→ live progress at :8100
              └────────────────────────┘
```

## Install

```bash
pip install git+https://github.com/ahmdngi/sirb.git
```

No dependencies beyond Python stdlib (3.11+).

## Commands

```bash
sirb run                        # Execute all tasks in the queue
sirb run --tasks vessels.json   # Load tasks from JSON file
sirb run --resume run-12345     # Resume from last checkpoint
sirb run --cron "0 */6 * * *"  # Install system cron job
sirb run --webhook URL          # POST assessment JSON on completion
sirb run --max-failures 5       # Tolerate 5 consecutive failures per worker
sirb list-workers               # Show all discovered workers
sirb init                       # Scaffold a new worker skeleton
sirb dashboard                  # Start live SSE dashboard (port 8100)
sirb dashboard --port 9000      # Custom port
sirb dashboard --run-id abc123  # Watch a specific previous run
```

## Configuration

```yaml
# sirb.yml
workers:
  - shipcrawler_worker    # simple list — pip-installed package
  my_worker:              # or dict with per-worker config
    option: value

max_workers: 10
task_timeout: 300
checkpoint_interval: 5

blackboard:
  decay_rate: 0.9
  trigger_check_interval: 60

triggers:
  - predicate: {severity: critical}
    action: alert

webhook: https://hooks.example.com/sirb

output_dir: ~/hermes-vault/sirb-reports/
```

## Write a Worker

```python
from sirb.core import SirbWorker, Task, Result, Finding

class MyWorker(SirbWorker):
    name = "my-worker"
    description = "Scans things"

    async def execute(self, task: Task) -> Result:
        return Result(
            task_id=task.id, worker=self.name, status="success",
            findings=[
                Finding(target_id="...", finding_type="scan",
                        severity="info", detail={}),
            ],
        )

    def rate_limits(self) -> dict[str, int]:
        return {"api-source": 5}   # 5 req/min
```

Package separately and pip-install. Sirb will auto-discover it via entry points.

## Components

| Module | Description |
|--------|-------------|
| `sirb.core.models` | `Task`, `Result`, `Finding`, `TaskStatus` — fully agnostic |
| `sirb.core.worker_base` | `SirbWorker` ABC — `execute()`, `discover()`, `validate()`, `rate_limits()` |
| `sirb.core.task_queue` | Thread-safe priority queue with deps, retry, dedup, serialization |
| `sirb.core.registry` | Worker discovery: entry points, config, package scan, filesystem |
| `sirb.core.router` | `Task.worker` → `SirbWorker` dispatch |
| `sirb.core.worker_pool` | `ThreadPoolExecutor` + token-bucket throttling + health checks |
| `sirb.core.blackboard` | Shared findings store with pheromone decay and triggers |
| `sirb.core.correlation` | Generic cross-finding grouping |
| `sirb.core.aggregator` | Markdown assessment generator |
| `sirb.core.trends` | Multi-run trend tracking (delta comparison) |
| `sirb.core.throttle` | `TokenBucket` rate limiter per resource |
| `sirb.core.persistence` | JSON checkpoint/resume |
| `sirb.cli` | `sirb run`, `list-workers`, `init`, `dashboard` |

## Tests

```
src/sirb/ ── 70 tests (core + queue + dedup + throttling + triggers +
             correlation + aggregator + health + webhook + trends)
```

## Workers

| Worker | Repo | Description |
|--------|------|-------------|
| ShipCrawler | [shipcrawler-worker](https://github.com/ahmdngi/shipcrawler-worker) | Vessel OSINT via Equasis + AIS + Shodan/Web pipeline |

## License

MIT — Ahmed Nagi Nasr / TalTech EMA.
