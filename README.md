# Sirb (سرب)

**Lightweight, zero-framework-dependency task orchestration engine.**

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

# Configure
echo 'workers: [shipcrawler_worker]' > sirb.yml

# Run
sirb run
```

## Architecture

```
                ┌──────────────────────┐
  Config ──────→│    WorkerRegistry     │──→ Register workers
    (YAML)      │ discover(config)      │    (SirbWorker subclasses)
                └──────────┬───────────┘
                           │
  discover()  ┌────────────▼───────────┐
  ───────────→│       TaskQueue        │──→ PENDING → CLAIMED → RUNNING
  (workers)   │ thread-safe state      │──→ COMPLETED / FAILED
              │ priority + deps + retry│
              └────────────┬───────────┘
                           │ claims
              ┌────────────▼───────────┐
              │     WorkerPool         │──→ ThreadPoolExecutor × N
              │ TokenBucket throttling │──→ enforces rate_limits()
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
              │   CorrelationEngine    │──→ group_by_detail_key()
              │   Aggregator           │──→ render_markdown()
              └────────────────────────┘
```

## Install

```bash
pip install git+https://github.com/ahmdngi/sirb.git
```

No dependencies beyond Python stdlib.

## Commands

```bash
sirb run                        # Execute all tasks in the queue
sirb run --tasks vessels.json   # Load tasks from JSON file
sirb run --resume run-12345     # Resume from last checkpoint
sirb run --cron "0 */6 * * *"  # Install system cron job
sirb list-workers               # Show all discovered workers
sirb init                       # Scaffold a new worker skeleton
```

## Configuration

See `sirb.yml` for all options:

```yaml
workers:
  - my_worker          # simple module name
  my_worker:           # with per-worker config
    option: value

max_workers: 10
task_timeout: 300
checkpoint_interval: 5
blackboard:
  decay_rate: 0.9
triggers:
  - predicate: {severity: critical}
    action: alert
output_dir: ~/hermes-vault/sirb-reports/
```

## Write a Worker

```python
from sirb.core import SirbWorker, Task, Result, Finding

class MyWorker(SirbWorker):
    name = "my-worker"
    description = "Scans things"

    async def execute(self, task: Task) -> Result:
        # Your logic here
        return Result(
            task_id=task.id, worker=self.name, status="success",
            findings=[
                Finding(target_id="...", finding_type="...",
                        severity="info", detail={}),
            ],
        )
```

Package separately and register in `sirb.yml`.

## Components

| Module | Description |
|--------|-------------|
| `sirb.core.models` | `Task`, `Result`, `Finding`, `TaskStatus` — fully agnostic |
| `sirb.core.worker_base` | `SirbWorker` ABC — `execute()`, `discover()`, `validate()`, `rate_limits()` |
| `sirb.core.task_queue` | Thread-safe priority queue with deps, retry, serialization |
| `sirb.core.registry` | Worker discovery: config + auto-detect |
| `sirb.core.router` | `Task.worker` → `SirbWorker` dispatch |
| `sirb.core.worker_pool` | `ThreadPoolExecutor` + token-bucket throttling |
| `sirb.core.blackboard` | Shared findings store with pheromone decay and triggers |
| `sirb.core.correlation` | Generic cross-finding grouping |
| `sirb.core.aggregator` | Markdown assessment generator |
| `sirb.core.throttle` | `TokenBucket` rate limiter per resource |
| `sirb.core.persistence` | JSON checkpoint/resume |
| `sirb.cli` | `sirb run`, `list-workers`, `init` |

## Roadmap

| Phase | Status | Description |
|-------|--------|-------------|
| **0 — Kernel** | ✅ | Core: models, queue, routing, pool, blackboard, persistence, CLI, 73 tests |
| **1 — ShipCrawler** | ✅ | First worker in own repo: [shipcrawler-worker](https://github.com/ahmdngi/shipcrawler-worker) |
| **2 — Intelligence** | ✅ | Correlation engine, aggregator, trigger predicates, port discovery |
| **3 — Production** | ⬜ | Rate limiting, cron mode, checkpoint recovery |
| **4 — More Workers** | ⬜ | Personnel OSINT, port authority — contributed by you |

## License

MIT — Ahmed Nagi Nasr / TalTech EMA.
