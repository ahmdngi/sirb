<span>سرب</span> Sirb
=====

**Agnostic multi-agent task swarm — run N workers in parallel, coordinated by a shared queue and blackboard.**

Sirb (Arabic: سرب, "swarm / flock") is a lightweight, zero-framework-dependency task orchestration engine. It manages N worker agents executing tasks concurrently from a thread-safe queue, routes them by type to registered workers, persists findings on a shared blackboard, and checkpoints state to disk for crash recovery.

Designed for maritime OSINT at scale — port scanning, fleet profiling, personnel intelligence — but the worker interface is deliberately agnostic. Any task that can be expressed as `(input → output)` can be a Sirb worker.

---

## Architecture

```
Discoverer(s) → TaskQueue → Router → ThreadPoolExecutor × N → Blackboard → Aggregator(s)
```

| Component | Role |
|-----------|------|
| **SirbWorker** | Abstract base — implement `execute(task)` and optionally `discover()`, `validate()`, `rate_limits()` |
| **TaskQueue** | Thread-safe priority queue with version-based optimistic concurrency, dependency tracking, and automatic retry |
| **Router** | Dispatches `Task.worker` → matching `SirbWorker` by name |
| **WorkerPool** | `ThreadPoolExecutor` managing the claim → execute → complete lifecycle with per-task timeout |
| **Blackboard** | Shared findings store with pheromone decay and trigger predicates |
| **Checkpointer** | JSON checkpoint/resume for queue + blackboard |
| **WorkerRegistry** | Auto-discovers workers from config + `sirb/workers/*_worker.py` |

## Quick Start

```bash
# Install
git clone https://github.com/ahmdngi/sirb
cd sirb
pip install -e .

# List discovered workers
sirb list-workers

# Generate a worker skeleton
sirb init

# Run with injected tasks
sirb run --workers my_worker --tasks tasks.json
```

## Write a Worker

Workers are the only thing you need to write. Everything else — queue, routing, pooling, persistence — is built in.

```python
# my_worker.py
from sirb.core import SirbWorker, Task, Result, Finding

class MyWorker(SirbWorker):
    name = "my-worker"
    description = "Does something useful"

    async def execute(self, task: Task) -> Result:
        """Execute one task. Required method."""
        return Result(
            task_id=task.id,
            worker=self.name,
            status="success",
            findings=[
                Finding(
                    target_id=task.params.get("target_id"),
                    target_type="example",
                    finding_type="info",
                    severity="info",
                    detail={"processed": True},
                ),
            ],
            artifacts=["/path/to/report.md"],
        )

    async def discover(self) -> list[Task]:
        """Optional: auto-discover targets."""
        return [
            Task(type="example", worker=self.name, params={"target_id": "001"}),
        ]

    def rate_limits(self) -> dict:
        """Optional: declare rate limits per resource."""
        return {"some_api": 30}  # 30 calls/minute
```

Drop `*_worker.py` into `sirb/workers/` or reference it in `sirb.yml`:

```yaml
workers:
  - my_worker      # auto-discovered from sirb/workers/
  # - path.to.module  # fully qualified Python import
```

## Run a Swarm

```bash
# Discover tasks from workers and execute
sirb run --workers my_worker

# Inject tasks from a JSON file
sirb run --workers my_worker --tasks targets.json

# Control concurrency
sirb run --workers my_worker --tasks targets.json --max-workers 20 --task-timeout 600

# Resume from a checkpoint
sirb run --workers my_worker --resume sirb-1747440000
```

## Task JSON Format

```json
[
  {
    "id": "vessel-001",
    "type": "vessel_osint",
    "worker": "shipcrawler",
    "params": {"mmsi": "273342890", "imo": "9122552"},
    "priority": 0,
    "depends_on": []
  }
]
```

## Data Model

```
Task         → A unit of work for a SirbWorker
  .id          Auto-generated or explicit
  .type        Task category ("vessel_osint", "personnel", etc.)
  .worker      Routes to SirbWorker.name
  .params      Worker-specific input dict
  .priority    0 = highest
  .depends_on  Task IDs that must complete first

Result       → Output from a single task execution
  .status      "success" | "partial" | "failure"
  .findings    List of structured Finding objects
  .artifacts   Paths to generated report files
  .raw         Full worker output dict

Finding      → A structured observation written to the blackboard
  .target_id   MMSI, ORCID, domain — worker-specific
  .target_type "vessel" | "person" | "port"
  .finding_type "shadow_fleet" | "exposed_vsat"
  .severity    "critical" | "high" | "medium" | "low" | "info"
  .weight      Pheromone weight (0.0–1.0)
```

## State Machine

```
PENDING → CLAIMED → RUNNING → COMPLETED
                              → FAILED (retries exhausted)
                              → CANCELLED
```

- **Optimistic concurrency:** every state transition checks a version field. Stale callers are rejected.
- **Retry:** tasks reset to PENDING on failure (up to `max_retries`).
- **Dependency ordering:** a task won't run until all `depends_on` IDs are COMPLETED.

## Output Structure

```
~/hermes-vault/sirb-reports/runs/{run_id}/
├── task_queue.json       # Final queue state (checkpointed)
├── blackboard.json       # All findings (checkpointed)
└── reports/              # Per-worker artifact directory
    └── ...               # Worker-generated files
```

## Phases

| Phase | Status | Delivers |
|-------|--------|----------|
| **0 — Sirb Kernel** | ✅ Done | TaskQueue, WorkerPool, SirbWorker ABC, Router, Registry, Blackboard, Checkpointer, CLI |
| **1 — ShipCrawler Worker** | ⬜ Next | First concrete worker — vessel OSINT via ShipCrawler |
| **2 — Intelligence** | ⬜ | Cross-vessel correlation, pheromone triggers, trend analysis |
| **3 — Additional Workers** | ⬜ | Personnel OSINT, port authority, continuous scanning |

## License

MIT — Ahmed Nagi Nasr / TalTech EMA.

---

*Sirb — سرب. One agent is a tool. A swarm is a platform.*
