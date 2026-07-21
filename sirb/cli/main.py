"""Sirb CLI — run, list-workers, init."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
import threading
import time
import urllib.parse
import uuid
from datetime import datetime, timezone
from pathlib import Path

try:
    from jinja2 import Environment, FileSystemLoader
    _JINJA_AVAILABLE = True
except ImportError:
    _JINJA_AVAILABLE = False

from sirb.core import (
    Task, Finding, TaskQueue, WorkerRegistry, Router,
    WorkerPool, Checkpointer, Blackboard, TokenBucketPool,
)
from sirb.core.worker_base import SirbWorker


_SIRB_VERSION = "0.2.0"
_TEMPLATES_DIR = Path(__file__).parent / "templates"


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="sirb",
        description="Sirb (سرب) — agnostic multi-agent task swarm",
    )
    p.add_argument(
        "-c", "--config",
        default=os.environ.get("SIRB_CONFIG", "sirb.yml"),
        help="Path to sirb config YAML (default: sirb.yml or $SIRB_CONFIG)",
    )
    p.add_argument(
        "--run-dir",
        default=os.environ.get("SIRB_RUN_DIR",
                                "~/hermes-vault/sirb-reports"),
        help="Output directory for reports and checkpoints",
    )

    sub = p.add_subparsers(dest="command", required=True)

    # run
    run_p = sub.add_parser("run", help="Execute a swarm run")
    run_p.add_argument("--tasks", help="JSON file with task list to inject")
    run_p.add_argument("--workers", nargs="*",
                       help="Override worker list from config")
    run_p.add_argument("--max-workers", type=int, default=10,
                       help="Max concurrent workers (default: 10)")
    run_p.add_argument("--task-timeout", type=int, default=300,
                       help="Per-task timeout in seconds (default: 300)")
    run_p.add_argument("--no-checkpoint", action="store_true",
                       help="Disable checkpointing")
    run_p.add_argument("--resume", help="Resume a previous run by run_id")
    run_p.add_argument("--cron", help="Cron schedule expression, e.g. '0 */6 * * *'")
    run_p.add_argument("--once", action="store_true",
                       help="Run once and exit (default behaviour)")
    run_p.add_argument("--webhook", help="URL to POST assessment JSON to on completion")
    run_p.add_argument("--max-failures", type=int, default=3,
                       help="Max consecutive failures before a worker is paused (default: 3)")

    # list-workers
    sub.add_parser("list-workers", help="List all discovered workers")

    # init
    init_p = sub.add_parser("init", help="Create a skeleton worker module")

    # dashboard
    dash_p = sub.add_parser("dashboard", help="Start live SSE dashboard for a running/previous run")
    dash_p.add_argument("--port", type=int, default=8100,
                        help="HTTP port (default: 8100)")
    dash_p.add_argument("--run-id", help="Specific run to watch (default: latest)")

    return p


def main(argv: list[str] = None):
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "list-workers":
        return _list_workers(args)
    elif args.command == "init":
        return _init_worker(args)
    elif args.command == "run":
        return _run(args)
    elif args.command == "dashboard":
        return _dashboard(args)

    parser.print_help()
    return 1


def _load_config(config_path: str) -> dict:
    """Load YAML config. Returns empty dict if file not found.

    Pure-Python approach — no PyYAML dependency. Only supports simple
    key-value YAML. For full YAML support, install PyYAML.
    """
    path = Path(config_path).expanduser().resolve()
    if not path.exists():
        # Search up the directory tree
        for parent in [Path.cwd()] + list(Path.cwd().parents):
            p = parent / "sirb.yml"
            if p.exists():
                path = p
                break
        else:
            return {}

    try:
        import yaml
        with open(path) as f:
            return yaml.safe_load(f) or {}
    except ImportError:
        # Minimal YAML reader as fallback
        config = {}
        with open(path) as f:
            for line in f:
                line = line.strip()
                if ":" in line and not line.startswith("#"):
                    k, v = line.split(":", 1)
                    key = k.strip()
                    val = v.strip().strip("\"'")
                    # Simple nested handling
                    parts = key.split(".")
                    target = config
                    for part in parts[:-1]:
                        target = target.setdefault(part, {})
                    target[parts[-1]] = val
        return config


def _load_tasks(tasks_path: str) -> list[Task]:
    """Load tasks from a JSON file."""
    path = Path(tasks_path).expanduser().resolve()
    with open(path) as f:
        data = json.load(f)

    if isinstance(data, list):
        return [Task.from_dict(d) for d in data]
    elif isinstance(data, dict) and "tasks" in data:
        return [Task.from_dict(d) for d in data["tasks"]]
    else:
        raise ValueError(
            f"tasks file must be a JSON array or {{'tasks': [...]}}, "
            f"got {type(data).__name__}"
        )


def _list_workers(args) -> int:
    config = _load_config(args.config)
    registry = _discover_workers(config.get("workers", []))

    workers = registry.list_workers()
    if not workers:
        print("[sirb] no workers discovered")
        return 0

    print(f"[sirb] {len(workers)} worker(s) discovered:\n")
    for w in workers:
        print(f"  {w['name']:20s}  {w['description']:50s}  ({w['cls']})")
    return 0


def _init_worker(args) -> int:
    """Generate a skeleton worker module."""
    name = input("Worker name (snake_case): ").strip()
    if not name:
        print("[sirb] aborting — name required")
        return 1

    desc = input("Description (one line): ").strip()

    content = f'''"""Sirb worker: {name}."""

from sirb.core import SirbWorker, Task, Result, Finding


class {name.title().replace("_", "")}Worker(SirbWorker):
    """{desc}"""

    name = "{name}"
    description = "{desc}"

    async def execute(self, task: Task) -> Result:
        """Execute one task."""
        # TODO: implement OSINT logic here
        return Result(
            task_id=task.id,
            worker=self.name,
            status="success",
            findings=[
                Finding(
                    target_id=task.params.get("target_id", ""),
                    target_type="{name}",
                    finding_type="info",
                    severity="info",
                    detail={{"raw": task.params}},
                ),
            ],
        )

    def rate_limits(self) -> dict:
        return {{}}
'''

    out_path = Path.cwd() / f"{name}_worker.py"
    out_path.write_text(content)
    print(f"[sirb] worker skeleton written to {out_path}")
    return 0


def _install_cron(args, config: dict) -> int:
    """Install a cron job for periodic Sirb runs."""
    import shlex

    # Build the command that cron will run
    cmd_parts = [sys.executable, "-m", "sirb.cli.main", "run"]
    if args.config:
        cmd_parts.extend(["-c", args.config])
    if args.workers:
        cmd_parts.extend(["--workers"] + args.workers)
    if args.max_workers:
        cmd_parts.extend(["--max-workers", str(args.max_workers)])
    if args.task_timeout:
        cmd_parts.extend(["--task-timeout", str(args.task_timeout)])
    if args.no_checkpoint:
        cmd_parts.append("--no-checkpoint")
    if args.tasks:
        cmd_parts.extend(["--tasks", args.tasks])

    cmd_str = shlex.join(cmd_parts)

    cron_line = f"{args.cron} cd {shlex.quote(os.getcwd())} && {cmd_str} >> ~/hermes-vault/sirb-reports/cron.log 2>&1"

    # Try to install via crontab
    try:
        import subprocess
        existing = subprocess.run(
            ["crontab", "-l"],
            capture_output=True, text=True,
        )
        existing_cron = existing.stdout if existing.returncode == 0 else ""

        # Check if a sirb cron already exists
        if "sirb.cli.main" in existing_cron:
            print("[sirb] WARN: a Sirb cron job already exists in crontab")
            print("       Remove it manually with `crontab -e` first.")
            # Still append? No — avoid duplicates
            return 1

        new_cron = existing_cron.strip() + "\n" + cron_line + "\n"
        proc = subprocess.run(
            ["crontab"],
            input=new_cron, text=True, capture_output=True,
        )
        if proc.returncode == 0:
            print(f"[sirb] cron job installed: {args.cron}")
            print(f"       {cmd_str}")
            print(f"       Log: ~/hermes-vault/sirb-reports/cron.log")
            return 0
        else:
            print(f"[sirb] ERROR: failed to install cron: {proc.stderr}")
            return 1

    except FileNotFoundError:
        print("[sirb] ERROR: `crontab` not found on this system")
        print("       Install cron or add this line manually:")
        print()
        print(f"  {cron_line}")
        return 1


def _run(args) -> int:
    config = _load_config(args.config)
    run_dir = os.path.expanduser(args.run_dir)

    # ── Cron mode: install scheduled job ────────────────────────────────
    if args.cron:
        return _install_cron(args, config)

    # ── Resolve config and run directory ────────────────────────────

    # Discover workers
    worker_names = args.workers or config.get("workers", [])
    registry = _discover_workers(worker_names)

    workers = registry.list_workers()
    print(f"[sirb] discovered {len(workers)} worker(s)")
    for w in workers:
        print(f"       {w['name']}: {w['description']}")

    if not workers:
        print("[sirb] ERROR: no workers discovered — nothing to run")
        return 1

    # Queue + Blackboard
    queue = TaskQueue()
    blackboard = Blackboard(
        decay_rate=config.get("blackboard", {}).get("decay_rate", 0.9),
    )
    router = Router(registry)

    # Checkpointer
    checkpoint_interval = config.get("checkpoint_interval", 5)
    checkpointer = Checkpointer(run_dir, checkpoint_interval)

    # Determine run_id
    if args.resume:
        run_id = args.resume
        restored = checkpointer.load_queue(run_id)
        if restored:
            queue = restored
            print(f"[sirb] resumed run {run_id} ({queue.count()} tasks)")
        else:
            print(f"[sirb] WARN: no checkpoint found for {run_id}, starting fresh")
            run_id = f"sirb-{int(time.time())}"
    else:
        run_id = f"sirb-{int(time.time())}"

    # Load or discover tasks
    if args.tasks:
        tasks = _load_tasks(args.tasks)
        queue.add_many(tasks)
        print(f"[sirb] loaded {len(tasks)} tasks from {args.tasks}")
    else:
        # Call discover() on each worker
        for worker_name, worker in registry.items():
            try:
                discovered = asyncio.run(worker.discover())
                if discovered:
                    queue.add_many(discovered)
                    print(f"[sirb] {worker_name}: discovered {len(discovered)} tasks")
            except Exception as e:
                print(f"[sirb] WARN: {worker_name}.discover() failed: {e}")

    total = queue.count(TaskStatus.PENDING)
    if total == 0:
        print("[sirb] no tasks to execute")
        return 0

    print(f"[sirb] starting swarm: {total} tasks, {args.max_workers} workers")

    # Register triggers from config (optional)
    trigger_config = config.get("triggers", [])
    for trigger in trigger_config:
        predicate = trigger.get("predicate", {})
        action = trigger.get("action", "")
        if predicate and action:
            blackboard.register_trigger(predicate, action)

    # Callback for checkpoint + triggers
    completed_before = 0

    def on_complete(task, result):
        nonlocal completed_before
        completed_before += 1

        # Write findings to blackboard and check triggers
        for finding in result.findings:
            blackboard.add(finding)
            triggered = blackboard.check_triggers(finding)
            for action in triggered:
                if action == "alert_aggregator":
                    print(
                        f"  ⚠ TRIGGER: {finding.finding_type} "
                        f"({finding.severity}) — {finding.target_id[:15]}"
                    )


        if not args.no_checkpoint and checkpointer.should_checkpoint(completed_before):
            checkpointer.save_all(run_id, queue, blackboard)

        # Print progress
        status = queue.get_status()
        print(
            f"  ✓ {task.id[:8]} ({task.worker}/{task.type}) "
            f"— {result.status} "
            f"[{status['progress']}]"
        )

    # Run pool with token bucket rate limiting
    throttle_pool = TokenBucketPool()
    for worker_name, worker in registry.items():
        limits = worker.rate_limits()
        throttle_pool.register_worker(worker_name, limits)

    pool = WorkerPool(
        queue=queue,
        router=router,
        max_workers=args.max_workers,
        task_timeout=args.task_timeout,
        on_complete=on_complete,
        throttle_pool=throttle_pool,
        max_failures=args.max_failures,
    )

    pool.run()

    # Final checkpoint
    if not args.no_checkpoint:
        checkpointer.save_all(run_id, queue, blackboard)

    # Generate assessment from blackboard
    try:
        from sirb.core import Aggregator
        agg = Aggregator()
        assessment = agg.assess(blackboard)
        assessment_md = agg.render_markdown(assessment)
        assessment_path = checkpointer._runs_dir / run_id / "assessment.md"
        assessment_path.parent.mkdir(parents=True, exist_ok=True)
        assessment_path.write_text(assessment_md)
        print(f"       assessment: {assessment_path}")
        # Save trend summary + render delta
        try:
            from sirb.core import TrendTracker
            tracker = TrendTracker(str(checkpointer._runs_dir))
            tracker.save_summary(run_id, assessment)
            prev = tracker.previous_summaries(run_id)
            if prev:
                delta = tracker.delta(assessment, prev[0])
                if delta.get("has_change"):
                    delta_md = tracker.render_delta_markdown(delta, prev[0].get("run_id", "?"))
                    print()
                    print(delta_md)
        except Exception as e:
            print(f"       [sirb] WARN: trend tracking failed: {e}")
    except Exception as e:
        print(f"       [sirb] WARN: assessment generation failed: {e}")

    # Webhook — POST assessment JSON if configured
    webhook_url = args.webhook or config.get("webhook", "")
    if webhook_url:
        try:
            import json as _json, urllib.request as _req
            assessment_json = _json.dumps(assessment).encode()
            _req.urlopen(_req.Request(
                webhook_url, data=assessment_json,
                headers={"Content-Type": "application/json"},
                method="POST",
            ))
            print(f"       webhook POSTED to {webhook_url}")
        except Exception as e:
            print(f"       [sirb] WARN: webhook failed: {e}")

    # Summary
    status = queue.get_status()
    print(f"\n[sirb] run {run_id} complete")
    print(f"       {status['progress']} tasks done")
    print(f"       {blackboard.count()} findings on blackboard")
    print(f"       reports: {checkpointer._runs_dir / run_id}")

    return 0


def _discover_workers(worker_config) -> WorkerRegistry:
    """Discover workers from config, entry points, and package auto-discover.

    ``worker_config`` can be:
    - A list of module names: `["my-worker"]`
    - A dict with nested config: `{"my-worker": {"option": "value"}}`
    - Empty (falls back to entry-point scan + auto-discover)
    """
    registry = WorkerRegistry()

    # 1. Entry-point discovery — pip-installed SirbWorker packages
    ep_count = registry.discover_entry_points()
    if ep_count > 0:
        print(f"[sirb] discovered {ep_count} worker(s) via entry points")

    # 2. Auto-discover from sirb.workers package
    registry.discover_package("sirb.workers")

    # 3. Config-based worker initialisation
    if isinstance(worker_config, list):
        registry.discover(worker_config)
    elif isinstance(worker_config, dict):
        worker_modules = {}
        for key, val in worker_config.items():
            if isinstance(val, dict):
                worker_modules[key] = val
            else:
                worker_modules[key] = {}
        registry.discover(worker_modules)

    return registry


# ── dashboard ────────────────────────────────────────────────────────────

def _dashboard(args):
    """Full-featured dashboard: run history, live view, assessment browser, map, launch panel."""

    import http.server
    import json
    import os
    import signal
    import socket
    import subprocess
    import sys
    import threading
    import time
    import urllib.parse
    from pathlib import Path
    from http.server import ThreadingHTTPServer

    port = args.port
    run_id_filter = args.run_id
    runs_base = _get_runs_dir(args)
    running_procs: dict[str, subprocess.Popen] = {}
    proc_lock = threading.Lock()

    def _load_assessment_json(rid: str) -> dict:
        sjp = runs_base / rid / "assessment-summary.json"
        if sjp.exists():
            return json.loads(sjp.read_text())
        return {}

    def _load_models() -> list[str]:
        """Read available models from Hermes config + known provider caches."""
        models: set[str] = set()

        # 1. Main config
        cfg_path = Path.home() / ".hermes" / "config.yaml"
        if cfg_path.exists():
            try:
                import yaml
                raw = cfg_path.read_text()
                cfg = yaml.safe_load(raw) or {}
                m = cfg.get("model", {})
                if isinstance(m, dict):
                    default = m.get("default")
                    if default:
                        models.add(default)
                for prov in cfg.get("custom_providers", []):
                    if isinstance(prov, dict):
                        for key in ("model", "models"):
                            val = prov.get(key)
                            if isinstance(val, str) and val:
                                models.add(val)
                            elif isinstance(val, list):
                                for v in val:
                                    if v:
                                        models.add(v)
                for fb in cfg.get("fallback_providers", []):
                    if isinstance(fb, dict) and fb.get("model"):
                        models.add(fb["model"])
            except Exception:
                pass

        # 2. API keys from profile .env files → match to provider names in cache
        profile_dir = Path.home() / ".hermes" / "profiles"
        active_providers: set[str] = set()
        for env_file in profile_dir.rglob(".env"):
            try:
                for line in env_file.read_text().splitlines():
                    line = line.strip()
                    if line.startswith("LLM_MODEL="):
                        val = line.split("=", 1)[1].strip().strip("'\"")
                        if val:
                            models.add(val)
            except Exception:
                pass

        # 2b. Map API_KEY env vars to cache provider names
        api_key_map = {
            "anthropic": "anthropic", "deepseek": "deepseek",
            "openai": "openai", "openrouter": "openrouter",
            "google": "google", "github": "github-copilot",
            "ollama": "ollama-cloud", "mistral": "mistral",
            "cohere": "cohere", "groq": "groq", "xai": "xai",
            "together": "togetherai", "perplexity": "perplexity",
        }
        for env_file in profile_dir.rglob(".env"):
            try:
                for line in env_file.read_text().splitlines():
                    line = line.strip()
                    for key, provider in api_key_map.items():
                        if line.startswith(key.upper() + "_API_KEY="):
                            val = line.split("=", 1)[1].strip().strip("'\"")
                            if val and val != "''":
                                active_providers.add(provider)
            except Exception:
                pass

        # 3. Scan model cache for active providers
        for cache_file in profile_dir.rglob("*model*cache*.json"):
            try:
                data = json.loads(cache_file.read_text())
                for prov_name, info in data.items():
                    if prov_name in active_providers:
                        prov_models = info.get("models", {})
                        if isinstance(prov_models, dict):
                            models.update(prov_models.keys())
            except Exception:
                pass

        # Fall back
        if not models:
            fallback = os.environ.get("HERMES_INFERENCE_MODEL", "deepseek-v4-flash")
            models.add(fallback)

        # Filter out non-LLM models
        known_non_llm = {"whisper-1", "base", "gpt-4o-mini-tts"}
        models = {
            m for m in models
            if m not in known_non_llm
            and "tts" not in m.lower()
            and "voxtral" not in m.lower()
            and "neuphonic" not in m.lower()
        }
        # Add known model aliases used by the user
        for alias in ("claude-fable-5",):
            if alias not in models:
                models.add(alias)
        return sorted(models)

    def _list_runs() -> list[dict]:
        if not runs_base.exists():
            return []
        out = []
        for d in sorted(runs_base.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True):
            if not d.is_dir():
                continue
            ap = d / "assessment.md"
            tr = d / "tracking.json"
            sr = d / "swarm-report.md"
            mtime = d.stat().st_mtime if d.stat() else 0
            info = {"id": d.name, "has_assessment": ap.exists() or sr.exists(),
                    "mtime": mtime, "generated_at": ""}
            if ap.exists():
                info["generated_at"] = datetime.fromtimestamp(mtime).isoformat()
            elif sr.exists():
                info["generated_at"] = datetime.fromtimestamp(mtime).isoformat()
            elif tr.exists():
                try:
                    t = json.loads(tr.read_text())
                    agents = t.get("agents", {})
                    done = sum(1 for a in agents.values() if a.get("status") == "done")
                    failed = sum(1 for a in agents.values() if a.get("status") in ("failed", "timeout", "error"))
                    info["targets"] = len(t.get("targets", []))
                    info["done"] = done
                    info["failed"] = failed
                    info["generated_at"] = t.get("created_at", "")
                    info["status"] = t.get("status", "running")
                except Exception:
                    pass
            out.append(info)
            if len(out) >= 50:
                break
        return out

    def _load_assessment(rid: str) -> str | None:
        mdp = runs_base / rid / "assessment.md"
        return mdp.read_text() if mdp.exists() else None

    def _load_assessment_json(rid: str) -> dict:
        sjp = runs_base / rid / "assessment-summary.json"
        if sjp.exists():
            return json.loads(sjp.read_text())
        return {}

    def _extract_vessel_data(target: str, vessels_dir: Path) -> dict:
        """Extract structured vessel data from analyst report markdown."""
        data = {"target": target, "name": target, "mmsi": "", "imo": "",
                "owner": "", "operator": "", "manager": "", "flag": "",
                "callsign": "", "type": "", "tonnage": "", "year_built": "",
                "ports": [], "connections": ""}

        # Read analyst report first (structured markdown), fall back to log
        sub = vessels_dir / target
        report_text = ""
        if sub.exists():
            for f in sorted(sub.glob("*.md")):
                report_text += f.read_text(errors="replace") + "\n"
        log_path = vessels_dir / f"{target}.log"
        log_text = log_path.read_text(errors="replace") if log_path.exists() else ""
        # Use report text for structured fields, log as fallback
        text = report_text or log_text

        # Extract IMO number
        m = re.search(r'\bIMO\s*:?\s*(\d{7})\b', text, re.IGNORECASE)
        if m: data["imo"] = m.group(1)
        if not data["imo"]:
            m = re.search(r'\bIMO\s*(\d{7})\b', text)
            if m: data["imo"] = m.group(1)

        # Extract MMSI
        m = re.search(r'\bMMSI\s*:?\s*(\d{9})\b', text, re.IGNORECASE)
        if m: data["mmsi"] = m.group(1)

        # Extract vessel name — match "| **Current Name** | Value |" or "**Target:** NAME (IMO"
        m = re.search(r'\|\s*\*?\*?Current\s*Name\*?\*?\s*\|\s*([^|]+?)\s*\|', text)
        if not m:
            m = re.search(r'\*?\*?Target\*?\*?\s*:\s*([A-Za-z0-9][A-Za-z0-9\s\-]+?)(?:\s*\(IMO|\s*\(MMSI|\s*\n)', text)
        if not m:
            m = re.search(r'(?i)(?:vessel\s*[:\-]\s*|name\s*[:\-]\s*|ship\s*[:\-]\s*["\']?)([A-Za-z0-9\s\-]+?)(?:\s*[,.]|\s*MMSI|\s*IMO|\s*Flag|\s*$)', text)
        if m: data["name"] = m.group(1).strip()[:50]

        # Extract flag — match "| **Flag** | Value |" in markdown table
        m = re.search(r'\|\s*\*?\*?Flag\*?\*?\s*\|\s*([^|]+?)\s*\|', text)
        if not m:
            m = re.search(r'(?:^|\n)\s*\*?\*?Flag\*?\*?\s*[:\|]\s*([A-Za-z][A-Za-z\s]{1,30}?)(?:\s*[,.]|\s*\(|\s*\n|$)', text)
        if m: data["flag"] = m.group(1).strip()[:40]
        # Filter out jinja placeholder values
        if data["flag"] and ("<!--" in data["flag"] or data["flag"].startswith("discovered")):
            data["flag"] = ""

        # Extract owner — match "| **Registered Owner** | Value |" in markdown table
        m = re.search(r'\|\s*\*?\*?Registered\s*Owner\*?\*?\s*\|\s*([^|]+?)\s*\|', text)
        if not m:
            m = re.search(r'\|\s*\*?\*?Owner\*?\*?\s*\|\s*([^|]+?)\s*\|', text)
        if not m:
            m = re.search(r'(?:^|\n)\s*\*?\*?(?:Registered\s*Owner|Owner)\*?\*?\s*[:\|]\s*([A-Za-z0-9][A-Za-z0-9\s\.,\-&]{1,60}?)(?:\s*\(|\s*since|\s*IMO|\s*\n|$)', text)
        if m:
            owner = m.group(1).strip().rstrip('.,&')[:60]
            data["owner"] = owner
        # Filter out jinja placeholder values
        if data["owner"] and ("<!--" in data["owner"] or data["owner"].startswith("discovered")):
            data["owner"] = ""

        # Extract operator from markdown
        m = re.search(r'(?i)(?:Operator|Commercial\s*Manager)\s*\|?\s*(.+?)(?:\s*\||\s*\n)', text)
        if not m:
            m = re.search(r'(?i)(?:Operator|Commercial\s*operator)\s*:?\s*["\']?([A-Za-z0-9\s\.\,\-&\']+?)(?:\s*[,.]|\s*$)', text, re.MULTILINE)
        if m: data["operator"] = m.group(1).strip()[:60]

        # Extract port calls
        port_matches = re.findall(r'(?i)(?:Port|Port\s*call|Called\s*at)\s*:?\s*["\']?([A-Za-z\s\-]+?)(?:\s*[,\.]|\s*on|\s*Date|\s*$)', text)
        data["ports"] = list(set(p.strip() for p in port_matches if len(p.strip()) > 2))

        # Extract AIS proximity / connections
        conn_matches = re.findall(r'(?i)(?:connection|related|linked|associated\s*with|same\s*owner|sister\s*ship)[^.]*\.', text)
        data["connections"] = " ".join(conn_matches[:5])

        return data

    def _generate_connections(agents: dict, vessels_dir: Path) -> str:
        """Analyze cross-vessel connections from agent outputs."""
        parts = ["# Cross-Vessel Connection Analysis\n\n"]
        targets = list(agents.keys())
        if len(targets) < 2:
            parts.append("*Only one target — no cross-vessel connections to analyze.*\n")
            return "".join(parts)

        # Extract data for each vessel
        vessel_data = {}
        for t in targets:
            d = _extract_vessel_data(t, vessels_dir)
            vessel_data[t] = d

        parts.append(f"**Targets analyzed:** {', '.join(targets)}\n\n")

        # ── Shared ownership ──
        parts.append("## Shared Ownership & Management\n\n")
        found_ownership = False
        for i, a in enumerate(targets):
            for b in targets[i+1:]:
                da = vessel_data[a]
                db = vessel_data[b]
                if da["owner"] and db["owner"] and da["owner"].lower() == db["owner"].lower():
                    parts.append(f"- **{a}** and **{b}** share same owner: **{da['owner']}** (HIGH confidence)\n")
                    found_ownership = True
                    break
                if da["operator"] and db["operator"] and da["operator"].lower() == db["operator"].lower():
                    parts.append(f"- **{a}** and **{b}** share same operator: **{da['operator']}** (HIGH confidence)\n")
                    found_ownership = True
                    break
        if not found_ownership:
            parts.append("*(No shared owners or operators detected across targets.)*\n")

        # ── Flag analysis ──
        parts.append("\n## Flag State\n\n")
        flags = {}
        for t in targets:
            d = vessel_data[t]
            f = d["flag"] or "unknown"
            flags.setdefault(f, []).append(t)
        for f, vessels in flags.items():
            if len(vessels) > 1:
                parts.append(f"- **{f}**: {', '.join(vessels)} — same flag state\n")
        if len(flags) <= 1:
            parts.append("*(All targets have different or unknown flag states.)*\n")

        # ── Overlapping port calls ──
        parts.append("\n## Overlapping Port Calls\n\n")
        found_ports = False
        for i, a in enumerate(targets):
            for b in targets[i+1:]:
                common = set(vessel_data[a]["ports"]) & set(vessel_data[b]["ports"])
                if common:
                    parts.append(f"- **{a}** & **{b}** both visited: {', '.join(f'**{p}**' for p in common)}\n")
                    found_ports = True
        if not found_ports:
            parts.append("*(No overlapping port calls detected.)*\n")

        # ── Connection summary table ──
        parts.append("\n## Connection Summary\n\n")
        parts.append("| Vessel Pair | Shared Owner | Shared Flag | Shared Ports | Confidence |\n")
        parts.append("|-------------|-------------|-------------|--------------|------------|\n")
        for i, a in enumerate(targets):
            for b in targets[i+1:]:
                da, db = vessel_data[a], vessel_data[b]
                owner_match = "✅" if (da["owner"] and db["owner"] and
                              da["owner"].lower() == db["owner"].lower()) else "❌"
                flag_match = "✅" if (da["flag"] and db["flag"] and
                             da["flag"].lower() == db["flag"].lower()) else "❌"
                port_match = "✅" if set(da["ports"]) & set(db["ports"]) else "❌"
                conf = "HIGH" if owner_match == "✅" else "MEDIUM" if flag_match == "✅" else "LOW"
                parts.append(f"| {a} — {b} | {owner_match} | {flag_match} | {port_match} | {conf} |\n")

        parts.append("\n---\n*Generated by Sirb swarm correlation engine.*\n")
        return "".join(parts)

    def _generate_swarm_report(rid: str, targets: list, mode: str,
                                agents: dict, connections: str) -> str:
        """Generate the combined Sirb swarm report.

        Uses a Jinja2 template (templates/swarm-report.j2) when available
        for consistent formatting. Falls back to inline string building.
        """
        vessels_dir = runs_base / rid / "vessels"
        vessel_data = {}
        for t in targets:
            vessel_data[t] = _extract_vessel_data(t, vessels_dir)

        generated_at = datetime.now(timezone.utc).isoformat()

        if _JINJA_AVAILABLE and (_TEMPLATES_DIR / "swarm-report.j2").exists():
            env = Environment(
                loader=FileSystemLoader(str(_TEMPLATES_DIR)),
                autoescape=False,
                trim_blocks=True,
                lstrip_blocks=True,
                keep_trailing_newline=True,
            )
            template = env.get_template("swarm-report.j2")
            return template.render(
                rid=rid,
                mode=mode,
                targets=targets,
                agents=agents,
                vessel_data=vessel_data,
                connections=connections,
                generated_at=generated_at,
                framework_version=_SIRB_VERSION,
            )

        # Fallback: inline string builder
        lines = [f"# Sirb Swarm Report: {rid}\n"]
        lines.append(f"\n**Mode:** {mode}\n")
        lines.append(f"**Targets ({len(targets)}):** {', '.join(targets)}\n")
        lines.append(f"**Generated:** {generated_at}\n\n")
        lines.append("## Agent Results\n\n")
        lines.append("| Target | Status | IMO | Flag | Owner |\n")
        lines.append("|--------|--------|-----|------|-------|\n")
        for t in targets:
            a = agents.get(t, {})
            s = a.get("status", "?")
            icon = "✅" if s in ("done", "success") else "❌"
            d = vessel_data.get(t, {})
            imo = d.get("imo") or "—"
            flag = d.get("flag") or "—"
            owner = (d.get("owner") or "")[:30] or "—"
            lines.append(f"| {t} | {icon} | {imo} | {flag} | {owner} |\n")
        lines.append(f"\n## Cross-Vessel Analysis\n\n{connections}\n")
        return "".join(lines)

    # ── Dashboard handler ──────────────────────────────────────────────

    class DashHandler(http.server.BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):
            pass

        def do_GET(self):
            parsed = urllib.parse.urlparse(self.path)
            path = parsed.path
            qs = urllib.parse.parse_qs(parsed.query)

            if path == "/":
                self._serve_html()
            elif path == "/events":
                self._serve_sse()
            elif path == "/health":
                self._send_json({"status": "ok", "time": time.time()})
            elif path == "/logo":
                self._serve_logo()
            elif path == "/runs":
                self._send_json(_list_runs())
            elif path.startswith("/run/") and path.endswith("/json"):
                rid = path.split("/")[2]
                self._send_json(_load_assessment_json(rid))
            elif path.startswith("/run/") and path.endswith("/assessment"):
                rid = path.split("/")[2]
                md = _load_assessment(rid)
                self._send_html(md or "<p>No assessment yet.</p>")
            elif path.startswith("/run/") and path.endswith("/report"):
                rid = path.split("/")[2]
                rp = runs_base / rid / "swarm-report.md"
                if rp.exists():
                    self._send_html(rp.read_text())
                else:
                    self._send_html("<p>Report not ready yet. Agents still running.</p>")
            elif re.match(r"^/run/[^/]+/tracking\.json$", path):
                rid = path.split("/")[2]
                tp = runs_base / rid / "tracking.json"
                if tp.exists():
                    self._send_json(json.loads(tp.read_text()))
                else:
                    self._send_json({})
            elif path.startswith("/run/") and path.endswith("/connections"):
                rid = path.split("/")[2]
                cp = runs_base / rid / "connections.md"
                if cp.exists():
                    self._send_html(cp.read_text())
                else:
                    self._send_html("<p>Connections analysis not ready yet.</p>")
            elif path.startswith("/run/") and path.endswith("/stats"):
                rid = path.split("/")[2]
                tr_path = runs_base / rid / "tracking.json"
                if not tr_path.exists():
                    self._send_json({"error": "run not found"}, 404)
                    return
                try:
                    tr = json.loads(tr_path.read_text())
                    vd = runs_base / rid / "vessels"
                    # Use worker's extract_stats (agnostic — no hardcoded stats)
                    from sirb.core.registry import WorkerRegistry
                    reg = WorkerRegistry()
                    reg.discover()
                    reg.discover_entry_points()
                    worker_name = tr.get("worker", "shipcrawler")
                    if worker_name not in reg:
                        self._send_json({"error": f"worker '{worker_name}' not installed"})
                        return
                    worker = reg[worker_name]
                    # Accumulate stats across all agents using worker's extract_stats
                    total = {}
                    agent_targets = tr.get("agents", {})
                    if not agent_targets and tr.get("targets"):
                        # Run still in progress — use targets list
                        agent_targets = {t: {} for t in tr["targets"]}
                    for target in agent_targets:
                        log_path = str(vd / f"{target}.log")
                        report_dir = str(vd / target)
                        s = worker.extract_stats(log_path, report_dir)
                        for k, v in s.items():
                            if isinstance(v, (int, float)):
                                total[k] = total.get(k, 0) + v
                            elif k == "phases":
                                total[k] = max(v, total.get(k, 0))
                            elif k == "duration":
                                total[k] = v  # last agent's duration
                            else:
                                total[k] = v  # last value wins for strings
                    if tr.get("model") and total.get("model", "—") == "—":
                        total["model"] = tr["model"]
                    self._send_json(total)
                except Exception as e:
                    self._send_json({"error": str(e)}, 500)
            elif path.startswith("/run/") and "/vessel/" in path and path.endswith(".md"):
                # /run/{rid}/vessel/{target}/{filename}.md
                parts = path.split("/")
                rid = parts[2]
                target = parts[4]
                filename = parts[5]
                fp = runs_base / rid / "vessels" / target / filename
                if fp.exists():
                    self._send_html(fp.read_text())
                else:
                    self._send_html("<p>File not found.</p>", 404)
            elif re.match(r"^/run/[^/]+/targets$", path):
                # List per-target report files (agnostic — used by per_target tabs)
                rid = path.split("/")[2]
                vdir = runs_base / rid / "vessels"
                if vdir.exists():
                    targets = []
                    for v in sorted(vdir.iterdir()):
                        if v.is_dir():
                            files = sorted(f.name for f in v.iterdir()
                                          if f.suffix in (".md", ".log", ".txt")
                                          and f.stat().st_size > 0)
                            if files:
                                targets.append({"target": v.name, "files": files})
                    self._send_json(targets)
                else:
                    self._send_json([])
            elif re.match(r"^/run/[^/]+/vessels$", path):
                rid = path.split("/")[2]
                vdir = runs_base / rid / "vessels"
                if vdir.exists():
                    vessels = []
                    for v in sorted(vdir.iterdir()):
                        if v.is_dir():
                            files = sorted(f.name for f in v.iterdir() if f.suffix in (".md", ".log", ".txt") and f.stat().st_size > 0)
                            if files:
                                vessels.append({"target": v.name, "files": files})
                        elif v.suffix in (".log", ".txt", ".md"):
                            # Also include flat log files
                            pass
                    self._send_json(vessels)
                else:
                    self._send_json([])
            elif path == "/map":
                self._serve_map_html()
            elif path == "/models":
                self._send_json(_load_models())
            elif path == "/api/workers":
                # List installed SirbWorkers (discovered via entry points)
                from sirb.core.registry import WorkerRegistry
                reg = WorkerRegistry()
                reg.discover()
                reg.discover_entry_points()
                workers = []
                for name, w in reg.items():
                    workers.append({"name": name, "description": getattr(w, "description", "")})
                self._send_json(workers)
            elif re.match(r"^/api/workers/[^/]+/schema$", path):
                # Get input form schema for a specific worker
                wname = path.split("/")[3]
                from sirb.core.registry import WorkerRegistry
                reg = WorkerRegistry()
                reg.discover()
                reg.discover_entry_points()
                if wname in reg:
                    self._send_json(reg[wname].input_schema())
                else:
                    self._send_json({"error": "worker not found"}, 404)
            elif re.match(r"^/api/workers/[^/]+/stats-schema$", path):
                # Get stats bar schema for a specific worker
                wname = path.split("/")[3]
                from sirb.core.registry import WorkerRegistry
                reg = WorkerRegistry()
                reg.discover()
                reg.discover_entry_points()
                if wname in reg:
                    self._send_json(reg[wname].stats_schema())
                else:
                    self._send_json({"error": "worker not found"}, 404)
            elif re.match(r"^/api/workers/[^/]+/report-tabs$", path):
                # Get report tab definitions for a specific worker
                wname = path.split("/")[3]
                from sirb.core.registry import WorkerRegistry
                reg = WorkerRegistry()
                reg.discover()
                reg.discover_entry_points()
                if wname in reg:
                    self._send_json(reg[wname].report_tabs(""))
                else:
                    self._send_json({"error": "worker not found"}, 404)
            elif path == "/api/profiles/models":
                pm_path = Path(__file__).parent / "profiles-models.json"
                try:
                    data = json.loads(pm_path.read_text())
                    self.send_response(200)
                    self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps(data).encode())
                except Exception as e:
                    self._send_json({"error": str(e)}, 500)
            else:
                self.send_response(404)
                self.end_headers()

        def do_POST(self):
            content_len = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_len).decode() if content_len else ""
            parsed = urllib.parse.urlparse(self.path)
            path = parsed.path
            params = urllib.parse.parse_qs(body)

            if re.match(r"^/api/workers/[^/]+/parse$", path):
                # Parse raw input into structured targets
                wname = path.split("/")[3]
                from sirb.core.registry import WorkerRegistry
                reg = WorkerRegistry()
                reg.discover()
                reg.discover_entry_points()
                if wname not in reg:
                    self._send_json({"error": "worker not found"}, 404)
                    return
                raw_input = params.get("input", [""])[0]
                targets = reg[wname].parse_targets(raw_input)
                self._send_json({"targets": targets})
            elif path == "/run/new":
                mmsis = params.get("mmsi", [""])[0].strip()
                mode = params.get("mode", ["fast"])[0].strip()
                profile = params.get("profile", [""])[0].strip()
                model = params.get("model", [""])[0].strip()
                if not mmsis:
                    self._send_json({"error": "No IMO/MMSI provided"}, 400)
                    return
                mmsi_list = [m.strip() for m in mmsis.replace(",", " ").split() if m.strip()]
                run_id = f"swarm-{int(time.time())}"
                rundir = runs_base / run_id
                rundir.mkdir(parents=True, exist_ok=True)
                vessels_dir = rundir / "vessels"
                vessels_dir.mkdir(exist_ok=True)
                worker_name = params.get("worker", ["shipcrawler"])[0].strip() or "shipcrawler"
                tracking = {"run_id": run_id, "targets": mmsi_list, "mode": mode, "model": model or "deepseek-v4-flash",
                             "worker": worker_name,
                             "created_at": datetime.now(timezone.utc).isoformat(),
                             "status": "running", "agents": {}}
                (rundir / "tracking.json").write_text(json.dumps(tracking))

                # Spawn hermes agents in background thread
                def _run_swarm(rid, targets, md, prof, mod, worker_name="shipcrawler"):
                    """Run tasks via core kernel (TaskQueue + WorkerPool + Blackboard).

                    Agnostic: discovers workers via pip entry points — sirb
                    doesn't know what a vessel or shipcrawler is.
                    """
                    from sirb.core import TaskQueue, WorkerPool, Blackboard, Router, Task
                    from sirb.core.registry import WorkerRegistry

                    vessels_path = runs_base / rid / "vessels"
                    tr_path = runs_base / rid / "tracking.json"

                    # Discover installed workers via entry points
                    registry = WorkerRegistry()
                    registry.discover()
                    registry.discover_entry_points()

                    if worker_name not in registry:
                        # Fallback: no workers installed
                        tr = {"run_id": rid, "targets": targets, "mode": md,
                              "model": mod or "glm-5.2",
                              "created_at": datetime.now(timezone.utc).isoformat(),
                              "status": "error",
                              "error": f"Worker '{worker_name}' not installed. pip install <worker-package>."}
                        tr_path.write_text(json.dumps(tr))
                        return

                    worker = registry[worker_name]
                    router = Router(registry)

                    # Create queue + blackboard
                    queue = TaskQueue()
                    blackboard = Blackboard(decay_rate=0.9)

                    # Add tasks to queue
                    for target in targets:
                        agent_dir = vessels_path / target
                        agent_dir.mkdir(parents=True, exist_ok=True)
                        log_path = vessels_path / f"{target}.log"
                        task = Task(
                            type="vessel_osint",
                            worker=worker_name,
                            params={"target": target, "mmsi": target, "run_id": rid,
                                    "agent_dir": str(agent_dir),
                                    "log_path": str(log_path),
                                    "mode": md,
                                    "profile": prof,
                                    "model": mod},
                        )
                        queue.add(task)

                    # Update tracking
                    def _update_tracking(status, agents_data=None):
                        # Preserve existing agents dict if not explicitly provided
                        if agents_data is None:
                            try:
                                existing = json.loads(tr_path.read_text())
                                agents_data = existing.get("agents", {})
                            except Exception:
                                agents_data = {}
                        tr = {"run_id": rid, "targets": targets, "mode": md,
                              "model": mod or "glm-5.2",
                              "created_at": datetime.now(timezone.utc).isoformat(),
                              "status": status, "agents": agents_data}
                        tr_path.write_text(json.dumps(tr))

                    _update_tracking("running")
                    # Mark all agents as running in tracking
                    try:
                        tr = json.loads(tr_path.read_text())
                        tr["agents"] = {t: {"status": "running"} for t in targets}
                        tr_path.write_text(json.dumps(tr))
                    except Exception:
                        pass

                    # on_complete callback — write findings to blackboard + update tracking
                    def on_complete(task, result):
                        for finding in result.findings:
                            blackboard.add(finding)
                        # Update tracking with agent status
                        try:
                            tr = json.loads(tr_path.read_text())
                        except Exception:
                            tr = {}
                        if "agents" not in tr:
                            tr["agents"] = {}
                        tr["agents"][task.params["target"]] = {
                            "status": result.status,
                            "findings": len(result.findings),
                            "artifacts": len(result.artifacts),
                        }
                        tr_path.write_text(json.dumps(tr))

                    # Run pool
                    pool = WorkerPool(
                        queue=queue, router=router,
                        max_workers=min(len(targets), 10),
                        on_complete=on_complete,
                    )
                    pool.run()

                    # Save blackboard
                    import json as _json
                    bb_data = {"findings": []}
                    for f in blackboard.query():
                        bb_data["findings"].append(f.to_dict())
                    (runs_base / rid / "blackboard.json").write_text(_json.dumps(bb_data))

                    _update_tracking("done")

                    # Generate connections analysis from blackboard findings
                    agents = {}
                    try:
                        tr = json.loads(tr_path.read_text())
                        agents = tr.get("agents", {})
                    except Exception:
                        pass
                    connections = _generate_connections(agents, vessels_path)
                    (runs_base / rid / "connections.md").write_text(connections)

                    # Generate swarm report
                    report = _generate_swarm_report(rid, targets, md, agents, connections)
                    (runs_base / rid / "swarm-report.md").write_text(report)

                def _run_swarm_safe(rid, targets, md, prof, mod, worker_name="shipcrawler"):
                    try:
                        _run_swarm(rid, targets, md, prof, mod, worker_name)
                    except Exception as e:
                        import traceback
                        tb = traceback.format_exc()
                        print(f"[sirb] ERROR in _run_swarm thread: {e}\n{tb}", flush=True)

                # Get worker from POST params or default
                worker_name = params.get("worker", ["shipcrawler"])[0].strip() or "shipcrawler"

                thread = threading.Thread(
                    target=_run_swarm_safe,
                    args=(run_id, mmsi_list, mode, profile, model, worker_name),
                    daemon=True,
                )
                thread.start()
                with proc_lock:
                    running_procs[run_id] = thread
                self._send_json({"run_id": run_id, "status": "started",
                                 "targets": len(mmsi_list)})

            elif path.startswith("/run/") and path.endswith("/stop"):
                rid = path.split("/")[2]
                with proc_lock:
                    proc = running_procs.pop(rid, None)
                if proc:
                    try:
                        proc.terminate()
                    except Exception:
                        pass
                    self._send_json({"status": "stopped"})
                else:
                    self._send_json({"status": "not_running"})

            elif path == "/run/geo":
                lat = params.get("lat", [""])[0].strip()
                lon = params.get("lon", [""])[0].strip()
                radius = params.get("radius", ["50"])[0].strip()
                label = params.get("label", [f"{lat},{lon}"])[0].strip()
                profile = params.get("profile", [""])[0].strip()
                model = params.get("model", [""])[0].strip()
                if not lat or not lon:
                    self._send_json({"error": "lat and lon required"}, 400)
                    return
                import tempfile
                tf = tempfile.NamedTemporaryFile(
                    mode="w", suffix=".json", delete=False, prefix="sirb-geo-")
                geo_cfg = [{"lat": float(lat), "lon": float(lon),
                            "radius_km": float(radius), "label": label}]
                tf.write(json.dumps(geo_cfg))
                tf.close()
                env = os.environ.copy()
                env["SIRB_GEO_TARGETS"] = json.dumps(geo_cfg)
                if profile and profile not in ("", "default"):
                    env["SIRB_HERMES_PROFILE"] = profile
                if model:
                    env["SIRB_HERMES_MODEL"] = model
                args_list = ["sirb", "run"]
                try:
                    proc = subprocess.Popen(
                        args_list, stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT, text=True, env=env,
                    )
                    run_id = f"geo-{int(time.time())}"
                    with proc_lock:
                        running_procs[run_id] = proc
                    threading.Thread(
                        target=lambda pid, p: [p.stdout.read()] or
                        running_procs.pop(pid, None),
                        args=(run_id, proc), daemon=True,
                    ).start()
                    self._send_json({"run_id": run_id, "status": "started"})
                except Exception as e:
                    self._send_json({"error": str(e)}, 500)

            elif path == "/run/port":
                port_key = params.get("port", [""])[0].strip()
                profile = params.get("profile", [""])[0].strip()
                model = params.get("model", [""])[0].strip()
                if not port_key:
                    self._send_json({"error": "port required"}, 400)
                    return
                # Look up port definition from PortConfig
                try:
                    from shipcrawler_worker.discovery import PortConfig
                    pc = PortConfig()
                    pd = pc.get(port_key)
                    if not pd:
                        self._send_json({"error": f"Unknown port: {port_key}"}, 400)
                        return
                    # Spawn run with port config in environment
                    port_cfg = {port_key: {"vessel_finder_url": pd.vessel_finder_url,
                                            "lat_min": pd.lat_min, "lat_max": pd.lat_max,
                                            "lon_min": pd.lon_min, "lon_max": pd.lon_max}}
                    hermes_python = sys.executable
                    env = os.environ.copy()
                    env["SIRB_WORKER_CONFIG"] = json.dumps({"ports": port_cfg})
                    if profile and profile not in ("", "default"):
                        env["SIRB_HERMES_PROFILE"] = profile
                    if model:
                        env["SIRB_HERMES_MODEL"] = model
                    args_list = ["sirb", "run"]
                    proc = subprocess.Popen(
                        args_list, stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT, text=True, env=env,
                    )
                    run_id = f"port-{port_key}-{int(time.time())}"
                    with proc_lock:
                        running_procs[run_id] = proc
                    threading.Thread(
                        target=lambda pid, p: [p.stdout.read()] or
                        running_procs.pop(pid, None),
                        args=(run_id, proc), daemon=True,
                    ).start()
                    self._send_json({"run_id": run_id, "status": "started",
                                     "port": port_key})
                except ImportError:
                    self._send_json({"error": "shipcrawler_worker not installed"}, 400)
                except Exception as e:
                    self._send_json({"error": str(e)}, 500)

            else:
                self._send_json({"error": "not found"}, 404)

        def do_DELETE(self):
            parsed = urllib.parse.urlparse(self.path)
            path = parsed.path
            if path.startswith("/run/"):
                rid = path.split("/")[2]
                rundir = runs_base / rid
                if rundir.exists():
                    import shutil
                    shutil.rmtree(str(rundir))
                    with proc_lock:
                        running_procs.pop(rid, None)
                    self._send_json({"status": "deleted"})
                else:
                    self._send_json({"error": "not found"}, 404)
            else:
                self._send_json({"error": "not found"}, 404)

        # ── response helpers ──────────────────────────────────────────

        def _send_json(self, data, status=200):
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps(data).encode())

        def _send_html(self, html, status=200):
            self.send_response(status)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(html.encode())

        _LOGO_PATH = os.path.join(os.path.dirname(__file__), "logo.png")

        def _serve_logo(self):
            try:
                with open(self._LOGO_PATH, "rb") as f:
                    data = f.read()
                self.send_response(200)
                self.send_header("Content-Type", "image/png")
                self.send_header("Cache-Control", "public, max-age=86400")
                self.end_headers()
                self.wfile.write(data)
            except FileNotFoundError:
                self._send_json({"error": "logo not found"}, 404)

        # ── main HTML page ────────────────────────────────────────────

        def _serve_html(self):
            html = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>SIRB — Agentic Swarm Dashboard</title>
<link rel="icon" type="image/svg+xml" href="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCAyNCAyNCIgZmlsbD0ibm9uZSIgc3Ryb2tlPSIjNThhNmZmIiBzdHJva2Utd2lkdGg9IjIiPjxwYXRoIGQ9Ik0xMiAyTDIgN2wxMCA1IDEwLTUtMTAtNXoiLz48cGF0aCBkPSJNMiAxN2wxMCA1IDEwLTUiLz48cGF0aCBkPSJNMiAxMmwxMCA1IDEwLTUiLz48Y2lyY2xlIGN4PSIxMiIgY3k9IjIiIHI9IjEuNSIgZmlsbD0iIzU4YTZmZmYiLz48Y2lyY2xlIGN4PSIyIiBjeT0iNyIgcj0iMS41IiBmaWxsPSIjNThhNmZmIi8+PGNpcmNsZSBjeD0iMjIiIGN5PSI3IiByPSIxLjUiIGZpbGw9IiM1OGE2ZmYiLz48Y2lyY2xlIGN4PSIxMiIgY3k9IjciIHI9IjEuNSIgZmlsbD0iIzU4YTZmZiIvPjxjaXJjbGUgY3g9IjIiIGN5PSIxMiIgcj0iMS41IiBmaWxsPSIjNThhNmZmIi8+PGNpcmNsZSBjeD0iMjIiIGN5PSIxMiIgcj0iMS41IiBmaWxsPSIjNThhNmZmIi8+PGNpcmNsZSBjeD0iMTIiIGN5PSIxMiIgcj0iMS41IiBmaWxsPSIjNThhNmZmIi8+PGNpcmNsZSBjeD0iMiIgY3k9IjE3IiByPSIxLjUiIGZpbGw9IiM1OGE2ZmYiLz48Y2lyY2xlIGN4PSIyMiIgY3k9IjE3IiByPSIxLjUiIGZpbGw9IiM1OGE2ZmYiLz48Y2lyY2xlIGN4PSIxMiIgY3k9IjIyIiByPSIxLjUiIGZpbGw9IiM1OGE2ZmYiLz48L3N2Zz4=">

<script src="https://cdn.jsdelivr.net/npm/marked/marked.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/three.js/r128/three.min.js"></script>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">
<style>
:root {
  --bg: #0d1117; --bg-2: #161b22; --bg-3: #1c2333;
  --border: #30363d; --border-2: #21262d;
  --text: #c9d1d9; --text-2: #8b949e; --text-3: #6e7681;
  --accent: #58a6ff; --green: #3fb950; --red: #f85149;
  --gold: #d29922; --orange: #db6d28; --cyan: #22d3ee;
}
[data-theme="light"] {
  --bg: #f6f8fa; --bg-2: #ffffff; --bg-3: #e1e4e8;
  --border: #d0d7de; --border-2: #d8dee4;
  --text: #1f2328; --text-2: #656d76; --text-3: #8c959f;
  --accent: #0969da; --green: #1a7f37; --red: #cf222e;
  --gold: #9a6700; --orange: #bc4c00; --cyan: #1b7c83;
}
[data-theme="classic"] {
  --bg: #1a1a2e; --bg-2: #16213e; --bg-3: #0f3460;
  --border: #334155; --border-2: #1e293b;
  --text: #e2e8f0; --text-2: #94a3b8; --text-3: #64748b;
  --accent: #7c3aed; --green: #10b981; --red: #ef4444;
  --gold: #f59e0b; --orange: #f97316; --cyan: #06b6d4;
}
* { margin:0; padding:0; box-sizing:border-box; }
body { font-family:"Inter",-apple-system,sans-serif; font-size:14px; background:var(--bg); color:var(--text); min-height:100vh; display:flex; }
::selection { background:var(--accent); color:#fff; }
::-webkit-scrollbar { width:5px; height:5px; }
::-webkit-scrollbar-track { background:transparent; }
::-webkit-scrollbar-thumb { background:var(--border); border-radius:3px; }
.sidebar { width:240px; flex-shrink:0; background:var(--bg-2); border-right:1px solid var(--border); display:flex; flex-direction:column; height:100vh; position:sticky; top:0; }
.sidebar-header { padding:1em; border-bottom:1px solid var(--border); }
.sidebar-header h2 { font-size:0.75rem; text-transform:uppercase; letter-spacing:0.08em; color:var(--text-2); font-weight:600; }
.sidebar-list { flex:1; overflow-y:auto; padding:0.5em; }
.run-item { position:relative; padding:0.55rem 0.65rem; margin-bottom:0.25rem; border-radius:6px; cursor:pointer; font-size:0.82rem; transition:background 0.15s,border-left 0.15s; border-left:3px solid transparent; }
.run-item:hover { background:var(--bg-3); }
.run-item.active { background:var(--bg-3); border-left-color:var(--accent); }
.run-item .date { font-size:0.7rem; color:var(--text-3); }
.sidebar-delete {
  position:absolute; top:0.3rem; right:0.3rem;
  width:1.4rem; height:1.4rem; padding:0;
  border:none; border-radius:3px; cursor:pointer;
  font-size:0.75rem; line-height:1.4rem; text-align:center;
  background:transparent; color:var(--text-3);
  opacity:0; transition:opacity 0.15s,color 0.15s;
}
.run-item:hover .sidebar-delete { opacity:1; }
.sidebar-delete:hover { color:var(--red) !important; background:rgba(248,81,73,0.1); }
.run-empty { padding:1.5em 0; text-align:center; color:var(--text-3); font-size:0.78em; }
.tab-btn { background:none;border:none;color:var(--text-3);font-size:0.75em;padding:0.4em 0.7em;cursor:pointer;border-bottom:2px solid transparent;margin-bottom:-1px;font-family:inherit;transition:color 0.15s,border-color 0.15s; }
.tab-btn:hover { color:var(--text-1); }
.tab-btn.active { color:var(--accent);border-bottom-color:var(--accent); }
.vessel-btn { background:none;border:none;color:var(--text-3);font-size:0.72em;padding:0.3em 0.5em;cursor:pointer;border-bottom:2px solid transparent;margin-bottom:-1px;font-family:inherit;transition:color 0.15s,border-color 0.15s; }
.vessel-btn:hover { color:var(--text-1); }
.vessel-btn.active { color:var(--green);border-bottom-color:var(--green); }
.final-summary { max-width:100%; margin:0.5em auto 0.75em; display:flex; justify-content:center; gap:0.75rem; flex-wrap:nowrap; padding:0.5rem 0.8rem; background:var(--bg-3); border:1px solid var(--border); border-radius:8px; overflow-x:auto; }
.summary-stat { text-align:center; display:flex; flex-direction:column; align-items:center; gap:0.1rem; min-width:0; flex-shrink:1; }
.stat-icon { font-size:0.85rem; line-height:1; opacity:0.7; }
.stat-value { font-size:1.05rem; font-weight:bold; color:var(--green); }
.stat-label { font-size:0.62rem; color:var(--text-3); text-transform:uppercase; letter-spacing:0.04em; }
.agent-grid { display:flex; gap:0.5rem; flex-wrap:wrap; margin:0.5em 0; }
.agent-card { background:var(--bg-3); border:1px solid var(--border); border-radius:6px; padding:0.4rem 0.7rem; min-width:180px; flex:1; }
.agent-card .ac-header { display:flex; align-items:center; gap:0.4rem; font-size:0.78em; font-weight:600; margin-bottom:0.2rem; }
.agent-card .ac-status { font-size:0.65em; opacity:0.7; }
.agent-card .ac-activity { font-size:0.65em; color:var(--text-2); white-space:nowrap; overflow:hidden; text-overflow:ellipsis; max-width:220px; }
.main { flex:1; display:flex; flex-direction:column; min-width:0; }
nav { position:sticky; top:0; z-index:100; background:color-mix(in srgb,var(--bg) 90%,transparent); backdrop-filter:blur(8px); border-bottom:1px solid var(--border); padding:0.7em 1.5em; display:flex; align-items:center; gap:1em; }
nav .brand { font-size:1.1rem; font-weight:700; letter-spacing:-0.01em; display:flex; align-items:center; gap:0.5em; }
nav .brand-logo { height:26px; width:auto; }
nav .brand span { color:var(--accent); }
nav .selected-run { font-size:0.82rem; color:var(--text-2); font-family:'JetBrains Mono',monospace; }
nav .nav-right { margin-left:auto; display:flex; align-items:center; gap:0.75em; }
.theme-switcher { display:flex; gap:2px; }
.theme-pill { padding:0.2rem 0.6rem; font-size:0.7rem; cursor:pointer; border-radius:4px; color:var(--text-3); transition:color 0.15s,background 0.15s; }
.theme-pill:hover { color:var(--text); background:var(--bg-3); }
.theme-pill.active { color:var(--accent); background:rgba(88,166,255,0.08); }
nav .nav-link { font-size:0.75rem; color:var(--text-2); text-decoration:none; }
nav .nav-link:hover { color:var(--accent); }
.sse-status { font-size:0.7rem; display:inline-flex; align-items:center; gap:4px; color:var(--green); }
.sse-status::before { content:''; width:6px; height:6px; border-radius:50%; background:var(--green); display:inline-block; }
.sse-status.disconnected { color:var(--red); }
.sse-status.disconnected::before { background:var(--red); }
.content { flex:1; display:flex; overflow:hidden; }
.panel { flex:1; padding:1.5em; overflow-y:auto; }
.terminal-window { background:var(--bg-2); border:1px solid var(--border); border-radius:10px; overflow:hidden; font-family:'JetBrains Mono',monospace; min-height:200px; }
.terminal-titlebar { display:flex; align-items:center; gap:0.65rem; padding:0.5rem 0.85rem; background:var(--bg-3); border-bottom:1px solid var(--border); font-size:0.72rem; user-select:none; }
.terminal-dots { display:flex; gap:5px; }
.terminal-dots span { width:9px; height:9px; border-radius:50%; display:inline-block; }
.tdot-red { background:#ff5f56; }
.tdot-yellow { background:#ffbd2e; }
.tdot-green { background:#27c93f; }
.terminal-title { color:var(--text-3); font-size:0.72rem; position:absolute; left:50%; transform:translateX(-50%); }
.terminal-body { padding:0.85rem; max-height:800px; overflow-y:auto; font-size:0.82rem; line-height:1.6; scroll-behavior:smooth; word-break:break-word; }
.terminal-body:has(.md-content) { white-space:normal; }
.md-content { font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif; font-size:0.85rem; line-height:1.7; color:var(--text-1); }
.md-content h1 { font-size:1.4em; font-weight:700; margin:1.2em 0 0.6em; padding-bottom:0.3em; border-bottom:1px solid var(--border); color:var(--text-1); }
.md-content h2 { font-size:1.15em; font-weight:600; margin:1em 0 0.5em; padding-bottom:0.2em; border-bottom:1px solid var(--border); color:var(--text-1); }
.md-content h3 { font-size:1em; font-weight:600; margin:0.8em 0 0.4em; color:var(--text-1); }
.md-content h4 { font-size:0.9em; font-weight:600; margin:0.6em 0 0.3em; color:var(--text-2); }
.md-content p { margin:0.5em 0; }
.md-content ul, .md-content ol { margin:0.5em 0; padding-left:1.5em; }
.md-content li { margin:0.2em 0; }
.md-content table { border-collapse:collapse; width:100%; margin:0.8em 0; font-size:0.78rem; }
.md-content th { background:var(--bg-3); border:1px solid var(--border); padding:0.4em 0.6em; text-align:left; font-weight:600; }
.md-content td { border:1px solid var(--border); padding:0.3em 0.6em; }
.md-content tr:nth-child(even) { background:var(--bg-2); }
.md-content code { font-family:'JetBrains Mono',monospace; font-size:0.85em; background:var(--bg-3); padding:0.1em 0.3em; border-radius:3px; }
.md-content pre { background:var(--bg-3); border:1px solid var(--border); border-radius:6px; padding:0.8em; overflow-x:auto; margin:0.8em 0; }
.md-content pre code { background:none; padding:0; font-size:0.8rem; }
.md-content blockquote { border-left:3px solid var(--accent); margin:0.8em 0; padding:0.3em 0.8em; color:var(--text-2); background:var(--bg-2); border-radius:0 4px 4px 0; }
.md-content hr { border:none; border-top:1px solid var(--border); margin:1em 0; }
.md-content strong { font-weight:600; color:var(--text-1); }
.md-content a { color:var(--accent); text-decoration:none; }
.md-content a:hover { text-decoration:underline; }
.md-content img { max-width:100%; border-radius:6px; }
.stat-grid { display:grid; grid-template-columns:repeat(4,1fr); gap:0.5em; margin-bottom:1em; }
.stat-card { background:var(--bg-2); border:1px solid var(--border); border-radius:6px; padding:0.75em; text-align:center; }
.stat-card .label { font-size:0.7em; color:var(--text-2); text-transform:uppercase; letter-spacing:0.04em; }
.stat-card .value { font-size:1.2em; font-weight:700; margin-top:0.15em; }
.panel-right { width:340px; padding:1.25em; overflow-y:auto; background:var(--bg-2); border-left:1px solid var(--border); flex-shrink:0; display:flex; flex-direction:column; }
.panel-right h3 { font-size:0.85em; font-weight:600; display:flex; align-items:center; gap:0.5em; margin-bottom:1em; color:var(--text); }
.hero-badge { display:inline-flex; align-items:center; gap:0.5rem; background:rgba(88,166,255,0.08); border:1px solid rgba(88,166,255,0.25); border-radius:999px; padding:0.35rem 1rem; font-size:0.75rem; color:var(--accent); text-transform:uppercase; letter-spacing:0.08em; margin-bottom:1.5rem; }
.sirb-hero { text-align:center; padding:1.5rem 1rem; }
.sirb-hero h1 { font-size:clamp(1.6rem,4vw,2.6rem); font-weight:700; letter-spacing:-0.01em; margin-bottom:0.4rem; }
.accent-gradient { background:linear-gradient(135deg,var(--accent),#79c0ff); -webkit-background-clip:text; -webkit-text-fill-color:transparent; background-clip:text; }
.sirb-hero p { color:var(--text-3); font-size:0.95rem; max-width:540px; margin:0 auto 1.5rem; line-height:1.6; }
.live-dot { display:inline-block; width:6px; height:6px; border-radius:50%; background:var(--green); animation:pulse-dot 1.5s ease-in-out infinite; }
@keyframes pulse-dot { 0%,100%{opacity:1;transform:scale(1)} 50%{opacity:.5;transform:scale(.85)} }
.form-group { margin-bottom:0.75em; }
.form-group label { display:block; font-size:0.8em; color:var(--text-2); margin-bottom:0.25em; font-weight:500; }
.form-group input,.form-group select { width:100%; padding:0.5em 0.65em; background:var(--bg); border:1px solid var(--border); border-radius:6px; color:var(--text); font-family:inherit; font-size:0.85em; outline:none; transition:border-color 0.2s,box-shadow 0.2s; }
.form-group input:focus,.form-group select:focus { border-color:var(--accent); box-shadow:0 0 0 2px rgba(88,166,255,0.15); }
.btn { padding:0.5em 1em; border:none; border-radius:6px; cursor:pointer; font-size:0.85em; font-weight:600; font-family:inherit; transition:opacity 0.2s,transform 0.1s; }
.btn:active { transform:scale(0.97); }
.btn-primary { background:#1f6feb; color:#fff; }
.btn-primary:hover { background:#388bfd; }
.btn-danger { background:#da3633; color:#fff; }
.btn-danger:hover { background:#f85149; }
.btn:disabled { opacity:0.5; cursor:not-allowed; }
.btn-row { display:flex; gap:0.5em; margin-top:0.5em; }
.method-panel { margin-bottom:0.5em; padding:0.5em; background:var(--bg-3); border-radius:6px; }
.method-panel .form-group { margin:0; }
.method-panel input,.method-panel select { background:var(--bg); border:1px solid var(--border); border-radius:6px; padding:0.4em 0.6em; color:var(--text); font-size:0.82em; font-family:'JetBrains Mono',monospace; outline:none; }
.method-panel input:focus { border-color:var(--accent); }
.phase-line { padding:0.2rem 0; opacity:0; animation:phase-appear 0.25s ease forwards; }
@keyframes phase-appear { from{opacity:0;transform:translateY(-3px)} to{opacity:1;transform:translateY(0)} }
.phase-complete { color:var(--green); }
.phase-error { color:var(--red); }
.phase-info { color:var(--accent); }
.phase-verbose { color:var(--text-3); font-size:0.75em; }
.final-summary { max-width:100%; margin-top:1em; display:flex; justify-content:center; gap:0.75rem; flex-wrap:nowrap; padding:0.6rem 0.8rem; background:var(--bg-3); border:1px solid var(--border); border-radius:8px; animation:fadeInUp 0.4s ease; overflow-x:auto; }
.summary-stat { text-align:center; display:flex; flex-direction:column; align-items:center; gap:0.1rem; }
.stat-icon { font-size:0.85rem; opacity:0.7; }
.stat-val { font-size:1.1rem; font-weight:bold; color:var(--accent); }
.stat-lbl { font-size:0.65rem; color:var(--text-2); text-transform:uppercase; letter-spacing:0.04em; }
@keyframes fadeInUp { from{opacity:0;transform:translateY(20px)} to{opacity:1;transform:translateY(0)} }
.fade-in-up { animation:fadeInUp 0.4s ease; }
.pulse-amber { animation:pulse-amber 2s ease-in-out infinite; }
@keyframes pulse-amber { 0%,100%{background:rgba(210,153,34,0.2);border-color:var(--gold)} 50%{background:rgba(210,153,34,0.05);border-color:transparent} }
.log-line { font-family:'JetBrains Mono',monospace; font-size:0.8em; color:var(--text-3); white-space:pre-wrap; }
@media (max-width:900px) { .sidebar{display:none} .panel-right{width:280px} .stat-grid{grid-template-columns:repeat(2,1fr)} }
@media (max-width:600px) { .content{flex-direction:column} .panel-right{width:100%;border-left:none;border-top:1px solid var(--border)} nav{padding:0.6em 1em;flex-wrap:wrap} nav .selected-run{display:none} }
</style>
</head>
<body>
<div class="sidebar">
  <div class="sidebar-header"><h2>Run History</h2></div>
  <div class="sidebar-list" id="run-list"><div class="run-empty">No runs yet</div></div>
</div>
<div class="main">
  <nav>
    <div class="brand" style="cursor:pointer;" onclick="goHome()"><img src="/logo" class="brand-logo" alt="SIRB"><span>SIRB</span> Swarm</div>
    <span class="selected-run" id="selected-run">No run selected</span>
    <div class="nav-right">
      <a href="https://github.com/ahmdngi/sirb" target="_blank" class="nav-link">GitHub</a>
      <div class="theme-switcher">
        <span class="theme-pill active" data-theme="dark" onclick="applyTheme('dark')">Dark</span>
        <span class="theme-pill" data-theme="light" onclick="applyTheme('light')">Light</span>
        <span class="theme-pill" data-theme="classic" onclick="applyTheme('classic')">Classic</span>
      </div>
      <span class="sse-status" id="sse-status">connected</span>
    </div>
  </nav>
  <div class="content">
    <div class="panel">
      <div id="live-stats" class="stat-grid"></div>
      <div id="agent-cards" class="agent-grid"></div>
      <div class="sirb-hero" id="sirb-hero">
        <div class="hero-badge"><span class="live-dot"></span> SIRB v0.3</div>
        <h1><span class="accent-gradient">Agentic</span> Swarm Terminal</h1>
        <p>Multi-agent swarm orchestration. Select a worker, paste targets, parse to verify, then launch a parallel investigation.</p>
      </div>
      <div id="report-tabs" style="display:none;border-bottom:1px solid var(--border);margin-bottom:0.5em;">
      </div>
      <div id="final-summary" class="final-summary" style="display:none;">
      </div>
      <div class="terminal-window">
        <div class="terminal-titlebar"><div class="terminal-dots"><span class="tdot-red"></span><span class="tdot-yellow"></span><span class="tdot-green"></span></div><span class="terminal-title" id="report-title">swarm-report.md</span></div>
        <div class="terminal-body" id="assessment-view">
          <div style="color:var(--text-3);font-size:0.82rem;line-height:1.6;">
            <div style="color:var(--accent);font-weight:600;">$ sirb --status</div>
            <div style="margin-top:0.5rem;">SIRB Swarm v0.3 — Agentic task orchestration engine</div>
            <div style="margin-top:0.3rem;color:var(--text-3);">No active runs. Select a worker, paste targets, and launch a swarm investigation.</div>
            <div style="margin-top:0.3rem;color:var(--text-3);">Past runs are available in the left sidebar.</div>
            <div style="margin-top:0.5rem;color:var(--accent);">$ <span class="prompt-cursor">▊</span></div>
          </div>
        </div>
      </div>
    </div>
    <div class="panel-right">
      <h3><span style="color:var(--accent)">▶</span> Launch Scan</h3>
      <div class="form-group"><label>Worker</label><select id="worker-select" style="background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:0.4rem 0.6rem;font-family:JetBrains Mono,monospace;font-size:0.78rem;color:var(--text-2);outline:none;cursor:pointer;" onchange="onWorkerChange()"><option value="">Loading...</option></select></div>
      <!-- Dynamic form rendered from worker schema -->
      <div id="worker-form"></div>
      <!-- Parse preview -->
      <div id="parse-preview" style="display:none;"></div>
      <div class="form-group"><label>Model</label><select id="model-select" style="background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:0.4rem 0.6rem;font-family:JetBrains Mono,monospace;font-size:0.78rem;color:var(--text-2);outline:none;cursor:pointer;width:100%;"><option value="">Loading models...</option></select></div>
      <div class="btn-row"><button class="btn btn-primary" id="run-btn" onclick="launchRun()">▶ Run</button><button class="btn btn-danger" id="stop-btn" onclick="stopRun()" style="display:none">■ Stop</button></div>
      <hr style="border-color:var(--border);margin:1em 0;" />
      <div id="globe-container" style="position:sticky;bottom:0;width:100%;height:200px;border-radius:8px;overflow:hidden;margin-top:auto;"></div>
  </div>
</div>
<script>
const AGENT_COLORS=["#f85149","#58a6ff","#3fb950","#d2a8ff","#f0883e","#79c0ff","#ff7b72","#a5d6ff"];
let currentRunId=null,reportCache={};
var _userScrolledSIRB=false;
function initSirbAutoScroll(){const av=document.getElementById("assessment-view");if(!av)return;av.addEventListener("scroll",function(){var threshold=50;var atBottom=av.scrollHeight-av.scrollTop-av.clientHeight<threshold;_userScrolledSIRB=!atBottom;});}
setTimeout(initSirbAutoScroll,500);
const sseEl=document.getElementById("sse-status"),evtSource=new EventSource("/events");
evtSource.onopen=()=>{sseEl.textContent="connected";sseEl.className="sse-status";};
evtSource.onerror=()=>{sseEl.textContent="disconnected";sseEl.className="sse-status disconnected";};
evtSource.onmessage=(e)=>{try{const d=JSON.parse(e.data);if(d.type=="stats"&&d.data){if(!currentRunId||d.data.run_id!==currentRunId)return;liveStats(d.data)}}catch(_){}};
function liveStats(d){const g=document.getElementById("live-stats");if(!g)return;g.innerHTML="";for(const[k,v]of Object.entries(d)){if(k==="agents")continue;const c=k=="Failed"||k=="Error"?"var(--red)":k=="Running"?"var(--accent)":"var(--green)";g.innerHTML+='<div class="stat-card"><div class="label">'+k+'</div><div class="value" style="color:'+c+'">'+v+'</div></div>'}const ac=document.getElementById("agent-cards");if(!ac)return;ac.innerHTML="";if(!d.agents||!d.agents.length)return;d.agents.forEach(a=>{const st=a.status=="success"?"done":a.status;const sc=st=="done"?"#3fb950":st=="running"?"#58a6ff":"#f85149";const icon=st=="done"?"✅":st=="running"?"⏳":"❌";ac.innerHTML+='<div class="agent-card"><div class="ac-header"><span>'+icon+" "+a.label+'</span><span class="ac-status" style="color:'+sc+'">'+st+'</span></div><div class="ac-activity">'+(a.activity||"Waiting\u2026")+'</div></div>'});const av=document.getElementById("assessment-view");if(!av)return;if(d.Status=="running"&&d.run_id===currentRunId&&!reportCache.swarm){let h='<div style="padding:0.3rem 0;font-family:JetBrains Mono,monospace;font-size:0.75rem;line-height:1.5;">';d.agents.forEach((a,i)=>{const c=AGENT_COLORS[i%AGENT_COLORS.length];const st=a.status=="success"?"done":a.status;const icon=st=="done"?"✅":st=="running"?"⏳":"❌";const lines=(a.activity||"Waiting...").split("\n");lines.forEach(l=>{const t=l.trim();if(!t)return;h+='<div style="display:flex;gap:0.5rem;"><span style="color:'+c+';font-weight:600;flex-shrink:0;">['+a.label+']</span><span style="color:var(--text-2);overflow:hidden;text-overflow:ellipsis;">'+t+'</span></div>'})});h+="</div>";av.innerHTML=h;if(!_userScrolledSIRB)av.scrollTop=av.scrollHeight}}
async function loadRuns(){const r=await fetch("/runs");const runs=await r.json();const el=document.getElementById("run-list");el.innerHTML=runs.map(r=>{const dt=r.generated_at||new Date(r.mtime*1000).toLocaleString();const a=r.id===currentRunId?"active":"";return'<div class="run-item '+a+'" onclick="selectRun(\''+r.id+'\')" id="ri-'+r.id+'"><div style="font-weight:'+(r.has_assessment?"600":"400")+';font-size:0.82em">'+r.id.slice(0,16)+'</div><div class="date">'+dt+(r.targets?" · "+r.targets+" targets":"")+'</div><button class="sidebar-delete" onclick="event.stopPropagation();deleteRun(\''+r.id+'\')" title="Delete run">🗑</button></div>'}).join("");if(!runs.length)el.innerHTML='<div class="run-empty">No runs yet</div>'}

async function deleteRun(rid){if(!confirm("Delete run "+rid+"?"))return;const r=await fetch("/run/"+rid,{method:"DELETE"});const d=await r.json();if(d.status==="deleted"){if(currentRunId===rid){currentRunId=null;document.getElementById("selected-run").textContent="No run selected";document.getElementById("assessment-view").innerHTML='<span style="color:var(--text-3)">Select a run to view its report.</span>';document.getElementById("report-tabs").style.display="none";document.getElementById("final-summary").style.display="none"}loadRuns()}else{alert("Delete failed: "+(d.error||"unknown"))}}

async function selectRun(rid){currentRunId=rid;_userScrolledSIRB=false;document.getElementById("sirb-hero").style.display="none";document.getElementById("selected-run").textContent="Run: "+rid;document.querySelectorAll(".run-item").forEach(e=>e.classList.remove("active"));const el=document.getElementById("ri-"+rid);if(el)el.classList.add("active");reportCache={};const r=await fetch("/run/"+rid+"/report");reportCache.swarm=await r.text();if(reportCache.swarm.includes("Report not ready")){reportCache.swarm=null;document.getElementById("assessment-view").innerHTML='<span style="color:var(--text-3)">⏳ Swarm in progress... agents running.</span>';document.getElementById("report-tabs").style.display="none";document.getElementById("final-summary").style.display="none";return}const tr=await fetch("/run/"+rid+"/tracking.json").then(x=>x.json()).catch(()=>({}));const workerName=tr.worker||"shipcrawler";const reportTabs=await fetch("/api/workers/"+encodeURIComponent(workerName)+"/report-tabs").then(x=>x.json()).catch(()=>[{id:"swarm",label:"Swarm",icon:"📋",type:"file",path:"swarm-report.md"}]);await renderReportTabs(reportTabs,rid,workerName);document.getElementById("report-tabs").style.display="";switchTab("swarm");renderTab("swarm");const statsSchema=await fetch("/api/workers/"+encodeURIComponent(workerName)+"/stats-schema").then(x=>x.json()).catch(()=>[{key:"tool_calls",icon:"⚙️",label:"Tool Calls"},{key:"duration",icon:"⏱",label:"Duration"},{key:"model",icon:"🧠",label:"Model"}]);const ss=document.getElementById("final-summary");ss.innerHTML=statsSchema.map(s=>'<div class="summary-stat"><span class="stat-icon">'+s.icon+'</span><span class="stat-value" id="s-'+s.key+'">—</span><span class="stat-label">'+s.label+'</span></div>').join("");fetch("/run/"+rid+"/stats").then(x=>x.json()).then(d=>{statsSchema.forEach(s=>{const el=document.getElementById("s-"+s.key);if(el)el.textContent=d[s.key]||"—"})}).catch(()=>{});document.getElementById("final-summary").style.display="flex";showExecutiveSummary(rid)}
async function showExecutiveSummary(rid){try{const r=await fetch("/run/"+rid+"/targets");const targets=await r.json();if(!targets.length)return;let html='<div style="font-family:JetBrains Mono,monospace;font-size:0.82rem;line-height:1.6;"><div style="color:var(--accent);font-weight:600;margin-bottom:0.5rem;">$ sirb --summary '+rid+'</div>';for(const t of targets){const fr=await fetch("/run/"+rid+"/vessel/"+t.target+"/analyst-report.md");const text=await fr.text();const m=text.match(/##\s*EXECUTIVE\s*SUMMARY\s*\n([\s\S]*?)(?:\n##|\n---|\n\*\*Overall)/i);if(m){let summary=m[1].trim().split("\n\n")[0].trim();summary=summary.replace(/\*\*/g,"").replace(/\[.*?\]/g,"").substring(0,500);const warnings=[];if(/shadow\s*fleet|dark\s*fleet/i.test(text))warnings.push("🔴 SHADOW FLEET");if(/sanctioned|sanctions/i.test(text))warnings.push("🟡 SANCTIONED");if(/AIS\s*shutdown|AIS\s*dark|AIS\s*off/i.test(text))warnings.push("🟠 AIS DARK");if(/kinetic|drone|attack|strike/i.test(text))warnings.push("💥 KINETIC THREAT");if(/casualty|repairing/i.test(text))warnings.push("⚠️ IN CASUALTY");html+='<div style="margin-bottom:0.75rem;padding:0.5rem;border-left:3px solid '+(warnings.length?"var(--red)":"var(--green)")+';background:var(--bg-2);border-radius:0 6px 6px 0;"><div style="font-weight:600;color:var(--accent);">'+t.target+'</div>';if(warnings.length)html+='<div style="margin:0.3rem 0;">'+warnings.join("  ")+"</div>";html+='<div style="color:var(--text-2);margin-top:0.2rem;">'+summary+"...</div></div>"}}html+="</div>";document.getElementById("assessment-view").innerHTML=html}catch(_){}}
async function renderReportTabs(tabs,rid,workerName){let html="";for(const t of tabs){if(t.type==="file"){html+='<button class="tab-btn" data-tab="'+t.id+'" onclick="switchTab(\''+t.id+'\')">'+t.icon+" "+t.label+"</button>"}else if(t.type==="per_target"){const ep=t.endpoint.replace("{rid}",rid);try{const targets=await fetch(ep).then(x=>x.json());for(const tgt of targets){html+='<button class="vessel-btn" onclick="loadVesselFile(\''+rid+'\',\''+tgt.target+'\',\''+tgt.files[0]+'\')">'+t.icon+" "+tgt.target.slice(0,12)+"</button>"}}catch(_){}}}document.getElementById("report-tabs").innerHTML=html}



async function loadVesselFile(rid,target,file){const r=await fetch("/run/"+rid+"/vessel/"+target+"/"+file);const text=await r.text();const key="vessel_"+target+"_"+file;reportCache[key]=text;switchTab(key);document.getElementById("report-title").textContent=target+"/"+file;const view=document.getElementById("assessment-view");view.innerHTML='<div class="md-content">'+marked.parse(text)+'</div>'}

function switchTab(tab){document.querySelectorAll(".tab-btn,.vessel-btn").forEach(b=>b.classList.remove("active"));if(tab=="swarm"){var sb=document.querySelector('[data-tab="swarm"]');if(sb)sb.classList.add("active");renderTab("swarm")}else{document.querySelectorAll(".vessel-btn").forEach(b=>{if(b.textContent.includes(tab))b.classList.add("active")});renderTab(tab)}}

function renderTab(tab){const view=document.getElementById("assessment-view");const content=reportCache[tab];if(tab=="swarm"){document.getElementById("report-title").textContent="swarm-report.md"}else if(tab=="connections"){document.getElementById("report-title").textContent="connections.md"}if(content){view.innerHTML='<div class="md-content">'+marked.parse(content)+'</div>'}else if(tab=="swarm"){view.innerHTML='<span style="color:var(--text-3)">Loading swarm report...</span>'}else if(tab=="connections"){view.innerHTML='<span style="color:var(--text-3)">⏳ Connections analysis not ready yet.</span>'}}



async function loadWorkers(){try{const r=await fetch("/api/workers?_="+Date.now());const data=await r.json();const sel=document.getElementById("worker-select");if(!data.length){sel.innerHTML='<option value="">No workers installed</option>';return}sel.innerHTML=data.map(w=>'<option value="'+w.name+'">'+w.name+(w.description?" — "+w.description:"")+"</option>").join("");if(!sel.value&&data.length)sel.selectedIndex=0;onWorkerChange()}catch(_){document.getElementById("worker-select").innerHTML='<option value="shipcrawler">shipcrawler</option>'}}
let _workerSchema=null,_parsedTargets=[];
async function onWorkerChange(){const w=document.getElementById("worker-select").value;if(!w)return;document.getElementById("parse-preview").style.display="none";_parsedTargets=[];try{const r=await fetch("/api/workers/"+encodeURIComponent(w)+"/schema?_="+Date.now());const schema=await r.json();_workerSchema=schema;renderWorkerForm(schema);const pf=document.getElementById("wf-profile");if(pf){loadProfileModels(pf.value||"")}}catch(_){document.getElementById("worker-form").innerHTML='<div style="color:var(--text-3);font-size:0.78rem;">No form available for this worker</div>'}}
function renderWorkerForm(schema){if(!schema||!schema.fields){document.getElementById("worker-form").innerHTML="";return}let html="";schema.fields.forEach(f=>{const fid="wf-"+f.name;var onchange="";if(f.name==="profile"){onchange=' onchange="onProfileFormChange()"'}if(f.type==="textarea"){html+='<div class="form-group"><label>'+f.label+'</label><textarea id="'+fid+'" placeholder="'+(f.placeholder||"")+'" style="width:100%;min-height:80px;background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:0.4rem 0.6rem;font-family:JetBrains Mono,monospace;font-size:0.78rem;color:var(--text-1);outline:none;resize:vertical;">'+(f.default||"")+'</textarea></div>'}else if(f.type==="select"){html+='<div class="form-group"><label>'+f.label+'</label><select id="'+fid+'"'+onchange+' style="background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:0.4rem 0.6rem;font-family:JetBrains Mono,monospace;font-size:0.78rem;color:var(--text-2);outline:none;cursor:pointer;">'+(f.options||[]).map(o=>'<option value="'+o.value+'"'+(o.value===(f.default||"")?" selected":"")+" >"+o.label+"</option>").join("")+'</select></div>'}else if(f.type==="number"){html+='<div class="form-group"><label>'+f.label+'</label><input id="'+fid+'" type="number" placeholder="'+(f.placeholder||"")+'" value="'+(f.default||"")+'" style="width:100%;background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:0.4rem 0.6rem;font-family:JetBrains Mono,monospace;font-size:0.78rem;color:var(--text-1);outline:none;" /></div>'}else{html+='<div class="form-group"><label>'+f.label+'</label><input id="'+fid+'" type="text" placeholder="'+(f.placeholder||"")+'" value="'+(f.default||"")+'" style="width:100%;background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:0.4rem 0.6rem;font-family:JetBrains Mono,monospace;font-size:0.78rem;color:var(--text-1);outline:none;" /></div>'}if(f.parse){html+='<button class="btn" style="margin-top:0.3rem;width:100%;" onclick="parseInput(\''+f.name+'\')">🔍 Parse</button>'}});document.getElementById("worker-form").innerHTML=html}
function onProfileFormChange(){const el=document.getElementById("wf-profile");if(el)loadProfileModels(el.value)}
async function parseInput(fieldName){const w=document.getElementById("worker-select").value;const input=document.getElementById("wf-"+fieldName).value;if(!input.trim()){alert("Enter some text first");return}try{const r=await fetch("/api/workers/"+encodeURIComponent(w)+"/parse",{method:"POST",headers:{"Content-Type":"application/x-www-form-urlencoded"},body:"input="+encodeURIComponent(input)});const data=await r.json();_parsedTargets=data.targets||[];showParsePreview(_parsedTargets)}catch(e){alert("Parse failed: "+e)}}
function showParsePreview(targets){const el=document.getElementById("parse-preview");if(!targets.length){el.style.display="block";el.innerHTML='<div style="color:var(--red);font-size:0.78rem;padding:0.5rem;">No targets detected.</div>';return}let html='<div style="background:var(--bg-3);border:1px solid var(--border);border-radius:6px;padding:0.5rem;margin-top:0.5rem;"><div style="font-size:0.75rem;color:var(--accent);font-weight:600;margin-bottom:0.3rem;">Found '+targets.length+' target'+(targets.length>1?"s":"")+':</div>';html+='<table style="width:100%;font-size:0.75rem;font-family:JetBrains Mono,monospace;"><thead><tr><th style="text-align:left;padding:0.2rem 0.4rem;color:var(--text-2);">#</th><th style="text-align:left;padding:0.2rem 0.4rem;color:var(--text-2);">Type</th><th style="text-align:left;padding:0.2rem 0.4rem;color:var(--text-2);">Target</th></tr></thead><tbody>';targets.forEach((t,i)=>{html+='<tr style="border-bottom:1px solid var(--border);"><td style="padding:0.2rem 0.4rem;color:var(--text-3);">'+(i+1)+'</td><td style="padding:0.2rem 0.4rem;color:var(--accent);">'+t.type+'</td><td style="padding:0.2rem 0.4rem;color:var(--text-1);">'+t.target+'</td></tr>'});html+='</tbody></table></div>';el.style.display="block";el.innerHTML=html}
async function launchRun(){const worker=document.getElementById("worker-select").value;const model=document.getElementById("model-select").value;if(!worker){alert("Select a worker first");return}if(_parsedTargets.length===0&&_workerSchema&&_workerSchema.parse){alert("Click Parse first to verify targets before running");return}if(_parsedTargets.length===0){alert("No targets detected — parse your input first");return}const targets=_parsedTargets.map(t=>t.target).join(" ");const params={mmsi:targets,worker,mode:"deep",profile:"",model};if(_workerSchema&&_workerSchema.fields){_workerSchema.fields.forEach(f=>{const el=document.getElementById("wf-"+f.name);if(el){if(f.name==="targets"){params.mmsi=_parsedTargets.map(t=>t.target).join(" ")}else if(f.name==="mode"){params.mode=el.value}else if(f.name==="profile"){params.profile=el.value}else{params[f.name]=el.value}}})}const body=Object.entries(params).map(([k,v])=>k+"="+encodeURIComponent(v)).join("&");const r=await fetch("/run/new",{method:"POST",headers:{"Content-Type":"application/x-www-form-urlencoded"},body});handleLaunchResponse(r)}
async function handleLaunchResponse(p){document.getElementById("run-btn").disabled=true;document.getElementById("run-btn").textContent="Running...";document.getElementById("stop-btn").style.display="inline-block";try{const r=await p;const d=await r.json();if(d.run_id){currentRunId=d.run_id;_userScrolledSIRB=false;document.getElementById("sirb-hero").style.display="none";document.getElementById("selected-run").textContent="Run: "+d.run_id;reportCache={};document.getElementById("report-tabs").style.display="none";document.getElementById("report-tabs").innerHTML="";document.getElementById("final-summary").style.display="none";document.getElementById("live-stats").innerHTML="";document.getElementById("agent-cards").innerHTML="";document.getElementById("assessment-view").innerHTML='<span style="color:var(--accent)">⏳ Run started... agents initializing.</span>';setTimeout(loadRuns,1000)}else if(d.error){alert("Error: "+d.error)}}catch(e){alert("Failed: "+e)}document.getElementById("run-btn").disabled=false;document.getElementById("run-btn").textContent="▶ Run"}
async function stopRun(){if(!currentRunId)return;await fetch("/run/"+currentRunId+"/stop",{method:"POST"});document.getElementById("stop-btn").style.display="none";document.getElementById("selected-run").textContent="Stopped: "+currentRunId;setTimeout(loadRuns,1000)}
async function loadProfileModels(profile){try{const r=await fetch("/api/profiles/models?_="+Date.now());const data=await r.json();const pk=profile||"";const ms=data[pk]||[];const sel=document.getElementById("model-select");sel.innerHTML=ms.map(m=>'<option value="'+m.value+'">'+m.label+"</option>").join("");if(!sel.value)sel.selectedIndex=0}catch(_){document.getElementById("model-select").innerHTML='<option value="deepseek-v4-flash">DeepSeek V4 Flash</option>'}}
function goHome(){currentRunId=null;document.getElementById("sirb-hero").style.display="";document.getElementById("selected-run").textContent="No run selected";document.querySelectorAll(".run-item").forEach(e=>e.classList.remove("active"));document.getElementById("report-tabs").style.display="none";document.getElementById("report-tabs").innerHTML="";document.getElementById("final-summary").style.display="none";document.getElementById("final-summary").innerHTML="";document.getElementById("live-stats").innerHTML="";document.getElementById("agent-cards").innerHTML="";document.getElementById("assessment-view").innerHTML='<div style="color:var(--text-3);font-size:0.82rem;line-height:1.6;"><div style="color:var(--accent);font-weight:600;">$ sirb --status</div><div style="margin-top:0.5rem;">SIRB Swarm v0.3 — Agentic task orchestration engine</div><div style="margin-top:0.3rem;color:var(--text-3);">No active runs. Select a worker, paste targets, and launch a swarm investigation.</div><div style="margin-top:0.3rem;color:var(--text-3);">Past runs are available in the left sidebar.</div><div style="margin-top:0.5rem;color:var(--accent);">$ <span class="prompt-cursor">▊</span></div></div>';reportCache={}}
function applyTheme(t){document.documentElement.setAttribute('data-theme',t);document.querySelectorAll('.theme-pill').forEach(p=>p.classList.toggle('active',p.dataset.theme===t));try{localStorage.setItem('sirb-theme',t)}catch(_){}}
function loadTheme(){try{const t=localStorage.getItem('sirb-theme')||'dark';applyTheme(t)}catch(_){applyTheme('dark')}}
loadTheme();
loadRuns();loadProfileModels("");loadWorkers();setInterval(loadRuns,5000);
// Clear live stats on page load — only show when a run is selected/started
document.getElementById("live-stats").innerHTML="";document.getElementById("agent-cards").innerHTML="";
</script>
<footer style="text-align:center;padding:0.5rem;font-size:0.7rem;color:var(--text-3);border-top:1px solid var(--border);">Built by Ahmed Nagi Nasr · SIRB v0.3 · AI Agent</footer>
<script>
// Globe — Three.js rotating particle globe
(function(){if(typeof THREE==='undefined')return;const c=document.getElementById('globe-container');if(!c)return;const W=c.clientWidth||240,H=200;const sc=new THREE.Scene();const cam=new THREE.PerspectiveCamera(45,W/H,0.1,1000);cam.position.z=3.2;const r=new THREE.WebGLRenderer({alpha:true,antialias:true});r.setSize(W,H);r.setPixelRatio(Math.min(window.devicePixelRatio,2));c.appendChild(r.domElement);const gR=1.4,pC=1800,pos=new Float32Array(pC*3),col=new Float32Array(pC*3);for(let i=0;i<pC;i++){const t=Math.random()*Math.PI*2,p=Math.acos(2*Math.random()-1),rr=gR+(Math.random()-0.5)*0.04;pos[i*3]=rr*Math.sin(p)*Math.cos(t);pos[i*3+1]=rr*Math.cos(p);pos[i*3+2]=rr*Math.sin(p)*Math.sin(t);const b=0.4+Math.random()*0.6;col[i*3]=0.3*b;col[i*3+1]=0.55*b;col[i*3+2]=1.0*b;}const gG=new THREE.BufferGeometry();gG.setAttribute('position',new THREE.BufferAttribute(pos,3));gG.setAttribute('color',new THREE.BufferAttribute(col,3));const gM=new THREE.PointsMaterial({size:0.035,vertexColors:true,transparent:true,opacity:0.95,blending:THREE.AdditiveBlending,depthWrite:false});const gl=new THREE.Points(gG,gM);sc.add(gl);const rM=new THREE.LineBasicMaterial({color:0x3273ff,transparent:true,opacity:0.12});for(let i=0;i<6;i++){const lat=(i/6)*Math.PI-Math.PI/2+Math.PI/12,rL=gR*Math.cos(lat)*1.01,y=gR*Math.sin(lat)*1.01,seg=48,rp=[];for(let j=0;j<=seg;j++){const t=(j/seg)*Math.PI*2;rp.push(rL*Math.cos(t),y,rL*Math.sin(t));}const rg=new THREE.BufferGeometry();rg.setAttribute('position',new THREE.Float32BufferAttribute(rp,3));sc.add(new THREE.Line(rg,rM));}for(let i=0;i<4;i++){const t=(i/4)*Math.PI,rp=[],seg=48;for(let j=0;j<=seg;j++){const p=(j/seg)*Math.PI*2,rr=gR*1.01;rp.push(rr*Math.cos(p)*Math.cos(t),rr*Math.sin(p),rr*Math.cos(p)*Math.sin(t));}const rg=new THREE.BufferGeometry();rg.setAttribute('position',new THREE.Float32BufferAttribute(rp,3));sc.add(new THREE.Line(rg,rM));}const sC=600,sP=new Float32Array(sC*3);for(let i=0;i<sC;i++){const t=Math.random()*Math.PI*2,p=Math.acos(2*Math.random()-1),rr=1.8+Math.random()*1.8;sP[i*3]=rr*Math.sin(p)*Math.cos(t);sP[i*3+1]=rr*Math.cos(p);sP[i*3+2]=rr*Math.sin(p)*Math.sin(t);}const sG=new THREE.BufferGeometry();sG.setAttribute('position',new THREE.BufferAttribute(sP,3));const sM=new THREE.PointsMaterial({size:0.015,color:0x4a8aff,transparent:true,opacity:0.6,blending:THREE.AdditiveBlending,depthWrite:false});const sp=new THREE.Points(sG,sM);sc.add(sp);const gg=new THREE.SphereGeometry(gR*1.25,32,32),gm=new THREE.MeshBasicMaterial({color:0x3273ff,transparent:true,opacity:0.06,side:THREE.BackSide,blending:THREE.AdditiveBlending});const gw=new THREE.Mesh(gg,gm);sc.add(gw);function a(){requestAnimationFrame(a);gl.rotation.y+=0.004;sp.rotation.y+=0.002;gw.rotation.y+=0.001;r.render(sc,cam);}a();window.addEventListener('resize',()=>{const w=c.clientWidth||240;cam.aspect=w/H;cam.updateProjectionMatrix();r.setSize(w,H);});})();
</script>
</body>
</html>"""
            self._send_html(html)

        # ── SSE ──────────────────────────────────────────────────────

        def _serve_sse(self):
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()

            # Always send a heartbeat so the browser fires onopen
            self.wfile.write(b"event: connected\ndata: {}\n\n")
            self.wfile.flush()

            # Immediately send current run status if available
            target = run_id_filter or self._latest_run_id()
            if target:
                status = self._read_run_status(target)
                if status:
                    msg = json.dumps({"type": "stats", "data": status,
                                       "run_id": target})
                    self.wfile.write(f"data: {msg}\n\n".encode())
                    self.wfile.flush()

            try:
                while True:
                    target = run_id_filter or self._latest_run_id()
                    if target:
                        status = self._read_run_status(target)
                        if status:
                            msg = json.dumps({"type": "stats", "data": status,
                                               "run_id": target})
                            self.wfile.write(f"data: {msg}\n\n".encode())
                            self.wfile.flush()
                    else:
                        # Keepalive when no runs exist — prevents
                        # browser SSE timeout (Chrome drops idle
                        # connections after ~30s)
                        self.wfile.write(b": keepalive\n\n")
                        self.wfile.flush()
                    time.sleep(2)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def _latest_run_id(self):
            d = _get_runs_dir(args)
            if not d.exists():
                return None
            for r in sorted(d.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True):
                if r.is_dir() and ((r / "tracking.json").exists() or (r / "task_queue.json").exists()):
                    return r.name
            return None

        def _read_run_status(self, rid):
            d = _get_runs_dir(args)
            tr = d / rid / "tracking.json"
            if tr.exists():
                try:
                    t = json.loads(tr.read_text())
                    agents_dict = t.get("agents", {})
                    targets = t.get("targets", [])
                    done = sum(1 for a in agents_dict.values() if a.get("status") in ("done", "success"))
                    failed = sum(1 for a in agents_dict.values() if a.get("status") in ("failed", "timeout", "error"))
                    total = len(targets)

                    # Import stream_formatter for clean output processing
                    try:
                        from .stream_formatter import process_output_line as _format_line
                    except ImportError:
                        try:
                            import sys as _sys
                            _sys.path.insert(0, str(Path(__file__).parent))
                            from stream_formatter import process_output_line as _format_line
                        except ImportError:
                            _format_line = None

                    # Build per-agent status with formatted log lines
                    agents_list = []
                    vessels_dir = d / rid / "vessels"
                    for idx, ti in enumerate(targets):
                        a = agents_dict.get(ti, {})
                        status = a.get("status", "running")
                        label = f"agent{idx+1}"
                        activity = ""
                        if status == "running":
                            log_path = vessels_dir / f"{ti}.log"
                            if log_path.exists():
                                raw_lines = log_path.read_text(errors="replace").strip().split("\n")
                                # Only last 25 lines to avoid overwhelming SSE
                                raw_lines = raw_lines[-25:]
                                if _format_line:
                                    events = []
                                    for ln in raw_lines:
                                        evt = _format_line(ln)
                                        if evt is not None:
                                            icon = evt.get("icon", "").strip()
                                            msg = evt.get("message", "").strip()
                                            if msg:
                                                events.append(f"{icon} {msg}" if icon else msg)
                                    activity = "\n".join(events[-20:])
                                else:
                                    meaningful = []
                                    for ln in raw_lines:
                                        ln = ln.strip()
                                        if not ln or len(ln) < 2:
                                            continue
                                        if ln.startswith(("─", "╭", "├", "└", "│", "╰")):
                                            continue
                                        meaningful.append(ln[:200])
                                    activity = "\n".join(meaningful[-20:])
                        elif status in ("done", "success"):
                            activity = "✅ Complete"
                        elif status in ("failed", "timeout", "error"):
                            activity = "❌ Failed"

                        agents_list.append({
                            "target": ti,
                            "label": label,
                            "status": status,
                            "activity": activity,
                        })

                    return {"Targets": total,
                            "Status": t.get("status", "unknown"),
                            "Mode": t.get("mode", "?"),
                            "Agents done": f"{done}/{total}" if done or total > 0 else "queued",
                            "Failed": failed,
                            "run_id": rid,
                            "agents": agents_list}
                except Exception:
                    return None

    server = ThreadingHTTPServer(("0.0.0.0", port), DashHandler)
    server.socket.settimeout(1.0)  # don't hang forever on stale connections
    server.timeout = 0.5
    print(f"[sirb] Dashboard at http://0.0.0.0:{port}")
    print(f"[sirb] Watching runs at {runs_base}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[sirb] dashboard stopped")
        server.server_close()
    return 0


def _get_runs_dir(args):
    # Hermes profile sandbox sets HOME to a profile-specific dir, but reports
    # live in the real home. Use HERMES_REAL_HOME to resolve ~ correctly.
    real_home = os.environ.get("HERMES_REAL_HOME") or os.path.expanduser("~")
    run_dir = getattr(args, "run_dir", None) or os.path.join(real_home, "hermes-vault", "sirb-reports")
    # If run_dir starts with ~, expand using real_home
    if run_dir.startswith("~"):
        run_dir = os.path.join(real_home, run_dir[2:])
    return Path(run_dir).expanduser() / "runs"


if __name__ == "__main__":
    sys.exit(main())
