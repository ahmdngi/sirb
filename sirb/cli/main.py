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
import uuid
from datetime import datetime, timezone
from pathlib import Path

from sirb.core import (
    Task, Finding, TaskQueue, WorkerRegistry, Router,
    WorkerPool, Checkpointer, Blackboard, TokenBucketPool,
)
from sirb.core.worker_base import SirbWorker


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
                    info["targets"] = len(t.get("targets", []))
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

    def _vessel_positions(rid: str) -> list[dict]:
        bp = runs_base / rid / "blackboard.json"
        if not bp.exists():
            return []
        try:
            data = json.loads(bp.read_text())
            findings = data.get("findings", [])
            positions = []
            for f in findings:
                if f.get("finding_type") == "current_position":
                    detail = f.get("detail", {})
                    lat = detail.get("lat")
                    lon = detail.get("lon")
                    if lat and lon:
                        positions.append({
                            "target_id": f.get("target_id"),
                            "lat": lat, "lon": lon,
                            "destination": detail.get("destination", ""),
                        })
            return positions
        except Exception:
            return []

    def _generate_connections(agents: dict, vessels_dir: Path) -> str:
        """Analyze cross-vessel connections from agent outputs."""
        parts = ["# Cross-Vessel Connection Analysis\n\n"]
        targets = list(agents.keys())
        if len(targets) < 2:
            parts.append("*Only one target — no cross-vessel connections to analyze.*\n")
            return "".join(parts)

        parts.append(f"**Targets analyzed:** {', '.join(targets)}\n\n")
        parts.append("## Shared Ownership & Management\n\n")
        parts.append("*(Requires Equasis/registry data from each vessel report — "
                     "check per-vessel logs for owner/operator/manager details.)*\n\n")
        parts.append("## Overlapping Port Calls\n\n")
        parts.append("*(Port call analysis requires AIS tracking data from each agent. "
                     "Cross-reference timestamps and ports.)*\n\n")
        parts.append("## Connection Summary\n\n")
        parts.append("| Vessel Pair | Potential Connection | Confidence |\n")
        parts.append("|-------------|---------------------|------------|\n")
        for i, a in enumerate(targets):
            for b in targets[i+1:]:
                parts.append(f"| {a} — {b} | Pending analysis | MEDIUM |\n")
        parts.append("\n---\n*Generated by Sirb swarm correlation engine.*\n")
        return "".join(parts)

    def _generate_swarm_report(rid: str, targets: list, mode: str,
                                agents: dict, connections: str) -> str:
        """Generate the combined Sirb swarm report."""
        lines = [f"# Sirb Swarm Report: {rid}\n"]
        lines.append(f"\n**Mode:** {mode}\n")
        lines.append(f"**Targets ({len(targets)}):** {', '.join(targets)}\n")
        lines.append(f"**Generated:** {datetime.now(timezone.utc).isoformat()}\n\n")
        lines.append("## Agent Results\n\n")
        lines.append("| Target | Status |\n")
        lines.append("|--------|--------|\n")
        for t, a in agents.items():
            s = a.get("status", "?")
            icon = "✅" if s == "done" else "❌"
            lines.append(f"| {t} | {icon} {s} |\n")
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
            elif path.startswith("/run/") and path.endswith("/connections"):
                rid = path.split("/")[2]
                cp = runs_base / rid / "connections.md"
                if cp.exists():
                    self._send_html(cp.read_text())
                else:
                    self._send_html("<p>Connections analysis not ready yet.</p>")
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
            elif re.match(r"^/run/[^/]+/vessels$", path):
                rid = path.split("/")[2]
                vdir = runs_base / rid / "vessels"
                if vdir.exists():
                    vessels = []
                    for v in sorted(vdir.iterdir()):
                        if v.is_dir():
                            files = sorted(f.name for f in v.iterdir() if f.suffix in (".md", ".log", ".txt"))
                            if files:
                                vessels.append({"target": v.name, "files": files})
                        elif v.suffix in (".log", ".txt", ".md"):
                            # Also include flat log files
                            pass
                    self._send_json(vessels)
                else:
                    self._send_json([])
            elif path.startswith("/run/") and path.endswith("/positions"):
                rid = path.split("/")[2]
                self._send_json(_vessel_positions(rid))
            elif path == "/map":
                self._serve_map_html()
            elif path == "/models":
                self._send_json(_load_models())
            elif path == "/api/profiles/models":
                pm_path = Path(__file__).parent / "profiles-models.json"
                try:
                    data = json.loads(pm_path.read_text())
                    self._send_json(data)
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

            if path == "/run/new":
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
                tracking = {"run_id": run_id, "targets": mmsi_list, "mode": mode,
                            "created_at": datetime.now(timezone.utc).isoformat(),
                            "status": "running", "agents": {}}
                (rundir / "tracking.json").write_text(json.dumps(tracking))

                # Spawn hermes agents in background thread
                def _run_swarm(rid, targets, md, prof, mod):
                    agents = {}
                    vessels_path = runs_base / rid / "vessels"
                    tr_path = runs_base / rid / "tracking.json"

                    def _run_agent(target):
                        agent_dir = runs_base / rid / "vessels" / target
                        agent_dir.mkdir(parents=True, exist_ok=True)
                        prompt = (
                            f"Using the shipcrawler OSINT framework, research the vessel {target}. "
                            f"Execute ALL phases: Equasis identity, AIS tracking, "
                            f"Shodan attack surface, CVE vulnerability assessment, "
                            f"threat intelligence from news and maritime cyber incidents. "
                            f"Mode: {md}. "
                            f"SAVE ALL report files to the directory: {agent_dir}/"
                        )
                        env = os.environ.copy()
                        cmd = ["hermes", "chat", "-q", prompt,
                               "--skills", "shipcrawler",
                               "-t", "web,terminal",
                               "--yolo", "--max-turns", "150",
                               "--source", "tool"]
                        if prof and prof != "default":
                            cmd.extend(["--profile", prof])
                        if mod:
                            cmd.extend(["--model", mod])
                        try:
                            proc = subprocess.Popen(
                                cmd,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                text=True, env=env,
                            )
                            output, _ = proc.communicate(timeout=600)
                            status = "done" if proc.returncode == 0 else "failed"
                            # Save agent output
                            (vessels_path / f"{target}.log").write_text(output)
                            return {"target": target, "status": status,
                                    "exit_code": proc.returncode}
                        except subprocess.TimeoutExpired:
                            proc.kill()
                            return {"target": target, "status": "timeout", "exit_code": -1}
                        except Exception as e:
                            return {"target": target, "status": "error", "error": str(e)}

                    # Run agents sequentially (avoid resource exhaustion)
                    for t in targets:
                        result = _run_agent(t)
                        agents[t] = result
                        # Update tracking after each agent
                        try:
                            tr = json.loads(tr_path.read_text())
                            tr["agents"] = agents
                            tr_path.write_text(json.dumps(tr))
                        except Exception:
                            pass

                    # Generate connections analysis
                    connections = _generate_connections(agents, vessels_path)
                    (runs_base / rid / "connections.md").write_text(connections)

                    # Generate swarm report
                    report = _generate_swarm_report(rid, targets, md, agents, connections)
                    (runs_base / rid / "swarm-report.md").write_text(report)

                    # Update final status
                    try:
                        tr = json.loads(tr_path.read_text())
                        tr["status"] = "done"
                        tr["agents"] = agents
                        tr_path.write_text(json.dumps(tr))
                    except Exception:
                        pass

                thread = threading.Thread(
                    target=_run_swarm,
                    args=(run_id, mmsi_list, mode, profile, model),
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
<title>Sirb Swarm — Agent OSINT Dashboard</title>
<link rel="icon" type="image/svg+xml" href="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCAyNCAyNCIgZmlsbD0ibm9uZSIgc3Ryb2tlPSIjNThhNmZmIiBzdHJva2Utd2lkdGg9IjIiPjxwYXRoIGQ9Ik0xMiAyTDIgN2wxMCA1IDEwLTUtMTAtNXoiLz48cGF0aCBkPSJNMiAxN2wxMCA1IDEwLTUiLz48cGF0aCBkPSJNMiAxMmwxMCA1IDEwLTUiLz48L3N2Zz4=">
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="https://cdn.jsdelivr.net/npm/marked/marked.min.js"></script>
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
* { margin:0; padding:0; box-sizing:border-box; }
body { font-family:"Inter",-apple-system,sans-serif; background:var(--bg); color:var(--text); min-height:100vh; display:flex; }
::selection { background:var(--accent); color:#fff; }
::-webkit-scrollbar { width:5px; height:5px; }
::-webkit-scrollbar-track { background:transparent; }
::-webkit-scrollbar-thumb { background:var(--border); border-radius:3px; }
.sidebar { width:240px; flex-shrink:0; background:var(--bg-2); border-right:1px solid var(--border); display:flex; flex-direction:column; height:100vh; position:sticky; top:0; }
.sidebar-header { padding:1em; border-bottom:1px solid var(--border); }
.sidebar-header h2 { font-size:0.75em; text-transform:uppercase; letter-spacing:0.08em; color:var(--text-2); font-weight:600; }
.sidebar-list { flex:1; overflow-y:auto; padding:0.5em; }
.run-item { position:relative; padding:0.55em 0.65em; margin-bottom:0.25em; border-radius:6px; cursor:pointer; font-size:0.82em; transition:background 0.15s,border-left 0.15s; border-left:3px solid transparent; }
.run-item:hover { background:var(--bg-3); }
.run-item.active { background:var(--bg-3); border-left-color:var(--accent); }
.run-item .date { font-size:0.7em; color:var(--text-3); }
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
.main { flex:1; display:flex; flex-direction:column; min-width:0; }
nav { position:sticky; top:0; z-index:100; background:color-mix(in srgb,var(--bg) 90%,transparent); backdrop-filter:blur(8px); border-bottom:1px solid var(--border); padding:0.7em 1.5em; display:flex; align-items:center; gap:1em; }
nav .brand { font-size:1.1em; font-weight:700; letter-spacing:-0.01em; display:flex; align-items:center; gap:0.5em; }
nav .brand-logo { height:26px; width:auto; }
nav .brand span { color:var(--accent); }
nav .selected-run { font-size:0.82em; color:var(--text-2); font-family:'JetBrains Mono',monospace; }
nav .nav-right { margin-left:auto; display:flex; align-items:center; gap:0.75em; }
.sse-status { font-size:0.7em; display:inline-flex; align-items:center; gap:4px; color:var(--green); }
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
.terminal-body { padding:0.85rem; max-height:500px; overflow-y:auto; font-size:0.82rem; line-height:1.6; scroll-behavior:smooth; white-space:pre-wrap; word-break:break-word; }
.stat-grid { display:grid; grid-template-columns:repeat(4,1fr); gap:0.5em; margin-bottom:1em; }
.stat-card { background:var(--bg-2); border:1px solid var(--border); border-radius:6px; padding:0.75em; text-align:center; }
.stat-card .label { font-size:0.7em; color:var(--text-2); text-transform:uppercase; letter-spacing:0.04em; }
.stat-card .value { font-size:1.2em; font-weight:700; margin-top:0.15em; }
.panel-right { width:340px; padding:1.25em; overflow-y:auto; background:var(--bg-2); border-left:1px solid var(--border); flex-shrink:0; }
.panel-right h3 { font-size:0.85em; font-weight:600; display:flex; align-items:center; gap:0.5em; margin-bottom:1em; color:var(--text); }
.hero-badge { display:inline-flex; align-items:center; gap:0.4rem; background:rgba(88,166,255,0.08); border:1px solid rgba(88,166,255,0.2); border-radius:999px; padding:0.25rem 0.85rem; font-size:0.7rem; color:var(--accent); text-transform:uppercase; letter-spacing:0.06em; margin-bottom:0.75rem; }
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
#map { height:250px; border-radius:6px; margin-top:0.5em; border:1px solid var(--border); }
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
    <div class="brand"><img src="/logo" class="brand-logo" alt="Sirb"><span>Sirb</span> Swarm</div>
    <span class="selected-run" id="selected-run">No run selected</span>
    <div class="nav-right"><span class="sse-status" id="sse-status">connected</span></div>
  </nav>
  <div class="content">
    <div class="panel">
      <div id="live-stats" class="stat-grid"></div>
      <div id="report-tabs" style="display:none;border-bottom:1px solid var(--border);margin-bottom:0.5em;">
        <button class="tab-btn active" data-tab="swarm" onclick="switchTab('swarm')">📋 Swarm</button>
        <button class="tab-btn" data-tab="connections" onclick="switchTab('connections')">🔗 Connections</button>
        <span id="vessel-tabs"></span>
      </div>
      <div class="terminal-window">
        <div class="terminal-titlebar"><div class="terminal-dots"><span class="tdot-red"></span><span class="tdot-yellow"></span><span class="tdot-green"></span></div><span class="terminal-title" id="report-title">swarm-report.md</span></div>
        <div class="terminal-body" id="assessment-view"><span style="color:var(--text-3)">Select a run to view its assessment.</span></div>
      </div>
    </div>
    <div class="panel-right">
      <h3><span style="color:var(--accent)">▶</span> Launch Scan</h3>
      <div class="hero-badge"><span class="live-dot"></span> SIRB v0.3</div>
      <div class="form-group"><label>Method</label><select id="method-select" onchange="toggleMethod()"><option value="mmsi">IMO / MMSI</option><option value="port">Port Scan</option><option value="geo">Geo Location</option></select></div>
      <div id="method-mmsi" class="method-panel">
        <div class="form-group" style="display:flex;gap:0.5em;align-items:end;">
          <div style="flex:1"><label>Number of vessels</label><input id="vessel-count" type="number" min="1" max="20" value="2" onchange="buildVesselInputs()" /></div>
          <button class="btn btn-small" onclick="buildVesselInputs()" style="margin-bottom:2px;">Apply</button>
        </div>
        <div id="vessel-inputs"></div>
      </div>
      <div id="method-port" class="method-panel" style="display:none"><div class="form-group"><label>Port</label><select id="port-select"><option value="tallinn">Tallinn</option><option value="helsinki">Helsinki</option></select></div></div>
      <div id="method-geo" class="method-panel" style="display:none">
        <div class="form-group"><label>Latitude</label><input id="geo-lat" placeholder="59.5" /></div>
        <div class="form-group"><label>Longitude</label><input id="geo-lon" placeholder="24.5" /></div>
        <div style="display:flex;gap:0.5em;"><div class="form-group" style="flex:1"><label>Radius (km)</label><input id="geo-radius" placeholder="50" value="50" /></div><div class="form-group" style="flex:1"><label>Label</label><input id="geo-label" placeholder="Tallinn Bay" /></div></div>
      </div>
      <div class="form-group"><label>Mode</label><select id="mode-select"><option value="fast">Fast (~2 min)</option><option value="deep">Deep (~10 min)</option></select></div>
      <div style="display:flex;gap:0.5rem;align-items:center;margin-bottom:0.6em;"><label style="font-size:0.75rem;color:#58a6ff;white-space:nowrap;font-weight:500;">👤 Profile:</label><select id="profile-select" style="background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:0.4rem 0.6rem;font-family:JetBrains Mono,monospace;font-size:0.78rem;color:var(--text-2);outline:none;cursor:pointer;width:auto;min-width:140px;" onchange="onProfileChange()"><option value="">default</option><option value="local">local</option><option value="research">research</option><option value="shipcrawler">shipcrawler</option></select></div>
      <div style="display:flex;gap:0.5rem;align-items:center;margin-bottom:0.6em;"><label style="font-size:0.75rem;color:#58a6ff;white-space:nowrap;font-weight:500;">🧠 Model:</label><select id="model-select" style="background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:0.4rem 0.6rem;font-family:JetBrains Mono,monospace;font-size:0.78rem;color:var(--text-2);outline:none;cursor:pointer;width:auto;min-width:170px;max-width:260px;"><option value="">Loading models...</option></select></div>
      <div class="btn-row"><button class="btn btn-primary" id="run-btn" onclick="launchRun()">▶ Run</button><button class="btn btn-danger" id="stop-btn" onclick="stopRun()" style="display:none">■ Stop</button></div>
      <hr style="border-color:var(--border);margin:1em 0;" />
      <h4 style="font-size:0.8em;color:var(--text-2);margin-bottom:0.5em;">Vessel Positions</h4>
      <div id="map"></div>
    </div>
  </div>
</div>
<script>
let currentRunId=null,map=null,markers=[],reportCache={};
const sseEl=document.getElementById("sse-status"),evtSource=new EventSource("/events");
evtSource.onopen=()=>{sseEl.textContent="connected";sseEl.className="sse-status";};
evtSource.onerror=()=>{sseEl.textContent="disconnected";sseEl.className="sse-status disconnected";};
evtSource.onmessage=(e)=>{try{const d=JSON.parse(e.data);if(d.type==="stats"&&d.data)liveStats(d.data)}catch(_){}};
function liveStats(d){const g=document.getElementById("live-stats");if(!g)return;g.innerHTML="";for(const[k,v]of Object.entries(d)){const c=k==="Failed"||k==="Error"?"var(--red)":k==="Running"?"var(--accent)":"var(--green)";g.innerHTML+='<div class="stat-card"><div class="label">'+k+'</div><div class="value" style="color:'+c+'">'+v+'</div></div>'}}
async function loadRuns(){const r=await fetch("/runs");const runs=await r.json();const el=document.getElementById("run-list");el.innerHTML=runs.map(r=>{const dt=r.generated_at||new Date(r.mtime*1000).toLocaleString();const a=r.id===currentRunId?"active":"";return'<div class="run-item '+a+'" onclick="selectRun(\''+r.id+'\')" id="ri-'+r.id+'"><div style="font-weight:'+(r.has_assessment?"600":"400")+';font-size:0.82em">'+r.id.slice(0,16)+'</div><div class="date">'+dt+(r.targets?" · "+r.targets+" targets":"")+'</div><button class="sidebar-delete" onclick="event.stopPropagation();deleteRun(\''+r.id+'\')" title="Delete run">🗑</button></div>'}).join("");if(!runs.length)el.innerHTML='<div class="run-empty">No runs yet</div>'}

async function deleteRun(rid){if(!confirm("Delete run "+rid+"?"))return;const r=await fetch("/run/"+rid,{method:"DELETE"});const d=await r.json();if(d.status==="deleted"){if(currentRunId===rid){currentRunId=null;document.getElementById("selected-run").textContent="No run selected";document.getElementById("assessment-view").innerHTML='<span style="color:var(--text-3)">Select a run to view its report.</span>';document.getElementById("report-tabs").style.display="none"}loadRuns()}else{alert("Delete failed: "+(d.error||"unknown"))}}

async function selectRun(rid){currentRunId=rid;document.getElementById("selected-run").textContent="Run: "+rid;document.querySelectorAll(".run-item").forEach(e=>e.classList.remove("active"));const el=document.getElementById("ri-"+rid);if(el)el.classList.add("active");reportCache={};document.getElementById("report-tabs").style.display="";switchTab("swarm");const r=await fetch("/run/"+rid+"/report");reportCache.swarm=await r.text();if(reportCache.swarm.includes("Report not ready")){document.getElementById("assessment-view").innerHTML='<span style="color:var(--text-3)">⏳ Swarm in progress... agents running.</span>';document.getElementById("report-tabs").style.display="none";return}renderTab("swarm");loadVessels(rid)}

async function loadVessels(rid){const r=await fetch("/run/"+rid+"/vessels");const v=await r.json();let html="";v.forEach(v=>{html+='<button class="vessel-btn" onclick="loadVesselFile(\''+rid+'\',\''+v.target+'\',\''+v.files[0]+'\')">🚢 '+v.target.slice(0,12)+'</button>'});document.getElementById("vessel-tabs").innerHTML=html;if(!v.length){document.getElementById("connections").disabled=true}}

async function loadVesselFile(rid,target,file){const r=await fetch("/run/"+rid+"/vessel/"+target+"/"+file);const text=await r.text();const key="vessel_"+target+"_"+file;reportCache[key]=text;switchTab(key);document.getElementById("report-title").textContent=target+"/"+file;const view=document.getElementById("assessment-view");view.innerHTML=marked.parse(text)}

function switchTab(tab){document.querySelectorAll(".tab-btn,.vessel-btn").forEach(b=>b.classList.remove("active"));if(tab==="swarm"){document.querySelector('[data-tab="swarm"]').classList.add("active");renderTab("swarm")}else if(tab==="connections"){document.querySelector('[data-tab="connections"]').classList.add("active");renderTab("connections")}else{document.getElementById("vessel-tabs").querySelectorAll(".vessel-btn").forEach(b=>{if(b.textContent.includes(tab))b.classList.add("active")});renderTab(tab)}}

function renderTab(tab){const view=document.getElementById("assessment-view");const content=reportCache[tab];if(tab==="swarm"){document.getElementById("report-title").textContent="swarm-report.md"}else if(tab==="connections"){document.getElementById("report-title").textContent="connections.md"}if(content){view.innerHTML=marked.parse(content)}else if(tab==="swarm"){view.innerHTML='<span style="color:var(--text-3)">Loading swarm report...</span>'}else if(tab==="connections"){view.innerHTML='<span style="color:var(--text-3)">⏳ Connections analysis not ready yet.</span>'}}
async function loadPositions(rid){const r=await fetch("/run/"+rid+"/positions");const pts=await r.json();if(!map)return;markers.forEach(m=>map.removeLayer(m));markers=[];if(!pts.length)return;pts.forEach(p=>{const m=L.circleMarker([p.lat,p.lon],{radius:6,color:"#f85149",fillColor:"#f85149",fillOpacity:0.8}).addTo(map).bindPopup("<b>"+p.target_id+"</b><br/>Dest: "+p.destination);markers.push(m)});if(pts.length)map.fitBounds(markers.map(m=>m.getLatLng()),{padding:[30,30]})}
function initMap(){if(!document.getElementById("map"))return;map=L.map("map",{zoomControl:true,attributionControl:false}).setView([59.5,24.5],3);L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",{maxZoom:18}).addTo(map)}
function toggleMethod(){const m=document.getElementById("method-select").value;document.getElementById("method-mmsi").style.display=m==="mmsi"?"":m==="port"?"none":"none";document.getElementById("method-port").style.display=m==="port"?"":"none";document.getElementById("method-geo").style.display=m==="geo"?"":"none"}
toggleMethod();
function buildVesselInputs(){const n=parseInt(document.getElementById("vessel-count").value)||2;const c=document.getElementById("vessel-inputs");let h="";for(let i=1;i<=n;i++){const v=document.getElementById("vi_"+i);h+='<div style="display:flex;align-items:center;gap:0.4em;margin-bottom:0.35em;"><span style="font-size:0.72em;color:var(--text-2);min-width:5em;">Vessel '+i+'</span><input id="vi_'+i+'" placeholder="IMO or MMSI" style="flex:1;background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:0.4rem 0.6rem;font-family:JetBrains Mono,monospace;font-size:0.78rem;color:var(--text-1);outline:none;" value="'+(v?v.value:'')+'" /></div>'}c.innerHTML=h}
buildVesselInputs();

async function launchRun(){const method=document.getElementById("method-select").value;const profile=document.getElementById("profile-select").value;const model=document.getElementById("model-select").value;const mode=document.getElementById("mode-select").value;if(method==="mmsi"){const n=parseInt(document.getElementById("vessel-count").value)||2;let mmsis=[];for(let i=1;i<=n;i++){const v=document.getElementById("vi_"+i);if(v&&v.value.trim())mmsis.push(v.value.trim())}if(!mmsis.length){alert("Enter at least one IMO or MMSI");return}const r=await fetch("/run/new",{method:"POST",headers:{"Content-Type":"application/x-www-form-urlencoded"},body:"mmsi="+encodeURIComponent(mmsis.join(" "))+"&mode="+encodeURIComponent(mode)+"&profile="+encodeURIComponent(profile)+"&model="+encodeURIComponent(model)});handleLaunchResponse(r)}else if(method==="port"){const port=document.getElementById("port-select").value;const r=await fetch("/run/port",{method:"POST",headers:{"Content-Type":"application/x-www-form-urlencoded"},body:"port="+encodeURIComponent(port)+"&mode="+encodeURIComponent(mode)+"&profile="+encodeURIComponent(profile)+"&model="+encodeURIComponent(model)});handleLaunchResponse(r)}else if(method==="geo"){const lat=document.getElementById("geo-lat").value.trim();const lon=document.getElementById("geo-lon").value.trim();const radius=document.getElementById("geo-radius").value.trim()||"50";const label=document.getElementById("geo-label").value.trim()||(lat+","+lon);if(!lat||!lon){alert("Enter latitude and longitude");return}const r=await fetch("/run/geo",{method:"POST",headers:{"Content-Type":"application/x-www-form-urlencoded"},body:"lat="+encodeURIComponent(lat)+"&lon="+encodeURIComponent(lon)+"&radius="+encodeURIComponent(radius)+"&label="+encodeURIComponent(label)+"&profile="+encodeURIComponent(profile)+"&model="+encodeURIComponent(model)});handleLaunchResponse(r)}}
async function handleLaunchResponse(p){document.getElementById("run-btn").disabled=true;document.getElementById("run-btn").textContent="Running...";document.getElementById("stop-btn").style.display="inline-block";try{const r=await p;const d=await r.json();if(d.run_id){currentRunId=d.run_id;document.getElementById("selected-run").textContent="Run: "+d.run_id;document.getElementById("assessment-view").innerHTML='<span style="color:var(--accent)">⏳ Run started...</span>';setTimeout(loadRuns,1000)}else if(d.error){alert("Error: "+d.error)}}catch(e){alert("Failed: "+e)}document.getElementById("run-btn").disabled=false;document.getElementById("run-btn").textContent="▶ Run"}
async function stopRun(){if(!currentRunId)return;await fetch("/run/"+currentRunId+"/stop",{method:"POST"});document.getElementById("stop-btn").style.display="none";document.getElementById("selected-run").textContent="Stopped: "+currentRunId;setTimeout(loadRuns,1000)}
async function loadProfileModels(profile){try{const r=await fetch("/api/profiles/models");const data=await r.json();const pk=profile||"";const ms=data[pk]||[];const sel=document.getElementById("model-select");sel.innerHTML=ms.map(m=>'<option value="'+m.value+'">'+m.label+"</option>").join("");if(!sel.value)sel.selectedIndex=0}catch(_){document.getElementById("model-select").innerHTML='<option value="deepseek-v4-flash">DeepSeek V4 Flash</option>'}}
function onProfileChange(){const p=document.getElementById("profile-select").value;loadProfileModels(p)}
loadRuns();loadProfileModels("");setInterval(loadRuns,5000);
initMap();
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
                    agents = t.get("agents", {})
                    done = sum(1 for a in agents.values() if a.get("status") == "done")
                    failed = sum(1 for a in agents.values() if a.get("status") in ("failed", "timeout", "error"))
                    total = len(t.get("targets", []))
                    return {"Targets": total,
                            "Status": t.get("status", "unknown"),
                            "Mode": t.get("mode", "?"),
                            "Agents done": f"{done}/{total}" if agents else "queued",
                            "Failed": failed}
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
    run_dir = getattr(args, "run_dir", None) or "~/hermes-vault/sirb-reports"
    return Path(run_dir).expanduser() / "runs"


if __name__ == "__main__":
    sys.exit(main())
