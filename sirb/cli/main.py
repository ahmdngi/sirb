"""Sirb CLI — run, list-workers, init."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
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
    """Start a live SSE dashboard showing swarm progress.

    Serves an HTML page at http://localhost:{port} that connects to SSE
    and shows real-time task progress by polling the latest run's
    checkpoint files.
    """
    import http.server
    import json
    import urllib.parse
    from pathlib import Path

    runs_dir = Path(args.run_dir).expanduser() / "runs" if hasattr(args, "run_dir") and args.run_dir else Path(
        "~/hermes-vault/sirb-reports"
    ).expanduser() / "runs"
    port = args.port
    run_id = args.run_id

    # ── SSE handler ──────────────────────────────────────────────────

    class SSEHandler(http.server.BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):
            pass  # suppress default HTTP log noise

        def do_GET(self):
            parsed = urllib.parse.urlparse(self.path)
            path = parsed.path

            if path == "/":
                self._serve_html()
            elif path == "/events":
                self._serve_sse()
            elif path == "/status":
                self._serve_status()
            else:
                self.send_response(404)
                self.end_headers()

        def _serve_html(self):
            html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Sirb Dashboard</title>
<style>
  body {{ font-family: system-ui, sans-serif; max-width: 640px; margin: 2em auto; background: #0d1117; color: #c9d1d9; }}
  h1 {{ color: #58a6ff; }}
  .stat {{ display: flex; justify-content: space-between; padding: 0.5em; border-bottom: 1px solid #21262d; }}
  .stat-label {{ color: #8b949e; }}
  .stat-value {{ font-weight: bold; }}
  .critical {{ color: #f85149; }}
  .high {{ color: #d29922; }}
  .info {{ color: #58a6ff; }}
  .delta {{ margin-top: 1em; padding: 1em; background: #161b22; border-radius: 6px; }}
  .delta pre {{ white-space: pre-wrap; font-size: 0.85em; }}
  #sse-status {{ font-size: 0.8em; color: #8b949e; }}
</style>
</head>
<body>
<h1>🐝 Sirb Swarm Dashboard</h1>
<p>Run: <span id="run-id">—</span> | <span id="sse-status">connecting...</span></p>
<div id="stats"></div>
<div id="delta"></div>
<script>
const evt = new EventSource("/events");
const sseStatus = document.getElementById("sse-status");
evt.onopen = () => sseStatus.textContent = "connected";
evt.onerror = () => sseStatus.textContent = "disconnected";
evt.onmessage = (e) => {{
  try {{
    const d = JSON.parse(e.data);
    if (d.type === "init") {{
      document.getElementById("run-id").textContent = d.run_id;
    }}
    else if (d.type === "stats") {{
      let html = "";
      for (const [k, v] of Object.entries(d.data)) {{
        html += `<div class="stat"><span class="stat-label">${{k}}</span><span class="stat-value ${{k.toLowerCase()}}">${{v}}</span></div>`;
      }}
      document.getElementById("stats").innerHTML = html;
    }}
    else if (d.type === "delta") {{
      document.getElementById("delta").innerHTML =
        `<div class="delta"><pre>${{d.markdown}}</pre></div>`;
    }}
  }} catch(e) {{}}
}};
</script>
</body>
</html>"""
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(html.encode())

        def _serve_sse(self):
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()

            # Resolve run to watch
            target_run_id = run_id or self._latest_run_id()
            if target_run_id:
                init_msg = json.dumps({"type": "init", "run_id": target_run_id})
                self.wfile.write(f"data: {init_msg}\n\n".encode())
                self.wfile.flush()

            # Poll the checkpoint file every 2 seconds
            import time
            try:
                while True:
                    target_run_id = run_id or self._latest_run_id()
                    if target_run_id:
                        status = self._read_run_status(target_run_id)
                        if status:
                            msg = json.dumps({"type": "stats", "data": status})
                            self.wfile.write(f"data: {msg}\n\n".encode())
                            self.wfile.flush()

                            # Also read delta if available
                            delta = self._read_delta(target_run_id)
                            if delta:
                                dmsg = json.dumps(
                                    {"type": "delta", "markdown": delta})
                                self.wfile.write(
                                    f"data: {dmsg}\n\n".encode())
                                self.wfile.flush()

                    time.sleep(2)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def _latest_run_id(self):
            d = _get_runs_dir(args)
            if not d.exists():
                return None
            runs = sorted(d.iterdir(), reverse=True)
            for r in runs:
                if r.is_dir() and (r / "task_queue.json").exists():
                    return r.name
            return None

        def _read_run_status(self, rid):
            d = _get_runs_dir(args)
            qp = d / rid / "task_queue.json"
            if not qp.exists():
                return None
            try:
                data = json.loads(qp.read_text())
                tasks = data.get("tasks", {})
                statuses = {}
                for t in tasks.values():
                    s = t.get("status", "unknown")
                    statuses[s] = statuses.get(s, 0) + 1
                total = len(tasks)
                done = statuses.get("completed", 0)
                return {
                    "Progress": f"{done}/{total}",
                    "Completed": done,
                    "Pending": statuses.get("pending", 0),
                    "Running": statuses.get("running", 0),
                    "Failed": statuses.get("failed", 0),
                    "Total": total,
                }
            except Exception:
                return None

        def _read_delta(self, rid):
            d = _get_runs_dir(args)
            md = d / rid / "assessment.md"
            if not md.exists():
                return None
            # Show last 10 lines of assessment for trend context
            try:
                lines = md.read_text().splitlines()
                return "\n".join(lines[:20])
            except Exception:
                return None

    server = http.server.HTTPServer(("0.0.0.0", port), SSEHandler)
    print(f"[sirb] Dashboard at http://localhost:{port}")
    print(f"[sirb] Watching run: {run_id or '(latest)'}")
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
