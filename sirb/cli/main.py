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

    def _load_assessment(rid: str) -> str | None:
        mdp = runs_base / rid / "assessment.md"
        return mdp.read_text() if mdp.exists() else None

    def _load_assessment_json(rid: str) -> dict:
        sjp = runs_base / rid / "assessment-summary.json"
        if sjp.exists():
            return json.loads(sjp.read_text())
        return {}

    def _list_runs() -> list[dict]:
        if not runs_base.exists():
            return []
        out = []
        for d in sorted(runs_base.iterdir(), reverse=True):
            if not d.is_dir():
                continue
            qp = d / "task_queue.json"
            ap = d / "assessment.md"
            sjp = d / "assessment-summary.json"
            mtime = d.stat().st_mtime if d.stat() else 0
            info = {"id": d.name, "has_queue": qp.exists(),
                    "has_assessment": ap.exists(), "mtime": mtime}
            if sjp.exists():
                try:
                    s = json.loads(sjp.read_text())
                    info["targets"] = s.get("unique_targets", 0)
                    info["generated_at"] = s.get("generated_at", "")
                except Exception:
                    pass
            out.append(info)
        return out

    def _vessel_positions(rid: str) -> list[dict]:
        """Extract vessel lat/lon from a run's blackboard."""
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
            elif path == "/runs":
                self._send_json(_list_runs())
            elif path.startswith("/run/") and path.endswith("/json"):
                rid = path.split("/")[2]
                self._send_json(_load_assessment_json(rid))
            elif path.startswith("/run/") and path.endswith("/assessment"):
                rid = path.split("/")[2]
                md = _load_assessment(rid)
                self._send_html(md or "<p>No assessment yet.</p>")
            elif path.startswith("/run/") and path.endswith("/positions"):
                rid = path.split("/")[2]
                self._send_json(_vessel_positions(rid))
            elif path == "/map":
                self._serve_map_html()
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
                if not mmsis:
                    self._send_json({"error": "No MMSI provided"}, 400)
                    return
                mmsi_list = [m.strip() for m in mmsis.replace(",", " ").split() if m.strip()]
                tasks_json = json.dumps({"mmsi": mmsi_list if len(mmsi_list) > 1 else mmsi_list[0],
                                          "mode": mode})
                # Write to temp file for sirb run --tasks
                import tempfile
                tf = tempfile.NamedTemporaryFile(
                    mode="w", suffix=".json", delete=False, prefix="sirb-dash-")
                tf.write(json.dumps({"tasks": [{"mmsi": mmsi_list if len(mmsi_list) > 1 else mmsi_list[0],
                                                "mode": mode}]}))
                tf.close()
                hermes_python = sys.executable
                args_list = [hermes_python, "-m", "sirb", "run",
                             "--tasks", tf.name]
                env = os.environ.copy()
                env["SIRB_RUN_DIR"] = str(runs_base.parent)
                try:
                    proc = subprocess.Popen(
                        args_list + ["--tasks", tasks_json],
                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                        text=True, env=env,
                    )
                    run_id = f"web-{int(time.time())}"
                    with proc_lock:
                        running_procs[run_id] = proc
                    # Log output in background
                    def _log_output(pid, p):
                        for line in p.stdout:
                            line = line.rstrip()
                        with proc_lock:
                            running_procs.pop(pid, None)
                    threading.Thread(target=_log_output, args=(run_id, proc), daemon=True).start()
                    self._send_json({"run_id": run_id, "status": "started"})
                except Exception as e:
                    self._send_json({"error": str(e)}, 500)

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
                hermes_python = sys.executable
                env = os.environ.copy()
                env["SIRB_GEO_TARGETS"] = json.dumps(geo_cfg)
                args_list = [hermes_python, "-m", "sirb", "run"]
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
                    args_list = [hermes_python, "-m", "sirb", "run"]
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

        # ── main HTML page ────────────────────────────────────────────

        def _serve_html(self):
            html = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Sirb Dashboard</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: system-ui, sans-serif; background: #0d1117; color: #c9d1d9; display: flex; height: 100vh; }
.sidebar { width: 260px; background: #161b22; border-right: 1px solid #30363d; padding: 1em; overflow-y: auto; flex-shrink: 0; }
.sidebar h2 { color: #58a6ff; font-size: 0.9em; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 0.5em; }
.sidebar .run-item { padding: 0.5em; cursor: pointer; border-radius: 4px; margin-bottom: 2px; font-size: 0.85em; }
.sidebar .run-item:hover { background: #21262d; }
.sidebar .run-item.active { background: #1f6feb; color: #fff; }
.sidebar .run-item .date { font-size: 0.75em; color: #8b949e; }
.main { flex: 1; display: flex; flex-direction: column; min-width: 0; }
.main .top-bar { padding: 1em; border-bottom: 1px solid #30363d; display: flex; align-items: center; gap: 1em; }
.top-bar h1 { font-size: 1.2em; color: #58a6ff; }
.top-bar #sse-status { font-size: 0.8em; color: #8b949e; margin-left: auto; }
.content { flex: 1; display: flex; overflow: hidden; }
.panel { flex: 1; padding: 1em; overflow-y: auto; min-width: 0; }
.panel-right { width: 340px; padding: 1em; overflow-y: auto; background: #161b22; border-left: 1px solid #30363d; flex-shrink: 0; }
.stat-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 0.5em; margin-bottom: 1em; }
.stat-card { background: #21262d; border-radius: 6px; padding: 0.75em; text-align: center; }
.stat-card .label { font-size: 0.75em; color: #8b949e; }
.stat-card .value { font-size: 1.3em; font-weight: bold; }
.critical { color: #f85149; } .high { color: #d29922; } .medium { color: #db6d28; }
.info { color: #58a6ff; } .success { color: #3fb950; }
table { width: 100%; border-collapse: collapse; font-size: 0.85em; }
th, td { text-align: left; padding: 0.4em 0.5em; border-bottom: 1px solid #21262d; }
th { color: #8b949e; font-weight: 600; }
#assessment-view { white-space: pre-wrap; font-size: 0.85em; line-height: 1.5; }
.form-group { margin-bottom: 0.75em; }
.form-group label { display: block; font-size: 0.85em; color: #8b949e; margin-bottom: 0.25em; }
.form-group input, .form-group select { width: 100%; padding: 0.5em; background: #0d1117; border: 1px solid #30363d; border-radius: 4px; color: #c9d1d9; }
.btn { padding: 0.5em 1em; border: none; border-radius: 4px; cursor: pointer; font-size: 0.85em; font-weight: 600; }
.btn-primary { background: #1f6feb; color: #fff; }
.btn-primary:hover { background: #388bfd; }
.btn-danger { background: #da3633; color: #fff; }
.btn-danger:hover { background: #f85149; }
#map { height: 300px; border-radius: 6px; margin-top: 0.5em; }
.log-line { font-family: monospace; font-size: 0.8em; color: #8b949e; white-space: pre-wrap; }
</style>
</head>
<body>
<div class="sidebar" id="sidebar">
  <h2>Runs</h2>
  <div id="run-list"></div>
</div>
<div class="main">
  <div class="top-bar">
    <h1>🐝 Sirb Swarm v0.1.0</h1>
    <span id="selected-run">No run selected</span>
    <span id="sse-status">connected</span>
  </div>
  <div class="content">
    <div class="panel" id="center-panel">
      <div id="live-stats" class="stat-grid"></div>
      <div id="assessment-view">Select a run to view its assessment.</div>
    </div>
    <div class="panel-right" id="right-panel">
      <h3 style="color:#58a6ff;margin-bottom:0.5em;">Launch Scan</h3>

      <div class="form-group">
        <label>Method</label>
        <select id="method-select" onchange="toggleMethod()">
          <option value="mmsi">Direct MMSI</option>
          <option value="port">Port Scan</option>
          <option value="geo">Geo Location</option>
        </select>
      </div>

      <!-- Direct MMSI -->
      <div id="method-mmsi" class="method-panel">
        <div class="form-group">
          <label>MMSI(s) — space or comma separated</label>
          <input id="mmsi-input" placeholder="273342890 311000987" />
        </div>
      </div>

      <!-- Port Scan -->
      <div id="method-port" class="method-panel" style="display:none">
        <div class="form-group">
          <label>Port</label>
          <select id="port-select">
            <option value="tallinn">Tallinn</option>
            <option value="helsinki">Helsinki</option>
          </select>
        </div>
      </div>

      <!-- Geo Location -->
      <div id="method-geo" class="method-panel" style="display:none">
        <div class="form-group">
          <label>Latitude</label>
          <input id="geo-lat" placeholder="59.5" />
        </div>
        <div class="form-group">
          <label>Longitude</label>
          <input id="geo-lon" placeholder="24.5" />
        </div>
        <div class="form-group">
          <label>Radius (km)</label>
          <input id="geo-radius" placeholder="50" value="50" />
        </div>
        <div class="form-group">
          <label>Label (optional)</label>
          <input id="geo-label" placeholder="Tallinn Bay" />
        </div>
      </div>

      <div class="form-group">
        <label>Mode</label>
        <select id="mode-select">
          <option value="fast">Fast (~2 min)</option>
          <option value="deep">Deep (~10 min)</option>
        </select>
      </div>
      <button class="btn btn-primary" id="run-btn" onclick="launchRun()">▶ Run</button>
      <button class="btn btn-danger" id="stop-btn" onclick="stopRun()" style="display:none">■ Stop</button>
      <hr style="border-color:#30363d;margin:1em 0;" />
      <h3 style="color:#58a6ff;margin-bottom:0.5em;">Vessel Positions</h3>
      <div id="map"></div>
      <div id="positions-info" style="font-size:0.8em;color:#8b949e;margin-top:0.5em;"></div>
    </div>
  </div>
</div>
<script>
let currentRunId = null;
let currentRunFromList = null;
let map = null;
let markers = [];
const evt = new EventSource("/events");
const sseStatus = document.getElementById("sse-status");

evt.onopen = () => sseStatus.textContent = "connected";
evt.onerror = () => sseStatus.textContent = "disconnected";
evt.onmessage = (e) => {
  try {
    const d = JSON.parse(e.data);
    if (d.type === "stats" && d.run_id === (currentRunFromList || currentRunId)) {
      renderStats(d.data);
    }
  } catch(e) {}
};

function renderStats(data) {
  const el = document.getElementById("live-stats");
  const cards = [
    {label:"Progress", value:data.Progress, cls:""},
    {label:"Completed", value:data.Completed, cls:"success"},
    {label:"Pending", value:data.Pending, cls:"info"},
    {label:"Running", value:data.Running, cls:"high"},
    {label:"Failed", value:data.Failed, cls:"critical"},
    {label:"Total", value:data.Total, cls:""},
  ];
  el.innerHTML = cards.map(c => `<div class="stat-card"><div class="label">${c.label}</div><div class="value ${c.cls}">${c.value}</div></div>`).join("");
}

async function loadRuns() {
  const r = await fetch("/runs");
  const runs = await r.json();
  const el = document.getElementById("run-list");
  el.innerHTML = runs.map(r => {
    const dt = r.generated_at || new Date(r.mtime*1000).toLocaleString();
    return `<div class="run-item" onclick="selectRun('${r.id}')" id="ri-${r.id}">
      <div style="font-weight:${r.has_assessment?'bold':'normal'}">${r.id.slice(0,16)}</div>
      <div class="date">${dt}${r.targets ? ' · '+r.targets+' targets' : ''}</div>
    </div>`;
  }).join("");
}

async function selectRun(rid) {
  currentRunFromList = rid;
  document.querySelectorAll(".run-item").forEach(el => el.classList.remove("active"));
  const el = document.getElementById("ri-" + rid);
  if (el) el.classList.add("active");
  document.getElementById("selected-run").textContent = rid;

  // Load assessment
  const r = await fetch("/run/" + rid + "/assessment");
  const html = await r.text();
  document.getElementById("assessment-view").innerHTML = html;

  // Load positions
  loadPositions(rid);

  // Try stats
  const sr = await fetch("/run/" + rid + "/json");
  try {
    const sj = await sr.json();
    renderStats({
      Progress: sj.unique_targets + " targets",
      Completed: sj.unique_targets || 0,
      Pending: 0, Running: 0, Failed: 0,
      Total: sj.unique_targets || 0,
    });
  } catch(e) {}
}

async function loadPositions(rid) {
  const r = await fetch("/run/" + (rid || "") + "/positions");
  const pts = await r.json();
  const info = document.getElementById("positions-info");
  if (!pts.length) { info.textContent = "No vessel positions found."; return; }
  info.textContent = pts.length + " vessel(s) positioned";
  if (!map) {
    map = L.map("map").setView([59.5, 24.5], 5);
    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {maxZoom:18}).addTo(map);
  }
  markers.forEach(m => map.removeLayer(m));
  markers = [];
  pts.forEach(p => {
    const m = L.circleMarker([p.lat, p.lon], {radius:6, color:"#f85149", fillColor:"#f85149", fillOpacity:0.8})
      .addTo(map)
      .bindPopup(`<b>${p.target_id}</b><br/>Dest: ${p.destination}`);
    markers.push(m);
  });
  if (pts.length) map.fitBounds(markers.map(m => m.getLatLng()), {padding:[30,30]});
}

async function launchRun() {
  const method = document.getElementById("method-select").value;

  if (method === "mmsi") {
    const mmsi = document.getElementById("mmsi-input").value.trim();
    const mode = document.getElementById("mode-select").value;
    if (!mmsi) { alert("Enter at least one MMSI"); return; }
    const r = await fetch("/run/new", {
      method: "POST",
      headers: {"Content-Type": "application/x-www-form-urlencoded"},
      body: "mmsi=" + encodeURIComponent(mmsi) + "&mode=" + encodeURIComponent(mode),
    });
    handleLaunchResponse(r);
  }
  else if (method === "port") {
    const port = document.getElementById("port-select").value;
    const mode = document.getElementById("mode-select").value;
    const r = await fetch("/run/port", {
      method: "POST",
      headers: {"Content-Type": "application/x-www-form-urlencoded"},
      body: "port=" + encodeURIComponent(port) + "&mode=" + encodeURIComponent(mode),
    });
    handleLaunchResponse(r);
  }
  else if (method === "geo") {
    const lat = document.getElementById("geo-lat").value.trim();
    const lon = document.getElementById("geo-lon").value.trim();
    const radius = document.getElementById("geo-radius").value.trim() || "50";
    const label = document.getElementById("geo-label").value.trim() || (lat + "," + lon);
    if (!lat || !lon) { alert("Enter latitude and longitude"); return; }
    const r = await fetch("/run/geo", {
      method: "POST",
      headers: {"Content-Type": "application/x-www-form-urlencoded"},
      body: "lat=" + encodeURIComponent(lat) + "&lon=" + encodeURIComponent(lon)
          + "&radius=" + encodeURIComponent(radius) + "&label=" + encodeURIComponent(label),
    });
    handleLaunchResponse(r);
  }
}

async function handleLaunchResponse(responsePromise) {
  document.getElementById("run-btn").disabled = true;
  document.getElementById("run-btn").textContent = "Running...";
  document.getElementById("stop-btn").style.display = "inline-block";
  try {
    const r = await responsePromise;
    const d = await r.json();
    if (d.run_id) {
      currentRunId = d.run_id;
      document.getElementById("selected-run").textContent = "Running: " + d.run_id;
      document.getElementById("assessment-view").textContent = "Run launched. Waiting for checkpoint data...";
    } else if (d.error) { alert("Error: " + d.error); }
  } catch(e) { alert("Failed: " + e); }
  document.getElementById("run-btn").disabled = false;
  document.getElementById("run-btn").textContent = "▶ Run";
}

function toggleMethod() {
  const m = document.getElementById("method-select").value;
  document.getElementById("method-mmsi").style.display = m === "mmsi" ? "" : "none";
  document.getElementById("method-port").style.display = m === "port" ? "" : "none";
  document.getElementById("method-geo").style.display = m === "geo" ? "" : "none";
}

async function stopRun() {
  if (!currentRunId) return;
  await fetch("/run/" + currentRunId + "/stop", {method:"POST"});
  document.getElementById("stop-btn").style.display = "none";
  document.getElementById("selected-run").textContent = "Stopped: " + currentRunId;
  setTimeout(loadRuns, 1000);
}

// Init
loadRuns();
setInterval(loadRuns, 5000);
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
            for r in sorted(d.iterdir(), reverse=True):
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
