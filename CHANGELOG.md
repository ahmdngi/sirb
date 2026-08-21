# Changelog

## v0.5.4 (2026-08-21)

### Fixed
- **CRITICAL: Task finalization broken — tasks stuck RUNNING.** `claim()`
  bumped version to v+1 and returned a frozen copy; `start()` bumped the real
  task to v+2; `_handle_result` passed the stale copy's version (v+1) to
  `complete()`/`fail()`, whose version check always failed → completion
  silently dropped. No retries ever fired; dedup permanently blocked re-runs;
  checkpoints persisted RUNNING tasks. Fix: `start()` now returns the
  post-start version and `WorkerPool` stashes it on the task copy before
  execution.
- **HIGH: `sirb run` CLI crashed with NameError.** `main.py` used
  `TaskStatus.PENDING` but the import was deleted in v0.5.2 cleanup.
  Re-added `TaskStatus` import.
- **HIGH: Dashboard bound 0.0.0.0 with no auth + CORS *.** Any LAN client
  could read OSINT data, spawn unlimited runs, stop runs, DELETE run dirs.
  Now binds `127.0.0.1` by default (override with `--host 0.0.0.0` for LAN).
  Removed wildcard CORS header.
- **HIGH: Stored XSS on report/vessel pages.** Agent-written markdown
  rendered via `marked.parse()` → `innerHTML` with no sanitization.
  Added DOMPurify — all `marked.parse()` output now wrapped in
  `DOMPurify.sanitize()`.
- **MEDIUM: Path traversal on 9 /run/{rid} endpoints.** The 6 GET endpoints
  (json, assessment, report, tracking, connections, stats) plus
  targets/terminal/vessels had no rid sanitization — `../` in rid escaped
  `runs_base`. Added `_safe_rid()` guard to all endpoints.
- **MEDIUM: Throttle could hang a run.** `block=True` acquire with a
  zero-refill bucket spun forever. Now bounded by `task_timeout`.
- **DESIGN: Agnosticism violated in CLI.** 3 hardcoded
  `from shipcrawler_worker.discovery import PortConfig` imports + 2
  `"shipcrawler"` fallbacks in the "agnostic" dashboard. Added
  `resolve_port_config()` to `SirbWorker` base class; dashboard now calls
  the worker interface. Zero `shipcrawler_worker` imports remain in cli/.
- **Registry docstring referenced nonexistent `discover_filesystem()`.**
  Removed.

### Added
- `--host` flag on `sirb dashboard` (default `127.0.0.1`).
- `resolve_port_config()` optional method on `SirbWorker` — workers that
  know about specific ports (e.g. shipcrawler) override this; default
  returns None.
- 3 regression tests: task completes after `pool.run()`, failure/retry
  path transitions correctly, `start()` returns post-start version.

### Changed
- README version: v0.5.0 → v0.5.4. Fixed `src/sirb/` path reference.
- AGENTS.md version: v0.5.3 → v0.5.4, tests: 70 → 71. Removed false
  "connections tab removed" claim.
- README agnosticism table: removed "Audit clean" (was false); updated
  cli/ evidence to mention `resolve_port_config()`.

## v0.5.3 (2026-08-19)

### Fixed
- **Auto-load report when SSE sends Status=done.** Dashboard stayed frozen showing
  agent activity after run completed — never transitioned to report view. Now
  auto-calls `selectRun()` when SSE receives `Status: "done"`.
- **profiles-models.json missing from package-data.** Model dropdown was broken —
  `profiles-models.json` not in `pyproject.toml` package-data, so editable install
  didn't map it. Added to package-data, force-reinstalled.

## v0.5.2 (2026-08-19)

### Fixed
- **`/stop` endpoint broken — `running_procs` type lie.** Typed as
  `dict[str, subprocess.Popen]` but stored `threading.Thread` for swarm runs.
  `proc.terminate()` raised `AttributeError` (Thread has no `.terminate`),
  swallowed by bare `except` → run never stopped. Now `dict[str, object]` with
  `hasattr` check.
- **`WorkerPool.run(timeout)` parameter was dead.** Signature documented timeout
  but body never used it — infinite swarm could hang forever. Now wired up with
  `time.time()` check + `TimeoutError` catch.
- **`_handle_result` validation silently accepted failures.** `except Exception:
  valid = True` swallowed validation errors. Now logs warning and sets
  `valid = False` — failed validation rejects the task.
- **Path traversal in vessel file endpoint.** `/run/{rid}/vessel/{target}/
  {filename}.md` had no sanitization — `../` in target/filename escaped
  `runs_base`. Now checks `..` + `resolve().relative_to()`.
- **Path traversal in DELETE `/run/{rid}`.** `shutil.rmtree` on unsanitized `rid`
  — `../../` could delete arbitrary dirs. Now validates `..` and `/` in rid +
  `resolve().relative_to()`.
- **Duplicate `_load_assessment_json` function.** Defined twice (L529 shadowed
  by L673). Removed the first.

### Removed
- 5 unused imports: `uuid` (main.py), `json` (blackboard.py), `os` (persistence.py),
  `os` (trends.py), `time` (task_queue.py), `Result` (router.py), `TaskStatus`
  (worker_pool.py), `signal` (main.py).

### Changed
- 6x `WorkerRegistry()` instantiation → cached `_get_registry()` helper.
  Was re-scanning pip entry points on every dashboard request.
- Version drift in AGENTS.md fixed (v0.3.0 → v0.5.1).
- Added `logging` module + `logger` to main.py.

## v0.5.1 (2026-08-17)

### Fixed
- Version drift: `_SIRB_VERSION` now read from `pyproject.toml` at runtime via
  `importlib.metadata.version("sirb")` instead of hardcoded string.
- Dashboard HTML version string injected at render time (was hardcoded "v0.3").
