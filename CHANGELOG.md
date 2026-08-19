# Changelog

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
