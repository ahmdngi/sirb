# Sirb — Pitfalls & Operational Notes

> Collected from the Aug 2026 runner→core migration and live debugging.
> These are the failure modes that cost the most time — read before touching
> the dashboard, worker registration, or deploy environment.

## 1. Worker discovery is venv-scoped (the "empty dropdown" bug)

The dashboard discovers workers via Python **entry points** registered in the
venv where `shipcrawler-worker` is installed. If the worker isn't installed in
the **same venv that runs the dashboard**, `/api/workers` returns `[]` and the
dropdown is empty — even though the code and repos are byte-identical to a
working host.

**Symptoms:** `sirb list-workers` → `no workers discovered`; dropdown empty.

**Fix:**
```bash
# must be the SAME venv as the dashboard process (here: /root/venvs/dashboards)
/root/venvs/dashboards/bin/pip install -e /root/git-repos-personal/shipcrawler-worker
/root/venvs/dashboards/bin/sirb list-workers   # verify discovery
```

**Verify:** `curl -s http://127.0.0.1:8502/api/workers`
→ `[{"name": "shipcrawler", ...}]`

## 2. Stale `__pycache__` serves OLD dashboard HTML

Python will serve a cached `.pyc` that is **newer** than the source `.py`,
even when `import sirb.cli.main` resolves to the repo file. Symptom: the
served page is missing a feature (e.g. the 100-batch option) although the
source file on disk has it.

**Fix:**
```bash
rm -rf /root/git-repos-personal/sirb/sirb/cli/__pycache__ \
       /root/git-repos-personal/sirb/sirb/__pycache__
# then restart the dashboard
```

**Also:** a regular `pip install` (non-editable) copies the package into
site-packages; repo edits never appear. Always use
`pip install -e /root/git-repos-personal/sirb`. Verify editable state:
```bash
cat /root/venvs/dashboards/lib/python3.11/site-packages/sirb-*.dist-info/direct_url.json
# {"dir_info": {"editable": true}, "url": "file:///root/git-repos-personal/sirb"}
```

## 3. HERMES_REAL_HOME

The Hermes profile sandbox sets `HOME=/root/.hermes/profiles/shipcrawler/home`.
The dashboard's `_get_runs_dir()` must resolve `~` against the REAL home
(`/root`) or runs land in the profile sandbox. Keep
`HERMES_REAL_HOME=/root` in the dashboard's environment.

## 4. Runs directories

Run outputs live in the **vault**, not `/root/sirb-reports`:
- sirb swarm runs → `~/hermes-vault/sirb-reports/<run_id>/`
- shipcrawler runs → `~/hermes-vault/osint-reports/<vessel>-<date>-<hash>-report/`

`/root/sirb-reports` and `/root/osint-reports` are **stale/empty** — the
real data is under `hermes-vault/`. Don't be fooled by a quick `ls` of the
top-level dirs when checking run history.

## 5. Version drift between hosts

"Sirb is not the same" across machines is almost never a git diff — it's a
**runtime environment** difference. Always compare, in order:
1. `git log --oneline -1 && git status --short` (both hosts)
2. byte parity: `find . -path ./.git -prune -o -type f -print0 | xargs -0 md5sum | sort -k2`
3. installed-vs-editable state (pitfall 2)
4. worker registration in the runtime venv (pitfall 1)
5. served HTML: `curl -s http://127.0.0.1:8502/ | grep -o 'value="[0-9]*"'`

## 6. Dashboard binding

The dashboard binds `0.0.0.0:8502` (accessible on the LAN/tailnet). It does
NOT bind the Tailscale IP only. If you need tailnet-only exposure, bind
`100.124.149.86:8502` explicitly.

## 7. Systemd unit (core, post-Aug-2026)

```bash
systemctl start|stop|restart sirb-dashboard     # system service
journalctl -u sirb-dashboard -f                 # logs
# ExecStart: /root/venvs/dashboards/bin/sirb dashboard --port 8502
# WorkingDirectory: /root/git-repos-personal/sirb
# Env: HERMES_REAL_HOME=/root
```

## 8. GitHub note

`pip install sirb` from PyPI installs a **different project** (a
decentralized-compute CLI by another author). Ahmed's sirb is only
distributed via `https://github.com/ahmdngi/sirb.git` — install editable
from the local repo, never from PyPI.

## 9. Batch/parallelism options

The 100-batch parallelism option lives in `sirb/cli/main.py` (option value
`100` in the launch panel) + `profiles-models.json`. If the served page is
missing it: check pitfall 2 (stale pycache / non-editable install) before
suspecting git state.
