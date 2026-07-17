"""Worker registry — config-file loading + auto-discover."""

from __future__ import annotations

import importlib
import inspect
import pkgutil
import sys
from pathlib import Path
from typing import Optional

from .worker_base import SirbWorker


class WorkerRegistry(dict[str, SirbWorker]):
    """Registry of discovered SirbWorker instances.

    Keys are ``SirbWorker.name``, values are instantiated worker objects.
    Behaves as a dict::

        worker = registry["shipcrawler"]
    """

    def discover(self, config_workers: dict[str, dict] = None,
                 scan_paths: list[str] = None) -> int:
        """Discover and register workers.

        Args:
            config_workers: Dict mapping worker module names to their config,
                e.g. ``{"shipcrawler": {"ports": ["tallinn"]}}``.
                A bare list of strings is also accepted for backward
                compatibility (workers without config).
            scan_paths: Additional filesystem paths to scan for workers.
                Each path is scanned for ``*_worker.py`` files containing
                ``SirbWorker`` subclasses.

        Returns:
            Number of workers registered.
        """
        count = 0

        # Normalise config_workers to dict format
        worker_configs: dict[str, dict] = {}
        if config_workers:
            if isinstance(config_workers, list):
                for item in config_workers:
                    if isinstance(item, str):
                        worker_configs[item] = {}
                    elif isinstance(item, dict):
                        worker_configs.update(item)
            elif isinstance(config_workers, dict):
                worker_configs = config_workers

        # 1. Explicit config imports
        for module_path, cfg in worker_configs.items():
            try:
                worker = self._import_worker(module_path, config=cfg)
                if worker:
                    self[worker.name] = worker
                    count += 1
            except Exception as e:
                print(f"[sirb] WARN failed to load worker {module_path}: {e}")

        # 2. Auto-discover from packages
        if scan_paths:
            for path in scan_paths:
                count += self._scan_directory(path)

        return count

    def discover_package(self, package_name: str = "sirb.workers") -> int:
        """Discover workers from an installed Python package."""
        count = 0
        try:
            pkg = importlib.import_module(package_name)
        except ImportError:
            return 0

        pkg_path = Path(pkg.__file__).parent
        for f in pkg_path.glob("*_worker.py"):
            module_path = f"{package_name}.{f.stem}"
            try:
                worker = self._import_worker(module_path)
                if worker:
                    self[worker.name] = worker
                    count += 1
            except Exception as e:
                print(f"[sirb] WARN auto-discover {module_path}: {e}")

        return count

    def list_workers(self) -> list[dict]:
        """Return a list of worker metadata for display."""
        return [
            {"name": w.name, "description": w.description, "cls": type(w).__name__}
            for w in self.values()
        ]

    # ── internal ────────────────────────────────────────────────────────

    def _import_worker(self, module_path: str,
                       config: dict = None) -> Optional[SirbWorker]:
        module = importlib.import_module(module_path)
        for _, obj in inspect.getmembers(module, inspect.isclass):
            if (issubclass(obj, SirbWorker) and obj is not SirbWorker
                    and hasattr(obj, "name") and obj.name):
                return obj(config=config or {})
        return None

    def _scan_directory(self, dir_path: str) -> int:
        count = 0
        p = Path(dir_path).expanduser().resolve()
        if not p.is_dir():
            return 0

        sys.path.insert(0, str(p.parent))
        try:
            for f in p.glob("*_worker.py"):
                module_name = f.stem
                try:
                    spec = importlib.util.spec_from_file_location(module_name, f)
                    if spec and spec.loader:
                        module = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(module)

                        for _, obj in inspect.getmembers(module, inspect.isclass):
                            if (issubclass(obj, SirbWorker)
                                    and obj is not SirbWorker
                                    and hasattr(obj, "name") and obj.name):
                                self[obj.name] = obj()
                                count += 1
                except Exception as e:
                    print(f"[sirb] WARN scan {f.name}: {e}")
        finally:
            if str(p.parent) in sys.path:
                sys.path.remove(str(p.parent))

        return count
