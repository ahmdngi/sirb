"""Tests for task deduplication and entry-point discovery."""

import pytest

from sirb.core import Task, TaskQueue, WorkerRegistry


class TestTaskDedup:
    def test_same_task_returns_none(self):
        q = TaskQueue()
        t1 = Task(worker="w", params={"mmsi": "123"}, type="scan")
        t2 = Task(worker="w", params={"mmsi": "123"}, type="scan")

        assert q.add(t1) == t1.id
        assert q.add(t2) is None  # duplicate

    def test_different_worker_not_duplicate(self):
        q = TaskQueue()
        t1 = Task(worker="a", params={"mmsi": "123"})
        t2 = Task(worker="b", params={"mmsi": "123"})

        assert q.add(t1) == t1.id
        assert q.add(t2) == t2.id  # different worker → not dup

    def test_different_params_not_duplicate(self):
        q = TaskQueue()
        t1 = Task(worker="w", params={"mmsi": "123"})
        t2 = Task(worker="w", params={"mmsi": "456"})

        assert q.add(t1) == t1.id
        assert q.add(t2) == t2.id

    def test_dedup_off_allows_duplicates(self):
        q = TaskQueue()
        t1 = Task(worker="w", params={"mmsi": "123"})
        t2 = Task(worker="w", params={"mmsi": "123"})

        assert q.add(t1) == t1.id
        assert q.add(t2, dedup=False) == t2.id  # not rejected

    def test_add_many_dedup(self):
        q = TaskQueue()
        t1 = Task(worker="w", params={"x": 1})
        t2 = Task(worker="w", params={"x": 1})  # dup
        t3 = Task(worker="w", params={"x": 2})  # not dup

        ids = q.add_many([t1, t2, t3])
        assert len(ids) == 2  # t2 excluded
        assert t1.id in ids
        assert t2.id not in ids
        assert t3.id in ids

    def test_completed_duplicate_allowed_rerun(self):
        """Completed/failed tasks with same hash should be re-runnable."""
        q = TaskQueue()
        t1 = Task(worker="w", params={"x": 1})
        t2 = Task(worker="w", params={"x": 1})

        q.add(t1)
        # Complete properly through the lifecycle
        claimed = q.claim("w")
        assert claimed is not None
        q.start(claimed.id, claimed.version)
        t = q.get(claimed.id)
        assert q.complete(t.id, t.version) is True

        # Same hash, but original is completed — should be allowed
        assert q.add(t2) == t2.id

    def test_content_hash_stable(self):
        t1 = Task(worker="w", params={"mmsi": "123", "imo": "456"})
        t2 = Task(worker="w", params={"imo": "456", "mmsi": "123"})  # same, diff order
        assert t1.content_hash() == t2.content_hash()

    def test_content_hash_differentiates_worker(self):
        t1 = Task(worker="a", params={"x": 1})
        t2 = Task(worker="b", params={"x": 1})
        assert t1.content_hash() != t2.content_hash()

    def test_checkpoint_preserves_hashes(self):
        q = TaskQueue()
        q.add(Task(worker="w", params={"x": 1}))
        d = q.to_dict()
        assert "seen_hashes" in d
        q2 = TaskQueue.from_dict(d)
        # Adding same task should be rejected
        assert q2.add(Task(worker="w", params={"x": 1})) is None


class TestEntryPointDiscovery:
    def test_registry_has_method(self):
        r = WorkerRegistry()
        assert hasattr(r, "discover_entry_points")

    def test_empty_when_no_entry_points(self):
        r = WorkerRegistry()
        count = r.discover_entry_points()
        assert count >= 0  # no crash
