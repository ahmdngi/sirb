"""Tests for Sirb core components."""

import pytest
import time
from sirb.core import (
    Task, TaskStatus, TaskQueue, Router, WorkerRegistry,
    Blackboard, Finding,
)
from sirb.core.worker_base import SirbWorker


# ── Dummy worker for tests ──────────────────────────────────────────────

class TestWorker(SirbWorker):
    name = "test-worker"
    description = "A test worker"

    async def execute(self, task):
        from sirb.core import Result
        return Result(task_id=task.id, worker=self.name, status="success")

    async def discover(self):
        return [
            Task(type="test", worker=self.name, params={"x": 1}),
            Task(type="test", worker=self.name, params={"x": 2}),
        ]

    def rate_limits(self):
        return {"test_api": 60}


# ── TaskQueue ───────────────────────────────────────────────────────────

class TestTaskQueue:
    def test_add_and_claim(self):
        q = TaskQueue()
        task = Task(type="test", worker="test", priority=1)
        q.add(task)
        assert q.count() == 1
        assert q.count(TaskStatus.PENDING) == 1

        claimed = q.claim("worker1")
        assert claimed is not None
        assert claimed.id == task.id
        assert claimed.status == TaskStatus.CLAIMED

    def test_priority_ordering(self):
        q = TaskQueue()
        low = Task(type="test", worker="test", priority=2, id="low",
                   params={"order": 2})
        high = Task(type="test", worker="test", priority=0, id="high",
                    params={"order": 0})
        mid = Task(type="test", worker="test", priority=1, id="mid",
                   params={"order": 1})
        q.add_many([low, high, mid])

        first = q.claim("w")
        assert first.id == "high"
        second = q.claim("w")
        assert second.id == "mid"
        third = q.claim("w")
        assert third.id == "low"

    def test_dependency_blocking(self):
        q = TaskQueue()
        dep = Task(type="dep", worker="w", id="dep", params={"sub": "dep"})
        blocked = Task(type="main", worker="w", id="main", depends_on=["dep"],
                       params={"sub": "main"})
        q.add_many([blocked, dep])

        # Should get dep first, not blocked
        first = q.claim("w")
        assert first.id == "dep"
        q.start("dep", first.version)
        # Read fresh state after transition
        t = q.get("dep")
        q.complete("dep", t.version)

        # Now blocked should be available
        second = q.claim("w")
        assert second.id == "main"

    def test_retry_logic(self):
        q = TaskQueue()
        task = Task(type="test", worker="w", max_retries=2)
        q.add(task)
        claimed = q.claim("w")
        q.start(task.id, claimed.version)

        # Fail once — read fresh version
        t = q.get(task.id)
        q.fail(task.id, "error 1", t.version)
        assert q.get(task.id).status == TaskStatus.PENDING  # reset for retry

        # Claim again, run, fail again
        c2 = q.claim("w2")
        assert c2 is not None
        q.start(task.id, c2.version)
        t = q.get(task.id)
        q.fail(task.id, "error 2", t.version)
        assert q.get(task.id).status == TaskStatus.PENDING

        # Third fail exhausts
        c3 = q.claim("w3")
        assert c3 is not None
        q.start(task.id, c3.version)
        t = q.get(task.id)
        q.fail(task.id, "error 3", t.version)
        assert q.get(task.id).status == TaskStatus.FAILED

    def test_clear_non_terminal(self):
        q = TaskQueue()
        t1 = Task(type="a", worker="w", id="t1", params={"sub": "a"})
        t2 = Task(type="b", worker="w", id="t2", params={"sub": "b"})
        q.add_many([t1, t2])

        # Complete t1
        c = q.claim("w")
        q.start(c.id, c.version)
        t = q.get("t1")
        q.complete("t1", t.version)

        removed = q.clear_non_terminal()
        assert removed == 1  # t2 was pending
        assert q.get("t1") is not None  # preserved
        assert q.get("t2") is None  # removed

    def test_serialization(self):
        q = TaskQueue()
        tasks = [
            Task(type="a", worker="w1", priority=1),
            Task(type="b", worker="w2", priority=2),
        ]
        q.add_many(tasks)

        d = q.to_dict()
        q2 = TaskQueue.from_dict(d)
        assert q2.count() == 2
        assert q2.claim("w") is not None


# ── Router ──────────────────────────────────────────────────────────────

class TestRouter:
    def test_route_success(self):
        registry = WorkerRegistry()
        registry["test-worker"] = TestWorker()
        router = Router(registry)

        task = Task(type="test", worker="test-worker")
        worker = router.route(task)
        assert worker is not None
        assert worker.name == "test-worker"

    def test_route_unknown(self):
        registry = WorkerRegistry()
        router = Router(registry)
        assert router.route(Task(worker="nowhere")) is None

    def test_validate_task(self):
        registry = WorkerRegistry()
        registry["test-worker"] = TestWorker()
        router = Router(registry)

        assert router.validate_task(Task(worker="")) is not None
        assert router.validate_task(Task(worker="nowhere")) is not None
        assert router.validate_task(Task(worker="test-worker", type="")) is not None
        assert router.validate_task(Task(worker="test-worker", type="ok")) is None


# ── Worker Discovery ────────────────────────────────────────────────────

class TestWorkerDiscovery:
    def test_direct_registration(self):
        registry = WorkerRegistry()
        w = TestWorker()
        registry[w.name] = w
        assert registry["test-worker"].name == "test-worker"

    def test_worker_discover(self):
        w = TestWorker()
        tasks = asyncio_run(w.discover())
        assert len(tasks) == 2
        assert tasks[0].worker == "test-worker"
        assert tasks[1].params["x"] == 2

    def test_list_workers(self):
        registry = WorkerRegistry()
        registry["test-worker"] = TestWorker()
        info = registry.list_workers()
        assert len(info) == 1
        assert info[0]["name"] == "test-worker"


# ── Blackboard ──────────────────────────────────────────────────────────

class TestBlackboard:
    def test_add_and_query(self):
        bb = Blackboard()
        f1 = Finding(target_id="vessel1", target_type="vessel",
                     finding_type="shadow_fleet", severity="critical",
                     weight=1.0, source="equasis", worker="test-worker")
        f2 = Finding(target_id="vessel2", target_type="vessel",
                     finding_type="info", severity="low",
                     weight=0.5, source="web", worker="test-worker")
        bb.add_many([f1, f2])
        assert bb.count() == 2

        critical = bb.query(severity="critical")
        assert len(critical) == 1
        assert critical[0].target_id == "vessel1"

    def test_pheromone_decay(self):
        bb = Blackboard(decay_rate=0.5)
        f = Finding(finding_type="test", weight=1.0)
        bb.add(f)
        bb.decay()
        assert bb.all()[0].weight == 0.5
        bb.decay()
        assert bb.all()[0].weight == 0.25
        bb.decay()
        bb.decay()
        assert bb.count() == 0  # pruned below 0.1

    def test_triggers(self):
        bb = Blackboard()
        bb.register_trigger({"severity": "critical", "source": "shodan"},
                            "wake_aggregator")
        f = Finding(finding_type="vuln", severity="critical", source="shodan")
        actions = bb.check_triggers(f)
        assert "wake_aggregator" in actions

    def test_serialization(self):
        bb = Blackboard()
        bb.add(Finding(finding_type="test", target_id="x"))
        bb.register_trigger({"severity": "high"}, "alert")

        d = bb.to_dict()
        bb2 = Blackboard.from_dict(d)
        assert bb2.count() == 1
        assert len(bb2._triggers) == 1


# ── Helpers ─────────────────────────────────────────────────────────────

def asyncio_run(coro):
    import asyncio
    return asyncio.run(coro)
