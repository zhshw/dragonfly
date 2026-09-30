"""Actual pinned worker with bounded dispatch; no installed-package patching."""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from docket import Docket
from docket.dependencies import CurrentExecution, Perpetual
from docket.execution import ExecutionState
from redis.asyncio import Redis
from redis.exceptions import ResponseError

from agent_memory_server.config import settings
from agent_memory_server.bounded_docket import BoundedDocket, QueueCapacityError
from agent_memory_server.docket_worker import (
    BOUNDED_SCHEDULER_SCRIPT,
    BoundedDocketWorker,
)
from agent_memory_server.models import TaskStatusEnum
from agent_memory_server.operations import COUNTER_KEY
from agent_memory_server.tasks import get_task
from agent_memory_server.utils import redis as redis_utils

from conftest import SCHEDULER_SHA
from test_persistence import persistence_task
from test_docket import blocked_task


async def automatic_probe(
    execution=CurrentExecution(), perpetual=Perpetual(every=timedelta(seconds=60), automatic=True)
):
    async with execution.docket.redis() as client:
        await client.set("automatic:executed", "yes")


async def test_bounded_scheduler_retains_remainder_and_future(dragonfly_factory):
    url = dragonfly_factory(f"--lua_undeclared_keys_shas={SCHEDULER_SHA}")
    async with Redis.from_url(url) as client, Docket(name="bounded", url=url) as docket:
        when = datetime.now(UTC) + timedelta(seconds=10)
        keys = [f"due-{i:02}" for i in range(11)]
        for key in keys:
            await docket.add(persistence_task, key=key, when=when)(17)
        await docket.add(persistence_task, key="future", when=when + timedelta(seconds=10))(18)
        worker = BoundedDocketWorker(docket, scheduler_batch_size=4)
        for remaining, total in ((8, 12), (4, 8), (1, 4)):
            result = await worker.schedule_due_tasks(client, now=when.timestamp() + 1)
            assert result == [total, min(4, total - 1)]
            assert await client.zcard(docket.queue_key) == remaining
        rows = await client.xrange(docket.stream_key)
        assert {row[1][b"key"].decode() for row in rows} == set(keys)
        assert len(rows) == 11
        assert await client.zrange(docket.queue_key, 0, -1) == [b"future"]
        assert await client.exists(docket.parked_task_key("future"))
        assert all(
            await asyncio.gather(
                *[client.hget(f"{docket.prefix}:runs:{key}", "state") for key in keys]
            )
        )
        script = client.register_script(BOUNDED_SCHEDULER_SCRIPT)
        for invalid in (0, 65, -1, "bad", 1.5):
            with pytest.raises(ResponseError, match="Invalid bounded scheduler"):
                await script(
                    keys=[docket.queue_key, docket.stream_key],
                    args=[when.timestamp() + 11, docket.prefix, invalid],
                )
        assert await client.zcard(docket.queue_key) == 1


async def test_bounded_worker_multiple_batches_delayed_and_pending(dragonfly_factory, monkeypatch):
    url = dragonfly_factory(f"--lua_undeclared_keys_shas={SCHEDULER_SHA}")
    async with Redis.from_url(url) as client, Docket(name="bounded", url=url) as docket:
        monkeypatch.setattr(redis_utils, "_redis_pool", client)
        docket.register(persistence_task)
        executions = [await docket.add(persistence_task, key="claimed")(9)]
        await docket._ensure_stream_and_group()
        assert await client.xreadgroup(
            docket.worker_group_name, "lost", {docket.stream_key: ">"}, count=1
        )
        when = datetime.now(UTC) + timedelta(milliseconds=150)
        executions += [
            await docket.add(persistence_task, key=f"batch-{i}", when=when)(i) for i in range(11)
        ]
        async with BoundedDocketWorker(
            docket,
            name="bounded-worker",
            scheduler_batch_size=4,
            concurrency=3,
            schedule_automatic_tasks=False,
            redelivery_timeout=timedelta(milliseconds=20),
            scheduling_resolution=timedelta(milliseconds=20),
            minimum_check_interval=timedelta(milliseconds=20),
        ) as worker:
            await asyncio.wait_for(worker.run_until_finished(), timeout=10)
        for execution in executions:
            loaded = await docket.get_execution(execution.key)
            assert loaded.state == ExecutionState.COMPLETED
            assert (await get_task(execution.key)).status == TaskStatusEnum.SUCCESS
            assert (await loaded.get_result(timeout=timedelta(seconds=1)))[
                "value"
            ] == execution.args[0]
            assert (
                await client.hget("probe:effects", execution.key) == str(execution.args[0]).encode()
            )
            assert not await client.exists(docket.parked_task_key(execution.key))
        assert not await client.zcard(docket.queue_key)
        assert not await client.xlen(docket.stream_key)
        assert (await client.xpending(docket.stream_key, docket.worker_group_name))["pending"] == 0


async def test_bounded_scheduler_discards_only_selected_orphans(dragonfly_factory):
    url = dragonfly_factory(f"--lua_undeclared_keys_shas={SCHEDULER_SHA}")
    async with Redis.from_url(url) as client, Docket(name="bounded", url=url) as docket:
        await client.zadd(docket.queue_key, {f"orphan-{i}": 1 for i in range(9)})
        worker = BoundedDocketWorker(docket, scheduler_batch_size=4)
        assert await worker.schedule_due_tasks(client, now=2) == [9, 0]
        assert await client.zcard(docket.queue_key) == 5
        assert await worker.schedule_due_tasks(client, now=2) == [5, 0]
        assert await client.zcard(docket.queue_key) == 1
        assert await worker.schedule_due_tasks(client, now=2) == [1, 0]
        assert not await client.zcard(docket.queue_key)


async def test_bounded_scheduler_denied_sha_retains_work_and_records_failure(dragonfly_factory):
    url = dragonfly_factory()
    async with Redis.from_url(url) as client, BoundedDocket(name="denied", url=url) as docket:
        when = datetime.now(UTC) + timedelta(seconds=60)
        await docket.add(persistence_task, key="retained", when=when)(1)
        worker = BoundedDocketWorker(docket)
        before = await client.hgetall(docket.parked_task_key("retained"))
        with pytest.raises(ResponseError, match="undeclared"):
            await worker.schedule_due_tasks(client, now=when.timestamp() + 1)
        assert await client.zrange(docket.queue_key, 0, -1) == [b"retained"]
        assert await client.hgetall(docket.parked_task_key("retained")) == before
        assert not await client.xlen(docket.stream_key)
        assert await client.hget(COUNTER_KEY, "lua_failure") == b"1"


@pytest.mark.parametrize("size", [0, 65, -1, True, 1.5])
async def test_bounded_worker_rejects_invalid_configuration(size):
    with pytest.raises(ValueError, match="batch size"):
        BoundedDocketWorker(None, scheduler_batch_size=size)


async def test_bounded_worker_run_uses_subclass(dragonfly_factory, monkeypatch):
    url = dragonfly_factory(f"--lua_undeclared_keys_shas={SCHEDULER_SHA}")
    calls = []
    original = BoundedDocketWorker.schedule_due_tasks

    async def measured(self, client, now=None):
        calls.append(self.scheduler_batch_size)
        return await original(self, client, now)

    monkeypatch.setattr(BoundedDocketWorker, "schedule_due_tasks", measured)
    monkeypatch.setattr(settings, "docket_scheduler_batch_size", 3)
    await asyncio.wait_for(
        BoundedDocketWorker.run(
            docket_name="empty",
            url=url,
            tasks=[],
            until_finished=True,
            schedule_automatic_tasks=False,
        ),
        timeout=5,
    )
    assert calls and set(calls) == {3}


@pytest.mark.parametrize("delayed", [False, True])
async def test_bounded_queue_atomic_concurrent_admission(dragonfly_factory, delayed):
    url = dragonfly_factory()
    async with (
        Redis.from_url(url) as client,
        BoundedDocket(name="capacity", url=url, max_pending_tasks=4) as docket,
    ):
        when = datetime.now(UTC) + timedelta(seconds=60) if delayed else None
        results = await asyncio.gather(
            *[docket.add(persistence_task, key=f"task-{i}", when=when)(i) for i in range(24)],
            return_exceptions=True,
        )
        accepted = [item for item in results if not isinstance(item, BaseException)]
        assert len(accepted) == 4
        assert sum(isinstance(item, QueueCapacityError) for item in results) == 20
        assert await client.zcard(docket.queue_key) + await client.xlen(docket.stream_key) == 4
        assert await client.hget(COUNTER_KEY, "queue_rejected") == b"20"
        before = await client.hgetall(f"{docket.prefix}:runs:{accepted[0].key}")
        await docket.add(persistence_task, key=accepted[0].key, when=when)(99)
        assert await client.hgetall(f"{docket.prefix}:runs:{accepted[0].key}") == before
        assert await client.zcard(docket.queue_key) + await client.xlen(docket.stream_key) == 4
        await docket.cancel(accepted[0].key)
        assert await client.zcard(docket.queue_key) + await client.xlen(docket.stream_key) == 3
        await docket.add(persistence_task, key="after-cancel", when=when)(25)
        assert await client.zcard(docket.queue_key) + await client.xlen(docket.stream_key) == 4


async def test_bounded_queue_mixed_reschedule_replace_and_rejection(dragonfly_factory):
    url = dragonfly_factory()
    async with (
        Redis.from_url(url) as client,
        BoundedDocket(name="capacity", url=url, max_pending_tasks=2) as docket,
    ):
        when = datetime.now(UTC) + timedelta(seconds=60)
        immediate = await docket.add(persistence_task, key="active")(1)
        await docket.add(persistence_task, key="delayed", when=when)(2)
        await docket._ensure_stream_and_group()
        deliveries = await client.xreadgroup(
            docket.worker_group_name, "lost", {docket.stream_key: ">"}, count=1
        )
        message_id = deliveries[0][1][0][0]
        with pytest.raises(QueueCapacityError):
            await docket.add(persistence_task, key="rejected")(3)
        assert not await client.exists(f"{docket.prefix}:runs:rejected")
        assert not await client.exists(docket.parked_task_key("rejected"))
        immediate.when = when
        await immediate.schedule(reschedule_message=message_id)
        assert not await client.xlen(docket.stream_key)
        assert (await client.xpending(docket.stream_key, docket.worker_group_name))["pending"] == 0
        assert await client.zcard(docket.queue_key) == 2
        await docket.replace(persistence_task, key="delayed", when=when)(4)
        assert await client.zcard(docket.queue_key) == 2
        with pytest.raises(QueueCapacityError):
            await docket.replace(persistence_task, key="new-replacement", when=when)(5)
        assert await client.zcard(docket.queue_key) == 2
        # Missing/stale stream IDs receive no credit: rejection must precede ACK.
        with pytest.raises(QueueCapacityError):
            await immediate.schedule(reschedule_message=b"1-0")
        assert await client.zcard(docket.queue_key) == 2


async def test_bounded_queue_completion_releases_capacity(dragonfly_factory, monkeypatch):
    url = dragonfly_factory(f"--lua_undeclared_keys_shas={SCHEDULER_SHA}")
    async with (
        Redis.from_url(url) as client,
        BoundedDocket(name="capacity", url=url, max_pending_tasks=3) as docket,
    ):
        monkeypatch.setattr(redis_utils, "_redis_pool", client)
        docket.register(persistence_task)
        when = datetime.now(UTC) + timedelta(milliseconds=50)
        for i in range(3):
            await docket.add(persistence_task, key=f"fill-{i}", when=when)(i)
        with pytest.raises(QueueCapacityError):
            await docket.add(persistence_task, key="blocked")(99)
        async with BoundedDocketWorker(
            docket,
            scheduler_batch_size=2,
            schedule_automatic_tasks=False,
            minimum_check_interval=timedelta(milliseconds=20),
            scheduling_resolution=timedelta(milliseconds=20),
        ) as worker:
            await asyncio.wait_for(worker.run_until_finished(), timeout=5)
        assert not await client.zcard(docket.queue_key)
        assert not await client.xlen(docket.stream_key)
        await docket.add(persistence_task, key="after-completion")(4)
        assert await client.xlen(docket.stream_key) == 1


@pytest.mark.parametrize("state", ["queued", "running"])
async def test_bounded_cancel_atomically_clears_pending_and_releases_capacity(
    dragonfly_factory, state
):
    url = dragonfly_factory(f"--lua_undeclared_keys_shas={SCHEDULER_SHA}")
    async with (
        Redis.from_url(url) as client,
        BoundedDocket(name="capacity", url=url, max_pending_tasks=1) as docket,
    ):
        when = datetime.now(UTC) + timedelta(seconds=60)
        execution = await docket.add(persistence_task, key="cancel-pending", when=when)(1)
        worker = BoundedDocketWorker(docket, scheduler_batch_size=1)
        await worker.schedule_due_tasks(client, now=when.timestamp() + 1)
        await docket._ensure_stream_and_group()
        delivery = await client.xreadgroup(
            docket.worker_group_name, "dead-worker", {docket.stream_key: ">"}, count=1
        )
        message_id = delivery[0][1][0][0]
        if state == "running":
            assert await execution.claim("dead-worker")
        assert await client.hget(f"{docket.prefix}:runs:cancel-pending", "stream_id") == message_id
        assert (await client.xpending(docket.stream_key, docket.worker_group_name))["pending"] == 1
        await docket.cancel("cancel-pending")
        assert (await docket.get_execution("cancel-pending")).state == ExecutionState.CANCELLED
        assert not await client.xlen(docket.stream_key)
        assert (await client.xpending(docket.stream_key, docket.worker_group_name))["pending"] == 0
        await docket.add(persistence_task, key="reused-slot")(2)
        assert await client.xlen(docket.stream_key) == 1


async def test_bounded_active_worker_cancel_converges(dragonfly_factory):
    url = dragonfly_factory(f"--lua_undeclared_keys_shas={SCHEDULER_SHA}")
    async with (
        Redis.from_url(url) as client,
        BoundedDocket(name="capacity", url=url, max_pending_tasks=1) as docket,
    ):
        docket.register(blocked_task)
        await docket.add(blocked_task, key="active-worker")(1)
        async with BoundedDocketWorker(
            docket,
            name="active-worker",
            schedule_automatic_tasks=False,
            minimum_check_interval=timedelta(milliseconds=20),
        ) as worker:
            running = asyncio.create_task(worker.run_until_finished())
            try:
                async with asyncio.timeout(5):
                    while (
                        await docket.get_execution("active-worker")
                    ).state != ExecutionState.RUNNING:
                        await asyncio.sleep(0.02)
                await docket.cancel("active-worker")
                await asyncio.wait_for(running, timeout=5)
            finally:
                if not running.done():
                    running.cancel()
                    await asyncio.gather(running, return_exceptions=True)
        assert (await docket.get_execution("active-worker")).state == ExecutionState.CANCELLED
        assert not await client.xlen(docket.stream_key)
        assert (await client.xpending(docket.stream_key, docket.worker_group_name))["pending"] == 0


async def test_bounded_queue_legacy_backlog_can_reschedule_without_growth(dragonfly_factory):
    url = dragonfly_factory()
    async with (
        Redis.from_url(url) as client,
        Docket(name="capacity", url=url) as legacy,
        BoundedDocket(name="capacity", url=url, max_pending_tasks=1) as docket,
    ):
        when = datetime.now(UTC) + timedelta(seconds=60)
        for i in range(3):
            await legacy.add(persistence_task, key=f"legacy-{i}", when=when)(i)
        assert await client.zcard(docket.queue_key) == 3
        with pytest.raises(QueueCapacityError):
            await docket.add(persistence_task, key="new")(4)
        await docket.replace(persistence_task, key="legacy-0", when=when)(5)
        assert await client.zcard(docket.queue_key) == 3


async def test_bounded_worker_full_startup_defers_and_recovers_automatic_tasks(
    dragonfly_factory, monkeypatch
):
    url = dragonfly_factory(f"--lua_undeclared_keys_shas={SCHEDULER_SHA}")
    async with (
        Redis.from_url(url) as client,
        BoundedDocket(name="capacity", url=url, max_pending_tasks=1) as docket,
    ):
        monkeypatch.setattr(redis_utils, "_redis_pool", client)
        docket.register(automatic_probe)
        await docket.add(persistence_task, key="startup-full")(1)
        async with BoundedDocketWorker(
            docket,
            name="automatic-worker",
            scheduling_resolution=timedelta(milliseconds=20),
            minimum_check_interval=timedelta(milliseconds=20),
        ) as worker:
            running = asyncio.create_task(worker.run_forever())
            try:
                async with asyncio.timeout(5):
                    while not await client.exists("automatic:executed"):
                        await asyncio.sleep(0.02)
                assert (
                    await docket.get_execution("startup-full")
                ).state == ExecutionState.COMPLETED
                assert not worker._automatic_capacity_deferred
            finally:
                running.cancel()
                await asyncio.gather(running, return_exceptions=True)
        assert await client.zcard(docket.queue_key) + await client.xlen(docket.stream_key) <= 1
