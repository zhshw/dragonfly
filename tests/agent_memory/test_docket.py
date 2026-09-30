"""Verify the pinned Lua script and actual serialized Docket task lifecycle."""

import ast
import asyncio
import hashlib
import inspect
import subprocess
import sys
import time
from pathlib import Path
from datetime import UTC, datetime, timedelta

import pytest
from docket import Docket, Worker
from docket.dependencies import CurrentExecution, Progress, Retry
from docket.execution import ExecutionState
from redis.asyncio import Redis
from redis.exceptions import ResponseError

from agent_memory_server.models import Task, TaskStatusEnum, TaskTypeEnum
from agent_memory_server.tasks import create_task, get_task, update_task_status
from agent_memory_server.utils import redis as redis_utils

from conftest import SCHEDULER_SHA


def scheduler_script():
    tree = ast.parse(inspect.getsource(Worker))
    method = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "_scheduler_loop"
    )
    call = next(
        node
        for node in ast.walk(method)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "register_script"
    )
    script = ast.literal_eval(call.args[0])
    assert hashlib.sha1(script.encode()).hexdigest() == SCHEDULER_SHA
    return script


async def snapshot(client, prefix, keys):
    return {
        "queue": await client.zrange(f"{prefix}:queue", 0, -1, withscores=True),
        "parked": [await client.hgetall(f"{prefix}:{key}") for key in keys],
        "runs": [await client.hgetall(f"{prefix}:runs:{key}") for key in keys],
        "stream": await client.xrange(f"{prefix}:stream"),
    }


@pytest.mark.parametrize("allow", [False, True])
async def test_scheduler_atomic_batch(dragonfly_factory, allow):
    flags = [f"--lua_undeclared_keys_shas={SCHEDULER_SHA}"] if allow else []
    async with Redis.from_url(dragonfly_factory(*flags), decode_responses=True) as client:
        script = client.register_script(scheduler_script())
        prefix = "memory-server"
        keys = [f"task-{i}" for i in range(4)]
        assert await script(keys=[f"{prefix}:queue", f"{prefix}:stream"], args=[100, prefix]) == [
            0,
            0,
        ]
        for key in keys:
            await client.hset(
                f"{prefix}:{key}",
                mapping={
                    "key": key,
                    "when": 1,
                    "function": "probe",
                    "args": "serialized",
                    "kwargs": "serialized",
                    "attempt": 1,
                    "generation": 2,
                },
            )
            await client.hset(f"{prefix}:runs:{key}", mapping={"state": "scheduled"})
            await client.zadd(f"{prefix}:queue", {key: 1})
        before = await snapshot(client, prefix, keys)
        if not allow:
            with pytest.raises(ResponseError, match="undeclared key"):
                await script(keys=[f"{prefix}:queue", f"{prefix}:stream"], args=[100, prefix])
            assert await snapshot(client, prefix, keys) == before
        else:
            assert await script(
                keys=[f"{prefix}:queue", f"{prefix}:stream"], args=[100, prefix]
            ) == [4, 4]
            after = await snapshot(client, prefix, keys)
            assert not after["queue"]
            assert after["parked"] == [{}, {}, {}, {}]
            assert after["runs"] == [{"state": "queued"}] * 4
            assert {row[1]["key"] for row in after["stream"]} == set(keys)
            assert all(row[1]["generation"] == "2" for row in after["stream"])


async def recorded_task(value: int, execution=CurrentExecution(), progress=Progress()):
    await create_task(Task(id=execution.key, type=TaskTypeEnum.SUMMARY_VIEW_FULL_RUN))
    await update_task_status(
        execution.key, status=TaskStatusEnum.RUNNING, started_at=datetime.now(UTC)
    )
    await progress.set_total(1)
    await progress.increment()
    async with execution.docket.redis() as client:
        await client.hset("probe:effects", execution.key, value)
        stored_progress = await client.hget(
            f"{execution.docket.prefix}:progress:{execution.key}", "current"
        )
        assert stored_progress == b"1"
    await update_task_status(
        execution.key, status=TaskStatusEnum.SUCCESS, completed_at=datetime.now(UTC)
    )
    return {"value": value}


async def retry_task(
    execution=CurrentExecution(), retry=Retry(attempts=2, delay=timedelta(milliseconds=50))
):
    if retry.attempt == 1:
        raise ValueError("temporary failure")
    return await recorded_task(42, execution=execution, progress=execution.progress)


async def run_worker(docket):
    async with Worker(
        docket,
        name="probe-worker",
        schedule_automatic_tasks=False,
        scheduling_resolution=timedelta(milliseconds=20),
        minimum_check_interval=timedelta(milliseconds=20),
    ) as worker:
        await asyncio.wait_for(worker.run_until_finished(), timeout=15)


async def test_real_worker_and_states(dragonfly_factory, monkeypatch):
    url = dragonfly_factory(f"--lua_undeclared_keys_shas={SCHEDULER_SHA}")
    async with (
        Redis.from_url(url) as metadata_client,
        Docket(name="memory-server", url=url) as docket,
    ):
        monkeypatch.setattr(redis_utils, "_redis_pool", metadata_client)
        executions = [
            await docket.add(recorded_task, key="immediate")(7),
            await docket.add(
                recorded_task, key="delayed", when=datetime.now(UTC) + timedelta(milliseconds=100)
            )(8),
        ]
        async with docket.redis() as client:
            assert await client.zcard(docket.queue_key) == 1
        completed = await asyncio.gather(
            run_worker(docket),
            *(execution.get_result(timeout=timedelta(seconds=5)) for execution in executions),
        )
        assert completed[1:] == [{"value": 7}, {"value": 8}]
        for execution, value in zip(executions, [7, 8]):
            loaded = await docket.get_execution(execution.key)
            assert loaded.state == ExecutionState.COMPLETED
            assert await loaded.get_result(timeout=timedelta(seconds=2)) == {"value": value}
            async with docket.redis() as client:
                task = await get_task(execution.key)
                assert task.status == TaskStatusEnum.SUCCESS
                assert task.started_at <= task.completed_at
                assert await client.ttl(f"task:{execution.key}") > 0
                assert await client.exists(docket.parked_task_key(execution.key)) == 0
                assert await client.exists(f"{docket.prefix}:progress:{execution.key}") == 0
        async with docket.redis() as client:
            assert await client.zcard(docket.queue_key) == 0
            assert await client.xlen(docket.stream_key) == 0
            assert (await client.xpending(docket.stream_key, docket.worker_group_name))[
                "pending"
            ] == 0


async def test_retry_and_cancel(dragonfly_factory, monkeypatch):
    url = dragonfly_factory(f"--lua_undeclared_keys_shas={SCHEDULER_SHA}")
    async with (
        Redis.from_url(url) as metadata_client,
        Docket(name="memory-server", url=url) as docket,
    ):
        monkeypatch.setattr(redis_utils, "_redis_pool", metadata_client)
        execution = await docket.add(retry_task, key="retry")()
        await docket.add(
            recorded_task, key="cancelled", when=datetime.now(UTC) + timedelta(seconds=30)
        )(9)
        await docket.cancel("cancelled")
        await run_worker(docket)
        loaded = await docket.get_execution(execution.key)
        assert loaded.state == ExecutionState.COMPLETED
        assert await loaded.get_result(timeout=timedelta(seconds=2)) == {"value": 42}
        cancelled = await docket.get_execution("cancelled")
        assert cancelled.state == ExecutionState.CANCELLED
        async with docket.redis() as client:
            assert await client.hget("probe:effects", "cancelled") is None
            assert await client.hget("probe:effects", "retry") == b"42"
            assert await client.zcard(docket.queue_key) == 0


async def blocked_task(value: int):
    await asyncio.sleep(60)
    return value


async def recovered_task(value: int, execution=CurrentExecution(), progress=Progress()):
    return await recorded_task(value, execution=execution, progress=progress)


async def test_crashed_worker_reclaims_pending_message(dragonfly_factory, monkeypatch, tmp_path):
    url = dragonfly_factory(f"--lua_undeclared_keys_shas={SCHEDULER_SHA}")
    async with (
        Redis.from_url(url) as metadata_client,
        Docket(name="memory-server", url=url) as docket,
    ):
        monkeypatch.setattr(redis_utils, "_redis_pool", metadata_client)
        execution = await docket.add(blocked_task, key="crashed")(17)
        child_code = """
import asyncio, sys
from docket import Docket, Worker
from test_docket import blocked_task

async def main():
    async with Docket(name='memory-server', url=sys.argv[1]) as docket:
        docket.register(blocked_task)
        async with Worker(docket, name='crashed-worker', schedule_automatic_tasks=False) as worker:
            await worker.run_forever()

asyncio.run(main())
"""
        log_path = tmp_path / "crashed-worker.log"
        with log_path.open("w") as log:
            child = subprocess.Popen(
                [sys.executable, "-c", child_code, url],
                cwd=Path(__file__).parent,
                stdout=log,
                stderr=log,
            )
            try:
                deadline = time.monotonic() + 10
                while True:
                    assert child.poll() is None, log_path.read_text()
                    loaded = await docket.get_execution(execution.key)
                    if loaded.state == ExecutionState.RUNNING:
                        break
                    assert time.monotonic() < deadline, log_path.read_text()
                    await asyncio.sleep(0.05)
                assert (
                    await metadata_client.xpending(docket.stream_key, docket.worker_group_name)
                )["pending"] == 1
                child.kill()
                child.wait(timeout=5)
                docket.register(recovered_task, names=["blocked_task"])
                async with Worker(
                    docket,
                    name="recovery-worker",
                    schedule_automatic_tasks=False,
                    redelivery_timeout=timedelta(milliseconds=50),
                    minimum_check_interval=timedelta(milliseconds=20),
                ) as worker:
                    await asyncio.wait_for(worker.run_until_finished(), timeout=10)
                completed = await docket.get_execution(execution.key)
                assert completed.state == ExecutionState.COMPLETED
                assert await completed.get_result(timeout=timedelta(seconds=2)) == {"value": 17}
                assert (
                    await metadata_client.xpending(docket.stream_key, docket.worker_group_name)
                )["pending"] == 0
                assert await metadata_client.hget("probe:effects", "crashed") == b"17"
            finally:
                if child.poll() is None:
                    child.kill()
                    child.wait(timeout=5)
