"""Task state races, expiry and ID reuse against real Redis-compatible storage."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from agent_memory_server import summary_views
from agent_memory_server.config import settings
from agent_memory_server.llm import LLMClient
from agent_memory_server.models import (
    MemoryRecord,
    SummaryView,
    Task,
    TaskStatusEnum,
    TaskTypeEnum,
)
from agent_memory_server.tasks import (
    InvalidTaskTransitionError,
    create_task,
    get_task,
    update_task_status,
)
from agent_memory_server.utils import redis as redis_utils


def task_record(task_id, **kwargs):
    return Task(id=task_id, type=TaskTypeEnum.SUMMARY_VIEW_FULL_RUN, **kwargs)


async def test_concurrent_terminal_transitions_revalidate(redis_client, monkeypatch):
    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    task = task_record("racing", status=TaskStatusEnum.RUNNING)
    await create_task(task)
    original_get = redis_client.get
    both_read = asyncio.Event()
    readers = 0

    async def synchronized_get(key, *args, **kwargs):
        nonlocal readers
        raw = await original_get(key, *args, **kwargs)
        if key == "task:racing" and readers < 2:
            readers += 1
            if readers == 2:
                both_read.set()
            await asyncio.wait_for(both_read.wait(), timeout=5)
        return raw

    monkeypatch.setattr(redis_client, "get", synchronized_get)
    outcomes = await asyncio.gather(
        update_task_status(
            task.id, status=TaskStatusEnum.SUCCESS, expected_generation=task.generation
        ),
        update_task_status(
            task.id, status=TaskStatusEnum.FAILED, expected_generation=task.generation
        ),
        return_exceptions=True,
    )
    assert outcomes.count(True) == 1
    assert sum(isinstance(outcome, InvalidTaskTransitionError) for outcome in outcomes) == 1
    stored = await get_task(task.id)
    winner = [TaskStatusEnum.SUCCESS, TaskStatusEnum.FAILED][outcomes.index(True)]
    assert stored.status == winner and stored.generation == task.generation


@pytest.mark.parametrize("replacement_mode", ["delete", "expire", "recreate"])
async def test_status_update_cannot_recreate_or_replace_task(
    redis_client, monkeypatch, replacement_mode
):
    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    old = task_record("reused", status=TaskStatusEnum.RUNNING)
    await create_task(old)
    original_get = redis_client.get
    read = asyncio.Event()
    release = asyncio.Event()
    paused = False

    async def paused_get(key, *args, **kwargs):
        nonlocal paused
        raw = await original_get(key, *args, **kwargs)
        if key == "task:reused" and not paused:
            paused = True
            read.set()
            await release.wait()
        return raw

    monkeypatch.setattr(redis_client, "get", paused_get)
    writer = asyncio.create_task(
        update_task_status(
            old.id, status=TaskStatusEnum.SUCCESS, expected_generation=old.generation
        )
    )
    try:
        await asyncio.wait_for(read.wait(), timeout=5)
        if replacement_mode == "expire":
            await redis_client.pexpire("task:reused", 1)
            deadline = asyncio.get_running_loop().time() + 5
            while await redis_client.exists("task:reused"):
                assert asyncio.get_running_loop().time() < deadline
                await asyncio.sleep(0.01)
        else:
            await redis_client.delete("task:reused")
        replacement = task_record(old.id)
        if replacement_mode == "recreate":
            await create_task(replacement)
            assert replacement.generation != old.generation
        release.set()
        assert await asyncio.wait_for(writer, timeout=5) is False
        if replacement_mode == "recreate":
            assert await get_task(old.id) == replacement
            assert (
                await update_task_status(
                    old.id,
                    status=TaskStatusEnum.FAILED,
                    expected_generation=old.generation,
                )
                is False
            )
            assert await get_task(old.id) == replacement
        else:
            assert not await redis_client.exists("task:reused")
    finally:
        release.set()
        if not writer.done():
            writer.cancel()
        await asyncio.gather(writer, return_exceptions=True)


@pytest.mark.parametrize("replace_during_model", [False, True])
async def test_summary_task_incarnation_guards_execution_and_publication(
    redis_client, monkeypatch, replace_during_model
):
    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    monkeypatch.setattr(settings, "openai_api_key", "test-only-placeholder")
    view = SummaryView(id="owned-view", source="long_term")
    await summary_views.save_summary_view(view)
    old = task_record("old-worker", view_id=view.id, view_generation=view.generation)
    await create_task(old)
    fetch = AsyncMock(return_value=[MemoryRecord(id="m1", text="old fact")])
    monkeypatch.setattr(summary_views, "_fetch_long_term_memories_for_view", fetch)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def model(*args, **kwargs):
        entered.set()
        await release.wait()
        return SimpleNamespace(content="old result")

    model_call = AsyncMock(side_effect=model)
    monkeypatch.setattr(LLMClient, "create_chat_completion", model_call)

    async def replace():
        await redis_client.delete(f"task:{old.id}")
        replacement = task_record(old.id, view_id=view.id, view_generation=view.generation)
        await create_task(replacement)
        return replacement

    if not replace_during_model:
        replacement = await replace()
    worker = asyncio.create_task(
        summary_views.refresh_summary_view(
            view.id,
            task_id=old.id,
            expected_generation=view.generation,
            task_generation=old.generation,
        )
    )
    try:
        if replace_during_model:
            await asyncio.wait_for(entered.wait(), timeout=5)
            assert (await get_task(old.id)).status == TaskStatusEnum.RUNNING
            replacement = await replace()
            release.set()
        await asyncio.wait_for(worker, timeout=5)
        assert await get_task(old.id) == replacement
        assert await summary_views.list_partition_results(view.id) == []
        if not replace_during_model:
            fetch.assert_not_awaited()
            model_call.assert_not_awaited()
        else:
            model_call.assert_awaited_once()
    finally:
        release.set()
        if not worker.done():
            worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)


async def test_duplicate_summary_delivery_converges_to_one_result(redis_client, monkeypatch):
    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    monkeypatch.setattr(settings, "openai_api_key", "test-only-placeholder")
    view = SummaryView(id="duplicate-view", source="long_term")
    await summary_views.save_summary_view(view)
    task = task_record("duplicate-task", view_id=view.id, view_generation=view.generation)
    await create_task(task)
    fetch = AsyncMock(return_value=[MemoryRecord(id="m1", text="one fact")])
    monkeypatch.setattr(summary_views, "_fetch_long_term_memories_for_view", fetch)
    calls = 0

    async def model(*args, **kwargs):
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.1)
        return SimpleNamespace(content="summary 1")

    monkeypatch.setattr(LLMClient, "create_chat_completion", model)
    arguments = {
        "task_id": task.id,
        "expected_generation": view.generation,
        "task_generation": task.generation,
    }
    await asyncio.wait_for(
        asyncio.gather(
            summary_views.refresh_summary_view(view.id, **arguments),
            summary_views.refresh_summary_view(view.id, **arguments),
        ),
        timeout=10,
    )
    stored = await get_task(task.id)
    assert stored.status == TaskStatusEnum.SUCCESS and stored.generation == task.generation
    results = await summary_views.list_partition_results(view.id)
    assert len(results) == 1 and results[0].memory_count == 1
    assert results[0].summary == "summary 1"
    await summary_views.refresh_summary_view(view.id, **arguments)
    assert calls == 1
    assert await get_task(task.id) == stored


async def test_active_task_metadata_survives_queue_delay_and_terminal_retention(
    redis_client, monkeypatch
):
    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    task = task_record("long-queued")
    await create_task(task)
    assert await redis_client.ttl("task:long-queued") == -1
    # Repair legacy expiring active metadata during its next guarded update.
    await redis_client.pexpire("task:long-queued", 60_000)
    assert await update_task_status(
        task.id, status=TaskStatusEnum.RUNNING, expected_generation=task.generation
    )
    assert await redis_client.ttl("task:long-queued") == -1
    assert await update_task_status(
        task.id, status=TaskStatusEnum.SUCCESS, expected_generation=task.generation
    )
    assert 604_790 <= await redis_client.ttl("task:long-queued") <= 604_800
    assert (await get_task(task.id)).generation == task.generation
