"""Verify summary writes and deletion without an external model service."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from agent_memory_server.config import settings
from agent_memory_server.llm import LLMClient
from agent_memory_server.models import MemoryRecord, SummaryView, SummaryViewPartitionResult
from agent_memory_server.summary_views import (
    delete_summary_view,
    get_summary_view,
    list_partition_results,
    list_summary_views,
    save_partition_result,
    save_summary_view,
    summarize_partition_long_term,
)
from agent_memory_server.utils import redis as redis_utils


async def test_model_stub_summary_and_delete(redis_client, monkeypatch):
    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    monkeypatch.setattr(settings, "openai_api_key", "test-only-placeholder")
    model = AsyncMock(return_value=SimpleNamespace(content="Alice prefers orchards."))
    monkeypatch.setattr(LLMClient, "create_chat_completion", model)
    view = SummaryView(id="view1", source="long_term", group_by=["user_id"])
    await save_summary_view(view)
    result = await summarize_partition_long_term(
        view,
        {"user_id": "alice"},
        [MemoryRecord(id="m1", text="Alice prefers orchards.", user_id="alice")],
    )
    assert result.summary == "Alice prefers orchards." and result.memory_count == 1
    model.assert_awaited_once()
    await save_partition_result(result)
    assert (await list_partition_results(view.id))[0].summary == result.summary
    await delete_summary_view(view.id)
    assert await get_summary_view(view.id) is None
    assert await list_summary_views() == []
    assert await list_partition_results(view.id) == []
    with pytest.raises(ValueError, match="deleted"):
        await save_partition_result(result)
    assert await list_partition_results(view.id) == []


async def test_delete_id_cannot_expand_glob(redis_client, monkeypatch):
    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    view = SummaryView(id="preserved", source="long_term")
    await save_summary_view(view)
    result = SummaryViewPartitionResult(
        view_id=view.id, generation=view.generation, group={}, summary="keep", memory_count=1
    )
    await save_partition_result(result)
    await delete_summary_view("*")
    assert (await list_partition_results(view.id))[0].summary == "keep"
    assert await get_summary_view(view.id) is not None


async def test_late_result_rejected_after_config_replacement(redis_client, monkeypatch):
    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    old_view = SummaryView(id="reused", source="long_term")
    await save_summary_view(old_view)
    old_result = await summarize_partition_long_term(old_view, {}, [])
    await save_partition_result(old_result)
    new_view = SummaryView(id=old_view.id, source="long_term", prompt="new instructions")
    await save_summary_view(new_view)
    assert new_view.generation != old_view.generation
    assert await list_partition_results(new_view.id) == []
    with pytest.raises(ValueError, match="changed"):
        await save_partition_result(old_result)
    new_result = await summarize_partition_long_term(new_view, {}, [])
    await save_partition_result(new_result)
    assert await list_partition_results(new_view.id) == [new_result]


@pytest.mark.parametrize("legacy", [False, True])
async def test_recreate_during_delete_preserves_new_results(redis_client, monkeypatch, legacy):
    from agent_memory_server import summary_views

    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    old_view = SummaryView(id="recreated", source="long_term")
    if legacy:
        await redis_client.set(summary_views._config_key(old_view.id), old_view.model_dump_json())
        await redis_client.sadd("summary_view:index", old_view.id)
    else:
        await save_summary_view(old_view)
    old_result = await summarize_partition_long_term(old_view, {}, [])
    await save_partition_result(old_result)
    cleanup = summary_views._delete_generation_results
    new_view = SummaryView(id=old_view.id, source="long_term")
    new_result = None

    async def recreate_before_cleanup(redis, retired_view):
        nonlocal new_result
        assert await get_summary_view(old_view.id) is None
        await save_summary_view(new_view)
        new_result = await summarize_partition_long_term(new_view, {}, [])
        await save_partition_result(new_result)
        await cleanup(redis, retired_view)

    monkeypatch.setattr(summary_views, "_delete_generation_results", recreate_before_cleanup)
    await delete_summary_view(old_view.id)
    assert (await get_summary_view(old_view.id)).generation == new_view.generation
    assert await list_partition_results(old_view.id) == [new_result]
    with pytest.raises(ValueError, match="changed"):
        await save_partition_result(old_result)
    keys = [key async for key in redis_client.scan_iter(match="summary_view:*:summary:*")]
    assert len(keys) == 1


async def test_result_without_revision_cannot_write_new_config(redis_client, monkeypatch):
    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    view = SummaryView(id="revision-required", source="long_term")
    await save_summary_view(view)
    result = SummaryViewPartitionResult(view_id=view.id, group={}, memory_count=0)
    with pytest.raises(ValueError, match="changed"):
        await save_partition_result(result)
    assert await list_partition_results(view.id) == []


async def test_refresh_in_flight_cannot_publish_into_recreated_view(redis_client, monkeypatch):
    from agent_memory_server import summary_views
    from agent_memory_server.models import TaskStatusEnum

    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    monkeypatch.setattr(settings, "openai_api_key", "test-only-placeholder")
    entered_model = asyncio.Event()
    release_model = asyncio.Event()

    async def delayed_model(*args, **kwargs):
        entered_model.set()
        await release_model.wait()
        return SimpleNamespace(content="obsolete summary")

    monkeypatch.setattr(LLMClient, "create_chat_completion", delayed_model)
    monkeypatch.setattr(
        summary_views,
        "_fetch_long_term_memories_for_view",
        AsyncMock(return_value=[MemoryRecord(id="m1", text="old fact")]),
    )
    status = AsyncMock()
    monkeypatch.setattr(summary_views, "update_task_status", status)
    view = SummaryView(id="in-flight", source="long_term")
    await save_summary_view(view)
    refresh = asyncio.create_task(summary_views.refresh_summary_view(view.id, task_id="old-task"))
    try:
        await asyncio.wait_for(entered_model.wait(), timeout=5)
        await delete_summary_view(view.id)
        replacement = SummaryView(id=view.id, source="long_term")
        await save_summary_view(replacement)
        current_result = await summarize_partition_long_term(replacement, {}, [])
        await save_partition_result(current_result)
        release_model.set()
        await asyncio.wait_for(refresh, timeout=5)
        assert await list_partition_results(view.id) == [current_result]
        assert status.await_args.kwargs["status"] == TaskStatusEnum.FAILED
        assert "changed" in status.await_args.kwargs["error_message"]
    finally:
        release_model.set()
        if not refresh.done():
            refresh.cancel()
        await asyncio.gather(refresh, return_exceptions=True)


async def test_partition_api_returns_conflict_when_config_changes(redis_client, monkeypatch):
    from fastapi import HTTPException

    from agent_memory_server import api
    from agent_memory_server.models import RunSummaryViewPartitionRequest

    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    view = SummaryView(id="api-conflict", source="long_term")
    await save_summary_view(view)

    async def replace_during_computation(snapshot, group):
        await save_summary_view(SummaryView(id=snapshot.id, source="long_term"))
        return await summarize_partition_long_term(snapshot, group, [])

    monkeypatch.setattr(api, "summarize_partition_for_view", replace_during_computation)
    with pytest.raises(HTTPException) as conflict:
        await api.run_summary_view_partition(
            view.id, RunSummaryViewPartitionRequest(group={}), current_user=None
        )
    assert conflict.value.status_code == 409
    assert await list_partition_results(view.id) == []
