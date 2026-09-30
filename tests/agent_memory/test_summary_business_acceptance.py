"""Deterministic summaries, retry outcomes, access effects and business forgetting."""

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import httpx
import pytest
from fastapi import FastAPI
from redis.asyncio import Redis
from redisvl.index import AsyncSearchIndex

from agent_memory_server import long_term_memory, summary_views
from agent_memory_server.api import router
from agent_memory_server.bounded_docket import BoundedDocket
from agent_memory_server.config import settings
from agent_memory_server.docket_worker import BoundedDocketWorker
from agent_memory_server.llm import LLMClient
from agent_memory_server.memory_vector_db import RedisVLMemoryVectorDatabase
from agent_memory_server.memory_vector_db_factory import _build_redis_schema
from agent_memory_server.models import (
    MemoryRecord,
    SummaryView,
    Task,
    TaskStatusEnum,
    TaskTypeEnum,
)
from agent_memory_server.tasks import create_task, get_task
from agent_memory_server.utils import redis as redis_utils
from agent_memory_server.utils.keys import Keys

from conftest import SCHEDULER_SHA
from test_search import FixedEmbeddings
import test_tenant_scope


signed_identity = test_tenant_scope.signed_identity


async def run_worker(docket):
    async with BoundedDocketWorker(
        docket,
        name="summary-business-worker",
        schedule_automatic_tasks=False,
        scheduling_resolution=timedelta(milliseconds=20),
        minimum_check_interval=timedelta(milliseconds=20),
    ) as worker:
        await asyncio.wait_for(worker.run_until_finished(), 15)


@pytest.fixture
async def summary_business(dragonfly_factory, monkeypatch):
    url = dragonfly_factory(f"--lua_undeclared_keys_shas={SCHEDULER_SHA}")
    monkeypatch.setattr(settings, "redis_url", url)
    monkeypatch.setattr(settings, "docket_name", "summary-business")
    monkeypatch.setattr(settings, "use_docket", True)
    monkeypatch.setattr(settings, "long_term_memory", True)
    monkeypatch.setattr(settings, "disable_auth", False)
    monkeypatch.setattr(settings, "auth_mode", "oauth2")
    monkeypatch.setattr(settings, "redisvl_vector_dimensions", 3)
    monkeypatch.setattr(settings, "embedding_model", "test-fixed-3d")
    monkeypatch.setattr(settings, "openai_api_key", "test-only-placeholder")
    monkeypatch.setattr(long_term_memory, "get_background_tasks", lambda: MagicMock())
    async with (
        Redis.from_url(url, decode_responses=True) as redis,
        BoundedDocket(name="summary-business", url=url) as docket,
    ):
        monkeypatch.setattr(redis_utils, "_redis_pool", redis)
        db = RedisVLMemoryVectorDatabase(
            AsyncSearchIndex.from_dict(_build_redis_schema(), redis_client=redis),
            FixedEmbeddings(),
        )

        async def get_db():
            return db

        monkeypatch.setattr(long_term_memory, "get_memory_vector_db", get_db)
        now = datetime.now(UTC)
        records = [
            MemoryRecord(
                id="old",
                text="old orchard",
                namespace="orchard",
                user_id="alice",
                created_at=now - timedelta(days=60),
                last_accessed=now - timedelta(days=60),
            ),
            MemoryRecord(
                id="recent",
                text="recent orchard",
                namespace="orchard",
                user_id="alice",
                created_at=now - timedelta(days=1),
                last_accessed=now - timedelta(days=1),
                access_count=2,
            ),
            MemoryRecord(
                id="pinned",
                text="old orchard",
                namespace="orchard",
                user_id="alice",
                created_at=now - timedelta(days=60),
                last_accessed=now - timedelta(days=60),
                pinned=True,
            ),
            MemoryRecord(
                id="old",
                text="other ocean",
                namespace="orchard",
                user_id="bob",
                created_at=now - timedelta(days=60),
                last_accessed=now - timedelta(days=60),
            ),
        ]
        for record in records:
            record.session_id = record.id
        await db.add_memories(records)
        docket.register(summary_views.refresh_summary_view)
        docket.register(long_term_memory.update_last_accessed)
        yield redis, docket, db


@pytest.mark.parametrize("outcome", ["transient", "empty_once", "permanent"])
async def test_summary_model_failure_uses_real_docket_retry_and_truthful_terminal_state(
    summary_business, monkeypatch, outcome
):
    redis, docket, _ = summary_business
    view = SummaryView(
        id="retry",
        source="long_term",
        owner_namespace="orchard",
        owner_user_id="alice",
        group_by=["user_id"],
    )
    await summary_views.save_summary_view(view)
    task = Task(
        id="summary-run",
        type=TaskTypeEnum.SUMMARY_VIEW_FULL_RUN,
        view_id=view.id,
        view_generation=view.generation,
        owner_namespace="orchard",
        owner_user_id="alice",
    )
    await create_task(task)
    calls = []

    async def model(model_name, messages):
        calls.append(messages)
        assert "other ocean" not in messages[0]["content"]
        assert "recent orchard" in messages[0]["content"]
        current = await get_task(task.id)
        assert current.status == TaskStatusEnum.RUNNING and await redis.ttl(f"task:{task.id}") == -1
        if len(calls) > 1:
            assert current.error_message
        if outcome == "permanent" or (outcome == "transient" and len(calls) == 1):
            raise ConnectionError("model temporarily unavailable")
        return SimpleNamespace(
            content="" if outcome == "empty_once" and len(calls) == 1 else "Alice orchard summary"
        )

    monkeypatch.setattr(LLMClient, "create_chat_completion", model)
    execution = await docket.add(summary_views.refresh_summary_view, key="retry-business")(
        view_id=view.id,
        task_id=task.id,
        expected_generation=view.generation,
        task_generation=task.generation,
    )
    await run_worker(docket)
    await execution.sync()
    actual = await get_task(task.id)
    results = await summary_views.list_partition_results(view.id)
    if outcome == "permanent":
        assert len(calls) == 3 and execution.state.value == "failed"
        assert actual.status == TaskStatusEnum.FAILED and "unavailable" in actual.error_message
        assert results == []
    else:
        assert len(calls) == 2 and execution.state.value == "completed"
        assert actual.status == TaskStatusEnum.SUCCESS and actual.error_message is None
        assert (
            len(results) == 1
            and results[0].summary == "Alice orchard summary"
            and results[0].memory_count == 3
        )
    assert actual.completed_at is not None and await redis.ttl(f"task:{task.id}") > 0
    assert (await redis.xpending(docket.stream_key, docket.worker_group_name))["pending"] == 0


async def test_interrupted_summary_metadata_remains_retryable_before_real_worker_replay(
    summary_business, monkeypatch
):
    _, docket, _ = summary_business
    view = SummaryView(
        id="interrupted",
        source="long_term",
        owner_namespace="orchard",
        owner_user_id="alice",
    )
    await summary_views.save_summary_view(view)
    task = Task(
        id="interrupted-run",
        type=TaskTypeEnum.SUMMARY_VIEW_FULL_RUN,
        view_id=view.id,
        view_generation=view.generation,
        owner_namespace="orchard",
        owner_user_id="alice",
    )
    await create_task(task)
    entered = asyncio.Event()
    calls = 0

    async def model(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            entered.set()
            await asyncio.Event().wait()
        return SimpleNamespace(content="Recovered summary")

    monkeypatch.setattr(LLMClient, "create_chat_completion", model)
    kwargs = dict(
        view_id=view.id,
        task_id=task.id,
        expected_generation=view.generation,
        task_generation=task.generation,
    )
    processing = asyncio.create_task(summary_views.refresh_summary_view(**kwargs))
    await asyncio.wait_for(entered.wait(), 5)
    processing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await processing
    current = await get_task(task.id)
    assert current.status == TaskStatusEnum.RUNNING and current.completed_at is None
    assert (
        "interrupted" in current.error_message
        and await summary_views.list_partition_results(view.id) == []
    )
    await docket.add(summary_views.refresh_summary_view, key="reclaimed-summary")(**kwargs)
    await run_worker(docket)
    assert calls == 2 and (await get_task(task.id)).status == TaskStatusEnum.SUCCESS
    assert (await get_task(task.id)).error_message is None
    assert (await summary_views.list_partition_results(view.id))[0].summary == "Recovered summary"


@pytest.mark.parametrize("policy", [{"max_age_days": 30}, {"max_inactive_days": 30}, {"budget": 1}])
async def test_authenticated_summary_access_forgetting_update_and_config_cleanup(
    summary_business, monkeypatch, signed_identity, policy
):
    redis, docket, db = summary_business
    bob_key = Keys.memory_key("old", "orchard", "bob")
    bob_before = await redis.execute_command("HGETALL", bob_key, NEVER_DECODE=True)
    model_calls = []

    async def model(model_name, messages):
        model_calls.append(messages)
        assert "other ocean" not in messages[0]["content"]
        return SimpleNamespace(content=f"Alice summary revision {len(model_calls)}")

    monkeypatch.setattr(LLMClient, "create_chat_completion", model)
    app = FastAPI()
    app.include_router(router)
    headers = signed_identity("orchard", "alice")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as http:
        created = await http.post(
            "/v1/summary-views",
            headers=headers,
            json={"source": "long_term", "group_by": ["user_id"]},
        )
        assert created.status_code == 200, created.text
        view_id = created.json()["id"]
        started = await http.post(
            f"/v1/summary-views/{view_id}/run",
            headers=headers,
            json={"task_id": "first"},
        )
        assert started.status_code == 200, started.text
        await run_worker(docket)
        partitions = await http.get(f"/v1/summary-views/{view_id}/partitions", headers=headers)
        assert (
            partitions.json()[0]["summary"] == "Alice summary revision 1"
            and partitions.json()[0]["memory_count"] == 3
        )
        old_result = (await summary_views.list_partition_results(view_id))[0]

        search = await http.post(
            "/v1/long-term-memory/search",
            headers=headers,
            json={"text": "recent", "search_mode": "keyword", "recency_boost": True},
        )
        assert search.status_code == 200 and [
            memory["id"] for memory in search.json()["memories"]
        ] == ["recent"]
        await run_worker(docket)
        recent = await long_term_memory.get_long_term_memory_by_id("recent", "orchard", "alice")
        assert recent.access_count == 3 and recent.last_accessed > recent.created_at
        assert await redis.execute_command("HGETALL", bob_key, NEVER_DECODE=True) == bob_before

        preview = await long_term_memory.forget_long_term_memories(
            policy, namespace="orchard", user_id="alice", dry_run=True
        )
        assert preview["deleted_ids"] == ["old"] and preview["scanned"] == 3
        assert await redis.exists(Keys.memory_key("old", "orchard", "alice"))
        removed = await long_term_memory.forget_long_term_memories(
            policy, namespace="orchard", user_id="alice", dry_run=False
        )
        assert removed["deleted"] == 1 and not await redis.exists(
            Keys.memory_key("old", "orchard", "alice")
        )
        assert await redis.exists(Keys.memory_key("pinned", "orchard", "alice"))
        assert await redis.execute_command("HGETALL", bob_key, NEVER_DECODE=True) == bob_before

        view = await summary_views.get_summary_view(view_id)
        view.prompt = "Updated summary instructions"
        await summary_views.save_summary_view(view)
        assert await summary_views.list_partition_results(view_id) == []
        with pytest.raises(ValueError, match="changed"):
            await summary_views.save_partition_result(old_result)
        updated = await http.post(
            f"/v1/summary-views/{view_id}/run",
            headers=headers,
            json={"task_id": "second"},
        )
        assert updated.status_code == 200, updated.text
        await run_worker(docket)
        results = await summary_views.list_partition_results(view_id)
        assert (
            len(results) == 1
            and results[0].summary == "Alice summary revision 2"
            and results[0].memory_count == 2
        )
        assert "Updated summary instructions" in model_calls[-1][0]["content"]
        deleted = await http.delete(f"/v1/summary-views/{view_id}", headers=headers)
        assert deleted.status_code == 200, deleted.text
        assert (
            await summary_views.get_summary_view(view_id) is None
            and await summary_views.list_partition_results(view_id) == []
        )
        with pytest.raises(ValueError, match="deleted"):
            await summary_views.save_partition_result(results[0])
        assert not [
            key async for key in redis.scan_iter(match=summary_views._summary_key(view_id, "*"))
        ]
        assert await redis.execute_command("HGETALL", bob_key, NEVER_DECODE=True) == bob_before
        assert (await redis.xpending(docket.stream_key, docket.worker_group_name))["pending"] == 0


async def test_active_summary_cancellation_fences_results_and_clears_real_pel(
    summary_business, monkeypatch
):
    from agent_memory_server.summary_task_dispatch import cancel_task

    redis, docket, _ = summary_business
    view = SummaryView(
        id="cancel-active",
        source="long_term",
        owner_namespace="orchard",
        owner_user_id="alice",
    )
    await summary_views.save_summary_view(view)
    task = Task(
        id="cancel-active-task",
        type=TaskTypeEnum.SUMMARY_VIEW_FULL_RUN,
        view_id=view.id,
        view_generation=view.generation,
        owner_namespace="orchard",
        owner_user_id="alice",
        docket_key="cancel-active-execution",
    )
    await create_task(task)
    entered = asyncio.Event()
    stopped = asyncio.Event()

    async def model(*args, **kwargs):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    monkeypatch.setattr(LLMClient, "create_chat_completion", model)
    execution = await docket.add(summary_views.refresh_summary_view, key=task.docket_key)(
        view_id=view.id,
        task_id=task.id,
        expected_generation=view.generation,
        task_generation=task.generation,
    )
    worker = asyncio.create_task(run_worker(docket))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        assert (await redis.xpending(docket.stream_key, docket.worker_group_name))["pending"] == 1
        assert await cancel_task(task.id, expected_generation=task.generation)
        await asyncio.wait_for(stopped.wait(), 5)
        await worker
    finally:
        if not worker.done():
            worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)
    await execution.sync()
    assert execution.state.value == "cancelled"
    current = await get_task(task.id)
    assert current.status == TaskStatusEnum.CANCELLED and current.completed_at is not None
    assert await summary_views.list_partition_results(view.id) == []
    assert (await redis.xpending(docket.stream_key, docket.worker_group_name))["pending"] == 0
    assert not [key async for key in redis.scan_iter(match="task_cancel_outbox:*")]
    await summary_views.refresh_summary_view(
        view.id,
        task_id=task.id,
        expected_generation=view.generation,
        task_generation=task.generation,
    )
    assert await get_task(task.id) == current


async def test_summary_stale_model_owner_cannot_publish_or_finalize_task(
    summary_business, monkeypatch
):
    from agent_memory_server.utils.model_execution_guard import (
        ModelExecutionLeaseLostError,
        current_model_execution_lease,
    )

    redis, _, _ = summary_business
    view = SummaryView(
        id="stale-owner",
        source="long_term",
        owner_namespace="orchard",
        owner_user_id="alice",
    )
    await summary_views.save_summary_view(view)
    task = Task(
        id="stale-owner-task",
        type=TaskTypeEnum.SUMMARY_VIEW_FULL_RUN,
        view_id=view.id,
        view_generation=view.generation,
        owner_namespace="orchard",
        owner_user_id="alice",
    )
    await create_task(task)
    lease_keys = []

    async def model(*args, **kwargs):
        lease = current_model_execution_lease()
        assert lease is not None
        lease_keys.append(lease.key)
        # Reclamation can happen after a process pause even before renewal.
        await redis.set(lease.key, "replacement-owner", px=30_000)
        return SimpleNamespace(content="stale output must not become visible")

    monkeypatch.setattr(LLMClient, "create_chat_completion", model)
    with pytest.raises(ModelExecutionLeaseLostError):
        await summary_views.refresh_summary_view(
            view.id,
            task_id=task.id,
            expected_generation=view.generation,
            task_generation=task.generation,
        )
    assert await summary_views.list_partition_results(view.id) == []
    current = await get_task(task.id)
    assert current.status == TaskStatusEnum.RUNNING and current.completed_at is None
    assert await redis.get(lease_keys[0]) == "replacement-owner"


async def test_full_refresh_prunes_disappeared_groups_and_empty_source(
    summary_business, monkeypatch
):
    redis, docket, _ = summary_business
    view = SummaryView(
        id="group-pruning",
        source="long_term",
        owner_namespace="orchard",
        owner_user_id="alice",
        group_by=["session_id"],
    )
    await summary_views.save_summary_view(view)
    model_calls = []

    async def model(model_name, messages):
        model_calls.append(messages)
        return SimpleNamespace(content="Current source facts")

    monkeypatch.setattr(LLMClient, "create_chat_completion", model)

    async def refresh(task_id):
        task = Task(
            id=task_id,
            type=TaskTypeEnum.SUMMARY_VIEW_FULL_RUN,
            view_id=view.id,
            view_generation=view.generation,
            owner_namespace="orchard",
            owner_user_id="alice",
        )
        await create_task(task)
        await docket.add(summary_views.refresh_summary_view, key=task_id)(
            view_id=view.id,
            task_id=task.id,
            expected_generation=view.generation,
            task_generation=task.generation,
        )
        await run_worker(docket)
        assert (await get_task(task.id)).status == TaskStatusEnum.SUCCESS
        return await summary_views.list_partition_results(view.id)

    assert {result.group["session_id"] for result in await refresh("prune-first")} == {
        "old",
        "recent",
        "pinned",
    }
    bob_before = await redis.execute_command(
        "HGETALL", Keys.memory_key("old", "orchard", "bob"), NEVER_DECODE=True
    )
    await long_term_memory.delete_long_term_memories(
        ["old", "recent"], namespace="orchard", user_id="alice"
    )
    assert {result.group["session_id"] for result in await refresh("prune-second")} == {"pinned"}
    assert (
        await redis.execute_command(
            "HGETALL", Keys.memory_key("old", "orchard", "bob"), NEVER_DECODE=True
        )
        == bob_before
    )
    await long_term_memory.delete_long_term_memories(
        ["pinned"], namespace="orchard", user_id="alice"
    )
    assert await refresh("prune-empty") == []
    assert len(model_calls) == 4
    assert not [
        key
        async for key in redis.scan_iter(
            match=summary_views._summary_key(view.id, "*", view.generation)
        )
    ]
    assert (await redis.xpending(docket.stream_key, docket.worker_group_name))["pending"] == 0


@pytest.mark.parametrize("change", ["delete", "recreate"])
async def test_summary_original_source_snapshot_rejects_delete_during_model(
    summary_business, monkeypatch, change
):
    redis, _, db = summary_business
    monkeypatch.setitem(db.embeddings.vectors, "new incarnation", [1.0, 0.0, 0.0])
    view = SummaryView(
        id="deleted-source",
        source="long_term",
        owner_namespace="orchard",
        owner_user_id="alice",
        group_by=["user_id"],
    )
    await summary_views.save_summary_view(view)
    task = Task(
        id="deleted-source-task",
        type=TaskTypeEnum.SUMMARY_VIEW_FULL_RUN,
        view_id=view.id,
        view_generation=view.generation,
        owner_namespace="orchard",
        owner_user_id="alice",
    )
    await create_task(task)
    bob_key = Keys.memory_key("old", "orchard", "bob")
    bob_before = await redis.execute_command("HGETALL", bob_key, NEVER_DECODE=True)

    async def model(*args, **kwargs):
        await long_term_memory.delete_long_term_memories(
            ["old"], namespace="orchard", user_id="alice"
        )
        if change == "recreate":
            await db.add_memories(
                [
                    MemoryRecord(
                        id="old",
                        text="new incarnation",
                        namespace="orchard",
                        user_id="alice",
                    )
                ]
            )
        return SimpleNamespace(content="Old orchard facts must never publish")

    monkeypatch.setattr(LLMClient, "create_chat_completion", model)
    await summary_views.refresh_summary_view(
        view.id,
        task_id=task.id,
        expected_generation=view.generation,
        task_generation=task.generation,
    )
    assert await summary_views.list_partition_results(view.id) == []
    current = await get_task(task.id)
    assert current.status == TaskStatusEnum.FAILED and "publication" in current.error_message
    assert await redis.execute_command("HGETALL", bob_key, NEVER_DECODE=True) == bob_before
    restored = await long_term_memory.get_long_term_memory_by_id("old", "orchard", "alice")
    if change == "recreate":
        assert restored.text == "new incarnation"
    else:
        assert restored is None
