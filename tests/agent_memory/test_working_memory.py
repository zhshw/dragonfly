"""Exercise atomic session writes and optimistic updates through real AMS code."""

import asyncio
import json
import subprocess
import sys
from pathlib import Path
from datetime import timedelta

import pytest
from docket import Docket
from docket.execution import ExecutionState
from fastapi import HTTPException
from redis.asyncio import Redis
from redisvl.index import AsyncSearchIndex

from conftest import SCHEDULER_SHA
from test_docket import run_worker
from test_search import FixedEmbeddings

from agent_memory_server.memory_vector_db import RedisVLMemoryVectorDatabase
from agent_memory_server.memory_vector_db_factory import _build_redis_schema

from agent_memory_server.api import put_working_memory_core
from agent_memory_server.config import settings
from agent_memory_server import long_term_memory
from agent_memory_server.models import (
    MemoryMessage,
    MemoryRecord,
    UpdateWorkingMemory,
    WorkingMemory,
)
from agent_memory_server.utils import redis as redis_utils
from agent_memory_server.utils.keys import Keys
from agent_memory_server.working_memory import (
    WorkingMemoryConflictError,
    WorkingMemoryTooLargeError,
    delete_working_memory,
    get_working_memory,
    list_sessions,
    set_working_memory,
)
from agent_memory_server.working_memory_index import ensure_working_memory_index


async def test_session_version_and_atomic_ttl(redis_client):
    await ensure_working_memory_index(redis_client)
    memory = WorkingMemory(
        session_id="s1",
        namespace="tenant-a",
        user_id="alice",
        ttl_seconds=10,
        data={"value": 1},
    )
    await set_working_memory(memory, redis_client)
    assert memory.version == 1
    key = Keys.working_memory_key("s1", "alice", "tenant-a")
    assert 0 < await redis_client.ttl(key) <= 10
    loaded = await get_working_memory("s1", "alice", "tenant-a", redis_client)
    assert loaded.version == 1 and loaded.data == {"value": 1}
    assert await list_sessions(redis_client, namespace="tenant-a", user_id="alice") == (
        1,
        ["s1"],
    )
    loaded.data = {"value": 2}
    loaded.ttl_seconds = None
    await set_working_memory(loaded, redis_client)
    assert loaded.version == 2
    assert await redis_client.ttl(key) == -1
    await delete_working_memory("s1", "alice", "tenant-a", redis_client)
    assert await get_working_memory("s1", "alice", "tenant-a", redis_client) is None
    assert await list_sessions(redis_client, namespace="tenant-a", user_id="alice") == (
        0,
        [],
    )
    with pytest.raises(WorkingMemoryConflictError):
        await set_working_memory(loaded, redis_client)
    assert not await redis_client.exists(key)


async def test_concurrent_updates_conflict_instead_of_losing_data(redis_client):
    memory = WorkingMemory(session_id="s1", data={"initial": True})
    await set_working_memory(memory, redis_client)
    left, right = await asyncio.gather(
        *(get_working_memory("s1", redis_client=redis_client) for _ in range(2))
    )
    left.data = {"left": True}
    right.data = {"right": True}
    outcomes = await asyncio.gather(
        set_working_memory(left, redis_client),
        set_working_memory(right, redis_client),
        return_exceptions=True,
    )
    assert sum(isinstance(outcome, WorkingMemoryConflictError) for outcome in outcomes) == 1
    final = await get_working_memory("s1", redis_client=redis_client)
    assert final.version == 2
    assert final.data in [{"left": True}, {"right": True}]
    # Retry explicitly from the latest revision, preserving the successful update.
    final.data.update(left=True, right=True)
    await set_working_memory(final, redis_client)
    assert (await get_working_memory("s1", redis_client=redis_client)).data == {
        "left": True,
        "right": True,
    }


async def test_size_and_ttl_validation_leave_existing_data_untouched(redis_client, monkeypatch):
    memory = WorkingMemory(session_id="s1", data={"initial": True})
    await set_working_memory(memory, redis_client)
    key = Keys.working_memory_key("s1")
    before = await redis_client.json().get(key)
    monkeypatch.setattr(settings, "working_memory_max_bytes", 16)
    with pytest.raises(WorkingMemoryTooLargeError):
        await set_working_memory(memory, redis_client)
    assert await redis_client.json().get(key) == before
    memory.ttl_seconds = -1
    with pytest.raises(ValueError, match="TTL"):
        await set_working_memory(memory, redis_client)
    assert await redis_client.json().get(key) == before


async def test_api_conflict_and_size_status(redis_client, monkeypatch):
    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    monkeypatch.setattr(settings, "long_term_memory", False)
    first = await put_working_memory_core("s1", UpdateWorkingMemory(data={"initial": True}), None)
    assert first.version == 1
    with pytest.raises(HTTPException) as conflict:
        await put_working_memory_core("s1", UpdateWorkingMemory(data={"stale": True}), None)
    assert conflict.value.status_code == 409
    second = await put_working_memory_core(
        "s1", UpdateWorkingMemory(version=1, data={"new": True}), None
    )
    assert second.version == 2
    monkeypatch.setattr(settings, "working_memory_max_bytes", 16)
    with pytest.raises(HTTPException) as too_large:
        await put_working_memory_core(
            "s1", UpdateWorkingMemory(version=2, data={"big": True}), None
        )
    assert too_large.value.status_code == 413


async def test_expiry_removes_session_from_index(redis_client):
    await ensure_working_memory_index(redis_client)
    memory = WorkingMemory(session_id="short", ttl_seconds=1)
    await set_working_memory(memory, redis_client)
    deadline = asyncio.get_running_loop().time() + 5
    while await redis_client.exists(Keys.working_memory_key("short")):
        assert asyncio.get_running_loop().time() < deadline
        await asyncio.sleep(0.05)
    assert await list_sessions(redis_client) == (0, [])


async def test_deleted_and_recreated_session_rejects_old_revision(redis_client):
    original = WorkingMemory(session_id="s1", data={"original": True})
    await set_working_memory(original, redis_client)
    stale = await get_working_memory("s1", redis_client=redis_client)
    await delete_working_memory("s1", redis_client=redis_client)
    recreated = WorkingMemory(session_id="s1", data={"recreated": True})
    await set_working_memory(recreated, redis_client)
    assert recreated.version > stale.version
    with pytest.raises(WorkingMemoryConflictError):
        await set_working_memory(stale, redis_client)
    assert (await get_working_memory("s1", redis_client=redis_client)).data == {"recreated": True}


@pytest.mark.parametrize("change", ["update", "delete", "recreate"])
async def test_delayed_extraction_rejects_changed_session_before_publish(
    redis_client, monkeypatch, change
):
    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    await ensure_working_memory_index(redis_client)
    memory = WorkingMemory(
        session_id="shared",
        namespace="orchard",
        user_id="alice",
        messages=[MemoryMessage(role="user", content="old input", discrete_memory_extracted="f")],
    )
    await set_working_memory(memory, redis_client)
    key = f"extraction_pending:{Keys.working_memory_key('shared', 'alice', 'orchard')}"
    await redis_client.set(key, "old-schedule", ex=60)
    entered = asyncio.Event()
    release = asyncio.Event()
    indexed = []

    async def extract(**kwargs):
        entered.set()
        await release.wait()
        return [
            MemoryRecord(
                id="old-output",
                text="stale result",
                namespace="orchard",
                user_id="alice",
                session_id="shared",
            )
        ]

    async def index(records, **kwargs):
        indexed.extend(records)

    monkeypatch.setattr(long_term_memory, "extract_memories_from_session_thread", extract)
    monkeypatch.setattr(long_term_memory, "index_long_term_memories", index)
    task = asyncio.create_task(
        long_term_memory.run_delayed_extraction(
            "shared",
            "orchard",
            "alice",
            "old-schedule",
            source_incarnation=str(memory.version),
        )
    )
    try:
        await asyncio.wait_for(entered.wait(), 5)
        if change == "update":
            current = await get_working_memory(
                "shared",
                user_id="alice",
                namespace="orchard",
                redis_client=redis_client,
            )
            current.messages.append(
                MemoryMessage(role="user", content="new input", discrete_memory_extracted="f")
            )
            await set_working_memory(current, redis_client)
        else:
            await delete_working_memory(
                "shared",
                user_id="alice",
                namespace="orchard",
                redis_client=redis_client,
            )
            if change == "recreate":
                # Creation uses version zero; the server assigns a revision
                # above the retained counter, rejecting the old task's version.
                current = WorkingMemory(
                    session_id="shared",
                    namespace="orchard",
                    user_id="alice",
                    version=0,
                    messages=[
                        MemoryMessage(
                            role="user",
                            content="replacement input",
                            discrete_memory_extracted="f",
                        )
                    ],
                )
                await set_working_memory(current, redis_client)
        await redis_client.set(key, "new-schedule", ex=60)
        release.set()
        with pytest.raises(WorkingMemoryConflictError):
            await asyncio.wait_for(task, 5)
        assert indexed == []
        assert await redis_client.get(key) == "new-schedule"
        assert not await redis_client.exists(
            f"extraction_debounce:{Keys.working_memory_key('shared', 'alice', 'orchard')}"
        )
        result = await get_working_memory(
            "shared", user_id="alice", namespace="orchard", redis_client=redis_client
        )
        if change == "delete":
            assert result is None
        else:
            assert all(message.discrete_memory_extracted == "f" for message in result.messages)
            assert result.messages[-1].content == (
                "new input" if change == "update" else "replacement input"
            )
    finally:
        release.set()
        await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), 5)


@pytest.mark.parametrize("concurrent_update", [False, True])
async def test_extraction_index_failure_restores_eligibility_without_overwriting_updates(
    redis_client, monkeypatch, concurrent_update
):
    from ulid import ULID

    previous_generation = str(ULID())
    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    await ensure_working_memory_index(redis_client)
    source = WorkingMemory(
        session_id="shared",
        namespace="orchard",
        user_id="alice",
        messages=[
            MemoryMessage(role="user", content="old input", discrete_memory_extracted="f"),
            MemoryMessage(
                role="user",
                content="already processed",
                discrete_memory_extracted="t",
                extraction_generation=previous_generation,
            ),
        ],
    )
    await set_working_memory(source, redis_client, extraction_batch=(previous_generation, []))
    await long_term_memory.mark_extraction_batch_published(
        redis_client,
        Keys.working_memory_key("shared", "alice", "orchard"),
        previous_generation,
    )

    async def extract(**kwargs):
        return [
            MemoryRecord(
                id="derived",
                text="result",
                namespace="orchard",
                user_id="alice",
                session_id="shared",
            )
        ]

    async def index(records, **kwargs):
        if concurrent_update:
            latest = await get_working_memory(
                "shared",
                user_id="alice",
                namespace="orchard",
                redis_client=redis_client,
            )
            latest.context = "concurrent context"
            latest.messages.append(
                MemoryMessage(role="user", content="new input", discrete_memory_extracted="f")
            )
            await set_working_memory(latest, redis_client)
        raise RuntimeError("injected index failure")

    monkeypatch.setattr(long_term_memory, "extract_memories_from_session_thread", extract)
    monkeypatch.setattr(long_term_memory, "index_long_term_memories", index)
    pending = f"extraction_pending:{Keys.working_memory_key('shared', 'alice', 'orchard')}"
    timestamp = "2099-01-01T00:00:00+00:00"
    await redis_client.set(pending, timestamp)
    with pytest.raises(RuntimeError, match="injected index failure"):
        await long_term_memory.run_delayed_extraction(
            "shared",
            "orchard",
            "alice",
            timestamp,
            source_incarnation=str(source.version),
        )
    assert await redis_client.get(pending) == timestamp
    result = await get_working_memory(
        "shared", user_id="alice", namespace="orchard", redis_client=redis_client
    )
    assert result.messages[0].discrete_memory_extracted == "f"
    assert result.messages[0].extraction_generation is None
    assert result.messages[1].discrete_memory_extracted == "t"
    assert result.messages[1].extraction_generation is None
    previous_batch = json.loads(
        await redis_client.get(
            f"extraction_batch:{Keys.working_memory_key('shared', 'alice', 'orchard')}:{previous_generation}"
        )
    )
    assert previous_batch["state"] == "finalized"
    if concurrent_update:
        assert result.context == "concurrent context"
        assert result.messages[-1].content == "new input"
        assert result.messages[-1].discrete_memory_extracted == "f"
    else:
        assert result.messages[0].discrete_memory_extracted == "f"
    assert not await redis_client.exists(
        f"extraction_debounce:{Keys.working_memory_key('shared', 'alice', 'orchard')}"
    )
    published = []

    async def successful_index(records, **kwargs):
        published.extend(records)

    monkeypatch.setattr(long_term_memory, "index_long_term_memories", successful_index)
    assert (
        await long_term_memory.run_delayed_extraction(
            "shared",
            "orchard",
            "alice",
            timestamp,
            source_incarnation=str(source.version),
        )
        == 1
    )
    assert len(published) == 1
    assert not await redis_client.exists(pending)
    latest = await get_working_memory(
        "shared", user_id="alice", namespace="orchard", redis_client=redis_client
    )
    assert latest.messages[0].discrete_memory_extracted == "t"
    assert latest.messages[0].extraction_generation is None


@pytest.mark.parametrize("concurrent_update", [False, True])
async def test_successful_extraction_clears_only_own_generation(
    redis_client, monkeypatch, concurrent_update
):
    from ulid import ULID

    previous_generation = str(ULID())
    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    await ensure_working_memory_index(redis_client)
    source = WorkingMemory(
        session_id="shared",
        namespace="orchard",
        user_id="alice",
        messages=[
            MemoryMessage(role="user", content="input", discrete_memory_extracted="f"),
            MemoryMessage(
                role="user",
                content="previous",
                discrete_memory_extracted="t",
                extraction_generation=previous_generation,
            ),
        ],
    )
    await set_working_memory(source, redis_client, extraction_batch=(previous_generation, []))
    await long_term_memory.mark_extraction_batch_published(
        redis_client,
        Keys.working_memory_key("shared", "alice", "orchard"),
        previous_generation,
    )

    scheduled = []

    async def schedule(*args):
        scheduled.append(args)

    monkeypatch.setattr(long_term_memory, "schedule_trailing_extraction", schedule)

    async def extract(**kwargs):
        return [
            MemoryRecord(
                id="derived",
                text="result",
                namespace="orchard",
                user_id="alice",
                session_id="shared",
            )
        ]

    async def index(records, **kwargs):
        latest = await get_working_memory(
            "shared", user_id="alice", namespace="orchard", redis_client=redis_client
        )
        assert latest.messages[0].extraction_generation
        assert latest.messages[0].discrete_memory_extracted == "t"
        if concurrent_update:
            latest.context = "new context"
            latest.messages.append(
                MemoryMessage(role="user", content="new input", discrete_memory_extracted="f")
            )
            await set_working_memory(latest, redis_client)

    monkeypatch.setattr(long_term_memory, "extract_memories_from_session_thread", extract)
    monkeypatch.setattr(long_term_memory, "index_long_term_memories", index)
    assert await long_term_memory.run_delayed_extraction("shared", "orchard", "alice") == 1
    result = await get_working_memory(
        "shared", user_id="alice", namespace="orchard", redis_client=redis_client
    )
    assert result.messages[0].discrete_memory_extracted == "t"
    assert result.messages[0].extraction_generation is None
    assert result.messages[1].extraction_generation is None
    previous_batch = json.loads(
        await redis_client.get(
            f"extraction_batch:{Keys.working_memory_key('shared', 'alice', 'orchard')}:{previous_generation}"
        )
    )
    assert previous_batch["state"] == "finalized"
    if concurrent_update:
        assert result.context == "new context"
        assert result.messages[-1].discrete_memory_extracted == "f"
    assert len(scheduled) == int(concurrent_update)
    debounce_key = f"extraction_debounce:{Keys.working_memory_key('shared', 'alice', 'orchard')}"
    assert bool(await redis_client.exists(debounce_key)) is (not concurrent_update)


async def test_cooldown_messages_schedule_followup_and_stale_revision_cannot_debounce(
    redis_client, monkeypatch
):
    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    monkeypatch.setattr(settings, "enable_discrete_memory_extraction", True)
    await ensure_working_memory_index(redis_client)
    source = WorkingMemory(
        session_id="shared",
        namespace="orchard",
        user_id="alice",
        messages=[MemoryMessage(role="user", content="new input", discrete_memory_extracted="f")],
    )
    await set_working_memory(source, redis_client)
    old_revision = source.version
    source.context = "updated"
    await set_working_memory(source, redis_client)
    assert not await long_term_memory.set_extraction_debounce(
        "shared",
        redis_client,
        namespace="orchard",
        user_id="alice",
        expected_revision=old_revision,
    )
    assert await long_term_memory.should_extract_session_thread(
        "shared", redis_client, namespace="orchard", user_id="alice"
    )
    assert await long_term_memory.set_extraction_debounce(
        "shared",
        redis_client,
        namespace="orchard",
        user_id="alice",
        expected_revision=source.version,
    )
    scheduled = []

    async def schedule(*args):
        scheduled.append(args)

    monkeypatch.setattr(long_term_memory, "schedule_trailing_extraction", schedule)
    await long_term_memory.promote_working_memory_to_long_term(
        "shared", namespace="orchard", user_id="alice", redis_client=redis_client
    )
    assert len(scheduled) == 1
    assert scheduled[0][:3] == ("shared", "orchard", "alice")


async def test_cooldown_followup_uses_real_docket_scoped_payloads(redis_client, monkeypatch):
    connection = redis_client.connection_pool.connection_kwargs
    url = f"redis://{connection['host']}:{connection['port']}/0"
    monkeypatch.setattr(settings, "redis_url", url)
    monkeypatch.setattr(settings, "docket_name", "extraction-live")
    monkeypatch.setattr(settings, "use_docket", True)
    monkeypatch.setattr(settings, "enable_discrete_memory_extraction", True)
    monkeypatch.setattr(settings, "extraction_debounce_seconds", 60)
    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    await ensure_working_memory_index(redis_client)
    owners = [("orchard", "alice"), ("orchard", "bob"), ("forest", "alice")]
    for namespace, user_id in owners:
        source = WorkingMemory(
            session_id="shared",
            namespace=namespace,
            user_id=user_id,
            messages=[
                MemoryMessage(role="user", content="new input", discrete_memory_extracted="f")
            ],
        )
        await set_working_memory(source, redis_client)
        await long_term_memory.set_extraction_debounce(
            "shared", redis_client, namespace=namespace, user_id=user_id
        )
        await long_term_memory.promote_working_memory_to_long_term(
            "shared", namespace=namespace, user_id=user_id, redis_client=redis_client
        )
    task_keys = await redis_client.zrange("extraction-live:queue", 0, -1)
    assert len(task_keys) == 3
    recovered = set()
    async with Docket(name="extraction-live", url=url) as docket:
        for key in task_keys:
            execution = await docket.get_execution(key)
            assert execution is not None
            payload = execution.kwargs
            recovered.add((payload["namespace"], payload["user_id"]))
            identity = Keys.working_memory_key("shared", payload["user_id"], payload["namespace"])
            assert key == f"extraction:{identity}:{payload['scheduled_timestamp']}"
            assert (
                await redis_client.get(f"extraction_pending:{identity}")
                == payload["scheduled_timestamp"]
            )
    assert recovered == set(owners)


@pytest.mark.parametrize("with_results", [False, True])
async def test_real_extraction_worker_scoped_sessions_converge(
    dragonfly_factory, monkeypatch, with_results
):
    url = dragonfly_factory(f"--lua_undeclared_keys_shas={SCHEDULER_SHA}")
    monkeypatch.setattr(settings, "redis_url", url)
    monkeypatch.setattr(settings, "docket_name", "extraction-worker")
    monkeypatch.setattr(settings, "use_docket", True)
    monkeypatch.setattr(settings, "extraction_debounce_seconds", 1)
    monkeypatch.setattr(settings, "redisvl_vector_dimensions", 3)
    monkeypatch.setattr(settings, "embedding_model", "test-fixed-3d")
    monkeypatch.setattr(settings, "compact_semantic_duplicates", False)
    monkeypatch.setattr(settings, "enable_discrete_memory_extraction", False)

    class SilentTasks:
        def add_task(self, *args, **kwargs):
            pass

    monkeypatch.setattr(long_term_memory, "get_background_tasks", lambda: SilentTasks())
    seen = []

    async def extract(**kwargs):
        seen.append((kwargs["namespace"], kwargs["user_id"]))
        return (
            [
                MemoryRecord(
                    id="derived",
                    text="recent orchard",
                    namespace=kwargs["namespace"],
                    user_id=kwargs["user_id"],
                    session_id="shared",
                )
            ]
            if with_results
            else []
        )

    monkeypatch.setattr(long_term_memory, "extract_memories_from_session_thread", extract)
    async with (
        Redis.from_url(url, decode_responses=True) as client,
        Docket(name="extraction-worker", url=url) as docket,
    ):
        monkeypatch.setattr(redis_utils, "_redis_pool", client)
        await ensure_working_memory_index(client)
        index = AsyncSearchIndex.from_dict(_build_redis_schema(), redis_client=client)
        db = RedisVLMemoryVectorDatabase(index, FixedEmbeddings())

        async def get_db():
            return db

        monkeypatch.setattr(long_term_memory, "get_memory_vector_db", get_db)
        owners = [("orchard", "alice"), ("orchard", "bob"), ("forest", "alice")]
        for namespace, user_id in owners:
            source = WorkingMemory(
                session_id="shared",
                namespace=namespace,
                user_id=user_id,
                messages=[
                    MemoryMessage(
                        role="user",
                        content=f"{namespace}/{user_id}",
                        discrete_memory_extracted="f",
                    )
                ],
            )
            await set_working_memory(source, client)
            await long_term_memory.schedule_trailing_extraction(
                "shared", namespace, user_id, client
            )
        keys = await client.zrange("extraction-worker:queue", 0, -1)
        assert len(keys) == 3
        docket.register(long_term_memory.run_delayed_extraction)
        executions = [await docket.get_execution(key) for key in keys]
        results = await asyncio.gather(
            run_worker(docket),
            *(execution.get_result(timeout=timedelta(seconds=10)) for execution in executions),
        )
        assert results[1:] == [int(with_results)] * 3
        assert set(seen) == set(owners)
        for namespace, user_id in owners:
            memory = await get_working_memory(
                "shared", user_id=user_id, namespace=namespace, redis_client=client
            )
            assert memory.messages[0].discrete_memory_extracted == "t"
            assert memory.messages[0].extraction_generation is None
            identity = Keys.working_memory_key("shared", user_id, namespace)
            assert not await client.exists(f"extraction_pending:{identity}")
        assert await client.zcard("extraction-worker:queue") == 0
        assert (await client.xpending("extraction-worker:stream", docket.worker_group_name))[
            "pending"
        ] == 0
        if with_results:
            results = await db.list_memories(limit=10)
            assert {
                (memory.namespace, memory.user_id, memory.id) for memory in results.memories
            } == {(namespace, user_id, "derived") for namespace, user_id in owners}
            assert all(
                [
                    await client.exists(Keys.memory_key("derived", namespace, user_id))
                    for namespace, user_id in owners
                ]
            )


async def test_real_worker_extracts_messages_arriving_after_first_publication(
    dragonfly_factory, monkeypatch
):
    url = dragonfly_factory(f"--lua_undeclared_keys_shas={SCHEDULER_SHA}")
    monkeypatch.setattr(settings, "redis_url", url)
    monkeypatch.setattr(settings, "docket_name", "continuous-extraction")
    monkeypatch.setattr(settings, "use_docket", True)
    monkeypatch.setattr(settings, "extraction_debounce_seconds", 1)
    monkeypatch.setattr(settings, "redisvl_vector_dimensions", 3)
    monkeypatch.setattr(settings, "embedding_model", "test-fixed-3d")
    monkeypatch.setattr(settings, "compact_semantic_duplicates", False)
    monkeypatch.setattr(settings, "enable_discrete_memory_extraction", False)

    class SilentTasks:
        def add_task(self, *args, **kwargs):
            pass

    monkeypatch.setattr(long_term_memory, "get_background_tasks", lambda: SilentTasks())
    async with (
        Redis.from_url(url, decode_responses=True) as client,
        Docket(name="continuous-extraction", url=url) as docket,
    ):
        monkeypatch.setattr(redis_utils, "_redis_pool", client)
        await ensure_working_memory_index(client)
        index = AsyncSearchIndex.from_dict(_build_redis_schema(), redis_client=client)
        db = RedisVLMemoryVectorDatabase(index, FixedEmbeddings())

        async def get_db():
            return db

        monkeypatch.setattr(long_term_memory, "get_memory_vector_db", get_db)
        seen = []

        async def extract(**kwargs):
            memory = await get_working_memory(
                "shared", user_id="alice", namespace="orchard", redis_client=client
            )
            texts = [
                message.content
                for message in memory.messages
                if message.discrete_memory_extracted == "f"
            ]
            seen.append(texts)
            return [
                MemoryRecord(
                    id=f"derived-{len(seen)}",
                    text="recent orchard",
                    namespace="orchard",
                    user_id="alice",
                    session_id="shared",
                )
            ]

        monkeypatch.setattr(long_term_memory, "extract_memories_from_session_thread", extract)
        original_index = long_term_memory.index_long_term_memories

        async def publish(records, **kwargs):
            await original_index(records, **kwargs)
            if len(seen) == 1:
                memory = await get_working_memory(
                    "shared", user_id="alice", namespace="orchard", redis_client=client
                )
                memory.messages.append(
                    MemoryMessage(
                        role="user",
                        content="second message",
                        discrete_memory_extracted="f",
                    )
                )
                await set_working_memory(memory, client)

        monkeypatch.setattr(long_term_memory, "index_long_term_memories", publish)

        # Different text prevents content-hash dedup from suppressing the second result.
        async def embed(texts):
            return [[1.0, 0.0, 0.0] for text in texts]

        monkeypatch.setattr(db.embeddings, "aembed_documents", embed)

        async def extract_distinct(**kwargs):
            records = await extract(**kwargs)
            records[0].text = f"result {len(seen)}"
            return records

        monkeypatch.setattr(
            long_term_memory, "extract_memories_from_session_thread", extract_distinct
        )
        queued_keys = set()
        original_schedule = long_term_memory.schedule_trailing_extraction

        async def schedule(*args, **kwargs):
            await original_schedule(*args, **kwargs)
            queued_keys.update(await client.zrange("continuous-extraction:queue", 0, -1))

        monkeypatch.setattr(long_term_memory, "schedule_trailing_extraction", schedule)
        source = WorkingMemory(
            session_id="shared",
            namespace="orchard",
            user_id="alice",
            messages=[
                MemoryMessage(role="user", content="first message", discrete_memory_extracted="f")
            ],
        )
        await set_working_memory(source, client)
        await long_term_memory.schedule_trailing_extraction("shared", "orchard", "alice", client)
        pending = f"extraction_pending:{Keys.working_memory_key('shared', 'alice', 'orchard')}"
        assert await client.ttl(pending) == -1
        # Start consumption after the former pending TTL would have expired.
        await asyncio.sleep(settings.extraction_debounce_seconds * 2 + 0.1)
        docket.register(long_term_memory.run_delayed_extraction)
        await run_worker(docket)
        assert seen == [["first message"], ["second message"]]
        memory = await get_working_memory(
            "shared", user_id="alice", namespace="orchard", redis_client=client
        )
        assert all(
            message.discrete_memory_extracted == "t" and message.extraction_generation is None
            for message in memory.messages
        )
        assert {memory.id for memory in (await db.list_memories(limit=10)).memories} == {
            "derived-1",
            "derived-2",
        }
        assert not await client.exists(
            f"extraction_pending:{Keys.working_memory_key('shared', 'alice', 'orchard')}"
        )
        assert await client.zcard("continuous-extraction:queue") == 0
        assert len(queued_keys) == 2
        for key in queued_keys:
            execution = await docket.get_execution(key)
            assert execution.state == ExecutionState.COMPLETED
            assert await execution.get_result(timeout=timedelta(seconds=2)) == 1
        # Docket removes acknowledged entries from the Stream.
        assert await client.xlen("continuous-extraction:stream") == 0
        assert (await client.xpending("continuous-extraction:stream", docket.worker_group_name))[
            "pending"
        ] == 0


async def test_extraction_dispatch_failure_is_visible_and_retry_really_queues(
    redis_client, monkeypatch
):
    connection = redis_client.connection_pool.connection_kwargs
    url = f"redis://{connection['host']}:{connection['port']}/0"
    monkeypatch.setattr(settings, "redis_url", url)
    monkeypatch.setattr(settings, "docket_name", "dispatch-retry")
    monkeypatch.setattr(settings, "use_docket", True)
    real_docket = Docket
    attempts = []

    def flaky_queue(**kwargs):
        attempts.append(kwargs)
        if len(attempts) == 1:
            raise ConnectionError("injected queue unavailable")
        return real_docket(**kwargs)

    monkeypatch.setattr(long_term_memory, "Docket", flaky_queue)
    with pytest.raises(ConnectionError, match="queue unavailable"):
        await long_term_memory.schedule_trailing_extraction(
            "shared", "orchard", "alice", redis_client
        )
    identity = Keys.working_memory_key("shared", "alice", "orchard")
    pending_key = f"extraction_pending:{identity}"
    failed_timestamp = await redis_client.get(pending_key)
    assert failed_timestamp
    assert await redis_client.zcard("dispatch-retry:queue") == 0
    await long_term_memory.schedule_trailing_extraction("shared", "orchard", "alice", redis_client)
    queued = await redis_client.zrange("dispatch-retry:queue", 0, -1)
    assert len(queued) == 1
    async with real_docket(name="dispatch-retry", url=url) as docket:
        execution = await docket.get_execution(queued[0])
        assert execution.kwargs["namespace"] == "orchard"
        assert execution.kwargs["user_id"] == "alice"
        assert execution.kwargs["scheduled_timestamp"] == await redis_client.get(pending_key)
        assert execution.kwargs["scheduled_timestamp"] != failed_timestamp


async def test_extraction_outbox_recovers_after_pending_expiry(redis_client, monkeypatch):
    connection = redis_client.connection_pool.connection_kwargs
    url = f"redis://{connection['host']}:{connection['port']}/0"
    monkeypatch.setattr(settings, "redis_url", url)
    monkeypatch.setattr(settings, "docket_name", "dispatch-retry")
    monkeypatch.setattr(settings, "use_docket", True)
    real_docket = Docket
    attempts = []

    def flaky_queue(**kwargs):
        attempts.append(kwargs)
        if len(attempts) == 1:
            raise ConnectionError("injected queue unavailable")
        return real_docket(**kwargs)

    monkeypatch.setattr(long_term_memory, "Docket", flaky_queue)
    with pytest.raises(ConnectionError, match="queue unavailable"):
        await long_term_memory.schedule_trailing_extraction(
            "shared", "orchard", "alice", redis_client
        )
    identity = Keys.working_memory_key("shared", "alice", "orchard")
    pending_key = f"extraction_pending:{identity}"
    failed_timestamp = await redis_client.get(pending_key)
    assert failed_timestamp
    assert await redis_client.zcard("dispatch-retry:queue") == 0
    outbox_key = f"extraction_outbox:{identity}"
    assert await redis_client.ttl(outbox_key) == -1
    await redis_client.pexpire(pending_key, 1)
    await asyncio.sleep(0.02)
    assert not await redis_client.exists(pending_key)
    assert await long_term_memory.reconcile_extraction_outbox(redis_client) == 1
    assert not await redis_client.exists(outbox_key)
    assert await long_term_memory.reconcile_extraction_outbox(redis_client) == 0
    queued = await redis_client.zrange("dispatch-retry:queue", 0, -1)
    assert len(queued) == 1
    async with real_docket(name="dispatch-retry", url=url) as docket:
        execution = await docket.get_execution(queued[0])
        assert execution.kwargs["namespace"] == "orchard"
        assert execution.kwargs["user_id"] == "alice"
        assert execution.kwargs["scheduled_timestamp"] == await redis_client.get(pending_key)
        assert execution.kwargs["scheduled_timestamp"] == failed_timestamp


async def test_outbox_retry_after_queue_success_before_cleanup(redis_client, monkeypatch):
    connection = redis_client.connection_pool.connection_kwargs
    url = f"redis://{connection['host']}:{connection['port']}/0"
    monkeypatch.setattr(settings, "redis_url", url)
    monkeypatch.setattr(settings, "docket_name", "cleanup-retry")
    monkeypatch.setattr(settings, "use_docket", True)
    original_eval = redis_client.eval
    failures = []

    async def fail_cleanup(script, numkeys, *args):
        if numkeys == 1 and not failures:
            failures.append(True)
            raise ConnectionError("cleanup connection lost")
        return await original_eval(script, numkeys, *args)

    monkeypatch.setattr(redis_client, "eval", fail_cleanup)
    with pytest.raises(ConnectionError, match="cleanup connection lost"):
        await long_term_memory.schedule_trailing_extraction(
            "shared", "orchard", "alice", redis_client
        )
    identity = Keys.working_memory_key("shared", "alice", "orchard")
    outbox = f"extraction_outbox:{identity}"
    assert await redis_client.exists(outbox)
    before = await redis_client.zrange("cleanup-retry:queue", 0, -1)
    assert len(before) == 1
    assert await long_term_memory.reconcile_extraction_outbox(redis_client) == 1
    assert await redis_client.zrange("cleanup-retry:queue", 0, -1) == before
    assert not await redis_client.exists(outbox)
    async with Docket(name="cleanup-retry", url=url) as docket:
        execution = await docket.get_execution(before[0])
        assert execution.kwargs["namespace"] == "orchard"
        assert execution.kwargs["user_id"] == "alice"


async def test_stale_outbox_snapshot_is_not_counted_or_dispatched(redis_client, monkeypatch):
    identity = Keys.working_memory_key("shared", "alice", "orchard")
    key = f"extraction_outbox:{identity}"
    timestamp = "2099-01-01T00:00:00+00:00"
    payload = {
        "session_id": "shared",
        "namespace": "orchard",
        "user_id": "alice",
        "scheduled_timestamp": timestamp,
        "task_key": f"extraction:{identity}:{timestamp}",
    }
    old = json.dumps(payload)
    new_timestamp = "2099-01-02T00:00:00+00:00"
    newer = json.dumps(
        dict(
            payload,
            scheduled_timestamp=new_timestamp,
            task_key=f"extraction:{identity}:{new_timestamp}",
        )
    )
    await redis_client.set(key, newer)
    pending = f"extraction_pending:{identity}"
    await redis_client.set(pending, new_timestamp)
    original_get = redis_client.get
    original_eval = redis_client.eval
    comparisons = []

    async def stale_read(redis_key):
        if redis_key == key:
            return old
        return await original_get(redis_key)

    async def observe_comparison(script, numkeys, *args):
        result = await original_eval(script, numkeys, *args)
        comparisons.append((numkeys, args, result))
        return result

    def unexpected_docket(*args, **kwargs):
        pytest.fail("Superseded outbox must not open Docket")

    monkeypatch.setattr(redis_client, "get", stale_read)
    monkeypatch.setattr(redis_client, "eval", observe_comparison)
    monkeypatch.setattr(long_term_memory, "Docket", unexpected_docket)
    assert await long_term_memory.reconcile_extraction_outbox(redis_client) == 0
    assert len(comparisons) == 1
    assert comparisons[0][0] == 3
    assert comparisons[0][1][:4] == (key, pending, identity, old)
    assert comparisons[0][2] == 0
    assert await original_get(key) == newer
    assert await original_get(pending) == new_timestamp


async def test_new_outbox_intent_survives_old_dispatch_cleanup(redis_client, monkeypatch):
    connection = redis_client.connection_pool.connection_kwargs
    url = f"redis://{connection['host']}:{connection['port']}/0"
    monkeypatch.setattr(settings, "redis_url", url)
    monkeypatch.setattr(settings, "docket_name", "new-intent")
    monkeypatch.setattr(settings, "use_docket", True)
    identity = Keys.working_memory_key("shared", "alice", "orchard")
    key = f"extraction_outbox:{identity}"
    pending = f"extraction_pending:{identity}"
    original_enter = Docket.__aenter__
    changes = []

    async def enter_and_replace(docket):
        entered = await original_enter(docket)
        if not changes:
            old = json.loads(await redis_client.get(key))
            new = dict(old)
            # Change the exact persisted intent during the old queue await.
            new["scheduled_timestamp"] = "2099-01-01T00:00:00+00:00"
            new["task_key"] = f"extraction:{identity}:{new['scheduled_timestamp']}"
            serialized = json.dumps(new, sort_keys=True)
            changes.append(serialized)
            await redis_client.eval(
                "redis.call('SET', KEYS[1], ARGV[1]); redis.call('SET', KEYS[2], ARGV[2]); return 1",
                2,
                key,
                pending,
                serialized,
                new["scheduled_timestamp"],
            )
        return entered

    monkeypatch.setattr(Docket, "__aenter__", enter_and_replace)
    await long_term_memory.schedule_trailing_extraction("shared", "orchard", "alice", redis_client)
    assert await redis_client.get(key) == changes[0]
    assert await redis_client.get(pending) == "2099-01-01T00:00:00+00:00"
    assert await redis_client.zcard("new-intent:queue") == 1
    assert await long_term_memory.reconcile_extraction_outbox(redis_client) == 1
    assert not await redis_client.exists(key)
    assert await redis_client.zcard("new-intent:queue") == 2
    assert await redis_client.get(pending) == "2099-01-01T00:00:00+00:00"


async def test_invalid_outbox_does_not_block_other_owner(redis_client, monkeypatch, caplog):
    connection = redis_client.connection_pool.connection_kwargs
    url = f"redis://{connection['host']}:{connection['port']}/0"
    monkeypatch.setattr(settings, "redis_url", url)
    monkeypatch.setattr(settings, "docket_name", "isolated-outbox")
    monkeypatch.setattr(settings, "use_docket", True)
    bad = "extraction_outbox:broken"
    await redis_client.set(bad, "{invalid-json")
    identity = Keys.working_memory_key("shared", "alice", "orchard")
    key = f"extraction_outbox:{identity}"
    timestamp = "2099-01-01T00:00:00+00:00"
    payload = json.dumps(
        {
            "session_id": "shared",
            "namespace": "orchard",
            "user_id": "alice",
            "scheduled_timestamp": timestamp,
            "task_key": f"extraction:{identity}:{timestamp}",
        }
    )
    await redis_client.set(key, payload)
    assert await long_term_memory.reconcile_extraction_outbox(redis_client) == 1
    assert await redis_client.get(bad) == "{invalid-json"
    assert not await redis_client.exists(key)
    assert await redis_client.zcard("isolated-outbox:queue") == 1
    assert "retained" in caplog.text and bad in caplog.text


@pytest.mark.parametrize("change", ["owner", "task", "timezone"])
async def test_invalid_outbox_identity_is_rejected_before_pending_write(redis_client, change):
    identity = Keys.working_memory_key("shared", "alice", "orchard")
    key = f"extraction_outbox:{identity}"
    payload = {
        "session_id": "shared",
        "namespace": "orchard",
        "user_id": "alice",
        "scheduled_timestamp": "2099-01-01T00:00:00+00:00",
        "task_key": f"extraction:{identity}:2099-01-01T00:00:00+00:00",
    }
    if change == "owner":
        payload["user_id"] = "bob"
    elif change == "task":
        payload["task_key"] = "foreign-task"
    else:
        payload["scheduled_timestamp"] = "2099-01-01T00:00:00"
    serialized = json.dumps(payload)
    await redis_client.set(key, serialized)
    with pytest.raises(ValueError):
        await long_term_memory._dispatch_extraction_outbox(redis_client, key, serialized)
    assert await redis_client.get(key) == serialized
    assert not await redis_client.exists(f"extraction_pending:{identity}")


async def test_real_extraction_worker_records_failure_and_retains_schedule(
    dragonfly_factory, monkeypatch
):
    url = dragonfly_factory(f"--lua_undeclared_keys_shas={SCHEDULER_SHA}")
    monkeypatch.setattr(settings, "redis_url", url)
    monkeypatch.setattr(settings, "docket_name", "failed-extraction")
    monkeypatch.setattr(settings, "use_docket", True)
    monkeypatch.setattr(settings, "extraction_debounce_seconds", 1)

    attempts = []

    class FailingStrategy:
        async def extract_memories(self, conversation, context):
            attempts.append(context)
            raise RuntimeError("injected model unavailable")

    monkeypatch.setattr(
        "agent_memory_server.memory_strategies.get_memory_strategy",
        lambda *args, **kwargs: FailingStrategy(),
    )
    async with (
        Redis.from_url(url, decode_responses=True) as client,
        Docket(name="failed-extraction", url=url) as docket,
    ):
        monkeypatch.setattr(redis_utils, "_redis_pool", client)
        await ensure_working_memory_index(client)
        source = WorkingMemory(
            session_id="shared",
            namespace="orchard",
            user_id="alice",
            messages=[MemoryMessage(role="user", content="input", discrete_memory_extracted="f")],
        )
        await set_working_memory(source, client)
        await long_term_memory.schedule_trailing_extraction("shared", "orchard", "alice", client)
        identity = Keys.working_memory_key("shared", "alice", "orchard")
        pending = f"extraction_pending:{identity}"
        timestamp = await client.get(pending)
        keys = await client.zrange("failed-extraction:queue", 0, -1)
        assert len(keys) == 1
        docket.register(long_term_memory.run_delayed_extraction)
        await run_worker(docket)
        execution = await docket.get_execution(keys[0])
        assert execution.state == ExecutionState.FAILED
        assert len(attempts) == 3
        with pytest.raises(RuntimeError, match="injected model unavailable"):
            await execution.get_result(timeout=timedelta(seconds=2))
        assert await client.get(pending) == timestamp
        assert await client.ttl(pending) == -1
        memory = await get_working_memory("shared", "alice", "orchard", client)
        assert memory.messages[0].discrete_memory_extracted == "f"
        assert memory.messages[0].extraction_generation is None
        assert await client.zcard("failed-extraction:queue") == 0
        assert await client.xlen("failed-extraction:stream") == 0
        assert (await client.xpending("failed-extraction:stream", docket.worker_group_name))[
            "pending"
        ] == 0

        recovery_attempts = []

        class RecoveringStrategy:
            async def extract_memories(self, conversation, context):
                recovery_attempts.append(context)
                if len(recovery_attempts) == 1:
                    raise RuntimeError("transient model failure")
                return []

        monkeypatch.setattr(
            "agent_memory_server.memory_strategies.get_memory_strategy",
            lambda *args, **kwargs: RecoveringStrategy(),
        )
        # Explicitly redeliver the original payload and task ID after recovery.
        await docket.add(long_term_memory.run_delayed_extraction, key=keys[0])(
            session_id="shared",
            namespace="orchard",
            user_id="alice",
            scheduled_timestamp=timestamp,
            source_incarnation=str(source.version),
        )
        await run_worker(docket)
        recovered = await docket.get_execution(keys[0])
        assert recovered.state == ExecutionState.COMPLETED
        assert len(recovery_attempts) == 2
        assert await recovered.get_result(timeout=timedelta(seconds=2)) == 0
        assert not await client.exists(pending)
        memory = await get_working_memory("shared", "alice", "orchard", client)
        assert memory.messages[0].discrete_memory_extracted == "t"
        assert memory.messages[0].extraction_generation is None
        assert await client.xlen("failed-extraction:stream") == 0
        assert (await client.xpending("failed-extraction:stream", docket.worker_group_name))[
            "pending"
        ] == 0


async def test_server_session_incarnation_survives_updates_but_not_recreation(
    redis_client,
):
    await ensure_working_memory_index(redis_client)
    source = WorkingMemory(session_id="incarnation", namespace="orchard", user_id="alice")
    await set_working_memory(source, redis_client)
    key = Keys.working_memory_key("incarnation", "alice", "orchard")
    first = (await redis_client.json().get(key))["_session_incarnation"]
    assert first == str(source.version)
    source.context = "ordinary update"
    await set_working_memory(source, redis_client)
    assert (await redis_client.json().get(key))["_session_incarnation"] == first
    assert source.version > int(first)
    # Even a forged serialized field must not replace the server identity.
    from agent_memory_server.working_memory import _SET_WORKING_MEMORY

    forged = json.loads(source.model_dump_json())
    forged["_session_incarnation"] = "client-chosen"
    script = redis_client.register_script(_SET_WORKING_MEMORY)
    await script(
        keys=[key, Keys.working_memory_revision_key("incarnation", "alice", "orchard")],
        args=[json.dumps(forged), source.version, 0],
    )
    assert (await redis_client.json().get(key))["_session_incarnation"] == first
    await delete_working_memory("incarnation", "alice", "orchard", redis_client)
    replacement = WorkingMemory(session_id="incarnation", namespace="orchard", user_id="alice")
    await set_working_memory(replacement, redis_client)
    second = (await redis_client.json().get(key))["_session_incarnation"]
    assert second == str(replacement.version)
    assert int(second) > int(first)
    await redis_client.pexpire(key, 1)
    await asyncio.sleep(0.02)
    assert not await redis_client.exists(key)
    replacement = WorkingMemory(session_id="incarnation", namespace="orchard", user_id="alice")
    await set_working_memory(replacement, redis_client)
    third = (await redis_client.json().get(key))["_session_incarnation"]
    assert third == str(replacement.version)
    assert int(third) > int(second)


@pytest.mark.parametrize("replace_source", [False, True])
async def test_queued_source_incarnation_allows_update_but_rejects_recreation(
    redis_client, monkeypatch, replace_source
):
    connection = redis_client.connection_pool.connection_kwargs
    url = f"redis://{connection['host']}:{connection['port']}/0"
    monkeypatch.setattr(settings, "redis_url", url)
    monkeypatch.setattr(settings, "docket_name", "source-bound")
    monkeypatch.setattr(settings, "use_docket", True)
    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    await ensure_working_memory_index(redis_client)
    source = WorkingMemory(
        session_id="shared",
        namespace="orchard",
        user_id="alice",
        messages=[MemoryMessage(role="user", content="input", discrete_memory_extracted="f")],
    )
    await set_working_memory(source, redis_client)
    initial = str(source.version)
    await long_term_memory.schedule_trailing_extraction("shared", "orchard", "alice", redis_client)
    keys = await redis_client.zrange("source-bound:queue", 0, -1)
    async with Docket(name="source-bound", url=url) as docket:
        execution = await docket.get_execution(keys[0])
        payload = execution.kwargs
    assert payload["source_incarnation"] == initial
    if replace_source:
        await delete_working_memory("shared", "alice", "orchard", redis_client)
        source = WorkingMemory(
            session_id="shared",
            namespace="orchard",
            user_id="alice",
            messages=[
                MemoryMessage(role="user", content="replacement", discrete_memory_extracted="f")
            ],
        )
    else:
        source.context = "updated context"
    await set_working_memory(source, redis_client)
    calls = []

    async def extract(**kwargs):
        calls.append(kwargs)
        return []

    monkeypatch.setattr(long_term_memory, "extract_memories_from_session_thread", extract)
    assert await long_term_memory.run_delayed_extraction(**payload) == 0
    assert len(calls) == int(not replace_source)
    latest = await get_working_memory("shared", "alice", "orchard", redis_client)
    assert latest.messages[0].discrete_memory_extracted == ("f" if replace_source else "t")
    if replace_source:
        assert latest.messages[0].content == "replacement"


async def test_unversioned_scheduled_extraction_requires_rescheduling(redis_client, monkeypatch):
    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    pending = f"extraction_pending:{Keys.working_memory_key('shared', 'alice', 'orchard')}"
    await redis_client.set(pending, "historical")
    with pytest.raises(ValueError, match="reschedule required"):
        await long_term_memory.run_delayed_extraction("shared", "orchard", "alice", "historical")
    assert await redis_client.get(pending) == "historical"


async def test_asyncio_schedule_atomically_binds_source_and_rejects_recreation(
    redis_client, monkeypatch
):
    monkeypatch.setattr(settings, "use_docket", False)
    monkeypatch.setattr(settings, "extraction_debounce_seconds", 1)
    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    await ensure_working_memory_index(redis_client)
    source = WorkingMemory(
        session_id="shared",
        namespace="orchard",
        user_id="alice",
        messages=[MemoryMessage(role="user", content="old", discrete_memory_extracted="f")],
    )
    await set_working_memory(source, redis_client)
    original_eval = redis_client.eval
    observed = []

    async def observe(script, numkeys, *args):
        result = await original_eval(script, numkeys, *args)
        observed.append((numkeys, args, result))
        return result

    monkeypatch.setattr(redis_client, "eval", observe)
    before = set(long_term_memory._pending_extraction_tasks)
    await long_term_memory.schedule_trailing_extraction("shared", "orchard", "alice", redis_client)
    tasks = long_term_memory._pending_extraction_tasks - before
    assert len(tasks) == 1
    key = Keys.working_memory_key("shared", "alice", "orchard")
    pending = f"extraction_pending:{key}"
    assert observed[0][0] == 2
    assert observed[0][1][:2] == (key, pending)
    assert observed[0][2] == str(source.version)
    assert 0 < await redis_client.ttl(pending) <= 2

    async def unexpected_extract(**kwargs):
        pytest.fail("Old asyncio task must not extract a replacement session")

    monkeypatch.setattr(
        long_term_memory, "extract_memories_from_session_thread", unexpected_extract
    )
    try:
        await delete_working_memory("shared", "alice", "orchard", redis_client)
        replacement = WorkingMemory(
            session_id="shared",
            namespace="orchard",
            user_id="alice",
            messages=[MemoryMessage(role="user", content="new", discrete_memory_extracted="f")],
        )
        await set_working_memory(replacement, redis_client)
        await asyncio.wait_for(asyncio.gather(*tasks), 5)
        latest = await get_working_memory("shared", "alice", "orchard", redis_client)
        assert latest.messages[0].content == "new"
        assert latest.messages[0].discrete_memory_extracted == "f"
        assert not await redis_client.exists(pending)
        assert not (tasks & long_term_memory._pending_extraction_tasks)
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.parametrize("source_exists", [False, True])
async def test_delete_session_atomically_retires_only_its_dispatch_metadata(
    redis_client, source_exists
):
    await ensure_working_memory_index(redis_client)
    owner = WorkingMemory(session_id="shared", namespace="orchard", user_id="alice")
    other = WorkingMemory(session_id="shared", namespace="orchard", user_id="bob")
    if source_exists:
        await set_working_memory(owner, redis_client)
    await set_working_memory(other, redis_client)
    key = Keys.working_memory_key("shared", "alice", "orchard")
    other_key = Keys.working_memory_key("shared", "bob", "orchard")
    prefixes = ["extraction_pending", "extraction_debounce", "extraction_outbox"]
    for prefix in prefixes:
        await redis_client.set(f"{prefix}:{key}", "old intent")
        await redis_client.set(f"{prefix}:{other_key}", "other intent")
    revision_key = Keys.working_memory_revision_key("shared", "alice", "orchard")
    revision = await redis_client.get(revision_key)
    await delete_working_memory("shared", "alice", "orchard", redis_client)
    assert not await redis_client.exists(key)
    assert await redis_client.get(revision_key) == revision
    for prefix in prefixes:
        assert not await redis_client.exists(f"{prefix}:{key}")
        assert await redis_client.get(f"{prefix}:{other_key}") == "other intent"
    assert (
        await get_working_memory("shared", "bob", "orchard", redis_client)
    ).version == other.version
    await delete_working_memory("shared", "alice", "orchard", redis_client)
    assert await redis_client.get(revision_key) == revision


@pytest.mark.parametrize("recreate", [False, True])
async def test_outbox_recovery_retires_expired_source_without_touching_new_pending(
    redis_client, monkeypatch, recreate
):
    monkeypatch.setattr(settings, "use_docket", True)
    await ensure_working_memory_index(redis_client)
    source = WorkingMemory(session_id="shared", namespace="orchard", user_id="alice")
    await set_working_memory(source, redis_client)

    def unavailable_queue(**kwargs):
        raise ConnectionError("queue unavailable")

    monkeypatch.setattr(long_term_memory, "Docket", unavailable_queue)
    with pytest.raises(ConnectionError):
        await long_term_memory.schedule_trailing_extraction(
            "shared", "orchard", "alice", redis_client
        )
    identity = Keys.working_memory_key("shared", "alice", "orchard")
    outbox = f"extraction_outbox:{identity}"
    pending = f"extraction_pending:{identity}"
    assert json.loads(await redis_client.get(outbox))["source_incarnation"] == str(source.version)
    await redis_client.pexpire(identity, 1)
    await asyncio.sleep(0.02)
    assert not await redis_client.exists(identity)
    if recreate:
        replacement = WorkingMemory(session_id="shared", namespace="orchard", user_id="alice")
        await set_working_memory(replacement, redis_client)
        await redis_client.set(pending, "new schedule")
    assert await long_term_memory.reconcile_extraction_outbox(redis_client) == 0
    assert not await redis_client.exists(outbox)
    if recreate:
        assert await redis_client.get(pending) == "new schedule"
        assert (
            await get_working_memory("shared", "alice", "orchard", redis_client)
        ).version == replacement.version
    else:
        assert not await redis_client.exists(pending)
    assert await long_term_memory.reconcile_extraction_outbox(redis_client) == 0


@pytest.mark.parametrize("crash_phase", ["prepare", "marked", "published"])
async def test_extraction_worker_process_crash_is_reclaimed_by_new_process(
    dragonfly_factory, monkeypatch, tmp_path, crash_phase
):
    url = dragonfly_factory(f"--lua_undeclared_keys_shas={SCHEDULER_SHA}")
    monkeypatch.setattr(settings, "redis_url", url)
    monkeypatch.setattr(settings, "docket_name", "process-extraction")
    monkeypatch.setattr(settings, "use_docket", True)
    monkeypatch.setattr(settings, "extraction_debounce_seconds", 1)
    child_code = """
import asyncio, sys
from datetime import timedelta
from docket import Docket, Worker
from redis.asyncio import Redis
from agent_memory_server import long_term_memory, memory_strategies
from agent_memory_server.config import settings
from agent_memory_server.utils import redis as redis_utils
from agent_memory_server.memory_vector_db import RedisVLMemoryVectorDatabase
from agent_memory_server.memory_vector_db_factory import _build_redis_schema
from redisvl.index import AsyncSearchIndex
from test_search import FixedEmbeddings

async def main():
    settings.redis_url = sys.argv[1]
    settings.docket_name = 'process-extraction'
    from agent_memory_server.utils import model_execution_guard as execution_guard
    execution_guard.DEFAULT_LEASE_TTL_MS = 300
    settings.use_docket = True
    async with Redis.from_url(sys.argv[1], decode_responses=True) as client:
        redis_utils._redis_pool = client
        settings.redisvl_vector_dimensions = 3
        settings.embedding_model = 'test-fixed-3d'
        settings.compact_semantic_duplicates = False
        db = RedisVLMemoryVectorDatabase(
            AsyncSearchIndex.from_dict(_build_redis_schema(), redis_client=client), FixedEmbeddings())
        async def embed(texts):
            return [[1.0, 0.0, 0.0] for _ in texts]
        db.embeddings.aembed_documents = embed
        async def get_db():
            return db
        class SilentTasks:
            def add_task(self, *args, **kwargs): pass
        long_term_memory.get_memory_vector_db = get_db
        long_term_memory.get_background_tasks = lambda: SilentTasks()
        class Strategy:
            async def extract_memories(self, conversation, context):
                await client.incr('probe:process-extraction:attempts')
                if sys.argv[2] == 'block' and sys.argv[3] == 'prepare':
                    await client.set('probe:process-extraction:entered', 'yes')
                    await asyncio.Event().wait()
                return [{'text': 'derived memory'}] if sys.argv[3] == 'marked' else []
        async def blocked_publish(records, **kwargs):
            await client.set('probe:process-extraction:entered', 'yes')
            await asyncio.Event().wait()
        if sys.argv[2] == 'block' and sys.argv[3] == 'marked':
            long_term_memory.index_long_term_memories = blocked_publish
        if sys.argv[2] == 'block' and sys.argv[3] == 'published':
            original_finalize = long_term_memory.finalize_published_extraction_batch
            async def blocked_cleanup(*args, **kwargs):
                await client.set('probe:process-extraction:entered', 'yes')
                await asyncio.Event().wait()
                return await original_finalize(*args, **kwargs)
            long_term_memory.finalize_published_extraction_batch = blocked_cleanup
        memory_strategies.get_memory_strategy = lambda *args, **kwargs: Strategy()
        async with Docket(name='process-extraction', url=sys.argv[1]) as docket:
            docket.register(long_term_memory.run_delayed_extraction)
            async with Worker(
                docket, name='process-' + sys.argv[2], schedule_automatic_tasks=False,
                redelivery_timeout=(timedelta(seconds=30) if sys.argv[2] == 'block'
                                    else timedelta(seconds=1)),
                minimum_check_interval=timedelta(milliseconds=20),
                scheduling_resolution=timedelta(milliseconds=20),
            ) as worker:
                if sys.argv[2] == 'block':
                    await worker.run_forever()
                else:
                    await asyncio.wait_for(worker.run_until_finished(), 10)

asyncio.run(main())
"""
    async with (
        Redis.from_url(url, decode_responses=True) as client,
        Docket(name="process-extraction", url=url) as docket,
    ):
        await ensure_working_memory_index(client)
        source = WorkingMemory(
            session_id="shared",
            namespace="orchard",
            user_id="alice",
            messages=[
                MemoryMessage(role="user", content="recover me", discrete_memory_extracted="f")
            ],
        )
        await set_working_memory(source, client)
        await long_term_memory.schedule_trailing_extraction("shared", "orchard", "alice", client)
        keys = await client.zrange("process-extraction:queue", 0, -1)
        assert len(keys) == 1
        docket.register(long_term_memory.run_delayed_extraction)
        processes = []
        log_path = tmp_path / "process-extraction.log"
        with log_path.open("w") as log:
            try:
                first = subprocess.Popen(
                    [sys.executable, "-c", child_code, url, "block", crash_phase],
                    cwd=Path(__file__).parent,
                    stdout=log,
                    stderr=log,
                )
                processes.append(first)
                deadline = asyncio.get_running_loop().time() + 10
                while not await client.exists("probe:process-extraction:entered"):
                    assert first.poll() is None, log_path.read_text()
                    assert asyncio.get_running_loop().time() < deadline, log_path.read_text()
                    await asyncio.sleep(0.05)
                assert (await docket.get_execution(keys[0])).state == ExecutionState.RUNNING
                assert (await client.xpending(docket.stream_key, docket.worker_group_name))[
                    "pending"
                ] == 1
                active_leases = [key async for key in client.scan_iter(match="model_execution:*")]
                assert len(active_leases) == 1 and await client.pttl(active_leases[0]) > 0
                first.kill()
                first.wait(timeout=5)
                second = subprocess.Popen(
                    [sys.executable, "-c", child_code, url, "recover", crash_phase],
                    cwd=Path(__file__).parent,
                    stdout=log,
                    stderr=log,
                )
                processes.append(second)
                deadline = asyncio.get_running_loop().time() + 15
                while second.poll() is None:
                    assert asyncio.get_running_loop().time() < deadline, log_path.read_text()
                    await asyncio.sleep(0.05)
                assert second.returncode == 0, log_path.read_text()
                assert not await client.exists(active_leases[0])
                completed = await docket.get_execution(keys[0])
                memory = await get_working_memory("shared", "alice", "orchard", client)
                pending = (
                    f"extraction_pending:{Keys.working_memory_key('shared', 'alice', 'orchard')}"
                )
                assert memory.messages[0].discrete_memory_extracted == "t"
                if crash_phase == "prepare":
                    assert completed.state == ExecutionState.COMPLETED
                    assert await completed.get_result(timeout=timedelta(seconds=2)) == 0
                    assert await client.get("probe:process-extraction:attempts") == "2"
                    assert memory.messages[0].extraction_generation is None
                    assert not await client.exists(pending)
                elif crash_phase == "published":
                    assert completed.state == ExecutionState.COMPLETED
                    assert await completed.get_result(timeout=timedelta(seconds=2)) == 0
                    assert await client.get("probe:process-extraction:attempts") == "1"
                    assert memory.messages[0].extraction_generation is None
                    assert not await client.exists(pending)
                    source_key = Keys.working_memory_key("shared", "alice", "orchard")
                    batches = [
                        key
                        async for key in client.scan_iter(match=f"extraction_batch:{source_key}:*")
                    ]
                    assert len(batches) == 1
                    assert json.loads(await client.get(batches[0]))["state"] == "finalized"
                else:
                    assert completed.state == ExecutionState.COMPLETED
                    assert await completed.get_result(timeout=timedelta(seconds=2)) == 1
                    assert await client.get("probe:process-extraction:attempts") == "1"
                    assert memory.messages[0].extraction_generation is None
                    source_key = Keys.working_memory_key("shared", "alice", "orchard")
                    batches = [
                        key
                        async for key in client.scan_iter(match=f"extraction_batch:{source_key}:*")
                    ]
                    assert len(batches) == 1
                    batch = json.loads(await client.get(batches[0]))
                    assert batch["state"] == "finalized"
                    assert batch["message_ids"] == [memory.messages[0].id]
                    assert batch["memories"][0]["text"] == "derived memory"
                    assert batch["schema_version"] == 2
                    assert batch["publication_generations"] == [["0"] * 4]
                    target = batch["record_receipts"]["0"]["target_key"]
                    assert await client.hget(target, "text") == "derived memory"
                    assert not await client.exists(pending)

                assert (await client.xpending(docket.stream_key, docket.worker_group_name))[
                    "pending"
                ] == 0
                assert await client.xlen(docket.stream_key) == 0
            finally:
                for process in processes:
                    if process.poll() is None:
                        process.kill()
                    process.wait(timeout=5)


async def test_api_put_normalizes_client_extraction_state_and_preserves_server_state(
    redis_client, monkeypatch
):
    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    monkeypatch.setattr(settings, "long_term_memory", False)

    async def no_summary(memory, **kwargs):
        return memory

    monkeypatch.setattr("agent_memory_server.api._summarize_working_memory", no_summary)
    await ensure_working_memory_index(redis_client)
    message = MemoryMessage(
        role="user",
        content="new input",
        discrete_memory_extracted="t",
        extraction_generation="forged",
        persisted_at=None,
    )
    payload = UpdateWorkingMemory(namespace="orchard", user_id="alice", messages=[message])
    response = await put_working_memory_core("shared", payload, None)
    assert response.messages[0].discrete_memory_extracted == "f"
    assert response.messages[0].extraction_generation is None
    stored = await get_working_memory("shared", "alice", "orchard", redis_client)
    stored.messages[0].discrete_memory_extracted = "t"
    stored.messages[0].extraction_generation = "server-owned"
    stored.messages[0].persisted_at = stored.messages[0].created_at
    await set_working_memory(stored, redis_client)
    forged = stored.messages[0].model_copy(deep=True)
    forged.discrete_memory_extracted = "f"
    forged.extraction_generation = "another-forgery"
    forged.persisted_at = None
    response = await put_working_memory_core(
        "shared",
        UpdateWorkingMemory(
            version=stored.version,
            namespace="orchard",
            user_id="alice",
            messages=[forged],
        ),
        None,
    )
    assert response.messages[0].discrete_memory_extracted == "t"
    assert response.messages[0].extraction_generation == "server-owned"
    assert response.messages[0].persisted_at == stored.messages[0].persisted_at
    # Finish the server-owned operation before allowing a client edit.
    finished = await get_working_memory("shared", "alice", "orchard", redis_client)
    finished.messages[0].extraction_generation = None
    await set_working_memory(finished, redis_client)
    edited = finished.messages[0].model_copy(deep=True)
    edited.content = "changed input"
    response = await put_working_memory_core(
        "shared",
        UpdateWorkingMemory(
            version=finished.version,
            namespace="orchard",
            user_id="alice",
            messages=[edited],
        ),
        None,
    )
    assert response.messages[0].discrete_memory_extracted == "f"
    assert response.messages[0].extraction_generation is None
    assert response.messages[0].persisted_at is None
    with pytest.raises(HTTPException) as duplicate:
        await put_working_memory_core(
            "shared",
            UpdateWorkingMemory(
                version=response.version,
                namespace="orchard",
                user_id="alice",
                messages=[edited, edited],
            ),
            None,
        )
    assert duplicate.value.status_code == 400
    assert (
        await get_working_memory("shared", "alice", "orchard", redis_client)
    ).version == response.version


async def test_summarized_put_keeps_context_in_actual_extraction_input(redis_client, monkeypatch):
    from types import SimpleNamespace
    from agent_memory_server import api

    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    monkeypatch.setattr(settings, "long_term_memory", False)
    monkeypatch.setattr(settings, "enable_working_memory_summarization", True)
    monkeypatch.setattr(api, "_calculate_messages_token_count", lambda messages: 30)
    monkeypatch.setattr(api, "_count_text_tokens", lambda text: 10)
    monkeypatch.setattr(
        api.LLMClient,
        "get_model_config",
        lambda model: SimpleNamespace(max_tokens=4096),
    )
    summaries = []

    async def summarize(model, context, messages):
        summaries.append(messages)
        return "Alice planted pear trees in the orchard.", 7

    monkeypatch.setattr(api, "_incremental_summary", summarize)
    await ensure_working_memory_index(redis_client)
    messages = [
        MemoryMessage(
            role="user",
            content=text,
            discrete_memory_extracted="t",
            extraction_generation="forged",
        )
        for text in [
            "I planted pear trees",
            "They are in my orchard",
            "How should I water them?",
        ]
    ]
    response = await put_working_memory_core(
        "shared",
        UpdateWorkingMemory(namespace="orchard", user_id="alice", messages=messages),
        None,
        context_window_max=20,
    )
    assert summaries == [["user: I planted pear trees", "user: They are in my orchard"]]
    stored = await get_working_memory("shared", "alice", "orchard", redis_client)
    assert len(stored.messages) == 1
    assert stored.context == response.context == "Alice planted pear trees in the orchard."
    assert stored.messages[0].discrete_memory_extracted == "f"
    assert stored.messages[0].extraction_generation is None
    calls = []

    class Strategy:
        async def extract_memories(self, conversation, context):
            calls.append((conversation, context))
            return []

    monkeypatch.setattr(
        "agent_memory_server.memory_strategies.get_memory_strategy",
        lambda *args, **kwargs: Strategy(),
    )
    assert (
        await long_term_memory.extract_memories_from_session_thread("shared", "orchard", "alice")
        == []
    )
    conversation, context = calls[0]
    assert stored.context in conversation
    assert "How should I water them?" in conversation
    assert context["working_memory_context"] == stored.context
    assert context["source_message_ids"] == [stored.messages[0].id]
    modified = stored.model_copy(deep=True)
    modified.context = "Alice planted apple trees."
    _, changed_context = long_term_memory._build_thread_extraction_context(modified)
    assert changed_context["source_message_fingerprint"] != context["source_message_fingerprint"]


async def test_put_defers_summary_without_discarding_unfinished_extraction(
    redis_client, monkeypatch
):
    from agent_memory_server import api

    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    monkeypatch.setattr(settings, "long_term_memory", False)
    monkeypatch.setattr(settings, "enable_working_memory_summarization", True)
    monkeypatch.setattr(api, "_calculate_messages_token_count", lambda messages: 100)

    async def unexpected_summary(*args, **kwargs):
        pytest.fail("Summary must not discard pending extraction source")

    monkeypatch.setattr(api, "_incremental_summary", unexpected_summary)
    await ensure_working_memory_index(redis_client)
    source = WorkingMemory(
        session_id="shared",
        namespace="orchard",
        user_id="alice",
        context="previous summary",
        messages=[
            MemoryMessage(
                role="user",
                content="pending publication",
                discrete_memory_extracted="t",
                extraction_generation="server-owned",
            ),
            MemoryMessage(role="user", content="recent input", discrete_memory_extracted="f"),
        ],
    )
    await set_working_memory(source, redis_client)
    response = await put_working_memory_core(
        "shared",
        UpdateWorkingMemory(
            version=source.version,
            namespace="orchard",
            user_id="alice",
            messages=source.messages,
            context=source.context,
        ),
        None,
        context_window_max=20,
    )
    assert len(response.messages) == 2
    assert response.context == "previous summary"
    actual = await get_working_memory("shared", "alice", "orchard", redis_client)
    assert actual.messages[0].content == "pending publication"
    assert actual.messages[0].extraction_generation == "server-owned"
    assert actual.messages[1].discrete_memory_extracted == "f"
    assert actual.version == response.version > source.version


@pytest.mark.parametrize("change", ["remove", "content", "role", "created_at"])
async def test_put_rejects_edit_or_removal_of_unfinished_server_message(
    redis_client, monkeypatch, change
):
    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    monkeypatch.setattr(settings, "long_term_memory", False)
    await ensure_working_memory_index(redis_client)
    source = WorkingMemory(
        session_id="shared",
        namespace="orchard",
        user_id="alice",
        messages=[
            MemoryMessage(
                role="user",
                content="publishing",
                discrete_memory_extracted="t",
                extraction_generation="server-owned",
            )
        ],
    )
    await set_working_memory(source, redis_client)
    key = Keys.working_memory_key("shared", "alice", "orchard")
    before = await redis_client.json().get(key)
    edited = source.messages[0].model_copy(deep=True)
    if change == "content":
        edited.content = "changed"
    elif change == "role":
        edited.role = "assistant"
    elif change == "created_at":
        edited.created_at += timedelta(seconds=1)
    with pytest.raises(HTTPException) as conflict:
        await put_working_memory_core(
            "shared",
            UpdateWorkingMemory(
                version=source.version,
                namespace="orchard",
                user_id="alice",
                messages=[] if change == "remove" else [edited],
            ),
            None,
        )
    assert conflict.value.status_code == 409
    assert "unfinished extraction" in conflict.value.detail
    assert await redis_client.json().get(key) == before
    appended = source.messages + [MemoryMessage(role="user", content="new input")]
    response = await put_working_memory_core(
        "shared",
        UpdateWorkingMemory(
            version=source.version,
            namespace="orchard",
            user_id="alice",
            messages=appended,
        ),
        None,
    )
    assert len(response.messages) == 2
    assert response.messages[0].extraction_generation == "server-owned"
    assert response.messages[1].discrete_memory_extracted == "f"


async def test_extraction_batch_cas_conflict_and_collision_preserve_storage(
    redis_client,
):
    from ulid import ULID

    await ensure_working_memory_index(redis_client)
    source = WorkingMemory(
        session_id="shared",
        namespace="orchard",
        user_id="alice",
        messages=[MemoryMessage(role="user", content="input")],
    )
    await set_working_memory(source, redis_client)
    stale = source.model_copy(deep=True)
    generation = str(ULID())
    source.messages[0].extraction_generation = generation
    source.messages[0].discrete_memory_extracted = "t"
    record = MemoryRecord(id="derived", text="result", namespace="orchard", user_id="alice")
    record._publication_generation = ["0"] * 4
    await set_working_memory(source, redis_client, extraction_batch=(generation, [record]))
    key = Keys.working_memory_key("shared", "alice", "orchard")
    batch_key = f"extraction_batch:{key}:{generation}"
    before = await redis_client.json().get(key)
    serialized = await redis_client.get(batch_key)
    batch = json.loads(serialized)
    assert batch["source_revision"] == source.version
    assert batch["source_incarnation"] == str(stale.version)
    assert batch["memories"][0]["id"] == "derived"
    with pytest.raises(ValueError, match="already exists"):
        await set_working_memory(source, redis_client, extraction_batch=(generation, [record]))
    assert await redis_client.json().get(key) == before
    assert await redis_client.get(batch_key) == serialized
    stale_generation = str(ULID())
    with pytest.raises(WorkingMemoryConflictError):
        await set_working_memory(stale, redis_client, extraction_batch=(stale_generation, [record]))
    assert not await redis_client.exists(f"extraction_batch:{key}:{stale_generation}")
    assert await redis_client.json().get(key) == before


async def test_prepared_batch_preserves_versions_when_target_deleted_before_publish(
    redis_client, monkeypatch
):
    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    monkeypatch.setattr(settings, "redisvl_vector_dimensions", 3)
    monkeypatch.setattr(settings, "embedding_model", "test-fixed-3d")
    await ensure_working_memory_index(redis_client)
    db = RedisVLMemoryVectorDatabase(
        AsyncSearchIndex.from_dict(_build_redis_schema(), redis_client=redis_client),
        FixedEmbeddings(),
    )
    source = WorkingMemory(
        session_id="shared",
        namespace="orchard",
        user_id="alice",
        messages=[MemoryMessage(role="user", content="input", discrete_memory_extracted="f")],
    )
    await set_working_memory(source, redis_client)
    record = MemoryRecord(id="derived", text="result", namespace="orchard", user_id="alice")

    async def extract(**kwargs):
        return [record]

    original_index = long_term_memory.index_long_term_memories

    async def delete_before_publish(records, **kwargs):
        assert kwargs["publication_generations"] == [["0"] * 4]
        assert await db.delete_memories(["derived"], "orchard", "alice", exact_scope=True) == 0
        await original_index(records, **kwargs)

    monkeypatch.setattr(long_term_memory, "extract_memories_from_session_thread", extract)
    monkeypatch.setattr(long_term_memory, "index_long_term_memories", delete_before_publish)
    with pytest.raises(ValueError, match="deleted before task execution"):
        await long_term_memory.run_delayed_extraction("shared", "orchard", "alice")
    source_key = Keys.working_memory_key("shared", "alice", "orchard")
    keys = [key async for key in redis_client.scan_iter(match=f"extraction_batch:{source_key}:*")]
    assert len(keys) == 1
    batch = json.loads(await redis_client.get(keys[0]))
    assert batch["publication_generations"] == [["0"] * 4]
    assert not await redis_client.exists(Keys.memory_key("derived", "orchard", "alice"))
    with pytest.raises(ValueError, match="deleted before task execution"):
        await original_index(
            [MemoryRecord.model_validate(batch["memories"][0])],
            redis_client=redis_client,
            publication_generations=batch["publication_generations"],
        )
    assert not await redis_client.exists(Keys.memory_key("derived", "orchard", "alice"))


@pytest.mark.parametrize("global_delete", [False, True])
async def test_summary_deleted_during_model_is_not_rebound_to_new_generation(
    redis_client, monkeypatch, global_delete
):
    from agent_memory_server.models import MemoryStrategyConfig

    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    monkeypatch.setattr(settings, "redisvl_vector_dimensions", 3)
    monkeypatch.setattr(settings, "embedding_model", "test-fixed-3d")
    await ensure_working_memory_index(redis_client)
    db = RedisVLMemoryVectorDatabase(
        AsyncSearchIndex.from_dict(_build_redis_schema(), redis_client=redis_client),
        FixedEmbeddings(),
    )
    source = WorkingMemory(
        session_id="shared",
        namespace="orchard",
        user_id="alice",
        long_term_memory_strategy=MemoryStrategyConfig(strategy="summary"),
        messages=[MemoryMessage(role="user", content="input", discrete_memory_extracted="f")],
    )
    await set_working_memory(source, redis_client)
    entered, release = asyncio.Event(), asyncio.Event()

    class Strategy:
        async def extract_memories(self, conversation, context):
            entered.set()
            await release.wait()
            return [{"text": "late summary"}]

    async def no_existing(**kwargs):
        return False

    monkeypatch.setattr(long_term_memory, "_existing_summary_matches_source", no_existing)
    monkeypatch.setattr(
        "agent_memory_server.memory_strategies.get_memory_strategy",
        lambda *args, **kwargs: Strategy(),
    )
    task = asyncio.create_task(
        long_term_memory.run_delayed_extraction("shared", "orchard", "alice")
    )
    summary_id = long_term_memory._thread_summary_memory_id(
        namespace="orchard", user_id="alice", session_id="shared"
    )
    try:
        await asyncio.wait_for(entered.wait(), 5)
        if global_delete:
            assert await db.delete_memories([summary_id]) == 0
        else:
            assert await db.delete_memories([summary_id], "orchard", "alice", exact_scope=True) == 0
        release.set()
        with pytest.raises(ValueError, match="deleted before task execution"):
            await asyncio.wait_for(task, 5)
        assert not await redis_client.exists(Keys.memory_key(summary_id, "orchard", "alice"))
        key = Keys.working_memory_key("shared", "alice", "orchard")
        batches = [
            batch async for batch in redis_client.scan_iter(match=f"extraction_batch:{key}:*")
        ]
        assert len(batches) == 1
        batch = json.loads(await redis_client.get(batches[0]))
        assert batch["publication_generations"] == [["0"] * 4]
        assert batch["memories"][0]["id"] == summary_id
        actual = await get_working_memory("shared", "alice", "orchard", redis_client)
        assert actual.messages[0].discrete_memory_extracted == "f"
    finally:
        release.set()
        await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), 5)


async def test_published_batch_receipt_is_idempotent_and_preserves_versions(
    redis_client,
):
    from ulid import ULID

    source = WorkingMemory(session_id="shared", namespace="orchard", user_id="alice")
    await set_working_memory(source, redis_client)
    generation = str(ULID())
    source.messages = [
        MemoryMessage(
            role="user",
            content="input",
            discrete_memory_extracted="t",
            extraction_generation=generation,
        )
    ]
    await set_working_memory(source, redis_client, extraction_batch=(generation, []))
    key = Keys.working_memory_key("shared", "alice", "orchard")
    batch_key = f"extraction_batch:{key}:{generation}"
    before = json.loads(await redis_client.get(batch_key))
    await long_term_memory.mark_extraction_batch_published(redis_client, key, generation)
    published = json.loads(await redis_client.get(batch_key))
    assert published["state"] == "published"
    assert published["published_at"]
    assert {k: v for k, v in published.items() if k not in ("state", "published_at")} == {
        k: v for k, v in before.items() if k != "state"
    }
    await long_term_memory.mark_extraction_batch_published(redis_client, key, generation)
    assert json.loads(await redis_client.get(batch_key)) == published
    with pytest.raises(ValueError, match="Missing or incompatible"):
        await long_term_memory.mark_extraction_batch_published(redis_client, key, str(ULID()))
    assert json.loads(await redis_client.get(batch_key)) == published


async def test_publication_receipt_failure_does_not_restore_extraction_eligibility(
    redis_client, monkeypatch
):
    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    await ensure_working_memory_index(redis_client)
    source = WorkingMemory(
        session_id="shared",
        namespace="orchard",
        user_id="alice",
        messages=[MemoryMessage(role="user", content="input")],
    )
    await set_working_memory(source, redis_client)
    original_eval = redis_client.eval

    async def fail_receipt(script, numkeys, *args):
        if "batch.state = 'published'" in script:
            raise ConnectionError("receipt unavailable")
        return await original_eval(script, numkeys, *args)

    async def extract(**kwargs):
        return []

    monkeypatch.setattr(redis_client, "eval", fail_receipt)
    monkeypatch.setattr(long_term_memory, "extract_memories_from_session_thread", extract)
    with pytest.raises(ConnectionError, match="receipt unavailable"):
        await long_term_memory.run_delayed_extraction("shared", "orchard", "alice")
    actual = await get_working_memory("shared", "alice", "orchard", redis_client)
    assert actual.messages[0].discrete_memory_extracted == "t"
    assert actual.messages[0].extraction_generation
    key = Keys.working_memory_key("shared", "alice", "orchard")
    batches = [batch async for batch in redis_client.scan_iter(match=f"extraction_batch:{key}:*")]
    assert len(batches) == 1
    assert json.loads(await redis_client.get(batches[0]))["state"] == "prepared"


@pytest.mark.parametrize("recreate", [False, True])
async def test_published_cleanup_cas_preserves_updated_or_recreated_source(redis_client, recreate):
    from ulid import ULID

    generation = str(ULID())
    source = WorkingMemory(
        session_id="shared",
        namespace="orchard",
        user_id="alice",
        ttl_seconds=30,
        messages=[
            MemoryMessage(
                role="user",
                content="input",
                discrete_memory_extracted="t",
                extraction_generation=generation,
            )
        ],
    )
    await set_working_memory(source, redis_client, extraction_batch=(generation, []))
    key = Keys.working_memory_key("shared", "alice", "orchard")
    batch_key = f"extraction_batch:{key}:{generation}"
    await long_term_memory.mark_extraction_batch_published(redis_client, key, generation)
    published = await redis_client.get(batch_key)
    old_revision = source.version
    if recreate:
        await delete_working_memory("shared", "alice", "orchard", redis_client)
        source = WorkingMemory(
            session_id="shared",
            namespace="orchard",
            user_id="alice",
            data={"replacement": True},
        )
    else:
        source.context = "concurrent context"
    await set_working_memory(source, redis_client)
    before = await redis_client.json().get(key)
    with pytest.raises(WorkingMemoryConflictError):
        await long_term_memory.finalize_published_extraction_batch(
            redis_client, key, generation, old_revision
        )
    assert await redis_client.json().get(key) == before
    assert await redis_client.get(batch_key) == published
    if recreate:
        with pytest.raises(RuntimeError, match="recovery required"):
            await long_term_memory.finalize_published_extraction_batch(
                redis_client, key, generation, source.version
            )
        assert await redis_client.json().get(key) == before
        assert await redis_client.get(batch_key) == published
    else:
        count, revision = await long_term_memory.finalize_published_extraction_batch(
            redis_client, key, generation, source.version
        )
        assert count == 0 and revision > source.version
        actual = await redis_client.json().get(key)
        assert actual["context"] == "concurrent context"
        assert actual["messages"][0]["discrete_memory_extracted"] == "t"
        assert actual["messages"][0]["extraction_generation"] is None
        assert 0 < await redis_client.ttl(key) <= 30
        assert json.loads(await redis_client.get(batch_key))["state"] == "finalized"


async def test_normal_extraction_records_finalized_batch_without_recovery(
    redis_client, monkeypatch
):
    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    await ensure_working_memory_index(redis_client)
    source = WorkingMemory(
        session_id="shared",
        namespace="orchard",
        user_id="alice",
        ttl_seconds=30,
        messages=[MemoryMessage(role="user", content="input")],
    )
    await set_working_memory(source, redis_client)

    async def extract(**kwargs):
        return []

    monkeypatch.setattr(long_term_memory, "extract_memories_from_session_thread", extract)
    assert await long_term_memory.run_delayed_extraction("shared", "orchard", "alice") == 0
    key = Keys.working_memory_key("shared", "alice", "orchard")
    batches = [batch async for batch in redis_client.scan_iter(match=f"extraction_batch:{key}:*")]
    assert len(batches) == 1
    batch = json.loads(await redis_client.get(batches[0]))
    assert batch["state"] == "finalized"
    assert batch["published_at"] and batch["finalized_at"]
    actual = await get_working_memory("shared", "alice", "orchard", redis_client)
    assert actual.messages[0].discrete_memory_extracted == "t"
    assert actual.messages[0].extraction_generation is None
    assert actual.version > batch["source_revision"]
    assert 0 < await redis_client.ttl(key) <= 30


async def test_finalized_cleanup_response_loss_retry_does_not_mutate_source(
    redis_client,
):
    from ulid import ULID

    generation = str(ULID())
    source = WorkingMemory(
        session_id="shared",
        namespace="orchard",
        user_id="alice",
        ttl_seconds=30,
        messages=[
            MemoryMessage(
                role="user",
                content="input",
                discrete_memory_extracted="t",
                extraction_generation=generation,
            )
        ],
    )
    await set_working_memory(source, redis_client, extraction_batch=(generation, []))
    key = Keys.working_memory_key("shared", "alice", "orchard")
    batch_key = f"extraction_batch:{key}:{generation}"
    await long_term_memory.mark_extraction_batch_published(redis_client, key, generation)
    expected = source.version
    first = await long_term_memory.finalize_published_extraction_batch(
        redis_client, key, generation, expected
    )
    before = await redis_client.json().get(key)
    receipt = await redis_client.get(batch_key)
    ttl = await redis_client.pttl(key)
    assert (
        await long_term_memory.finalize_published_extraction_batch(
            redis_client, key, generation, expected
        )
        == first
    )
    assert await redis_client.json().get(key) == before
    assert await redis_client.get(batch_key) == receipt
    assert 0 < await redis_client.pttl(key) <= ttl
    await delete_working_memory("shared", "alice", "orchard", redis_client)
    replacement = WorkingMemory(session_id="shared", namespace="orchard", user_id="alice")
    await set_working_memory(replacement, redis_client)
    with pytest.raises(RuntimeError, match="recovery required"):
        await long_term_memory.finalize_published_extraction_batch(
            redis_client, key, generation, replacement.version
        )
    assert await redis_client.get(batch_key) == receipt


@pytest.mark.parametrize("published", [True, False])
@pytest.mark.parametrize("change_context", [False, True])
async def test_mixed_pending_generation_resolved_before_new_model(
    redis_client, monkeypatch, published, change_context
):
    from ulid import ULID

    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    await ensure_working_memory_index(redis_client)
    generation = str(ULID())
    source = WorkingMemory(
        session_id="shared",
        namespace="orchard",
        user_id="alice",
        messages=[
            MemoryMessage(
                role="user",
                content="old",
                discrete_memory_extracted="t",
                extraction_generation=generation,
            )
        ],
    )
    await set_working_memory(source, redis_client, extraction_batch=(generation, []))
    key = Keys.working_memory_key("shared", "alice", "orchard")
    batch_key = f"extraction_batch:{key}:{generation}"
    if published:
        await long_term_memory.mark_extraction_batch_published(redis_client, key, generation)
    if change_context:
        source.context = "changed extraction input"
    source.messages.append(MemoryMessage(role="user", content="fresh"))
    await set_working_memory(source, redis_client)
    before = await redis_client.json().get(key)
    calls = []

    async def extract(**kwargs):
        actual = await redis_client.json().get(key)
        assert actual["messages"][0]["extraction_generation"] is None
        assert actual["messages"][1]["discrete_memory_extracted"] == "f"
        assert json.loads(await redis_client.get(batch_key))["state"] == "finalized"
        calls.append(kwargs)
        return []

    monkeypatch.setattr(long_term_memory, "extract_memories_from_session_thread", extract)
    if not published and change_context:
        with pytest.raises(RuntimeError, match="recovery required"):
            await long_term_memory.run_delayed_extraction("shared", "orchard", "alice")
        assert calls == []
        assert await redis_client.json().get(key) == before
        assert json.loads(await redis_client.get(batch_key))["state"] == "prepared"
    else:
        assert await long_term_memory.run_delayed_extraction("shared", "orchard", "alice") == 0
        assert len(calls) == 1
        actual = await redis_client.json().get(key)
        assert [message["content"] for message in actual["messages"]] == [
            "old",
            "fresh",
        ]
        assert all(
            message["discrete_memory_extracted"] == "t" and message["extraction_generation"] is None
            for message in actual["messages"]
        )
        batches = [
            json.loads(await redis_client.get(batch))
            async for batch in redis_client.scan_iter(match=f"extraction_batch:{key}:*")
        ]
        assert len(batches) == 2 and all(batch["state"] == "finalized" for batch in batches)


@pytest.mark.parametrize("failure", ["before", "response_loss", "cas_exhaustion"])
async def test_publication_cleanup_failure_retains_pending_and_retry_does_not_reextract(
    redis_client, monkeypatch, failure
):
    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    await ensure_working_memory_index(redis_client)
    source = WorkingMemory(
        session_id="shared",
        namespace="orchard",
        user_id="alice",
        messages=[MemoryMessage(role="user", content="input")],
    )
    await set_working_memory(source, redis_client)
    key = Keys.working_memory_key("shared", "alice", "orchard")
    pending_key = f"extraction_pending:{key}"
    timestamp = "2099-01-01T00:00:00+00:00"
    await redis_client.set(pending_key, timestamp)
    model_calls = []

    async def extract(**kwargs):
        model_calls.append(kwargs)
        return []

    monkeypatch.setattr(long_term_memory, "extract_memories_from_session_thread", extract)
    original_eval = redis_client.eval
    attempts = []

    async def fail_cleanup(script, numkeys, *args):
        if numkeys == 3 and "batch.finalized_from_revision" in script:
            attempts.append(args)
            if failure == "cas_exhaustion":
                latest = await get_working_memory("shared", "alice", "orchard", redis_client)
                latest.context = f"concurrent-{len(attempts)}"
                await set_working_memory(latest, redis_client)
                return await original_eval(script, numkeys, *args)
            if failure == "response_loss":
                await original_eval(script, numkeys, *args)
            raise ConnectionError("cleanup transport failure")
        return await original_eval(script, numkeys, *args)

    monkeypatch.setattr(redis_client, "eval", fail_cleanup)
    expected_error = WorkingMemoryConflictError if failure == "cas_exhaustion" else ConnectionError
    with pytest.raises(expected_error):
        await long_term_memory.run_delayed_extraction(
            "shared",
            "orchard",
            "alice",
            timestamp,
            source_incarnation=str(source.version),
        )
    assert len(attempts) == (5 if failure == "cas_exhaustion" else 1)
    assert await redis_client.get(pending_key) == timestamp
    assert not await redis_client.exists(f"extraction_debounce:{key}")
    batches = [batch async for batch in redis_client.scan_iter(match=f"extraction_batch:{key}:*")]
    assert len(batches) == 1
    batch = json.loads(await redis_client.get(batches[0]))
    assert batch["state"] == ("finalized" if failure == "response_loss" else "published")
    monkeypatch.setattr(redis_client, "eval", original_eval)
    assert (
        await long_term_memory.run_delayed_extraction(
            "shared",
            "orchard",
            "alice",
            timestamp,
            source_incarnation=str(source.version),
        )
        == 0
    )
    assert len(model_calls) == 1
    assert not await redis_client.exists(pending_key)
    assert json.loads(await redis_client.get(batches[0]))["state"] == "finalized"
    actual = await get_working_memory("shared", "alice", "orchard", redis_client)
    assert actual.messages[0].discrete_memory_extracted == "t"
    assert actual.messages[0].extraction_generation is None
    if failure == "cas_exhaustion":
        assert actual.context == "concurrent-5"


async def test_record_publication_receipt_is_atomic_with_hash_on_response_loss(
    redis_client, monkeypatch
):
    from ulid import ULID

    monkeypatch.setattr(settings, "redisvl_vector_dimensions", 3)
    monkeypatch.setattr(settings, "embedding_model", "test-fixed-3d")
    db = RedisVLMemoryVectorDatabase(
        AsyncSearchIndex.from_dict(_build_redis_schema(), redis_client=redis_client),
        FixedEmbeddings(),
    )

    async def embed(texts):
        return [[1.0, 0.0, 0.0] for _ in texts]

    monkeypatch.setattr(db.embeddings, "aembed_documents", embed)
    generation = str(ULID())
    source = WorkingMemory(
        session_id="shared",
        namespace="orchard",
        user_id="alice",
        messages=[
            MemoryMessage(
                role="user",
                content="input",
                discrete_memory_extracted="t",
                extraction_generation=generation,
            )
        ],
    )
    record = MemoryRecord(id="derived", text="result", namespace="orchard", user_id="alice")
    record._publication_generation = ["0"] * 4
    await set_working_memory(source, redis_client, extraction_batch=(generation, [record]))
    key = Keys.working_memory_key("shared", "alice", "orchard")
    batch_key = f"extraction_batch:{key}:{generation}"
    record._source_session = (
        key,
        Keys.working_memory_revision_key("shared", "alice", "orchard"),
        source.version,
    )
    record._extraction_batch = (batch_key, 0)
    target_key = Keys.memory_key("derived", "orchard", "alice")
    original_eval = redis_client.eval

    async def lose_response(script, numkeys, *args):
        result = await original_eval(script, numkeys, *args)
        if "batch.record_receipts['0']" in script:
            assert result == 1
            raise ConnectionError("publication response lost")
        return result

    monkeypatch.setattr(redis_client, "eval", lose_response)
    with pytest.raises(ConnectionError, match="publication response lost"):
        await db.add_memories([record])
    assert await redis_client.hget(target_key, "id_") == "derived"
    batch = json.loads(await redis_client.get(batch_key))
    assert batch["state"] == "prepared"
    assert batch["record_receipts"] == {"0": {"target_key": target_key, "state": "published"}}
    assert not [stage async for stage in redis_client.scan_iter(match="memory_stage:*")]
    monkeypatch.setattr(redis_client, "eval", original_eval)
    bad_record = record.model_copy(deep=True)
    bad_record._extraction_batch = (batch_key, 1)
    with pytest.raises(ValueError, match="does not match publication"):
        await db.add_memories([bad_record])
    assert json.loads(await redis_client.get(batch_key)) == batch

    # Replay of the same durable result must not overwrite a later update.
    await redis_client.hset(target_key, mapping={"text": "later update", "access_count": "7"})
    before = await redis_client.execute_command("HGETALL", target_key, NEVER_DECODE=True)
    assert await db.add_memories([record]) == ["derived"]
    assert await redis_client.execute_command("HGETALL", target_key, NEVER_DECODE=True) == before
    assert json.loads(await redis_client.get(batch_key)) == batch
    assert not [stage async for stage in redis_client.scan_iter(match="memory_stage:*")]
    await db.delete_memories(["derived"], "orchard", "alice", exact_scope=True)
    with pytest.raises(ValueError, match="deleted before publication"):
        await db.add_memories([record])
    assert not await redis_client.exists(target_key)
    assert json.loads(await redis_client.get(batch_key)) == batch


@pytest.mark.parametrize("response_loss", [False, True])
async def test_partial_hash_publication_keeps_owned_messages_and_prevents_reextraction(
    redis_client, monkeypatch, response_loss
):
    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    monkeypatch.setattr(settings, "redisvl_vector_dimensions", 3)
    monkeypatch.setattr(settings, "embedding_model", "test-fixed-3d")
    await ensure_working_memory_index(redis_client)
    db = RedisVLMemoryVectorDatabase(
        AsyncSearchIndex.from_dict(_build_redis_schema(), redis_client=redis_client),
        FixedEmbeddings(),
    )

    async def embed(texts):
        return [[1.0, 0.0, 0.0] for _ in texts]

    monkeypatch.setattr(db.embeddings, "aembed_documents", embed)
    source = WorkingMemory(
        session_id="shared",
        namespace="orchard",
        user_id="alice",
        messages=[MemoryMessage(role="user", content="input")],
    )
    await set_working_memory(source, redis_client)
    key = Keys.working_memory_key("shared", "alice", "orchard")
    pending_key = f"extraction_pending:{key}"
    timestamp = "2099-01-01T00:00:00+00:00"
    await redis_client.set(pending_key, timestamp)
    model_calls = []

    async def extract(**kwargs):
        model_calls.append(kwargs)
        return [
            MemoryRecord(
                id=f"derived-{i}",
                text=f"result-{i}",
                namespace="orchard",
                user_id="alice",
            )
            for i in range(2)
        ]

    async def index(records, **kwargs):
        await db.add_memories(records)

    original_index = long_term_memory.index_long_term_memories
    original_eval = redis_client.eval

    async def fail_publication(script, numkeys, *args):
        if not response_loss and "batch.record_receipts['1']" in script:
            raise ConnectionError("partial publication failure")
        result = await original_eval(script, numkeys, *args)
        if response_loss and "batch.record_receipts['0']" in script:
            raise ConnectionError("partial publication failure")
        return result

    monkeypatch.setattr(long_term_memory, "extract_memories_from_session_thread", extract)
    monkeypatch.setattr(long_term_memory, "index_long_term_memories", index)
    monkeypatch.setattr(redis_client, "eval", fail_publication)
    with pytest.raises(ConnectionError, match="partial publication failure"):
        await long_term_memory.run_delayed_extraction(
            "shared",
            "orchard",
            "alice",
            timestamp,
            source_incarnation=str(source.version),
        )
    assert await redis_client.exists(Keys.memory_key("derived-0", "orchard", "alice"))
    assert not await redis_client.exists(Keys.memory_key("derived-1", "orchard", "alice"))
    actual = await get_working_memory("shared", "alice", "orchard", redis_client)
    assert actual.messages[0].discrete_memory_extracted == "t"
    generation = actual.messages[0].extraction_generation
    assert generation
    batch = json.loads(await redis_client.get(f"extraction_batch:{key}:{generation}"))
    assert batch["state"] == "prepared" and set(batch["record_receipts"]) == {"0"}
    assert await redis_client.get(pending_key) == timestamp
    monkeypatch.setattr(redis_client, "eval", original_eval)
    monkeypatch.setattr(settings, "compact_semantic_duplicates", False)
    monkeypatch.setattr(settings, "enable_discrete_memory_extraction", False)

    async def get_db():
        return db

    class SilentTasks:
        def add_task(self, *args, **kwargs):
            pass

    monkeypatch.setattr(long_term_memory, "get_memory_vector_db", get_db)
    monkeypatch.setattr(long_term_memory, "get_background_tasks", lambda: SilentTasks())
    monkeypatch.setattr(long_term_memory, "index_long_term_memories", original_index)
    first_key = Keys.memory_key("derived-0", "orchard", "alice")
    await redis_client.hset(first_key, mapping={"text": "later update", "access_count": "7"})
    first_before = await redis_client.execute_command("HGETALL", first_key, NEVER_DECODE=True)
    assert (
        await long_term_memory.run_delayed_extraction(
            "shared",
            "orchard",
            "alice",
            timestamp,
            source_incarnation=str(source.version),
        )
        == 2
    )
    assert (
        await redis_client.execute_command("HGETALL", first_key, NEVER_DECODE=True) == first_before
    )
    assert (
        await redis_client.hget(Keys.memory_key("derived-1", "orchard", "alice"), "id_")
        == "derived-1"
    )
    completed = json.loads(await redis_client.get(f"extraction_batch:{key}:{generation}"))
    assert set(completed["record_receipts"]) == {"0", "1"}
    assert completed["state"] == "finalized"
    actual = await get_working_memory("shared", "alice", "orchard", redis_client)
    assert actual.messages[0].extraction_generation is None
    assert not await redis_client.exists(pending_key)
    assert len(model_calls) == 1


@pytest.mark.parametrize("embedding_failure", [False, True])
async def test_same_id_summary_replacement_keeps_old_hash_until_atomic_publish(
    redis_client, monkeypatch, embedding_failure
):
    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    monkeypatch.setattr(settings, "redisvl_vector_dimensions", 3)
    monkeypatch.setattr(settings, "embedding_model", "test-fixed-3d")
    monkeypatch.setattr(settings, "compact_semantic_duplicates", False)
    monkeypatch.setattr(settings, "enable_discrete_memory_extraction", False)
    await ensure_working_memory_index(redis_client)
    db = RedisVLMemoryVectorDatabase(
        AsyncSearchIndex.from_dict(_build_redis_schema(), redis_client=redis_client),
        FixedEmbeddings(),
    )

    async def embed(texts):
        return [[1.0, 0.0, 0.0] for _ in texts]

    monkeypatch.setattr(db.embeddings, "aembed_documents", embed)
    old = MemoryRecord(
        id="summary",
        text="old summary",
        namespace="orchard",
        user_id="alice",
        persisted_at="2026-09-01T00:00:00+00:00",
        extraction_strategy="summary",
    )
    await db.add_memories([old])
    target = Keys.memory_key("summary", "orchard", "alice")
    before = await redis_client.execute_command("HGETALL", target, NEVER_DECODE=True)
    source = WorkingMemory(
        session_id="shared",
        namespace="orchard",
        user_id="alice",
        messages=[MemoryMessage(role="user", content="new information")],
    )
    await set_working_memory(source, redis_client)

    class SilentTasks:
        def add_task(self, *args, **kwargs):
            pass

    async def get_db():
        return db

    async def extract(**kwargs):
        return [
            MemoryRecord(
                id="summary",
                text="updated summary",
                namespace="orchard",
                user_id="alice",
                extraction_strategy="summary",
            )
        ]

    async def fail_embedding(texts):
        raise ConnectionError("replacement embedding failed")

    monkeypatch.setattr(long_term_memory, "get_memory_vector_db", get_db)
    monkeypatch.setattr(long_term_memory, "get_background_tasks", lambda: SilentTasks())
    monkeypatch.setattr(long_term_memory, "extract_memories_from_session_thread", extract)
    if embedding_failure:
        monkeypatch.setattr(db.embeddings, "aembed_documents", fail_embedding)
        with pytest.raises(ConnectionError, match="replacement embedding failed"):
            await long_term_memory.run_delayed_extraction("shared", "orchard", "alice")
        assert await redis_client.execute_command("HGETALL", target, NEVER_DECODE=True) == before
    else:
        assert await long_term_memory.run_delayed_extraction("shared", "orchard", "alice") == 1
        assert await redis_client.hget(target, "text") == "updated summary"
        assert (
            float(await redis_client.hget(target, "persisted_at")) == old.persisted_at.timestamp()
        )
        key = Keys.working_memory_key("shared", "alice", "orchard")
        batches = [
            json.loads(await redis_client.get(batch))
            async for batch in redis_client.scan_iter(match=f"extraction_batch:{key}:*")
        ]
        assert len(batches) == 1 and batches[0]["state"] == "finalized"
        assert batches[0]["record_receipts"]["0"]["target_key"] == target
    for generation_key in Keys.memory_generation_keys("summary", "orchard", "alice"):
        assert (await redis_client.get(generation_key) or "0") == "0"


@pytest.mark.parametrize("child_failure", [False, True])
async def test_discrete_source_is_marked_only_after_child_publication(
    redis_client, monkeypatch, child_failure
):
    from agent_memory_server import (
        extraction,
        memory_strategies,
        memory_vector_db_factory,
    )

    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    monkeypatch.setattr(settings, "redisvl_vector_dimensions", 3)
    monkeypatch.setattr(settings, "embedding_model", "test-fixed-3d")
    monkeypatch.setattr(settings, "enable_discrete_memory_extraction", False)
    db = RedisVLMemoryVectorDatabase(
        AsyncSearchIndex.from_dict(_build_redis_schema(), redis_client=redis_client),
        FixedEmbeddings(),
    )

    async def embed(texts):
        return [[1.0, 0.0, 0.0] for _ in texts]

    monkeypatch.setattr(db.embeddings, "aembed_documents", embed)
    source = MemoryRecord(
        id="source",
        text="input",
        namespace="orchard",
        user_id="alice",
        memory_type="message",
        discrete_memory_extracted="f",
    )
    await db.add_memories([source])
    key = Keys.memory_key("source", "orchard", "alice")
    before = await redis_client.execute_command("HGETALL", key, NEVER_DECODE=True)

    model_calls = []

    class Strategy:
        async def extract_memories(self, text):
            model_calls.append(text)
            return [{"text": "derived fact"}]

    class SilentTasks:
        def add_task(self, *args, **kwargs):
            pass

    async def get_db():
        return db

    monkeypatch.setattr(
        memory_strategies, "get_memory_strategy", lambda *args, **kwargs: Strategy()
    )
    monkeypatch.setattr(memory_vector_db_factory, "get_memory_vector_db", get_db)
    monkeypatch.setattr(long_term_memory, "get_memory_vector_db", get_db)
    monkeypatch.setattr(long_term_memory, "get_background_tasks", lambda: SilentTasks())
    original_index = long_term_memory.index_long_term_memories
    children = []

    async def index(records, **kwargs):
        assert await redis_client.hget(key, "discrete_memory_extracted") == "f"
        children.extend(records)
        if child_failure:
            raise ConnectionError("child publication failed")
        await original_index(records, **kwargs)

    monkeypatch.setattr(long_term_memory, "index_long_term_memories", index)
    if child_failure:
        with pytest.raises(ConnectionError, match="child publication failed"):
            await extraction.extract_memories_with_strategy([source], deduplicate=False)
        assert await redis_client.execute_command("HGETALL", key, NEVER_DECODE=True) == before
        assert not await redis_client.exists(Keys.memory_key(children[0].id, "orchard", "alice"))
        saved = await db.get_discrete_extraction(source)
        assert len(saved) == 1 and saved[0]["_child_id"] == children[0].id
        assert saved[0]["_publication_generation"] == ["0"] * 4
        monkeypatch.setattr(long_term_memory, "index_long_term_memories", original_index)
        await extraction.extract_memories_with_strategy([source], deduplicate=False)
        assert len(model_calls) == 1
        assert (
            await redis_client.hget(Keys.memory_key(children[0].id, "orchard", "alice"), "text")
            == "derived fact"
        )
        assert await redis_client.hget(key, "discrete_memory_extracted") == "t"

    else:
        await extraction.extract_memories_with_strategy([source], deduplicate=False)
        assert await redis_client.hget(key, "discrete_memory_extracted") == "t"
        assert (
            await redis_client.hget(Keys.memory_key(children[0].id, "orchard", "alice"), "text")
            == "derived fact"
        )


@pytest.mark.parametrize("change", ["content", "delete", "stats"])
async def test_discrete_source_completion_preserves_concurrent_changes(
    redis_client, monkeypatch, change
):
    from agent_memory_server import (
        extraction,
        memory_strategies,
        memory_vector_db_factory,
    )

    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    monkeypatch.setattr(settings, "redisvl_vector_dimensions", 3)
    monkeypatch.setattr(settings, "embedding_model", "test-fixed-3d")
    db = RedisVLMemoryVectorDatabase(
        AsyncSearchIndex.from_dict(_build_redis_schema(), redis_client=redis_client),
        FixedEmbeddings(),
    )

    async def embed(texts):
        return [[1.0, 0.0, 0.0] for _ in texts]

    monkeypatch.setattr(db.embeddings, "aembed_documents", embed)
    source = MemoryRecord(
        id="source",
        text="input",
        namespace="orchard",
        user_id="alice",
        memory_type="message",
        discrete_memory_extracted="f",
    )
    await db.add_memories([source])
    key = Keys.memory_key("source", "orchard", "alice")

    class Strategy:
        async def extract_memories(self, text):
            return [{"text": "fact"}]

    async def get_db():
        return db

    async def index(records, **kwargs):
        if change == "content":
            await redis_client.hset(key, mapping={"text": "new input", "metadata": '{"new":true}'})
        elif change == "delete":
            await db.delete_memories(["source"], "orchard", "alice", exact_scope=True)
        else:
            await db.add_memories(records)
            await redis_client.hset(key, mapping={"access_count": "17", "last_accessed": "123.0"})

    monkeypatch.setattr(
        memory_strategies, "get_memory_strategy", lambda *args, **kwargs: Strategy()
    )
    monkeypatch.setattr(memory_vector_db_factory, "get_memory_vector_db", get_db)
    monkeypatch.setattr(long_term_memory, "index_long_term_memories", index)
    if change == "stats":
        await extraction.extract_memories_with_strategy([source], deduplicate=False)
        assert await redis_client.hget(key, "discrete_memory_extracted") == "t"
        assert await redis_client.hget(key, "access_count") == "17"
        assert await redis_client.hget(key, "last_accessed") == "123.0"
    else:
        with pytest.raises(ValueError, match="source changed before completion"):
            await extraction.extract_memories_with_strategy([source], deduplicate=False)
        if change == "content":
            assert await redis_client.hget(key, "text") == "new input"
            assert await redis_client.hget(key, "metadata") == '{"new":true}'
            assert await redis_client.hget(key, "discrete_memory_extracted") == "f"
        else:
            assert not await redis_client.exists(key)


async def test_discrete_prepared_results_keep_first_output_and_deletion_snapshot(
    redis_client, monkeypatch
):
    monkeypatch.setattr(settings, "redisvl_vector_dimensions", 3)
    monkeypatch.setattr(settings, "embedding_model", "test-fixed-3d")
    db = RedisVLMemoryVectorDatabase(
        AsyncSearchIndex.from_dict(_build_redis_schema(), redis_client=redis_client),
        FixedEmbeddings(),
    )

    async def embed(texts):
        return [[1.0, 0.0, 0.0] for _ in texts]

    monkeypatch.setattr(db.embeddings, "aembed_documents", embed)
    source = MemoryRecord(id="source", text="input", namespace="orchard", user_id="alice")
    await db.add_memories([source])
    original = await db.prepare_discrete_extraction(
        source, [{"text": "first", "_child_id": "child"}]
    )
    assert (
        await db.prepare_discrete_extraction(source, [{"text": "second", "_child_id": "other"}])
        == original
    )
    batches = [key async for key in redis_client.scan_iter(match="discrete_batch:*")]
    assert len(batches) == 1 and await redis_client.ttl(batches[0]) == -1
    assert await db.delete_memories(["child"], "orchard", "alice", exact_scope=True) == 0
    restored = await db.get_discrete_extraction(source)
    assert restored == original and restored[0]["_publication_generation"] == ["0"] * 4
    record = MemoryRecord(
        id="child", text=restored[0]["text"], namespace="orchard", user_id="alice"
    )
    record._publication_generation = restored[0]["_publication_generation"]
    with pytest.raises(ValueError, match="deleted before publication"):
        await db.add_memories([record])
    assert not await redis_client.exists(Keys.memory_key("child", "orchard", "alice"))
    await redis_client.hset(Keys.memory_key("source", "orchard", "alice"), "text", "new input")
    with pytest.raises(ValueError, match="source changed before preparation"):
        await db.get_discrete_extraction(source)


@pytest.mark.parametrize(
    "deduplicate,hash_duplicate", [(False, False), (True, False), (True, True)]
)
@pytest.mark.parametrize("compact_semantics", [False, True])
@pytest.mark.parametrize("response_loss", [False, True])
async def test_discrete_partial_publication_receipts_skip_completed_child_on_retry(
    redis_client,
    monkeypatch,
    response_loss,
    deduplicate,
    hash_duplicate,
    compact_semantics,
):
    from agent_memory_server import (
        extraction,
        memory_strategies,
        memory_vector_db_factory,
    )

    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    monkeypatch.setattr(settings, "redisvl_vector_dimensions", 3)
    monkeypatch.setattr(settings, "embedding_model", "test-fixed-3d")
    monkeypatch.setattr(settings, "enable_discrete_memory_extraction", False)
    monkeypatch.setattr(settings, "compact_semantic_duplicates", compact_semantics)
    db = RedisVLMemoryVectorDatabase(
        AsyncSearchIndex.from_dict(_build_redis_schema(), redis_client=redis_client),
        FixedEmbeddings(),
    )

    async def embed(texts):
        return [[1.0, 0.0, 0.0] for _ in texts]

    monkeypatch.setattr(db.embeddings, "aembed_documents", embed)

    async def query_embed(text):
        return [1.0, 0.0, 0.0]

    monkeypatch.setattr(db.embeddings, "aembed_query", query_embed)
    source = MemoryRecord(
        id="source",
        text="input",
        namespace="orchard",
        user_id="alice",
        memory_type="message",
        discrete_memory_extracted="f",
    )
    await db.add_memories([source])
    if hash_duplicate:
        await db.add_memories(
            [
                MemoryRecord(
                    id="existing-fact",
                    text="fact one",
                    namespace="orchard",
                    user_id="alice",
                    memory_type="episodic",
                    discrete_memory_extracted="t",
                )
            ]
        )
    model_calls = []

    class Strategy:
        async def extract_memories(self, text):
            model_calls.append(text)
            return [{"text": "fact one"}, {"text": "fact two"}]

    class SilentTasks:
        def add_task(self, *args, **kwargs):
            pass

    async def get_db():
        return db

    monkeypatch.setattr(
        memory_strategies, "get_memory_strategy", lambda *args, **kwargs: Strategy()
    )
    monkeypatch.setattr(memory_vector_db_factory, "get_memory_vector_db", get_db)
    monkeypatch.setattr(long_term_memory, "get_memory_vector_db", get_db)
    monkeypatch.setattr(long_term_memory, "get_background_tasks", lambda: SilentTasks())
    original_eval = redis_client.eval

    async def fail(script, numkeys, *args):
        if not response_loss and "batch.record_receipts['1']" in script:
            raise ConnectionError("discrete publication failed")
        result = await original_eval(script, numkeys, *args)
        if response_loss and "batch.record_receipts['0']" in script:
            raise ConnectionError("discrete publication failed")
        return result

    monkeypatch.setattr(redis_client, "eval", fail)
    with pytest.raises(ConnectionError, match="discrete publication failed"):
        await extraction.extract_memories_with_strategy([source], deduplicate=deduplicate)
    batches = [key async for key in redis_client.scan_iter(match="discrete_batch:*")]
    assert len(batches) == 1
    batch = json.loads(await redis_client.get(batches[0]))
    assert set(batch["record_receipts"]) == {"0"}
    receipt = batch["record_receipts"]["0"]
    first_key = receipt.get("duplicate_key", receipt["target_key"])
    if hash_duplicate:
        assert receipt["outcome"] == "hash_duplicate"
        assert not await redis_client.exists(receipt["target_key"])
        assert await redis_client.hget(first_key, "access_count") == "1"
    second_key = Keys.memory_key(batch["records"][1]["_child_id"], "orchard", "alice")
    assert not await redis_client.exists(second_key)
    assert (
        await redis_client.hget(
            Keys.memory_key("source", "orchard", "alice"), "discrete_memory_extracted"
        )
        == "f"
    )
    await redis_client.hset(first_key, mapping={"text": "later update", "access_count": "19"})
    first_before = await redis_client.execute_command("HGETALL", first_key, NEVER_DECODE=True)

    async def lose_completion_response(script, numkeys, *args):
        result = await original_eval(script, numkeys, *args)
        if "batch.finalized_at = ARGV[6]" in script:
            raise ConnectionError("completion response lost")
        return result

    monkeypatch.setattr(redis_client, "eval", lose_completion_response)
    with pytest.raises(ConnectionError, match="completion response lost"):
        await extraction.extract_memories_with_strategy([source], deduplicate=deduplicate)
    finalized = await redis_client.get(batches[0])
    assert json.loads(finalized)["state"] == "finalized"
    monkeypatch.setattr(redis_client, "eval", original_eval)

    async def must_not_publish(*args, **kwargs):
        raise AssertionError("Finalized batch must not re-enter publication")

    monkeypatch.setattr(long_term_memory, "index_long_term_memories", must_not_publish)
    await extraction.extract_memories_with_strategy([source], deduplicate=deduplicate)
    assert await redis_client.get(batches[0]) == finalized
    assert len(model_calls) == 1
    assert (
        await redis_client.execute_command("HGETALL", first_key, NEVER_DECODE=True) == first_before
    )
    assert await redis_client.hget(second_key, "text") == "fact two"
    assert set(json.loads(await redis_client.get(batches[0]))["record_receipts"]) == {
        "0",
        "1",
    }
    assert (
        await redis_client.hget(
            Keys.memory_key("source", "orchard", "alice"), "discrete_memory_extracted"
        )
        == "t"
    )


async def test_discrete_completion_rejects_missing_receipts_and_finalizes_empty_batch(
    redis_client, monkeypatch
):
    monkeypatch.setattr(settings, "redisvl_vector_dimensions", 3)
    monkeypatch.setattr(settings, "embedding_model", "test-fixed-3d")
    db = RedisVLMemoryVectorDatabase(
        AsyncSearchIndex.from_dict(_build_redis_schema(), redis_client=redis_client),
        FixedEmbeddings(),
    )

    async def embed(texts):
        return [[1.0, 0.0, 0.0] for _ in texts]

    monkeypatch.setattr(db.embeddings, "aembed_documents", embed)
    source = MemoryRecord(id="source", text="input", namespace="orchard", user_id="alice")
    await db.add_memories([source])
    await db.prepare_discrete_extraction(source, [{"text": "fact", "_child_id": "child"}])
    with pytest.raises(ValueError, match="publication incomplete"):
        await db.mark_extraction_completed([source])
    assert (
        await redis_client.hget(
            Keys.memory_key("source", "orchard", "alice"), "discrete_memory_extracted"
        )
        == "f"
    )
    empty = MemoryRecord(id="empty-source", text="input", namespace="orchard", user_id="alice")
    await db.add_memories([empty])
    assert await db.prepare_discrete_extraction(empty, []) == []
    await db.mark_extraction_completed([empty])
    batches = [
        json.loads(await redis_client.get(key))
        async for key in redis_client.scan_iter(match="discrete_batch:*")
    ]
    assert sum(batch["state"] == "finalized" for batch in batches) == 1
    assert await db.get_discrete_extraction(empty) == []
    assert await db.mark_extraction_completed([empty]) == 1


@pytest.mark.parametrize("response_loss", [False, True])
async def test_thread_semantic_merge_plan_recovers_without_second_model(
    redis_client, monkeypatch, response_loss
):
    from ulid import ULID

    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    monkeypatch.setattr(settings, "redisvl_vector_dimensions", 3)
    monkeypatch.setattr(settings, "embedding_model", "test-fixed-3d")
    monkeypatch.setattr(settings, "compact_semantic_duplicates", True)
    monkeypatch.setattr(settings, "enable_discrete_memory_extraction", False)
    db = RedisVLMemoryVectorDatabase(
        AsyncSearchIndex.from_dict(_build_redis_schema(), redis_client=redis_client),
        FixedEmbeddings(),
    )
    existing = MemoryRecord(id="existing", text="old orchard", namespace="orchard", user_id="alice")
    await db.add_memories([existing])
    generation = str(ULID())
    source = WorkingMemory(
        session_id="shared",
        namespace="orchard",
        user_id="alice",
        messages=[
            MemoryMessage(
                role="user",
                content="input",
                discrete_memory_extracted="t",
                extraction_generation=generation,
            )
        ],
    )
    record = MemoryRecord(
        id="incoming", text="recent orchard", namespace="orchard", user_id="alice"
    )
    record._publication_generation = ["0"] * 4
    await set_working_memory(source, redis_client, extraction_batch=(generation, [record]))
    key = Keys.working_memory_key("shared", "alice", "orchard")
    batch_key = f"extraction_batch:{key}:{generation}"
    record._source_session = (
        key,
        Keys.working_memory_revision_key("shared", "alice", "orchard"),
        source.version,
    )
    record._extraction_batch = (batch_key, 0)
    calls = []

    async def get_db():
        return db

    async def merge(records):
        calls.append(records)
        return MemoryRecord(
            id="model-id", text="merged orchard", namespace="orchard", user_id="alice"
        )

    async def embed(texts):
        return [[1.0, 0.0, 0.0] for _ in texts]

    class SilentTasks:
        def add_task(self, *args, **kwargs):
            pass

    monkeypatch.setattr(long_term_memory, "get_memory_vector_db", get_db)
    monkeypatch.setattr(long_term_memory, "get_background_tasks", lambda: SilentTasks())
    monkeypatch.setattr(long_term_memory, "merge_memories_with_llm", merge)
    monkeypatch.setattr(db.embeddings, "aembed_documents", embed)
    original_eval = redis_client.eval

    async def fail(script, numkeys, *args):
        if "batch.record_receipts['0']" in script and not response_loss:
            raise ConnectionError("semantic publication failure")
        result = await original_eval(script, numkeys, *args)
        if "batch.record_receipts['0']" in script and response_loss:
            raise ConnectionError("semantic publication failure")
        return result

    monkeypatch.setattr(redis_client, "eval", fail)
    with pytest.raises(ConnectionError, match="semantic publication failure"):
        await long_term_memory.index_long_term_memories(
            [record], deduplicate=True, vector_distance_threshold=2.0
        )
    batch = json.loads(await redis_client.get(batch_key))
    assert batch["semantic_plans"]["0"]["memory"]["text"] == "merged orchard"
    assert len(calls) == 1
    assert bool(await redis_client.exists(Keys.memory_key("existing", "orchard", "alice"))) == (
        not response_loss
    )
    incoming_key = Keys.memory_key("incoming", "orchard", "alice")
    if response_loss:
        await redis_client.hset(incoming_key, mapping={"text": "later update", "access_count": "9"})
        before = await redis_client.execute_command("HGETALL", incoming_key, NEVER_DECODE=True)
    monkeypatch.setattr(redis_client, "eval", original_eval)
    # Recovery must honor the saved plan even if compaction was disabled meanwhile.
    monkeypatch.setattr(settings, "compact_semantic_duplicates", False)
    await long_term_memory.index_long_term_memories(
        [record], deduplicate=True, vector_distance_threshold=2.0
    )
    assert len(calls) == 1
    assert not await redis_client.exists(Keys.memory_key("existing", "orchard", "alice"))
    if response_loss:
        assert (
            await redis_client.execute_command("HGETALL", incoming_key, NEVER_DECODE=True) == before
        )
    else:
        assert await redis_client.hget(incoming_key, "text") == "merged orchard"
    await long_term_memory.mark_extraction_batch_published(redis_client, key, generation)
    await long_term_memory.finalize_published_extraction_batch(
        redis_client, key, generation, source.version
    )
    assert json.loads(await redis_client.get(batch_key))["state"] == "finalized"


@pytest.mark.parametrize("phase", ["prepared", "finalized"])
async def test_structure_worker_sigkill_recovers_cached_results_and_ack(
    dragonfly_factory, monkeypatch, tmp_path, phase
):
    url = dragonfly_factory(f"--lua_undeclared_keys_shas={SCHEDULER_SHA}")
    monkeypatch.setattr(settings, "redis_url", url)
    monkeypatch.setattr(settings, "docket_name", "structure-process")
    monkeypatch.setattr(settings, "use_docket", True)
    monkeypatch.setattr(settings, "enable_topic_extraction", True)
    monkeypatch.setattr(settings, "enable_ner", True)
    monkeypatch.setattr(settings, "redisvl_vector_dimensions", 3)
    monkeypatch.setattr(settings, "embedding_model", "test-fixed-3d")
    child_code = """
import asyncio, sys
from datetime import timedelta
from docket import Docket, Worker
from redis.asyncio import Redis
from agent_memory_server import long_term_memory
from agent_memory_server.config import settings

async def main():
    settings.redis_url = sys.argv[1]
    settings.docket_name = 'structure-process'
    from agent_memory_server.utils import model_execution_guard as execution_guard
    execution_guard.DEFAULT_LEASE_TTL_MS = 300
    settings.use_docket = True
    async with Redis.from_url(sys.argv[1], decode_responses=True) as client:
        async def get_redis(): return client
        async def extract(text):
            await client.incr('probe:structure:model_calls')
            return ['fruit'], ['tree']
        long_term_memory.get_redis_conn = get_redis
        long_term_memory.handle_extraction = extract
        original_eval = client.eval
        async def barrier(script, numkeys, *args):
            if sys.argv[2] == 'block' and "batch.state = 'finalized'" in script:
                if sys.argv[3] == 'finalized':
                    await original_eval(script, numkeys, *args)
                await client.set('probe:structure:entered', 'yes')
                await asyncio.Event().wait()
            return await original_eval(script, numkeys, *args)
        client.eval = barrier
        async with Docket(name='structure-process', url=sys.argv[1]) as docket:
            docket.register(long_term_memory.extract_memory_structure)
            async with Worker(docket, name='structure-' + sys.argv[2], schedule_automatic_tasks=False,
                redelivery_timeout=timedelta(seconds=30 if sys.argv[2] == 'block' else 1),
                minimum_check_interval=timedelta(milliseconds=20), scheduling_resolution=timedelta(milliseconds=20)) as worker:
                if sys.argv[2] == 'block': await worker.run_forever()
                else: await asyncio.wait_for(worker.run_until_finished(), 10)
asyncio.run(main())
"""
    async with (
        Redis.from_url(url, decode_responses=True) as client,
        Docket(name="structure-process", url=url) as docket,
    ):
        db = RedisVLMemoryVectorDatabase(
            AsyncSearchIndex.from_dict(_build_redis_schema(), redis_client=client),
            FixedEmbeddings(),
        )
        await db.add_memories(
            [
                MemoryRecord(
                    id="structured",
                    text="recent orchard",
                    namespace="orchard",
                    user_id="alice",
                    topics=["existing"],
                )
            ]
        )
        intents = [key async for key in client.scan_iter(match="structure_outbox:*")]
        assert len(intents) == 1
        assert await long_term_memory.reconcile_structure_outbox(client) == 1
        processes = []
        log_path = tmp_path / "structure-process.log"
        with log_path.open("w") as log:
            try:
                first = subprocess.Popen(
                    [sys.executable, "-c", child_code, url, "block", phase],
                    cwd=Path(__file__).parent,
                    stdout=log,
                    stderr=log,
                )
                processes.append(first)
                deadline = asyncio.get_running_loop().time() + 10
                while not await client.exists("probe:structure:entered"):
                    assert first.poll() is None, log_path.read_text()
                    assert asyncio.get_running_loop().time() < deadline, log_path.read_text()
                    await asyncio.sleep(0.05)
                batches = [key async for key in client.scan_iter(match="structure_batch:*")]
                assert len(batches) == 1
                before_receipt = await client.get(batches[0])
                assert json.loads(before_receipt)["state"] == phase
                assert (await docket.get_execution(intents[0])).state == ExecutionState.RUNNING
                assert await client.exists(intents[0])
                active_leases = [key async for key in client.scan_iter(match="model_execution:*")]
                assert len(active_leases) == 1 and await client.pttl(active_leases[0]) > 0
                first.kill()
                first.wait(timeout=5)
                second = subprocess.Popen(
                    [sys.executable, "-c", child_code, url, "recover", phase],
                    cwd=Path(__file__).parent,
                    stdout=log,
                    stderr=log,
                )
                processes.append(second)
                deadline = asyncio.get_running_loop().time() + 15
                while second.poll() is None:
                    assert asyncio.get_running_loop().time() < deadline, log_path.read_text()
                    await asyncio.sleep(0.05)
                assert second.returncode == 0, log_path.read_text()
                assert (await docket.get_execution(intents[0])).state == ExecutionState.COMPLETED
                assert not await client.exists(active_leases[0])
                assert await client.get("probe:structure:model_calls") == "1"
                target = Keys.memory_key("structured", "orchard", "alice")
                assert await client.hget(target, "topics") == "existing,fruit"
                assert await client.hget(target, "entities") == "tree"
                receipt = await client.get(batches[0])
                assert json.loads(receipt)["state"] == "finalized"
                if phase == "finalized":
                    assert receipt == before_receipt
                assert await long_term_memory.reconcile_structure_outbox(client) == 0
                assert not await client.exists(intents[0])
                assert (await client.xpending(docket.stream_key, docket.worker_group_name))[
                    "pending"
                ] == 0
                assert await client.xlen(docket.stream_key) == 0
            finally:
                for process in processes:
                    if process.poll() is None:
                        process.kill()
                    process.wait(timeout=5)
