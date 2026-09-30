"""Old read results cannot mutate delete/recreated or updated source identities."""

from datetime import UTC, datetime, timedelta

import pytest

from test_search import memory_db as _memory_db, seed
from agent_memory_server import long_term_memory
from agent_memory_server.filters import Namespace, UserId
from agent_memory_server.models import MemoryRecord
from agent_memory_server.utils.keys import Keys
from agent_memory_server.utils.recency import rerank_with_recency

memory_db = _memory_db


async def test_access_original_snapshot_survives_rerank_and_rejects_same_id_recreation(memory_db):
    await seed(memory_db)
    client = await memory_db.index._get_client()
    results = await memory_db.list_memories(
        namespace=Namespace(eq="tenant-a"), user_id=UserId(eq="alice")
    )
    old = next(m for m in results.memories if m.id == "old")
    ranked = rerank_with_recency([old], now=datetime.now(UTC), params={})
    assert ranked[0] is old and old._publication_generation == ["0"] * 4
    snapshot = {old.id: long_term_memory.memory_access_snapshot(old)}
    key = Keys.memory_key(old.id, old.namespace, old.user_id)
    await memory_db.delete_memories([old.id], namespace=old.namespace, user_id=old.user_id)
    replacement = MemoryRecord.model_validate(old.model_dump())
    replacement.access_count = 13
    await memory_db.add_memories([replacement])
    before = await client.execute_command("HGETALL", key, NEVER_DECODE=True)
    assert (
        await long_term_memory.update_last_accessed(
            [old.id],
            redis_client=client,
            namespace=old.namespace,
            user_id=old.user_id,
            min_interval_seconds=0,
            expected_snapshots=snapshot,
        )
        == 0
    )
    assert await client.execute_command("HGETALL", key, NEVER_DECODE=True) == before
    fresh = (
        await memory_db.list_memories(
            namespace=Namespace(eq="tenant-a"), user_id=UserId(eq="alice")
        )
    ).memories
    source = next(m for m in fresh if m.id == old.id)
    assert (
        await long_term_memory.update_last_accessed(
            [old.id],
            redis_client=client,
            namespace=old.namespace,
            user_id=old.user_id,
            min_interval_seconds=0,
            expected_snapshots={old.id: long_term_memory.memory_access_snapshot(source)},
        )
        == 1
    )
    assert await client.hget(key, "access_count") == "14"


async def test_access_rejects_updated_and_unversioned_jobs(memory_db):
    await seed(memory_db)
    client = await memory_db.index._get_client()
    result = await memory_db.list_memories(
        namespace=Namespace(eq="tenant-a"), user_id=UserId(eq="alice")
    )
    old = next(m for m in result.memories if m.id == "old")
    key = Keys.memory_key(old.id, old.namespace, old.user_id)
    await client.hset(key, "updated_at", str((old.updated_at + timedelta(seconds=1)).timestamp()))
    before = await client.execute_command("HGETALL", key, NEVER_DECODE=True)
    assert (
        await long_term_memory.update_last_accessed(
            [old.id],
            redis_client=client,
            namespace=old.namespace,
            user_id=old.user_id,
            min_interval_seconds=0,
            expected_snapshots={old.id: long_term_memory.memory_access_snapshot(old)},
        )
        == 0
    )
    with pytest.raises(ValueError, match="original source snapshot"):
        await long_term_memory.update_last_accessed([old.id], redis_client=client)
    assert await client.execute_command("HGETALL", key, NEVER_DECODE=True) == before


async def test_hash_dedup_query_never_rebinds_recreated_candidate(memory_db, monkeypatch):
    await seed(memory_db)
    client = await memory_db.index._get_client()

    async def get_db():
        return memory_db

    monkeypatch.setattr(long_term_memory, "get_memory_vector_db", get_db)
    query = memory_db.list_memories

    async def recreate_after_query(**kwargs):
        result = await query(**kwargs)
        candidate = result.memories[0]
        await memory_db.delete_memories(
            [candidate.id], namespace=candidate.namespace, user_id=candidate.user_id
        )
        await memory_db.add_memories([MemoryRecord.model_validate(candidate.model_dump())])
        return result

    monkeypatch.setattr(memory_db, "list_memories", recreate_after_query)
    incoming = MemoryRecord(
        id="duplicate", text="old orchard", namespace="tenant-a", user_id="alice"
    )
    record, duplicate = await long_term_memory.deduplicate_by_hash(incoming, redis_client=client)
    assert record is incoming and not duplicate
    assert await client.hget(Keys.memory_key("old", "tenant-a", "alice"), "access_count") == "0"


async def test_semantic_dedup_query_never_rebinds_recreated_candidate(memory_db, monkeypatch):
    await seed(memory_db)
    client = await memory_db.index._get_client()

    async def get_db():
        return memory_db

    monkeypatch.setattr(long_term_memory, "get_memory_vector_db", get_db)
    query = memory_db.search_memories
    touched = False

    async def recreate_after_query(**kwargs):
        nonlocal touched
        result = await query(**kwargs)
        if not touched:
            touched = True
            candidate = result.memories[0]
            await memory_db.delete_memories(
                [candidate.id], namespace=candidate.namespace, user_id=candidate.user_id
            )
            await memory_db.add_memories([MemoryRecord.model_validate(candidate.model_dump())])
        return result

    async def merge(memories):
        return MemoryRecord(id="merged", text="old orchard", namespace="tenant-a", user_id="alice")

    monkeypatch.setattr(memory_db, "search_memories", recreate_after_query)
    monkeypatch.setattr(long_term_memory, "merge_memories_with_llm", merge)
    incoming = MemoryRecord(id="merged", text="old orchard", namespace="tenant-a", user_id="alice")
    merged, duplicate = await long_term_memory.deduplicate_by_semantic_search(
        incoming, redis_client=client
    )
    assert duplicate and merged._semantic_candidates
    assert all(candidate["generations"] == ["0"] * 4 for candidate in merged._semantic_candidates)
    before = {
        key: await client.execute_command("HGETALL", key, NEVER_DECODE=True)
        for key in [
            Keys.memory_key("old", "tenant-a", "alice"),
            Keys.memory_key("recent", "tenant-a", "alice"),
        ]
    }
    with pytest.raises(ValueError, match="changed|deleted|Semantic|semantic"):
        await memory_db.add_memories([merged])
    assert not await client.exists(Keys.memory_key("merged", "tenant-a", "alice"))
    for key, fields in before.items():
        assert await client.execute_command("HGETALL", key, NEVER_DECODE=True) == fields


async def test_queued_legacy_index_and_promote_and_extract_require_original_versions(
    redis_client, monkeypatch
):
    from docket.dependencies._base import current_execution
    from types import SimpleNamespace
    from agent_memory_server.utils import redis as redis_utils

    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    for name, call in [
        (
            "index_long_term_memories",
            lambda: long_term_memory.index_long_term_memories(
                [MemoryRecord(id="stale", text="old orchard")]
            ),
        ),
        (
            "promote_working_memory_to_long_term",
            lambda: long_term_memory.promote_working_memory_to_long_term("recreated"),
        ),
        ("run_delayed_extraction", lambda: long_term_memory.run_delayed_extraction("recreated")),
    ]:
        token = current_execution.set(SimpleNamespace(function_name=name))
        try:
            with pytest.raises(ValueError, match="original|incarnation"):
                await call()
        finally:
            current_execution.reset(token)
    assert await redis_client.dbsize() == 0


async def test_promotion_original_revision_rejects_recreated_session(redis_client, monkeypatch):
    from agent_memory_server.working_memory import set_working_memory
    from agent_memory_server.models import WorkingMemory
    from agent_memory_server.utils import redis as redis_utils

    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    source = WorkingMemory(session_id="recreated", namespace="orchard", user_id="alice")
    await set_working_memory(source, redis_client)
    original_revision = source.version
    await redis_client.delete(Keys.working_memory_key("recreated", "alice", "orchard"))
    recreated = WorkingMemory(
        session_id="recreated",
        namespace="orchard",
        user_id="alice",
        memories=[MemoryRecord(id="new", text="new source", namespace="orchard", user_id="alice")],
    )
    await set_working_memory(recreated, redis_client)
    key = Keys.working_memory_key("recreated", "alice", "orchard")
    before = await redis_client.json().get(key)
    assert recreated.version > original_revision
    assert (
        await long_term_memory.promote_working_memory_to_long_term(
            "recreated",
            "orchard",
            "alice",
            redis_client,
            expected_source_revision=original_revision,
        )
        == 0
    )
    assert await redis_client.json().get(key) == before
    assert not await redis_client.exists(Keys.memory_key("new", "orchard", "alice"))


async def test_real_worker_replayed_unversioned_tasks_cannot_bind_current_sources(
    dragonfly_factory, monkeypatch
):
    from docket import Docket
    from docket.execution import ExecutionState
    from redis.asyncio import Redis
    from conftest import SCHEDULER_SHA
    from test_docket import run_worker
    from agent_memory_server.models import WorkingMemory
    from agent_memory_server.working_memory import set_working_memory
    from agent_memory_server.utils import redis as redis_utils

    url = dragonfly_factory(f"--lua_undeclared_keys_shas={SCHEDULER_SHA}")
    async with (
        Redis.from_url(url, decode_responses=True) as client,
        Docket(name="legacy-source-replay", url=url) as docket,
    ):
        monkeypatch.setattr(redis_utils, "_redis_pool", client)
        source = WorkingMemory(
            session_id="recreated",
            namespace="orchard",
            user_id="alice",
            memories=[
                MemoryRecord(
                    id="current", text="current data", namespace="orchard", user_id="alice"
                )
            ],
        )
        await set_working_memory(source, client)
        key = Keys.working_memory_key("recreated", "alice", "orchard")
        before = await client.json().get(key)
        for function in [
            long_term_memory.index_long_term_memories,
            long_term_memory.promote_working_memory_to_long_term,
            long_term_memory.run_delayed_extraction,
        ]:
            docket.register(function)
        executions = [
            await docket.add(long_term_memory.index_long_term_memories, key="old-index")(
                [
                    MemoryRecord(
                        id="old-result", text="old data", namespace="orchard", user_id="alice"
                    )
                ]
            ),
            await docket.add(
                long_term_memory.promote_working_memory_to_long_term, key="old-promote"
            )("recreated", "orchard", "alice"),
            await docket.add(long_term_memory.run_delayed_extraction, key="old-extract")(
                "recreated", "orchard", "alice"
            ),
        ]
        await run_worker(docket)
        for execution in executions:
            assert (await docket.get_execution(execution.key)).state == ExecutionState.FAILED
        assert await client.json().get(key) == before
        assert not await client.exists(Keys.memory_key("old-result", "orchard", "alice"))
        assert not await client.exists(Keys.memory_key("current", "orchard", "alice"))
        assert not [key async for key in client.scan_iter(match="model_execution:*")]


async def test_semantic_compaction_anchor_metadata_update_rejects_original_query(
    memory_db, monkeypatch
):
    await seed(memory_db)
    client = await memory_db.index._get_client()

    async def get_db():
        return memory_db

    monkeypatch.setattr(long_term_memory, "get_memory_vector_db", get_db)
    rows = (
        await memory_db.list_memories(
            namespace=Namespace(eq="tenant-a"), user_id=UserId(eq="alice")
        )
    ).memories
    original = next(m for m in rows if m.id == "old")
    anchor = MemoryRecord.model_validate(original.model_dump())
    anchor._publication_generation = original._publication_generation
    anchor._source_fields = original._source_fields

    async def merge(memories):
        return MemoryRecord(id="old", text="old orchard", namespace="tenant-a", user_id="alice")

    monkeypatch.setattr(long_term_memory, "merge_memories_with_llm", merge)
    merged, duplicate = await long_term_memory.deduplicate_by_semantic_search(
        anchor, redis_client=client
    )
    assert duplicate and merged._source_fields == original._source_fields
    key = Keys.memory_key("old", "tenant-a", "alice")
    await client.hset(key, "metadata", '{"updated": true}')
    before = await client.execute_command("HGETALL", key, NEVER_DECODE=True)
    with pytest.raises(ValueError, match="Semantic merge candidate changed"):
        await memory_db.add_memories([merged])
    assert await client.execute_command("HGETALL", key, NEVER_DECODE=True) == before
    assert await client.exists(Keys.memory_key("recent", "tenant-a", "alice"))
