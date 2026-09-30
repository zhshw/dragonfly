"""Renewal, cancellation and owner fencing on real two-shard Dragonfly."""

import asyncio

import pytest

from agent_memory_server.utils.model_execution_guard import (
    ModelExecutionBusyError,
    ModelExecutionLeaseLostError,
    current_model_execution_lease,
    model_execution_guard,
    model_execution_key,
)


async def test_model_execution_guard_waiters_reuse_one_durable_result(redis_client):
    entered, release = asyncio.Event(), asyncio.Event()
    calls = 0
    identity = ["structure", "tenant-a", "alice", "memory"]

    async def compute():
        nonlocal calls
        async with model_execution_guard(redis_client, identity, ttl_ms=150) as lease:
            assert current_model_execution_lease() is lease
            raw = await redis_client.get("model-result")
            if raw is not None:
                return raw
            calls += 1
            entered.set()
            await release.wait()
            return await lease.publish_if_absent("model-result", "prepared-once")

    first = asyncio.create_task(compute())
    await entered.wait()
    second, third = asyncio.create_task(compute()), asyncio.create_task(compute())
    await asyncio.sleep(0.4)  # More than two original TTLs; renewal retains owner.
    assert calls == 1
    assert await redis_client.pttl(model_execution_key(identity)) > 0
    release.set()
    assert await asyncio.gather(first, second, third) == ["prepared-once"] * 3
    assert calls == 1
    assert not await redis_client.exists(model_execution_key(identity))


async def test_model_execution_guard_cancel_waiter_never_releases_holder(redis_client):
    entered, release = asyncio.Event(), asyncio.Event()
    identity = ["thread", "scoped-source"]

    async def holder():
        async with model_execution_guard(redis_client, identity) as lease:
            entered.set()
            await release.wait()
            return lease.owner

    async def waiter():
        async with model_execution_guard(redis_client, identity):
            pytest.fail("A cancelled waiter must not enter")

    first = asyncio.create_task(holder())
    await entered.wait()
    owner = await redis_client.get(model_execution_key(identity))
    waiting = asyncio.create_task(waiter())
    await asyncio.sleep(0.05)
    waiting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting
    assert await redis_client.get(model_execution_key(identity)) == owner
    release.set()
    assert await first == owner
    assert not await redis_client.exists(model_execution_key(identity))


async def test_model_execution_guard_cancel_holder_releases_without_waiting_ttl(redis_client):
    entered = asyncio.Event()
    identity = ["discrete", "scoped-source"]

    async def holder():
        async with model_execution_guard(redis_client, identity):
            entered.set()
            await asyncio.Event().wait()

    first = asyncio.create_task(holder())
    await entered.wait()
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    assert not await redis_client.exists(model_execution_key(identity))
    async with model_execution_guard(redis_client, identity, wait_timeout=0) as lease:
        assert await lease.publish_if_absent("cancelled-recovery", "new-result") == "new-result"


async def test_model_execution_guard_owner_loss_blocks_cancel_suppressing_runner(redis_client):
    entered = asyncio.Event()
    identity = ["semantic", "scope", "same-output"]
    key = model_execution_key(identity)

    async def broken_model_runner():
        async with model_execution_guard(redis_client, identity, ttl_ms=120) as lease:
            entered.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                pass  # Even a model adapter swallowing cancellation is fenced.
            await lease.publish_if_absent("stale-model-output", "must-not-persist")

    task = asyncio.create_task(broken_model_runner())
    await entered.wait()
    await redis_client.set(key, "replacement-owner", px=30000)
    with pytest.raises(ModelExecutionLeaseLostError):
        await task
    assert not await redis_client.exists("stale-model-output")
    assert await redis_client.get(key) == "replacement-owner"


async def test_model_execution_guard_busy_and_child_context_are_independent(redis_client):
    identity = ["thread", "same-source"]
    async with model_execution_guard(redis_client, identity) as lease:

        async def inherited_context():
            assert current_model_execution_lease() is None
            with pytest.raises(ModelExecutionBusyError):
                async with model_execution_guard(redis_client, identity, wait_timeout=0):
                    pytest.fail("Another task cannot inherit execution ownership")

        await asyncio.create_task(inherited_context())
        assert await redis_client.get(model_execution_key(identity)) == lease.owner


async def test_model_execution_guard_lost_acquire_response_releases_owned_lock(
    redis_client, monkeypatch
):
    identity = ["thread", "response-loss"]
    original = redis_client.set
    lost = False

    async def response_lost(key, value, **kwargs):
        nonlocal lost
        result = await original(key, value, **kwargs)
        if key == model_execution_key(identity) and not lost:
            lost = True
            raise RuntimeError("SET response lost")
        return result

    monkeypatch.setattr(redis_client, "set", response_lost)
    with pytest.raises(RuntimeError, match="SET response"):
        async with model_execution_guard(redis_client, identity):
            pytest.fail("No model work without acquire acknowledgement")
    assert not await redis_client.exists(model_execution_key(identity))


@pytest.fixture
async def guarded_memory_db(redis_client, monkeypatch):
    from redisvl.index import AsyncSearchIndex
    from test_search import FixedEmbeddings
    from agent_memory_server.config import settings
    from agent_memory_server.memory_vector_db import RedisVLMemoryVectorDatabase
    from agent_memory_server.memory_vector_db_factory import _build_redis_schema

    monkeypatch.setattr(settings, "redisvl_vector_dimensions", 3)
    monkeypatch.setattr(settings, "embedding_model", "test-fixed-3d")
    monkeypatch.setattr(settings, "redisvl_indexing_algorithm", "flat")
    monkeypatch.setattr(settings, "enable_topic_extraction", False)
    monkeypatch.setattr(settings, "enable_ner", False)
    monkeypatch.setattr(settings, "enable_discrete_memory_extraction", False)
    monkeypatch.setattr(settings, "compact_semantic_duplicates", False)
    from agent_memory_server import long_term_memory

    class SilentTasks:
        def add_task(self, *args, **kwargs):
            pass

    monkeypatch.setattr(long_term_memory, "get_background_tasks", lambda: SilentTasks())
    db = RedisVLMemoryVectorDatabase(
        AsyncSearchIndex.from_dict(_build_redis_schema(), redis_client=redis_client),
        FixedEmbeddings(),
    )
    await db._ensure_index()
    return db


async def test_model_execution_guard_structure_active_duplicates_make_one_model_call(
    guarded_memory_db, redis_client, monkeypatch
):
    from agent_memory_server import long_term_memory
    from agent_memory_server.models import MemoryRecord
    from agent_memory_server.utils.keys import Keys

    record = MemoryRecord(id="same", text="recent orchard", namespace="tenant", user_id="alice")
    await guarded_memory_db.add_memories([record])
    entered, release = asyncio.Event(), asyncio.Event()
    calls = 0

    async def get_redis():
        return redis_client

    async def extract(text):
        nonlocal calls
        calls += 1
        entered.set()
        await release.wait()
        return ["fruit"], ["tree"]

    monkeypatch.setattr(long_term_memory, "get_redis_conn", get_redis)
    monkeypatch.setattr(long_term_memory, "handle_extraction", extract)
    first = asyncio.create_task(long_term_memory.extract_memory_structure(record))
    await entered.wait()
    second = asyncio.create_task(long_term_memory.extract_memory_structure(record))
    third = asyncio.create_task(long_term_memory.extract_memory_structure(record))
    await asyncio.sleep(0.1)
    assert calls == 1
    release.set()
    await asyncio.gather(first, second, third)
    assert calls == 1
    key = Keys.memory_key(record.id, record.namespace, record.user_id)
    assert await redis_client.hget(key, "topics") == "fruit"
    assert await redis_client.hget(key, "entities") == "tree"
    assert not [key async for key in redis_client.scan_iter(match="model_execution:*")]


async def test_model_execution_guard_discrete_active_duplicates_reuse_children(
    guarded_memory_db, redis_client, monkeypatch
):
    from agent_memory_server import (
        extraction,
        long_term_memory,
        memory_strategies,
        memory_vector_db_factory,
    )
    from agent_memory_server.models import MemoryRecord
    from agent_memory_server.utils.keys import Keys

    record = MemoryRecord(
        id="same-source",
        text="recent orchard",
        namespace="tenant",
        user_id="alice",
        memory_type="message",
    )
    await guarded_memory_db.add_memories([record])
    entered, release = asyncio.Event(), asyncio.Event()
    calls = 0

    class Strategy:
        async def extract_memories(self, text):
            nonlocal calls
            calls += 1
            entered.set()
            await release.wait()
            return [{"text": "old orchard", "type": "semantic"}]

    async def get_db():
        return guarded_memory_db

    monkeypatch.setattr(memory_vector_db_factory, "get_memory_vector_db", get_db)
    monkeypatch.setattr(long_term_memory, "get_memory_vector_db", get_db)
    monkeypatch.setattr(memory_strategies, "get_memory_strategy", lambda *a, **kw: Strategy())
    first = asyncio.create_task(
        extraction.extract_memories_with_strategy([record], deduplicate=False)
    )
    await entered.wait()
    second = asyncio.create_task(
        extraction.extract_memories_with_strategy([record], deduplicate=False)
    )
    await asyncio.sleep(0.1)
    assert calls == 1
    release.set()
    await asyncio.gather(first, second)
    assert calls == 1
    rows = (await guarded_memory_db.list_memories(limit=10)).memories
    children = [row for row in rows if row.id != record.id]
    assert len(children) == 1 and children[0].text == "old orchard"
    assert (
        await redis_client.hget(
            Keys.memory_key(record.id, record.namespace, record.user_id),
            "discrete_memory_extracted",
        )
        == "t"
    )
    assert not [key async for key in redis_client.scan_iter(match="model_execution:*")]


async def test_model_execution_guard_thread_active_duplicates_publish_once(
    guarded_memory_db, redis_client, monkeypatch
):
    from agent_memory_server import long_term_memory, memory_strategies
    from agent_memory_server.config import settings
    from agent_memory_server.models import MemoryMessage, WorkingMemory
    from agent_memory_server.utils import redis as redis_utils
    from agent_memory_server.working_memory import get_working_memory, set_working_memory
    from agent_memory_server.working_memory_index import get_working_memory_index

    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    monkeypatch.setattr(settings, "use_docket", False)
    await (await get_working_memory_index(redis_client)).create()
    source = WorkingMemory(
        session_id="same-session",
        namespace="tenant",
        user_id="alice",
        messages=[
            MemoryMessage(role="user", content="extract orchard", discrete_memory_extracted="f")
        ],
    )
    await set_working_memory(source, redis_client)
    entered, release = asyncio.Event(), asyncio.Event()
    calls = 0

    class Strategy:
        async def extract_memories(self, text, context):
            nonlocal calls
            calls += 1
            entered.set()
            await release.wait()
            return [{"text": "old orchard"}]

    class SilentTasks:
        def add_task(self, *args, **kwargs):
            pass

    async def get_db():
        return guarded_memory_db

    async def get_redis():
        return redis_client

    monkeypatch.setattr(long_term_memory, "get_memory_vector_db", get_db)
    monkeypatch.setattr(long_term_memory, "get_redis_conn", get_redis)
    monkeypatch.setattr(long_term_memory, "get_background_tasks", lambda: SilentTasks())
    monkeypatch.setattr(
        memory_strategies, "get_memory_strategy", lambda *args, **kwargs: Strategy()
    )
    first = asyncio.create_task(
        long_term_memory.run_delayed_extraction("same-session", "tenant", "alice")
    )
    await entered.wait()
    second = asyncio.create_task(
        long_term_memory.run_delayed_extraction("same-session", "tenant", "alice")
    )
    await asyncio.sleep(0.1)
    assert calls == 1
    release.set()
    assert await asyncio.gather(first, second) == [1, 0]
    assert calls == 1
    result = (await guarded_memory_db.list_memories(limit=10)).memories
    assert len(result) == 1 and result[0].text == "old orchard"
    source = await get_working_memory("same-session", "alice", "tenant", redis_client)
    assert source.messages[0].discrete_memory_extracted == "t"
    assert source.messages[0].extraction_generation is None
    assert not [key async for key in redis_client.scan_iter(match="model_execution:*")]


@pytest.mark.parametrize("kind", ["structure", "discrete"])
async def test_model_execution_guard_business_cancel_retries_from_preserved_source(
    guarded_memory_db, redis_client, monkeypatch, kind
):
    from agent_memory_server import (
        extraction,
        long_term_memory,
        memory_strategies,
        memory_vector_db_factory,
    )
    from agent_memory_server.models import MemoryRecord
    from agent_memory_server.utils.keys import Keys

    record = MemoryRecord(
        id="cancel-source",
        text="recent orchard",
        namespace="tenant",
        user_id="alice",
        memory_type="message",
    )
    await guarded_memory_db.add_memories([record])
    entered = asyncio.Event()
    calls = 0

    async def model(text):
        nonlocal calls
        calls += 1
        if calls == 1:
            entered.set()
            await asyncio.Event().wait()
        return (["fruit"], ["tree"]) if kind == "structure" else [{"text": "old orchard"}]

    class Strategy:
        extract_memories = staticmethod(model)

    async def get_db():
        return guarded_memory_db

    async def get_redis():
        return redis_client

    monkeypatch.setattr(long_term_memory, "get_redis_conn", get_redis)
    monkeypatch.setattr(long_term_memory, "handle_extraction", model)
    monkeypatch.setattr(long_term_memory, "get_memory_vector_db", get_db)
    monkeypatch.setattr(memory_vector_db_factory, "get_memory_vector_db", get_db)
    monkeypatch.setattr(
        memory_strategies, "get_memory_strategy", lambda *args, **kwargs: Strategy()
    )

    async def run():
        if kind == "structure":
            return await long_term_memory.extract_memory_structure(record)
        return await extraction.extract_memories_with_strategy([record], deduplicate=False)

    task = asyncio.create_task(run())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not [key async for key in redis_client.scan_iter(match="model_execution:*")]
    assert (
        await redis_client.hget(
            Keys.memory_key(record.id, record.namespace, record.user_id),
            "discrete_memory_extracted",
        )
        == "f"
    )
    assert len((await guarded_memory_db.list_memories(limit=10)).memories) == 1
    await run()
    assert calls == 2
    rows = (await guarded_memory_db.list_memories(limit=10)).memories
    if kind == "structure":
        assert len(rows) == 1 and rows[0].topics == ["fruit"]
    else:
        assert len(rows) == 2
        assert (
            await redis_client.hget(
                Keys.memory_key(record.id, record.namespace, record.user_id),
                "discrete_memory_extracted",
            )
            == "t"
        )


async def test_model_execution_guard_lost_owner_cannot_publish_hash_even_if_model_swallows_cancel(
    guarded_memory_db, redis_client, monkeypatch
):
    from agent_memory_server.models import MemoryRecord
    from agent_memory_server.utils.keys import Keys

    entered = asyncio.Event()
    identity = ["thread", "guarded-publication"]
    lease_key = model_execution_key(identity)

    async def embed(texts):
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            pass
        return [[1.0, 0.0, 0.0] for _ in texts]

    monkeypatch.setattr(guarded_memory_db.embeddings, "aembed_documents", embed)

    async def stale_runner():
        async with model_execution_guard(redis_client, identity, ttl_ms=120):
            await guarded_memory_db.add_memories(
                [
                    MemoryRecord(
                        id="stale-model", text="recent orchard", namespace="tenant", user_id="alice"
                    )
                ]
            )

    running = asyncio.create_task(stale_runner())
    await entered.wait()
    await redis_client.set(lease_key, "new-owner", px=5000)
    with pytest.raises(ModelExecutionLeaseLostError):
        await asyncio.wait_for(running, 2)
    assert not await redis_client.exists(Keys.memory_key("stale-model", "tenant", "alice"))
    assert await redis_client.get(lease_key) == "new-owner"
    assert not [key async for key in redis_client.scan_iter(match="memory_stage:*")]
