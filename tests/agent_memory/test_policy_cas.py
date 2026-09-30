"""Policy decisions never rebind old query results to a changed/live incarnation."""

import json
from datetime import UTC, datetime

import pytest

from agent_memory_server import long_term_memory
from agent_memory_server.filters import Namespace, UserId
from agent_memory_server.models import MemoryRecord
from agent_memory_server.utils.keys import Keys

from test_search import memory_db as _memory_db

memory_db = _memory_db


async def old_records(db, count=1):
    past = datetime(2026, 7, 1, tzinfo=UTC)
    records = [
        MemoryRecord(
            id=f"old-{i}",
            text="old orchard",
            namespace="orchard",
            user_id="alice",
            created_at=past,
            updated_at=past,
            last_accessed=past,
            access_count=i + 2,
            topics=[f"topic-{i}"],
            metadata={f"field-{i}": i},
            discrete_memory_extracted="t",
        )
        for i in range(count)
    ]
    await db.add_memories(records)
    result = await db.list_memories(
        namespace=Namespace(eq="orchard"), user_id=UserId(eq="alice"), limit=10
    )
    assert len(result.memories) == count
    assert all(
        memory._source_fields and memory._publication_generation for memory in result.memories
    )
    return sorted(result.memories, key=lambda memory: memory.id)


async def generations(client, memory):
    return await client.mget(
        Keys.memory_generation_keys(memory.id, memory.namespace, memory.user_id)
    )


async def hash_snapshot(client, memory):
    return await client.execute_command(
        "HGETALL",
        Keys.memory_key(memory.id, memory.namespace, memory.user_id),
        NEVER_DECODE=True,
    )


async def mutate(client, db, memory, mutation):
    key = Keys.memory_key(memory.id, memory.namespace, memory.user_id)
    if mutation == "accessed":
        await client.hset(
            key,
            mapping={
                "last_accessed": str(datetime.now(UTC).timestamp()),
                "access_count": "99",
            },
        )
    elif mutation == "pinned":
        await client.hset(key, "pinned", "1")
    elif mutation == "body":
        await client.hset(key, "text", "new immutable decision input")
    elif mutation == "metadata":
        await client.hset(key, "metadata", json.dumps({"concurrent": True}))
    elif mutation == "aba":
        original = await hash_snapshot(client, memory)
        await db.delete_memories([memory.id], memory.namespace, memory.user_id, exact_scope=True)
        # Deliberately clone every byte of the old Hash, including timestamps,
        # vector, and stored pubgen. Live deletion counters must still fence it.
        await client.hset(key, mapping=original)
    elif mutation.startswith("generation-"):
        index = int(mutation.rsplit("-", 1)[1])
        await client.incr(
            Keys.memory_generation_keys(memory.id, memory.namespace, memory.user_id)[index]
        )
    else:
        raise AssertionError(mutation)


@pytest.mark.parametrize(
    "mutation",
    [
        "accessed",
        "pinned",
        "body",
        "metadata",
        "aba",
        "generation-0",
        "generation-1",
        "generation-2",
        "generation-3",
    ],
)
async def test_policy_delete_rejects_changed_original_snapshot(memory_db, mutation):
    client = await memory_db.index._get_client()
    candidate = (await old_records(memory_db))[0]
    await mutate(client, memory_db, candidate, mutation)
    before_hash, before_generations = await hash_snapshot(client, candidate), await generations(
        client, candidate
    )
    assert await memory_db.delete_unchanged_memories([candidate]) == 0
    assert await hash_snapshot(client, candidate) == before_hash
    assert await generations(client, candidate) == before_generations


async def test_policy_delete_exact_tombstone_and_safe_missing_snapshot(memory_db):
    client = await memory_db.index._get_client()
    candidate = (await old_records(memory_db))[0]
    before = await generations(client, candidate)
    unsafe = candidate.model_copy(deep=True)
    unsafe._source_fields = None
    assert await memory_db.delete_unchanged_memories([unsafe]) == 0
    assert await generations(client, candidate) == before
    assert await memory_db.delete_unchanged_memories([candidate]) == 1
    assert not await client.exists(
        Keys.memory_key(candidate.id, candidate.namespace, candidate.user_id)
    )
    after = await generations(client, candidate)
    assert after[0] == str(int(before[0] or "0") + 1)
    assert after[1:] == before[1:]
    assert await memory_db.delete_unchanged_memories([candidate]) == 0
    assert await generations(client, candidate) == after


@pytest.mark.parametrize("mutation", ["accessed", "pinned", "body", "aba"])
async def test_forgetting_query_race_skips_and_reports_only_real_deletions(
    memory_db, monkeypatch, mutation
):
    client = await memory_db.index._get_client()
    candidates = await old_records(memory_db)
    original_list = memory_db.list_memories

    async def racing_list(*args, **kwargs):
        result = await original_list(*args, **kwargs)
        await mutate(client, memory_db, result.memories[0], mutation)
        return result

    async def provider():
        return memory_db

    monkeypatch.setattr(memory_db, "list_memories", racing_list)
    monkeypatch.setattr(long_term_memory, "get_memory_vector_db", provider)
    result = await long_term_memory.forget_long_term_memories(
        {"max_age_days": 30}, namespace="orchard", user_id="alice", dry_run=False
    )
    assert result["scanned"] == 1 and result["deleted"] == 0
    assert result["deleted_ids"] == []
    assert await client.exists(Keys.memory_key(candidates[0].id, "orchard", "alice"))


@pytest.mark.parametrize(
    "mutation",
    [
        "accessed",
        "pinned",
        "body",
        "metadata",
        "aba",
        "generation-0",
        "generation-1",
        "generation-2",
        "generation-3",
    ],
)
async def test_hash_compaction_never_rebinds_snapshot_or_changes_tombstones(memory_db, mutation):
    client = await memory_db.index._get_client()
    candidates = await old_records(memory_db, count=2)
    await mutate(client, memory_db, candidates[0], mutation)
    before_hashes = [await hash_snapshot(client, memory) for memory in candidates]
    before_generations = [await generations(client, memory) for memory in candidates]
    assert await memory_db.compact_hash_group(candidates) == 0
    assert [await hash_snapshot(client, memory) for memory in candidates] == before_hashes
    assert [await generations(client, memory) for memory in candidates] == before_generations


async def test_hash_compaction_current_snapshot_preserves_stats_and_invalidates_removed(
    memory_db,
):
    client = await memory_db.index._get_client()
    candidates = await old_records(memory_db, count=2)
    before_generations = [await generations(client, memory) for memory in candidates]
    assert await memory_db.compact_hash_group(candidates) == 1
    survivor = candidates[1]
    assert not await client.exists(Keys.memory_key(candidates[0].id, "orchard", "alice"))
    key = Keys.memory_key(survivor.id, "orchard", "alice")
    assert await client.hget(key, "access_count") == "5"
    assert set((await client.hget(key, "topics")).split(",")) == {"topic-0", "topic-1"}
    assert json.loads(await client.hget(key, "metadata")) == {
        "field-0": 0,
        "field-1": 1,
    }
    after = [await generations(client, memory) for memory in candidates]
    assert after[0][0] == str(int(before_generations[0][0] or "0") + 1)
    assert after[0][1:] == before_generations[0][1:]
    assert after[1] == before_generations[1]


async def test_hash_compaction_missing_original_snapshot_refuses(memory_db):
    client = await memory_db.index._get_client()
    candidates = await old_records(memory_db, count=2)
    before_hashes = [await hash_snapshot(client, memory) for memory in candidates]
    before_generations = [await generations(client, memory) for memory in candidates]
    candidates[0]._source_fields = None
    assert await memory_db.compact_hash_group(candidates) == 0
    assert [await hash_snapshot(client, memory) for memory in candidates] == before_hashes
    assert [await generations(client, memory) for memory in candidates] == before_generations
