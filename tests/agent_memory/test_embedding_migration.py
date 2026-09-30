"""Actual re-embedding publication and rollback against two-shard Dragonfly."""

import copy
import json

import pytest
from redisvl.index import AsyncSearchIndex
from redisvl.query import VectorQuery

from agent_memory_server.config import settings
from agent_memory_server.memory_vector_db import RedisVLMemoryVectorDatabase
from agent_memory_server.memory_vector_db_factory import _build_redis_schema
from agent_memory_server.models import MemoryRecord
from agent_memory_server.utils.embedding_migration import (
    _receipt_key,
    apply_embedding_migration,
    prepare_embedding_migration,
    rollback_embedding_migration,
)
from agent_memory_server.utils.index_contract import require_index_contract
from agent_memory_server.utils.keys import Keys


class CountingEmbeddings:
    def __init__(self, dims):
        self.dims = dims
        self.calls = 0

    async def aembed_documents(self, texts):
        self.calls += 1
        return [[0.0] * (self.dims - 1) + [1.0] for _ in texts]

    async def aembed_query(self, text):
        return [0.0] * (self.dims - 1) + [1.0]


@pytest.fixture
async def migration(redis_client, monkeypatch):
    monkeypatch.setattr(settings, "redisvl_vector_dimensions", 3)
    monkeypatch.setattr(settings, "embedding_model", "source-model")
    monkeypatch.setattr(settings, "embedding_model_version", "1")
    monkeypatch.setattr(settings, "redisvl_indexing_algorithm", "flat")
    old = RedisVLMemoryVectorDatabase(
        AsyncSearchIndex.from_dict(_build_redis_schema(), redis_client=redis_client),
        CountingEmbeddings(3),
    )
    memories = [
        MemoryRecord(id=str(i), text=f"orchard {i}", namespace="tenant", user_id="alice")
        for i in range(3)
    ]
    await old.add_memories(memories)
    keys = [Keys.memory_key(memory.id, "tenant", "alice") for memory in memories]
    snapshots = [
        await redis_client.execute_command("HGETALL", key, NEVER_DECODE=True) for key in keys
    ]
    source_contract = await redis_client.get(f"memory_index_contract:{old.index.name}")
    monkeypatch.setattr(settings, "embedding_model", "target-model")
    monkeypatch.setattr(settings, "embedding_model_version", "2")
    schema = copy.deepcopy(old.index.schema.to_dict())
    vector = next(field for field in schema["fields"] if field["name"] == "vector")
    vector["attrs"].update(dims=4, datatype="float64", algorithm="hnsw")
    target = RedisVLMemoryVectorDatabase(
        AsyncSearchIndex.from_dict(schema, redis_client=redis_client),
        CountingEmbeddings(4),
    )
    return old, target, keys, snapshots, source_contract


async def prepare(target, keys):
    first = await prepare_embedding_migration(target, "batch-1", keys[:2])
    second = await prepare_embedding_migration(target, "batch-2", keys[2:])
    assert await prepare_embedding_migration(target, "batch-1", keys[:2]) == first
    assert first["state"] == second["state"] == "prepared"
    assert target.embeddings.calls == 2
    return ["batch-1", "batch-2"]


async def test_embedding_migration_changes_model_dimension_and_rolls_back(migration, redis_client):
    old, target, keys, snapshots, source_contract = migration
    ids = await prepare(target, keys)
    for key, snapshot in zip(keys, snapshots, strict=True):
        assert await redis_client.execute_command("HGETALL", key, NEVER_DECODE=True) == snapshot
    receipt = await apply_embedding_migration(target, "all-documents", ids)
    assert receipt["state"] == "completed"
    assert target.embeddings.calls == 2
    assert await apply_embedding_migration(target, "all-documents", ids) == receipt
    result = await target.search_memories("orchard", distance_threshold=0.01)
    assert {record.id for record in result.memories} == {"0", "1", "2"}
    for key, snapshot in zip(keys, snapshots, strict=True):
        before = dict(snapshot)
        raw = await redis_client.execute_command("HGETALL", key, NEVER_DECODE=True)
        after = dict(raw)
        assert len(after[b"vector"]) == 32
        after.pop(b"vector")
        before.pop(b"vector")
        assert after == before
    reverted = await rollback_embedding_migration(target, "all-documents")
    assert reverted["state"] == "rolled_back"
    assert await rollback_embedding_migration(target, "all-documents") == reverted
    assert await redis_client.get(f"memory_index_contract:{old.index.name}") == source_contract
    for key, snapshot in zip(keys, snapshots, strict=True):
        raw = await redis_client.execute_command("HGETALL", key, NEVER_DECODE=True)
        assert dict(raw) == dict(snapshot)
    rows = await old.index.query(
        VectorQuery(
            vector=[0, 0, 1],
            vector_field_name="vector",
            return_fields=["id_"],
            num_results=3,
        )
    )
    assert {row["id_"] for row in rows} == {"0", "1", "2"}
    assert not await redis_client.exists(f"memory_index_rebuild_lock:{old.index.name}")


@pytest.mark.parametrize("change", ["text", "generation", "delete", "omitted", "new"])
async def test_embedding_migration_cas_rejects_changed_or_missing_sources(
    migration, redis_client, change
):
    old, target, keys, snapshots, _ = migration
    ids = await prepare(target, keys)
    if change == "text":
        await redis_client.hset(keys[0], "text", "changed source")
    elif change == "generation":
        await redis_client.incr(Keys.memory_generation_keys("0", "tenant", "alice")[0])
    elif change == "delete":
        await redis_client.delete(keys[0])
    elif change == "new":
        await redis_client.hset(Keys.memory_key("new", "tenant", "alice"), mapping={"text": "new"})
    else:
        ids = ids[:1]
    with pytest.raises(ValueError, match="source|cover"):
        await apply_embedding_migration(target, "rejected", ids)
    assert target.embeddings.calls == 2
    raw = await redis_client.execute_command("HGET", keys[1], "vector", NEVER_DECODE=True)
    before = dict(snapshots[1])
    assert raw == before[b"vector"]
    assert await redis_client.exists(f"memory_index_rebuild_lock:{old.index.name}")
    manifest = json.loads(
        await redis_client.get(_receipt_key(target.index.name, "rejected", manifest=True))
    )
    changed = [
        await redis_client.execute_command("HGETALL", key, NEVER_DECODE=True) for key in keys
    ]
    assert (await rollback_embedding_migration(target, "rejected", manifest["owner"]))[
        "state"
    ] == "rolled_back"
    assert [
        await redis_client.execute_command("HGETALL", key, NEVER_DECODE=True) for key in keys
    ] == changed
    assert not await redis_client.exists(f"memory_index_rebuild_lock:{old.index.name}")


@pytest.mark.parametrize("phase", ["vectors_applied", "index_ready"])
async def test_embedding_migration_lost_response_resumes_without_model(
    migration, redis_client, monkeypatch, phase
):
    _, target, keys, _, _ = migration
    ids = await prepare(target, keys)
    evaluate = redis_client.eval
    injected = False

    async def lose_response(script, numkeys, *args):
        nonlocal injected
        value = await evaluate(script, numkeys, *args)
        raw = await redis_client.get(_receipt_key(target.index.name, "lost", manifest=True))
        if not injected and raw is not None and json.loads(raw)["state"] == phase:
            injected = True
            raise RuntimeError("response lost after durable mutation")
        return value

    monkeypatch.setattr(redis_client, "eval", lose_response)
    with pytest.raises(RuntimeError, match="response lost"):
        await apply_embedding_migration(target, "lost", ids)
    assert await redis_client.exists(f"memory_index_rebuild_lock:{target.index.name}")
    monkeypatch.setattr(redis_client, "eval", evaluate)
    journal = json.loads(
        await redis_client.get(_receipt_key(target.index.name, "lost", manifest=True))
    )
    with pytest.raises(ValueError, match="owner"):
        await apply_embedding_migration(target, "lost", ids)
    receipt = await apply_embedding_migration(target, "lost", ids, journal["owner"])
    assert receipt["state"] == "completed"
    assert target.embeddings.calls == 2
    result = await target.search_memories("orchard")
    assert {record.id for record in result.memories} == {"0", "1", "2"}


async def test_embedding_migration_failed_index_create_restores_original(
    migration, redis_client, monkeypatch
):
    old, target, keys, snapshots, source_contract = migration
    ids = await prepare(target, keys)
    create = target.index.create

    async def fail(**kwargs):
        raise RuntimeError("index creation interrupted")

    monkeypatch.setattr(target.index, "create", fail)
    with pytest.raises(RuntimeError, match="index creation"):
        await apply_embedding_migration(target, "failed", ids)
    journal = json.loads(
        await redis_client.get(_receipt_key(target.index.name, "failed", manifest=True))
    )
    assert journal["state"] == "vectors_applied"
    with pytest.raises(ValueError, match="owner"):
        await rollback_embedding_migration(target, "failed", "wrong")
    monkeypatch.setattr(target.index, "create", create)
    assert (await rollback_embedding_migration(target, "failed", journal["owner"]))[
        "state"
    ] == "rolled_back"
    assert await redis_client.get(f"memory_index_contract:{old.index.name}") == source_contract
    for key, snapshot in zip(keys, snapshots, strict=True):
        raw = await redis_client.execute_command("HGETALL", key, NEVER_DECODE=True)
        assert dict(raw) == dict(snapshot)
    profile = json.loads(source_contract)
    await require_index_contract(
        old.index, profile["embedding_model"], profile["embedding_model_version"]
    )


@pytest.mark.parametrize("change", ["text", "generation", "delete"])
async def test_embedding_migration_rollback_never_overwrites_new_or_deleted_memory(
    migration, redis_client, change
):
    _, target, keys, _, _ = migration
    ids = await prepare(target, keys)
    await apply_embedding_migration(target, "complete", ids)
    if change == "text":
        await redis_client.hset(keys[0], "text", "new memory update")
    elif change == "generation":
        await redis_client.incr(Keys.memory_generation_keys("0", "tenant", "alice")[0])
    else:
        await redis_client.delete(keys[0])
    before = [await redis_client.execute_command("HGETALL", key, NEVER_DECODE=True) for key in keys]
    with pytest.raises(ValueError, match="source|cover"):
        await rollback_embedding_migration(target, "complete")
    after = [await redis_client.execute_command("HGETALL", key, NEVER_DECODE=True) for key in keys]
    assert after == before


async def test_embedding_migration_preparation_rejects_bad_output_without_receipt(
    migration, redis_client, monkeypatch
):
    _, target, keys, snapshots, _ = migration

    async def invalid(texts):
        return [[1.0, 0.0, 0.0] for _ in texts]

    monkeypatch.setattr(target.embeddings, "aembed_documents", invalid)
    with pytest.raises(ValueError, match="dimension"):
        await prepare_embedding_migration(target, "bad-output", keys)
    assert not await redis_client.exists(_receipt_key(target.index.name, "bad-output"))
    assert [
        await redis_client.execute_command("HGETALL", key, NEVER_DECODE=True) for key in keys
    ] == snapshots


async def test_embedding_migration_explicit_known_legacy_adoption_then_real_migration(
    migration, redis_client
):
    from agent_memory_server.utils.embedding_migration import (
        adopt_unversioned_embedding_contract,
    )

    old, target, keys, snapshots, source_contract = migration
    contract_key = f"memory_index_contract:{old.index.name}"
    await redis_client.delete(contract_key)
    with pytest.raises(ValueError, match="Unversioned"):
        await prepare_embedding_migration(target, "unknown-old", keys)
    receipt = await adopt_unversioned_embedding_contract(old, "known-source", "source-model", "1")
    assert receipt["state"] == "completed"
    assert await redis_client.get(contract_key) == source_contract
    assert [
        await redis_client.execute_command("HGETALL", key, NEVER_DECODE=True) for key in keys
    ] == snapshots
    assert (
        await adopt_unversioned_embedding_contract(old, "known-source", "source-model", "1")
        == receipt
    )
    ids = await prepare(target, keys)
    assert (await apply_embedding_migration(target, "legacy-to-new", ids))["state"] == "completed"
    assert {record.id for record in (await target.search_memories("orchard")).memories} == {
        "0",
        "1",
        "2",
    }


async def test_embedding_migration_unknown_legacy_model_cannot_be_implicitly_adopted(
    migration, redis_client
):
    from agent_memory_server.utils.embedding_migration import (
        adopt_unversioned_embedding_contract,
    )

    old, _, _, _, _ = migration
    key = f"memory_index_contract:{old.index.name}"
    await redis_client.delete(key)
    with pytest.raises(ValueError, match="assertions"):
        await adopt_unversioned_embedding_contract(old, "unknown", "", "1")
    assert not await redis_client.exists(key)


async def test_embedding_migration_rollback_response_lost_can_resume(
    migration, redis_client, monkeypatch
):
    _, target, keys, snapshots, _ = migration
    ids = await prepare(target, keys)
    await apply_embedding_migration(target, "restore-retry", ids)
    evaluate = redis_client.eval
    injected = False

    async def lose_response(script, numkeys, *args):
        nonlocal injected
        value = await evaluate(script, numkeys, *args)
        raw = await redis_client.get(
            _receipt_key(target.index.name, "restore-retry", manifest=True)
        )
        if not injected and raw and json.loads(raw)["state"] == "vectors_restored":
            injected = True
            raise RuntimeError("rollback response lost")
        return value

    monkeypatch.setattr(redis_client, "eval", lose_response)
    with pytest.raises(RuntimeError, match="response lost"):
        await rollback_embedding_migration(target, "restore-retry")
    monkeypatch.setattr(redis_client, "eval", evaluate)
    journal = json.loads(
        await redis_client.get(_receipt_key(target.index.name, "restore-retry", manifest=True))
    )
    assert (await rollback_embedding_migration(target, "restore-retry", journal["owner"]))[
        "state"
    ] == "rolled_back"
    assert [
        await redis_client.execute_command("HGETALL", key, NEVER_DECODE=True) for key in keys
    ] == snapshots


@pytest.mark.parametrize("phase", ["prepared", "vectors_applied", "index_ready"])
@pytest.mark.parametrize("recovery", ["resume", "rollback"])
async def test_embedding_migration_sigkill_all_phases(migration, redis_client, phase, recovery):
    import asyncio
    import subprocess
    import sys

    old, target, keys, snapshots, source_contract = migration
    ids = await prepare(target, keys)
    schema = target.index.schema.to_dict()
    conn = redis_client.connection_pool.connection_kwargs
    url = f"redis://{conn['host']}:{conn['port']}"
    marker = f"migration-kill-ready:{phase}"
    code = """
import asyncio,json,sys
from redis.asyncio import Redis
from redisvl.index import AsyncSearchIndex
from agent_memory_server.config import settings
from agent_memory_server.memory_vector_db import RedisVLMemoryVectorDatabase
from agent_memory_server.utils.embedding_migration import apply_embedding_migration,_receipt_key
async def main():
    client=Redis.from_url(sys.argv[1],decode_responses=True)
    schema,phase,marker=json.loads(sys.argv[2]),sys.argv[3],sys.argv[4]
    settings.embedding_model="target-model"
    settings.embedding_model_version="2"
    db=RedisVLMemoryVectorDatabase(AsyncSearchIndex.from_dict(schema,redis_client=client),None)
    evaluate=client.eval
    async def pause_after_mutation(script,numkeys,*args):
        result=await evaluate(script,numkeys,*args)
        raw=await client.get(_receipt_key(db.index.name,"kill-migration",manifest=True))
        if raw is not None and json.loads(raw)["state"] == phase:
            await client.set(marker,"ready")
            await asyncio.Event().wait()
        return result
    client.eval=pause_after_mutation
    await apply_embedding_migration(db,"kill-migration",["batch-1","batch-2"])
asyncio.run(main())
"""
    child = subprocess.Popen(
        [sys.executable, "-c", code, url, json.dumps(schema), phase, marker],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        for _ in range(200):
            if await redis_client.get(marker):
                break
            if child.poll() is not None:
                pytest.fail(child.communicate()[1].decode())
            await asyncio.sleep(0.05)
        else:
            pytest.fail("Migration child did not reach selected phase")
        child.kill()
        await asyncio.to_thread(child.wait, 5)
        assert child.returncode == -9
        manifest_key = _receipt_key(target.index.name, "kill-migration", manifest=True)
        journal = json.loads(await redis_client.get(manifest_key))
        assert journal["state"] == phase
        assert (
            await redis_client.get(f"memory_index_rebuild_lock:{target.index.name}")
            == journal["owner"]
        )
        with pytest.raises(ValueError, match="unavailable"):
            await old.list_memories(limit=1)
        if recovery == "resume":
            receipt = await apply_embedding_migration(
                target, "kill-migration", ids, journal["owner"]
            )
            assert receipt["state"] == "completed"
            result = await target.search_memories("orchard")
            assert {record.id for record in result.memories} == {"0", "1", "2"}
        else:
            receipt = await rollback_embedding_migration(target, "kill-migration", journal["owner"])
            assert receipt["state"] == "rolled_back"
            assert (
                await redis_client.get(f"memory_index_contract:{old.index.name}") == source_contract
            )
            assert [
                await redis_client.execute_command("HGETALL", key, NEVER_DECODE=True)
                for key in keys
            ] == snapshots
            result = await old.index.query(
                VectorQuery(
                    vector=[0, 0, 1],
                    vector_field_name="vector",
                    return_fields=["id_"],
                    num_results=3,
                )
            )
            assert {row["id_"] for row in result} == {"0", "1", "2"}
        assert target.embeddings.calls == 2
        assert not await redis_client.exists(f"memory_index_rebuild_lock:{target.index.name}")
    finally:
        if child.poll() is None:
            child.kill()
        await asyncio.to_thread(child.wait, 5)


async def test_embedding_migration_retired_cached_provider_cannot_publish_old_layout(
    migration, redis_client
):
    old, target, keys, _, _ = migration
    ids = await prepare(target, keys)
    await apply_embedding_migration(target, "new-epoch", ids)
    before = [await redis_client.execute_command("HGETALL", key, NEVER_DECODE=True) for key in keys]
    with pytest.raises(ValueError, match="contract|configuration|migration"):
        await old.add_memories(
            [
                MemoryRecord(
                    id="old-provider",
                    text="orchard",
                    namespace="tenant",
                    user_id="alice",
                )
            ]
        )
    with pytest.raises(ValueError, match="contract|configuration|migration"):
        await old.search_memories("orchard")
    assert not await redis_client.exists(Keys.memory_key("old-provider", "tenant", "alice"))
    assert [
        await redis_client.execute_command("HGETALL", key, NEVER_DECODE=True) for key in keys
    ] == before


@pytest.mark.parametrize("existing_index", [False, True])
async def test_embedding_migration_unversioned_empty_rebuild_recovers(
    redis_client, monkeypatch, existing_index
):
    from agent_memory_server.utils.embedding_migration import (
        recover_unversioned_empty_index,
    )

    monkeypatch.setattr(settings, "redisvl_vector_dimensions", 3)
    monkeypatch.setattr(settings, "embedding_model", "empty-model")
    monkeypatch.setattr(settings, "embedding_model_version", "1")
    monkeypatch.setattr(settings, "redisvl_indexing_algorithm", "flat")
    index = AsyncSearchIndex.from_dict(_build_redis_schema(), redis_client=redis_client)
    if existing_index:
        await index.create()
    db = RedisVLMemoryVectorDatabase(index, CountingEmbeddings(3))
    profile = {
        "schema_version": 1,
        "schema": index.schema.to_dict(),
        "embedding_model": "empty-model",
        "embedding_model_version": "1",
    }
    journal = {
        "version": 1,
        "owner": "stopped-empty-owner",
        "previous": False,
        "target": json.dumps(profile, sort_keys=True),
    }
    journal_key = f"memory_index_rebuild_journal:{index.name}"
    lock_key = f"memory_index_rebuild_lock:{index.name}"
    await redis_client.set(journal_key, json.dumps(journal))
    await redis_client.set(lock_key, journal["owner"])
    with pytest.raises(ValueError, match="owner"):
        await recover_unversioned_empty_index(db, "wrong-owner")
    await recover_unversioned_empty_index(db, journal["owner"])
    assert not await redis_client.exists(lock_key, journal_key)
    assert (await db.list_memories(limit=1)).total == 0
    assert db.embeddings.calls == 0
    await require_index_contract(index, "empty-model", "1")


async def test_embedding_migration_empty_rebuild_refuses_unindexed_legacy_hash(
    redis_client, monkeypatch
):
    from agent_memory_server.utils.embedding_migration import (
        recover_unversioned_empty_index,
    )

    monkeypatch.setattr(settings, "redisvl_vector_dimensions", 3)
    monkeypatch.setattr(settings, "embedding_model", "empty-model")
    index = AsyncSearchIndex.from_dict(_build_redis_schema(), redis_client=redis_client)
    await index.create()
    db = RedisVLMemoryVectorDatabase(index, CountingEmbeddings(3))
    key = Keys.memory_key("incomplete-old", None, None)
    await redis_client.hset(key, "text", "invalid old document without vector")
    await index.delete(drop=False)
    assert not await index.exists()
    profile = {
        "schema_version": 1,
        "schema": index.schema.to_dict(),
        "embedding_model": "empty-model",
        "embedding_model_version": "1",
    }
    journal = {
        "version": 1,
        "owner": "old-owner",
        "previous": False,
        "target": json.dumps(profile, sort_keys=True),
    }
    await redis_client.set(f"memory_index_rebuild_journal:{index.name}", json.dumps(journal))
    await redis_client.set(f"memory_index_rebuild_lock:{index.name}", journal["owner"])
    with pytest.raises(ValueError, match="Nonempty"):
        await recover_unversioned_empty_index(db, journal["owner"])
    assert await redis_client.hget(key, "text") == "invalid old document without vector"
    assert not await redis_client.exists(f"memory_index_contract:{index.name}")
    assert await redis_client.get(f"memory_index_rebuild_lock:{index.name}") == journal["owner"]


@pytest.mark.parametrize("change", ["model", "model_version"])
async def test_embedding_migration_model_identity_changes_with_same_vector_layout(
    migration, redis_client, monkeypatch, change
):
    old, _, keys, _, source_contract = migration
    monkeypatch.setattr(
        settings,
        "embedding_model",
        "source-model" if change == "model_version" else "target-model",
    )
    monkeypatch.setattr(
        settings, "embedding_model_version", "2" if change == "model_version" else "1"
    )
    target = RedisVLMemoryVectorDatabase(
        AsyncSearchIndex.from_dict(old.index.schema.to_dict(), redis_client=redis_client),
        CountingEmbeddings(3),
    )
    await prepare_embedding_migration(target, "same-layout-batch", keys)
    assert (await apply_embedding_migration(target, "same-layout", ["same-layout-batch"]))[
        "state"
    ] == "completed"
    assert target.embeddings.calls == 1
    assert {record.id for record in (await target.search_memories("orchard")).memories} == {
        "0",
        "1",
        "2",
    }
    with pytest.raises(ValueError, match="contract"):
        await old.search_memories("orchard")
    await rollback_embedding_migration(target, "same-layout")
    assert await redis_client.get(f"memory_index_contract:{old.index.name}") == source_contract
    assert {record.id for record in (await old.search_memories("orchard")).memories} == {
        "0",
        "1",
        "2",
    }


async def test_embedding_migration_legacy_identity_schema_migrates_and_restores_exact_definition(
    migration, redis_client
):
    from agent_memory_server.utils.embedding_migration import (
        adopt_unversioned_embedding_contract,
    )

    old, target, keys, snapshots, _ = migration
    legacy_schema = copy.deepcopy(old.index.schema.to_dict())
    for field in legacy_schema["fields"]:
        if field["type"] == "tag":
            field["attrs"]["case_sensitive"] = False
    legacy = AsyncSearchIndex.from_dict(legacy_schema, redis_client=redis_client)
    await legacy.create(overwrite=True, drop=False)
    await redis_client.delete(f"memory_index_contract:{old.index.name}")
    source_provider = RedisVLMemoryVectorDatabase(legacy, None)
    with pytest.raises(ValueError, match="case sensitivity"):
        await adopt_unversioned_embedding_contract(old, "wrong-source-schema", "source-model", "1")
    # A mistaken source schema is rejected before claiming maintenance or
    # recording a contract; the correct historical schema can proceed safely.
    assert not await redis_client.exists(f"memory_index_contract:{old.index.name}")
    assert not await redis_client.exists(f"memory_index_rebuild_lock:{old.index.name}")
    await adopt_unversioned_embedding_contract(
        source_provider, "actual-source-schema", "source-model", "1"
    )
    source_contract = await redis_client.get(f"memory_index_contract:{old.index.name}")
    assert json.loads(source_contract)["schema"] == legacy.schema.to_dict()
    ids = await prepare(target, keys)
    await apply_embedding_migration(target, "identity-schema-upgrade", ids)
    assert {record.id for record in (await target.search_memories("orchard")).memories} == {
        "0",
        "1",
        "2",
    }
    await rollback_embedding_migration(target, "identity-schema-upgrade")
    await require_index_contract(legacy, "source-model", "1")
    assert [
        await redis_client.execute_command("HGETALL", key, NEVER_DECODE=True) for key in keys
    ] == snapshots


@pytest.mark.parametrize("phase", ["drop", "create", "contract"])
async def test_embedding_migration_unversioned_empty_real_sigkill_recovery(
    redis_client, monkeypatch, phase
):
    import asyncio
    import subprocess
    import sys

    monkeypatch.setattr(settings, "redisvl_vector_dimensions", 3)
    monkeypatch.setattr(settings, "embedding_model", "empty-model")
    monkeypatch.setattr(settings, "embedding_model_version", "1")
    index = AsyncSearchIndex.from_dict(_build_redis_schema(), redis_client=redis_client)
    await index.create()
    schema = index.schema.to_dict()
    conn = redis_client.connection_pool.connection_kwargs
    url = f"redis://{conn['host']}:{conn['port']}"
    marker = "empty-real-sigkill"
    code = """
import asyncio,json,sys
from redis.asyncio import Redis
from redisvl.index import AsyncSearchIndex
from agent_memory_server.config import settings
from agent_memory_server.memory_vector_db import RedisVLMemoryVectorDatabase
async def main():
    client=Redis.from_url(sys.argv[1],decode_responses=True)
    schema,phase=json.loads(sys.argv[2]),sys.argv[3]
    settings.embedding_model="empty-model"
    settings.embedding_model_version="1"
    index=AsyncSearchIndex.from_dict(schema,redis_client=client)
    db=RedisVLMemoryVectorDatabase(index,None)
    create=index.create
    async def pause():
        await client.set("empty-real-sigkill","ready")
        await asyncio.Event().wait()
    async def interrupted(**kwargs):
        if phase == "drop":
            await index.delete(drop=False)
        else:
            await create(**kwargs)
        if phase != "contract":
            await pause()
    index.create=interrupted
    if phase == "contract":
        db._ensure_index=pause
    await db.rebuild_index()
asyncio.run(main())
"""
    child = subprocess.Popen(
        [sys.executable, "-c", code, url, json.dumps(schema), phase],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        for _ in range(200):
            if await redis_client.get(marker):
                break
            if child.poll() is not None:
                pytest.fail(child.communicate()[1].decode())
            await asyncio.sleep(0.05)
        else:
            pytest.fail("Empty rebuild process did not reach expected phase")
        child.kill()
        await asyncio.to_thread(child.wait, 5)
        assert child.returncode == -9
        journal = json.loads(await redis_client.get(f"memory_index_rebuild_journal:{index.name}"))
        assert journal["previous"] is False
        db = RedisVLMemoryVectorDatabase(index, CountingEmbeddings(3))
        await db.recover_index_rebuild(journal["owner"])
        assert not await redis_client.exists(
            f"memory_index_rebuild_journal:{index.name}", f"memory_index_rebuild_lock:{index.name}"
        )
        assert json.loads(
            await redis_client.get(f"memory_index_contract:{index.name}")
        ) == json.loads(journal["target"])
        assert (await db.list_memories(limit=1)).total == 0
        assert db.embeddings.calls == 0
    finally:
        if child.poll() is None:
            child.kill()
        await asyncio.to_thread(child.wait, 5)
