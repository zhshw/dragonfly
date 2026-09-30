"""Test V0's actual schemas, codecs and queries, without model API calls."""

import asyncio
import math
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
import json

import pytest
from redis.asyncio import Redis
from conftest import SCHEDULER_SHA
from test_docket import run_worker
from docket import Docket
from pydantic import ValidationError
from redisvl.index import AsyncSearchIndex
from redisvl.exceptions import RedisSearchError
from redisvl.query import FilterQuery, VectorQuery
from redisvl.query.filter import Tag

from agent_memory_server import (
    extraction,
    memory_strategies,
    memory_vector_db_factory,
    migrations,
)
from agent_memory_server import long_term_memory, summarization, working_memory
from agent_memory_server.config import settings
from agent_memory_server.dependencies import HybridBackgroundTasks
from agent_memory_server.filters import Namespace, Topics
from agent_memory_server.memory_vector_db import RedisVLMemoryVectorDatabase
from agent_memory_server.memory_vector_db_factory import _build_redis_schema
from agent_memory_server.models import MemoryRecord, SearchModeEnum
from agent_memory_server.utils.keys import Keys
from agent_memory_server.utils.recency import rerank_with_recency
from agent_memory_server.utils.redis_query import RecencyAggregationQuery
from agent_memory_server.working_memory_index import get_working_memory_index


class FixedEmbeddings:
    vectors = {
        "recent orchard": [1.0, 0.0, 0.0],
        "old orchard": [1.0, 0.0, 0.0],
        "other ocean": [0.0, 1.0, 0.0],
    }

    async def aembed_documents(self, texts):
        return [self.vectors[text] for text in texts]

    async def aembed_query(self, text):
        return [1.0, 0.0, 0.0]


@pytest.fixture(params=["flat", "hnsw"])
async def memory_db(redis_client, monkeypatch, request):
    monkeypatch.setattr(settings, "redisvl_vector_dimensions", 3)
    monkeypatch.setattr(settings, "embedding_model", "test-fixed-3d")
    monkeypatch.setattr(settings, "redisvl_indexing_algorithm", request.param)
    monkeypatch.setattr(settings, "redisvl_distance_metric", "cosine")
    index = AsyncSearchIndex.from_dict(_build_redis_schema(), redis_client=redis_client)
    db = RedisVLMemoryVectorDatabase(index, FixedEmbeddings())
    await db._ensure_index()
    yield db


async def seed(db):
    now = datetime.now(UTC)
    records = [
        MemoryRecord(
            id="recent",
            text="recent orchard",
            namespace="tenant-a",
            user_id="alice",
            topics=["fruit tree", "a-b"],
            created_at=now,
            last_accessed=now,
        ),
        MemoryRecord(
            id="old",
            text="old orchard",
            namespace="tenant-a",
            user_id="alice",
            created_at=now - timedelta(days=60),
            last_accessed=now - timedelta(days=14),
        ),
        MemoryRecord(id="other", text="other ocean", namespace="tenant-b", user_id="bob"),
    ]
    assert await db.add_memories(records) == ["recent", "old", "other"]
    return records


async def test_working_memory_schema(redis_client):
    index = await get_working_memory_index(redis_client)
    await index.create()
    document = {
        "session_id": "session-a",
        "namespace": "tenant-a",
        "user_id": "alice",
        "created_at": 1,
        "updated_at": 2,
        "messages": [{"role": "user", "content": "hello"}],
    }
    keys = await index.load([document], keys=["working_memory:tenant-a:alice:session-a"])
    assert await redis_client.json().get(keys[0]) == document
    query = FilterQuery(filter_expression=Tag("namespace") == "tenant-a", return_fields=["$"])
    rows = await index.query(query)
    assert len(rows) == 1
    document["updated_at"] = 3
    await index.load([document], keys=keys)
    assert (await redis_client.json().get(keys[0]))["updated_at"] == 3
    assert await redis_client.delete(*keys) == 1
    assert await index.query(query) == []


@pytest.mark.parametrize(
    "mode", [SearchModeEnum.SEMANTIC, SearchModeEnum.KEYWORD, SearchModeEnum.HYBRID]
)
async def test_memory_queries(memory_db, mode):
    await seed(memory_db)
    result = await memory_db.search_memories(
        "orchard", search_mode=mode, namespace=Namespace(eq="tenant-a")
    )
    assert {row.id for row in result.memories} == {"recent", "old"}
    assert all(row.namespace == "tenant-a" and math.isfinite(row.score) for row in result.memories)
    assert await memory_db.delete_memories(["old"]) == 1
    remaining = await memory_db.search_memories("orchard", namespace=Namespace(eq="tenant-a"))
    assert [row.id for row in remaining.memories] == ["recent"]
    updated = MemoryRecord(id="recent", text="old orchard", namespace="tenant-a", user_id="alice")
    await memory_db.update_memories([updated])
    read_back = await memory_db.list_memories(namespace=Namespace(eq="tenant-a"))
    assert read_back.memories[0].text == "old orchard"


async def test_vector_range_and_tag_encoding(memory_db):
    await seed(memory_db)
    result = await memory_db.search_memories("orchard", distance_threshold=0.1)
    assert {row.id for row in result.memories} == {"recent", "old"}
    assert all(row.dist == pytest.approx(0.0, abs=1e-6) for row in result.memories)
    tagged = await memory_db.search_memories("orchard", topics=Topics(any=["fruit tree"]))
    assert [row.id for row in tagged.memories] == ["recent"]
    assert tagged.memories[0].topics == ["fruit tree", "a-b"]
    assert not (await memory_db.search_memories("orchard", namespace=Namespace(eq="none"))).memories


async def test_recency_contract_and_pagination(memory_db):
    await seed(memory_db)
    # Calling the aggregation method directly makes fallback impossible to hide.
    first = await memory_db._search_with_recency_aggregation(
        "orchard", Tag("namespace") == "tenant-a", 1, 0, None, {"candidate_limit": 10}
    )
    second = await memory_db._search_with_recency_aggregation(
        "orchard", Tag("namespace") == "tenant-a", 1, 1, None, {"candidate_limit": 10}
    )
    assert [m.id for m in first.memories] == ["recent"]
    assert first.memories[0].dist == pytest.approx(0.0, abs=1e-6)
    assert first.next_offset == 1
    assert [m.id for m in second.memories] == ["old"]
    assert second.next_offset is None
    assert first.memories[0].score > second.memories[0].score
    assert first.memories[0].score == pytest.approx(1.0, abs=1e-5)
    assert second.memories[0].score == pytest.approx(0.85, abs=1e-5)
    plain = await memory_db.search_memories("orchard", namespace=Namespace(eq="tenant-a"))
    expected = rerank_with_recency(plain.memories, now=datetime.now(UTC), params={})
    assert [m.id for m in expected] == [m.id for m in first.memories + second.memories]


async def test_recency_missing_future_and_ties(memory_db):
    await seed(memory_db)
    client = await memory_db.index._get_client()
    await client.hdel(Keys.memory_key("old", "tenant-a", "alice"), "created_at", "last_accessed")
    future = int((datetime.now(UTC) + timedelta(days=1)).timestamp())
    await client.hset(
        Keys.memory_key("recent", "tenant-a", "alice"),
        mapping={"created_at": future, "last_accessed": future},
    )
    result = await memory_db._search_with_recency_aggregation(
        "orchard", Tag("namespace") == "tenant-a", 10, 0, None, {}
    )
    assert [m.id for m in result.memories] == ["recent", "old"]
    assert result.memories[0].score == pytest.approx(1.0)
    assert result.memories[1].score == pytest.approx(0.8)
    await client.hset(
        Keys.memory_key("old", "tenant-a", "alice"),
        mapping={"created_at": future, "last_accessed": future},
    )
    tied = await memory_db._search_with_recency_aggregation(
        "orchard", Tag("namespace") == "tenant-a", 10, 0, None, {}
    )
    assert [m.id for m in tied.memories] == ["old", "recent"]


async def test_recency_range(memory_db):
    await seed(memory_db)
    result = await memory_db._search_with_recency_aggregation("orchard", None, 10, 0, 0.1, {})
    assert [m.id for m in result.memories] == ["recent", "old"]


def test_recency_args():
    vector_query = VectorQuery(vector=[1.0, 0.0, 0.0], vector_field_name="vector", num_results=10)
    query = (
        RecencyAggregationQuery.from_vector_query(
            vector_query, filter_expression=Tag("namespace") == "tenant-a"
        )
        .load_default_fields()
        .apply_recency(now_ts=604800)
        .sort_by_boosted_desc()
        .paginate(1, 2)
    )
    args = query.build_args()
    assert isinstance(args[0], str)
    assert "@namespace:{tenant\\-a}" in args[0]
    assert not any(isinstance(arg, (list, tuple)) for arg in args)
    assert "FILTER" not in args
    assert query.params["vector"] == vector_query.params["vector"]
    assert "@vector_distance" in args
    position = args.index("LIMIT")
    assert args[position + 1 : position + 3] == ["1", "2"]


def test_tag_comma_rejected():
    with pytest.raises(ValidationError, match="comma"):
        MemoryRecord(id="invalid", text="old orchard", topics=["fruit,tree"])


async def test_wrong_vector_dimensions(memory_db):
    await seed(memory_db)
    with pytest.raises(RedisSearchError, match="dimensions"):
        await memory_db.index.query(VectorQuery(vector=[1.0, 0.0], vector_field_name="vector"))


async def test_business_read_dedup_and_access(memory_db, monkeypatch):
    await seed(memory_db)

    async def get_db():
        return memory_db

    monkeypatch.setattr(long_term_memory, "get_memory_vector_db", get_db)
    client = await memory_db.index._get_client()
    old = await long_term_memory.get_long_term_memory_by_id("old")
    assert old.text == "old orchard"
    outcomes = await asyncio.gather(
        *(
            long_term_memory.update_last_accessed(
                ["old", "old"],
                redis_client=client,
                namespace="tenant-a",
                user_id="alice",
                expected_snapshots={"old": long_term_memory.memory_access_snapshot(old)},
            )
            for _ in range(2)
        )
    )
    assert sum(outcomes) == 1
    accessed = await long_term_memory.get_long_term_memory_by_id("old")
    assert accessed.access_count == old.access_count + 1
    assert accessed.last_accessed > old.last_accessed
    duplicate = MemoryRecord(
        id="duplicate", text="old orchard", namespace="tenant-a", user_id="alice"
    )
    record, was_duplicate = await long_term_memory.deduplicate_by_hash(
        duplicate, redis_client=client
    )
    assert was_duplicate and record is None
    assert await long_term_memory.delete_long_term_memories(["old"]) == 1
    assert await long_term_memory.get_long_term_memory_by_id("old") is None
    assert (
        await long_term_memory.update_last_accessed(
            ["old"],
            redis_client=client,
            namespace="tenant-a",
            user_id="alice",
            expected_snapshots={"old": long_term_memory.memory_access_snapshot(old)},
        )
        == 0
    )
    assert not await client.exists(Keys.memory_key("old", "tenant-a", "alice"))


async def test_identity_schema_mismatch_requires_non_destructive_rebuild(memory_db):
    await seed(memory_db)
    client = await memory_db.index._get_client()
    schema = _build_redis_schema()
    for field in schema["fields"]:
        if field["name"] in {"namespace", "user_id", "session_id", "id_"}:
            field["attrs"]["case_sensitive"] = False
    await memory_db.index.delete(drop=False)
    unsafe_index = AsyncSearchIndex.from_dict(schema, redis_client=client)
    await unsafe_index.create()
    db = RedisVLMemoryVectorDatabase(unsafe_index, FixedEmbeddings())
    with pytest.raises(ValueError, match="CASESENSITIVE"):
        await db.list_memories(namespace=Namespace(eq="tenant-a"))
    assert await client.exists(Keys.memory_key("recent", "tenant-a", "alice"))
    await unsafe_index.delete(drop=False)
    rebuilt = RedisVLMemoryVectorDatabase(
        AsyncSearchIndex.from_dict(_build_redis_schema(), redis_client=client),
        FixedEmbeddings(),
    )
    result = await rebuilt.list_memories(namespace=Namespace(eq="tenant-a"))
    assert {memory.id for memory in result.memories} == {"recent", "old"}


@pytest.mark.parametrize("policy", [{"max_age_days": 30}, {"budget": 1}])
async def test_forgetting_same_id_preserves_other_owners(
    memory_db, redis_client, monkeypatch, policy
):
    async def get_db():
        return memory_db

    monkeypatch.setattr(long_term_memory, "get_memory_vector_db", get_db)
    now = datetime.now(UTC)
    records = [
        MemoryRecord(
            id="shared",
            text="old orchard",
            namespace="tenant-a",
            user_id="alice",
            created_at=now - timedelta(days=60),
            last_accessed=now - timedelta(days=60),
        ),
        MemoryRecord(
            id="shared",
            text="old orchard",
            namespace="tenant-b",
            user_id="bob",
            created_at=now - timedelta(days=60),
            pinned=True,
        ),
        MemoryRecord(id="shared", text="recent orchard", created_at=now, last_accessed=now),
    ]
    await memory_db.add_memories(records)
    keys = [Keys.memory_key(m.id, m.namespace, m.user_id) for m in records]
    retained = [
        await redis_client.execute_command("HGETALL", key, NEVER_DECODE=True) for key in keys[1:]
    ]
    preview = await long_term_memory.forget_long_term_memories(policy, dry_run=True)
    assert preview["scanned"] == 3
    assert preview["deleted"] == 1
    assert all([await redis_client.exists(key) for key in keys])
    result = await long_term_memory.forget_long_term_memories(policy, dry_run=False)
    assert result["deleted"] == 1
    assert result["deleted_ids"] == ["shared"]
    assert not await redis_client.exists(keys[0])
    assert [
        await redis_client.execute_command("HGETALL", key, NEVER_DECODE=True) for key in keys[1:]
    ] == retained
    remaining = await memory_db.list_memories(limit=10)
    assert {(m.namespace, m.user_id, m.id) for m in remaining.memories} == {
        ("tenant-b", "bob", "shared"),
        (None, None, "shared"),
    }


async def test_exact_unowned_delete_scans_past_other_owners(memory_db, redis_client):
    records = [
        MemoryRecord(id="shared", text="old orchard", namespace=f"tenant-{i}", user_id="alice")
        for i in range(502)
    ]
    # Preserve the >500-row pagination probe while respecting production admission.
    for start in range(0, len(records), 100):
        await memory_db.add_memories(records[start : start + 100])
    query = FilterQuery(
        filter_expression=Tag("id_") == "shared",
        return_fields=["namespace"],
        num_results=500,
    )
    first_page = await memory_db._index.query(query)
    assert len(first_page) == 500
    first_keys = {row["id"] for row in first_page}
    target = next(
        m for m in records if Keys.memory_key(m.id, m.namespace, m.user_id) not in first_keys
    )
    target_key = Keys.memory_key(target.id, target.namespace, target.user_id)
    # Make a legacy-style unowned record beyond the first page. The key stays
    # unchanged: deletion must select and validate the actual document identity.
    await redis_client.hdel(target_key, "namespace", "user_id")
    assert target_key not in {row["id"] for row in await memory_db._index.query(query)}
    assert await memory_db.delete_memories(["shared"], exact_scope=True) == 1
    assert not await redis_client.exists(target_key)
    assert (
        sum(
            [
                await redis_client.exists(Keys.memory_key(m.id, m.namespace, m.user_id))
                for m in records
            ]
        )
        == 501
    )
    assert await memory_db.delete_memories(["shared"], exact_scope=True) == 0
    assert await memory_db.delete_memories(["shared"]) == 501
    assert await memory_db._index.query(query) == []


async def test_extraction_debounce_is_scoped_by_full_session_identity(redis_client, monkeypatch):
    monkeypatch.setattr(settings, "extraction_debounce_seconds", 60)
    owners = [
        ("orchard", "alice"),
        ("forest", "alice"),
        ("orchard", "bob"),
        (None, None),
        ("a:b", "c"),
        ("a", "b:c"),
    ]
    for namespace, user_id in owners:
        assert await long_term_memory.should_extract_session_thread(
            "shared", redis_client, namespace=namespace, user_id=user_id
        )
    await long_term_memory.set_extraction_debounce(
        "shared", redis_client, namespace="orchard", user_id="alice"
    )
    for namespace, user_id in owners:
        allowed = await long_term_memory.should_extract_session_thread(
            "shared", redis_client, namespace=namespace, user_id=user_id
        )
        assert allowed is ((namespace, user_id) != ("orchard", "alice"))
    for namespace, user_id in owners[1:]:
        await long_term_memory.set_extraction_debounce(
            "shared", redis_client, namespace=namespace, user_id=user_id
        )
    keys = [
        f"extraction_debounce:{Keys.working_memory_key('shared', user_id, namespace)}"
        for namespace, user_id in owners
    ]
    assert len(set(keys)) == len(owners)
    assert all([0 < await redis_client.ttl(key) <= 60 for key in keys])
    assert not await redis_client.exists("extraction_debounce:shared")


async def test_trailing_extraction_pending_and_task_keys_are_scoped(redis_client, monkeypatch):
    monkeypatch.setattr(settings, "use_docket", True)
    monkeypatch.setattr(settings, "extraction_debounce_seconds", 60)
    scheduled = []

    class Queue:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        def add(self, function, **options):
            async def enqueue(**payload):
                scheduled.append((options, payload))

            return enqueue

    monkeypatch.setattr(long_term_memory, "Docket", Queue)
    owners = [("orchard", "alice"), ("orchard", "bob"), ("forest", "alice")]
    for namespace, user_id in owners:
        await long_term_memory.schedule_trailing_extraction(
            "shared", namespace, user_id, redis_client
        )
    assert len(scheduled) == 3
    assert len({options["key"] for options, _ in scheduled}) == 3
    for (namespace, user_id), (options, payload) in zip(owners, scheduled, strict=True):
        identity = Keys.working_memory_key("shared", user_id, namespace)
        assert options["key"] == f"extraction:{identity}:{payload['scheduled_timestamp']}"
        assert (
            await redis_client.get(f"extraction_pending:{identity}")
            == payload["scheduled_timestamp"]
        )
        assert payload["namespace"] == namespace
        assert payload["user_id"] == user_id
    assert not await redis_client.exists("extraction_pending:shared")


@pytest.mark.parametrize("replacement", ["old-schedule", "new-schedule"])
async def test_old_extraction_cleanup_preserves_new_pending_schedule(
    redis_client, monkeypatch, replacement
):
    namespace, user_id, session_id = "orchard", "alice", "shared"
    key = f"extraction_pending:{Keys.working_memory_key(session_id, user_id, namespace)}"
    await redis_client.set(key, "old-schedule", ex=60)

    async def get_redis():
        return redis_client

    async def get_session(**kwargs):
        assert kwargs == {
            "session_id": session_id,
            "namespace": namespace,
            "user_id": user_id,
        }
        await redis_client.set(key, replacement, ex=60)
        return None

    monkeypatch.setattr("agent_memory_server.utils.redis.get_redis_conn", get_redis)
    monkeypatch.setattr(long_term_memory, "get_redis_conn", get_redis)
    monkeypatch.setattr("agent_memory_server.working_memory.get_working_memory", get_session)
    assert (
        await long_term_memory.run_delayed_extraction(
            session_id, namespace, user_id, "old-schedule", source_incarnation="1"
        )
        == 0
    )
    if replacement == "new-schedule":
        assert await redis_client.get(key) == replacement
        assert 0 < await redis_client.ttl(key) <= 60
    else:
        assert not await redis_client.exists(key)


async def test_legacy_list_summary_uses_explicit_owner_keys(redis_client, monkeypatch):
    async def get_redis():
        return redis_client

    prompts = []

    async def summarize(model, context, messages):
        prompts.append((context, messages))
        return "orchard summary", 12

    monkeypatch.setattr(summarization, "get_redis_conn", get_redis)
    monkeypatch.setattr(summarization, "_incremental_summary", summarize)
    monkeypatch.setattr(
        summarization,
        "get_model_config",
        lambda model: SimpleNamespace(max_tokens=10000),
    )
    owners = [("orchard", "alice"), ("orchard", "bob"), ("forest", "alice")]
    snapshots = []
    for namespace, user_id in owners:
        messages = Keys.scoped_messages_key("shared", namespace, user_id)
        metadata = Keys.scoped_metadata_key("shared", namespace, user_id)
        await redis_client.rpush(
            messages,
            *[
                json.dumps({"role": "user", "content": f"{namespace}/{user_id} message {i}"})
                for i in range(3)
            ],
        )
        await redis_client.hset(metadata, mapping={"context": f"{namespace}/{user_id} context"})
        snapshots.append(
            (
                await redis_client.lrange(messages, 0, -1),
                await redis_client.hgetall(metadata),
            )
        )
    await redis_client.hset(Keys.metadata_key("shared"), mapping={"context": "legacy untouched"})
    await summarization.summarize_session(
        "shared",
        "test-model",
        max_context_tokens=1,
        namespace="orchard",
        user_id="alice",
    )
    assert prompts
    assert all("bob" not in text and "forest" not in text for text in prompts[0][1])
    assert (
        await redis_client.hget(Keys.scoped_metadata_key("shared", "orchard", "alice"), "context")
        == "orchard summary"
    )
    assert await redis_client.llen(Keys.scoped_messages_key("shared", "orchard", "alice")) == 1
    for (namespace, user_id), snapshot in zip(owners[1:], snapshots[1:], strict=True):
        assert (
            await redis_client.lrange(
                Keys.scoped_messages_key("shared", namespace, user_id), 0, -1
            ),
            await redis_client.hgetall(Keys.scoped_metadata_key("shared", namespace, user_id)),
        ) == snapshot
    assert await redis_client.hget(Keys.metadata_key("shared"), "context") == "legacy untouched"
    count = len(prompts)
    await summarization.summarize_session(
        "shared",
        "test-model",
        max_context_tokens=1,
        namespace="unknown",
        user_id="alice",
    )
    assert len(prompts) == count
    assert await redis_client.hget(Keys.metadata_key("shared"), "context") == "legacy untouched"


@pytest.mark.parametrize("change", ["delete", "replace"])
async def test_summary_watch_conflict_rereads_session(redis_client, monkeypatch, change):
    messages_key = Keys.scoped_messages_key("shared", "orchard", "alice")
    metadata_key = Keys.scoped_metadata_key("shared", "orchard", "alice")

    def messages(prefix):
        return [json.dumps({"role": "user", "content": f"{prefix} message {i}"}) for i in range(3)]

    await redis_client.rpush(messages_key, *messages("old"))
    await redis_client.hset(metadata_key, mapping={"context": "old context"})
    prompts = []

    async def get_redis():
        return redis_client

    async def summarize(model, context, texts):
        prompts.append((context, list(texts)))
        if len(prompts) == 1:
            await redis_client.delete(messages_key, metadata_key)
            if change == "replace":
                await redis_client.rpush(messages_key, *messages("new"))
                await redis_client.hset(metadata_key, mapping={"context": "new context"})
            return "stale summary", 12
        assert context == "new context"
        assert all("new" in text and "old" not in text for text in texts)
        return "fresh summary", 12

    monkeypatch.setattr(summarization, "get_redis_conn", get_redis)
    monkeypatch.setattr(summarization, "_incremental_summary", summarize)
    monkeypatch.setattr(
        summarization,
        "get_model_config",
        lambda model: SimpleNamespace(max_tokens=10000),
    )
    await summarization.summarize_session(
        "shared",
        "test-model",
        max_context_tokens=1,
        namespace="orchard",
        user_id="alice",
    )
    if change == "delete":
        assert len(prompts) == 1
        assert not await redis_client.exists(messages_key, metadata_key)
    else:
        assert len(prompts) == 2
        assert await redis_client.hget(metadata_key, "context") == "fresh summary"
        assert all("new" in text for text in await redis_client.lrange(messages_key, 0, -1))


@pytest.mark.parametrize("change", ["none", "update", "delete", "recreate"])
async def test_guarded_memory_publication_checks_source_after_embedding(
    memory_db, redis_client, monkeypatch, change
):
    source = working_memory.WorkingMemory(session_id="shared", namespace="orchard", user_id="alice")
    await working_memory.set_working_memory(source, redis_client)
    record = MemoryRecord(
        id="derived",
        text="recent orchard",
        namespace="orchard",
        user_id="alice",
        session_id="shared",
    )
    source_key = Keys.working_memory_key("shared", "alice", "orchard")
    revision_key = Keys.working_memory_revision_key("shared", "alice", "orchard")
    record._source_session = (source_key, revision_key, source.version)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def embed(texts):
        entered.set()
        await release.wait()
        return [[1.0, 0.0, 0.0]]

    monkeypatch.setattr(memory_db.embeddings, "aembed_documents", embed)
    publishing = asyncio.create_task(memory_db.add_memories([record]))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        if change == "update":
            source.context = "new context"
            await working_memory.set_working_memory(source, redis_client)
        elif change in {"delete", "recreate"}:
            await working_memory.delete_working_memory("shared", "alice", "orchard", redis_client)
            if change == "recreate":
                await working_memory.set_working_memory(
                    working_memory.WorkingMemory(
                        session_id="shared", namespace="orchard", user_id="alice"
                    ),
                    redis_client,
                )
        release.set()
        key = Keys.memory_key("derived", "orchard", "alice")
        if change == "none":
            assert await asyncio.wait_for(publishing, 5) == ["derived"]
            assert await redis_client.ttl(key) == -1
            results = await memory_db.list_memories(limit=10)
            assert [m.id for m in results.memories] == ["derived"]
        else:
            with pytest.raises(ValueError, match="Source working memory changed"):
                await asyncio.wait_for(publishing, 5)
            assert not await redis_client.exists(key)
            assert not (await memory_db.list_memories(limit=10)).memories
        assert [key async for key in redis_client.scan_iter("memory_stage:*")] == []
    finally:
        release.set()
        if not publishing.done():
            await asyncio.wait_for(publishing, 5)


async def test_concurrent_database_instances_validate_winning_index(redis_client, monkeypatch):
    monkeypatch.setattr(settings, "redisvl_vector_dimensions", 3)
    monkeypatch.setattr(settings, "embedding_model", "test-fixed-3d")
    instances = []
    arrived = 0
    release = asyncio.Event()
    for _ in range(8):
        index = AsyncSearchIndex.from_dict(_build_redis_schema(), redis_client=redis_client)
        original_exists = index.exists
        first = [True]

        async def exists(original=original_exists, initial=first):
            nonlocal arrived
            value = await original()
            if initial[0]:
                initial[0] = False
                arrived += 1
                if arrived == 8:
                    release.set()
                await asyncio.wait_for(release.wait(), 5)
            return value

        monkeypatch.setattr(index, "exists", exists)
        instances.append(RedisVLMemoryVectorDatabase(index, FixedEmbeddings()))
    await asyncio.wait_for(asyncio.gather(*(db._ensure_index() for db in instances)), 10)
    assert all(db._index_created for db in instances)


async def test_deleted_inflight_extraction_cannot_publish(memory_db, redis_client, monkeypatch):
    source = working_memory.WorkingMemory(session_id="source", namespace="orchard", user_id="alice")
    await working_memory.set_working_memory(source, redis_client)
    record = MemoryRecord(id="late", text="recent orchard", namespace="orchard", user_id="alice")
    record._source_session = (
        Keys.working_memory_key("source", "alice", "orchard"),
        Keys.working_memory_revision_key("source", "alice", "orchard"),
        source.version,
    )
    entered, release = asyncio.Event(), asyncio.Event()

    async def embed(texts):
        entered.set()
        await release.wait()
        return [[1.0, 0.0, 0.0]]

    monkeypatch.setattr(memory_db.embeddings, "aembed_documents", embed)
    publishing = asyncio.create_task(memory_db.add_memories([record]))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        assert await memory_db.delete_memories(["late"], "orchard", "bob", exact_scope=True) == 0
        assert await memory_db.delete_memories(["late"], "orchard", "alice", exact_scope=True) == 0
        generation = Keys.memory_generation_key("late", "orchard", "alice")
        assert await redis_client.get(generation) == "1"
        assert await redis_client.ttl(generation) == -1
        release.set()
        with pytest.raises(ValueError, match="Long-term memory deleted"):
            await asyncio.wait_for(publishing, 5)
        assert not await redis_client.exists(Keys.memory_key("late", "orchard", "alice"))
        assert not (await memory_db.list_memories(limit=10)).memories
        assert [key async for key in redis_client.scan_iter("memory_stage:*")] == []
    finally:
        release.set()
        if not publishing.done():
            await asyncio.wait_for(publishing, 5)


@pytest.mark.parametrize("delete_owner", ["alice", "bob"])
async def test_plain_write_checks_deletion_generation(
    memory_db, redis_client, monkeypatch, delete_owner
):
    record = MemoryRecord(id="plain", text="recent orchard", namespace="orchard", user_id="alice")
    entered, release = asyncio.Event(), asyncio.Event()

    async def embed(texts):
        entered.set()
        await release.wait()
        return [[1.0, 0.0, 0.0]]

    monkeypatch.setattr(memory_db.embeddings, "aembed_documents", embed)
    publishing = asyncio.create_task(memory_db.add_memories([record]))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        assert (
            await memory_db.delete_memories(["plain"], "orchard", delete_owner, exact_scope=True)
            == 0
        )
        release.set()
        if delete_owner == "alice":
            with pytest.raises(ValueError, match="Long-term memory deleted"):
                await asyncio.wait_for(publishing, 5)
            assert not await redis_client.exists(Keys.memory_key("plain", "orchard", "alice"))
            # An explicit new create after deletion captures the new generation.
            assert await memory_db.add_memories([record]) == ["plain"]
        else:
            assert await asyncio.wait_for(publishing, 5) == ["plain"]
        assert await redis_client.ttl(Keys.memory_key("plain", "orchard", "alice")) == -1
        assert [m.id for m in (await memory_db.list_memories(limit=10)).memories] == ["plain"]
        assert [key async for key in redis_client.scan_iter("memory_stage:*")] == []
    finally:
        release.set()
        if not publishing.done():
            await asyncio.wait_for(publishing, 5)


@pytest.mark.parametrize(
    "selector,reject",
    [
        ((None, None), True),
        (("orchard", None), True),
        ((None, "alice"), True),
        (("ocean", None), False),
        ((None, "bob"), False),
    ],
)
async def test_admin_delete_invalidates_absent_inflight_write(
    memory_db, redis_client, monkeypatch, selector, reject
):
    record = MemoryRecord(
        id="admin-late", text="recent orchard", namespace="orchard", user_id="alice"
    )
    entered, release = asyncio.Event(), asyncio.Event()

    async def embed(texts):
        entered.set()
        await release.wait()
        return [[1.0, 0.0, 0.0]]

    monkeypatch.setattr(memory_db.embeddings, "aembed_documents", embed)
    publishing = asyncio.create_task(memory_db.add_memories([record]))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        assert await memory_db.delete_memories([record.id], *selector) == 0
        release.set()
        if reject:
            with pytest.raises(ValueError, match="Long-term memory deleted"):
                await asyncio.wait_for(publishing, 5)
            assert not await redis_client.exists(Keys.memory_key(record.id, "orchard", "alice"))
            assert not (await memory_db.list_memories(limit=10)).memories
        else:
            assert await asyncio.wait_for(publishing, 5) == [record.id]
            assert [m.id for m in (await memory_db.list_memories(limit=10)).memories] == [record.id]
    finally:
        release.set()
        if not publishing.done():
            await asyncio.wait_for(publishing, 5)


async def test_read_before_delete_cannot_resurrect_via_update(memory_db, redis_client):
    record = MemoryRecord(
        id="stale-update", text="recent orchard", namespace="orchard", user_id="alice"
    )
    await memory_db.add_memories([record])
    old = (await memory_db.list_memories(limit=10)).memories[0]
    assert old._publication_generation is not None
    assert "_publication_generation" not in old.model_dump()
    assert await memory_db.delete_memories([record.id], "orchard", "alice", exact_scope=True) == 1
    old.text = "old orchard"
    with pytest.raises(ValueError, match="Long-term memory deleted"):
        await memory_db.update_memories([old])
    assert not await redis_client.exists(Keys.memory_key(record.id, "orchard", "alice"))
    assert not (await memory_db.list_memories(limit=10)).memories
    assert [key async for key in redis_client.scan_iter("memory_stage:*")] == []


async def test_batch_cannot_replace_stale_generation_with_fresh_record(memory_db, redis_client):
    old_record = MemoryRecord(
        id="batch", text="recent orchard", namespace="orchard", user_id="alice"
    )
    await memory_db.add_memories([old_record])
    stale = (await memory_db.list_memories(limit=10)).memories[0]
    assert await memory_db.delete_memories(["batch"], "orchard", "alice", exact_scope=True) == 1
    fresh = MemoryRecord(id="batch", text="old orchard", namespace="orchard", user_id="alice")
    with pytest.raises(ValueError, match="Long-term memory deleted"):
        await memory_db.add_memories([stale, fresh])
    assert not await redis_client.exists(Keys.memory_key("batch", "orchard", "alice"))
    assert not (await memory_db.list_memories(limit=10)).memories
    assert [key async for key in redis_client.scan_iter("memory_stage:*")] == []
    assert await memory_db.add_memories([fresh]) == ["batch"]


async def test_serialized_publication_versions_reject_deleted_task_before_dedup(
    memory_db, redis_client, monkeypatch
):
    record = MemoryRecord(id="queued", text="recent orchard", namespace="orchard", user_id="alice")
    versions = await long_term_memory.capture_publication_generations([record], redis_client)
    connection = redis_client.connection_pool.connection_kwargs
    url = f"redis://{connection['host']}:{connection['port']}"
    async with Docket(name="generation-payload", url=url) as docket:
        await docket.add(long_term_memory.index_long_term_memories, key="queued-version")(
            memories=[record], deduplicate=True, publication_generations=versions
        )
        execution = await docket.get_execution("queued-version")
        assert execution is not None
        assert execution.kwargs["publication_generations"] == versions
        assert (
            await memory_db.delete_memories([record.id], "orchard", "alice", exact_scope=True) == 0
        )
        with pytest.raises(ValueError, match="deleted before task execution"):
            await long_term_memory.index_long_term_memories(
                **execution.kwargs, redis_client=redis_client
            )
    assert not await redis_client.exists(Keys.memory_key(record.id, "orchard", "alice"))
    assert not (await memory_db.list_memories(limit=10)).memories


@pytest.mark.parametrize("recreate", [None, False, True, "updated", "tags", "stats", "tags_cas"])
async def test_structure_task_does_not_modify_deleted_incarnation(
    memory_db, redis_client, monkeypatch, recreate
):
    record = MemoryRecord(
        id="structure-race", text="recent orchard", namespace="orchard", user_id="alice"
    )
    await memory_db.add_memories([record])
    expected = list(record._publication_generation)
    entered, release = asyncio.Event(), asyncio.Event()

    async def get_redis():
        return redis_client

    async def extract(text):
        entered.set()
        await release.wait()
        return ["stale-topic"], ["stale-entity"]

    monkeypatch.setattr(long_term_memory, "get_redis_conn", get_redis)
    monkeypatch.setattr(long_term_memory, "handle_extraction", extract)
    task = asyncio.create_task(
        long_term_memory.extract_memory_structure(
            MemoryRecord.model_validate_json(record.model_dump_json()),
            publication_generation=expected,
        )
    )
    try:
        await asyncio.wait_for(entered.wait(), 5)
        if recreate is False or recreate is True:
            await memory_db.delete_memories([record.id], "orchard", "alice", exact_scope=True)
        if recreate is True or recreate == "updated":
            await memory_db.add_memories(
                [
                    MemoryRecord(
                        id=record.id,
                        text="old orchard",
                        namespace="orchard",
                        user_id="alice",
                        topics=["fresh-topic"],
                        entities=["fresh-entity"],
                    )
                ]
            )
        if recreate == "tags":
            await redis_client.hset(
                Keys.memory_key(record.id, "orchard", "alice"),
                mapping={"topics": "concurrent-topic", "entities": "concurrent-entity"},
            )
        if recreate == "stats":
            await redis_client.hset(
                Keys.memory_key(record.id, "orchard", "alice"),
                mapping={"access_count": "17"},
            )
        if recreate == "tags_cas":
            original_eval = redis_client.eval
            raced = False

            async def tag_race(script, numkeys, *args):
                nonlocal raced
                if "return -1 end" in script and not raced:
                    raced = True
                    await redis_client.hset(
                        Keys.memory_key(record.id, "orchard", "alice"),
                        mapping={
                            "topics": "concurrent-topic",
                            "entities": "concurrent-entity",
                        },
                    )
                return await original_eval(script, numkeys, *args)

            monkeypatch.setattr(redis_client, "eval", tag_race)
        release.set()
        await asyncio.wait_for(task, 5)
        key = Keys.memory_key(record.id, "orchard", "alice")
        if recreate is None or recreate == "stats":
            assert await redis_client.hget(key, "topics") == "stale-topic"
            assert await redis_client.hget(key, "entities") == "stale-entity"
        elif recreate is True or recreate == "updated":
            assert await redis_client.hget(key, "topics") == "fresh-topic"
            assert await redis_client.hget(key, "entities") == "fresh-entity"
        elif recreate in {"tags", "tags_cas"}:
            assert set((await redis_client.hget(key, "topics")).split(",")) == {
                "concurrent-topic",
                "stale-topic",
            }
            assert set((await redis_client.hget(key, "entities")).split(",")) == {
                "concurrent-entity",
                "stale-entity",
            }
        else:
            assert not await redis_client.exists(key)
        if recreate == "stats":
            assert await redis_client.hget(key, "access_count") == "17"
        if recreate in {None, "stats", "tags", "tags_cas"}:
            before = await redis_client.execute_command("HGETALL", key, NEVER_DECODE=True)
            await long_term_memory.extract_memory_structure(record, publication_generation=expected)
            assert await redis_client.execute_command("HGETALL", key, NEVER_DECODE=True) == before
    finally:
        release.set()
        if not task.done():
            await asyncio.wait_for(task, 5)


async def test_unversioned_structure_task_requires_rescheduling(
    memory_db, redis_client, monkeypatch
):
    record = MemoryRecord(
        id="old-task", text="recent orchard", namespace="orchard", user_id="alice"
    )
    await memory_db.add_memories([record])
    unversioned = MemoryRecord.model_validate_json(record.model_dump_json())
    await memory_db.delete_memories([record.id], "orchard", "alice", exact_scope=True)
    await memory_db.add_memories(
        [
            MemoryRecord(
                id=record.id,
                text="old orchard",
                namespace="orchard",
                user_id="alice",
                topics=["fresh"],
            )
        ]
    )

    async def get_redis():
        return redis_client

    async def unexpected_model(text):
        pytest.fail("Unversioned tasks must fail before calling the model")

    monkeypatch.setattr(long_term_memory, "get_redis_conn", get_redis)
    monkeypatch.setattr(long_term_memory, "handle_extraction", unexpected_model)
    with pytest.raises(ValueError, match="reschedule from current memory"):
        await long_term_memory.extract_memory_structure(unversioned)
    assert (
        await redis_client.hget(Keys.memory_key(record.id, "orchard", "alice"), "topics") == "fresh"
    )


async def test_discrete_extraction_preserves_each_source_owner(
    memory_db, redis_client, monkeypatch
):
    records = [
        MemoryRecord(
            id="same-source",
            text="recent orchard",
            namespace="orchard",
            user_id=user,
            session_id="shared",
            memory_type="message",
        )
        for user in ["alice", "bob"]
    ]
    await memory_db.add_memories(records)

    async def get_db():
        return memory_db

    class Strategy:
        async def extract_memories(self, text):
            return [{"text": "old orchard", "type": "semantic"}]

    captured = []

    async def index(memories, **kwargs):
        captured.extend(memories)
        await memory_db.add_memories(memories)

    monkeypatch.setattr(memory_vector_db_factory, "get_memory_vector_db", get_db)
    monkeypatch.setattr(
        memory_strategies, "get_memory_strategy", lambda *args, **kwargs: Strategy()
    )
    monkeypatch.setattr(long_term_memory, "index_long_term_memories", index)
    await extraction.extract_memories_with_strategy(memories=records)
    assert {(m.namespace, m.user_id, m.session_id) for m in captured} == {
        ("orchard", "alice", "shared"),
        ("orchard", "bob", "shared"),
    }
    assert all(m.extracted_from == ["same-source"] for m in captured)
    for memory in captured:
        assert await redis_client.exists(
            Keys.memory_key(memory.id, memory.namespace, memory.user_id)
        )
    results = (await memory_db.list_memories(limit=10)).memories
    assert len(results) == 4
    assert all(m.user_id in {"alice", "bob"} and m.namespace == "orchard" for m in results)


async def test_discrete_model_failure_keeps_source_retryable(memory_db, redis_client, monkeypatch):
    record = MemoryRecord(
        id="retry-source",
        text="recent orchard",
        namespace="orchard",
        user_id="alice",
        memory_type="message",
    )
    await memory_db.add_memories([record])

    async def get_db():
        return memory_db

    attempts = []

    class Strategy:
        async def extract_memories(self, text):
            attempts.append(text)
            if len(attempts) == 1:
                raise ConnectionError("model unavailable")
            return [{"text": "old orchard", "type": "semantic"}]

    async def index(memories, **kwargs):
        await memory_db.add_memories(memories)

    monkeypatch.setattr(memory_vector_db_factory, "get_memory_vector_db", get_db)
    monkeypatch.setattr(
        memory_strategies, "get_memory_strategy", lambda *args, **kwargs: Strategy()
    )
    monkeypatch.setattr(long_term_memory, "index_long_term_memories", index)
    with pytest.raises(ConnectionError, match="model unavailable"):
        await extraction.extract_memories_with_strategy(memories=[record])
    key = Keys.memory_key(record.id, "orchard", "alice")
    assert await redis_client.hget(key, "discrete_memory_extracted") == "f"
    assert len((await memory_db.list_memories(limit=10)).memories) == 1
    await extraction.extract_memories_with_strategy(memories=[record])
    assert attempts == ["recent orchard", "recent orchard"]
    assert await redis_client.hget(key, "discrete_memory_extracted") == "t"
    results = (await memory_db.list_memories(limit=10)).memories
    assert len(results) == 2
    assert all(m.namespace == "orchard" and m.user_id == "alice" for m in results)


@pytest.mark.parametrize("change", ["none", "delete", "recreate"])
async def test_child_publication_checks_source_long_term_memory(
    memory_db, redis_client, monkeypatch, change
):
    source = MemoryRecord(id="parent", text="recent orchard", namespace="orchard", user_id="alice")
    await memory_db.add_memories([source])
    child = MemoryRecord(id="child", text="old orchard", namespace="orchard", user_id="alice")
    child._source_memory = (
        Keys.memory_key(source.id, "orchard", "alice"),
        Keys.memory_generation_keys(source.id, "orchard", "alice"),
        list(source._publication_generation),
    )
    entered, release = asyncio.Event(), asyncio.Event()

    async def embed(texts):
        if texts == ["old orchard"]:
            entered.set()
            await release.wait()
        return [[1.0, 0.0, 0.0]]

    monkeypatch.setattr(memory_db.embeddings, "aembed_documents", embed)
    task = asyncio.create_task(memory_db.add_memories([child]))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        if change != "none":
            await memory_db.delete_memories([source.id], "orchard", "alice", exact_scope=True)
        if change == "recreate":
            await memory_db.add_memories(
                [
                    MemoryRecord(
                        id=source.id,
                        text="recent orchard",
                        namespace="orchard",
                        user_id="alice",
                    )
                ]
            )
        release.set()
        if change == "none":
            assert await asyncio.wait_for(task, 5) == [child.id]
        else:
            with pytest.raises(ValueError, match="Source long-term memory changed"):
                await asyncio.wait_for(task, 5)
            assert not await redis_client.exists(Keys.memory_key(child.id, "orchard", "alice"))
        assert [key async for key in redis_client.scan_iter("memory_stage:*")] == []
    finally:
        release.set()
        if not task.done():
            await asyncio.wait_for(task, 5)


async def test_discrete_queue_payload_rejects_deleted_source_before_model(
    memory_db, redis_client, monkeypatch
):
    source = MemoryRecord(
        id="queued-discrete",
        text="recent orchard",
        namespace="orchard",
        user_id="alice",
        memory_type="message",
    )
    await memory_db.add_memories([source])

    async def get_db():
        return memory_db

    async def get_redis():
        return redis_client

    monkeypatch.setattr(memory_vector_db_factory, "get_memory_vector_db", get_db)
    monkeypatch.setattr(long_term_memory, "get_redis_conn", get_redis)
    monkeypatch.setattr(
        memory_strategies,
        "get_memory_strategy",
        lambda *a, **k: pytest.fail("Deleted queue task must fail before the model"),
    )
    connection = redis_client.connection_pool.connection_kwargs
    async with Docket(
        name="discrete-versions",
        url=f"redis://{connection['host']}:{connection['port']}",
    ) as docket:
        await docket.add(extraction.extract_memories_with_strategy, key="discrete-task")(
            memories=[MemoryRecord.model_validate_json(source.model_dump_json())],
            publication_generations=[list(source._publication_generation)],
        )
        execution = await docket.get_execution("discrete-task")
        assert execution.kwargs["publication_generations"] == [source._publication_generation]
        await memory_db.delete_memories([source.id], "orchard", "alice", exact_scope=True)
        with pytest.raises(ValueError, match="deleted before extraction"):
            await extraction.extract_memories_with_strategy(**execution.kwargs)
    assert not (await memory_db.list_memories(limit=10)).memories


async def test_explicit_empty_extraction_batch_does_not_scan_owners(memory_db, monkeypatch):
    await memory_db.add_memories(
        [
            MemoryRecord(
                id="pending-owner",
                text="recent orchard",
                namespace="orchard",
                user_id="alice",
                memory_type="message",
            )
        ]
    )

    async def get_db():
        return memory_db

    monkeypatch.setattr(memory_vector_db_factory, "get_memory_vector_db", get_db)
    monkeypatch.setattr(
        memory_strategies,
        "get_memory_strategy",
        lambda *a, **k: pytest.fail("An empty batch must not scan unrelated records"),
    )
    await extraction.extract_memories_with_strategy(memories=[])
    records = (await memory_db.list_memories(limit=10)).memories
    assert len(records) == 1 and records[0].discrete_memory_extracted == "f"


async def test_unversioned_discrete_payload_cannot_claim_new_incarnation(
    memory_db, redis_client, monkeypatch
):
    source = MemoryRecord(
        id="legacy-discrete",
        text="recent orchard",
        namespace="orchard",
        user_id="alice",
        memory_type="message",
    )
    await memory_db.add_memories([source])
    old_payload = MemoryRecord.model_validate_json(source.model_dump_json())
    await memory_db.delete_memories([source.id], "orchard", "alice", exact_scope=True)
    await memory_db.add_memories(
        [
            MemoryRecord(
                id=source.id,
                text="old orchard",
                namespace="orchard",
                user_id="alice",
                memory_type="message",
            )
        ]
    )

    async def get_db():
        return memory_db

    monkeypatch.setattr(memory_vector_db_factory, "get_memory_vector_db", get_db)
    monkeypatch.setattr(
        memory_strategies,
        "get_memory_strategy",
        lambda *a, **k: pytest.fail("Old unversioned payload must not run a model"),
    )
    with pytest.raises(ValueError, match="reschedule from current memory"):
        await extraction.extract_memories_with_strategy(memories=[old_payload])
    rows = (await memory_db.list_memories(limit=10)).memories
    assert len(rows) == 1
    assert rows[0].text == "old orchard"
    assert rows[0].discrete_memory_extracted == "f"


async def test_scoped_hash_migration_preserves_vector_ttl_and_is_retryable(memory_db, redis_client):
    record = MemoryRecord(
        id="legacy-migrate", text="recent orchard", namespace="orchard", user_id="alice"
    )
    await memory_db.add_memories([record])
    target = Keys.memory_key(record.id, "orchard", "alice")
    legacy = "memory_idx:legacy-migrate"
    await redis_client.rename(target, legacy)
    await redis_client.hdel(legacy, "_publication_generation")
    await redis_client.pexpire(legacy, 60000)
    before = await redis_client.execute_command("HGETALL", legacy, NEVER_DECODE=True)
    assert await migrations.migrate_scoped_memory_keys_5(redis_client, dry_run=True) == 1
    assert await redis_client.execute_command("HGETALL", legacy, NEVER_DECODE=True) == before
    assert not await redis_client.exists(target)
    assert await migrations.migrate_scoped_memory_keys_5(redis_client) == 1
    after = await redis_client.execute_command("HGETALL", target, NEVER_DECODE=True)
    version = after.pop(b"_publication_generation")
    assert after == before
    assert json.loads(version) == ["0"] * 4
    assert 0 < await redis_client.pttl(target) <= 60000
    assert not await redis_client.exists(legacy)
    assert await migrations.migrate_scoped_memory_keys_5(redis_client) == 0
    results = (await memory_db.list_memories(limit=10)).memories
    assert len(results) == 1 and results[0]._publication_generation == ["0"] * 4


async def test_scoped_hash_migration_conflict_keeps_both_hashes(memory_db, redis_client):
    record = MemoryRecord(
        id="collision", text="recent orchard", namespace="orchard", user_id="alice"
    )
    await memory_db.add_memories([record])
    target = Keys.memory_key(record.id, "orchard", "alice")
    legacy = "memory_idx:collision"
    original = await redis_client.execute_command("HGETALL", target, NEVER_DECODE=True)
    await redis_client.hset(
        legacy,
        mapping={
            "id_": record.id,
            "namespace": "orchard",
            "user_id": "alice",
            "text": "old orchard",
        },
    )
    before = await redis_client.execute_command("HGETALL", legacy, NEVER_DECODE=True)
    for dry_run in [True, False]:
        with pytest.raises(ValueError, match="migration conflict"):
            await migrations.migrate_scoped_memory_keys_5(redis_client, dry_run=dry_run)
        assert await redis_client.execute_command("HGETALL", target, NEVER_DECODE=True) == original
        assert await redis_client.execute_command("HGETALL", legacy, NEVER_DECODE=True) == before


@pytest.mark.parametrize("version", ["not-json", '["0"]', "[0,0,0,0]", '["-1","0","0","0"]'])
async def test_migration_rejects_invalid_stored_versions(memory_db, redis_client, version):
    record = MemoryRecord(
        id="invalid-version",
        text="recent orchard",
        namespace="orchard",
        user_id="alice",
    )
    await memory_db.add_memories([record])
    key = Keys.memory_key(record.id, "orchard", "alice")
    await redis_client.hset(key, "_publication_generation", version)
    for dry_run in [True, False]:
        with pytest.raises(ValueError, match="Invalid stored publication generation"):
            await migrations.migrate_scoped_memory_keys_5(redis_client, dry_run=dry_run)
        assert await redis_client.hget(key, "_publication_generation") == version


async def test_migration_does_not_rebind_stale_versions(memory_db, redis_client):
    record = MemoryRecord(
        id="stale-version", text="recent orchard", namespace="orchard", user_id="alice"
    )
    await memory_db.add_memories([record])
    generation_key = Keys.memory_generation_key(record.id, "orchard", "alice")
    await redis_client.incr(generation_key)
    with pytest.raises(ValueError, match="generation is stale"):
        await migrations.migrate_scoped_memory_keys_5(redis_client)
    key = Keys.memory_key(record.id, "orchard", "alice")
    assert json.loads(await redis_client.hget(key, "_publication_generation")) == ["0"] * 4


async def test_migration_expired_source_is_not_recreated(memory_db, redis_client, monkeypatch):
    record = MemoryRecord(
        id="expires-migration",
        text="recent orchard",
        namespace="orchard",
        user_id="alice",
    )
    await memory_db.add_memories([record])
    target = Keys.memory_key(record.id, "orchard", "alice")
    legacy = "memory_idx:expires-migration"
    await redis_client.rename(target, legacy)
    await redis_client.hdel(legacy, "_publication_generation")
    original_eval = redis_client.eval

    async def expire_then_migrate(*args, **kwargs):
        await redis_client.pexpire(legacy, 1)
        await asyncio.sleep(0.02)
        assert not await redis_client.exists(legacy)
        return await original_eval(*args, **kwargs)

    monkeypatch.setattr(redis_client, "eval", expire_then_migrate)
    assert await migrations.migrate_scoped_memory_keys_5(redis_client) == 0
    assert not await redis_client.exists(legacy, target)
    assert not (await memory_db.list_memories(limit=10)).memories


async def test_migration_retry_after_committed_response_loss(memory_db, redis_client, monkeypatch):
    record = MemoryRecord(
        id="response-loss", text="recent orchard", namespace="orchard", user_id="alice"
    )
    await memory_db.add_memories([record])
    target = Keys.memory_key(record.id, "orchard", "alice")
    legacy = "memory_idx:response-loss"
    await redis_client.rename(target, legacy)
    await redis_client.hdel(legacy, "_publication_generation")
    original_eval = redis_client.eval

    async def commit_then_lose_response(*args, **kwargs):
        result = await original_eval(*args, **kwargs)
        assert result == 1
        raise ConnectionError("migration response lost")

    monkeypatch.setattr(redis_client, "eval", commit_then_lose_response)
    with pytest.raises(ConnectionError, match="response lost"):
        await migrations.migrate_scoped_memory_keys_5(redis_client)
    assert not await redis_client.exists(legacy)
    assert await redis_client.exists(target)
    monkeypatch.setattr(redis_client, "eval", original_eval)
    assert await migrations.migrate_scoped_memory_keys_5(redis_client) == 0
    rows = (await memory_db.list_memories(limit=10)).memories
    assert len(rows) == 1 and rows[0].text == "recent orchard"


async def test_migration_explicit_identical_resolution(memory_db, redis_client):
    record = MemoryRecord(
        id="identical", text="recent orchard", namespace="orchard", user_id="alice"
    )
    await memory_db.add_memories([record])
    target = Keys.memory_key(record.id, "orchard", "alice")
    legacy = "memory_idx:identical"
    original = await redis_client.execute_command("HGETALL", target, NEVER_DECODE=True)
    await redis_client.hset(legacy, mapping=original)
    assert (
        await migrations.migrate_scoped_memory_keys_5(
            redis_client, dry_run=True, resolve_identical=True
        )
        == 1
    )
    assert await redis_client.exists(legacy, target) == 2
    assert await migrations.migrate_scoped_memory_keys_5(redis_client, resolve_identical=True) == 1
    assert not await redis_client.exists(legacy)
    assert await redis_client.execute_command("HGETALL", target, NEVER_DECODE=True) == original
    assert await migrations.migrate_scoped_memory_keys_5(redis_client, resolve_identical=True) == 0
    assert len((await memory_db.list_memories(limit=10)).memories) == 1


async def test_migration_identical_resolution_rejects_different_ttl(memory_db, redis_client):
    record = MemoryRecord(
        id="ttl-conflict", text="recent orchard", namespace="orchard", user_id="alice"
    )
    await memory_db.add_memories([record])
    target = Keys.memory_key(record.id, "orchard", "alice")
    legacy = "memory_idx:ttl-conflict"
    await redis_client.hset(
        legacy,
        mapping=await redis_client.execute_command("HGETALL", target, NEVER_DECODE=True),
    )
    await redis_client.expire(legacy, 60)
    with pytest.raises(ValueError, match="migration conflict"):
        await migrations.migrate_scoped_memory_keys_5(redis_client, resolve_identical=True)
    assert await redis_client.exists(legacy, target) == 2
    assert await redis_client.ttl(target) == -1


async def test_migration_partial_batch_conflict_and_resume(memory_db, redis_client, monkeypatch):
    records = [
        MemoryRecord(id=id_, text="recent orchard", namespace="orchard", user_id="alice")
        for id_ in ["first", "conflict"]
    ]
    await memory_db.add_memories(records)
    first_target = Keys.memory_key("first", "orchard", "alice")
    first_legacy = "memory_idx:first"
    await redis_client.rename(first_target, first_legacy)
    await redis_client.hdel(first_legacy, "_publication_generation")
    conflict_target = Keys.memory_key("conflict", "orchard", "alice")
    conflict_legacy = "memory_idx:conflict"
    await redis_client.hset(
        conflict_legacy,
        mapping={
            "id_": "conflict",
            "namespace": "orchard",
            "user_id": "alice",
            "text": "old orchard",
        },
    )
    conflict_before = await redis_client.execute_command(
        "HGETALL", conflict_legacy, NEVER_DECODE=True
    )
    target_before = await redis_client.execute_command(
        "HGETALL", conflict_target, NEVER_DECODE=True
    )
    original_scan = redis_client.scan_iter

    async def ordered_scan(**kwargs):
        for key in [first_legacy, conflict_legacy]:
            yield key

    monkeypatch.setattr(redis_client, "scan_iter", ordered_scan)
    with pytest.raises(ValueError, match="migration conflict"):
        await migrations.migrate_scoped_memory_keys_5(redis_client)
    assert await redis_client.exists(first_target) and not await redis_client.exists(first_legacy)
    first_after = await redis_client.execute_command("HGETALL", first_target, NEVER_DECODE=True)
    assert (
        await redis_client.execute_command("HGETALL", conflict_legacy, NEVER_DECODE=True)
        == conflict_before
    )
    assert (
        await redis_client.execute_command("HGETALL", conflict_target, NEVER_DECODE=True)
        == target_before
    )
    monkeypatch.setattr(redis_client, "scan_iter", original_scan)
    with pytest.raises(ValueError, match="migration conflict"):
        await migrations.migrate_scoped_memory_keys_5(redis_client)
    assert (
        await redis_client.execute_command("HGETALL", first_target, NEVER_DECODE=True)
        == first_after
    )
    # An explicit operator choice can archive the conflicting legacy record;
    # retain its contents instead of silently deleting or overwriting it.
    archive = "migration_archive:conflict"
    await redis_client.rename(conflict_legacy, archive)
    assert await migrations.migrate_scoped_memory_keys_5(redis_client) == 0
    assert (
        await redis_client.execute_command("HGETALL", archive, NEVER_DECODE=True) == conflict_before
    )
    assert len((await memory_db.list_memories(limit=10)).memories) == 2


async def test_conflict_archive_and_restore_preserves_data_and_ttl(memory_db, redis_client):
    record = MemoryRecord(id="archive", text="recent orchard", namespace="orchard", user_id="alice")
    await memory_db.add_memories([record])
    target = Keys.memory_key(record.id, "orchard", "alice")
    legacy = "memory_idx:archive"
    await redis_client.hset(
        legacy,
        mapping={
            "id_": record.id,
            "namespace": "orchard",
            "user_id": "alice",
            "text": "old orchard",
        },
    )
    await redis_client.expire(legacy, 60)
    before = await redis_client.hgetall(legacy)
    receipt = await migrations.archive_scoped_memory_conflict(legacy, redis_client)
    archive = f"memory_migration_archive:{receipt}"
    assert not await redis_client.exists(legacy)
    assert await redis_client.hgetall(archive) == before
    assert 0 < await redis_client.ttl(archive) <= 60
    assert await redis_client.hget(target, "text") == "recent orchard"
    assert await migrations.restore_scoped_memory_archive(receipt, redis_client)
    assert await redis_client.hgetall(legacy) == before
    assert 0 < await redis_client.ttl(legacy) <= 60
    assert not await migrations.restore_scoped_memory_archive(receipt, redis_client)


async def test_conflict_archive_restore_never_overwrites_original_key(memory_db, redis_client):
    record = MemoryRecord(
        id="archive-collision",
        text="recent orchard",
        namespace="orchard",
        user_id="alice",
    )
    await memory_db.add_memories([record])
    legacy = "memory_idx:archive-collision"
    await redis_client.hset(
        legacy,
        mapping={
            "id_": record.id,
            "namespace": "orchard",
            "user_id": "alice",
            "text": "old orchard",
        },
    )
    receipt = await migrations.archive_scoped_memory_conflict(legacy, redis_client)
    await redis_client.hset(legacy, "text", "replacement")
    with pytest.raises(ValueError, match="restore refused"):
        await migrations.restore_scoped_memory_archive(receipt, redis_client)
    assert await redis_client.hget(legacy, "text") == "replacement"
    assert await redis_client.hget(f"memory_migration_archive:{receipt}", "text") == "old orchard"


async def test_archive_response_loss_is_recoverable_from_receipt_lookup(
    memory_db, redis_client, monkeypatch
):
    record = MemoryRecord(
        id="archive-lost", text="recent orchard", namespace="orchard", user_id="alice"
    )
    await memory_db.add_memories([record])
    legacy = "memory_idx:archive-lost"
    await redis_client.hset(
        legacy,
        mapping={
            "id_": record.id,
            "namespace": "orchard",
            "user_id": "alice",
            "text": "old orchard",
        },
    )
    original_eval = redis_client.eval

    async def commit_then_disconnect(*args, **kwargs):
        assert await original_eval(*args, **kwargs) == 1
        raise ConnectionError("archive response lost")

    monkeypatch.setattr(redis_client, "eval", commit_then_disconnect)
    with pytest.raises(ConnectionError, match="response lost"):
        await migrations.archive_scoped_memory_conflict(legacy, redis_client)
    monkeypatch.setattr(redis_client, "eval", original_eval)
    receipts = await migrations.find_memory_archive_receipts(legacy, redis_client)
    assert len(receipts) == 1
    assert receipts[0]["state"] == "archived" and receipts[0]["archive_pttl"] == -1
    receipt = receipts[0]["receipt"]
    assert await migrations.restore_scoped_memory_archive(receipt, redis_client)
    assert await redis_client.hget(legacy, "text") == "old orchard"
    receipts = await migrations.find_memory_archive_receipts(legacy, redis_client)
    assert receipts[0]["state"] == "restored" and receipts[0]["archive_pttl"] == -2


async def test_expired_archive_keeps_receipt_and_cannot_restore(memory_db, redis_client):
    record = MemoryRecord(
        id="archive-expired",
        text="recent orchard",
        namespace="orchard",
        user_id="alice",
    )
    await memory_db.add_memories([record])
    legacy = "memory_idx:archive-expired"
    await redis_client.hset(
        legacy,
        mapping={
            "id_": record.id,
            "namespace": "orchard",
            "user_id": "alice",
            "text": "old orchard",
        },
    )
    receipt = await migrations.archive_scoped_memory_conflict(legacy, redis_client)
    await redis_client.pexpire(f"memory_migration_archive:{receipt}", 1)
    await asyncio.sleep(0.02)
    rows = await migrations.find_memory_archive_receipts(legacy, redis_client)
    assert len(rows) == 1 and rows[0]["archive_pttl"] == -2
    with pytest.raises(ValueError, match="restore refused"):
        await migrations.restore_scoped_memory_archive(receipt, redis_client)
    assert not await redis_client.exists(legacy)


async def test_restore_response_loss_is_idempotent(memory_db, redis_client, monkeypatch):
    record = MemoryRecord(
        id="restore-lost", text="recent orchard", namespace="orchard", user_id="alice"
    )
    await memory_db.add_memories([record])
    legacy = "memory_idx:restore-lost"
    await redis_client.hset(
        legacy,
        mapping={
            "id_": record.id,
            "namespace": "orchard",
            "user_id": "alice",
            "text": "old orchard",
        },
    )
    receipt = await migrations.archive_scoped_memory_conflict(legacy, redis_client)
    original_eval = redis_client.eval

    async def commit_then_disconnect(*args, **kwargs):
        assert await original_eval(*args, **kwargs) == 1
        raise ConnectionError("restore response lost")

    monkeypatch.setattr(redis_client, "eval", commit_then_disconnect)
    with pytest.raises(ConnectionError, match="response lost"):
        await migrations.restore_scoped_memory_archive(receipt, redis_client)
    before = await redis_client.hgetall(legacy)
    monkeypatch.setattr(redis_client, "eval", original_eval)
    assert not await migrations.restore_scoped_memory_archive(receipt, redis_client)
    assert await redis_client.hgetall(legacy) == before
    rows = await migrations.find_memory_archive_receipts(legacy, redis_client)
    assert rows[0]["state"] == "restored"


@pytest.mark.parametrize(
    "failure",
    [
        "immediate",
        "publication_response_loss",
        "dispatch_failure",
        "structure_before_write",
        "structure_response_loss",
    ],
)
async def test_structure_outbox_real_worker_recovers_original_model_result(
    dragonfly_factory, monkeypatch, failure
):
    url = dragonfly_factory(f"--lua_undeclared_keys_shas={SCHEDULER_SHA}")
    monkeypatch.setattr(settings, "redis_url", url)
    monkeypatch.setattr(settings, "docket_name", "structure-recovery")
    monkeypatch.setattr(settings, "use_docket", True)
    monkeypatch.setattr(settings, "enable_topic_extraction", True)
    monkeypatch.setattr(settings, "enable_ner", True)
    monkeypatch.setattr(settings, "redisvl_vector_dimensions", 3)
    monkeypatch.setattr(settings, "embedding_model", "test-fixed-3d")
    async with (
        Redis.from_url(url, decode_responses=True) as client,
        Docket(name="structure-recovery", url=url) as docket,
    ):
        db = RedisVLMemoryVectorDatabase(
            AsyncSearchIndex.from_dict(_build_redis_schema(), redis_client=client),
            FixedEmbeddings(),
        )

        async def get_redis():
            return client

        calls = []

        async def extract(text):
            calls.append(text)
            return ["fruit"], ["tree"]

        monkeypatch.setattr(long_term_memory, "get_redis_conn", get_redis)
        monkeypatch.setattr(long_term_memory, "handle_extraction", extract)

        async def get_db():
            return db

        monkeypatch.setattr(long_term_memory, "get_memory_vector_db", get_db)
        monkeypatch.setattr(long_term_memory, "get_background_tasks", HybridBackgroundTasks)
        monkeypatch.setattr(settings, "enable_discrete_memory_extraction", False)
        original_eval = client.eval

        async def lose_publication(script, numkeys, *args):
            result = await original_eval(script, numkeys, *args)
            if "RENAME" in script:
                raise ConnectionError("publication response lost")
            return result

        record = MemoryRecord(
            id="structured", text="recent orchard", namespace="orchard", user_id="alice"
        )
        if failure == "publication_response_loss":
            monkeypatch.setattr(client, "eval", lose_publication)
            with pytest.raises(ConnectionError, match="publication response lost"):
                await db.add_memories([record])
            monkeypatch.setattr(client, "eval", original_eval)
        elif failure == "immediate":
            await long_term_memory.index_long_term_memories([record])
        else:
            await db.add_memories([record])
        intents = [key async for key in client.scan_iter(match="structure_outbox:*")]
        assert len(intents) == 1 and await client.ttl(intents[0]) == -1
        assert await client.exists(Keys.memory_key(record.id, "orchard", "alice"))
        docket.register(long_term_memory.extract_memory_structure)
        original_add = Docket.add
        if failure == "dispatch_failure":

            def fail_add(*args, **kwargs):
                raise ConnectionError("structure dispatch unavailable")

            monkeypatch.setattr(Docket, "add", fail_add)
            with pytest.raises(ConnectionError, match="structure dispatch unavailable"):
                await long_term_memory.reconcile_structure_outbox(client)
            assert await client.exists(intents[0])
            monkeypatch.setattr(Docket, "add", original_add)
        assert await long_term_memory.reconcile_structure_outbox(client) == (
            0 if failure == "immediate" else 1
        )
        # Active work is not re-enqueued by a concurrent reconciliation pass.
        assert await long_term_memory.reconcile_structure_outbox(client) == 0
        if failure in {"structure_before_write", "structure_response_loss"}:

            async def fail_structure(script, numkeys, *args):
                if "batch.state = 'finalized'" in script and failure == "structure_before_write":
                    raise ConnectionError("structure write failure")
                result = await original_eval(script, numkeys, *args)
                if "batch.state = 'finalized'" in script and failure == "structure_response_loss":
                    raise ConnectionError("structure write failure")
                return result

            monkeypatch.setattr(client, "eval", fail_structure)
            await run_worker(docket)
            assert (await docket.get_execution(intents[0])).state.value == "failed"
            assert await client.exists(intents[0])
            assert len(calls) == 1
            monkeypatch.setattr(client, "eval", original_eval)
            assert await long_term_memory.reconcile_structure_outbox(client) == 1
        await run_worker(docket)
        assert (await docket.get_execution(intents[0])).state.value == "completed"
        assert len(calls) == 1
        target = Keys.memory_key(record.id, "orchard", "alice")
        assert await client.hget(target, "topics") == "fruit"
        assert await client.hget(target, "entities") == "tree"
        batches = [key async for key in client.scan_iter(match="structure_batch:*")]
        assert (
            len(batches) == 1 and json.loads(await client.get(batches[0]))["state"] == "finalized"
        )
        assert await long_term_memory.reconcile_structure_outbox(client) == 0
        assert not await client.exists(intents[0])
        assert (await client.xpending(docket.stream_key, docket.worker_group_name))["pending"] == 0


@pytest.mark.parametrize(
    "mismatch",
    [
        "dim",
        "datatype",
        "algorithm",
        "distance_metric",
        "field_type",
        "prefix",
        "model",
        "model_version",
        "unversioned_data",
    ],
)
async def test_index_contract_rejects_incompatible_configuration_before_embedding(
    redis_client, monkeypatch, mismatch
):
    monkeypatch.setattr(settings, "redisvl_vector_dimensions", 3)
    monkeypatch.setattr(settings, "embedding_model", "test-fixed-3d")
    monkeypatch.setattr(settings, "redisvl_indexing_algorithm", "flat")
    schema = _build_redis_schema()
    index = AsyncSearchIndex.from_dict(schema, redis_client=redis_client)
    await index.create()
    contract_key = f"memory_index_contract:{index.name}"
    if mismatch == "unversioned_data":
        await index.load(
            [
                {
                    "id_": "legacy",
                    "text": "recent orchard",
                    "namespace": "orchard",
                    "user_id": "alice",
                }
            ],
            keys=[Keys.memory_key("legacy", "orchard", "alice")],
        )
    else:
        initial = RedisVLMemoryVectorDatabase(index, FixedEmbeddings())
        await initial._ensure_index()
    before = await redis_client.get(contract_key)
    vector = next(field for field in schema["fields"] if field["name"] == "vector")
    if mismatch == "dim":
        vector["attrs"]["dims"] = 4
    elif mismatch == "datatype":
        vector["attrs"]["datatype"] = "float64"
    elif mismatch == "algorithm":
        vector["attrs"]["algorithm"] = "hnsw"
    elif mismatch == "distance_metric":
        vector["attrs"]["distance_metric"] = "l2"
    elif mismatch == "field_type":
        field = next(field for field in schema["fields"] if field["name"] == "text")
        field["type"], field["attrs"] = "tag", {}
    elif mismatch == "prefix":
        schema["index"]["prefix"] = "different:"
    elif mismatch == "model":
        monkeypatch.setattr(settings, "embedding_model", "different-model")
    elif mismatch == "model_version":
        monkeypatch.setattr(settings, "embedding_model_version", "2")
    candidate = RedisVLMemoryVectorDatabase(
        AsyncSearchIndex.from_dict(schema, redis_client=redis_client), FixedEmbeddings()
    )

    async def forbidden_embedding(texts):
        pytest.fail("Incompatible index must reject before embedding")

    monkeypatch.setattr(candidate.embeddings, "aembed_documents", forbidden_embedding)
    with pytest.raises(ValueError, match="incompatible|contract|migration"):
        await candidate.add_memories(
            [
                MemoryRecord(
                    id="incoming", text="recent orchard", namespace="orchard", user_id="alice"
                )
            ]
        )
    assert not await redis_client.exists(Keys.memory_key("incoming", "orchard", "alice"))
    assert await redis_client.get(contract_key) == before
    if mismatch == "unversioned_data":
        assert (
            await redis_client.hget(Keys.memory_key("legacy", "orchard", "alice"), "text")
            == "recent orchard"
        )


@pytest.mark.parametrize("change", ["algorithm", "model", "model_version", "dimensions"])
async def test_index_rebuild_preserves_hashes_and_rejects_reembedding_changes(
    redis_client, monkeypatch, change
):
    monkeypatch.setattr(settings, "redisvl_vector_dimensions", 3)
    monkeypatch.setattr(settings, "embedding_model", "test-fixed-3d")
    monkeypatch.setattr(settings, "redisvl_indexing_algorithm", "flat")
    schema = _build_redis_schema()
    db = RedisVLMemoryVectorDatabase(
        AsyncSearchIndex.from_dict(schema, redis_client=redis_client), FixedEmbeddings()
    )
    memory = MemoryRecord(
        id="existing", text="recent orchard", namespace="orchard", user_id="alice"
    )
    await db.add_memories([memory])
    key = Keys.memory_key("existing", "orchard", "alice")
    before = await redis_client.execute_command("HGETALL", key, NEVER_DECODE=True)
    contract_key = f"memory_index_contract:{db.index.name}"
    old_contract = await redis_client.get(contract_key)
    vector = next(field for field in schema["fields"] if field["name"] == "vector")
    if change == "algorithm":
        vector["attrs"]["algorithm"] = "hnsw"
    elif change == "dimensions":
        vector["attrs"]["dims"] = 4
    elif change == "model_version":
        monkeypatch.setattr(settings, "embedding_model_version", "2")
    else:
        monkeypatch.setattr(settings, "embedding_model", "different-model")
    replacement = RedisVLMemoryVectorDatabase(
        AsyncSearchIndex.from_dict(schema, redis_client=redis_client), FixedEmbeddings()
    )
    if change == "algorithm":
        await replacement.rebuild_index()
        results = await replacement.search_memories(query="recent orchard")
        assert [record.id for record in results.memories] == ["existing"]
        assert await redis_client.get(contract_key) != old_contract
        assert replacement._index_created
    else:
        with pytest.raises(ValueError, match="re-embedding migration required"):
            await replacement.rebuild_index()
        assert await redis_client.get(contract_key) == old_contract
    assert await redis_client.execute_command("HGETALL", key, NEVER_DECODE=True) == before


@pytest.mark.parametrize("transport", ["sse", "http", "stdio"])
async def test_mcp_startup_rejects_persisted_embedding_contract(memory_db, monkeypatch, transport):
    from agent_memory_server import readiness
    from agent_memory_server.mcp import FastMCP

    client = await memory_db.index._get_client()
    contract_key = f"memory_index_contract:{memory_db.index.name}"
    before = await client.get(contract_key)
    monkeypatch.setattr(settings, "long_term_memory", True)
    monkeypatch.setattr(settings, "embedding_model_version", "different-version")
    candidate = RedisVLMemoryVectorDatabase(memory_db.index, FixedEmbeddings())

    async def provider():
        return candidate

    monkeypatch.setattr(readiness, "get_memory_vector_db", provider)
    server = FastMCP("real-index-readiness")
    with pytest.raises(ValueError, match="contract differs"):
        if transport == "stdio":
            await server.run_stdio_async()
        else:
            app = server.sse_app() if transport == "sse" else server.streamable_http_app()
            async with app.router.lifespan_context(app):
                pytest.fail("Incompatible persisted contract must reject startup")
    assert not candidate._index_created
    assert await client.get(contract_key) == before


@pytest.mark.parametrize(
    "failure", ["after_drop", "after_create", "contract_response", "readiness"]
)
async def test_index_rebuild_failure_restores_original_search(memory_db, monkeypatch, failure):
    client = await memory_db.index._get_client()
    await memory_db.add_memories([MemoryRecord(id="kept", text="recent orchard")])
    key = Keys.memory_key("kept", None, None)
    before = await client.execute_command("HGETALL", key, NEVER_DECODE=True)
    contract_key = f"memory_index_contract:{memory_db.index.name}"
    contract = await client.get(contract_key)
    schema = memory_db.index.schema.to_dict()
    vector = next(field for field in schema["fields"] if field["type"] == "vector")
    vector["attrs"]["algorithm"] = (
        "hnsw" if vector["attrs"]["algorithm"].lower() == "flat" else "flat"
    )
    candidate = RedisVLMemoryVectorDatabase(
        AsyncSearchIndex.from_dict(schema, redis_client=client), FixedEmbeddings()
    )
    create = candidate.index.create

    async def fail_create(**kwargs):
        if failure == "after_drop":
            await candidate.index.delete(drop=False)
        else:
            await create(**kwargs)
        raise ConnectionError("injected rebuild failure")

    async def fail_readiness():
        raise ConnectionError("injected rebuild failure")

    if failure in ("after_drop", "after_create"):
        monkeypatch.setattr(candidate.index, "create", fail_create)
    elif failure == "readiness":
        monkeypatch.setattr(candidate, "_ensure_index", fail_readiness)
    else:
        evaluate = client.eval
        fired = False

        async def lose_contract_response(script, numkeys, *args):
            nonlocal fired
            result = await evaluate(script, numkeys, *args)
            if not fired and args[0] == contract_key and "local current" in script:
                fired = True
                raise ConnectionError("injected rebuild failure")
            return result

        monkeypatch.setattr(client, "eval", lose_contract_response)
    with pytest.raises(ConnectionError, match="injected rebuild failure"):
        await candidate.rebuild_index()
    assert await client.get(contract_key) == contract
    assert not await client.exists(f"memory_index_rebuild_lock:{memory_db.index.name}")
    assert await client.execute_command("HGETALL", key, NEVER_DECODE=True) == before
    restored = RedisVLMemoryVectorDatabase(memory_db.index, FixedEmbeddings())
    result = await restored.search_memories(query="recent orchard")
    assert [record.id for record in result.memories] == ["kept"]


async def test_index_rebuild_rejects_concurrent_owner(memory_db, monkeypatch):
    client = await memory_db.index._get_client()
    entered, release = asyncio.Event(), asyncio.Event()
    create = memory_db.index.create

    async def paused_create(**kwargs):
        entered.set()
        await release.wait()
        await create(**kwargs)

    monkeypatch.setattr(memory_db.index, "create", paused_create)
    first = asyncio.create_task(memory_db.rebuild_index())
    try:
        await asyncio.wait_for(entered.wait(), 2)
        contender = RedisVLMemoryVectorDatabase(memory_db.index, FixedEmbeddings())
        with pytest.raises(ValueError, match="already active"):
            await contender.rebuild_index()
    finally:
        release.set()
        await first
    assert not await client.exists(f"memory_index_rebuild_lock:{memory_db.index.name}")


@pytest.mark.parametrize("phase", ["drop", "create", "contract"])
async def test_index_rebuild_sigkill_recovers_original(memory_db, monkeypatch, phase):
    import subprocess
    import sys

    client = await memory_db.index._get_client()
    await memory_db.add_memories([MemoryRecord(id="survivor", text="recent orchard")])
    key = Keys.memory_key("survivor", None, None)
    before = await client.execute_command("HGETALL", key, NEVER_DECODE=True)
    contract_key = f"memory_index_contract:{memory_db.index.name}"
    contract = await client.get(contract_key)
    schema = memory_db.index.schema.to_dict()
    vector = next(field for field in schema["fields"] if field["type"] == "vector")
    vector["attrs"]["algorithm"] = (
        "hnsw" if vector["attrs"]["algorithm"].lower() == "flat" else "flat"
    )
    conn = client.connection_pool.connection_kwargs
    url = f"redis://{conn['host']}:{conn['port']}"
    marker = f"rebuild-test-reached:{phase}"
    code = """
import asyncio, json, sys
from redis.asyncio import Redis
from redisvl.index import AsyncSearchIndex
from agent_memory_server.config import settings
from agent_memory_server.memory_vector_db import RedisVLMemoryVectorDatabase
async def main():
    client = Redis.from_url(sys.argv[1], decode_responses=True)
    schema, phase, marker = json.loads(sys.argv[2]), sys.argv[3], sys.argv[4]
    profile = json.loads(await client.get("memory_index_contract:" + schema["index"]["name"]))
    settings.embedding_model = profile["embedding_model"]
    settings.embedding_model_version = profile["embedding_model_version"]
    index = AsyncSearchIndex.from_dict(schema, redis_client=client)
    db = RedisVLMemoryVectorDatabase(index, None)
    create = index.create
    async def pause():
        await client.set(marker, "ready")
        await asyncio.Event().wait()
    async def interrupted_create(**kwargs):
        if phase == "drop":
            await index.delete(drop=False)
        else:
            await create(**kwargs)
        if phase != "contract":
            await pause()
    if phase == "contract":
        db._ensure_index = pause
    index.create = interrupted_create
    await db.rebuild_index()
asyncio.run(main())
"""
    child = subprocess.Popen(
        [sys.executable, "-c", code, url, json.dumps(schema), phase, marker],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        for _ in range(200):
            if await client.get(marker):
                break
            if child.poll() is not None:
                pytest.fail(child.communicate()[1].decode())
            await asyncio.sleep(0.05)
        else:
            pytest.fail("Rebuild child did not reach injected phase")
        child.kill()
        await asyncio.to_thread(child.wait, 5)
        assert child.returncode == -9
        journal_key = f"memory_index_rebuild_journal:{memory_db.index.name}"
        lock_key = f"memory_index_rebuild_lock:{memory_db.index.name}"
        raw = await client.get(journal_key)
        journal = json.loads(raw)
        assert journal["previous"] == contract
        assert await client.get(lock_key) == journal["owner"]
        recovery = RedisVLMemoryVectorDatabase(memory_db.index, FixedEmbeddings())
        with pytest.raises(ValueError, match="owner"):
            await recovery.recover_index_rebuild("wrong-owner")
        assert await client.get(journal_key) == raw
        await recovery.recover_index_rebuild(journal["owner"])
        assert not await client.exists(lock_key, journal_key)
        assert await client.get(contract_key) == contract
        assert await client.execute_command("HGETALL", key, NEVER_DECODE=True) == before
        result = await recovery.search_memories(query="recent orchard")
        assert [record.id for record in result.memories] == ["survivor"]
    finally:
        if child.poll() is None:
            child.kill()
        await asyncio.to_thread(child.wait, 5)
        child.communicate()


async def test_index_rebuild_rollback_and_recovery_failures_retain_journal(memory_db, monkeypatch):
    client = await memory_db.index._get_client()
    await memory_db.add_memories([MemoryRecord(id="retry", text="recent orchard")])
    contract_key = f"memory_index_contract:{memory_db.index.name}"
    before = await client.get(contract_key)
    journal_key = f"memory_index_rebuild_journal:{memory_db.index.name}"
    lock_key = f"memory_index_rebuild_lock:{memory_db.index.name}"
    create = AsyncSearchIndex.create
    calls = 0

    async def failing_create(index, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            await index.delete(drop=False)
        if calls <= 3:
            raise ConnectionError("unavailable during rebuild/recovery")
        await create(index, **kwargs)

    monkeypatch.setattr(AsyncSearchIndex, "create", failing_create)
    with pytest.raises(ValueError, match="rollback failed"):
        await memory_db.rebuild_index()
    journal = json.loads(await client.get(journal_key))
    assert await client.get(lock_key) == journal["owner"]
    assert journal["previous"] == before
    await client.set(contract_key, "unrelated-contract")
    with pytest.raises(ValueError, match="state changed"):
        await memory_db.recover_index_rebuild(journal["owner"])
    assert calls == 2
    assert await client.get(lock_key) == journal["owner"]
    assert json.loads(await client.get(journal_key)) == journal
    await client.set(contract_key, before)
    with pytest.raises(ConnectionError, match="unavailable"):
        await memory_db.recover_index_rebuild(journal["owner"])
    retry = json.loads(await client.get(journal_key))
    assert retry["owner"] != journal["owner"]
    assert await client.get(lock_key) == retry["owner"]
    await memory_db.recover_index_rebuild(retry["owner"])
    assert not await client.exists(lock_key, journal_key)
    assert await client.get(contract_key) == before
    result = await memory_db.search_memories(query="recent orchard")
    assert [record.id for record in result.memories] == ["retry"]


async def test_index_rebuild_gate_blocks_cached_provider_and_other_tasks(memory_db, monkeypatch):
    client = await memory_db.index._get_client()
    await memory_db.add_memories([MemoryRecord(id="guarded", text="recent orchard")])
    assert memory_db._index_created
    entered, release = asyncio.Event(), asyncio.Event()
    create = memory_db.index.create

    async def paused_create(**kwargs):
        entered.set()
        await release.wait()
        await create(**kwargs)

    async def forbidden_embedding(texts):
        pytest.fail("Blocked operations must reject before embedding")

    monkeypatch.setattr(memory_db.index, "create", paused_create)
    monkeypatch.setattr(memory_db.embeddings, "aembed_documents", forbidden_embedding)
    rebuilding = asyncio.create_task(memory_db.rebuild_index())
    try:
        await asyncio.wait_for(entered.wait(), 2)
        operations = [
            memory_db.add_memories([MemoryRecord(id="new", text="recent orchard")]),
            memory_db.search_memories(query="recent orchard"),
            memory_db.list_memories(limit=1),
            memory_db.delete_memories(memory_ids=["guarded"]),
            memory_db.compact_hash_group([]),
            memory_db.mark_extraction_completed([]),
            memory_db.get_discrete_extraction(MemoryRecord(id="guarded", text="recent orchard")),
        ]
        results = await asyncio.gather(*operations, return_exceptions=True)
        assert all(
            isinstance(result, ValueError) and "storage unavailable" in str(result)
            for result in results
        )
        assert await client.exists(Keys.memory_key("guarded", None, None))
        assert not await client.exists(Keys.memory_key("new", None, None))
    finally:
        release.set()
        await rebuilding
    result = await memory_db.search_memories(query="recent orchard")
    assert [record.id for record in result.memories] == ["guarded"]


@pytest.mark.parametrize(
    "invalid",
    [
        [1, 0],
        [[1, 0, 0]],
        [float("nan"), 0, 0],
        [float("inf"), 0, 0],
        [1e100, 0, 0],
        [True, False, False],
        ["1", "0", "0"],
    ],
)
async def test_embedding_contract_rejects_batch_before_any_publication(
    memory_db, monkeypatch, invalid
):
    client = await memory_db.index._get_client()
    await memory_db.add_memories([MemoryRecord(id="original", text="recent orchard")])
    key = Keys.memory_key("original", None, None)
    before = await client.execute_command("HGETALL", key, NEVER_DECODE=True)

    async def embed(texts):
        return [[1, 0, 0], invalid]

    monkeypatch.setattr(memory_db.embeddings, "aembed_documents", embed)
    with pytest.raises(ValueError, match="Embedding"):
        await memory_db.add_memories(
            [
                MemoryRecord(id="original", text="old orchard"),
                MemoryRecord(id="invalid", text="other ocean"),
            ]
        )
    assert await client.execute_command("HGETALL", key, NEVER_DECODE=True) == before
    assert not await client.exists(Keys.memory_key("invalid", None, None))
    assert not [key async for key in client.scan_iter(match="memory_stage:*")]


@pytest.mark.parametrize("mode", ["semantic", "range", "hybrid", "recency"])
async def test_embedding_contract_rejects_query_before_search(memory_db, monkeypatch, mode):
    await memory_db.add_memories([MemoryRecord(id="kept", text="recent orchard")])

    async def embed(text):
        return [float("nan"), 0, 0]

    monkeypatch.setattr(memory_db.embeddings, "aembed_query", embed)

    async def forbidden_query(*args, **kwargs):
        pytest.fail("Invalid query vector must reject before search/aggregate")

    monkeypatch.setattr(memory_db.index, "query", forbidden_query)
    monkeypatch.setattr(memory_db.index, "aggregate", forbidden_query)
    kwargs = {"search_mode": SearchModeEnum.HYBRID} if mode == "hybrid" else {}
    if mode == "range":
        kwargs["distance_threshold"] = 0.2
    with pytest.raises(ValueError, match="Embedding"):
        if mode == "recency":
            await memory_db._search_with_recency_aggregation(
                "recent orchard", None, 10, 0, None, {}
            )
        else:
            await memory_db.search_memories(query="recent orchard", **kwargs)


@pytest.mark.parametrize("datatype", ["float32", "float64"])
async def test_embedding_contract_uses_declared_storage_and_query_datatype(
    redis_client, monkeypatch, datatype
):
    monkeypatch.setattr(settings, "redisvl_vector_dimensions", 3)
    monkeypatch.setattr(settings, "embedding_model", "test-fixed-3d")
    schema = _build_redis_schema()
    vector = next(field for field in schema["fields"] if field["type"] == "vector")
    vector["attrs"]["datatype"] = datatype
    db = RedisVLMemoryVectorDatabase(
        AsyncSearchIndex.from_dict(schema, redis_client=redis_client), FixedEmbeddings()
    )
    await db.add_memories([MemoryRecord(id="typed", text="recent orchard")])
    raw = await redis_client.execute_command(
        "HGET", Keys.memory_key("typed", None, None), "vector", NEVER_DECODE=True
    )
    assert len(raw) == 3 * (4 if datatype == "float32" else 8)
    for mode in [SearchModeEnum.SEMANTIC, SearchModeEnum.HYBRID]:
        result = await db.search_memories(query="recent orchard", search_mode=mode)
        assert [record.id for record in result.memories] == ["typed"]
    result = await db.search_memories(query="recent orchard", distance_threshold=0.1)
    assert [record.id for record in result.memories] == ["typed"]
    result = await db._search_with_recency_aggregation("recent orchard", None, 10, 0, None, {})
    assert [record.id for record in result.memories] == ["typed"]
