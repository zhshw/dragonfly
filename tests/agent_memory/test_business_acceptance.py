"""Complete long-term-memory business sequences on the locked SDK and real database."""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from redisvl.index import AsyncSearchIndex

from agent_memory_server import long_term_memory
from agent_memory_server.config import settings
from agent_memory_server.filters import Namespace, UserId
from agent_memory_server.llm import LLMClient
from agent_memory_server.memory_vector_db import RedisVLMemoryVectorDatabase
from agent_memory_server.memory_vector_db_factory import _build_redis_schema
from agent_memory_server.models import MemoryRecord, SearchModeEnum
from agent_memory_server.utils import redis as redis_utils
from agent_memory_server.utils.keys import Keys


class BusinessEmbeddings:
    vectors = {
        "recent orchard": [1.0, 0.0, 0.0],
        "old orchard": [0.8, 0.6, 0.0],
        "other orchard": [0.0, 1.0, 0.0],
        "orchard keeper": [1.0, 0.0, 0.0],
        "orchard grower": [1.0, 0.0, 0.0],
        "merged orchard": [1.0, 0.0, 0.0],
        "orchard": [1.0, 0.0, 0.0],
    }

    async def aembed_documents(self, texts):
        return [self.vectors[text] for text in texts]

    async def aembed_query(self, text):
        return self.vectors[text]


@pytest.mark.parametrize("algorithm", ["flat", "hnsw"])
async def test_long_term_business_complete_sequence(redis_client, monkeypatch, algorithm):
    """P1-02: real writes/reads/ranking, all dedup paths, stats and final deletion."""
    monkeypatch.setattr(settings, "redisvl_vector_dimensions", 3)
    monkeypatch.setattr(settings, "embedding_model", "business-fixed-3d")
    monkeypatch.setattr(settings, "redisvl_indexing_algorithm", algorithm)
    monkeypatch.setattr(settings, "redisvl_distance_metric", "cosine")
    monkeypatch.setattr(settings, "enable_discrete_memory_extraction", False)
    monkeypatch.setattr(settings, "compact_semantic_duplicates", False)
    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    db = RedisVLMemoryVectorDatabase(
        AsyncSearchIndex.from_dict(_build_redis_schema(), redis_client=redis_client),
        BusinessEmbeddings(),
    )

    async def get_db():
        return db

    # Structure extraction is outside P1-02: retain its real durable outbox, but
    # don't call another model merely to test the long-term business sequence.
    class PendingTasks:
        def add_task(self, *args, **kwargs):
            pass

    monkeypatch.setattr(long_term_memory, "get_memory_vector_db", get_db)
    monkeypatch.setattr(long_term_memory, "get_background_tasks", lambda: PendingTasks())
    created = datetime(2026, 9, 1, tzinfo=UTC)
    records = [
        MemoryRecord(
            id=id_,
            text=text,
            namespace="orchard",
            user_id="alice",
            session_id="session-a",
            created_at=created,
            last_accessed=created,
            topics=["fruit"],
            metadata={"source": "fixed"},
            extracted_from=["source-a"],
            pinned=id_ == "recent",
            access_count=count,
        )
        for id_, text, count in [
            ("recent", "recent orchard", 2),
            ("old", "old orchard", 3),
            ("other", "other orchard", 4),
        ]
    ]
    foreign = MemoryRecord(id="recent", text="recent orchard", namespace="orchard", user_id="bob")
    await long_term_memory.index_long_term_memories(records + [foreign], redis_client)
    foreign_key = Keys.memory_key("recent", "orchard", "bob")
    foreign_before = await redis_client.execute_command("HGETALL", foreign_key, NEVER_DECODE=True)
    for record in records:
        actual = await long_term_memory.get_long_term_memory_by_id(record.id, "orchard", "alice")
        assert actual.text == record.text and actual.access_count == record.access_count
        assert actual.topics == ["fruit"] and actual.metadata == {"source": "fixed"}
        assert (
            await redis_client.hget(Keys.memory_key(record.id, "orchard", "alice"), "text")
            == record.text
        )
    assert await long_term_memory.get_long_term_memory_by_id("missing", "orchard", "alice") is None

    raw_calls = []
    query = db.index.query
    aggregate = db.index.aggregate

    async def observe_query(request, *args, **kwargs):
        result = await query(request, *args, **kwargs)
        raw_calls.append(("query", result))
        return result

    async def observe_aggregate(request, *args, **kwargs):
        result = await aggregate(request, *args, **kwargs)
        raw_calls.append(("aggregate", result.rows))
        return result

    monkeypatch.setattr(db.index, "query", observe_query)
    monkeypatch.setattr(db.index, "aggregate", observe_aggregate)
    for mode in SearchModeEnum.SEMANTIC, SearchModeEnum.KEYWORD, SearchModeEnum.HYBRID:
        raw_calls.clear()
        result = await long_term_memory.search_long_term_memories(
            "orchard",
            search_mode=mode,
            namespace=Namespace(eq="orchard"),
            user_id=UserId(eq="alice"),
            server_side_recency=False,
        )
        assert {memory.id for memory in result.memories} == {"recent", "old", "other"}
        assert all(memory.user_id == "alice" for memory in result.memories)
        assert len(raw_calls) == 1
        branch, raw_rows = raw_calls[0]
        assert branch == ("aggregate" if mode == SearchModeEnum.HYBRID else "query")
        rows = [db._coerce_aggregate_row(row) for row in raw_rows]
        assert [memory.id for memory in result.memories] == [row["id_"] for row in rows]
        if mode == SearchModeEnum.SEMANTIC:
            assert [memory.id for memory in result.memories] == [
                "recent",
                "old",
                "other",
            ]
            assert [memory.dist for memory in result.memories] == pytest.approx(
                [0, 0.2, 1], abs=1e-5
            )
            expected = [max(0, 1 - float(row["vector_distance"])) for row in rows]
        else:
            field = "hybrid_score" if mode == SearchModeEnum.HYBRID else "score"
            raw_scores = [float(row[field]) for row in rows]
            assert max(raw_scores) > 0
            expected = [score / max(raw_scores) for score in raw_scores]
        assert [memory.score for memory in result.memories] == pytest.approx(expected)
        assert all(memory.dist == pytest.approx(1 - memory.score) for memory in result.memories)
        assert all(memory.score_type.value == mode.value for memory in result.memories)

    recent_source = await long_term_memory.get_long_term_memory_by_id("recent", "orchard", "alice")
    access_snapshot = {"recent": long_term_memory.memory_access_snapshot(recent_source)}
    assert (
        await long_term_memory.update_last_accessed(
            ["recent", "recent"],
            redis_client=redis_client,
            namespace="orchard",
            user_id="alice",
            expected_snapshots=access_snapshot,
        )
        == 1
    )
    assert (
        await long_term_memory.update_last_accessed(
            ["recent"],
            redis_client=redis_client,
            namespace="orchard",
            user_id="alice",
            expected_snapshots=access_snapshot,
        )
        == 0
    )
    accessed = await long_term_memory.get_long_term_memory_by_id("recent", "orchard", "alice")
    assert accessed.access_count == 3 and accessed.last_accessed > created

    replacement = MemoryRecord(
        id="recent",
        text="orchard keeper",
        namespace="orchard",
        user_id="alice",
        topics=["keeper"],
        metadata={"source": "replacement"},
        pinned=True,
        access_count=3,
        created_at=created,
        extracted_from=["source-a"],
    )
    await long_term_memory.index_long_term_memories([replacement], redis_client, deduplicate=True)
    actual = await long_term_memory.get_long_term_memory_by_id("recent", "orchard", "alice")
    assert actual.text == "orchard keeper" and actual.persisted_at == accessed.persisted_at
    assert (
        len(
            (
                await db.list_memories(
                    namespace=Namespace(eq="orchard"), user_id=UserId(eq="alice")
                )
            ).memories
        )
        == 3
    )

    hash_duplicate = replacement.model_copy(update={"id": "hash-duplicate"})
    await long_term_memory.index_long_term_memories(
        [hash_duplicate], redis_client, deduplicate=True
    )
    actual = await long_term_memory.get_long_term_memory_by_id("recent", "orchard", "alice")
    assert actual.access_count == 4
    assert (
        await long_term_memory.get_long_term_memory_by_id("hash-duplicate", "orchard", "alice")
        is None
    )

    model_calls = []

    async def model(**kwargs):
        model_calls.append(kwargs)
        assert "orchard keeper" in kwargs["messages"][0]["content"]
        assert "orchard grower" in kwargs["messages"][0]["content"]
        return SimpleNamespace(content="merged orchard")

    monkeypatch.setattr(LLMClient, "create_chat_completion", model)
    monkeypatch.setattr(settings, "compact_semantic_duplicates", True)
    incoming = MemoryRecord(
        id="merged",
        text="orchard grower",
        namespace="orchard",
        user_id="alice",
        session_id="session-b",
        topics=["grower"],
        metadata={"incoming": True},
        created_at=created + timedelta(days=1),
        extracted_from=["source-b"],
        access_count=2,
    )
    await long_term_memory.index_long_term_memories(
        [incoming], redis_client, deduplicate=True, vector_distance_threshold=0.01
    )
    merged = await long_term_memory.get_long_term_memory_by_id("merged", "orchard", "alice")
    assert len(model_calls) == 1 and merged.text == "merged orchard"
    assert merged.access_count == 6 and merged.pinned
    assert set(merged.topics) == {"keeper", "grower"}
    assert merged.metadata == {"source": "replacement", "incoming": True}
    assert merged.extracted_from == ["source-a", "source-b"] and merged.created_at == created
    assert await long_term_memory.get_long_term_memory_by_id("recent", "orchard", "alice") is None
    assert (
        await redis_client.get(Keys.memory_generation_keys("recent", "orchard", "alice")[0]) == "1"
    )
    recall = await long_term_memory.search_long_term_memories(
        "orchard",
        namespace=Namespace(eq="orchard"),
        user_id=UserId(eq="alice"),
        distance_threshold=0.01,
    )
    assert [memory.id for memory in recall.memories] == ["merged"]
    assert recall.memories[0].score == pytest.approx(1)

    assert (
        await long_term_memory.delete_long_term_memories(
            ["merged", "old", "other"], "orchard", "alice"
        )
        == 3
    )
    assert (
        await long_term_memory.delete_long_term_memories(
            ["merged", "old", "other"], "orchard", "alice"
        )
        == 0
    )
    for mode in SearchModeEnum.SEMANTIC, SearchModeEnum.KEYWORD, SearchModeEnum.HYBRID:
        empty = await long_term_memory.search_long_term_memories(
            "orchard",
            search_mode=mode,
            namespace=Namespace(eq="orchard"),
            user_id=UserId(eq="alice"),
        )
        assert empty.memories == []
    assert (
        await redis_client.execute_command("HGETALL", foreign_key, NEVER_DECODE=True)
        == foreign_before
    )
    assert not [key async for key in redis_client.scan_iter(match="memory_stage:*")]
