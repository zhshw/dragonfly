"""Operational alarms and batch admission against the real database."""

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from redis.exceptions import ResponseError

from agent_memory_server import healthcheck, readiness
from agent_memory_server.auth import UserInfo, get_current_user
from agent_memory_server.config import settings
from agent_memory_server.models import MemoryRecord
from agent_memory_server.operations import (
    COUNTER_KEY,
    EVENT_KEY,
    increment_counter,
    observed_eval,
    operations_snapshot,
)
from agent_memory_server.utils.keys import Keys
from agent_memory_server.utils.redis import docket_prefix, docket_stream_key
from test_search import memory_db as _memory_db

memory_db = _memory_db


async def test_operations_alarm_snapshot_with_real_queue_and_failures(memory_db, monkeypatch):
    client = await memory_db.index._get_client()

    async def provider():
        return memory_db

    monkeypatch.setattr(readiness, "get_memory_vector_db", provider)
    monkeypatch.setattr(settings, "long_term_memory", True)
    monkeypatch.setattr(settings, "operations_queue_alert_threshold", 2)
    monkeypatch.setattr(settings, "operations_memory_limit_bytes", 1)
    monkeypatch.setattr(settings, "operations_scan_limit", 1)
    stream = docket_stream_key(settings.docket_name, settings.redis_url)
    prefix = docket_prefix(settings.docket_name, settings.redis_url)
    await client.xadd(stream, {"task": "fixture"})
    await client.xgroup_create(stream, "operations-test", id="0")
    await client.xreadgroup("operations-test", "consumer", {stream: ">"}, count=1)
    await client.zadd(f"{prefix}:queue", {"delayed": 1})
    await client.set("extraction_outbox:fixture", "bad")
    with pytest.raises(ResponseError):
        await observed_eval(client, "return redis.call('MISSING_OPERATION')", 0)
    snapshot = await operations_snapshot(client)
    assert snapshot["index_ready"]
    assert snapshot["queue"] == {"queued": 1, "delayed": 1, "pending": 1}
    assert snapshot["counters"]["lua_failure"] == 1
    assert {"lua_failure", "memory_capacity", "queue_backlog", "outbox_scan_limit"} <= set(
        snapshot["alerts"]
    )
    await client.set(f"memory_index_rebuild_lock:{memory_db.index.name}", "interrupted")
    snapshot = await operations_snapshot(client)
    assert not snapshot["index_ready"] and "index_unavailable" in snapshot["alerts"]
    assert snapshot["counters"]["index_unavailable"] >= 1


async def test_batch_capacity_rejects_before_embedding_and_preserves_records(
    memory_db, monkeypatch
):
    client = await memory_db.index._get_client()
    monkeypatch.setattr(settings, "memory_vector_batch_limit", 1)

    async def forbidden(texts):
        pytest.fail("Oversize batch must reject before model work")

    monkeypatch.setattr(memory_db.embeddings, "aembed_documents", forbidden)
    with pytest.raises(ValueError, match="record limit"):
        await memory_db.add_memories(
            [MemoryRecord(id="a", text="recent orchard"), MemoryRecord(id="b", text="other ocean")]
        )
    assert not await client.exists(
        Keys.memory_key("a", None, None), Keys.memory_key("b", None, None)
    )
    assert await client.hget(COUNTER_KEY, "batch_rejected") == "1"


async def test_recency_failure_is_visible_and_fallback_still_returns_data(memory_db, monkeypatch):
    client = await memory_db.index._get_client()
    await memory_db.add_memories([MemoryRecord(id="fallback", text="recent orchard")])

    async def fail(*args, **kwargs):
        raise ResponseError("injected recency failure")

    monkeypatch.setattr(memory_db.index, "aggregate", fail)
    results = await memory_db.search_memories(query="recent orchard", server_side_recency=True)
    assert [record.id for record in results.memories] == ["fallback"]
    assert await client.hget(COUNTER_KEY, "recency_fallback") == "1"


@pytest.mark.parametrize("role,status", [(None, 403), ("admin", 403), ("operations", 200)])
async def test_operations_requires_explicit_role(role, status, monkeypatch):
    monkeypatch.setattr(settings, "disable_auth", False)
    monkeypatch.setattr(settings, "auth_mode", "token")
    app = FastAPI()
    app.include_router(healthcheck.router)
    app.dependency_overrides[get_current_user] = lambda: UserInfo(
        sub="operator", roles=[role] if role else []
    )

    async def snapshot(client):
        return {"alerts": []}

    async def connection():
        return object()

    monkeypatch.setattr(healthcheck, "operations_snapshot", snapshot)
    monkeypatch.setattr(healthcheck, "get_redis_conn", connection)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://operations"
    ) as client:
        response = await client.get("/v1/operations")
    assert response.status_code == status


async def test_transient_alarm_recovers_without_losing_cumulative_counter(memory_db, monkeypatch):
    client = await memory_db.index._get_client()

    async def provider():
        return memory_db

    monkeypatch.setattr(readiness, "get_memory_vector_db", provider)
    monkeypatch.setattr(settings, "operations_error_window_seconds", 5)
    await increment_counter(client, "lua_failure")
    initial = await operations_snapshot(client)
    assert "lua_failure" in initial["alerts"]
    seconds, _ = await client.time()
    await client.hset(EVENT_KEY, "lua_failure", seconds - 6)
    recovered = await operations_snapshot(client)
    assert "lua_failure" not in recovered["alerts"]
    assert recovered["counters"]["lua_failure"] == 1
    await increment_counter(client, "lua_failure")
    repeated = await operations_snapshot(client)
    assert "lua_failure" in repeated["alerts"]
    assert repeated["counters"]["lua_failure"] == 2
