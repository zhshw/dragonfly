"""Authenticated working-memory isolation against a real two-shard database."""

import time
import asyncio
import json
import socket
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
import uvicorn
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client
from mcp.client.sse import sse_client
from docket import Docket
from redis.asyncio import Redis
from redisvl.index import AsyncSearchIndex
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import FastAPI
from starlette.responses import JSONResponse
from starlette.requests import Request
from jose import jwt

from agent_memory_server import auth, long_term_memory, readiness, summary_views
from agent_memory_server import mcp as mcp_module
from agent_memory_server.api import router
from agent_memory_server.config import settings
from agent_memory_server.dependencies import HybridBackgroundTasks
from agent_memory_server.llm import LLMClient
from agent_memory_server.memory_vector_db import RedisVLMemoryVectorDatabase
from agent_memory_server.memory_vector_db_factory import _build_redis_schema
from agent_memory_server.models import (
    MemoryRecord,
    MemoryRecordResults,
    SummaryView,
    WorkingMemory,
)
from agent_memory_server.mcp import FastMCP
from agent_memory_server.utils import redis as redis_utils
from agent_memory_server.utils.keys import Keys
from agent_memory_server.working_memory import (
    WorkingMemoryConflictError,
    delete_working_memory,
    get_working_memory,
    set_working_memory,
)
from agent_memory_server.working_memory_index import ensure_working_memory_index

from conftest import SCHEDULER_SHA
from test_docket import run_worker
from test_search import FixedEmbeddings, seed


@pytest.fixture
def signed_identity(monkeypatch):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    public = (
        key.public_key()
        .public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
        .decode()
    )
    monkeypatch.setattr(auth, "get_public_key", lambda token: public)
    monkeypatch.setattr(settings, "disable_auth", False)
    monkeypatch.setattr(settings, "auth_mode", "oauth2")
    monkeypatch.setattr(settings, "token_auth_enabled", False)
    monkeypatch.setattr(settings, "oauth2_issuer_url", "https://identity.test")
    monkeypatch.setattr(settings, "oauth2_audience", "memory-test")
    monkeypatch.setattr(settings, "oauth2_algorithms", ["RS256"])

    def issue(namespace, subject="alice"):
        now = int(time.time())
        claims = {
            "sub": subject,
            "iss": "https://identity.test",
            "aud": "memory-test",
            "iat": now,
            "exp": now + 600,
            "roles": ["admin"],
        }
        if namespace is not None:
            claims["namespace"] = namespace
        return {"Authorization": "Bearer " + jwt.encode(claims, private, algorithm="RS256")}

    return issue


async def test_working_memory_http_scope(redis_client, monkeypatch, signed_identity):
    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    monkeypatch.setattr(settings, "long_term_memory", False)
    await ensure_working_memory_index(redis_client)
    app = FastAPI()
    app.include_router(router)
    owners = [("orchard", "alice"), ("forest", "alice"), ("orchard", "bob")]
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        for namespace, user in owners:
            headers = signed_identity(namespace, user)
            response = await client.put(
                "/v1/working-memory/shared",
                headers=headers,
                json={"data": {"owner": f"{namespace}/{user}"}},
            )
            assert response.status_code == 200, response.text
            assert response.json()["namespace"] == namespace
            assert response.json()["user_id"] == user
        headers = signed_identity("orchard")
        own = await client.get("/v1/working-memory/shared", headers=headers)
        assert own.json()["data"] == {"owner": "orchard/alice"}
        listing = await client.get("/v1/working-memory/", headers=headers)
        assert listing.status_code == 200, listing.text
        assert listing.json() == {"sessions": ["shared"], "total": 1}
        for path in [
            "/v1/working-memory/shared?namespace=forest",
            "/v1/working-memory/shared?user_id=bob",
            "/v1/working-memory/?namespace=forest",
        ]:
            assert (await client.get(path, headers=headers)).status_code == 403
        assert (
            await client.delete("/v1/working-memory/shared?user_id=bob", headers=headers)
        ).status_code == 403
        assert (
            await client.put(
                "/v1/working-memory/shared",
                headers=headers,
                json={"namespace": "forest", "version": 1, "data": {"owner": "stolen"}},
            )
        ).status_code == 403
        assert (
            await client.delete("/v1/working-memory/shared", headers=headers)
        ).status_code == 200
        assert (await client.get("/v1/working-memory/shared", headers=headers)).status_code == 404
        for namespace, user in owners[1:]:
            response = await client.get(
                "/v1/working-memory/shared", headers=signed_identity(namespace, user)
            )
            assert response.json()["data"] == {"owner": f"{namespace}/{user}"}
        assert (
            await client.get("/v1/working-memory/shared", headers=signed_identity(None))
        ).status_code == 403
        assert (await client.get("/v1/working-memory/shared")).status_code == 401


async def test_authenticated_search_fallback_keeps_tenant_scope(monkeypatch, signed_identity):
    monkeypatch.setattr(settings, "long_term_memory", True)
    search = AsyncMock(return_value=MemoryRecordResults(memories=[], total=0))
    monkeypatch.setattr(long_term_memory, "search_long_term_memories", search)
    app = FastAPI()
    app.include_router(router)
    headers = signed_identity("orchard", "alice")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/v1/long-term-memory/search",
            headers=headers,
            json={"text": "missing fact"},
        )
        assert response.status_code == 200, response.text
        assert search.await_count == 2
        for call in search.await_args_list:
            assert call.kwargs["namespace"].eq == "orchard"
            assert call.kwargs["user_id"].eq == "alice"
        before = search.await_count
        for filters in [
            {"namespace": {"eq": "forest"}},
            {"user_id": {"eq": "bob"}},
            {"namespace": {"any": ["orchard", "forest"]}},
            {"namespace": {"startswith": "orch"}},
            {"user_id": {"ne": "bob"}},
        ]:
            response = await client.post(
                "/v1/long-term-memory/search",
                headers=headers,
                json={"text": "secret", **filters},
            )
            assert response.status_code == 403, response.text
        assert search.await_count == before


async def test_memory_prompt_cannot_bypass_working_scope(
    redis_client, monkeypatch, signed_identity
):
    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    monkeypatch.setattr(settings, "long_term_memory", False)
    app = FastAPI()
    app.include_router(router)
    await set_working_memory(
        WorkingMemory(session_id="foreign", namespace="forest", user_id="bob", data={"secret": 1}),
        redis_client,
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/v1/memory/prompt",
            headers=signed_identity("orchard"),
            json={
                "query": "secret",
                "session": {"session_id": "foreign", "namespace": "forest"},
            },
        )
        assert response.status_code == 403, response.text


async def test_working_memory_key_components_do_not_collide(redis_client):
    scopes = [
        (None, "a", "s"),
        ("a", None, "s"),
        ("a:b", "c", "s"),
        ("a", "b:c", "s"),
        ("a%3Ab", "c", "s"),
        ("a", "b", "c:s"),
        (None, None, "s"),
        ("", None, "s"),
        (None, "", "s"),
        ("~", "+", "s"),
    ]
    keys = set()
    for index, (namespace, user_id, session_id) in enumerate(scopes):
        keys.add(Keys.working_memory_key(session_id, user_id, namespace))
        await set_working_memory(
            WorkingMemory(
                session_id=session_id,
                namespace=namespace,
                user_id=user_id,
                data={"index": index},
            ),
            redis_client,
        )
    assert len(keys) == len(scopes)
    for index, (namespace, user_id, session_id) in enumerate(scopes):
        memory = await get_working_memory(session_id, user_id, namespace, redis_client)
        assert memory.data == {"index": index}


@pytest.mark.parametrize("operation", ["read", "write", "delete"])
async def test_legacy_key_migration_preserves_scope_ttl_revision(redis_client, operation):
    await ensure_working_memory_index(redis_client)
    original = WorkingMemory(
        session_id="legacy",
        namespace="orchard",
        user_id="alice",
        version=7,
        data={"kept": True},
    )
    legacy_key = "working_memory:orchard:alice:legacy"
    document = original.model_dump(mode="json")
    for field in ("created_at", "updated_at", "last_accessed"):
        document[field] = int(getattr(original, field).timestamp())
    await redis_client.json().set(legacy_key, "$", document)
    # A counter from an older deployment may lag behind the document version.
    await redis_client.set(f"working_memory_revision:{legacy_key}", 6)
    await redis_client.expire(legacy_key, 300)
    new_key = Keys.working_memory_key("legacy", "alice", "orchard")
    if operation == "delete":
        await delete_working_memory("legacy", "alice", "orchard", redis_client)
        assert not await redis_client.exists(legacy_key)
        assert not await redis_client.exists(new_key)
        assert await redis_client.get(f"working_memory_revision:{new_key}") == "7"
        return
    if operation == "write":
        original.data = {"written_without_get": True}
        await set_working_memory(original, redis_client)
        loaded = await get_working_memory("legacy", "alice", "orchard", redis_client)
        assert loaded.version == 8 and loaded.data == original.data
        assert not await redis_client.exists(legacy_key)
        assert await redis_client.get(f"working_memory_revision:{new_key}") == "8"
        return
    loaded = await get_working_memory("legacy", "alice", "orchard", redis_client)
    assert loaded.version == 7 and loaded.data == {"kept": True}
    assert not await redis_client.exists(legacy_key)
    assert 0 < await redis_client.ttl(new_key) <= 300
    assert await redis_client.get(f"working_memory_revision:{new_key}") == "7"
    loaded.data = {"updated": True}
    await set_working_memory(loaded, redis_client)
    assert loaded.version == 8
    with pytest.raises(WorkingMemoryConflictError):
        await set_working_memory(original, redis_client)


async def test_stale_legacy_key_cannot_resurrect_deleted_scope(redis_client):
    await ensure_working_memory_index(redis_client)
    original = WorkingMemory(session_id="deleted", namespace="orchard", user_id="alice")
    await set_working_memory(original, redis_client)
    document = await redis_client.json().get(Keys.working_memory_key("deleted", "alice", "orchard"))
    await delete_working_memory("deleted", "alice", "orchard", redis_client)
    await redis_client.json().set("working_memory:orchard:alice:deleted", "$", document)
    assert await get_working_memory("deleted", "alice", "orchard", redis_client) is None
    assert not await redis_client.exists(Keys.working_memory_key("deleted", "alice", "orchard"))


async def test_summary_and_task_http_ownership(redis_client, monkeypatch, signed_identity):
    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    scheduled = []
    monkeypatch.setattr(
        HybridBackgroundTasks,
        "add_task",
        lambda self, *args, **kwargs: scheduled.append((args, kwargs)),
    )
    app = FastAPI()
    app.include_router(router)
    alice = signed_identity("orchard", "alice")
    bob = signed_identity("orchard", "bob")
    forest = signed_identity("forest", "alice")
    await summary_views.save_summary_view(SummaryView(id="legacy-unowned", source="long_term"))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        ids = []
        for headers in [alice, bob, forest]:
            response = await client.post(
                "/v1/summary-views",
                headers=headers,
                json={"source": "long_term", "group_by": ["user_id"]},
            )
            assert response.status_code == 200, response.text
            ids.append(response.json()["id"])
        view_id = ids[0]
        view = await summary_views.get_summary_view(view_id)
        assert view.owner_namespace == "orchard" and view.owner_user_id == "alice"
        assert view.filters == {"namespace": "orchard", "user_id": "alice"}
        listed = await client.get("/v1/summary-views", headers=alice)
        assert [view["id"] for view in listed.json()] == [view_id]
        assert (
            await client.get("/v1/summary-views/legacy-unowned", headers=alice)
        ).status_code == 404
        for headers in [bob, forest]:
            for path in [
                f"/v1/summary-views/{view_id}",
                f"/v1/summary-views/{view_id}/partitions",
            ]:
                assert (await client.get(path, headers=headers)).status_code == 404
            assert (
                await client.delete(f"/v1/summary-views/{view_id}", headers=headers)
            ).status_code == 404
            assert (
                await client.post(f"/v1/summary-views/{view_id}/run", headers=headers, json={})
            ).status_code == 404
            assert (
                await client.post(
                    f"/v1/summary-views/{view_id}/partitions/run",
                    headers=headers,
                    json={"group": {"user_id": "alice"}},
                )
            ).status_code == 404
        assert scheduled == []
        for filters in [
            {"namespace": "forest"},
            {"user_id": "bob"},
            {"namespace": {"any": ["orchard", "forest"]}},
            {"namespace": {"eq": "orchard", "ne": "forest"}},
        ]:
            response = await client.post(
                "/v1/summary-views",
                headers=alice,
                json={"source": "long_term", "filters": filters},
            )
            assert response.status_code == 403, response.text
        assert (
            await client.post(
                f"/v1/summary-views/{view_id}/partitions/run",
                headers=alice,
                json={"group": {"user_id": "bob"}},
            )
        ).status_code == 403
        assert (
            await client.get(
                f"/v1/summary-views/{view_id}/partitions?namespace=forest",
                headers=alice,
            )
        ).status_code == 403
        first = await client.post(
            f"/v1/summary-views/{view_id}/run",
            headers=alice,
            json={"task_id": "custom:task"},
        )
        assert first.status_code == 200, first.text
        task = first.json()
        assert task["owner_namespace"] == "orchard" and task["owner_user_id"] == "alice"
        assert task["view_generation"] == view.generation
        assert len(scheduled) == 1
        assert scheduled[0][1]["expected_generation"] == view.generation
        duplicate = await client.post(
            f"/v1/summary-views/{view_id}/run",
            headers=alice,
            json={"task_id": "custom:task"},
        )
        assert duplicate.json() == task
        assert len(scheduled) == 1
        other = await client.post(
            f"/v1/summary-views/{ids[1]}/run",
            headers=bob,
            json={"task_id": "custom:task"},
        )
        assert other.status_code == 200, other.text
        assert other.json()["id"] != task["id"]
        for headers in [bob, forest]:
            assert (await client.get(f"/v1/tasks/{task['id']}", headers=headers)).status_code == 404
        assert (await client.get(f"/v1/tasks/{task['id']}", headers=alice)).json() == task
        assert (
            await client.delete(f"/v1/summary-views/{view_id}", headers=alice)
        ).status_code == 200
        assert await summary_views.get_summary_view(ids[1]) is not None


async def test_background_summary_owner_filters_cannot_be_overridden():
    view = SummaryView(
        id="owned",
        source="long_term",
        owner_namespace="orchard",
        owner_user_id="alice",
        filters={"namespace": "forest", "user_id": "bob"},
    )
    filters = summary_views._build_long_term_filters_for_view(view)
    assert filters["namespace"].eq == "orchard" and filters["user_id"].eq == "alice"
    with pytest.raises(ValueError, match="ownership"):
        summary_views._build_long_term_filters_for_view(view, {"namespace": "forest"})


async def test_http_summary_runs_through_real_docket_and_scoped_search(
    dragonfly_factory, monkeypatch, signed_identity
):
    url = dragonfly_factory(f"--lua_undeclared_keys_shas={SCHEDULER_SHA}")
    monkeypatch.setattr(settings, "redis_url", url)
    monkeypatch.setattr(settings, "docket_name", "memory-server")
    monkeypatch.setattr(settings, "use_docket", True)
    monkeypatch.setattr(settings, "long_term_memory", True)
    monkeypatch.setattr(settings, "redisvl_vector_dimensions", 3)
    monkeypatch.setattr(settings, "embedding_model", "test-fixed-3d")
    monkeypatch.setattr(settings, "openai_api_key", "test-only-placeholder")
    model = AsyncMock(return_value=SimpleNamespace(content="Two orchard facts."))
    monkeypatch.setattr(LLMClient, "create_chat_completion", model)
    async with (
        Redis.from_url(url, decode_responses=True) as client,
        Docket(name="memory-server", url=url) as docket,
    ):
        monkeypatch.setattr(redis_utils, "_redis_pool", client)
        index = AsyncSearchIndex.from_dict(_build_redis_schema(), redis_client=client)
        db = RedisVLMemoryVectorDatabase(index, FixedEmbeddings())
        await db._ensure_index()
        await seed(db)

        async def get_db():
            return db

        monkeypatch.setattr(long_term_memory, "get_memory_vector_db", get_db)
        docket.register(summary_views.refresh_summary_view)
        app = FastAPI()
        app.include_router(router)
        headers = signed_identity("tenant-a", "alice")
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app), base_url="http://test"
        ) as http:
            response = await http.post(
                "/v1/summary-views",
                headers=headers,
                json={"source": "long_term", "group_by": ["user_id"]},
            )
            assert response.status_code == 200, response.text
            view_id = response.json()["id"]
            response = await http.post(
                f"/v1/summary-views/{view_id}/run",
                headers=headers,
                json={"task_id": "run"},
            )
            assert response.status_code == 200, response.text
            task = response.json()
            assert task["status"] == "pending"
            assert await client.xlen(docket.stream_key) == 1
            duplicate = await http.post(
                f"/v1/summary-views/{view_id}/run",
                headers=headers,
                json={"task_id": "run"},
            )
            assert duplicate.json() == task
            assert await client.xlen(docket.stream_key) == 1
            await run_worker(docket)
            status = await http.get(f"/v1/tasks/{task['id']}", headers=headers)
            assert status.json()["status"] == "success", status.text
            result = await http.get(f"/v1/summary-views/{view_id}/partitions", headers=headers)
            assert len(result.json()) == 1
            assert result.json()[0]["summary"] == "Two orchard facts."
            assert result.json()[0]["memory_count"] == 2
            assert result.json()[0]["group"] == {"user_id": "alice"}
            model.assert_awaited_once()

            prompt = model.await_args.args[1][0]["content"]
            assert "recent orchard" in prompt and "old orchard" in prompt
            assert "other ocean" not in prompt
            assert (await client.xpending(docket.stream_key, docket.worker_group_name))[
                "pending"
            ] == 0
            queued = await http.post(
                f"/v1/summary-views/{view_id}/run",
                headers=headers,
                json={"task_id": "obsolete"},
            )
            assert queued.status_code == 200, queued.text
            old_task = queued.json()
            view = await summary_views.get_summary_view(view_id)
            await summary_views.save_summary_view(view)
            conflicting = await http.post(
                f"/v1/summary-views/{view_id}/run",
                headers=headers,
                json={"task_id": "run"},
            )
            assert conflicting.status_code == 409, conflicting.text
            await run_worker(docket)
            status = await http.get(f"/v1/tasks/{old_task['id']}", headers=headers)
            assert status.json()["status"] == "failed"
            assert "changed before execution" in status.json()["error_message"]
            assert await summary_views.list_partition_results(view_id) == []
            model.assert_awaited_once()


async def test_http_long_term_same_id_is_independent_per_owner(
    dragonfly_factory, monkeypatch, signed_identity
):
    url = dragonfly_factory(f"--lua_undeclared_keys_shas={SCHEDULER_SHA}")
    monkeypatch.setattr(settings, "redis_url", url)
    monkeypatch.setattr(settings, "docket_name", "memory-server")
    monkeypatch.setattr(settings, "use_docket", True)
    monkeypatch.setattr(settings, "long_term_memory", True)
    monkeypatch.setattr(settings, "redisvl_vector_dimensions", 3)
    monkeypatch.setattr(settings, "embedding_model", "test-fixed-3d")
    # This test verifies storage/ownership, not model-driven topic extraction.
    monkeypatch.setattr(settings, "enable_discrete_memory_extraction", False)
    monkeypatch.setattr(long_term_memory, "get_background_tasks", lambda: MagicMock())
    async with (
        Redis.from_url(url, decode_responses=True) as client,
        Docket(name="memory-server", url=url) as docket,
    ):
        monkeypatch.setattr(redis_utils, "_redis_pool", client)
        index = AsyncSearchIndex.from_dict(_build_redis_schema(), redis_client=client)
        db = RedisVLMemoryVectorDatabase(index, FixedEmbeddings())
        await db._ensure_index()

        async def get_db():
            return db

        monkeypatch.setattr(long_term_memory, "get_memory_vector_db", get_db)
        docket.register(long_term_memory.index_long_term_memories)
        app = FastAPI()
        app.include_router(router)
        owners = [
            ("orchard", "alice"),
            ("forest", "alice"),
            ("orchard", "bob"),
            ("Orchard", "alice"),
            ("orchard", "Alice"),
        ]
        texts = [
            "recent orchard",
            "old orchard",
            "other ocean",
            "recent orchard",
            "old orchard",
        ]
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app), base_url="http://test"
        ) as http:
            for (namespace, user), text in zip(owners, texts):
                response = await http.post(
                    "/v1/long-term-memory/",
                    headers=signed_identity(namespace, user),
                    json={
                        "memories": [{"id": "shared:id%", "text": text}],
                        "deduplicate": False,
                    },
                )
                assert response.status_code == 200, response.text
            await run_worker(docket)
            assert len({Keys.memory_key("shared:id%", *owner) for owner in owners}) == 5
            assert await long_term_memory.get_long_term_memory_by_id("shared:id%") is None
            for (namespace, user), text in zip(owners, texts):
                response = await http.get(
                    "/v1/long-term-memory/shared:id%25",
                    headers=signed_identity(namespace, user),
                )
                assert response.status_code == 200, response.text
                assert response.json()["text"] == text
                assert response.json()["namespace"] == namespace
                assert response.json()["user_id"] == user
                assert await client.exists(Keys.memory_key("shared:id%", namespace, user))
            alice = signed_identity("orchard", "alice")
            denied = await http.patch(
                "/v1/long-term-memory/shared:id%25",
                headers=alice,
                json={"namespace": "forest"},
            )
            assert denied.status_code == 403, denied.text
            changed = await http.patch(
                "/v1/long-term-memory/shared:id%25",
                headers=alice,
                json={"text": "old orchard"},
            )
            assert changed.status_code == 200, changed.text
            assert (
                changed.json()["namespace"] == "orchard" and changed.json()["text"] == "old orchard"
            )
            deleted = await http.delete(
                "/v1/long-term-memory",
                headers=alice,
                params={"memory_ids": "shared:id%"},
            )
            assert deleted.status_code == 200 and "deleted 1" in deleted.json()["status"]
            assert (
                await http.get("/v1/long-term-memory/shared:id%25", headers=alice)
            ).status_code == 404
            for namespace, user in owners[1:]:
                response = await http.get(
                    "/v1/long-term-memory/shared:id%25",
                    headers=signed_identity(namespace, user),
                )
                assert response.status_code == 200, response.text
            bad_create = await http.post(
                "/v1/long-term-memory/",
                headers=alice,
                json={"memories": [{"id": "foreign", "text": "recent orchard", "user_id": "bob"}]},
            )
            assert bad_create.status_code == 403, bad_create.text


async def test_mcp_http_validates_signed_bearer_identity(signed_identity):
    server = FastMCP("signed-mcp-auth")

    class Manager:
        async def handle_request(self, scope, receive, send):
            user = server._request_context.get().state.mcp_user
            await JSONResponse({"namespace": user.namespace, "user": user.sub})(
                scope, receive, send
            )

        def run(self):
            raise AssertionError("Lifespan unused in this transport test")

    server._session_manager = Manager()
    app = server.streamable_http_app()
    path = server.settings.streamable_http_path
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        own = await client.get("/orchard" + path, headers=signed_identity("orchard"))
        assert own.status_code == 200
        assert own.json() == {"namespace": "orchard", "user": "alice"}
        assert (
            await client.get("/forest" + path, headers=signed_identity("orchard"))
        ).status_code == 403
        assert (await client.get(path, headers=signed_identity(None))).status_code == 403
        assert (
            await client.get(path, headers={"Authorization": "Bearer invalid"})
        ).status_code == 401
    assert server._request_context.get() is None


async def test_mcp_identity_reaches_actual_same_id_read_delete(
    redis_client, monkeypatch, signed_identity
):
    monkeypatch.setattr(settings, "redisvl_vector_dimensions", 3)
    monkeypatch.setattr(settings, "embedding_model", "test-fixed-3d")
    monkeypatch.setattr(settings, "long_term_memory", True)
    index = AsyncSearchIndex.from_dict(_build_redis_schema(), redis_client=redis_client)
    db = RedisVLMemoryVectorDatabase(index, FixedEmbeddings())

    async def get_db():
        return db

    monkeypatch.setattr(long_term_memory, "get_memory_vector_db", get_db)
    records = [
        MemoryRecord(id="shared", text="recent orchard", namespace="orchard", user_id="alice"),
        MemoryRecord(id="shared", text="old orchard", namespace="forest", user_id="alice"),
    ]
    await db.add_memories(records)
    headers = [(k.lower().encode(), v.encode()) for k, v in signed_identity("orchard").items()]
    request = Request({"type": "http", "headers": headers, "path_params": {}})
    await mcp_module._authenticate_mcp_request(request)
    token = mcp_module.mcp_app._request_context.set(request)
    try:
        own = await mcp_module.get_long_term_memory("shared")
        assert own.namespace == "orchard"
        assert own.text == "recent orchard"
        ack = await mcp_module.delete_long_term_memories(["shared"])
        assert ack.status == "ok, deleted 1 memories"
    finally:
        mcp_module.mcp_app._request_context.reset(token)
    assert not await redis_client.exists(Keys.memory_key("shared", "orchard", "alice"))
    assert await redis_client.exists(Keys.memory_key("shared", "forest", "alice"))


@pytest.mark.parametrize("transport", ["http", "sse"])
async def test_real_mcp_client_owner_and_database(
    redis_client, monkeypatch, signed_identity, transport
):
    monkeypatch.setattr(settings, "redisvl_vector_dimensions", 3)
    monkeypatch.setattr(settings, "embedding_model", "test-fixed-3d")
    monkeypatch.setattr(settings, "long_term_memory", True)
    index = AsyncSearchIndex.from_dict(_build_redis_schema(), redis_client=redis_client)
    db = RedisVLMemoryVectorDatabase(index, FixedEmbeddings())

    async def get_db():
        return db

    monkeypatch.setattr(long_term_memory, "get_memory_vector_db", get_db)
    monkeypatch.setattr(readiness, "get_memory_vector_db", get_db)
    await db.add_memories(
        [
            MemoryRecord(id="shared", text="recent orchard", namespace="orchard", user_id="alice"),
            MemoryRecord(id="shared", text="old orchard", namespace="orchard", user_id="bob"),
        ]
    )
    mcp = mcp_module.mcp_app
    monkeypatch.setattr(mcp, "_session_manager", None)
    monkeypatch.setattr(mcp.settings, "stateless_http", False)
    app = mcp.streamable_http_app() if transport == "http" else mcp.sse_app()
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    port = listener.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, log_level="error", lifespan="on"))
    serving = asyncio.create_task(server.serve(sockets=[listener]))
    url = f"http://127.0.0.1:{port}" + (
        mcp.settings.streamable_http_path if transport == "http" else mcp.settings.sse_path
    )

    @asynccontextmanager
    async def connect():
        if transport == "http":
            async with streamablehttp_client(url, headers=signed_identity("orchard")) as streams:
                yield streams
        else:
            async with sse_client(url, headers=signed_identity("orchard")) as (
                read,
                write,
            ):
                yield read, write, lambda: None

    try:
        async with asyncio.timeout(20):
            while not server.started:
                if serving.done():
                    await serving
                    raise AssertionError("MCP server stopped during startup")
                await asyncio.sleep(0.01)
            async with connect() as (read, write, get_session_id):
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    session_id = get_session_id()
                    assert session_id or transport == "sse"
                    result = await session.call_tool(
                        "get_long_term_memory", {"memory_id": "shared"}
                    )
                    assert not result.isError, result
                    payload = json.loads(result.content[0].text)
                    assert payload["text"] == "recent orchard"
                    assert payload["user_id"] == "alice"
                    if transport == "http":
                        async with httpx.AsyncClient() as client:
                            stolen = await client.post(
                                url,
                                headers={
                                    **signed_identity("orchard", "bob"),
                                    "Mcp-Session-Id": session_id,
                                },
                                json={},
                            )
                            assert stolen.status_code == 404
                    deleted = await session.call_tool(
                        "delete_long_term_memories", {"memory_ids": ["shared"]}
                    )
                    assert not deleted.isError, deleted
                    assert not await redis_client.exists(
                        Keys.memory_key("shared", "orchard", "alice")
                    )
                    assert await redis_client.exists(Keys.memory_key("shared", "orchard", "bob"))
                    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
                    monkeypatch.setattr(settings, "long_term_memory", False)
                    await ensure_working_memory_index(redis_client)

                    async def no_summary(memory, **kwargs):
                        return memory

                    monkeypatch.setattr(
                        "agent_memory_server.api._summarize_working_memory", no_summary
                    )
                    message = {
                        "id": "mcp-input",
                        "role": "user",
                        "content": "new input",
                        "created_at": "2026-09-30T00:00:00Z",
                        "discrete_memory_extracted": "t",
                        "extraction_generation": "forged",
                    }
                    created = await session.call_tool(
                        "set_working_memory",
                        {"session_id": "shared", "messages": [message]},
                    )
                    assert not created.isError, created
                    working = json.loads(created.content[0].text)
                    assert working["namespace"] == "orchard" and working["user_id"] == "alice"
                    key = Keys.working_memory_key("shared", "alice", "orchard")
                    actual = await redis_client.json().get(key)
                    assert actual["messages"][0]["discrete_memory_extracted"] == "f"
                    assert actual["messages"][0]["extraction_generation"] is None
                    message["content"] = "edited input"
                    update = {
                        "session_id": "shared",
                        "version": working["version"],
                        "messages": [message],
                    }
                    changed = await session.call_tool("set_working_memory", update)
                    assert not changed.isError, changed
                    stale = await session.call_tool("set_working_memory", update)
                    assert stale.isError
                    actual = await redis_client.json().get(key)
                    assert actual["version"] == json.loads(changed.content[0].text)["version"]
                    assert actual["messages"][0]["content"] == "edited input"
                    assert actual["messages"][0]["extraction_generation"] is None
                    denied = await session.call_tool(
                        "set_working_memory", dict(update, user_id="bob")
                    )
                    assert denied.isError
                    assert await redis_client.json().get(key) == actual
            if transport == "http":
                async with httpx.AsyncClient() as client:
                    ended = await client.post(
                        url,
                        headers={
                            **signed_identity("orchard"),
                            "Mcp-Session-Id": session_id,
                        },
                        json={},
                    )
                    assert ended.status_code == 404
    finally:
        server.should_exit = True
        await asyncio.wait_for(serving, 10)
        listener.close()


async def test_http_update_cannot_revive_deleted_memory(redis_client, monkeypatch, signed_identity):
    monkeypatch.setattr(settings, "long_term_memory", True)
    monkeypatch.setattr(settings, "redisvl_vector_dimensions", 3)
    monkeypatch.setattr(settings, "embedding_model", "test-fixed-3d")
    index = AsyncSearchIndex.from_dict(_build_redis_schema(), redis_client=redis_client)
    db = RedisVLMemoryVectorDatabase(index, FixedEmbeddings())
    await db.add_memories(
        [
            MemoryRecord(
                id="patch-race",
                text="recent orchard",
                namespace="orchard",
                user_id="alice",
            )
        ]
    )

    async def get_db():
        return db

    monkeypatch.setattr(long_term_memory, "get_memory_vector_db", get_db)
    entered, release = asyncio.Event(), asyncio.Event()

    async def embed(texts):
        entered.set()
        await release.wait()
        return [[1.0, 0.0, 0.0]]

    monkeypatch.setattr(db.embeddings, "aembed_documents", embed)
    app = FastAPI()
    app.include_router(router)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as http:
        headers = signed_identity("orchard", "alice")
        pending = asyncio.create_task(
            http.patch(
                "/v1/long-term-memory/patch-race",
                headers=headers,
                json={"text": "old orchard"},
            )
        )
        try:
            await asyncio.wait_for(entered.wait(), 5)
            removed = await http.delete(
                "/v1/long-term-memory",
                headers=headers,
                params={"memory_ids": "patch-race"},
            )
            assert removed.status_code == 200, removed.text
            assert "deleted 1" in removed.json()["status"]
            release.set()
            response = await asyncio.wait_for(pending, 5)
            assert response.status_code == 400, response.text
            assert "deleted before publication" in response.json()["detail"]
            assert not await redis_client.exists(Keys.memory_key("patch-race", "orchard", "alice"))
            missing = await http.get("/v1/long-term-memory/patch-race", headers=headers)
            assert missing.status_code == 404, missing.text
            assert [key async for key in redis_client.scan_iter("memory_stage:*")] == []
        finally:
            release.set()
            if not pending.done():
                await asyncio.wait_for(pending, 5)


@pytest.mark.parametrize("changed_field", ["content", "role", "created_at"])
async def test_authenticated_put_normalizes_message_state_on_real_storage(
    redis_client, monkeypatch, signed_identity, changed_field
):
    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    monkeypatch.setattr(settings, "long_term_memory", False)

    async def no_summary(memory, **kwargs):
        return memory

    monkeypatch.setattr("agent_memory_server.api._summarize_working_memory", no_summary)
    await ensure_working_memory_index(redis_client)
    app = FastAPI()
    app.include_router(router)
    path = "/v1/working-memory/shared"
    headers = signed_identity("orchard", "alice")
    message = {
        "id": "input",
        "role": "user",
        "content": "new input",
        "created_at": "2026-09-30T00:00:00Z",
        "discrete_memory_extracted": "t",
        "extraction_generation": "forged",
        "persisted_at": "2026-09-30T00:00:01Z",
    }
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        created = await client.put(path, headers=headers, json={"messages": [message]})
        assert created.status_code == 200, created.text
        normalized = created.json()["messages"][0]
        assert normalized["discrete_memory_extracted"] == "f"
        assert normalized["extraction_generation"] is None
        assert normalized["persisted_at"] is None
        stored = await get_working_memory("shared", "alice", "orchard", redis_client)
        stored.messages[0].discrete_memory_extracted = "t"
        stored.messages[0].extraction_generation = "server-owned"
        await set_working_memory(stored, redis_client)
        unchanged = await client.put(
            path,
            headers=headers,
            json={"version": stored.version, "messages": [message]},
        )
        assert unchanged.status_code == 200, unchanged.text
        assert unchanged.json()["messages"][0]["extraction_generation"] == "server-owned"
        assert unchanged.json()["messages"][0]["discrete_memory_extracted"] == "t"
        finished = await get_working_memory("shared", "alice", "orchard", redis_client)
        finished.messages[0].extraction_generation = None
        await set_working_memory(finished, redis_client)
        edited = dict(message)
        edited[changed_field] = {
            "content": "edited input",
            "role": "assistant",
            "created_at": "2026-09-30T00:01:00Z",
        }[changed_field]
        update = {"version": finished.version, "messages": [edited]}
        changed = await client.put(path, headers=headers, json=update)
        assert changed.status_code == 200, changed.text
        stale = await client.put(path, headers=headers, json=update)
        assert stale.status_code == 409, stale.text
        key = Keys.working_memory_key("shared", "alice", "orchard")
        actual = await redis_client.json().get(key)
        assert actual["version"] == changed.json()["version"]
        assert actual["messages"][0]["discrete_memory_extracted"] == "f"
        assert actual["messages"][0]["extraction_generation"] is None
        assert actual["messages"][0]["persisted_at"] is None
        denied = await client.put(
            path,
            headers=signed_identity("orchard", "bob"),
            json=dict(update, user_id="alice"),
        )
        assert denied.status_code == 403, denied.text
        assert await redis_client.json().get(key) == actual


async def test_authenticated_http_working_memory_worker_recall_delete_chain(
    dragonfly_factory, monkeypatch, signed_identity
):
    url = dragonfly_factory(f"--lua_undeclared_keys_shas={SCHEDULER_SHA}")
    monkeypatch.setattr(settings, "redis_url", url)
    monkeypatch.setattr(settings, "docket_name", "http-memory-chain")
    monkeypatch.setattr(settings, "use_docket", True)
    monkeypatch.setattr(settings, "long_term_memory", True)
    monkeypatch.setattr(settings, "enable_discrete_memory_extraction", True)
    monkeypatch.setattr(settings, "extraction_debounce_seconds", 1)
    monkeypatch.setattr(settings, "compact_semantic_duplicates", False)
    monkeypatch.setattr(settings, "redisvl_vector_dimensions", 3)
    monkeypatch.setattr(settings, "embedding_model", "test-fixed-3d")
    monkeypatch.setattr(long_term_memory, "get_background_tasks", lambda: MagicMock())
    model_calls = []

    async def extract(**kwargs):
        model_calls.append(kwargs)
        return [
            MemoryRecord(
                id="derived",
                text="recent orchard",
                namespace=kwargs["namespace"],
                user_id=kwargs["user_id"],
                session_id=kwargs["session_id"],
                discrete_memory_extracted="t",
            )
        ]

    monkeypatch.setattr(long_term_memory, "extract_memories_from_session_thread", extract)
    async with (
        Redis.from_url(url, decode_responses=True) as redis,
        Docket(name="http-memory-chain", url=url) as docket,
    ):
        monkeypatch.setattr(redis_utils, "_redis_pool", redis)
        await ensure_working_memory_index(redis)
        db = RedisVLMemoryVectorDatabase(
            AsyncSearchIndex.from_dict(_build_redis_schema(), redis_client=redis),
            FixedEmbeddings(),
        )

        async def get_db():
            return db

        monkeypatch.setattr(long_term_memory, "get_memory_vector_db", get_db)
        docket.register(long_term_memory.promote_working_memory_to_long_term)
        docket.register(long_term_memory.run_delayed_extraction)
        app = FastAPI()
        app.include_router(router)
        headers = signed_identity("orchard", "alice")
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app), base_url="http://test"
        ) as http:
            response = await http.put(
                "/v1/working-memory/shared",
                headers=headers,
                json={
                    "messages": [
                        {
                            "role": "user",
                            "content": "Remember my orchard preference",
                            "created_at": "2026-09-01T00:00:00+00:00",
                        }
                    ]
                },
            )
            assert response.status_code == 200, response.text
            await run_worker(docket)
            assert len(model_calls) == 1
            response = await http.get("/v1/long-term-memory/derived", headers=headers)
            assert (
                response.status_code == 200 and response.json()["text"] == "recent orchard"
            ), response.text
            recall = await http.post(
                "/v1/long-term-memory/search",
                headers=headers,
                json={"text": "orchard", "search_mode": "keyword"},
            )
            assert recall.status_code == 200, recall.text
            assert [memory["id"] for memory in recall.json()["memories"]] == ["derived"]
            foreign = await http.get(
                "/v1/long-term-memory/derived",
                headers=signed_identity("orchard", "bob"),
            )
            assert foreign.status_code == 404
            working = await http.get("/v1/working-memory/shared", headers=headers)
            assert working.json()["messages"][0]["discrete_memory_extracted"] == "t"
            batches = [
                json.loads(await redis.get(key))
                async for key in redis.scan_iter(match="extraction_batch:*")
            ]
            assert len(batches) == 1 and batches[0]["state"] == "finalized"
            response = await http.delete(
                "/v1/long-term-memory",
                headers=headers,
                params={"memory_ids": "derived"},
            )
            assert response.status_code == 200, response.text
            assert not await redis.exists(Keys.memory_key("derived", "orchard", "alice"))
            recall = await http.post(
                "/v1/long-term-memory/search",
                headers=headers,
                json={"text": "orchard", "search_mode": "keyword"},
            )
            assert recall.status_code == 200 and recall.json()["memories"] == [], recall.text
            assert (await redis.xpending(docket.stream_key, docket.worker_group_name))[
                "pending"
            ] == 0


@pytest.mark.parametrize("transport", ["http", "sse"])
async def test_authenticated_mcp_working_memory_worker_recall_delete_chain(
    dragonfly_factory, monkeypatch, signed_identity, transport
):
    url = dragonfly_factory(f"--lua_undeclared_keys_shas={SCHEDULER_SHA}")
    monkeypatch.setattr(settings, "redis_url", url)
    monkeypatch.setattr(settings, "docket_name", "http-memory-chain")
    monkeypatch.setattr(settings, "use_docket", True)
    monkeypatch.setattr(settings, "long_term_memory", True)
    monkeypatch.setattr(settings, "enable_discrete_memory_extraction", True)
    monkeypatch.setattr(settings, "extraction_debounce_seconds", 1)
    monkeypatch.setattr(settings, "compact_semantic_duplicates", False)
    monkeypatch.setattr(settings, "redisvl_vector_dimensions", 3)
    monkeypatch.setattr(settings, "embedding_model", "test-fixed-3d")
    monkeypatch.setattr(long_term_memory, "get_background_tasks", lambda: MagicMock())
    model_calls = []

    async def extract(**kwargs):
        model_calls.append(kwargs)
        return [
            MemoryRecord(
                id="derived",
                text="recent orchard",
                namespace=kwargs["namespace"],
                user_id=kwargs["user_id"],
                session_id=kwargs["session_id"],
                discrete_memory_extracted="t",
            )
        ]

    monkeypatch.setattr(long_term_memory, "extract_memories_from_session_thread", extract)
    async with (
        Redis.from_url(url, decode_responses=True) as redis,
        Docket(name="http-memory-chain", url=url) as docket,
    ):
        monkeypatch.setattr(redis_utils, "_redis_pool", redis)
        await ensure_working_memory_index(redis)
        db = RedisVLMemoryVectorDatabase(
            AsyncSearchIndex.from_dict(_build_redis_schema(), redis_client=redis),
            FixedEmbeddings(),
        )

        async def get_db():
            return db

        monkeypatch.setattr(long_term_memory, "get_memory_vector_db", get_db)
        docket.register(long_term_memory.promote_working_memory_to_long_term)
        docket.register(long_term_memory.run_delayed_extraction)
        mcp = mcp_module.mcp_app
        monkeypatch.setattr(readiness, "get_memory_vector_db", get_db)
        monkeypatch.setattr(mcp, "_session_manager", None)
        monkeypatch.setattr(mcp.settings, "stateless_http", False)
        app = mcp.streamable_http_app() if transport == "http" else mcp.sse_app()
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        port = listener.getsockname()[1]
        server = uvicorn.Server(uvicorn.Config(app, log_level="error", lifespan="on"))
        serving = asyncio.create_task(server.serve(sockets=[listener]))
        endpoint = f"http://127.0.0.1:{port}" + (
            mcp.settings.streamable_http_path if transport == "http" else mcp.settings.sse_path
        )

        @asynccontextmanager
        async def connect():
            headers = signed_identity("orchard", "alice")
            if transport == "http":
                async with streamablehttp_client(endpoint, headers=headers) as streams:
                    yield streams
            else:
                async with sse_client(endpoint, headers=headers) as (read, write):
                    yield read, write, lambda: None

        try:
            async with asyncio.timeout(25):
                while not server.started:
                    if serving.done():
                        await serving
                        raise AssertionError("MCP stopped during startup")
                    await asyncio.sleep(0.01)
                async with connect() as (read, write, _):
                    async with ClientSession(read, write) as session:
                        await session.initialize()

                        async def call(name, arguments):
                            result = await session.call_tool(name, arguments)
                            assert not result.isError, result
                            return json.loads(result.content[0].text)

                        await call(
                            "set_working_memory",
                            {
                                "session_id": "shared",
                                "messages": [
                                    {
                                        "role": "user",
                                        "content": "Remember my orchard preference",
                                        "created_at": "2026-09-01T00:00:00+00:00",
                                    }
                                ],
                            },
                        )
                        await run_worker(docket)
                        assert len(model_calls) == 1
                        memory = await call("get_long_term_memory", {"memory_id": "derived"})
                        assert memory["text"] == "recent orchard" and memory["user_id"] == "alice"
                        recall = await call(
                            "search_long_term_memory",
                            {"text": "orchard", "search_mode": "keyword"},
                        )
                        assert [item["id"] for item in recall["memories"]] == ["derived"]
                        denied = await session.call_tool(
                            "search_long_term_memory",
                            {"text": "orchard", "user_id": {"eq": "bob"}},
                        )
                        assert denied.isError
                        await call("delete_long_term_memories", {"memory_ids": ["derived"]})
                        assert not await redis.exists(
                            Keys.memory_key("derived", "orchard", "alice")
                        )
                        recall = await call(
                            "search_long_term_memory",
                            {"text": "orchard", "search_mode": "keyword"},
                        )
                        assert recall["memories"] == []
                        batches = [
                            json.loads(await redis.get(key))
                            async for key in redis.scan_iter(match="extraction_batch:*")
                        ]
                        assert len(batches) == 1 and batches[0]["state"] == "finalized"
                        assert (await redis.xpending(docket.stream_key, docket.worker_group_name))[
                            "pending"
                        ] == 0
        finally:
            server.should_exit = True
            await asyncio.wait_for(serving, 10)
            listener.close()


@pytest.mark.parametrize("candidate_state", ["deleted", "changed", "current"])
async def test_hash_dedup_revalidates_actual_candidate_before_discarding_input(
    redis_client, monkeypatch, candidate_state
):
    monkeypatch.setattr(settings, "redisvl_vector_dimensions", 3)
    monkeypatch.setattr(settings, "embedding_model", "test-fixed-3d")
    db = RedisVLMemoryVectorDatabase(
        AsyncSearchIndex.from_dict(_build_redis_schema(), redis_client=redis_client),
        FixedEmbeddings(),
    )
    existing = MemoryRecord(
        id="existing", text="recent orchard", namespace="orchard", user_id="alice"
    )
    await db.add_memories([existing])
    snapshot = await db.list_memories()
    assert len(snapshot.memories) == 1
    key = Keys.memory_key("existing", "orchard", "alice")
    if candidate_state == "deleted":
        await db.delete_memories(["existing"], "orchard", "alice", exact_scope=True)
    elif candidate_state == "changed":
        await db.add_memories(
            [
                MemoryRecord(
                    id="existing",
                    text="old orchard",
                    namespace="orchard",
                    user_id="alice",
                )
            ]
        )
    before = await redis_client.execute_command("HGETALL", key, NEVER_DECODE=True)

    async def get_db():
        return db

    async def stale_search(**kwargs):
        return snapshot

    monkeypatch.setattr(db, "list_memories", stale_search)
    monkeypatch.setattr(long_term_memory, "get_memory_vector_db", get_db)
    incoming = MemoryRecord(
        id="incoming", text="recent orchard", namespace="orchard", user_id="alice"
    )
    result, duplicate = await long_term_memory.deduplicate_by_hash(
        incoming, redis_client=redis_client
    )
    if candidate_state == "current":
        assert result is None and duplicate
        assert await redis_client.hget(key, "access_count") == "1"
    else:
        assert result is incoming and not duplicate
        assert await redis_client.execute_command("HGETALL", key, NEVER_DECODE=True) == before


@pytest.mark.parametrize(
    "publication_state",
    [
        "success",
        "embedding_failure",
        "candidate_changed",
        "candidate_metadata_changed",
        "candidate_deleted",
        "input_deleted",
    ],
)
async def test_semantic_merge_publishes_replacement_and_deletes_candidates_atomically(
    redis_client, monkeypatch, publication_state
):
    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    monkeypatch.setattr(settings, "redisvl_vector_dimensions", 3)
    monkeypatch.setattr(settings, "embedding_model", "test-fixed-3d")
    db = RedisVLMemoryVectorDatabase(
        AsyncSearchIndex.from_dict(_build_redis_schema(), redis_client=redis_client),
        FixedEmbeddings(),
    )
    existing = MemoryRecord(id="existing", text="old orchard", namespace="orchard", user_id="alice")
    await db.add_memories([existing])
    candidate_key = Keys.memory_key("existing", "orchard", "alice")
    incoming_key = Keys.memory_key("incoming", "orchard", "alice")
    before = await redis_client.execute_command("HGETALL", candidate_key, NEVER_DECODE=True)

    async def get_db():
        return db

    async def merge(records):
        assert (
            await redis_client.execute_command("HGETALL", candidate_key, NEVER_DECODE=True)
            == before
        )
        if publication_state == "candidate_changed":
            await db.add_memories(
                [
                    MemoryRecord(
                        id="existing",
                        text="other ocean",
                        namespace="orchard",
                        user_id="alice",
                    )
                ]
            )
        elif publication_state == "candidate_metadata_changed":
            await redis_client.hset(candidate_key, mapping={"topics": "later topic"})
        elif publication_state == "candidate_deleted":
            await db.delete_memories(["existing"], "orchard", "alice", exact_scope=True)
        elif publication_state == "input_deleted":
            await db.delete_memories(["incoming"], "orchard", "alice", exact_scope=True)
        if publication_state == "success":
            await redis_client.hset(candidate_key, mapping={"access_count": "11"})
        return MemoryRecord(
            id="model-id", text="merged orchard", namespace="orchard", user_id="alice"
        )

    monkeypatch.setattr(long_term_memory, "get_memory_vector_db", get_db)
    monkeypatch.setattr(long_term_memory, "merge_memories_with_llm", merge)
    incoming = MemoryRecord(
        id="incoming", text="recent orchard", namespace="orchard", user_id="alice"
    )
    merged, was_merged = await long_term_memory.deduplicate_by_semantic_search(
        incoming, redis_client=redis_client, vector_distance_threshold=2.0
    )
    assert was_merged and merged.id == "incoming"
    after_model = await redis_client.execute_command("HGETALL", candidate_key, NEVER_DECODE=True)

    async def embed(texts):
        if publication_state == "embedding_failure":
            raise ConnectionError("embedding unavailable")
        return [[1.0, 0.0, 0.0] for _ in texts]

    monkeypatch.setattr(db.embeddings, "aembed_documents", embed)
    if publication_state == "success":
        assert await db.add_memories([merged]) == ["incoming"]
        assert await redis_client.hget(incoming_key, "text") == "merged orchard"
        assert await redis_client.hget(incoming_key, "access_count") == "11"
        assert not await redis_client.exists(candidate_key)
        assert (
            await redis_client.get(Keys.memory_generation_keys("existing", "orchard", "alice")[0])
            == "1"
        )
    else:
        with pytest.raises((ConnectionError, ValueError)):
            await db.add_memories([merged])
        assert not await redis_client.exists(incoming_key)
        assert (
            await redis_client.execute_command("HGETALL", candidate_key, NEVER_DECODE=True)
            == after_model
        )
    assert not [key async for key in redis_client.scan_iter(match="memory_stage:*")]


@pytest.mark.parametrize(
    "state", ["current", "pinned", "changed", "accessed", "deleted", "response_loss"]
)
async def test_background_hash_compaction_preserves_scope_stats_and_deletion_versions(
    redis_client, monkeypatch, state
):
    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    monkeypatch.setattr(settings, "redisvl_vector_dimensions", 3)
    monkeypatch.setattr(settings, "embedding_model", "test-fixed-3d")
    db = RedisVLMemoryVectorDatabase(
        AsyncSearchIndex.from_dict(_build_redis_schema(), redis_client=redis_client),
        FixedEmbeddings(),
    )
    now = datetime(2026, 9, 1, tzinfo=UTC)
    old = MemoryRecord(
        id="old",
        text="recent orchard",
        namespace="orchard",
        user_id="alice",
        created_at=now - timedelta(days=2),
        updated_at=now - timedelta(days=2),
        last_accessed=now - timedelta(days=2),
        topics=["old topic"],
        metadata={"old": 1, "shared": "old"},
        access_count=2,
        pinned=state == "pinned",
        discrete_memory_extracted="t",
    )
    new = MemoryRecord(
        id="new",
        text="recent orchard",
        namespace="orchard",
        user_id="alice",
        created_at=now,
        updated_at=now,
        last_accessed=now,
        topics=["new topic"],
        metadata={"new": 2, "shared": "new"},
        access_count=3,
        discrete_memory_extracted="t",
    )
    await db.add_memories([old, new])
    foreign = MemoryRecord(
        id="old",
        text="recent orchard",
        namespace="orchard",
        user_id="bob",
        memory_hash=old.memory_hash,
        discrete_memory_extracted="t",
    )
    await db.add_memories([foreign])
    old_key = Keys.memory_key("old", "orchard", "alice")
    new_key = Keys.memory_key("new", "orchard", "alice")
    foreign_key = Keys.memory_key("old", "orchard", "bob")
    foreign_before = await redis_client.execute_command("HGETALL", foreign_key, NEVER_DECODE=True)
    original_group = db.compact_hash_group

    async def get_db():
        return db

    async def racing_group(memories):
        if state == "changed":
            await redis_client.hset(old_key, mapping={"topics": "concurrent topic"})
        elif state == "deleted":
            await db.delete_memories(["old"], "orchard", "alice", exact_scope=True)
        elif state == "accessed":
            await redis_client.hset(old_key, mapping={"access_count": "7"})
        return await original_group(memories)

    monkeypatch.setattr(long_term_memory, "get_memory_vector_db", get_db)
    monkeypatch.setattr(db, "compact_hash_group", racing_group)
    original_eval = redis_client.eval

    async def lose_response(script, numkeys, *args):
        result = await original_eval(script, numkeys, *args)
        if "return #snapshots - 1" in script:
            raise ConnectionError("compaction response lost")
        return result

    if state == "response_loss":
        monkeypatch.setattr(redis_client, "eval", lose_response)
        with pytest.raises(ConnectionError, match="compaction response lost"):
            await long_term_memory.compact_long_term_memories(
                namespace="orchard",
                user_id="alice",
                redis_client=redis_client,
                compact_semantic_duplicates=False,
            )
        monkeypatch.setattr(redis_client, "eval", original_eval)
    else:
        await long_term_memory.compact_long_term_memories(
            namespace="orchard",
            user_id="alice",
            redis_client=redis_client,
            compact_semantic_duplicates=False,
        )
    assert (
        await redis_client.execute_command("HGETALL", foreign_key, NEVER_DECODE=True)
        == foreign_before
    )
    if state in {"changed", "accessed"}:
        assert await redis_client.exists(old_key, new_key) == 2
        if state == "changed":
            assert await redis_client.hget(old_key, "topics") == "concurrent topic"
        else:
            assert await redis_client.hget(old_key, "access_count") == "7"
        assert not await redis_client.get(Keys.memory_generation_keys("old", "orchard", "alice")[0])
    elif state == "deleted":
        assert not await redis_client.exists(old_key)
        assert await redis_client.hget(new_key, "access_count") == "3"
    else:
        survivor = old_key if state == "pinned" else new_key
        removed_id = "new" if state == "pinned" else "old"
        assert await redis_client.exists(old_key, new_key) == 1
        assert await redis_client.hget(survivor, "access_count") == "5"
        assert set((await redis_client.hget(survivor, "topics")).split(",")) == {
            "old topic",
            "new topic",
        }
        assert json.loads(await redis_client.hget(survivor, "metadata")) == {
            "old": 1,
            "new": 2,
            "shared": "new",
        }
        assert (
            await redis_client.get(Keys.memory_generation_keys(removed_id, "orchard", "alice")[0])
            == "1"
        )
        before = await redis_client.execute_command("HGETALL", survivor, NEVER_DECODE=True)
        await long_term_memory.compact_long_term_memories(
            namespace="orchard",
            user_id="alice",
            redis_client=redis_client,
            compact_semantic_duplicates=False,
        )
        assert await redis_client.execute_command("HGETALL", survivor, NEVER_DECODE=True) == before
