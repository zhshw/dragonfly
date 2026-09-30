"""Authenticated MCP SDK acceptance on every transport and real worker/storage."""

import asyncio
import json
import os
import socket
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import uvicorn
from docket import Docket
from fastapi import HTTPException
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.client.sse import sse_client
from mcp.client.streamable_http import streamablehttp_client
from pydantic import SecretStr
from redis.asyncio import Redis
from redisvl.index import AsyncSearchIndex

from agent_memory_server import auth, long_term_memory, readiness
from agent_memory_server import mcp as mcp_module
from agent_memory_server.auth import UserInfo
from agent_memory_server.config import settings
from agent_memory_server.mcp import FastMCP, _FastMCPBase
from agent_memory_server.memory_vector_db import RedisVLMemoryVectorDatabase
from agent_memory_server.memory_vector_db_factory import _build_redis_schema
from agent_memory_server.models import MemoryRecord
from agent_memory_server.utils import redis as redis_utils
from agent_memory_server.utils.keys import Keys
from agent_memory_server.working_memory_index import ensure_working_memory_index

from conftest import SCHEDULER_SHA
from test_docket import run_worker
from test_search import FixedEmbeddings
import test_tenant_scope


signed_identity = test_tenant_scope.signed_identity


@pytest.mark.parametrize("failure", ["missing", "invalid", "unbound"])
async def test_stdio_owner_startup_fails_before_storage(monkeypatch, failure):
    monkeypatch.setattr(settings, "disable_auth", False)
    monkeypatch.setattr(settings, "auth_mode", "oauth2")
    monkeypatch.setattr(
        settings,
        "mcp_stdio_bearer_token",
        None if failure == "missing" else SecretStr("test"),
    )
    calls = []

    async def verify(credentials):
        calls.append("verify")
        if failure == "invalid":
            raise HTTPException(status_code=401, detail="Invalid test credential")
        return UserInfo(sub="alice")

    async def forbidden():
        raise AssertionError("An unauthenticated STDIO server must not touch storage")

    monkeypatch.setattr(mcp_module, "get_current_user", verify)
    monkeypatch.setattr(mcp_module, "ensure_long_term_memory_ready", forbidden)
    with pytest.raises(HTTPException) as rejected:
        await FastMCP("stdio-auth").run_stdio_async()
    assert rejected.value.status_code == (403 if failure == "unbound" else 401)
    assert calls == ([] if failure == "missing" else ["verify"])


async def test_stdio_owner_reverified_per_call_and_context_reset(monkeypatch):
    monkeypatch.setattr(settings, "disable_auth", False)
    monkeypatch.setattr(settings, "auth_mode", "oauth2")
    monkeypatch.setattr(settings, "mcp_stdio_bearer_token", SecretStr("test"))
    server = FastMCP("stdio-auth")
    calls = []

    async def verify(credentials):
        assert credentials.credentials == "test"
        calls.append("verify")
        if len(calls) == 3:
            raise HTTPException(status_code=401, detail="Credential revoked")
        return UserInfo(sub="alice", namespace="orchard")

    async def ready():
        pass

    async def storage():
        return None

    async def tool(self, name, arguments):
        assert self._stdio_user.get() == UserInfo(sub="alice", namespace="orchard")
        raise RuntimeError("tool failure")

    async def run(self):
        with pytest.raises(RuntimeError, match="tool failure"):
            await self.call_tool("test", {})
        assert self._stdio_user.get() is None
        with pytest.raises(HTTPException) as rejected:
            await self.call_tool("test", {})
        assert rejected.value.status_code == 401
        assert self._stdio_user.get() is None

    monkeypatch.setattr(mcp_module, "get_current_user", verify)
    monkeypatch.setattr(mcp_module, "ensure_long_term_memory_ready", ready)
    monkeypatch.setattr(redis_utils, "get_redis_conn", storage)
    monkeypatch.setattr(_FastMCPBase, "call_tool", tool)
    monkeypatch.setattr(_FastMCPBase, "run_stdio_async", run)
    await server.run_stdio_async()
    assert calls == ["verify"] * 3 and server._stdio_credentials.get() is None


@pytest.mark.parametrize("transport", ["stdio", "http", "sse"])
async def test_authenticated_mcp_all_tools_worker_reconnect_and_owner_isolation(
    dragonfly_factory, monkeypatch, signed_identity, tmp_path, caplog, transport
):
    url = dragonfly_factory(f"--lua_undeclared_keys_shas={SCHEDULER_SHA}")
    monkeypatch.setattr(settings, "redis_url", url)
    monkeypatch.setattr(settings, "docket_name", "stdio-business")
    monkeypatch.setattr(settings, "use_docket", True)
    monkeypatch.setattr(settings, "long_term_memory", True)
    monkeypatch.setattr(settings, "extraction_debounce_seconds", 1)
    monkeypatch.setattr(settings, "enable_working_memory_summarization", False)
    monkeypatch.setattr(settings, "redisvl_vector_dimensions", 3)
    monkeypatch.setattr(settings, "embedding_model", "test-fixed-3d")
    monkeypatch.setattr(settings, "compact_semantic_duplicates", False)
    monkeypatch.setattr(settings, "enable_discrete_memory_extraction", True)
    monkeypatch.setattr(long_term_memory, "get_background_tasks", lambda: MagicMock())
    public_key = tmp_path / "public.pem"
    public_key.write_text(auth.get_public_key("fixture"))
    bootstrap = tmp_path / "stdio_server.py"
    bootstrap.write_text(
        """
import asyncio
import os
from pathlib import Path
from agent_memory_server.logging import configure_mcp_logging
configure_mcp_logging()
from redisvl.index import AsyncSearchIndex
from agent_memory_server import auth, long_term_memory, readiness
from agent_memory_server import mcp as mcp_module
from agent_memory_server.memory_vector_db import RedisVLMemoryVectorDatabase
from agent_memory_server.memory_vector_db_factory import _build_redis_schema
from agent_memory_server.utils.redis import get_redis_conn
from agent_memory_server.working_memory_index import ensure_working_memory_index
from test_search import FixedEmbeddings

auth.get_public_key = lambda token: Path(os.environ["TEST_PUBLIC_KEY"]).read_text()

async def main():
    redis = await get_redis_conn()
    await ensure_working_memory_index(redis)
    db = RedisVLMemoryVectorDatabase(
        AsyncSearchIndex.from_dict(_build_redis_schema(), redis_client=redis), FixedEmbeddings()
    )
    async def get_db():
        return db
    long_term_memory.get_memory_vector_db = get_db
    readiness.get_memory_vector_db = get_db
    try:
        await mcp_module.mcp_app.run_stdio_async()
    finally:
        await redis.aclose()

asyncio.run(main())
"""
    )
    test_directory = Path(__file__).resolve().parent
    ams_root = Path(os.environ["AMS_ROOT"])
    env = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join([str(ams_root), str(test_directory)]),
        "REDIS_URL": url,
        "REDISVL_VECTOR_DIMENSIONS": "3",
        "EMBEDDING_MODEL": "test-fixed-3d",
        "LONG_TERM_MEMORY": "true",
        "DISABLE_AUTH": "false",
        "AUTH_MODE": "oauth2",
        "OAUTH2_ISSUER_URL": "https://identity.test",
        "OAUTH2_AUDIENCE": "memory-test",
        "TOKEN_AUTH_ENABLED": "false",
        "ENABLE_WORKING_MEMORY_SUMMARIZATION": "false",
        "ENABLE_DISCRETE_MEMORY_EXTRACTION": "true",
        "COMPACT_SEMANTIC_DUPLICATES": "false",
        "EXTRACTION_DEBOUNCE_SECONDS": "1",
        "USE_DOCKET": "true",
        "DOCKET_NAME": "stdio-business",
        "TEST_PUBLIC_KEY": str(public_key),
        "LOG_LEVEL": "ERROR",
        "DEFAULT_MCP_NAMESPACE": "foreign-default",
        "DEFAULT_MCP_USER_ID": "foreign-default",
    }

    endpoint = None

    @asynccontextmanager
    async def streams(owner):
        if transport == "http":
            async with streamablehttp_client(
                endpoint, headers=signed_identity("orchard", owner)
            ) as (read, write, _):
                yield read, write
            return
        if transport == "sse":
            async with sse_client(endpoint, headers=signed_identity("orchard", owner)) as (
                read,
                write,
            ):
                yield read, write
            return
        token = signed_identity("orchard", owner)["Authorization"].split(" ", 1)[1]
        parameters = StdioServerParameters(
            command=sys.executable,
            args=[str(bootstrap)],
            env={**env, "MCP_STDIO_BEARER_TOKEN": token},
        )
        async with stdio_client(parameters) as (read, write):
            yield read, write

    @asynccontextmanager
    async def connect(owner):
        async with (
            streams(owner) as (read, write),
            ClientSession(read, write) as session,
        ):
            await session.initialize()
            yield session

    @asynccontextmanager
    async def live_server():
        nonlocal endpoint
        if transport == "stdio":
            yield
            return
        mcp = mcp_module.mcp_app
        monkeypatch.setattr(mcp, "_session_manager", None)
        monkeypatch.setattr(mcp.settings, "stateless_http", False)
        app = mcp.streamable_http_app() if transport == "http" else mcp.sse_app()
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        endpoint = f"http://127.0.0.1:{listener.getsockname()[1]}" + (
            mcp.settings.streamable_http_path if transport == "http" else mcp.settings.sse_path
        )
        server = uvicorn.Server(uvicorn.Config(app, log_level="error", lifespan="on"))
        serving = asyncio.create_task(server.serve(sockets=[listener]))
        try:
            while not server.started:
                if serving.done():
                    await serving
                    raise AssertionError("MCP stopped during startup")
                await asyncio.sleep(0.01)
            yield
        finally:
            server.should_exit = True
            await asyncio.wait_for(serving, 10)
            listener.close()

    async def call(session, name, arguments):
        result = await session.call_tool(name, arguments)
        assert not result.isError, result
        return json.loads(result.content[0].text)

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
        Docket(name="stdio-business", url=url) as docket,
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
        monkeypatch.setattr(readiness, "get_memory_vector_db", get_db)
        await db.add_memories(
            [MemoryRecord(id="derived", text="old orchard", namespace="orchard", user_id="bob")]
        )
        bob_key = Keys.memory_key("derived", "orchard", "bob")
        bob_before = await redis.execute_command("HGETALL", bob_key, NEVER_DECODE=True)
        docket.register(long_term_memory.promote_working_memory_to_long_term)
        docket.register(long_term_memory.run_delayed_extraction)
        docket.register(long_term_memory.index_long_term_memories)

        async with asyncio.timeout(40), live_server():
            async with connect("alice") as alice, connect("bob") as bob:
                tools = await alice.list_tools()
                assert {tool.name for tool in tools.tools} == {
                    "get_current_datetime",
                    "create_long_term_memories",
                    "search_long_term_memory",
                    "memory_prompt",
                    "set_working_memory",
                    "get_working_memory",
                    "get_long_term_memory",
                    "edit_long_term_memory",
                    "delete_long_term_memories",
                    "compact_long_term_memories",
                }
                clock = await call(alice, "get_current_datetime", {})
                assert clock
                working = await call(
                    alice,
                    "set_working_memory",
                    {
                        "session_id": "shared",
                        "messages": [
                            {
                                "role": "user",
                                "content": "Remember my orchard preference",
                            }
                        ],
                    },
                )
                assert working["namespace"] == "orchard" and working["user_id"] == "alice"
                stored = await call(alice, "get_working_memory", {"session_id": "shared"})
                assert stored["messages"][0]["content"] == "Remember my orchard preference"
                bob_empty = await call(bob, "get_working_memory", {"session_id": "shared"})
                assert bob_empty["messages"] == [] and bob_empty["memories"] == []
                assert not await redis.exists(Keys.working_memory_key("shared", "bob", "orchard"))
                await run_worker(docket)
                assert len(model_calls) == 1 and model_calls[0]["user_id"] == "alice"
                first, second = await asyncio.gather(
                    call(alice, "get_long_term_memory", {"memory_id": "derived"}),
                    call(bob, "get_long_term_memory", {"memory_id": "derived"}),
                )
                assert first["text"] == "recent orchard" and second["text"] == "old orchard"
                await call(
                    alice,
                    "create_long_term_memories",
                    {"memories": [{"id": "direct", "text": "old orchard"}]},
                )
                await run_worker(docket)
                assert (await call(alice, "get_long_term_memory", {"memory_id": "direct"}))[
                    "user_id"
                ] == "alice"
                edited = await call(
                    alice,
                    "edit_long_term_memory",
                    {"memory_id": "direct", "topics": ["verified"]},
                )
                assert edited["topics"] == ["verified"]
                for mode in ["semantic", "keyword", "hybrid"]:
                    recall = await call(
                        alice,
                        "search_long_term_memory",
                        {
                            "text": "orchard",
                            "search_mode": mode,
                            "recency_boost": False,
                        },
                    )
                    assert {memory["id"] for memory in recall["memories"]} == {
                        "derived",
                        "direct",
                    }
                    assert all(memory["user_id"] == "alice" for memory in recall["memories"])
                prompt = await call(
                    alice,
                    "memory_prompt",
                    {
                        "query": "orchard",
                        "search_mode": "keyword",
                        "recency_boost": False,
                    },
                )
                assert "recent orchard" in json.dumps(prompt)
                await call(alice, "compact_long_term_memories", {})
                for name, arguments in [
                    (
                        "create_long_term_memories",
                        {
                            "memories": [
                                {
                                    "id": "forged",
                                    "text": "old orchard",
                                    "user_id": "bob",
                                }
                            ]
                        },
                    ),
                    (
                        "search_long_term_memory",
                        {"text": "orchard", "user_id": {"eq": "bob"}},
                    ),
                    (
                        "memory_prompt",
                        {"query": "orchard", "namespace": {"eq": "forest"}},
                    ),
                    ("set_working_memory", {"session_id": "shared", "user_id": "bob"}),
                    ("get_working_memory", {"session_id": "shared", "user_id": "bob"}),
                    (
                        "edit_long_term_memory",
                        {"memory_id": "derived", "user_id": "bob"},
                    ),
                    ("compact_long_term_memories", {"user_id": "bob"}),
                ]:
                    denied = await alice.call_tool(name, arguments)
                    assert denied.isError, name
                assert (
                    await redis.execute_command("HGETALL", bob_key, NEVER_DECODE=True) == bob_before
                )
            # Reconnect through a fresh transport (new process for STDIO).
            async with connect("alice") as alice:
                record = await call(alice, "get_long_term_memory", {"memory_id": "derived"})
                assert record["text"] == "recent orchard" and record["user_id"] == "alice"
                await call(
                    alice,
                    "delete_long_term_memories",
                    {"memory_ids": ["derived", "direct"]},
                )
                assert not await redis.exists(Keys.memory_key("derived", "orchard", "alice"))
                recall = await call(
                    alice,
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
            assert await redis.execute_command("HGETALL", bob_key, NEVER_DECODE=True) == bob_before
            assert not [
                record
                for record in caplog.records
                if "Failed to parse JSONRPC" in record.getMessage()
            ]
