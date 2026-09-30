"""Verify MCP transport request context isolation without model calls."""

import asyncio
from contextlib import asynccontextmanager

import httpx
import pytest
from fastapi import HTTPException
from starlette.requests import Request

from agent_memory_server import mcp as mcp_module
from agent_memory_server.auth import UserInfo
from agent_memory_server.config import settings

from agent_memory_server.mcp import FastMCP, _FastMCPBase


async def test_concurrent_mcp_namespaces_and_argument_ownership(monkeypatch):
    server = FastMCP("context-test", default_namespace="default")
    arrived = asyncio.Event()
    calls = []

    async def capture(self, name, arguments):
        calls.append(arguments)
        return arguments

    monkeypatch.setattr(_FastMCPBase, "call_tool", capture)
    original = {"text": "hello"}

    async def invoke(namespace, wait):
        request = Request({"type": "http", "path_params": {"namespace": namespace}})
        token = server._request_context.set(request)
        try:
            if wait:
                await arrived.wait()
            else:
                arrived.set()
                await asyncio.sleep(0)
            result = await server.call_tool("search_long_term_memory", original)
            assert result["namespace"].eq == namespace
        finally:
            server._request_context.reset(token)
        assert server._request_context.get() is None

    await asyncio.gather(invoke("orchard", True), invoke("forest", False))
    assert {call["namespace"].eq for call in calls} == {"orchard", "forest"}
    assert original == {"text": "hello"}
    result = await server.call_tool("search_long_term_memory", original)
    assert result["namespace"].eq == "default"


async def test_streamable_transport_resets_context_on_failure(monkeypatch):
    monkeypatch.setattr(settings, "disable_auth", True)
    server = FastMCP("transport-context")

    class Manager:
        async def handle_request(self, scope, receive, send):
            assert server._request_context.get().path_params["namespace"] == "orchard"
            raise RuntimeError("transport failed")

    server._session_manager = Manager()
    app = server.streamable_http_app()
    handler = app.routes[0].endpoint
    try:
        await handler(
            {"type": "http", "headers": [], "path_params": {"namespace": "orchard"}}, None, None
        )
    except RuntimeError as exc:
        assert str(exc) == "transport failed"
    else:
        raise AssertionError("Expected transport failure")
    assert server._request_context.get() is None


@pytest.mark.parametrize("transport", ["sse", "http"])
async def test_mcp_transport_rejects_missing_identity_and_foreign_url(monkeypatch, transport):
    monkeypatch.setattr(settings, "disable_auth", False)
    monkeypatch.setattr(settings, "auth_mode", "oauth2")
    calls = []

    async def verified_user(credentials):
        if credentials is None:
            raise HTTPException(status_code=401, detail="Missing authorization header")
        calls.append(credentials.credentials)
        return UserInfo(sub="alice", namespace="orchard")

    monkeypatch.setattr(mcp_module, "get_current_user", verified_user)
    server = FastMCP("auth-test")

    class Manager:
        async def handle_request(self, scope, receive, send):
            response = mcp_module.JSONResponse(
                {"owner": server._request_context.get().state.mcp_user.sub}
            )
            await response(scope, receive, send)

        def run(self):
            raise AssertionError("Lifespan is unused by this transport test")

    server._session_manager = Manager()
    app = server.sse_app() if transport == "sse" else server.streamable_http_app()
    path = server.settings.sse_path if transport == "sse" else server.settings.streamable_http_path
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(path)
        assert response.status_code == 401
        response = await client.get("/forest" + path, headers={"Authorization": "Bearer verified"})
        assert response.status_code == 403
        if transport == "http":
            response = await client.get(
                "/orchard" + path, headers={"Authorization": "Bearer verified"}
            )
            assert response.status_code == 200
            assert response.json() == {"owner": "alice"}
    assert calls
    assert server._request_context.get() is None


async def test_mcp_tools_inject_identity_and_reject_foreign_scope(monkeypatch):
    monkeypatch.setattr(settings, "disable_auth", False)
    monkeypatch.setattr(settings, "auth_mode", "oauth2")
    request = Request(
        {
            "type": "http",
            "headers": [],
            "state": {"mcp_user": UserInfo(sub="alice", namespace="orchard")},
        }
    )
    token = mcp_module.mcp_app._request_context.set(request)
    calls = []

    async def get_record(**kwargs):
        calls.append(kwargs)
        return "record"

    monkeypatch.setattr(mcp_module, "core_get_long_term_memory", get_record)
    try:
        assert await mcp_module.get_long_term_memory("shared") == "record"
        assert calls[0]["current_user"].namespace == "orchard"
        assert calls[0]["current_user"].sub == "alice"
        with pytest.raises(HTTPException) as error:
            await mcp_module.get_working_memory("session", namespace="forest")
        assert error.value.status_code == 403
        with pytest.raises(HTTPException) as error:
            await mcp_module.memory_prompt(
                "query", namespace=mcp_module.Namespace(any=["orchard", "forest"])
            )
        assert error.value.status_code == 403
    finally:
        mcp_module.mcp_app._request_context.reset(token)
    with pytest.raises(HTTPException) as error:
        await mcp_module.get_long_term_memory("shared")
    assert error.value.status_code == 401


async def test_authenticated_mcp_ignores_development_defaults(monkeypatch):
    monkeypatch.setattr(settings, "disable_auth", False)
    monkeypatch.setattr(settings, "auth_mode", "oauth2")
    monkeypatch.setattr(settings, "default_mcp_namespace", "foreign-default")
    monkeypatch.setattr(settings, "default_mcp_user_id", "other-user")
    request = Request(
        {
            "type": "http",
            "headers": [],
            "state": {"mcp_user": UserInfo(sub="alice", namespace="orchard")},
        }
    )
    token = mcp_module.mcp_app._request_context.set(request)
    calls = []

    async def search(payload, **kwargs):
        namespace, user_id = mcp_module.authorize_filter_scope(
            kwargs["current_user"], payload.namespace, payload.user_id
        )
        calls.append((namespace.eq, user_id.eq))
        return mcp_module.MemoryRecordResults(total=0, memories=[])

    monkeypatch.setattr(mcp_module, "core_search_long_term_memory", search)
    try:
        await mcp_module.search_long_term_memory("orchard")
        assert calls == [("orchard", "alice")]
    finally:
        mcp_module.mcp_app._request_context.reset(token)

    server = FastMCP("default-auth-test", default_namespace="foreign-default")

    async def capture(self, name, arguments):
        return arguments

    monkeypatch.setattr(_FastMCPBase, "call_tool", capture)
    assert await server.call_tool("search_long_term_memory", {"text": "orchard"}) == {
        "text": "orchard"
    }


async def test_sse_session_owner_and_disconnect_cleanup(monkeypatch):
    monkeypatch.setattr(settings, "disable_auth", False)
    monkeypatch.setattr(settings, "auth_mode", "oauth2")
    started = asyncio.Event()
    release = asyncio.Event()
    submitted = []
    session_id = "1234567890abcdef1234567890abcdef"

    async def verified_user(credentials):
        return UserInfo(sub=credentials.credentials, namespace="orchard")

    monkeypatch.setattr(mcp_module, "get_current_user", verified_user)

    class Transport:
        def __init__(self, endpoint):
            pass

        @asynccontextmanager
        async def connect_sse(self, scope, receive, send):
            await send(
                {
                    "type": "http.response.body",
                    "body": f"event: endpoint\r\ndata: /messages/?session_id={session_id}\r\n\r\n".encode(),
                }
            )
            yield None, None

        async def handle_post_message(self, scope, receive, send):
            submitted.append(scope)
            await mcp_module.JSONResponse({"accepted": True}, status_code=202)(scope, receive, send)

    monkeypatch.setattr("mcp.server.sse.SseServerTransport", Transport)
    server = FastMCP("sse-owner")

    async def run(*args):
        started.set()
        await release.wait()

    monkeypatch.setattr(server._mcp_server, "run", run)
    app = server.sse_app()

    async def send(message):
        pass

    request = Request(
        {"type": "http", "headers": [(b"authorization", b"Bearer alice")], "path_params": {}},
        send=send,
    )
    connection = asyncio.create_task(app.routes[0].endpoint(request))
    await asyncio.wait_for(started.wait(), 5)
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app), base_url="http://test"
        ) as client:
            path = server.settings.message_path.rstrip("/") + "/?session_id=" + session_id
            assert (
                await client.post(path, headers={"Authorization": "Bearer bob"})
            ).status_code == 404
            assert not submitted
            assert (
                await client.post(path, headers={"Authorization": "Bearer alice"})
            ).status_code == 202
            assert len(submitted) == 1
            release.set()
            await connection
            assert (
                await client.post(path, headers={"Authorization": "Bearer alice"})
            ).status_code == 404
            assert len(submitted) == 1
    finally:
        release.set()
        await connection


async def test_stateful_http_session_owner_and_termination(monkeypatch):
    monkeypatch.setattr(settings, "disable_auth", False)
    monkeypatch.setattr(settings, "auth_mode", "oauth2")

    async def verified_user(credentials):
        return UserInfo(sub=credentials.credentials, namespace="orchard")

    monkeypatch.setattr(mcp_module, "get_current_user", verified_user)
    server = FastMCP("http-owner")
    calls = []

    class Transport:
        is_terminated = False

    class Manager:
        _server_instances = {}

        async def handle_request(self, scope, receive, send):
            calls.append(scope["method"])
            self._server_instances.setdefault("session-a", Transport())
            if scope["method"] == "DELETE":
                self._server_instances["session-a"].is_terminated = True
            await mcp_module.JSONResponse(
                {"accepted": True}, headers={"Mcp-Session-Id": "session-a"}
            )(scope, receive, send)

        def run(self):
            raise AssertionError("Lifespan unused")

    server._session_manager = Manager()
    app = server.streamable_http_app()
    path = server.settings.streamable_http_path
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        created = await client.post(path, headers={"Authorization": "Bearer alice"})
        assert created.status_code == 200
        owner = {
            "Authorization": "Bearer alice",
            "Mcp-Session-Id": created.headers["mcp-session-id"],
        }
        foreign = {**owner, "Authorization": "Bearer bob"}
        assert (await client.post(path, headers=foreign)).status_code == 404
        assert calls == ["POST"]
        assert (await client.post(path, headers=owner)).status_code == 200
        assert (await client.delete(path, headers=owner)).status_code == 200
        assert (await client.post(path, headers=owner)).status_code == 404
        assert calls == ["POST", "POST", "DELETE"]
    assert server._request_context.get() is None
