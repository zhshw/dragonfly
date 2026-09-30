"""Summary admission failures and explicit cancellation retain recoverable intent."""

import json

import httpx
import pytest
from fastapi import FastAPI
from redis.asyncio import Redis

from agent_memory_server import summary_views, summary_task_dispatch
from agent_memory_server.api import router
from agent_memory_server.bounded_docket import BoundedDocket, QueueCapacityError
from agent_memory_server.config import settings
from agent_memory_server.models import SummaryView, Task, TaskStatusEnum, TaskTypeEnum
from agent_memory_server.tasks import create_task, get_task
from agent_memory_server.utils import redis as redis_utils

from conftest import SCHEDULER_SHA
from test_docket import run_worker
import test_tenant_scope

signed_identity = test_tenant_scope.signed_identity


@pytest.fixture
async def dispatch_storage(dragonfly_factory, monkeypatch):
    url = dragonfly_factory(f"--lua_undeclared_keys_shas={SCHEDULER_SHA}")
    monkeypatch.setattr(settings, "redis_url", url)
    monkeypatch.setattr(settings, "docket_name", "summary-dispatch")
    monkeypatch.setattr(settings, "use_docket", True)
    async with Redis.from_url(url, decode_responses=True) as client:
        monkeypatch.setattr(redis_utils, "_redis_pool", client)
        view = SummaryView(
            id="owned", source="long_term", owner_namespace="orchard", owner_user_id="alice"
        )
        await summary_views.save_summary_view(view)
        task = Task(
            id="owned-task",
            type=TaskTypeEnum.SUMMARY_VIEW_FULL_RUN,
            view_id=view.id,
            view_generation=view.generation,
            owner_namespace="orchard",
            owner_user_id="alice",
        )
        await create_task(task, dispatch_summary=True)
        yield client, task


async def test_summary_outbox_replays_committed_enqueue_after_response_loss(
    dispatch_storage, monkeypatch
):
    client, task = dispatch_storage
    key = f"summary_outbox:{task.docket_key}"
    raw = await client.get(key)
    original_add = BoundedDocket.add

    def lost_response(self, *args, **kwargs):
        operation = original_add(self, *args, **kwargs)

        async def committed(*values, **options):
            await operation(*values, **options)
            raise ConnectionError("enqueue response lost")

        return committed

    monkeypatch.setattr(BoundedDocket, "add", lost_response)
    with pytest.raises(ConnectionError, match="response lost"):
        await summary_task_dispatch.dispatch_summary_intent(client, key, raw)
    assert await client.get(key) == raw
    assert await client.ttl(f"task:{task.id}") == -1
    monkeypatch.setattr(BoundedDocket, "add", original_add)
    assert await summary_task_dispatch.reconcile_summary_task_intents(client) == 0
    assert await client.xlen("summary-dispatch:stream") == 1

    async def empty(*args):
        return []

    monkeypatch.setattr(summary_views, "_fetch_long_term_memories_for_view", empty)
    async with BoundedDocket(name=settings.docket_name, url=settings.redis_url) as docket:
        docket.register(summary_views.refresh_summary_view)
        await run_worker(docket)
    assert (await get_task(task.id)).status == TaskStatusEnum.SUCCESS
    await summary_task_dispatch.reconcile_summary_task_intents(client)
    assert not await client.exists(key)
    assert await client.xlen("summary-dispatch:stream") == 0


async def test_explicit_cancel_fences_task_before_signal_failure_and_recovers(
    dispatch_storage, monkeypatch
):
    client, task = dispatch_storage
    original_cancel = BoundedDocket.cancel

    async def failure(*args):
        raise ConnectionError("cancel unavailable")

    monkeypatch.setattr(BoundedDocket, "cancel", failure)
    with pytest.raises(ConnectionError, match="unavailable"):
        await summary_task_dispatch.cancel_task(task.id, expected_generation=task.generation)
    stored = await get_task(task.id)
    assert stored.status == TaskStatusEnum.CANCELLED
    intent_key = f"task_cancel_outbox:{task.docket_key}"
    assert await client.exists(intent_key)
    assert json.loads(await client.get(intent_key))["generation"] == task.generation
    monkeypatch.setattr(BoundedDocket, "cancel", original_cancel)
    assert await summary_task_dispatch.reconcile_summary_task_intents(client) == 1
    assert not await client.exists(intent_key, f"summary_outbox:{task.docket_key}")
    assert (await get_task(task.id)).status == TaskStatusEnum.CANCELLED
    assert not await summary_task_dispatch.cancel_task(
        task.id, expected_generation="old-generation"
    )


async def test_capacity_is_not_invalid_intent_and_retains_dispatch(dispatch_storage, monkeypatch):
    client, task = dispatch_storage

    async def full(*args):
        raise QueueCapacityError("fixture capacity")

    monkeypatch.setattr(summary_task_dispatch, "dispatch_summary_intent", full)
    assert await summary_task_dispatch.reconcile_summary_task_intents(client) == 0
    assert await client.exists(f"summary_outbox:{task.docket_key}")
    assert not await client.hget("agent_memory_operation_counters", "outbox_invalid")


async def test_cancel_http_owner_and_generation_fencing(dispatch_storage, signed_identity):
    client, task = dispatch_storage
    app = FastAPI()
    app.include_router(router)
    path = f"/v1/tasks/{task.id}/cancel"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as http:
        for owner in [("orchard", "bob"), ("forest", "alice")]:
            response = await http.post(
                path, params={"generation": task.generation}, headers=signed_identity(*owner)
            )
            assert response.status_code == 404
        assert (await get_task(task.id)).status == TaskStatusEnum.PENDING
        stale = await http.post(
            path, params={"generation": "stale"}, headers=signed_identity("orchard", "alice")
        )
        assert stale.status_code == 409
        response = await http.post(
            path,
            params={"generation": task.generation},
            headers=signed_identity("orchard", "alice"),
        )
        assert response.status_code == 200, response.text
        assert response.json()["status"] == "cancelled"
    assert await client.xlen("summary-dispatch:stream") == 0
