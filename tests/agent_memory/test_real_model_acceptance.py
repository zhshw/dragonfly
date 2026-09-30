"""Opt-in real provider acceptance; never substitutes fixed model outputs."""

import asyncio
import json
import os
from datetime import timedelta

import httpx
import pytest
from fastapi import FastAPI
from redis.asyncio import Redis
from redisvl.index import AsyncSearchIndex

from agent_memory_server import long_term_memory, readiness
from agent_memory_server.api import router
from agent_memory_server.bounded_docket import BoundedDocket
from agent_memory_server.config import settings
from agent_memory_server.docket_tasks import task_collection
from agent_memory_server.docket_worker import BoundedDocketWorker
from agent_memory_server.memory_vector_db import RedisVLMemoryVectorDatabase
from agent_memory_server.memory_vector_db_factory import _build_redis_schema, create_embeddings
from agent_memory_server.utils import redis as redis_utils
from agent_memory_server.utils.keys import Keys
from agent_memory_server.working_memory_index import ensure_working_memory_index

from conftest import SCHEDULER_SHA
import test_tenant_scope

signed_identity = test_tenant_scope.signed_identity


@pytest.mark.skipif(
    os.environ.get("AMS_REAL_MODEL_ACCEPTANCE") != "1",
    reason="Real generation/embedding service required; set AMS_REAL_MODEL_ACCEPTANCE=1 explicitly",
)
async def test_real_provider_http_worker_extraction_recall_and_delete(
    dragonfly_factory, monkeypatch, signed_identity
):
    """Use configured providers and synthetic facts through the production queue."""
    url = dragonfly_factory(f"--lua_undeclared_keys_shas={SCHEDULER_SHA}")
    monkeypatch.setattr(settings, "redis_url", url)
    monkeypatch.setattr(settings, "docket_name", "real-model-acceptance")
    monkeypatch.setattr(settings, "use_docket", True)
    monkeypatch.setattr(settings, "long_term_memory", True)
    monkeypatch.setattr(settings, "enable_discrete_memory_extraction", True)
    monkeypatch.setattr(settings, "extraction_debounce_seconds", 1)
    monkeypatch.setattr(settings, "compact_semantic_duplicates", False)
    async with (
        Redis.from_url(url, decode_responses=True) as client,
        BoundedDocket(name=settings.docket_name, url=url) as docket,
    ):
        monkeypatch.setattr(redis_utils, "_redis_pool", client)
        # Only storage routing is injected. Embedding and all generation calls
        # use the user's configured LLMClient and actual network providers.
        db = RedisVLMemoryVectorDatabase(
            AsyncSearchIndex.from_dict(_build_redis_schema(), redis_client=client),
            create_embeddings(),
        )

        async def provider():
            return db

        monkeypatch.setattr(long_term_memory, "get_memory_vector_db", provider)
        monkeypatch.setattr(readiness, "get_memory_vector_db", provider)
        await readiness.ensure_long_term_memory_ready()
        await ensure_working_memory_index(client)
        for function in task_collection:
            docket.register(function)
        app = FastAPI()
        app.include_router(router)
        headers = signed_identity("synthetic-acceptance", "alice")
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app), base_url="http://real-model"
        ) as http:
            response = await http.put(
                "/v1/working-memory/preferences",
                headers=headers,
                json={
                    "version": 0,
                    "messages": [
                        {
                            "id": "explicit-fact",
                            "role": "user",
                            "content": "My favorite drink is green tea. I drink green tea every morning.",
                        },
                    ],
                },
            )
            assert response.status_code == 200, response.text
            async with BoundedDocketWorker(
                docket,
                schedule_automatic_tasks=False,
                scheduling_resolution=timedelta(milliseconds=20),
                minimum_check_interval=timedelta(milliseconds=20),
            ) as worker:
                await asyncio.wait_for(worker.run_until_finished(), timeout=300)
            working = await client.json().get(
                Keys.working_memory_key("preferences", "alice", "synthetic-acceptance")
            )
            assert all(
                message["discrete_memory_extracted"] == "t" for message in working["messages"]
            )
            receipts = [key async for key in client.scan_iter(match="extraction_batch:*")]
            assert receipts, "Actual extraction must produce a durable batch"
            for key in receipts:
                assert json.loads(await client.get(key))["state"] == "finalized"
            recalled = await http.post(
                "/v1/long-term-memory/search",
                headers=headers,
                json={"text": "What drink do I prefer every morning?", "search_mode": "semantic"},
            )
            assert recalled.status_code == 200, recalled.text
            records = recalled.json()["memories"]
            assert records and any("green tea" in record["text"].lower() for record in records)
            assert all(
                record["namespace"] == "synthetic-acceptance" and record["user_id"] == "alice"
                for record in records
            )
            for record in records:
                assert await client.exists(
                    Keys.memory_key(record["id"], "synthetic-acceptance", "alice")
                )
            foreign = await http.get(
                f"/v1/long-term-memory/{records[0]['id']}",
                headers=signed_identity("synthetic-acceptance", "bob"),
            )
            assert foreign.status_code == 404
            async with BoundedDocketWorker(
                docket,
                schedule_automatic_tasks=False,
                scheduling_resolution=timedelta(milliseconds=20),
                minimum_check_interval=timedelta(milliseconds=20),
            ) as worker:
                await asyncio.wait_for(worker.run_until_finished(), timeout=60)
            assert await client.xlen(docket.stream_key) == 0
            assert (await client.xpending(docket.stream_key, docket.worker_group_name))[
                "pending"
            ] == 0
            deletion = await http.delete(
                "/v1/long-term-memory",
                headers=headers,
                params=[("memory_ids", record["id"]) for record in records],
            )
            assert deletion.status_code == 200, deletion.text
            for record in records:
                assert not await client.exists(
                    Keys.memory_key(record["id"], "synthetic-acceptance", "alice")
                )
        assert await client.xlen(docket.stream_key) == 0
