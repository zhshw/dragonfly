"""Legacy owner migration preserves old bytes and recomputes versioned scoped work."""

import json
from unittest.mock import MagicMock

import pytest
from docket import Docket
from redis.asyncio import Redis
from redisvl.index import AsyncSearchIndex

from agent_memory_server import long_term_memory, summary_views, tenant_migration
from agent_memory_server.config import settings
from agent_memory_server.memory_vector_db import RedisVLMemoryVectorDatabase
from agent_memory_server.memory_vector_db_factory import _build_redis_schema
from agent_memory_server.models import (
    MemoryRecord,
    SummaryView,
    Task,
    TaskStatusEnum,
    TaskTypeEnum,
)
from agent_memory_server.tasks import scoped_task_id
from agent_memory_server.tenant_migration import (
    migrate_legacy_summary_view,
    source_digest,
)
from agent_memory_server.utils import redis as redis_utils

from conftest import SCHEDULER_SHA
from test_docket import run_worker
from test_search import FixedEmbeddings


async def _seed_legacy(redis, owner="alice"):
    view = SummaryView(
        id="historical",
        source="long_term",
        continuous=True,
        filters={"namespace": "orchard", "user_id": owner},
        group_by=["user_id"],
    )
    task = Task(id="historical-run", view_id=view.id, type=TaskTypeEnum.SUMMARY_VIEW_FULL_RUN)
    view_raw, task_raw = view.model_dump_json(), task.model_dump_json()
    await redis.set(summary_views._config_key(view.id), view_raw, ex=600)
    await redis.sadd(summary_views._SUMMARY_VIEW_INDEX_KEY, view.id)
    await redis.set(f"task:{task.id}", task_raw, ex=600)
    result_key = summary_views._summary_key(view.id, "user_id=bob")
    result_raw = '{"view_id":"historical","group":{"user_id":"bob"},"summary":"unsafe old output"}'
    await redis.set(result_key, result_raw, ex=600)
    request = dict(
        namespace="orchard",
        user_id=owner,
        expected_view_sha256=source_digest(view_raw),
        legacy_task_id=task.id,
        expected_task_sha256=source_digest(task_raw),
    )
    return view_raw, task_raw, result_key, result_raw, request


async def test_production_legacy_summary_and_task_fail_closed_on_real_storage(
    redis_client, monkeypatch
):
    monkeypatch.setattr(settings, "disable_auth", False)
    monkeypatch.setattr(settings, "auth_mode", "oauth2")
    monkeypatch.setattr(settings, "long_term_memory", True)
    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    view_raw, task_raw, result_key, result_raw, _ = await _seed_legacy(redis_client)

    async def forbidden(*args, **kwargs):
        raise AssertionError("Legacy unowned/unversioned work cannot read tenant memories")

    monkeypatch.setattr(long_term_memory, "search_long_term_memories", forbidden)
    await summary_views.periodic_refresh_summary_views()
    with pytest.raises(ValueError, match="rescheduling"):
        await summary_views.refresh_summary_view("historical", task_id="historical-run")
    with pytest.raises(ValueError, match="owner migration"):
        await summary_views.refresh_summary_view("historical", expected_generation="old-job")
    view = SummaryView.model_validate_json(view_raw)
    with pytest.raises(ValueError, match="owner migration"):
        summary_views._build_long_term_filters_for_view(view)
    assert await redis_client.get(summary_views._config_key("historical")) == view_raw
    assert await redis_client.get("task:historical-run") == task_raw
    assert await redis_client.get(result_key) == result_raw


async def test_production_summary_task_owner_must_match_versioned_view(redis_client, monkeypatch):
    monkeypatch.setattr(settings, "disable_auth", False)
    monkeypatch.setattr(settings, "auth_mode", "oauth2")
    monkeypatch.setattr(redis_utils, "_redis_pool", redis_client)
    view = SummaryView(
        id="owned", source="long_term", owner_namespace="orchard", owner_user_id="alice"
    )
    await summary_views.save_summary_view(view)
    task = Task(
        id="different-owner",
        generation="task-generation",
        owner_namespace="orchard",
        owner_user_id="bob",
        view_id=view.id,
        view_generation=view.generation,
        type=TaskTypeEnum.SUMMARY_VIEW_FULL_RUN,
    )
    raw = task.model_dump_json()
    await redis_client.set(f"task:{task.id}", raw)
    with pytest.raises(ValueError, match="ownership"):
        await summary_views.refresh_summary_view(
            view.id,
            task_id=task.id,
            expected_generation=view.generation,
            task_generation=task.generation,
        )
    assert await redis_client.get(f"task:{task.id}") == raw
    assert await summary_views.list_partition_results(view.id) == []


@pytest.mark.parametrize("retirement", ["view_deleted", "task_replaced"])
async def test_legacy_migration_pending_retirement_is_terminal_without_resurrection(
    redis_client, monkeypatch, retirement
):
    _, _, _, _, request = await _seed_legacy(redis_client)

    async def defer(*args, **kwargs):
        return False

    original_dispatch = tenant_migration.dispatch_legacy_summary_migration
    monkeypatch.setattr(tenant_migration, "dispatch_legacy_summary_migration", defer)
    receipt = await migrate_legacy_summary_view("historical", redis=redis_client, **request)
    view = SummaryView.model_validate_json(receipt["target_view_raw"])
    task = Task.model_validate_json(receipt["target_task_raw"])
    if retirement == "view_deleted":
        await redis_client.delete(summary_views._config_key(view.id))
    else:
        task.generation = "replacement"
        await redis_client.set(f"task:{task.id}", task.model_dump_json())
    monkeypatch.setattr(tenant_migration, "dispatch_legacy_summary_migration", original_dispatch)
    assert await tenant_migration.reconcile_legacy_summary_migrations(redis_client) == 0
    final = await migrate_legacy_summary_view("historical", redis=redis_client, **request)
    assert final["state"] == "superseded" and final["reason"]
    assert not await redis_client.exists(
        summary_views._config_key("historical"), "task:historical-run"
    )
    assert (await redis_client.get(summary_views._config_key(view.id)) is None) == (
        retirement == "view_deleted"
    )
    if retirement == "task_replaced":
        assert (
            Task.model_validate_json(await redis_client.get(f"task:{task.id}")).generation
            == "replacement"
        )


@pytest.mark.parametrize(
    "invalid",
    [
        "unknown_owner",
        "broad_owner",
        "different_owner",
        "task_owner",
        "task_source",
        "fingerprint",
    ],
)
async def test_legacy_owner_migration_rejects_unproven_identity(redis_client, monkeypatch, invalid):
    view_raw, task_raw, result_key, result_raw, request = await _seed_legacy(redis_client)
    document = json.loads(view_raw)
    task = json.loads(task_raw)
    if invalid == "unknown_owner":
        document["filters"] = {}
    elif invalid == "broad_owner":
        document["filters"]["user_id"] = {"any": ["alice", "bob"]}
    elif invalid == "different_owner":
        document["owner_namespace"], document["owner_user_id"] = "orchard", "bob"
    elif invalid == "task_owner":
        task["owner_namespace"], task["owner_user_id"] = "orchard", "bob"
    elif invalid == "task_source":
        task["view_id"] = "other"
    view_raw, task_raw = json.dumps(document), json.dumps(task)
    await redis_client.set(summary_views._config_key("historical"), view_raw)
    await redis_client.set("task:historical-run", task_raw)
    request["expected_view_sha256"] = source_digest(view_raw)
    request["expected_task_sha256"] = source_digest(task_raw)
    if invalid == "fingerprint":
        request["expected_view_sha256"] = "0" * 64
    with pytest.raises(ValueError):
        await migrate_legacy_summary_view("historical", redis=redis_client, **request)
    assert await redis_client.get(summary_views._config_key("historical")) == view_raw
    assert await redis_client.get("task:historical-run") == task_raw
    assert await redis_client.get(result_key) == result_raw
    assert not [key async for key in redis_client.scan_iter(match="tenant_migration:*")]


@pytest.mark.parametrize("failure", ["source_changed", "source_expired", "target_conflict"])
async def test_legacy_migration_commit_guards_keep_sources_and_archive(
    redis_client, monkeypatch, failure
):
    view_raw, task_raw, result_key, result_raw, request = await _seed_legacy(redis_client)
    original_eval = redis_client.eval
    target_id = "migrated-" + scoped_task_id("historical", "orchard", "alice")

    async def interfere(script, numkeys, *args):
        if "receipt.state = 'pending'" in script:
            if failure == "source_changed":
                await redis_client.set(summary_views._config_key("historical"), "changed")
            elif failure == "source_expired":
                await redis_client.delete(summary_views._config_key("historical"))
            else:
                await redis_client.set(summary_views._config_key(target_id), "other owner's target")
        return await original_eval(script, numkeys, *args)

    monkeypatch.setattr(redis_client, "eval", interfere)
    with pytest.raises(ValueError):
        await migrate_legacy_summary_view("historical", redis=redis_client, **request)
    receipts = [
        json.loads(await redis_client.get(key))
        async for key in redis_client.scan_iter(match="tenant_migration:summary:*")
    ]
    assert len(receipts) == 1 and receipts[0]["state"] == "prepared"
    assert receipts[0]["source_view_raw"] == view_raw and receipts[0]["source_task_raw"] == task_raw
    assert await redis_client.get("task:historical-run") == task_raw
    assert await redis_client.get(result_key) == result_raw
    assert await redis_client.get(summary_views._config_key(target_id)) == (
        "other owner's target" if failure == "target_conflict" else None
    )


@pytest.mark.parametrize("failure", [None, "dispatch", "commit_response_lost"])
async def test_legacy_summary_migration_real_worker_replay_archive_and_scoped_results(
    dragonfly_factory, monkeypatch, failure
):
    url = dragonfly_factory(f"--lua_undeclared_keys_shas={SCHEDULER_SHA}")
    monkeypatch.setattr(settings, "redis_url", url)
    monkeypatch.setattr(settings, "docket_name", "legacy-summary")
    monkeypatch.setattr(settings, "disable_auth", False)
    monkeypatch.setattr(settings, "auth_mode", "oauth2")
    monkeypatch.setattr(settings, "redisvl_vector_dimensions", 3)
    monkeypatch.setattr(settings, "embedding_model", "test-fixed-3d")
    monkeypatch.setattr(settings, "openai_api_key", None)
    monkeypatch.setattr(settings, "anthropic_api_key", None)
    monkeypatch.setattr(settings, "aws_access_key_id", None)
    monkeypatch.setattr(long_term_memory, "get_background_tasks", lambda: MagicMock())
    async with (
        Redis.from_url(url, decode_responses=True) as redis,
        Docket(name="legacy-summary", url=url) as docket,
    ):
        monkeypatch.setattr(redis_utils, "_redis_pool", redis)
        db = RedisVLMemoryVectorDatabase(
            AsyncSearchIndex.from_dict(_build_redis_schema(), redis_client=redis),
            FixedEmbeddings(),
        )

        async def get_db():
            return db

        monkeypatch.setattr(long_term_memory, "get_memory_vector_db", get_db)
        await db.add_memories(
            [
                MemoryRecord(
                    id="same",
                    text="recent orchard",
                    namespace="orchard",
                    user_id="alice",
                ),
                MemoryRecord(id="same", text="old orchard", namespace="orchard", user_id="bob"),
            ]
        )
        view_raw, task_raw, result_key, result_raw, request = await _seed_legacy(redis)
        original_dispatch = tenant_migration.dispatch_legacy_summary_migration
        original_eval = redis.eval

        async def lost_dispatch(*args, **kwargs):
            raise ConnectionError("dispatch unavailable")

        async def lost_commit(script, numkeys, *args):
            result = await original_eval(script, numkeys, *args)
            if "receipt.state = 'pending'" in script:
                raise ConnectionError("commit response lost")
            return result

        if failure == "dispatch":
            monkeypatch.setattr(
                tenant_migration, "dispatch_legacy_summary_migration", lost_dispatch
            )
        if failure == "commit_response_lost":
            monkeypatch.setattr(redis, "eval", lost_commit)
        if failure:
            with pytest.raises(ConnectionError):
                await migrate_legacy_summary_view("historical", redis=redis, **request)
            monkeypatch.setattr(
                tenant_migration, "dispatch_legacy_summary_migration", original_dispatch
            )
            monkeypatch.setattr(redis, "eval", original_eval)
        receipt = await migrate_legacy_summary_view("historical", redis=redis, **request)
        assert receipt["state"] == "pending"
        assert receipt["source_view_raw"] == view_raw and receipt["source_task_raw"] == task_raw
        assert receipt["source_results"] == [
            {
                "key": result_key,
                "raw": result_raw,
                "ttl_ms": receipt["source_results"][0]["ttl_ms"],
            }
        ]
        assert receipt["source_view_ttl_ms"] > 0 and receipt["source_task_ttl_ms"] > 0
        assert not await redis.exists(
            summary_views._config_key("historical"), "task:historical-run", result_key
        )
        new_view = SummaryView.model_validate_json(receipt["target_view_raw"])
        new_task = Task.model_validate_json(receipt["target_task_raw"])
        assert new_view.owner_namespace == "orchard" and new_view.owner_user_id == "alice"
        assert (
            new_view.generation
            and new_task.generation
            and new_task.view_generation == new_view.generation
        )
        assert await redis.ttl(f"task:{new_task.id}") == -1
        assert await summary_views.list_partition_results(new_view.id) == []
        assert await tenant_migration.reconcile_legacy_summary_migrations(redis) == 0
        docket.register(summary_views.refresh_summary_view)
        await run_worker(docket)
        result = await summary_views.list_partition_results(new_view.id)
        assert len(result) == 1 and result[0].group == {"user_id": "alice"}
        assert result[0].memory_count == 1 and "recent orchard" in result[0].summary
        assert (
            "old orchard" not in result[0].summary and "unsafe old output" not in result[0].summary
        )
        current_task = Task.model_validate_json(await redis.get(f"task:{new_task.id}"))
        assert (
            current_task.status == TaskStatusEnum.SUCCESS and current_task.owner_user_id == "alice"
        )
        assert await tenant_migration.reconcile_legacy_summary_migrations(redis) == 0
        final = await migrate_legacy_summary_view("historical", redis=redis, **request)
        assert (
            final["state"] == "finalized" and final["target_view_raw"] == receipt["target_view_raw"]
        )
        await summary_views.delete_summary_view(new_view.id)
        assert await summary_views.list_partition_results(new_view.id) == []
        again = await migrate_legacy_summary_view("historical", redis=redis, **request)
        assert again["state"] == "finalized" and not await redis.exists(
            summary_views._config_key(new_view.id)
        )
        assert (await redis.xpending(docket.stream_key, docket.worker_group_name))["pending"] == 0


@pytest.mark.parametrize(
    "owner",
    [{}, {"namespace": "orchard"}, {"namespace": "orchard", "user_id": "alice"}],
)
async def test_retired_list_hash_summary_task_refuses_production_before_storage(
    dragonfly_factory, monkeypatch, owner
):
    from unittest.mock import AsyncMock
    from agent_memory_server import summarization
    from agent_memory_server.bounded_docket import BoundedDocket
    from agent_memory_server.docket_tasks import task_collection
    from agent_memory_server.utils.keys import Keys

    url = dragonfly_factory(f"--lua_undeclared_keys_shas={SCHEDULER_SHA}")
    monkeypatch.setattr(settings, "disable_auth", False)
    monkeypatch.setattr(settings, "auth_mode", "oauth2")
    assert summarization.summarize_session in task_collection
    storage = AsyncMock()
    model = AsyncMock()
    monkeypatch.setattr(summarization, "get_redis_conn", storage)
    monkeypatch.setattr(summarization, "_incremental_summary", model)
    async with (
        Redis.from_url(url, decode_responses=True) as redis,
        BoundedDocket(name="retired-summary", url=url) as docket,
    ):
        messages_key = Keys.messages_key("historical-list")
        metadata_key = Keys.metadata_key("historical-list")
        await redis.rpush(messages_key, '{"role":"user","content":"unowned private fact"}')
        await redis.hset(metadata_key, mapping={"context": "old private context", "tokens": "10"})
        before = (
            await redis.lrange(messages_key, 0, -1),
            await redis.hgetall(metadata_key),
        )
        with pytest.raises(ValueError, match="explicit migration"):
            await summarization.summarize_session("historical-list", "gpt-4o-mini", **owner)
        storage.assert_not_awaited()
        model.assert_not_awaited()
        execution = await docket.add(summarization.summarize_session, key="historical-task")(
            "historical-list", "gpt-4o-mini", **owner
        )
        await run_worker(docket)
        await execution.sync()
        assert execution.state.value == "failed"
        assert await redis.lrange(messages_key, 0, -1) == before[0]
        assert await redis.hgetall(metadata_key) == before[1]
        storage.assert_not_awaited()
        model.assert_not_awaited()
        assert (await redis.xpending(docket.stream_key, docket.worker_group_name))["pending"] == 0
