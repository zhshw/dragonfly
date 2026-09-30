"""Restore AMS data and actual Docket deliveries on isolated two-shard servers."""

import asyncio
import os
import socket
import subprocess
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from docket import Docket, Worker
from docket.dependencies import CurrentExecution
from docket.execution import ExecutionState
from redis import Redis as SyncRedis
from redis.asyncio import Redis
from redis.exceptions import ConnectionError, BusyLoadingError
from redisvl.index import AsyncSearchIndex
from redisvl.query import FilterQuery
from redisvl.query.filter import Tag

from agent_memory_server.config import settings
from agent_memory_server.filters import Namespace
from agent_memory_server.memory_vector_db import RedisVLMemoryVectorDatabase
from agent_memory_server.memory_vector_db_factory import _build_redis_schema
from agent_memory_server.models import MemoryRecord, Task, TaskStatusEnum, TaskTypeEnum
from agent_memory_server.tasks import create_task, get_task, update_task_status
from agent_memory_server.utils import redis as redis_utils
from agent_memory_server.working_memory_index import get_working_memory_index

from conftest import ROOT, SCHEDULER_SHA
from test_docket import run_worker
from test_search import FixedEmbeddings


class PersistentServer:
    def __init__(self, directory):
        self.directory = directory
        directory.mkdir()
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            self.port = listener.getsockname()[1]
        self.url = f"redis://127.0.0.1:{self.port}/0"
        self.process = None
        self.log = None

    def start(self):
        binary = Path(os.environ.get("DRAGONFLY_PATH", ROOT / "build-dbg/dragonfly"))
        self.log = (self.directory / "server.log").open("a")
        self.process = subprocess.Popen(
            [
                str(binary),
                f"--port={self.port}",
                "--bind=127.0.0.1",
                "--force_epoll=true",
                "--proactor_threads=3",
                "--num_shards=2",
                f"--dir={self.directory}",
                "--dbfilename=memory",
                "--logtostderr",
                "--default_lua_flags=",
                "--lua_allow_undeclared_auto_correct=false",
                f"--lua_undeclared_keys_shas={SCHEDULER_SHA}",
            ],
            cwd=self.directory,
            stdout=self.log,
            stderr=self.log,
        )
        with SyncRedis.from_url(self.url, socket_timeout=1) as client:
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                assert self.process.poll() is None, (self.directory / "server.log").read_text()
                try:
                    if client.ping() and not client.info("persistence")["loading"]:
                        return self
                except (ConnectionError, BusyLoadingError):
                    time.sleep(0.02)
        pytest.fail((self.directory / "server.log").read_text())

    def kill(self):
        if self.process is not None:
            if self.process.poll() is None:
                self.process.kill()
            self.process.wait(timeout=5)
            self.process = None
            self.log.close()


@pytest.fixture
def persistent_servers(tmp_path):
    servers = []

    def start(name):
        server = PersistentServer(tmp_path / name)
        servers.append(server)
        return server.start()

    yield start
    for server in reversed(servers):
        server.kill()


def memory_db(client):
    index = AsyncSearchIndex.from_dict(_build_redis_schema(), redis_client=client)
    return RedisVLMemoryVectorDatabase(index, FixedEmbeddings())


async def persistence_task(value: int, execution=CurrentExecution()):
    task = await get_task(execution.key)
    if task is None:
        await create_task(Task(id=execution.key, type=TaskTypeEnum.SUMMARY_VIEW_FULL_RUN))
    await update_task_status(
        execution.key, status=TaskStatusEnum.RUNNING, started_at=datetime.now(UTC)
    )
    async with execution.docket.redis() as client:
        await client.hset("probe:effects", execution.key, value)
    await update_task_status(
        execution.key, status=TaskStatusEnum.SUCCESS, completed_at=datetime.now(UTC)
    )
    return {"value": value}


async def durable_state(client, docket):
    # Delivery idle times and TTL countdowns are deliberately excluded; absolute
    # expiry deadlines and the stable PEL/consumer identities are checked instead.
    hashes = {}
    for pattern in (f"{settings.redisvl_index_prefix}:*", f"{docket.prefix}:runs:*"):
        async for key in client.scan_iter(match=pattern):
            if await client.type(key) == b"hash":
                hashes[key] = await client.hgetall(key)
    return {
        "hashes": hashes,
        "document": await client.json().get("working_memory:tenant-a:alice:session-a"),
        "tasks": {key: await client.get(f"task:{key}") for key in ("done", "pending", "delayed")},
        "completed_result": await docket.result_storage.get(
            (await docket.get_execution("done")).result_key
        ),
        "queue": await client.zrange(docket.queue_key, 0, -1, withscores=True),
        "parked": await client.hgetall(docket.parked_task_key("delayed")),
        "stream": await client.xrange(docket.stream_key),
        "groups": [
            {key: group[key] for key in ("name", "pending", "last-delivered-id")}
            for group in await client.xinfo_groups(docket.stream_key)
        ],
        "consumers": [
            {key: consumer[key] for key in ("name", "pending")}
            for consumer in await client.xinfo_consumers(
                docket.stream_key, docket.worker_group_name
            )
        ],
        "pel": [
            (row["message_id"], row["consumer"], row["times_delivered"])
            for row in await client.xpending_range(
                docket.stream_key, docket.worker_group_name, "-", "+", 10
            )
        ],
        "expires": {
            key: await client.pexpiretime(key)
            for key in (
                "working_memory:tenant-a:alice:session-a",
                "ttl:short",
                "ttl:long",
                "task:done",
            )
        },
        "contract": await client.get(f"memory_index_contract:{settings.redisvl_index_name}"),
    }


async def seed_state(client, docket):
    working_index = await get_working_memory_index(client)
    await working_index.create()
    document = {
        "session_id": "session-a",
        "namespace": "tenant-a",
        "user_id": "alice",
        "created_at": 1,
        "updated_at": 2,
        "messages": [{"role": "user", "content": "durable orchard"}],
    }
    await working_index.load([document], keys=["working_memory:tenant-a:alice:session-a"])
    await client.pexpire("working_memory:tenant-a:alice:session-a", 60000)
    db = memory_db(client)
    await db.add_memories(
        [
            MemoryRecord(
                id="durable", text="recent orchard", namespace="tenant-a", user_id="alice"
            ),
            MemoryRecord(id="other", text="other ocean", namespace="tenant-b", user_id="bob"),
        ]
    )
    await client.set("ttl:short", "expires", px=5000)
    await client.set("ttl:long", "retained", px=60000)
    docket.register(persistence_task)
    await docket.add(persistence_task, key="done")(11)
    await run_worker(docket)
    for key in ("pending", "delayed"):
        await create_task(Task(id=key, type=TaskTypeEnum.SUMMARY_VIEW_FULL_RUN))
    await docket.add(persistence_task, key="pending")(12)
    await docket.add(
        persistence_task, key="delayed", when=datetime.now(UTC) + timedelta(seconds=2)
    )(13)
    deliveries = await client.xreadgroup(
        docket.worker_group_name, "lost-worker", {docket.stream_key: ">"}, count=1
    )
    assert deliveries and len(deliveries[0][1]) == 1
    assert (await client.xpending(docket.stream_key, docket.worker_group_name))["pending"] == 1
    state = await durable_state(client, docket)
    memory_hashes = [
        key for key in state["hashes"] if key.startswith(settings.redisvl_index_prefix.encode())
    ]
    assert len(memory_hashes) == 2
    assert all(state["hashes"][key][b"vector"] for key in memory_hashes)
    assert all(state["tasks"].values())
    assert state["completed_result"] is not None
    assert state["queue"] and state["parked"] and state["pel"]
    return state


async def verify_and_continue(client, docket, expected):
    assert await durable_state(client, docket) == expected
    # Both actual SDK schemas must be restored, including vector bytes and tenant filters.
    working_index = await get_working_memory_index(client)
    query = FilterQuery(filter_expression=Tag("namespace") == "tenant-a", return_fields=["$"])
    assert len(await working_index.query(query)) == 1
    db = memory_db(client)
    await db._ensure_index()
    rows = await db.search_memories("orchard", namespace=Namespace(eq="tenant-a"))
    assert [row.id for row in rows.memories] == ["durable"]
    assert rows.memories[0].dist == pytest.approx(0, abs=1e-6)
    completed = await docket.get_execution("done")
    assert completed.state == ExecutionState.COMPLETED
    assert await completed.get_result(timeout=timedelta(seconds=1)) == {"value": 11}
    docket.register(persistence_task)
    async with Worker(
        docket,
        name="restored-worker",
        schedule_automatic_tasks=False,
        # Reclaim the lost worker without also stealing a healthy execution
        # before it has time to persist SUCCESS and ACK on a busy host.
        redelivery_timeout=timedelta(seconds=1),
        minimum_check_interval=timedelta(milliseconds=20),
        scheduling_resolution=timedelta(milliseconds=20),
    ) as worker:
        await asyncio.wait_for(worker.run_until_finished(), timeout=12)
    for key, value in (("done", 11), ("pending", 12), ("delayed", 13)):
        execution = await docket.get_execution(key)
        assert execution.state == ExecutionState.COMPLETED
        assert await execution.get_result(timeout=timedelta(seconds=1)) == {"value": value}
        assert (await get_task(key)).status == TaskStatusEnum.SUCCESS
        assert await client.hget("probe:effects", key) == str(value).encode()
        assert not await client.exists(docket.parked_task_key(key))
    assert await client.zcard(docket.queue_key) == 0
    assert await client.xlen(docket.stream_key) == 0
    assert (await client.xpending(docket.stream_key, docket.worker_group_name))["pending"] == 0
    # Expiration remains absolute, rather than being reset on restart/promotion.
    remaining = expected["expires"]["ttl:short"] / 1000 - time.time()
    if remaining > 0:
        await asyncio.sleep(remaining + 0.05)
    assert not await client.exists("ttl:short")
    assert await client.get("ttl:long") == b"retained"
    assert await client.pttl("ttl:long") > 0


@pytest.fixture
def persistence_settings(monkeypatch):
    monkeypatch.setattr(settings, "redisvl_vector_dimensions", 3)
    monkeypatch.setattr(settings, "embedding_model", "test-fixed-3d")
    monkeypatch.setattr(settings, "redisvl_indexing_algorithm", "flat")
    monkeypatch.setattr(settings, "redisvl_distance_metric", "cosine")


async def test_native_snapshot_restores_ams_and_docket(
    persistent_servers, persistence_settings, monkeypatch
):
    server = persistent_servers("snapshot")
    async with (
        Redis.from_url(server.url) as client,
        Docket(name="memory-server", url=server.url) as docket,
    ):
        monkeypatch.setattr(redis_utils, "_redis_pool", client)
        expected = await seed_state(client, docket)
        assert await client.execute_command("SAVE", "DF") is True
    assert (server.directory / "memory-summary.dfs").is_file()
    assert len(list(server.directory.glob("memory-*.dfs"))) == 3
    # SIGKILL prevents graceful-shutdown persistence from masking missing SAVE data.
    server.kill()
    server.start()
    async with (
        Redis.from_url(server.url) as client,
        Docket(name="memory-server", url=server.url) as docket,
    ):
        monkeypatch.setattr(redis_utils, "_redis_pool", client)
        await verify_and_continue(client, docket, expected)


async def wait_replication(master, replica):
    deadline = time.monotonic() + 15
    offset = expected_offset = None
    while time.monotonic() < deadline:
        role = await replica.role()
        if role[0] == b"slave" and role[3] == b"online":
            expected_offset = await master.execute_command("DFLY", "REPLICAOFFSET")
            _, offset = await replica.execute_command("DEBUG", "REPLICA", "OFFSET")
            if offset == expected_offset:
                return
        await asyncio.sleep(0.02)
    pytest.fail(
        f"Replica failed to catch up: {await replica.role()}, {offset!r} != {expected_offset!r}"
    )


@pytest.mark.parametrize("replication_phase", ["full", "incremental"])
async def test_replica_promotion_restores_ams_and_docket(
    persistent_servers, persistence_settings, monkeypatch, replication_phase
):
    master_server = persistent_servers("master")
    replica_server = persistent_servers("replica")
    async with (
        Redis.from_url(master_server.url) as master,
        Redis.from_url(replica_server.url) as replica,
        Docket(name="memory-server", url=master_server.url) as docket,
    ):
        monkeypatch.setattr(redis_utils, "_redis_pool", master)
        if replication_phase == "full":
            await seed_state(master, docket)
        assert await replica.execute_command("REPLICAOF", "127.0.0.1", master_server.port) == b"OK"
        await wait_replication(master, replica)
        if replication_phase == "incremental":
            await seed_state(master, docket)
        # Also test incremental JSON/hash/TTL replication after the full sync.
        await master.json().set("working_memory:tenant-a:alice:session-a", "$.updated_at", 3)
        await master.hset("replicated:incremental", mapping={"value": "after-full-sync"})
        await master.pexpire("ttl:long", 45000)
        expected = await durable_state(master, docket)
        await wait_replication(master, replica)
        assert await replica.hgetall("replicated:incremental") == {b"value": b"after-full-sync"}
        master_server.kill()
        assert await replica.execute_command("REPLICAOF", "NO", "ONE") == b"OK"
    async with (
        Redis.from_url(replica_server.url) as client,
        Docket(name="memory-server", url=replica_server.url) as docket,
    ):
        monkeypatch.setattr(redis_utils, "_redis_pool", client)
        await verify_and_continue(client, docket, expected)
