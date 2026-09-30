"""Repeatable local AMS/SDK benchmark; starts and destroys its own Dragonfly.

Activate the sibling AMS V0 .venv before running this file. The report describes
backend SDK latency, not HTTP, model, extraction, or production capacity.
"""

import argparse
import ast
import asyncio
import hashlib
import inspect
import json
import os
import platform
import socket
import subprocess
import tempfile
import time
from datetime import UTC, datetime, timedelta
from importlib.metadata import version
from pathlib import Path

import numpy as np
from docket import Docket, Worker
from redis import Redis as SyncRedis
from redis.asyncio import Redis
from redis.exceptions import ConnectionError
from redisvl.index import AsyncSearchIndex
from redisvl.query import VectorQuery

from agent_memory_server.config import settings
from agent_memory_server.memory_vector_db_factory import _build_redis_schema
from agent_memory_server.models import WorkingMemory
from agent_memory_server.working_memory import get_working_memory, set_working_memory
from agent_memory_server.working_memory_index import ensure_working_memory_index

from audit_docket import inventory, MANIFEST


ROOT = Path(__file__).resolve().parents[2]
DECLARED_SCRIPT = """
for iteration = 1, tonumber(ARGV[1]) do
    for _, key in ipairs(KEYS) do
        redis.call('HINCRBY', key, 'counter', 1)
    end
end
return #KEYS * tonumber(ARGV[1])
"""


async def benchmark_task(value):
    return value


def scheduler_source():
    tree = ast.parse(inspect.getsource(Worker))
    method = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "_scheduler_loop"
    )
    call = next(
        node
        for node in ast.walk(method)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "register_script"
    )
    return ast.literal_eval(call.args[0])


def summarize(samples, duration):
    values = np.asarray(samples) * 1000
    return {
        "samples": len(samples),
        "elapsed_seconds": round(duration, 6),
        "throughput_per_second": round(len(samples) / duration, 2),
        "latency_ms": {
            "p50": round(float(np.percentile(values, 50)), 4),
            "p95": round(float(np.percentile(values, 95)), 4),
            "p99": round(float(np.percentile(values, 99)), 4),
            "max": round(float(values.max()), 4),
        },
    }


async def seed_vectors(client, count, dimensions):
    settings.embedding_model = f"benchmark-fixed-{dimensions}d"
    settings.redisvl_vector_dimensions = dimensions
    settings.redisvl_indexing_algorithm = "flat"
    settings.redisvl_distance_metric = "cosine"
    index = AsyncSearchIndex.from_dict(_build_redis_schema(), redis_client=client)
    await index.create()
    rng = np.random.default_rng(20261001)
    vectors = rng.normal(size=(count, dimensions)).astype(np.float32)
    vectors /= np.linalg.norm(vectors, axis=1, keepdims=True)
    rows = [
        {
            "id_": str(i),
            "text": f"fixed benchmark orchard memory {i}",
            "namespace": "benchmark",
            "user_id": "benchmark",
            "vector": vector.tobytes(),
            "created_at": 1,
            "updated_at": 1,
        }
        for i, vector in enumerate(vectors)
    ]
    keys = await index.load(rows)
    assert len(keys) == count
    query = VectorQuery(
        vector=vectors[0].tolist(),
        vector_field_name="vector",
        num_results=10,
        return_fields=["id_", "vector_distance"],
    )
    result = await index.query(query)
    assert result[0]["id_"] == "0" and float(result[0]["vector_distance"]) < 1e-5
    return index, query


async def foreground(client, index, query, args, workload=None):
    run_id = time.monotonic_ns()
    sessions = [
        WorkingMemory(
            session_id=f"benchmark-{run_id}-{i}",
            namespace="benchmark",
            user_id="benchmark",
            data={"payload": "x" * args.json_bytes, "counter": 0},
        )
        for i in range(args.concurrency)
    ]
    for session in sessions:
        await set_working_memory(session, client)
    # Equal warm-up in every scenario; warm-up is excluded from reported samples.
    for _ in range(10):
        await index.query(query)
    start = asyncio.Event()
    stop = asyncio.Event()
    workload_samples = []
    completed_before_frontend_end = 0

    async def background():
        nonlocal completed_before_frontend_end
        if workload is None:
            return
        await start.wait()
        # Offer equal work in every lock-mode trial and drain after foreground
        # completion. Record overlap so scheduling fairness remains visible.
        for _ in range(args.background_calls):
            before = time.perf_counter()
            await workload()
            workload_samples.append(time.perf_counter() - before)
            if not stop.is_set():
                completed_before_frontend_end += 1

    async def vector_requests():
        await start.wait()
        samples = []
        before_group = time.perf_counter()
        for _ in range(args.samples):
            before = time.perf_counter()
            result = await index.query(query)
            samples.append(time.perf_counter() - before)
            assert result[0]["id_"] == "0" and len(result) == 10
        return samples, time.perf_counter() - before_group

    async def json_requests(session):
        await start.wait()
        samples = []
        before_group = time.perf_counter()
        for i in range(args.samples):
            session.data["counter"] = i + 1
            before = time.perf_counter()
            await set_working_memory(session, client)
            samples.append(time.perf_counter() - before)
        final = await get_working_memory(
            session.session_id, session.user_id, session.namespace, client
        )
        assert final.version == args.samples + 1 and final.data["counter"] == args.samples
        return samples, time.perf_counter() - before_group

    task = asyncio.create_task(background())
    workers = [asyncio.create_task(vector_requests()) for _ in sessions]
    workers += [asyncio.create_task(json_requests(session)) for session in sessions]
    start.set()
    results = await asyncio.gather(*workers)
    stop.set()
    await task
    vector = results[: args.concurrency]
    json_updates = results[args.concurrency :]
    report = {
        "vector": summarize(
            [x for samples, _ in vector for x in samples], max(d for _, d in vector)
        ),
        "json_cas": summarize(
            [x for samples, _ in json_updates for x in samples], max(d for _, d in json_updates)
        ),
    }
    if workload_samples:
        report["background"] = summarize(workload_samples, sum(workload_samples))
        report["background"]["offered_calls"] = args.background_calls
        report["background"]["completed_before_frontend_end"] = completed_before_frontend_end
    return report


async def docket_batches(client, docket, scheduler, batch_size, repeats):
    measurements = []
    when = datetime.now(UTC) + timedelta(days=1)
    for repeat in range(repeats):
        before = time.perf_counter()
        # Genuine Docket producer calls create the parked hashes and serialized payloads.
        keys = [f"batch-{batch_size}-{repeat}-{i}" for i in range(batch_size)]
        for offset in range(0, batch_size, 16):
            await asyncio.gather(
                *[
                    docket.add(benchmark_task, key=key, when=when)(17)
                    for key in keys[offset : offset + 16]
                ]
            )
        enqueue_duration = time.perf_counter() - before
        before = time.perf_counter()
        result = await scheduler(
            keys=[docket.queue_key, docket.stream_key], args=[when.timestamp() + 1, docket.prefix]
        )
        scheduling_duration = time.perf_counter() - before
        assert result == [batch_size, batch_size]
        assert await client.zcard(docket.queue_key) == 0
        assert await client.xlen(docket.stream_key) == batch_size
        states = await asyncio.gather(
            *[client.hmget(f"{docket.prefix}:runs:{key}", "state") for key in keys]
        )
        assert all(state == [b"queued"] for state in states)
        measurements.append(
            {
                "enqueue_seconds": enqueue_duration,
                "scheduler_seconds": scheduling_duration,
                "total_seconds": enqueue_duration + scheduling_duration,
            }
        )
        await client.delete(docket.stream_key, *(f"{docket.prefix}:runs:{key}" for key in keys))
    return {
        "batch_size": batch_size,
        "repeats": repeats,
        "tasks": batch_size * repeats,
        "producer_concurrency": min(16, batch_size),
        "enqueue_tasks_per_second": round(
            batch_size * repeats / sum(x["enqueue_seconds"] for x in measurements), 2
        ),
        "scheduler_tasks_per_second": round(
            batch_size * repeats / sum(x["scheduler_seconds"] for x in measurements), 2
        ),
        "producer_to_stream_tasks_per_second": round(
            batch_size * repeats / sum(x["total_seconds"] for x in measurements), 2
        ),
        "scheduler_latency_ms": summarize(
            [x["scheduler_seconds"] for x in measurements],
            sum(x["scheduler_seconds"] for x in measurements),
        )["latency_ms"],
        "raw": measurements,
    }


async def benchmark(url, args):
    async with Redis.from_url(url) as client, Docket(name="benchmark", url=url) as docket:
        await ensure_working_memory_index(client)
        index, query = await seed_vectors(client, args.vectors, args.dimensions)
        declared = client.register_script(DECLARED_SCRIPT)
        global_script = client.register_script(DECLARED_SCRIPT + "\n-- scoped GLOBAL comparison\n")
        await client.script_load(DECLARED_SCRIPT)
        await client.script_load(DECLARED_SCRIPT + "\n-- scoped GLOBAL comparison\n")
        await client.execute_command("SCRIPT", "FLAGS", global_script.sha, "allow-undeclared-keys")
        workload_keys = [f"benchmark:background:{i}" for i in range(args.batch_size)]

        async def declared_workload():
            assert (
                await declared(keys=workload_keys, args=[args.lua_repetitions])
                == args.batch_size * args.lua_repetitions
            )

        async def global_workload():
            # All accessed keys remain explicitly declared; only the transaction
            # lock mode differs. Both scripts retain atomicity.
            assert (
                await global_script(keys=workload_keys, args=[args.lua_repetitions])
                == args.batch_size * args.lua_repetitions
            )

        script_source = scheduler_source()
        scheduler = client.register_script(script_source)
        assert scheduler.sha == args.allowed_sha
        when = datetime.now(UTC) + timedelta(days=1)
        await docket.add(benchmark_task, key="template", when=when)(17)
        template = await client.hgetall(docket.parked_task_key("template"))
        assert (
            template[b"args"] and template[b"kwargs"] and template[b"function"] == b"benchmark_task"
        )
        await client.delete(
            docket.parked_task_key("template"), docket.queue_key, f"{docket.prefix}:runs:template"
        )
        cycle = 0

        async def scheduling_workload():
            nonlocal cycle
            cycle += 1
            keys = [f"concurrent-{cycle}-{i}" for i in range(args.batch_size)]
            async with client.pipeline(transaction=False) as pipeline:
                for key in keys:
                    payload = dict(template)
                    payload[b"key"] = key.encode()
                    pipeline.hset(docket.parked_task_key(key), mapping=payload)
                    pipeline.hset(f"{docket.prefix}:runs:{key}", mapping={"state": "scheduled"})
                pipeline.zadd(docket.queue_key, {key: when.timestamp() for key in keys})
                await pipeline.execute()
            assert await scheduler(
                keys=[docket.queue_key, docket.stream_key],
                args=[when.timestamp() + 1, docket.prefix],
            ) == [args.batch_size, args.batch_size]
            assert await client.xlen(docket.stream_key) == args.batch_size
            await client.delete(docket.stream_key, *(f"{docket.prefix}:runs:{key}" for key in keys))

        workloads = {
            "baseline": None,
            "declared_atomic": declared_workload,
            "global_atomic": global_workload,
            "docket_global_scheduler": scheduling_workload,
        }
        trials = []
        for repeat in range(args.repeats):
            # Alternate order to reduce consistent warming/order bias.
            names = list(workloads)
            if repeat % 2:
                names.reverse()
            for name in names:
                trials.append(
                    {
                        "scenario": name,
                        "repeat": repeat,
                        **await foreground(client, index, query, args, workloads[name]),
                    }
                )
        batches = [
            await docket_batches(client, docket, scheduler, size, args.repeats)
            for size in args.docket_batch_sizes
        ]
        return {
            "trials": trials,
            "docket_batches": batches,
            "database_memory": await client.info("memory"),
            "server_version": (await client.info("server"))["dragonfly_version"],
        }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=ROOT / "build-dbg/dragonfly")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--vectors", type=int, default=1000)
    parser.add_argument("--dimensions", type=int, default=1536)
    parser.add_argument("--json-bytes", type=int, default=1024)
    parser.add_argument("--samples", type=int, default=100)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--lua-repetitions", type=int, default=8)
    parser.add_argument("--background-calls", type=int, default=64)
    parser.add_argument("--docket-batch-sizes", type=int, nargs="+", default=[1, 16, 64, 256])
    args = parser.parse_args()
    assert all(
        getattr(args, field) > 0
        for field in (
            "vectors",
            "dimensions",
            "json_bytes",
            "samples",
            "concurrency",
            "repeats",
            "batch_size",
            "lua_repetitions",
            "background_calls",
        )
    )
    assert args.vectors >= 10 and all(x > 0 for x in args.docket_batch_sizes)
    reviewed = json.loads(MANIFEST.read_text())
    assert version("pydocket") == reviewed["version"]
    assert set(inventory()) == {item["sha"] for item in reviewed["scripts"]}
    args.allowed_sha = reviewed["allowed_sha"]
    with tempfile.TemporaryDirectory(prefix="ams-benchmark-") as directory:
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        flags = [
            f"--port={port}",
            "--bind=127.0.0.1",
            "--force_epoll=true",
            "--proactor_threads=3",
            "--num_shards=2",
            "--dbfilename=",
            "--logtostderr",
            "--default_lua_flags=",
            "--lua_allow_undeclared_auto_correct=false",
            f"--lua_undeclared_keys_shas={args.allowed_sha}",
        ]
        url = f"redis://127.0.0.1:{port}/0"
        with (Path(directory) / "server.log").open("w") as log:
            process = subprocess.Popen(
                [str(args.binary.resolve()), *flags], cwd=directory, stdout=log, stderr=log
            )
            try:
                deadline = time.monotonic() + 15
                with SyncRedis.from_url(url, socket_timeout=1) as client:
                    while time.monotonic() < deadline:
                        assert process.poll() is None, (Path(directory) / "server.log").read_text()
                        try:
                            if client.ping():
                                break
                        except ConnectionError:
                            time.sleep(0.02)
                    else:
                        raise RuntimeError("Benchmark server did not start")
                result = asyncio.run(benchmark(url, args))
            finally:
                process.kill()
                process.wait(timeout=5)
        report = {
            "timestamp_utc": datetime.now(UTC).isoformat(),
            "versions": {name: version(name) for name in ("redisvl", "redis", "pydocket")},
            "python": platform.python_version(),
            "host": platform.platform(),
            "cpu_count": os.cpu_count(),
            "load_average": os.getloadavg(),
            "binary_sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest(),
            "binary": str(args.binary.resolve()),
            "server_flags": flags,
            "configuration": {
                key: str(value) if isinstance(value, Path) else value
                for key, value in vars(args).items()
            },
            "scope": f"SDK backend latency on local Debug binary; {args.concurrency} vector and {args.concurrency} JSON clients; no HTTP/models/workers; shared host, not production capacity",
            "atomicity": "Both comparison scripts explicitly declare all keys. GLOBAL uses only that SHA's allow-undeclared-keys flag; atomicity stays enabled.",
            **result,
        }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2, default=str) + "\n")
        print(f"Saved benchmark to {args.output}")


if __name__ == "__main__":
    main()
