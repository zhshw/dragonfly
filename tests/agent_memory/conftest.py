"""Real SDK contracts against disposable, two-shard Dragonfly processes."""

import os
import socket
import subprocess
import time
from importlib.metadata import version
from pathlib import Path

import pytest
from redis import Redis
from redis.asyncio import Redis as AsyncRedis
from redis.exceptions import ConnectionError


ROOT = Path(__file__).resolve().parents[2]
SCHEDULER_SHA = "8929ea55fc26d043e4a7d3f010a74baeff87a465"


@pytest.fixture(scope="session", autouse=True)
def locked_dependencies():
    for package, expected in {"redisvl": "0.12.1", "redis": "6.4.0", "pydocket": "0.18.2"}.items():
        assert version(package) == expected, f"Re-audit {package} before changing its version"


@pytest.fixture
def dragonfly_factory(tmp_path):
    processes = []

    def start(*extra_flags):
        # Preserve the explicit-deny cases. Instances that already authorize
        # the audited upstream scheduler also authorize our bounded adapter.
        bounded_sha = "46dec5a67c168033d38f45742aba3572a9d73be9"
        extra_flags = [
            (
                f"{flag},{bounded_sha}"
                if flag.startswith("--lua_undeclared_keys_shas=")
                and SCHEDULER_SHA in flag
                and bounded_sha not in flag
                else flag
            )
            for flag in extra_flags
        ]
        binary = Path(os.environ.get("DRAGONFLY_PATH", ROOT / "build-dbg/dragonfly")).resolve()
        assert binary.is_file(), f"Build {binary} first"
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        directory = tmp_path / str(port)
        directory.mkdir()
        log = (directory / "server.log").open("w")
        args = [
            str(binary),
            f"--port={port}",
            "--bind=127.0.0.1",
            "--force_epoll=true",
            "--proactor_threads=3",
            "--num_shards=2",
            "--dbfilename=",
            "--logtostderr",
            "--default_lua_flags=",
            "--lua_allow_undeclared_auto_correct=false",
            *extra_flags,
        ]
        process = subprocess.Popen(args, cwd=directory, stdout=log, stderr=log)
        processes.append((process, log))
        url = f"redis://127.0.0.1:{port}/0"
        deadline = time.monotonic() + 15
        with Redis.from_url(url, socket_timeout=1) as client:
            while time.monotonic() < deadline:
                assert process.poll() is None, (directory / "server.log").read_text()
                try:
                    if client.ping():
                        return url
                except ConnectionError:
                    time.sleep(0.05)
        pytest.fail((directory / "server.log").read_text())

    yield start
    for process, log in reversed(processes):
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
        log.close()


@pytest.fixture
async def redis_client(dragonfly_factory):
    async with AsyncRedis.from_url(dragonfly_factory(), decode_responses=True) as client:
        yield client
