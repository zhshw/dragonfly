"""Profile the existing SET/pipeline/WAIT/recovery workload on one connection."""

import argparse
from contextlib import redirect_stdout
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import signal
import sys
import time
import unittest

from prepare import REPO, replace


def process_memory(pid):
    fields = {"VmRSS": "rss_kib", "VmHWM": "peak_rss_kib", "RssAnon": "anon_rss_kib"}
    result = {}
    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
        name, _, value = line.partition(":")
        if name in fields:
            result[fields[name]] = int(value.split()[0])
    return result


def collect(binary, output, mode, profiler, mib):
    output.mkdir(parents=True, exist_ok=False)
    for folder in ("cpu", "spans", "logs", "testdata", "bin"):
        (output / folder).mkdir()
    wrapper = output / "bin/dragonfly"
    wrapper.write_text(
        "#!/usr/bin/env python3\n"
        "import os, sys\n"
        "from pathlib import Path\n"
        "node = next((a.split('=', 1)[1] for a in sys.argv if a.startswith('--raft_node_id=')), None)\n"
        "if node is None:\n"
        "    node = str(int(next(a.split('=', 1)[1] for a in sys.argv if a.startswith('--cluster_node_id=')), 16))\n"
        f"root = Path({str(output)!r})\n"
        "spans = root / 'spans' / ('node-' + node)\n"
        "spans.mkdir(exist_ok=True)\n"
        "os.environ['RAFT_OBSERVE_DIR'] = str(spans)\n"
        + (
            f"os.environ['LD_PRELOAD'] = {str(profiler)!r}\n"
            "os.environ['CPUPROFILE'] = str(root / 'cpu' / ('node-' + node + '.prof'))\n"
            "os.environ['CPUPROFILESIGNAL'] = '12'\n"
            "os.environ['CPUPROFILE_FREQUENCY'] = '500'\n"
            if profiler
            else ""
        )
        + f"os.execv({str(binary)!r}, [{str(binary)!r}, *sys.argv[1:]])\n"
    )
    wrapper.chmod(0o755)
    (output / "bin/raft_storage").symlink_to(binary.with_name("raft_storage"))
    os.environ.update(
        TMPDIR=str(output / "testdata"),
        DRAGONFLY_PATH=str(wrapper),
        RAFT_TEST_DATA_FLAGS="--raft_async_io=true --raft_drain_writes=true --raft_data_ack_mode=memory",
        REPLICATION_TEST_CLIENTS="1",
        REPLICATION_TEST_PIPELINE="64",
        REPLICATION_TEST_MIB=str(mib),
        REPLICATION_TEST_WARMUP_WRITES="64",
        REPLICATION_TEST_SETTLE_SECONDS="5",
    )
    source = (REPO / "tests/integration/replication_comparison_test.py").read_text()
    harness_hash = hashlib.sha256(source.encode()).hexdigest()
    source = replace(
        source,
        "                samples = write_batch(data)",
        "                self.observation_start()\n                samples = write_batch(data)",
    )
    source = replace(
        source,
        "                replication_end = time.monotonic()",
        "                replication_end = time.monotonic()\n"
        "                self.observation_stop(replication_end)",
    )
    source = replace(
        source,
        "                latencies.append((last_ack - sent) * 1000)",
        "                latencies.append((last_ack - sent) * 1000)\n"
        "                if self.observing:\n"
        "                    self.client_spans.append(dict(start_ns=int(sent * 1e9),\n"
        "                        end_ns=int(last_ack * 1e9), stage='client.pipeline',\n"
        "                        arg0=offset, arg1=len(chunk)))",
    )
    harness = output / "observed_workload.py"
    harness.write_text(source)
    sys.path.insert(0, str(REPO / "tests/integration"))
    spec = importlib.util.spec_from_file_location("observed_workload", harness)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    base = (
        module.RaftReplicationComparisonTest
        if mode == "memory"
        else module.AsyncReplicationComparisonTest
    )
    metadata = dict(
        mode="raft.memory" if mode == "memory" else "cluster",
        binary=str(binary),
        binary_sha256=hashlib.file_digest(binary.open("rb"), "sha256").hexdigest(),
        harness_sha256=harness_hash,
        profiler=str(profiler) if profiler else None,
        frequency_hz=500 if profiler else None,
        wall_offset_ns=time.time_ns() - time.monotonic_ns(),
        recovery="Original fixture: offline replica restart, WAIT 2, and full value readback",
    )

    class Observed(base):
        observing = False
        client_spans = []
        profiling_nodes = ()

        def observation_start(self):
            self.profiling_nodes = (self.leader, next(n for n in (11, 12) if n != self.leader))
            metadata["nodes"] = {
                str(n): dict(
                    pid=self.proc[n].pid,
                    role="leader" if n == self.leader else "follower",
                    memory_before=process_memory(self.proc[n].pid),
                )
                for n in self.profiling_nodes
            }
            if profiler:
                for n in self.profiling_nodes:
                    self.proc[n].send_signal(signal.SIGUSR2)
                deadline = time.monotonic() + 3
                while not all(
                    (output / f"cpu/node-{n}.prof.0").exists() for n in self.profiling_nodes
                ):
                    if time.monotonic() > deadline:
                        raise RuntimeError("CPU profiler did not start")
                    time.sleep(0.01)
            self.observing = True
            metadata["start_ns"] = time.monotonic_ns()
            (output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")

        def observation_stop(self, end):
            metadata["end_ns"] = int(end * 1e9)
            self.observing = False
            if profiler:
                for n in self.profiling_nodes:
                    self.proc[n].send_signal(signal.SIGUSR2)
            for n in self.profiling_nodes:
                metadata["nodes"][str(n)]["memory_after"] = process_memory(self.proc[n].pid)
            (output / "client.json").write_text(json.dumps(self.client_spans, indent=2) + "\n")
            (output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")

        def stop(self, node, crash=False):
            try:
                super().stop(node, crash=crash)
            finally:
                if (self.root / f"{node}.log").exists():
                    shutil.copyfile(self.root / f"{node}.log", output / f"logs/{node}.log")

    captured = io.StringIO()
    with (output / "test.log").open("w") as log, redirect_stdout(captured):
        result = unittest.TextTestRunner(stream=log, verbosity=2).run(
            Observed("test_lagging_replica_catches_up_bulk")
        )
    (output / "test.log").write_text((output / "test.log").read_text() + captured.getvalue())
    if not result.wasSuccessful():
        raise RuntimeError(f"Workload failed; see {output / 'test.log'}")
    (row,) = [json.loads(line) for line in captured.getvalue().splitlines() if line.startswith("{")]
    (output / "result.json").write_text(json.dumps(row, indent=2) + "\n")
    print(json.dumps(dict(variant=mode, **row)), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--mode", choices=("memory", "cluster"), required=True)
    parser.add_argument("--profiler", type=Path)
    parser.add_argument("--mib", type=int, default=256)
    args = parser.parse_args()
    collect(
        args.binary.resolve(),
        args.output.resolve(),
        args.mode,
        args.profiler.resolve() if args.profiler else None,
        args.mib,
    )
