"""Run alternating async-I/O and ready-drain trials on the work directory's filesystem."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

import io_report

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
FLAGS = {
    "async": "--raft_async_io=true --raft_drain_writes=false",
    "drain": "--raft_async_io=true --raft_drain_writes=true",
}


def run(work):
    work = work.resolve()
    data = work / "testdata"
    data.mkdir(exist_ok=True)
    env = dict(os.environ, TMPDIR=str(data))
    metadata = dict(
        kernel=subprocess.check_output(["uname", "-a"], text=True).strip(),
        mount=subprocess.check_output(
            ["findmnt", "-no", "SOURCE,FSTYPE,OPTIONS", "-T", data], text=True
        ).strip(),
        data_directory=str(data),
    )
    (work / "environment.json").write_text(json.dumps(metadata, indent=2) + "\n")
    summaries = []
    for trial, mode in enumerate(("async", "drain") * 3, 1):
        with (work / f"trial-{trial}.log").open("x") as log:
            subprocess.run(
                [
                    sys.executable,
                    HERE / "probe.py",
                    "run",
                    "--work-dir",
                    work,
                    "--trial",
                    str(trial),
                ],
                env=dict(env, RAFT_TEST_DATA_FLAGS=FLAGS[mode]),
                stdout=log,
                stderr=log,
                check=True,
            )
        result = dict(variant=mode, **io_report.probe(work, trial))
        summaries.append(result)
        print(json.dumps(result), flush=True)
    for clients, modes in ((1, ("async", "drain")), (8, ("async", "drain"))):
        for trial in range(1, 4):
            for mode in modes:
                name = f"client-{clients}-{mode}-{trial}"
                trace = work / name
                trace.mkdir()
                with (work / f"{name}.log").open("x") as log:
                    subprocess.run(
                        [
                            sys.executable,
                            "-m",
                            "unittest",
                            "tests.integration.replication_comparison_test."
                            "RaftReplicationComparisonTest.test_lagging_replica_catches_up_bulk",
                        ],
                        cwd=REPO,
                        env=dict(
                            env,
                            RAFT_TEST_DATA_FLAGS=FLAGS[mode],
                            DRAGONFLY_PATH=str(work / "probe/dragonfly"),
                            RAFT_COMMIT_BENCH_VALUES=str(work / "probe/payloads"),
                            RAFT_IO_TRACE_DIR=str(trace),
                            REPLICATION_TEST_CLIENTS=str(clients),
                        ),
                        stdout=log,
                        stderr=log,
                        check=True,
                    )
                result = dict(variant=mode, **io_report.client(work, name))
                summaries.append(result)
                print(json.dumps(result), flush=True)
    (work / "summary.json").write_text(json.dumps(summaries, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("work", type=Path)
    run(parser.parse_args().work)
