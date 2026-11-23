"""Compare test-only drain/Fetch budgets with client timing, I/O traces and recovery."""

import argparse
import csv
import json
import os
from pathlib import Path
from statistics import mean, median
import subprocess
import sys

import io_report
import probe

VARIANTS = ((1, 1), (4, 1), (4, 4), (8, 8))


def io_summary(events, row):
    begin, end = row["measure_start_ns"], row["measure_end_ns"]
    measured = [e for e in events if begin <= e["ns"] <= end]
    leaders = {
        e["node"]
        for e in measured
        if e["stage"] == "write_issue" and e["node"] and e["file"].endswith(".log")
    }
    if len(leaders) != 1:
        raise ValueError(f"Expected one async leader in the measured window, got {leaders}")
    (leader,) = leaders
    result = io_report.summarize_io(events, begin, end, row["keys"])
    result["leader"] = leader
    for role in ("leader", "follower"):
        appends = [
            e
            for e in measured
            if e["stage"] == "append" and e["node"] and (e["node"] == leader) == (role == "leader")
        ]
        if sum(e["batches"] for e in appends) != row["keys"]:
            raise ValueError(f"{role} trace did not account for every measured SET")
        result.update(
            {
                f"{role}_rounds": len(appends),
                f"{role}_batches_mean": round(mean(e["batches"] for e in appends), 2),
                f"{role}_batches_p50": median(e["batches"] for e in appends),
                f"{role}_append_bytes_max": max(e["bytes"] for e in appends),
            }
        )
    return result


def run(build, output, clients, trials, mib, probes_dir=None):
    build, output = build.resolve(), output.resolve()
    output.mkdir()
    data = output / "testdata"
    data.mkdir()
    config = build / "src/server/cluster/raft/generated/raft_io_config.h"
    if "#define DFLY_RAFT_USE_FOLLY 1" not in config.read_text():
        raise ValueError("This experiment requires a Folly build")
    metadata = dict(
        kernel=subprocess.check_output(["uname", "-a"], text=True).strip(),
        mount=subprocess.check_output(
            ["findmnt", "-no", "SOURCE,FSTYPE,OPTIONS", "-T", data], text=True
        ).strip(),
        build=str(build),
        clients=clients,
        trials=trials,
        payload_mib=mib,
        warmup_writes=64,
        settle_seconds=5,
        instrumented=True,
        variants={},
    )
    for drain, fetch in VARIANTS:
        name = f"drain-{drain}-fetch-{fetch}"
        prepared = (probes_dir.resolve() if probes_dir else output) / name
        if probes_dir:
            experiment = json.loads((prepared / "probe/experiment.json").read_text())
            if experiment != dict(drain_mib=drain, fetch_mib=fetch, faults=False):
                raise ValueError(f"Unexpected probe configuration: {experiment}")
        else:
            with (output / f"{name}-build.log").open("x") as log:
                subprocess.run(
                    [
                        sys.executable,
                        probe.HERE / "probe.py",
                        "prepare",
                        "--build-dir",
                        build,
                        "--work-dir",
                        prepared,
                        "--drain-mib",
                        str(drain),
                        "--fetch-mib",
                        str(fetch),
                    ],
                    stdout=log,
                    stderr=log,
                    check=True,
                )
        binary = prepared / "probe/dragonfly"
        metadata["variants"][name] = dict(path=str(binary), sha256=probe.sha256(binary))
        print(f"Built {name}", flush=True)
    (output / "environment.json").write_text(json.dumps(metadata, indent=2) + "\n")
    results = []
    for count in clients:
        for trial in range(trials):
            shift = trial % len(VARIANTS)
            for drain, fetch in VARIANTS[shift:] + VARIANTS[:shift]:
                variant = f"drain-{drain}-fetch-{fetch}"
                name = f"client-{count}-trial-{trial + 1}-{variant}"
                trace = output / name
                trace.mkdir()
                with (output / f"{name}.log").open("x") as log:
                    subprocess.run(
                        [
                            sys.executable,
                            "-m",
                            "unittest",
                            "tests.integration.replication_comparison_test."
                            "RaftReplicationComparisonTest.test_lagging_replica_catches_up_bulk",
                        ],
                        cwd=probe.REPO,
                        env=dict(
                            os.environ,
                            TMPDIR=str(data),
                            DRAGONFLY_PATH=metadata["variants"][variant]["path"],
                            RAFT_COMMIT_BENCH_VALUES=str(
                                Path(metadata["variants"][variant]["path"]).parent / "payloads"
                            ),
                            RAFT_TEST_DATA_FLAGS="--raft_async_io=true --raft_drain_writes=true",
                            RAFT_IO_TRACE_DIR=str(trace),
                            REPLICATION_TEST_CLIENTS=str(count),
                            REPLICATION_TEST_MIB=str(mib),
                            REPLICATION_TEST_SETTLE_SECONDS="5",
                            REPLICATION_TEST_WARMUP_WRITES="64",
                        ),
                        stdout=log,
                        stderr=log,
                        check=True,
                        timeout=240,
                    )
                (row,) = (
                    json.loads(line)
                    for line in (output / f"{name}.log").read_text().splitlines()
                    if line.startswith("{")
                )
                result = dict(
                    run_order=len(results) + 1,
                    trial=trial + 1,
                    drain_mib=drain,
                    fetch_mib=fetch,
                    **row,
                    **io_summary(io_report.load(trace), row),
                )
                results.append(result)
                print(json.dumps(result), flush=True)
                (output / "summary.json").write_text(json.dumps(results, indent=2) + "\n")
                with (output / "rounds.csv").open("w", newline="") as target:
                    writer = csv.DictWriter(target, fieldnames=list(results[0]))
                    writer.writeheader()
                    writer.writerows(results)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--clients", type=int, nargs="+", default=[1, 2, 8])
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--mib", type=int, default=128)
    parser.add_argument(
        "--probes-dir", type=Path, help="Reuse verified variants from an earlier run"
    )
    args = parser.parse_args()
    if args.trials < 1 or args.mib < 1 or any(c < 1 or c > args.mib * 4 for c in args.clients):
        parser.error("Require positive trials/MiB and 1 <= clients <= measured SET count")
    run(args.build_dir, args.output_dir, args.clients, args.trials, args.mib, args.probes_dir)
