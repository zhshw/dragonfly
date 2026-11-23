"""Compare one pipeline workload; evaluate its benefit before testing more connections."""

import argparse
import csv
import json
import os
from pathlib import Path
import subprocess
import sys

import probe


def run(build, output, baseline=None, include_cluster=False, clients=1, depth=64):
    build, output = build.resolve(), output.resolve()
    output.mkdir()
    data = output / "testdata"
    data.mkdir()
    metadata = dict(
        binary=str(build / "dragonfly"),
        sha256=probe.sha256(build / "dragonfly"),
        harness_sha256=probe.sha256(
            probe.REPO / "tests/integration/replication_comparison_test.py"
        ),
        flags="--raft_async_io=true --raft_drain_writes=true",
        primary_metric="replication_mib_per_s",
        distinct_keys=True,
        clients=clients,
        pipeline_depth=depth,
        pipeline_connection="Same reserved connection for packed SET commands and final WAIT",
        mount=subprocess.check_output(
            ["findmnt", "-no", "SOURCE,FSTYPE,OPTIONS", "-T", data], text=True
        ).strip(),
    )
    builds = [("candidate", build)]
    if baseline is not None:
        builds.insert(0, ("baseline", baseline.resolve()))
    if include_cluster:
        builds.append(("cluster", build))
    metadata["builds"] = {
        label: dict(
            binary=str(path / "dragonfly"),
            sha256=probe.sha256(path / "dragonfly"),
            mode="yes" if label == "cluster" else "raft",
        )
        for label, path in builds
    }
    (output / "environment.json").write_text(json.dumps(metadata, indent=2) + "\n")
    rows = []
    variants = ((clients, depth),)
    for trial in range(3):
        for clients, depth in variants[trial:] + variants[:trial]:
            shift = (trial + clients + depth) % len(builds)
            ordered_builds = builds[shift:] + builds[:shift]
            for label, binary_dir in ordered_builds:
                name = f"trial-{trial + 1}-{label}-clients-{clients}-pipeline-{depth}"
                log_path = output / f"{name}.log"
                test_class = (
                    "AsyncReplicationComparisonTest"
                    if label == "cluster"
                    else "RaftReplicationComparisonTest"
                )
                with log_path.open("x") as log:
                    subprocess.run(
                        [
                            sys.executable,
                            "-m",
                            "unittest",
                            "tests.integration.replication_comparison_test."
                            f"{test_class}.test_lagging_replica_catches_up_bulk",
                        ],
                        cwd=probe.REPO,
                        env=dict(
                            os.environ,
                            TMPDIR=str(data),
                            DRAGONFLY_PATH=str(binary_dir / "dragonfly"),
                            RAFT_TEST_DATA_FLAGS=metadata["flags"],
                            REPLICATION_TEST_CLIENTS=str(clients),
                            REPLICATION_TEST_PIPELINE=str(depth),
                            REPLICATION_TEST_MIB="256",
                            REPLICATION_TEST_WARMUP_WRITES="64",
                            REPLICATION_TEST_SETTLE_SECONDS="5",
                        ),
                        stdout=log,
                        stderr=log,
                        check=True,
                        timeout=240,
                    )
                (row,) = [
                    json.loads(s) for s in log_path.read_text().splitlines() if s.startswith("{")
                ]
                row = dict(run_order=len(rows) + 1, trial=trial + 1, variant=label, **row)
                rows.append(row)
                print(json.dumps(row), flush=True)
                (output / "summary.json").write_text(json.dumps(rows, indent=2) + "\n")
                with (output / "rounds.csv").open("w", newline="") as target:
                    writer = csv.DictWriter(target, fieldnames=list(rows[0]))
                    writer.writeheader()
                    writer.writerows(rows)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--baseline-build", type=Path)
    parser.add_argument("--include-cluster", action="store_true")
    parser.add_argument("--clients", type=int, default=1, help="Only this connection count")
    parser.add_argument("--pipeline-depth", type=int, default=64)
    args = parser.parse_args()
    if not 1 <= args.clients <= 1024 or args.pipeline_depth < 1:
        parser.error("Require 1..1024 clients and positive pipeline depth")
    run(
        args.build_dir,
        args.output_dir,
        args.baseline_build,
        args.include_cluster,
        args.clients,
        args.pipeline_depth,
    )
