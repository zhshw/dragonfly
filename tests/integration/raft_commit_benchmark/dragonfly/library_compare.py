"""Compare two io_uring builds with identical Raft durability and client workloads."""

import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

REPO = Path(__file__).resolve().parents[4]


def run(
    helio,
    folly,
    output,
    clients=(1, 8),
    mib=64,
    warmup=32,
    labels=("helio", "folly"),
    modes=("raft", "raft"),
):
    builds = dict(zip(labels, (helio.resolve(), folly.resolve())))
    fixtures = {
        "yes": "AsyncReplicationComparisonTest",
        "raft": "RaftReplicationComparisonTest",
    }
    mode_by_label = dict(zip(labels, modes))
    for build in builds.values():
        for name in ("dragonfly", "raft_storage"):
            if not (build / name).is_file():
                raise FileNotFoundError(build / name)
    output = output.resolve()
    output.mkdir()
    data = output / "testdata"
    data.mkdir()
    metadata = {
        "kernel": subprocess.check_output(["uname", "-a"], text=True).strip(),
        "mount": subprocess.check_output(
            ["findmnt", "-no", "SOURCE,FSTYPE,OPTIONS", "-T", data], text=True
        ).strip(),
        "binaries": {},
        "clients": clients,
        "payload_mib": mib,
        "warmup_writes": warmup,
        "settle_seconds": 5,
        "modes": mode_by_label,
        "raft_flags": "--raft_async_io=true --raft_drain_writes=true",
        "replication_completion": "all writing connections returned WAIT 1 = 1",
        "primary_metric": "replication_mib_per_s" if len(set(modes)) > 1 else "mib_per_s",
        "harness_sha256": hashlib.sha256(
            (REPO / "tests/integration/replication_comparison_test.py").read_bytes()
        ).hexdigest(),
    }
    for name, build in builds.items():
        digest = hashlib.sha256()
        with (build / "dragonfly").open("rb") as source:
            for chunk in iter(lambda: source.read(1 << 20), b""):
                digest.update(chunk)
        metadata["binaries"][name] = {
            "path": str(build / "dragonfly"),
            "sha256": digest.hexdigest(),
        }
    (output / "environment.json").write_text(json.dumps(metadata, indent=2) + "\n")
    results = []
    for count in clients:
        for trial in range(1, 4):
            order = labels if trial % 2 else labels[::-1]
            for library in order:
                log_path = output / f"{count}-{trial}-{library}.log"
                env = dict(
                    os.environ,
                    TMPDIR=str(data),
                    DRAGONFLY_PATH=str(builds[library] / "dragonfly"),
                    RAFT_TEST_DATA_FLAGS="--raft_async_io=true --raft_drain_writes=true",
                    REPLICATION_TEST_CLIENTS=str(count),
                    REPLICATION_TEST_MIB=str(mib),
                    REPLICATION_TEST_SETTLE_SECONDS="5",
                    REPLICATION_TEST_WARMUP_WRITES=str(warmup),
                )
                with log_path.open("x") as log:
                    subprocess.run(
                        [
                            sys.executable,
                            "-m",
                            "unittest",
                            "tests.integration.replication_comparison_test."
                            f"{fixtures[mode_by_label[library]]}.test_lagging_replica_catches_up_bulk",
                        ],
                        cwd=REPO,
                        env=env,
                        stdout=log,
                        stderr=log,
                        check=True,
                        timeout=120,
                    )
                (sample,) = (
                    json.loads(line)
                    for line in log_path.read_text().splitlines()
                    if line.startswith("{")
                )
                result = dict(run_order=len(results) + 1, trial=trial, library=library, **sample)
                results.append(result)
                print(json.dumps(result), flush=True)
                (output / "summary.json").write_text(json.dumps(results, indent=2) + "\n")
                with (output / "rounds.csv").open("w") as target:
                    writer = csv.DictWriter(target, fieldnames=list(results[0]))
                    writer.writeheader()
                    writer.writerows(results)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--helio-build", "--baseline-build", type=Path, required=True)
    parser.add_argument("--folly-build", "--candidate-build", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--labels", nargs=2, default=["helio", "folly"])
    parser.add_argument("--clients", type=int, nargs="+", default=[1, 8])
    parser.add_argument("--mib", type=int, default=64)
    parser.add_argument("--warmup", type=int, default=32)
    args = parser.parse_args()
    if (
        len(set(args.labels)) != 2
        or any(
            not label or not all(c.isalnum() or c in "-_" for c in label) for label in args.labels
        )
        or args.mib < 1
        or args.warmup < 0
        or any(c < 1 or c > args.mib * 4 for c in args.clients)
    ):
        parser.error("Require distinct simple labels, positive MiB/clients and nonnegative warmup")
    run(
        args.helio_build,
        args.folly_build,
        args.output_dir,
        args.clients,
        args.mib,
        args.warmup,
        args.labels,
    )
