"""Measure encoding, file barriers and committed application in async Raft only."""

import argparse
import csv
import json
import os
from pathlib import Path
import subprocess
import sys

import drain_compare
import io_report
import probe


def union_ns(spans):
    total = 0
    finish = 0
    for begin, end in sorted(spans):
        total += max(0, end - max(begin, finish))
        finish = max(finish, end)
    return total


def summarize(events, row):
    result = drain_compare.io_summary(events, row)
    begin, end = row["measure_start_ns"], row["measure_end_ns"]
    duration = end - begin
    nodes = {e["pid"]: e["node"] for e in events if e["node"] is not None}
    phases = {"leader_encode": [], "leader_apply": [], "follower_apply": []}
    starts = {}
    for event in events:
        stage = event["stage"]
        if stage not in ("encode_begin", "encode_end", "apply_begin", "apply_end"):
            continue
        if event["bytes"] != 10:
            continue
        kind, edge = stage.split("_")
        key = event["pid"], kind
        if edge == "begin":
            if key in starts:
                raise ValueError(f"Overlapping phase: {key}")
            starts[key] = event["ns"]
        else:
            start = starts.pop(key)
            role = "leader" if nodes[event["pid"]] == result["leader"] else "follower"
            if begin <= start <= event["ns"] <= end:
                phases[f"{role}_{kind}"].append((start, event["ns"]))
    if starts:
        raise ValueError("Incomplete encoding/application phase")
    barriers = [s for s in io_report.intervals(events) if begin <= s[2] <= s[3] <= end]
    for role in ("leader", "follower"):
        for kind in ("segment", "marker"):
            spans = [
                (s[2], s[3])
                for s in barriers
                if s[1] == kind and (s[0] == result["leader"]) == (role == "leader")
            ]
            phases[f"{role}_{kind}"] = spans
    for name, spans in phases.items():
        if not spans:
            raise ValueError(f"Missing measured phase: {name}")
        result[name + "_ms"] = round(sum(b - a for a, b in spans) / 1e6, 3)
        result[name + "_pct"] = round(sum(b - a for a, b in spans) * 100 / duration, 2)
        result[name + "_p50_ms"] = round(
            io_report.percentile([(b - a) / 1e6 for a, b in spans], 0.5), 3
        )
    result["barrier_union_pct"] = round(
        union_ns([(s[2], s[3]) for s in barriers]) * 100 / duration, 2
    )
    return result


def run(prepared, output, clients, trials, mib):
    prepared, output = prepared.resolve(), output.resolve()
    experiment = json.loads((prepared / "probe/experiment.json").read_text())
    if experiment != dict(drain_mib=None, fetch_mib=None, faults=False, phases=True):
        raise ValueError(f"Expected current adaptive async probe with phases: {experiment}")
    output.mkdir()
    data = output / "testdata"
    data.mkdir()
    binary = prepared / "probe/dragonfly"
    metadata = dict(
        binary=str(binary),
        sha256=probe.sha256(binary),
        flags="--raft_async_io=true --raft_drain_writes=true",
        instrumented=True,
        mount=subprocess.check_output(
            ["findmnt", "-no", "SOURCE,FSTYPE,OPTIONS", "-T", data], text=True
        ).strip(),
        phases="phase_trace.patch; fsync issue-to-completion includes scheduling",
    )
    (output / "environment.json").write_text(json.dumps(metadata, indent=2) + "\n")
    rows = []
    for count in clients:
        for trial in range(1, trials + 1):
            name = f"clients-{count}-trial-{trial}"
            trace = output / name
            trace.mkdir()
            log_path = output / f"{name}.log"
            with log_path.open("x") as log:
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
                        DRAGONFLY_PATH=str(binary),
                        RAFT_COMMIT_BENCH_VALUES=str(prepared / "probe/payloads"),
                        RAFT_IO_TRACE_DIR=str(trace),
                        RAFT_TEST_DATA_FLAGS=metadata["flags"],
                        REPLICATION_TEST_CLIENTS=str(count),
                        REPLICATION_TEST_MIB=str(mib),
                        REPLICATION_TEST_WARMUP_WRITES="64",
                        REPLICATION_TEST_SETTLE_SECONDS="5",
                    ),
                    stdout=log,
                    stderr=log,
                    check=True,
                    timeout=240,
                )
            (row,) = [json.loads(s) for s in log_path.read_text().splitlines() if s.startswith("{")]
            row = dict(trial=trial, **row, **summarize(io_report.load(trace), row))
            rows.append(row)
            print(json.dumps(row), flush=True)
            (output / "summary.json").write_text(json.dumps(rows, indent=2) + "\n")
            with (output / "rounds.csv").open("w", newline="") as target:
                writer = csv.DictWriter(target, fieldnames=list(rows[0]))
                writer.writeheader()
                writer.writerows(rows)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prepared", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--clients", type=int, nargs="+", default=[1, 8, 64])
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--mib", type=int, default=256)
    args = parser.parse_args()
    if args.trials < 1 or args.mib < 1 or any(c < 1 or c > args.mib * 4 for c in args.clients):
        parser.error("Require positive trials/MiB and 1 <= clients <= measured SET count")
    run(args.prepared, args.output_dir, args.clients, args.trials, args.mib)
