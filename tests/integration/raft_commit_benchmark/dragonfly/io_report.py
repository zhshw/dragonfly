"""Summarize buffered I/O traces alongside leader-local or client timing trials."""

import argparse
from collections import defaultdict, deque
import csv
import json
import math
from pathlib import Path
from statistics import median


def percentile(values, fraction):
    return sorted(values)[math.ceil(len(values) * fraction) - 1] if values else 0


def load(directory):
    events = []
    for path in directory.glob("*.csv"):
        with path.open() as source:
            for row in csv.DictReader(source):
                event = {k: int(v) for k, v in row.items() if k not in ("stage", "file")}
                event.update(stage=row["stage"], file=row["file"], pid=path.stem.split("-")[0])
                event["node"] = (
                    int(Path(row["file"]).parts[-3])
                    if "__dragonfly_data-10/" in row["file"]
                    else None
                )
                events.append(event)
    return sorted(events, key=lambda event: event["ns"])


def intervals(events):
    starts = {}
    result = []
    for event in events:
        if event["node"] is None:
            continue
        stage = event["stage"]
        if stage in ("segment_begin", "marker_begin", "fsync_issue"):
            kind = stage.split("_")[0]
            starts[event["pid"], event["fd"], kind] = event
        elif stage in ("segment_end", "marker_end", "fsync_complete"):
            kind = stage.split("_")[0]
            begin = starts.pop((event["pid"], event["fd"], kind))
            if kind == "fsync":
                kind = "segment" if event["file"].endswith(".log") else "marker"
            result.append((event["node"], kind, begin["ns"], event["ns"]))
    if starts:
        raise ValueError("Incomplete I/O intervals")
    return result


def summarize_io(events, begin, end, writes):
    measured = [event for event in events if begin <= event["ns"] <= end]
    spans = [span for span in intervals(events) if begin <= span[2] and span[3] <= end]
    appends = [event for event in measured if event["stage"] == "append" and event["node"]]
    for event in measured:
        if event["stage"] == "queued" and (
            event["batches"] > 256 or (event["bytes"] > 8 << 20 and event["batches"] != 1)
        ):
            raise ValueError("Ready queue exceeded its request or byte budget")
    queues = defaultdict(deque)
    waits = []
    for event in events:
        key = event["pid"], event["epoch"], event["end"]
        if event["stage"] == "fetch_queued" and event["bytes"] == 10:
            queues[key].append(event["ns"])
        elif event["stage"] == "fetch_served" and event["bytes"] == 10 and queues[key]:
            queued = queues[key].popleft()
            if begin <= queued <= event["ns"] <= end:
                waits.append((event["ns"] - queued) / 1e6)
    return dict(
        segment_syncs=sum(span[1] == "segment" for span in spans),
        marker_syncs=sum(span[1] == "marker" for span in spans),
        segment_syncs_per_1000=sum(span[1] == "segment" for span in spans) * 1000 / writes,
        marker_syncs_per_1000=sum(span[1] == "marker" for span in spans) * 1000 / writes,
        batches_per_round=median([event["batches"] for event in appends]) if appends else 0,
        queue_bytes_peak=max(
            (event["bytes"] for event in measured if event["stage"] == "queued"), default=0
        ),
        fetch_queue_p95_ms=percentile(waits, 0.95),
    )


def probe(work, trial):
    with (work / f"raw-{trial}.csv").open() as source:
        samples = [{k: int(v) for k, v in row.items()} for row in csv.DictReader(source)]
    if len(samples) != 288:
        raise ValueError("Expected 32 warmup and 256 measured writes")
    samples = samples[32:]
    events = load(work / f"io-{trial}")
    spans = intervals(events)
    overlaps = []
    for row in samples:
        if not row["append_ns"] < row["durable_ns"] <= row["hw_ns"]:
            raise ValueError("Commit preceded local durability")
        active = [s for s in spans if row["append_ns"] <= s[2] and s[3] <= row["hw_ns"]]
        leader = [s for s in active if s[0] == row["leader"]]
        follower = [s for s in active if s[0] != row["leader"]]
        overlap = sum(max(0, min(a[3], b[3]) - max(a[2], b[2])) for a in leader for b in follower)
        overlaps.append(overlap / 1e6)
    begin, end = samples[0]["append_ns"], samples[-1]["hw_ns"]
    latency = [(row["hw_ns"] - row["append_ns"]) / 1e6 for row in samples]
    return dict(
        kind="probe",
        trial=trial,
        p50_ms=median(latency),
        p95_ms=percentile(latency, 0.95),
        mib_per_s=64 * 1e9 / (end - begin),
        overlap_p50_ms=median(overlaps),
        overlap_writes=sum(value > 0 for value in overlaps),
        **summarize_io(events, begin, end, 256),
    )


def client(work, name):
    rows = [
        json.loads(line)
        for line in (work / f"{name}.log").read_text().splitlines()
        if line.startswith("{")
    ]
    if len(rows) != 1:
        raise ValueError("Expected exactly one client benchmark result")
    row = rows[0]
    return dict(
        kind="client",
        name=name,
        **row,
        **summarize_io(
            load(work / name), row["measure_start_ns"], row["measure_end_ns"], row["keys"]
        ),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("work", type=Path)
    parser.add_argument("--trial", type=int)
    parser.add_argument("--client")
    args = parser.parse_args()
    print(
        json.dumps(client(args.work, args.client) if args.client else probe(args.work, args.trial))
    )
