"""Validate both probes' traces and report one distribution per fresh cluster."""

import csv
import math
from statistics import median


RUN_ORDER = (
    ("dragonfly", 1),
    ("kraft", 1),
    ("kraft", 2),
    ("dragonfly", 2),
    ("dragonfly", 3),
    ("kraft", 3),
)
LABELS = {"dragonfly": "Dragonfly", "kraft": "KRaft"}
WARMUP = 32
MEASURED = 256
VALUE_BYTES = 256 * 1024
TRACE_FIELDS = (
    "engine",
    "trial",
    "epoch",
    "end_offset",
    "submit_ns",
    "append_ns",
    "durable_ns",
    "hw_ns",
    "listener_ns",
)


def read_trial(path, engine, trial):
    with path.open(newline="") as source:
        samples = [{k: int(v) for k, v in row.items()} for row in csv.DictReader(source)]
    if len(samples) != WARMUP + MEASURED or len({s["epoch"] for s in samples}) != 1:
        raise ValueError(f"{path}: incomplete trace or epoch change")

    is_df = engine == "dragonfly"
    if is_df and len({s["leader"] for s in samples}) != 1:
        raise ValueError(f"{path}: multiple leaders")
    origin = samples[WARMUP]["submit_ns" if is_df else "prepare_ns"]
    rows = []
    last_end = last_listener = None
    for i, sample in enumerate(samples):
        if is_df:
            if not VALUE_BYTES <= sample["wire_bytes"] < VALUE_BYTES + 512:
                raise ValueError(f"{path}: expected a single large SET at sample {i}")
            end, submit, listener = sample["end"], sample["submit_ns"], sample["listener_ns"]
        else:
            expected = (0, i) if i < WARMUP else (1, i - WARMUP)
            if (sample["phase"], sample["ordinal"]) != expected:
                raise ValueError(f"{path}: unexpected phase or ordinal at sample {i}")
            end, submit, listener = (
                sample["offset"] + 1,
                sample["prepare_ns"],
                sample["callback_ns"],
            )

        append, hw = sample["append_ns"], sample["hw_ns"]
        if not submit < append < hw <= listener:
            raise ValueError(f"{path}: invalid append or commit time at offset {end}")
        if is_df and not append < sample["durable_ns"] <= hw:
            raise ValueError(f"{path}: invalid durability timestamp at offset {end}")

        # Kafka commits a readiness marker between the warmup and measurement phases.
        step = 2 if not is_df and i == WARMUP else 1
        if last_end is not None and (end != last_end + step or submit < last_listener):
            raise ValueError(f"{path}: offset gap or overlapping submissions at offset {end}")
        last_end, last_listener = end, listener
        if i >= WARMUP:
            rows.append(
                dict(
                    engine=LABELS[engine],
                    trial=trial,
                    epoch=sample["epoch"],
                    end_offset=end,
                    submit_ns=submit - origin,
                    append_ns=append - origin,
                    durable_ns=sample["durable_ns"] - origin if is_df else "",
                    hw_ns=hw - origin,
                    listener_ns=listener - origin,
                )
            )
    return rows


def percentiles(name, values):
    ordered = sorted(values)
    return {
        f"{name}_p{pct}_ms": ordered[math.ceil(len(ordered) * pct / 100) - 1] / 1e6
        for pct in (50, 95)
    }


def summarize(rows, order):
    first = rows[0]
    elapsed = rows[-1]["hw_ns"] - first["append_ns"]
    commits = [row["hw_ns"] - row["append_ns"] for row in rows]
    gaps = [b["append_ns"] - a["hw_ns"] for a, b in zip(rows, rows[1:])]
    if elapsed != sum(commits) + sum(gaps):
        raise ValueError("Commit times and gaps disagree with the elapsed time")
    prep = [row["append_ns"] - row["submit_ns"] for row in rows[1:]]
    summary = dict(
        run_order=order,
        trial=first["trial"],
        engine=first["engine"],
        **percentiles("append_hw", commits),
        **percentiles("next_append", gaps),
        **percentiles("prepare", prep),
        gap_pct=100 * sum(gaps) / elapsed,
        prepare_share_of_gap_pct=100 * sum(prep) / sum(gaps),
        elapsed_s=elapsed / 1e9,
        mib_per_s=(len(rows) * VALUE_BYTES / 2**20) / (elapsed / 1e9),
    )
    if first["durable_ns"] != "":
        summary.update(
            percentiles("append_durable", [r["durable_ns"] - r["append_ns"] for r in rows])
        )
        summary.update(percentiles("durable_hw", [r["hw_ns"] - r["durable_ns"] for r in rows]))
    return summary


def write_csv(path, fields, rows):
    with path.open("x", newline="") as out:
        writer = csv.DictWriter(out, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def write_results(output):
    traces, rounds = [], []
    for order, (engine, trial) in enumerate(RUN_ORDER, 1):
        rows = read_trial(output / engine / f"raw-{trial}.csv", engine, trial)
        rounds.append(summarize(rows, order))
        traces.extend(rows)
    write_csv(output / "raft-log-timings.csv", TRACE_FIELDS, traces)
    write_csv(output / "rounds.csv", rounds[0].keys(), rounds)
    for label in LABELS.values():
        data = [row for row in rounds if row["engine"] == label]
        p50 = median(row["append_hw_p50_ms"] for row in data)
        p95 = median(row["append_hw_p95_ms"] for row in data)
        print(f"{label} append to HW: P50 {p50:.3f} ms, P95 {p95:.3f} ms")
    print(f"Per-trial results and timestamps: {output}")
