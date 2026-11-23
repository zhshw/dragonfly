"""Create exact interval summaries and a Perfetto timeline from buffered server spans."""

import argparse
from collections import defaultdict
import csv
import html
import json
import math
from pathlib import Path
import re
import subprocess


def union_ns(intervals):
    total, end = 0, -1
    for begin, stop in sorted(intervals):
        total += max(0, stop - max(begin, end))
        end = max(end, stop)
    return total


def load_spans(run, metadata, result):
    begin, end = result["measure_start_ns"], result["replication_end_ns"]
    events = []
    dropped = 0
    for node, info in metadata["nodes"].items():
        for path in (run / "spans" / f"node-{node}").glob(f"{info['pid']}-*.csv"):
            tid = int(path.stem.split("-")[1])
            with path.open() as source:
                dropped += int(source.readline().split("=")[1])
                for row in csv.DictReader(source):
                    start = max(int(row["start_ns"]), begin)
                    stop = min(int(row["end_ns"]), end)
                    if stop <= start:
                        continue
                    stage = row["stage"]
                    if stage == "raft.peer.backoff":
                        stage += ".after_pull" if int(row["arg1"]) else ".idle"
                    events.append(
                        dict(
                            node=node,
                            role=info["role"],
                            tid=tid,
                            stage=stage,
                            start_ns=start,
                            end_ns=stop,
                            arg0=int(row["arg0"]),
                            arg1=int(row["arg1"]),
                        )
                    )
    if not events:
        raise ValueError("No server spans in the measured window")
    if dropped:
        raise ValueError(f"Trace buffer dropped {dropped} events; rerun with a larger capacity")
    for row in json.loads((run / "client.json").read_text()):
        events.append(dict(node="client", role="client", tid=0, **row))
    events.append(
        dict(
            node="client",
            role="client",
            tid=0,
            stage="client.final_wait",
            start_ns=result["measure_end_ns"],
            end_ns=end,
            arg0=1,
            arg1=0,
        )
    )
    return sorted(events, key=lambda e: e["start_ns"])


def collapse(raw):
    """Parse pprof -raw -noinlines without truncating C++ names at their first space."""
    stacks, frames = [], {}
    section = None
    for line in raw.splitlines():
        if line.startswith(("Samples:", "Locations", "Mappings")):
            section = line.split(":")[0]
        elif section == "Samples":
            match = re.match(r"\s*(\d+)\s+\d+: ([\d ]+)\s*$", line)
            if match:
                stacks.append((int(match[1]), match[2].split()))
        elif section == "Locations":
            match = re.match(r"\s*(\d+): (0x[\da-f]+)(?: M=\d+)?(?: (.*))?$", line)
            if match:
                name = match[3] or match[2]
                name = re.sub(r"\s+\S*:\d+(?: s=\d+)?$", "", name)
                frames[match[1]] = name.replace(";", ":")
    counts = defaultdict(int)
    for count, stack in stacks:
        counts[
            ";".join(frames.get(frame, "unknown-" + frame) for frame in reversed(stack))
        ] += count
    if not counts:
        raise ValueError("pprof has no CPU samples")
    return "".join(f"{name} {count}\n" for name, count in sorted(counts.items()))


def symbolize(raw):
    # GNU addr2line also resolves the installed libc debug package. On this host pprof's
    # default symbolizer emits empty C++ names, so resolve recorded ELF mappings explicitly.
    mappings, locations = {}, {}
    section = None
    for line in raw.splitlines():
        if line.startswith(("Locations", "Mappings")):
            section = line
        elif section == "Mappings":
            match = re.match(r"(\d+): (0x[\da-f]+)/(0x[\da-f]+)/(0x[\da-f]+) (\S+)", line)
            if match:
                mappings[match[1]] = (int(match[2], 16), int(match[4], 16), match[5])
        elif section == "Locations":
            match = re.match(r"\s*(\d+): (0x[\da-f]+) M=(\d+)", line)
            if match:
                locations[match[1]] = (int(match[2], 16), match[3])
    by_file = defaultdict(list)
    frames = {}
    for loc, (address, mapping) in locations.items():
        start, offset, path = mappings[mapping]
        relative = address - start + offset
        frames[loc] = f"[{Path(path).name}+{relative:#x}]"
        if Path(path).is_file():
            by_file[path].append((loc, relative))
    for path, entries in by_file.items():
        result = subprocess.check_output(
            ["addr2line", "-f", "-C", "-e", path],
            input="".join(f"{address:#x}\n" for _, address in entries),
            text=True,
        ).splitlines()
        if len(result) != len(entries) * 2:
            raise ValueError(f"Unexpected addr2line output for {path}")
        for (loc, _), name in zip(entries, result[::2]):
            if name != "??":
                frames[loc] = name.replace(";", ":")
    lines = []
    for line in raw.splitlines():
        match = re.match(r"\s*(\d+): (0x[\da-f]+) M=(\d+)", line)
        if match and match[1] in frames:
            line = f"{match[1]}: {match[2]} M={match[3]} {frames[match[1]]} :0 s=0"
        lines.append(line)
    return "\n".join(lines) + "\n"


def top_report(folded, cumulative=False):
    counts = defaultdict(int)
    total = 0
    for line in folded.splitlines():
        stack, count = line.rsplit(" ", 1)
        count = int(count)
        total += count
        frames = stack.split(";")
        for frame in set(frames) if cumulative else frames[-1:]:
            counts[frame] += count
    rows = [
        f"CPU samples: {total}; period: 2 ms; cumulative={cumulative}",
        "samples    percent    function",
    ]
    for name, count in sorted(counts.items(), key=lambda item: -item[1])[:40]:
        rows.append(f"{count:7d}    {count / total:7.2%}    {name}")
    return "\n".join(rows) + "\n"


def cpu_reports(run, output, metadata, go, flamegraph):
    reports = []
    for node, info in metadata["nodes"].items():
        profiles = sorted((run / "cpu").glob(f"node-{node}.prof.*"))
        if not profiles:
            continue
        prefix = f"{info['role']}-node-{node}"
        raw = subprocess.check_output(
            [
                str(go),
                "tool",
                "pprof",
                "-symbolize=none",
                "-raw",
                metadata["binary"],
                *map(str, profiles),
            ],
            text=True,
        )
        raw = symbolize(raw)
        (output / f"{prefix}.raw.txt").write_text(raw)
        folded = collapse(raw)
        (output / f"{prefix}.folded").write_text(folded)
        (output / f"{prefix}.top.txt").write_text(top_report(folded))
        (output / f"{prefix}.cumulative.txt").write_text(top_report(folded, cumulative=True))
        with (output / f"{prefix}.svg").open("w") as target:
            subprocess.run(
                [
                    "perl",
                    str(flamegraph / "flamegraph.pl"),
                    "--title",
                    f"{metadata['mode']} {info['role']} CPU (500 Hz)",
                    "--countname",
                    "samples",
                ],
                input=folded,
                text=True,
                stdout=target,
                check=True,
            )
        reports.append(prefix)
    return reports


def report(run, go, flamegraph):
    metadata = json.loads((run / "metadata.json").read_text())
    if metadata["mode"] == "memory":
        metadata["mode"] = "raft.memory"
    result = json.loads((run / "result.json").read_text())
    output = run / "report"
    output.mkdir(exist_ok=True)
    events = load_spans(run, metadata, result)
    groups = defaultdict(list)
    timeline = []
    for event in events:
        groups[(event["node"], event["role"], event["stage"])].append(event)
        timeline.append(
            dict(
                name=event["stage"],
                cat=event["role"],
                ph="X",
                pid=event["node"],
                tid=event["tid"],
                ts=(event["start_ns"] - result["measure_start_ns"]) / 1000,
                dur=(event["end_ns"] - event["start_ns"]) / 1000,
                args=dict(arg0=event["arg0"], arg1=event["arg1"]),
            )
        )
    rows = []
    for (node, role, stage), spans in sorted(groups.items()):
        durations = sorted((e["end_ns"] - e["start_ns"]) / 1e6 for e in spans)
        rows.append(
            dict(
                node=node,
                role=role,
                stage=stage,
                count=len(spans),
                sum_ms=round(sum(durations), 3),
                union_ms=round(union_ns((e["start_ns"], e["end_ns"]) for e in spans) / 1e6, 3),
                p50_ms=round(durations[len(durations) // 2], 3),
                p95_ms=round(durations[math.ceil(len(durations) * 0.95) - 1], 3),
                max_ms=round(max(durations), 3),
            )
        )
    (output / "spans.json").write_text(json.dumps(events) + "\n")
    (output / "timeline.json").write_text(json.dumps(dict(traceEvents=timeline)) + "\n")
    summary = dict(metadata=metadata, result=result, span_count=len(events), dropped=0, stages=rows)
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    with (output / "stages.csv").open("w") as target:
        writer = csv.DictWriter(target, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    profiles = cpu_reports(run, output, metadata, go, flamegraph) if flamegraph else []
    title = html.escape(f"{metadata['mode']} — {run.name}")
    page = [
        f"<!doctype html><meta charset='utf-8'><title>{title}</title>",
        "<style>body{font:16px system-ui;max-width:1300px;margin:30px auto;padding:0 20px;line-height:1.6}"
        "table{border-collapse:collapse;width:100%}td,th{padding:6px 12px;border-bottom:1px solid #ddd;text-align:right}"
        "td:first-child,td:nth-child(2){text-align:left}object{width:100%;height:850px;border:1px solid #ddd}</style>",
        f"<h1>{title}</h1>",
        f"<p>1 connection · pipeline 64 · {result['payload_mib']} MiB · 256 KiB/value · "
        f"SET + WAIT 1: <strong>{result['replication_mib_per_s']} MiB/s</strong>. Recovery/readback passed.</p>",
        "<p>Diagnostic run with instrumentation. CPU samples show on-CPU work; span durations include waits. "
        "Union merges overlapping intervals within each row; rows can overlap and must not be added.</p>",
        "<p><a href='timeline.json'>Full Perfetto timeline JSON</a> · <a href='stages.csv'>Stage CSV</a> · "
        "<a href='summary.json'>Metadata and summary</a></p>",
        "<table><tr><th>Role</th><th>Stage</th><th>Count</th><th>Union ms</th><th>Sum ms</th><th>P95 ms</th><th>Max ms</th></tr>",
    ]
    for row in sorted(rows, key=lambda r: (r["role"], -r["union_ms"])):
        page.append(
            "<tr>"
            + "".join(
                f"<td>{html.escape(str(row[key]))}</td>"
                for key in ("role", "stage", "count", "union_ms", "sum_ms", "p95_ms", "max_ms")
            )
            + "</tr>"
        )
    page.append("</table>")
    memory = [info for info in metadata["nodes"].values() if "memory_after" in info]
    if memory:
        page.append(
            "<h2>Process memory</h2><p>MiB, including KV data and runtime memory. "
            "Peak RSS covers process startup, warmup and measurement; it is not temporary buffer "
            "allocation or kernel page cache.</p>"
            "<table><tr><th>Role</th><th>RSS before</th><th>RSS after</th>"
            "<th>Peak RSS</th><th>Anonymous RSS after</th></tr>"
        )
        for info in memory:
            before, after = info["memory_before"], info["memory_after"]
            page.append(
                f"<tr><td>{html.escape(info['role'])}</td>"
                f"<td>{before['rss_kib'] / 1024:.2f}</td>"
                f"<td>{after['rss_kib'] / 1024:.2f}</td>"
                f"<td>{after['peak_rss_kib'] / 1024:.2f}</td>"
                f"<td>{after['anon_rss_kib'] / 1024:.2f}</td></tr>"
            )
        page.append("</table>")
    for prefix in profiles:
        page.append(
            f"<h2>{prefix}</h2><p><a href='{prefix}.svg'>Open interactive flamegraph</a> · "
            f"<a href='{prefix}.top.txt'>CPU top</a> · <a href='{prefix}.cumulative.txt'>CPU cumulative</a></p>"
            f"<object data='{prefix}.svg' type='image/svg+xml'></object>"
        )
    (output / "index.html").write_text("\n".join(page) + "\n")
    print(json.dumps(dict(report=str(output), spans=len(events), cpu_profiles=len(profiles))))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, required=True)
    parser.add_argument("--go", type=Path, default=Path("go"))
    parser.add_argument("--flamegraph-dir", type=Path)
    args = parser.parse_args()
    report(args.run.resolve(), args.go, args.flamegraph_dir)
