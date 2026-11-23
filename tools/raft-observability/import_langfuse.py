"""Export completed diagnostic spans via Langfuse v4 OTLP/HTTP JSON, then verify by ID."""

import argparse
import base64
from collections import defaultdict
from datetime import datetime, timezone
import json
from pathlib import Path
import time
import urllib.parse
import urllib.request
import uuid


def import_run(run, env_file, url, artifacts_url, per_stage):
    credentials = dict(
        line.split("=", 1) for line in env_file.read_text().splitlines() if "=" in line
    )
    auth = base64.b64encode(
        f"{credentials['LANGFUSE_PUBLIC_KEY']}:{credentials['LANGFUSE_SECRET_KEY']}".encode()
    ).decode()

    def request(path, data=None):
        req = urllib.request.Request(
            url.rstrip("/") + path,
            data=json.dumps(data).encode() if data is not None else None,
            headers={
                "Authorization": "Basic " + auth,
                "Content-Type": "application/json",
                "x-langfuse-ingestion-version": "4",
            },
        )
        with urllib.request.urlopen(req, timeout=60) as response:
            content = response.read()
            return json.loads(content) if content else {}

    summary = json.loads((run / "report/summary.json").read_text())
    metadata, result = summary["metadata"], summary["result"]
    events = json.loads((run / "report/spans.json").read_text())
    trace_id = uuid.uuid5(uuid.NAMESPACE_URL, f"{run}:{result['measure_start_ns']}").hex
    root_id = uuid.uuid5(uuid.UUID(trace_id), "root").hex[:16]
    name = f"replication/{metadata['mode']}/{run.name}"
    begin, end = result["measure_start_ns"], result["replication_end_ns"]
    clock_offset = metadata["wall_offset_ns"]

    def timestamp(ns):
        return datetime.fromtimestamp((ns + clock_offset) / 1e9, timezone.utc).isoformat()

    def attribute(key, value):
        if not isinstance(value, str):
            value = json.dumps(value)
        return dict(key=key, value=dict(stringValue=value))

    common = {
        "langfuse.trace.name": name,
        "langfuse.session.id": "dragonfly-replication-observability",
        "langfuse.environment": "performance",
        "langfuse.observation.type": "span",
        "langfuse.trace.metadata.mode": metadata["mode"],
        "langfuse.trace.metadata.binary_sha256": metadata["binary_sha256"],
        "langfuse.trace.metadata.flamegraphs": f"{artifacts_url.rstrip('/')}/{run.name}/index.html",
    }
    spans = []

    def emit(span_id, parent, span_name, start, stop, details, extra=None):
        attrs = dict(common)
        attrs.update({"langfuse.observation.metadata." + k: v for k, v in details.items()})
        attrs.update(extra or {})
        span = dict(
            traceId=trace_id,
            spanId=span_id,
            name=span_name,
            kind=1,
            startTimeUnixNano=str(start + clock_offset),
            endTimeUnixNano=str(stop + clock_offset),
            attributes=[attribute(k, v) for k, v in attrs.items()],
            status=dict(code=1),
        )
        if parent:
            span["parentSpanId"] = parent
        spans.append(span)

    emit(
        root_id,
        None,
        name,
        begin,
        end,
        dict(
            full_span_count=len(events),
            selection=f"Longest {per_stage} spans per node/stage",
            full_timeline=f"{artifacts_url.rstrip('/')}/{run.name}/timeline.json",
            warning="Durations include waits; overlapping spans are not additive",
        ),
        {
            "langfuse.observation.input": dict(
                clients=1, pipeline=64, payload_mib=result["payload_mib"], value_kib=256
            ),
            "langfuse.observation.output": result,
        },
    )
    parents = {}
    for node in [*metadata["nodes"], "client"]:
        parent = uuid.uuid5(uuid.UUID(trace_id), "node-" + node).hex[:16]
        parents[node] = parent
        role = metadata["nodes"].get(node, {}).get("role", "client")
        emit(
            parent,
            root_id,
            f"{role} / node {node}",
            begin,
            end,
            dict(container=True, stages=[s for s in summary["stages"] if s["node"] == node]),
        )
    groups = defaultdict(list)
    for event in events:
        groups[(event["node"], event["stage"])].append(event)
    for (node, stage), entries in sorted(groups.items()):
        selected = sorted(entries, key=lambda e: e["end_ns"] - e["start_ns"], reverse=True)[
            :per_stage
        ]
        for index, event in enumerate(selected):
            span_id = uuid.uuid5(uuid.UUID(trace_id), f"{node}/{stage}/{index}").hex[:16]
            emit(
                span_id,
                parents[node],
                stage,
                event["start_ns"],
                event["end_ns"],
                dict(
                    tid=event["tid"],
                    arg0=event["arg0"],
                    arg1=event["arg1"],
                    stage_total=len(entries),
                    stage_selected=len(selected),
                ),
            )

    def observed_ids():
        params = dict(
            traceId=trace_id,
            fromStartTime=timestamp(begin - 1_000_000_000),
            toStartTime=timestamp(end + 1_000_000_000),
            limit=1000,
            fields="core",
        )
        ids = set()
        while True:
            response = request("/api/public/v2/observations?" + urllib.parse.urlencode(params))
            ids.update(row["id"] for row in response["data"])
            cursor = response.get("meta", {}).get("cursor")
            if not cursor:
                return ids
            params["cursor"] = cursor

    # V4 spans are immutable. An explicit rerun verifies existing IDs before sending missing ones.
    existing = observed_ids()
    missing = [span for span in spans if span["spanId"] not in existing]
    for offset in range(0, len(missing), 250):
        payload = dict(
            resourceSpans=[
                dict(
                    resource=dict(
                        attributes=[attribute("service.name", "dragonfly-replication-profile")]
                    ),
                    scopeSpans=[
                        dict(
                            scope=dict(name="dragonfly.raft.observe", version="1"),
                            spans=missing[offset : offset + 250],
                        )
                    ],
                )
            ]
        )
        response = request("/api/public/otel/v1/traces", payload)
        rejected = response.get("partialSuccess", {}).get("rejectedSpans", 0)
        if int(rejected):
            raise RuntimeError(f"Langfuse rejected {rejected} spans")
    expected = {span["spanId"] for span in spans}
    for _ in range(20):
        verified = observed_ids()
        if expected <= verified:
            break
        time.sleep(3)
    else:
        raise RuntimeError(
            f"Langfuse accepted spans; {len(expected - verified)} are not readable yet"
        )
    outcome = dict(
        trace_id=trace_id,
        verified_observations=len(expected),
        full_spans=len(events),
        url=url.rstrip("/") + "/project/dragonfly-performance/traces/" + trace_id,
    )
    (run / "report/langfuse.json").write_text(json.dumps(outcome, indent=2) + "\n")
    print(json.dumps(outcome))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, required=True)
    parser.add_argument(
        "--env-file", type=Path, default=Path("/data/dragonfly-observability/config/langfuse.env")
    )
    parser.add_argument("--url", default="http://172.31.23.2:3007")
    parser.add_argument("--artifacts-url", default="http://172.31.23.2:3137")
    parser.add_argument("--per-stage", type=int, default=50)
    args = parser.parse_args()
    import_run(args.run.resolve(), args.env_file, args.url, args.artifacts_url, args.per_stage)
