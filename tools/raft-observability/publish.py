"""Publish diagnostic reports to the persistent remote flamegraph service."""

import argparse
import html
import json
from pathlib import Path
import shutil


def publish(runs, destination):
    if not destination.is_dir():
        raise FileNotFoundError("Start setup_host.sh and mount the data disk first")
    for run in runs:
        source = run / "report"
        if not (source / "index.html").is_file():
            raise FileNotFoundError(f"Run report.py first: {run}")
        target = destination / run.name
        shutil.copytree(source, target, dirs_exist_ok=True)

        # Go prepares reports in a private temporary directory. Nginx serves the published copy.
        target.chmod(0o755)
    page = [
        "<!doctype html><meta charset='utf-8'><title>Dragonfly 复制性能分析</title>",
        "<style>body{font:17px system-ui;max-width:1000px;margin:50px auto;padding:0 20px;line-height:1.8}"
        "table{width:100%;border-collapse:collapse}td,th{padding:12px;text-align:left;border-bottom:1px solid #ddd}</style>",
        "<h1>Dragonfly 复制性能分析</h1>",
        "<p>raft.memory 与原 cluster：1 连接，pipeline 64，256 KiB/value。每轮保留离线副本恢复和逐值读回校验。</p>",
        "<p><a href='http://172.31.23.2:3007'>Langfuse Trace</a> · "
        "点击报告查看可缩放、可搜索的 CPU 火焰图，以及阶段等待统计和完整时间线。</p>",
        "<p>下表为插桩诊断值；正式吞吐结论采用仓库 benchmark 文档的三轮未插桩结果。</p>",
        "<table><tr><th>报告</th><th>模式</th><th>SET + WAIT 1 MiB/s</th><th>Langfuse</th></tr>",
    ]
    for summary_path in sorted(destination.glob("*/summary.json")):
        report = summary_path.parent
        summary = json.loads(summary_path.read_text())
        trace = report / "langfuse.json"
        trace_link = ""
        if trace.exists():
            trace_link = (
                "<a href='"
                + html.escape(json.loads(trace.read_text())["url"], quote=True)
                + "'>查看 trace</a>"
            )
        page.append(
            f"<tr><td><a href='{html.escape(report.name, quote=True)}/index.html'>{html.escape(report.name)}</a></td>"
            f"<td>{html.escape(summary['metadata']['mode'])}</td>"
            f"<td>{summary['result']['replication_mib_per_s']}</td><td>{trace_link}</td></tr>"
        )
    page.append("</table>")
    temporary = destination / "index.html.tmp"
    temporary.write_text("\n".join(page) + "\n")
    temporary.replace(destination / "index.html")
    print("Published: http://172.31.23.2:3137/")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, action="append", required=True)
    parser.add_argument(
        "--destination", type=Path, default=Path("/data/dragonfly-observability/state/artifacts")
    )
    args = parser.parse_args()
    publish([run.resolve() for run in args.run], args.destination)
