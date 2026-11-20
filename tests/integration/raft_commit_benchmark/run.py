"""Measure local log append to majority commit in Dragonfly and Kafka KRaft."""

import argparse
import subprocess
import sys
import tempfile
from pathlib import Path

from report import RUN_ORDER, write_results


HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]


def run_probe(output, engine, action, *args, log_name):
    log = output / f"{log_name}.log"
    command = [
        sys.executable,
        "-u",
        str(HERE / engine / "probe.py"),
        action,
        "--work-dir",
        str(output / engine),
        *map(str, args),
    ]
    print(f"{log_name} ... {log}", flush=True)
    with log.open("x") as stream:
        result = subprocess.run(command, stdout=stream, stderr=subprocess.STDOUT)
    if result.returncode:
        print(log.read_text(errors="replace")[-4000:], file=sys.stderr)
        raise SystemExit(f"{log_name} failed (exit {result.returncode}), see {log}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", type=Path, default=REPO / "build-dbg")
    parser.add_argument("--kafka-dir", type=Path, default=REPO.parent / "kafka")
    parser.add_argument("--kafka-libs", type=Path, help="Kafka distribution libs directory")
    parser.add_argument("--scala-compiler", type=Path, help="Matching scala-compiler jar")
    parser.add_argument("--output-dir", type=Path, help="New directory for probes, logs and CSVs")
    args = parser.parse_args()
    if args.output_dir is None:
        output = Path(tempfile.mkdtemp(prefix="raft-commit-"))
    else:
        output = args.output_dir.resolve()
        try:
            output.mkdir(parents=True)
        except FileExistsError:
            parser.error(f"{output} already exists, choose a new output directory")

    run_probe(
        output,
        "dragonfly",
        "prepare",
        "--build-dir",
        args.build_dir.resolve(),
        log_name="prepare-dragonfly",
    )
    kafka_options = ["--kafka-dir", args.kafka_dir.resolve()]
    if args.kafka_libs:
        kafka_options.extend(["--libs-dir", args.kafka_libs.resolve()])
    if args.scala_compiler:
        kafka_options.extend(["--scala-compiler", args.scala_compiler.resolve()])
    run_probe(output, "kraft", "prepare", *kafka_options, log_name="prepare-kraft")
    for engine, trial in RUN_ORDER:
        run_probe(output, engine, "run", "--trial", trial, log_name=f"{engine}-{trial}")
    write_results(output)


if __name__ == "__main__":
    main()
