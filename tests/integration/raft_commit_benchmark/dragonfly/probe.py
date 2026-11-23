"""Build and run a test-only Dragonfly binary with append and commit timestamps."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import random
import re
import shlex
import shutil
import subprocess
import sys
import tempfile

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
RAFT = REPO / "src/server/cluster/raft"


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def prepare(build, work, faults=False, drain_mib=None, fetch_mib=None, phases=False):
    """Compile instrumented units and relink into the results directory."""
    if any(value is not None and not 1 <= value <= 8 for value in (drain_mib, fetch_mib)):
        raise ValueError("Experimental drain and Fetch budgets must be between 1 and 8 MiB")
    work.mkdir(parents=True, exist_ok=True)
    destination = work / "probe"
    if destination.exists():
        raise FileExistsError(f"Probe already built in {destination}")
    subprocess.run(["ninja", "-C", build, "dragonfly", "raft_storage"], check=True)
    commands = json.loads((build / "compile_commands.json").read_text())
    with tempfile.TemporaryDirectory(dir=work) as tmp:
        stage = Path(tmp)
        units = [
            RAFT / "runtime/data_node.cc",
            RAFT / "quorum/fetch_replication.cc",
            RAFT / "log/partition_log.cc",
            RAFT / "runtime/group.cc",
        ]
        folly_unit = RAFT / "storage/folly_io.cc"
        if any(Path(cc["directory"], cc["file"]).resolve() == folly_unit for cc in commands):
            units.append(folly_unit)
        for unit in units:
            shutil.copyfile(unit, stage / unit.name)
        subprocess.run(
            ["patch", "--batch", "--fuzz=0", "-p1", "-i", HERE / "instrumentation.patch"],
            cwd=stage,
            check=True,
        )
        subprocess.run(
            ["patch", "--batch", "--fuzz=0", "-p1", "-i", HERE / "io_trace.patch"],
            cwd=stage,
            check=True,
        )
        if folly_unit in units:
            subprocess.run(
                ["patch", "--batch", "--fuzz=0", "-p1", "-i", HERE / "folly_io_trace.patch"],
                cwd=stage,
                check=True,
            )
        if phases:
            subprocess.run(
                ["patch", "--batch", "--fuzz=0", "-p1", "-i", HERE / "phase_trace.patch"],
                cwd=stage,
                check=True,
            )
        replacements = []
        if drain_mib is not None:
            replacements.append(
                (
                    "data_node.cc",
                    r"batch.byte_budget =[^;]+;",
                    f"batch.byte_budget = {drain_mib}U << 20;",
                )
            )
        if fetch_mib is not None:
            replacements.append(
                (
                    "group.cc",
                    r"constexpr int32_t kMaxDataFetchBytes = \d+ << 20;",
                    f"constexpr int32_t kMaxDataFetchBytes = {fetch_mib} << 20;",
                )
            )
        for name, before, after in replacements:
            path = stage / name
            contents = path.read_text()
            contents, count = re.subn(before, after, contents)
            if count != 1:
                raise ValueError(f"Expected one experimental budget site in {name}")
            path.write_text(contents)
        (stage / "experiment.json").write_text(
            json.dumps(
                dict(drain_mib=drain_mib, fetch_mib=fetch_mib, faults=faults)
                | ({"phases": True} if phases else {}),
                indent=2,
            )
            + "\n"
        )
        for unit in units:
            (entry,) = (
                cc for cc in commands if Path(cc["directory"], cc["file"]).resolve() == unit
            )
            args = entry.get("arguments") or shlex.split(entry["command"])
            args[args.index("-c") + 1] = str(stage / unit.name)
            args[args.index("-o") + 1] = str(stage / unit.with_suffix(".o").name)
            if faults:
                args.append("-DRAFT_IO_FAULTS")
            subprocess.run([*args, "-iquote", HERE], cwd=entry["directory"], check=True)

        # CMake wraps its link command in ": && ... && :". Keep the same flags and libraries.
        lines = subprocess.check_output(
            ["ninja", "-t", "commands", "dragonfly"], cwd=build, text=True
        ).splitlines()
        link = shlex.split(lines[-1])
        if link[:2] == [":", "&&"]:
            link = link[2:]
        if "&&" in link:
            link = link[: link.index("&&")]
        (data_node_object,) = (arg for arg in link if arg.endswith("/runtime/data_node.cc.o"))

        # data_node is in the executable. Supply fetch_replication before the static Raft archive
        # so the linker uses the instrumented definition instead of extracting its normal member.
        link[link.index(data_node_object)] = str(stage / "data_node.o")
        output_arg = link.index("-o")
        link[output_arg + 1] = str(stage / "dragonfly")
        for unit in units[1:]:
            link.insert(output_arg, str(stage / unit.with_suffix(".o").name))
        subprocess.run(link, cwd=build, check=True)
        (stage / "raft_storage").symlink_to(build / "raft_storage")
        rng = random.Random(42)
        with (stage / "payloads").open("wb") as payloads:
            for _ in range(32 + 256):
                payloads.write(rng.randbytes(256 << 10))
        files = [
            ("baseline/dragonfly", build / "dragonfly"),
            ("probe/dragonfly", stage / "dragonfly"),
        ]
        for name in ("log_files.cc", "log_files.h", "partition_log.cc", "partition_log.h"):
            path = RAFT / "log" / name
            files.append((path.relative_to(REPO), path))
        for path in units:
            files.append((f"probe/{path.name}", stage / path.name))
            if path.name != "partition_log.cc":
                files.append((path.relative_to(REPO), path))
        for name in (
            "trace.h",
            "io_trace.h",
            "io_faults.h",
            "instrumentation.patch",
            "io_trace.patch",
            "folly_io_trace.patch",
            "phase_trace.patch",
        ):
            path = HERE / name
            files.append((path.relative_to(REPO), path))
        (stage / "sources.sha256").write_text(
            "".join(f"{sha256(path)}  {label}\n" for label, path in files)
        )
        stage.rename(destination)
        (destination / "sources.sha256").replace(work / "sources.sha256")


def run(work, trial):
    probe = work / "probe"
    raw = work / f"raw-{trial}.csv"
    if raw.exists():
        raise FileExistsError(f"Trial already exists: {raw}")
    trace_dir = work / f"io-{trial}"
    trace_dir.mkdir()
    with tempfile.TemporaryDirectory(dir=work) as tmp:
        pending = Path(tmp) / "raw.csv"
        env = dict(
            os.environ,
            RAFT_TEST_DATA_FLAGS=os.getenv(
                "RAFT_TEST_DATA_FLAGS", "--raft_async_io=true --raft_drain_writes=true"
            ),
            DRAGONFLY_PATH=str(probe / "dragonfly"),
            RAFT_COMMIT_BENCH_VALUES=str(probe / "payloads"),
            RAFT_COMMIT_BENCH_TRACE=str(pending),
            RAFT_IO_TRACE_DIR=str(trace_dir),
            TMPDIR=str(Path(tmp)),
        )
        subprocess.run([sys.executable, HERE / "trial.py"], env=env, check=True)
        pending.rename(raw)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_subparsers(dest="action", required=True)
    build = actions.add_parser("prepare")
    build.add_argument("--build-dir", type=Path, required=True)
    build.add_argument("--faults", action="store_true")
    build.add_argument("--phases", action="store_true")
    build.add_argument("--drain-mib", type=int, choices=(1, 4, 8))
    build.add_argument("--fetch-mib", type=int, choices=(1, 4, 8))
    build.add_argument("--work-dir", type=Path, required=True)
    trial = actions.add_parser("run")
    trial.add_argument("--work-dir", type=Path, required=True)
    trial.add_argument("--trial", type=int, required=True)
    args = parser.parse_args()
    if args.action == "prepare":
        prepare(
            args.build_dir.resolve(),
            args.work_dir.resolve(),
            args.faults,
            args.drain_mib,
            args.fetch_mib,
            args.phases,
        )
    else:
        run(args.work_dir.resolve(), args.trial)
