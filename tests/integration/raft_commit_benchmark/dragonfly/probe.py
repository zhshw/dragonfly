"""Build and run a test-only Dragonfly binary with append and commit timestamps."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import random
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


def prepare(build, work):
    """Compile the two instrumented translation units and relink into the results directory."""
    work.mkdir(parents=True, exist_ok=True)
    destination = work / "probe"
    if destination.exists():
        raise FileExistsError(f"Probe already built in {destination}")
    subprocess.run(["ninja", "-C", build, "dragonfly", "raft_storage"], check=True)
    commands = json.loads((build / "compile_commands.json").read_text())
    with tempfile.TemporaryDirectory(dir=work) as tmp:
        stage = Path(tmp)
        units = [RAFT / "runtime/data_node.cc", RAFT / "quorum/fetch_replication.cc"]
        for unit in units:
            shutil.copyfile(unit, stage / unit.name)
        subprocess.run(
            ["patch", "--batch", "--fuzz=0", "-p1", "-i", HERE / "instrumentation.patch"],
            cwd=stage,
            check=True,
        )
        for unit in units:
            (entry,) = (
                cc for cc in commands if Path(cc["directory"], cc["file"]).resolve() == unit
            )
            args = entry.get("arguments") or shlex.split(entry["command"])
            args[args.index("-c") + 1] = str(stage / unit.name)
            args[args.index("-o") + 1] = str(stage / unit.with_suffix(".o").name)
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
        link.insert(output_arg, str(stage / "fetch_replication.o"))
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
    with tempfile.TemporaryDirectory(dir=work) as tmp:
        pending = Path(tmp) / "raw.csv"
        env = dict(
            os.environ,
            DRAGONFLY_PATH=str(probe / "dragonfly"),
            RAFT_COMMIT_BENCH_VALUES=str(probe / "payloads"),
            RAFT_COMMIT_BENCH_TRACE=str(pending),
        )
        subprocess.run([sys.executable, HERE / "trial.py"], env=env, check=True)
        pending.rename(raw)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_subparsers(dest="action", required=True)
    build = actions.add_parser("prepare")
    build.add_argument("--build-dir", type=Path, required=True)
    build.add_argument("--work-dir", type=Path, required=True)
    trial = actions.add_parser("run")
    trial.add_argument("--work-dir", type=Path, required=True)
    trial.add_argument("--trial", type=int, required=True)
    args = parser.parse_args()
    if args.action == "prepare":
        prepare(args.build_dir.resolve(), args.work_dir.resolve())
    else:
        run(args.work_dir.resolve(), args.trial)
