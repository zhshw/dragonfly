"""Compare original cluster replication and async Raft through replica confirmation."""

import argparse
from pathlib import Path

import library_compare


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--clients", type=int, nargs="+", default=[1, 8, 64])
    parser.add_argument("--mib", type=int, default=256)
    parser.add_argument("--warmup", type=int, default=64)
    args = parser.parse_args()
    if args.mib < 1 or args.warmup < 0 or any(c < 1 or c > args.mib * 4 for c in args.clients):
        parser.error("Require positive MiB/clients, clients <= SET count and nonnegative warmup")
    library_compare.run(
        args.build_dir,
        args.build_dir,
        args.output_dir,
        args.clients,
        args.mib,
        args.warmup,
        labels=("cluster", "raft-async"),
        modes=("yes", "raft"),
    )
