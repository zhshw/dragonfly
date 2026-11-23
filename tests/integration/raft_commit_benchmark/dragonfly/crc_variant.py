"""Build a host-only CRC32C diagnostic variant without changing the normal build."""

import argparse
import json
from pathlib import Path
import shlex
import subprocess

import probe


def relink(build, output, target, extra):
    command = subprocess.check_output(
        ["ninja", "-t", "commands", target], cwd=build, text=True
    ).splitlines()[-1]
    args = shlex.split(command)
    if args[:2] == [":", "&&"]:
        args = args[2:]
    if "&&" in args:
        args = args[: args.index("&&")]
    index = args.index("-o")
    args[index + 1] = str(output / target)
    args.insert(index, str(extra))
    subprocess.run(args, cwd=build, check=True)


def run(build, output):
    flags = Path("/proc/cpuinfo").read_text().split("flags", 1)[1].splitlines()[0].split()
    if not {"sse4_2", "pclmulqdq"} <= set(flags):
        raise ValueError("This diagnostic binary requires SSE4.2 and PCLMULQDQ on the test host")
    build, output = build.resolve(), output.resolve()
    output.mkdir()
    targets = (
        "record_batch_test",
        "set_effect_test",
        "partition_log_test",
        "fetch_replication_test",
    )
    subprocess.run(["ninja", "-C", build, "dragonfly", "raft_storage", *targets], check=True)
    commands = json.loads((build / "compile_commands.json").read_text())
    (entry,) = [r for r in commands if r["file"].endswith("/crc_x86_arm_combined.cc")]
    args = entry.get("arguments") or shlex.split(entry["command"])
    obj = output / "crc_accelerated.o"
    args[args.index("-o") + 1] = str(obj)
    args += ["-msse4.2", "-mpclmul"]
    subprocess.run(args, cwd=entry["directory"], check=True)
    for target in ("dragonfly", *targets):
        relink(build, output, target, obj)
    (output / "raft_storage").symlink_to(build / "raft_storage")
    for target in targets:
        with (output / f"{target}.log").open("x") as log:
            subprocess.run([output / target], cwd=build, stdout=log, stderr=log, check=True)

    # Compare checksums across implementations, including empty, unaligned and large inputs.
    source = output / "crc_parity.cc"
    source.write_text(
        """#include <absl/crc/crc32c.h>
#include <cstdint>
#include <iostream>
#include <string>
#include <string_view>
int main() {
  std::string bytes((4 << 20) + 16, '\\0');
  uint32_t state = 42;
  for (char& c : bytes) {
    state ^= state << 13; state ^= state >> 17; state ^= state << 5;
    c = static_cast<char>(state);
  }
  std::cout << static_cast<uint32_t>(absl::ComputeCrc32c("123456789")) << '\\n';
  for (size_t size : {0, 1, 7, 8, 15, 16, 31, 32, 63, 64, 127, 128, 255, 256,
                     1023, 1024, 2048, 4096, 65536, 262144, 1048576, 4194304})
    for (size_t shift = 0; shift < 16; ++shift)
      std::cout << static_cast<uint32_t>(absl::ComputeCrc32c(
          std::string_view(bytes).substr(shift, size))) << '\\n';
}
"""
    )
    include = Path(entry["file"]).parents[3]
    libraries = sorted(build.rglob("libabsl_*.a"))
    outputs = []
    for variant, objects in (("base", []), ("accelerated", [obj])):
        binary = output / f"crc_parity_{variant}"
        subprocess.run(
            [
                "g++",
                "-O2",
                "-std=c++20",
                "-I",
                include,
                source,
                *objects,
                "-Wl,--start-group",
                *libraries,
                "-Wl,--end-group",
                "-pthread",
                "-o",
                binary,
            ],
            check=True,
        )
        outputs.append(subprocess.check_output([binary], text=True))
    if outputs[0] != outputs[1] or outputs[0].splitlines()[0] != str(0xE3069283):
        raise ValueError("CRC32C compatibility check failed")
    (output / "crc-parity.txt").write_text(outputs[0])
    metadata = dict(
        baseline_sha256=probe.sha256(build / "dragonfly"),
        candidate_sha256=probe.sha256(output / "dragonfly"),
        crc_source=str(entry["file"]),
        crc_source_sha256=probe.sha256(Path(entry["file"])),
        crc_compile_command=args,
        changed="Only crc_x86_arm_combined.cc recompiled with -msse4.2 -mpclmul and relinked",
        host_only=True,
        crc_parity_cases=len(outputs[0].splitlines()),
        tests=targets,
    )
    (output / "experiment.json").write_text(json.dumps(metadata, indent=2) + "\n")
    print(json.dumps(metadata), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    run(args.build_dir, args.output_dir)
