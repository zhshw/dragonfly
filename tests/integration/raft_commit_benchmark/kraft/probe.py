"""Build and run the KRaft half of the single-in-flight append-to-HW benchmark."""

import argparse
import base64
import json
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import tempfile
import time
import uuid
import zipfile
from contextlib import ExitStack

HERE = Path(__file__).resolve().parent
KAFKA_REVISION = "5b648ca9cbeed3cebe553fba86c516b08bcaeb40"
SCALA_VERSION = "2.13.18"
RAFT_PACKAGE = Path("raft/src/main/java/org/apache/kafka/raft")
SCALA_SERVER = Path("core/src/main/scala/kafka/tools/TestRaftServer.scala")
NODES = (1, 2, 3)


def jdk_tool(name):
    home = os.environ.get("JAVA_HOME")
    executable = str(Path(home) / "bin" / name) if home else name
    path = shutil.which(executable)
    if not path:
        raise ValueError(f"Cannot find {name}. Set JAVA_HOME to a JDK 21 installation.")
    return path


def runtime_jars(kafka, libs_dir):
    if libs_dir:
        jars = sorted(libs_dir.glob("*.jar"))
    else:
        jars = sorted((kafka / f"core/build/dependant-libs-{SCALA_VERSION}").glob("*.jar"))
        jars += [
            kafka / "core/build/libs/kafka_2.13-4.5.0-SNAPSHOT.jar",
            kafka / "clients/build/libs/kafka-clients-4.5.0-SNAPSHOT.jar",
        ]
    by_name = {p.name: p for p in jars}
    client = by_name.get("kafka-clients-4.5.0-SNAPSHOT.jar")
    if not client or not all(
        name in by_name and by_name[name].is_file()
        for name in (
            "kafka-clients-4.5.0-SNAPSHOT.jar",
            "kafka-raft-4.5.0-SNAPSHOT.jar",
            "kafka_2.13-4.5.0-SNAPSHOT.jar",
            f"scala-library-{SCALA_VERSION}.jar",
            f"scala-reflect-{SCALA_VERSION}.jar",
        )
    ):
        raise ValueError("Build :core:jar :clients:jar in Kafka or pass the release libs directory")
    with zipfile.ZipFile(client) as jar:
        properties = jar.read("kafka/kafka-version.properties").decode().splitlines()
    if f"commitId={KAFKA_REVISION[:16]}" not in properties:
        raise ValueError(f"Kafka client jar comes from a different revision: {properties}")
    return by_name.values()


def prepare(args):
    work = args.work_dir.resolve()
    kafka = args.kafka_dir.resolve()
    head = subprocess.check_output(["git", "-C", kafka, "rev-parse", "HEAD"], text=True).strip()
    if head != KAFKA_REVISION:
        raise ValueError(f"Probe targets Kafka {KAFKA_REVISION}, checkout is {head}")
    javac = jdk_tool("javac")
    java = jdk_tool("java")
    compiler_version = subprocess.check_output([javac, "-version"], text=True).strip()
    if not compiler_version.startswith("javac 21"):
        raise ValueError(f"Build the KRaft probe with JDK 21 (found {compiler_version})")
    compiler = args.scala_compiler
    if compiler is None:
        cache = Path(os.getenv("GRADLE_USER_HOME", Path.home() / ".gradle"))
        modules = cache / "caches/modules-2/files-2.1/org.scala-lang/scala-compiler" / SCALA_VERSION
        compiler = next(modules.glob(f"*/scala-compiler-{SCALA_VERSION}.jar"), None)
    if not compiler or not compiler.is_file():
        raise ValueError(f"Pass --scala-compiler /path/to/scala-compiler-{SCALA_VERSION}.jar")

    work.mkdir(parents=True, exist_ok=True)
    build = work / "build"
    with tempfile.TemporaryDirectory(dir=work) as tmp:
        staging = Path(tmp) / "build"
        libs = staging / "libs"
        classes = staging / "classes"
        libs.mkdir(parents=True)
        classes.mkdir()
        for jar in runtime_jars(kafka, args.libs_dir):
            shutil.copyfile(jar, libs / jar.name)
        classpath = os.pathsep.join(str(p) for p in sorted(libs.glob("*.jar")))

        # Pin the exact upstream sources even when the supplied checkout has local edits.
        source = Path(tmp) / "source"
        raft_sources = [
            RAFT_PACKAGE / f"{name}.java" for name in ("KafkaRaftClient", "LeaderState")
        ]
        for relative in [*raft_sources, SCALA_SERVER]:
            path = source / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(
                subprocess.check_output(
                    ["git", "-C", kafka, "show", f"{head}:{relative.as_posix()}"]
                )
            )
        # Use a separate Git root so an output directory inside another checkout works as well.
        subprocess.run(["git", "init", "-q", str(source)], check=True)
        subprocess.run(
            ["git", "apply", "--unidiff-zero", HERE / "timing.patch"], cwd=source, check=True
        )
        subprocess.run(
            [
                javac,
                "--release",
                "21",
                "-proc:none",
                "-cp",
                classpath,
                "-d",
                classes,
                HERE / "AlignedProbe.java",
                *(source / path for path in raft_sources),
            ],
            check=True,
        )
        subprocess.run(
            [
                java,
                "-XX:ActiveProcessorCount=2",
                "-Xmx448m",
                "-cp",
                os.pathsep.join((str(compiler.resolve()), classpath)),
                "scala.tools.nsc.Main",
                "-classpath",
                os.pathsep.join((str(classes), classpath)),
                "-d",
                classes,
                source / SCALA_SERVER,
            ],
            check=True,
        )
        (staging / "revision.json").write_text(
            json.dumps({"kafka": head, "javac": compiler_version}) + "\n"
        )
        staging.rename(build)
    print(f"Prepared KRaft {head} in {build}", flush=True)


def ports():
    with ExitStack() as stack:
        sockets = [stack.enter_context(socket.socket()) for _ in NODES]
        for listener in sockets:
            listener.bind(("127.0.0.1", 0))
        return [s.getsockname()[1] for s in sockets]


def launch(node, port, voters, data_dir, run_dir, classpath, java):
    node_dir = data_dir / str(node)
    node_dir.mkdir()
    config = node_dir / "server.properties"
    config.write_text(
        "\n".join(
            (
                "process.roles=controller",
                f"node.id={node}",
                f"controller.quorum.voters={voters}",
                "controller.listener.names=CONTROLLER",
                "listener.security.protocol.map=CONTROLLER:PLAINTEXT,BROKER:PLAINTEXT",
                "inter.broker.listener.name=BROKER",
                f"listeners=CONTROLLER://127.0.0.1:{port}",
                f"log.dirs={node_dir}/data",
                "num.io.threads=2",
                "num.network.threads=1",
                "controller.quorum.append.linger.ms=0",
            )
        )
        + "\n"
    )
    directory_id = base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip("=")
    log_path = run_dir / f"node-{node}.log"
    with log_path.open("w") as log:
        process = subprocess.Popen(
            [
                java,
                "-Xms128m",
                "-Xmx640m",
                "-XX:ActiveProcessorCount=2",
                "-cp",
                classpath,
                "kafka.tools.TestRaftServer",
                "--config",
                config,
                "--replica-directory-id",
                directory_id,
                "--record-size",
                "262144",
            ],
            stdout=log,
            stderr=subprocess.STDOUT,
            env=dict(
                os.environ,
                KRAFT_PROBE_GO=str(run_dir / "go"),
                KRAFT_PROBE_RAW=str(run_dir / "raw.csv"),
                KRAFT_PROBE_NONCE=run_dir.name,
            ),
        )
    return process, log_path


def log_text(node, cluster):
    process, path = cluster[node]
    text = path.read_text(errors="replace")
    if process.poll() is not None:
        raise RuntimeError(f"Node {node} exited: {path}\n{text[-1800:]}")
    return text


def barrier(kind, nodes, cluster):
    """Wait for every live voter to see the same election and the same committed marker."""
    deadline = time.monotonic() + 120
    previous = None
    confirmations = 0
    while time.monotonic() < deadline:
        observed = []
        for node in nodes:
            text = log_text(node, cluster)
            views = re.findall(rf"PROBE_VIEW node={node} leader=(-?\d+) epoch=(\d+)", text)
            marks = re.findall(
                rf"PROBE_MARKER_COMMITTED kind={kind} node={node} epoch=(\d+) offset=(\d+)", text
            )
            if views and marks and views[-1][1] == marks[-1][0]:
                observed.append(tuple(map(int, (*views[-1], marks[-1][1]))))
        state = None
        if len(observed) == len(nodes) and len(set(observed)) == 1 and observed[0][0] in nodes:
            state = observed[0]
        confirmations = confirmations + 1 if state is not None and state == previous else 1
        previous = state
        if state is not None and confirmations >= 3:
            print(f"Committed {kind}: live={nodes} leader,epoch,offset={state}", flush=True)
            return state
        time.sleep(0.15)
    raise TimeoutError(
        f"Quorum did not reach {kind}, see node logs in {cluster[nodes[0]][1].parent}"
    )


def stop_nodes(cluster):
    for process, _ in cluster.values():
        if process.poll() is None:
            process.terminate()
    for process, _ in cluster.values():
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def run(args):
    work = args.work_dir.resolve()
    out = work / f"raw-{args.trial}.csv"
    if out.exists():
        raise ValueError(f"Trial output already exists: {out}")
    build = work / "build"
    classpath = os.pathsep.join((str(build / "classes"), str(build / "libs" / "*")))
    java = jdk_tool("java")
    run_dir = Path(tempfile.mkdtemp(dir=work, prefix=f"trial-{args.trial}-"))
    node_ports = ports()
    voters = ",".join(f"{n}@127.0.0.1:{p}" for n, p in zip(NODES, node_ports))
    cluster = {}
    with tempfile.TemporaryDirectory(dir=run_dir) as data:
        try:
            for node, port in zip(NODES, node_ports):
                cluster[node] = launch(node, port, voters, Path(data), run_dir, classpath, java)
            leader, epoch, initial = barrier("all", NODES, cluster)
            offline = next(n for n in reversed(NODES) if n != leader)
            cluster[offline][0].kill()
            cluster[offline][0].wait()
            live = tuple(n for n in NODES if n != offline)
            (run_dir / "go").touch()
            if barrier("majority", live, cluster) != (leader, epoch, initial + 1):
                raise ValueError("Election or offset changed while stopping one voter")
            time.sleep(5)
            (run_dir / "warmup").touch()
            warmed = initial + 1 + 32 + 1
            if barrier("warmed", live, cluster) != (leader, epoch, warmed):
                raise ValueError("Warmup must commit 32 records and one marker in the same epoch")
            (run_dir / "timed").touch()

            follower = next(n for n in live if n != leader)
            verified = f"PROBE_FOLLOWER_VERIFIED node={follower} n=288 offset={warmed + 256}"
            deadline = time.monotonic() + 185
            while time.monotonic() < deadline:
                leader_text = log_text(leader, cluster)
                follower_text = log_text(follower, cluster)
                if "PROBE_DONE" in leader_text and verified in follower_text.splitlines():
                    break
                time.sleep(0.15)
            else:
                raise TimeoutError(f"Final record was not verified by both voters, see {run_dir}")
            if barrier("warmed", live, cluster) != (leader, epoch, warmed):
                raise ValueError("Leader or epoch changed during measurement")
            os.link(run_dir / "raw.csv", out)
            print(f"Verified 288 values on nodes {live}, timestamps in {out}", flush=True)
        finally:
            stop_nodes(cluster)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    build = sub.add_parser("prepare", help="compile the probe using Kafka sources and runtime jars")
    build.add_argument("--kafka-dir", type=Path, required=True)
    build.add_argument(
        "--libs-dir", type=Path, help="libs/ from the matching Kafka binary distribution"
    )
    build.add_argument(
        "--scala-compiler", type=Path, help="Scala compiler jar from the Gradle build"
    )
    trial = sub.add_parser(
        "run", help="run a single fresh three-voter cluster and verify its output"
    )
    trial.add_argument("--trial", type=int, required=True, choices=(1, 2, 3))
    for command in (build, trial):
        command.add_argument("--work-dir", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "prepare":
        prepare(args)
    else:
        run(args)


if __name__ == "__main__":
    main()
