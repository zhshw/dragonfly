"""Run against a local build containing dragonfly and raft_storage (DRAGONFLY_PATH)."""

import base64
import io
import json
import os
from pathlib import Path
import socket
import struct
import subprocess
import tempfile
import time
import unittest
from contextlib import ExitStack
from typing import NamedTuple


class View(NamedTuple):
    leader: int
    epoch: int
    hw: int
    voters: dict
    observers: dict


def describe(port, topic=b"__cluster_metadata", partition_id=0):
    # Kafka DescribeQuorum v0, flexible headers, one topic and one partition.
    topic_field = bytes([len(topic) + 1]) + topic
    header = struct.pack(">hhihB", 55, 0, 21, -1, 0)
    request = header + b"\x02" + topic_field + b"\x02" + struct.pack(">iBBB", partition_id, 0, 0, 0)
    with socket.create_connection(("127.0.0.1", port), timeout=1) as sock, sock.makefile("rb") as r:
        sock.sendall(struct.pack(">i", len(request)) + request)
        size = struct.unpack(">i", r.read(4))[0]
        assert 0 < size < 65536
        body = io.BytesIO(r.read(size))
    assert struct.unpack(">iBhB", body.read(8)) == (21, 0, 0, 2)
    assert body.read(len(topic_field)) == topic_field
    count, partition, error, leader, epoch, hw = struct.unpack(">Bihiiq", body.read(23))
    assert count == 2 and partition == partition_id
    if error == 6:  # NOT_LEADER_OR_FOLLOWER
        return None
    assert error == 0, error

    def replicas():
        offsets = {}
        for _ in range(body.read(1)[0] - 1):
            node, end, tags = struct.unpack(">iqB", body.read(13))
            assert tags == 0
            offsets[node] = end
        return offsets

    voters, observers = replicas(), replicas()
    assert body.read() == b"\x00\x00\x00"
    return View(leader, epoch, hw, voters, observers)


def compact(value):
    data = value.encode()
    assert len(data) < 127
    return bytes([len(data) + 1]) + data


def broker_rpc(port, key, version, request):
    # Flexible RequestHeader v2 and ResponseHeader v1, a single in-flight request.
    correlation = 28
    frame = struct.pack(">hhihB", key, version, correlation, -1, 0) + request
    with socket.create_connection(("127.0.0.1", port), timeout=7) as sock, sock.makefile("rb") as r:
        sock.sendall(struct.pack(">i", len(frame)) + frame)
        size = struct.unpack(">i", r.read(4))[0]
        assert 5 < size < 4096
        result = r.read(size)
    assert struct.unpack(">iB", result[:5]) == (correlation, 0)
    return result[5:]


def change_voter(port, key, cluster, node, directory, *, listener=None, endpoint=0, epoch=0):
    identity = struct.pack(">i", node) + directory
    req = compact(cluster)
    if key == 80:
        req += struct.pack(">i", 5000)
    elif key == 82:
        req += struct.pack(">i", epoch)
    req += identity
    if key in (80, 82):
        req += b"\x02" + compact(listener) + compact("127.0.0.1") + struct.pack(">HB", endpoint, 0)
    if key == 80:
        req += b"\x01"  # ackWhenCommitted
    if key == 82:
        req += struct.pack(">hhB", 0, 1, 0)
    result = broker_rpc(port, key, int(key == 80), req + b"\x00")
    return struct.unpack_from(">h", result, 4)[0]


def register(port, cluster, incarnation, resp_port, raft_port):
    req = struct.pack(">i", 7) + compact(cluster) + bytes([incarnation]) * 16 + b"\x03"
    for name, endpoint in (("RESP", resp_port), ("RAFT", raft_port)):
        req += compact(name) + compact("127.0.0.1") + struct.pack(">HhB", endpoint, 0, 0)
    req += b"\x03"
    for name, level in (("metadata.version", 33), ("dragonfly.cluster", 1)):
        req += compact(name) + struct.pack(">hhB", level, level, 0)
    # null rack, isMigratingZkBroker=false, one non-reserved directory UUID, previousEpoch=-1.
    req += b"\x00\x00\x02" + b"\x11" * 16 + struct.pack(">qB", -1, 0)
    _, error, epoch, tags = struct.unpack(">ihqB", broker_rpc(port, 62, 4, req))
    assert tags == 0
    return error, epoch


def heartbeat(port, epoch, applied_offset):
    req = struct.pack(">iqqBBB", 7, epoch, applied_offset, 0, 0, 0)
    _, error, caught, fenced, shutdown, tags = struct.unpack(
        ">ihBBBB", broker_rpc(port, 63, 2, req)
    )
    assert tags == 0 and not shutdown
    return error, bool(caught), bool(fenced)


class ControllerProcessTest(unittest.TestCase):
    def setUp(self):
        self.server = Path(os.getenv("DRAGONFLY_PATH", "build-dbg/dragonfly")).resolve()
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.nodes = {}
        self.ports = {}
        self.cluster_id = "HIJ1rG6XThu4T7D5Jg8xIg"

    def configure(self, count, initial_count=None):
        with ExitStack() as stack:
            for node in range(1, count + 1):
                sock = stack.enter_context(socket.socket())
                sock.bind(("127.0.0.1", 0))
                self.ports[node] = sock.getsockname()[1]
            for node in self.ports:
                options = []
                if initial_count is not None:

                    def directory(n):
                        return base64.urlsafe_b64encode(bytes([n]) * 16).decode().rstrip("=")

                    initial = ",".join(
                        f"{n}@127.0.0.1:{p}:{directory(n)}"
                        for n, p in self.ports.items()
                        if n <= initial_count
                    )
                    options = [directory(node)]
                    if node <= initial_count:
                        options += ["--initial-controllers", initial]
                    else:
                        options += ["--no-initial-controllers"]
                subprocess.run(
                    [
                        self.server.with_name("raft_storage"),
                        "format",
                        self.root / str(node),
                        self.cluster_id,
                        str(node),
                        *options,
                    ],
                    check=True,
                    stdout=subprocess.DEVNULL,
                )
        self.voters = ",".join(f"{node}@127.0.0.1:{port}" for node, port in self.ports.items())

    def args(self, node):
        return [
            self.server,
            "--cluster_mode=raft",
            "--raft_role=controller",
            f"--raft_cluster_id={self.cluster_id}",
            f"--raft_node_id={node}",
            f"--raft_quorum_voters={self.voters}",
            f"--dir={self.root / str(node)}",
            *getattr(self, "bootstrap_flags", []),
            "--force_epoll",
            "--proactor_threads=1",
            "--port=0",
            "--logtostderr",
        ]

    def start(self, node):
        with (self.root / f"{node}.log").open("ab") as log:
            proc = subprocess.Popen(self.args(node), stdout=log, stderr=log)
        self.nodes[node] = proc
        self.addCleanup(self.stop, node)

    def stop(self, node, crash=False):
        proc = self.nodes.pop(node, None)
        if proc is not None:
            if crash:
                proc.kill()
            else:
                proc.terminate()
            try:
                proc.wait(5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
                self.fail(f"controller {node} did not stop")

    def wait_for(self, match):
        deadline = time.monotonic() + 20
        views = {}
        while time.monotonic() < deadline:
            for node, proc in self.nodes.items():
                if proc.poll() is not None:
                    self.fail((self.root / f"{node}.log").read_text()[-3000:])
                try:
                    views[node] = describe(self.ports[node])
                except ConnectionRefusedError:
                    continue
                if match(node, views[node]):
                    return views[node]
            time.sleep(0.05)
        self.fail(f"quorum did not reach expected state, last views {views}")

    def committed(self, after):
        members = set(self.nodes)
        return self.wait_for(
            lambda node, v: v
            and v.leader == node
            and v.hw > after
            and members <= v.voters.keys()
            and all(v.voters[k] >= v.hw for k in members)
        )

    def test_dynamic_add_update_remove_leader_and_restart(self):
        self.configure(3, initial_count=2)
        for node in self.ports:
            self.start(node)
        before = self.wait_for(
            lambda n, v: v and n == v.leader and v.hw > 0 and v.observers.get(3, -1) >= v.hw
        )
        self.assertEqual(set(before.voters), {1, 2})
        port = self.ports[before.leader]
        fields = dict(listener="CONTROLLER", endpoint=self.ports[3])
        self.assertEqual(change_voter(port, 80, self.cluster_id, 3, bytes([3]) * 16, **fields), 0)
        promoted = self.committed(before.hw)
        self.assertEqual(
            change_voter(
                port, 82, self.cluster_id, 3, bytes([3]) * 16, epoch=promoted.epoch - 1, **fields
            ),
            74,
        )
        self.assertEqual(
            change_voter(
                port, 82, self.cluster_id, 3, bytes([3]) * 16, epoch=promoted.epoch, **fields
            ),
            0,
        )
        self.assertEqual(
            change_voter(port, 81, self.cluster_id, before.leader, bytes([before.leader]) * 16), 0
        )
        kept = {1, 2, 3} - {before.leader}
        self.wait_for(
            lambda n, v: v
            and n == v.leader
            and v.epoch > before.epoch
            and set(v.voters) == kept
            and v.hw > promoted.hw
        )
        self.stop(before.leader)
        self.start(before.leader)
        recovered = self.wait_for(
            lambda n, v: v
            and n == v.leader
            and set(v.voters) == kept
            and v.observers.get(before.leader, -1) >= v.hw
        )
        self.assertIn(recovered.leader, kept)

    def test_single_voter_and_incompatible_restart(self):
        self.configure(1)
        self.start(1)
        view = self.committed(0)
        self.assertEqual(set(view.voters), {1})
        self.stop(1)
        args = self.args(1)
        args = [a for a in args if not str(a).startswith("--raft_quorum_voters=")]
        args.append(f"--raft_quorum_voters={self.voters},2@127.0.0.1:12345")
        rejected = subprocess.run(args, capture_output=True, timeout=10)
        self.assertNotEqual(rejected.returncode, 0)
        self.assertIn(b"Static voter IDs differ", rejected.stderr)
        self.start(1)
        self.assertGreater(self.committed(view.hw).epoch, view.epoch)

    def bootstrap_broker(self, resp_port, raft_port):
        path = self.root / "bootstrap.json"
        path.write_text(
            json.dumps(
                [
                    {
                        "group_id": 10,
                        "master": {
                            "id": "A",
                            "ip": "127.0.0.1",
                            "port": resp_port,
                            "broker_id": 7,
                            "raft_port": raft_port,
                        },
                        "replicas": [],
                        "slot_ranges": [{"start": 0, "end": 16383}],
                    }
                ]
            )
        )
        self.bootstrap_flags = [f"--raft_bootstrap_topology={path}"]

    def test_bootstrap_broker_fencing_and_controller_failover(self):
        self.configure(3)
        with ExitStack() as stack:
            ports = []
            for _ in range(2):
                sock = stack.enter_context(socket.socket())
                sock.bind(("127.0.0.1", 0))
                ports.append(sock.getsockname()[1])
            resp_port, raft_port = ports
            self.bootstrap_broker(resp_port, raft_port)
            for node in self.ports:
                self.start(node)
            initial = self.committed(1)
            port = self.ports[initial.leader]
            peer = next(n for n in self.nodes if n != initial.leader)
            self.assertEqual(register(self.ports[peer], self.cluster_id, 1, *ports), (41, -1))
            self.assertEqual(register(port, "wrong-cluster", 1, *ports), (104, -1))
            error, epoch = register(port, self.cluster_id, 1, *ports)
            self.assertEqual(error, 0)
            self.assertEqual(heartbeat(port, epoch, epoch - 1), (0, False, True))
            self.assertEqual(heartbeat(port, epoch, epoch), (0, True, False))
            self.assertEqual(register(port, self.cluster_id, 2, *ports), (101, -1))
            stable = self.committed(initial.hw)
            self.assertGreater(stable.hw, epoch)
            self.stop(initial.leader, crash=True)
            next_view = self.committed(stable.hw)
            new_port = self.ports[next_view.leader]
            self.assertEqual(heartbeat(new_port, epoch, next_view.hw - 1), (0, True, False))
            self.assertEqual(heartbeat(new_port, epoch - 1, next_view.hw - 1), (77, False, True))
            self.assertEqual(register(new_port, self.cluster_id, 1, *ports), (0, epoch))

    def test_recovers_broker_and_registration_barrier_from_offline_snapshot(self):
        self.configure(1)
        with ExitStack() as stack:
            ports = []
            for _ in range(2):
                sock = stack.enter_context(socket.socket())
                sock.bind(("127.0.0.1", 0))
                ports.append(sock.getsockname()[1])
            self.bootstrap_broker(*ports)
            self.bootstrap_flags.append("--raft_metadata_snapshot_bytes=1")
            self.start(1)
            initial = self.committed(1)
            port = self.ports[1]
            error, epoch = register(port, self.cluster_id, 1, *ports)
            self.assertEqual(error, 0)
            self.assertEqual(heartbeat(port, epoch, epoch), (0, True, False))
            stable = self.committed(initial.hw)
            checkpoint = (
                self.root
                / "1"
                / "__cluster_metadata-0"
                / f"{stable.hw:020d}-{stable.epoch:010d}.checkpoint"
            )
            deadline = time.monotonic() + 10
            while not checkpoint.is_file() and time.monotonic() < deadline:
                time.sleep(0.05)
            self.assertTrue(checkpoint.is_file(), checkpoint)
            self.stop(1, crash=True)
            self.bootstrap_flags = []
            self.start(1)
            restarted = self.committed(stable.hw)
            self.assertGreater(restarted.epoch, stable.epoch)

            # Snapshot replay reconstructs registration provenance at its inclusive cut.
            # A log-only recovery would retain the older registration offset (epoch).
            self.assertGreater(stable.hw - 1, epoch)
            self.assertEqual(heartbeat(port, epoch, epoch), (0, False, False))
            self.assertEqual(heartbeat(port, epoch, stable.hw - 1), (0, True, False))

    def test_election_minority_and_durable_prefix(self):
        self.configure(3)
        for node in self.ports:
            self.start(node)
        first = self.committed(0)
        self.stop(first.leader, crash=True)
        second = self.committed(first.hw)
        self.assertGreater(second.epoch, first.epoch)
        logname = Path("__cluster_metadata-0") / "00000000000000000000.log"
        prefix = (self.root / str(second.leader) / logname).read_bytes()
        peer = next(n for n in self.nodes if n != second.leader)
        self.stop(peer, crash=True)
        self.wait_for(lambda node, view: node == second.leader and view is None)
        self.assertEqual(prefix, (self.root / str(second.leader) / logname).read_bytes())
        self.start(peer)
        self.start(first.leader)
        third = self.committed(second.hw)
        self.assertGreater(third.epoch, second.epoch)
        for node in self.nodes:
            self.assertTrue((self.root / str(node) / logname).read_bytes().startswith(prefix))


if __name__ == "__main__":
    unittest.main()
