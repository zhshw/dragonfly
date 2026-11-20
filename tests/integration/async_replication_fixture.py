"""Local manager-configured Dragonfly cluster for replication comparison tests."""

from contextlib import ExitStack
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile

if __package__:
    from .raft_data_group_test import HOST, RaftDataGroupFixture
else:
    from raft_data_group_test import HOST, RaftDataGroupFixture


class AsyncReplicationFixture(RaftDataGroupFixture):
    """Use the same client, stop and polling helpers as the Raft process fixture."""

    data_threads = 2
    groups = ((11, 12, 13), (21, 22))

    def setUp(self):
        self.bin = Path(os.environ.get("DRAGONFLY_PATH", "build-dbg/dragonfly")).resolve()
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.proc = {}
        self.resp = {}
        self.upstream = {}
        self.log_offsets = {}
        groups = self.groups
        with ExitStack() as stack:
            reservations = {}
            for nodes in groups:
                for node in nodes:
                    sock = stack.enter_context(socket.socket())
                    sock.bind((HOST, 0))
                    self.resp[node] = sock.getsockname()[1]
                    reservations[node] = sock
                    (self.root / str(node)).mkdir()
                self.upstream.update({r: nodes[0] for r in nodes[1:]})

            def endpoint(node):
                return {"id": f"{node:040x}", "ip": HOST, "port": self.resp[node]}

            self.config = json.dumps(
                [
                    {
                        "slot_ranges": [
                            {
                                "start": i * 16384 // len(groups),
                                "end": (i + 1) * 16384 // len(groups) - 1,
                            }
                        ],
                        "master": endpoint(nodes[0]),
                        "replicas": [endpoint(n) for n in nodes[1:]],
                    }
                    for i, nodes in enumerate(groups)
                ]
            )
            for node, sock in reservations.items():
                sock.close()
                self.start(node)
        self.wait_for(self.cluster_ready)

    def cluster_ready(self):
        """All nodes agree on slots and both ends of each configured replica link are online."""
        slots = self.slots()
        if not slots or len(slots) != len(self.groups):
            return False
        online = (b"online", b"stable_sync")
        for i, group in enumerate(self.groups):
            leader, *replicas = group
            start, end, primary, *followers = slots[i]
            endpoints = [(HOST.encode(), self.resp[n], f"{n:040x}".encode()) for n in group]
            if (
                (start, end) != (i * 16384 // len(slots), (i + 1) * 16384 // len(slots) - 1)
                or tuple(primary[:3]) != endpoints[0]
                or sorted(tuple(f[:3]) for f in followers) != sorted(endpoints[1:])
            ):
                return False
            with self.client(leader) as client:
                role = client.execute_command("ROLE")
            if (
                role[0] != b"master"
                or sorted(int(r[1]) for r in role[1]) != sorted(self.resp[n] for n in replicas)
                or any(r[2] not in online for r in role[1])
            ):
                return False
            for replica in replicas:
                with self.client(replica) as client:
                    role = client.execute_command("ROLE")
                if (
                    len(role) != 4
                    or role[0] not in (b"slave", b"replica")
                    or role[1] != HOST.encode()
                    or int(role[2]) != self.resp[leader]
                    or role[3] not in online
                ):
                    return False
        return True

    def start(self, node):
        flags = [
            "--cluster_mode=yes",
            f"--cluster_node_id={node:040x}",
            f"--proactor_threads={self.data_threads}",
            f"--port={self.resp[node]}",
            f"--bind={HOST}",
            f"--dir={self.root / str(node)}",
            "--dbfilename=",
            "--force_epoll",
            "--logtostderr",
        ]
        with (self.root / f"{node}.log").open("ab") as log:
            self.log_offsets[node] = log.tell()
            self.proc[node] = subprocess.Popen(
                [self.bin, *flags], stdout=log, stderr=log, cwd=self.root / str(node)
            )
        self.addCleanup(self.stop, node)
        with self.client(node) as client:
            self.wait_for(client.ping)
            self.assertEqual(client.execute_command("DFLYCLUSTER", "CONFIG", self.config), b"OK")
            if node in self.upstream:
                result = client.execute_command("REPLICAOF", HOST, self.resp[self.upstream[node]])
                self.assertEqual(result, b"OK")
