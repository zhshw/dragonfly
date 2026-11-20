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

    def setUp(self):
        self.bin = Path(os.environ.get("DRAGONFLY_PATH", "build-dbg/dragonfly")).resolve()
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.proc = {}
        self.resp = {}
        self.upstream = {}
        self.log_offsets = {}
        groups = ((11, 12, 13), (21, 22))
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
        for replica in self.upstream:
            with self.client(replica) as client:
                self.wait_for(lambda: client.execute_command("ROLE")[3] == b"online")

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
