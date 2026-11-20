"""Exercise the original two-shard topology with seven local Dragonfly processes."""

import base64
import binascii
from contextlib import ExitStack
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import time
import unittest

import redis

if __package__:
    from .raft_controller_test import change_voter, describe
else:
    from raft_controller_test import change_voter, describe

HOST = "127.0.0.1"
GROUPS = {10: (11, 12), 20: (21, 22)}


class RaftDataGroupFixture(unittest.TestCase):
    learner = None

    def setUp(self):
        self.bin = Path(os.environ.get("DRAGONFLY_PATH", "build-dbg/dragonfly")).resolve()
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.proc = {}
        self.raft = {}
        self.resp = {}
        self.cluster_id = "HIJ1rG6XThu4T7D5Jg8xIg"
        with ExitStack() as sockets:

            def port():
                sock = sockets.enter_context(socket.socket())
                sock.bind((HOST, 0))
                return sock.getsockname()[1]

            nodes = [1, 2, 3, 11, 12, 21, 22]
            if self.learner is not None:
                nodes.append(self.learner)
            for node in nodes:
                self.raft[node] = port()
                if node > 3:
                    self.resp[node] = port()
            for node in nodes:
                options = []
                if self.learner and (node in GROUPS[10] or node == self.learner):
                    directory = (
                        lambda n: base64.urlsafe_b64encode(bytes([n]) * 16).decode().rstrip("=")
                    )
                    options = [directory(node), "--data-group", "10"]
                    if node == self.learner:
                        options.append("--no-initial-controllers")
                    else:
                        initial = ",".join(
                            f"{n}@{HOST}:{self.raft[n]}:{directory(n)}" for n in GROUPS[10]
                        )
                        options += ["--initial-controllers", initial]
                subprocess.run(
                    [
                        self.bin.with_name("raft_storage"),
                        "format",
                        self.root / str(node),
                        self.cluster_id,
                        str(node),
                        *options,
                    ],
                    check=True,
                    stdout=subprocess.DEVNULL,
                )
        self.voters = ",".join(f"{i}@{HOST}:{self.raft[i]}" for i in (1, 2, 3))

        def entry(node):
            return dict(
                id=f"{node:040x}",
                broker_id=node,
                ip=HOST,
                port=self.resp[node],
                raft_port=self.raft[node],
            )

        layout = []
        for gid, (a, b) in GROUPS.items():
            start = 0 if gid == 10 else 8192
            layout.append(
                dict(
                    group_id=gid,
                    master=entry(a),
                    replicas=[entry(b)]
                    + ([entry(self.learner)] if self.learner and gid == 10 else []),
                    slot_ranges=[dict(start=start, end=start + 8191)],
                )
            )
        self.config = self.root / "topology.json"
        self.config.write_text(json.dumps(layout))
        for node in self.raft:
            self.start(node)

    def start(self, node):
        flags = [
            f"--dir={self.root / str(node)}",
            f"--raft_node_id={node}",
            "--cluster_mode=raft",
            f"--raft_cluster_id={self.cluster_id}",
            f"--raft_quorum_voters={self.voters}",
            "--force_epoll",
            "--proactor_threads=1",
            "--logtostderr",
        ]
        if node in self.resp:
            flags += [
                "--raft_role=data",
                f"--raft_listen={HOST}:{self.raft[node]}",
                f"--port={self.resp[node]}",
                f"--bind={HOST}",
            ]
        else:
            flags += [
                "--raft_role=controller",
                f"--raft_bootstrap_topology={self.config}",
                "--port=0",
            ]
        with (self.root / f"{node}.log").open("ab") as log:
            self.proc[node] = subprocess.Popen(
                [self.bin, *flags], stdout=log, stderr=log, cwd=self.root / str(node)
            )
        self.addCleanup(self.stop, node)

    def stop(self, node, crash=False):
        proc = self.proc.pop(node, None)
        if proc is None:
            return
        if crash:
            proc.kill()
        else:
            proc.terminate()
        try:
            status = proc.wait(8)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
            self.fail(f"node {node} did not stop")
        self.assertEqual(
            status, -signal.SIGKILL if crash else 0, (self.root / f"{node}.log").read_text()[-4000:]
        )

    def client(self, node):
        return redis.Redis(host=HOST, port=self.resp[node], socket_timeout=7)

    def wait_for(self, check):
        deadline = time.monotonic() + 35
        while time.monotonic() < deadline:
            for node, proc in self.proc.items():
                if proc.poll() is not None:
                    self.fail((self.root / f"{node}.log").read_text()[-4000:])
            try:
                result = check()
                if result:
                    return result
            except (
                redis.exceptions.ConnectionError,
                redis.exceptions.ResponseError,
                redis.exceptions.RedisClusterException,
            ):
                pass
            time.sleep(0.1)
        tail = "\n".join((self.root / f"{node}.log").read_text()[-1500:] for node in self.proc)
        self.fail(f"timed out waiting for cluster: {tail}")

    def slots(self):
        replies = [
            self.client(n).execute_command("CLUSTER", "SLOTS") for n in self.resp if n in self.proc
        ]
        if not replies or any(r != replies[0] for r in replies) or len(replies[0]) != 2:
            return None
        return sorted(replies[0])

    @staticmethod
    def key_for_range(start, end):
        for number in range(1000):
            key = f"k{number}".encode()
            if start <= binascii.crc_hqx(key, 0) % 16384 <= end:
                return key
        raise AssertionError("no test key in slot range")


class RaftDataGroupTest(RaftDataGroupFixture):
    def test_two_voters_per_group(self):
        slots = self.wait_for(self.slots)
        self.assertEqual([(r[0], r[1]) for r in slots], [(0, 8191), (8192, 16383)])
        for row, members in zip(slots, GROUPS.values()):
            self.assertEqual(
                {endpoint[2] for endpoint in row[2:]}, {f"{node:040x}".encode() for node in members}
            )
        by_port = {port: node for node, port in self.resp.items()}
        leader_a, leader_b = (by_port[row[2][1]] for row in slots)
        follower_a = next(n for n in GROUPS[10] if n != leader_a)
        key_a, key_b = (self.key_for_range(row[0], row[1]) for row in slots)
        cluster = redis.RedisCluster(host=HOST, port=self.resp[follower_a], socket_timeout=7)
        self.addCleanup(cluster.close)
        self.assertTrue(cluster.set(key_a, b"\x00left"))
        self.assertTrue(cluster.set(key_b, b"right\xff"))
        self.assertEqual(cluster.get(key_a), b"\x00left")
        self.assertEqual(cluster.get(key_b), b"right\xff")
        cli = subprocess.run(
            ["redis-cli", "-c", "--raw", "-h", HOST, "-p", str(self.resp[leader_b]), "GET", key_a],
            check=True,
            capture_output=True,
            timeout=5,
        )
        self.assertEqual(cli.stdout, b"\x00left\n")
        with self.assertRaises(redis.exceptions.ResponseError) as moved:
            self.client(leader_b).set(key_a, b"wrong-owner")
        self.assertIn(f"{HOST}:{self.resp[leader_a]}", str(moved.exception))

        # No data group can borrow a controller vote or a vote from the other data group.
        self.stop(follower_a, crash=True)
        with self.assertRaises(redis.exceptions.ResponseError):
            self.client(leader_a).set(key_a, b"unconfirmed")
        self.assertTrue(self.client(leader_b).set(key_b, b"independent"))
        self.start(follower_a)
        self.wait_for(lambda: cluster.set(key_a, b"after-recovery"))
        self.assertEqual(cluster.get(key_a), b"after-recovery")

        # An already configured data group keeps its majority if the controllers stop.
        for node in (1, 2, 3):
            self.stop(node, crash=True)
        self.assertTrue(self.client(leader_b).set(key_b, b"metadata-offline"))
        self.assertEqual(self.client(leader_b).get(key_b), b"metadata-offline")

    def test_recover_confirmed_values_after_power_loss(self):
        slots = self.wait_for(self.slots)
        values = {
            self.key_for_range(row[0], row[1]): bytes([i, 0, 255]) for i, row in enumerate(slots)
        }
        seed = next(iter(self.resp.values()))
        with redis.RedisCluster(host=HOST, port=seed, socket_timeout=7) as cluster:
            for key, value in values.items():
                self.assertTrue(cluster.set(key, value))
        for node in list(self.proc):
            self.stop(node, crash=True)
        for node in self.raft:
            self.start(node)
        recovered = self.wait_for(self.slots)
        for before, after in zip(slots, recovered):
            self.assertEqual(before[:2], after[:2])
            self.assertEqual({n[2] for n in before[2:]}, {n[2] for n in after[2:]})
        with redis.RedisCluster(host=HOST, port=seed, socket_timeout=7) as cluster:
            for key, value in values.items():
                self.assertEqual(self.wait_for(lambda: cluster.get(key)), value)

    def test_conditional_writes_and_wait(self):
        slots = self.wait_for(self.slots)
        master, follower = slots[0][2:4]
        key = self.key_for_range(*slots[0][:2])
        with redis.Redis(host=HOST, port=master[1], single_connection_client=True) as client:
            self.assertIsNone(client.set(key, "missing", xx=True))
            self.assertTrue(client.set(key, "first", nx=True))
            self.assertIsNone(client.set(key, "ignored", nx=True))
            self.assertTrue(client.set(key, "last", xx=True))
            self.assertEqual(client.get(key), b"last")
            self.assertEqual(client.wait(1, 2500), 1)
            self.assertEqual(client.wait(2, 50), 1)
            self.assertEqual(client.execute_command("ROLE")[0], b"master")
            self.assertEqual(client.info("replication")["connected_slaves"], 1)
        with redis.Redis(host=HOST, port=follower[1]) as replica:
            self.assertEqual(replica.execute_command("ROLE")[0], b"slave")
            self.assertEqual(replica.info("replication")["master_port"], master[1])

    @unittest.skipUnless(
        os.getenv("KAFKA_TEST_CLASSPATH"), "requires compiled LiveQuorum and kafka-clients"
    )
    def test_live_kafka_java_protocol(self):
        slots = self.wait_for(self.slots)

        def probe(node, topic, partition, voters):
            return subprocess.run(
                [
                    "java",
                    "-cp",
                    os.environ["KAFKA_TEST_CLASSPATH"],
                    "LiveQuorum",
                    HOST,
                    str(self.raft[node]),
                    topic,
                    str(partition),
                    str(voters),
                ],
                capture_output=True,
                text=True,
                timeout=6,
            )

        responses = [probe(node, "__cluster_metadata", 0, 3) for node in (1, 2, 3)]
        self.assertTrue(any(p.returncode == 0 for p in responses), [p.stderr for p in responses])
        for (gid, members), row in zip(GROUPS.items(), slots):
            leader = next(n for n in members if self.resp[n] == row[2][1])
            result = probe(leader, "__dragonfly_data", gid, len(members))
            self.assertEqual(result.returncode, 0, result.stderr)


class DynamicDataGroupTest(RaftDataGroupFixture):
    learner = 13

    def test_promote_caught_up_learner_and_survive_primary_failure(self):
        slots = self.wait_for(self.slots)
        leader = next(n for n in GROUPS[10] if self.resp[n] == slots[0][2][1])
        key = self.key_for_range(0, 8191)
        self.assertTrue(self.client(leader).set(key, b"confirmed-with-two"))
        view = lambda n: describe(self.raft[n], b"__dragonfly_data", 10)
        self.wait_for(
            lambda: (v := view(leader))
            and self.learner in v.observers
            and v.observers[self.learner] >= v.hw
        )
        self.assertEqual(set(view(leader).voters), set(GROUPS[10]))
        error = change_voter(
            self.raft[leader],
            80,
            self.cluster_id,
            self.learner,
            bytes([self.learner]) * 16,
            listener="RAFT",
            endpoint=self.raft[self.learner],
        )
        self.assertEqual(error, 0)
        before = self.wait_for(
            lambda: (v := view(leader))
            and len(v.voters) == 3
            and min(v.voters.values()) >= v.hw
            and v
        )
        self.stop(leader, crash=True)

        def next_leader():
            for n in (*GROUPS[10], self.learner):
                if (
                    n in self.proc
                    and (v := view(n))
                    and v.epoch > before.epoch
                    and v.hw > before.hw
                ):
                    return v
            return None

        elected = self.wait_for(next_leader)
        self.assertIn(elected.leader, set((*GROUPS[10], self.learner)) - {leader})
        self.assertEqual(
            self.wait_for(lambda: self.client(elected.leader).get(key)), b"confirmed-with-two"
        )
        self.assertTrue(self.client(elected.leader).set(key, b"confirmed-after-failover"))
        routes = self.wait_for(self.slots)
        self.assertEqual([(r[0], r[1]) for r in routes], [(0, 8191), (8192, 16383)])


if __name__ == "__main__":
    unittest.main()
