"""Exercise the original two-shard topology with seven local Dragonfly processes."""

import base64
import binascii
from concurrent.futures import ThreadPoolExecutor
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
    controller_admin = False

    def setUp(self):
        self.bin = Path(os.environ.get("DRAGONFLY_PATH", "build-dbg/dragonfly")).resolve()
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.proc = {}
        self.raft = {}
        self.resp = {}
        self.admin = {}
        self.admin_password = os.urandom(16).hex()
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
                elif self.controller_admin:
                    self.admin[node] = port()
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

        groups = []
        for gid, node_ids in GROUPS.items():
            start = 0 if gid == 10 else 8192
            members = [entry(node) for node in node_ids]
            if self.learner is not None and gid == 10:
                members.append(entry(self.learner))
            groups.append(
                dict(
                    group_id=gid,
                    members=members,
                    slot_ranges=[dict(start=start, end=start + 8191)],
                )
            )
        self.config = self.root / "topology.json"
        self.config.write_text(json.dumps({"groups": groups}))
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
                "--port=0",
            ]
            if self.config is not None:
                flags.append(f"--raft_bootstrap_topology={self.config}")
            if node in self.admin:
                flags += [
                    f"--admin_port={self.admin[node]}",
                    f"--admin_bind={HOST}",
                    f"--requirepass={self.admin_password}",
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

    def test_recover_confirmed_values_without_bootstrap_after_cluster_crash(self):
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

        # Every restarted process must recover slot ownership and node IDs from the quorum log.
        self.config.unlink()
        self.config = None
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


class ControllerAdminTest(RaftDataGroupFixture):
    controller_admin = True

    def test_submit_query_and_recover_management_request(self):
        slots = self.wait_for(self.slots)
        clients = {
            node: redis.Redis(host=HOST, port=port, password=self.admin_password, socket_timeout=8)
            for node, port in self.admin.items()
        }
        for client in clients.values():
            self.addCleanup(client.close)

        def active():
            for node, client in clients.items():
                if node in self.proc:
                    try:
                        if client.hmget("ctl:slot:42", "owner")[0] in (b"10", b"20"):
                            return node
                    except redis.exceptions.RedisError:
                        pass

        node = self.wait_for(active)
        admin = clients[node]
        request_key = "ctl:req:move-42-001"
        op_key = "ctl:op:move-42-001"
        value = b'{ "slot": 42, "from": 10, "to": 20, "generation": 1 }'
        self.assertEqual(admin.hmget("ctl:slot:42", "owner", "generation"), [b"10", b"1"])

        with redis.Redis(host=HOST, port=self.admin[node]) as unauthenticated:
            with self.assertRaises(redis.exceptions.AuthenticationError):
                unauthenticated.get(request_key)
        with self.assertRaises(redis.exceptions.ResponseError):
            admin.hset("ctl:slot:42", "owner", "20")
        standby = next(client for peer, client in clients.items() if peer != node)
        with self.assertRaises(redis.exceptions.ResponseError):
            standby.set(request_key, value, nx=True)

        # Incompressible values exceed one transfer chunk, while individual SETs fit a batch.
        keys = [b"{12561}:item:" + bytes([i]) for i in range(6)]
        values = [os.urandom(256 * 1024) for _ in keys]
        neighbor = b"{104856}:neighbor"
        source_port = slots[0][2][1]
        with redis.Redis(host=HOST, port=source_port) as source:
            self.assertEqual(source.execute_command("CLUSTER", "KEYSLOT", keys[0]), 42)
            self.assertEqual(source.execute_command("CLUSTER", "KEYSLOT", neighbor), 43)
            self.assertTrue(source.set(neighbor, b"unmoved"))
            for key, payload in zip(keys, values):
                self.assertTrue(source.set(key, payload))
        with redis.Redis(host=HOST, port=source_port, db=1) as source:
            self.assertTrue(source.set(keys[0], b"db1\x00\xff"))

        # Stop a target voter so acceptance and completion can be observed independently.
        target_follower = next(node for node in GROUPS[20] if self.resp[node] == slots[1][3][1])
        seed = next(iter(self.resp.values()))
        with redis.RedisCluster(host=HOST, port=seed, socket_timeout=7) as business:
            self.assertTrue(business.set(request_key, b"business value"))
            self.assertIsNone(admin.get(request_key))
            self.stop(target_follower, crash=True)
            with ThreadPoolExecutor(max_workers=2) as workers:
                replies = list(
                    workers.map(lambda _: admin.set(request_key, value, nx=True), range(2))
                )
            self.assertCountEqual(replies, [True, None])

        status = admin.hgetall(op_key)
        self.assertIn(status[b"phase"], (b"preparing", b"frozen"))
        self.assertEqual(status[b"slot"], b"42")
        self.assertEqual(admin.get(request_key), value)
        self.assertEqual(admin.hmget("ctl:slot:42", "owner", "generation"), [b"10", b"1"])

        self.stop(node, crash=True)
        successor = clients[self.wait_for(active)]
        self.assertIsNone(successor.set(request_key, b"retry with another value", nx=True))
        self.assertEqual(successor.get(request_key), value)
        self.assertEqual(successor.hgetall(op_key)[b"migration_id"], status[b"migration_id"])
        self.wait_for(lambda: successor.hgetall(op_key).get(b"phase") == b"frozen")
        with redis.Redis(host=HOST, port=source_port) as source:
            for args in (("GET", keys[0]), ("SET", keys[0], b"frozen write")):
                with self.assertRaises(redis.exceptions.ResponseError) as rejected:
                    source.execute_command(*args)
                error = rejected.exception
                self.assertTrue(
                    isinstance(error, redis.exceptions.TryAgainError)
                    or str(error).startswith("TRYAGAIN "),
                    str(error),
                )
            self.assertTrue(source.set(neighbor, b"written while frozen"))
            self.assertEqual(source.get(neighbor), b"written while frozen")
        self.start(target_follower)
        self.wait_for(lambda: successor.hgetall(op_key).get(b"phase") == b"finished")

        def completed_routes():
            routes = [sorted(self.client(n).execute_command("CLUSTER", "SLOTS")) for n in self.resp]
            if any(r != routes[0] for r in routes):
                return None
            expected = [(0, 41), (42, 42), (43, 8191), (8192, 16383)]
            if [tuple(row[:2]) for row in routes[0]] != expected:
                return None
            return routes[0] if all(len(row) == 4 for row in routes[0]) else None

        def check_completed():
            admin = clients[self.wait_for(active)]
            self.assertEqual(admin.get(request_key), value)
            self.assertEqual(admin.hgetall(op_key)[b"phase"], b"finished")
            self.assertEqual(admin.hmget("ctl:slot:42", "owner", "generation"), [b"20", b"2"])
            self.assertEqual(admin.hmget("ctl:slot:43", "owner", "generation"), [b"10", b"1"])
            routes = self.wait_for(completed_routes)
            for row, gid in zip(routes, (10, 20, 10, 20)):
                self.assertEqual(
                    {endpoint[1] for endpoint in row[2:]}, {self.resp[n] for n in GROUPS[gid]}
                )
            target_port = routes[1][2][1]
            with redis.Redis(host=HOST, port=routes[2][2][1]) as source:
                self.assertEqual(source.get(neighbor), b"written while frozen")
                with self.assertRaises(redis.exceptions.ResponseError) as rejected:
                    source.set(keys[0], b"stale write")
                self.assertIn(f"{HOST}:{target_port}", str(rejected.exception))
            with redis.Redis(host=HOST, port=target_port) as target:
                for key, payload in zip(keys, values):
                    self.assertEqual(target.get(key), payload)
                self.assertEqual(target.get(request_key), b"business value")
            with redis.Redis(host=HOST, port=target_port, db=1) as target:
                self.assertEqual(target.get(keys[0]), b"db1\x00\xff")
            return target_port

        new_key = b"{12561}:after-cutover"
        with redis.Redis(host=HOST, port=check_completed()) as target:
            self.assertTrue(target.set(new_key, b"new owner write"))
            self.assertEqual(target.get(new_key), b"new owner write")
        for running in list(self.proc):
            self.stop(running, crash=True)
        self.config.unlink()
        self.config = None
        for member in self.raft:
            self.start(member)
        with redis.Redis(host=HOST, port=check_completed()) as target:
            self.assertEqual(target.get(new_key), b"new owner write")


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
