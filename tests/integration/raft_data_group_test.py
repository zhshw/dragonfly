"""Exercise the original two-shard topology with seven local Dragonfly processes."""

import base64
import binascii
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
import json
import os
from pathlib import Path
import shlex
import signal
import socket
import subprocess
import tempfile
from threading import Barrier
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
    new_controller = None
    controller_admin = False
    data_threads = 1
    data_flags = ()
    snapshot_bytes = {}
    snapshot_interval_ms = 0

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
            if self.new_controller is not None:
                nodes.append(self.new_controller)
            if self.learner is not None:
                nodes.append(self.learner)
            for node in nodes:
                self.raft[node] = port()
                if node > 3 and node != self.new_controller:
                    self.resp[node] = port()
                elif self.controller_admin:
                    self.admin[node] = port()

            def directory(n):
                return base64.urlsafe_b64encode(bytes([n]) * 16).decode().rstrip("=")

            def initial_voters(members):
                return ",".join(f"{n}@{HOST}:{self.raft[n]}:{directory(n)}" for n in members)

            for node in nodes:
                options = []
                if self.new_controller and node not in self.resp:
                    options = [directory(node)]
                    if node == self.new_controller:
                        options.append("--no-initial-controllers")
                    else:
                        options += ["--initial-controllers", initial_voters((1, 2, 3))]
                elif self.learner and (node in GROUPS[10] or node == self.learner):
                    options = [directory(node), "--data-group", "10"]
                    if node == self.learner:
                        options.append("--no-initial-controllers")
                    else:
                        options += ["--initial-controllers", initial_voters(GROUPS[10])]
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
            if node != self.new_controller:
                self.start(node)

    def start(self, node):
        voters = self.voters
        if node == self.new_controller:
            voters += f",{node}@{HOST}:{self.raft[node]}"
        flags = [
            f"--dir={self.root / str(node)}",
            f"--raft_node_id={node}",
            "--cluster_mode=raft",
            f"--raft_cluster_id={self.cluster_id}",
            f"--raft_quorum_voters={voters}",
            "--force_epoll",
            f"--proactor_threads={self.data_threads if node in self.resp else 1}",
            "--logtostderr",
        ]
        if node in self.resp:
            flags += shlex.split(os.environ.get("RAFT_TEST_DATA_FLAGS", ""))
            flags += self.data_flags
            flags += [
                "--raft_role=data",
                f"--raft_listen={HOST}:{self.raft[node]}",
                f"--port={self.resp[node]}",
                f"--bind={HOST}",
            ]
            if node in self.snapshot_bytes:
                flags += [
                    f"--raft_data_snapshot_bytes={self.snapshot_bytes[node]}",
                    f"--raft_data_snapshot_interval_ms={self.snapshot_interval_ms}",
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

    def wait_for(self, check, *, timeout=35, poll_interval=0.1):
        deadline = time.monotonic() + timeout
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
            time.sleep(poll_interval)
        tail = "\n".join((self.root / f"{node}.log").read_text()[-1500:] for node in self.proc)
        self.fail(f"timed out waiting for cluster: {tail}")

    def slots(self):
        replies = [
            self.client(n).execute_command("CLUSTER", "SLOTS") for n in self.resp if n in self.proc
        ]
        if not replies or any(r != replies[0] for r in replies) or len(replies[0]) != 2:
            return None
        return sorted(replies[0])

    def retained_checkpoint(self, node, group, min_offset):
        directory = self.root / str(node) / f"__dragonfly_data-{group}"
        marker = directory / "dragonfly-log-end"
        try:
            with marker.open("rb") as source:
                size = os.fstat(source.fileno()).st_size

                # DFE3 prepares one of two blocks and atomically selects it via inode size.
                slot_stride = 64 * 1024
                first_slot_size = 2 * slot_stride
                if size in (first_slot_size, first_slot_size + 15):
                    source.seek(0 if size == first_slot_size else slot_stride)
                    magic = b"DFE3"
                elif size == 44:
                    magic = b"DFE2"
                else:
                    # DFE4 keeps the retained snapshot position in the fixed anchor.
                    magic = b"DFE4"
                data = source.read(44)
                if os.fstat(source.fileno()).st_size != size:
                    return None
        except FileNotFoundError:
            return None
        if len(data) != 44 or data[:4] != magic:
            return None
        offset = int.from_bytes(data[4:12], "big")
        epoch = int.from_bytes(data[12:16], "big")
        if offset < min_offset:
            return None
        checkpoint = directory / f"{offset:020d}-{epoch:010d}.checkpoint"
        return checkpoint if checkpoint.exists() else None

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
        values = [f"writer-{i}".encode() for i in range(8)]
        ordinary = {value: b"{" + key + b"}:" + value for value in values}
        ready = Barrier(len(values))

        def compete(value):
            with redis.Redis(
                host=HOST, port=master[1], single_connection_client=True, socket_timeout=7
            ) as writer:
                writer.ping()
                ready.wait(timeout=7)
                self.assertTrue(writer.set(ordinary[value], value))
                self.assertEqual(writer.wait(1, 2500), 1)
                ready.wait(timeout=7)
                result = writer.set(key, value, nx=True)
                if result is True:
                    self.assertEqual(writer.wait(1, 2500), 1)
                return result

        with redis.Redis(host=HOST, port=master[1], single_connection_client=True) as client:
            self.assertIsNone(client.set(key, "missing", xx=True))
            with ThreadPoolExecutor(max_workers=len(values)) as workers:
                results = list(workers.map(compete, values))
            for value in values:
                self.assertEqual(client.get(ordinary[value]), value)
            self.assertCountEqual(results, [True] + [None] * (len(values) - 1))
            self.assertEqual(client.get(key), values[results.index(True)])
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


class RaftPipelineTest(RaftDataGroupFixture):
    data_threads = 4
    data_flags = ("--raft_async_io=true", "--raft_drain_writes=true")

    def writer(self):
        slots = self.wait_for(self.slots)
        master, follower = slots[0][2:4]
        by_port = {port: node for node, port in self.resp.items()}
        key = self.key_for_range(*slots[0][:2])
        client = redis.Redis(
            host=HOST, port=master[1], single_connection_client=True, socket_timeout=10
        )
        self.addCleanup(client.close)
        client.ping()
        return client, key, by_port[master[1]], by_port[follower[1]]

    def test_pipeline_admits_before_majority_and_survives_disconnect(self):
        client, key, leader, follower = self.writer()
        self.assertTrue(client.set(key, b"initial"))
        self.assertEqual(client.wait(1, 2500), 1)

        def view():
            return describe(self.raft[leader], b"__dragonfly_data", 10)

        for disconnect in (False, True):
            before = self.wait_for(view)
            commands = [("SET", key, f"{disconnect}-{i}".encode()) for i in range(64)]
            self.proc[follower].send_signal(signal.SIGSTOP)
            try:
                connection = client.connection
                connection.send_packed_command(connection.pack_commands(commands))

                # The old serial path appends just one write here. Require all 64 while no
                # follower can acknowledge, and verify that no OK has reached the client.
                def admitted():
                    current = view()
                    if current and current.voters[leader] >= before.voters[leader] + 64:
                        return current

                after = self.wait_for(admitted, timeout=2.5, poll_interval=0.01)
                self.assertEqual(after.hw, before.hw)
                self.assertFalse(connection.can_read(timeout=0))
                if disconnect:
                    client.close()
            finally:
                self.proc[follower].send_signal(signal.SIGCONT)
            if not disconnect:
                self.assertEqual([connection.read_response() for _ in commands], [b"OK"] * 64)
                self.assertEqual(client.wait(1, 2500), 1)
            with self.client(leader) as reader:
                self.wait_for(lambda: reader.get(key) == commands[-1][2])

    def test_shutdown_drains_pending_pipeline_without_false_success(self):
        client, key, leader, follower = self.writer()
        self.assertTrue(client.set(key, b"confirmed"))
        self.assertEqual(client.wait(1, 2500), 1)
        before = describe(self.raft[leader], b"__dragonfly_data", 10)
        commands = [("SET", b"{" + key + b"}:pending", str(i)) for i in range(32)]
        connection = client.connection
        self.proc[follower].send_signal(signal.SIGSTOP)
        try:
            connection.send_packed_command(connection.pack_commands(commands))
            self.wait_for(
                lambda: describe(self.raft[leader], b"__dragonfly_data", 10).voters[leader]
                >= before.voters[leader] + len(commands),
                timeout=2.5,
                poll_interval=0.01,
            )
            self.assertFalse(connection.can_read(timeout=0))
            self.stop(leader)
            for _ in commands:
                try:
                    reply = connection.read_response()
                except redis.exceptions.ResponseError as error:
                    self.assertIsInstance(
                        error, (redis.exceptions.TryAgainError, redis.exceptions.ClusterDownError)
                    )
                except redis.exceptions.ConnectionError:
                    break
                else:
                    self.fail(f"unexpected success without a majority: {reply!r}")
        finally:
            self.proc[follower].send_signal(signal.SIGCONT)
        self.start(leader)
        slots = self.wait_for(self.slots)
        with redis.Redis(host=HOST, port=slots[0][2][1]) as recovered:
            self.assertEqual(recovered.get(key), b"confirmed")

    def test_pipeline_barriers_saturation_and_recovery(self):
        client, key, leader, _ = self.writer()
        tag = b"{" + key + b"}:"
        commands = []
        expected = []
        values = {}
        for count, size in ((300, 16), (48, 256 * 1024)):
            for i in range(count):
                name = tag + f"{size}-{i}".encode()
                value = bytes([i % 256]) * size
                values[name] = value
                commands.append(("SET", name, value))
                expected.append(b"OK")
        commands.extend(
            [
                ("SET", key, b"first"),
                ("SET", key, b"last"),
                ("GET", key),
                ("SET", key, b"x" * (9 * 1024 * 1024)),
                ("SET", key, b"ignored", "NX"),
                ("GET", key),
                ("SET", key, b"conditional", "XX"),
                ("WAIT", 1, 2500),
                ("GET", key),
                ("SELECT", 1),
                ("GET", key),
                ("SET", key, b"db-one"),
                ("GET", key),
                ("RESET",),
                ("GET", key),
                ("WAIT", 1, 2500),
            ]
        )
        expected.extend(
            [
                b"OK",
                b"OK",
                b"last",
                ("error", "Dragonfly SET effect exceeds Kafka batch size"),
                None,
                b"last",
                b"OK",
                1,
                b"conditional",
                b"OK",
                None,
                b"OK",
                b"db-one",
                b"RESET",
                b"conditional",
                1,
            ]
        )
        connection = client.connection
        connection.send_packed_command(connection.pack_commands(commands))
        replies = []
        for _ in commands:
            try:
                replies.append(connection.read_response())
            except redis.exceptions.ResponseError as error:
                replies.append(("error", str(error)))
        self.assertEqual(replies, expected)
        values[key] = b"conditional"

        # Confirmed replies remain recoverable after both data voters are killed.
        for node in GROUPS[10]:
            self.stop(node, crash=True)
        for node in GROUPS[10]:
            self.start(node)
        slots = self.wait_for(self.slots)
        with redis.Redis(host=HOST, port=slots[0][2][1]) as recovered:
            for name, value in values.items():
                self.assertEqual(recovered.get(name), value)
            self.assertTrue(recovered.select(1))
            self.assertEqual(recovered.get(key), b"db-one")


class ControllerAdminTest(RaftDataGroupFixture):
    controller_admin = True
    snapshot_bytes = {node: 64 << 20 for members in GROUPS.values() for node in members}
    snapshot_interval_ms = 250

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

        def assert_frozen(client, *args):
            with self.assertRaises(redis.exceptions.ResponseError) as rejected:
                client.execute_command(*args)
            error = rejected.exception
            self.assertTrue(
                isinstance(error, redis.exceptions.TryAgainError)
                or str(error).startswith("TRYAGAIN "),
                str(error),
            )

        with redis.Redis(host=HOST, port=source_port) as source:
            assert_frozen(source, "GET", keys[0])
            assert_frozen(source, "SET", keys[0], b"frozen write")
            self.assertTrue(source.set(neighbor, b"written while frozen"))
            self.assertEqual(source.get(neighbor), b"written while frozen")
        # Restart both source voters after their durable log starts beyond the frozen image.
        source_leader = next(n for n in GROUPS[10] if self.resp[n] == source_port)
        frozen_cut = describe(self.raft[source_leader], b"__dragonfly_data", 10).hw
        for member in GROUPS[10]:
            self.wait_for(lambda: self.retained_checkpoint(member, 10, frozen_cut))
        for member in GROUPS[10]:
            self.stop(member, crash=True)
        for member in GROUPS[10]:
            self.start(member)

        def restored_leader():
            for member in GROUPS[10]:
                try:
                    if self.client(member).get(neighbor) == b"written while frozen":
                        return member
                except redis.RedisError:
                    continue

        source_leader = self.wait_for(restored_leader)
        assert_frozen(self.client(source_leader), "GET", keys[0])
        self.assertEqual(successor.hgetall(op_key)[b"phase"], b"frozen")

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
        target_port = check_completed()
        with redis.Redis(host=HOST, port=target_port) as target:
            self.assertTrue(target.set(new_key, b"new owner write"))
            self.assertEqual(target.get(new_key), b"new owner write")
        target_leader = next(n for n in GROUPS[20] if self.resp[n] == target_port)
        target_cut = describe(self.raft[target_leader], b"__dragonfly_data", 20).hw
        for member in GROUPS[20]:
            self.wait_for(lambda: self.retained_checkpoint(member, 20, target_cut))
        for running in list(self.proc):
            self.stop(running, crash=True)
        self.config.unlink()
        self.config = None
        for member in self.raft:
            self.start(member)
        with redis.Redis(host=HOST, port=check_completed()) as target:
            self.assertEqual(target.get(new_key), b"new owner write")


class DynamicControllerDiscoveryTest(RaftDataGroupFixture):
    new_controller = 4
    controller_admin = True

    def test_data_nodes_follow_controller_outside_bootstrap_list(self):
        slots = self.wait_for(self.slots)
        key = b"{12561}:move"
        with redis.Redis(host=HOST, port=slots[0][2][1]) as source:
            self.assertTrue(source.set(key, b"from original controller"))

        added = self.new_controller
        self.start(added)

        def caught_up_observer():
            for node in (1, 2, 3):
                view = describe(self.raft[node])
                if view and view.hw > 0 and view.observers.get(added, -1) >= view.hw:
                    return view

        old = self.wait_for(caught_up_observer)
        old_port = self.raft[old.leader]
        self.assertEqual(
            change_voter(
                old_port,
                80,
                self.cluster_id,
                added,
                bytes([added]) * 16,
                listener="CONTROLLER",
                endpoint=self.raft[added],
            ),
            0,
        )
        # Each removal waits for its commit. Removing the old leader last elects C4 deterministically.
        for node in sorted(old.voters, key=lambda n: n == old.leader):
            self.assertEqual(
                change_voter(old_port, 81, self.cluster_id, node, bytes([node]) * 16), 0
            )
        self.wait_for(
            lambda: (v := describe(self.raft[added]))
            and v.leader == added
            and set(v.voters) == {added}
            and v.hw > old.hw
        )
        with redis.Redis(host=HOST, port=self.admin[added], password=self.admin_password) as admin:
            self.wait_for(lambda: admin.hmget("ctl:slot:42", "owner") == [b"10"])
            request = '{"slot":42,"from":10,"to":20,"generation":1}'
            self.assertTrue(admin.set("ctl:req:after-election", request, nx=True))
            self.wait_for(
                lambda: admin.hgetall("ctl:op:after-election").get(b"phase") == b"finished"
            )
            self.assertEqual(admin.hmget("ctl:slot:42", "owner", "generation"), [b"20", b"2"])

        # The restarted follower must register a new incarnation on C4. All other brokers must
        # keep heartbeating through the previous incarnation's session timeout.
        follower = next(n for n in GROUPS[20] if self.resp[n] == slots[1][3][1])
        self.stop(follower, crash=True)
        self.start(follower)

        def rediscovered_routes():
            rows = [sorted(self.client(n).execute_command("CLUSTER", "SLOTS")) for n in self.resp]
            if len(rows[0]) == 4 and all(r == rows[0] for r in rows):
                return rows[0]

        routes = self.wait_for(rediscovered_routes)
        self.assertEqual(routes[1][:2], [42, 42])
        with redis.Redis(host=HOST, port=routes[1][2][1]) as target:
            self.assertEqual(target.get(key), b"from original controller")
            self.assertTrue(target.set(key, b"after controller election"))


class DynamicDataGroupFixture(RaftDataGroupFixture):
    learner = 13

    def wait_benchmark_topology(self):
        """Wait for the promoted topology on every data node and return both write leaders."""
        controllers = {1, 2, 3}
        groups = (
            (10, (0, 8191), {*GROUPS[10], self.learner}),
            (20, (8192, 16383), set(GROUPS[20])),
        )
        endpoints = {
            (HOST.encode(), port, f"{node:040x}".encode()): node for node, port in self.resp.items()
        }

        def caught_up(view, leader, members):
            return (
                view
                and view.leader == leader
                and view.epoch > 0
                and view.hw > 0
                and set(view.voters) == members
                and all(offset >= view.hw for offset in view.voters.values())
            )

        def ready():
            if not self.raft.keys() <= self.proc.keys():
                return None
            rows = self.slots()
            if not rows:
                return None
            leaders = []
            try:
                if not any(caught_up(describe(self.raft[n]), n, controllers) for n in controllers):
                    return None
                for row, (gid, bounds, members) in zip(rows, groups):
                    nodes = [endpoints.get(tuple(node[:3])) for node in row[2:]]
                    if (
                        tuple(row[:2]) != bounds
                        or len(nodes) != len(members)
                        or set(nodes) != members
                    ):
                        return None
                    leader = nodes[0]
                    view = describe(self.raft[leader], b"__dragonfly_data", gid)
                    if not caught_up(view, leader, members):
                        return None
                    leaders.append(leader)
            except (ConnectionError, TimeoutError):
                return None
            return tuple(leaders)

        return self.wait_for(ready)

    def elect_recovered_learner(self, leader):
        # Remove the old leader last so each change goes to the active leader. Direct GET
        # requests after this election inspect the recovered voter's own engine.
        for member in sorted(GROUPS[10], key=lambda n: n == leader):
            self.assertEqual(
                change_voter(self.raft[leader], 81, self.cluster_id, member, bytes([member]) * 16),
                0,
            )
        self.wait_for(
            lambda: (v := describe(self.raft[self.learner], b"__dragonfly_data", 10))
            and v.leader == self.learner
            and set(v.voters) == {self.learner}
        )

    def promote_learner(self, leader):
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
        return self.wait_for(
            lambda: (v := view(leader))
            and len(v.voters) == 3
            and min(v.voters.values()) >= v.hw
            and v
        )


class DynamicDataGroupTest(DynamicDataGroupFixture):

    def test_promote_caught_up_learner_and_survive_primary_failure(self):
        slots = self.wait_for(self.slots)
        leader = next(n for n in GROUPS[10] if self.resp[n] == slots[0][2][1])
        key = self.key_for_range(0, 8191)
        self.assertTrue(self.client(leader).set(key, b"confirmed-with-two"))
        before = self.promote_learner(leader)
        view = lambda n: describe(self.raft[n], b"__dragonfly_data", 10)
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


class DataSnapshotRecoveryTest(DynamicDataGroupFixture):
    data_threads = 2
    snapshot_bytes = {11: 1280 * 1024, 12: 1280 * 1024, 13: 64 << 20}

    def test_lagging_voter_installs_snapshot_and_replays_tail(self):
        slots = self.wait_for(self.slots)
        leader = next(n for n in GROUPS[10] if self.resp[n] == slots[0][2][1])
        promoted = self.promote_learner(leader)
        self.stop(self.learner, crash=True)

        # Random values cross the size trigger and require multiple 1 MiB FetchSnapshot responses.
        # These tags route to group 10, with one on each of the two engine shards.
        tags = (b"{12561}", b"{104856}")
        values = {tags[i % 2] + f":part{i}".encode(): os.urandom(256 * 1024) for i in range(6)}
        with self.client(leader) as client:
            self.assertEqual(client.info("server")["thread_count"], self.data_threads)
            for key, value in values.items():
                self.assertTrue(client.set(key, value))
        checkpoint = self.wait_for(
            lambda: self.retained_checkpoint(leader, 10, promoted.voters[self.learner] + 1)
        )
        expected_image = checkpoint.read_bytes()
        self.assertGreater(len(expected_image), 1 << 20)

        # This committed suffix is smaller than the next checkpoint trigger and is delivered by Fetch.
        tail_key = b"{12561}:tail"
        with redis.Redis(host=HOST, port=self.resp[leader], db=1) as other_db:
            self.assertTrue(other_db.set(tail_key, b"db1 incremental value"))
        with redis.Redis(
            host=HOST, port=self.resp[leader], single_connection_client=True
        ) as client:
            self.assertTrue(client.set(tail_key, b"from incremental fetch"))
            self.start(self.learner)
            self.wait_for(lambda: client.wait(2, 500) == 2)

        installed = self.wait_for(
            lambda: self.retained_checkpoint(self.learner, 10, promoted.voters[self.learner] + 1)
        )
        self.assertEqual(installed.name, checkpoint.name)
        self.assertEqual(installed.read_bytes(), expected_image)

        self.elect_recovered_learner(leader)
        with self.client(self.learner) as client:
            self.assertEqual(self.wait_for(lambda: client.get(tail_key)), b"from incremental fetch")
            for key, value in values.items():
                self.assertEqual(client.get(key), value)
            self.assertTrue(client.set(tail_key, b"after promotion"))

        # The new leader's log still starts at the downloaded cut; recover its suffix on restart.
        self.stop(self.learner, crash=True)
        self.start(self.learner)
        with self.client(self.learner) as client:
            self.assertEqual(self.wait_for(lambda: client.get(tail_key)), b"after promotion")
            for key, value in values.items():
                self.assertEqual(client.get(key), value)
        with redis.Redis(host=HOST, port=self.resp[self.learner], db=1) as other_db:
            self.assertEqual(other_db.get(tail_key), b"db1 incremental value")


if __name__ == "__main__":
    unittest.main()
