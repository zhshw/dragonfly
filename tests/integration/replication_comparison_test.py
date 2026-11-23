"""Compare Redis SET/WAIT and recovery with identical workloads on both data planes."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, ExitStack
import json
import math
import os
import random
import signal
from threading import Barrier
import time
import unittest

import redis

if __package__:
    from .async_replication_fixture import AsyncReplicationFixture
    from .raft_controller_test import describe
    from .raft_data_group_test import DynamicDataGroupFixture, HOST
else:
    from async_replication_fixture import AsyncReplicationFixture
    from raft_controller_test import describe
    from raft_data_group_test import DynamicDataGroupFixture, HOST


MIB = 1 << 20
BLOCK = 256 << 10
TAGS = (b"{12561}", b"{104856}")
PROBE = TAGS[0] + b":blocking"
STABLE = TAGS[1] + b":confirmed"
WAIT_MS = 500


def elapsed_ms(start):
    return round((time.monotonic() - start) * 1000, 2)


def percentile(samples, fraction):
    return sorted(samples)[math.ceil(len(samples) * fraction) - 1]


@contextmanager
def paused(process):
    process.send_signal(signal.SIGSTOP)
    try:
        _, status = os.waitpid(process.pid, os.WUNTRACED)
        if not os.WIFSTOPPED(status):
            raise AssertionError(f"{process.pid} did not stop")
        yield
    finally:
        process.send_signal(signal.SIGCONT)


class ReplicationComparison:
    """Both runners share topology/data; default to one SET in flight per client."""

    def setUp(self):
        super().setUp()
        self.leader, self.other_leader = self.leaders()

    def writer(self, leader):
        return redis.Redis(
            host=HOST, port=self.resp[leader], single_connection_client=True, socket_timeout=12
        )

    def check_other_group(self, leader):
        key = self.key_for_range(8192, 16383)
        with self.client(leader) as other:
            self.assertTrue(other.set(key, b"other group online"))
            self.assertEqual(other.get(key), b"other group online")

    def test_lagging_replica_catches_up_bulk(self):
        leader = self.leader
        mib = int(os.getenv("REPLICATION_TEST_MIB", "64"))
        clients = int(os.getenv("REPLICATION_TEST_CLIENTS", "1"))
        pipeline_depth = int(os.getenv("REPLICATION_TEST_PIPELINE", "1"))
        settle_seconds = float(os.getenv("REPLICATION_TEST_SETTLE_SECONDS", "5"))
        warmup_writes = int(os.getenv("REPLICATION_TEST_WARMUP_WRITES", "32"))
        self.assertGreater(mib, 0)
        self.assertGreater(pipeline_depth, 0)
        self.assertGreaterEqual(settle_seconds, 0)
        self.assertGreaterEqual(warmup_writes, 0)
        rng = random.Random(42)
        data = [
            (TAGS[i % 2] + f":{i}".encode(), rng.randbytes(BLOCK))
            for i in range(mib * MIB // BLOCK)
        ]

        self.assertTrue(1 <= clients <= len(data))
        warmup = [
            (TAGS[i % len(TAGS)] + f":warmup:{i}".encode(), rng.randbytes(BLOCK))
            for i in range(warmup_writes)
        ]

        def write_slice(writer, records, barrier):
            latencies = []
            barrier.wait(timeout=10)
            start = time.monotonic()
            for offset in range(0, len(records), pipeline_depth):
                sent = time.monotonic()
                chunk = records[offset : offset + pipeline_depth]
                if pipeline_depth == 1:
                    self.assertTrue(writer.set(*chunk[0]))
                else:
                    # Use the reserved writing connection so WAIT covers this exact stream.
                    connection = writer.connection
                    connection.send_packed_command(
                        connection.pack_commands(("SET", key, value) for key, value in chunk)
                    )
                    for _ in chunk:
                        self.assertTrue(writer.parse_response(connection, "SET"))
                last_ack = time.monotonic()
                latencies.append((last_ack - sent) * 1000)
            return start, last_ack, latencies

        def confirm(writer, key, value, replicas):
            self.assertTrue(writer.set(key, value))
            self.assertEqual(writer.wait(replicas, 5000), replicas)
            self.assertEqual(writer.get(key), value)

        with ExitStack() as stack:
            writers = [stack.enter_context(self.writer(leader)) for _ in range(clients)]
            for writer in writers:
                self.assertEqual(writer.info("server")["thread_count"], 2)
            other = stack.enter_context(self.writer(self.other_leader))
            other_key = self.key_for_range(8192, 16383)
            confirm(writers[0], STABLE, b"ready", 2)
            confirm(other, other_key, b"ready", 1)
            self.stop(13, crash=True)
            confirm(writers[0], PROBE, b"after stop", 1)
            confirm(other, other_key, b"after stop", 1)
            time.sleep(settle_seconds)

            with ThreadPoolExecutor(max_workers=clients) as executor:

                def write_batch(batch):
                    active = min(clients, len(batch))
                    if not active:
                        return []
                    barrier = Barrier(active)
                    pending = [
                        executor.submit(write_slice, writers[i], batch[i::clients], barrier)
                        for i in range(active)
                    ]
                    return [f.result() for f in pending]

                write_batch(warmup)
                for writer in writers[: min(clients, warmup_writes)]:
                    self.assertEqual(writer.wait(1, 5000), 1)
                samples = write_batch(data)

                # Include every connection's replica confirmation in the replication metric.
                # Reuse the writers and pool; each connection is accessed by one task at a time.
                confirmations = [executor.submit(writer.wait, 1, 5000) for writer in writers]
                for confirmation in confirmations:
                    self.assertEqual(confirmation.result(), 1)
                replication_end = time.monotonic()

            write_ms = round(
                (max(end for _, end, _ in samples) - min(s for s, _, _ in samples)) * 1000, 2
            )
            latencies = [ms for _, _, times in samples for ms in times]
            replication_ms = (replication_end - min(s for s, _, _ in samples)) * 1000
            replication_tail_ms = (replication_end - max(end for _, end, _ in samples)) * 1000
            self.assertGreaterEqual(replication_tail_ms, 0)

            # Include process launch and reconnection in time to recover. Legacy WAIT takes a
            # fresh replica snapshot per invocation, so retry if the new process is not online.
            start = time.monotonic()
            self.start(13)
            self.wait_for(lambda: all(w.wait(2, 500) == 2 for w in writers), timeout=60 + mib)
            recovery_ms = elapsed_ms(start)

        self.prepare_readback(leader)
        with self.client(13) as recovered:
            self.assertEqual(self.wait_for(lambda: recovered.get(STABLE)), b"ready")
            for key, value in warmup + data:
                self.assertTrue(recovered.get(key) == value, key)

        print(
            json.dumps(
                dict(
                    mode=self.mode,
                    clients=clients,
                    pipeline_depth=pipeline_depth,
                    payload_mib=mib,
                    keys=len(data),
                    settle_seconds=settle_seconds,
                    warmup_writes=warmup_writes,
                    readback_node=13,
                    write_ms=write_ms,
                    measure_start_ns=int(min(s for s, _, _ in samples) * 1_000_000_000),
                    measure_end_ns=int(max(end for _, end, _ in samples) * 1_000_000_000),
                    mib_per_s=round(mib * 1000 / write_ms, 2),
                    replication_ms=round(replication_ms, 2),
                    replication_mib_per_s=round(mib * 1000 / replication_ms, 2),
                    replication_tail_ms=round(replication_tail_ms, 2),
                    replication_end_ns=int(replication_end * 1_000_000_000),
                    confirmed_replicas=1,
                    first_set_p50_ms=(
                        round(percentile([times[0] for _, _, times in samples], 0.5), 2)
                        if pipeline_depth == 1
                        else None
                    ),
                    set_p50_ms=(
                        round(percentile(latencies, 0.5), 2) if pipeline_depth == 1 else None
                    ),
                    set_p95_ms=(
                        round(percentile(latencies, 0.95), 2) if pipeline_depth == 1 else None
                    ),
                    set_max_ms=round(max(latencies), 2) if pipeline_depth == 1 else None,
                    pipeline_batch_p50_ms=round(percentile(latencies, 0.5), 2),
                    pipeline_batch_p95_ms=round(percentile(latencies, 0.95), 2),
                    restart_to_wait_two_ms=recovery_ms,
                )
            ),
            flush=True,
        )

    def test_set_and_wait_with_stopped_replicas(self):
        leader = self.leader
        followers = sorted({11, 12, 13} - {leader})
        with self.writer(leader) as writer:
            self.assertTrue(writer.set(STABLE, b"confirmed"))
            self.assertEqual(writer.wait(2, 5000), 2)
            with paused(self.proc[13]):
                start = time.monotonic()
                self.assertTrue(writer.set(PROBE, b"one stopped"))
                set_ms = elapsed_ms(start)
                self.assertEqual(writer.wait(1, 5000), 1)
                start = time.monotonic()
                self.assertEqual(writer.wait(2, WAIT_MS), 1)
                wait_ms = elapsed_ms(start)
                self.assertGreaterEqual(wait_ms, WAIT_MS - 1)
            self.assertEqual(writer.wait(2, 5000), 2)

            with ExitStack() as stack:
                for node in followers:
                    stack.enter_context(paused(self.proc[node]))
                metrics = self.check_without_followers(writer, leader, self.other_leader)

        # After a long minority interval the elected Raft leader may have changed.
        def write_after_recovery():
            current, _ = self.leaders()
            with self.writer(current) as writer:
                if writer.get(STABLE) != b"confirmed":
                    return False
                return writer.set(PROBE, b"recovered") and writer.wait(2, 1000) == 2

        self.wait_for(write_after_recovery)
        print(
            json.dumps(
                dict(
                    mode=self.mode,
                    one_stopped_set_ms=set_ms,
                    one_stopped_wait_two_ms=wait_ms,
                    **metrics,
                )
            ),
            flush=True,
        )


class AsyncReplicationComparisonTest(ReplicationComparison, AsyncReplicationFixture):
    mode = "yes"

    def leaders(self):
        return 11, 21

    def prepare_readback(self, leader):
        with (self.root / "13.log").open("rb") as log:
            log.seek(self.log_offsets[13])
            self.assertIn(b"Started full sync with ", log.read())

    def check_without_followers(self, writer, leader, other):
        start = time.monotonic()
        self.assertTrue(writer.set(PROBE, b"minority"))
        set_ms = elapsed_ms(start)
        self.assertEqual(writer.get(PROBE), b"minority")
        start = time.monotonic()
        self.assertEqual(writer.wait(2, WAIT_MS), 0)
        wait_ms = elapsed_ms(start)
        self.assertGreaterEqual(wait_ms, WAIT_MS - 1)
        self.check_other_group(other)
        return dict(both_stopped_set_ms=set_ms, both_stopped_wait_two_ms=wait_ms)


class RaftReplicationComparisonTest(ReplicationComparison, DynamicDataGroupFixture):
    mode = "raft"
    data_threads = 2

    # Keep the whole backlog readable by ordinary Fetch. Snapshot recovery has a separate test.
    snapshot_bytes = {11: 1 << 62, 12: 1 << 62, 13: 1 << 62}

    def leaders(self):
        rows = self.wait_for(self.slots)
        by_port = {port: node for node, port in self.resp.items()}
        return by_port[rows[0][2][1]], by_port[rows[1][2][1]]

    def setUp(self):
        super().setUp()
        self.promote_learner(self.leader)
        self.leader, self.other_leader = self.wait_benchmark_topology()

    def prepare_readback(self, leader):
        self.assertIsNone(self.retained_checkpoint(leader, 10, 1))
        self.elect_recovered_learner(leader)

    def view(self):
        return describe(self.raft[self.leader], b"__dragonfly_data", 10)

    def wait_for_uncommitted(self, before, pending):
        observed = self.wait_for(
            lambda: (v := self.view()) and v.voters[self.leader] > before.hw and v
        )
        self.assertEqual(observed.epoch, before.epoch)
        self.assertEqual(observed.hw, before.hw)
        self.assertFalse(pending.done())
        return observed

    def test_pending_set_completes_after_second_vote(self):
        follower = next(n for n in (11, 12) if n != self.leader)
        with self.writer(self.leader) as writer, ThreadPoolExecutor(max_workers=1) as executor:
            self.assertTrue(writer.set(PROBE, b"before"))
            self.assertEqual(writer.wait(2, 5000), 2)
            with paused(self.proc[13]):
                with paused(self.proc[follower]):
                    before = self.view()
                    start = time.monotonic()
                    pending = executor.submit(writer.set, PROBE, b"after")
                    observed = self.wait_for_uncommitted(before, pending)
                    with self.client(self.leader) as reader:
                        self.assertEqual(reader.get(PROBE), b"before")
                self.assertTrue(pending.result(timeout=7))
                ack_ms = elapsed_ms(start)
                committed = self.view()
                self.assertGreaterEqual(committed.hw, observed.voters[self.leader])
                self.assertEqual(writer.get(PROBE), b"after")
                self.assertEqual(writer.wait(2, WAIT_MS), 1)
            self.assertEqual(writer.wait(2, 5000), 2)
        print(
            json.dumps(
                dict(
                    mode=self.mode,
                    before_hw=before.hw,
                    resumed_hw=committed.hw,
                    set_ms_including_pause=ack_ms,
                )
            ),
            flush=True,
        )

    def check_without_followers(self, writer, leader, other):
        before = self.view()
        self.assertEqual(before.hw, before.voters[leader])
        with ThreadPoolExecutor(max_workers=1) as executor:
            start = time.monotonic()
            pending = executor.submit(writer.set, PROBE, b"minority")
            observed = self.wait_for_uncommitted(before, pending)
            with self.client(leader) as reader:
                self.assertEqual(reader.get(PROBE), b"one stopped")
            self.check_other_group(other)
            with self.assertRaises(redis.exceptions.ResponseError) as failure:
                pending.result(timeout=10)
            error_ms = elapsed_ms(start)

        # A transport timeout does not establish that the server fenced the write.
        self.assertIn("Outcome of Raft write is unknown", str(failure.exception))
        self.wait_for(lambda: self.view() is None)
        return dict(
            both_stopped_leo=observed.voters[leader],
            both_stopped_hw=observed.hw,
            both_stopped_error_ms=error_ms,
            both_stopped_error=str(failure.exception),
        )


if __name__ == "__main__":
    unittest.main()
