"""Interrupt a real FetchSnapshot before the downloaded checkpoint is published."""

import hashlib
import json
import os
import signal
import time
import unittest

import redis

if __package__:
    from .raft_controller_test import describe
    from .raft_data_group_test import DynamicDataGroupFixture, GROUPS, HOST
else:
    from raft_controller_test import describe
    from raft_data_group_test import DynamicDataGroupFixture, GROUPS, HOST


MIB = 1 << 20
VALUE_BYTES = 256 << 10
TAGS = (b"{12561}", b"{104856}")  # Group 10, one tag on each engine shard.


def file_digest(path):
    digest = hashlib.sha256()
    with path.open("rb") as image:
        while data := image.read(MIB):
            digest.update(data)
    return digest.digest()


class LargeSnapshotStallTest(DynamicDataGroupFixture):
    data_threads = 2

    def setUp(self):
        self.payload_mib = int(os.environ.get("REPLICATION_TEST_MIB", "64"))
        if self.payload_mib < 4:
            raise ValueError("REPLICATION_TEST_MIB must cover at least four 1 MiB chunks")
        self.key_count = self.payload_mib * MIB // VALUE_BYTES

        # Leave room for record headers. Rewrites of the last key trigger compaction only after
        # every distinct value has committed. Disable local snapshot generation on the downloader.
        trigger = self.payload_mib * MIB + self.key_count * 1024
        self.snapshot_bytes = {11: trigger, 12: trigger, self.learner: 1 << 62}
        super().setUp()

    def test_commits_while_a_voter_is_stalled_mid_snapshot(self):
        rows = self.wait_for(self.slots)
        leader = next(n for n in GROUPS[10] if self.resp[n] == rows[0][2][1])
        self.promote_learner(leader)
        lagging = self.learner
        self.stop(lagging, crash=True)
        expected = {}
        with self.client(leader) as source:
            for i in range(self.key_count):
                key = TAGS[i % len(TAGS)] + f":blob{i}".encode()
                value = os.urandom(VALUE_BYTES)
                expected[key] = hashlib.sha256(value).digest()
                self.assertTrue(source.set(key, value))
            full_hw = describe(self.raft[leader], b"__dragonfly_data", 10).hw
            for _ in range((self.key_count * 1024 + VALUE_BYTES - 1) // VALUE_BYTES):
                self.assertTrue(source.set(key, value))
        checkpoint = self.wait_for(lambda: self.retained_checkpoint(leader, 10, full_hw))
        size = checkpoint.stat().st_size
        self.assertGreater(size, self.payload_mib * MIB)
        digest = file_digest(checkpoint)
        directory = self.root / str(lagging) / "__dragonfly_data-10"
        installed = directory / checkpoint.name

        def partial_download():
            self.assertFalse(installed.exists(), "snapshot was installed before the voter paused")
            for path in directory.glob(f"{checkpoint.stem}-*.checkpoint.part"):
                try:
                    written = path.stat().st_size
                except FileNotFoundError:
                    continue
                if MIB <= written < size:
                    process.send_signal(signal.SIGSTOP)
                    return path

        self.start(lagging)
        process = self.proc[lagging]
        try:
            partial = self.wait_for(partial_download, poll_interval=0.005)
            _, status = os.waitpid(process.pid, os.WUNTRACED)
            self.assertTrue(os.WIFSTOPPED(status))
            self.assertFalse(installed.exists())
            self.assertTrue(partial.is_file())
            partial_bytes = partial.stat().st_size
            self.assertGreaterEqual(partial_bytes, MIB)
            self.assertLess(partial_bytes, size)
            self.assertIsNone(self.retained_checkpoint(lagging, 10, full_hw))

            # WAIT must use the same connection as the tail write. One healthy follower can
            # acknowledge it while the stopped process cannot advance its applied offset.
            with redis.Redis(
                host=HOST, port=self.resp[leader], single_connection_client=True, socket_timeout=7
            ) as client:
                tail = b"{12561}:written-while-stopped"
                start = time.monotonic()
                self.assertTrue(client.set(tail, b"majority"))
                set_ms = (time.monotonic() - start) * 1000
                self.assertEqual(client.wait(1, 5000), 1)
                start = time.monotonic()
                self.assertEqual(client.wait(2, 500), 1)
                wait_ms = (time.monotonic() - start) * 1000
                self.assertGreaterEqual(wait_ms, 499)
                self.assertEqual(client.get(tail), b"majority")
                self.assertEqual(partial.stat().st_size, partial_bytes)
        finally:
            self.stop(lagging, crash=True)

        # SIGKILL leaves the partial on disk. Restart must discard it, install the same
        # immutable snapshot as the leader, and apply the tail using ordinary Fetch.
        self.assertTrue(partial.exists())
        self.assertFalse(installed.exists())
        start = time.monotonic()
        self.start(lagging)
        with redis.Redis(
            host=HOST,
            port=self.resp[leader],
            single_connection_client=True,
            socket_timeout=65 + self.payload_mib,
        ) as client:
            suffix = b"{104856}:after-restart"
            self.assertTrue(client.set(suffix, b"incremental"))
            self.assertEqual(client.wait(2, (60 + self.payload_mib) * 1000), 2)
        recovered_s = time.monotonic() - start
        self.assertFalse(partial.exists())
        self.assertEqual(self.retained_checkpoint(lagging, 10, full_hw), installed)
        self.assertEqual(file_digest(installed), digest)

        # Data followers redirect GET. Elect the recovered voter to inspect its own KV image.
        self.elect_recovered_learner(leader)
        with self.client(lagging) as recovered:
            self.assertEqual(self.wait_for(lambda: recovered.get(suffix)), b"incremental")
            self.assertEqual(recovered.get(tail), b"majority")
            for key, original in expected.items():
                self.assertEqual(hashlib.sha256(recovered.get(key)).digest(), original, key)
        print(
            json.dumps(
                dict(
                    payload_mib=self.payload_mib,
                    keys=self.key_count,
                    value_bytes=VALUE_BYTES,
                    checkpoint_bytes=size,
                    stopped_after_bytes=partial_bytes,
                    majority_set_ms=round(set_ms, 2),
                    wait_two_timeout_ms=round(wait_ms, 2),
                    restart_to_wait_two_s=round(recovered_s, 3),
                )
            ),
            flush=True,
        )


if __name__ == "__main__":
    unittest.main()
