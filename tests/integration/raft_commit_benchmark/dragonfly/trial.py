"""Run one leader-local timing trial using the ordinary multi-process Raft test fixture."""

import os
from pathlib import Path
import sys
import time
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import replication_comparison_test as replication


class CommitTimingTrial(replication.RaftReplicationComparisonTest):
    def confirm(self, writer, key, value, replicas):
        self.assertTrue(writer.set(key, value))
        self.assertEqual(writer.wait(replicas, 5000), replicas)
        self.assertEqual(writer.get(key), value)

    def test_in_process(self):
        with self.writer(self.leader) as writer, self.writer(self.other_leader) as other:
            other_key = self.key_for_range(8192, 16383)
            self.confirm(writer, replication.STABLE, b"ready", 2)
            self.confirm(other, other_key, b"ready", 1)
            self.stop(13, crash=True)
            self.confirm(writer, replication.PROBE, b"after stop", 1)
            self.confirm(other, other_key, b"after stop", 1)
            time.sleep(5)

            for phase, count in ((b"warm", 32), (b"measure", 256)):
                before = self.view()
                self.assertTrue(writer.set(replication.TAGS[0] + b":start-aligned-probe", phase))
                self.assertEqual(writer.wait(1, 5000), 1)
                after = self.view()
                self.assertEqual(after.epoch, before.epoch)
                self.assertEqual(after.hw, before.hw + count)
                print(
                    f"{phase.decode()}: leader {self.leader}, epoch {before.epoch}, HW {after.hw}"
                )

            self.start(13)
            self.wait_for(lambda: writer.wait(2, 500) == 2, timeout=120)

        self.prepare_readback(self.leader)
        with (
            self.client(13) as recovered,
            open(os.environ["RAFT_COMMIT_BENCH_VALUES"], "rb") as f,
        ):
            self.assertEqual(self.wait_for(lambda: recovered.get(replication.STABLE)), b"ready")
            for i in range(32 + 256):
                key = replication.TAGS[i % 2] + b":aligned:" + str(i).encode()
                expected = f.read(256 << 10)
                self.assertEqual(len(expected), 256 << 10)
                self.assertTrue(recovered.get(key) == expected, key)
        print("Recovered node 13 and verified 288 values")


if __name__ == "__main__":
    unittest.main(defaultTest="CommitTimingTrial.test_in_process")
