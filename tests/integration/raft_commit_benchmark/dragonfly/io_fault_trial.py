"""Use the --faults probe to hold leader barriers while a restarted follower persists the tail."""

from concurrent.futures import ThreadPoolExecutor
import os
import signal
import time
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import replication_comparison_test as replication


class AsyncBarrierTrial(replication.RaftReplicationComparisonTest):
    def test_leader_waits_for_its_own_barriers(self):
        controls = Path(os.environ["RAFT_IO_FAULT_DIR"])
        follower = next(node for node in (11, 12) if node != self.leader)
        with self.writer(self.leader) as writer, ThreadPoolExecutor(max_workers=1) as executor:
            self.assertTrue(writer.set(replication.STABLE, b"confirmed"))
            self.assertEqual(writer.wait(2, 5000), 2)
            self.stop(13, crash=True)
            for stage in ("segment", "marker"):
                before = self.view()
                self.stop(follower, crash=True)
                control = controls / f"hold-{self.leader}-{stage}"
                control.touch()
                key = replication.TAGS[0] + b":held:" + stage.encode()
                value = b"x" * replication.BLOCK
                pending = executor.submit(writer.set, key, value)
                try:
                    self.wait_for(lambda: Path(str(control) + ".entered").exists(), timeout=3)

                    # No old parked Fetch survives this restart. Its new Fetch must run while
                    # the leader still has the selected durability barrier in flight.
                    self.start(follower)
                    published = controls / f"published-{follower}"

                    def durable_tail():
                        return published.exists() and int(published.read_text()) >= before.hw + 1

                    self.wait_for(durable_tail, timeout=5)
                    self.assertFalse(pending.done(), "Follower alone acknowledged the write")
                finally:
                    control.unlink(missing_ok=True)
                self.assertTrue(pending.result(timeout=7))
                self.assertEqual(writer.wait(1, 5000), 1)
                self.assertEqual(writer.get(key), value)
                print(f"{stage}: restarted follower durable before leader; OK waited for leader")

            control = controls / f"hold-{self.leader}-segment"
            entered = Path(str(control) + ".entered")
            entered.unlink(missing_ok=True)
            control.touch()
            pending = executor.submit(writer.set, replication.PROBE, b"shutdown in flight")
            process = self.proc[self.leader]
            try:
                self.wait_for(entered.exists, timeout=3)
                process.send_signal(signal.SIGTERM)
                time.sleep(0.1)
                self.assertIsNone(process.poll(), "Shutdown did not drain the outstanding I/O")
            finally:
                control.unlink(missing_ok=True)
            self.assertEqual(process.wait(timeout=10), 0)
            pending.exception(timeout=10)
            print("shutdown: process drained the held I/O before exiting")


if __name__ == "__main__":
    unittest.main(defaultTest="AsyncBarrierTrial.test_leader_waits_for_its_own_barriers")
