// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.List;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.LeaderChangeMessage;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.SimpleRecord;

// Regenerates RecordBatchTest fixtures with the pinned Kafka clients jar.
class BatchVectors {
  static String hex(MemoryRecords batch) {
    ByteBuffer buf = batch.buffer();
    byte[] bytes = new byte[buf.remaining()];
    buf.get(bytes);
    return HexFormat.of().formatHex(bytes);
  }

  public static void main(String[] args) {
    var voters = List.of(1, 2, 3)
                     .stream()
                     .map(id -> new LeaderChangeMessage.Voter().setVoterId(id))
                     .toList();
    var message = new LeaderChangeMessage()
                      .setVersion((short) 0)
                      .setLeaderId(1)
                      .setVoters(voters)
                      .setGrantingVoters(voters.subList(0, 2));
    System.out.println(
        hex(MemoryRecords.withLeaderChangeMessage(0, 123, 2, ByteBuffer.allocate(4096), message)));
    System.out.println(hex(MemoryRecords.withRecords(1, Compression.NONE, 2,
        new SimpleRecord(456, "foo".getBytes(StandardCharsets.UTF_8)),
        new SimpleRecord(456, "bar".getBytes(StandardCharsets.UTF_8)))));
  }
}
