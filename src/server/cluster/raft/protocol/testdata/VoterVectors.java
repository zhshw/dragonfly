// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

import java.nio.ByteBuffer;
import java.util.HexFormat;
import java.util.List;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.*;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.internal.MemoryRecords;

// Compile with QuorumVectors.java and FrameVectors.java against the pinned Kafka client.
class VoterVectors {
  static void batch(MemoryRecords records) {
    ByteBuffer buf = records.buffer();
    byte[] bytes = new byte[buf.remaining()];
    buf.get(bytes);
    System.out.println(HexFormat.of().formatHex(bytes));
  }
  public static void main(String[] args) {
    var id = new Uuid(0, 2);
    var voter = new VotersRecord.Voter().setVoterId(1).setVoterDirectoryId(id);
    voter.endpoints().add(new VotersRecord.Endpoint().setName("RAFT").setHost("b").setPort(9001));
    voter.kRaftVersionFeature().setMinSupportedVersion((short) 0).setMaxSupportedVersion((short) 1);
    var members = new VotersRecord().setVoters(List.of(voter));
    batch(MemoryRecords.withKRaftVersionRecord(
        0, 123, 2, ByteBuffer.allocate(4096), new KRaftVersionRecord().setKRaftVersion((short) 1)));
    batch(MemoryRecords.withVotersRecord(1, 123, 2, ByteBuffer.allocate(4096), members));
    for (short v : ApiKeys.ADD_RAFT_VOTER.allVersions()) {
      var add = new AddRaftVoterRequestData()
                    .setClusterId("c")
                    .setVoterId(1)
                    .setVoterDirectoryId(id)
                    .setTimeoutMs(500)
                    .setAckWhenCommitted(v == 0);
      add.listeners().add(
          new AddRaftVoterRequestData.Listener().setName("RAFT").setHost("b").setPort(9001));
      QuorumVectors.emit(ApiKeys.ADD_RAFT_VOTER, v, add,
          new AddRaftVoterResponseData().setErrorCode((short) 126).setErrorMessage("duplicate"));
    }
    QuorumVectors.emit(ApiKeys.REMOVE_RAFT_VOTER, (short) 0,
        new RemoveRaftVoterRequestData().setClusterId("c").setVoterId(1).setVoterDirectoryId(id),
        new RemoveRaftVoterResponseData().setErrorCode((short) 127));
    var update = new UpdateRaftVoterRequestData()
                     .setClusterId("c")
                     .setCurrentLeaderEpoch(3)
                     .setVoterId(1)
                     .setVoterDirectoryId(id);
    update.listeners().add(
        new UpdateRaftVoterRequestData.Listener().setName("RAFT").setHost("b").setPort(9001));
    update.kRaftVersionFeature().setMaxSupportedVersion((short) 1);
    var result = new UpdateRaftVoterResponseData();
    result.currentLeader().setLeaderId(2).setLeaderEpoch(3).setHost("a").setPort(9000);
    QuorumVectors.emit(ApiKeys.UPDATE_RAFT_VOTER, (short) 0, update, result);
  }
}
