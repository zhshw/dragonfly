// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

import java.util.List;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.*;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Message;

class EpochVectors {
  static void print(ApiKeys api, short version, Message req, Message res) {
    var id = FrameVectors.header(api, version);
    var reply = new ResponseHeaderData().setCorrelationId(id.correlationId());
    System.out.println(api + " v" + version + " request "
        + FrameVectors.frame(id, api.requestHeaderVersion(version), req, version));
    System.out.println(api + " v" + version + " response "
        + FrameVectors.frame(reply, api.responseHeaderVersion(version), res, version));
  }
  public static void main(String[] args) {
    for (short version = 0; version <= 1; version++) {
      var partition = new BeginQuorumEpochRequestData.PartitionData()
                          .setPartitionIndex(2)
                          .setLeaderId(1)
                          .setLeaderEpoch(7);
      var begin = new BeginQuorumEpochRequestData().setClusterId("c").setTopics(
          List.of(new BeginQuorumEpochRequestData.TopicData().setTopicName("t").setPartitions(
              List.of(partition))));
      var beginReply = new BeginQuorumEpochResponseData();
      var endPartition = new EndQuorumEpochRequestData.PartitionData()
                             .setPartitionIndex(2)
                             .setLeaderId(1)
                             .setLeaderEpoch(7)
                             .setPreferredSuccessors(List.of(2, 3));
      var end = new EndQuorumEpochRequestData().setClusterId("c").setTopics(
          List.of(new EndQuorumEpochRequestData.TopicData().setTopicName("t").setPartitions(
              List.of(endPartition))));
      var endReply = new EndQuorumEpochResponseData();
      if (version == 1) {
        var directory = new Uuid(0, 42);
        endPartition.setPreferredCandidates(List.of(
            new EndQuorumEpochRequestData.ReplicaInfo().setCandidateId(2).setCandidateDirectoryId(
                directory)));
        partition.setVoterDirectoryId(directory);
        begin.setVoterId(2);
        begin.leaderEndpoints().add(
            new BeginQuorumEpochRequestData.LeaderEndpoint().setName("r").setHost("h").setPort(
                9092));
        endReply.nodeEndpoints().add(
            new EndQuorumEpochResponseData.NodeEndpoint().setNodeId(2).setHost("h").setPort(9092));
      }
      print(ApiKeys.BEGIN_QUORUM_EPOCH, version, begin, beginReply);
      print(ApiKeys.END_QUORUM_EPOCH, version, end, endReply);
    }
  }
}
