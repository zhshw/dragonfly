// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

import java.util.HexFormat;
import java.util.List;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.VoteRequestData;
import org.apache.kafka.common.message.VoteResponseData;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.MessageUtil;

// Reproduce vote_test.cc's golden bodies with the sibling Kafka checkout:
// java -cp <kafka-clients.jar> VoteVectors.java
class VoteVectors {
  static String hex(Message message, short version) {
    return HexFormat.of().formatHex(
        MessageUtil.byteBufferToArray(MessageUtil.toByteBufferAccessor(message, version).buffer()));
  }

  public static void main(String[] args) {
    for (short version = 0; version <= 2; version++) {
      // Same inputs as RequestResponseTest.createVoteRequest/createVoteResponse.
      var partition = new VoteRequestData.PartitionData()
                          .setPartitionIndex(0)
                          .setReplicaEpoch(1)
                          .setReplicaId(2)
                          .setLastOffset(3L)
                          .setLastOffsetEpoch(4)
                          .setPreVote(version >= 2);
      var request = new VoteRequestData()
                        .setClusterId("clusterId")
                        .setTopics(List.of(
                            new VoteRequestData.TopicData().setTopicName("topic1").setPartitions(
                                List.of(partition))));
      var result = new VoteResponseData.PartitionData()
                       .setPartitionIndex(1)
                       .setLeaderId(2)
                       .setLeaderEpoch(0)
                       .setVoteGranted(false);
      var response = new VoteResponseData().setTopics(
          List.of(new VoteResponseData.TopicData().setPartitions(List.of(result))));
      if (version >= 1) {
        response.nodeEndpoints().add(
            new VoteResponseData.NodeEndpoint().setNodeId(2).setHost("localhost").setPort(9092));
      }
      System.out.println(version + " request " + hex(request, version));
      System.out.println(version + " response " + hex(response, version));
    }
    var partition = new VoteRequestData.PartitionData()
                        .setReplicaId(3)
                        .setReplicaEpoch(7)
                        .setLastOffset(0x0102030405060708L)
                        .setReplicaDirectoryId(new Uuid(0x1122334455667788L, 0x99aabbccddeeff00L))
                        .setVoterDirectoryId(new Uuid(0xffeeddccbbaa0099L, 0x8877665544332211L));
    var request = new VoteRequestData().setVoterId(2).setTopics(List.of(
        new VoteRequestData.TopicData().setTopicName("组").setPartitions(List.of(partition))));
    var response = new VoteResponseData().setErrorCode((short) -1);
    response.nodeEndpoints().add(
        new VoteResponseData.NodeEndpoint().setNodeId(2).setHost("::1").setPort(65535));
    System.out.println("1 directory-ids " + hex(request, (short) 1));
    System.out.println("1 error-response " + hex(response, (short) 1));
  }
}
