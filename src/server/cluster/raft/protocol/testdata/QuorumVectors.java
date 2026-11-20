// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.*;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.record.internal.MemoryRecords;

// Compile together with FrameVectors.java using the sibling Kafka client's jar.
class QuorumVectors {
  static void emit(ApiKeys api, short version, Message request, Message response) {
    var header = FrameVectors.header(api, version);
    var reply = new ResponseHeaderData().setCorrelationId(0x1020304);
    System.out.print(api + " " + version + " ");
    System.out.print(
        FrameVectors.frame(header, api.requestHeaderVersion(version), request, version));
    System.out.println(
        " " + FrameVectors.frame(reply, api.responseHeaderVersion(version), response, version));
  }
  public static void main(String[] args) {
    for (short version : ApiKeys.DESCRIBE_QUORUM.allVersions()) {
      var request = new DescribeQuorumRequestData().setTopics(
          List.of(new DescribeQuorumRequestData.TopicData().setTopicName("t").setPartitions(
              List.of(new DescribeQuorumRequestData.PartitionData().setPartitionIndex(42)))));
      var replica =
          new DescribeQuorumResponseData.ReplicaState().setReplicaId(1).setLogEndOffset(102);
      if (version >= 1)
        replica.setLastFetchTimestamp(100).setLastCaughtUpTimestamp(99);
      var result = new DescribeQuorumResponseData.PartitionData()
                       .setPartitionIndex(42)
                       .setLeaderId(1)
                       .setLeaderEpoch(3)
                       .setHighWatermark(101)
                       .setCurrentVoters(List.of(replica));
      var response = new DescribeQuorumResponseData().setTopics(
          List.of(new DescribeQuorumResponseData.TopicData().setTopicName("t").setPartitions(
              List.of(result))));
      if (version == 2) {
        replica.setReplicaDirectoryId(new Uuid(0, 88));
        var node = new DescribeQuorumResponseData.Node().setNodeId(1);
        node.listeners().add(
            new DescribeQuorumResponseData.Listener().setName("r").setHost("h").setPort(9092));
        response.nodes().add(node);
      }
      emit(ApiKeys.DESCRIBE_QUORUM, version, request, response);
    }
    for (short version : ApiKeys.FETCH_SNAPSHOT.allVersions()) {
      var part = new FetchSnapshotRequestData.PartitionSnapshot()
                     .setPartition(42)
                     .setCurrentLeaderEpoch(3)
                     .setPosition(3)
                     .setSnapshotId(
                         new FetchSnapshotRequestData.SnapshotId().setEpoch(2).setEndOffset(45));
      if (version == 1)
        part.setReplicaDirectoryId(new Uuid(0, 77));
      var request =
          new FetchSnapshotRequestData().setClusterId("c").setMaxBytes(3).setReplicaId(2).setTopics(
              List.of(new FetchSnapshotRequestData.TopicSnapshot().setName("t").setPartitions(
                  List.of(part))));
      var response = new FetchSnapshotResponseData().setTopics(
          List.of(new FetchSnapshotResponseData.TopicSnapshot().setName("t").setPartitions(List.of(
              new FetchSnapshotResponseData.PartitionSnapshot()
                  .setIndex(42)
                  .setPosition(3)
                  .setSize(16)
                  .setSnapshotId(
                      new FetchSnapshotResponseData.SnapshotId().setEpoch(2).setEndOffset(45))
                  .setUnalignedRecords(
                      MemoryRecords.readableRecords(ByteBuffer.wrap(new byte[] {1, 2, 3})))))));
      if (version == 1)
        response.nodeEndpoints().add(
            new FetchSnapshotResponseData.NodeEndpoint().setNodeId(1).setHost("h").setPort(9092));
      emit(ApiKeys.FETCH_SNAPSHOT, version, request, response);
    }
  }
}
