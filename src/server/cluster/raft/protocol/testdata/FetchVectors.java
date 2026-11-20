// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.*;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.requests.FetchRequest;

// Compile together with FrameVectors.java using the sibling Kafka client's jar.
class FetchVectors {
  public static void main(String[] args) {
    var api = ApiKeys.FETCH;
    for (short version = 4; version <= 18; ++version) {
      var id = new Uuid(0, 20);
      var part = new FetchRequestData.FetchPartition()
                     .setPartition(10)
                     .setCurrentLeaderEpoch(7)
                     .setFetchOffset(333)
                     .setReplicaDirectoryId(new Uuid(0, 30))
                     .setHighWatermark(222);
      if (version >= 12)
        part.setLastFetchedEpoch(5);
      var request =
          new FetchRequestData()
              .setClusterId("c")
              .setMaxBytes(4096)
              .setMaxWaitMs(250)
              .setReplicaState(new FetchRequestData.ReplicaState().setReplicaId(1))
              .setTopics(List.of(
                  new FetchRequestData.FetchTopic().setTopic("t").setTopicId(id).setPartitions(
                      List.of(part))));
      var responsePart =
          new FetchResponseData.PartitionData()
              .setPartitionIndex(10)
              .setHighWatermark(350)
              .setRecords(MemoryRecords.readableRecords(ByteBuffer.wrap(new byte[] {1, 2, 3})));
      if (version >= 12)
        responsePart.currentLeader().setLeaderId(2).setLeaderEpoch(7);
      var response = new FetchResponseData().setResponses(List.of(
          new FetchResponseData.FetchableTopicResponse().setTopic("t").setTopicId(id).setPartitions(
              List.of(responsePart))));
      // The SimpleBuilder moves replicaId out of ReplicaState before v15.
      var wireRequest = new FetchRequest.SimpleBuilder(request).build(version).data();
      System.out.println(version + " req "
          + FrameVectors.frame(FrameVectors.header(api, version), api.requestHeaderVersion(version),
              wireRequest, version));
      System.out.println(version + " res "
          + FrameVectors.frame(new ResponseHeaderData().setCorrelationId(0x1020304),
              api.responseHeaderVersion(version), response, version));
    }
  }
}
