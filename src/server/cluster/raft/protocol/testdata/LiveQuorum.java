// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.kafka.common.message.*;
import org.apache.kafka.common.protocol.*;
import org.apache.kafka.common.requests.RequestUtils;

// Run against an elected leader: LiveQuorum host port topic partition expectedVoters.
class LiveQuorum {
  static ByteBufferAccessor call(Socket socket, ApiKeys api, short version, Message request,
      int correlation) throws Exception {
    var header = new RequestHeaderData()
                     .setCorrelationId(correlation)
                     .setRequestApiKey(api.id)
                     .setRequestApiVersion(version)
                     .setClientId("kafka-java");
    ByteBuffer frame =
        RequestUtils.serialize(header, api.requestHeaderVersion(version), request, version);
    byte[] bytes = new byte[frame.remaining()];
    frame.get(bytes);
    var out = new DataOutputStream(socket.getOutputStream());
    out.writeInt(bytes.length);
    out.write(bytes);
    var in = new DataInputStream(socket.getInputStream());
    int size = in.readInt();
    if (size < 4 || size > (1 << 20))
      throw new AssertionError("Unexpected response size " + size);
    byte[] response = new byte[size];
    in.readFully(response);
    var reader = new ByteBufferAccessor(ByteBuffer.wrap(response));
    var reply = new ResponseHeaderData(reader, api.responseHeaderVersion(version));
    if (reply.correlationId() != correlation)
      throw new AssertionError("Response correlation ID " + reply.correlationId());
    return reader;
  }

  public static void main(String[] args) throws Exception {
    int port = Integer.parseInt(args[1]);
    int partition = Integer.parseInt(args[3]);
    int voters = Integer.parseInt(args[4]);
    try (var socket = new Socket()) {
      socket.connect(new InetSocketAddress(args[0], port), 3000);
      socket.setSoTimeout(3000);
      short version = 4;
      var versions = new ApiVersionsRequestData()
                         .setClientSoftwareName("kafka-java")
                         .setClientSoftwareVersion("4.5");
      var advertised = new ApiVersionsResponseData(
          call(socket, ApiKeys.API_VERSIONS, version, versions, 1), version);
      var api = advertised.apiKeys().find(ApiKeys.DESCRIBE_QUORUM.id);
      if (advertised.errorCode() != 0 || api == null || api.maxVersion() < 2)
        throw new AssertionError(advertised);
      var target = new DescribeQuorumRequestData.PartitionData().setPartitionIndex(partition);
      var topic = new DescribeQuorumRequestData.TopicData().setTopicName(args[2]).setPartitions(
          List.of(target));
      var request = new DescribeQuorumRequestData().setTopics(List.of(topic));
      version = 2;
      var result = new DescribeQuorumResponseData(
          call(socket, ApiKeys.DESCRIBE_QUORUM, version, request, 2), version);
      if (result.errorCode() != 0 || result.topics().size() != 1
          || !args[2].equals(result.topics().get(0).topicName()))
        throw new AssertionError(result);
      var state = result.topics().get(0).partitions().get(0);
      if (state.errorCode() != 0 || state.partitionIndex() != partition || state.leaderId() < 0
          || state.highWatermark() <= 0 || state.currentVoters().size() != voters)
        throw new AssertionError(state);
      System.out.printf("%s/%d leader=%d epoch=%d HW=%d voters=%d%n", args[2], partition,
          state.leaderId(), state.leaderEpoch(), state.highWatermark(),
          state.currentVoters().size());
    }
  }
}
