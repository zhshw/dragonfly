// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

import java.nio.ByteBuffer;
import java.util.HexFormat;
import org.apache.kafka.common.message.*;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.requests.RequestUtils;

// Reproduce rpc_frame_test.cc's frames: java -cp <kafka-clients.jar> FrameVectors.java
class FrameVectors {
  static String frame(Message header, short headerVersion, Message body, short version) {
    var payload = RequestUtils.serialize(header, headerVersion, body, version);
    var bytes =
        ByteBuffer.allocate(payload.remaining() + 4).putInt(payload.remaining()).put(payload);
    return HexFormat.of().formatHex(bytes.array());
  }

  static RequestHeaderData header(ApiKeys api, short version) {
    return new RequestHeaderData()
        .setRequestApiKey(api.id)
        .setRequestApiVersion(version)
        .setCorrelationId(0x1020304)
        .setClientId("k");
  }

  static ApiVersionsResponseData.ApiVersion advertise(ApiKeys api) {
    return new ApiVersionsResponseData.ApiVersion()
        .setApiKey(api.id)
        .setMinVersion(api.oldestVersion())
        .setMaxVersion(api.latestVersion());
  }

  public static void main(String[] args) {
    var api = ApiKeys.API_VERSIONS;
    var reply = new ResponseHeaderData().setCorrelationId(0x1020304);
    for (short version : api.allVersions()) {
      var request =
          new ApiVersionsRequestData().setClientSoftwareName("df").setClientSoftwareVersion("1");
      if (version >= 5)
        request.setClusterId("c").setNodeId(42);
      var response = new ApiVersionsResponseData();
      response.apiKeys().add(advertise(api));
      response.apiKeys().add(advertise(ApiKeys.VOTE));
      System.out.println(version + " req "
          + frame(header(api, version), api.requestHeaderVersion(version), request, version));
      System.out.println(
          version + " res " + frame(reply, api.responseHeaderVersion(version), response, version));
    }
    short v = 2;
    api = ApiKeys.VOTE;
    var header = header(api, v);
    header.unknownTaggedFields().add(new RawTaggedField(6, new byte[] {(byte) 0xa5}));
    System.out.println(
        "vote req " + frame(header, api.requestHeaderVersion(v), new VoteRequestData(), v));
    System.out.println(
        "vote res " + frame(reply, api.responseHeaderVersion(v), new VoteResponseData(), v));
    var features =
        new ApiVersionsResponseData().setFinalizedFeaturesEpoch(8).setZkMigrationReady(true);
    features.supportedFeatures().add(new ApiVersionsResponseData.SupportedFeatureKey()
                                         .setName("kraft.version")
                                         .setMinVersion((short) 0)
                                         .setMaxVersion((short) 1));
    features.finalizedFeatures().add(new ApiVersionsResponseData.FinalizedFeatureKey()
                                         .setName("kraft.version")
                                         .setMinVersionLevel((short) 1)
                                         .setMaxVersionLevel((short) 1));
    System.out.println("features res " + frame(reply, (short) 0, features, (short) 4));
  }
}
