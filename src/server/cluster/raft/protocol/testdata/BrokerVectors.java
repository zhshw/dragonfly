// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

import java.util.List;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.*;
import org.apache.kafka.common.protocol.*;

// Compile alongside FrameVectors.java against the pinned kafka-clients jar.
class BrokerVectors {
  static void print(ApiKeys key, short version, Message request, Message response) {
    System.out.println(FrameVectors.frame(
        FrameVectors.header(key, version), key.requestHeaderVersion(version), request, version));
    System.out.println(FrameVectors.frame(new ResponseHeaderData().setCorrelationId(0x1020304),
        key.responseHeaderVersion(version), response, version));
  }

  public static void main(String[] args) {
    var uuid = new Uuid(0, 123);
    for (short v : ApiKeys.BROKER_REGISTRATION.allVersions()) {
      var request = new BrokerRegistrationRequestData()
                        .setBrokerId(7)
                        .setClusterId("c")
                        .setIncarnationId(uuid)
                        .setRack(null);
      request.listeners().add(
          new BrokerRegistrationRequestData.Listener().setName("r").setHost("h").setPort(9092));
      request.features().add(new BrokerRegistrationRequestData.Feature()
                                 .setName("f")
                                 .setMinSupportedVersion((short) 1)
                                 .setMaxSupportedVersion((short) 3));
      if (v >= 1)
        request.setIsMigratingZkBroker(true);
      if (v >= 2)
        request.setLogDirs(List.of(uuid));
      if (v >= 3)
        request.setPreviousBrokerEpoch(9);
      print(ApiKeys.BROKER_REGISTRATION, v, request,
          new BrokerRegistrationResponseData().setBrokerEpoch(24));
    }
    for (short v : ApiKeys.BROKER_HEARTBEAT.allVersions()) {
      var request = new BrokerHeartbeatRequestData()
                        .setBrokerId(7)
                        .setBrokerEpoch(24)
                        .setCurrentMetadataOffset(25)
                        .setWantFence(true);
      if (v >= 1)
        request.setOfflineLogDirs(List.of(uuid));
      if (v >= 2)
        request.setCordonedLogDirs(List.of());
      print(ApiKeys.BROKER_HEARTBEAT, v, request,
          new BrokerHeartbeatResponseData().setErrorCode((short) 77));
    }
  }
}
