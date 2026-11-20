// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

import java.nio.ByteBuffer;
import java.util.HexFormat;
import java.util.List;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.*;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.metadata.MetadataRecordSerde;
import org.apache.kafka.server.common.ApiMessageAndVersion;

public class MetadataVectors {
  private static void emit(ApiMessage msg, short version) {
    var serde = MetadataRecordSerde.INSTANCE;
    var record = new ApiMessageAndVersion(msg, version);
    var cache = new ObjectSerializationCache();
    var bytes = ByteBuffer.allocate(serde.recordSize(record, cache));
    serde.write(record, cache, new ByteBufferAccessor(bytes));
    bytes.flip();
    if (!record.equals(serde.read(new ByteBufferAccessor(bytes), bytes.remaining())))
      throw new AssertionError();
    System.out.println(HexFormat.of().formatHex(bytes.array()));
  }

  public static void main(String[] args) {
    for (short v = 0; v <= RegisterBrokerRecord.HIGHEST_SUPPORTED_VERSION; v++) {
      var reg = new RegisterBrokerRecord().setBrokerId(2).setBrokerEpoch(12).setIncarnationId(
          new Uuid(0, 7));
      var endpoint =
          new RegisterBrokerRecord.BrokerEndpoint().setName("RESP").setHost("db").setPort(6379);
      endpoint.unknownTaggedFields().add(new RawTaggedField(8, new byte[] {0x33}));
      reg.endPoints().add(endpoint);
      reg.features().add(new RegisterBrokerRecord.BrokerFeature()
                             .setName("dragonfly.cluster")
                             .setMinSupportedVersion((short) 0)
                             .setMaxSupportedVersion((short) 1));
      if (v >= 1)
        reg.setInControlledShutdown(true).setRack(null);
      if (v >= 2)
        reg.setIsMigratingZkBroker(true);
      if (v >= 3)
        reg.setLogDirs(List.of(new Uuid(0, 3)));
      if (v >= 4)
        reg.setCordonedLogDirs(List.of());
      emit(reg, v);
    }
    for (short v = 0; v <= BrokerRegistrationChangeRecord.HIGHEST_SUPPORTED_VERSION; v++) {
      var change = new BrokerRegistrationChangeRecord().setBrokerId(2).setBrokerEpoch(12).setFenced(
          (byte) -1);
      if (v >= 1)
        change.setInControlledShutdown((byte) 1);
      if (v >= 2)
        change.setLogDirs(List.of(new Uuid(0, 3)));
      if (v >= 3)
        change.setCordonedLogDirs(List.of());
      emit(change, v);
    }
    emit(new FeatureLevelRecord().setName("metadata.version").setFeatureLevel((short) 33),
        (short) 0);
    emit(new BeginTransactionRecord().setName("slots"), (short) 0);
    emit(new EndTransactionRecord(), (short) 0);
    emit(new AbortTransactionRecord().setReason("stale"), (short) 0);
    emit(new NoOpRecord(), (short) 0);
    emit(new FenceBrokerRecord().setId(2).setEpoch(12), (short) 0);
    emit(new UnfenceBrokerRecord().setId(2).setEpoch(12), (short) 0);
    emit(new UnregisterBrokerRecord().setBrokerId(2).setBrokerEpoch(12), (short) 0);
  }
}
