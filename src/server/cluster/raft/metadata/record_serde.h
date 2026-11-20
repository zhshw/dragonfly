// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/status/statusor.h>

#include <variant>

#include "server/cluster/raft/protocol/metadata_records.h"
#include "server/cluster/raft/protocol/slot_records.h"

namespace dfly::cluster::raft {

using MetadataPayload =
    std::variant<protocol::RegisterBrokerRecord, protocol::UnregisterBrokerRecord,
                 protocol::FenceBrokerRecord, protocol::UnfenceBrokerRecord,
                 protocol::FeatureLevelRecord, protocol::BrokerRegistrationChangeRecord,
                 protocol::NoOpRecord, protocol::BeginTransactionRecord,
                 protocol::EndTransactionRecord, protocol::AbortTransactionRecord,
                 protocol::RedisNodeRecord, protocol::DataGroupRecord, protocol::GroupLeaderRecord,
                 protocol::SlotAssignmentRecord>;

struct MetadataRecord {
  MetadataPayload message;
  int16_t version = 0;
  bool operator==(const MetadataRecord&) const = default;
};

// Encode the value of a metadata data record with Kafka's frame-version, type and
// record-version prefix. The surrounding Kafka record batch carries its offset and CRC.
absl::StatusOr<std::string> EncodeMetadataRecord(const MetadataRecord& record);
absl::StatusOr<MetadataRecord> DecodeMetadataRecord(std::string_view bytes);
int16_t RegisterBrokerRecordVersion(int16_t metadata_version);

}  // namespace dfly::cluster::raft
