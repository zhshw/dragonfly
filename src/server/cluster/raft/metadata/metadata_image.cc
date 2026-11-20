// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/metadata/metadata_image.h"

#include <absl/strings/str_cat.h>

#include "server/cluster/raft/log/partition_log.h"
#include "server/cluster/raft/log/record_batch.h"

namespace dfly::cluster::raft {
namespace {
using namespace std;
using namespace protocol;

absl::StatusOr<RegisterBrokerRecord*> Lookup(MetadataImage* image, int32_t id, int64_t epoch) {
  auto it = image->brokers.find(id);
  if (it == image->brokers.end() || it->second.broker_epoch != epoch)
    return absl::DataLossError(
        absl::StrCat("Metadata references missing broker ", id, " at epoch ", epoch));
  return &it->second;
}

absl::Status Replay(const FeatureLevelRecord& record, MetadataImage* image) {
  // The pinned MetadataVersion enum supports 3.3-IV3 through 4.5-IV0.
  if (record.name == "metadata.version" && (record.feature_level < 7 || record.feature_level > 34))
    return absl::UnimplementedError("Unsupported metadata.version feature level");
  if (record.feature_level < 0)
    return absl::DataLossError("Negative metadata feature level");
  if (record.feature_level == 0)
    image->features.erase(record.name);
  else
    image->features[record.name] = record.feature_level;
  return absl::OkStatus();
}

absl::Status Replay(const RegisterBrokerRecord& record, MetadataImage* image) {
  image->brokers[record.broker_id] = record;
  return absl::OkStatus();
}

absl::Status Replay(const UnregisterBrokerRecord& record, MetadataImage* image) {
  image->brokers.erase(record.broker_id);
  return absl::OkStatus();
}

absl::Status Replay(const BrokerRegistrationChangeRecord& record, MetadataImage* image) {
  auto broker = Lookup(image, record.broker_id, record.broker_epoch);
  if (!broker.ok())
    return broker.status();
  if (record.fenced < -1 || record.fenced > 1 || record.in_controlled_shutdown < 0 ||
      record.in_controlled_shutdown > 1)
    return absl::DataLossError("Unknown broker registration change value");
  if (record.fenced != 0)
    (*broker)->fenced = record.fenced == 1;
  if (record.in_controlled_shutdown == 1)
    (*broker)->in_controlled_shutdown = true;
  if (!record.log_dirs.empty())
    (*broker)->log_dirs = record.log_dirs;
  if (record.cordoned_log_dirs)
    (*broker)->cordoned_log_dirs = record.cordoned_log_dirs;
  return absl::OkStatus();
}

template <typename T>
requires(std::is_same_v<T, FenceBrokerRecord> ||
         std::is_same_v<T, UnfenceBrokerRecord>) absl::Status
    Replay(const T& record, MetadataImage* image) {
  auto broker = Lookup(image, record.id, record.epoch);
  if (!broker.ok())
    return broker.status();
  (*broker)->fenced = std::is_same_v<T, FenceBrokerRecord>;
  return absl::OkStatus();
}

absl::Status Replay(const NoOpRecord&, MetadataImage*) {
  return absl::OkStatus();
}
}  // namespace

absl::Status MetadataLoader::ApplyBatch(string_view bytes) {
  vector<RecordView> records;
  auto batch = DecodeRecordBatch(bytes, &records);
  if (!batch.ok())
    return batch.status();
  if (batch->base_offset != consumed_end_offset_)
    return absl::DataLossError("Non-contiguous metadata batch");

  // Keep the cursor and both published and transactional images untouched until this
  // entire batch parses. Its prefix before a Begin can be published while the txn is open.
  auto stable = image_;
  bool in_transaction = transaction_ != nullptr;
  auto working = make_unique<MetadataImage>(in_transaction ? *transaction_ : *image_);
  if (!batch->control) {
    for (const auto& rec : records) {
      if (!rec.value)
        return absl::DataLossError("Missing metadata record payload");
      auto decoded = DecodeMetadataRecord(*rec.value);
      if (!decoded.ok())
        return decoded.status();
      auto replayed = visit(
          [&](const auto& message) -> absl::Status {
            using T = decay_t<decltype(message)>;
            if constexpr (is_same_v<T, BeginTransactionRecord>) {
              if (in_transaction)
                return absl::DataLossError("Nested metadata transaction");
              auto mv = working->features.find("metadata.version");
              if (mv != working->features.end() && mv->second < 13)
                return absl::FailedPreconditionError(
                    "metadata.version does not support transactions");
              if (rec.offset != stable->stable_end_offset) {
                working->stable_end_offset = rec.offset;
                stable = make_shared<MetadataImage>(*working);
              }
              in_transaction = true;
            } else if constexpr (is_same_v<T, EndTransactionRecord> ||
                                 is_same_v<T, AbortTransactionRecord>) {
              if (!in_transaction)
                return absl::DataLossError("Metadata transaction terminator without a begin");
              if constexpr (is_same_v<T, AbortTransactionRecord>)
                *working = *stable;
              in_transaction = false;
            } else {
              return Replay(message, working.get());
            }
            return absl::OkStatus();
          },
          decoded->message);
      if (!replayed.ok())
        return replayed;
    }
  }

  if (in_transaction) {
    transaction_ = std::move(working);
    image_ = std::move(stable);
  } else {
    transaction_.reset();
    working->stable_end_offset = batch->end.end_offset;
    image_ = std::move(working);
  }
  consumed_end_offset_ = batch->end.end_offset;
  return absl::OkStatus();
}

absl::Status MetadataLoader::Advance(const PartitionLog& log, int64_t high_watermark) {
  if (high_watermark < 0 || high_watermark > log.End().end_offset)
    return absl::OutOfRangeError("Metadata HW is outside the local log");
  if (consumed_end_offset_ < log.StartOffset())
    return absl::FailedPreconditionError(
        "Install the metadata snapshot before consuming its log suffix");
  while (consumed_end_offset_ < high_watermark) {
    // With this byte budget the log returns its first complete batch only.
    auto bytes = log.Read(consumed_end_offset_, high_watermark, 1);
    if (!bytes.ok())
      return bytes.status();
    if (bytes->empty())
      return absl::InvalidArgumentError("Metadata HW falls inside a record batch");
    if (auto status = ApplyBatch(*bytes); !status.ok())
      return status;
  }
  return absl::OkStatus();
}

}  // namespace dfly::cluster::raft
