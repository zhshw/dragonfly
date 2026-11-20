// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/metadata/metadata_snapshot.h"

#include <absl/base/internal/endian.h>

#include <bit>

#include "server/cluster/raft/log/record_batch.h"

namespace dfly::cluster::raft {
namespace {
using namespace std;
using namespace protocol;

// Snapshot batch offsets start at zero, regardless of the source log's cut.
class ImageWriter {
 public:
  ImageWriter(LogPosition cut, int64_t now) : cut_(cut), now_(now) {
  }

  absl::Status Header(const MembershipHistory& membership, int64_t last_time) {
    auto header = membership.SnapshotHeader(cut_, now_, last_time);
    if (!header.ok())
      return header.status();
    auto batch = DecodeRecordBatch(*header);
    if (!batch.ok())
      return batch.status();
    return AddBatch(std::move(header), batch->end.end_offset);
  }

  absl::Status Record(MetadataRecord record) {
    auto encoded = EncodeMetadataRecord(record);
    if (!encoded.ok())
      return encoded.status();
    if (!values_.empty() && value_bytes_ + encoded->size() > 1U << 20) {
      if (auto s = Flush(); !s.ok())
        return s;
    }
    value_bytes_ += encoded->size() + 32;
    values_.push_back(*std::move(encoded));
    return absl::OkStatus();
  }

  absl::Status Finish(int64_t hw, PartitionLog* log) {
    if (auto s = Flush(); !s.ok())
      return s;
    if (auto s = AddBatch(MakeSnapshotFooterBatch(offset_, cut_.epoch, now_), 1); !s.ok())
      return s;
    auto writer = log->BeginSnapshot(cut_, file_bytes_);
    if (!writer.ok())
      return writer.status();
    for (const auto& batch : batches_) {
      if (auto s = (*writer)->Append((*writer)->position(), batch); !s.ok())
        return s;
    }
    return log->InstallSnapshot(writer->get(), hw);
  }

 private:
  absl::Status AddBatch(absl::StatusOr<string> data, size_t count) {
    if (!data.ok())
      return data.status();
    offset_ += count;
    file_bytes_ += data->size();
    batches_.push_back(*std::move(data));
    return absl::OkStatus();
  }

  absl::Status Flush() {
    if (values_.empty())
      return absl::OkStatus();
    auto status = AddBatch(MakeDataBatch(offset_, cut_.epoch, now_, values_), values_.size());
    values_.clear();
    value_bytes_ = 0;
    return status;
  }

  LogPosition cut_;
  int64_t now_;
  int64_t offset_ = 0;
  int64_t file_bytes_ = 0;
  size_t value_bytes_ = 0;
  vector<string> values_, batches_;
};

absl::Status WriteDirectory(const SlotDirectory& directory, ImageWriter* writer) {
  for (const auto& [id, name] : directory.nodes()) {
    if (auto s = writer->Record({RedisNodeRecord{id, name, {}}}); !s.ok())
      return s;
  }
  for (const auto& [id, group] : directory.groups()) {
    if (auto s = writer->Record({DataGroupRecord{id, group.members, {}}}); !s.ok())
      return s;
    if (group.leadership) {
      if (auto s = writer->Record({*group.leadership}); !s.ok())
        return s;
    }
  }
  const auto& ranges = directory.ranges();
  for (auto i = ranges.begin(); i != ranges.end(); ++i) {
    const auto& owner = i->second;
    if (owner.group_id < 0)
      continue;
    auto after = next(i);
    SlotId last = after == ranges.end() ? kMaxSlotNum : after->first - 1;
    auto s = writer->Record(
        {SlotAssignmentRecord{i->first, last, -1, 0, owner.group_id, owner.generation, {}}});
    if (!s.ok())
      return s;
  }
  return absl::OkStatus();
}
}  // namespace

absl::Status WriteMetadataSnapshot(const MetadataImage& image, const MembershipHistory& membership,
                                   int64_t hw, int64_t timestamp_ms, PartitionLog* log) {
  const auto offset = image.stable_end_offset;
  if (offset <= 0 || offset > hw || hw > log->End().end_offset || membership.next_offset() < offset)
    return absl::InvalidArgumentError("Invalid metadata snapshot boundary or membership offset");
  if (auto previous = log->SnapshotAtOrBefore(hw); previous && previous->end_offset >= offset)
    return absl::OkStatus();
  auto last = log->Read(offset - 1, offset, 1);
  if (!last.ok())
    return last.status();
  auto batch = DecodeRecordBatch(*last);
  if (!batch.ok() || batch->end.end_offset != offset)
    return absl::InvalidArgumentError("Metadata snapshot must end at a whole committed batch");
  ImageWriter writer(batch->end, timestamp_ms);

  // KRaft's RecordsDecodingStrategy uses the magic-v2 batch maxTimestamp as appendTimestamp.
  auto max_time = bit_cast<int64_t>(absl::big_endian::Load64(last->data() + 35));
  if (auto s = writer.Header(membership, max_time); !s.ok())
    return s;
  auto version = image.features.find("metadata.version");
  if (version == image.features.end() && !image.brokers.empty())
    return absl::FailedPreconditionError(
        "Snapshot of registered brokers requires metadata.version");
  if (version != image.features.end()) {
    if (auto s = writer.Record({FeatureLevelRecord{version->first, version->second, {}}}); !s.ok())
      return s;
  }
  for (const auto& [name, level] : image.features) {
    if (name != "metadata.version") {
      if (auto s = writer.Record({FeatureLevelRecord{name, level, {}}}); !s.ok())
        return s;
    }
  }
  for (const auto& [id, broker] : image.brokers) {
    if (auto s = writer.Record({broker, RegisterBrokerRecordVersion(version->second)}); !s.ok())
      return s;
  }
  if (auto s = WriteDirectory(image.directory, &writer); !s.ok())
    return s;
  for (const auto& [id, migration] : image.migrations.records()) {
    if (auto s = writer.Record({migration}); !s.ok())
      return s;
  }
  return writer.Finish(hw, log);
}
}  // namespace dfly::cluster::raft
