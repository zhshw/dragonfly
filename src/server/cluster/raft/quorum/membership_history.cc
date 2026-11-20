// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/quorum/membership_history.h"

#include <absl/base/internal/endian.h>

#include <set>

#include "server/cluster/raft/log/record_batch.h"

namespace dfly::cluster::raft {
namespace {
using namespace std;
using namespace protocol;

template <typename T> const T* ValueAt(const map<int64_t, T>& versions, int64_t offset) {
  auto it = versions.upper_bound(offset);
  return it == versions.begin() ? nullptr : &prev(it)->second;
}

template <typename T> void DiscardBefore(int64_t offset, map<int64_t, T>* values) {
  auto it = values->upper_bound(offset);
  if (it != values->begin())
    values->erase(values->begin(), prev(it));
}

bool ValidVoters(const VotersRecord& record) {
  if (record.version != 0 || record.voters.empty())
    return false;
  set<int32_t> ids;
  for (const auto& voter : record.voters) {
    const auto& range = voter.k_raft_version_feature;
    if (voter.voter_id < 0 || !ids.insert(voter.voter_id).second ||
        range.min_supported_version < 0 ||
        range.max_supported_version < range.min_supported_version)
      return false;
    set<string> listeners;
    for (auto& address : voter.endpoints) {
      if (address.name.empty() || address.host.empty() || !address.port ||
          !listeners.insert(address.name).second)
        return false;
    }
  }
  return true;
}
}  // namespace

optional<int64_t> MembershipHistory::LastVoterOffset() const {
  return voters_.empty() ? nullopt : optional<int64_t>{voters_.rbegin()->first};
}

const VotersRecord* MembershipHistory::VotersAt(int64_t offset) const {
  return offset >= next_offset_ ? nullptr : ValueAt(voters_, offset);
}

int16_t MembershipHistory::KraftVersionAt(int64_t offset) const {
  auto value = offset >= next_offset_ ? nullptr : ValueAt(versions_, offset);
  return value ? *value : 0;
}

absl::StatusOr<string> MembershipHistory::SnapshotHeader(LogPosition cut, int64_t now_ms,
                                                         int64_t last_record_ms) const {
  if (cut.end_offset <= 0 || cut.end_offset > next_offset_)
    return absl::InvalidArgumentError("Quorum membership has not reached the snapshot cut");
  string version, voters;
  vector<ControlRecordValue> controls;
  auto kraft = KraftVersionAt(cut.end_offset - 1);
  if (kraft == 1) {
    if (Encode(KRaftVersionRecord{0, kraft, {}}, 0, &version) != CodecError::kOk)
      return absl::InvalidArgumentError("Cannot encode snapshot KRaft version");
    controls.push_back({5, version});
  }
  if (const auto* v = VotersAt(cut.end_offset - 1)) {
    if (kraft != 1 || Encode(*v, 0, &voters) != CodecError::kOk)
      return absl::InvalidArgumentError("Cannot encode snapshot voter configuration");
    controls.push_back({6, voters});
  }
  return MakeSnapshotHeaderBatch(cut.epoch, now_ms, last_record_ms, controls);
}

void MembershipHistory::TruncateNewEntries(int64_t end_offset) {
  voters_.erase(voters_.lower_bound(end_offset), voters_.end());
  versions_.erase(versions_.lower_bound(end_offset), versions_.end());
  next_offset_ = min(next_offset_, end_offset);
}

void MembershipHistory::TruncateOldEntries(int64_t start_offset) {
  DiscardBefore(start_offset, &voters_);
  DiscardBefore(start_offset, &versions_);
}

absl::Status MembershipHistory::ReplayBatch(string_view bytes, optional<int64_t> snapshot_offset) {
  vector<RecordView> records;
  auto batch = DecodeRecordBatch(bytes, &records);
  if (!batch.ok())
    return batch.status();
  if (!snapshot_offset && next_offset_ != batch->base_offset)
    return absl::DataLossError("Offset gap in membership history");

  // Validate the whole batch before publishing its changes to the election state.
  map<int64_t, VotersRecord> voters;
  map<int64_t, int16_t> versions;
  if (batch->control) {
    for (const auto& record : records) {
      const int64_t offset = snapshot_offset.value_or(record.offset);
      auto type = absl::big_endian::Load16(record.key->data() + 2);
      if (type == 5) {
        KRaftVersionRecord version;
        if (Decode(*record.value, 0, &version) != CodecError::kOk || version.version != 0 ||
            version.k_raft_version < 0 || version.k_raft_version > 1)
          return absl::FailedPreconditionError("Unsupported KRaftVersionRecord");
        if ((!versions_.empty() && versions_.rbegin()->first >= offset) ||
            !versions.emplace(offset, version.k_raft_version).second)
          return absl::DataLossError("Out-of-order KRaftVersionRecord");
      } else if (type == 6) {
        VotersRecord value;
        if (Decode(*record.value, 0, &value) != CodecError::kOk || !ValidVoters(value))
          return absl::DataLossError("Invalid VotersRecord");
        if ((!voters_.empty() && voters_.rbegin()->first >= offset) ||
            !voters.emplace(offset, std::move(value)).second)
          return absl::DataLossError("Out-of-order VotersRecord");
      }
    }
  }
  versions_.merge(versions);
  voters_.merge(voters);
  if (!snapshot_offset)
    next_offset_ = batch->end.end_offset;
  return absl::OkStatus();
}

absl::Status MembershipHistory::LoadSnapshot(const PartitionLog& log, LogPosition id) {
  MembershipHistory recovered;
  int64_t position = 0;
  while (true) {
    auto prefix = log.ReadSnapshot(id, position, 12);
    if (!prefix.ok())
      return prefix.status();
    auto size = RecordBatchSize(prefix->bytes);
    if (!size.ok())
      return size.status();
    auto bytes = log.ReadSnapshot(id, position, *size);
    if (!bytes.ok())
      return bytes.status();
    if (auto s = recovered.ReplayBatch(bytes->bytes, id.end_offset - 1); !s.ok())
      return s;
    position += bytes->bytes.size();
    if (position == bytes->total_size)
      break;
  }
  recovered.next_offset_ = id.end_offset;
  *this = std::move(recovered);
  return absl::OkStatus();
}

absl::Status MembershipHistory::Appended(string_view bytes, int64_t durable_end) {
  while (next_offset_ < durable_end) {
    auto size = RecordBatchSize(bytes);
    if (!size.ok() || *size > bytes.size() ||
        absl::big_endian::Load64(bytes.data()) != static_cast<uint64_t>(next_offset_))
      return absl::DataLossError("Validated append does not match membership history offset");
    if (absl::big_endian::Load16(bytes.data() + 21) & 0x20) {
      if (auto s = ReplayBatch(bytes.substr(0, *size), nullopt); !s.ok())
        return s;
    } else {
      const auto count = absl::big_endian::Load32(bytes.data() + 57);
      if (count == 0 || count > static_cast<uint64_t>(durable_end - next_offset_))
        return absl::DataLossError("Data batch extends beyond the durable end");
      next_offset_ += count;
    }
    bytes.remove_prefix(*size);
  }
  return next_offset_ == durable_end ? absl::OkStatus() : absl::DataLossError("Regressing log end");
}

absl::Status MembershipHistory::Update(const PartitionLog& log) {
  if (next_offset_ < log.StartOffset()) {
    if (auto snapshot = log.LatestSnapshot()) {
      if (auto s = LoadSnapshot(log, *snapshot); !s.ok())
        return s;
    } else if (next_offset_ == -1 && log.StartOffset() == 0) {
      next_offset_ = 0;
    } else {
      return absl::DataLossError("Cannot recover membership history without a snapshot");
    }
  }
  const auto end = log.End().end_offset;
  if (next_offset_ > end)
    return absl::FailedPreconditionError("Membership history requires suffix truncation");
  while (next_offset_ < end) {
    // Read one complete batch, even across segment rolls, without retaining the entire log.
    auto bytes = log.Read(next_offset_, end, 1);
    if (!bytes.ok())
      return bytes.status();
    if (auto s = ReplayBatch(*bytes, nullopt); !s.ok())
      return s;
  }
  return absl::OkStatus();
}

}  // namespace dfly::cluster::raft
