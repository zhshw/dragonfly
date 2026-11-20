// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/log/record_batch.h"

#include <absl/base/internal/endian.h>
#include <absl/crc/crc32c.h>

#include <algorithm>
#include <bit>
#include <limits>
#include <set>

#include "server/cluster/raft/protocol/flexible_message.h"

namespace dfly::cluster::raft::protocol::detail {

// LeaderChangeMessage.json, v0. Kafka's ControlRecordUtils selects this version
// at epoch start, including when the cluster supports dynamic membership.
struct LeaderChangeVoter {
  int32_t id;
  TaggedFields tags;
};

template <> struct Schema<LeaderChangeVoter> {
  template <typename IO, typename T> static bool Visit(IO& io, T& voter) {
    return io.Field(voter.id) && io.Tags(voter.tags);
  }
};
}  // namespace dfly::cluster::raft::protocol::detail

namespace dfly::cluster::raft {
namespace {
using namespace std;
using protocol::detail::LeaderChangeVoter;
namespace be = absl::big_endian;
constexpr char kLeaderChangeKey[] = {0, 0, 0, 2};
constexpr uint16_t kControl = 0x20;

// DefaultRecord integers are signed zigzag varints; flexible message lengths
// inside a control record use the Kafka RPC unsigned-varint encoding instead.
template <typename T> bool ReadSigned(string_view* input, T* value) {
  using U = make_unsigned_t<T>;
  U raw = 0;
  for (unsigned shift = 0; shift < sizeof(U) * 8 && !input->empty(); shift += 7) {
    uint8_t b = input->front();
    input->remove_prefix(1);
    if (static_cast<U>(b & 0x7f) > (numeric_limits<U>::max() >> shift))
      return false;
    raw |= static_cast<U>(b & 0x7f) << shift;
    if (b < 128) {
      *value = bit_cast<T>(static_cast<U>((raw >> 1) ^ (U{0} - (raw & 1))));
      return true;
    }
  }
  return false;
}

void PutSigned(int32_t n, string* bytes) {
  uint32_t raw = (static_cast<uint32_t>(n) << 1) ^ static_cast<uint32_t>(-(n < 0));
  while (raw >= 128) {
    bytes->push_back(static_cast<char>((raw & 0x7f) | 0x80));
    raw >>= 7;
  }
  bytes->push_back(static_cast<char>(raw));
}

bool ReadBytes(string_view* body, optional<string_view>* result) {
  int32_t n;
  if (!ReadSigned(body, &n) || n < -1)
    return false;
  if (n == -1) {
    result->reset();
    return true;
  }
  if (static_cast<size_t>(n) > body->size())
    return false;
  *result = body->substr(0, n);
  body->remove_prefix(n);
  return true;
}

bool ReadRecord(string_view* input, int32_t index, RecordView* record) {
  int32_t length;
  if (!ReadSigned(input, &length) || length < 1 || static_cast<size_t>(length) > input->size())
    return false;
  string_view body = input->substr(0, length);
  input->remove_prefix(length);

  // Record attributes are reserved; Kafka readers skip them.
  body.remove_prefix(1);
  int64_t timestamp_delta;
  int32_t delta, headers;
  if (!ReadSigned(&body, &timestamp_delta) || !ReadSigned(&body, &delta) || delta != index ||
      !ReadBytes(&body, &record->key) || !ReadBytes(&body, &record->value) ||
      !ReadSigned(&body, &headers) || headers < 0)
    return false;
  for (int32_t i = 0; i < headers; ++i) {
    optional<string_view> key, value;
    if (!ReadBytes(&body, &key) || !key || !ReadBytes(&body, &value))
      return false;
  }
  return body.empty();
}

bool ValidLeaderChange(string_view value) {
  protocol::detail::Reader reader(0, value);
  int16_t version;
  int32_t leader;
  vector<LeaderChangeVoter> voters, grants;
  protocol::TaggedFields tags;
  return reader.Field(version) && version == 0 && reader.Field(leader) && reader.Field(voters) &&
         reader.Field(grants) && reader.Tags(tags) && reader.empty();
}

void PutRecord(int32_t index, optional<string_view> key, string_view value, string* records) {
  string body(2, '\0');  // attributes and timestamp delta
  PutSigned(index, &body);
  PutSigned(key ? static_cast<int32_t>(key->size()) : -1, &body);
  if (key)
    body.append(*key);
  PutSigned(value.size(), &body);
  body.append(value);
  body.push_back(0);  // header count
  PutSigned(body.size(), records);
  records->append(body);
}

absl::StatusOr<string> BuildBatch(int64_t offset, int32_t epoch, int64_t timestamp_ms,
                                  int32_t count, bool control, string records) {
  if (offset < 0 || count <= 0 || offset > INT64_MAX - count || epoch < 0 || timestamp_ms < 0)
    return absl::InvalidArgumentError("Invalid KRaft batch offset, epoch, count or timestamp");
  if (records.size() > kMaxBatchSize - kBatchHeaderSize)
    return absl::ResourceExhaustedError("KRaft batch exceeds maximum size");

  string batch(kBatchHeaderSize, '\0');
  be::Store64(batch.data(), offset);
  be::Store32(batch.data() + 8, kBatchHeaderSize + records.size() - 12);
  be::Store32(batch.data() + 12, epoch);
  batch[16] = 2;
  be::Store16(batch.data() + 21, control ? kControl : 0);
  be::Store32(batch.data() + 23, count - 1);
  be::Store64(batch.data() + 27, timestamp_ms);
  be::Store64(batch.data() + 35, timestamp_ms);
  be::Store64(batch.data() + 43, UINT64_MAX);
  be::Store16(batch.data() + 51, UINT16_MAX);
  be::Store32(batch.data() + 53, UINT32_MAX);
  be::Store32(batch.data() + 57, count);
  batch += records;
  be::Store32(batch.data() + 17,
              static_cast<uint32_t>(absl::ComputeCrc32c(string_view(batch).substr(21))));
  return batch;
}
}  // namespace

absl::StatusOr<size_t> RecordBatchSize(std::string_view prefix) {
  if (prefix.size() < 12)
    return absl::OutOfRangeError("Incomplete record batch prefix");
  const uint32_t length = be::Load32(prefix.data() + 8);
  if (length < kBatchHeaderSize - 12 || length > kMaxBatchSize - 12)
    return absl::DataLossError("Invalid record batch length");
  return static_cast<size_t>(length) + 12;
}

absl::StatusOr<BatchInfo> DecodeRecordBatch(std::string_view batch, std::vector<RecordView>* out) {
  auto size = RecordBatchSize(batch);
  if (!size.ok() || *size != batch.size() || batch[16] != 2)
    return absl::DataLossError("Invalid or incomplete magic-v2 record batch");
  if (static_cast<uint32_t>(absl::ComputeCrc32c(batch.substr(21))) != be::Load32(batch.data() + 17))
    return absl::DataLossError("Record batch CRC32C mismatch");
  uint16_t flags = be::Load16(batch.data() + 21);
  if ((flags & 7) != 0)
    return absl::UnimplementedError("KRaft quorum log uses uncompressed batches");
  int64_t base = bit_cast<int64_t>(be::Load64(batch.data()));
  int32_t epoch = bit_cast<int32_t>(be::Load32(batch.data() + 12));
  int32_t count = bit_cast<int32_t>(be::Load32(batch.data() + 57));
  if (base < 0 || epoch < 0 || count <= 0 || base > INT64_MAX - count ||
      be::Load32(batch.data() + 23) != static_cast<uint32_t>(count - 1))
    return absl::DataLossError("Invalid KRaft batch offsets, epoch or record count");

  vector<RecordView> records;
  bool control = flags & kControl;
  auto body = batch.substr(kBatchHeaderSize);
  for (int32_t i = 0; i < count; ++i) {
    RecordView record{base + i, {}, {}};
    if (!ReadRecord(&body, i, &record))
      return absl::DataLossError("Invalid DefaultRecord in KRaft batch");
    if (control) {
      if (!record.key || record.key->size() < 4 || !record.value)
        return absl::DataLossError("Invalid KRaft control record");
      if (record.key == string_view(kLeaderChangeKey, 4) && !ValidLeaderChange(*record.value))
        return absl::DataLossError("Invalid LeaderChangeMessage");
    }
    if (out)
      records.push_back(record);
  }
  if (!body.empty())
    return absl::DataLossError("Trailing bytes after final record");
  if (out)
    *out = std::move(records);
  return BatchInfo{base, {epoch, base + count}, control};
}

absl::StatusOr<std::string> MakeDataBatch(int64_t offset, int32_t epoch, int64_t timestamp_ms,
                                          span<const string> values) {
  string records;
  for (size_t i = 0; i < values.size(); ++i) {
    if (values[i].size() > kMaxBatchSize - records.size())
      return absl::ResourceExhaustedError("KRaft data batch exceeds maximum size");
    PutRecord(i, nullopt, values[i], &records);
    if (records.size() > kMaxBatchSize - kBatchHeaderSize)
      return absl::ResourceExhaustedError("KRaft data batch exceeds maximum size");
  }
  return BuildBatch(offset, epoch, timestamp_ms, values.size(), false, std::move(records));
}

absl::StatusOr<std::string> MakeControlBatch(int64_t offset, int32_t epoch, int64_t timestamp_ms,
                                             span<const ControlRecordValue> controls) {
  string records;
  for (size_t i = 0; i < controls.size(); ++i) {
    if (controls[i].type < 0)
      return absl::InvalidArgumentError("Negative KRaft control record type");
    if (controls[i].value.size() > kMaxBatchSize - records.size())
      return absl::ResourceExhaustedError("KRaft control batch exceeds maximum size");
    char key[4] = {};
    be::Store16(key + 2, controls[i].type);
    PutRecord(i, string_view(key, sizeof(key)), controls[i].value, &records);
    if (records.size() > kMaxBatchSize - kBatchHeaderSize)
      return absl::ResourceExhaustedError("KRaft control batch exceeds maximum size");
  }
  return BuildBatch(offset, epoch, timestamp_ms, controls.size(), true, std::move(records));
}

absl::StatusOr<std::string> MakeLeaderChangeBatch(int64_t offset, int32_t epoch,
                                                  int64_t timestamp_ms, int32_t leader,
                                                  span<const int32_t> voters,
                                                  span<const int32_t> granting_voters,
                                                  span<const ControlRecordValue> trailing) {
  set<int32_t> members(voters.begin(), voters.end());
  set<int32_t> grants(granting_voters.begin(), granting_voters.end());
  if (members.empty() || members.size() != voters.size() || *members.begin() < 0 ||
      grants.size() != granting_voters.size() || grants.size() <= members.size() / 2 ||
      !grants.contains(leader) ||
      !includes(members.begin(), members.end(), grants.begin(), grants.end()))
    return absl::InvalidArgumentError(
        "LeaderChange requires independent majority votes including self");

  string value;
  protocol::detail::Writer writer(0, &value);
  writer.Field(int16_t{0});
  writer.Field(leader);
  for (auto ids : {voters, granting_voters}) {
    vector<LeaderChangeVoter> array;
    for (int32_t id : ids)
      array.push_back({id, {}});
    if (!writer.Field(array))
      return absl::ResourceExhaustedError("Too many voters in LeaderChangeMessage");
  }
  writer.Tags({});
  vector<ControlRecordValue> controls{{2, value}};
  controls.insert(controls.end(), trailing.begin(), trailing.end());
  return MakeControlBatch(offset, epoch, timestamp_ms, controls);
}

absl::StatusOr<std::string> MakeSnapshotHeaderBatch(int32_t epoch, int64_t timestamp_ms,
                                                    int64_t last_contained_log_timestamp,
                                                    span<const ControlRecordValue> trailing) {
  string value;
  protocol::detail::Writer writer(0, &value);
  writer.Field(int16_t{0});
  writer.Field(last_contained_log_timestamp);
  writer.Tags({});
  vector<ControlRecordValue> controls{{3, value}};
  controls.insert(controls.end(), trailing.begin(), trailing.end());
  return MakeControlBatch(0, epoch, timestamp_ms, controls);
}

absl::StatusOr<std::string> MakeSnapshotFooterBatch(int64_t offset, int32_t epoch,
                                                    int64_t timestamp_ms) {
  // SnapshotFooterRecord v0 contains its version and an empty tagged-field array.
  const ControlRecordValue footer{4, string_view("\0\0\0", 3)};
  return MakeControlBatch(offset, epoch, timestamp_ms, span(&footer, 1));
}

}  // namespace dfly::cluster::raft
