// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/data/kv_snapshot.h"

#include <absl/base/internal/endian.h>
#include <openssl/sha.h>

#include "server/cluster/raft/data/kv_rdb.h"
#include "server/cluster/raft/log/record_batch.h"
#include "server/cluster/raft/protocol/flexible_message.h"
#include "server/cluster_support.h"

namespace dfly::cluster::raft {
namespace {
using namespace std;
constexpr size_t kChunk = 256 * 1024;
constexpr string_view kMagic{"DFKS\x01", 5};

struct Manifest {
  KvSnapshotScope scope;
  uint64_t rdb_size;
  string rdb_hash;
  uint64_t migrations_size;
  string migrations_hash;
};

bool Valid(const Manifest& m) {
  if (m.scope.cluster_id.empty() || m.scope.group_id < 0 || m.scope.cut.epoch < 0 ||
      m.scope.cut.end_offset <= 0 || m.rdb_size < 9 || m.rdb_hash.size() != SHA256_DIGEST_LENGTH ||
      m.migrations_hash.size() != SHA256_DIGEST_LENGTH)
    return false;
  int previous = -1;
  for (const auto& r : m.scope.slots) {
    if (r.first <= previous || r.last < r.first || r.last > kMaxSlotNum || r.generation < 0)
      return false;
    previous = r.last;
  }
  return true;
}

template <typename Stream, typename M> bool Fields(Stream& wire, M& m) {
  return wire.Field(m.scope.cluster_id) && wire.Field(m.scope.group_id) &&
         wire.Field(m.scope.cut.epoch) && wire.Field(m.scope.cut.end_offset) &&
         wire.Field(m.rdb_size) && wire.Field(m.rdb_hash) && wire.Field(m.migrations_size) &&
         wire.Field(m.migrations_hash);
}

absl::StatusOr<string> EncodeManifest(const Manifest& m) {
  if (!Valid(m))
    return absl::InvalidArgumentError("Invalid KV snapshot manifest");
  string bytes(kMagic);
  protocol::detail::Writer wire(0, &bytes);
  if (!Fields(wire, m) || !wire.Field(static_cast<uint16_t>(m.scope.slots.size())))
    return absl::InvalidArgumentError("Invalid KV snapshot fields");
  for (auto r : m.scope.slots) {
    if (!wire.Field(r.first) || !wire.Field(r.last) || !wire.Field(r.generation))
      return absl::InvalidArgumentError("Invalid KV snapshot slot generation");
  }
  return bytes;
}

absl::StatusOr<Manifest> DecodeManifest(string_view bytes) {
  if (!bytes.starts_with(kMagic))
    return absl::DataLossError("Unsupported KV snapshot envelope");
  protocol::detail::Reader wire(0, bytes.substr(kMagic.size()));
  Manifest m;
  uint16_t ranges;
  if (!Fields(wire, m) || !wire.Field(ranges) || ranges > kMaxSlotNum + 1)
    return absl::DataLossError("Invalid KV snapshot fields");
  for (unsigned i = 0; i < ranges; ++i) {
    auto& r = m.scope.slots.emplace_back();
    if (!wire.Field(r.first) || !wire.Field(r.last) || !wire.Field(r.generation))
      return absl::DataLossError("Missing KV snapshot slot generation");
  }
  if (!wire.empty() || !Valid(m))
    return absl::DataLossError("Invalid KV snapshot manifest");
  return m;
}
}  // namespace

absl::Status WriteKvSnapshot(const KvSnapshotScope& scope, string_view rdb, string_view migrations,
                             const MembershipHistory& membership, int64_t hw, int64_t now,
                             PartitionLog* log) {
  const auto cut = scope.cut;
  if (!log || cut.end_offset <= 0 || cut.end_offset > hw || hw > log->End().end_offset ||
      !rdb.starts_with("REDIS"))
    return absl::InvalidArgumentError("Invalid committed KV snapshot cut or RDB");
  if (auto prior = log->SnapshotAtOrBefore(hw); prior && prior->end_offset >= cut.end_offset)
    return absl::OkStatus();
  auto last = log->Read(cut.end_offset - 1, cut.end_offset, 1);
  if (!last.ok())
    return last.status();
  auto batch = DecodeRecordBatch(*last);
  if (!batch.ok() || batch->end != cut)
    return absl::InvalidArgumentError("KV snapshot must end at a committed batch boundary");
  auto header = membership.SnapshotHeader(
      cut, now, bit_cast<int64_t>(absl::big_endian::Load64(last->data() + 35)));
  if (!header.ok())
    return header.status();
  auto info = DecodeRecordBatch(*header);
  if (!info.ok())
    return info.status();
  auto manifest = EncodeManifest(
      {scope, rdb.size(), KvRdbDigest(rdb), migrations.size(), KvRdbDigest(migrations)});
  if (!manifest.ok())
    return manifest.status();
  const auto first_record = info->end.end_offset;

  // Size and then stream the encoded batches. Keep only one additional chunk in memory.
  auto emit = [&](absl::FunctionRef<absl::Status(string_view)> append) -> absl::Status {
    if (auto s = append(*header); !s.ok())
      return s;
    int64_t offset = first_record;
    for (string_view blob : {string_view(*manifest), rdb, migrations}) {
      while (!blob.empty()) {
        array<string, 1> values{string(blob.substr(0, kChunk))};
        auto encoded = MakeDataBatch(offset++, cut.epoch, now, values);
        if (!encoded.ok())
          return encoded.status();
        if (auto s = append(*encoded); !s.ok())
          return s;
        blob.remove_prefix(values.front().size());
      }
    }
    auto footer = MakeSnapshotFooterBatch(offset, cut.epoch, now);
    return footer.ok() ? append(*footer) : footer.status();
  };
  int64_t size = 0;
  auto status = emit([&](string_view data) {
    size += data.size();
    return absl::OkStatus();
  });
  if (!status.ok())
    return status;
  auto writer = log->BeginSnapshot(cut, size);
  if (!writer.ok())
    return writer.status();
  if (auto s =
          emit([&](string_view data) { return (*writer)->Append((*writer)->position(), data); });
      !s.ok())
    return s;
  return log->InstallSnapshot(writer->get(), hw);
}

namespace {
using ReadChunk = absl::FunctionRef<absl::StatusOr<SnapshotChunk>(int64_t, size_t)>;

absl::StatusOr<KvSnapshot> DecodeKvSnapshot(LogPosition id, string_view cluster_id,
                                            int32_t group_id, ReadChunk read) {
  optional<Manifest> manifest;
  KvSnapshot snapshot;
  int64_t pos = 0, size;
  do {
    auto prefix = read(pos, 12);
    if (!prefix.ok())
      return prefix.status();
    size = prefix->total_size;
    auto len = RecordBatchSize(prefix->bytes);
    if (!len.ok())
      return len.status();
    auto frame = read(pos, *len);
    if (!frame.ok())
      return frame.status();
    vector<RecordView> records;
    auto batch = DecodeRecordBatch(frame->bytes, &records);
    if (!batch.ok())
      return batch.status();
    pos += frame->bytes.size();
    if (batch->control)
      continue;
    if (records.size() != 1 || records[0].key || !records[0].value || records[0].value->empty())
      return absl::DataLossError("Invalid KV snapshot data record");
    auto bytes = *records.front().value;
    if (!manifest) {
      auto decoded = DecodeManifest(bytes);
      if (!decoded.ok())
        return decoded.status();
      if (decoded->scope.cluster_id != cluster_id || decoded->scope.group_id != group_id ||
          decoded->scope.cut != id)
        return absl::FailedPreconditionError("KV snapshot identity mismatch");
      manifest = *std::move(decoded);
    } else {
      bool kv = snapshot.rdb.size() < manifest->rdb_size;
      auto& out = kv ? snapshot.rdb : snapshot.migrations;
      if (bytes.size() > (kv ? manifest->rdb_size : manifest->migrations_size) - out.size())
        return absl::DataLossError("KV snapshot chunk crosses the declared boundary");
      out.append(bytes);
    }
  } while (pos < size);
  if (!manifest || snapshot.rdb.size() != manifest->rdb_size ||
      snapshot.migrations.size() != manifest->migrations_size ||
      KvRdbDigest(snapshot.rdb) != manifest->rdb_hash ||
      KvRdbDigest(snapshot.migrations) != manifest->migrations_hash)
    return absl::DataLossError("KV snapshot length or SHA256 mismatch");
  snapshot.scope = std::move(manifest->scope);
  return snapshot;
}

}  // namespace

absl::StatusOr<KvSnapshot> ReadKvSnapshot(const PartitionLog& log, LogPosition id,
                                          string_view cluster_id, int32_t group_id) {
  return DecodeKvSnapshot(id, cluster_id, group_id,
                          [&](int64_t pos, size_t len) { return log.ReadSnapshot(id, pos, len); });
}

absl::StatusOr<KvSnapshot> ReadKvSnapshot(const SnapshotWriter& writer, string_view cluster_id,
                                          int32_t group_id) {
  return DecodeKvSnapshot(writer.id(), cluster_id, group_id,
                          [&](int64_t pos, size_t len) { return writer.Read(pos, len); });
}

}  // namespace dfly::cluster::raft
