// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/metadata/record_serde.h"

#include <absl/strings/str_cat.h>

#include "server/cluster/raft/protocol/flexible_message.h"

namespace dfly::cluster::raft {
namespace {
using namespace std;
using namespace protocol;

constexpr int16_t kFrameVersion = 1;

template <typename T>
absl::StatusOr<MetadataRecord> ReadMessage(string_view body, int16_t version) {
  T message;
  auto error = Decode(body, version, &message);
  if (error == CodecError::kUnsupportedVersion)
    return absl::UnimplementedError(
        absl::StrCat("Metadata record ", T::kApiKey, " has unsupported version ", version));
  if (error != CodecError::kOk)
    return absl::DataLossError(absl::StrCat("Malformed metadata record ", T::kApiKey));
  return MetadataRecord{std::move(message), version};
}

using Decoder = absl::StatusOr<MetadataRecord> (*)(string_view, int16_t);

template <typename... Ts> constexpr auto Decoders(variant<Ts...>*) {
  return array{pair<int16_t, Decoder>{Ts::kApiKey, &ReadMessage<Ts>}...};
}

}  // namespace

absl::StatusOr<string> EncodeMetadataRecord(const MetadataRecord& record) {
  string body, bytes;
  detail::Writer header(kFrameVersion, &bytes);
  header.Varint(kFrameVersion);
  auto error = visit(
      [&](const auto& message) {
        header.Varint(message.kApiKey);
        header.Varint(record.version);
        return Encode(message, record.version, &body);
      },
      record.message);
  if (error != CodecError::kOk)
    return absl::InvalidArgumentError("Cannot encode metadata record at the requested version");
  bytes.append(body);
  return bytes;
}

absl::StatusOr<MetadataRecord> DecodeMetadataRecord(string_view bytes) {
  detail::Reader header(kFrameVersion, bytes);
  uint32_t frame, type, version;
  if (!header.Varint(&frame))
    return absl::DataLossError("Malformed metadata record frame version");
  if (frame != kFrameVersion)
    return absl::UnimplementedError(absl::StrCat("Unsupported metadata frame version ", frame));
  if (!header.Varint(&type) || type > INT16_MAX || !header.Varint(&version) || version > INT16_MAX)
    return absl::DataLossError("Malformed metadata record type or version");
  static constexpr auto decoders = Decoders(static_cast<MetadataPayload*>(nullptr));
  for (const auto& [id, decode] : decoders) {
    if (type == static_cast<uint32_t>(id))
      return decode(header.remaining(), version);
  }
  return absl::UnimplementedError(absl::StrCat("Unknown metadata record type ", type));
}

}  // namespace dfly::cluster::raft
