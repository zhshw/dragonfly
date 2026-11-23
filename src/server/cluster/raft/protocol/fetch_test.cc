// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include <gtest/gtest.h>

#include "server/cluster/raft/protocol/flexible_message.h"
#include "server/cluster/raft/protocol/rpc_frame.h"

namespace dfly::cluster::raft::protocol {
namespace {

constexpr int32_t kCorrelation = 0x01020304;
constexpr size_t kMaxFrame = 1024;

struct Golden {
  std::string_view request;
  std::string_view response;
};

// size-prefixed v4–18 frames from the pinned Kafka client (testdata/FetchVectors.java).
constexpr Golden kGolden[] = {
    {"00000037000100040102030400016b00000001000000fa000000000000100000000000010001740000000100"
     "00000a000000000000014d00000000",
     "00000034010203040000000000000001000174000000010000000a0000000000000000015effffffffffffff"
     "ff0000000000000003010203"},
    {"0000003f000100050102030400016b00000001000000fa000000000000100000000000010001740000000100"
     "00000a000000000000014dffffffffffffffff00000000",
     "0000003c010203040000000000000001000174000000010000000a0000000000000000015effffffffffffff"
     "ffffffffffffffffff0000000000000003010203"},
    {"0000003f000100060102030400016b00000001000000fa000000000000100000000000010001740000000100"
     "00000a000000000000014dffffffffffffffff00000000",
     "0000003c010203040000000000000001000174000000010000000a0000000000000000015effffffffffffff"
     "ffffffffffffffffff0000000000000003010203"},
    {"0000004b000100070102030400016b00000001000000fa00000000000010000000000000ffffffff00000001"
     "000174000000010000000a000000000000014dffffffffffffffff0000000000000000",
     "00000042010203040000000000000000000000000001000174000000010000000a0000000000000000015eff"
     "ffffffffffffffffffffffffffffff0000000000000003010203"},
    {"0000004b000100080102030400016b00000001000000fa00000000000010000000000000ffffffff00000001"
     "000174000000010000000a000000000000014dffffffffffffffff0000000000000000",
     "00000042010203040000000000000000000000000001000174000000010000000a0000000000000000015eff"
     "ffffffffffffffffffffffffffffff0000000000000003010203"},
    {"0000004f000100090102030400016b00000001000000fa00000000000010000000000000ffffffff00000001"
     "000174000000010000000a00000007000000000000014dffffffffffffffff0000000000000000",
     "00000042010203040000000000000000000000000001000174000000010000000a0000000000000000015eff"
     "ffffffffffffffffffffffffffffff0000000000000003010203"},
    {"0000004f0001000a0102030400016b00000001000000fa00000000000010000000000000ffffffff00000001"
     "000174000000010000000a00000007000000000000014dffffffffffffffff0000000000000000",
     "00000042010203040000000000000000000000000001000174000000010000000a0000000000000000015eff"
     "ffffffffffffffffffffffffffffff0000000000000003010203"},
    {"000000510001000b0102030400016b00000001000000fa00000000000010000000000000ffffffff00000001"
     "000174000000010000000a00000007000000000000014dffffffffffffffff00000000000000000000",
     "00000046010203040000000000000000000000000001000174000000010000000a0000000000000000015eff"
     "ffffffffffffffffffffffffffffff00000000ffffffff00000003010203"},
    {"000000520001000c0102030400016b0000000001000000fa00000000000010000000000000ffffffff020274"
     "020000000a00000007000000000000014d00000005ffffffffffffffff00000000000001010100020263",
     "00000048010203040000000000000000000000020274020000000a0000000000000000015effffffffffffff"
     "ffffffffffffffffff01ffffffff040102030101090000000200000007000000"},
    {"000000600001000d0102030400016b0000000001000000fa00000000000010000000000000ffffffff020000"
     "0000000000000000000000000014020000000a00000007000000000000014d00000005ffffffffffffffff00"
     "000000000001010100020263",
     "000000560102030400000000000000000000000200000000000000000000000000000014020000000a000000"
     "0000000000015effffffffffffffffffffffffffffffff01ffffffff04010203010109000000020000000700"
     "0000"},
    {"000000600001000e0102030400016b0000000001000000fa00000000000010000000000000ffffffff020000"
     "0000000000000000000000000014020000000a00000007000000000000014d00000005ffffffffffffffff00"
     "000000000001010100020263",
     "000000560102030400000000000000000000000200000000000000000000000000000014020000000a000000"
     "0000000000015effffffffffffffffffffffffffffffff01ffffffff04010203010109000000020000000700"
     "0000"},
    {"0000006b0001000f0102030400016b00000000fa00000000000010000000000000ffffffff02000000000000"
     "00000000000000000014020000000a00000007000000000000014d00000005ffffffffffffffff0000000000"
     "0001010200020263010d00000001ffffffffffffffff00",
     "000000560102030400000000000000000000000200000000000000000000000000000014020000000a000000"
     "0000000000015effffffffffffffffffffffffffffffff01ffffffff04010203010109000000020000000700"
     "0000"},
    {"0000006b000100100102030400016b00000000fa00000000000010000000000000ffffffff02000000000000"
     "00000000000000000014020000000a00000007000000000000014d00000005ffffffffffffffff0000000000"
     "0001010200020263010d00000001ffffffffffffffff00",
     "000000560102030400000000000000000000000200000000000000000000000000000014020000000a000000"
     "0000000000015effffffffffffffffffffffffffffffff01ffffffff04010203010109000000020000000700"
     "0000"},
    {"0000007d000100110102030400016b00000000fa00000000000010000000000000ffffffff02000000000000"
     "00000000000000000014020000000a00000007000000000000014d00000005ffffffffffffffff0000000001"
     "00100000000000000000000000000000001e0001010200020263010d00000001ffffffffffffffff00",
     "000000560102030400000000000000000000000200000000000000000000000000000014020000000a000000"
     "0000000000015effffffffffffffffffffffffffffffff01ffffffff04010203010109000000020000000700"
     "0000"},
    {"00000087000100120102030400016b00000000fa00000000000010000000000000ffffffff02000000000000"
     "00000000000000000014020000000a00000007000000000000014d00000005ffffffffffffffff0000000002"
     "00100000000000000000000000000000001e010800000000000000de0001010200020263010d00000001ffff"
     "ffffffffffff00",
     "000000560102030400000000000000000000000200000000000000000000000000000014020000000a000000"
     "0000000000015effffffffffffffffffffffffffffffff01ffffffff04010203010109000000020000000700"
     "0000"},
};

std::string FromHex(std::string_view text) {
  constexpr std::string_view digits = "0123456789abcdef";
  std::string bytes;
  for (size_t i = 0; i < text.size(); i += 2)
    bytes.push_back(static_cast<char>((digits.find(text[i]) << 4) | digits.find(text[i + 1])));
  return bytes;
}

FetchRequest MakeFetch() {
  FetchRequest request;
  request.cluster_id = "c";
  request.max_wait_ms = 250;
  request.max_bytes = 4096;
  request.replica_state.replica_id = 1;
  auto& topic = request.topics.emplace_back();
  topic.topic = "t";
  topic.topic_id.back() = 20;
  auto& part = topic.partitions.emplace_back();
  part.partition = 10;
  part.current_leader_epoch = 7;
  part.last_fetched_epoch = 5;
  part.fetch_offset = 333;
  part.replica_directory_id.back() = 30;
  part.high_watermark = 222;
  return request;
}

FetchResponse MakeReply(int16_t version) {
  FetchResponse response;
  auto& topic = response.responses.emplace_back();
  if (version <= 12)
    topic.topic = "t";
  else
    topic.topic_id.back() = 20;
  auto& part = topic.partitions.emplace_back();
  part.partition_index = 10;
  part.high_watermark = 350;
  part.records.data = FromHex("010203");
  if (version >= 12) {
    part.current_leader.leader_id = 2;
    part.current_leader.leader_epoch = 7;
  }
  return response;
}

TEST(FetchCodecTest, DecodeAndEncodeAllJavaBodies) {
  for (int16_t version = FetchRequest::kMinVersion; version <= FetchRequest::kMaxVersion;
       ++version) {
    SCOPED_TRACE(version);
    const auto& golden = kGolden[version - FetchRequest::kMinVersion];
    const auto request = FromHex(golden.request);
    const auto response = FromHex(golden.response);

    // The fixture uses a one-byte legacy client id and an empty tagged-field section.
    const auto request_body = std::string_view(request).substr(version >= 12 ? 16 : 15);
    FetchRequest decoded;
    ASSERT_EQ(CodecError::kOk, Decode(request_body, version, &decoded));
    ASSERT_EQ(1u, decoded.topics.size());
    ASSERT_EQ(1u, decoded.topics[0].partitions.size());
    const auto& partition = decoded.topics[0].partitions[0];
    EXPECT_EQ(333, partition.fetch_offset);
    EXPECT_EQ(version >= 12 ? 5 : -1, partition.last_fetched_epoch);
    EXPECT_EQ(version < 15 ? 1 : -1, decoded.replica_id);
    EXPECT_EQ(version >= 15 ? 1 : -1, decoded.replica_state.replica_id);
    EXPECT_EQ(version >= 13 ? 20 : 0, decoded.topics[0].topic_id.back());
    EXPECT_EQ(version >= 17 ? 30 : 0, partition.replica_directory_id.back());
    EXPECT_EQ(version >= 18 ? 222 : INT64_MAX, partition.high_watermark);
    std::string encoded;
    EXPECT_EQ(CodecError::kOk, Encode(decoded, version, &encoded));
    EXPECT_EQ(request_body, encoded);
    EXPECT_EQ(CodecError::kOk, Encode(MakeReply(version), version, &encoded));
    EXPECT_EQ(std::string_view(response).substr(version >= 12 ? 9 : 8), encoded);
    ASSERT_EQ(CodecError::kOk,
              EncodeResponseFrame(MakeReply(version), version, kCorrelation, &encoded));
    EXPECT_EQ(response, encoded);
    FetchResponse reply;
    ASSERT_EQ(FrameStatus::kComplete,
              DecodeResponseFrame(response, kMaxFrame, version, kCorrelation, &reply).status);
    ASSERT_EQ(1u, reply.responses.size());
    ASSERT_EQ(1u, reply.responses[0].partitions.size());
    EXPECT_EQ(FromHex("010203"), reply.responses[0].partitions[0].records.data);
  }
}

TEST(FetchCodecTest, RaftListenerNegotiatesTopicIdVersionsAndDispatchesOneFetch) {
  RpcHandlers handlers;
  size_t handled = 0;
  handlers.fetch = [&](int16_t version, const FetchRequest& request, FetchResponse* response) {
    ++handled;
    if (request.topics.size() != 1 || request.topics[0].topic_id.back() != 20)
      return std::make_error_code(std::errc::invalid_argument);
    *response = MakeReply(version);
    return std::error_code{};
  };
  const RequestMetadata header{kCorrelation, "k", {}};
  std::string frame, reply;
  ASSERT_EQ(CodecError::kOk, EncodeRequestFrame(header, ApiVersionsRequest{}, 0, &frame));
  ASSERT_EQ(FrameStatus::kComplete, DispatchRpcFrame(frame, kMaxFrame, handlers, &reply).status);
  ApiVersionsResponse advertised;
  ASSERT_EQ(FrameStatus::kComplete,
            DecodeResponseFrame(reply, kMaxFrame, 0, kCorrelation, &advertised).status);
  ASSERT_EQ(2u, advertised.api_keys.size());
  EXPECT_EQ(FetchRequest::kApiKey, advertised.api_keys[0].api_key);
  EXPECT_EQ(13, advertised.api_keys[0].min_version);
  EXPECT_EQ(18, advertised.api_keys[0].max_version);
  for (int16_t version = 4; version <= 18; ++version) {
    SCOPED_TRACE(version);
    const auto& golden = kGolden[version - 4];
    const auto request = FromHex(golden.request);
    const auto result = DispatchRpcFrame(request, kMaxFrame, handlers, &reply);
    auto error = EncodeRequestFrame(header, MakeFetch(), version, &frame);
    if (version < kMinRaftFetchVersion) {
      EXPECT_EQ(CodecError::kUnsupportedVersion, error);
      EXPECT_EQ(FrameStatus::kUnsupportedVersion, result.status);
      EXPECT_EQ(0u, result.consumed);
    } else {
      ASSERT_EQ(CodecError::kOk, error);
      EXPECT_EQ(request, frame);
      ASSERT_EQ(FrameStatus::kComplete, result.status);
      EXPECT_EQ(request.size(), result.consumed);
      EXPECT_EQ(FromHex(golden.response), reply);
    }
  }
  EXPECT_EQ(6u, handled);
}

TEST(FetchCodecTest, RetainsDivergingEpochSnapshotAndNullableRecords) {
  auto res = MakeReply(18);
  auto& partition = res.responses[0].partitions[0];
  partition.diverging_epoch.epoch = 4;
  partition.diverging_epoch.end_offset = 20;
  partition.snapshot_id.epoch = 5;
  partition.snapshot_id.end_offset = 21;
  partition.aborted_transactions.reset();
  partition.unknown_tags[127] = std::string(128, 'p');
  res.responses[0].unknown_tags[128] = "topic";
  res.unknown_tags[255] = "response";
  res.node_endpoints.push_back({2, "host", 9092, "rack", {{128, "node"}}});
  for (auto records : {std::optional<std::string>{}, std::optional<std::string>(""),
                       std::optional<std::string>(std::string(4 << 20, 'a'))}) {
    partition.records.data = std::move(records);
    std::string bytes;
    ASSERT_EQ(CodecError::kOk, Encode(res, 18, &bytes));
    FetchResponse decoded;
    ASSERT_EQ(CodecError::kOk, Decode(bytes, 18, &decoded));
    EXPECT_EQ(res, decoded);

    // The independent body encoder and Java fixtures pin the complete frame's wire format.
    std::string expected;
    detail::Writer header(0, &expected);
    header.Field(static_cast<int32_t>(5 + bytes.size()));
    header.Field(kCorrelation);
    header.Tags({});
    expected.append(bytes);
    std::string frame = "previous";
    ASSERT_EQ(CodecError::kOk, EncodeResponseFrame(res, 18, kCorrelation, &frame));
    EXPECT_EQ(expected, frame);
    ASSERT_EQ(FrameStatus::kComplete,
              DecodeResponseFrame(frame, frame.size(), 18, kCorrelation, &decoded).status);
    EXPECT_EQ(res, decoded);
    EXPECT_EQ(FrameStatus::kNeedMore,
              DecodeResponseFrame(std::string_view(frame).substr(0, frame.size() - 1), frame.size(),
                                  18, kCorrelation, &decoded)
                  .status);
    EXPECT_EQ(res, decoded);
  }
  std::string bytes;
  EXPECT_EQ(CodecError::kUnsupportedField, Encode(res, 11, &bytes));
}

TEST(FetchCodecTest, FramePreflightRejectsInvalidFieldsWithoutReplacingOutput) {
  auto response = MakeReply(18);
  response.responses[0].partitions[0].records.data = std::string(4 << 20, 'v');
  response.node_endpoints.push_back({2, std::string(32768, 'h'), 9092, {}, {}});
  std::string output = "unchanged";
  EXPECT_EQ(CodecError::kInvalidData, EncodeResponseFrame(response, 18, kCorrelation, &output));
  EXPECT_EQ("unchanged", output);
  response.node_endpoints[0].host = "host";
  EXPECT_EQ(CodecError::kUnsupportedField,
            EncodeResponseFrame(response, 15, kCorrelation, &output));
  EXPECT_EQ("unchanged", output);
  response.unknown_tags[0] = "collides with node_endpoints";
  EXPECT_EQ(CodecError::kInvalidData, EncodeResponseFrame(response, 18, kCorrelation, &output));
  EXPECT_EQ("unchanged", output);
  response.node_endpoints.clear();
  EXPECT_EQ(CodecError::kInvalidData, EncodeResponseFrame(response, 18, kCorrelation, &output));
  EXPECT_EQ("unchanged", output);
  EXPECT_EQ(CodecError::kUnsupportedVersion,
            EncodeResponseFrame(response, 19, kCorrelation, &output));
  EXPECT_EQ("unchanged", output);

  // Exercise signed frame-length overflow without allocating a multi-gigabyte payload.
  detail::SizeWriter size(18, nullptr);
  const std::string chunk(32767, 'v');
  size_t count = 0;
  while (size.Field(chunk))
    ++count;
  EXPECT_EQ(static_cast<size_t>(INT32_MAX) / (chunk.size() + 3), count);
  EXPECT_EQ(static_cast<size_t>(INT32_MAX) + 1, size.size());
  EXPECT_FALSE(size.Field(uint8_t{0}));
  EXPECT_EQ(static_cast<size_t>(INT32_MAX) + 1, size.size());
}

}  // namespace
}  // namespace dfly::cluster::raft::protocol
