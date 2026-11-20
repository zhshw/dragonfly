// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/protocol/vote.h"

#include <gtest/gtest.h>

namespace dfly::cluster::raft::protocol {
namespace {

// Kafka 5b648ca9's generated Java messages, from testdata/VoteVectors.java.
constexpr std::string_view kRequests[] = {
    "0a636c757374657249640207746f7069633102000000000000000100000002000000040000000000000003000000",
    "0a636c75737465724964ffffffff0207746f7069633102000000000000000100000002"
    "000000000000000000000000000000000000000000000000000000000000000000000004000000000000000300000"
    "0",
    "0a636c75737465724964ffffffff0207746f7069633102000000000000000100000002"
    "0000000000000000000000000000000000000000000000000000000000000000000000040000000000000003010000"
    "00",
};
constexpr std::string_view kResponses[] = {
    "0000020102000000010000000000020000000000000000",
    "0000020102000000010000000000020000000000000001001202000000020a6c6f63616c686f7374238400",
    "0000020102000000010000000000020000000000000001001202000000020a6c6f63616c686f7374238400",
};

std::string FromHex(std::string_view hex) {
  constexpr std::string_view digits = "0123456789abcdef";
  std::string bytes;
  for (size_t i = 0; i < hex.size(); i += 2)
    bytes.push_back(static_cast<char>((digits.find(hex[i]) << 4) | digits.find(hex[i + 1])));
  return bytes;
}

VoteRequest SampleRequest(int16_t version) {
  VoteRequest result;
  result.cluster_id = "clusterId";
  auto& topic = result.topics.emplace_back();
  topic.topic_name = "topic1";
  auto& partition = topic.partitions.emplace_back();
  partition.replica_epoch = 1;
  partition.replica_id = 2;
  partition.last_offset = 3;
  partition.last_offset_epoch = 4;
  partition.pre_vote = version >= 2;
  return result;
}

VoteResponse SampleResponse(int16_t version) {
  VoteResponse result;
  auto& topic = result.topics.emplace_back();
  auto& partition = topic.partitions.emplace_back();
  partition.partition_index = 1;
  partition.leader_id = 2;
  if (version >= 1) {
    auto& endpoint = result.node_endpoints.emplace_back();
    endpoint.node_id = 2;
    endpoint.host = "localhost";
    endpoint.port = 9092;
  }
  return result;
}

template <typename T> void CheckGolden(const T& message, int16_t version, std::string_view hex) {
  const auto expected = FromHex(hex);
  T decoded;
  ASSERT_EQ(CodecError::kOk, Decode(expected, version, &decoded));
  EXPECT_EQ(message, decoded);
  std::string bytes;
  ASSERT_EQ(CodecError::kOk, Encode(message, version, &bytes));
  EXPECT_EQ(expected, bytes);
}

TEST(VoteCodecTest, KafkaJavaRequestResponseTestVectors) {
  for (int16_t version = 0; version <= 2; ++version) {
    SCOPED_TRACE(version);
    CheckGolden(SampleRequest(version), version, kRequests[version]);
    CheckGolden(SampleResponse(version), version, kResponses[version]);
  }
}

TEST(VoteCodecTest, KafkaJavaDirectoryIdsAndErrorResponse) {
  VoteRequest request;
  request.voter_id = 2;
  auto& topic = request.topics.emplace_back();
  topic.topic_name = "组";
  auto& partition = topic.partitions.emplace_back();
  partition.replica_epoch = 7;
  partition.replica_id = 3;
  partition.last_offset = 0x0102030405060708LL;
  partition.replica_directory_id = {0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
                                    0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00};
  partition.voter_directory_id = {0xff, 0xee, 0xdd, 0xcc, 0xbb, 0xaa, 0x00, 0x99,
                                  0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11};
  CheckGolden(request, 1,
              "00000000020204e7bb8402000000000000000700000003112233445566778899aabbccddeeff00"
              "ffeeddccbbaa00998877665544332211000000000102030405060708000000");
  VoteResponse response;
  response.error_code = -1;
  auto& endpoint = response.node_endpoints.emplace_back();
  endpoint.node_id = 2;
  endpoint.host = "::1";
  endpoint.port = 65535;
  CheckGolden(response, 1, "ffff0101000c0200000002043a3a31ffff00");
}

TEST(VoteCodecTest, DowngradeUsesKafkaIgnorableAndRequiredFieldRules) {
  auto request = SampleRequest(2);
  request.voter_id = 9;
  auto& partition = request.topics.front().partitions.front();
  partition.replica_directory_id[0] = 42;
  partition.voter_directory_id[15] = 19;
  std::string bytes = "unchanged";
  EXPECT_EQ(CodecError::kUnsupportedField, Encode(request, 1, &bytes));
  EXPECT_EQ(CodecError::kUnsupportedField, Encode(SampleResponse(1), 0, &bytes));
  EXPECT_EQ(CodecError::kUnsupportedVersion, Encode(request, 3, &bytes));
  EXPECT_EQ("unchanged", bytes);
  partition.pre_vote = false;
  ASSERT_EQ(CodecError::kOk, Encode(request, 0, &bytes));
  EXPECT_EQ(FromHex(kRequests[0]), bytes);
  ASSERT_EQ(CodecError::kOk, Encode(request, 1, &bytes));
  VoteRequest copy;
  ASSERT_EQ(CodecError::kOk, Decode(bytes, 1, &copy));
  EXPECT_EQ(request, copy);
}

TEST(VoteCodecTest, NullableClusterIdAndEmptyClusterIdAreDistinct) {
  VoteRequest request;
  CheckGolden(request, 0, "000100");
  request.cluster_id = "";
  CheckGolden(request, 0, "010100");
}

TEST(VoteCodecTest, TaggedFieldsAtEveryStructLevelSurviveRoundTrip) {
  auto response = SampleResponse(1);
  response.error_code = -1;
  response.node_endpoints.front().port = 65535;
  response.unknown_tags.emplace(130, std::string(128, 'x'));
  response.topics.front().unknown_tags.emplace(1, "");
  response.topics.front().partitions.front().unknown_tags.emplace(3, "partition");
  response.node_endpoints.front().unknown_tags.emplace(0, "endpoint");
  std::string bytes;
  ASSERT_EQ(CodecError::kOk, Encode(response, 1, &bytes));
  VoteResponse copy;
  ASSERT_EQ(CodecError::kOk, Decode(bytes, 1, &copy));
  EXPECT_EQ(response, copy);
}

TEST(VoteCodecTest, RejectMalformedBodyWithoutModifyingDestination) {
  const auto bytes = FromHex(kResponses[2]);
  const auto original = SampleResponse(2);
  auto copy = original;
  for (size_t i = 0; i < bytes.size(); ++i) {
    SCOPED_TRACE(i);
    EXPECT_EQ(CodecError::kInvalidData, Decode(std::string_view(bytes).substr(0, i), 2, &copy));
    EXPECT_EQ(original, copy);
  }
  EXPECT_EQ(CodecError::kInvalidData, Decode(bytes + "x", 2, &copy));
  EXPECT_EQ(CodecError::kInvalidData, Decode(FromHex("00000000"), 2, &copy));
  EXPECT_EQ(CodecError::kInvalidData, Decode(FromHex("00000101000101"), 0, &copy));
  EXPECT_EQ(CodecError::kInvalidData, Decode(FromHex("0000010100020100"), 2, &copy));
  EXPECT_EQ(CodecError::kUnsupportedVersion, Decode(bytes, -1, &copy));
  EXPECT_EQ(original, copy);

  VoteRequest request;
  EXPECT_EQ(CodecError::kInvalidData, Decode(FromHex("ffffffff1f"), 0, &request));
  EXPECT_EQ(CodecError::kInvalidData, Decode(FromHex("00feffffff0f00"), 0, &request));
}

}  // namespace
}  // namespace dfly::cluster::raft::protocol
