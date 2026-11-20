// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/protocol/rpc_frame.h"

#include <gtest/gtest.h>

namespace dfly::cluster::raft::protocol {
namespace {

constexpr size_t kMaxFrame = 1024;
constexpr int32_t kCorrelationId = 0x01020304;

// Frames emitted by Kafka 5b648ca9's RequestUtils.serialize (testdata/FrameVectors.java).
constexpr std::string_view kRequest[] = {
    "0000000b001200000102030400016b",
    "0000000b001200010102030400016b",
    "0000000b001200020102030400016b",
    "00000012001200030102030400016b00036466023100",
    "00000012001200040102030400016b00036466023100",
    "00000018001200050102030400016b00036466023102630000002a00",
};
constexpr std::string_view kResponse[] = {
    "0000001601020304000000000002001200000005003400000002",
    "0000001a0102030400000000000200120000000500340000000200000000",
    "0000001a0102030400000000000200120000000500340000000200000000",
    "0000001a0102030400000300120000000500003400000002000000000000",
    "0000001a0102030400000300120000000500003400000002000000000000",
    "0000001a0102030400000300120000000500003400000002000000000000",
};
constexpr std::string_view kVote = "00000016003400020102030400016b010601a500ffffffff0100";
constexpr std::string_view kVoteReply = "00000009010203040000000100";

std::string FromHex(std::string_view text) {
  constexpr std::string_view digits = "0123456789abcdef";
  std::string bytes;
  for (size_t i = 0; i < text.size(); i += 2)
    bytes.push_back(static_cast<char>((digits.find(text[i]) << 4) | digits.find(text[i + 1])));
  return bytes;
}

FrameResult Negotiate(const ApiVersionsRequest& request, int16_t version,
                      ApiVersionsResponse* out) {
  RequestMetadata metadata{kCorrelationId, "k", {}};
  std::string frame, reply;
  EXPECT_EQ(CodecError::kOk, EncodeRequestFrame(metadata, request, version, &frame));
  auto result = DispatchRequestFrame(frame, kMaxFrame, {}, &reply);
  if (result.status == FrameStatus::kComplete)
    result = DecodeResponseFrame(reply, kMaxFrame, version, kCorrelationId, out);
  return result;
}

TEST(RpcFrameTest, KafkaJavaApiVersionsAllVersionsAndHeaders) {
  VoteHandler enabled = [](int16_t, const VoteRequest&, VoteResponse*) {
    ADD_FAILURE() << "ApiVersions dispatched as Vote";
    return std::error_code{};
  };
  for (int16_t version = 0; version <= 5; ++version) {
    SCOPED_TRACE(version);
    ApiVersionsRequest request;
    request.client_software_name = "df";
    request.client_software_version = "1";
    if (version >= 5) {
      request.cluster_id = "c";
      request.node_id = 42;
    }
    std::string frame, reply;
    ASSERT_EQ(CodecError::kOk,
              EncodeRequestFrame({kCorrelationId, "k", {}}, request, version, &frame));
    EXPECT_EQ(FromHex(kRequest[version]), frame);
    auto result = DispatchRequestFrame(frame, kMaxFrame, enabled, &reply);
    ASSERT_EQ(FrameStatus::kComplete, result.status);
    EXPECT_EQ(frame.size(), result.consumed);
    EXPECT_EQ(FromHex(kResponse[version]), reply);
    ApiVersionsResponse decoded;
    result = DecodeResponseFrame(reply, kMaxFrame, version, kCorrelationId, &decoded);
    ASSERT_EQ(FrameStatus::kComplete, result.status);
    EXPECT_EQ(reply.size(), result.consumed);
    ASSERT_EQ(2u, decoded.api_keys.size());
    EXPECT_EQ(VoteRequest::kApiKey, decoded.api_keys[1].api_key);
    EXPECT_EQ(VoteRequest::kMaxVersion, decoded.api_keys[1].max_version);
  }
}

TEST(RpcFrameTest, UnsupportedApiVersionsRequestUsesV0ResponseAndIgnoresFutureBody) {
  auto unsupported = FromHex(kRequest[5]);
  unsupported[7] = 20;
  unsupported.back() = static_cast<char>(0xfe);
  std::string reply;
  ASSERT_EQ(FrameStatus::kComplete,
            DispatchRequestFrame(unsupported, kMaxFrame, {}, &reply).status);
  ApiVersionsResponse decoded;
  ASSERT_EQ(FrameStatus::kComplete,
            DecodeResponseFrame(reply, kMaxFrame, 20, kCorrelationId, &decoded).status);
  ASSERT_EQ(35, decoded.error_code);
  ASSERT_EQ(1u, decoded.api_keys.size());
  EXPECT_EQ(ApiVersionsRequest::kApiKey, decoded.api_keys.front().api_key);
  EXPECT_EQ(ApiVersionsRequest::kMaxVersion, decoded.api_keys.front().max_version);
  EXPECT_EQ(FrameStatus::kComplete,
            DecodeResponseFrame(reply, kMaxFrame, 0, kCorrelationId, &decoded).status);
}

TEST(RpcFrameTest, ApiVersionsValidatesControllerRequestAndAdvertisesOnlyHandlers) {
  ApiVersionsResponse response;
  ApiVersionsRequest request;
  ASSERT_EQ(FrameStatus::kComplete, Negotiate(request, 2, &response).status);
  EXPECT_EQ(0, response.error_code);
  ASSERT_EQ(1u, response.api_keys.size());
  EXPECT_EQ(ApiVersionsRequest::kApiKey, response.api_keys[0].api_key);
  request.client_software_version = "0.0.0-SNAPSHOT";
  for (const auto* name : {"", "a@b", "-bad", "bad.", "under_score"}) {
    request.client_software_name = name;
    ASSERT_EQ(FrameStatus::kComplete, Negotiate(request, 4, &response).status);
    EXPECT_EQ(42, response.error_code);
    EXPECT_TRUE(response.api_keys.empty());
  }
  request.client_software_name = "df";
  request.node_id = 3;
  ASSERT_EQ(FrameStatus::kComplete, Negotiate(request, 5, &response).status);
  EXPECT_EQ(42, response.error_code);
  request.cluster_id = "cluster";
  ASSERT_EQ(FrameStatus::kComplete, Negotiate(request, 5, &response).status);
  EXPECT_EQ(0, response.error_code);
}

TEST(RpcFrameTest, FrameFragmentationAndCoalescingDispatchVoteOnlyOnce) {
  RequestMetadata header{kCorrelationId, "k", {{6, FromHex("a5")}}};
  std::string frame;
  ASSERT_EQ(CodecError::kOk, EncodeRequestFrame(header, VoteRequest{}, 2, &frame));
  ASSERT_EQ(FromHex(kVote), frame);
  size_t calls = 0;
  VoteHandler handle = [&](int16_t version, const VoteRequest& request, VoteResponse*) {
    ++calls;
    EXPECT_EQ(2, version);
    EXPECT_EQ(VoteRequest{}, request);
    return std::error_code{};
  };
  std::string reply = "original";
  for (size_t n = 0; n < frame.size(); ++n) {
    auto partial =
        DispatchRequestFrame(std::string_view(frame).substr(0, n), kMaxFrame, handle, &reply);
    EXPECT_EQ(FrameStatus::kNeedMore, partial.status);
    EXPECT_EQ(0u, partial.consumed);
    EXPECT_EQ(0u, calls);
    EXPECT_EQ("original", reply);
  }
  auto result = DispatchRequestFrame(frame + frame, kMaxFrame, handle, &reply);
  ASSERT_EQ(FrameStatus::kComplete, result.status);
  EXPECT_EQ(1u, calls);
  EXPECT_EQ(frame.size(), result.consumed);
  EXPECT_EQ(FromHex(kVoteReply), reply);
  VoteResponse response;
  EXPECT_EQ(FrameStatus::kComplete,
            DecodeResponseFrame(reply, kMaxFrame, 2, kCorrelationId, &response).status);
  result = DecodeResponseFrame(reply, kMaxFrame, 2, -1, &response);
  EXPECT_EQ(FrameStatus::kInvalidFrame, result.status);
  EXPECT_EQ(0u, result.consumed);
}

TEST(RpcFrameTest, InvalidFramesAndPersistenceErrorsNeverProduceReply) {
  auto vote = FromHex(kVote);
  std::string reply = "unchanged";
  VoteHandler failed = [](int16_t, const VoteRequest&, VoteResponse*) {
    return std::make_error_code(std::errc::io_error);
  };
  auto disk = DispatchRequestFrame(vote, kMaxFrame, failed, &reply);
  EXPECT_EQ(FrameStatus::kHandlerError, disk.status);
  EXPECT_EQ(std::errc::io_error, disk.handler_error);
  EXPECT_EQ(0u, disk.consumed);
  for (auto prefix : {"00000000", "ffffffff", "00000401"}) {
    auto result = DispatchRequestFrame(FromHex(prefix), kMaxFrame, failed, &reply);
    EXPECT_EQ(FrameStatus::kInvalidFrame, result.status);
    EXPECT_EQ(0u, result.consumed);
  }
  EXPECT_EQ(FrameStatus::kUnknownApi, DispatchRequestFrame(vote, kMaxFrame, {}, &reply).status);
  vote[5] = 99;
  EXPECT_EQ(FrameStatus::kUnknownApi, DispatchRequestFrame(vote, kMaxFrame, failed, &reply).status);
  vote = FromHex(kVote);
  vote[7] = 3;
  EXPECT_EQ(FrameStatus::kUnsupportedVersion,
            DispatchRequestFrame(vote, kMaxFrame, failed, &reply).status);
  vote = FromHex(kVote);
  vote.back() = static_cast<char>(0xff);
  EXPECT_EQ(FrameStatus::kInvalidFrame,
            DispatchRequestFrame(vote, kMaxFrame, failed, &reply).status);
  EXPECT_EQ("unchanged", reply);
}

TEST(RpcFrameTest, KafkaJavaBeginEndEpochFrames) {
  constexpr std::string_view begin_requests[] = {
      "00000025003500000102030400016b0001630000000100017400000001000000020000000100000007",
      "0000003d003500010102030400016b0002630000000202027402000000020000000000000000000000000000002a"
      "00000001000000070000020272026823840000"};
  constexpr std::string_view end_requests[] = {
      "00000031003600000102030400016b000163000000010001740000000100000002000000010000000700000002"
      "0000000200000003",
      "00000038003600010102030400016b00026302027402000000020000000100000007020000000200000000000000"
      "00000000000000002a0000000100"};
  constexpr std::string_view begin_responses[] = {"0000000a01020304000000000000",
                                                  "00000009010203040000000100"};
  constexpr std::string_view end_responses[] = {
      "0000000a01020304000000000000", "00000015010203040000000101000a02000000020268238400"};
  RequestMetadata metadata{kCorrelationId, "k", {}};
  for (int16_t version = 0; version <= 1; ++version) {
    SCOPED_TRACE(version);
    RpcHandlers handlers;
    handlers.begin_epoch = [&](int16_t received, const BeginQuorumEpochRequest& request,
                               BeginQuorumEpochResponse*) {
      EXPECT_EQ(version, received);
      EXPECT_EQ("c", request.cluster_id);
      if (request.topics.size() != 1 || request.topics[0].partitions.size() != 1)
        return std::make_error_code(std::errc::invalid_argument);
      const auto& part = request.topics[0].partitions[0];
      EXPECT_EQ(7, part.leader_epoch);
      EXPECT_EQ(2, part.partition_index);
      EXPECT_EQ(version ? 42 : 0, part.voter_directory_id.back());
      if (version == 1) {
        EXPECT_EQ(2, request.voter_id);
        EXPECT_EQ(1u, request.leader_endpoints.size());
      }
      std::string encoded;
      EXPECT_EQ(CodecError::kOk, EncodeRequestFrame(metadata, request, received, &encoded));
      EXPECT_EQ(FromHex(begin_requests[version]), encoded);
      return std::error_code{};
    };
    handlers.end_epoch = [&](int16_t received, const EndQuorumEpochRequest& request,
                             EndQuorumEpochResponse* response) {
      EXPECT_EQ(version, received);
      if (request.topics.size() != 1 || request.topics[0].partitions.size() != 1)
        return std::make_error_code(std::errc::invalid_argument);
      const auto& part = request.topics[0].partitions[0];
      if (version == 0) {
        EXPECT_EQ((std::vector<int32_t>{2, 3}), part.preferred_successors);
        EXPECT_TRUE(part.preferred_candidates.empty());
      } else {
        EXPECT_TRUE(part.preferred_successors.empty());
        if (part.preferred_candidates.size() != 1)
          return std::make_error_code(std::errc::invalid_argument);
        EXPECT_EQ(2, part.preferred_candidates[0].candidate_id);
        EXPECT_EQ(42, part.preferred_candidates[0].candidate_directory_id.back());
        auto& endpoint = response->node_endpoints.emplace_back();
        endpoint.node_id = 2;
        endpoint.host = "h";
        endpoint.port = 9092;
      }
      std::string encoded;
      EXPECT_EQ(CodecError::kOk, EncodeRequestFrame(metadata, request, received, &encoded));
      EXPECT_EQ(FromHex(end_requests[version]), encoded);
      return std::error_code{};
    };
    std::string reply;
    auto result = DispatchRpcFrame(FromHex(begin_requests[version]), kMaxFrame, handlers, &reply);
    ASSERT_EQ(FrameStatus::kComplete, result.status);
    EXPECT_EQ(FromHex(begin_responses[version]), reply);
    BeginQuorumEpochResponse begin;
    ASSERT_EQ(FrameStatus::kComplete,
              DecodeResponseFrame(reply, kMaxFrame, version, kCorrelationId, &begin).status);
    result = DispatchRpcFrame(FromHex(end_requests[version]), kMaxFrame, handlers, &reply);
    ASSERT_EQ(FrameStatus::kComplete, result.status);
    EXPECT_EQ(FromHex(end_responses[version]), reply);
    EndQuorumEpochResponse end;
    ASSERT_EQ(FrameStatus::kComplete,
              DecodeResponseFrame(reply, kMaxFrame, version, kCorrelationId, &end).status);
    EXPECT_EQ(static_cast<size_t>(version), end.node_endpoints.size());
    ASSERT_EQ(FrameStatus::kComplete,
              DispatchRpcFrame(FromHex(kRequest[0]), kMaxFrame, handlers, &reply).status);
    ApiVersionsResponse advertised;
    ASSERT_EQ(FrameStatus::kComplete,
              DecodeResponseFrame(reply, kMaxFrame, 0, kCorrelationId, &advertised).status);
    ASSERT_EQ(3u, advertised.api_keys.size());
    EXPECT_EQ(53, advertised.api_keys[1].api_key);
    EXPECT_EQ(54, advertised.api_keys[2].api_key);
  }
}

// Unaligned snapshot chunks may end in the middle of a record batch.
TEST(RpcFrameTest, KafkaJavaFetchSnapshotChunks) {
  constexpr std::string_view requests[] = {
      "0000003c003b00000102030400016b000000000200000003020274020000002a00000003000000000000002d"
      "0000000200000000000000000300000100020263",
      "0000004e003b00010102030400016b000000000200000003020274020000002a00000003000000000000002d"
      "000000020000000000000000030100100000000000000000000000000000004d000100020263"};
  constexpr std::string_view responses[] = {
      "000000390102030400000000000000020274020000002a0000000000000000002d0000000200000000000000"
      "0010000000000000000304010203000000",
      "000000450102030400000000000000020274020000002a0000000000000000002d0000000200000000000000"
      "0010000000000000000304010203000001000a02000000010268238400"};

  for (int16_t v = 0; v <= 1; ++v) {
    SCOPED_TRACE(v);
    RpcHandlers handlers;
    handlers.fetch_snapshot = [&](int16_t version, const FetchSnapshotRequest& request,
                                  FetchSnapshotResponse* response) {
      EXPECT_EQ(v, version);
      std::string encoded;
      EXPECT_EQ(CodecError::kOk,
                EncodeRequestFrame({kCorrelationId, "k", {}}, request, version, &encoded));
      EXPECT_EQ(FromHex(requests[v]), encoded);
      if (request.topics.size() != 1 || request.topics[0].partitions.size() != 1)
        return std::make_error_code(std::errc::invalid_argument);
      const auto& input = request.topics[0].partitions[0];
      EXPECT_EQ(45, input.snapshot_id.end_offset);
      EXPECT_EQ(2, input.snapshot_id.epoch);
      EXPECT_EQ(version ? 77 : 0, input.replica_directory_id.back());
      auto& topic = response->topics.emplace_back();
      topic.name = request.topics[0].name;
      auto& part = topic.partitions.emplace_back();
      part.index = input.partition;
      part.size = 16;
      part.position = input.position;
      part.snapshot_id.end_offset = input.snapshot_id.end_offset;
      part.snapshot_id.epoch = input.snapshot_id.epoch;
      part.unaligned_records.data = FromHex("010203");
      if (version) {
        auto& node = response->node_endpoints.emplace_back();
        node.node_id = 1;
        node.host = "h";
        node.port = 9092;
      }
      return std::error_code{};
    };
    std::string reply;
    ASSERT_EQ(FrameStatus::kComplete,
              DispatchRpcFrame(FromHex(requests[v]), kMaxFrame, handlers, &reply).status);
    EXPECT_EQ(FromHex(responses[v]), reply);
    FetchSnapshotResponse decoded;
    ASSERT_EQ(FrameStatus::kComplete,
              DecodeResponseFrame(reply, kMaxFrame, v, kCorrelationId, &decoded).status);
    auto& part = decoded.topics.at(0).partitions.at(0);
    EXPECT_EQ(FromHex("010203"), part.unaligned_records.data);
    part.unaligned_records.data.reset();
    std::string body;
    EXPECT_EQ(CodecError::kInvalidData, Encode(decoded, v, &body));
    part.unaligned_records.data.emplace();
    ASSERT_EQ(CodecError::kOk, Encode(decoded, v, &body));
  }
}

TEST(RpcFrameTest, KafkaJavaDescribeQuorumVersions) {
  constexpr std::string_view requests[] = {
      "00000017003700000102030400016b00020274020000002a000000",
      "00000017003700010102030400016b00020274020000002a000000",
      "00000017003700020102030400016b00020274020000002a000000"};
  constexpr std::string_view responses[] = {
      "0000003301020304000000020274020000002a00000000000100000003000000000000006502000000010000"
      "0000000000660001000000",
      "0000004301020304000000020274020000002a00000000000100000003000000000000006502000000010000"
      "000000000066000000000000006400000000000000630001000000",
      "000000630102030400000001020274020000002a000001000000010000000300000000000000650200000001"
      "0000000000000000000000000000005800000000000000660000000000000064000000000000006300010000"
      "020000000102027202682384000000"};

  for (int16_t v = 0; v <= 2; ++v) {
    SCOPED_TRACE(v);
    RpcHandlers handlers;
    handlers.describe_quorum = [&](int16_t version, const DescribeQuorumRequest& request,
                                   DescribeQuorumResponse* response) {
      EXPECT_EQ(v, version);
      if (request.topics.size() != 1 || request.topics[0].partitions.size() != 1)
        return std::make_error_code(std::errc::invalid_argument);
      std::string encoded;
      EXPECT_EQ(CodecError::kOk,
                EncodeRequestFrame({kCorrelationId, "k", {}}, request, version, &encoded));
      EXPECT_EQ(FromHex(requests[v]), encoded);
      auto& topic = response->topics.emplace_back();
      topic.topic_name = request.topics[0].topic_name;
      auto& part = topic.partitions.emplace_back();
      part.partition_index = request.topics[0].partitions[0].partition_index;
      part.leader_id = 1;
      part.leader_epoch = 3;
      part.high_watermark = 101;
      auto& voter = part.current_voters.emplace_back();
      voter.replica_id = part.leader_id;
      voter.log_end_offset = 102;
      if (version >= 1) {
        voter.last_fetch_timestamp = 100;
        voter.last_caught_up_timestamp = 99;
      }
      if (version >= 2) {
        voter.replica_directory_id.back() = 88;
        auto& node = response->nodes.emplace_back();
        node.node_id = voter.replica_id;
        auto& listener = node.listeners.emplace_back();
        listener.name = "r";
        listener.host = "h";
        listener.port = 9092;
      }
      return std::error_code{};
    };
    std::string reply;
    ASSERT_EQ(FrameStatus::kComplete,
              DispatchRpcFrame(FromHex(requests[v]), kMaxFrame, handlers, &reply).status);
    EXPECT_EQ(FromHex(responses[v]), reply);
    DescribeQuorumResponse decoded;
    ASSERT_EQ(FrameStatus::kComplete,
              DecodeResponseFrame(reply, kMaxFrame, v, kCorrelationId, &decoded).status);
    const auto& part = decoded.topics.at(0).partitions.at(0);
    EXPECT_EQ(101, part.high_watermark);
    EXPECT_EQ(102, part.current_voters.at(0).log_end_offset);
  }
}

TEST(RpcFrameTest, JavaVoterReconfigurationFrames) {
  // VoterVectors.java uses RequestUtils.serialize for these complete, length-prefixed frames.
  constexpr std::string_view requests[] = {
      "00000032005000000102030400016b000263000001f400000001000000000000000000000000000000020205"
      "52414654026223290000",
      "00000033005000010102030400016b000263000001f400000001000000000000000000000000000000020205"
      "5241465402622329000000",
      "00000023005100000102030400016b000263000000010000000000000000000000000000000200",
      "00000037005200000102030400016b0002630000000300000001000000000000000000000000000000020205"
      "524146540262232900000000010000",
  };
  constexpr std::string_view replies[] = {
      "00000016010203040000000000007e0a6475706c696361746500",
      "00000016010203040000000000007e0a6475706c696361746500",
      "0000000d010203040000000000007f0100",
      "0000001d010203040000000000000001000f000000020000000302610000232800",
  };
  RpcHandlers handlers;
  size_t index = 0;
  auto roundtrip = [&](int16_t version, const auto& request) {
    std::string bytes;
    EXPECT_EQ(EncodeRequestFrame({kCorrelationId, "k", {}}, request, version, &bytes),
              CodecError::kOk);
    EXPECT_EQ(bytes, FromHex(requests[index]));
    EXPECT_EQ(request.voter_id, 1);
    EXPECT_EQ(request.voter_directory_id.back(), 2);
  };
  handlers.add_voter = [&](int16_t version, const AddRaftVoterRequest& request,
                           AddRaftVoterResponse* out) {
    roundtrip(version, request);
    EXPECT_EQ(request.ack_when_committed, version == 0);
    EXPECT_EQ(request.listeners.at(0).port, 9001);
    out->error_code = 126;
    out->error_message = "duplicate";
    return std::error_code{};
  };
  handlers.remove_voter = [&](int16_t version, const RemoveRaftVoterRequest& request,
                              RemoveRaftVoterResponse* out) {
    roundtrip(version, request);
    out->error_code = 127;
    return std::error_code{};
  };
  handlers.update_voter = [&](int16_t version, const UpdateRaftVoterRequest& request,
                              UpdateRaftVoterResponse* out) {
    roundtrip(version, request);
    EXPECT_EQ(request.k_raft_version_feature.max_supported_version, 1);
    out->current_leader.leader_id = 2;
    out->current_leader.leader_epoch = 3;
    out->current_leader.host = "a";
    out->current_leader.port = 9000;
    return std::error_code{};
  };
  for (index = 0; index < std::size(requests); ++index) {
    std::string reply;
    auto request = FromHex(requests[index]);
    ASSERT_EQ(DispatchRpcFrame(request, kMaxFrame, {}, &reply).status, FrameStatus::kUnknownApi);
    ASSERT_EQ(DispatchRpcFrame(request, kMaxFrame, handlers, &reply).status,
              FrameStatus::kComplete);
    EXPECT_EQ(reply, FromHex(replies[index]));
  }
}

TEST(RpcFrameTest, KafkaJavaBrokerRegistrationAndHeartbeatFrames) {
  // BrokerVectors.java exercises every version, including the nullable v2 heartbeat tag.
  constexpr std::string_view requests[] = {
      "00000036003e00000102030400016b000000000702630000000000000000000000000000007b020272026823"
      "8400000002026600010003000000",
      "00000037003e00010102030400016b000000000702630000000000000000000000000000007b020272026823"
      "840000000202660001000300000100",
      "00000048003e00020102030400016b000000000702630000000000000000000000000000007b020272026823"
      "8400000002026600010003000001020000000000000000000000000000007b00",
      "00000050003e00030102030400016b000000000702630000000000000000000000000000007b020272026823"
      "8400000002026600010003000001020000000000000000000000000000007b000000000000000900",
      "00000050003e00040102030400016b000000000702630000000000000000000000000000007b020272026823"
      "8400000002026600010003000001020000000000000000000000000000007b000000000000000900",
      "00000023003f00000102030400016b000000000700000000000000180000000000000019010000",
      "00000036003f00010102030400016b0000000007000000000000001800000000000000190100010011020000"
      "000000000000000000000000007b",
      "00000039003f00020102030400016b0000000007000000000000001800000000000000190100020011020000"
      "000000000000000000000000007b010101",
  };
  constexpr std::string_view registration_reply =
      "000000140102030400000000000000000000000000001800";
  constexpr std::string_view heartbeat_reply = "0000000f010203040000000000004d00010000";
  RpcHandlers handlers;
  size_t index = 0;
  auto roundtrip = [&](int16_t version, const auto& request) {
    std::string encoded;
    EXPECT_EQ(CodecError::kOk,
              EncodeRequestFrame({kCorrelationId, "k", {}}, request, version, &encoded));
    EXPECT_EQ(FromHex(requests[index]), encoded);
    EXPECT_EQ(request.broker_id, 7);
  };
  handlers.broker_registration = [&](int16_t version, const BrokerRegistrationRequest& req,
                                     BrokerRegistrationResponse* res) {
    roundtrip(version, req);
    EXPECT_EQ(req.incarnation_id.back(), 123);
    EXPECT_FALSE(req.rack.has_value());
    EXPECT_EQ(req.listeners.at(0).port, 9092);
    EXPECT_EQ(req.features.at(0).max_supported_version, 3);
    EXPECT_EQ(req.is_migrating_zk_broker, version >= 1);
    EXPECT_EQ(req.log_dirs.size(), version >= 2 ? 1u : 0u);
    EXPECT_EQ(req.previous_broker_epoch, version >= 3 ? 9 : -1);
    res->broker_epoch = 24;
    return std::error_code{};
  };
  handlers.broker_heartbeat = [&](int16_t version, const BrokerHeartbeatRequest& req,
                                  BrokerHeartbeatResponse* res) {
    roundtrip(version, req);
    EXPECT_EQ(req.broker_epoch, 24);
    EXPECT_EQ(req.current_metadata_offset, 25);
    EXPECT_TRUE(req.want_fence);
    EXPECT_EQ(req.offline_log_dirs.size(), version >= 1 ? 1u : 0u);
    EXPECT_EQ(req.cordoned_log_dirs.has_value(), version >= 2);
    if (req.cordoned_log_dirs)
      EXPECT_TRUE(req.cordoned_log_dirs->empty());
    res->error_code = 77;
    return std::error_code{};
  };
  for (index = 0; index < std::size(requests); ++index) {
    std::string reply;
    ASSERT_EQ(DispatchRpcFrame(FromHex(requests[index]), kMaxFrame, handlers, &reply).status,
              FrameStatus::kComplete);
    if (index <= BrokerRegistrationRequest::kMaxVersion) {
      EXPECT_EQ(reply, FromHex(registration_reply));
      BrokerRegistrationResponse res;
      EXPECT_EQ(DecodeResponseFrame(reply, kMaxFrame, index, kCorrelationId, &res).status,
                FrameStatus::kComplete);
      EXPECT_EQ(res.broker_epoch, 24);
    } else {
      EXPECT_EQ(reply, FromHex(heartbeat_reply));
      BrokerHeartbeatResponse res;
      int16_t version = index - BrokerRegistrationRequest::kMaxVersion - 1;
      EXPECT_EQ(DecodeResponseFrame(reply, kMaxFrame, version, kCorrelationId, &res).status,
                FrameStatus::kComplete);
      EXPECT_EQ(res.error_code, 77);
      EXPECT_TRUE(res.is_fenced);
    }
  }
}

TEST(RpcFrameTest, KafkaJavaApiVersionsFeatureTags) {
  auto frame = FromHex(
      "000000450102030400000100000000040014020e6b726166742e76657273696f6e00000001000108"
      "00000000000000080214020e6b726166742e76657273696f6e0001000100030101");
  ApiVersionsResponse decoded;
  ASSERT_EQ(FrameStatus::kComplete,
            DecodeResponseFrame(frame, kMaxFrame, 4, kCorrelationId, &decoded).status);
  EXPECT_EQ(8, decoded.finalized_features_epoch);
  EXPECT_TRUE(decoded.zk_migration_ready);
  ASSERT_EQ(1u, decoded.supported_features.size());
  EXPECT_EQ("kraft.version", decoded.supported_features[0].name);
  ASSERT_EQ(1u, decoded.finalized_features.size());
  EXPECT_EQ(1, decoded.finalized_features[0].min_version_level);
  std::string body;
  ASSERT_EQ(CodecError::kOk, Encode(decoded, 4, &body));
  EXPECT_EQ(std::string_view(frame).substr(8), body);
}

TEST(RpcFrameTest, DynamicQuorumAdvertisesKraftVersionFromApiVersionsV4) {
  RpcHandlers handlers;
  auto handler = [](int16_t, const auto&, auto*) { return std::error_code{}; };
  handlers.add_voter = handler;
  handlers.remove_voter = handler;
  handlers.update_voter = handler;
  for (int16_t version : {3, 4, 5}) {
    std::string reply;
    ASSERT_EQ(DispatchRpcFrame(FromHex(kRequest[version]), kMaxFrame, handlers, &reply).status,
              FrameStatus::kComplete);
    ApiVersionsResponse response;
    ASSERT_EQ(DecodeResponseFrame(reply, kMaxFrame, version, kCorrelationId, &response).status,
              FrameStatus::kComplete);
    if (version == 3) {
      EXPECT_TRUE(response.supported_features.empty());
    } else {
      ASSERT_EQ(response.supported_features.size(), 1u);
      EXPECT_EQ(response.supported_features[0].name, "kraft.version");
      EXPECT_EQ(response.supported_features[0].min_version, 0);
      EXPECT_EQ(response.supported_features[0].max_version, 1);
    }
  }
}

}  // namespace
}  // namespace dfly::cluster::raft::protocol
