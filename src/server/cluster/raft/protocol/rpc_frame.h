// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <functional>
#include <optional>
#include <system_error>

#include "server/cluster/raft/protocol/add_voter.h"
#include "server/cluster/raft/protocol/api_versions.h"
#include "server/cluster/raft/protocol/begin_epoch.h"
#include "server/cluster/raft/protocol/broker_heartbeat.h"
#include "server/cluster/raft/protocol/broker_registration.h"
#include "server/cluster/raft/protocol/describe_quorum.h"
#include "server/cluster/raft/protocol/end_epoch.h"
#include "server/cluster/raft/protocol/fetch.h"
#include "server/cluster/raft/protocol/fetch_slot_image.h"
#include "server/cluster/raft/protocol/fetch_snapshot.h"
#include "server/cluster/raft/protocol/remove_voter.h"
#include "server/cluster/raft/protocol/slot_migration.h"
#include "server/cluster/raft/protocol/update_voter.h"
#include "server/cluster/raft/protocol/vote.h"

namespace dfly::cluster::raft::protocol {

enum class FrameStatus {
  kComplete,
  kNeedMore,
  kInvalidFrame,
  kUnknownApi,
  kUnsupportedVersion,
  kHandlerError,
};

struct FrameResult {
  FrameStatus status;
  size_t consumed = 0;
  std::error_code handler_error{};
};

// Returns the total wire size from a complete, valid Kafka length prefix.
[[nodiscard]] std::optional<size_t> RpcFrameSize(std::string_view prefix, size_t max_payload_size);

template <typename Request, typename Response>
using RpcHandler = std::function<std::error_code(int16_t, const Request&, Response*)>;

using VoteHandler = RpcHandler<VoteRequest, VoteResponse>;

// KRaft locates the replicated log by topic UUID, available starting with Fetch v13.
inline constexpr int16_t kMinRaftFetchVersion = 13;

struct RpcHandlers {
  VoteHandler vote;
  RpcHandler<BrokerRegistrationRequest, BrokerRegistrationResponse> broker_registration;
  RpcHandler<BrokerHeartbeatRequest, BrokerHeartbeatResponse> broker_heartbeat;
  RpcHandler<AddRaftVoterRequest, AddRaftVoterResponse> add_voter;
  RpcHandler<RemoveRaftVoterRequest, RemoveRaftVoterResponse> remove_voter;
  RpcHandler<UpdateRaftVoterRequest, UpdateRaftVoterResponse> update_voter;
  RpcHandler<BeginQuorumEpochRequest, BeginQuorumEpochResponse> begin_epoch;
  RpcHandler<EndQuorumEpochRequest, EndQuorumEpochResponse> end_epoch;
  RpcHandler<DescribeQuorumRequest, DescribeQuorumResponse> describe_quorum;
  RpcHandler<FetchSnapshotRequest, FetchSnapshotResponse> fetch_snapshot;
  RpcHandler<FetchRequest, FetchResponse> fetch;
  RpcHandler<SlotMigrationRequest, SlotMigrationResponse> slot_migration;
  RpcHandler<FetchSlotImageRequest, FetchSlotImageResponse> fetch_slot_image;
};

// Reads one size-prefixed request from the accumulated receive buffer. Only kComplete consumes
// bytes and replaces response. On kNeedMore append more data; all other failures close this
// connection without replying. A local persistence failure is returned in handler_error.
// ApiVersions advertises only installed handlers.
[[nodiscard]] FrameResult DispatchRpcFrame(std::string_view input, size_t max_payload_size,
                                           const RpcHandlers& handlers, std::string* response);

// Convenience entry point for listeners that implement only Vote and ApiVersions.
[[nodiscard]] FrameResult DispatchRequestFrame(std::string_view input, size_t max_payload_size,
                                               const VoteHandler& vote_handler,
                                               std::string* response);

struct RequestMetadata {
  int32_t correlation_id = 0;
  std::optional<std::string> client_id;
  TaggedFields unknown_tags;
};

// Writes a complete size-prefixed request, including the version-specific Kafka header.
[[nodiscard]] CodecError EncodeRequestFrame(const RequestMetadata& header,
                                            const VoteRequest& request, int16_t version,
                                            std::string* output);
[[nodiscard]] CodecError EncodeRequestFrame(const RequestMetadata& header,
                                            const ApiVersionsRequest& request, int16_t version,
                                            std::string* output);

[[nodiscard]] CodecError EncodeRequestFrame(const RequestMetadata& header,
                                            const BeginQuorumEpochRequest& request, int16_t version,
                                            std::string* output);

[[nodiscard]] CodecError EncodeRequestFrame(const RequestMetadata& header,
                                            const EndQuorumEpochRequest& request, int16_t version,
                                            std::string* output);

// The caller supplies the expected API version and correlation ID of the in-flight request.
[[nodiscard]] FrameResult DecodeResponseFrame(std::string_view input, size_t max_payload_size,
                                              int16_t version, int32_t correlation_id,
                                              VoteResponse* response);
[[nodiscard]] FrameResult DecodeResponseFrame(std::string_view input, size_t max_payload_size,
                                              int16_t version, int32_t correlation_id,
                                              ApiVersionsResponse* response);

[[nodiscard]] FrameResult DecodeResponseFrame(std::string_view input, size_t max_payload_size,
                                              int16_t version, int32_t correlation_id,
                                              BeginQuorumEpochResponse* response);

[[nodiscard]] FrameResult DecodeResponseFrame(std::string_view input, size_t max_payload_size,
                                              int16_t version, int32_t correlation_id,
                                              EndQuorumEpochResponse* response);

// Fetches built from a follower's replica_state are adapted to the v13/v14 replica_id
// representation on the wire, as in Kafka FetchRequest.SimpleBuilder.
[[nodiscard]] CodecError EncodeRequestFrame(const RequestMetadata& header,
                                            const FetchRequest& request, int16_t version,
                                            std::string* output);
[[nodiscard]] FrameResult DecodeResponseFrame(std::string_view input, size_t max_payload_size,
                                              int16_t version, int32_t correlation_id,
                                              FetchResponse* response);

[[nodiscard]] CodecError EncodeRequestFrame(const RequestMetadata& header,
                                            const DescribeQuorumRequest& request, int16_t version,
                                            std::string* output);
[[nodiscard]] FrameResult DecodeResponseFrame(std::string_view input, size_t max_payload_size,
                                              int16_t version, int32_t correlation_id,
                                              DescribeQuorumResponse* response);

[[nodiscard]] CodecError EncodeRequestFrame(const RequestMetadata& header,
                                            const FetchSnapshotRequest& request, int16_t version,
                                            std::string* output);
[[nodiscard]] FrameResult DecodeResponseFrame(std::string_view input, size_t max_payload_size,
                                              int16_t version, int32_t correlation_id,
                                              FetchSnapshotResponse* response);

[[nodiscard]] CodecError EncodeRequestFrame(const RequestMetadata& header,
                                            const AddRaftVoterRequest& request, int16_t version,
                                            std::string* output);
[[nodiscard]] FrameResult DecodeResponseFrame(std::string_view input, size_t max_payload_size,
                                              int16_t version, int32_t correlation_id,
                                              AddRaftVoterResponse* response);

[[nodiscard]] CodecError EncodeRequestFrame(const RequestMetadata& header,
                                            const RemoveRaftVoterRequest& request, int16_t version,
                                            std::string* output);
[[nodiscard]] FrameResult DecodeResponseFrame(std::string_view input, size_t max_payload_size,
                                              int16_t version, int32_t correlation_id,
                                              RemoveRaftVoterResponse* response);

[[nodiscard]] CodecError EncodeRequestFrame(const RequestMetadata& header,
                                            const UpdateRaftVoterRequest& request, int16_t version,
                                            std::string* output);
[[nodiscard]] FrameResult DecodeResponseFrame(std::string_view input, size_t max_payload_size,
                                              int16_t version, int32_t correlation_id,
                                              UpdateRaftVoterResponse* response);

[[nodiscard]] CodecError EncodeRequestFrame(const RequestMetadata& header,
                                            const BrokerRegistrationRequest& request,
                                            int16_t version, std::string* output);
[[nodiscard]] FrameResult DecodeResponseFrame(std::string_view input, size_t max_payload_size,
                                              int16_t version, int32_t correlation_id,
                                              BrokerRegistrationResponse* response);

[[nodiscard]] CodecError EncodeRequestFrame(const RequestMetadata& header,
                                            const BrokerHeartbeatRequest& request, int16_t version,
                                            std::string* output);
[[nodiscard]] FrameResult DecodeResponseFrame(std::string_view input, size_t max_payload_size,
                                              int16_t version, int32_t correlation_id,
                                              BrokerHeartbeatResponse* response);

[[nodiscard]] CodecError EncodeRequestFrame(const RequestMetadata& header,
                                            const SlotMigrationRequest& request, int16_t version,
                                            std::string* output);
[[nodiscard]] FrameResult DecodeResponseFrame(std::string_view input, size_t max_payload_size,
                                              int16_t version, int32_t correlation_id,
                                              SlotMigrationResponse* response);

[[nodiscard]] CodecError EncodeRequestFrame(const RequestMetadata& header,
                                            const FetchSlotImageRequest& request, int16_t version,
                                            std::string* output);
[[nodiscard]] FrameResult DecodeResponseFrame(std::string_view input, size_t max_payload_size,
                                              int16_t version, int32_t correlation_id,
                                              FetchSlotImageResponse* response);

}  // namespace dfly::cluster::raft::protocol
