// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <functional>
#include <system_error>

#include "server/cluster/raft/protocol/api_versions.h"
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

using VoteHandler = std::function<std::error_code(int16_t, const VoteRequest&, VoteResponse*)>;

// Reads one size-prefixed request from the accumulated receive buffer. Only kComplete consumes
// bytes and replaces response. On kNeedMore append more data; all other failures close this
// connection without replying. A local persistence failure is returned in handler_error.
// ApiVersions advertises Vote only when this listener has a Vote handler.
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

// The caller supplies the expected API version and correlation ID of the in-flight request.
[[nodiscard]] FrameResult DecodeResponseFrame(std::string_view input, size_t max_payload_size,
                                              int16_t version, int32_t correlation_id,
                                              VoteResponse* response);
[[nodiscard]] FrameResult DecodeResponseFrame(std::string_view input, size_t max_payload_size,
                                              int16_t version, int32_t correlation_id,
                                              ApiVersionsResponse* response);

}  // namespace dfly::cluster::raft::protocol
