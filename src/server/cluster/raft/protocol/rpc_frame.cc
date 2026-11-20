// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/protocol/rpc_frame.h"

#include "server/cluster/raft/protocol/flexible_message.h"

namespace dfly::cluster::raft::protocol {
namespace {

using detail::Reader;
using detail::Writer;

constexpr int16_t kUnsupportedVersion = 35;
constexpr int16_t kInvalidRequest = 42;

FrameResult ReadFrame(std::string_view input, size_t max_payload_size, std::string_view* payload) {
  Reader reader(0, input);
  int32_t size;
  if (!reader.Field(size))
    return {FrameStatus::kNeedMore};
  if (size <= 0 || static_cast<size_t>(size) > max_payload_size)
    return {FrameStatus::kInvalidFrame};
  if (reader.remaining().size() < static_cast<size_t>(size))
    return {FrameStatus::kNeedMore};
  *payload = reader.remaining().substr(0, size);
  return {FrameStatus::kComplete, sizeof(size) + static_cast<size_t>(size)};
}

CodecError WriteFrame(std::string_view header, std::string_view body, std::string* output) {
  constexpr size_t kMaxSize = std::numeric_limits<int32_t>::max();
  if (header.size() > kMaxSize || body.size() > kMaxSize - header.size())
    return CodecError::kInvalidData;
  std::string result;
  result.reserve(sizeof(int32_t) + header.size() + body.size());
  Writer(0, &result).Field(static_cast<int32_t>(header.size() + body.size()));
  result.append(header);
  result.append(body);
  *output = std::move(result);
  return CodecError::kOk;
}

template <typename Request>
CodecError WriteRequest(const RequestMetadata& metadata, const Request& request, int16_t version,
                        int16_t header_version, std::string* output) {
  std::string body;
  auto err = Encode(request, version, &body);
  if (err != CodecError::kOk)
    return err;
  std::string header;
  Writer writer(header_version, &header, header_version >= 2);
  writer.Field(Request::kApiKey);
  writer.Field(version);
  writer.Field(metadata.correlation_id);
  if (!writer.LegacyClientId(metadata.client_id) || !writer.Tags(metadata.unknown_tags))
    return writer.error;
  return WriteFrame(header, body, output);
}

template <typename Response>
FrameStatus WriteResponse(const Response& response, int16_t version, int32_t correlation_id,
                          std::string* output) {
  std::string body;
  if (Encode(response, version, &body) != CodecError::kOk)
    return FrameStatus::kInvalidFrame;
  std::string header;
  Writer writer(Response::kHeaderVersion, &header);
  writer.Field(correlation_id);
  if constexpr (Response::kHeaderVersion >= 1)
    writer.Tags({});
  if (WriteFrame(header, body, output) != CodecError::kOk)
    return FrameStatus::kInvalidFrame;
  return FrameStatus::kComplete;
}

bool ValidSoftwareId(std::string_view id) {
  auto alnum = [](char ch) {
    return (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9');
  };
  return !id.empty() && alnum(id.front()) && alnum(id.back()) &&
         std::all_of(id.begin(), id.end(),
                     [&](char ch) { return alnum(ch) || ch == '-' || ch == '.'; });
}

bool IsValid(const ApiVersionsRequest& request, int16_t version) {
  if (version >= 5 && request.cluster_id.has_value() != (request.node_id != -1))
    return false;
  return version < 3 || (ValidSoftwareId(request.client_software_name) &&
                         ValidSoftwareId(request.client_software_version));
}

template <typename Request> void Advertise(ApiVersionsResponse* response) {
  auto& api = response->api_keys.emplace_back();
  api.api_key = Request::kApiKey;
  api.min_version = Request::kMinVersion;
  api.max_version = Request::kMaxVersion;
}

FrameStatus HandleApiVersions(std::string_view body, int16_t version, int32_t correlation_id,
                              bool serve_votes, std::string* output) {
  ApiVersionsResponse response;
  if (version < ApiVersionsRequest::kMinVersion || version > ApiVersionsRequest::kMaxVersion) {
    // KIP-511 uses a v0 response before the client and server have negotiated a version.
    response.error_code = kUnsupportedVersion;
    Advertise<ApiVersionsRequest>(&response);
    return WriteResponse(response, 0, correlation_id, output);
  }

  ApiVersionsRequest request;
  if (Decode(body, version, &request) != CodecError::kOk)
    return FrameStatus::kInvalidFrame;
  if (IsValid(request, version)) {
    Advertise<ApiVersionsRequest>(&response);
    if (serve_votes)
      Advertise<VoteRequest>(&response);
  } else {
    response.error_code = kInvalidRequest;
  }
  return WriteResponse(response, version, correlation_id, output);
}

template <typename Response>
FrameResult ReadResponse(std::string_view input, size_t max_payload_size, int16_t version,
                         int32_t correlation_id, Response* response) {
  std::string_view payload;
  auto result = ReadFrame(input, max_payload_size, &payload);
  if (result.status != FrameStatus::kComplete)
    return result;

  Reader reader(Response::kHeaderVersion, payload);
  int32_t received_id;
  if (!reader.Field(received_id) || received_id != correlation_id)
    return {FrameStatus::kInvalidFrame};
  if constexpr (Response::kHeaderVersion >= 1) {
    TaggedFields tags;
    if (!reader.Tags(tags))
      return {FrameStatus::kInvalidFrame};
  }
  auto error = Decode(reader.remaining(), version, response);
  if constexpr (std::is_same_v<Response, ApiVersionsResponse>) {
    if (error != CodecError::kOk && version != 0)
      error = Decode(reader.remaining(), 0, response);
  }
  if (error == CodecError::kUnsupportedVersion)
    return {FrameStatus::kUnsupportedVersion};
  if (error != CodecError::kOk)
    return {FrameStatus::kInvalidFrame};
  return result;
}

}  // namespace

FrameResult DispatchRequestFrame(std::string_view input, size_t max_payload_size,
                                 const VoteHandler& vote_handler, std::string* response) {
  std::string_view payload;
  auto result = ReadFrame(input, max_payload_size, &payload);
  if (result.status != FrameStatus::kComplete)
    return result;

  Reader reader(0, payload);
  int16_t api_key, version;
  int32_t correlation_id;
  if (!reader.Field(api_key) || !reader.Field(version) || !reader.Field(correlation_id))
    return {FrameStatus::kInvalidFrame};
  if (api_key != ApiVersionsRequest::kApiKey && (api_key != VoteRequest::kApiKey || !vote_handler))
    return {FrameStatus::kUnknownApi};
  std::optional<std::string> client_id;
  if (!reader.LegacyClientId(client_id))
    return {FrameStatus::kInvalidFrame};
  const int16_t header_version = api_key == ApiVersionsRequest::kApiKey
                                     ? ApiVersionsRequest::HeaderVersion(version)
                                     : VoteRequest::kHeaderVersion;
  if (header_version >= 2) {
    TaggedFields tags;
    if (!reader.Tags(tags))
      return {FrameStatus::kInvalidFrame};
  }

  FrameStatus status;
  if (api_key == ApiVersionsRequest::kApiKey) {
    status = HandleApiVersions(reader.remaining(), version, correlation_id,
                               static_cast<bool>(vote_handler), response);
  } else {
    VoteRequest request;
    auto error = Decode(reader.remaining(), version, &request);
    if (error == CodecError::kUnsupportedVersion)
      return {FrameStatus::kUnsupportedVersion};
    if (error != CodecError::kOk)
      return {FrameStatus::kInvalidFrame};
    VoteResponse reply;
    if (auto err = vote_handler(version, request, &reply))
      return {FrameStatus::kHandlerError, 0, err};
    status = WriteResponse(reply, version, correlation_id, response);
  }

  if (status != FrameStatus::kComplete)
    return {status};
  return result;
}

CodecError EncodeRequestFrame(const RequestMetadata& metadata, const VoteRequest& request,
                              int16_t version, std::string* output) {
  return WriteRequest(metadata, request, version, VoteRequest::kHeaderVersion, output);
}

CodecError EncodeRequestFrame(const RequestMetadata& metadata, const ApiVersionsRequest& request,
                              int16_t version, std::string* output) {
  return WriteRequest(metadata, request, version, ApiVersionsRequest::HeaderVersion(version),
                      output);
}

FrameResult DecodeResponseFrame(std::string_view input, size_t max_payload_size, int16_t version,
                                int32_t correlation_id, VoteResponse* response) {
  return ReadResponse(input, max_payload_size, version, correlation_id, response);
}

FrameResult DecodeResponseFrame(std::string_view input, size_t max_payload_size, int16_t version,
                                int32_t correlation_id, ApiVersionsResponse* response) {
  return ReadResponse(input, max_payload_size, version, correlation_id, response);
}

}  // namespace dfly::cluster::raft::protocol
