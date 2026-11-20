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
  Writer writer(Response::HeaderVersion(version), &header);
  writer.Field(correlation_id);
  if (Response::HeaderVersion(version) >= 1)
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

template <typename Request>
void Advertise(ApiVersionsResponse* response, int16_t min_version = Request::kMinVersion) {
  auto& api = response->api_keys.emplace_back();
  api.api_key = Request::kApiKey;
  api.min_version = min_version;
  api.max_version = Request::kMaxVersion;
}

FrameStatus HandleApiVersions(std::string_view body, int16_t version, int32_t correlation_id,
                              const RpcHandlers& handlers, std::string* output) {
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
    if (handlers.fetch)
      Advertise<FetchRequest>(&response, kMinRaftFetchVersion);
    Advertise<ApiVersionsRequest>(&response);
    if (handlers.vote)
      Advertise<VoteRequest>(&response);
    if (handlers.begin_epoch)
      Advertise<BeginQuorumEpochRequest>(&response);
    if (handlers.end_epoch)
      Advertise<EndQuorumEpochRequest>(&response);
    if (handlers.describe_quorum)
      Advertise<DescribeQuorumRequest>(&response);
    if (handlers.broker_registration)
      Advertise<BrokerRegistrationRequest>(&response);
    if (handlers.broker_heartbeat)
      Advertise<BrokerHeartbeatRequest>(&response);
    if (handlers.fetch_snapshot)
      Advertise<FetchSnapshotRequest>(&response);
    if (handlers.add_voter)
      Advertise<AddRaftVoterRequest>(&response);
    if (handlers.remove_voter)
      Advertise<RemoveRaftVoterRequest>(&response);
    if (handlers.update_voter)
      Advertise<UpdateRaftVoterRequest>(&response);

    // Kafka omits features with minimum version zero in ApiVersions responses before v4.
    if (version >= 4 && handlers.add_voter && handlers.remove_voter && handlers.update_voter)
      response.supported_features.push_back({"kraft.version", 0, 1, {}});
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

  Reader reader(Response::HeaderVersion(version), payload);
  int32_t received_id;
  if (!reader.Field(received_id) || received_id != correlation_id)
    return {FrameStatus::kInvalidFrame};
  if (Response::HeaderVersion(version) >= 1) {
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

bool ReadHeaderTags(int16_t version, Reader* reader) {
  TaggedFields tags;
  return version < 2 || reader->Tags(tags);
}

template <typename Request, typename Response>
FrameResult Dispatch(int16_t version, int32_t correlation_id, Reader* reader,
                     const RpcHandler<Request, Response>& handler, std::string* output) {
  if (!handler)
    return {FrameStatus::kUnknownApi};
  if (!ReadHeaderTags(Request::HeaderVersion(version), reader))
    return {FrameStatus::kInvalidFrame};
  Request request;
  auto error = Decode(reader->remaining(), version, &request);
  if (error == CodecError::kUnsupportedVersion)
    return {FrameStatus::kUnsupportedVersion};
  if (error != CodecError::kOk)
    return {FrameStatus::kInvalidFrame};
  Response reply;
  if (auto err = handler(version, request, &reply))
    return {FrameStatus::kHandlerError, 0, err};
  return {WriteResponse(reply, version, correlation_id, output)};
}

}  // namespace

FrameResult DispatchRequestFrame(std::string_view input, size_t max_payload_size,
                                 const VoteHandler& vote_handler, std::string* response) {
  RpcHandlers handlers;
  handlers.vote = vote_handler;
  return DispatchRpcFrame(input, max_payload_size, handlers, response);
}

FrameResult DispatchRpcFrame(std::string_view input, size_t max_payload_size,
                             const RpcHandlers& handlers, std::string* response) {
  std::string_view payload;
  auto result = ReadFrame(input, max_payload_size, &payload);
  if (result.status != FrameStatus::kComplete)
    return result;

  Reader reader(0, payload);
  int16_t api_key, version;
  int32_t correlation_id;
  if (!reader.Field(api_key) || !reader.Field(version) || !reader.Field(correlation_id))
    return {FrameStatus::kInvalidFrame};
  std::optional<std::string> client_id;
  if (!reader.LegacyClientId(client_id))
    return {FrameStatus::kInvalidFrame};

  FrameResult dispatched;
  switch (api_key) {
    case ApiVersionsRequest::kApiKey:
      if (!ReadHeaderTags(ApiVersionsRequest::HeaderVersion(version), &reader))
        return {FrameStatus::kInvalidFrame};
      dispatched = {
          HandleApiVersions(reader.remaining(), version, correlation_id, handlers, response)};
      break;
    case FetchRequest::kApiKey:
      if (handlers.fetch && version < kMinRaftFetchVersion)
        return {FrameStatus::kUnsupportedVersion};
      dispatched = Dispatch(version, correlation_id, &reader, handlers.fetch, response);
      break;
    case VoteRequest::kApiKey:
      dispatched = Dispatch(version, correlation_id, &reader, handlers.vote, response);
      break;
    case BeginQuorumEpochRequest::kApiKey:
      dispatched = Dispatch(version, correlation_id, &reader, handlers.begin_epoch, response);
      break;
    case EndQuorumEpochRequest::kApiKey:
      dispatched = Dispatch(version, correlation_id, &reader, handlers.end_epoch, response);
      break;
    case DescribeQuorumRequest::kApiKey:
      dispatched = Dispatch(version, correlation_id, &reader, handlers.describe_quorum, response);
      break;
    case BrokerRegistrationRequest::kApiKey:
      dispatched =
          Dispatch(version, correlation_id, &reader, handlers.broker_registration, response);
      break;
    case BrokerHeartbeatRequest::kApiKey:
      dispatched = Dispatch(version, correlation_id, &reader, handlers.broker_heartbeat, response);
      break;
    case FetchSnapshotRequest::kApiKey:
      dispatched = Dispatch(version, correlation_id, &reader, handlers.fetch_snapshot, response);
      break;
    case AddRaftVoterRequest::kApiKey:
      dispatched = Dispatch(version, correlation_id, &reader, handlers.add_voter, response);
      break;
    case RemoveRaftVoterRequest::kApiKey:
      dispatched = Dispatch(version, correlation_id, &reader, handlers.remove_voter, response);
      break;
    case UpdateRaftVoterRequest::kApiKey:
      dispatched = Dispatch(version, correlation_id, &reader, handlers.update_voter, response);
      break;
    default:
      return {FrameStatus::kUnknownApi};
  }
  if (dispatched.status != FrameStatus::kComplete)
    return dispatched;
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

CodecError EncodeRequestFrame(const RequestMetadata& metadata,
                              const BeginQuorumEpochRequest& request, int16_t version,
                              std::string* output) {
  return WriteRequest(metadata, request, version, BeginQuorumEpochRequest::HeaderVersion(version),
                      output);
}

FrameResult DecodeResponseFrame(std::string_view input, size_t max_payload_size, int16_t version,
                                int32_t correlation_id, BeginQuorumEpochResponse* response) {
  return ReadResponse(input, max_payload_size, version, correlation_id, response);
}

CodecError EncodeRequestFrame(const RequestMetadata& metadata, const EndQuorumEpochRequest& request,
                              int16_t version, std::string* output) {
  return WriteRequest(metadata, request, version, EndQuorumEpochRequest::HeaderVersion(version),
                      output);
}

FrameResult DecodeResponseFrame(std::string_view input, size_t max_payload_size, int16_t version,
                                int32_t correlation_id, EndQuorumEpochResponse* response) {
  return ReadResponse(input, max_payload_size, version, correlation_id, response);
}

CodecError EncodeRequestFrame(const RequestMetadata& metadata, const FetchRequest& request,
                              int16_t version, std::string* output) {
  if (version < kMinRaftFetchVersion)
    return CodecError::kUnsupportedVersion;
  if (version < 15 && request.replica_state != FetchRequest::ReplicaState{}) {
    if (request.replica_id != -1)
      return CodecError::kInvalidData;
    FetchRequest legacy = request;
    legacy.replica_id = request.replica_state.replica_id;
    legacy.replica_state = {};
    return WriteRequest(metadata, legacy, version, FetchRequest::HeaderVersion(version), output);
  }
  return WriteRequest(metadata, request, version, FetchRequest::HeaderVersion(version), output);
}

FrameResult DecodeResponseFrame(std::string_view input, size_t max_payload_size, int16_t version,
                                int32_t correlation_id, FetchResponse* response) {
  return ReadResponse(input, max_payload_size, version, correlation_id, response);
}

CodecError EncodeRequestFrame(const RequestMetadata& metadata, const DescribeQuorumRequest& request,
                              int16_t version, std::string* output) {
  return WriteRequest(metadata, request, version, DescribeQuorumRequest::HeaderVersion(version),
                      output);
}

FrameResult DecodeResponseFrame(std::string_view input, size_t max_payload_size, int16_t version,
                                int32_t correlation_id, DescribeQuorumResponse* response) {
  return ReadResponse(input, max_payload_size, version, correlation_id, response);
}

CodecError EncodeRequestFrame(const RequestMetadata& metadata, const FetchSnapshotRequest& request,
                              int16_t version, std::string* output) {
  return WriteRequest(metadata, request, version, FetchSnapshotRequest::HeaderVersion(version),
                      output);
}

FrameResult DecodeResponseFrame(std::string_view input, size_t max_payload_size, int16_t version,
                                int32_t correlation_id, FetchSnapshotResponse* response) {
  return ReadResponse(input, max_payload_size, version, correlation_id, response);
}

CodecError EncodeRequestFrame(const RequestMetadata& metadata, const AddRaftVoterRequest& request,
                              int16_t version, std::string* output) {
  return WriteRequest(metadata, request, version, AddRaftVoterRequest::HeaderVersion(version),
                      output);
}

FrameResult DecodeResponseFrame(std::string_view input, size_t max_payload_size, int16_t version,
                                int32_t correlation_id, AddRaftVoterResponse* response) {
  return ReadResponse(input, max_payload_size, version, correlation_id, response);
}

CodecError EncodeRequestFrame(const RequestMetadata& metadata,
                              const RemoveRaftVoterRequest& request, int16_t version,
                              std::string* output) {
  return WriteRequest(metadata, request, version, RemoveRaftVoterRequest::HeaderVersion(version),
                      output);
}

FrameResult DecodeResponseFrame(std::string_view input, size_t max_payload_size, int16_t version,
                                int32_t correlation_id, RemoveRaftVoterResponse* response) {
  return ReadResponse(input, max_payload_size, version, correlation_id, response);
}

CodecError EncodeRequestFrame(const RequestMetadata& metadata,
                              const UpdateRaftVoterRequest& request, int16_t version,
                              std::string* output) {
  return WriteRequest(metadata, request, version, UpdateRaftVoterRequest::HeaderVersion(version),
                      output);
}

FrameResult DecodeResponseFrame(std::string_view input, size_t max_payload_size, int16_t version,
                                int32_t correlation_id, UpdateRaftVoterResponse* response) {
  return ReadResponse(input, max_payload_size, version, correlation_id, response);
}

CodecError EncodeRequestFrame(const RequestMetadata& header,
                              const BrokerRegistrationRequest& request, int16_t version,
                              std::string* output) {
  return WriteRequest(header, request, version, BrokerRegistrationRequest::HeaderVersion(version),
                      output);
}
FrameResult DecodeResponseFrame(std::string_view input, size_t max_payload_size, int16_t version,
                                int32_t correlation_id, BrokerRegistrationResponse* response) {
  return ReadResponse(input, max_payload_size, version, correlation_id, response);
}

CodecError EncodeRequestFrame(const RequestMetadata& header, const BrokerHeartbeatRequest& request,
                              int16_t version, std::string* output) {
  return WriteRequest(header, request, version, BrokerHeartbeatRequest::HeaderVersion(version),
                      output);
}
FrameResult DecodeResponseFrame(std::string_view input, size_t max_payload_size, int16_t version,
                                int32_t correlation_id, BrokerHeartbeatResponse* response) {
  return ReadResponse(input, max_payload_size, version, correlation_id, response);
}

}  // namespace dfly::cluster::raft::protocol
