// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/status/status.h>

#include <functional>
#include <string>

#include "server/cluster/raft/trace.h"

namespace dfly::cluster::raft::otel {

struct TraceIdentity {
  std::string release;
  std::string binary_sha;
  std::string workload;
  size_t expected_streams = 1;
  size_t dropped_streams = 0;
  std::string experiment;
  std::string expected_nodes;
};

// Uses official SDK recordables and OTEL protobuf serialization. The capture must be quiescent.
// begin == events().size() produces the stream manifest; all other batches contain source events.
std::string EncodeTraceBatch(const trace::Capture& capture, const TraceIdentity& identity,
                             size_t begin, size_t count);

// Sends bounded batches and the manifest last. A sender must reject partial or failed responses.
// This runs after recording/draining, never in Capture::Emit or a Raft I/O completion callback.
absl::Status ExportTrace(const trace::Capture& capture, const TraceIdentity& identity,
                         const std::function<absl::Status(std::string_view)>& send);

// Fiber-aware OTLP/HTTP transport. Endpoint and authorization are supplied separately so secrets
// never appear in URLs, process flags, trace attributes or error messages.
absl::Status SendTrace(std::string_view endpoint, std::string_view authorization,
                       std::string_view protobuf);

}  // namespace dfly::cluster::raft::otel
