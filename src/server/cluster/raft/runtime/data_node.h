// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <filesystem>

#include "server/cluster/raft/data/command_handler.h"

namespace util {
class AcceptServer;
class ProactorPool;
}  // namespace util

namespace dfly {
class Service;
namespace cluster::raft {

// One RESP identity and one data partition per process. Metadata is followed as an observer.
class DataNode : public DataCommandHandler {
 public:
  static absl::StatusOr<std::unique_ptr<DataNode>> Open(
      const std::filesystem::path& dir, const std::string& cluster_id, int32_t node_id,
      const std::string& controllers, const std::string& listen, uint16_t resp_port,
      Service* service, util::ProactorPool* pool, util::AcceptServer* acceptor);

  // Configure before Start; each connection receives its own serial-owner capture.
  using TraceFactory = std::function<std::shared_ptr<trace::Capture>(std::string)>;
  virtual void SetTraceFactory(TraceFactory factory) = 0;
  virtual void Start() = 0;
  virtual void Stop() = 0;
};

}  // namespace cluster::raft
}  // namespace dfly
