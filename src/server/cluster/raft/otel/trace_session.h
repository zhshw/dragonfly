// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/status/statusor.h>

#include "server/cluster/raft/otel/trace_exporter.h"

namespace util::fb2 {
class ProactorBase;
}

namespace dfly::cluster::raft::otel {

// Owns a bounded diagnostic session. Each capture has one serial recording owner. Registration
// is marshalled to the session owner; export starts only after connections and Raft have stopped.
class TraceSession {
 public:
  static absl::StatusOr<std::unique_ptr<TraceSession>> FromEnvironment(
      int32_t node, util::fb2::ProactorBase* owner);
  std::shared_ptr<trace::Capture> Create(std::string group);
  void SetExpectedNodes(const std::vector<int32_t>& nodes);
  absl::Status Export();

 private:
  TraceSession(int32_t node, util::fb2::ProactorBase* owner);
  util::fb2::ProactorBase* owner_;
  int32_t node_;
  std::string capture_id_, process_, endpoint_, authorization_;
  TraceIdentity identity_;
  std::vector<int32_t> selected_nodes_;
  std::vector<std::shared_ptr<trace::Capture>> captures_;
};

}  // namespace dfly::cluster::raft::otel
