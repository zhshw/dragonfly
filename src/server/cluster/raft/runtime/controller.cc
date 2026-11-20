// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/runtime/controller.h"

#include "server/cluster/raft/runtime/group.h"
#include "server/cluster/raft/transport/raft_listener.h"
#include "util/accept_server.h"
#include "util/fibers/pool.h"

namespace dfly::cluster::raft {
using namespace std;

absl::Status RunController(const filesystem::path& dir, const string& cluster_id, int32_t node_id,
                           const string& voters, util::ProactorPool* pool,
                           util::AcceptServer* acceptor) {
  auto identity = ReadRaftDirectory(dir);
  if (!identity.ok())
    return identity.status();
  if (identity->node_id != node_id || identity->cluster_id != cluster_id)
    return absl::FailedPreconditionError(
        "Configured cluster ID or node ID differs from meta.properties");

  auto endpoints = ParseVoterConnections(voters);
  if (!endpoints.ok())
    return endpoints.status();
  if (!endpoints->contains(node_id))
    return absl::InvalidArgumentError("Local controller must be in raft_quorum_voters");
  auto opened = RaftGroup::Open(dir, *identity, RaftGroup::Kind::kMetadata, 0, *endpoints);
  if (!opened.ok())
    return opened.status();
  RaftGroup& group = **opened;
  auto listener = make_unique<RaftListener>(kMaxRaftFrame, group.Handlers());
  const auto& local = endpoints->at(node_id);
  if (auto ec = acceptor->AddListener(local.host.c_str(), local.port, listener.get()); ec)
    return absl::ErrnoToStatus(ec.value(), "Binding controller listener");
  listener.release();
  acceptor->TriggerOnBreakSignal([&] { group.RequestStop(); });
  acceptor->Run();
  group.Start(pool->GetNextProactor());
  acceptor->Wait();
  group.Join();
  acceptor->TriggerOnBreakSignal({});
  return absl::OkStatus();
}

}  // namespace dfly::cluster::raft
