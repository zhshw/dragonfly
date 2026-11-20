// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <functional>
#include <memory>

#include "server/cluster/raft/metadata/metadata_image.h"
#include "server/cluster/raft/protocol/slot_transfer_record.h"
#include "server/cluster/raft/runtime/group.h"

namespace dfly {
class Service;
}

namespace dfly::cluster::raft {

class CommittedEffects;
class PeerConnection;

// Tick is owned by one proactor fiber. Fetch may run on any internal listener fiber.
// Only the group's serialized worker reads applied transfers or proposes data log entries.
class SlotMigrationDriver {
 public:
  using ReadImage = std::function<std::shared_ptr<const MetadataImage>()>;

  SlotMigrationDriver(int32_t group_id, int32_t node_id, std::string cluster_id,
                      protocol::Uuid incarnation, RaftGroup& group, const CommittedEffects& effects,
                      ReadImage read_image, Service& service);

  void Tick(PeerConnection& controller);
  void Fetch(const protocol::FetchSlotImageRequest& request,
             protocol::FetchSlotImageResponse* response);

 private:
  struct Capture {
    protocol::Uuid id;
    int32_t epoch;
    bool restart;
    std::string bytes;
    std::vector<int8_t> digest;
  };

  bool Ready(RaftGroup::State state, const MetadataImage& image) const;
  absl::Status Append(RaftGroup::State state, const protocol::SlotTransferRecord& record);
  void Advance(const protocol::SlotMigrationRecord& migration, PeerConnection& controller);
  void Pull(const protocol::SlotMigrationRecord& migration, QuorumEndpoint source, int32_t epoch,
            int64_t position);

  int32_t group_id_;
  int32_t node_id_;
  std::string cluster_id_;
  protocol::Uuid incarnation_;
  RaftGroup& group_;
  const CommittedEffects& effects_;
  ReadImage read_image_;
  Service& service_;

  // Bound private snapshot memory to one slot. A partial, unsealed image can be recaptured.
  std::optional<Capture> capture_;
  LogPosition pending_{};
};

}  // namespace dfly::cluster::raft
