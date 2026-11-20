// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/cluster/raft/metadata/metadata_image.h"
#include "server/cluster/raft/protocol/broker_heartbeat.h"
#include "server/cluster/raft/protocol/broker_registration.h"

namespace dfly::cluster::raft {

// Active-controller state. Reset on election with the stable metadata image after the
// new term's leader change commits. Serialize proposals against that image and hold
// successful mutating RPC responses until their full record list is committed.
class BrokerControl {
 public:
  explicit BrokerControl(int64_t session_timeout_ms) : timeout_ms_(session_timeout_ms) {
  }

  void Activate(const MetadataImage& image, int64_t monotonic_ms);

  std::vector<MetadataRecord> Register(const MetadataImage& image, std::string_view cluster_id,
                                       int64_t next_write_offset, int64_t monotonic_ms,
                                       const protocol::BrokerRegistrationRequest& request,
                                       protocol::BrokerRegistrationResponse* response) const;
  std::vector<MetadataRecord> Heartbeat(const MetadataImage& image, int64_t monotonic_ms,
                                        const protocol::BrokerHeartbeatRequest& request,
                                        protocol::BrokerHeartbeatResponse* response);

  // Propose fencing at most one expired unfenced broker. Its data group's voter
  // configuration is independent. Re-evaluate against the committed image on retry.
  std::optional<MetadataRecord> Expire(const MetadataImage& image, int64_t monotonic_ms) const;

 private:
  struct Session {
    int64_t epoch;
    int64_t last_contact;
    int64_t metadata_offset;
  };
  std::map<int32_t, Session> sessions_;
  int64_t timeout_ms_;
};

}  // namespace dfly::cluster::raft
