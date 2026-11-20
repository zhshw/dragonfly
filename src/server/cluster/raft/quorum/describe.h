// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <system_error>

#include "server/cluster/raft/protocol/describe_quorum.h"

namespace dfly::cluster::raft {

class ElectionCampaign;
class FetchReplication;

// Describe the current quorum on its serialized worker. Fetch and Describe use
// Unix milliseconds for replica timestamps. Local storage errors fence replies.
[[nodiscard]] std::error_code DescribeQuorum(const ElectionCampaign& campaign,
                                             FetchReplication& replication, int16_t version,
                                             const protocol::DescribeQuorumRequest& request,
                                             int64_t wall_ms,
                                             protocol::DescribeQuorumResponse* out);

}  // namespace dfly::cluster::raft
