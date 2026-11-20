// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/quorum/describe.h"

#include "server/cluster/raft/quorum/fetch_replication.h"

namespace dfly::cluster::raft {

std::error_code DescribeQuorum(const ElectionCampaign& campaign, FetchReplication& replication,
                               int16_t version, const protocol::DescribeQuorumRequest& request,
                               int64_t wall_ms, protocol::DescribeQuorumResponse* out) {
  *out = {};
  const auto& state = campaign.state();
  if (state.storage_error)
    return state.storage_error;
  if (version < request.kMinVersion || version > request.kMaxVersion) {
    out->error_code = 35;
    out->error_message = "The version of API is not supported.";
    return {};
  }

  const auto& target = campaign.target();
  if (request.topics.size() != 1 || request.topics.front().topic_name != target.topic ||
      request.topics.front().partitions.size() != 1 ||
      request.topics.front().partitions.front().partition_index != target.partition) {
    for (const auto& topic : request.topics) {
      auto& result = out->topics.emplace_back();
      result.topic_name = topic.topic_name;
      for (const auto& partition : topic.partitions) {
        auto& part = result.partitions.emplace_back();
        part.partition_index = partition.partition_index;
        part.error_code = 3;
        part.error_message = "This server does not host this topic-partition.";
      }
    }
    return {};
  }

  auto& topic = out->topics.emplace_back();
  topic.topic_name = target.topic;
  auto& part = topic.partitions.emplace_back();
  part.partition_index = target.partition;
  if (state.role != QuorumRole::kLeader) {
    part.error_code = 6;
    part.error_message =
        "For requests intended only for the leader, this error indicates that the broker is not "
        "the current leader. For requests intended for any replica, this error indicates that the "
        "broker is not a replica of the topic partition.";
    return {};
  }

  part.leader_id = target.local_replica.id;
  part.leader_epoch = state.election.epoch;
  part.high_watermark = replication.HighWatermark().value_or(-1);
  replication.replica_progress().Describe(version, replication.LogEndOffset(), wall_ms, &part);
  if (version >= 2) {
    for (const auto& voter : campaign.voters()) {
      auto& node = out->nodes.emplace_back();
      node.node_id = voter.key.id;
      auto& listener = node.listeners.emplace_back();
      listener.name = replication.ListenerName();
      listener.host = voter.endpoint.host;
      listener.port = voter.endpoint.port;
    }
  }
  return {};
}

}  // namespace dfly::cluster::raft
