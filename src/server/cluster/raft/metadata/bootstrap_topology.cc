// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/metadata/bootstrap_topology.h"

#include <jsoncons/json.hpp>
#include <set>

namespace dfly::cluster::raft {
namespace {
using namespace std;
using namespace protocol;
using Json = jsoncons::ojson;

struct BootstrapState {
  RegisteredBrokers brokers;
  SlotDirectory directory;
  set<pair<string, uint16_t>> endpoints;
  vector<MetadataRecord> records;
};

template <typename T> optional<T> Number(const Json& object, const char* field) {
  if (!object.is_object() || !object.contains(field) || !object.at(field).template is<T>())
    return nullopt;
  return object.at(field).template as<T>();
}

optional<string> Text(const Json& object, const char* field) {
  if (!object.is_object() || !object.contains(field) || !object.at(field).is_string())
    return nullopt;
  return object.at(field).as<string>();
}

absl::Status AddNode(const Json& node, BootstrapState* state, DataGroupRecord* group) {
  auto id = Number<int32_t>(node, "broker_id");
  auto redis_id = Text(node, "id");
  auto host = Text(node, "ip");
  auto port = Number<uint16_t>(node, "port");
  auto raft_port = Number<uint16_t>(node, "raft_port");
  auto raft_host = node.contains("raft_host") ? Text(node, "raft_host") : host;
  if (!id || *id < 0 || !redis_id || redis_id->empty() || !host || host->empty() || !port ||
      *port == 0 || !raft_host || raft_host->empty() || !raft_port || *raft_port == 0)
    return absl::InvalidArgumentError("Invalid bootstrap node id, Redis or Raft endpoint");
  if (state->brokers.contains(*id) || !state->endpoints.emplace(*host, *port).second ||
      !state->endpoints.emplace(*raft_host, *raft_port).second)
    return absl::InvalidArgumentError("Repeated bootstrap broker ID or endpoint");

  RegisterBrokerRecord broker;
  broker.broker_id = *id;
  broker.end_points.push_back({"RESP", *host, *port, 0, {}});
  broker.end_points.push_back({"RAFT", *raft_host, *raft_port, 0, {}});
  broker.features.push_back({"dragonfly.cluster", 1, 1, {}});
  state->brokers.emplace(*id, broker);
  RedisNodeRecord identity;
  identity.broker_id = *id;
  identity.redis_node_id = *std::move(redis_id);
  if (auto s = state->directory.Replay(identity, state->brokers); !s.ok())
    return s;
  state->records.push_back({std::move(broker), 4});
  state->records.push_back({std::move(identity)});
  group->members.push_back(*id);
  return absl::OkStatus();
}

absl::Status AddGroup(const Json& shard, BootstrapState* state) {
  auto id = Number<int32_t>(shard, "group_id");
  if (!id || state->directory.groups().contains(*id) || !shard.contains("master") ||
      !shard.contains("replicas") || !shard.at("replicas").is_array() ||
      !shard.contains("slot_ranges") || !shard.at("slot_ranges").is_array())
    return absl::InvalidArgumentError("Invalid bootstrap shard or repeated group ID");
  if (shard.contains("migrations") &&
      (!shard.at("migrations").is_array() || !shard.at("migrations").empty()))
    return absl::FailedPreconditionError("Complete existing slot migrations before bootstrap");
  DataGroupRecord group;
  group.group_id = *id;
  if (auto s = AddNode(shard.at("master"), state, &group); !s.ok())
    return s;
  for (const auto& replica : shard.at("replicas").array_range()) {
    if (auto s = AddNode(replica, state, &group); !s.ok())
      return s;
  }
  if (auto s = state->directory.Replay(group); !s.ok())
    return s;
  state->records.push_back({std::move(group)});
  for (const auto& range : shard.at("slot_ranges").array_range()) {
    auto first = Number<SlotId>(range, "start"), last = Number<SlotId>(range, "end");
    if (!first || !last)
      return absl::InvalidArgumentError("Invalid slot bounds in bootstrap group");
    SlotAssignmentRecord assignment;
    assignment.first_slot = *first;
    assignment.last_slot = *last;
    assignment.group_id = *id;
    assignment.generation = 1;
    if (auto s = state->directory.Replay(assignment); !s.ok())
      return s;
    state->records.push_back({assignment});
  }
  return absl::OkStatus();
}
}  // namespace

absl::StatusOr<vector<MetadataRecord>> ParseBootstrapTopology(string_view input) {
  BootstrapState state;
  BeginTransactionRecord begin;
  begin.name = "Bootstrap Dragonfly cluster";
  state.records.push_back({std::move(begin)});
  state.records.push_back({FeatureLevelRecord{"metadata.version", 33, {}}});
  state.records.push_back({FeatureLevelRecord{"dragonfly.cluster", 1, {}}});
  try {
    auto shards = Json::parse(input);
    if (!shards.is_array())
      return absl::InvalidArgumentError("Expected a bootstrap shard array");
    for (const auto& shard : shards.array_range()) {
      if (auto s = AddGroup(shard, &state); !s.ok())
        return s;
    }
  } catch (const jsoncons::json_exception& e) {
    return absl::InvalidArgumentError(e.what());
  }
  if (!state.directory.HasCompleteCoverage())
    return absl::InvalidArgumentError("Bootstrap topology does not cover all 16384 slots");
  state.records.push_back({EndTransactionRecord{}});
  return std::move(state.records);
}

absl::StatusOr<optional<MetadataRecord>> PrepareLeaderReport(const MetadataImage& image,
                                                             int32_t source_broker_id,
                                                             const GroupLeaderRecord& report) {
  if (report.reporter_id != source_broker_id)
    return absl::InvalidArgumentError("Data group report does not match its source broker");
  auto change = image.directory.CheckLeaderReport(report, image.brokers);
  if (!change.ok())
    return change.status();
  if (!*change)
    return nullopt;
  return MetadataRecord{report};
}

}  // namespace dfly::cluster::raft
