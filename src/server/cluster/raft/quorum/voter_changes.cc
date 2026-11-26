// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/quorum/voter_changes.h"

#include <absl/strings/ascii.h>
#include <absl/strings/match.h>

#include <algorithm>

#include "server/cluster/raft/log/record_batch.h"

namespace dfly::cluster::raft {
namespace {
using namespace std;
using namespace protocol;
constexpr int16_t kTimeout = 7;
constexpr int16_t kNotLeader = 6;
constexpr int16_t kInvalid = 42;
constexpr int16_t kDuplicate = 126;
constexpr int16_t kNotFound = 127;

VoterChanges::Result Rejected(int16_t code, string message) {
  return make_shared<VoterChangeResult>(VoterChangeResult{code, std::move(message)});
}

}  // namespace

void VoterChanges::Finish(int16_t error, optional<string> message) {
  if (!pending_->result->error) {
    pending_->result->error = error;
    pending_->result->message = std::move(message);
  }
  pending_.reset();
  if (completed_)
    completed_();
}

void VoterChanges::Poll(int64_t now_ms) {
  const auto& state = campaign_.state();
  if (state.role != QuorumRole::kLeader || state.election.epoch != volatile_epoch_) {
    volatile_voters_.voters.clear();
    volatile_epoch_ = -1;
  }
  if (!pending_)
    return;
  bool own_removal = pending_->op == Operation::kRemove &&
                     pending_->voter.voter_id == campaign_.target().local_replica.id &&
                     state.role == QuorumRole::kResigned;
  if (state.storage_error) {
    Finish(-1, "Quorum storage is unavailable");
  } else if (pending_->epoch != state.election.epoch ||
             (state.role != QuorumRole::kLeader && !own_removal)) {
    Finish(kNotLeader, "Quorum leadership has changed");
  } else if (pending_->offset && fetch_.HighWatermark() > pending_->offset) {
    Finish(0);
  } else if (now_ms >= pending_->deadline) {
    Finish(kTimeout, "Voter change timed out");
  }
}

template <typename Request>
VoterChanges::Result VoterChanges::Start(const Request& req, Operation op, int64_t now_ms,
                                         int64_t wall_ms) {
  Poll(now_ms);
  const auto& state = campaign_.state();
  const auto& target = campaign_.target();
  if (req.cluster_id && *req.cluster_id != target.cluster_id)
    return Rejected(104, "Inconsistent cluster ID");
  if constexpr (is_same_v<Request, UpdateRaftVoterRequest>) {
    if (req.current_leader_epoch < state.election.epoch)
      return Rejected(74, "Fenced leader epoch");
    if (req.current_leader_epoch > state.election.epoch)
      return Rejected(75, "Unknown leader epoch");
  }
  if (state.role != QuorumRole::kLeader || state.storage_error)
    return Rejected(kNotLeader, "The request requires the current leader");
  if (req.voter_id < 0 || req.voter_directory_id == DirectoryId{})
    return Rejected(kInvalid, "Invalid voter ID or directory ID");

  VotersRecord::Voter voter;
  voter.voter_id = req.voter_id;
  voter.voter_directory_id = req.voter_directory_id;
  QuorumEndpoint endpoint;
  if constexpr (!is_same_v<Request, RemoveRaftVoterRequest>) {
    set<string> names;
    for (const auto& ep : req.listeners) {
      string name = absl::AsciiStrToUpper(ep.name);
      if (name.empty() || ep.host.empty() || !ep.port || !names.insert(name).second)
        return Rejected(kInvalid, "Invalid voter listeners");
      if (absl::EqualsIgnoreCase(name, fetch_.ListenerName()))
        endpoint = {ep.host, ep.port};
      voter.endpoints.push_back({std::move(name), ep.host, ep.port, {}});
    }
    if (endpoint.host.empty())
      return Rejected(kInvalid, "Endpoint for the quorum listener is missing");
  }
  if constexpr (is_same_v<Request, UpdateRaftVoterRequest>) {
    const auto& feature = req.k_raft_version_feature;
    if (feature.min_supported_version < 0 ||
        feature.max_supported_version < feature.min_supported_version)
      return Rejected(kInvalid, "Invalid supported kraft.version range");
    voter.k_raft_version_feature = {
        feature.min_supported_version, feature.max_supported_version, {}};
  }
  const auto& history = fetch_.membership();
  if (pending_ || !fetch_.HighWatermark())
    return Rejected(kTimeout, "Awaiting a committed leader epoch or a previous voter change");
  if (op != Operation::kUpdate) {
    if (history.KraftVersion() < 1)
      return Rejected(35, "Changing the voter set requires kraft.version 1");
    if (!history.LastVoterOffset() || history.LastVoterOffset() >= fetch_.HighWatermark())
      return Rejected(kTimeout, "The latest voter set is not committed");
    if (op == Operation::kAdd &&
        ranges::any_of(history.LatestVoters()->voters,
                       [&](const auto& node) { return node.voter_id == voter.voter_id; }))
      return Rejected(kDuplicate, "Voter ID is already present");
  }
  bool wait = op != Operation::kUpdate;
  if constexpr (is_same_v<Request, AddRaftVoterRequest>)
    wait = req.ack_when_committed;
  auto result = make_shared<VoterChangeResult>();
  pending_.emplace(Pending{op, std::move(voter), wait, false, nullopt, state.election.epoch,
                           now_ms + timeout_ms_, ++sequence_, std::move(endpoint), result});
  if (op == Operation::kRemove) {
    if (auto status = Apply(now_ms, wall_ms); !status.ok())
      Finish(-1, string(status.message()));
  }
  return result;
}

VoterChanges::Result VoterChanges::Begin(const AddRaftVoterRequest& req, int64_t now,
                                         int64_t wall) {
  return Start(req, Operation::kAdd, now, wall);
}

VoterChanges::Result VoterChanges::Begin(const RemoveRaftVoterRequest& req, int64_t now,
                                         int64_t wall) {
  return Start(req, Operation::kRemove, now, wall);
}

VoterChanges::Result VoterChanges::Begin(const UpdateRaftVoterRequest& req, int64_t now,
                                         int64_t wall) {
  return Start(req, Operation::kUpdate, now, wall);
}

optional<VoterChanges::Probe> VoterChanges::TakeProbe() {
  if (!pending_ || pending_->op == Operation::kRemove || pending_->probe_sent)
    return nullopt;
  pending_->probe_sent = true;
  return Probe{pending_->sequence,
               {pending_->voter.voter_id, pending_->voter.voter_directory_id},
               pending_->endpoint};
}

absl::Status VoterChanges::OnApiVersions(const Probe& probe, const ApiVersionsResponse& response,
                                         int64_t now_ms, int64_t wall_ms) {
  Poll(now_ms);
  if (!pending_ || !pending_->probe_sent || pending_->sequence != probe.sequence ||
      pending_->offset ||
      probe.key != ReplicaKey{pending_->voter.voter_id, pending_->voter.voter_directory_id})
    return absl::OkStatus();
  if (response.error_code) {
    Finish(kTimeout, "ApiVersions probe failed");
    return absl::OkStatus();
  }
  const int16_t level = fetch_.membership().KraftVersion();
  auto feature = ranges::find_if(response.supported_features,
                                 [](const auto& f) { return f.name == "kraft.version"; });
  auto& range = pending_->voter.k_raft_version_feature;
  if (feature == response.supported_features.end() || feature->min_version < 0 ||
      feature->min_version > level || feature->max_version < level ||
      (pending_->op == Operation::kUpdate &&
       (range.min_supported_version != feature->min_version ||
        range.max_supported_version != feature->max_version))) {
    Finish(kInvalid, "ApiVersions kraft.version range is missing or incompatible");
    return absl::OkStatus();
  }
  if (pending_->op == Operation::kAdd &&
      !fetch_.replica_progress().IsObserverCaughtUp(probe.key, wall_ms)) {
    Finish(kTimeout, "New voter has not caught up as an observer");
    return absl::OkStatus();
  }
  range.min_supported_version = feature->min_version;
  range.max_supported_version = feature->max_version;
  auto status = Apply(now_ms, wall_ms);
  if (!status.ok())
    Finish(-1, string(status.message()));
  return status;
}

absl::Status VoterChanges::Apply(int64_t now_ms, int64_t wall_ms) {
  const auto& history = fetch_.membership();
  const bool dynamic = history.KraftVersion() >= 1;
  VotersRecord members;
  if (dynamic) {
    auto hw = fetch_.HighWatermark();
    if (!history.LastVoterOffset() || !hw || history.LastVoterOffset() >= hw) {
      Finish(kTimeout, "Previous voter change has not committed");
      return absl::OkStatus();
    }
    members = *history.LatestVoters();
  } else {
    // A static leader collects UpdateRaftVoter announcements for a future version upgrade.
    if (volatile_epoch_ != pending_->epoch) {
      for (const auto& voter : campaign_.voters()) {
        auto& node = volatile_voters_.voters.emplace_back();
        node.voter_id = voter.key.id;
        node.voter_directory_id = voter.key.directory_id;
        node.endpoints.push_back(
            {string(fetch_.ListenerName()), voter.endpoint.host, voter.endpoint.port, {}});
      }
      volatile_epoch_ = pending_->epoch;
    }
    members = volatile_voters_;
  }
  const auto& node = pending_->voter;
  const auto key = ReplicaKey{node.voter_id, node.voter_directory_id};
  auto it =
      ranges::find_if(members.voters, [&](const auto& v) { return v.voter_id == node.voter_id; });
  if (pending_->op == Operation::kAdd) {
    if (it != members.voters.end()) {
      Finish(kDuplicate, "Voter ID is already present");
      return absl::OkStatus();
    }
    members.voters.push_back(node);
  } else if (it == members.voters.end() ||
             (pending_->op == Operation::kRemove &&
              (members.voters.size() <= 1 || it->voter_directory_id != node.voter_directory_id)) ||
             (dynamic && !ReplicaKey{it->voter_id, it->voter_directory_id}.Matches(key))) {
    Finish(kNotFound, "Voter cannot be removed or updated in the current configuration");
    return absl::OkStatus();
  } else if (pending_->op == Operation::kRemove) {
    members.voters.erase(it);
  } else {
    *it = node;
  }
  if (!dynamic) {
    volatile_voters_ = std::move(members);
    epochs_.RecordFetch(key, pending_->epoch, now_ms, nullopt);
    Finish(0);
    return absl::OkStatus();
  }

  string value;
  if (Encode(members, 0, &value) != CodecError::kOk)
    return absl::ResourceExhaustedError("Cannot encode VotersRecord");
  const int64_t offset = fetch_.LogEndOffset();
  const array<ControlRecordValue, 1> controls{{{6, value}}};
  auto batch = MakeControlBatch(offset, pending_->epoch, wall_ms, controls);
  if (!batch.ok())
    return batch.status();
  if (auto s = fetch_.Append(*std::move(batch), pending_->epoch); !s.ok())
    return s;
  pending_->offset = offset;
  if (pending_->op == Operation::kUpdate)
    epochs_.RecordFetch(key, pending_->epoch, now_ms, nullopt);
  if (!pending_->wait_for_commit)
    pending_->result->error = 0;
  Poll(now_ms);
  return absl::OkStatus();
}

}  // namespace dfly::cluster::raft
