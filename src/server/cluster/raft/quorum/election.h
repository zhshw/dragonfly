// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <array>
#include <compare>
#include <cstdint>
#include <map>
#include <optional>
#include <span>
#include <vector>

namespace dfly::cluster::raft {

using DirectoryId = std::array<uint8_t, 16>;

struct ReplicaKey {
  int32_t id;

  // Kafka encodes an unspecified directory as the all-zero UUID.
  DirectoryId directory_id{};

  bool operator==(const ReplicaKey&) const = default;

  // An unspecified directory in a persisted vote or voter configuration matches any
  // directory on that node. The converse does not hold.
  bool Matches(const ReplicaKey& replica) const;
};

struct LogPosition {
  int32_t epoch;
  int64_t end_offset;

  auto operator<=>(const LogPosition&) const = default;
};

enum class QuorumRole { kUnattached, kProspective, kCandidate, kFollower, kLeader, kResigned };
enum class VoteType { kPreVote, kVote };

// Facts about the local replica in the epoch of a validated Vote request.
struct VoteContext {
  QuorumRole role;
  std::optional<int32_t> leader_id;
  std::optional<ReplicaKey> voted_key;
  bool fetched_from_leader = false;

  // Checks an incoming request after cluster, epoch and recipient validation. Before
  // answering a new standard vote affirmatively the caller must persist it.
  bool CanGrantVote(const ReplicaKey& candidate, LogPosition candidate_log, LogPosition local_log,
                    VoteType type) const;
};

// Responses to a single election round in a fixed voter configuration.
class EpochElection {
 public:
  enum class Outcome { kPending, kGranted, kRejected };
  enum class RecordResult { kFirst, kRepeat, kInvalid };

  // Starts with one vote from the local nominee. Returns nullopt for an invalid voter
  // configuration or a local replica which is not eligible to start an election.
  static std::optional<EpochElection> Start(std::span<const ReplicaKey> voters,
                                            const ReplicaKey& local, VoteType type);

  // Only accepts members of this round. A repeated pre-vote may change, in which case
  // the new decision replaces the old and the result is kRepeat.
  RecordResult RecordVote(int32_t voter_id, bool granted);
  Outcome GetOutcome() const;
  std::vector<ReplicaKey> UnrecordedVoters() const;
  std::vector<int32_t> GrantingVoters() const;

 private:
  struct Voter {
    DirectoryId directory_id;
    std::optional<bool> granted;
  };

  EpochElection(int32_t local_id, VoteType type) : local_id_(local_id), type_(type) {
  }

  int32_t local_id_;
  VoteType type_;
  std::map<int32_t, Voter> voters_;
};

}  // namespace dfly::cluster::raft
