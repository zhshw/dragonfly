// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/bootstrap/quorum_bootstrap.h"

#include <absl/strings/ascii.h>
#include <absl/strings/escaping.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>

#include <algorithm>
#include <chrono>
#include <set>

#include "server/cluster/raft/bootstrap/raft_directory.h"
#include "server/cluster/raft/config/quorum_config.h"
#include "server/cluster/raft/log/record_batch.h"
#include "server/cluster/raft/quorum/membership_history.h"
#include "server/cluster/raft/runtime/group.h"

namespace dfly::cluster::raft {
using namespace std;
using namespace protocol;

absl::StatusOr<QuorumBootstrap> ParseQuorumBootstrap(optional<string_view> initial,
                                                     optional<int32_t> data_group) {
  if (data_group && *data_group < 0)
    return absl::InvalidArgumentError("Negative data group ID");
  QuorumBootstrap result{data_group, {}};
  if (!initial)
    return result;
  set<int32_t> ids;
  for (string_view entry : absl::StrSplit(*initial, ',', absl::SkipEmpty())) {
    entry = absl::StripAsciiWhitespace(entry);
    size_t colon = entry.rfind(':');
    if (colon == entry.npos)
      return absl::InvalidArgumentError("Expected id@host:port:directoryId");
    auto location = entry.substr(0, colon);
    auto at = location.find('@');
    auto port = location.rfind(':');
    if (at == location.npos || port == location.npos || port <= at)
      return absl::InvalidArgumentError("Expected an initial voter ID, host and port");
    auto host = location.substr(at + 1, port - at - 1);
    if (host.starts_with('[') != host.ends_with(']') ||
        (host.find(':') != host.npos && !host.starts_with('[')))
      return absl::InvalidArgumentError("Initial voter IPv6 addresses need square brackets");
    auto endpoints = ParseVoterConnections(location);
    auto dir_id = ParseDirectoryId(entry.substr(colon + 1));
    if (!endpoints.ok() || endpoints->size() != 1 || !dir_id.ok())
      return absl::InvalidArgumentError("Invalid initial voter endpoint or directory ID");
    const auto& [id, ep] = *endpoints->begin();
    if (id < 0 || ep.host.empty() || ep.host == "::" || ep.port == 0 || !ids.insert(id).second ||
        IsReservedDirectoryId(*dir_id))
      return absl::InvalidArgumentError("Invalid or duplicate initial voter");
    auto& voter = result.voters.voters.emplace_back();
    voter.voter_id = id;
    voter.voter_directory_id = *dir_id;
    voter.endpoints.push_back({data_group ? "RAFT" : "CONTROLLER", ep.host, ep.port, {}});
    voter.k_raft_version_feature = {0, 1, {}};
  }
  if (result.voters.voters.empty())
    return absl::InvalidArgumentError("No initial voters supplied");
  sort(result.voters.voters.begin(), result.voters.voters.end(),
       [](const auto& lhs, const auto& rhs) { return lhs.voter_id < rhs.voter_id; });
  return result;
}

string QuorumBootstrap::Partition() const {
  return data_group ? absl::StrCat(kDataTopic, "-", *data_group)
                    : absl::StrCat(kMetadataTopic, "-0");
}

absl::StatusOr<string> QuorumBootstrap::Identity() const {
  string bytes;
  if (Encode(voters, 0, &bytes) != CodecError::kOk)
    return absl::InvalidArgumentError("Cannot encode initial voters");
  return absl::StrCat(Partition(), "/", absl::WebSafeBase64Escape(bytes));
}

absl::Status QuorumBootstrap::Install(const filesystem::path& dir) const {
  auto opened = PartitionLog::Open(dir / Partition());
  if (!opened.ok())
    return opened.status();
  auto& log = **opened;
  LogPosition zero{0, 0};
  if (log.End() != zero)
    return absl::FailedPreconditionError("Cannot initialize a populated quorum");
  if (log.LatestSnapshot()) {
    MembershipHistory restored;
    if (auto s = restored.Update(log); !s.ok())
      return s;
    const auto* existing = restored.LatestVoters();
    if (*log.LatestSnapshot() != zero || restored.KraftVersion() != 1 ||
        (existing ? *existing != voters : !voters.voters.empty()))
      return absl::FailedPreconditionError(
          "Existing bootstrap snapshot differs from initial voters");
    return absl::OkStatus();
  }
  string level, membership;
  if (Encode(KRaftVersionRecord{0, 1, {}}, 0, &level) != CodecError::kOk ||
      Encode(voters, 0, &membership) != CodecError::kOk)
    return absl::InvalidArgumentError("Cannot encode initial quorum records");
  vector<ControlRecordValue> controls{{5, level}};
  if (!voters.voters.empty())
    controls.push_back({6, membership});
  const auto now =
      chrono::duration_cast<chrono::milliseconds>(chrono::system_clock::now().time_since_epoch())
          .count();
  auto header = MakeSnapshotHeaderBatch(0, now, now, controls);
  if (!header.ok())
    return header.status();
  auto footer = MakeSnapshotFooterBatch(1 + controls.size(), 0, now);
  if (!footer.ok())
    return footer.status();
  auto writer = log.BeginSnapshot(zero, header->size() + footer->size());
  if (!writer.ok())
    return writer.status();
  if (auto s = (*writer)->Append(0, *header); !s.ok())
    return s;
  if (auto s = (*writer)->Append(header->size(), *footer); !s.ok())
    return s;
  return log.InstallSnapshot(writer->get(), 0);
}

}  // namespace dfly::cluster::raft
