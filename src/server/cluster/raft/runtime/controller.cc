// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/runtime/controller.h"

#include <absl/flags/flag.h>

#include <atomic>
#include <chrono>
#include <fstream>

#include "base/logging.h"
#include "server/cluster/raft/log/record_batch.h"
#include "server/cluster/raft/metadata/bootstrap_topology.h"
#include "server/cluster/raft/metadata/broker_control.h"
#include "server/cluster/raft/metadata/metadata_snapshot.h"
#include "server/cluster/raft/quorum/fetch_replication.h"
#include "server/cluster/raft/runtime/group.h"
#include "server/cluster/raft/transport/peer_connection.h"
#include "server/cluster/raft/transport/raft_listener.h"
#include "util/accept_server.h"
#include "util/fibers/pool.h"

ABSL_FLAG(int64_t, raft_metadata_snapshot_bytes, 20 << 20,
          "Committed metadata log bytes between snapshots.");
ABSL_FLAG(int64_t, raft_metadata_snapshot_interval_ms, 3600000,
          "Maximum time between snapshots of new committed metadata. Zero disables the timer.");

namespace dfly::cluster::raft {
namespace {
using namespace std;
using namespace chrono;
using namespace protocol;
namespace fb2 = util::fb2;

int64_t NowMs() {
  return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
}

absl::StatusOr<vector<MetadataRecord>> ReadBootstrap(const string& path) {
  if (path.empty())
    return vector<MetadataRecord>{};
  ifstream file(path, ios::binary);
  if (!file)
    return absl::NotFoundError("Cannot open raft_bootstrap_topology " + path);
  string contents{istreambuf_iterator<char>{file}, istreambuf_iterator<char>{}};
  if (file.bad())
    return absl::DataLossError("Cannot read raft_bootstrap_topology " + path);
  return ParseBootstrapTopology(contents);
}

absl::Status Append(RaftGroup::State state, span<const MetadataRecord> records) {
  if (records.empty())
    return absl::OkStatus();
  const auto epoch = state.election.state().election.epoch;
  auto offset = state.log.End().end_offset;
  const auto wall = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
  string output;
  vector<string> values;
  size_t bytes = 0;
  auto flush = [&]() -> absl::Status {
    auto batch = MakeDataBatch(offset, epoch, wall, values);
    if (!batch.ok())
      return batch.status();
    output += *batch;
    offset += values.size();
    values.clear();
    bytes = 0;
    return absl::OkStatus();
  };
  for (const auto& record : records) {
    auto value = EncodeMetadataRecord(record);
    if (!value.ok())
      return value.status();
    if (!values.empty() && bytes + value->size() > (1U << 20)) {
      if (auto s = flush(); !s.ok())
        return s;
    }
    bytes += value->size() + 32;
    values.push_back(*std::move(value));
  }
  if (auto s = flush(); !s.ok())
    return s;
  return state.replication.Append(output, epoch);
}

struct ProbeTarget {
  QuorumEndpoint endpoint;
  int32_t group_id;
  int32_t controller_epoch;
  int64_t broker_epoch;
};

// Metadata and broker sessions are touched only by the RaftGroup owner queue.
// Socket calls and waits for committed offsets run on distinct proactor fibers.
class ActiveController {
 public:
  ActiveController(RaftGroup& group, MetadataLoader& loader, string cluster_id,
                   absl::StatusOr<vector<MetadataRecord>> bootstrap)
      : group_(group),
        loader_(loader),
        cluster_id_(std::move(cluster_id)),
        bootstrap_(std::move(bootstrap)) {
  }

  RpcHandlers Handlers() {
    auto handlers = group_.Handlers();
    handlers.broker_registration = [this](int16_t, const auto& req, auto* res) {
      return ApplyRequest(res, [&](const MetadataImage& image, int64_t offset) {
        return brokers_.Register(image, cluster_id_, offset, NowMs(), req, res);
      });
    };
    handlers.broker_heartbeat = [this](int16_t, const auto& req, auto* res) {
      return ApplyRequest(res, [&](const MetadataImage& image, int64_t) {
        return brokers_.Heartbeat(image, NowMs(), req, res);
      });
    };
    handlers.slot_migration = [this](int16_t, const auto& req, auto* res) {
      return ApplyRequest(res, [&](const MetadataImage& image, int64_t) {
        return ProposeSlotMigration(image, cluster_id_, req, res);
      });
    };
    return handlers;
  }

  absl::Status Run(fb2::ProactorBase* owner) {
    map<int32_t, fb2::Fiber> probes;
    absl::Status bootstrap_error;
    while (!stopping_) {
      vector<pair<int32_t, int32_t>> needed;
      auto status = group_.Visit([&](RaftGroup::State state) {
        if (auto s = MaybeSnapshot(state); !s.ok())
          LOG(ERROR) << "Writing metadata snapshot: " << s;
        if (!Ready(state))
          return absl::OkStatus();
        if (loader_.transaction_open()) {
          // The new term's HW has committed its entire inherited prefix. The old
          // controller can no longer finish a transaction left open at its end.
          const MetadataRecord abort{AbortTransactionRecord{}};
          return Append(state, span(&abort, 1));
        }
        auto image = loader_.image();
        if (image->directory.groups().empty()) {
          if (!bootstrap_.ok()) {
            bootstrap_error = bootstrap_.status();
            return bootstrap_error;
          }
          if (!bootstrap_->empty())
            return Append(state, *bootstrap_);
        }
        if (auto expired = brokers_.Expire(*image, NowMs()); expired)
          return Append(state, span(&*expired, 1));
        for (const auto& [gid, group] : image->directory.groups()) {
          for (auto member : group.members) {
            if (!probes.contains(member))
              needed.emplace_back(member, gid);
          }
        }
        return absl::OkStatus();
      });
      if (!bootstrap_error.ok()) {
        Stop();
        break;
      }
      if (!status.ok() && !stopping_)
        VLOG(1) << "Metadata controller: " << status;
      for (auto [id, gid] : needed)
        probes.emplace(id, owner->LaunchFiber([this, id, gid] { Probe(id, gid); }));
      util::ThisFiber::SleepFor(milliseconds(50));
    }
    for (auto& [id, fiber] : probes)
      fiber.Join();
    return bootstrap_error;
  }

  void Stop() {
    stopping_ = true;
  }

 private:
  absl::Status MaybeSnapshot(RaftGroup::State state) {
    const auto hw = state.replication.HighWatermark();
    auto image = loader_.image();
    auto latest = state.log.LatestSnapshot();
    if (!hw || !image->stable_end_offset || loader_.transaction_open() ||
        (latest && latest->end_offset >= image->stable_end_offset))
      return absl::OkStatus();
    auto now = steady_clock::now();
    if (now < next_snapshot_retry_)
      return absl::OkStatus();
    auto interval = absl::GetFlag(FLAGS_raft_metadata_snapshot_interval_ms);
    if (loader_.bytes_since_snapshot() <
            static_cast<uint64_t>(absl::GetFlag(FLAGS_raft_metadata_snapshot_bytes)) &&
        (!interval || now - last_snapshot_time_ < milliseconds(interval)))
      return absl::OkStatus();
    const auto wall = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    auto s = WriteMetadataSnapshot(*image, state.replication.membership(), *hw, wall, &state.log);
    if (s.ok()) {
      last_snapshot_time_ = now;
      loader_.SnapshotWritten();
    } else {
      next_snapshot_retry_ = now + seconds(5);
    }
    return s;
  }

  bool Ready(RaftGroup::State state) {
    const auto& election = state.election.state();
    if (election.role != QuorumRole::kLeader || !state.replication.HighWatermark() ||
        loader_.consumed_end_offset() != state.log.End().end_offset)
      return false;
    if (epoch_ != election.election.epoch) {
      brokers_.Activate(*loader_.image(), NowMs());
      epoch_ = election.election.epoch;
    }
    return true;
  }

  template <typename Response, typename Plan>
  error_code ApplyRequest(Response* response, Plan plan) {
    *response = {};
    optional<LogPosition> pending;
    auto deadline = steady_clock::now() + seconds(5);
    while (!stopping_ && steady_clock::now() < deadline) {
      bool done = false;
      auto status = group_.Visit([&](RaftGroup::State state) {
        const auto& election = state.election.state();
        if (election.role != QuorumRole::kLeader ||
            (pending && pending->epoch != election.election.epoch)) {
          *response = {};
          response->error_code = 41;
          done = true;
          return absl::OkStatus();
        }
        if (pending) {
          done = loader_.image()->stable_end_offset >= pending->end_offset;
          return absl::OkStatus();
        }
        if (!Ready(state) || loader_.transaction_open())
          return absl::OkStatus();
        auto records = plan(*loader_.image(), state.log.End().end_offset);
        if (records.empty()) {
          done = true;
          return absl::OkStatus();
        }
        auto s = Append(state, records);
        if (s.ok())
          pending = state.log.End();
        return s;
      });
      if (!status.ok())
        return make_error_code(errc::io_error);
      if (done)
        return {};
      util::ThisFiber::SleepFor(milliseconds(20));
    }
    *response = {};
    response->error_code = 7;
    return {};
  }

  void Probe(int32_t id, int32_t gid) {
    unique_ptr<PeerConnection> peer;
    optional<QuorumEndpoint> address;
    DescribeQuorumRequest req;
    auto& topic = req.topics.emplace_back();
    topic.topic_name = kDataTopic;
    topic.partitions.emplace_back().partition_index = gid;
    while (!stopping_) {
      optional<ProbeTarget> target;
      auto status = group_.Visit([&](RaftGroup::State state) {
        if (!Ready(state) || loader_.transaction_open())
          return absl::OkStatus();
        auto image = loader_.image();
        auto broker = image->brokers.find(id);
        if (broker == image->brokers.end() || broker->second.fenced)
          return absl::OkStatus();
        for (const auto& ep : broker->second.end_points) {
          if (ep.name == "RAFT")
            target = ProbeTarget{{ep.host, ep.port}, gid, epoch_, broker->second.broker_epoch};
        }
        return absl::OkStatus();
      });
      if (status.ok() && target) {
        if (address != target->endpoint) {
          if (peer)
            peer->Close();
          address = target->endpoint;
          peer = make_unique<PeerConnection>(*address, 750, kMaxRaftFrame);
        }
        DescribeQuorumResponse res;
        if (!peer->Call(req, 0, &res) && !res.error_code && res.topics.size() == 1 &&
            res.topics[0].topic_name == kDataTopic && res.topics[0].partitions.size() == 1) {
          const auto& p = res.topics[0].partitions[0];
          if (!p.error_code && p.partition_index == gid && p.leader_id == id &&
              p.leader_epoch >= 0 && p.high_watermark >= 0 &&
              any_of(p.current_voters.begin(), p.current_voters.end(),
                     [id](const auto& v) { return v.replica_id == id; })) {
            status = group_.Visit([&](RaftGroup::State state) {
              if (!Ready(state) || epoch_ != target->controller_epoch || loader_.transaction_open())
                return absl::OkStatus();
              GroupLeaderRecord report;
              report.group_id = gid;
              report.reporter_id = report.leader_id = id;
              report.reporter_epoch = target->broker_epoch;
              report.leader_epoch = p.leader_epoch;
              auto proposal = PrepareLeaderReport(*loader_.image(), id, report);
              if (!proposal.ok())
                return proposal.status();
              return *proposal ? Append(state, span(&proposal->value(), 1)) : absl::OkStatus();
            });
          }
        }
      } else if (peer) {
        peer->Close();
        peer.reset();
        address.reset();
      }
      util::ThisFiber::SleepFor(milliseconds(200));
    }
    if (peer)
      peer->Close();
  }

  RaftGroup& group_;
  MetadataLoader& loader_;
  string cluster_id_;
  absl::StatusOr<vector<MetadataRecord>> bootstrap_;
  BrokerControl brokers_{9000};
  int32_t epoch_ = -1;
  steady_clock::time_point last_snapshot_time_ = steady_clock::now();
  steady_clock::time_point next_snapshot_retry_{};
  atomic_bool stopping_{false};
};
}  // namespace

absl::Status RunController(const filesystem::path& dir, const string& cluster_id, int32_t node_id,
                           const string& voters, util::ProactorPool* pool,
                           util::AcceptServer* acceptor, const string& bootstrap_file) {
  auto identity = ReadRaftDirectory(dir);
  if (!identity.ok())
    return identity.status();
  if (identity->node_id != node_id || identity->cluster_id != cluster_id)
    return absl::FailedPreconditionError(
        "Configured cluster ID or node ID differs from meta.properties");
  if (absl::GetFlag(FLAGS_raft_metadata_snapshot_bytes) < 1 ||
      absl::GetFlag(FLAGS_raft_metadata_snapshot_interval_ms) < 0)
    return absl::InvalidArgumentError("Invalid metadata snapshot size or interval");
  auto endpoints = ParseVoterConnections(voters);
  if (!endpoints.ok())
    return endpoints.status();
  if (!endpoints->contains(node_id))
    return absl::InvalidArgumentError("Local controller must be in raft_quorum_voters");
  MetadataLoader loader;
  auto opened =
      RaftGroup::Open(dir, *identity, RaftGroup::Kind::kMetadata, 0, *endpoints, false,
                      [&](const PartitionLog& log, int64_t hw) { return loader.Advance(log, hw); });
  if (!opened.ok())
    return opened.status();
  RaftGroup& group = **opened;
  auto bootstrap = ReadBootstrap(bootstrap_file);
  if (!bootstrap.ok()) {
    // An empty log cannot supply a topology. Report a bad bootstrap file on a fresh controller
    // before it starts listening, even if the other voters are offline. On a restart, committed
    // metadata decides whether the original bootstrap file is still needed.
    auto status = group.Visit([&](RaftGroup::State state) {
      return state.log.End().end_offset == 0 ? bootstrap.status() : absl::OkStatus();
    });
    if (!status.ok())
      return status;
  }
  ActiveController controller(group, loader, cluster_id, std::move(bootstrap));
  auto listener = make_unique<RaftListener>(kMaxRaftFrame, controller.Handlers());
  const auto& local = endpoints->at(node_id);
  if (auto ec = acceptor->AddListener(local.host.c_str(), local.port, listener.get()); ec)
    return absl::ErrnoToStatus(ec.value(), "Binding controller listener");
  listener.release();
  acceptor->TriggerOnBreakSignal([&] {
    controller.Stop();
    group.RequestStop();
  });
  acceptor->Run();
  auto owner = pool->GetNextProactor();
  group.Start(owner);
  absl::Status controller_status;
  auto controller_fiber = owner->LaunchFiber([&] {
    controller_status = controller.Run(owner);
    if (!controller_status.ok())
      acceptor->Stop();
  });
  acceptor->Wait();
  controller.Stop();
  controller_fiber.Join();
  group.Join();
  acceptor->TriggerOnBreakSignal({});
  return controller_status;
}

}  // namespace dfly::cluster::raft
