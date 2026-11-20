// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/runtime/controller.h"

#include <absl/random/random.h>

#include <atomic>
#include <chrono>

#include "base/logging.h"
#include "server/cluster/raft/bootstrap/raft_directory.h"
#include "server/cluster/raft/quorum/describe.h"
#include "server/cluster/raft/quorum/fetch_replication.h"
#include "server/cluster/raft/transport/peer_connection.h"
#include "server/cluster/raft/transport/raft_listener.h"
#include "util/accept_server.h"
#include "util/fibers/fiberqueue_threadpool.h"
#include "util/fibers/pool.h"

namespace dfly::cluster::raft {
namespace {

using namespace std;
using namespace protocol;
using namespace std::chrono;
namespace fb2 = util::fb2;

constexpr int32_t kElectionTimeoutMs = 1000;
constexpr int32_t kFetchTimeoutMs = 2000;
constexpr uint32_t kRequestTimeoutMs = 1000;
constexpr size_t kMaxFrameBytes = 16 << 20;
constexpr int32_t kMaxFetchBytes = 1 << 20;

int64_t NowMs() {
  return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
}

int64_t WallMs() {
  return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

absl::Status Status(error_code ec, string_view operation) {
  return ec ? absl::ErrnoToStatus(ec.value(), operation) : absl::OkStatus();
}

protocol::Uuid MetadataTopicId() {
  protocol::Uuid id{};
  id.back() = 1;
  return id;
}

using PeerTask = variant<monostate, OutboundVote, EpochNotification, int32_t>;

// The disk queue owns every election, log and timer mutation. Each peer's RPC runs
// on its own fiber, so a disconnected voter cannot stall the majority or disk queue.
class MetadataQuorum {
 public:
  MetadataQuorum(VoteTarget target, vector<CampaignVoter> voters, const filesystem::path& dir,
                 unique_ptr<PartitionLog> log)
      : log_(std::move(log)),
        store_(dir),
        campaign_(
            std::move(target), std::move(voters),
            [this](const ElectionState& state) { return store_.WriteElectionState(state, 0); }),
        epochs_(campaign_, "CONTROLLER", kFetchTimeoutMs, 1000),
        replication_(campaign_, epochs_, *log_, MetadataTopicId()) {
  }

  absl::Status Recover() {
    optional<ElectionState> state;
    if (auto ec = store_.ReadElectionState(&state); ec)
      return Status(ec, "Reading quorum-state");
    set<int32_t> configured;
    for (const auto& voter : campaign_.voters())
      configured.insert(voter.key.id);
    if (state && state->voters != configured)
      return absl::FailedPreconditionError("Static voter IDs differ from durable quorum-state");
    if (!state && log_->End().end_offset != 0)
      return absl::DataLossError("Missing quorum-state for a populated static quorum");
    if (auto ec = campaign_.Recover(state.value_or(ElectionState{}), log_->End()); ec)
      return Status(ec, "Recovering metadata quorum");
    return Synchronize();
  }

  void Start(fb2::ProactorBase* owner) {
    fibers_.push_back(owner->LaunchFiber([this] {
      while (!stopping_) {
        worker_.Await([this] { Tick(); });
        ThisFiberSleep();
      }
    }));
    for (const auto& voter : campaign_.voters()) {
      if (voter.key.id != campaign_.target().local_replica.id)
        fibers_.push_back(owner->LaunchFiber([this, voter] { RunPeer(voter); }));
    }
  }

  void RequestStop() {
    stopping_ = true;
  }

  void Join() {
    RequestStop();
    for (auto& fiber : fibers_)
      fiber.Join();
  }

  RpcHandlers Handlers();

 private:
  static void ThisFiberSleep() {
    util::ThisFiber::SleepFor(chrono::milliseconds(25));
  }

  void ResetDeadline(int64_t now) {
    deadline_ =
        now + (campaign_.state().role == QuorumRole::kFollower
                   ? kFetchTimeoutMs
                   : kElectionTimeoutMs + absl::Uniform<int>(random_, 0, kElectionTimeoutMs));
  }

  absl::Status Synchronize() {
    auto status = replication_.SyncRole(WallMs());
    if (!status.ok())
      return status;
    auto role = campaign_.state().role;
    auto epoch = campaign_.state().election.epoch;
    if (role != role_ || epoch != epoch_) {
      notifications_.clear();
      role_ = role;
      epoch_ = epoch;
      ResetDeadline(NowMs());
      LOG(INFO) << "Metadata quorum node " << campaign_.target().local_replica.id << " epoch "
                << epoch << " leader " << campaign_.state().election.leader_id.value_or(-1)
                << " log end " << log_->End().end_offset;
    }
    return absl::OkStatus();
  }

  void Tick() {
    if (failed_)
      return;
    auto status = PollTimers();
    if (!status.ok()) {
      failed_ = true;
      LOG(ERROR) << "Stopped metadata quorum: " << status;
    }
  }

  absl::Status PollTimers() {
    if (auto s = Synchronize(); !s.ok())
      return s;
    auto now = NowMs();
    if (role_ != QuorumRole::kLeader && now >= deadline_) {
      if (auto ec = campaign_.OnElectionTimeout(); ec)
        return Status(ec, "Advancing election");
      ResetDeadline(now);
    }
    vector<EpochNotification> outbound;
    if (auto ec = epochs_.Poll(now, &outbound); ec)
      return Status(ec, "Polling epoch notifications");
    if (auto s = Synchronize(); !s.ok())
      return s;
    for (auto& message : outbound)
      notifications_.insert_or_assign(message.recipient.id, std::move(message));
    return absl::OkStatus();
  }

  PeerTask Next(ReplicaKey key) {
    Tick();
    if (failed_)
      return {};
    auto notification = notifications_.extract(key.id);
    if (notification)
      return std::move(notification.mapped());
    if (auto vote = campaign_.PrepareVote(key, log_->End()))
      return std::move(*vote);
    if (role_ == QuorumRole::kFollower && campaign_.state().election.leader_id == key.id)
      return key.id;
    return {};
  }

  template <typename F> error_code OnWorker(F&& f) {
    return worker_.Await([&] {
      Tick();
      if (failed_ || stopping_)
        return make_error_code(errc::state_not_recoverable);
      return f();
    });
  }

  void RunPeer(const CampaignVoter& voter) {
    PeerConnection peer(voter.endpoint, kRequestTimeoutMs, kMaxFrameBytes);
    while (!stopping_) {
      auto task = worker_.Await([&] { return Next(voter.key); });
      if (auto* vote = get_if<OutboundVote>(&task)) {
        VoteResponse response;
        bool pre_vote = vote->request.topics[0].partitions[0].pre_vote;
        auto ec = peer.Call(vote->request, pre_vote ? 2 : 0, &response);
        if (pre_vote && ec == errc::protocol_not_supported) {
          response.error_code = 35;
          ec.clear();
        }
        if (!ec)
          ec = OnWorker(
              [&] { return campaign_.ReceiveVoteResponse(voter.key, vote->sequence, response); });
        if (ec)
          VLOG(1) << "Vote to node " << voter.key.id << ": " << ec.message();
      } else if (auto* notification = get_if<EpochNotification>(&task)) {
        auto ec = SendEpoch(peer, *notification);
        if (ec)
          VLOG(1) << "Epoch notification for node " << voter.key.id << ": " << ec.message();
      } else if (auto* leader = get_if<int32_t>(&task)) {
        FetchFrom(peer, *leader);
      }
      ThisFiberSleep();
    }
    peer.Close();
  }

  error_code SendEpoch(PeerConnection& peer, const EpochNotification& n) {
    if (auto* begin = get_if<BeginQuorumEpochRequest>(&n.request)) {
      BeginQuorumEpochResponse response;
      if (auto ec = peer.Call(*begin, 0, &response); ec)
        return ec;
      return OnWorker([&] { return epochs_.ReceiveBeginResponse(n.recipient, n.epoch, response); });
    }
    EndQuorumEpochResponse response;
    if (auto ec = peer.Call(get<EndQuorumEpochRequest>(n.request), 0, &response); ec)
      return ec;
    return OnWorker([&] { return epochs_.ReceiveEndResponse(n.recipient, n.epoch, response); });
  }

  void FetchFrom(PeerConnection& peer, int32_t leader) {
    int16_t version;
    if (peer.Negotiate<FetchRequest>(kMinRaftFetchVersion, &version))
      return;
    auto fetch = worker_.Await([&]() -> optional<OutboundFetch> {
      if (failed_ || campaign_.state().role != QuorumRole::kFollower ||
          campaign_.state().election.leader_id != leader)
        return nullopt;
      return replication_.PrepareFetch(leader, version, 100, kMaxFetchBytes);
    });
    if (!fetch)
      return;
    FetchResponse response;
    if (peer.Call(fetch->request, version, &response))
      return;
    worker_.Await([&] {
      bool reset = false;
      auto s = replication_.ReceiveFetchResponse(leader, fetch->sequence, response, &reset);
      if (!s.ok())
        VLOG(1) << "Fetch from node " << leader << ": " << s;
      if (reset)
        ResetDeadline(NowMs());
    });
  }

  error_code Fetch(int16_t version, const FetchRequest& request, FetchResponse* response);

  unique_ptr<PartitionLog> log_;
  FileQuorumStateStore store_;
  ElectionCampaign campaign_;
  EpochControl epochs_;
  FetchReplication replication_;
  fb2::FiberQueueThreadPool worker_{1};
  absl::BitGen random_;
  int64_t deadline_ = 0;
  QuorumRole role_ = QuorumRole::kUnattached;
  int32_t epoch_ = -1;
  bool failed_ = false;
  map<int32_t, EpochNotification> notifications_;
  atomic_bool stopping_{false};
  vector<fb2::Fiber> fibers_;
};

RpcHandlers MetadataQuorum::Handlers() {
  RpcHandlers handlers;
  handlers.vote = [this](int16_t v, const auto& req, auto* res) {
    return OnWorker([&] { return campaign_.ReceiveVote(req, v, log_->End(), res); });
  };
  handlers.begin_epoch = [this](int16_t v, const auto& req, auto* res) {
    return OnWorker([&] { return epochs_.ReceiveBegin(req, v, res); });
  };
  handlers.end_epoch = [this](int16_t v, const auto& req, auto* res) {
    return OnWorker([&] {
      optional<int32_t> timeout;
      auto ec = epochs_.ReceiveEnd(req, v, res, &timeout);
      if (!ec && timeout) {
        if (!Synchronize().ok())
          return make_error_code(errc::io_error);
        deadline_ = NowMs() + *timeout;
      }
      return ec;
    });
  };
  handlers.fetch = [this](int16_t v, const auto& req, auto* res) { return Fetch(v, req, res); };
  handlers.describe_quorum = [this](int16_t v, const auto& req, auto* res) {
    return OnWorker([&] { return DescribeQuorum(campaign_, replication_, v, req, WallMs(), res); });
  };
  return handlers;
}

error_code MetadataQuorum::Fetch(int16_t version, const FetchRequest& request,
                                 FetchResponse* response) {
  // Delay an empty Fetch until data or the commit boundary changes, or until maxWaitMs.
  // Checking the watched values must not count a parked request as repeated voter contact.
  using Watch = tuple<QuorumRole, int32_t, LogPosition, optional<int64_t>>;
  optional<Watch> previous;
  bool can_wait = false;
  const auto deadline = steady_clock::now() + milliseconds(request.max_wait_ms);
  do {
    if (auto ec = OnWorker([&] {
          Watch current(role_, epoch_, log_->End(), replication_.HighWatermark());
          if (previous == current)
            return error_code{};
          const bool completing = previous.has_value();
          previous = current;
          auto s =
              replication_.HandleFetch(request, version, NowMs(), WallMs(), response, &can_wait);
          if (completing)
            can_wait = false;
          return s.ok() ? error_code{} : make_error_code(errc::io_error);
        });
        ec)
      return ec;
    if (!can_wait || steady_clock::now() >= deadline)
      return {};
    util::ThisFiber::SleepUntil(min(deadline, steady_clock::now() + milliseconds(25)));
  } while (!stopping_);
  return make_error_code(errc::operation_canceled);
}

}  // namespace

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
  vector<CampaignVoter> members;
  for (const auto& [id, endpoint] : *endpoints) {
    if (id < 0 || endpoint.host.empty() || endpoint.port == 0 || endpoint.host == "::")
      return absl::InvalidArgumentError(
          "Static voter requires a non-negative ID and a routable endpoint");
    members.push_back({ReplicaKey{id}, endpoint});
  }
  auto group_dir = dir / "__cluster_metadata-0";
  auto log = PartitionLog::Open(group_dir);
  if (!log.ok())
    return log.status();
  MetadataQuorum group({cluster_id, "__cluster_metadata", 0, {node_id, identity->directory_id}},
                       std::move(members), group_dir, *std::move(log));
  if (auto s = group.Recover(); !s.ok())
    return s;
  auto listener = make_unique<RaftListener>(kMaxFrameBytes, group.Handlers());
  const auto& local = endpoints->at(node_id);
  if (auto ec = acceptor->AddListener(local.host.c_str(), local.port, listener.get()); ec)
    return Status(ec, "Binding controller listener");
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
