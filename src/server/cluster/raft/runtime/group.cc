// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/runtime/group.h"

#include <absl/cleanup/cleanup.h>
#include <absl/random/random.h>

#include <atomic>
#include <chrono>
#include <tuple>

#include "base/flags.h"
#include "base/logging.h"
#include "raft_io_config.h"
#include "server/cluster/raft/bootstrap/raft_directory.h"
#include "server/cluster/raft/quorum/describe.h"
#include "server/cluster/raft/quorum/fetch_replication.h"
#include "server/cluster/raft/quorum/snapshot_transfer.h"
#include "server/cluster/raft/quorum/voter_changes.h"
#include "server/cluster/raft/transport/peer_connection.h"
#include "util/fibers/event_count.h"
#include "util/fibers/fiberqueue_threadpool.h"
#include "util/fibers/pool.h"
#include "util/fibers/proactor_base.h"
#if DFLY_RAFT_USE_FOLLY
#include "server/cluster/raft/runtime/group_loop.h"
#include "server/cluster/raft/storage/folly_io.h"
#elif defined(__linux__)
#include "util/fibers/uring_proactor.h"
#endif

ABSL_FLAG(bool, raft_async_io, true, "Use asynchronous io_uring for data quorum appends");
ABSL_FLAG(uint32_t, raft_data_segment_bytes, 1U << 30, "Data quorum log segment size in bytes");
ABSL_FLAG(std::string, raft_data_ack_mode, "flush",
          "Data quorum acknowledgement: flush (durable) or memory (page cache)");

namespace dfly::cluster::raft {
namespace {

using namespace std;
using namespace protocol;
using namespace std::chrono;
namespace fb2 = util::fb2;

constexpr int32_t kElectionTimeoutMs = 1000;
constexpr int32_t kFetchTimeoutMs = 2000;
constexpr uint32_t kRequestTimeoutMs = 1000;
constexpr int32_t kMaxFetchBytes = 1 << 20;
constexpr int32_t kMaxDataFetchBytes = 4 << 20;

int64_t NowMs() {
  return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
}

int64_t WallMs() {
  return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

absl::Status Status(error_code ec, string_view operation) {
  return ec ? absl::ErrnoToStatus(ec.value(), operation) : absl::OkStatus();
}

protocol::Uuid TopicId(RaftGroup::Kind kind) {
  protocol::Uuid id{};
  id.back() = kind == RaftGroup::Kind::kMetadata ? 1 : 2;
  return id;
}

enum class Pull { kFetch, kSnapshot, kDiscover };
using PeerTask = variant<monostate, OutboundVote, EpochNotification, Pull>;

}  // namespace

// The disk queue owns every election, log and timer mutation. Each peer's RPC runs
// on its own fiber, so a disconnected voter cannot stall the majority or disk queue.
class RaftGroup::Impl {
  enum class IoAccess { kIdle, kData, kFetch };

 public:
  Impl(VoteTarget target, vector<CampaignVoter> voters, const filesystem::path& dir,
       unique_ptr<PartitionLog> log, Kind kind, bool observer, ApplyCommitted apply,
       SnapshotValidator validate)
      : log_(std::move(log)),
        store_(dir),
        bootstrap_(voters),
        campaign_(std::move(target), std::move(voters),
                  [this](const ElectionState& state) {
                    if (auto s = log_->FlushBuffered(); !s.ok())
                      return make_error_code(errc::io_error);
                    return store_.WriteElectionState(state,
                                                     replication_.membership().KraftVersion());
                  }),
        epochs_(campaign_, kind == Kind::kMetadata ? "CONTROLLER" : "RAFT", kFetchTimeoutMs, 1000),
        replication_(campaign_, epochs_, *log_, TopicId(kind)),
        snapshots_(campaign_, epochs_, replication_, *log_, std::move(validate)),
        changes_(campaign_, epochs_, replication_, kRequestTimeoutMs),
        kind_(kind),
        observer_(observer),
        apply_(std::move(apply)) {
    if (kind == Kind::kData)
      replication_.SetFetchWakeup([this] { fetch_changed_.notifyAll(); });
  }

  absl::Status Recover() {
    if (auto s = replication_.RefreshMembership(); !s.ok())
      return s;
    optional<ElectionState> state;
    if (auto ec = store_.ReadElectionState(&state); ec)
      return Status(ec, "Reading quorum-state");
    set<int32_t> configured;
    for (const auto& voter : campaign_.voters())
      configured.insert(voter.key.id);
    const bool dynamic = replication_.membership().KraftVersion() >= 1;
    if (!dynamic && state && state->voters != configured)
      return absl::FailedPreconditionError("Static voter IDs differ from durable quorum-state");
    if (!dynamic && !observer_ && !configured.contains(campaign_.target().local_replica.id))
      return absl::InvalidArgumentError("Local node is not in its static voter set");
    if (!state && log_->End().end_offset != 0)
      return absl::DataLossError("Missing quorum-state for a populated static quorum");
    if (auto ec = campaign_.Recover(state.value_or(ElectionState{}), log_->End()); ec)
      return Status(ec, "Recovering metadata quorum");
    return Synchronize();
  }

  ~Impl() {
    Join();
    if (disk_pool_)
      disk_pool_->Stop();
  }

  void Start(fb2::ProactorBase* owner) {
    if (kind_ == Kind::kData)
      LOG(INFO) << "Raft data acknowledgement mode: " << absl::GetFlag(FLAGS_raft_data_ack_mode);
#ifdef __linux__
    if (kind_ == Kind::kData && absl::GetFlag(FLAGS_raft_async_io)) {
#if DFLY_RAFT_USE_FOLLY
      LOG(INFO) << "Raft group loop and file I/O: io_uring (Folly)";
#else
      disk_pool_.reset(fb2::Pool::IOUring(256, 1));
      disk_pool_->Run();
      LOG(INFO) << "Raft file I/O: io_uring (Helio)";
#endif
      RunOnWorker([&] {
        async_io_ = [this](int fd, int64_t offset, string_view bytes,
                           PartitionLog::IoCallback done) {
          SubmitIO(fd, offset, bytes, std::move(done));
        };
        replication_.SetAsyncIO(async_io_);
      });
    }
#endif
    supervisor_ = owner->LaunchFiber([this, owner] { ManagePeers(owner); });
  }

  void RequestStop() {
    stopping_ = true;
    fetch_changed_.notifyAll();
    commit_changed_.notifyAll();
    WakeData();
  }

  void Join() {
    RequestStop();
    if (supervisor_.IsJoinable())
      supervisor_.Join();

    // Completion callbacks retain the log and its descriptors until both barriers drain.
    RunOnWorker([&] { CheckBufferedFlush(log_->FlushBuffered()); });
  }

  RpcHandlers Handlers();
  optional<int32_t> LeaderEpoch() const {
    const int32_t epoch = leader_epoch_.load(memory_order_acquire);
    return epoch < 0 || stopping_ ? nullopt : optional<int32_t>{epoch};
  }

  void SetDataDriver(DataDriver driver) {
    RunOnWorker([&] { data_driver_ = std::move(driver); });
  }

  void SetApplyLane(ApplyStep step, function<bool()> busy) {
    RunOnWorker([&] {
      apply_step_ = std::move(step);
      apply_busy_ = std::move(busy);
    });
  }

  void CompleteApply(function<void()> completion) {
    worker_.Await([&] {
      const auto before = CommitState();
      const auto end = log_->End();
      completion();
      if (!log_->AppendPending())
        Tick();
      io_changed_.notifyAll();
      commit_changed_.notifyAll();
      NotifyProgress(before, end);
    });
  }

  void Encode(function<void()> work, function<void()> completion) {
    CHECK(!encoding_);
    encoding_ = true;
    auto done = [this, completion = std::move(completion)] {
      const auto before = CommitState();
      const auto end = log_->End();
      completion();
      encoding_ = false;
      io_changed_.notifyAll();
      commit_changed_.notifyAll();
      NotifyProgress(before, end);
    };
#if DFLY_RAFT_USE_FOLLY
    worker_.io().Encode(std::move(work), std::move(done));
#else
    auto job =
        make_shared<pair<function<void()>, function<void()>>>(std::move(work), std::move(done));
    encoder_.Add([this, job] {
      job->first();
      worker_.Add([job] { job->second(); });
    });
#endif
  }

  void WakeData() {
    if (data_wake_queued_.exchange(true, memory_order_acq_rel))
      return;
    if (!worker_.GetQueue(0)->TryAdd([this] {
          data_wake_queued_.store(false, memory_order_release);
          const auto before = CommitState();
          const auto end = log_->End();
          if (!log_->AppendPending())
            Tick();
          NotifyProgress(before, end);
        }))
      data_wake_queued_.store(false, memory_order_release);
  }

  absl::Status Visit(Operation operation, IoAccess access = IoAccess::kIdle) {
    return RunOnWorker(
        [&] {
          Tick();
          if (failed_ || stopping_)
            return absl::UnavailableError("Quorum is unavailable");
          return operation({campaign_, replication_, *log_});
        },
        access);
  }

  absl::Status VisitUntil(steady_clock::time_point deadline,
                          absl::FunctionRef<absl::Status(State, bool*)> operation,
                          bool data_only = false) {
    while (steady_clock::now() < deadline) {
      // Subscribe before checking the ticket so a concurrent commit cannot be missed.
      auto key = commit_changed_.prepareWait();
      bool done = false;
      auto status = Visit([&](State state) { return operation(state, &done); },
                          data_only ? IoAccess::kData : IoAccess::kIdle);
      if (!status.ok() || done)
        return status;
      commit_changed_.wait_until(key.epoch(), deadline);
    }
    return absl::DeadlineExceededError("Timed out waiting for quorum commit");
  }

 private:
  tuple<QuorumRole, int32_t, optional<int64_t>, bool> CommitState() const {
    const auto& state = campaign_.state();
    return tuple{state.role, state.election.epoch, replication_.HighWatermark(), failed_};
  }

  template <typename F>
  auto RunOnWorker(
      F&& operation, IoAccess access = IoAccess::kIdle,
      absl::FunctionRef<bool()> ready = [] { return true; }) -> decltype(operation()) {
    util::detail::ResultMover<decltype(operation())> result;
    while (true) {
      auto key = io_changed_.prepareWait();
      bool ran = worker_.Await([&] {
        if ((log_->AppendPending() && access != IoAccess::kFetch) ||
            ((log_->FlushPending() || encoding_ || (apply_busy_ && apply_busy_())) &&
             access == IoAccess::kIdle) ||
            !ready()) {
          NotifyProgress(CommitState(), log_->End());
          return false;
        }
        const auto commit = CommitState();
        const auto end = log_->End();
        result.Apply(operation);
        MaybeFlushBuffered();
        NotifyProgress(commit, end);
        return true;
      });
      if (ran)
        return std::move(result).get();
      io_changed_.wait(key.epoch());
    }
  }

  template <typename Commit> void NotifyProgress(const Commit& before, LogPosition end) {
    auto next_deadline = data_driver_
                             ? data_driver_({campaign_, replication_, *log_}, failed_ || stopping_)
                             : nullopt;
#if DFLY_RAFT_USE_FOLLY
    if (next_deadline != data_deadline_) {
      data_deadline_ = next_deadline;
      worker_.io().SetDeadline(next_deadline, [this] {
        data_deadline_.reset();
        NotifyProgress(CommitState(), log_->End());
      });
    }
#endif
    const auto& state = campaign_.state();
    const int32_t leader_epoch =
        !failed_ && state.role == QuorumRole::kLeader ? state.election.epoch : -1;
    if (leader_epoch_.load(memory_order_relaxed) != leader_epoch)
      leader_epoch_.store(leader_epoch, memory_order_release);
    if (CommitState() != before) {
      commit_changed_.notifyAll();
      fetch_changed_.notifyAll();
    } else if (log_->End() != end) {
      fetch_changed_.notifyAll();
    }
  }

  void CheckBufferedFlush(absl::Status status) {
    if (!status.ok()) {
      failed_ = true;
      campaign_.FenceStorage();
      LOG(ERROR) << "Stopped quorum " << campaign_.target().topic << ": " << status;
    }
  }

  void MaybeFlushBuffered() {
    if (failed_ || stopping_ || log_->IoPending() || !log_->BufferedFlushDue())
      return;
    auto started = log_->TryFlushBufferedAsync(
        async_io_, [this](absl::Status status) { CheckBufferedFlush(status); });
    if (!started.ok())
      CheckBufferedFlush(started.status());
    else if (!*started)
      CheckBufferedFlush(log_->FlushBuffered());
  }

  void CompleteIO(const PartitionLog::IoCallback& completion, int result) {
    const auto commit = CommitState();
    const auto end = log_->End();
    const bool flushing = log_->FlushPending();
    completion(result);
    const bool flushed = flushing && !log_->FlushPending();
    if (!log_->AppendPending()) {
      Tick();
      MaybeFlushBuffered();
    }
    if (flushed)
      commit_changed_.notifyAll();
    io_changed_.notifyAll();
    NotifyProgress(commit, end);
  }

#ifdef __linux__
  void SubmitIO(int fd, int64_t offset, string_view bytes, PartitionLog::IoCallback done) {
#if DFLY_RAFT_USE_FOLLY

    // The CQE is already on the serial group owner; no worker queue or epoll bridge is needed.
    worker_.io().Submit(fd, offset, bytes, [this, done = std::move(done)](int result) {
      CompleteIO(done, result);
      return true;
    });
#else
    auto* proactor = static_cast<fb2::UringProactor*>(disk_pool_->at(0));
    auto completion = make_shared<PartitionLog::IoCallback>(std::move(done));
    proactor->Dispatch([this, proactor, fd, offset, bytes, completion] {
      auto entry = proactor->GetSubmitEntry([this, completion](auto*, int result, auto, auto) {
        // CQE callbacks cannot block on the bounded worker queue.
        fb2::Fiber([this, completion, result] {
          worker_.Add([this, completion, result] { CompleteIO(*completion, result); });
        }).Detach();
      });
      if (offset < 0)
        entry.PrepFSync(fd, 0);
      else
        entry.PrepWrite(fd, bytes.data(), bytes.size(), offset, 0);
    });
#endif
  }
#endif

  bool LocalVoter() const {
    return !observer_ && ranges::any_of(campaign_.voters(), [&](const CampaignVoter& peer) {
      return peer.key.Matches(campaign_.target().local_replica);
    });
  }

  map<int32_t, CampaignVoter> DesiredPeers() const {
    map<int32_t, CampaignVoter> desired;
    for (const auto& peer : campaign_.voters())
      desired.emplace(peer.key.id, peer);
    if (const auto& leader = campaign_.LeaderEndpoint(); leader)
      desired.try_emplace(leader->node_id,
                          CampaignVoter{{leader->node_id}, {leader->host, leader->port}});
    if (!LocalVoter()) {
      for (const auto& seed : bootstrap_)
        desired.try_emplace(seed.key.id, seed);
    }
    desired.erase(campaign_.target().local_replica.id);
    return desired;
  }

  void ManagePeers(fb2::ProactorBase* owner) {
    struct PeerWorker {
      CampaignVoter voter;
      atomic_bool active{true};
      fb2::Fiber fiber;
    };
    map<int32_t, unique_ptr<PeerWorker>> peers;
    while (!stopping_) {
      auto desired = RunOnWorker([&] {
        Tick();
        return DesiredPeers();
      });
      vector<unique_ptr<PeerWorker>> retiring;
      for (auto it = peers.begin(); it != peers.end();) {
        auto target = desired.find(it->first);
        if (target != desired.end() && target->second == it->second->voter) {
          desired.erase(target);
          ++it;
        } else {
          it->second->active = false;
          retiring.push_back(std::move(it->second));
          it = peers.erase(it);
        }
      }
      for (auto& [id, voter] : desired) {
        auto next = make_unique<PeerWorker>();
        next->voter = std::move(voter);
        next->fiber =
            owner->LaunchFiber([this, slot = next.get()] { RunPeer(slot->voter, slot->active); });
        peers.emplace(id, std::move(next));
      }
      for (auto& expired : retiring)
        expired->fiber.Join();
      ThisFiberSleep();
    }
    for (auto& [id, peer] : peers)
      peer->fiber.Join();
  }

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
    snapshots_.SyncRole();
    changes_.Poll(NowMs());
    if (auto s = Apply(); !s.ok())
      return s;
    auto role = campaign_.state().role;
    auto epoch = campaign_.state().election.epoch;
    if (role != role_ || epoch != epoch_) {
      notifications_.clear();
      role_ = role;
      epoch_ = epoch;
      ResetDeadline(NowMs());
      LOG(INFO) << campaign_.target().topic << " partition " << campaign_.target().partition
                << " node " << campaign_.target().local_replica.id << " epoch " << epoch
                << " leader " << campaign_.state().election.leader_id.value_or(-1) << " log end "
                << log_->End().end_offset;
    }
    return absl::OkStatus();
  }

  absl::Status Apply() {
    if (auto hw = replication_.HighWatermark(); hw && apply_step_) {
      auto result = apply_step_(*log_, *hw);
      if (!result.ok())
        return result.status();
      applied_ = *result;
      return absl::OkStatus();
    }
    if (auto hw = replication_.HighWatermark(); hw && apply_ && *hw != applied_) {
      if (auto s = apply_(*log_, *hw); !s.ok())
        return s;
      applied_ = *hw;
    }
    return absl::OkStatus();
  }

  void Tick() {
    if (failed_)
      return;

    // Only ordinary data can advance during a flush; election and structural work must drain it.
    auto status = log_->FlushPending() ? Apply() : PollTimers();
    if (!status.ok()) {
      failed_ = true;
      LOG(ERROR) << "Stopped quorum " << campaign_.target().topic << ": " << status;
    }
  }

  absl::Status PollTimers() {
    if (auto s = Synchronize(); !s.ok())
      return s;
    auto now = NowMs();
    if ((LocalVoter() || role_ == QuorumRole::kResigned) && role_ != QuorumRole::kLeader &&
        now >= deadline_) {
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
    if (log_->FlushPending()) {
      if (role_ == QuorumRole::kFollower && campaign_.state().election.leader_id == key.id &&
          !replication_.RequestedSnapshot())
        return Pull::kFetch;
      return {};
    }
    auto notification = notifications_.extract(key.id);
    if (notification)
      return std::move(notification.mapped());
    if (auto vote = campaign_.PrepareVote(key, log_->End()))
      return std::move(*vote);
    if (!LocalVoter() && (role_ == QuorumRole::kUnattached || NowMs() >= deadline_) &&
        NowMs() >= discovery_after_[key.id]) {
      discovery_after_[key.id] = NowMs() + 250;
      return Pull::kDiscover;
    }
    if (role_ == QuorumRole::kFollower && campaign_.state().election.leader_id == key.id)
      return replication_.RequestedSnapshot() ? Pull::kSnapshot : Pull::kFetch;
    return {};
  }

  template <typename F> error_code OnWorker(F&& f) {
    return RunOnWorker([&] {
      Tick();
      if (failed_ || stopping_)
        return make_error_code(errc::state_not_recoverable);
      return f();
    });
  }

  void RunPeer(const CampaignVoter& voter, const atomic_bool& active) {
    PeerConnection peer(voter.endpoint, kRequestTimeoutMs, kMaxRaftFrame);
    while (!stopping_ && active) {
      auto task = RunOnWorker([&] { return Next(voter.key); }, IoAccess::kData);
      if (auto* vote = get_if<OutboundVote>(&task)) {
        VoteResponse response;
        bool pre_vote = vote->request.topics[0].partitions[0].pre_vote;
        auto ec = peer.Call(vote->request, pre_vote ? 2 : 0, &response);
        if (pre_vote && ec == errc::protocol_not_supported) {
          response.error_code = 35;
          ec.clear();
        }
        if (!ec)
          ec = OnWorker([&] {
            return active ? campaign_.ReceiveVoteResponse(voter.key, vote->sequence, response)
                          : error_code{};
          });
        if (ec)
          VLOG(1) << "Vote to node " << voter.key.id << ": " << ec.message();
      } else if (auto* notification = get_if<EpochNotification>(&task)) {
        auto ec = SendEpoch(peer, *notification, active);
        if (ec)
          VLOG(1) << "Epoch notification for node " << voter.key.id << ": " << ec.message();
      } else if (auto* pull = get_if<Pull>(&task)) {
        switch (*pull) {
          case Pull::kDiscover:
            Discover(peer, voter, active);
            break;
          case Pull::kFetch:
            if (FetchFrom(peer, voter.key.id, active))
              continue;
            break;
          case Pull::kSnapshot:
            if (FetchSnapshotFrom(peer, voter.key.id, active))
              continue;
            break;
        }
      }
      ThisFiberSleep();
    }
    peer.Close();
  }

  error_code SendEpoch(PeerConnection& peer, const EpochNotification& n,
                       const atomic_bool& active) {
    if (auto* begin = get_if<BeginQuorumEpochRequest>(&n.request)) {
      BeginQuorumEpochResponse response;
      if (auto ec = peer.Call(*begin, 0, &response); ec)
        return ec;
      return OnWorker([&] {
        return active ? epochs_.ReceiveBeginResponse(n.recipient, n.epoch, response) : error_code{};
      });
    }
    EndQuorumEpochResponse response;
    if (auto ec = peer.Call(get<EndQuorumEpochRequest>(n.request), 0, &response); ec)
      return ec;
    return OnWorker([&] {
      return active ? epochs_.ReceiveEndResponse(n.recipient, n.epoch, response) : error_code{};
    });
  }

  bool FetchFrom(PeerConnection& peer, int32_t leader, const atomic_bool& active) {
    int16_t version;
    if (peer.Negotiate<FetchRequest>(kMinRaftFetchVersion, &version))
      return false;
    auto fetch = RunOnWorker(
        [&]() -> optional<OutboundFetch> {
          if (failed_ || campaign_.state().role != QuorumRole::kFollower ||
              campaign_.state().election.leader_id != leader)
            return nullopt;
          const auto max_bytes = kind_ == Kind::kData ? kMaxDataFetchBytes : kMaxFetchBytes;

          // Advertise receipt while the lane works, but do not park an obsolete applied offset
          // in a long poll. An empty immediate response waits for the local batch event below.
          const auto max_wait = apply_busy_ && apply_busy_() ? 0 : 100;
          return replication_.PrepareFetch(leader, version, max_wait, max_bytes);
        },
        IoAccess::kData);
    if (!fetch)
      return false;
    FetchResponse response;
    const auto start = steady_clock::now();
    if (peer.Call(fetch->request, version, &response))
      return false;
    const bool waited = steady_clock::now() - start >= milliseconds(fetch->request.max_wait_ms);
    auto applied = commit_changed_.prepareWait();
    bool await_apply = false;
    const bool progressed = RunOnWorker(
        [&] {
          if (!active)
            return false;
          const auto end = log_->End();
          const auto hw = replication_.HighWatermark();
          bool idle_response = false;
          if (response.responses.size() == 1 && response.responses.front().partitions.size() == 1) {
            const auto& part = response.responses.front().partitions.front();
            auto records = part.records.View();
            idle_response = part.diverging_epoch.epoch < 0 && part.snapshot_id.epoch < 0 &&
                            (!records || records->empty());
          }
          bool reset = false;
          auto s = replication_.ReceiveFetchResponse(leader, fetch->sequence, std::move(response),
                                                     &reset);
          if (!s.ok()) {
            VLOG(1) << "Fetch from node " << leader << ": " << s;
            return false;
          }
          if (!reset)
            return false;
          ResetDeadline(NowMs());
          await_apply =
              idle_response && fetch->request.max_wait_ms == 0 && apply_busy_ && apply_busy_();
          if (log_->End() != end || replication_.HighWatermark() != hw ||
              replication_.RequestedSnapshot())
            return true;

          // An idle long poll already waited. Re-arm it immediately so the next append can wake it.
          return waited && idle_response;
        },
        IoAccess::kData,
        [&] {
          return !log_->FlushPending() ||
                 replication_.CanReceiveDuringFlush(leader, fetch->sequence, response);
        });
    if (await_apply) {
      commit_changed_.wait_until(applied.epoch(),
                                 steady_clock::now() + milliseconds(kRequestTimeoutMs));
    }
    return progressed;
  }

  void Discover(PeerConnection& peer, const CampaignVoter& voter, const atomic_bool& active) {
    DescribeQuorumRequest req;
    auto& topic = req.topics.emplace_back();
    topic.topic_name = campaign_.target().topic;
    topic.partitions.emplace_back().partition_index = campaign_.target().partition;
    DescribeQuorumResponse res;
    if (peer.Call(req, 0, &res) || res.error_code || res.topics.size() != 1 ||
        res.topics[0].topic_name != topic.topic_name || res.topics[0].partitions.size() != 1)
      return;
    const auto& part = res.topics[0].partitions.front();
    if (part.partition_index != campaign_.target().partition || part.error_code ||
        part.leader_id != voter.key.id)
      return;
    RunOnWorker([&] {
      if (active && part.leader_epoch >= campaign_.state().election.epoch) {
        if (auto ec = campaign_.ObserveLeader(part.leader_epoch, voter.key.id, voter.endpoint); ec)
          VLOG(1) << "Metadata discovery: " << ec.message();
      }
    });
  }

  bool FetchSnapshotFrom(PeerConnection& peer, int32_t leader, const atomic_bool& active) {
    int16_t version;
    if (peer.Negotiate<FetchSnapshotRequest>(0, &version))
      return false;
    auto fetch = RunOnWorker([&] { return snapshots_.Prepare(version, kMaxFetchBytes); });
    if (!fetch || fetch->peer_id != leader)
      return false;
    FetchSnapshotResponse response;
    if (peer.Call(fetch->request, version, &response))
      return false;
    return RunOnWorker([&] {
      if (!active)
        return false;
      const auto position = snapshots_.DownloadPosition();
      const auto latest = log_->LatestSnapshot();
      bool reset = false;
      auto s = snapshots_.ReceiveResponse(leader, fetch->sequence, response, &reset);
      if (!s.ok()) {
        VLOG(1) << "Snapshot from node " << leader << ": " << s;
        return false;
      }
      if (!reset)
        return false;
      ResetDeadline(NowMs());
      const bool progressed =
          snapshots_.DownloadPosition() > position || log_->LatestSnapshot() != latest;

      // Deliver a newly installed cut before the next Fetch can advertise application progress.
      if (!replication_.RequestedSnapshot())
        Tick();
      return progressed;
    });
  }

  error_code Fetch(int16_t version, const FetchRequest& request, FetchResponse* response);
  template <typename Request, typename Response>
  error_code ChangeVoters(const Request& req, Response* res);

  unique_ptr<PartitionLog> log_;
  FileQuorumStateStore store_;
  const vector<CampaignVoter> bootstrap_;
  ElectionCampaign campaign_;
  EpochControl epochs_;
  FetchReplication replication_;
  SnapshotTransfer snapshots_;
  VoterChanges changes_;
  const Kind kind_;
  const bool observer_;
  ApplyCommitted apply_;
  ApplyStep apply_step_;
  function<bool()> apply_busy_;
  DataDriver data_driver_;
  atomic_bool data_wake_queued_{false};
  optional<steady_clock::time_point> data_deadline_;
  bool encoding_ = false;
  fb2::EventCount fetch_changed_;
  fb2::EventCount commit_changed_;
  fb2::EventCount io_changed_;
  PartitionLog::AsyncIO async_io_;
  unique_ptr<fb2::Pool> disk_pool_;
  int64_t applied_ = -1;
  map<int32_t, int64_t> discovery_after_;
#if DFLY_RAFT_USE_FOLLY
  GroupLoop worker_;
#else
  fb2::FiberQueueThreadPool worker_{1};
  fb2::FiberQueueThreadPool encoder_{1};
#endif
  absl::BitGen random_;
  int64_t deadline_ = 0;
  QuorumRole role_ = QuorumRole::kUnattached;
  int32_t epoch_ = -1;
  bool failed_ = false;
  atomic<int32_t> leader_epoch_{-1};
  map<int32_t, EpochNotification> notifications_;
  atomic_bool stopping_{false};
  fb2::Fiber supervisor_;
};

RpcHandlers RaftGroup::Impl::Handlers() {
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
  // The caller supplies a group-specific listener for messages without a partition.
  handlers.add_voter = [this](int16_t, const auto& req, auto* res) {
    return ChangeVoters(req, res);
  };
  handlers.remove_voter = [this](int16_t, const auto& req, auto* res) {
    return ChangeVoters(req, res);
  };
  handlers.update_voter = [this](int16_t, const auto& req, auto* res) {
    return ChangeVoters(req, res);
  };
  handlers.fetch_snapshot = [this](int16_t v, const auto& req, auto* res) {
    return OnWorker([&] {
      auto s = snapshots_.HandleRequest(req, v, NowMs(), res);
      return s.ok() ? error_code{} : make_error_code(errc::io_error);
    });
  };
  handlers.describe_quorum = [this](int16_t v, const auto& req, auto* res) {
    return OnWorker([&] { return DescribeQuorum(campaign_, replication_, v, req, WallMs(), res); });
  };
  return handlers;
}

template <typename Request, typename Response>
error_code RaftGroup::Impl::ChangeVoters(const Request& req, Response* res) {
  VoterChanges::Result result;
  optional<VoterChanges::Probe> probe;
  if (auto ec = OnWorker([&] {
        result = changes_.Begin(req, NowMs(), WallMs());
        probe = changes_.TakeProbe();
        return error_code{};
      });
      ec)
    return ec;
  if (probe) {
    PeerConnection peer(probe->endpoint, kRequestTimeoutMs, kMaxRaftFrame);
    ApiVersionsRequest versions;
    versions.client_software_name = "dragonfly";
    versions.client_software_version = "1";
    ApiVersionsResponse reply;
    if (peer.Call(versions, 3, &reply))
      reply.error_code = 7;
    peer.Close();
    if (auto ec = OnWorker([&] {
          auto s = changes_.OnApiVersions(*probe, reply, NowMs(), WallMs());
          return s.ok() ? error_code{} : make_error_code(errc::io_error);
        });
        ec)
      return ec;
  }
  // The listener fiber may park; the quorum's disk worker must remain available to
  // voters whose next Fetch can commit this very configuration change.
  while (!stopping_) {
    bool ready = false;
    if (auto ec = OnWorker([&] {
          if (!result->error)
            return error_code{};
          ready = true;
          res->error_code = *result->error;
          if constexpr (is_same_v<Response, UpdateRaftVoterResponse>) {
            const auto& election = campaign_.state().election;
            res->current_leader.leader_id = election.leader_id.value_or(-1);
            res->current_leader.leader_epoch = election.epoch;
            if (const auto& endpoint = campaign_.LeaderEndpoint(); endpoint) {
              res->current_leader.host = endpoint->host;
              res->current_leader.port = endpoint->port;
            }
          } else {
            res->error_message = result->message;
          }
          return error_code{};
        });
        ec)
      return ec;
    if (ready)
      return {};
    ThisFiberSleep();
  }
  return make_error_code(errc::operation_canceled);
}

error_code RaftGroup::Impl::Fetch(int16_t version, const FetchRequest& request,
                                  FetchResponse* response) {
  const auto deadline = steady_clock::now() + milliseconds(request.max_wait_ms);
  auto key = fetch_changed_.prepareWait();
  bool can_wait = false;
  shared_ptr<const FetchReplication::ParkedFetch> parked;
  auto respond = [&](bool initial) {
    while (true) {
      auto io_key = io_changed_.prepareWait();
      auto result = RunOnWorker(
          [&] {
            if (!log_->AppendPending())
              Tick();
            if (failed_ || stopping_)
              return make_error_code(errc::state_not_recoverable);

            // A follower may persist the exposed tail first. Revalidate its original request
            // after local publication, without recording contact or claiming divergence early.
            if (request.topics.size() == 1 && request.topics.front().partitions.size() == 1) {
              const auto& part = request.topics.front().partitions.front();
              if (log_->PendingContains(part.fetch_offset, part.last_fetched_epoch))
                return make_error_code(errc::resource_unavailable_try_again);
            }
            auto contact = initial ? optional<int64_t>{NowMs()} : nullopt;
            auto s = replication_.HandleFetch(request, version, contact, WallMs(), response,
                                              &can_wait, initial ? &parked : nullptr);
            return s.ok() ? error_code{} : make_error_code(errc::io_error);
          },
          IoAccess::kFetch);
      if (result != errc::resource_unavailable_try_again)
        return result;
      io_changed_.wait(io_key.epoch());
    }
  };
  if (auto ec = respond(true); ec || !can_wait || steady_clock::now() >= deadline)
    return ec;

  // Timeout returns the retained empty response. Waking a parked RPC never adds voter contact.
  if (fetch_changed_.wait_until(key.epoch(), deadline) == cv_status::timeout)
    return {};
  if (parked && !stopping_) {
    if (auto records = parked->Records(request.max_bytes)) {
      // The response retains the previously validated epoch and HW. Only a later Fetch can
      // report a higher acknowledged offset under the group's configured durability mode.
      response->responses.front().partitions.front().records.data->assign(*records);
      return {};
    }
  }
  return respond(false);
}

RaftGroup::RaftGroup(unique_ptr<Impl> impl) : impl_(std::move(impl)) {
}
RaftGroup::~RaftGroup() = default;

absl::StatusOr<unique_ptr<RaftGroup>> RaftGroup::Open(const filesystem::path& dir,
                                                      const RaftDirectoryIdentity& identity,
                                                      Kind kind, int32_t partition,
                                                      const VoterConnections& voters, bool observer,
                                                      ApplyCommitted apply,
                                                      SnapshotValidator validate) {
  if (partition < 0 || voters.empty() || (kind == Kind::kMetadata && partition != 0) ||
      (observer && (kind != Kind::kMetadata || voters.contains(identity.node_id))))
    return absl::InvalidArgumentError("Invalid quorum partition, local role or voter set");
  vector<CampaignVoter> members;
  for (const auto& [id, ep] : voters) {
    if (id < 0 || ep.host.empty() || ep.port == 0 || ep.host == "::")
      return absl::InvalidArgumentError("Invalid static voter endpoint");
    members.push_back({ReplicaKey{id}, ep});
  }
  const string topic = kind == Kind::kMetadata ? kMetadataTopic : kDataTopic;
  const auto mode = absl::GetFlag(FLAGS_raft_data_ack_mode);
  if (mode != "flush" && mode != "memory")
    return absl::InvalidArgumentError("raft_data_ack_mode must be flush or memory");
  auto group_dir = dir / (topic + "-" + to_string(partition));
  auto log = PartitionLog::Open(
      group_dir, kind == Kind::kData ? absl::GetFlag(FLAGS_raft_data_segment_bytes) : 1U << 30,
      kind == Kind::kData && mode == "memory" ? PartitionLog::AckMode::kMemory
                                              : PartitionLog::AckMode::kFlush);
  if (!log.ok())
    return log.status();
  VoteTarget target{
      identity.cluster_id, topic, partition, {identity.node_id, identity.directory_id}};
  auto core = make_unique<Impl>(std::move(target), std::move(members), group_dir, *std::move(log),
                                kind, observer, std::move(apply), std::move(validate));
  if (auto s = core->Recover(); !s.ok())
    return s;
  return unique_ptr<RaftGroup>(new RaftGroup(std::move(core)));
}
void RaftGroup::Start(fb2::ProactorBase* owner) {
  impl_->Start(owner);
}
void RaftGroup::RequestStop() {
  impl_->RequestStop();
}
void RaftGroup::Join() {
  impl_->Join();
}
RpcHandlers RaftGroup::Handlers() {
  return impl_->Handlers();
}
absl::Status RaftGroup::Visit(Operation operation) {
  return impl_->Visit(operation);
}

optional<int32_t> RaftGroup::LeaderEpoch() const {
  return impl_->LeaderEpoch();
}

void RaftGroup::SetDataDriver(DataDriver driver) {
  impl_->SetDataDriver(std::move(driver));
}

void RaftGroup::WakeData() {
  impl_->WakeData();
}

void RaftGroup::SetApplyLane(ApplyStep step, function<bool()> busy) {
  impl_->SetApplyLane(std::move(step), std::move(busy));
}

void RaftGroup::CompleteApply(function<void()> completion) {
  impl_->CompleteApply(std::move(completion));
}

void RaftGroup::Encode(function<void()> work, function<void()> completion) {
  impl_->Encode(std::move(work), std::move(completion));
}

absl::Status RaftGroup::VisitUntil(steady_clock::time_point deadline,
                                   absl::FunctionRef<absl::Status(State, bool*)> operation) {
  return impl_->VisitUntil(deadline, operation);
}

absl::Status RaftGroup::VisitDataUntil(steady_clock::time_point deadline,
                                       absl::FunctionRef<absl::Status(State, bool*)> operation) {
  return impl_->VisitUntil(deadline, operation, true);
}

}  // namespace dfly::cluster::raft
