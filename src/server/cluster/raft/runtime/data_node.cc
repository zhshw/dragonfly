// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/runtime/data_node.h"

#include <absl/cleanup/cleanup.h>
#include <absl/random/random.h>
#include <absl/strings/str_cat.h>

#include <atomic>
#include <chrono>
#include <deque>

#include "base/flags.h"
#include "base/logging.h"
#include "server/cluster/raft/data/apply_progress.h"
#include "server/cluster/raft/data/kv_rdb.h"
#include "server/cluster/raft/data/set_effect.h"
#include "server/cluster/raft/data/set_executor.h"
#include "server/cluster/raft/data/write_progress.h"
#include "server/cluster/raft/metadata/metadata_image.h"
#include "server/cluster/raft/quorum/fetch_replication.h"
#include "server/cluster/raft/runtime/data_snapshot.h"
#include "server/cluster/raft/runtime/group.h"
#include "server/cluster/raft/runtime/slot_migration_driver.h"
#include "server/cluster/raft/transport/peer_connection.h"
#include "server/cluster/raft/transport/raft_listener.h"
#include "server/journal/executor.h"
#include "util/accept_server.h"
#include "util/fibers/fiberqueue_threadpool.h"
#include "util/fibers/pool.h"
#include "util/fibers/synchronization.h"

#ifdef DFLY_RAFT_OTEL
#include "server/cluster/raft/otel/trace_session.h"
#endif

ABSL_FLAG(bool, raft_drain_writes, true,
          "Drain ready ordinary SETs with bounded queue backpressure");
ABSL_DECLARE_FLAG(int64_t, raft_data_snapshot_bytes);
ABSL_DECLARE_FLAG(int64_t, raft_data_snapshot_interval_ms);

namespace dfly::cluster::raft {
namespace {
using namespace std;
using namespace chrono;
using namespace protocol;
namespace fb2 = util::fb2;
using Broker = RegisterBrokerRecord;

optional<QuorumEndpoint> Endpoint(const Broker& broker, string_view name) {
  for (const auto& ep : broker.end_points)
    if (ep.name == name)
      return QuorumEndpoint{ep.host, ep.port};
  return nullopt;
}

absl::Status ClusterDown() {
  return absl::UnavailableError("-CLUSTERDOWN Data group unavailable");
}

struct AsyncPendingSet;

struct PendingSet {
  DbIndex db;
  SlotId slot;
  string_view key;
  string_view value;
  optional<SetCondition> condition;
  int32_t epoch;
  absl::Status status{};
  bool stored = true;
  optional<SetEffectView> effect{};
  size_t encoded_bytes = 0;
  size_t key_offset = 0;
  LogPosition end{};
  fb2::EmbeddedBlockingCounter* appended = nullptr;
  AsyncPendingSet* async = nullptr;
  const steady_clock::time_point deadline = steady_clock::now() + seconds(5);
};

struct SetBatch {
  vector<PendingSet*> requests;
  size_t value_bytes = 0;
  size_t byte_budget = SIZE_MAX;
  absl::Status status;
  bool encoding = false;
  absl::Status encode_status;
  shared_ptr<const string> encoded;
  LogPosition base{};
  uint64_t generation = 0;
  fb2::EmbeddedBlockingCounter appended{1};
};

struct LocalPartition {
  shared_ptr<trace::Capture> trace;
  LocalPartition(int32_t group, const string& cluster_id, Service& service,
                 fb2::ProactorBase* owner)
      : id(group),
        effects(group),
        snapshots(cluster_id, group, service, owner),
        apply_executor(&service) {
  }

  // Client fibers may run on different proactors. Only registration and detaching
  // the open batch need the mutex; all validation and appends run on the group worker.
  fb2::Mutex batch_mu;
  shared_ptr<SetBatch> open_batch;
  deque<PendingSet*> ready_sets;
  size_t queued_bytes = 0;
  size_t queued_requests = 0;
  SetBatch active_batch;
  deque<shared_ptr<AsyncPendingSet>> pending_writes;
  atomic<shared_ptr<WriteEpoch>> reply_epoch;
  base::mpmc_bounded_queue<shared_ptr<AsyncPendingSet>> admissions{256};

  // Reservations include completed writes until their ordered replies release the ticket.
  AdmissionBudget budget{8U << 20, 256};
  fb2::EventCount queue_changed;
  int32_t id;
  CommittedEffects effects;
  DataSnapshotter snapshots;

  // One persistent apply lane executes committed batches on the owner proactor.
  // Keeping its executor lets journal replay activate each DB once across all shards.
  JournalExecutor apply_executor;
  fb2::FiberQueue apply_queue{8};
  fb2::Fiber apply_fiber;
  shared_ptr<CommittedEffects::ApplyBatch> applying;
  absl::Status apply_status;
  DataApplyProgress progress;
  atomic_uint64_t replica_version{0};
  fb2::EventCount replica_changed;
  unique_ptr<RaftGroup> raft;
  unique_ptr<SlotMigrationDriver> migrations;
  RpcHandlers handlers;
};

struct AsyncPendingSet final : WatermarkWrite {
  AsyncPendingSet(shared_ptr<LocalPartition> local, DbIndex db, string_view key, string_view value,
                  shared_ptr<WriteEpoch> epoch, cmn::BackedArguments* owned_args,
                  AdmissionBudget::Reservation reservation)
      : WatermarkWrite(epoch),
        local(std::move(local)),
        reservation(std::move(reservation)),
        proposal{db, KeySlot(key), {}, {}, nullopt, epoch->epoch} {
    proposal.async = this;
    if (owned_args) {
      input.SwapArgs(*owned_args);
      proposal.key = input[1];
      proposal.value = input[2];
    } else {
      this->key = key;
      this->value = value;
      proposal.key = this->key;
      proposal.value = this->value;
    }
  }

  ~AsyncPendingSet() override {
    reservation.Release();
    local->queue_changed.notifyAll();
  }

  void ReleaseInput() {
    if (local->trace) {
      auto event =
          local->trace->MakeEvent("raft.set.input_lifetime", queued_at, local->trace->Now());
      event.attributes["node_role"] = "Leader";
      event.attributes["epoch"] = to_string(proposal.epoch);
      event.attributes["reserved_bytes"] = to_string(reservation.bytes());
      event.attributes["input_heap_bytes"] = to_string(input.HeapMemory());
      local->trace->Emit(std::move(event));
    }

    // The drainer has finished all reads; cached effects now refer to the immutable WAL.
    proposal.effect.reset();
    proposal.key = {};
    proposal.value = {};
    cmn::BackedArguments{}.SwapArgs(input);
    string{}.swap(key);
    string{}.swap(value);
  }

  shared_ptr<LocalPartition> local;
  AdmissionBudget::Reservation reservation;
  const trace::Time queued_at = local->trace ? local->trace->Now() : 0;
  cmn::BackedArguments input;
  string key;
  string value;
  PendingSet proposal;
  bool execution_done = false;
};

class DataNodeImpl final : public DataNode {
 public:
  DataNodeImpl(filesystem::path dir, RaftDirectoryIdentity identity, VoterConnections controllers,
               QuorumEndpoint listen, uint16_t resp_port, Service* service,
               fb2::ProactorBase* owner)
      : dir_(std::move(dir)),
        identity_(std::move(identity)),
        controllers_(std::move(controllers)),
        listen_(std::move(listen)),
        resp_port_(resp_port),
        service_(service),
        owner_(owner) {
    absl::BitGen random;
    for (auto& byte : incarnation_)
      byte = absl::Uniform<unsigned>(random, 0, 256);
    incarnation_[6] = (incarnation_[6] & 15) | 64;
    incarnation_[8] = (incarnation_[8] & 63) | 128;
  }

  absl::Status Init(util::AcceptServer* acceptor) {
#ifdef DFLY_RAFT_OTEL
    auto trace = otel::TraceSession::FromEnvironment(identity_.node_id, owner_);
    if (!trace.ok())
      return trace.status();
    trace_session_ = *std::move(trace);
    if (trace_session_)
      trace_factory_ = [this](string group) { return trace_session_->Create(std::move(group)); };
#endif
    auto group = RaftGroup::Open(dir_, identity_, RaftGroup::Kind::kMetadata, 0, controllers_, true,
                                 [this](const PartitionLog& log, int64_t hw) {
                                   auto status = loader_.Advance(log, hw);
                                   if (status.ok())
                                     image_.store(loader_.image());
                                   return status;
                                 });
    if (!group.ok())
      return group.status();
    observer_ = *std::move(group);
    if (trace_factory_)
      observer_->SetTraceCapture(trace_factory_("0"));

    // The RESP and internal ports can open before this broker receives its slot assignment.
    RpcHandlers handlers;
    handlers.vote = Forward(&RpcHandlers::vote);
    handlers.add_voter = Forward(&RpcHandlers::add_voter);
    handlers.remove_voter = Forward(&RpcHandlers::remove_voter);
    handlers.update_voter = Forward(&RpcHandlers::update_voter);
    handlers.begin_epoch = Forward(&RpcHandlers::begin_epoch);
    handlers.end_epoch = Forward(&RpcHandlers::end_epoch);
    handlers.fetch = Forward(&RpcHandlers::fetch);
    handlers.fetch_snapshot = Forward(&RpcHandlers::fetch_snapshot);
    handlers.fetch_slot_image = Forward(&RpcHandlers::fetch_slot_image);
    handlers.describe_quorum = Forward(&RpcHandlers::describe_quorum);
    rpc_server_ =
        make_unique<RaftServer>(kMaxRaftFrame, std::move(handlers), [this] { RequestStop(); });
    if (auto ec = rpc_server_->Listen(listen_.host, listen_.port, acceptor); ec)
      return absl::ErrnoToStatus(ec.value(), "Binding data quorum listener");
    acceptor_ = acceptor;
    acceptor_->TriggerOnBreakSignal([this] { RequestStop(); });
    return absl::OkStatus();
  }

  void Start() override {
    observer_->Start(owner_);
    control_fiber_ = owner_->LaunchFiber([this] { BrokerLoop(); });
    migration_fiber_ = owner_->LaunchFiber([this] { MigrationLoop(); });
  }

  void Stop() override {
    RequestStop();
    rpc_server_->Stop();
    acceptor_->TriggerOnBreakSignal({});
    if (control_fiber_.IsJoinable())
      control_fiber_.Join();
    if (migration_fiber_.IsJoinable())
      migration_fiber_.Join();
    if (snapshot_fiber_.IsJoinable())
      snapshot_fiber_.Join();
    if (auto group = partition_.load()) {
      group->raft->Join();
      group->raft->SetDataDriver({});
      group->apply_queue.Shutdown();
      if (group->apply_fiber.IsJoinable())
        group->apply_fiber.Join();
    }
    observer_->Join();
#ifdef DFLY_RAFT_OTEL
    if (trace_session_) {
      auto status = trace_session_->Export();
      LOG_IF(ERROR, !status.ok()) << "Raft trace export: " << status;
      LOG_IF(INFO, status.ok())
          << "Raft trace export accepted; verify stored capture before publishing";
    }
#endif
  }
  void SetTraceFactory(TraceFactory factory) override {
    trace_factory_ = std::move(factory);
    if (observer_)
      observer_->SetTraceCapture(trace_factory_ ? trace_factory_("0") : nullptr);
  }
  shared_ptr<trace::Admission> NewAdmissionTrace(uint64_t connection) override {
    auto local = partition_.load();
    if (!local || !trace_factory_)
      return nullptr;
    auto capture = trace_factory_(to_string(local->id));
    return capture ? make_shared<trace::Admission>(std::move(capture), connection,
                                                   local->budget.progress())
                   : nullptr;
  }
  shared_ptr<PendingDataWrite> TrySetObserved(DbIndex db, cmn::BackedArguments* args,
                                              trace::Admission* admission) override {
    return TrySetImpl(db, (*args)[1], (*args)[2], args, admission);
  }
  bool SupportsAsyncSet() const override {
    return absl::GetFlag(FLAGS_raft_drain_writes);
  }
  shared_ptr<PendingDataWrite> TrySet(DbIndex db, string_view key, string_view value) override;
  shared_ptr<PendingDataWrite> TrySetOwned(DbIndex db, cmn::BackedArguments* args) override;
  absl::Status Set(DbIndex db, string_view key, string_view value,
                   DataWritePosition* position) override;
  absl::StatusOr<bool> SetIf(DbIndex db, string_view key, string_view value, SetCondition condition,
                             DataWritePosition* position) override;
  absl::StatusOr<optional<string>> Get(DbIndex db, string_view key, DataReadMode mode) override;
  absl::StatusOr<DataTopology> Topology() override;
  absl::StatusOr<DataReplicationState> ReplicationState() override;

 private:
  void RequestStop() {
    stopping_ = true;
    observer_->RequestStop();
    if (auto group = partition_.load()) {
      group->raft->RequestStop();
      group->replica_version.fetch_add(1);
      group->replica_changed.notifyAll();
    }
  }

  template <typename R, typename S>
  auto Forward(RpcHandler<R, S> RpcHandlers::*member) -> RpcHandler<R, S> {
    return [this, member](int16_t version, const R& request, S* response) {
      auto local = partition_.load();
      if (!local || stopping_)
        return make_error_code(errc::resource_unavailable_try_again);
      return (local->handlers.*member)(version, request, response);
    };
  }
  absl::StatusOr<bool> ProposeSet(DbIndex db, string_view key, string_view value,
                                  optional<SetCondition> condition, DataWritePosition* position);
  shared_ptr<PendingDataWrite> TrySetImpl(DbIndex db, string_view key, string_view value,
                                          cmn::BackedArguments* owned_args,
                                          trace::Admission* admission = nullptr);
  absl::Status QueueSet(shared_ptr<LocalPartition> local, PendingSet* request);
  optional<steady_clock::time_point> PumpSets(LocalPartition& local, RaftGroup::State state,
                                              bool unavailable);
  absl::Status WaitApplied(shared_ptr<LocalPartition> local, const PendingSet& proposal,
                           steady_clock::time_point deadline, DataWritePosition* position);
  absl::Status AppendSetBatch(LocalPartition& local, SetBatch& batch, RaftGroup::State state,
                              bool* done);
  absl::Status CommitSetBatch(LocalPartition& local, SetBatch& batch, RaftGroup::State state);
  absl::Status OpenPartition(const MetadataImage& image, const Broker& local);
  void BrokerLoop();
  void MigrationLoop();
  void RegisterOrHeartbeat(PeerConnection& peer, const MetadataImage& image, const Broker& broker);
  absl::StatusOr<RedisRoute> Route(SlotId slot, const LocalPartition* partition,
                                   DataReadMode mode = DataReadMode::kLeaderOnly) const;

  filesystem::path dir_;
  RaftDirectoryIdentity identity_;
  VoterConnections controllers_;
  QuorumEndpoint listen_;
  uint16_t resp_port_;
  TraceFactory trace_factory_;
#ifdef DFLY_RAFT_OTEL
  unique_ptr<otel::TraceSession> trace_session_;
#endif
  Service* service_;
  fb2::ProactorBase* owner_;
  fb2::FiberQueueThreadPool opener_{1};
  Uuid incarnation_{};
  MetadataLoader loader_;
  unique_ptr<RaftGroup> observer_;
  unique_ptr<RaftServer> rpc_server_;
  util::AcceptServer* acceptor_ = nullptr;
  atomic<shared_ptr<const MetadataImage>> image_;
  atomic<shared_ptr<LocalPartition>> partition_;
  int64_t registered_epoch_ = -1;
  atomic_bool stopping_{false};
  fb2::Fiber control_fiber_;
  fb2::Fiber migration_fiber_;
  fb2::Fiber snapshot_fiber_;

  // Both loops run on owner_, each with a separate RPC connection.
  optional<QuorumEndpoint> controller_leader_;
};

absl::Status DataNodeImpl::OpenPartition(const MetadataImage& image, const Broker& broker) {
  auto raft = Endpoint(broker, "RAFT");
  auto resp = Endpoint(broker, "RESP");
  if (raft != listen_ || !resp || resp->port != resp_port_)
    return absl::FailedPreconditionError(
        "Local ports or Raft host differ from registered topology");
  for (const auto& [id, group] : image.directory.groups()) {
    if (find(group.members.begin(), group.members.end(), identity_.node_id) == group.members.end())
      continue;
    VoterConnections voters;
    for (int32_t member : group.members) {
      auto it = image.brokers.find(member);
      auto ep = it == image.brokers.end() ? nullopt : Endpoint(it->second, "RAFT");
      if (!ep)
        return absl::FailedPreconditionError("Data voter is missing a Raft endpoint");
      voters.emplace(member, *ep);
    }
    auto local = make_shared<LocalPartition>(id, identity_.cluster_id, *service_, owner_);
    auto apply = [this, replica = local.get()](const PartitionLog& log, int64_t hw) {
      if (auto s = replica->snapshots.Restore(log, hw, &replica->effects); !s.ok())
        return s;
      auto apply_sets = [&](span<const SetEffectView* const> effects, size_t* applied) {
        auto trace = replica->trace ? replica->raft->NewTraceBatch() : nullptr;
        const auto queued = trace ? trace->Now() : 0;
        absl::Cleanup publish = [&] {
          if (trace)
            trace->Publish();
        };

        // Re-enter once per bounded batch, preserving log order and the completed prefix.
        return owner_->Await([&] {
          if (trace) {
            trace->Record("raft.apply.owner_queue", queued, trace->Now());
            replica->apply_executor.SetApplyObserver([trace](int64_t start, int64_t end) {
              trace->Record("journal.apply", start, end);
            });
          }
          absl::Cleanup observer = [&] {
            if (trace)
              replica->apply_executor.SetApplyObserver({});
          };
          const auto deadline = steady_clock::now() + milliseconds(1);
          for (const auto* effect : effects) {
            if (auto s = ExecuteSetEffect(replica->apply_executor, *effect); !s.ok())
              return s;
            ++*applied;
            if (steady_clock::now() >= deadline)
              break;
          }
          return absl::OkStatus();
        });
      };
      auto apply_slot = [&](SlotId slot, optional<string_view> bytes, string_view digest) {
        return owner_->Await([&] {
          SlotRanges ranges({{slot, slot}});
          if (bytes)
            return InstallKvRdb(service_, ranges, *bytes, digest);
          ClearKvSlots(*service_, ranges);
          return absl::OkStatus();
        });
      };
      return replica->effects.ApplyToBatched(log, hw, apply_sets, apply_slot);
    };
    auto opened = opener_.Await([&] {
      return RaftGroup::Open(dir_, identity_, RaftGroup::Kind::kData, id, voters, false, apply,
                             [replica = local.get()](const SnapshotWriter& w) {
                               return replica->snapshots.Validate(w);
                             });
    });
    if (!opened.ok())
      return opened.status();
    local->raft = *std::move(opened);
#ifdef DFLY_RAFT_OTEL
    if (trace_session_)
      trace_session_->SetExpectedNodes(group.members);
#endif
    if (trace_factory_) {
      local->trace = trace_factory_(to_string(local->id));
      local->raft->SetTraceCapture(local->trace);
    }
    local->raft->SetApplyLane(
        [this, replica = local.get(), apply](const PartitionLog& log,
                                             int64_t hw) -> absl::StatusOr<int64_t> {
          if (!replica->apply_status.ok())
            return replica->apply_status;
          if (replica->applying || stopping_)
            return replica->effects.applied_offset();
          if (replica->effects.applied_offset() >= log.StartOffset() &&
              replica->effects.applied_offset() <= hw) {
            auto batch = replica->effects.PrepareApply(log, hw);
            if (!batch.ok())
              return batch.status();
            if (*batch) {
              replica->applying = *batch;
              auto trace = replica->raft->NewTraceBatch();
              const auto queued = trace ? trace->Now() : 0;
              CHECK(replica->apply_queue.TryAdd([replica, batch = *batch, trace, queued] {
                const auto start = trace ? trace->Now() : 0;
                if (trace)
                  trace->Record("raft.apply.lane_queue", queued, start);
                size_t completed = 0;
                auto status =
                    ExecuteSetBatch(replica->apply_executor, *batch, trace.get(), &completed);
                const auto finished = trace ? trace->Now() : 0;
                if (trace)
                  trace->Record("raft.apply.lane_run", start, finished);
                replica->raft->CompleteApply([replica, batch, completed, status, trace, finished] {
                  if (trace) {
                    trace->Record("raft.apply.complete_queue", finished, trace->Now());
                    trace->Publish();
                  }
                  auto finish = replica->effects.FinishApply(*batch, completed);
                  replica->apply_status = finish.ok() ? status : finish;
                  replica->applying.reset();
                });
              }));
              return replica->effects.applied_offset();
            }
          }

          // Recovery, conditional no-ops and migration records retain their serialized barrier.
          if (auto s = apply(log, hw); !s.ok())
            return s;
          return replica->effects.applied_offset();
        },
        [replica = local.get()] { return bool(replica->applying); });
    if (auto s = local->raft->Visit([replica = local.get()](RaftGroup::State state) {
          state.log.SetWalCompleteHook([progress = replica->budget.progress()](int64_t now) {
            progress->wal_complete.store(now, memory_order_release);
          });
          state.replication.SetDataApplyHooks(
              [replica](auto* p) {
                DataApplyProgress::Advertise(replica->effects.applied_offset(), p);
              },
              [replica](ReplicaKey key, const auto& p, int64_t offset, int64_t now) {
                replica->progress.Observe(key, p, offset, now);
                replica->replica_version.fetch_add(1);
                replica->replica_changed.notifyAll();
              });
          state.replication.SetFetchedRecordsHook(
              [replica](const PartitionLog& log, const shared_ptr<const void>& buffer,
                        size_t capacity, span<const PartitionLog::ValidatedBatch> batches) {
                replica->effects.RememberFetched(log, buffer, capacity, batches);
              });
          return absl::OkStatus();
        });
        !s.ok())
      return s;
    local->apply_fiber = owner_->LaunchFiber([local] { local->apply_queue.Run(); });
    local->raft->SetDataDriver(
        [this, replica = local.get()](RaftGroup::State state, bool unavailable) {
          return PumpSets(*replica, state, unavailable);
        });
    local->handlers = local->raft->Handlers();
    local->migrations = make_unique<SlotMigrationDriver>(
        id, identity_.node_id, identity_.cluster_id, incarnation_, *local->raft, local->effects,
        [this] { return image_.load(); }, *service_);
    local->handlers.fetch_slot_image = [driver = local->migrations.get()](
                                           int16_t, const FetchSlotImageRequest& request,
                                           FetchSlotImageResponse* response) {
      driver->Fetch(request, response);
      return error_code{};
    };
    local->raft->Start(owner_);
    snapshot_fiber_ = owner_->LaunchFiber([this, local] {
      while (!stopping_) {
        auto s = local->snapshots.MaybeSave(*local->raft, local->effects);
        if (!s.ok() && !stopping_)
          LOG(WARNING) << "Data group " << local->id << " snapshot: " << s;
        util::ThisFiber::SleepFor(s.ok() ? milliseconds(100) : milliseconds(1000));
      }
    });
    partition_.store(std::move(local));
    return absl::OkStatus();
  }
  return absl::NotFoundError("Local broker is not assigned to a data group");
}

void DataNodeImpl::RegisterOrHeartbeat(PeerConnection& peer, const MetadataImage& image,
                                       const Broker& broker) {
  if (registered_epoch_ < 0 || broker.incarnation_id != incarnation_ ||
      broker.broker_epoch != registered_epoch_) {
    BrokerRegistrationRequest req;
    req.broker_id = identity_.node_id;
    req.cluster_id = identity_.cluster_id;
    req.incarnation_id = incarnation_;
    req.log_dirs = {identity_.directory_id};
    req.features.push_back({"metadata.version", 7, 34, {}});
    req.features.push_back({"dragonfly.cluster", 1, 1, {}});
    req.previous_broker_epoch = registered_epoch_;
    for (const auto& ep : broker.end_points)
      req.listeners.push_back({ep.name, ep.host, ep.port, ep.security_protocol, {}});
    BrokerRegistrationResponse response;
    if (!peer.Call(req, 3, &response) && response.error_code == 0)
      registered_epoch_ = response.broker_epoch;
    return;
  }
  BrokerHeartbeatRequest req;
  req.broker_id = identity_.node_id;
  req.broker_epoch = registered_epoch_;
  req.current_metadata_offset = image.stable_end_offset - 1;
  BrokerHeartbeatResponse response;
  if (!peer.Call(req, 0, &response) && response.error_code == 77)
    registered_epoch_ = -1;
}

void DataNodeImpl::BrokerLoop() {
  optional<QuorumEndpoint> connected;
  unique_ptr<PeerConnection> peer;
  while (!stopping_) {
    auto view = image_.load();
    if (view && view->directory.HasCompleteCoverage()) {
      auto local = view->brokers.find(identity_.node_id);
      if (local != view->brokers.end()) {
        if (!partition_.load()) {
          auto s = OpenPartition(*view, local->second);
          if (!s.ok()) {
            LOG(ERROR) << "Opening data group: " << s;
            break;
          }
        }
        optional<QuorumEndpoint> leader;
        auto s = observer_->Visit([&](RaftGroup::State state) {
          if (const auto& ep = state.election.LeaderEndpoint(); ep)
            leader = QuorumEndpoint{ep->host, ep->port};
          return absl::OkStatus();
        });
        controller_leader_ = s.ok() ? leader : nullopt;
        if (controller_leader_) {
          if (connected != leader) {
            if (peer)
              peer->Close();
            peer = make_unique<PeerConnection>(*leader, 2000, kMaxRaftFrame);
            connected = leader;
          }
          RegisterOrHeartbeat(*peer, *view, local->second);
        }
      }
    }
    util::ThisFiber::SleepFor(milliseconds(100));
  }
  if (peer)
    peer->Close();
}

void DataNodeImpl::MigrationLoop() {
  optional<QuorumEndpoint> connected;
  unique_ptr<PeerConnection> peer;
  while (!stopping_) {
    auto local = partition_.load();
    const auto leader = controller_leader_;
    if (local && leader) {
      if (connected != leader) {
        if (peer)
          peer->Close();
        connected = leader;
        peer = make_unique<PeerConnection>(*leader, 2000, kMaxRaftFrame);
      }
      local->migrations->Tick(*peer);
    }
    util::ThisFiber::SleepFor(milliseconds(25));
  }
  if (peer)
    peer->Close();
}

absl::StatusOr<RedisRoute> DataNodeImpl::Route(SlotId slot, const LocalPartition* partition,
                                               DataReadMode mode) const {
  auto image = image_.load();
  if (stopping_ || !image)
    return ClusterDown();
  auto route = image->directory.Route(slot, image->brokers);
  if (!route)
    return ClusterDown();
  auto local = image->directory.nodes().find(identity_.node_id);
  if (local == image->directory.nodes().end())
    return ClusterDown();
  const auto& members = image->directory.groups().at(route->owner.group_id).members;
  const bool local_read = mode == DataReadMode::kAllowFollower && partition &&
                          partition->id == route->owner.group_id &&
                          find(members.begin(), members.end(), identity_.node_id) != members.end();
  if (route->node_id != local->second && !local_read)
    return absl::FailedPreconditionError(
        absl::StrCat("-MOVED ", slot, " ", route->host, ":", route->port));
  const auto& broker = image->brokers.at(identity_.node_id);
  if (broker.incarnation_id != incarnation_ || broker.fenced || !partition ||
      partition->id != route->owner.group_id)
    return ClusterDown();

  // Fence the source before its freeze proposal can race the next client append.
  const auto& migrations = image->migrations;
  const auto& transfers = partition->effects.transfers();
  auto active = migrations.active_slots().find(slot);
  if (active != migrations.active_slots().end()) {
    const auto& move = migrations.records().at(active->second);
    if (move.source_group == partition->id && !move.source_finished)
      return absl::UnavailableError("-TRYAGAIN Slot migration in progress");
  }
  if (transfers.BlocksAccess(slot, route->owner.generation))
    return absl::UnavailableError("-TRYAGAIN Slot migration in progress");

  // A broker may replay the new owner before its data quorum has replayed the import.
  const auto* move = migrations.LatestCutover(slot);
  if (move && move->target_group == partition->id &&
      move->generation + 1 == route->owner.generation) {
    const auto* progress = transfers.Find(move->migration_id);
    if (!progress || progress->install != LogPosition{move->target_epoch, move->target_end_offset})
      return absl::UnavailableError("-TRYAGAIN Slot import is catching up");
  }
  return *route;
}

absl::StatusOr<optional<string>> DataNodeImpl::Get(DbIndex db, string_view key, DataReadMode mode) {
  auto slot = KeySlot(key);
  auto local = partition_.load();
  if (!local)
    return Route(slot, nullptr).status();
  optional<string> value;
  auto status = local->raft->Visit([&](RaftGroup::State state) {
    auto route = Route(slot, local.get(), mode);
    if (!route.ok())
      return route.status();
    auto hw = state.replication.HighWatermark();
    const auto& election = state.election.state();
    const bool follower_read = mode == DataReadMode::kAllowFollower &&
                               election.role == QuorumRole::kFollower &&
                               election.fetched_from_leader;
    if ((election.role != QuorumRole::kLeader && !follower_read) || !hw ||
        local->effects.applied_offset() < *hw)
      return ClusterDown();
    return owner_->Await([&] {
      JournalExecutor executor(service_);
      auto result = ReadCommittedValue(executor, db, key);
      if (result.ok())
        value = *std::move(result);
      return result.status();
    });
  });
  if (!status.ok())
    return status;
  return value;
}

shared_ptr<PendingDataWrite> DataNodeImpl::TrySet(DbIndex db, string_view key, string_view value) {
  return TrySetImpl(db, key, value, nullptr);
}

shared_ptr<PendingDataWrite> DataNodeImpl::TrySetOwned(DbIndex db, cmn::BackedArguments* args) {
  DCHECK_EQ(args->size(), 3u);
  return TrySetImpl(db, (*args)[1], (*args)[2], args);
}

shared_ptr<PendingDataWrite> DataNodeImpl::TrySetImpl(DbIndex db, string_view key,
                                                      string_view value,
                                                      cmn::BackedArguments* owned_args,
                                                      trace::Admission* admission) {
  auto local = partition_.load();
  if (!local || stopping_ || !SupportsAsyncSet())
    return nullptr;
  const size_t bytes = key.size() + value.size();
  auto epoch = local->raft->LeaderEpoch();
  if (!epoch)
    return nullptr;
  if (admission)
    admission->SetEpoch(*epoch);
  auto reservation = local->budget.TryReserve(bytes);
  if (!reservation) {
    if (admission)
      admission->Rejected(owned_args, bytes, local->budget);
    return nullptr;
  }
  auto progress = local->reply_epoch.load();
  while (!progress || progress->epoch != *epoch) {
    if (progress && progress->epoch > *epoch) {
      return nullptr;
    }
    auto next = make_shared<WriteEpoch>(*epoch);
    if (local->reply_epoch.compare_exchange_weak(progress, next)) {
      progress = std::move(next);
      break;
    }
  }
  if (admission)
    admission->Admitted(owned_args, &*reservation);
  auto request = make_shared<AsyncPendingSet>(local, db, key, value, progress, owned_args,
                                              std::move(*reservation));

  // The reservation covers every outstanding record, so the admission ring always has a slot.
  // Input ownership changes only after capacity is reserved, never on a rejected TrySetOwned.
  CHECK(local->admissions.try_enqueue(request));

  // One bounded record enters the group; no per-command execution context or confirmation task.
  local->raft->WakeData();
  return request;
}

absl::Status DataNodeImpl::Set(DbIndex db, string_view key, string_view value,
                               DataWritePosition* position) {
  return ProposeSet(db, key, value, nullopt, position).status();
}

absl::StatusOr<bool> DataNodeImpl::SetIf(DbIndex db, string_view key, string_view value,
                                         SetCondition condition, DataWritePosition* position) {
  return ProposeSet(db, key, value, condition, position);
}

absl::StatusOr<bool> DataNodeImpl::ProposeSet(DbIndex db, string_view key, string_view value,
                                              optional<SetCondition> condition,
                                              DataWritePosition* position) {
  const auto slot = KeySlot(key);
  auto local = partition_.load();
  if (!local) {
    return Route(slot, nullptr).status();
  }

  auto epoch = local->raft->LeaderEpoch();
  if (!epoch) {
    return local->raft->Visit([&](RaftGroup::State) {
      auto route = Route(slot, local.get());
      return route.ok() ? ClusterDown() : route.status();
    });
  }
  if (!condition && SupportsAsyncSet()) {
    const auto deadline = steady_clock::now() + seconds(5);
    while (!stopping_ && steady_clock::now() < deadline) {
      auto changed = local->queue_changed.prepareWait();
      if (auto write = TrySet(db, key, value)) {
        write->Wait();
        *position = write->position;
        if (!write->status.ok())
          return write->status;
        return true;
      }
      if (!local->raft->LeaderEpoch())
        break;
      local->queue_changed.wait_until(changed.epoch(), deadline);
    }
    return absl::UnavailableError("-TRYAGAIN Data group has no admission capacity");
  }

  fb2::EmbeddedBlockingCounter appended{1};
  PendingSet proposal{db, slot, key, value, condition, *epoch};
  proposal.appended = &appended;
  const auto deadline = steady_clock::now() + seconds(5);
  if (absl::GetFlag(FLAGS_raft_drain_writes)) {
    if (auto status = QueueSet(local, &proposal); !status.ok()) {
      if (absl::IsDeadlineExceeded(status))
        return absl::UnavailableError("-TRYAGAIN Data group is catching up");
      return status;
    }
  } else {
    shared_ptr<SetBatch> batch;
    bool dispatch;
    {
      // The first request dispatches immediately. Only writes already waiting when the worker
      // picks up the batch share its flush. Conditional SET evaluates against committed KV alone.
      constexpr size_t kMaxGroupPayload = 1U << 20;
      constexpr size_t kMaxGroupRequests = 32;
      const size_t size = key.size() + value.size();
      lock_guard lock(local->batch_mu);
      batch = local->open_batch;
      dispatch = !batch || condition || batch->requests.back()->epoch != *epoch ||
                 batch->requests.size() >= kMaxGroupRequests ||
                 batch->value_bytes + size > kMaxGroupPayload;
      if (dispatch) {
        batch = make_shared<SetBatch>();
        local->open_batch = condition ? nullptr : batch;
      }
      batch->value_bytes += size;
      batch->requests.push_back(&proposal);
    }

    if (dispatch) {
      auto seal = [&] {
        lock_guard lock(local->batch_mu);
        if (local->open_batch == batch)
          local->open_batch.reset();
      };
      batch->status =
          local->raft->VisitDataUntil(deadline, [&](RaftGroup::State state, bool* done) {
            seal();
            return AppendSetBatch(*local, *batch, state, done);
          });

      // VisitUntil can stop before invoking the callback, for example during shutdown.
      if (!batch->status.ok())
        seal();
      batch->appended.Dec();
    } else {
      batch->appended.Wait();
    }

    if (!proposal.status.ok())
      return proposal.status;
    if (!batch->status.ok()) {
      if (absl::IsDeadlineExceeded(batch->status))
        return absl::UnavailableError("-TRYAGAIN Data group is catching up");
      return batch->status;
    }
  }
  auto status = WaitApplied(local, proposal, deadline, position);
  if (!status.ok())
    return status;
  return proposal.stored;
}

absl::Status DataNodeImpl::WaitApplied(shared_ptr<LocalPartition> local, const PendingSet& proposal,
                                       steady_clock::time_point deadline,
                                       DataWritePosition* position) {
  if (proposal.stored)
    *position = {local->id, proposal.end.epoch, proposal.end.end_offset};
  auto status = local->raft->VisitDataUntil(deadline, [&](RaftGroup::State state, bool* done) {
    if (stopping_ || state.election.state().role != QuorumRole::kLeader ||
        state.election.state().election.epoch != proposal.end.epoch)
      return ClusterDown();
    *done = local->effects.applied_offset() >= proposal.end.end_offset;
    return absl::OkStatus();
  });
  if (!status.ok())
    return absl::UnavailableError("-TRYAGAIN Outcome of Raft write is unknown");
  return absl::OkStatus();
}

absl::Status DataNodeImpl::QueueSet(shared_ptr<LocalPartition> local, PendingSet* request) {
  constexpr size_t kQueueBytes = 8U << 20;
  constexpr size_t kQueueRequests = 256;
  const size_t bytes = request->key.size() + request->value.size();
  while (true) {
    auto key = local->queue_changed.prepareWait();
    {
      lock_guard lock(local->batch_mu);
      if (stopping_)
        return ClusterDown();
      if (local->raft->LeaderEpoch() != request->epoch)
        return ClusterDown();

      // One oversized request is admitted alone, so a legal large SET cannot starve.
      if (local->queued_requests == 0 ||
          (local->queued_requests < kQueueRequests &&
           bytes <= kQueueBytes - min(kQueueBytes, local->queued_bytes))) {
        local->ready_sets.push_back(request);
        local->queued_bytes += bytes;
        ++local->queued_requests;
        break;
      }
    }
    local->queue_changed.wait_until(key.epoch(), steady_clock::now() + milliseconds(100));
  }
  local->raft->WakeData();
  request->appended->Wait();
  return request->status;
}

optional<steady_clock::time_point> DataNodeImpl::PumpSets(LocalPartition& local,
                                                          RaftGroup::State state,
                                                          bool unavailable) {
  const auto& election = state.election.state();
  const auto now = steady_clock::now();
  vector<shared_ptr<WriteEpoch>> changed;
  auto remember = [&](const shared_ptr<WriteEpoch>& epoch) {
    if (find(changed.begin(), changed.end(), epoch) == changed.end())
      changed.push_back(epoch);
  };
  {
    lock_guard lock(local.batch_mu);
    shared_ptr<AsyncPendingSet> accepted;
    while (local.admissions.try_dequeue(accepted)) {
      if (local.trace) {
        auto event = local.trace->MakeEvent("raft.set.admission_queue", accepted->queued_at,
                                            local.trace->Now());
        event.attributes["node_role"] = "Leader";
        event.attributes["epoch"] = to_string(accepted->proposal.epoch);
        local.trace->Emit(std::move(event));
      }
      local.ready_sets.push_back(&accepted->proposal);
      local.queued_bytes += accepted->reservation.bytes();
      ++local.queued_requests;
      local.pending_writes.push_back(std::move(accepted));
    }
    auto reply_epoch = local.reply_epoch.load();
    if (!unavailable && election.role == QuorumRole::kLeader && reply_epoch &&
        reply_epoch->epoch == election.election.epoch) {
      const auto r =
          min({state.log.End().end_offset, state.replication.HighWatermark().value_or(-1),
               local.effects.applied_offset()});
      if (r > reply_epoch->reply_offset.load(memory_order_relaxed)) {
        if (local.trace) {
          const auto stamp = local.trace->Now();
          auto event = local.trace->MakeEvent("raft.reply.advance", stamp, stamp);
          event.attributes["epoch"] = to_string(reply_epoch->epoch);
          event.attributes["previous_offset"] = to_string(reply_epoch->reply_offset.load());
          event.attributes["end_offset"] = to_string(r);
          local.trace->Emit(std::move(event));
        }
        reply_epoch->reply_offset.store(r, memory_order_release);
        remember(reply_epoch);
      }
    }
    for (auto it = local.pending_writes.begin(); it != local.pending_writes.end();) {
      auto& write = **it;
      if (!write.IsReady() &&
          (unavailable || election.role != QuorumRole::kLeader ||
           write.epoch()->epoch != election.election.epoch || now >= write.proposal.deadline)) {
        write.Reject(absl::UnavailableError("-TRYAGAIN Outcome of Raft write is unknown"));
        remember(write.epoch());
      }
      if (write.execution_done && write.IsReady())
        it = local.pending_writes.erase(it);
      else
        ++it;
    }
  }

  // I/O completions and control events must still be processed when admission is saturated.
  // At most one bounded batch is prepared per event; remaining input arranges one more wake.
  if (!state.log.AppendPending() && !local.active_batch.encoding) {
    auto& batch = local.active_batch;
    if (batch.requests.empty()) {
      lock_guard lock(local.batch_mu);
      batch.byte_budget = local.queued_bytes >= (4U << 20) ? (4U << 20) : (1U << 20);
      while (!local.ready_sets.empty() && (unavailable || batch.requests.size() < 32)) {
        auto* next = local.ready_sets.front();
        const size_t bytes = next->key.size() + next->value.size();
        if (!unavailable && !batch.requests.empty() &&
            (next->condition || next->epoch != batch.requests.front()->epoch ||
             batch.value_bytes + bytes > batch.byte_budget))
          break;
        batch.requests.push_back(next);
        batch.value_bytes += bytes;
        local.ready_sets.pop_front();
        if (!unavailable && next->condition)
          break;
      }
    }
    if (!batch.requests.empty()) {
      bool done = false;
      auto status = unavailable ? ClusterDown() : AppendSetBatch(local, batch, state, &done);
      if (done || !status.ok()) {
        {
          lock_guard lock(local.batch_mu);
          local.queued_bytes -= batch.value_bytes;
          local.queued_requests -= batch.requests.size();
        }
        for (auto* request : batch.requests) {
          if (request->status.ok() && !status.ok())
            request->status = status;
          if (auto* write = request->async) {
            if (request->status.ok())
              write->Bind({local.id, request->end.epoch, request->end.end_offset});
            else
              write->Reject(request->status);
            write->ReleaseInput();
            write->execution_done = true;
            remember(write->epoch());
          } else {
            request->appended->Dec();
          }
        }
        batch.requests.clear();
        batch.value_bytes = 0;
        batch.encoded.reset();
        batch.encode_status = absl::OkStatus();
        local.queue_changed.notifyAll();
        local.raft->WakeData();
      }
    }
  }
  optional<steady_clock::time_point> deadline;
  {
    lock_guard lock(local.batch_mu);
    erase_if(local.pending_writes,
             [](const auto& write) { return write->execution_done && write->IsReady(); });
    auto track = [&](steady_clock::time_point value) {
      if (value > now && (!deadline || value < *deadline))
        deadline = value;
    };
    for (const auto& write : local.pending_writes) {
      if (!write->IsReady())
        track(write->proposal.deadline);
    }
    for (const auto* request : local.ready_sets)
      track(request->deadline);
    for (const auto* request : local.active_batch.requests)
      track(request->deadline);
  }
  for (const auto& epoch : changed)
    epoch->changed.notifyAll();
  return deadline;
}

absl::Status DataNodeImpl::AppendSetBatch(LocalPartition& local, SetBatch& batch,
                                          RaftGroup::State state, bool* done) {
  const auto& election = state.election.state();
  const auto hw = state.replication.HighWatermark();
  const int32_t epoch = election.election.epoch;
  if (!batch.encode_status.ok())
    return batch.encode_status;
  if (batch.encoded) {
    if (batch.base.epoch != state.log.End().epoch ||
        batch.base.end_offset != state.log.End().end_offset ||
        batch.generation != state.log.content_generation()) {
      batch.encoded.reset();
      for (auto* request : batch.requests)
        request->effect.reset();
      local.raft->WakeData();
      return absl::OkStatus();
    }
    *done = true;
    return CommitSetBatch(local, batch, state);
  }
  int64_t offset = state.log.End().end_offset;
  const auto timestamp =
      duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
  if (state.log.FlushPending() && batch.requests.front()->condition &&
      steady_clock::now() < batch.requests.front()->deadline)
    return absl::OkStatus();
  size_t records_size = 0;
  for (size_t i = 0; i < batch.requests.size(); ++i) {
    auto* request = batch.requests[i];

    // A rollover/flush barrier can retry preparation after routing or deadlines have changed.
    request->effect.reset();
    if (steady_clock::now() >= request->deadline) {
      request->status = absl::UnavailableError("-TRYAGAIN Data group is catching up");
      continue;
    }
    if (request->async && request->async->IsReady()) {
      request->status = request->async->status;
      continue;
    }
    auto route = Route(request->slot, &local);
    if (!route.ok()) {
      request->status = route.status();
      continue;
    }
    if (stopping_ || election.role != QuorumRole::kLeader || request->epoch != epoch || !hw) {
      request->status = ClusterDown();
      continue;
    }
    if (request->condition) {
      DCHECK_EQ(batch.requests.size(), 1u);
      if (local.effects.applied_offset() != offset || local.effects.applied_offset() < *hw)
        return absl::OkStatus();
      auto exists = owner_->Await([&] {
        JournalExecutor executor(service_);
        return ExistsForSet(executor, request->db, request->key);
      });
      if (!exists.ok())
        return exists.status();
      request->stored = *exists == (*request->condition == SetCondition::kIfExists);
    }
    auto action = request->stored ? SetEffect::Action::kReplace : SetEffect::Action::kNoop;
    SetEffectView effect{local.id,      request->db,
                         request->slot, route->owner.generation,
                         request->key,  request->stored ? request->value : string_view{},
                         action};
    auto encoded_size = SetEffectBatchSize(effect);
    if (!encoded_size.ok() || offset == INT64_MAX) {
      request->status = encoded_size.ok() ? absl::OutOfRangeError("Raft log offset exhausted")
                                          : encoded_size.status();
      continue;
    }

    // Drain only the contiguous prefix fitting this segment and the encoded-byte budget.
    // If even the first valid request does not fit, append it alone through the rollover path.
    if (batch.byte_budget != SIZE_MAX && records_size != 0 &&
        (*encoded_size > batch.byte_budget - min(batch.byte_budget, records_size) ||
         !state.log.FitsActiveSegment(records_size + *encoded_size, offset + 1))) {
      lock_guard lock(local.batch_mu);
      for (size_t j = batch.requests.size(); j > i; --j) {
        auto* deferred = batch.requests[j - 1];
        local.ready_sets.push_front(deferred);
        batch.value_bytes -= deferred->key.size() + deferred->value.size();
      }
      batch.requests.resize(i);
      break;
    }
    request->effect = std::move(effect);
    request->encoded_bytes = *encoded_size;
    request->end = {epoch, ++offset};
    records_size += *encoded_size;
  }
  if (state.log.FlushPending() && !state.log.FitsActiveSegment(records_size, offset))
    return absl::OkStatus();
  *done = true;
  if (records_size == 0)
    return absl::OkStatus();
  batch.base = state.log.End();
  batch.generation = state.log.content_generation();
  auto encode = [&batch, epoch, timestamp, records_size] {
    string records;
    records.reserve(records_size);
    for (auto* request : batch.requests) {
      if (request->effect) {
        auto s = AppendSetEffectBatch(request->end.end_offset - 1, epoch, timestamp,
                                      *request->effect, &records, &request->key_offset);
        if (!s.ok()) {
          batch.encode_status = s;
          return;
        }
      }
    }
    batch.encoded = make_shared<const string>(std::move(records));
  };
  if (&batch == &local.active_batch) {
    *done = false;
    batch.encoding = true;
    local.raft->Encode(std::move(encode), [&batch] { batch.encoding = false; });
    return absl::OkStatus();
  }
  encode();
  if (!batch.encode_status.ok())
    return batch.encode_status;
  return CommitSetBatch(local, batch, state);
}

absl::Status DataNodeImpl::CommitSetBatch(LocalPartition& local, SetBatch& batch,
                                          RaftGroup::State state) {
  const auto& election = state.election.state();
  const auto epoch = election.election.epoch;
  for (const auto* request : batch.requests) {
    if (!request->effect)
      continue;
    if (election.role != QuorumRole::kLeader || request->epoch != epoch || stopping_ ||
        steady_clock::now() >= request->deadline)
      return ClusterDown();
    auto route = Route(request->slot, &local);
    if (!route.ok())
      return route.status();
    if (route->owner.generation != request->effect->generation)
      return ClusterDown();
  }
  bool may_have_appended = false;
  auto buffer = batch.encoded;
  auto status = state.replication.Append(buffer, epoch, &may_have_appended);
  if (!status.ok()) {
    if (may_have_appended)
      return absl::UnavailableError("-TRYAGAIN Outcome of Raft write is unknown");
    return status;
  }
  for (auto* request : batch.requests) {
    if (request->effect)
      local.effects.RememberAppended(request->end, request->encoded_bytes, *request->effect, buffer,
                                     request->key_offset);
  }
  return absl::OkStatus();
}

absl::StatusOr<DataReplicationState> DataNodeImpl::ReplicationState() {
  auto local = partition_.load();
  auto image = image_.load();
  if (!local || !image || stopping_)
    return ClusterDown();
  DataReplicationState result{};
  const auto version = local->replica_version.load();
  result.await_change = [local, version](steady_clock::time_point deadline) {
    local->replica_changed.await_until([&] { return local->replica_version.load() != version; },
                                       deadline);
  };
  auto status = local->raft->Visit([&](RaftGroup::State s) {
    const auto& state = s.election.state();
    if (!state.election.leader_id ||
        (state.role != QuorumRole::kLeader && state.role != QuorumRole::kFollower))
      return ClusterDown();
    result.group_id = local->id;
    result.epoch = state.election.epoch;
    result.is_leader = state.role == QuorumRole::kLeader;
    result.connected_to_leader = state.role == QuorumRole::kFollower && state.fetched_from_leader;
    result.local_applied_offset = local->effects.applied_offset();
    auto now = duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
    for (const auto& v : s.election.voters()) {
      auto broker = image->brokers.find(v.key.id);
      auto id = image->directory.nodes().find(v.key.id);
      if (broker == image->brokers.end() || id == image->directory.nodes().end())
        return ClusterDown();
      auto ep = Endpoint(broker->second, "RESP");
      if (!ep)
        return ClusterDown();
      ClusterNodeInfo node{id->second, ep->host, ep->port};
      if (*state.election.leader_id == v.key.id)
        result.leader = std::move(node);
      else
        result.replicas.push_back(
            {std::move(node),
             result.is_leader ? local->progress.Applied(v.key, result.epoch, now, 2000) : nullopt});
    }
    return absl::OkStatus();
  });
  if (!status.ok())
    return status;
  return result;
}

absl::StatusOr<DataTopology> DataNodeImpl::Topology() {
  const auto image = image_.load();
  if (!image || !image->directory.HasCompleteCoverage())
    return ClusterDown();
  auto local = image->directory.nodes().find(identity_.node_id);
  if (local == image->directory.nodes().end())
    return ClusterDown();
  // A restarted observer can replay the previous incarnation's routes before registering.
  // Wait for the controller to accept this process before exposing that image to new clients.
  auto broker = image->brokers.find(identity_.node_id);
  if (broker == image->brokers.end() || broker->second.incarnation_id != incarnation_ ||
      broker->second.fenced)
    return ClusterDown();
  vector<ClusterShardInfo> shards;
  const auto& ranges = image->directory.ranges();
  size_t assigned_groups = 0;
  for (const auto& [id, group] : image->directory.groups()) {
    vector<SlotRange> slots;
    for (auto it = ranges.begin(); it != ranges.end(); ++it) {
      if (it->second.group_id != id)
        continue;
      auto after = next(it);
      slots.push_back(
          {it->first, static_cast<SlotId>(after == ranges.end() ? kMaxSlotNum : after->first - 1)});
    }
    if (slots.empty())
      continue;
    ++assigned_groups;
    if (!group.leadership || !image->directory.Route(slots.front().start, image->brokers))
      continue;
    ClusterShardInfo shard;
    shard.slot_ranges = SlotRanges(std::move(slots));
    for (auto member : group.members) {
      auto it = image->brokers.find(member);
      if (it == image->brokers.end())
        return ClusterDown();
      auto ep = Endpoint(it->second, "RESP");
      if (!ep)
        return ClusterDown();
      ClusterExtendedNodeInfo node{{image->directory.nodes().at(member), ep->host, ep->port},
                                   it->second.fenced ? NodeHealth::FAIL : NodeHealth::ONLINE};
      if (member == group.leadership->leader_id)
        shard.master = std::move(node);
      else
        shard.replicas.push_back(std::move(node));
    }
    shards.push_back(std::move(shard));
  }
  return DataTopology{ClusterShardInfos(std::move(shards)), local->second,
                      image->directory.nodes().size(), assigned_groups};
}

}  // namespace

absl::StatusOr<unique_ptr<DataNode>> DataNode::Open(const filesystem::path& dir,
                                                    const string& cluster_id, int32_t node_id,
                                                    const string& controllers, const string& listen,
                                                    uint16_t resp_port, Service* service,
                                                    util::ProactorPool* pool,
                                                    util::AcceptServer* acceptor) {
  if (absl::GetFlag(FLAGS_raft_data_snapshot_bytes) < 1 ||
      absl::GetFlag(FLAGS_raft_data_snapshot_interval_ms) < 0)
    return absl::InvalidArgumentError("Invalid data snapshot size or interval");
  auto identity = ReadRaftDirectory(dir);
  if (!identity.ok())
    return identity.status();
  if (identity->cluster_id != cluster_id || identity->node_id != node_id)
    return absl::FailedPreconditionError("Configured identity differs from meta.properties");
  auto endpoints = ParseVoterConnections(controllers);
  if (!endpoints.ok())
    return endpoints.status();
  auto address = ParseBootstrapServers(listen);
  if (!address.ok() || address->size() != 1 || address->front().host.empty() ||
      !address->front().port)
    return absl::InvalidArgumentError("Set raft_listen to the data voter's internal host:port");
  auto result = make_unique<DataNodeImpl>(dir, *identity, *endpoints, address->front(), resp_port,
                                          service, pool->GetNextProactor());
  if (auto s = result->Init(acceptor); !s.ok())
    return s;
  return result;
}

}  // namespace dfly::cluster::raft
