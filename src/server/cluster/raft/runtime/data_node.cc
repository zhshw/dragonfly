// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/runtime/data_node.h"

#include <absl/random/random.h>
#include <absl/strings/str_cat.h>

#include <atomic>
#include <chrono>

#include "base/logging.h"
#include "server/cluster/raft/data/apply_progress.h"
#include "server/cluster/raft/data/kv_rdb.h"
#include "server/cluster/raft/data/set_effect.h"
#include "server/cluster/raft/data/set_executor.h"
#include "server/cluster/raft/metadata/metadata_image.h"
#include "server/cluster/raft/quorum/fetch_replication.h"
#include "server/cluster/raft/runtime/group.h"
#include "server/cluster/raft/runtime/slot_migration_driver.h"
#include "server/cluster/raft/transport/peer_connection.h"
#include "server/cluster/raft/transport/raft_listener.h"
#include "server/journal/executor.h"
#include "util/accept_server.h"
#include "util/fibers/fiberqueue_threadpool.h"
#include "util/fibers/pool.h"

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

struct LocalPartition {
  explicit LocalPartition(int32_t group) : id(group), effects(group) {
  }
  int32_t id;
  CommittedEffects effects;
  DataApplyProgress progress;
  unique_ptr<RaftGroup> raft;
  unique_ptr<SlotMigrationDriver> migrations;
  RpcHandlers handlers;
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

    // The RESP and internal ports can open before this broker receives its slot assignment.
    RpcHandlers handlers;
    handlers.vote = Forward(&RpcHandlers::vote);
    handlers.add_voter = Forward(&RpcHandlers::add_voter);
    handlers.remove_voter = Forward(&RpcHandlers::remove_voter);
    handlers.update_voter = Forward(&RpcHandlers::update_voter);
    handlers.begin_epoch = Forward(&RpcHandlers::begin_epoch);
    handlers.end_epoch = Forward(&RpcHandlers::end_epoch);
    handlers.fetch = Forward(&RpcHandlers::fetch);
    handlers.fetch_slot_image = Forward(&RpcHandlers::fetch_slot_image);
    handlers.describe_quorum = Forward(&RpcHandlers::describe_quorum);
    auto listener = make_unique<RaftListener>(kMaxRaftFrame, std::move(handlers));
    if (auto ec = acceptor->AddListener(listen_.host.c_str(), listen_.port, listener.get()); ec)
      return absl::ErrnoToStatus(ec.value(), "Binding data quorum listener");
    listener.release();
    return absl::OkStatus();
  }

  void Start() override {
    observer_->Start(owner_);
    control_fiber_ = owner_->LaunchFiber([this] { BrokerLoop(); });
    migration_fiber_ = owner_->LaunchFiber([this] { MigrationLoop(); });
  }

  void Stop() override {
    stopping_ = true;
    observer_->RequestStop();
    if (control_fiber_.IsJoinable())
      control_fiber_.Join();
    if (migration_fiber_.IsJoinable())
      migration_fiber_.Join();
    if (auto group = partition_.load())
      group->raft->Join();
    observer_->Join();
  }
  absl::Status Set(DbIndex db, string_view key, string_view value,
                   DataWritePosition* position) override;
  absl::StatusOr<bool> SetIf(DbIndex db, string_view key, string_view value, SetCondition condition,
                             DataWritePosition* position) override;
  absl::StatusOr<optional<string>> Get(DbIndex db, string_view key) override;
  absl::StatusOr<DataTopology> Topology() override;
  absl::StatusOr<DataReplicationState> ReplicationState() override;

 private:
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
  absl::Status OpenPartition(const MetadataImage& image, const Broker& local);
  void BrokerLoop();
  void MigrationLoop();
  void RegisterOrHeartbeat(PeerConnection& peer, const MetadataImage& image, const Broker& broker);
  absl::StatusOr<RedisRoute> Route(SlotId slot, const LocalPartition* partition) const;

  filesystem::path dir_;
  RaftDirectoryIdentity identity_;
  VoterConnections controllers_;
  QuorumEndpoint listen_;
  uint16_t resp_port_;
  Service* service_;
  fb2::ProactorBase* owner_;
  fb2::FiberQueueThreadPool opener_{1};
  Uuid incarnation_{};
  MetadataLoader loader_;
  unique_ptr<RaftGroup> observer_;
  atomic<shared_ptr<const MetadataImage>> image_;
  atomic<shared_ptr<LocalPartition>> partition_;
  int64_t registered_epoch_ = -1;
  atomic_bool stopping_{false};
  fb2::Fiber control_fiber_;
  fb2::Fiber migration_fiber_;

  // Both loops run on owner_, each with a separate RPC connection.
  int32_t controller_leader_ = -1;
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
    auto local = make_shared<LocalPartition>(id);
    auto apply = [this, replica = local.get()](const PartitionLog& log, int64_t hw) {
      auto apply_set = [&](const SetEffect& effect) {
        // Re-enter the service on a proactor. Client writes wait outside shard transactions.
        return owner_->Await([&] {
          JournalExecutor executor(service_);
          return ExecuteSetEffect(executor, effect);
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
      return replica->effects.ApplyTo(log, hw, apply_set, apply_slot);
    };
    auto opened = opener_.Await([&] {
      return RaftGroup::Open(dir_, identity_, RaftGroup::Kind::kData, id, voters, false, apply);
    });
    if (!opened.ok())
      return opened.status();
    local->raft = *std::move(opened);
    if (auto s = local->raft->Visit([replica = local.get()](RaftGroup::State state) {
          state.replication.SetDataApplyHooks(
              [replica](auto* p) {
                DataApplyProgress::Advertise(replica->effects.applied_offset(), p);
              },
              [replica](ReplicaKey key, const auto& p, int64_t offset, int64_t now) {
                replica->progress.Observe(key, p, offset, now);
              });
          return absl::OkStatus();
        });
        !s.ok())
      return s;
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
  int32_t connected = -1;
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
        int32_t leader = -1;
        auto s = observer_->Visit([&](RaftGroup::State state) {
          leader = state.election.state().election.leader_id.value_or(-1);
          return absl::OkStatus();
        });
        controller_leader_ = s.ok() && controllers_.contains(leader) ? leader : -1;
        if (controller_leader_ >= 0) {
          if (connected != leader) {
            if (peer)
              peer->Close();
            peer = make_unique<PeerConnection>(controllers_.at(leader), 2000, kMaxRaftFrame);
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
  int32_t connected = -1;
  unique_ptr<PeerConnection> peer;
  while (!stopping_) {
    auto local = partition_.load();
    const auto leader = controller_leader_;
    if (local && leader >= 0) {
      if (connected != leader) {
        if (peer)
          peer->Close();
        connected = leader;
        peer = make_unique<PeerConnection>(controllers_.at(connected), 2000, kMaxRaftFrame);
      }
      local->migrations->Tick(*peer);
    }
    util::ThisFiber::SleepFor(milliseconds(25));
  }
  if (peer)
    peer->Close();
}

absl::StatusOr<RedisRoute> DataNodeImpl::Route(SlotId slot, const LocalPartition* partition) const {
  auto image = image_.load();
  if (stopping_ || !image)
    return ClusterDown();
  auto route = image->directory.Route(slot, image->brokers);
  if (!route)
    return ClusterDown();
  auto local = image->directory.nodes().find(identity_.node_id);
  if (local == image->directory.nodes().end())
    return ClusterDown();
  if (route->node_id != local->second)
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

absl::StatusOr<optional<string>> DataNodeImpl::Get(DbIndex db, string_view key) {
  auto slot = KeySlot(key);
  auto local = partition_.load();
  if (!local)
    return Route(slot, nullptr).status();
  optional<string> value;
  auto status = local->raft->Visit([&](RaftGroup::State state) {
    auto route = Route(slot, local.get());
    if (!route.ok())
      return route.status();
    auto hw = state.replication.HighWatermark();
    if (state.election.state().role != QuorumRole::kLeader || !hw ||
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

  optional<LogPosition> ticket;
  bool stored = true;
  auto deadline = steady_clock::now() + seconds(5);
  while (!stopping_ && steady_clock::now() < deadline) {
    bool applied = false;
    auto status = local->raft->Visit([&](RaftGroup::State state) {
      int32_t epoch = state.election.state().election.epoch;
      if (ticket) {
        if (state.election.state().role != QuorumRole::kLeader || epoch != ticket->epoch)
          return ClusterDown();
        applied = local->effects.applied_offset() >= ticket->end_offset;
        return absl::OkStatus();
      }
      auto route = Route(slot, local.get());
      if (!route.ok())
        return route.status();
      auto hw = state.replication.HighWatermark();
      if (local->id != route->owner.group_id ||
          state.election.state().role != QuorumRole::kLeader || !hw ||
          local->effects.applied_offset() < *hw)
        return ClusterDown();

      // A predicate must see every earlier proposal. Yield outside the group worker until
      // those proposals have committed and applied, then recheck leadership and routing.
      int64_t offset = state.log.End().end_offset;
      if (condition) {
        if (local->effects.applied_offset() != offset)
          return absl::OkStatus();
        auto exists = owner_->Await([&] {
          JournalExecutor executor(service_);
          return ExistsForSet(executor, db, key);
        });
        if (!exists.ok())
          return exists.status();
        stored = *exists == (*condition == SetCondition::kIfExists);
      }
      auto action = stored ? SetEffect::Action::kReplace : SetEffect::Action::kNoop;
      SetEffect effect{local->id,   db,
                       slot,        route->owner.generation,
                       string(key), stored ? string(value) : string{},
                       action};
      auto timestamp = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
      auto batch = MakeSetEffectBatch(offset, epoch, timestamp, effect);
      if (!batch.ok())
        return batch.status();
      auto appended = state.replication.Append(*batch, epoch);
      if (appended.ok()) {
        ticket = LogPosition{epoch, offset + 1};
        if (stored)
          *position = {local->id, epoch, offset + 1};
      }
      return appended;
    });
    if (!status.ok()) {
      if (ticket)
        break;
      return status;
    }
    if (applied)
      return stored;
    util::ThisFiber::SleepFor(milliseconds(5));
  }
  if (!ticket)
    return absl::UnavailableError("-TRYAGAIN Data group is catching up");
  return absl::UnavailableError("-TRYAGAIN Outcome of Raft write is unknown");
}

absl::StatusOr<DataReplicationState> DataNodeImpl::ReplicationState() {
  auto local = partition_.load();
  auto image = image_.load();
  if (!local || !image || stopping_)
    return ClusterDown();
  DataReplicationState result{};
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
