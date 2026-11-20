// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/metadata/broker_control.h"

#include <absl/base/internal/endian.h>

#include <algorithm>
#include <bit>
#include <set>

namespace dfly::cluster::raft {
namespace {
using namespace std;
using namespace protocol;

bool ReservedDirectory(const Uuid& dir) {
  return absl::big_endian::Load64(dir.data()) == 0 &&
         bit_cast<int64_t>(absl::big_endian::Load64(dir.data() + 8)) < 100;
}

int16_t RegisterVersion(int16_t metadata_version) {
  if (metadata_version >= 30)
    return 4;
  if (metadata_version >= 17)
    return 3;
  return metadata_version >= 8 ? 2 : 1;
}

int16_t ValidateRegistration(const MetadataImage& image, const BrokerRegistrationRequest& req,
                             const RegisterBrokerRecord& existing, int16_t mv) {
  if (req.is_migrating_zk_broker)
    return 102;
  if (req.incarnation_id == Uuid{})
    return 119;
  if (mv >= 17) {
    set<Uuid> dirs(req.log_dirs.begin(), req.log_dirs.end());
    if (dirs.empty() || dirs.size() != req.log_dirs.size() ||
        any_of(dirs.begin(), dirs.end(), ReservedDirectory))
      return 119;
    for (const auto& [id, broker] : image.brokers) {
      if (id != req.broker_id && any_of(broker.log_dirs.begin(), broker.log_dirs.end(),
                                        [&](const auto& dir) { return dirs.contains(dir); }))
        return 119;
    }
  }

  // These are the stable endpoints advertised to existing Redis clients and the
  // controller's source of truth when it polls a data quorum for its elected leader.
  for (const auto& bound : existing.end_points) {
    if (bound.name == "RESP" || bound.name == "RAFT") {
      if (none_of(req.listeners.begin(), req.listeners.end(), [&](const auto& ep) {
            return ep.name == bound.name && ep.host == bound.host && ep.port == bound.port &&
                   ep.security_protocol == bound.security_protocol;
          }))
        return 119;
    }
  }
  set<string_view> seen;
  for (const auto& ep : req.listeners) {
    if (ep.name.empty() || ep.host.empty() || ep.port == 0 || !seen.insert(ep.name).second)
      return 119;
  }
  seen.clear();
  for (const auto& feature : req.features) {
    auto it = image.features.find(feature.name);
    int16_t level = it == image.features.end() ? 0 : it->second;
    if (!seen.insert(feature.name).second || feature.min_supported_version > level ||
        feature.max_supported_version < level)
      return 35;
  }
  if (!seen.contains("metadata.version"))
    return 119;
  for (const auto& [name, level] : image.features) {
    if (level > 0 && !seen.contains(name))
      return 35;
  }
  return 0;
}

BrokerRegistrationChangeRecord Change(const RegisterBrokerRecord& broker) {
  BrokerRegistrationChangeRecord change;
  change.broker_id = broker.broker_id;
  change.broker_epoch = broker.broker_epoch;
  return change;
}
}  // namespace

void BrokerControl::Activate(const MetadataImage& image, int64_t now) {
  sessions_.clear();
  for (const auto& [id, broker] : image.brokers) {
    if (broker.incarnation_id != Uuid{} && !broker.fenced)
      sessions_.emplace(id, Session{broker.broker_epoch, now, -1});
  }
}

vector<MetadataRecord> BrokerControl::Register(const MetadataImage& image, string_view cluster_id,
                                               int64_t next_offset, int64_t now,
                                               const BrokerRegistrationRequest& req,
                                               BrokerRegistrationResponse* res) const {
  *res = {};
  if (req.cluster_id != cluster_id) {
    res->error_code = 104;
    return {};
  }
  auto existing = image.brokers.find(req.broker_id);
  auto mv = image.features.find("metadata.version");
  if (existing == image.brokers.end() || !image.directory.nodes().contains(req.broker_id) ||
      mv == image.features.end()) {
    res->error_code = 102;
    return {};
  }
  const auto& old = existing->second;
  bool same = req.incarnation_id == old.incarnation_id;
  auto session = sessions_.find(req.broker_id);
  if (!same && session != sessions_.end() && session->second.epoch == old.broker_epoch &&
      now - session->second.last_contact < timeout_ms_) {
    res->error_code = 101;
    return {};
  }
  res->error_code = ValidateRegistration(image, req, old, mv->second);
  if (res->error_code)
    return {};
  RegisterBrokerRecord broker;
  broker.broker_id = req.broker_id;
  broker.incarnation_id = req.incarnation_id;
  broker.broker_epoch = same ? old.broker_epoch : next_offset;
  broker.rack = req.rack;
  broker.fenced = !same || old.fenced;
  broker.in_controlled_shutdown = same && old.in_controlled_shutdown;
  if (mv->second >= 17)
    broker.log_dirs = req.log_dirs;
  for (const auto& ep : req.listeners)
    broker.end_points.push_back({ep.name, ep.host, ep.port, ep.security_protocol, {}});
  for (const auto& f : req.features)
    broker.features.push_back({f.name, f.min_supported_version, f.max_supported_version, {}});
  res->broker_epoch = broker.broker_epoch;
  return {{std::move(broker), RegisterVersion(mv->second)}};
}

vector<MetadataRecord> BrokerControl::Heartbeat(const MetadataImage& image, int64_t now,
                                                const BrokerHeartbeatRequest& req,
                                                BrokerHeartbeatResponse* res) {
  *res = {};
  auto it = image.brokers.find(req.broker_id);
  auto registration = image.registration_offsets.find(req.broker_id);
  if (it == image.brokers.end() || it->second.broker_epoch != req.broker_epoch ||
      it->second.incarnation_id == Uuid{} || registration == image.registration_offsets.end()) {
    res->error_code = 77;
    return {};
  }
  const auto& broker = it->second;
  sessions_[req.broker_id] = {req.broker_epoch, now, req.current_metadata_offset};
  res->is_caught_up = req.current_metadata_offset >= registration->second;
  res->is_fenced = broker.fenced;
  auto change = Change(broker);
  if (broker.fenced) {
    res->should_shut_down = req.want_shut_down;
    if (!req.want_fence && !req.want_shut_down && res->is_caught_up)
      res->is_fenced = false;
  } else {
    bool has_leadership = any_of(image.directory.groups().begin(), image.directory.groups().end(),
                                 [&](const auto& entry) {
                                   auto& lead = entry.second.leadership;
                                   return lead && lead->leader_id == req.broker_id &&
                                          lead->reporter_epoch == req.broker_epoch;
                                 });
    if (broker.in_controlled_shutdown) {
      auto offset = image.shutdown_offsets.find(req.broker_id);
      bool caught = true;
      for (const auto& [id, other] : image.brokers) {
        if (other.fenced || other.in_controlled_shutdown)
          continue;
        auto s = sessions_.find(id);
        if (s == sessions_.end() || s->second.epoch != other.broker_epoch ||
            offset == image.shutdown_offsets.end() || s->second.metadata_offset < offset->second)
          caught = false;
      }
      res->should_shut_down = !has_leadership && caught;
      res->is_fenced = res->should_shut_down;
    } else if (req.want_fence) {
      res->is_fenced = true;
      res->should_shut_down = req.want_shut_down;
    } else if (req.want_shut_down) {
      if (has_leadership)
        change.in_controlled_shutdown = 1;
      else
        res->should_shut_down = res->is_fenced = true;
    }
  }
  if (res->is_fenced != broker.fenced)
    change.fenced = res->is_fenced ? 1 : -1;

  vector<MetadataRecord> result;
  if (change.fenced || change.in_controlled_shutdown)
    result.push_back({change, static_cast<int16_t>(change.in_controlled_shutdown ? 1 : 0)});
  auto level = image.features.at("metadata.version");
  if (level >= 17 && !req.offline_log_dirs.empty()) {
    auto dirs = broker.log_dirs;
    erase_if(dirs, [&](const auto& dir) {
      return find(req.offline_log_dirs.begin(), req.offline_log_dirs.end(), dir) !=
             req.offline_log_dirs.end();
    });
    if (dirs != broker.log_dirs) {
      auto offline = Change(broker);
      offline.log_dirs = std::move(dirs);
      result.push_back({std::move(offline), 2});
    }
  }
  if (level >= 30 && req.cordoned_log_dirs && req.cordoned_log_dirs != broker.cordoned_log_dirs) {
    auto cordoned = Change(broker);
    cordoned.cordoned_log_dirs = req.cordoned_log_dirs;
    result.push_back({std::move(cordoned), 3});
  }
  return result;
}

optional<MetadataRecord> BrokerControl::Expire(const MetadataImage& image, int64_t now) const {
  for (const auto& [id, session] : sessions_) {
    auto it = image.brokers.find(id);
    if (now - session.last_contact >= timeout_ms_ && it != image.brokers.end() &&
        it->second.broker_epoch == session.epoch && !it->second.fenced) {
      auto change = Change(it->second);
      change.fenced = 1;
      return MetadataRecord{std::move(change)};
    }
  }
  return nullopt;
}

}  // namespace dfly::cluster::raft
