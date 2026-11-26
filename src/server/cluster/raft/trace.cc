// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/trace.h"

#include <openssl/rand.h>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <stdexcept>

namespace dfly::cluster::raft::trace {
using namespace std;

const array<Definition, size_t(Attribute::kCount)>& Definitions() {
  static constexpr array<Definition, size_t(Attribute::kCount)> definitions{{
      {"raft.fetch.rpc", "ms", "RPC start to transport return"},
      {"raft.fetch.return_wait", "ms", "Transport return to receive entry"},
      {"raft.fetch.receive", "ms", "Receive entry to processing completion"},
      {"raft.fetch", "ms", "RPC start to processing completion"},
      {"raft.fetch.payload_bytes", "bytes", "Decoded response records size"},
      {"raft.fetch.appended_bytes", "bytes", "Successfully written complete prefix"},
      {"raft.fetch.appended_records", "records", "Successfully advanced log offsets"},
      {"raft.fetch.partial", "boolean", "Successful appended bytes less than payload bytes"},
      {"raft.fetch.flush_gate.rejections", "count", "Actual rejected gate evaluations"},
      {"raft.fetch.await_flush", "ms", "First rejection to acceptance, union of episodes"},
      {"raft.await_io", "ms", "Actual I/O wait union inside return wait"},
      {"raft.fetch.fsync_overlap", "ms", "Group fsync active union inside return wait"},
      {"raft.fetch.io_fsync_overlap", "ms",
       "I/O wait and group fsync intersection inside return wait"},
      {"raft.folly.fsync", "ms", "Submission to completion callback"},
      {"raft.flush", "ms", "Background flush start to recovery publication"},
      {"raft.set.admission.wait", "ms", "First capacity rejection to actual admission"},
      {"raft.set.admission.retired_tickets", "count",
       "Tickets actually retired in this connection/group wait"},
      {"raft.set.admission.full_wave_retired", "boolean",
       "All tickets in the rejected wave retired"},
      {"raft.set.admission.wal_idle", "ms", "Admission minus latest WAL/first retirement"},
  }};
  return definitions;
}

string_view GateName(Gate gate) {
  constexpr array<string_view, 12> names{
      "Allowed",           "Not memory mode",  "Append pending",     "Storage error",
      "Incomplete suffix", "Invalid batch",    "Control record",     "Epoch change",
      "Offset mismatch",   "Segment rollover", "Unmatched response", "Structural response"};
  return names.at(size_t(gate));
}

Time Capture::MonotonicNow() {
  return chrono::duration_cast<chrono::nanoseconds>(chrono::steady_clock::now().time_since_epoch())
      .count();
}

Capture::Capture(string capture_id, string process, string group, int32_t node, size_t byte_limit,
                 Clock clock)
    : clock_(std::move(clock)),
      capture_id_(std::move(capture_id)),
      process_(std::move(process)),
      group_(std::move(group)),
      node_(node),
      byte_limit_(byte_limit) {
  array<unsigned char, 16> random;
  if (RAND_bytes(random.data(), random.size()) != 1)
    throw runtime_error("Cannot allocate native capture identity");
  constexpr string_view digits = "0123456789abcdef";
  for (auto byte : random) {
    stream_id_ += digits[byte >> 4];
    stream_id_ += digits[byte & 15];
  }
  wall_offset_ns_ =
      chrono::duration_cast<chrono::nanoseconds>(chrono::system_clock::now().time_since_epoch())
          .count() -
      Now();
}

Capture::~Capture() {
  assert(requests_.empty());
}

Event Capture::MakePoint(string_view name, uint64_t request) {
  const auto now = Now();
  return MakeEvent(name, now, now, request);
}

Event Capture::MakeEvent(string_view name, Time start, Time end, uint64_t request) {
  Event event;
  event.id = NextId();
  event.request = request;
  event.name = EventName(name);
  event.start = start;
  event.end = end;
  event.attributes = Labels();
  if (!node_role_.empty()) {
    event.attributes["node_role"] = node_role_;
    event.attributes["epoch"] = epoch_;
  }
  return event;
}

void Capture::SetNodeRole(string_view role, int32_t epoch) {
  node_role_ = role;
  epoch_ = to_string(epoch);
}

void Capture::SetEventPrefix(string_view prefix) {
  assert(events_.empty() && requests_.empty() && io_.empty());
  event_prefix_ = prefix;
}

string Capture::EventName(string_view name) const {
  if (name.starts_with("raft."))
    return event_prefix_ + string(name.substr(5));
  return string(name);
}

map<string, string> Capture::Labels() const {
  return {{"capture_id", capture_id_},
          {"process_instance", process_},
          {"group_id", group_},
          {"node_id", to_string(node_)}};
}

void Capture::Emit(Event event) {
  if (event.end < event.start) {
    ++dropped_;
    return;
  }
  size_t bytes = sizeof(event) + event.name.size() + event.outcome.size();
  for (const auto& [key, value] : event.attributes)
    bytes += sizeof(pair<string, string>) + 64 + key.size() + value.size();
  for (const auto& attribute : event.values)
    bytes += sizeof(attribute) + attribute.state.size();
  if (bytes > byte_limit_ - min(byte_limit_, bytes_)) {
    ++dropped_;
    return;
  }
  bytes_ += bytes;
  events_.push_back(std::move(event));
}

bool Capture::Register(FetchTrace* request) {
  if (requests_.size() >= 256) {
    ++dropped_;
    return false;
  }
  requests_.push_back(request);
  return true;
}

Batch::Batch(Capture& capture, map<string, string> labels)
    : capture_(capture), labels_(std::move(labels)) {
  if (!capture.node_role_.empty()) {
    labels_.try_emplace("node_role", capture.node_role_);
    labels_.try_emplace("epoch", capture.epoch_);
  }
}

Time Batch::Now() const {
  return capture_.Now();
}

void Batch::Record(string_view name, Time start, Time end, map<string, string> labels) {
  if (events_.size() >= 8192) {
    ++dropped_;
    return;
  }
  Event event;
  event.name = name;
  event.start = start;
  event.end = end;
  event.attributes = std::move(labels);
  events_.push_back(std::move(event));
}

void Batch::Publish() {
  for (auto& pending : events_) {
    auto event = capture_.MakeEvent(pending.name, pending.start, pending.end);
    for (const auto& [key, value] : labels_)
      event.attributes.insert_or_assign(key, value);
    for (auto& [key, value] : pending.attributes)
      event.attributes.insert_or_assign(key, std::move(value));
    capture_.Emit(std::move(event));
  }
  capture_.dropped_ += dropped_;
  events_.clear();
  dropped_ = 0;
}

void Capture::Unregister(FetchTrace* request) {
  erase(requests_, request);
}

void Capture::Advance(Time now) {
  for (auto* request : requests_)
    request->Advance(now);
}

uint64_t Capture::BeginFsync(uint64_t flush, string_view role) {
  const auto now = Now();
  Advance(now);
  if (io_.size() >= 256) {
    ++dropped_;
    return 0;
  }
  auto event = MakeEvent("raft.folly.fsync", now, now);
  event.attributes["flush_id"] = to_string(flush);
  event.attributes["role"] = role;
  const auto id = event.id;
  io_.emplace(id, std::move(event));
  for (auto* request : requests_)
    request->LinkIo(id, flush, false);
  return id;
}

void Capture::EndFsync(uint64_t id, int result) {
  const auto now = Now();
  Advance(now);
  auto it = io_.find(id);
  if (it == io_.end())
    return;
  auto event = std::move(it->second);
  io_.erase(it);
  event.end = now;
  event.outcome = result == 0 ? "Success" : "Error";
  event.attributes["result"] = to_string(result);
  event.values.push_back({Attribute::kFsync, now - event.start});
  Emit(std::move(event));
}

FetchTrace::FetchTrace(Capture& capture, int32_t peer, int32_t epoch, uint64_t sequence,
                       int64_t offset)
    : capture_(capture), record_(capture.MakePoint("raft.fetch")) {
  record_.request = record_.id;
  record_.attributes["peer_id"] = to_string(peer);
  record_.attributes["node_role"] = "Follower";
  record_.attributes["epoch"] = to_string(epoch);
  record_.attributes["sequence"] = to_string(sequence);
  record_.attributes["requested_offset"] = to_string(offset);
  registered_ = capture_.Register(this);
  history_start_ = capture_.events().size();
}

FetchTrace::~FetchTrace() {
  if (!finished_)
    Finish("Unfinished");
}

Event FetchTrace::MakeEvent(string_view name, Time start, Time end) {
  auto event = capture_.MakeEvent(name, start, end, id());
  event.attributes["node_role"] = record_.attributes.at("node_role");
  event.attributes["epoch"] = record_.attributes.at("epoch");
  return event;
}

void FetchTrace::EmitEvent(string_view name, Time start, Time end, string_view outcome) {
  auto event = MakeEvent(name, start, end);
  event.outcome = outcome;
  capture_.Emit(std::move(event));
}

void FetchTrace::RpcStarted(Time now) {
  rpc_start_ = now;
  record_.start = now;
  auto report = MakeEvent("raft.fetch.report", now, now);
  report.attributes["reported_offset"] = record_.attributes["requested_offset"];
  capture_.Emit(std::move(report));
}

void FetchTrace::RpcReturned(Time now, size_t payload, bool success) {
  rpc_end_ = now;
  advanced_ = capture_.Now();
  payload_ = payload;
  record_.attributes["payload_bytes"] = to_string(payload);
  record_.values.push_back({Attribute::kPayloadBytes, int64_t(payload)});
  if (rpc_start_) {
    record_.values.push_back({Attribute::kRpc, now - *rpc_start_});
    EmitEvent("raft.fetch.rpc", *rpc_start_, now, success ? "Success" : "Error");
  }
  if (!success) {
    Finish("Error");
    return;
  }
  // Transport completion crosses owners. Replay only the explicitly observed group I/O edges
  // between that original timestamp and delivery of this message, then accumulate on transitions.
  vector<pair<Time, Time>> intervals;
  vector<pair<uint64_t, uint64_t>> links;
  auto intersect = [&](const Event& event, Time end) {
    const auto begin = max(now, event.start);
    end = min(advanced_, end);
    if (begin < end) {
      intervals.emplace_back(begin, end);
      links.emplace_back(event.id, stoull(event.attributes.at("flush_id")));
    }
  };
  const auto& history = capture_.events();
  for (size_t i = history_start_; i < history.size(); ++i) {
    if (history[i].name == capture_.EventName("raft.folly.fsync"))
      intersect(history[i], history[i].end);
  }
  for (const auto& [io, event] : capture_.active_io()) {
    intersect(event, advanced_);
    links.emplace_back(io, stoull(event.attributes.at("flush_id")));
  }
  sort(intervals.begin(), intervals.end());
  Time end = now;
  for (const auto& interval : intervals) {
    fsync_overlap_ += max<Time>(0, interval.second - max(end, interval.first));
    end = max(end, interval.second);
  }
  sort(links.begin(), links.end());
  links.erase(unique(links.begin(), links.end()), links.end());
  for (const auto& [io, flush] : links)
    LinkIo(io, flush, false);
}

void FetchTrace::Advance(Time now) {
  if (!rpc_end_ || receiving_ || finished_ || now < advanced_)
    return;
  const auto elapsed = now - advanced_;
  if (io_start_)
    io_wait_ += elapsed;
  if (capture_.FsyncActive()) {
    fsync_overlap_ += elapsed;
    if (io_start_)
      io_fsync_overlap_ += elapsed;
  }
  advanced_ = now;
}

void FetchTrace::LinkIo(uint64_t io, uint64_t flush, bool dependency) {
  if (!rpc_end_ || receiving_ || finished_)
    return;
  dependency = dependency || (gate_start_ && awaited_flush_ == flush);
  const auto now = capture_.Now();
  auto event =
      MakeEvent(dependency ? "raft.fetch.io_dependency" : "raft.fetch.io_overlap", now, now);
  event.attributes["io_id"] = to_string(io);
  event.attributes["flush_id"] = to_string(flush);
  capture_.Emit(std::move(event));
}

void FetchTrace::CheckGate(Gate gate, uint64_t flush) {
  const auto now = capture_.Now();
  if (gate == Gate::kAllowed && !gate_start_)
    return;
  auto event = MakeEvent(
      gate == Gate::kAllowed ? "raft.fetch.flush_gate.ready" : "raft.fetch.flush_gate.blocked", now,
      now);
  event.attributes["reason"] = GateName(gate);
  event.attributes["flush_id"] = to_string(flush);
  event.outcome = gate == Gate::kAllowed ? "Accepted" : "Rejected";
  capture_.Emit(std::move(event));
  if (gate != Gate::kAllowed) {
    ++gate_rejections_;
    awaited_flush_ = flush;
    if (!gate_start_)
      gate_start_ = now;
    for (const auto& [io, active] : capture_.active_io()) {
      if (active.attributes.at("flush_id") == to_string(flush))
        LinkIo(io, flush, true);
    }
  } else if (gate_start_) {
    gate_wait_ += now - *gate_start_;
    EmitEvent("raft.fetch.await_flush", *gate_start_, now, "Accepted");
    gate_start_.reset();
  }
}

void FetchTrace::BeginIoWait(Time now) {
  Advance(now);
  if (!io_start_)
    io_start_ = now;
}

void FetchTrace::EndIoWait(Time now) {
  Advance(now);
  if (io_start_) {
    EmitEvent("raft.await_io", *io_start_, now);
    io_start_.reset();
  }
}

void FetchTrace::ReceiveStarted() {
  const auto now = capture_.Now();
  Advance(now);
  receive_start_ = now;
  receiving_ = true;
  if (rpc_end_)
    EmitEvent("raft.fetch.return_wait", *rpc_end_, now);
}

void FetchTrace::Appended(Time start, size_t bytes, int64_t first, int64_t end, bool success) {
  const auto now = capture_.Now();
  auto event = MakeEvent("raft.log.append", start, now);
  event.outcome = success ? "Success" : "Error";
  if (success) {
    append_seen_ = true;
    record_.attributes["first_offset"] = to_string(first);
    record_.attributes["last_offset"] = to_string(end - 1);
    record_.values.push_back({Attribute::kAppendedBytes, int64_t(bytes)});
    record_.values.push_back({Attribute::kAppendedRecords, end - first});
    record_.values.push_back({Attribute::kPartial, bytes < payload_});
  }
  capture_.Emit(std::move(event));
}

void FetchTrace::Finish(string_view outcome) {
  if (finished_)
    return;
  const auto now = capture_.Now();
  Advance(now);
  record_.end = now;
  record_.outcome = outcome;
  const bool complete = registered_ && rpc_start_ && rpc_end_ && receive_start_ && !gate_start_ &&
                        !io_start_ && outcome != "Unfinished" && outcome != "Cancelled";
  if (receive_start_)
    EmitEvent("raft.fetch.receive", *receive_start_, now, outcome);
  auto value = [&](Attribute attribute, int64_t count, bool valid) {
    record_.values.push_back(
        {attribute, valid ? optional<int64_t>(count) : nullopt, valid ? "Observed" : "Incomplete"});
  };
  value(Attribute::kReturnWait, receive_start_.value_or(now) - rpc_end_.value_or(now), complete);
  value(Attribute::kReceive, now - receive_start_.value_or(now), complete);
  value(Attribute::kElapsed, now - rpc_start_.value_or(now), complete);
  value(Attribute::kGateRejections, gate_rejections_, complete);
  value(Attribute::kGateWait, gate_wait_, complete);
  value(Attribute::kIoWait, io_wait_, complete);
  value(Attribute::kFsyncOverlap, fsync_overlap_, complete && capture_.dropped() == 0);
  value(Attribute::kIoFsyncOverlap, io_fsync_overlap_, complete && capture_.dropped() == 0);
  if (!append_seen_) {
    const bool empty = complete && outcome == "Empty";
    value(Attribute::kAppendedBytes, 0, empty);
    value(Attribute::kAppendedRecords, 0, empty);
    value(Attribute::kPartial, 0, empty);
  }
  if (gate_start_)
    EmitEvent("raft.fetch.await_flush", *gate_start_, now, "Unfinished");
  if (io_start_)
    EmitEvent("raft.await_io", *io_start_, now, "Unfinished");
  finished_ = true;
  capture_.Unregister(this);
  capture_.Emit(std::move(record_));
}

}  // namespace dfly::cluster::raft::trace
