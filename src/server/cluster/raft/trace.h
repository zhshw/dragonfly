// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <array>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace dfly::cluster::raft::trace {

inline constexpr int kSchema = 3;
using Time = int64_t;

// Per-operation span attributes, not OpenTelemetry metric instruments. Values are computed once
// on the owning execution path and retained with the corresponding trace event.
enum class Attribute {
  kRpc,
  kReturnWait,
  kReceive,
  kElapsed,
  kPayloadBytes,
  kAppendedBytes,
  kAppendedRecords,
  kPartial,
  kGateRejections,
  kGateWait,
  kIoWait,
  kFsyncOverlap,
  kIoFsyncOverlap,
  kFsync,
  kFlush,
  kAdmissionWait,
  kRetiredTickets,
  kFullWaveRetired,
  kWalIdle,
  kCount,
};

struct Definition {
  std::string_view name;
  std::string_view unit;
  std::string_view boundary;
};
const std::array<Definition, size_t(Attribute::kCount)>& Definitions();

enum class Gate {
  kAllowed,
  kNotMemory,
  kAppendPending,
  kStorageError,
  kIncompleteSuffix,
  kInvalidBatch,
  kControlRecord,
  kEpochChange,
  kOffsetMismatch,
  kSegmentRollover,
  kUnmatchedResponse,
  kStructuralResponse,
};
std::string_view GateName(Gate gate);

// Durations are accumulated as integer nanoseconds and exported as exact decimal milliseconds.
// Counts remain integers. Missing is never zero.
struct Value {
  Attribute attribute;
  std::optional<int64_t> value;
  std::string state = "Observed";
};

struct Event {
  uint64_t id = 0;
  uint64_t request = 0;
  std::string name;
  Time start = 0;
  Time end = 0;
  std::string outcome = "Observed";
  std::map<std::string, std::string> attributes;
  std::vector<Value> values;
};

class FetchTrace;
class Batch;

// One capture per serial owner, never shared implicitly through thread-local state.
// The caller must drain requests/I/O before destroying it. Recording does no network or file I/O.
class Capture {
 public:
  using Clock = std::function<Time()>;
  Capture(std::string capture_id, std::string process, std::string group, int32_t node,
          size_t byte_limit = 16U << 20, Clock clock = MonotonicNow);
  ~Capture();
  static Time MonotonicNow();
  Time Now() const {
    return clock_();
  }
  const Clock& clock() const {
    return clock_;
  }
  uint64_t NextId() {
    return ++next_id_;
  }
  Event MakePoint(std::string_view name, uint64_t request = 0);
  Event MakeEvent(std::string_view name, Time start, Time end, uint64_t request = 0);
  std::map<std::string, std::string> Labels() const;

  // Configure before recording: data events keep raft.*, metadata events use metadata.*.
  void SetEventPrefix(std::string_view prefix);
  std::string EventName(std::string_view name) const;

  // Set on the serial owner before recording; an event retains its creation-time identity.
  void SetNodeRole(std::string_view role, int32_t epoch);
  void Emit(Event event);
  uint64_t BeginFsync(uint64_t flush, std::string_view role);
  void EndFsync(uint64_t id, int result);
  bool FsyncActive() const {
    return !io_.empty();
  }
  const std::map<uint64_t, Event>& active_io() const {
    return io_;
  }
  const std::vector<Event>& events() const {
    return events_;
  }
  size_t dropped() const {
    return dropped_;
  }
  const std::string& id() const {
    return capture_id_;
  }
  const std::string& stream_id() const {
    return stream_id_;
  }
  Time wall_offset_ns() const {
    return wall_offset_ns_;
  }
  bool drained() const {
    return requests_.empty() && io_.empty();
  }

 private:
  friend class FetchTrace;
  friend class Batch;
  bool Register(FetchTrace* request);
  void Unregister(FetchTrace* request);
  void Advance(Time now);
  Clock clock_;
  const std::string capture_id_, process_, group_;
  const int32_t node_;
  const size_t byte_limit_;
  std::string stream_id_;
  std::string node_role_, epoch_;
  std::string event_prefix_ = "raft.";
  Time wall_offset_ns_ = 0;
  size_t bytes_ = 0;
  size_t dropped_ = 0;
  uint64_t next_id_ = 0;
  std::vector<Event> events_;
  std::vector<FetchTrace*> requests_;
  std::map<uint64_t, Event> io_;
};

// A bounded handoff of timings from a synchronous operation on another executor. Only one
// execution owner may access it at a time; Publish must run on the capture's serial owner.
// IDs and capture storage are touched only at publication, never on the remote executor.
class Batch {
 public:
  explicit Batch(Capture& capture, std::map<std::string, std::string> labels = {});
  Time Now() const;
  void Record(std::string_view name, Time start, Time end,
              std::map<std::string, std::string> labels = {});
  void Publish();

 private:
  Capture& capture_;
  std::map<std::string, std::string> labels_;
  std::vector<Event> events_;
  size_t dropped_ = 0;
};

// Explicit context carried from RPC through the worker's gate, wait, receive and append.
class FetchTrace {
 public:
  FetchTrace(Capture& capture, int32_t peer, int32_t epoch, uint64_t sequence, int64_t offset);
  ~FetchTrace();
  FetchTrace(const FetchTrace&) = delete;
  FetchTrace& operator=(const FetchTrace&) = delete;
  void RpcStarted(Time now);
  void RpcReturned(Time now, size_t payload, bool success);
  void CheckGate(Gate gate, uint64_t flush);
  void BeginIoWait(Time now);
  void EndIoWait(Time now);
  void ReceiveStarted();
  void Appended(Time start, size_t bytes, int64_t first, int64_t end, bool success);
  void Finish(std::string_view outcome);
  Time Now() const {
    return capture_.Now();
  }
  uint64_t id() const {
    return record_.id;
  }

 private:
  friend class Capture;
  void Advance(Time now);
  void LinkIo(uint64_t io, uint64_t flush, bool dependency);
  Event MakeEvent(std::string_view name, Time start, Time end);
  void EmitEvent(std::string_view name, Time start, Time end,
                 std::string_view outcome = "Observed");
  Capture& capture_;
  Event record_;
  bool registered_ = false;
  bool finished_ = false;
  bool receiving_ = false;
  bool append_seen_ = false;
  std::optional<Time> rpc_start_, rpc_end_, receive_start_, io_start_, gate_start_;
  Time advanced_ = 0;
  size_t history_start_ = 0;
  uint64_t awaited_flush_ = 0;
  size_t payload_ = 0;
  int64_t gate_rejections_ = 0, gate_wait_ = 0, io_wait_ = 0;
  int64_t fsync_overlap_ = 0, io_fsync_overlap_ = 0;
};

}  // namespace dfly::cluster::raft::trace
