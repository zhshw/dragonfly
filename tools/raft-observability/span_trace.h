// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <sys/syscall.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <unordered_map>
#include <vector>

namespace dfly_observe {

inline int64_t Now() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

// Only the separate diagnostic binary includes this header. Threads buffer bounded spans;
// shutdown writes CSV, and the offline exporter clips to the client's measurement window.
struct Buffer {
  struct Event {
    int64_t start, end;
    const char* stage;
    int64_t arg0, arg1;
  };
  const char* directory = std::getenv("RAFT_OBSERVE_DIR");
  std::vector<Event> events;
  size_t dropped = 0;

  Buffer() {
    if (directory)
      events.reserve(65536);
  }
  ~Buffer() {
    if (!directory || events.empty())
      return;
    auto path = std::string(directory) + "/" + std::to_string(getpid()) + "-" +
                std::to_string(syscall(SYS_gettid)) + ".csv";
    auto* out = std::fopen(path.c_str(), "w");
    if (!out)
      std::abort();
    std::fprintf(out, "# dropped=%zu\nstart_ns,end_ns,stage,arg0,arg1\n", dropped);
    for (const auto& e : events)
      std::fprintf(out, "%ld,%ld,%s,%ld,%ld\n", e.start, e.end, e.stage, e.arg0, e.arg1);
    if (std::fclose(out))
      std::abort();
  }
};

inline void Record(const char* stage, int64_t start, int64_t arg0 = 0, int64_t arg1 = 0) {
  const auto end = Now();
  static thread_local Buffer buffer;
  if (!buffer.directory)
    return;
  if (buffer.events.size() == 262144) {
    ++buffer.dropped;
    return;
  }
  buffer.events.push_back({start, end, stage, arg0, arg1});
}

// A declined parsed SET stays on the connection's proactor until retried or released.
inline auto& AdmissionWaits() {
  static thread_local std::unordered_map<const void*, int64_t> waits;
  return waits;
}

inline void RecordAdmissionRejection(const void* command, const char* stage, int64_t bytes,
                                     int64_t occupied) {
  const auto since = Now();
  Record(stage, since, bytes, occupied);
  if (command)
    AdmissionWaits().try_emplace(command, since);
}

inline void FinishAdmissionWait(const void* command, int64_t bytes) {
  if (!command)
    return;
  auto& waits = AdmissionWaits();
  if (auto it = waits.find(command); it != waits.end()) {
    Record("raft.set.admission.wait", it->second, bytes);
    waits.erase(it);
  }
}

class Scope {
 public:
  explicit Scope(const char* stage, int64_t arg0 = 0, int64_t arg1 = 0)
      : stage_(stage), start_(Now()), arg0_(arg0), arg1_(arg1) {
  }
  ~Scope() {
    Finish();
  }
  void Finish() {
    if (stage_)
      Record(stage_, start_, arg0_, arg1_);
    stage_ = nullptr;
  }
  Scope(const Scope&) = delete;
  Scope& operator=(const Scope&) = delete;

 private:
  const char* stage_;
  int64_t start_, arg0_, arg1_;
};

}  // namespace dfly_observe
