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
#include <map>
#include <string>
#include <vector>

#ifdef RAFT_IO_FAULTS
#include "io_faults.h"
#endif

namespace raft_io_trace {

// Test-only, buffered timestamps: no trace-file writes occur in the measured I/O path.
struct Trace {
  struct Event {
    int64_t ns;
    const char* stage;
    int fd;
    int32_t epoch;
    int64_t end;
    size_t bytes;
    size_t batches;
  };
  const char* directory = std::getenv("RAFT_IO_TRACE_DIR");
  std::vector<Event> events;
  std::map<int, std::string> files;

  Trace() {
    if (directory)
      events.reserve(65536);
  }
  ~Trace() {
    if (!directory || events.empty())
      return;
    auto path = std::string(directory) + "/" + std::to_string(getpid()) + "-" +
                std::to_string(syscall(SYS_gettid)) + ".csv";
    auto* out = std::fopen(path.c_str(), "w");
    if (!out)
      std::abort();
    std::fputs("ns,stage,fd,epoch,end,bytes,batches,file\n", out);
    for (const auto& e : events)
      std::fprintf(out, "%ld,%s,%d,%d,%ld,%zu,%zu,%s\n", e.ns, e.stage, e.fd, e.epoch, e.end,
                   e.bytes, e.batches, files[e.fd].c_str());
    std::fclose(out);
  }
};

inline void Record(const char* stage, int fd, int32_t epoch, int64_t end, size_t bytes = 0,
                   size_t batches = 0) {
  static thread_local Trace trace;
  if (!trace.directory)
    return;
  if (fd >= 0 && !trace.files.contains(fd)) {
    char path[4096];
    auto link = "/proc/self/fd/" + std::to_string(fd);
    auto size = readlink(link.c_str(), path, sizeof(path));
    trace.files[fd] = size < 0 ? "unknown" : std::string(path, size);
  }
#ifdef RAFT_IO_FAULTS
  raft_io_faults::Observe(stage, trace.files[fd], end);
#endif
  using namespace std::chrono;
  trace.events.push_back(
      {duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count(), stage, fd, epoch,
       end, bytes, batches});
}

}  // namespace raft_io_trace
