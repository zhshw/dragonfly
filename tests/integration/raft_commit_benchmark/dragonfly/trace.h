// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <array>
#include <chrono>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>

namespace commit_benchmark {

constexpr size_t kWarmup = 32;
constexpr size_t kTotal = kWarmup + 256;
constexpr size_t kPayload = 256 << 10;

struct Row {
  int32_t leader = -1;
  int32_t epoch = -1;
  int64_t end = 0;
  int64_t wire_bytes = 0;
  int64_t submit = 0;
  int64_t append = 0;
  int64_t durable = 0;
  int64_t hw = 0;
  int64_t listener = 0;
};

// A group's worker records one in-flight append at a time. Only the leader dumps its timestamps.
inline thread_local std::array<Row, kTotal> rows;
inline thread_local Row* active = nullptr;

inline int64_t Now() {
  using namespace std::chrono;
  return duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count();
}

inline bool WriteTrace() {
  auto* file = std::fopen(std::getenv("RAFT_COMMIT_BENCH_TRACE"), "wx");
  if (!file)
    return false;

  std::fputs("leader,epoch,end,wire_bytes,submit_ns,append_ns,durable_ns,hw_ns,listener_ns\n",
             file);
  for (const Row& row : rows) {
    std::fprintf(file,
                 "%" PRId32 ",%" PRId32 ",%" PRId64 ",%" PRId64 ",%" PRId64 ",%" PRId64 ",%" PRId64
                 ",%" PRId64 ",%" PRId64 "\n",
                 row.leader, row.epoch, row.end, row.wire_bytes, row.submit, row.append,
                 row.durable, row.hw, row.listener);
  }
  return std::fclose(file) == 0;
}
}  // namespace commit_benchmark
