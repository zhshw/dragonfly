// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string_view>

namespace dfly::cluster::raft {

// Owns a Folly EventBase with an io_uring backend, isolated from the networking proactors.
// Callers retain descriptors and buffers until completion and drain all I/O before destruction.
class FollyIo {
 public:
  FollyIo();
  ~FollyIo();

  // offset < 0 requests fsync; otherwise writes at the given offset (short writes are possible).
  // done receives the CQE result on the Folly owner and must not block. A group using this
  // owner for its state machine can advance its serialized state directly from the callback.
  // Return false when the recipient queue is full; Folly retries delivery in a later loop.
  // A retry delivers the same result and never repeats the I/O operation.
  void Submit(int fd, int64_t offset, std::string_view bytes, std::function<bool(int)> done);

  void Run(std::function<void()> task);
  void Drain();
  void Encode(std::function<void()> work, std::function<void()> complete);

  // Replaces the single group deadline. Null cancels it; called only on the owning loop.
  void SetDeadline(std::optional<std::chrono::steady_clock::time_point> deadline,
                   std::function<void()> task);

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace dfly::cluster::raft
