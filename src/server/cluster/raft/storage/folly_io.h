// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string_view>

namespace dfly::cluster::raft {

// Owns a Folly EventBase with an io_uring backend, isolated from the networking proactors.
// Callers retain descriptors and buffers until completion and drain all I/O before destruction.
class FollyIo {
 public:
  FollyIo();
  ~FollyIo();

  // offset < 0 requests fsync; otherwise writes at the given offset (short writes are possible).
  // done receives the CQE result on the Folly thread and must not block or modify Raft state.
  // Return false when the recipient queue is full; Folly retries delivery in a later loop.
  // A retry delivers the same result and never repeats the I/O operation.
  void Submit(int fd, int64_t offset, std::string_view bytes, std::function<bool(int)> done);

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace dfly::cluster::raft
