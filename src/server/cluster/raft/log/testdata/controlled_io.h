// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <deque>

#include "server/cluster/raft/log/partition_log.h"

namespace dfly::cluster::raft::test {

// Execute real file operations one completion at a time, with optional short/error results.
class ControlledIO {
 public:
  PartitionLog::AsyncIO Submitter() {
    return [this](int fd, int64_t offset, std::string_view bytes, PartitionLog::IoCallback cb) {
      requests.push_back({fd, offset, bytes, std::move(cb)});
    };
  }

  void Complete(std::optional<int> forced = std::nullopt) {
    auto request = std::move(requests.front());
    requests.pop_front();
    int result;
    if (forced && *forced <= 0) {
      result = *forced;
    } else if (request.offset < 0) {
      result = fsync(request.fd) == 0 ? 0 : -errno;
    } else {
      size_t count =
          forced ? std::min(request.bytes.size(), size_t(*forced)) : request.bytes.size();
      result = pwrite(request.fd, request.bytes.data(), count, request.offset);
      if (result < 0)
        result = -errno;
    }
    request.callback(result);
  }

  struct Request {
    int fd;
    int64_t offset;
    std::string_view bytes;
    PartitionLog::IoCallback callback;
  };
  std::deque<Request> requests;
};

}  // namespace dfly::cluster::raft::test
