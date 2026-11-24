// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "util/fibers/event_count.h"

namespace facade {

// A deferred reply may depend on shared progress rather than its own completion counter.
// Only the connection head subscribes. Subscriptions remain valid after a non-ready wakeup.
class ReplyProgress {
 public:
  virtual ~ReplyProgress() = default;
  virtual bool IsReady() const = 0;
  virtual void Wait() = 0;
  virtual std::optional<util::fb2::EventCount::SubKey> Subscribe(
      util::fb2::detail::Waiter* waiter) = 0;
};

}  // namespace facade
