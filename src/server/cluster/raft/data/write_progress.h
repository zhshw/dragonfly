// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <atomic>

#include "server/cluster/raft/data/command_handler.h"

namespace dfly::cluster::raft {

// One object per admitted leader epoch. Old records retain their own epoch after leadership
// changes; progress in a new epoch can never complete an old record with a reused offset.
struct WriteEpoch {
  explicit WriteEpoch(int32_t epoch) : epoch(epoch) {
  }
  const int32_t epoch;
  std::atomic<int64_t> reply_offset{-1};
  util::fb2::EventCount changed;
};

// Binding, rejection and watermark publication are serialized by the group. There is no
// per-record waiter or timer. Notifications belong to the epoch and subscribers to connections.
class WatermarkWrite : public PendingDataWrite {
 public:
  explicit WatermarkWrite(std::shared_ptr<WriteEpoch> epoch) : epoch_(std::move(epoch)) {
  }

  bool IsReady() const override {
    // Read progress first: a later progress publication must expose an earlier rejection.
    const auto offset = epoch_->reply_offset.load(std::memory_order_acquire);
    const auto state = state_.load(std::memory_order_acquire);
    return state == State::kRejected || (state == State::kBound && position.end_offset <= offset);
  }

  void Wait() override {
    epoch_->changed.await([this] { return IsReady(); });
  }

  std::optional<util::fb2::EventCount::SubKey> Subscribe(
      util::fb2::detail::Waiter* waiter) override {
    // A progress wake may leave this head unready. Keep the subscription until it is consumed;
    // rechecking after subscribing also handles progress arriving just before registration.
    auto key = epoch_->changed.subscribe_persistent(waiter);
    if (IsReady())
      return std::nullopt;
    return key;
  }

  void Bind(DataWritePosition value) {
    if (state_.load(std::memory_order_relaxed) != State::kQueued)
      return;
    if (value.epoch != epoch_->epoch || value.end_offset < 0) {
      Reject(absl::FailedPreconditionError("Invalid Raft reply binding"));
      return;
    }
    position = value;
    state_.store(State::kBound, std::memory_order_release);
  }

  void Reject(absl::Status error) {
    if (IsReady())
      return;
    status = std::move(error);
    state_.store(State::kRejected, std::memory_order_release);
  }

  const std::shared_ptr<WriteEpoch>& epoch() const {
    return epoch_;
  }

 private:
  enum class State : uint8_t { kQueued, kBound, kRejected };
  std::shared_ptr<WriteEpoch> epoch_;
  std::atomic<State> state_{State::kQueued};
};

}  // namespace dfly::cluster::raft
