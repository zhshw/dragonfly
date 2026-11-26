// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <algorithm>
#include <atomic>
#include <memory>
#include <optional>
#include <utility>

#include "server/cluster/raft/trace.h"

namespace dfly::cluster::raft {

// The same reservation owns credits through execution AND ordered reply retirement.
// Optional trace state survives the reservation without retaining its credits.
class AdmissionBudget {
 public:
  struct Ticket {
    uint64_t id;
    size_t bytes;
    int32_t epoch = -1;
    trace::Capture::Clock clock;
    std::atomic<trace::Time> retired{-1};
  };
  struct Progress {
    std::atomic<trace::Time> wal_complete{-1};
  };
  class Reservation {
   public:
    Reservation(AdmissionBudget* budget, size_t bytes) : budget_(budget), bytes_(bytes) {
    }
    Reservation(Reservation&& other) noexcept
        : budget_(std::exchange(other.budget_, nullptr)),
          bytes_(other.bytes_),
          ticket_(std::move(other.ticket_)) {
    }
    Reservation(const Reservation&) = delete;
    ~Reservation() {
      Release();
    }
    void Observe(std::shared_ptr<Ticket> ticket) {
      ticket_ = std::move(ticket);
    }
    void Release() {
      if (!budget_)
        return;
      budget_->bytes_.fetch_sub(bytes_);
      budget_->requests_.fetch_sub(1);
      if (ticket_)
        ticket_->retired.store(ticket_->clock(), std::memory_order_release);
      budget_ = nullptr;
    }
    size_t bytes() const {
      return bytes_;
    }

   private:
    AdmissionBudget* budget_;
    size_t bytes_;
    std::shared_ptr<Ticket> ticket_;
  };

  AdmissionBudget(size_t bytes, size_t requests)
      : max_bytes_(bytes), max_requests_(requests), progress_(std::make_shared<Progress>()) {
  }
  std::optional<Reservation> TryReserve(size_t bytes) {
    const auto count = requests_.fetch_add(1);
    if (count >= max_requests_) {
      requests_.fetch_sub(1);
      return std::nullopt;
    }
    auto reserved = bytes_.load();
    do {
      if ((reserved || count) && bytes > max_bytes_ - std::min(max_bytes_, reserved)) {
        requests_.fetch_sub(1);
        return std::nullopt;
      }
    } while (!bytes_.compare_exchange_weak(reserved, reserved + bytes));
    return Reservation(this, bytes);
  }
  size_t requests() const {
    return requests_.load();
  }
  size_t bytes() const {
    return bytes_.load();
  }
  const std::shared_ptr<Progress>& progress() const {
    return progress_;
  }

 private:
  const size_t max_bytes_, max_requests_;
  std::atomic_size_t bytes_{0}, requests_{0};
  std::shared_ptr<Progress> progress_;
};

namespace trace {

// Owned by one connection. Ticket retirement may occur on another owner, so only that immutable
// ticket's atomic timestamp crosses threads. Finalization never guesses retirement from replies.
class Admission {
 public:
  Admission(std::shared_ptr<Capture> capture, uint64_t connection,
            std::shared_ptr<AdmissionBudget::Progress> progress);
  ~Admission();
  void SetEpoch(int32_t epoch);
  void Attempt(const void* command);
  void Rejected(const void* command, size_t bytes, const AdmissionBudget& budget);
  void Admitted(const void* command, AdmissionBudget::Reservation* reservation);
  void Refresh();

 private:
  void Finish(std::string_view outcome);
  std::shared_ptr<Capture> capture_;
  const uint64_t connection_;
  int32_t epoch_ = -1;
  std::shared_ptr<AdmissionBudget::Progress> progress_;
  std::vector<std::shared_ptr<AdmissionBudget::Ticket>> tickets_;
  std::vector<std::shared_ptr<AdmissionBudget::Ticket>> wave_;
  const void* command_ = nullptr;
  std::optional<Event> wait_;
};

}  // namespace trace
}  // namespace dfly::cluster::raft
