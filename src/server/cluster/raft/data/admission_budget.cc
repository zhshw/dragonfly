// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/data/admission_budget.h"

#include <algorithm>

namespace dfly::cluster::raft::trace {
using namespace std;

Admission::Admission(shared_ptr<Capture> capture, uint64_t connection,
                     shared_ptr<AdmissionBudget::Progress> progress)
    : capture_(std::move(capture)), connection_(connection), progress_(std::move(progress)) {
}

Admission::~Admission() {
  Finish("Cancelled");
  Refresh();
}

void Admission::SetEpoch(int32_t epoch) {
  if (epoch_ != epoch)
    Finish("Cancelled");
  epoch_ = epoch;
  capture_->SetNodeRole("Leader", epoch);
}

void Admission::Refresh() {
  erase_if(tickets_, [&](const auto& ticket) {
    auto retired = ticket->retired.load(memory_order_acquire);
    if (retired < 0)
      return false;
    auto event = capture_->MakeEvent("raft.set.ticket.retired", retired, retired);
    event.attributes["ticket_id"] = to_string(ticket->id);
    event.attributes["connection_id"] = to_string(connection_);
    event.attributes["released_bytes"] = to_string(ticket->bytes);
    if (ticket->epoch >= 0) {
      event.attributes["node_role"] = "Leader";
      event.attributes["epoch"] = to_string(ticket->epoch);
    }
    capture_->Emit(std::move(event));
    return true;
  });
}

void Admission::Attempt(const void* command) {
  if (wait_ && command_ != command)
    Finish("Cancelled");
  Refresh();
  if (wait_) {
    auto event = capture_->MakePoint("raft.set.admission.retry", wait_->id);
    event.attributes["connection_id"] = to_string(connection_);
    capture_->Emit(std::move(event));
  }
}

void Admission::Rejected(const void* command, size_t bytes, const AdmissionBudget& budget) {
  if (wait_)
    return;
  command_ = command;
  wait_ = capture_->MakePoint("raft.set.admission.wait");
  wait_->request = wait_->id;
  wait_->attributes["connection_id"] = to_string(connection_);
  wait_->attributes["required_bytes"] = to_string(bytes);
  wait_->attributes["reserved_bytes"] = to_string(budget.bytes());
  wait_->attributes["reserved_requests"] = to_string(budget.requests());
  wait_->attributes["scope"] = "Connection/group";
  wave_ = tickets_;
  string ticket_ids;
  for (const auto& ticket : wave_) {
    if (!ticket_ids.empty())
      ticket_ids += ',';
    ticket_ids += to_string(ticket->id);
  }
  wait_->attributes["ticket_ids"] = std::move(ticket_ids);
  wait_->attributes["wave_size"] = to_string(wave_.size());
}

void Admission::Admitted(const void* command, AdmissionBudget::Reservation* reservation) {
  if (wait_ && command_ == command)
    Finish("Granted");
  Refresh();
  auto ticket = make_shared<AdmissionBudget::Ticket>();
  ticket->id = capture_->NextId();
  ticket->bytes = reservation->bytes();
  ticket->epoch = epoch_;

  // The clock is immutable; calling it does not mutate the connection's capture on another owner.
  ticket->clock = capture_->clock();
  reservation->Observe(ticket);
  tickets_.push_back(ticket);
}

void Admission::Finish(string_view outcome) {
  if (!wait_)
    return;
  const auto now = capture_->Now();
  auto event = std::move(*wait_);
  event.end = now;
  event.outcome = outcome;
  int64_t retired_count = 0;
  optional<Time> first;
  for (const auto& ticket : wave_) {
    const auto retired = ticket->retired.load(memory_order_acquire);
    if (retired >= event.start && retired <= now) {
      ++retired_count;
      first = first ? min(*first, retired) : retired;
    }
  }
  const auto wal = progress_->wal_complete.load(memory_order_acquire);
  const bool granted = outcome == "Granted";
  event.values = {
      {Attribute::kAdmissionWait, granted ? optional<Time>(now - event.start) : nullopt,
       granted ? "Observed" : "Incomplete"},
      {Attribute::kRetiredTickets, retired_count},
      {Attribute::kFullWaveRetired, !wave_.empty() && retired_count == int64_t(wave_.size())},
      {Attribute::kWalIdle,
       first && wal >= 0 && wal <= now && granted
           ? optional<Time>(max<Time>(0, now - max(*first, wal)))
           : nullopt,
       first && wal >= 0 && wal <= now && granted ? "Observed" : "Incomplete"}};
  if (first)
    event.attributes["first_retirement_ns"] = to_string(*first);
  if (wal >= 0 && wal <= now)
    event.attributes["last_wal_completion_ns"] = to_string(wal);
  if (first && wal >= 0 && wal <= now && granted) {
    auto idle = capture_->MakeEvent("raft.set.admission.wal_idle", max(*first, wal), now, event.id);
    idle.attributes["connection_id"] = to_string(connection_);
    idle.attributes["last_wal_completion_ns"] = to_string(wal);
    idle.attributes["first_retirement_ns"] = to_string(*first);
    capture_->Emit(std::move(idle));
  }
  if (granted) {
    auto grant = capture_->MakeEvent("raft.set.admission.granted", now, now, event.id);
    grant.attributes["connection_id"] = to_string(connection_);
    capture_->Emit(std::move(grant));
  }
  capture_->Emit(std::move(event));
  wait_.reset();
  wave_.clear();
  command_ = nullptr;
}

}  // namespace dfly::cluster::raft::trace
