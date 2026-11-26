// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/functional/function_ref.h>
#include <absl/status/status.h>
#include <absl/types/span.h>

#include <functional>

#include "facade/service_interface.h"
#include "server/cluster/cluster_defs.h"
#include "server/conn_context.h"
#include "server/journal/types.h"

namespace facade {
class CapturingReplyBuilder;
}  // namespace facade

namespace dfly {

class Service;

// JournalExecutor allows executing journal entries.
class JournalExecutor {
 public:
  explicit JournalExecutor(Service* service);
  ~JournalExecutor();

  JournalExecutor(JournalExecutor&&) = delete;

  // Returns the result of Service::DispatchCommand
  facade::DispatchResult Execute(DbIndex dbid, journal::ParsedEntry::CmdData& cmd);

  // Capture a reply when replay requires the command's own success acknowledgement.
  facade::DispatchResult Execute(DbIndex dbid, journal::ParsedEntry::CmdData& cmd,
                                 facade::SinkReplyBuilder& reply);

  // Replay a resolved replacement without copying its key/value into command storage.
  // The views must remain valid until this synchronous call returns, including fiber yields.
  facade::DispatchResult ExecuteSet(DbIndex dbid, std::string_view key, std::string_view value,
                                    facade::SinkReplyBuilder& reply);

  // Run synchronous single-shard commands with one reusable transaction. The caller runs on
  // the target shard and must wait for the callback before using this executor again.
  absl::Status WithShardLocalTransaction(absl::FunctionRef<absl::Status()> run);

  // Observe synchronous dispatch boundaries on its execution thread, excluding shard enqueue.
  // The caller owns the observer lifetime and serializes all use of this executor.
  using ApplyObserver = std::function<void(int64_t, int64_t)>;
  void SetApplyObserver(ApplyObserver observer) {
    apply_observer_ = std::move(observer);
  }

  void FlushAll();  // Execute FLUSHALL.
  void FlushSlots(const cluster::SlotRange& slot_range);

 private:
  facade::DispatchResult Execute(facade::ParsedArgs args, CommandContext* cmd_cntx);

  // Select database. Ensure it exists if accessed for first time.
  bool SelectDb(DbIndex dbid);

  Service* service_;
  std::unique_ptr<facade::CapturingReplyBuilder> reply_builder_;
  ConnectionContext conn_context_;

  std::vector<bool> ensured_dbs_;
  ApplyObserver apply_observer_;
};

}  // namespace dfly
