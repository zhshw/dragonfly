// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/data/set_executor.h"

#include <absl/cleanup/cleanup.h>

#include "facade/reply_capture.h"
#include "server/cluster/raft/trace.h"
#include "server/cluster_support.h"
#include "server/engine_shard_set.h"
#include "server/journal/executor.h"

namespace dfly::cluster::raft {

absl::Status ExecuteSetEffect(JournalExecutor& executor, const SetEffectView& effect) {
  if (KeySlot(effect.key) != effect.slot)
    return absl::DataLossError("Committed SET key hashes to a different slot");
  if (effect.action == SetEffect::Action::kNoop)
    return absl::OkStatus();
  facade::CapturingReplyBuilder capture{facade::ReplyMode::FULL};
  auto result = executor.ExecuteSet(effect.db, effect.key, effect.value, capture);
  auto payload = capture.Take();
  if (result == facade::DispatchResult::OOM)
    return absl::ResourceExhaustedError("OOM applying committed SET");
  if (auto error = facade::CapturingReplyBuilder::TryExtractError(payload))
    return absl::FailedPreconditionError(std::string(error->first));
  auto* reply = std::get_if<facade::payload::SimpleString>(&payload);
  if (result != facade::DispatchResult::OK || !reply || *reply != "OK")
    return absl::FailedPreconditionError("Committed SET did not return OK");
  return absl::OkStatus();
}

absl::Status ExecuteSetBatch(JournalExecutor& executor, const CommittedEffects::ApplyBatch& batch,
                             size_t* completed) {
  return ExecuteSetBatch(executor, batch, nullptr, completed);
}

absl::Status ExecuteSetBatch(JournalExecutor& executor, const CommittedEffects::ApplyBatch& batch,
                             trace::Batch* trace, size_t* completed) {
  if (trace) {
    executor.SetApplyObserver(
        [trace](int64_t start, int64_t end) { trace->Record("journal.apply", start, end); });
  }
  absl::Cleanup observer = [&] {
    if (trace)
      executor.SetApplyObserver({});
  };
  *completed = 0;
  const auto shard_count = shard_set->size();
  while (*completed < batch.size()) {
    const size_t begin = *completed;
    const auto sid = Shard(batch.View(begin).key, shard_count);
    size_t end = begin + 1;
    while (end < batch.size() && Shard(batch.View(end).key, shard_count) == sid)
      ++end;

    // The owning ApplyLane waits before starting the next run, even if it targets another shard.
    auto apply_run = [&] {
      const auto start = trace ? trace->Now() : 0;
      absl::Cleanup record = [&] {
        if (trace)
          trace->Record(
              "raft.apply.shard_run", start, trace->Now(),
              {{"shard_id", std::to_string(sid)}, {"records", std::to_string(end - begin)}});
      };
      auto apply = [&] {
        for (; *completed < end; ++*completed) {
          if (auto status = ExecuteSetEffect(executor, batch.View(*completed)); !status.ok())
            return status;
        }
        return absl::OkStatus();
      };
      return end - begin == 1 ? apply() : executor.WithShardLocalTransaction(apply);
    };
    absl::Status status;
    if (auto* local = EngineShard::tlocal(); local && local->shard_id() == sid) {
      status = apply_run();
    } else {
      // A contended transaction may itself wait on the primary shard queue.
      util::fb2::FiberBlockingCounter done(1);
      const auto queued = trace ? trace->Now() : 0;
      shard_set->AddL2(sid, [&, done]() mutable {
        if (trace)
          trace->Record("raft.apply.shard_queue", queued, trace->Now(),
                        {{"shard_id", std::to_string(sid)}});
        status = apply_run();
        std::move(done).Release();
      });
      done.Wait();
    }
    if (!status.ok())
      return status;
  }
  return absl::OkStatus();
}

absl::StatusOr<bool> ExistsForSet(JournalExecutor& executor, DbIndex db, std::string_view key) {
  std::array<std::string_view, 2> parts{"EXISTS", key};
  journal::ParsedEntry::CmdData cmd;
  cmd.Assign(parts.begin(), parts.end(), parts.size());
  facade::CapturingReplyBuilder capture;
  auto result = executor.Execute(db, cmd, capture);
  auto payload = capture.Take();
  if (auto error = facade::CapturingReplyBuilder::TryExtractError(payload))
    return absl::FailedPreconditionError(std::string(error->first));
  auto* count = std::get_if<long>(&payload);
  if (result != facade::DispatchResult::OK || !count || (*count != 0 && *count != 1))
    return absl::InternalError("EXISTS did not return a single key count");
  return *count == 1;
}

absl::StatusOr<std::optional<std::string>> ReadCommittedValue(JournalExecutor& executor, DbIndex db,
                                                              std::string_view key) {
  std::array<std::string_view, 2> parts{"GET", key};
  journal::ParsedEntry::CmdData cmd;
  cmd.Assign(parts.begin(), parts.end(), parts.size());
  facade::CapturingReplyBuilder capture;
  auto result = executor.Execute(db, cmd, capture);
  auto payload = capture.Take();
  if (auto error = facade::CapturingReplyBuilder::TryExtractError(payload))
    return absl::FailedPreconditionError(std::string(error->first));
  if (result != facade::DispatchResult::OK)
    return absl::InternalError("Committed GET failed");
  if (std::holds_alternative<facade::payload::Null>(payload))
    return std::nullopt;
  if (auto* str = std::get_if<facade::payload::BulkString>(&payload))
    return std::string(std::move(*str));
  if (auto* ref = std::get_if<facade::payload::BulkStringRef>(&payload))
    return std::string(*ref);
  if (auto* borrowed = std::get_if<cmn::BorrowedString>(&payload))
    return cmn::DecodeToString(*borrowed);
  return absl::InternalError("GET did not return a bulk string or null");
}

}  // namespace dfly::cluster::raft
