// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/data/set_executor.h"

#include "facade/reply_capture.h"
#include "server/cluster/raft/data/set_effect.h"
#include "server/cluster_support.h"
#include "server/journal/executor.h"

namespace dfly::cluster::raft {

absl::Status ExecuteSetEffect(JournalExecutor& executor, const SetEffect& effect) {
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
