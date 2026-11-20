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
  std::array<std::string_view, 3> parts{"SET", effect.key, effect.value};
  journal::ParsedEntry::CmdData cmd;
  cmd.Assign(parts.begin(), parts.end(), parts.size());
  facade::CapturingReplyBuilder capture{facade::ReplyMode::FULL};
  auto result = executor.Execute(effect.db, cmd, capture);
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

}  // namespace dfly::cluster::raft
