// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/executor.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>

#include <algorithm>
#include <memory>

#include "base/flags.h"
#include "base/logging.h"
#include "facade/reply_capture.h"
#include "facade/service_interface.h"
#include "server/db_slice.h"
#include "server/engine_shard.h"
#include "server/engine_shard_set.h"
#include "server/main_service.h"
#include "server/namespaces.h"
#include "server/transaction.h"

ABSL_DECLARE_FLAG(uint32_t, dbnum);

using namespace std;

namespace dfly {

namespace {
// Build a CmdData from parts passed to absl::StrCat.
template <typename... Ts> void BuildFromParts(cmn::BackedArguments* dest, Ts... parts) {
  vector<string> raw_parts{absl::StrCat(std::forward<Ts>(parts))...};

  dest->Assign(raw_parts.begin(), raw_parts.end(), raw_parts.size());
}

}  // namespace

JournalExecutor::JournalExecutor(Service* service)
    : service_{service},
      reply_builder_{new facade::CapturingReplyBuilder{facade::ReplyMode::NONE}},
      conn_context_{nullptr, acl::UserCredentials{}} {
  conn_context_.is_replicating = true;
  conn_context_.journal_emulated = true;
  conn_context_.skip_acl_validation = true;
  conn_context_.ns = &namespaces->GetDefaultNamespace();
}

JournalExecutor::~JournalExecutor() {
}

facade::DispatchResult JournalExecutor::Execute(DbIndex dbid, journal::ParsedEntry::CmdData& cmd) {
  return Execute(dbid, cmd, *reply_builder_);
}

facade::DispatchResult JournalExecutor::Execute(DbIndex dbid, journal::ParsedEntry::CmdData& cmd,
                                                facade::SinkReplyBuilder& reply) {
  if (!SelectDb(dbid))
    return facade::DispatchResult::ERROR;
  CommandContext cntx_cmd;
  cntx_cmd.Init(&reply, &conn_context_);

  // TODO: we should improve interfaces in callers (replica and rdb_load) so that we pass
  // CommandContext directly and avoid this swap.
  cntx_cmd.SwapArgs(cmd);
  return Execute(facade::ParsedArgs{cntx_cmd}, &cntx_cmd);
}

facade::DispatchResult JournalExecutor::ExecuteSet(DbIndex dbid, string_view key, string_view value,
                                                   facade::SinkReplyBuilder& reply) {
  if (!SelectDb(dbid))
    return facade::DispatchResult::ERROR;
  if (conn_context_.transaction)
    conn_context_.transaction->InitTxTime();
  array<string_view, 3> parts{"SET", key, value};
  CommandContext cmd(&reply, &conn_context_);
  return Execute(facade::ArgSlice{parts}, &cmd);
}

absl::Status JournalExecutor::WithShardLocalTransaction(absl::FunctionRef<absl::Status()> run) {
  DCHECK(EngineShard::tlocal());
  DCHECK(!conn_context_.transaction);
  const auto* exec = service_->FindCmd("EXEC");
  if (!exec)
    return run();

  boost::intrusive_ptr<Transaction> tx(new Transaction(exec));
  tx->StartMultiNonAtomic(Transaction::SHARD_LOCAL);
  conn_context_.transaction = tx.get();
  absl::Cleanup cleanup = [&] {
    tx->UnlockMulti();
    conn_context_.transaction = nullptr;
  };
  return run();
}

void JournalExecutor::FlushAll() {
  CommandContext cmd;
  cmd.Init(reply_builder_.get(), &conn_context_);
  BuildFromParts(&cmd, "FLUSHALL");
  std::ignore = Execute(facade::ParsedArgs{cmd}, &cmd);
}

void JournalExecutor::FlushSlots(const cluster::SlotRange& slot_range) {
  CommandContext cmd;
  cmd.Init(reply_builder_.get(), &conn_context_);
  BuildFromParts(&cmd, "DFLYCLUSTER", "FLUSHSLOTS", slot_range.start, slot_range.end);
  std::ignore = Execute(facade::ParsedArgs{cmd}, &cmd);
}

facade::DispatchResult JournalExecutor::Execute(facade::ParsedArgs args, CommandContext* cmd_cntx) {
  return service_->DispatchCommand(args, cmd_cntx, facade::AsyncPreference::ONLY_SYNC, nullptr);
}

bool JournalExecutor::SelectDb(DbIndex dbid) {
  if (dbid >= absl::GetFlag(FLAGS_dbnum)) {
    LOG(ERROR) << "Database index out of range in journal: " << dbid;
    return false;
  }
  if (ensured_dbs_.size() <= dbid)
    ensured_dbs_.resize(dbid + 1);

  if (!ensured_dbs_[dbid]) {
    // Journal replay can target nonzero databases even when client SELECT is disabled.
    shard_set->RunBriefInParallel([ns = conn_context_.ns, dbid](EngineShard* shard) {
      ns->GetDbSlice(shard->shard_id()).ActivateDb(dbid);
    });
    ensured_dbs_[dbid] = true;
  }
  conn_context_.conn_state.db_index = dbid;
  return true;
}

}  // namespace dfly
