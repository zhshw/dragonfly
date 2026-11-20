// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/data/kv_rdb.h"

#include <absl/strings/str_cat.h>
#include <openssl/sha.h>

#include "server/cluster/slot_set.h"
#include "server/db_slice.h"
#include "server/engine_shard_set.h"
#include "server/main_service.h"
#include "server/namespaces.h"
#include "server/rdb_load.h"
#include "server/rdb_save.h"
#include "server/transaction.h"

namespace dfly::cluster::raft {

namespace {

void ScheduleGlobal(Service& service, std::string_view command, Transaction::RunnableType cb) {
  boost::intrusive_ptr<Transaction> tx(new Transaction{service.FindCmd(command)});
  tx->InitByArgs(&namespaces->GetDefaultNamespace(), 0, {});
  tx->ScheduleSingleHop(cb);
}

}  // namespace

absl::Status SaveKvRdb(Service& service, const SlotSet& slots, io::Sink* destination,
                       absl::FunctionRef<void()> on_cut) {
  if (!destination || slots.Empty())
    return absl::InvalidArgumentError("KV RDB requires a destination and assigned slots");
  RdbSaver saver(destination, SaveMode::RDB, false, {}, DflyVersion::CURRENT_VER);
  ExecutionState state;
  if (auto ec = saver.SaveHeader({}); ec)
    return absl::InternalError(absl::StrCat("Writing KV RDB header: ", ec.message()));
  ScheduleGlobal(service, "SAVE", [&](Transaction*, EngineShard* shard) {
    saver.StartSlotSnapshotInShard(slots, &state, shard);
    return OpStatus::OK;
  });
  on_cut();
  if (auto ec = saver.SaveBody(state); ec)
    return absl::InternalError(absl::StrCat("Writing KV RDB: ", ec.message()));
  return absl::OkStatus();
}

void ClearKvSlots(Service& service, const SlotRanges& ranges) {
  std::vector<util::fb2::Fiber> flushes(shard_set->size());
  ScheduleGlobal(service, "FLUSHALL", [&](Transaction*, EngineShard* shard) {
    auto& slice = namespaces->GetDefaultNamespace().GetDbSlice(shard->shard_id());
    flushes[shard->shard_id()] = slice.StartFlushSlots(ranges);
    return OpStatus::OK;
  });

  // Finish removal after the registration transaction has released the shard locks.
  shard_set->RunBlockingInParallel([&](EngineShard* shard) { flushes[shard->shard_id()].Join(); });
}

absl::Status InstallKvRdb(Service* service, const SlotRanges& ranges, std::string_view bytes,
                          std::string_view expected_digest) {
  if (!service || ranges.Empty())
    return absl::InvalidArgumentError("KV RDB requires a service and assigned slots");
  if (expected_digest.size() != SHA256_DIGEST_LENGTH || expected_digest != KvRdbDigest(bytes))
    return absl::DataLossError("Incorrect KV RDB digest");
  for (const auto& range : ranges) {
    if (!range.IsValid())
      return absl::InvalidArgumentError("Invalid KV RDB slot range");
  }
  SlotSet slots(ranges);
  auto load = [&](bool validate) {
    io::BytesSource source(bytes);
    RdbLoadContext context;
    RdbLoader loader(service, &context);
    loader.SetSlotImage(slots, validate);
    loader.set_source_limit(bytes.size());
    if (auto ec = loader.Load(&source); ec)
      return absl::DataLossError(absl::StrCat("Invalid slot RDB: ", ec.message()));
    if (loader.bytes_read() - loader.Leftover().size() != bytes.size())
      return absl::DataLossError("Trailing bytes in KV RDB");
    return absl::OkStatus();
  };
  if (auto s = load(true); !s.ok())
    return s;
  ClearKvSlots(*service, ranges);
  return load(false);
}

}  // namespace dfly::cluster::raft
