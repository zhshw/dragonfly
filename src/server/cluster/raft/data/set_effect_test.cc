// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/data/set_effect.h"

#include <absl/base/internal/endian.h>
#include <gtest/gtest.h>
#include <stdlib.h>

#include "server/cluster/raft/data/kv_rdb.h"
#include "server/cluster/raft/log/record_batch.h"

namespace dfly::cluster::raft {
namespace {
using namespace std;
namespace fs = filesystem;

class SetEffectTest : public testing::Test {
 protected:
  void SetUp() override {
    string tmp = (fs::temp_directory_path() / "raft-set-effects-XXXXXX").string();
    ASSERT_NE(mkdtemp(tmp.data()), nullptr);
    dir_ = tmp;
    auto log = PartitionLog::Open(dir_, 128);
    ASSERT_TRUE(log.ok()) << log.status();
    log_ = *std::move(log);
  }

  void TearDown() override {
    log_.reset();
    if (!dir_.empty())
      fs::remove_all(dir_);
  }

  absl::Status Append(const SetEffect& effect) {
    auto batch = MakeSetEffectBatch(log_->End().end_offset, 3, 10, effect);
    return batch.ok() ? log_->Append(*batch, 3) : batch.status();
  }

  absl::Status AppendMarker(TransferKind kind, protocol::SlotTransferRecord record) {
    record.kind = static_cast<int8_t>(kind);
    auto batch = MakeSlotTransferBatch(log_->End().end_offset, 3, 10, record);
    return batch.ok() ? log_->Append(*batch, 3) : batch.status();
  }

  protocol::SlotTransferRecord Migration() const {
    protocol::SlotTransferRecord record;
    record.migration_id[15] = 1;
    record.source_group = first_.group_id;
    record.target_group = 20;
    record.generation = first_.generation;
    record.slot = first_.slot;
    return record;
  }

  SetEffect first_{10, 0, 42, 7, string("x\0y", 3), string("a\0b", 3)};
  fs::path dir_;
  unique_ptr<PartitionLog> log_;
};

TEST_F(SetEffectTest, OnlyCommittedEffectsAreAppliedOnceInLogOrder) {
  array<int32_t, 1> voters{9};
  ASSERT_TRUE(log_->AppendLeaderChange(3, 1, 9, voters, voters).ok());
  ASSERT_TRUE(Append(first_).ok());
  auto second = first_;
  second.value = string(65'536, 'v');
  second.generation = 8;
  ASSERT_TRUE(Append(second).ok());
  vector<SetEffect> seen;
  auto apply = [&](const SetEffect& effect) {
    seen.push_back(effect);
    return absl::OkStatus();
  };
  CommittedEffects state(10);
  ASSERT_TRUE(state.ApplyTo(*log_, 1, apply).ok());
  EXPECT_TRUE(seen.empty());
  ASSERT_TRUE(state.ApplyTo(*log_, 3, apply).ok());
  ASSERT_EQ(seen.size(), 2u);
  EXPECT_EQ(seen[0], first_);
  EXPECT_EQ(seen[1], second);

  // A delayed older completion and a duplicate current completion cannot re-execute either SET.
  EXPECT_TRUE(state.ApplyTo(*log_, 1, apply).ok());
  EXPECT_TRUE(state.ApplyTo(*log_, 3, apply).ok());
  EXPECT_EQ(seen.size(), 2u);
  EXPECT_EQ(state.applied_offset(), 3);
  log_.reset();
  log_ = PartitionLog::Open(dir_).value();

  // Simulate the caller restoring a KV image that already included the first SET.
  CommittedEffects restored(10, 2);
  seen.clear();
  EXPECT_TRUE(restored.ApplyTo(*log_, 3, apply).ok());
  EXPECT_EQ(seen, vector<SetEffect>{second});
}

TEST_F(SetEffectTest, BatchesRespectHighWatermarkEntryBudgetAndRecordBarriers) {
  CommittedEffects cached(10);
  uint64_t committed_bytes = 0;
  for (int i = 0; i < 45; ++i) {
    if (i == 42) {
      auto control = MakeSnapshotFooterBatch(i, 3, 10).value();
      ASSERT_TRUE(log_->Append(control, 3).ok());
      committed_bytes += control.size();
      continue;
    }
    auto effect = first_;
    effect.value = to_string(i);
    if (i == 40) {
      effect.action = SetEffect::Action::kNoop;
      effect.value.clear();
    }
    auto batch = MakeSetEffectBatch(i, 3, 10, effect).value();
    ASSERT_TRUE(log_->Append(batch, 3).ok());
    cached.RememberAppended({3, i + 1}, batch.size(), effect);
    if (i < 44)
      committed_bytes += batch.size();
  }
  CommittedEffects replayed(10);
  for (auto* state : {&cached, &replayed}) {
    vector<size_t> sizes;
    vector<string> seen;
    auto apply = [&](span<const SetEffect* const> effects, size_t* applied) {
      sizes.push_back(effects.size());
      EXPECT_FALSE(state->SnapshotMigrations(state->applied_position()).ok());
      for (const auto* effect : effects) {
        seen.push_back(effect->value);
        ++*applied;
      }
      return absl::OkStatus();
    };
    auto image = [](SlotId, optional<string_view>, string_view) {
      ADD_FAILURE() << "No migration record was written";
      return absl::OkStatus();
    };
    ASSERT_TRUE(state->ApplyToBatched(*log_, 0, apply, image).ok());
    EXPECT_TRUE(sizes.empty());
    ASSERT_TRUE(state->ApplyToBatched(*log_, 44, apply, image).ok());
    EXPECT_EQ(sizes, (vector<size_t>{32, 8, 1, 1}));
    ASSERT_EQ(seen.size(), 42u);
    for (int i = 0; i < 40; ++i)
      EXPECT_EQ(seen[i], to_string(i));
    EXPECT_EQ(seen[40], "41");
    EXPECT_EQ(seen[41], "43");
    EXPECT_EQ(state->applied_offset(), 44);
    EXPECT_EQ(state->applied_bytes(), committed_bytes);
  }
}

TEST_F(SetEffectTest, BatchesLimitBytesAndAllowOneOversizedEffect) {
  CommittedEffects cached(10);
  for (int i = 0; i < 12; ++i) {
    auto effect = first_;
    effect.value.assign(i == 10 ? 2 << 20 : 200 << 10, 'a' + i);
    auto batch = MakeSetEffectBatch(i, 3, 10, effect).value();
    ASSERT_TRUE(log_->Append(batch, 3).ok());

    // Exercise a cache gap followed by another cached effect in the same apply batch.
    if (i != 7)
      cached.RememberAppended({3, i + 1}, batch.size(), std::move(effect));
  }
  CommittedEffects replayed(10);
  for (auto* state : {&cached, &replayed}) {
    vector<size_t> sizes;
    size_t seen = 0;
    auto apply = [&](span<const SetEffect* const> effects, size_t* applied) {
      sizes.push_back(effects.size());
      size_t bytes = 0;
      for (const auto* effect : effects) {
        EXPECT_EQ(effect->value.front(), 'a' + seen++);
        bytes += effect->key.size() + effect->value.size();
        ++*applied;
      }
      EXPECT_TRUE(bytes <= 1 << 20 || effects.size() == 1);
      return absl::OkStatus();
    };
    auto image = [](SlotId, optional<string_view>, string_view) { return absl::OkStatus(); };
    ASSERT_TRUE(state->ApplyToBatched(*log_, 12, apply, image).ok());
    EXPECT_EQ(sizes, (vector<size_t>{5, 5, 1, 1}));
    EXPECT_EQ(seen, 12u);
  }
}

TEST_F(SetEffectTest, BatchedFailureAndTimeBudgetAdvanceOnlyTheCompletedPrefix) {
  CommittedEffects state(10);
  vector<size_t> bytes;
  for (int i = 0; i < 4; ++i) {
    auto effect = first_;
    effect.value = to_string(i);
    effect.generation += i;
    auto batch = MakeSetEffectBatch(i, 3, 10, effect).value();
    ASSERT_TRUE(log_->Append(batch, 3).ok());
    bytes.push_back(batch.size());
    state.RememberAppended({3, i + 1}, batch.size(), effect);
  }
  vector<string> seen;
  auto image = [](SlotId, optional<string_view>, string_view) { return absl::OkStatus(); };
  auto status = state.ApplyToBatched(
      *log_, 4,
      [&](span<const SetEffect* const> effects, size_t* applied) {
        EXPECT_EQ(effects.size(), 4u);
        seen.push_back(effects[0]->value);
        seen.push_back(effects[1]->value);
        *applied = 2;
        return absl::ResourceExhaustedError("third SET failed");
      },
      image);
  EXPECT_TRUE(absl::IsResourceExhausted(status));
  EXPECT_EQ(state.applied_offset(), 2);
  EXPECT_EQ(state.applied_bytes(), bytes[0] + bytes[1]);
  EXPECT_EQ(state.SnapshotSlots(), (vector<SnapshotSlotRange>{{42, 42, 8}}));

  // A time-limited callback can finish just one entry without dropping or repeating the rest.
  auto apply_one = [&](span<const SetEffect* const> effects, size_t* applied) {
    seen.push_back(effects.front()->value);
    *applied = 1;
    return absl::OkStatus();
  };
  ASSERT_TRUE(state.ApplyToBatched(*log_, 4, apply_one, image).ok());
  EXPECT_EQ(seen, (vector<string>{"0", "1", "2", "3"}));
  EXPECT_EQ(state.applied_offset(), 4);
  EXPECT_EQ(state.applied_bytes(), bytes[0] + bytes[1] + bytes[2] + bytes[3]);
  EXPECT_EQ(state.SnapshotSlots(), (vector<SnapshotSlotRange>{{42, 42, 10}}));

  // Reject a broken callback instead of spinning or publishing entries it did not apply.
  CommittedEffects invalid(10);
  EXPECT_FALSE(invalid
                   .ApplyToBatched(
                       *log_, 4,
                       [](span<const SetEffect* const>, size_t*) { return absl::OkStatus(); },
                       image)
                   .ok());
  EXPECT_EQ(invalid.applied_offset(), 0);
}

TEST_F(SetEffectTest, LeaderDeliversCachedEffectsOnlyAfterCommitAndDiscardsReplacedSuffix) {
  CommittedEffects state(10);
  auto first = MakeSetEffectBatch(0, 3, 10, first_).value();
  auto old = first_;
  old.value = "uncommitted";
  auto second = MakeSetEffectBatch(1, 3, 10, old).value();
  ASSERT_TRUE(log_->Append(first, 3).ok());
  state.RememberAppended({3, 1}, first.size(), first_);
  ASSERT_TRUE(log_->Append(second, 3).ok());
  state.RememberAppended({3, 2}, second.size(), old);

  vector<SetEffect> seen;
  auto apply = [&](const SetEffect& effect) {
    seen.push_back(effect);
    return absl::OkStatus();
  };
  ASSERT_TRUE(state.ApplyTo(*log_, 0, apply).ok());
  EXPECT_TRUE(seen.empty());

  // Hide the closed first segment to prove that a cache hit does not reopen it.
  auto segment = dir_ / "00000000000000000000.log";
  fs::rename(segment, dir_ / "held-segment");
  ASSERT_FALSE(log_->Read(0, 1, 1).ok());
  ASSERT_TRUE(state.ApplyTo(*log_, 1, apply).ok());
  fs::rename(dir_ / "held-segment", segment);
  EXPECT_EQ(seen, vector<SetEffect>{first_});
  EXPECT_EQ(state.applied_bytes(), first.size());

  ASSERT_TRUE(log_->TruncateTo(1, 1).ok());
  auto replacement = first_;
  replacement.value = "new leader";
  second = MakeSetEffectBatch(1, 4, 20, replacement).value();
  ASSERT_TRUE(log_->AppendAsFollower(second, 4).ok());
  ASSERT_TRUE(state.ApplyTo(*log_, 2, apply).ok());
  EXPECT_EQ(seen, (vector<SetEffect>{first_, replacement}));
  EXPECT_EQ(state.applied_bytes(), first.size() + second.size());
}

TEST_F(SetEffectTest, FailedApplyAndWrongGroupDoNotAdvanceCursor) {
  ASSERT_TRUE(Append(first_).ok());
  auto second = first_;
  second.value = "second";
  ASSERT_TRUE(Append(second).ok());
  auto foreign = first_;
  foreign.group_id = 20;
  ASSERT_TRUE(Append(foreign).ok());
  CommittedEffects state(10);
  vector<string> values;
  auto failure = state.ApplyTo(*log_, 3, [&](const SetEffect& effect) {
    if (effect.value == "second")
      return absl::ResourceExhaustedError("apply failed");
    values.push_back(effect.value);
    return absl::OkStatus();
  });
  EXPECT_TRUE(absl::IsResourceExhausted(failure));
  EXPECT_EQ(state.applied_offset(), 1);
  auto retry = state.ApplyTo(*log_, 3, [&](const SetEffect& effect) {
    values.push_back(effect.value);
    return absl::OkStatus();
  });
  EXPECT_TRUE(absl::IsFailedPrecondition(retry));
  EXPECT_EQ(state.applied_offset(), 2);
  EXPECT_EQ(values, (vector<string>{first_.value, second.value}));
}

TEST_F(SetEffectTest, ACommittedFailedConditionPreservesTheValueAcrossReplay) {
  ASSERT_TRUE(Append(first_).ok());
  auto failed = first_;
  failed.action = SetEffect::Action::kNoop;
  EXPECT_FALSE(Append(failed).ok());
  failed.value.clear();
  ASSERT_TRUE(Append(failed).ok());
  log_.reset();
  log_ = PartitionLog::Open(dir_).value();
  CommittedEffects state(10);
  vector<SetEffect> writes;
  auto apply = [&](const SetEffect& effect) {
    writes.push_back(effect);
    return absl::OkStatus();
  };
  ASSERT_TRUE(state.ApplyTo(*log_, 2, apply).ok());
  EXPECT_EQ(state.applied_offset(), 2);
  EXPECT_EQ(writes, vector<SetEffect>{first_});
}

TEST_F(SetEffectTest, RejectsUnsupportedPayloadsAndInvalidAddresses) {
  SetEffect effect{10, 2, 1234, 7, "", ""};
  auto encoded = MakeSetEffectBatch(0, 3, 1, effect).value();
  vector<RecordView> records;
  ASSERT_TRUE(DecodeRecordBatch(encoded, &records).ok());
  string payload(*records.front().value);
  int calls = 0;
  auto apply = [&](const SetEffect&) {
    ++calls;
    return absl::OkStatus();
  };
  for (int pos : {0, 4, 5, 6, 10, 12, 14, 22}) {
    array<string, 1> bad{payload};
    if (pos == 10)
      bad.front().replace(10, 2, 2, static_cast<char>(0xff));
    else
      bad.front()[pos] = static_cast<char>(0xff);
    auto batch = MakeDataBatch(0, 3, 1, bad).value();
    ASSERT_TRUE(log_->Append(batch, 3).ok());
    CommittedEffects state(10);
    EXPECT_FALSE(state.ApplyTo(*log_, 1, apply).ok()) << pos;
    EXPECT_EQ(state.applied_offset(), 0);
    ASSERT_TRUE(log_->TruncateTo(0, 0).ok());
  }
  EXPECT_EQ(calls, 0);
  ASSERT_TRUE(Append(effect).ok());
  CommittedEffects state(10);
  EXPECT_TRUE(state.ApplyTo(*log_, 1, apply).ok());
  EXPECT_EQ(calls, 1);
  effect.slot = 16384;
  EXPECT_FALSE(MakeSetEffectBatch(1, 3, 1, effect).ok());
  effect.slot = 9;
  effect.generation = -1;
  EXPECT_FALSE(MakeSetEffectBatch(1, 3, 1, effect).ok());
}

TEST_F(SetEffectTest, EntryBoundariesAndReentrantApply) {
  ASSERT_TRUE(Append(first_).ok());
  CommittedEffects state(10);
  auto noop = [](const SetEffect&) { return absl::OkStatus(); };
  EXPECT_FALSE(state.ApplyTo(*log_, 2, noop).ok());
  ASSERT_TRUE(state
                  .ApplyTo(*log_, 1,
                           [&](const SetEffect&) {
                             EXPECT_FALSE(state.ApplyTo(*log_, 1, noop).ok());
                             return absl::OkStatus();
                           })
                  .ok());

  // A future multi-command record requires atomic apply support before it can be consumed.
  array<string, 2> values{"x", "y"};
  auto batch = MakeDataBatch(1, 3, 1, values).value();
  ASSERT_TRUE(log_->Append(batch, 3).ok());
  EXPECT_TRUE(state.ApplyTo(*log_, 2, noop).ok());
  EXPECT_EQ(state.applied_offset(), 1);
  EXPECT_FALSE(state.ApplyTo(*log_, 3, noop).ok());
  CommittedEffects split(10, 2);
  EXPECT_FALSE(split.ApplyTo(*log_, 3, noop).ok());
}

TEST_F(SetEffectTest, SnapshotTracksTheMaterializedSlotsAtTheAppliedBatchBoundary) {
  auto set = first_;
  for (SlotId slot : {42, 43, 45}) {
    set.slot = slot;
    ASSERT_TRUE(Append(set).ok());
  }
  set.slot = 44;
  set.action = SetEffect::Action::kNoop;
  set.value.clear();
  ASSERT_TRUE(Append(set).ok());
  auto ok = [](const SetEffect&) { return absl::OkStatus(); };
  CommittedEffects state(10);
  ASSERT_TRUE(state.ApplyTo(*log_, 4, ok).ok());
  LogPosition cut{3, 4};
  EXPECT_EQ(state.applied_position(), cut);
  EXPECT_GT(state.applied_bytes(), 0u);
  EXPECT_FALSE(state.SnapshotMigrations({2, 4}).ok());
  EXPECT_FALSE(state.SnapshotMigrations({3, 3}).ok());
  auto slots = state.SnapshotSlots();
  EXPECT_EQ(slots, (vector<SnapshotSlotRange>{{42, 43, 7}, {45, 45, 7}}));
  EXPECT_EQ(state.SnapshotMigrations(cut).value(), "");
  auto restored = CommittedEffects::FromSnapshot(10, cut, {}, slots);
  ASSERT_TRUE(restored.ok()) << restored.status();
  EXPECT_EQ(restored->SnapshotSlots(), slots);
  EXPECT_EQ(restored->applied_position(), cut);
  EXPECT_EQ(restored->applied_bytes(), 0u);
  EXPECT_TRUE(CommittedEffects::FromSnapshot(10, cut, {}, {}).ok());
  slots.back().first = 43;
  EXPECT_FALSE(CommittedEffects::FromSnapshot(10, cut, {}, slots).ok());
}

TEST_F(SetEffectTest, FrozenSourceReplaysItsSealedImageAndRetiresTheOldGeneration) {
  auto record = Migration();
  ASSERT_TRUE(Append(first_).ok());
  ASSERT_TRUE(AppendMarker(TransferKind::kFreeze, record).ok());
  auto chunk = record;
  chunk.bytes.data = "unfinished serialization";
  ASSERT_TRUE(AppendMarker(TransferKind::kSourceChunk, chunk).ok());
  chunk.bytes.data = "restarted";
  ASSERT_TRUE(AppendMarker(TransferKind::kSourceChunk, chunk).ok());
  chunk.position = 9;
  chunk.bytes.data = string("\0data", 5);
  ASSERT_TRUE(AppendMarker(TransferKind::kSourceChunk, chunk).ok());
  auto seal = record;
  const string image("restarted\0data", 14);
  const auto hash = KvRdbDigest(image);
  seal.position = image.size();
  seal.digest.assign(hash.begin(), hash.end());
  ASSERT_TRUE(AppendMarker(TransferKind::kSeal, seal).ok());
  ASSERT_TRUE(AppendMarker(TransferKind::kFinish, record).ok());
  ASSERT_TRUE(AppendMarker(TransferKind::kFinish, record).ok());

  // A delayed SET from this generation is still fenced after cleanup and after reopening the log.
  ASSERT_TRUE(Append(first_).ok());
  log_.reset();
  log_ = PartitionLog::Open(dir_).value();
  CommittedEffects state(record.source_group);
  int writes = 0, clears = 0;
  auto apply = [&](const SetEffect&) {
    ++writes;
    return absl::OkStatus();
  };
  auto clear = [&](SlotId slot, optional<string_view> blob, string_view digest) {
    EXPECT_EQ(slot, record.slot);
    EXPECT_FALSE(blob);
    EXPECT_TRUE(digest.empty());
    ++clears;
    return absl::OkStatus();
  };
  ASSERT_TRUE(state.ApplyTo(*log_, 6, apply, clear).ok());
  const auto& transfers = state.transfers();
  const auto* progress = transfers.Find(record.migration_id);
  ASSERT_NE(progress, nullptr);
  EXPECT_EQ(progress->bytes, image);
  EXPECT_EQ(progress->digest, seal.digest);
  EXPECT_EQ(progress->freeze, (LogPosition{3, 2}));
  EXPECT_EQ(progress->seal, (LogPosition{3, 6}));
  EXPECT_TRUE(transfers.BlocksAccess(record.slot, record.generation));
  EXPECT_TRUE(transfers.BlocksAccess(record.slot, record.generation + 1));
  EXPECT_EQ(clears, 0);
  const auto cut = state.applied_position();
  auto sealed = state.SnapshotMigrations(cut);
  ASSERT_TRUE(sealed.ok()) << sealed.status();
  auto slots = state.SnapshotSlots();
  EXPECT_EQ(slots, (vector<SnapshotSlotRange>{{42, 42, 7}}));
  auto copy = CommittedEffects::FromSnapshot(10, cut, *sealed, slots);
  ASSERT_TRUE(copy.ok()) << copy.status();
  EXPECT_EQ(copy->transfers().Find(record.migration_id)->bytes, image);
  EXPECT_EQ(copy->SnapshotMigrations(cut).value(), *sealed);
  EXPECT_FALSE(CommittedEffects::FromSnapshot(20, cut, *sealed, slots).ok());
  EXPECT_FALSE(CommittedEffects::FromSnapshot(10, {3, 5}, *sealed, slots).ok());
  EXPECT_FALSE(CommittedEffects::FromSnapshot(10, cut, *sealed + "extra", slots).ok());
  sealed->back() ^= 1;
  EXPECT_FALSE(CommittedEffects::FromSnapshot(10, cut, *sealed, slots).ok());
  sealed->pop_back();
  EXPECT_FALSE(CommittedEffects::FromSnapshot(10, cut, *sealed, slots).ok());
  ASSERT_TRUE(state.ApplyTo(*log_, 8, apply, clear).ok());
  EXPECT_EQ(progress->finish, (LogPosition{3, 7}));
  EXPECT_TRUE(progress->bytes.empty());
  EXPECT_TRUE(transfers.BlocksAccess(record.slot, record.generation));
  EXPECT_FALSE(transfers.BlocksAccess(record.slot, record.generation + 1));
  EXPECT_FALSE(state.ApplyTo(*log_, 9, apply, clear).ok());
  EXPECT_EQ(state.applied_offset(), 8);
  EXPECT_EQ(writes, 1);
  EXPECT_EQ(clears, 1);

  // The retired generation must survive prefix deletion even though its image has been freed.
  auto retired = state.SnapshotMigrations(state.applied_position());
  ASSERT_TRUE(retired.ok());
  EXPECT_TRUE(state.SnapshotSlots().empty());
  auto restored = CommittedEffects::FromSnapshot(10, {3, 8}, *retired, {});
  ASSERT_TRUE(restored.ok()) << restored.status();
  EXPECT_TRUE(restored->transfers().BlocksAccess(record.slot, record.generation));
  EXPECT_FALSE(restored->ApplyTo(*log_, 9, apply, clear).ok());
  EXPECT_EQ(clears, 1);
  EXPECT_EQ(writes, 1);
}

TEST_F(SetEffectTest, TargetOnlyExposesGenerationAfterSuccessfulInstall) {
  auto record = Migration();
  auto chunk = record;
  chunk.source_epoch = 2;
  chunk.source_end_offset = 17;
  chunk.bytes.data = string("bin\0ary", 7);
  ASSERT_TRUE(AppendMarker(TransferKind::kTargetChunk, chunk).ok());
  auto install = chunk;
  auto hash = KvRdbDigest(*chunk.bytes.data);
  install.digest.assign(hash.begin(), hash.end());
  install.position = chunk.bytes.data->size();
  install.bytes.data.reset();
  ASSERT_TRUE(AppendMarker(TransferKind::kInstall, install).ok());
  ASSERT_TRUE(AppendMarker(TransferKind::kInstall, install).ok());
  SetEffect incoming = first_;
  incoming.group_id = record.target_group;
  ++incoming.generation;
  ASSERT_TRUE(Append(incoming).ok());
  ASSERT_TRUE(AppendMarker(TransferKind::kFinish, record).ok());
  log_.reset();
  log_ = PartitionLog::Open(dir_).value();
  CommittedEffects state(record.target_group);
  string value;
  auto apply = [&](const SetEffect& effect) {
    EXPECT_EQ(value, *chunk.bytes.data);
    value = effect.value;
    return absl::OkStatus();
  };
  int installs = 0;
  auto load = [&](SlotId slot, optional<string_view> blob, string_view digest) {
    EXPECT_EQ(slot, record.slot);
    EXPECT_EQ(blob, chunk.bytes.data);
    EXPECT_EQ(digest, hash);
    if (installs++ == 0)
      return absl::UnavailableError("recoverable RDB install failure");
    value.assign(*blob);
    return absl::OkStatus();
  };
  EXPECT_FALSE(state.ApplyTo(*log_, 5, apply, load).ok());
  EXPECT_EQ(state.applied_offset(), 1);
  auto staged = state.SnapshotMigrations(state.applied_position());
  ASSERT_TRUE(staged.ok());
  EXPECT_TRUE(state.SnapshotSlots().empty());
  auto restored = CommittedEffects::FromSnapshot(record.target_group, {3, 1}, *staged, {});
  ASSERT_TRUE(restored.ok()) << restored.status();
  state = *std::move(restored);
  const auto* p = state.transfers().Find(record.migration_id);
  ASSERT_NE(p, nullptr);
  EXPECT_EQ(p->install.end_offset, 0);
  EXPECT_TRUE(state.transfers().BlocksAccess(record.slot, incoming.generation));
  ASSERT_TRUE(state.ApplyTo(*log_, 3, apply, load).ok());
  EXPECT_EQ(p->install, (LogPosition{3, 2}));
  EXPECT_EQ(p->source, (LogPosition{2, 17}));
  EXPECT_FALSE(state.transfers().BlocksAccess(record.slot, incoming.generation));
  EXPECT_TRUE(state.transfers().BlocksAccess(record.slot, record.generation));
  auto slots = state.SnapshotSlots();
  EXPECT_EQ(slots, (vector<SnapshotSlotRange>{{42, 42, incoming.generation}}));
  auto installed = state.SnapshotMigrations(state.applied_position());
  ASSERT_TRUE(installed.ok());
  restored = CommittedEffects::FromSnapshot(record.target_group, {3, 3}, *installed, slots);
  ASSERT_TRUE(restored.ok()) << restored.status();
  state = *std::move(restored);
  ASSERT_TRUE(state.ApplyTo(*log_, 5, apply, load).ok());
  EXPECT_EQ(installs, 2);
  EXPECT_EQ(value, incoming.value);
}

TEST_F(SetEffectTest, CancelUnfreezesSourceAndDoesNotClearAnotherTransfer) {
  auto record = Migration();
  ASSERT_TRUE(AppendMarker(TransferKind::kFreeze, record).ok());
  auto cancelled = record;
  cancelled.migration_id[15] = 2;
  ASSERT_TRUE(AppendMarker(TransferKind::kCancel, cancelled).ok());
  ASSERT_TRUE(AppendMarker(TransferKind::kCancel, record).ok());
  ASSERT_TRUE(Append(first_).ok());
  CommittedEffects state(record.source_group);
  auto apply = [](const SetEffect&) { return absl::OkStatus(); };
  ASSERT_TRUE(state.ApplyTo(*log_, 2, apply).ok());
  auto saved = state.SnapshotMigrations(state.applied_position());
  ASSERT_TRUE(saved.ok());
  auto restored = CommittedEffects::FromSnapshot(record.source_group, {3, 2}, *saved, {});
  ASSERT_TRUE(restored.ok()) << restored.status();
  EXPECT_TRUE(restored->transfers().Find(cancelled.migration_id)->cancelled);
  state = *std::move(restored);
  EXPECT_TRUE(state.transfers().BlocksAccess(record.slot, record.generation));
  ASSERT_TRUE(state.ApplyTo(*log_, 4, apply).ok());
  EXPECT_FALSE(state.transfers().BlocksAccess(record.slot, record.generation));
  ASSERT_TRUE(state.transfers().Find(record.migration_id)->cancelled);
}

TEST_F(SetEffectTest, CorruptTargetImageCannotInstallAndCancelDiscardsOnlyItsOwnKeys) {
  auto record = Migration();
  auto chunk = record;
  chunk.source_epoch = 1;
  chunk.source_end_offset = 18;
  chunk.bytes.data = "abc";
  ASSERT_TRUE(AppendMarker(TransferKind::kTargetChunk, chunk).ok());
  auto install = chunk;
  install.bytes.data.reset();
  install.position = 3;
  auto hash = KvRdbDigest("abc");
  install.digest.assign(hash.begin(), hash.end());
  auto wrong = install;
  ++wrong.source_end_offset;
  ASSERT_TRUE(AppendMarker(TransferKind::kInstall, wrong).ok());
  CommittedEffects state(record.target_group);
  auto noop = [](const SetEffect&) { return absl::OkStatus(); };
  int installs = 0, clears = 0;
  auto load = [&](SlotId, optional<string_view> blob, string_view) {
    if (blob)
      ++installs;
    else
      ++clears;
    return absl::OkStatus();
  };
  EXPECT_FALSE(state.ApplyTo(*log_, 2, noop, load).ok());
  EXPECT_EQ(state.applied_offset(), 1);
  ASSERT_TRUE(log_->TruncateTo(1, 1).ok());
  wrong = install;
  wrong.digest[0] ^= 1;
  ASSERT_TRUE(AppendMarker(TransferKind::kInstall, wrong).ok());
  EXPECT_FALSE(state.ApplyTo(*log_, 2, noop, load).ok());
  EXPECT_EQ(state.applied_offset(), 1);
  EXPECT_EQ(installs, 0);
  ASSERT_TRUE(log_->TruncateTo(1, 1).ok());
  ASSERT_TRUE(AppendMarker(TransferKind::kInstall, install).ok());
  ASSERT_TRUE(AppendMarker(TransferKind::kCancel, record).ok());

  // Retry the same slot generation under a fresh operation ID, then replay the old cancellation.
  chunk.migration_id[15] = 2;
  install.migration_id = chunk.migration_id;
  ASSERT_TRUE(AppendMarker(TransferKind::kTargetChunk, chunk).ok());
  ASSERT_TRUE(AppendMarker(TransferKind::kInstall, install).ok());
  ASSERT_TRUE(AppendMarker(TransferKind::kCancel, record).ok());
  ASSERT_TRUE(state.ApplyTo(*log_, 6, noop, load).ok());
  EXPECT_EQ(clears, 1);
  EXPECT_EQ(installs, 2);
  EXPECT_FALSE(state.transfers().BlocksAccess(record.slot, record.generation + 1));
}

}  // namespace
}  // namespace dfly::cluster::raft
