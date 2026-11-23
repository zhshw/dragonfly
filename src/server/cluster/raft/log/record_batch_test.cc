// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/log/record_batch.h"

#include <absl/base/internal/endian.h>
#include <absl/crc/crc32c.h>
#include <absl/strings/escaping.h>
#include <gtest/gtest.h>

#include "server/cluster/raft/log/testdata/snapshot_fixture.h"

namespace dfly::cluster::raft {
namespace {
using namespace std;

// testdata/BatchVectors.java emits these complete batches from Kafka MemoryRecords.
constexpr string_view kJavaLeader =
    "00000000000000000000005e0000000202dc87af1b002000000000000000000000007b000000000000007bff"
    "ffffffffffffffffffffffffff00000001580000000800000002440000000000010400000001000000000200"
    "000000030003000000010000000002000000";
constexpr string_view kJavaData =
    "0000000000000001000000450000000202647a5ea500000000000100000000000001c800000000000001c8ff"
    "ffffffffffffffffffffffffff00000002120000000106666f6f0012000002010662617200";

TEST(RecordBatchTest, MatchesKafkaLeaderChangeAndBasicRecords) {
  const array<int32_t, 3> voters{1, 2, 3};
  auto leader = MakeLeaderChangeBatch(0, 2, 123, 1, voters, span(voters).first(2));
  ASSERT_TRUE(leader.ok()) << leader.status();
  EXPECT_EQ(absl::BytesToHexString(*leader), kJavaLeader);
  const array<string, 2> values{"foo", "bar"};
  auto data = MakeDataBatch(1, 2, 456, values);
  ASSERT_TRUE(data.ok()) << data.status();
  EXPECT_EQ(absl::BytesToHexString(*data), kJavaData);
  vector<RecordView> records;
  auto info = DecodeRecordBatch(*data, &records);
  ASSERT_TRUE(info.ok()) << info.status();
  EXPECT_EQ(info->end, (LogPosition{2, 3}));
  ASSERT_EQ(records.size(), values.size());
  for (size_t i = 0; i < values.size(); ++i) {
    EXPECT_EQ(records[i].offset, i + 1);
    EXPECT_FALSE(records[i].key);
    EXPECT_EQ(records[i].value, values[i]);
  }
  EXPECT_TRUE(DecodeRecordBatch(*leader)->control);
}

TEST(RecordBatchTest, SplitDataValueMatchesContiguousEncoding) {
  for (size_t size : {0u, 1u, 55u, 56u, 63u, 64u, 8191u, 8192u, 262144u,
                      unsigned(kMaxBatchSize - kBatchHeaderSize - 13)}) {
    SCOPED_TRACE(size);
    array<string, 1> values{string(size, '\0')};
    for (size_t i = 0; i < size; ++i)
      values.front()[i] = static_cast<char>(i % 251);
    const string_view value = values.front();
    const array<string_view, 4> parts{value.substr(0, size / 3),
                                      {},
                                      value.substr(size / 3, size / 3),
                                      value.substr(2 * (size / 3))};
    auto batch = MakeSingleRecordBatch(7, 2, 123, parts);
    ASSERT_TRUE(batch.ok()) << batch.status();
    ASSERT_EQ(batch, MakeDataBatch(7, 2, 123, values));
    auto planned = SingleRecordBatchSize(size);
    ASSERT_TRUE(planned.ok()) << planned.status();
    ASSERT_EQ(*planned, batch->size());
    string aggregate = "existing prefix";
    const size_t begin = aggregate.size();
    aggregate.reserve(begin + 2 * batch->size());
    const char* storage = aggregate.data();
    ASSERT_TRUE(AppendSingleRecordBatch(7, 2, 123, parts, &aggregate).ok());
    ASSERT_TRUE(AppendSingleRecordBatch(7, 2, 123, parts, &aggregate).ok());
    EXPECT_EQ(aggregate.data(), storage);
    EXPECT_EQ(string_view(aggregate).substr(begin), *batch + *batch);
    EXPECT_EQ(string_view(aggregate).substr(0, begin), "existing prefix");
    vector<RecordView> records;
    auto info = DecodeRecordBatch(*batch, &records);
    ASSERT_TRUE(info.ok()) << info.status();
    EXPECT_EQ(info->end, (LogPosition{2, 8}));
    ASSERT_EQ(records.size(), 1u);
    EXPECT_FALSE(records.front().key);
    EXPECT_EQ(records.front().value, value);
    if (size == kMaxBatchSize - kBatchHeaderSize - 13) {
      EXPECT_EQ(batch->size(), kMaxBatchSize);
    }
  }
  const string large(kMaxBatchSize - kBatchHeaderSize - 12, 'v');
  const array<string_view, 1> oversized{large};
  EXPECT_FALSE(MakeSingleRecordBatch(0, 2, 123, oversized).ok());
  EXPECT_FALSE(MakeSingleRecordBatch(-1, 2, 123, {}).ok());
  string unchanged = "prior batches";
  EXPECT_FALSE(AppendSingleRecordBatch(-1, 2, 123, {}, &unchanged).ok());
  EXPECT_FALSE(AppendSingleRecordBatch(INT64_MAX, 2, 123, {}, &unchanged).ok());
  EXPECT_FALSE(AppendSingleRecordBatch(0, -1, 123, {}, &unchanged).ok());
  EXPECT_FALSE(AppendSingleRecordBatch(0, 2, -1, {}, &unchanged).ok());
  EXPECT_FALSE(AppendSingleRecordBatch(0, 2, 123, oversized, &unchanged).ok());
  EXPECT_FALSE(SingleRecordBatchSize(SIZE_MAX).ok());
  EXPECT_EQ(unchanged, "prior batches");
}

TEST(RecordBatchTest, RejectsBadCrcAndMalformedRecordsEvenWithValidCrc) {
  string good;
  ASSERT_TRUE(absl::HexStringToBytes(kJavaData, &good));
  for (size_t length = 0; length < good.size(); ++length)
    EXPECT_FALSE(DecodeRecordBatch(string_view(good).substr(0, length)).ok());
  string bad = good;
  bad.back() ^= 1;
  EXPECT_FALSE(DecodeRecordBatch(bad).ok());
  for (size_t pos : {size_t{61}, size_t{63}, size_t{64}, size_t{65}, good.size() - 1}) {
    bad = good;
    bad[pos] = static_cast<char>(0xff);
    absl::big_endian::Store32(
        bad.data() + 17, static_cast<uint32_t>(absl::ComputeCrc32c(string_view(bad).substr(21))));
    EXPECT_FALSE(DecodeRecordBatch(bad).ok()) << pos;
  }
  ASSERT_TRUE(absl::HexStringToBytes(kJavaLeader, &bad));
  bad.back() = 127;
  absl::big_endian::Store32(
      bad.data() + 17, static_cast<uint32_t>(absl::ComputeCrc32c(string_view(bad).substr(21))));
  EXPECT_FALSE(DecodeRecordBatch(bad).ok());
}

TEST(RecordBatchTest, SnapshotEnvelopeMatchesJavaAndControlRecordsRetainOrder) {
  auto header = MakeSnapshotHeaderBatch(2, 123, 99);
  auto footer = MakeSnapshotFooterBatch(2, 2, 123);
  ASSERT_TRUE(header.ok());
  ASSERT_TRUE(footer.ok());
  const array<string, 1> value{"metadata"};
  EXPECT_EQ(*header + MakeDataBatch(1, 2, 123, value).value() + *footer, test::SnapshotBytes(2));

  const array<ControlRecordValue, 2> extra{{{5, "version"}, {6, "voters"}}};
  const array<int32_t, 1> voters{1};
  auto batch = MakeLeaderChangeBatch(7, 2, 123, 1, voters, voters, extra);
  ASSERT_TRUE(batch.ok()) << batch.status();
  vector<RecordView> records;
  auto info = DecodeRecordBatch(*batch, &records);
  ASSERT_TRUE(info.ok()) << info.status();
  ASSERT_EQ(records.size(), 3u);
  EXPECT_TRUE(info->control);
  EXPECT_EQ(info->end.end_offset, 10);
  for (size_t i = 0; i < extra.size(); ++i) {
    EXPECT_EQ(absl::big_endian::Load16(records[i + 1].key->data() + 2), extra[i].type);
    EXPECT_EQ(records[i + 1].value, extra[i].value);
  }
}

TEST(RecordBatchTest, LeaderChangeRequiresActualMajority) {
  const array<int32_t, 3> members{0, 1, 2};
  EXPECT_FALSE(MakeLeaderChangeBatch(0, 1, 1, 0, members, span(members).first(1)).ok());
  const array<int32_t, 2> repeated{0, 0}, foreign{0, 3};
  EXPECT_FALSE(MakeLeaderChangeBatch(0, 1, 1, 0, members, repeated).ok());
  EXPECT_FALSE(MakeLeaderChangeBatch(0, 1, 1, 0, members, foreign).ok());
  EXPECT_FALSE(MakeDataBatch(0, 1, 0, {}).ok());
}
}  // namespace
}  // namespace dfly::cluster::raft
