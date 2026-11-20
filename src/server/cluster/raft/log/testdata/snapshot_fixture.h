// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/base/internal/endian.h>
#include <absl/strings/escaping.h>

#include <cstdlib>

#include "server/cluster/raft/log/record_batch.h"

namespace dfly::cluster::raft::test {

// SnapshotVectors.java uses Kafka MemoryRecords to produce these envelope batches.
// The leader epoch is outside the record batch CRC; change it for per-epoch fixtures.
inline std::string SnapshotBytes(int32_t epoch) {
  std::string header, footer;
  if (!absl::HexStringToBytes(
          "0000000000000000000000470000000202b329dae1002000000000000000000000007b000000000000007b"
          "ffffffffffffffffffffffffffff000000012a000000080000000316000000000000000000630000",
          &header) ||
      !absl::HexStringToBytes(
          "00000000000000020000003f00000002024097ddc7002000000000000000000000007b000000000000007b"
          "ffffffffffffffffffffffffffff000000011a00000008000000040600000000",
          &footer))
    std::abort();
  absl::big_endian::Store32(header.data() + 12, epoch);
  absl::big_endian::Store32(footer.data() + 12, epoch);
  const std::array<std::string, 1> record{"metadata"};
  return header + MakeDataBatch(1, epoch, 123, record).value() + footer;
}

}  // namespace dfly::cluster::raft::test
