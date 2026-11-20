// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/cluster/raft/metadata/metadata_image.h"
#include "server/cluster/raft/quorum/membership_history.h"

namespace dfly::cluster::raft {

// Write a complete image at its committed batch boundary K. Include membership effective
// at K-1 even when the log contains a more recent, uncommitted voter change.
[[nodiscard]] absl::Status WriteMetadataSnapshot(const MetadataImage& image,
                                                 const MembershipHistory& membership,
                                                 int64_t high_watermark, int64_t timestamp_ms,
                                                 PartitionLog* log);

}  // namespace dfly::cluster::raft
