// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/functional/function_ref.h>
#include <absl/status/status.h>

#include <string>
#include <string_view>

namespace io {
class Sink;
}

namespace dfly {
class Service;
namespace cluster {
class SlotRanges;
class SlotSet;
namespace raft {

// Call on a proactor with group apply stopped at K. on_cut runs after all participating
// shards have installed their copy-before-write callbacks, allowing apply to resume.
absl::Status SaveKvRdb(Service& service, const SlotSet& slots, io::Sink* destination,
                       absl::FunctionRef<void()> on_cut);

// The exact bytes of the transferred RDB are bound into the group log by their 32-byte SHA256.
std::string KvRdbDigest(std::string_view bytes);

// Validate the digest and the complete key-only RDB before changing the target. Hold the
// slot/group service fence throughout replacement and through any retry following a load error.
absl::Status InstallKvRdb(Service* service, const SlotRanges& ranges, std::string_view bytes,
                          std::string_view expected_digest);

// Validate against the slots materialized at the cut without modifying any database.
absl::Status ValidateKvRdb(Service& service, const SlotSet& slots, std::string_view bytes);

// Replace the complete contents of a single-group data process after validating its snapshot.
absl::Status RestoreGroupKvRdb(Service& service, const SlotSet& slots, std::string_view bytes);

// Wait until all old entries in the selected slots and databases have been removed.
void ClearKvSlots(Service& service, const SlotRanges& ranges);

}  // namespace raft
}  // namespace cluster
}  // namespace dfly
