// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/runtime/admin_command.h"

#include <absl/base/internal/endian.h>
#include <absl/strings/escaping.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>

#include <charconv>
#include <jsoncons/json.hpp>
#include <limits>

#include "server/cluster/raft/metadata/metadata_image.h"

namespace dfly::cluster::raft {
namespace {
using namespace std;
using namespace protocol;
using Kind = AdminReply::Kind;
using Json = jsoncons::ojson;

constexpr string_view kRequests = "ctl:req:";
constexpr string_view kOperations = "ctl:op:";
constexpr string_view kSlots = "ctl:slot:";

AdminReply Error(string_view message) {
  return {Kind::kError, absl::StrCat("ERR ", message)};
}

template <typename T> optional<T> Number(const Json& object, const char* field) {
  if (!object.contains(field) || !object.at(field).template is<T>())
    return nullopt;
  return object.at(field).template as<T>();
}

absl::StatusOr<SlotMigrationRequest> ParseMigration(string_view value) {
  SlotMigrationRequest req;
  try {
    auto object = Json::parse(value);
    if (!object.is_object() || object.size() != 4)
      return absl::InvalidArgumentError("Expected an object with slot, from, to and generation");
    auto slot = Number<uint16_t>(object, "slot");
    auto source = Number<int32_t>(object, "from");
    auto target = Number<int32_t>(object, "to");
    auto generation = Number<int64_t>(object, "generation");
    if (!slot || *slot > kMaxSlotNum || !source || !target || !generation)
      return absl::InvalidArgumentError("Invalid slot, from, to or generation");
    req.slot = *slot;
    req.source_group = *source;
    req.target_group = *target;
    req.generation = *generation;
  } catch (const jsoncons::json_exception&) {
    return absl::InvalidArgumentError("Invalid migration request JSON");
  }
  return req;
}

Uuid AllocateMigrationId(const MetadataImage& image) {
  // Start at this stable log position. The suffix disambiguates IDs chosen
  // independently by internal RPC callers in the same migration table.
  Uuid id{};
  absl::big_endian::Store64(id.data(), image.stable_end_offset);
  for (uint64_t suffix = 1;; ++suffix) {
    absl::big_endian::Store64(id.data() + 8, suffix);
    if (!image.migrations.records().contains(id))
      return id;
  }
}

vector<MetadataRecord> SetRequest(const MetadataImage& image, string_view cluster_id,
                                  string_view request_id, string_view value, AdminReply* reply) {
  if (image.migrations.FindAdminRequest(request_id)) {
    *reply = {};
    return {};
  }

  // The flexible metadata codec bounds each string by the Kafka string limit.
  if (request_id.size() > numeric_limits<int16_t>::max() ||
      value.size() > numeric_limits<int16_t>::max()) {
    *reply = Error("Request ID or value exceeds the metadata string limit");
    return {};
  }
  auto req = ParseMigration(value);
  if (!req.ok()) {
    *reply = Error(req.status().message());
    return {};
  }
  req->cluster_id = cluster_id;
  req->action = static_cast<int8_t>(MigrationAction::kStart);
  req->migration_id = AllocateMigrationId(image);
  SlotMigrationResponse res;
  auto records = ProposeSlotMigration(image, cluster_id, *req, &res);
  if (res.error_code) {
    *reply = Error(res.error_message);
    return {};
  }

  // The start operation proposes one migration record. Its tagged request data
  // is retained by later phase transitions and the metadata snapshot writer.
  auto& stored = get<SlotMigrationRecord>(records.front().message).admin_request;
  stored.request_id = request_id;
  stored.value = value;
  *reply = {Kind::kSimple, "OK"};
  return records;
}

string_view PhaseName(const SlotMigrationRecord& record) {
  switch (static_cast<MigrationPhase>(record.phase)) {
    case MigrationPhase::kPreparing:
      return "preparing";
    case MigrationPhase::kFrozen:
      return "frozen";
    case MigrationPhase::kCutover:
      return record.source_finished && record.target_finished ? "finished" : "cutover";
    case MigrationPhase::kCancelled:
      return "cancelled";
  }
  return "unknown";
}

AdminReply Operation(const SlotMigrationRecord* record) {
  AdminReply reply{Kind::kMap};
  if (record) {
    string_view uuid(reinterpret_cast<const char*>(record->migration_id.data()),
                     record->migration_id.size());
    reply.elements = {"id",
                      record->admin_request.request_id,
                      "migration_id",
                      absl::BytesToHexString(uuid),
                      "slot",
                      to_string(record->slot),
                      "from",
                      to_string(record->source_group),
                      "to",
                      to_string(record->target_group),
                      "generation",
                      to_string(record->generation),
                      "phase",
                      string(PhaseName(*record)),
                      "source_finished",
                      record->source_finished ? "1" : "0",
                      "target_finished",
                      record->target_finished ? "1" : "0"};
  }
  return reply;
}

AdminReply SlotFields(const MetadataImage& image, string_view key, span<const string_view> fields) {
  SlotId slot;
  const char* end = key.data() + key.size();
  auto parsed = from_chars(key.data(), end, slot);
  if (parsed.ec != errc{} || parsed.ptr != end || slot > kMaxSlotNum)
    return Error("Invalid slot number");
  auto owner = image.directory.Lookup(slot);
  AdminReply reply{Kind::kArray};
  for (auto field : fields) {
    if (owner && field == "owner")
      reply.elements.emplace_back(to_string(owner->group_id));
    else if (owner && field == "generation")
      reply.elements.emplace_back(to_string(owner->generation));
    else
      reply.elements.emplace_back(nullopt);
  }
  return reply;
}
}  // namespace

vector<MetadataRecord> PlanAdminCommand(const MetadataImage& image, string_view cluster_id,
                                        span<const string_view> args, AdminReply* reply) {
  if (args.size() < 2) {
    *reply = Error("Expected a controller management command and key");
    return {};
  }
  auto key = args[1];
  if (absl::EqualsIgnoreCase(args[0], "SET")) {
    if (args.size() != 4 || !absl::EqualsIgnoreCase(args[3], "NX") || !key.starts_with(kRequests) ||
        key.size() == kRequests.size()) {
      *reply = Error("Expected SET ctl:req:<id> <json> NX");
      return {};
    }
    return SetRequest(image, cluster_id, key.substr(kRequests.size()), args[2], reply);
  }
  if (absl::EqualsIgnoreCase(args[0], "GET") && args.size() == 2 && key.starts_with(kRequests) &&
      key.size() > kRequests.size()) {
    auto request = image.migrations.FindAdminRequest(key.substr(kRequests.size()));
    *reply = request ? AdminReply{Kind::kBulk, request->admin_request.value} : AdminReply{};
  } else if (absl::EqualsIgnoreCase(args[0], "HGETALL") && args.size() == 2 &&
             key.starts_with(kOperations) && key.size() > kOperations.size()) {
    *reply = Operation(image.migrations.FindAdminRequest(key.substr(kOperations.size())));
  } else if (absl::EqualsIgnoreCase(args[0], "HMGET") && args.size() >= 3 &&
             key.starts_with(kSlots)) {
    *reply = SlotFields(image, key.substr(kSlots.size()), args.subspan(2));
  } else {
    *reply = Error("Unsupported controller management command, key or arguments");
  }
  return {};
}
}  // namespace dfly::cluster::raft
