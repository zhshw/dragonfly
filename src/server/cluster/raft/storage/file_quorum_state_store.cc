// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/storage/file_quorum_state_store.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/escaping.h>
#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <jsoncons/json.hpp>

namespace dfly::cluster::raft {
namespace {
using namespace std;
using Json = jsoncons::ojson;

error_code LastError() {
  return {errno, generic_category()};
}

error_code UnlinkIfExists(const filesystem::path& path) {
  if (unlink(path.c_str()) == -1 && errno != ENOENT)
    return LastError();
  return {};
}

error_code SyncParent(const filesystem::path& path) {
  auto parent = path.parent_path();
  int fd = open(parent.empty() ? "." : parent.c_str(), O_RDONLY | O_CLOEXEC | O_DIRECTORY);
  if (fd == -1)
    return LastError();
  absl::Cleanup close_dir = [fd] { close(fd); };
  return fsync(fd) == -1 ? LastError() : error_code{};
}

bool ReadDirectoryId(const Json& node, DirectoryId* id) {
  if (!node.is_string())
    return false;
  string_view encoded = node.as_string_view();

  // Kafka accepts URL-safe base64 with or without its two trailing padding bytes.
  if (encoded.size() == 24 && encoded.ends_with("=="))
    encoded.remove_suffix(2);
  if (encoded.size() != 22 ||
      encoded.find_first_not_of(
          "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_") != string_view::npos)
    return false;
  string bytes;
  if (!absl::WebSafeBase64Unescape(encoded, &bytes) || bytes.size() != id->size())
    return false;
  copy(bytes.begin(), bytes.end(), id->begin());
  return true;
}

error_code DecodeState(string_view line, optional<ElectionState>* result) {
  try {
    Json root = Json::parse(line);
    const Json& version_node = root.at("data_version");
    if (!version_node.is<int16_t>())
      return make_error_code(errc::bad_message);
    int16_t version = version_node.as<int16_t>();
    if (version != 0 && version != 1)
      return make_error_code(errc::protocol_not_supported);

    ElectionState state;
    if (!root.at("leaderEpoch").is<int32_t>() || !root.at("leaderId").is<int32_t>() ||
        !root.at("votedId").is<int32_t>())
      return make_error_code(errc::bad_message);
    state.epoch = root.at("leaderEpoch").as<int32_t>();
    int32_t leader = root.at("leaderId").as<int32_t>();
    int32_t voted = root.at("votedId").as<int32_t>();
    if (leader < -1 || voted < -1)
      return make_error_code(errc::bad_message);
    if (leader != -1)
      state.leader_id = leader;
    DirectoryId directory{};
    if (version == 0) {
      if (!root.at("clusterId").is_string() || !root.at("appliedOffset").is<int64_t>() ||
          !root.at("currentVoters").is_array())
        return make_error_code(errc::bad_message);
      for (const auto& voter : root.at("currentVoters").array_range()) {
        if (!voter.at("voterId").is<int32_t>())
          return make_error_code(errc::bad_message);
        state.voters.insert(voter.at("voterId").as<int32_t>());
      }
    } else if (!ReadDirectoryId(root.at("votedDirectoryId"), &directory)) {
      return make_error_code(errc::bad_message);
    }
    if (voted != -1)
      state.voted_key = ReplicaKey{voted, directory};
    *result = std::move(state);
    return {};
  } catch (const jsoncons::json_exception&) {
    return make_error_code(errc::bad_message);
  }
}

string EncodeState(const ElectionState& state, int16_t version) {
  Json root;
  if (version == 0)
    root["clusterId"] = "";
  root["leaderId"] = state.leader_id.value_or(-1);
  root["leaderEpoch"] = state.epoch;
  root["votedId"] = state.voted_key ? state.voted_key->id : -1;
  if (version == 0) {
    root["appliedOffset"] = 0;
    Json voters(jsoncons::json_array_arg);
    for (int32_t id : state.voters) {
      Json voter;
      voter["voterId"] = id;
      voters.push_back(std::move(voter));
    }
    root["currentVoters"] = std::move(voters);
  } else {
    const DirectoryId directory = state.voted_key ? state.voted_key->directory_id : DirectoryId{};
    root["votedDirectoryId"] = absl::WebSafeBase64Escape(
        string_view(reinterpret_cast<const char*>(directory.data()), directory.size()));
  }
  root["data_version"] = version;
  return root.to_string();
}

}  // namespace

std::error_code FileQuorumStateStore::ReadElectionState(std::optional<ElectionState>* state) const {
  FILE* file = fopen(path_.c_str(), "r");
  if (!file) {
    if (errno != ENOENT)
      return LastError();
    state->reset();
    return {};
  }
  absl::Cleanup close_file = [file] { fclose(file); };

  // FileQuorumStateStore reads the first UTF-8 JSON line of the installed file.
  char* line = nullptr;
  size_t capacity = 0;
  absl::Cleanup free_line = [&] { free(line); };
  ssize_t length = getline(&line, &capacity, file);
  if (length < 0)
    return ferror(file) ? LastError() : make_error_code(errc::bad_message);
  return DecodeState(string_view(line, length), state);
}

std::error_code FileQuorumStateStore::WriteElectionState(const ElectionState& state,
                                                         int16_t kraft_version) const {
  if (kraft_version != 0 && kraft_version != 1)
    return make_error_code(errc::protocol_not_supported);
  if ((state.leader_id && *state.leader_id < 0) || (state.voted_key && state.voted_key->id < 0))
    return make_error_code(errc::invalid_argument);

  const string bytes = EncodeState(state, kraft_version);
  const string temp = path_.string() + ".tmp";
  if (auto ec = UnlinkIfExists(temp); ec)
    return ec;
  int fd = open(temp.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
  if (fd == -1)
    return LastError();
  absl::Cleanup unlink_temp = [&] { unlink(temp.c_str()); };
  absl::Cleanup close_fd = [fd] { close(fd); };
  string_view remaining = bytes;
  while (!remaining.empty()) {
    ssize_t written = write(fd, remaining.data(), remaining.size());
    if (written == -1 && errno == EINTR)
      continue;
    if (written < 0)
      return LastError();
    if (written == 0)
      return make_error_code(errc::io_error);
    remaining.remove_prefix(written);
  }
  if (fsync(fd) == -1)
    return LastError();
  std::move(close_fd).Cancel();
  if (close(fd) == -1 || rename(temp.c_str(), path_.c_str()) == -1)
    return LastError();
  std::move(unlink_temp).Cancel();
  return SyncParent(path_);
}

std::error_code FileQuorumStateStore::Clear() const {
  if (auto ec = UnlinkIfExists(path_); ec)
    return ec;
  if (auto ec = UnlinkIfExists(path_.string() + ".tmp"); ec)
    return ec;
  return SyncParent(path_);
}

}  // namespace dfly::cluster::raft
