// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <charconv>
#include <iostream>

#include "server/cluster/raft/bootstrap/quorum_bootstrap.h"
#include "server/cluster/raft/bootstrap/raft_directory.h"

namespace {
bool ParseNumber(std::string_view arg, int32_t* number) {
  auto [end, ec] = std::from_chars(arg.data(), arg.data() + arg.size(), *number);
  return ec == std::errc{} && end == arg.data() + arg.size() && *number >= 0;
}

int Usage() {
  std::cerr << "Usage: raft_storage format|verify DIRECTORY CLUSTER_ID NODE_ID [DIRECTORY_ID]\n"
               "       format options: [--data-group POSITIVE_ID] "
               "(--initial-controllers id@host:port:directoryId,... | --no-initial-controllers)\n";
  return 2;
}
}  // namespace

int main(int argc, char** argv) {
  using namespace dfly::cluster::raft;
  using namespace std;
  if (argc < 5 || (string_view(argv[1]) != "format" && string_view(argv[1]) != "verify"))
    return Usage();
  RaftDirectoryConfig config{argv[3], 0, nullopt};
  if (!ParseNumber(argv[4], &config.node_id)) {
    cerr << "NODE_ID must be a non-negative 32-bit integer\n";
    return 2;
  }
  int pos = 5;
  if (pos < argc && !string_view(argv[pos]).starts_with("--")) {
    auto id = ParseDirectoryId(argv[pos++]);
    if (!id.ok()) {
      cerr << id.status() << '\n';
      return 2;
    }
    config.directory_id = *id;
  }
  if (string_view(argv[1]) == "verify") {
    if (pos != argc)
      return Usage();
    auto status = VerifyRaftDirectory(argv[2], config);
    if (!status.ok())
      cerr << status << '\n';
    return status.ok() ? 0 : 1;
  }
  optional<int32_t> group;
  optional<string_view> initial;
  bool dynamic = false;
  while (pos < argc) {
    string_view flag = argv[pos++];
    if (flag == "--data-group" && !group && pos < argc) {
      if (!ParseNumber(argv[pos++], &group.emplace()))
        return Usage();
    } else if (flag == "--initial-controllers" && !dynamic && pos < argc) {
      dynamic = true;
      initial = argv[pos++];
    } else if (flag == "--no-initial-controllers" && !dynamic) {
      dynamic = true;
    } else {
      return Usage();
    }
  }
  if (group && !dynamic)
    return Usage();
  optional<QuorumBootstrap> bootstrap;
  if (dynamic) {
    auto parsed = ParseQuorumBootstrap(initial, group);
    if (!parsed.ok()) {
      cerr << parsed.status() << '\n';
      return 2;
    }
    bootstrap = *std::move(parsed);
  }
  auto result = FormatRaftDirectory(argv[2], config, bootstrap ? &*bootstrap : nullptr);
  if (!result.ok()) {
    cerr << result.status() << '\n';
    return 1;
  }
  cout << "directory.id=" << FormatDirectoryId(result->directory_id) << '\n';
  return 0;
}
