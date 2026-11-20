// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <charconv>
#include <iostream>

#include "server/cluster/raft/bootstrap/raft_directory.h"

int main(int argc, char** argv) {
  using namespace dfly::cluster::raft;
  using namespace std;
  if ((argc != 5 && argc != 6) ||
      (string_view(argv[1]) != "format" && string_view(argv[1]) != "verify")) {
    cerr << "Usage: raft_storage format|verify DIRECTORY CLUSTER_ID NODE_ID [DIRECTORY_ID]\n";
    return 2;
  }
  const string_view node_arg = argv[4];
  RaftDirectoryConfig config{argv[3], 0, nullopt};
  auto [end, error] =
      from_chars(node_arg.data(), node_arg.data() + node_arg.size(), config.node_id);
  if (error != errc{} || end != node_arg.data() + node_arg.size() || config.node_id < 0) {
    cerr << "NODE_ID must be a non-negative 32-bit integer\n";
    return 2;
  }
  if (argc == 6) {
    auto id = ParseDirectoryId(argv[5]);
    if (!id.ok()) {
      cerr << id.status() << '\n';
      return 2;
    }
    config.directory_id = *id;
  }
  if (string_view(argv[1]) == "verify") {
    auto status = VerifyRaftDirectory(argv[2], config);
    if (!status.ok())
      cerr << status << '\n';
    return status.ok() ? 0 : 1;
  }
  auto result = FormatRaftDirectory(argv[2], config);
  if (!result.ok()) {
    cerr << result.status() << '\n';
    return 1;
  }
  cout << "directory.id=" << FormatDirectoryId(result->directory_id) << '\n';
  return 0;
}
