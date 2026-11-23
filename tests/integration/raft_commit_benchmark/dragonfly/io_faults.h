// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <unistd.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <string>

namespace raft_io_faults {

// Compiled only into the --faults test binary. Each control file targets one data node.
inline void Observe(const char* stage, const std::string& file, int64_t end) {
  const char* directory = std::getenv("RAFT_IO_FAULT_DIR");
  if (!directory || file.find("__dragonfly_data-10/") == std::string::npos)
    return;
  auto node = std::filesystem::path(file).parent_path().parent_path().filename().string();
  const auto prefix = std::string(directory) + "/";
  if (std::string_view(stage) == "fsync_issue") {
    auto control = prefix + "hold-" + node + (file.ends_with(".log") ? "-segment" : "-marker");
    if (access(control.c_str(), F_OK) == 0) {
      if (auto* entered = std::fopen((control + ".entered").c_str(), "w"))
        std::fclose(entered);
      while (access(control.c_str(), F_OK) == 0)
        usleep(1000);
    }
  } else if (std::string_view(stage) == "marker_end") {
    auto path = prefix + "published-" + node;
    auto temporary = path + ".tmp";
    if (auto* out = std::fopen(temporary.c_str(), "w")) {
      std::fprintf(out, "%ld\n", end);
      std::fclose(out);
      std::rename(temporary.c_str(), path.c_str());
    }
  }
}

}  // namespace raft_io_faults
