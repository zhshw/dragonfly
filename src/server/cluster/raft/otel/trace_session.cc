// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/otel/trace_session.h"

#include <absl/strings/ascii.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_split.h>
#include <unistd.h>

#include <cstdlib>

#include "util/fibers/proactor_base.h"

namespace dfly::cluster::raft::otel {
namespace {
using namespace std;

string Env(const char* name) {
  auto* value = getenv(name);
  return value ? value : "";
}

absl::StatusOr<string> Authorization(string_view headers) {
  for (absl::string_view header : absl::StrSplit(headers, ',')) {
    const auto pos = header.find('=');
    if (pos == string_view::npos ||
        absl::AsciiStrToLower(absl::StripAsciiWhitespace(header.substr(0, pos))) != "authorization")
      continue;
    auto value = absl::StripAsciiWhitespace(header.substr(pos + 1));
    string decoded;
    auto hex = [](char c) -> int {
      if (c >= '0' && c <= '9')
        return c - '0';
      if (c >= 'a' && c <= 'f')
        return c - 'a' + 10;
      if (c >= 'A' && c <= 'F')
        return c - 'A' + 10;
      return -1;
    };
    for (size_t i = 0; i < value.size(); ++i) {
      if (value[i] != '%') {
        decoded += value[i];
      } else {
        if (i + 2 >= value.size() || hex(value[i + 1]) < 0 || hex(value[i + 2]) < 0)
          return absl::InvalidArgumentError("Invalid OTEL authorization encoding");
        decoded += char(hex(value[i + 1]) * 16 + hex(value[i + 2]));
        i += 2;
      }
    }
    if (decoded.empty() || decoded.find_first_of("\r\n") != string::npos)
      return absl::InvalidArgumentError("Invalid OTEL authorization header");
    return decoded;
  }
  return absl::InvalidArgumentError("OTEL_EXPORTER_OTLP_TRACES_HEADERS requires Authorization");
}

}  // namespace

TraceSession::TraceSession(int32_t node, util::fb2::ProactorBase* owner)
    : owner_(owner), node_(node) {
  process_ = absl::StrCat(node, ":", getpid(), ":", trace::Capture::MonotonicNow());
}

absl::StatusOr<unique_ptr<TraceSession>> TraceSession::FromEnvironment(
    int32_t node, util::fb2::ProactorBase* owner) {
  const auto id = Env("RAFT_TRACE_CAPTURE_ID");
  if (id.empty())
    return nullptr;
  if (!owner)
    return absl::InvalidArgumentError("Trace session requires an explicit proactor owner");
  auto session = unique_ptr<TraceSession>(new TraceSession(node, owner));
  const auto selected = Env("RAFT_TRACE_NODES");
  if (!selected.empty()) {
    for (absl::string_view part : absl::StrSplit(selected, ',')) {
      int32_t value;
      if (!absl::SimpleAtoi(part, &value) || value < 0)
        return absl::InvalidArgumentError("RAFT_TRACE_NODES must contain node IDs");
      session->selected_nodes_.push_back(value);
    }
    if (find(session->selected_nodes_.begin(), session->selected_nodes_.end(), node) ==
        session->selected_nodes_.end())
      return nullptr;
  }
  session->capture_id_ = id;
  session->endpoint_ = Env("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT");
  session->identity_.release = Env("RAFT_TRACE_RELEASE");
  session->identity_.binary_sha = Env("RAFT_TRACE_BINARY_SHA256");
  session->identity_.workload = Env("RAFT_TRACE_WORKLOAD");
  session->identity_.experiment = Env("RAFT_TRACE_EXPERIMENT");
  if (session->endpoint_.empty() || session->identity_.release.empty() ||
      session->identity_.binary_sha.empty() || session->identity_.workload.empty() ||
      session->identity_.experiment.empty())
    return absl::InvalidArgumentError(
        "Raft trace requires endpoint, release, binary SHA and workload identity");
  auto auth = Authorization(Env("OTEL_EXPORTER_OTLP_TRACES_HEADERS"));
  if (!auth.ok())
    return auth.status();
  session->authorization_ = *std::move(auth);
  return session;
}

void TraceSession::SetExpectedNodes(const vector<int32_t>& nodes) {
  vector<int32_t> selected;
  for (auto node : nodes) {
    if (selected_nodes_.empty() ||
        find(selected_nodes_.begin(), selected_nodes_.end(), node) != selected_nodes_.end())
      selected.push_back(node);
  }
  identity_.expected_nodes = absl::StrJoin(selected, ",");
}

shared_ptr<trace::Capture> TraceSession::Create(string group) {
  return owner_->Await([&]() -> shared_ptr<trace::Capture> {
    // At most 16 streams of 32 MiB, including one group stream. Dropped streams invalidate
    // manifests.
    if (captures_.size() >= 16) {
      ++identity_.dropped_streams;
      return nullptr;
    }
    auto capture =
        make_shared<trace::Capture>(capture_id_, process_, std::move(group), node_, 32U << 20);
    captures_.push_back(capture);
    return capture;
  });
}

absl::Status TraceSession::Export() {
  return owner_->Await([&] {
    identity_.expected_streams = captures_.size();
    for (const auto& capture : captures_) {
      auto status = ExportTrace(*capture, identity_, [&](string_view payload) {
        absl::Status last;
        for (int attempt = 0; attempt < 3; ++attempt) {
          last = SendTrace(endpoint_, authorization_, payload);
          if (last.ok() || last.code() != absl::StatusCode::kUnavailable)
            break;
        }
        return last;
      });
      if (!status.ok())
        return status;
    }
    return absl::OkStatus();
  });
}

}  // namespace dfly::cluster::raft::otel
