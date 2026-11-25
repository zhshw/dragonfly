// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/otel/trace_exporter.h"

#include <gtest/gtest.h>
#include <opentelemetry/proto/collector/trace/v1/trace_service.pb.h>

namespace dfly::cluster::raft::otel {
namespace {
using namespace std;
using Request = opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest;

map<string, string> Attributes(const opentelemetry::proto::trace::v1::Span& span) {
  map<string, string> result;
  for (const auto& attr : span.attributes())
    result.emplace(attr.key(), attr.value().string_value());
  return result;
}

TEST(TraceExporterTest, PreservesIdentityBoundariesMissingValuesAndEvidence) {
  trace::Time now = 2;
  trace::Capture capture("unit", "process", "10", 2, 1U << 20, [&] { return now; });
  trace::FetchTrace fetch(capture, 1, 3, 7, 218);
  fetch.RpcStarted(now);
  now = 4;
  fetch.RpcReturned(now, 200, true);
  const auto io = capture.BeginFsync(123, "Log");
  fetch.CheckGate(trace::Gate::kIncompleteSuffix, 123);
  fetch.BeginIoWait(now);
  now = 17;
  capture.EndFsync(io, 0);
  fetch.EndIoWait(now);
  fetch.CheckGate(trace::Gate::kAllowed, 123);
  fetch.ReceiveStarted();
  fetch.Appended(now, 199, 218, 220, true);
  fetch.Finish("Data");
  TraceIdentity identity{"v15-trace", "binary-sha", "unit", 1, 0, "v15", "2"};
  const auto bytes = EncodeTraceBatch(capture, identity, 0, capture.events().size());
  EXPECT_EQ(bytes, EncodeTraceBatch(capture, identity, 0, capture.events().size()));
  Request request;
  ASSERT_TRUE(request.ParseFromString(bytes));
  ASSERT_EQ(request.resource_spans_size(), 1);
  const auto& spans = request.resource_spans(0).scope_spans(0).spans();
  ASSERT_EQ(spans.size(), capture.events().size());
  bool found = false, dependency = false, blocked = false, ready = false, fsync = false;
  for (const auto& span : spans) {
    const auto attrs = Attributes(span);
    blocked |= span.name() == "raft.fetch.flush_gate.blocked";
    ready |= span.name() == "raft.fetch.flush_gate.ready";
    fsync |= span.name() == "raft.folly.fsync";
    EXPECT_EQ(span.trace_id().size(), 16u);
    EXPECT_EQ(span.span_id().size(), 8u);
    if (span.name() == "raft.fetch") {
      found = true;
      EXPECT_EQ(attrs.at("langfuse.observation.metadata.value.raft.fetch.return_wait"), "0.000013");
      EXPECT_EQ(attrs.at("langfuse.observation.metadata.value.raft.fetch"), "0.000015");
      EXPECT_EQ(attrs.at("langfuse.observation.metadata.value.raft.fetch.appended_bytes"), "199");
      EXPECT_EQ(attrs.at("langfuse.observation.metadata.first_offset"), "218");
      EXPECT_EQ(span.start_time_unix_nano(), capture.wall_offset_ns() + 2);
      EXPECT_EQ(span.end_time_unix_nano(), capture.wall_offset_ns() + 17);
    }
    if (span.name() == "raft.fetch.io_dependency") {
      dependency = true;
      EXPECT_EQ(attrs.at("langfuse.observation.metadata.io_id"), to_string(io));
    }
  }
  EXPECT_TRUE(found && dependency && blocked && ready && fsync);

  trace::FetchTrace cancelled(capture, 1, 3, 8, 220);
  cancelled.RpcStarted(now);
  cancelled.Finish("Cancelled");
  Request missing;
  ASSERT_TRUE(
      missing.ParseFromString(EncodeTraceBatch(capture, identity, capture.events().size() - 1, 1)));
  auto attrs = Attributes(missing.resource_spans(0).scope_spans(0).spans(0));
  EXPECT_FALSE(attrs.contains("langfuse.observation.metadata.value.raft.fetch.return_wait"));
  EXPECT_EQ(attrs.at("langfuse.observation.metadata.state.raft.fetch.return_wait"), "Incomplete");
}

TEST(TraceExporterTest, PublishesManifestLastAndNeverMasksDroppedOrFailedBatches) {
  trace::Capture capture("unit", "process", "10", 2);
  for (size_t i = 0; i < 300; ++i)
    capture.Emit(capture.MakeEvent("point", i, i));
  size_t batches = 0;
  auto status = ExportTrace(capture, {}, [&](string_view bytes) {
    Request request;
    EXPECT_TRUE(request.ParseFromArray(bytes.data(), bytes.size()));
    const auto& spans = request.resource_spans(0).scope_spans(0).spans();
    if (++batches == 3) {
      EXPECT_EQ(spans.size(), 1);
      EXPECT_EQ(spans[0].name(), "raft.capture");
      auto attrs = Attributes(spans[0]);
      EXPECT_EQ(attrs.at("langfuse.observation.metadata.event_count"), "300");
      EXPECT_EQ(attrs.at("langfuse.observation.metadata.unit.raft.fetch.return_wait"), "ms");
      EXPECT_EQ(attrs.at("langfuse.observation.metadata.unit.raft.folly.fsync"), "ms");
      EXPECT_EQ(attrs.at("langfuse.observation.metadata.unit.raft.fetch.payload_bytes"), "bytes");
      EXPECT_EQ(attrs.at("langfuse.observation.metadata.outcome"), "Complete");
    }
    return absl::OkStatus();
  });
  EXPECT_TRUE(status.ok());
  EXPECT_EQ(batches, 3u);
  batches = 0;
  status = ExportTrace(capture, {}, [&](string_view) {
    ++batches;
    return absl::UnavailableError("test failure");
  });
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(batches, 1u);

  trace::Capture dropped("unit", "process", "10", 2, 1);
  dropped.Emit(dropped.MakeEvent("point", 1, 1));
  Request request;
  ASSERT_TRUE(request.ParseFromString(EncodeTraceBatch(dropped, {}, 0, 0)));
  auto attrs = Attributes(request.resource_spans(0).scope_spans(0).spans(0));
  EXPECT_EQ(attrs.at("langfuse.observation.metadata.outcome"), "Incomplete");
  EXPECT_EQ(attrs.at("langfuse.observation.metadata.dropped"), "1");
}

}  // namespace
}  // namespace dfly::cluster::raft::otel
