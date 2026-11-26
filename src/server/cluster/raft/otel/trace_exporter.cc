// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/otel/trace_exporter.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>
#include <opentelemetry/exporters/otlp/otlp_recordable.h>
#include <opentelemetry/exporters/otlp/otlp_recordable_utils.h>
#include <opentelemetry/proto/collector/trace/v1/trace_service.pb.h>

#include <boost/beast/http/string_body.hpp>
#include <jsoncons/json.hpp>

#include "util/http/http_client.h"

namespace dfly::cluster::raft::otel {
namespace {
using namespace std;
namespace ot = opentelemetry;
using Recordable = ot::exporter::otlp::OtlpRecordable;
constexpr uint64_t kRoot = UINT64_MAX;

ot::trace::SpanId SpanId(uint64_t id, string_view stream) {
  if (id == 0)
    return ot::trace::SpanId();
  uint64_t prefix = 0;
  for (char c : stream.substr(0, 16))
    prefix = (prefix << 4) | (c <= '9' ? c - '0' : c - 'a' + 10);
  id ^= prefix;
  array<uint8_t, 8> bytes{};
  for (size_t i = 0; i < bytes.size(); ++i)
    bytes[7 - i] = id >> (i * 8);
  return ot::trace::SpanId(ot::nostd::span<const uint8_t, 8>(bytes.data(), bytes.size()));
}

ot::trace::TraceId TraceId(string_view hex) {
  array<uint8_t, 16> bytes{};
  auto digit = [](char c) { return c <= '9' ? c - '0' : c - 'a' + 10; };
  for (size_t i = 0; i < bytes.size(); ++i)
    bytes[i] = digit(hex[i * 2]) * 16 + digit(hex[i * 2 + 1]);
  return ot::trace::TraceId(ot::nostd::span<const uint8_t, 16>(bytes.data(), bytes.size()));
}

void Attribute(Recordable* span, string_view name, const string& value) {
  span->SetAttribute(absl::StrCat("langfuse.observation.metadata.", name), value);
}

string AttributeValue(int64_t value, string_view unit) {
  if (unit != "ms")
    return to_string(value);

  // Convert the native nanosecond accumulator without floating-point rounding.
  const uint64_t magnitude = value < 0 ? uint64_t(-(value + 1)) + 1 : uint64_t(value);
  string fraction = to_string(1000000 + magnitude % 1000000).substr(1);
  return absl::StrCat(value < 0 ? "-" : "", magnitude / 1000000, ".", fraction);
}

}  // namespace

string EncodeTraceBatch(const trace::Capture& capture, const TraceIdentity& identity, size_t begin,
                        size_t count) {
  auto resource = ot::sdk::resource::Resource::Create(
      {{"service.name", "dragonfly.raft"}, {"service.version", identity.release}});
  auto scope = ot::sdk::instrumentationscope::InstrumentationScope::Create(
      "dragonfly.raft", to_string(trace::kSchema));
  vector<unique_ptr<ot::sdk::trace::Recordable>> spans;
  auto add = [&](const trace::Event& event) {
    auto span = make_unique<Recordable>();
    const auto parent = event.id == kRoot                            ? 0
                        : event.request && event.request != event.id ? event.request
                                                                     : kRoot;
    span->SetIdentity(
        ot::trace::SpanContext(TraceId(capture.stream_id()), SpanId(event.id, capture.stream_id()),
                               ot::trace::TraceFlags(1), false),
        SpanId(parent, capture.stream_id()));
    span->SetName(event.name);
    span->SetResource(resource);
    span->SetInstrumentationScope(*scope);
    span->SetSpanKind(ot::trace::SpanKind::kInternal);
    span->SetStartTime(
        ot::common::SystemTimestamp(chrono::nanoseconds(event.start + capture.wall_offset_ns())));
    span->SetDuration(chrono::nanoseconds(event.end - event.start));
    span->SetStatus(
        event.outcome == "Error" ? ot::trace::StatusCode::kError : ot::trace::StatusCode::kUnset,
        event.outcome);
    span->SetAttribute("langfuse.environment", "raft-diagnostic");
    span->SetAttribute("langfuse.trace.name", capture.id());
    span->SetAttribute("langfuse.release", identity.release);
    Attribute(span.get(), "schema", to_string(trace::kSchema));
    Attribute(span.get(), "capture_id", capture.id());
    Attribute(span.get(), "stream_id", capture.stream_id());
    Attribute(span.get(), "event_id", to_string(event.id));
    Attribute(span.get(), "request_id", to_string(event.request));
    Attribute(span.get(), "start_ns", to_string(event.start));
    Attribute(span.get(), "end_ns", to_string(event.end));
    Attribute(span.get(), "outcome", event.outcome);
    Attribute(span.get(), "binary_sha", identity.binary_sha);
    Attribute(span.get(), "release", identity.release);
    Attribute(span.get(), "workload", identity.workload);
    Attribute(span.get(), "experiment_id", identity.experiment);
    for (const auto& [key, value] : event.attributes)
      Attribute(span.get(), key, value);
    for (const auto& value : event.values) {
      const auto& def = trace::Definitions().at(size_t(value.attribute));
      const auto name = capture.EventName(def.name);
      Attribute(span.get(), absl::StrCat("state.", name), value.state);
      if (value.value)
        Attribute(span.get(), absl::StrCat("value.", name), AttributeValue(*value.value, def.unit));
    }
    spans.push_back(std::move(span));
  };
  const auto& events = capture.events();
  if (begin < events.size()) {
    const auto end = begin + min(count, events.size() - begin);
    for (size_t i = begin; i < end; ++i)
      add(events[i]);
  } else {
    trace::Event manifest;
    manifest.id = kRoot;
    manifest.name = "raft.capture";
    manifest.attributes = capture.Labels();
    manifest.start = manifest.end = capture.Now();
    manifest.outcome = capture.dropped() == 0 && capture.drained() && identity.dropped_streams == 0
                           ? "Complete"
                           : "Incomplete";
    if (!events.empty()) {
      manifest.start = events.front().start;
      manifest.end = events.front().end;
      for (const auto& event : events) {
        manifest.start = min(manifest.start, event.start);
        manifest.end = max(manifest.end, event.end);
      }
    }
    manifest.attributes["event_count"] = to_string(events.size());
    manifest.attributes["dropped"] = to_string(capture.dropped());
    manifest.attributes["expected_streams"] = to_string(identity.expected_streams);
    manifest.attributes["dropped_streams"] = to_string(identity.dropped_streams);
    manifest.attributes["expected_nodes"] = identity.expected_nodes;
    for (const auto& def : trace::Definitions())
      manifest.attributes[absl::StrCat("unit.", capture.EventName(def.name))] = def.unit;
    add(manifest);
  }
  ot::proto::collector::trace::v1::ExportTraceServiceRequest request;
  ot::exporter::otlp::OtlpRecordableUtils::PopulateRequest(spans, &request);
  return request.SerializeAsString();
}

absl::Status ExportTrace(const trace::Capture& capture, const TraceIdentity& identity,
                         const function<absl::Status(string_view)>& send) {
  constexpr size_t kBatch = 256;
  for (size_t i = 0; i < capture.events().size(); i += kBatch) {
    if (auto status = send(EncodeTraceBatch(capture, identity, i, kBatch)); !status.ok())
      return status;
  }
  return send(EncodeTraceBatch(capture, identity, capture.events().size(), 0));
}

absl::Status SendTrace(string_view endpoint, string_view authorization, string_view protobuf) {
  bool tls = endpoint.starts_with("https://");
  if (!tls && !endpoint.starts_with("http://"))
    return absl::InvalidArgumentError("Trace endpoint must use HTTP or HTTPS");
  endpoint.remove_prefix(tls ? 8 : 7);
  auto slash = endpoint.find('/');
  const string authority(endpoint.substr(0, slash));
  const string path(slash == string_view::npos ? "/v1/traces" : endpoint.substr(slash));
  const auto colon = authority.rfind(':');
  const string host = authority.substr(0, colon);
  const string port = colon == string::npos ? (tls ? "443" : "80") : authority.substr(colon + 1);
  if (host.empty() || authority.find('@') != string::npos)
    return absl::InvalidArgumentError("Invalid trace endpoint authority");
  auto* proactor = util::fb2::ProactorBase::me();
  if (!proactor)
    return absl::FailedPreconditionError("Trace upload requires a proactor fiber");
  util::http::TlsClient client(proactor);
  client.set_connect_timeout_ms(5000);
  client.set_timeout_ms(30000);
  client.set_retry_count(1);
  unique_ptr<SSL_CTX, decltype(&util::http::TlsClient::FreeContext)> ssl(
      tls ? util::http::TlsClient::CreateSslContext() : nullptr,
      util::http::TlsClient::FreeContext);
  auto ec =
      tls ? client.Connect(host, port, ssl.get()) : client.util::http::Client::Connect(host, port);
  if (ec)
    return absl::UnavailableError("Connecting to trace endpoint failed");
  namespace http = boost::beast::http;
  http::request<http::string_body> request{http::verb::post, path, 11};
  request.set(http::field::host, authority);
  request.set(http::field::authorization, string(authorization));
  request.set(http::field::content_type, "application/x-protobuf");
  request.set("x-langfuse-ingestion-version", "4");
  request.body() = protobuf;
  request.prepare_payload();
  http::response<http::string_body> response;
  if (client.Send(request, &response))
    return absl::UnavailableError("Sending trace batch failed");
  if (response.result_int() < 200 || response.result_int() >= 300)
    return absl::UnavailableError(
        absl::StrCat("Trace endpoint returned HTTP ", response.result_int()));
  if (response[http::field::content_type].starts_with("application/json")) {
    try {
      auto body = jsoncons::json::parse(response.body());
      if (body.contains("partialSuccess")) {
        const auto& partial = body.at("partialSuccess");
        if (partial.contains("rejectedSpans") && partial.at("rejectedSpans").as<int64_t>() != 0)
          return absl::DataLossError("Trace endpoint rejected spans");
      }
    } catch (const exception&) {
      return absl::DataLossError("Invalid trace response JSON");
    }
  } else {
    ot::proto::collector::trace::v1::ExportTraceServiceResponse result;
    if (!result.ParseFromString(response.body()) || result.partial_success().rejected_spans() != 0)
      return absl::DataLossError("Invalid or partially rejected trace response");
  }
  return absl::OkStatus();
}

}  // namespace dfly::cluster::raft::otel
