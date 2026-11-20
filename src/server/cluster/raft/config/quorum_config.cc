// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/config/quorum_config.h"

#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>

#include <algorithm>
#include <charconv>

namespace dfly::cluster::raft {
namespace {

bool ValidHostChar(char c) {
  return absl::ascii_isalnum(c) || c == '-' || c == '%' || c == '.' || c == '_';
}

absl::StatusOr<QuorumEndpoint> ParseEndpoint(std::string_view address) {
  const auto invalid = [address] {
    return absl::InvalidArgumentError(absl::StrCat("Invalid quorum endpoint: ", address));
  };

  // Kafka Utils.getHost accepts a listener prefix and bracketed or bare IPv6.
  size_t prefix = address.find("://");
  if (prefix != address.npos) {
    auto listener = address.substr(0, prefix);
    if (!std::all_of(listener.begin(), listener.end(), ValidHostChar))
      return invalid();
    address.remove_prefix(prefix + 3);
  }
  size_t colon = address.rfind(':');
  if (colon == address.npos)
    return invalid();
  std::string_view port = address.substr(colon + 1);
  uint16_t port_number = 0;
  auto [end, ec] = std::from_chars(port.data(), port.data() + port.size(), port_number);
  if (ec != std::errc{} || end != port.data() + port.size())
    return invalid();

  auto host = address.substr(0, colon);
  if (host.starts_with('['))
    host.remove_prefix(1);
  if (host.ends_with(']'))
    host.remove_suffix(1);
  if (!std::all_of(host.begin(), host.end(), [](char c) { return c == ':' || ValidHostChar(c); }))
    return invalid();
  return QuorumEndpoint{std::string(host), port_number};
}

}  // namespace

absl::StatusOr<VoterConnections> ParseVoterConnections(std::string_view value) {
  VoterConnections voters;
  if (absl::StripAsciiWhitespace(value).empty())
    return voters;
  for (std::string_view entry : absl::StrSplit(value, ',')) {
    entry = absl::StripAsciiWhitespace(entry);

    // Java String.split("@") drops trailing empty fields.
    while (entry.ends_with('@'))
      entry.remove_suffix(1);
    size_t at = entry.find('@');
    if (at == entry.npos)
      return absl::InvalidArgumentError("Expected id@host:port in quorum voters");
    auto id = entry.substr(0, at);
    if (id.starts_with('+')) {
      id.remove_prefix(1);
      if (id.empty() || !absl::ascii_isdigit(id.front()))
        return absl::InvalidArgumentError(absl::StrCat("Invalid quorum voter id: ", entry));
    }
    int32_t voter_id;
    auto [end, ec] = std::from_chars(id.data(), id.data() + id.size(), voter_id);
    if (ec != std::errc{} || end != id.data() + id.size())
      return absl::InvalidArgumentError(absl::StrCat("Invalid quorum voter id: ", entry));
    auto endpoint = ParseEndpoint(entry.substr(at + 1));
    if (!endpoint.ok())
      return endpoint.status();
    if (endpoint->host == "0.0.0.0")
      return absl::InvalidArgumentError("A quorum voter cannot advertise 0.0.0.0");

    // Kafka uses Map.put: a repeated id replaces its address, never increasing the quorum.
    voters.insert_or_assign(voter_id, std::move(*endpoint));
  }
  return voters;
}

absl::StatusOr<std::vector<QuorumEndpoint>> ParseBootstrapServers(std::string_view value) {
  std::vector<QuorumEndpoint> endpoints;
  if (absl::StripAsciiWhitespace(value).empty())
    return endpoints;
  for (std::string_view entry : absl::StrSplit(value, ',')) {
    auto endpoint = ParseEndpoint(absl::StripAsciiWhitespace(entry));
    if (!endpoint.ok())
      return endpoint.status();
    endpoints.push_back(std::move(*endpoint));
  }
  return endpoints;
}

}  // namespace dfly::cluster::raft
