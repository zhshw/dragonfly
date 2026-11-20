// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/bootstrap/meta_properties.h"

#include <absl/strings/ascii.h>
#include <absl/strings/escaping.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <charconv>
#include <jsoncons/utility/unicode_traits.hpp>
#include <map>

namespace dfly::cluster::raft {
namespace {
using namespace std;
namespace unicode = jsoncons::unicode_traits;
constexpr string_view kWhitespace = " \t\f";

// Java Properties.load(InputStream) decodes ISO-8859-1 and permits UTF-16 \uXXXX escapes.
absl::StatusOr<string> Unescape(string_view text) {
  u16string units;
  for (size_t pos = 0; pos < text.size(); ++pos) {
    unsigned char ch = text[pos];
    if (ch != '\\') {
      units.push_back(ch);
      continue;
    }
    if (++pos == text.size())
      break;
    char escaped = text[pos];
    if (escaped == 'u') {
      uint16_t unit;
      auto digits = text.substr(pos + 1, 4);
      auto [end, ec] = from_chars(digits.data(), digits.data() + digits.size(), unit, 16);
      if (digits.size() != 4 || end != digits.data() + 4 || ec != errc{})
        return absl::DataLossError("Invalid Unicode escape in meta.properties");
      units.push_back(unit);
      pos += 4;
    } else {
      switch (escaped) {
        case 't':
          units.push_back('\t');
          break;
        case 'r':
          units.push_back('\r');
          break;
        case 'n':
          units.push_back('\n');
          break;
        case 'f':
          units.push_back('\f');
          break;
        default:
          units.push_back(static_cast<unsigned char>(escaped));
      }
    }
  }

  string decoded;
  if (unicode::convert(units.data(), units.size(), decoded).ec != unicode::conv_errc::success)
    return absl::DataLossError("Invalid UTF-16 in meta.properties");
  return decoded;
}

absl::Status ParseLine(string_view line, map<string, string>* properties) {
  size_t split = 0;
  while (split < line.size() && kWhitespace.find(line[split]) == string_view::npos &&
         line[split] != '=' && line[split] != ':') {
    if (line[split++] == '\\' && split < line.size())
      ++split;
  }
  auto key = Unescape(line.substr(0, split));
  if (!key.ok())
    return key.status();

  size_t value_pos = line.find_first_not_of(kWhitespace, split);
  if (value_pos != string_view::npos && (line[value_pos] == '=' || line[value_pos] == ':'))
    value_pos = line.find_first_not_of(kWhitespace, value_pos + 1);
  auto value = Unescape(value_pos == string_view::npos ? string_view{} : line.substr(value_pos));
  if (!value.ok())
    return value.status();
  (*properties)[*key] = *std::move(value);
  return absl::OkStatus();
}

absl::StatusOr<map<string, string>> ParseProperties(string_view bytes) {
  map<string, string> properties;
  string logical;
  bool continued = false;
  while (!bytes.empty()) {
    size_t eol = bytes.find_first_of("\r\n");
    auto line = bytes.substr(0, eol);
    if (eol == string_view::npos) {
      bytes = {};
    } else {
      bool crlf = bytes.substr(eol, 2) == "\r\n";
      bytes.remove_prefix(eol + (crlf ? 2 : 1));
    }

    size_t start = line.find_first_not_of(kWhitespace);
    line = start == string_view::npos ? string_view{} : line.substr(start);
    if (!continued && (line.empty() || line.front() == '#' || line.front() == '!'))
      continue;
    logical += line;
    size_t trailing = 0;
    while (trailing < line.size() && line[line.size() - trailing - 1] == '\\')
      ++trailing;
    continued = trailing % 2 == 1;
    if (continued)
      logical.pop_back();
    if (continued && !bytes.empty())
      continue;
    if (auto status = ParseLine(logical, &properties); !status.ok())
      return status;
    logical.clear();
  }
  return properties;
}

optional<int32_t> ParseInt32(string_view decimal) {
  if (decimal.starts_with('+')) {
    decimal.remove_prefix(1);
    if (decimal.empty() || decimal.front() == '-')
      return nullopt;
  }
  int32_t n;
  auto [end, ec] = from_chars(decimal.data(), decimal.data() + decimal.size(), n);
  if (ec != errc{} || end != decimal.data() + decimal.size())
    return nullopt;
  return n;
}

}  // namespace

absl::StatusOr<DirectoryId> ParseDirectoryId(std::string_view encoded) {
  if (encoded.size() == 24 && encoded.ends_with("=="))
    encoded.remove_suffix(2);
  constexpr string_view alphabet =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
  string bytes;
  DirectoryId id;
  if (encoded.size() != 22 || encoded.find_first_not_of(alphabet) != string_view::npos ||
      !absl::WebSafeBase64Unescape(encoded, &bytes) || bytes.size() != id.size())
    return absl::InvalidArgumentError("directory.id must be a Kafka base64url UUID");
  copy(bytes.begin(), bytes.end(), id.begin());
  return id;
}

std::string FormatDirectoryId(const DirectoryId& id) {
  return absl::WebSafeBase64Escape(
      string_view(reinterpret_cast<const char*>(id.data()), id.size()));
}

absl::StatusOr<RaftDirectoryConfig> DecodeMetaProperties(std::string_view bytes,
                                                         std::string* bootstrap_id) {
  auto properties = ParseProperties(bytes);
  if (!properties.ok())
    return properties.status();
  const auto& props = *properties;
  if (bootstrap_id) {
    auto it = props.find("dragonfly.bootstrap");
    *bootstrap_id = it == props.end() ? std::string{} : it->second;
  }
  if (!props.contains("version") ||
      ParseInt32(absl::StripAsciiWhitespace(props.at("version"))) != 1)
    return absl::FailedPreconditionError("Expected KRaft meta.properties version 1");
  if (!props.contains("cluster.id") || !props.contains("node.id"))
    return absl::DataLossError("Missing cluster.id or node.id in meta.properties");
  auto node_id = ParseInt32(props.at("node.id"));
  if (!node_id)
    return absl::DataLossError("Invalid node.id in meta.properties");
  RaftDirectoryConfig config{props.at("cluster.id"), *node_id, nullopt};
  if (props.contains("directory.id")) {
    auto id = ParseDirectoryId(props.at("directory.id"));
    if (!id.ok())
      return absl::DataLossError(id.status().message());
    config.directory_id = *id;
  }
  return config;
}

absl::StatusOr<std::string> EncodeMetaProperties(const RaftDirectoryConfig& identity) {
  u16string units;
  if (unicode::convert(identity.cluster_id.data(), identity.cluster_id.size(), units).ec !=
      unicode::conv_errc::success)
    return absl::InvalidArgumentError("cluster.id must be UTF-8");

  string value;
  for (char16_t unit : units) {
    if (unit > ' ' && unit < 127 && unit != '\\') {
      value.push_back(static_cast<char>(unit));
    } else {
      absl::StrAppend(&value, "\\u", absl::Hex(static_cast<uint16_t>(unit), absl::kZeroPad4));
    }
  }
  string bytes =
      absl::StrCat("version=1\ncluster.id=", value, "\nnode.id=", identity.node_id, "\n");
  if (identity.directory_id)
    absl::StrAppend(&bytes, "directory.id=", FormatDirectoryId(*identity.directory_id), "\n");
  return bytes;
}

}  // namespace dfly::cluster::raft
