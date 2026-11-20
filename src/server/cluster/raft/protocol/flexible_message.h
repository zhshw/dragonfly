// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <algorithm>
#include <bit>
#include <limits>
#include <optional>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "server/cluster/raft/protocol/wire.h"

namespace dfly::cluster::raft::protocol::detail {

// The generated specializations visit fields in Kafka schema order.
template <typename Message> struct Schema;

constexpr size_t kMaxArrayLength = 1'000'000;
constexpr size_t kMaxTaggedFields = 10'000;
constexpr size_t kMaxStringLength = std::numeric_limits<int16_t>::max();

template <typename T> struct Tagged {
  uint32_t id;
  int16_t first_version;
  T& value;
};
template <typename T> Tagged(uint32_t, int16_t, T&) -> Tagged<T>;

class Reader {
 public:
  Reader(int16_t version, std::string_view bytes) : version(version), bytes_(bytes) {
  }

  bool empty() const {
    return bytes_.empty();
  }

  template <typename T>
  requires(std::is_integral_v<T> && !std::is_same_v<T, bool>) bool Field(T& value) {
    if (bytes_.size() < sizeof(T))
      return false;
    using Unsigned = std::make_unsigned_t<T>;
    Unsigned result = 0;
    for (size_t i = 0; i < sizeof(T); ++i)
      result = (result << 8) | static_cast<uint8_t>(bytes_[i]);
    bytes_.remove_prefix(sizeof(T));
    value = std::bit_cast<T>(result);
    return true;
  }

  bool Field(bool& value) {
    uint8_t byte;
    if (!Field(byte))
      return false;
    value = byte != 0;
    return true;
  }

  bool Field(Uuid& value) {
    if (bytes_.size() < value.size())
      return false;
    std::copy_n(bytes_.begin(), value.size(), value.begin());
    bytes_.remove_prefix(value.size());
    return true;
  }

  bool Field(std::string& value) {
    uint32_t length;
    return Varint(&length) && length != 0 && String(length - 1, &value);
  }

  bool Field(std::optional<std::string>& value) {
    uint32_t length;
    if (!Varint(&length))
      return false;
    if (length == 0) {
      value.reset();
      return true;
    }
    return String(length - 1, &value.emplace());
  }

  template <typename T> bool Field(std::vector<T>& values) {
    uint32_t length;
    if (!Varint(&length) || length == 0)
      return false;
    const auto count = length - 1;
    if (count > bytes_.size() || count > kMaxArrayLength)
      return false;
    for (uint32_t i = 0; i < count; ++i) {
      if (!Field(values.emplace_back()))
        return false;
    }
    return true;
  }

  template <typename T>
  requires requires {
    sizeof(Schema<T>);
  }
  bool Field(T& message) {
    return Schema<T>::Visit(*this, message);
  }

  template <typename... Fields> bool Tags(TaggedFields& unknown, Fields... known) {
    uint32_t count;
    if (!Varint(&count) || count > kMaxTaggedFields || count > bytes_.size() / 2)
      return false;
    uint32_t previous = 0;
    for (uint32_t i = 0; i < count; ++i) {
      uint32_t id, size;
      if (!Varint(&id) || !Varint(&size) || (i != 0 && id <= previous) || size > bytes_.size())
        return false;
      previous = id;
      auto payload = bytes_.substr(0, size);
      bytes_.remove_prefix(size);
      if (((id == known.id) || ...)) {
        if (!((id != known.id || ReadTag(payload, known)) && ...))
          return false;
      } else {
        unknown.emplace(id, payload);
      }
    }
    return true;
  }

  template <typename T> bool Absent(const T&) const {
    // The destination is constructed with the schema defaults before decoding.
    return true;
  }

  const int16_t version;

 private:
  bool Varint(uint32_t* result) {
    *result = 0;
    for (int shift = 0; shift <= 28; shift += 7) {
      uint8_t byte;
      if (!Field(byte) || (shift == 28 && byte > 15))
        return false;
      *result |= static_cast<uint32_t>(byte & 127) << shift;
      if (byte < 128)
        return true;
    }
    return false;
  }

  bool String(uint32_t length, std::string* value) {
    if (length > kMaxStringLength || length > bytes_.size())
      return false;
    value->assign(bytes_.substr(0, length));
    bytes_.remove_prefix(length);
    return true;
  }

  template <typename T> bool ReadTag(std::string_view payload, Tagged<T> tag) {
    Reader field(version, payload);
    return version >= tag.first_version && field.Field(tag.value) && field.empty();
  }

  std::string_view bytes_;
};

class Writer {
 public:
  Writer(int16_t version, std::string* bytes) : version(version), bytes_(bytes) {
  }

  template <typename T>
  requires(std::is_integral_v<T> && !std::is_same_v<T, bool>) bool Field(T value) {
    auto bits = static_cast<std::make_unsigned_t<T>>(value);
    for (int i = sizeof(T) - 1; i >= 0; --i)
      bytes_->push_back(static_cast<char>(bits >> (8 * i)));
    return true;
  }

  bool Field(bool value) {
    return Field(static_cast<uint8_t>(value));
  }

  bool Field(const Uuid& value) {
    bytes_->append(reinterpret_cast<const char*>(value.data()), value.size());
    return true;
  }

  bool Field(const std::string& value) {
    if (value.size() > kMaxStringLength)
      return false;
    Varint(value.size() + 1);
    bytes_->append(value);
    return true;
  }

  bool Field(const std::optional<std::string>& value) {
    if (value)
      return Field(*value);
    Varint(0);
    return true;
  }

  template <typename T> bool Field(const std::vector<T>& values) {
    if (values.size() > kMaxArrayLength)
      return false;
    Varint(values.size() + 1);
    for (const auto& value : values) {
      if (!Field(value))
        return false;
    }
    return true;
  }

  template <typename T>
  requires requires {
    sizeof(Schema<T>);
  }
  bool Field(const T& message) {
    return Schema<T>::Visit(*this, message);
  }

  bool Tags(const TaggedFields& unknown) {
    if (unknown.size() > kMaxTaggedFields)
      return false;
    Varint(unknown.size());
    for (const auto& [id, payload] : unknown) {
      if (payload.size() > std::numeric_limits<int32_t>::max())
        return false;
      Varint(id);
      Varint(payload.size());
      bytes_->append(payload);
    }
    return true;
  }

  template <typename... Fields>
  requires(sizeof...(Fields) > 0) bool Tags(const TaggedFields& unknown, Fields... known) {
    auto fields = unknown;
    return (AddTag(known, &fields) && ...) && Tags(fields);
  }

  template <typename T> bool Absent(const T& value) {
    if (value != T{}) {
      error = CodecError::kUnsupportedField;
      return false;
    }
    return true;
  }

  const int16_t version;
  CodecError error = CodecError::kInvalidData;

 private:
  void Varint(uint32_t value) {
    while (value >= 128) {
      bytes_->push_back(static_cast<char>((value & 127) | 128));
      value >>= 7;
    }
    bytes_->push_back(static_cast<char>(value));
  }

  template <typename T> bool AddTag(Tagged<T> tag, TaggedFields* fields) {
    if (fields->contains(tag.id))
      return false;
    if (version < tag.first_version)
      return Absent(tag.value);
    if (tag.value.empty())
      return true;
    Writer field(version, &(*fields)[tag.id]);
    if (field.Field(tag.value))
      return true;
    error = field.error;
    return false;
  }

  std::string* bytes_;
};

template <typename T> CodecError EncodeMessage(const T& value, int16_t version, std::string* out) {
  if (version < T::kMinVersion || version > T::kMaxVersion)
    return CodecError::kUnsupportedVersion;
  std::string buffer;
  Writer writer(version, &buffer);
  if (!writer.Field(value))
    return writer.error;
  *out = std::move(buffer);
  return CodecError::kOk;
}

template <typename T> CodecError DecodeMessage(std::string_view bytes, int16_t version, T* out) {
  if (version < T::kMinVersion || version > T::kMaxVersion)
    return CodecError::kUnsupportedVersion;
  T value;
  Reader reader(version, bytes);
  if (!reader.Field(value) || !reader.empty())
    return CodecError::kInvalidData;
  *out = std::move(value);
  return CodecError::kOk;
}

}  // namespace dfly::cluster::raft::protocol::detail
