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
  bool present;
  bool ignorable;
};
template <typename T> Tagged(uint32_t, int16_t, T&, bool, bool) -> Tagged<T>;

class Reader {
 public:
  Reader(int16_t version, std::string_view bytes, bool flexible = true)
      : version(version), bytes_(bytes), flexible_(flexible) {
  }

  std::string_view remaining() const {
    return bytes_;
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

  bool LegacyClientId(std::optional<std::string>& value) {
    int16_t length;
    if (!Field(length))
      return false;
    if (length < 0) {
      value.reset();
      return true;
    }
    return String(length, &value.emplace());
  }

  bool Field(std::string& value) {
    if (!flexible_) {
      int16_t length;
      return Field(length) && length >= 0 && String(length, &value);
    }
    uint32_t length;
    return Varint(&length) && length != 0 && String(length - 1, &value);
  }

  bool Field(std::optional<std::string>& value) {
    if (!flexible_)
      return LegacyClientId(value);
    uint32_t length;
    if (!Varint(&length))
      return false;
    if (length == 0) {
      value.reset();
      return true;
    }
    return String(length - 1, &value.emplace());
  }

  bool Field(RecordBytes& value) {
    int32_t length;
    if (!Length(&length))
      return false;
    if (length < 0) {
      value.data.reset();
      return true;
    }
    if (static_cast<size_t>(length) > bytes_.size())
      return false;
    value.data.emplace(bytes_.substr(0, length));
    bytes_.remove_prefix(length);
    return true;
  }

  template <typename T> bool Field(std::vector<T>& values) {
    int32_t count;
    return Length(&count) && count >= 0 && Array(count, &values);
  }

  template <typename T> bool Field(std::optional<std::vector<T>>& values) {
    int32_t count;
    if (!Length(&count))
      return false;
    if (count < 0) {
      values.reset();
      return true;
    }
    return Array(count, &values.emplace());
  }

  template <typename T>
  requires requires {
    sizeof(Schema<T>);
  }
  bool Field(T& message) {
    return Schema<T>::Visit(*this, message);
  }

  template <typename... Fields> bool Tags(TaggedFields& unknown, Fields... known) {
    if (!flexible_)
      return true;
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

  template <typename T> bool Absent(const T&, const std::type_identity_t<T>& = T{}) const {
    // The destination is constructed with the schema defaults before decoding.
    return true;
  }

  const int16_t version;

 private:
  bool Length(int32_t* length) {
    if (!flexible_)
      return Field(*length);
    uint32_t compact;
    if (!Varint(&compact) || compact > uint32_t{1} + std::numeric_limits<int32_t>::max())
      return false;
    *length = compact ? static_cast<int32_t>(compact - 1) : -1;
    return true;
  }

  template <typename T> bool Array(uint32_t count, std::vector<T>* values) {
    if (count > bytes_.size() || count > kMaxArrayLength)
      return false;
    for (uint32_t i = 0; i < count; ++i) {
      if (!Field(values->emplace_back()))
        return false;
    }
    return true;
  }

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
  bool flexible_;
};

class Writer {
 public:
  Writer(int16_t version, std::string* bytes, bool flexible = true)
      : version(version), bytes_(bytes), flexible_(flexible) {
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

  bool LegacyClientId(const std::optional<std::string>& value) {
    if (!value)
      return Field(int16_t{-1});
    if (value->size() > kMaxStringLength)
      return false;
    Field(static_cast<int16_t>(value->size()));
    bytes_->append(*value);
    return true;
  }

  bool Field(const std::string& value) {
    if (value.size() > kMaxStringLength)
      return false;
    if (flexible_)
      Varint(value.size() + 1);
    else
      Field(static_cast<int16_t>(value.size()));
    bytes_->append(value);
    return true;
  }

  bool Field(const std::optional<std::string>& value) {
    if (!flexible_)
      return LegacyClientId(value);
    if (value)
      return Field(*value);
    Varint(0);
    return true;
  }

  bool Field(const RecordBytes& value) {
    if (!value.data)
      return Length(-1);
    if (value.data->size() > std::numeric_limits<int32_t>::max())
      return false;
    Length(static_cast<int32_t>(value.data->size()));
    bytes_->append(*value.data);
    return true;
  }

  template <typename T> bool Field(const std::optional<std::vector<T>>& values) {
    return values ? Field(*values) : Length(-1);
  }

  template <typename T> bool Field(const std::vector<T>& values) {
    if (values.size() > kMaxArrayLength)
      return false;
    Length(static_cast<int32_t>(values.size()));
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
    if (!flexible_)
      return Absent(unknown);
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

  template <typename T> bool Absent(const T& value, const std::type_identity_t<T>& fallback = T{}) {
    if (value != fallback) {
      error = CodecError::kUnsupportedField;
      return false;
    }
    return true;
  }

  const int16_t version;
  CodecError error = CodecError::kInvalidData;

 private:
  bool Length(int32_t size) {
    if (flexible_)
      Varint(static_cast<uint32_t>(size) + 1);
    else
      Field(size);
    return true;
  }

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
    if (!tag.present)
      return true;
    if (version < tag.first_version) {
      if (tag.ignorable)
        return true;
      error = CodecError::kUnsupportedField;
      return false;
    }
    Writer field(version, &(*fields)[tag.id]);
    if (field.Field(tag.value))
      return true;
    error = field.error;
    return false;
  }

  std::string* bytes_;
  bool flexible_;
};

template <typename T> CodecError EncodeMessage(const T& value, int16_t version, std::string* out) {
  if (version < T::kMinVersion || version > T::kMaxVersion)
    return CodecError::kUnsupportedVersion;
  std::string buffer;
  Writer writer(version, &buffer, version >= T::kFirstFlexibleVersion);
  if (!writer.Field(value))
    return writer.error;
  *out = std::move(buffer);
  return CodecError::kOk;
}

template <typename T> CodecError DecodeMessage(std::string_view bytes, int16_t version, T* out) {
  if (version < T::kMinVersion || version > T::kMaxVersion)
    return CodecError::kUnsupportedVersion;
  T value;
  Reader reader(version, bytes, version >= T::kFirstFlexibleVersion);
  if (!reader.Field(value) || !reader.empty())
    return CodecError::kInvalidData;
  *out = std::move(value);
  return CodecError::kOk;
}

}  // namespace dfly::cluster::raft::protocol::detail
