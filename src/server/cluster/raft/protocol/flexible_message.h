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
  Reader(int16_t version, std::string_view bytes, bool flexible = true,
         const std::shared_ptr<const void>* owner = nullptr, size_t capacity = 0)
      : version(version), bytes_(bytes), flexible_(flexible), owner_(owner), capacity_(capacity) {
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
    value = {};
    if (length < 0) {
      return true;
    }
    if (static_cast<size_t>(length) > bytes_.size())
      return false;
    if (owner_ && length > 0)
      value.borrowed = RecordBytes::Borrowed{*owner_, bytes_.substr(0, length), capacity_};
    else
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

  bool String(uint32_t length, std::string* value) {
    if (length > kMaxStringLength || length > bytes_.size())
      return false;
    value->assign(bytes_.substr(0, length));
    bytes_.remove_prefix(length);
    return true;
  }

  template <typename T> bool ReadTag(std::string_view payload, Tagged<T> tag) {
    Reader field(version, payload, true, owner_, capacity_);
    return version >= tag.first_version && field.Field(tag.value) && field.empty();
  }

  std::string_view bytes_;
  bool flexible_;
  const std::shared_ptr<const void>* owner_;
  size_t capacity_;
};

template <bool kMeasure> class WriterImpl {
 public:
  WriterImpl(int16_t version, std::string* bytes, bool flexible = true)
      : version(version), bytes_(bytes), flexible_(flexible) {
  }

  template <typename T>
  requires(std::is_integral_v<T> && !std::is_same_v<T, bool>) bool Field(T value) {
    if constexpr (kMeasure)
      return AddSize(sizeof(T));
    auto bits = static_cast<std::make_unsigned_t<T>>(value);
    for (int i = sizeof(T) - 1; i >= 0; --i)
      bytes_->push_back(static_cast<char>(bits >> (8 * i)));
    return true;
  }

  bool Field(bool value) {
    return Field(static_cast<uint8_t>(value));
  }

  bool Field(const Uuid& value) {
    return Append({reinterpret_cast<const char*>(value.data()), value.size()});
  }

  bool LegacyClientId(const std::optional<std::string>& value) {
    if (!value)
      return Field(int16_t{-1});
    if (value->size() > kMaxStringLength)
      return false;
    Field(static_cast<int16_t>(value->size()));
    return Append(*value);
  }

  bool Field(const std::string& value) {
    if (value.size() > kMaxStringLength)
      return false;
    if (flexible_)
      Varint(value.size() + 1);
    else
      Field(static_cast<int16_t>(value.size()));
    return Append(value);
  }

  bool Field(const std::optional<std::string>& value) {
    if (!flexible_)
      return LegacyClientId(value);
    if (value)
      return Field(*value);
    Varint(0);
    return Fits();
  }

  bool Field(const RecordBytes& value) {
    auto bytes = value.View();
    if (!bytes)
      return Length(-1);
    if (bytes->size() > std::numeric_limits<int32_t>::max())
      return false;
    Length(static_cast<int32_t>(bytes->size()));
    return Append(*bytes);
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
      if (!Append(payload))
        return false;
    }
    return Fits();
  }

  template <typename... Fields>
  requires(sizeof...(Fields) > 0) bool Tags(const TaggedFields& unknown, Fields... known) {
    if constexpr (kMeasure) {
      // Retain only tag lengths during preflight, never copies of opaque payloads.
      std::map<uint32_t, size_t> fields;
      for (const auto& [id, payload] : unknown)
        fields.emplace(id, payload.size());
      if (!(MeasureTag(known, &fields) && ...))
        return false;
      if (!flexible_)
        return Absent(fields);
      if (fields.size() > kMaxTaggedFields)
        return false;
      Varint(fields.size());
      for (const auto& [id, size] : fields) {
        if (size > std::numeric_limits<int32_t>::max())
          return false;
        Varint(id);
        Varint(size);
        if (!AddSize(size))
          return false;
      }
      return Fits();
    } else {
      auto fields = unknown;
      return (AddTag(known, &fields) && ...) && Tags(fields);
    }
  }

  template <typename T> bool Absent(const T& value, const std::type_identity_t<T>& fallback = T{}) {
    if (value != fallback) {
      error = CodecError::kUnsupportedField;
      return false;
    }
    return true;
  }

  void Varint(uint32_t value) {
    while (value >= 128) {
      Field(static_cast<uint8_t>((value & 127) | 128));
      value >>= 7;
    }
    Field(static_cast<uint8_t>(value));
  }

  size_t size() const requires(kMeasure) {
    return size_;
  }

  const int16_t version;
  CodecError error = CodecError::kInvalidData;

 private:
  bool Fits() const {
    return !kMeasure || size_ <= std::numeric_limits<int32_t>::max();
  }

  bool AddSize(size_t size) {
    constexpr size_t kMaxSize = std::numeric_limits<int32_t>::max();
    if (size_ > kMaxSize || size > kMaxSize - size_) {
      size_ = kMaxSize + 1;
      return false;
    }
    size_ += size;
    return true;
  }

  bool Append(std::string_view bytes) {
    if constexpr (kMeasure) {
      return AddSize(bytes.size());
    } else {
      bytes_->append(bytes);
      return true;
    }
  }

  bool Length(int32_t size) {
    if (flexible_)
      Varint(static_cast<uint32_t>(size) + 1);
    else
      Field(size);
    return Fits();
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
    WriterImpl<false> field(version, &(*fields)[tag.id]);
    if (field.Field(tag.value))
      return true;
    error = field.error;
    return false;
  }

  template <typename T> bool MeasureTag(Tagged<T> tag, std::map<uint32_t, size_t>* fields) {
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
    WriterImpl<true> field(version, nullptr);
    if (!field.Field(tag.value)) {
      error = field.error;
      return false;
    }
    fields->emplace(tag.id, field.size());
    return true;
  }

  std::string* bytes_;
  bool flexible_;
  size_t size_ = 0;
};

using Writer = WriterImpl<false>;
using SizeWriter = WriterImpl<true>;

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

// Preflight and write a complete response into one allocation. The body and header retain
// their schema/version validation, and failures leave the caller's destination unchanged.
template <typename T>
CodecError EncodeResponseFrameMessage(const T& value, int16_t version, int32_t correlation_id,
                                      std::string* out) {
  if (version < T::kMinVersion || version > T::kMaxVersion)
    return CodecError::kUnsupportedVersion;
  SizeWriter size(version, nullptr, version >= T::kFirstFlexibleVersion);
  if (!size.Field(value))
    return size.error;
  const size_t header_size = sizeof(correlation_id) + (T::HeaderVersion(version) >= 1);
  if (size.size() > std::numeric_limits<int32_t>::max() - header_size)
    return CodecError::kInvalidData;
  const size_t payload_size = header_size + size.size();
  std::string buffer;
  buffer.reserve(sizeof(int32_t) + payload_size);
  Writer header(0, &buffer);
  header.Field(static_cast<int32_t>(payload_size));
  header.Field(correlation_id);
  if (T::HeaderVersion(version) >= 1)
    header.Tags({});
  Writer writer(version, &buffer, version >= T::kFirstFlexibleVersion);
  if (!writer.Field(value))
    return writer.error;
  if (buffer.size() != sizeof(int32_t) + payload_size)
    return CodecError::kInvalidData;
  *out = std::move(buffer);
  return CodecError::kOk;
}

template <typename T>
CodecError DecodeMessage(std::string_view bytes, int16_t version,
                         const std::shared_ptr<const void>& owner, size_t capacity, T* out) {
  if (version < T::kMinVersion || version > T::kMaxVersion)
    return CodecError::kUnsupportedVersion;
  if (owner && capacity < bytes.size())
    return CodecError::kInvalidData;
  T value;
  Reader reader(version, bytes, version >= T::kFirstFlexibleVersion, owner ? &owner : nullptr,
                capacity);
  if (!reader.Field(value) || !reader.empty())
    return CodecError::kInvalidData;
  *out = std::move(value);
  return CodecError::kOk;
}

template <typename T> CodecError DecodeMessage(std::string_view bytes, int16_t version, T* out) {
  return DecodeMessage(bytes, version, {}, 0, out);
}

}  // namespace dfly::cluster::raft::protocol::detail
