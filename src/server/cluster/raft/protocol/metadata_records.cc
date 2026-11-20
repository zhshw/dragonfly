// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//
// Generated from Kafka message schemas by generate.py.

#include "server/cluster/raft/protocol/metadata_records.h"

#include "server/cluster/raft/protocol/flexible_message.h"

namespace dfly::cluster::raft::protocol {
namespace detail {

template <> struct Schema<RegisterBrokerRecord::BrokerEndpoint> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.name))
      return false;
    if (!io.Field(m.host))
      return false;
    if (!io.Field(m.port))
      return false;
    if (!io.Field(m.security_protocol))
      return false;
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<RegisterBrokerRecord::BrokerFeature> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.name))
      return false;
    if (!io.Field(m.min_supported_version))
      return false;
    if (!io.Field(m.max_supported_version))
      return false;
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<RegisterBrokerRecord> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.broker_id))
      return false;
    if (io.version >= 2) {
      if (!io.Field(m.is_migrating_zk_broker))
        return false;
    } else if (!io.Absent(m.is_migrating_zk_broker)) {
      return false;
    }
    if (!io.Field(m.incarnation_id))
      return false;
    if (!io.Field(m.broker_epoch))
      return false;
    if (!io.Field(m.end_points))
      return false;
    if (!io.Field(m.features))
      return false;
    if (!io.Field(m.rack))
      return false;
    if (!io.Field(m.fenced))
      return false;
    if (io.version >= 1) {
      if (!io.Field(m.in_controlled_shutdown))
        return false;
    } else if (!io.Absent(m.in_controlled_shutdown)) {
      return false;
    }
    return io.Tags(m.unknown_tags, Tagged{0, 3, m.log_dirs, !m.log_dirs.empty(), false},
                   Tagged{1, 4, m.cordoned_log_dirs, m.cordoned_log_dirs.has_value(), false});
  }
};

template <> struct Schema<UnregisterBrokerRecord> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.broker_id))
      return false;
    if (!io.Field(m.broker_epoch))
      return false;
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<FenceBrokerRecord> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.id))
      return false;
    if (!io.Field(m.epoch))
      return false;
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<UnfenceBrokerRecord> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.id))
      return false;
    if (!io.Field(m.epoch))
      return false;
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<FeatureLevelRecord> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.name))
      return false;
    if (!io.Field(m.feature_level))
      return false;
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<BrokerRegistrationChangeRecord> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.broker_id))
      return false;
    if (!io.Field(m.broker_epoch))
      return false;
    return io.Tags(m.unknown_tags, Tagged{0, 0, m.fenced, m.fenced != 0, false},
                   Tagged{1, 1, m.in_controlled_shutdown, m.in_controlled_shutdown != 0, false},
                   Tagged{2, 2, m.log_dirs, !m.log_dirs.empty(), false},
                   Tagged{3, 3, m.cordoned_log_dirs, m.cordoned_log_dirs.has_value(), false});
  }
};

template <> struct Schema<NoOpRecord> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<BeginTransactionRecord> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    return io.Tags(m.unknown_tags, Tagged{0, 0, m.name, m.name.has_value(), false});
  }
};

template <> struct Schema<EndTransactionRecord> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<AbortTransactionRecord> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    return io.Tags(m.unknown_tags, Tagged{0, 0, m.reason, m.reason.has_value(), false});
  }
};

}  // namespace detail

CodecError Encode(const RegisterBrokerRecord& message, int16_t version, std::string* out) {
  return detail::EncodeMessage(message, version, out);
}

CodecError Decode(std::string_view bytes, int16_t version, RegisterBrokerRecord* out) {
  return detail::DecodeMessage(bytes, version, out);
}

CodecError Encode(const UnregisterBrokerRecord& message, int16_t version, std::string* out) {
  return detail::EncodeMessage(message, version, out);
}

CodecError Decode(std::string_view bytes, int16_t version, UnregisterBrokerRecord* out) {
  return detail::DecodeMessage(bytes, version, out);
}

CodecError Encode(const FenceBrokerRecord& message, int16_t version, std::string* out) {
  return detail::EncodeMessage(message, version, out);
}

CodecError Decode(std::string_view bytes, int16_t version, FenceBrokerRecord* out) {
  return detail::DecodeMessage(bytes, version, out);
}

CodecError Encode(const UnfenceBrokerRecord& message, int16_t version, std::string* out) {
  return detail::EncodeMessage(message, version, out);
}

CodecError Decode(std::string_view bytes, int16_t version, UnfenceBrokerRecord* out) {
  return detail::DecodeMessage(bytes, version, out);
}

CodecError Encode(const FeatureLevelRecord& message, int16_t version, std::string* out) {
  return detail::EncodeMessage(message, version, out);
}

CodecError Decode(std::string_view bytes, int16_t version, FeatureLevelRecord* out) {
  return detail::DecodeMessage(bytes, version, out);
}

CodecError Encode(const BrokerRegistrationChangeRecord& message, int16_t version,
                  std::string* out) {
  return detail::EncodeMessage(message, version, out);
}

CodecError Decode(std::string_view bytes, int16_t version, BrokerRegistrationChangeRecord* out) {
  return detail::DecodeMessage(bytes, version, out);
}

CodecError Encode(const NoOpRecord& message, int16_t version, std::string* out) {
  return detail::EncodeMessage(message, version, out);
}

CodecError Decode(std::string_view bytes, int16_t version, NoOpRecord* out) {
  return detail::DecodeMessage(bytes, version, out);
}

CodecError Encode(const BeginTransactionRecord& message, int16_t version, std::string* out) {
  return detail::EncodeMessage(message, version, out);
}

CodecError Decode(std::string_view bytes, int16_t version, BeginTransactionRecord* out) {
  return detail::DecodeMessage(bytes, version, out);
}

CodecError Encode(const EndTransactionRecord& message, int16_t version, std::string* out) {
  return detail::EncodeMessage(message, version, out);
}

CodecError Decode(std::string_view bytes, int16_t version, EndTransactionRecord* out) {
  return detail::DecodeMessage(bytes, version, out);
}

CodecError Encode(const AbortTransactionRecord& message, int16_t version, std::string* out) {
  return detail::EncodeMessage(message, version, out);
}

CodecError Decode(std::string_view bytes, int16_t version, AbortTransactionRecord* out) {
  return detail::DecodeMessage(bytes, version, out);
}

}  // namespace dfly::cluster::raft::protocol
