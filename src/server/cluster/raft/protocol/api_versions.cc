// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//
// Generated from Kafka message schemas by generate.py.

#include "server/cluster/raft/protocol/api_versions.h"

#include "server/cluster/raft/protocol/flexible_message.h"

namespace dfly::cluster::raft::protocol {
namespace detail {

template <> struct Schema<ApiVersionsRequest> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (io.version >= 3) {
      if (!io.Field(m.client_software_name))
        return false;
    }
    if (io.version >= 3) {
      if (!io.Field(m.client_software_version))
        return false;
    }
    if (io.version >= 5) {
      if (!io.Field(m.cluster_id))
        return false;
    }
    if (io.version >= 5) {
      if (!io.Field(m.node_id))
        return false;
    }
    if (io.version < 3)
      return io.Absent(m.unknown_tags);
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<ApiVersionsResponse::ApiVersion> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.api_key))
      return false;
    if (!io.Field(m.min_version))
      return false;
    if (!io.Field(m.max_version))
      return false;
    if (io.version < 3)
      return io.Absent(m.unknown_tags);
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<ApiVersionsResponse::SupportedFeatureKey> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (io.version >= 3) {
      if (!io.Field(m.name))
        return false;
    } else if (!io.Absent(m.name)) {
      return false;
    }
    if (io.version >= 3) {
      if (!io.Field(m.min_version))
        return false;
    } else if (!io.Absent(m.min_version)) {
      return false;
    }
    if (io.version >= 3) {
      if (!io.Field(m.max_version))
        return false;
    } else if (!io.Absent(m.max_version)) {
      return false;
    }
    if (io.version < 3)
      return io.Absent(m.unknown_tags);
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<ApiVersionsResponse::FinalizedFeatureKey> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (io.version >= 3) {
      if (!io.Field(m.name))
        return false;
    } else if (!io.Absent(m.name)) {
      return false;
    }
    if (io.version >= 3) {
      if (!io.Field(m.max_version_level))
        return false;
    } else if (!io.Absent(m.max_version_level)) {
      return false;
    }
    if (io.version >= 3) {
      if (!io.Field(m.min_version_level))
        return false;
    } else if (!io.Absent(m.min_version_level)) {
      return false;
    }
    if (io.version < 3)
      return io.Absent(m.unknown_tags);
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<ApiVersionsResponse> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.error_code))
      return false;
    if (!io.Field(m.api_keys))
      return false;
    if (io.version >= 1) {
      if (!io.Field(m.throttle_time_ms))
        return false;
    }
    if (io.version < 3)
      return io.Absent(m.unknown_tags);
    return io.Tags(m.unknown_tags,
                   Tagged{0, 3, m.supported_features, !m.supported_features.empty()},
                   Tagged{1, 3, m.finalized_features_epoch, m.finalized_features_epoch != -1},
                   Tagged{2, 3, m.finalized_features, !m.finalized_features.empty()},
                   Tagged{3, 3, m.zk_migration_ready, m.zk_migration_ready});
  }
};

}  // namespace detail

CodecError Encode(const ApiVersionsRequest& message, int16_t version, std::string* out) {
  return detail::EncodeMessage(message, version, out);
}

CodecError Decode(std::string_view bytes, int16_t version, ApiVersionsRequest* out) {
  return detail::DecodeMessage(bytes, version, out);
}

CodecError Encode(const ApiVersionsResponse& message, int16_t version, std::string* out) {
  return detail::EncodeMessage(message, version, out);
}

CodecError Decode(std::string_view bytes, int16_t version, ApiVersionsResponse* out) {
  return detail::DecodeMessage(bytes, version, out);
}

}  // namespace dfly::cluster::raft::protocol
