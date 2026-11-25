include(FetchContent)
set(OTELCPP_BUILD_TESTING OFF CACHE BOOL "" FORCE)
set(OTELCPP_WITH_EXAMPLES OFF CACHE BOOL "" FORCE)
set(OTELCPP_INSTALL OFF CACHE BOOL "" FORCE)
set(OTELCPP_WITH_OTLP_HTTP ON CACHE BOOL "" FORCE)
set(OTELCPP_WITH_OTLP_GRPC OFF CACHE BOOL "" FORCE)
set(OTELCPP_WITH_HTTP_CLIENT_CURL OFF CACHE BOOL "" FORCE)
FetchContent_Declare(opentelemetry-cpp
  URL https://codeload.github.com/open-telemetry/opentelemetry-cpp/tar.gz/refs/tags/v1.30.0
  URL_HASH SHA256=85aae7a42d7b71d17e510ddebc85db622d9078cc6d29cf9fb7373014b063859f)
FetchContent_MakeAvailable(opentelemetry-cpp)

add_library(dfly_raft_otel trace_exporter.cc trace_session.cc)
target_compile_features(dfly_raft_otel PUBLIC cxx_std_20)
cxx_link(dfly_raft_otel dfly_raft opentelemetry_otlp_recordable http_client_lib)
helio_cxx_test(trace_exporter_test dfly_raft_otel LABELS DFLY)
