add_library(dfly_raft
            bootstrap/meta_properties.cc
            bootstrap/quorum_bootstrap.cc
            bootstrap/raft_directory.cc
            config/quorum_config.cc
            data/apply_progress.cc
            data/kv_snapshot.cc
            data/set_effect.cc
            data/slot_transfer_state.cc
            log/log_files.cc
            log/partition_log.cc
            log/partition_snapshot.cc
            log/record_batch.cc
            log/snapshot_file.cc
            metadata/bootstrap_topology.cc
            metadata/broker_control.cc
            metadata/metadata_image.cc
            metadata/metadata_snapshot.cc
            metadata/record_serde.cc
            metadata/slot_directory.cc
            metadata/slot_migration.cc
            protocol/add_voter.cc
            protocol/api_versions.cc
            protocol/begin_epoch.cc
            protocol/broker_heartbeat.cc
            protocol/broker_registration.cc
            protocol/describe_quorum.cc
            protocol/end_epoch.cc
            protocol/fetch.cc
            protocol/fetch_slot_image.cc
            protocol/fetch_snapshot.cc
            protocol/metadata_records.cc
            protocol/remove_voter.cc
            protocol/rpc_frame.cc
            protocol/slot_migration.cc
            protocol/slot_records.cc
            protocol/slot_transfer_record.cc
            protocol/update_voter.cc
            protocol/vote.cc
            protocol/voter_records.cc
            quorum/campaign.cc
            quorum/describe.cc
            quorum/election.cc
            quorum/epoch_control.cc
            quorum/fetch_replication.cc
            quorum/membership_history.cc
            quorum/replica_progress.cc
            quorum/snapshot_transfer.cc
            quorum/voter_changes.cc
            quorum/vote_handler.cc
            replication/high_watermark.cc
            runtime/admin_command.cc
            storage/file_quorum_state_store.cc)
target_compile_features(dfly_raft PUBLIC cxx_std_20)
cxx_link(dfly_raft absl::crc32c absl::statusor absl::str_format absl::strings absl::cleanup
         OpenSSL::Crypto TRDP::jsoncons)

add_executable(raft_storage bootstrap/raft_storage.cc)
cxx_link(raft_storage dfly_raft)

helio_cxx_test(bootstrap/raft_directory_test dfly_raft LABELS DFLY)
helio_cxx_test(config/quorum_config_test dfly_raft LABELS DFLY)
helio_cxx_test(data/apply_progress_test dfly_raft LABELS DFLY)
helio_cxx_test(data/kv_snapshot_test dfly_raft LABELS DFLY)
helio_cxx_test(data/set_effect_test dfly_raft LABELS DFLY)
helio_cxx_test(log/partition_log_test dfly_raft LABELS DFLY)
helio_cxx_test(log/partition_snapshot_test dfly_raft LABELS DFLY)
helio_cxx_test(log/record_batch_test dfly_raft LABELS DFLY)
helio_cxx_test(metadata/bootstrap_topology_test dfly_raft LABELS DFLY)
helio_cxx_test(metadata/broker_control_test dfly_raft LABELS DFLY)
helio_cxx_test(metadata/metadata_image_test dfly_raft LABELS DFLY)
helio_cxx_test(metadata/record_serde_test dfly_raft LABELS DFLY)
helio_cxx_test(metadata/slot_directory_test dfly_raft LABELS DFLY)
helio_cxx_test(metadata/slot_migration_test dfly_raft LABELS DFLY)
helio_cxx_test(protocol/fetch_test dfly_raft LABELS DFLY)
helio_cxx_test(protocol/rpc_frame_test dfly_raft LABELS DFLY)
helio_cxx_test(protocol/vote_test dfly_raft LABELS DFLY)
helio_cxx_test(quorum/campaign_test dfly_raft LABELS DFLY)
helio_cxx_test(quorum/describe_test dfly_raft LABELS DFLY)
helio_cxx_test(quorum/election_test dfly_raft LABELS DFLY)
helio_cxx_test(quorum/epoch_control_test dfly_raft LABELS DFLY)
helio_cxx_test(quorum/fetch_replication_test dfly_raft LABELS DFLY)
helio_cxx_test(quorum/membership_history_test dfly_raft LABELS DFLY)
helio_cxx_test(quorum/snapshot_transfer_test dfly_raft LABELS DFLY)
helio_cxx_test(quorum/voter_changes_test dfly_raft LABELS DFLY)
helio_cxx_test(quorum/vote_handler_test dfly_raft LABELS DFLY)
helio_cxx_test(replication/high_watermark_test dfly_raft LABELS DFLY)
helio_cxx_test(runtime/admin_command_test dfly_raft LABELS DFLY)
helio_cxx_test(storage/file_quorum_state_store_test dfly_raft LABELS DFLY)

add_library(dfly_raft_transport runtime/admin_listener.cc runtime/controller.cc runtime/group.cc
            transport/peer_connection.cc transport/raft_listener.cc)
cxx_link(dfly_raft_transport dfly_raft dfly_facade fibers2 absl::random_random)
set(DFLY_RAFT_USE_FOLLY ${WITH_RAFT_FOLLY})
configure_file(storage/io_config.h.in generated/raft_io_config.h)
target_include_directories(dfly_raft_transport PRIVATE ${CMAKE_CURRENT_BINARY_DIR}/generated)
if(WITH_RAFT_FOLLY)
  add_library(dfly_raft_folly storage/folly_io.cc transport/folly_rpc.cc)
  target_compile_features(dfly_raft_folly PUBLIC cxx_std_20)
  if(TARGET Folly::folly_io_async_io_uring_backend)
    target_link_libraries(dfly_raft_folly PRIVATE Folly::folly_io_async_io_uring_backend
                          Folly::folly_io_async_scoped_event_base_thread
                          Folly::folly_io_async_async_io_uring_socket
                          Folly::folly_io_async_server_socket
                          Folly::folly_executors_cpu_thread_pool_executor)
    set(raft_folly_test_lib Folly::folly_synchronization_baton)
  else()
    target_link_libraries(dfly_raft_folly PRIVATE Folly::folly)
    set(raft_folly_test_lib Folly::folly)
  endif()
  cxx_link(dfly_raft_transport dfly_raft_folly)
  helio_cxx_test(storage/folly_io_test dfly_raft_folly ${raft_folly_test_lib} LABELS DFLY)
endif()
if (DF_USE_SSL)
  target_compile_definitions(dfly_raft_transport PRIVATE DFLY_USE_SSL)
endif()
helio_cxx_test(transport/raft_listener_test dfly_raft_transport LABELS DFLY)
target_include_directories(raft_listener_test PRIVATE ${CMAKE_CURRENT_BINARY_DIR}/generated)
