add_library(dfly_raft
            bootstrap/meta_properties.cc
            bootstrap/raft_directory.cc
            config/quorum_config.cc
            log/log_files.cc
            log/partition_log.cc
            log/partition_snapshot.cc
            log/record_batch.cc
            log/snapshot_file.cc
            metadata/record_serde.cc
            protocol/api_versions.cc
            protocol/begin_epoch.cc
            protocol/describe_quorum.cc
            protocol/end_epoch.cc
            protocol/fetch.cc
            protocol/fetch_snapshot.cc
            protocol/metadata_records.cc
            protocol/rpc_frame.cc
            protocol/vote.cc
            quorum/campaign.cc
            quorum/describe.cc
            quorum/election.cc
            quorum/epoch_control.cc
            quorum/fetch_replication.cc
            quorum/replica_progress.cc
            quorum/vote_handler.cc
            replication/high_watermark.cc
            storage/file_quorum_state_store.cc)
target_compile_features(dfly_raft PUBLIC cxx_std_20)
cxx_link(dfly_raft absl::crc32c absl::statusor absl::str_format absl::strings absl::cleanup TRDP::jsoncons)

add_executable(raft_storage bootstrap/raft_storage.cc)
cxx_link(raft_storage dfly_raft)

helio_cxx_test(bootstrap/raft_directory_test dfly_raft LABELS DFLY)
helio_cxx_test(config/quorum_config_test dfly_raft LABELS DFLY)
helio_cxx_test(log/partition_log_test dfly_raft LABELS DFLY)
helio_cxx_test(log/partition_snapshot_test dfly_raft LABELS DFLY)
helio_cxx_test(log/record_batch_test dfly_raft LABELS DFLY)
helio_cxx_test(metadata/record_serde_test dfly_raft LABELS DFLY)
helio_cxx_test(protocol/fetch_test dfly_raft LABELS DFLY)
helio_cxx_test(protocol/rpc_frame_test dfly_raft LABELS DFLY)
helio_cxx_test(protocol/vote_test dfly_raft LABELS DFLY)
helio_cxx_test(quorum/campaign_test dfly_raft LABELS DFLY)
helio_cxx_test(quorum/describe_test dfly_raft LABELS DFLY)
helio_cxx_test(quorum/election_test dfly_raft LABELS DFLY)
helio_cxx_test(quorum/epoch_control_test dfly_raft LABELS DFLY)
helio_cxx_test(quorum/fetch_replication_test dfly_raft LABELS DFLY)
helio_cxx_test(quorum/vote_handler_test dfly_raft LABELS DFLY)
helio_cxx_test(replication/high_watermark_test dfly_raft LABELS DFLY)
helio_cxx_test(storage/file_quorum_state_store_test dfly_raft LABELS DFLY)

add_library(dfly_raft_transport runtime/controller.cc transport/peer_connection.cc transport/raft_listener.cc)
cxx_link(dfly_raft_transport dfly_raft fibers2 absl::random_random)
helio_cxx_test(transport/raft_listener_test dfly_raft_transport LABELS DFLY)
