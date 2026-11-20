add_library(dfly_raft
            bootstrap/meta_properties.cc
            bootstrap/raft_directory.cc
            config/quorum_config.cc
            protocol/api_versions.cc
            protocol/begin_epoch.cc
            protocol/end_epoch.cc
            protocol/rpc_frame.cc
            protocol/vote.cc
            quorum/campaign.cc
            quorum/election.cc
            quorum/vote_handler.cc
            replication/high_watermark.cc
            storage/file_quorum_state_store.cc)
target_compile_features(dfly_raft PUBLIC cxx_std_20)
cxx_link(dfly_raft absl::statusor absl::strings absl::cleanup TRDP::jsoncons)

add_executable(raft_storage bootstrap/raft_storage.cc)
cxx_link(raft_storage dfly_raft)

helio_cxx_test(bootstrap/raft_directory_test dfly_raft LABELS DFLY)
helio_cxx_test(config/quorum_config_test dfly_raft LABELS DFLY)
helio_cxx_test(protocol/rpc_frame_test dfly_raft LABELS DFLY)
helio_cxx_test(protocol/vote_test dfly_raft LABELS DFLY)
helio_cxx_test(quorum/campaign_test dfly_raft LABELS DFLY)
helio_cxx_test(quorum/election_test dfly_raft LABELS DFLY)
helio_cxx_test(quorum/vote_handler_test dfly_raft LABELS DFLY)
helio_cxx_test(replication/high_watermark_test dfly_raft LABELS DFLY)
helio_cxx_test(storage/file_quorum_state_store_test dfly_raft LABELS DFLY)
