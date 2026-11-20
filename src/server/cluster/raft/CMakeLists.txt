add_library(dfly_raft
            quorum/election.cc
            replication/high_watermark.cc
            storage/file_quorum_state_store.cc)
target_compile_features(dfly_raft PUBLIC cxx_std_20)
cxx_link(dfly_raft absl::strings absl::cleanup TRDP::jsoncons)

helio_cxx_test(quorum/election_test dfly_raft LABELS DFLY)
helio_cxx_test(replication/high_watermark_test dfly_raft LABELS DFLY)
helio_cxx_test(storage/file_quorum_state_store_test dfly_raft LABELS DFLY)
