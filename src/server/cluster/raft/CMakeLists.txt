add_library(dfly_raft
            quorum/election.cc
            replication/high_watermark.cc)
target_compile_features(dfly_raft PUBLIC cxx_std_20)

helio_cxx_test(quorum/election_test dfly_raft LABELS DFLY)
helio_cxx_test(replication/high_watermark_test dfly_raft LABELS DFLY)
