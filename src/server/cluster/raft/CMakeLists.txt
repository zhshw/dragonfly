add_library(dfly_raft replication/high_watermark.cc)
target_compile_features(dfly_raft PUBLIC cxx_std_20)

helio_cxx_test(replication/high_watermark_test dfly_raft LABELS DFLY)
