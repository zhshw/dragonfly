// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/config/quorum_config.h"

#include <gtest/gtest.h>

namespace dfly::cluster::raft {
namespace {

// KafkaConfigTest.testValidQuorumVotersParsingWithIpAddress / WithMultipleHost.
TEST(QuorumConfigTest, StaticVoters) {
  auto loopback = ParseVoterConnections("1@127.0.0.1:9092");
  ASSERT_TRUE(loopback.ok());
  EXPECT_EQ(loopback->at(1), (QuorumEndpoint{"127.0.0.1", 9092}));
  auto hosts = ParseVoterConnections("1@kafka1:9092, 2@kafka2:9092,3@kafka3:9092");
  ASSERT_TRUE(hosts.ok());
  EXPECT_EQ(*hosts, (VoterConnections{
                        {1, {"kafka1", 9092}}, {2, {"kafka2", 9092}}, {3, {"kafka3", 9092}}}));
}

TEST(QuorumConfigTest, InvalidEntriesAndNonRoutableVoters) {
  for (auto input : {"1", "1@", "1:", "x@server:1", "1@kafka1", "1@kafka1:9092,", "1@kafka1:9092,2",
                     "1@kafka1:9092:1@kafka2:9092", "1@0.0.0.0:9092", "+-1@host:123",
                     "2147483648@host:1", "1@host:65536", "1@host:-1", "1@host name:9"}) {
    EXPECT_FALSE(ParseVoterConnections(input).ok()) << input;
  }
}

TEST(QuorumConfigTest, DuplicateVoterChangesAddressAndPreservesOneVote) {
  auto voters = ParseVoterConnections("3000@old:1,+3000@CONTROLLER://[::1]:9092@");
  ASSERT_TRUE(voters.ok());
  EXPECT_EQ(*voters, (VoterConnections{{3000, {"::1", 9092}}}));
}

// KafkaConfigTest.testParseQuorumBootstrapServers.
TEST(QuorumConfigTest, BootstrapKeepsOrderAndDoesNotAssignVoterIds) {
  auto endpoints = ParseBootstrapServers("kafka1:9092,kafka2:9092,[fe80::1%eth0]:65535");
  ASSERT_TRUE(endpoints.ok());
  EXPECT_EQ(*endpoints, (std::vector<QuorumEndpoint>{
                            {"kafka1", 9092}, {"kafka2", 9092}, {"fe80::1%eth0", 65535}}));
  EXPECT_FALSE(ParseBootstrapServers("1@kafka1:9092").ok());
  EXPECT_TRUE(ParseBootstrapServers("")->empty());
  EXPECT_TRUE(ParseVoterConnections("")->empty());
}

}  // namespace
}  // namespace dfly::cluster::raft
