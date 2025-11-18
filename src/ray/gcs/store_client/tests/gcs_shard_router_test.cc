// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/gcs/store_client/gcs_shard_router.h"

#include <unordered_map>

#include "gtest/gtest.h"
#include "ray/common/id.h"

namespace ray {
namespace gcs {

class GcsShardRouterTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(GcsShardRouterTest, SingleShardAlwaysReturnsZero) {
  GcsShardRouter router(1);

  // All operations should return shard 0
  for (int i = 0; i < 100; ++i) {
    ActorID actor_id = ActorID::FromRandom();
    EXPECT_EQ(router.GetShard(actor_id), 0);
  }
}

TEST_F(GcsShardRouterTest, ConsistentHashingIsConsistent) {
  GcsShardRouter router(5);

  // Same key should always map to same shard
  ActorID actor_id = ActorID::FromRandom();
  int first_shard = router.GetShard(actor_id);

  for (int i = 0; i < 100; ++i) {
    EXPECT_EQ(router.GetShard(actor_id), first_shard);
  }
}

TEST_F(GcsShardRouterTest, DifferentIDTypesCanMapToDifferentShards) {
  GcsShardRouter router(10);

  // Generate IDs and verify distribution
  std::unordered_map<int, int> actor_shard_counts;
  std::unordered_map<int, int> job_shard_counts;

  for (int i = 0; i < 1000; ++i) {
    ActorID actor_id = ActorID::FromRandom();
    JobID job_id = JobID::FromInt(i);

    actor_shard_counts[router.GetShard(actor_id)]++;
    job_shard_counts[router.GetShard(job_id)]++;
  }

  // All shards should have some entries (probabilistic, but very likely)
  for (int shard = 0; shard < 10; ++shard) {
    EXPECT_GT(actor_shard_counts[shard], 0) << "Shard " << shard << " has no actors";
    EXPECT_GT(job_shard_counts[shard], 0) << "Shard " << shard << " has no jobs";
  }
}

TEST_F(GcsShardRouterTest, NodeDataAlwaysGoesToShardZero) {
  GcsShardRouter router(10);

  // Node data should always go to shard 0 (replicated)
  for (int i = 0; i < 100; ++i) {
    NodeID node_id = NodeID::FromRandom();
    EXPECT_EQ(router.GetShardForNode(node_id), 0);
  }
}

TEST_F(GcsShardRouterTest, ReplicatedTablesReturnShardZero) {
  GcsShardRouter router(5);

  // Replicated tables should always return shard 0
  EXPECT_TRUE(router.IsReplicatedTable("NODE"));
  EXPECT_TRUE(router.IsReplicatedTable("NODE_RESOURCE"));
  EXPECT_TRUE(router.IsReplicatedTable("CLUSTER_RESOURCE"));

  EXPECT_EQ(router.GetShard("NODE", "some_key"), 0);
  EXPECT_EQ(router.GetShard("NODE_RESOURCE", "some_key"), 0);
}

TEST_F(GcsShardRouterTest, NonReplicatedTablesAreSharded) {
  GcsShardRouter router(5);

  EXPECT_FALSE(router.IsReplicatedTable("ACTOR"));
  EXPECT_FALSE(router.IsReplicatedTable("JOB"));
  EXPECT_FALSE(router.IsReplicatedTable("PLACEMENT_GROUP"));
}

TEST_F(GcsShardRouterTest, GetAllShardsReturnsAllIndices) {
  GcsShardRouter router(7);

  auto shards = router.GetAllShards();
  EXPECT_EQ(shards.size(), 7);

  for (int i = 0; i < 7; ++i) {
    EXPECT_EQ(shards[i], i);
  }
}

TEST_F(GcsShardRouterTest, DistributionIsReasonablyUniform) {
  GcsShardRouter router(5);

  std::unordered_map<int, int> shard_counts;
  int num_keys = 10000;

  for (int i = 0; i < num_keys; ++i) {
    std::string key = "key_" + std::to_string(i);
    int shard = router.GetShard("TEST_TABLE", key);
    shard_counts[shard]++;
  }

  // Each shard should have roughly 2000 keys (±30%)
  int expected = num_keys / 5;
  int min_expected = expected * 0.7;
  int max_expected = expected * 1.3;

  for (int shard = 0; shard < 5; ++shard) {
    EXPECT_GE(shard_counts[shard], min_expected)
        << "Shard " << shard << " has too few keys: " << shard_counts[shard];
    EXPECT_LE(shard_counts[shard], max_expected)
        << "Shard " << shard << " has too many keys: " << shard_counts[shard];
  }
}

TEST_F(GcsShardRouterTest, ShardCountCanBeUpdated) {
  GcsShardRouter router(3);
  EXPECT_EQ(router.GetNumShards(), 3);

  router.UpdateNumShards(5);
  EXPECT_EQ(router.GetNumShards(), 5);

  // Verify distribution still works after update
  ActorID actor_id = ActorID::FromRandom();
  int shard = router.GetShard(actor_id);
  EXPECT_GE(shard, 0);
  EXPECT_LT(shard, 5);
}

TEST_F(GcsShardRouterTest, InvalidShardCountThrows) {
  EXPECT_DEATH(GcsShardRouter(0), "Number of shards must be positive");
  EXPECT_DEATH(GcsShardRouter(-1), "Number of shards must be positive");
}

TEST_F(GcsShardRouterTest, PlacementGroupSharding) {
  GcsShardRouter router(5);

  std::unordered_map<int, int> shard_counts;
  for (int i = 0; i < 1000; ++i) {
    PlacementGroupID pg_id = PlacementGroupID::FromRandom();
    shard_counts[router.GetShard(pg_id)]++;
  }

  // All shards should have entries
  for (int shard = 0; shard < 5; ++shard) {
    EXPECT_GT(shard_counts[shard], 0);
  }
}

TEST_F(GcsShardRouterTest, WorkerSharding) {
  GcsShardRouter router(5);

  std::unordered_map<int, int> shard_counts;
  for (int i = 0; i < 1000; ++i) {
    WorkerID worker_id = WorkerID::FromRandom();
    shard_counts[router.GetShard(worker_id)]++;
  }

  // All shards should have entries
  for (int shard = 0; shard < 5; ++shard) {
    EXPECT_GT(shard_counts[shard], 0);
  }
}

}  // namespace gcs
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
