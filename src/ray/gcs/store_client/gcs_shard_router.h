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

#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "ray/common/id.h"

namespace ray {
namespace gcs {

/// \class GcsShardRouter
/// Routes GCS operations to appropriate shards using consistent hashing.
///
/// Data Distribution Strategy:
/// - Actors: Sharded by ActorID
/// - Jobs: Sharded by JobID
/// - Placement Groups: Sharded by PlacementGroupID
/// - Nodes: Replicated to all shards (shard 0 is primary)
/// - Workers: Sharded by WorkerID
class GcsShardRouter {
 public:
  /// Create a shard router with the specified number of shards.
  ///
  /// \param num_shards The number of shards to distribute data across.
  /// \param virtual_nodes_per_shard Virtual nodes for consistent hashing (default 150).
  explicit GcsShardRouter(int num_shards, int virtual_nodes_per_shard = 150);

  /// Get the shard for an ActorID.
  int GetShard(const ActorID& actor_id) const;

  /// Get the shard for a JobID.
  int GetShard(const JobID& job_id) const;

  /// Get the shard for a PlacementGroupID.
  int GetShard(const PlacementGroupID& pg_id) const;

  /// Get the shard for a WorkerID.
  int GetShard(const WorkerID& worker_id) const;

  /// Get the shard for a NodeID.
  /// Node data is replicated to all shards, but shard 0 is the primary.
  int GetShardForNode(const NodeID& node_id) const;

  /// Get the shard for a generic key (table_name + key).
  /// Used for key-value operations.
  int GetShard(const std::string& table_name, const std::string& key) const;

  /// Get all shards (for scatter-gather operations like GetAll).
  std::vector<int> GetAllShards() const;

  /// Get the number of shards.
  int GetNumShards() const { return num_shards_; }

  /// Check if data should be replicated to all shards.
  /// Currently only node data is replicated.
  bool IsReplicatedTable(const std::string& table_name) const;

  /// Update the number of shards (for rebalancing).
  /// This will rebuild the consistent hash ring.
  void UpdateNumShards(int num_shards);

 private:
  /// Compute hash for consistent hashing.
  uint64_t ComputeHash(const std::string& key) const;

  /// Jump consistent hash implementation.
  /// Provides better distribution than modulo for small shard counts.
  int JumpConsistentHash(uint64_t key) const;

  /// Build the consistent hash ring with virtual nodes.
  void BuildHashRing();

  /// Number of shards.
  int num_shards_;

  /// Virtual nodes per physical shard for better distribution.
  int virtual_nodes_per_shard_;

  /// Consistent hash ring: hash value -> shard id.
  /// Sorted by hash value for binary search.
  std::vector<std::pair<uint64_t, int>> hash_ring_;

  /// Tables that should be replicated to all shards.
  static const std::vector<std::string> kReplicatedTables;
};

}  // namespace gcs
}  // namespace ray
