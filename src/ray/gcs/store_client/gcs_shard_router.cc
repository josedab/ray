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

#include <algorithm>
#include <cstring>

#include "ray/util/logging.h"

namespace ray {
namespace gcs {

// Tables that contain node-related data which should be replicated to all shards.
const std::vector<std::string> GcsShardRouter::kReplicatedTables = {
    "NODE",
    "NODE_RESOURCE",
    "CLUSTER_RESOURCE",
};

GcsShardRouter::GcsShardRouter(int num_shards, int virtual_nodes_per_shard)
    : num_shards_(num_shards), virtual_nodes_per_shard_(virtual_nodes_per_shard) {
  RAY_CHECK(num_shards_ > 0) << "Number of shards must be positive";
  BuildHashRing();
}

int GcsShardRouter::GetShard(const ActorID& actor_id) const {
  return GetShard("ACTOR", actor_id.Binary());
}

int GcsShardRouter::GetShard(const JobID& job_id) const {
  return GetShard("JOB", job_id.Binary());
}

int GcsShardRouter::GetShard(const PlacementGroupID& pg_id) const {
  return GetShard("PLACEMENT_GROUP", pg_id.Binary());
}

int GcsShardRouter::GetShard(const WorkerID& worker_id) const {
  return GetShard("WORKER", worker_id.Binary());
}

int GcsShardRouter::GetShardForNode(const NodeID& node_id) const {
  // Node data is replicated, but shard 0 is the primary for writes.
  // For reads, any shard can be used.
  return 0;
}

int GcsShardRouter::GetShard(const std::string& table_name,
                              const std::string& key) const {
  // If it's a replicated table, always route to shard 0 for writes.
  if (IsReplicatedTable(table_name)) {
    return 0;
  }

  // For single shard setup, always return 0.
  if (num_shards_ == 1) {
    return 0;
  }

  // Combine table name and key for hashing.
  std::string combined = table_name + ":" + key;
  uint64_t hash = ComputeHash(combined);

  // Use jump consistent hash for better distribution.
  return JumpConsistentHash(hash);
}

std::vector<int> GcsShardRouter::GetAllShards() const {
  std::vector<int> shards;
  shards.reserve(num_shards_);
  for (int i = 0; i < num_shards_; ++i) {
    shards.push_back(i);
  }
  return shards;
}

bool GcsShardRouter::IsReplicatedTable(const std::string& table_name) const {
  return std::find(kReplicatedTables.begin(), kReplicatedTables.end(), table_name) !=
         kReplicatedTables.end();
}

void GcsShardRouter::UpdateNumShards(int num_shards) {
  RAY_CHECK(num_shards > 0) << "Number of shards must be positive";
  num_shards_ = num_shards;
  BuildHashRing();
}

uint64_t GcsShardRouter::ComputeHash(const std::string& key) const {
  // Use FNV-1a hash for good distribution.
  const uint64_t FNV_OFFSET_BASIS = 14695981039346656037ULL;
  const uint64_t FNV_PRIME = 1099511628211ULL;

  uint64_t hash = FNV_OFFSET_BASIS;
  for (char c : key) {
    hash ^= static_cast<uint64_t>(static_cast<unsigned char>(c));
    hash *= FNV_PRIME;
  }
  return hash;
}

int GcsShardRouter::JumpConsistentHash(uint64_t key) const {
  // Jump consistent hash algorithm by Google.
  // Provides excellent distribution with minimal memory usage.
  // Reference: https://arxiv.org/pdf/1406.2294.pdf

  int64_t b = -1, j = 0;
  while (j < num_shards_) {
    b = j;
    key = key * 2862933555777941757ULL + 1;
    j = static_cast<int64_t>((b + 1) * (static_cast<double>(1LL << 31) /
                                        static_cast<double>((key >> 33) + 1)));
  }
  return static_cast<int>(b);
}

void GcsShardRouter::BuildHashRing() {
  hash_ring_.clear();
  hash_ring_.reserve(num_shards_ * virtual_nodes_per_shard_);

  for (int shard = 0; shard < num_shards_; ++shard) {
    for (int vnode = 0; vnode < virtual_nodes_per_shard_; ++vnode) {
      std::string vnode_key =
          "shard:" + std::to_string(shard) + ":vnode:" + std::to_string(vnode);
      uint64_t hash = ComputeHash(vnode_key);
      hash_ring_.emplace_back(hash, shard);
    }
  }

  // Sort by hash value for binary search.
  std::sort(hash_ring_.begin(), hash_ring_.end(),
            [](const auto& a, const auto& b) { return a.first < b.first; });
}

}  // namespace gcs
}  // namespace ray
