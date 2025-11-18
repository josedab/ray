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

#include <atomic>
#include <memory>
#include <vector>

#include "ray/gcs/store_client/gcs_shard_router.h"
#include "ray/gcs/store_client/store_client.h"

namespace ray {
namespace gcs {

/// \class ShardedStoreClient
/// A store client that distributes data across multiple shards using consistent hashing.
/// This enables horizontal scaling of GCS storage.
class ShardedStoreClient : public StoreClient {
 public:
  /// Create a sharded store client.
  ///
  /// \param shard_clients Vector of store clients, one per shard.
  /// \param router The shard router for determining shard assignment.
  ShardedStoreClient(std::vector<std::shared_ptr<StoreClient>> shard_clients,
                     std::shared_ptr<GcsShardRouter> router);

  ~ShardedStoreClient() override = default;

  /// Write data to the appropriate shard based on key hash.
  void AsyncPut(const std::string &table_name,
                const std::string &key,
                std::string data,
                bool overwrite,
                Postable<void(bool)> callback) override;

  /// Get data from the appropriate shard.
  void AsyncGet(const std::string &table_name,
                const std::string &key,
                ToPostable<OptionalItemCallback<std::string>> callback) override;

  /// Get all data from all shards (scatter-gather).
  void AsyncGetAll(
      const std::string &table_name,
      Postable<void(absl::flat_hash_map<std::string, std::string>)> callback) override;

  /// Get multiple keys from appropriate shards.
  void AsyncMultiGet(
      const std::string &table_name,
      const std::vector<std::string> &keys,
      Postable<void(absl::flat_hash_map<std::string, std::string>)> callback) override;

  /// Delete data from the appropriate shard.
  void AsyncDelete(const std::string &table_name,
                   const std::string &key,
                   Postable<void(bool)> callback) override;

  /// Batch delete from appropriate shards.
  void AsyncBatchDelete(const std::string &table_name,
                        const std::vector<std::string> &keys,
                        Postable<void(int64_t)> callback) override;

  /// Get next job ID (always from shard 0).
  void AsyncGetNextJobID(Postable<void(int)> callback) override;

  /// Get keys matching prefix from all shards.
  void AsyncGetKeys(const std::string &table_name,
                    const std::string &prefix,
                    Postable<void(std::vector<std::string>)> callback) override;

  /// Check if key exists in the appropriate shard.
  void AsyncExists(const std::string &table_name,
                   const std::string &key,
                   Postable<void(bool)> callback) override;

  /// Get the number of shards.
  int GetNumShards() const { return shard_clients_.size(); }

  /// Get a specific shard client.
  std::shared_ptr<StoreClient> GetShardClient(int shard) const {
    return shard_clients_[shard];
  }

 private:
  /// Get the shard for a given table and key.
  int GetShard(const std::string &table_name, const std::string &key) const;

  /// Group keys by their target shard.
  absl::flat_hash_map<int, std::vector<std::string>> GroupKeysByShard(
      const std::string &table_name, const std::vector<std::string> &keys) const;

  /// Store clients for each shard.
  std::vector<std::shared_ptr<StoreClient>> shard_clients_;

  /// Router for determining shard assignment.
  std::shared_ptr<GcsShardRouter> router_;
};

}  // namespace gcs
}  // namespace ray
