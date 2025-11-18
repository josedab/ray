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

#include "ray/gcs/store_client/sharded_store_client.h"

#include <memory>
#include <mutex>

#include "ray/util/logging.h"

namespace ray {
namespace gcs {

ShardedStoreClient::ShardedStoreClient(
    std::vector<std::shared_ptr<StoreClient>> shard_clients,
    std::shared_ptr<GcsShardRouter> router)
    : shard_clients_(std::move(shard_clients)), router_(std::move(router)) {
  RAY_CHECK(!shard_clients_.empty()) << "Must have at least one shard client";
  RAY_CHECK(router_ != nullptr) << "Router cannot be null";
  RAY_CHECK(static_cast<int>(shard_clients_.size()) == router_->GetNumShards())
      << "Number of shard clients must match router shard count";
}

int ShardedStoreClient::GetShard(const std::string &table_name,
                                  const std::string &key) const {
  return router_->GetShard(table_name, key);
}

absl::flat_hash_map<int, std::vector<std::string>> ShardedStoreClient::GroupKeysByShard(
    const std::string &table_name, const std::vector<std::string> &keys) const {
  absl::flat_hash_map<int, std::vector<std::string>> shard_to_keys;
  for (const auto &key : keys) {
    int shard = GetShard(table_name, key);
    shard_to_keys[shard].push_back(key);
  }
  return shard_to_keys;
}

void ShardedStoreClient::AsyncPut(const std::string &table_name,
                                   const std::string &key,
                                   std::string data,
                                   bool overwrite,
                                   Postable<void(bool)> callback) {
  // For replicated tables, write to all shards.
  if (router_->IsReplicatedTable(table_name)) {
    auto num_shards = shard_clients_.size();
    auto counter = std::make_shared<std::atomic<size_t>>(num_shards);
    auto any_added = std::make_shared<std::atomic<bool>>(false);

    for (size_t i = 0; i < num_shards; ++i) {
      shard_clients_[i]->AsyncPut(
          table_name,
          key,
          data,
          overwrite,
          [callback, counter, any_added](bool added) mutable {
            if (added) {
              any_added->store(true);
            }
            if (counter->fetch_sub(1) == 1) {
              // All shards have responded.
              callback(any_added->load());
            }
          });
    }
  } else {
    // Route to the appropriate shard.
    int shard = GetShard(table_name, key);
    shard_clients_[shard]->AsyncPut(
        table_name, key, std::move(data), overwrite, std::move(callback));
  }
}

void ShardedStoreClient::AsyncGet(
    const std::string &table_name,
    const std::string &key,
    ToPostable<OptionalItemCallback<std::string>> callback) {
  int shard = GetShard(table_name, key);
  shard_clients_[shard]->AsyncGet(table_name, key, std::move(callback));
}

void ShardedStoreClient::AsyncGetAll(
    const std::string &table_name,
    Postable<void(absl::flat_hash_map<std::string, std::string>)> callback) {
  // Scatter-gather from all shards.
  auto num_shards = shard_clients_.size();
  auto results =
      std::make_shared<absl::flat_hash_map<std::string, std::string>>();
  auto counter = std::make_shared<std::atomic<size_t>>(num_shards);
  auto mutex = std::make_shared<std::mutex>();

  for (size_t i = 0; i < num_shards; ++i) {
    shard_clients_[i]->AsyncGetAll(
        table_name,
        [callback, results, counter, mutex](
            absl::flat_hash_map<std::string, std::string> shard_results) mutable {
          {
            std::lock_guard<std::mutex> lock(*mutex);
            for (auto &[key, value] : shard_results) {
              results->emplace(std::move(key), std::move(value));
            }
          }
          if (counter->fetch_sub(1) == 1) {
            callback(std::move(*results));
          }
        });
  }
}

void ShardedStoreClient::AsyncMultiGet(
    const std::string &table_name,
    const std::vector<std::string> &keys,
    Postable<void(absl::flat_hash_map<std::string, std::string>)> callback) {
  if (keys.empty()) {
    callback(absl::flat_hash_map<std::string, std::string>());
    return;
  }

  // Group keys by shard.
  auto shard_to_keys = GroupKeysByShard(table_name, keys);
  auto num_shards_to_query = shard_to_keys.size();
  auto results =
      std::make_shared<absl::flat_hash_map<std::string, std::string>>();
  auto counter = std::make_shared<std::atomic<size_t>>(num_shards_to_query);
  auto mutex = std::make_shared<std::mutex>();

  for (auto &[shard, shard_keys] : shard_to_keys) {
    shard_clients_[shard]->AsyncMultiGet(
        table_name,
        shard_keys,
        [callback, results, counter, mutex](
            absl::flat_hash_map<std::string, std::string> shard_results) mutable {
          {
            std::lock_guard<std::mutex> lock(*mutex);
            for (auto &[key, value] : shard_results) {
              results->emplace(std::move(key), std::move(value));
            }
          }
          if (counter->fetch_sub(1) == 1) {
            callback(std::move(*results));
          }
        });
  }
}

void ShardedStoreClient::AsyncDelete(const std::string &table_name,
                                      const std::string &key,
                                      Postable<void(bool)> callback) {
  // For replicated tables, delete from all shards.
  if (router_->IsReplicatedTable(table_name)) {
    auto num_shards = shard_clients_.size();
    auto counter = std::make_shared<std::atomic<size_t>>(num_shards);
    auto any_deleted = std::make_shared<std::atomic<bool>>(false);

    for (size_t i = 0; i < num_shards; ++i) {
      shard_clients_[i]->AsyncDelete(
          table_name,
          key,
          [callback, counter, any_deleted](bool deleted) mutable {
            if (deleted) {
              any_deleted->store(true);
            }
            if (counter->fetch_sub(1) == 1) {
              callback(any_deleted->load());
            }
          });
    }
  } else {
    int shard = GetShard(table_name, key);
    shard_clients_[shard]->AsyncDelete(table_name, key, std::move(callback));
  }
}

void ShardedStoreClient::AsyncBatchDelete(const std::string &table_name,
                                           const std::vector<std::string> &keys,
                                           Postable<void(int64_t)> callback) {
  if (keys.empty()) {
    callback(0);
    return;
  }

  // For replicated tables, delete from all shards.
  if (router_->IsReplicatedTable(table_name)) {
    auto num_shards = shard_clients_.size();
    auto counter = std::make_shared<std::atomic<size_t>>(num_shards);
    auto total_deleted = std::make_shared<std::atomic<int64_t>>(0);

    for (size_t i = 0; i < num_shards; ++i) {
      shard_clients_[i]->AsyncBatchDelete(
          table_name,
          keys,
          [callback, counter, total_deleted](int64_t deleted) mutable {
            total_deleted->fetch_add(deleted);
            if (counter->fetch_sub(1) == 1) {
              // Return the max deleted count (since replicated data should be same).
              callback(total_deleted->load() / static_cast<int64_t>(counter->load() + 1));
            }
          });
    }
  } else {
    // Group keys by shard.
    auto shard_to_keys = GroupKeysByShard(table_name, keys);
    auto num_shards_to_query = shard_to_keys.size();
    auto counter = std::make_shared<std::atomic<size_t>>(num_shards_to_query);
    auto total_deleted = std::make_shared<std::atomic<int64_t>>(0);

    for (auto &[shard, shard_keys] : shard_to_keys) {
      shard_clients_[shard]->AsyncBatchDelete(
          table_name,
          shard_keys,
          [callback, counter, total_deleted](int64_t deleted) mutable {
            total_deleted->fetch_add(deleted);
            if (counter->fetch_sub(1) == 1) {
              callback(total_deleted->load());
            }
          });
    }
  }
}

void ShardedStoreClient::AsyncGetNextJobID(Postable<void(int)> callback) {
  // Job ID counter is always on shard 0 to ensure uniqueness.
  shard_clients_[0]->AsyncGetNextJobID(std::move(callback));
}

void ShardedStoreClient::AsyncGetKeys(const std::string &table_name,
                                       const std::string &prefix,
                                       Postable<void(std::vector<std::string>)> callback) {
  // Scatter-gather from all shards.
  auto num_shards = shard_clients_.size();
  auto results = std::make_shared<std::vector<std::string>>();
  auto counter = std::make_shared<std::atomic<size_t>>(num_shards);
  auto mutex = std::make_shared<std::mutex>();

  for (size_t i = 0; i < num_shards; ++i) {
    shard_clients_[i]->AsyncGetKeys(
        table_name,
        prefix,
        [callback, results, counter, mutex](
            std::vector<std::string> shard_keys) mutable {
          {
            std::lock_guard<std::mutex> lock(*mutex);
            results->insert(results->end(), shard_keys.begin(), shard_keys.end());
          }
          if (counter->fetch_sub(1) == 1) {
            callback(std::move(*results));
          }
        });
  }
}

void ShardedStoreClient::AsyncExists(const std::string &table_name,
                                      const std::string &key,
                                      Postable<void(bool)> callback) {
  int shard = GetShard(table_name, key);
  shard_clients_[shard]->AsyncExists(table_name, key, std::move(callback));
}

}  // namespace gcs
}  // namespace ray
