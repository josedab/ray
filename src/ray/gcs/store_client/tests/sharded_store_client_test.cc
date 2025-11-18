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

#include "gtest/gtest.h"
#include "ray/gcs/store_client/in_memory_store_client.h"

namespace ray {
namespace gcs {

class ShardedStoreClientTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create in-memory clients for each shard
    for (int i = 0; i < num_shards_; ++i) {
      shard_clients_.push_back(std::make_shared<InMemoryStoreClient>(io_service_));
    }

    router_ = std::make_shared<GcsShardRouter>(num_shards_);
    client_ = std::make_unique<ShardedStoreClient>(shard_clients_, router_);
  }

  void TearDown() override {
    client_.reset();
    shard_clients_.clear();
  }

  void RunIOService() {
    io_service_.run();
    io_service_.restart();
  }

  instrumented_io_context io_service_;
  int num_shards_ = 3;
  std::vector<std::shared_ptr<StoreClient>> shard_clients_;
  std::shared_ptr<GcsShardRouter> router_;
  std::unique_ptr<ShardedStoreClient> client_;
};

TEST_F(ShardedStoreClientTest, PutAndGetRoutesToCorrectShard) {
  std::string table = "TEST_TABLE";
  std::string key = "test_key";
  std::string value = "test_value";

  bool put_done = false;
  client_->AsyncPut(table, key, value, true, [&](bool added) {
    EXPECT_TRUE(added);
    put_done = true;
  });
  RunIOService();
  EXPECT_TRUE(put_done);

  bool get_done = false;
  client_->AsyncGet(table, key,
                    [&](const Status &status, const std::optional<std::string> &result) {
                      EXPECT_TRUE(status.ok());
                      EXPECT_TRUE(result.has_value());
                      EXPECT_EQ(result.value(), value);
                      get_done = true;
                    });
  RunIOService();
  EXPECT_TRUE(get_done);
}

TEST_F(ShardedStoreClientTest, GetAllScattersToAllShards) {
  std::string table = "ACTOR";

  // Put data that will go to different shards
  int num_keys = 100;
  for (int i = 0; i < num_keys; ++i) {
    std::string key = "actor_" + std::to_string(i);
    std::string value = "value_" + std::to_string(i);
    bool done = false;
    client_->AsyncPut(table, key, value, true, [&](bool) { done = true; });
    RunIOService();
    EXPECT_TRUE(done);
  }

  // GetAll should gather from all shards
  bool get_all_done = false;
  client_->AsyncGetAll(
      table, [&](absl::flat_hash_map<std::string, std::string> results) {
        EXPECT_EQ(results.size(), num_keys);
        get_all_done = true;
      });
  RunIOService();
  EXPECT_TRUE(get_all_done);
}

TEST_F(ShardedStoreClientTest, MultiGetRoutesToCorrectShards) {
  std::string table = "TEST_TABLE";

  // Put some data
  std::vector<std::string> keys;
  for (int i = 0; i < 50; ++i) {
    std::string key = "key_" + std::to_string(i);
    std::string value = "value_" + std::to_string(i);
    keys.push_back(key);

    bool done = false;
    client_->AsyncPut(table, key, value, true, [&](bool) { done = true; });
    RunIOService();
  }

  // MultiGet should route to appropriate shards
  bool multi_get_done = false;
  client_->AsyncMultiGet(
      table, keys, [&](absl::flat_hash_map<std::string, std::string> results) {
        EXPECT_EQ(results.size(), keys.size());
        for (int i = 0; i < 50; ++i) {
          std::string key = "key_" + std::to_string(i);
          EXPECT_EQ(results[key], "value_" + std::to_string(i));
        }
        multi_get_done = true;
      });
  RunIOService();
  EXPECT_TRUE(multi_get_done);
}

TEST_F(ShardedStoreClientTest, DeleteRoutesToCorrectShard) {
  std::string table = "TEST_TABLE";
  std::string key = "test_key";
  std::string value = "test_value";

  // Put
  bool put_done = false;
  client_->AsyncPut(table, key, value, true, [&](bool) { put_done = true; });
  RunIOService();
  EXPECT_TRUE(put_done);

  // Delete
  bool delete_done = false;
  client_->AsyncDelete(table, key, [&](bool deleted) {
    EXPECT_TRUE(deleted);
    delete_done = true;
  });
  RunIOService();
  EXPECT_TRUE(delete_done);

  // Verify deleted
  bool get_done = false;
  client_->AsyncGet(table, key,
                    [&](const Status &status, const std::optional<std::string> &result) {
                      EXPECT_TRUE(status.ok());
                      EXPECT_FALSE(result.has_value());
                      get_done = true;
                    });
  RunIOService();
  EXPECT_TRUE(get_done);
}

TEST_F(ShardedStoreClientTest, BatchDeleteRoutesToCorrectShards) {
  std::string table = "TEST_TABLE";

  // Put data
  std::vector<std::string> keys;
  for (int i = 0; i < 30; ++i) {
    std::string key = "key_" + std::to_string(i);
    keys.push_back(key);
    bool done = false;
    client_->AsyncPut(table, key, "value", true, [&](bool) { done = true; });
    RunIOService();
  }

  // Batch delete
  bool delete_done = false;
  client_->AsyncBatchDelete(table, keys, [&](int64_t deleted) {
    EXPECT_EQ(deleted, 30);
    delete_done = true;
  });
  RunIOService();
  EXPECT_TRUE(delete_done);
}

TEST_F(ShardedStoreClientTest, GetNextJobIDAlwaysUsesShardZero) {
  // Job ID counter should always be on shard 0
  int job_id_1 = -1;
  int job_id_2 = -1;

  bool done1 = false;
  client_->AsyncGetNextJobID([&](int id) {
    job_id_1 = id;
    done1 = true;
  });
  RunIOService();
  EXPECT_TRUE(done1);

  bool done2 = false;
  client_->AsyncGetNextJobID([&](int id) {
    job_id_2 = id;
    done2 = true;
  });
  RunIOService();
  EXPECT_TRUE(done2);

  EXPECT_EQ(job_id_2, job_id_1 + 1);
}

TEST_F(ShardedStoreClientTest, GetKeysScattersToAllShards) {
  std::string table = "TEST_TABLE";

  // Put data with same prefix
  for (int i = 0; i < 50; ++i) {
    std::string key = "prefix_" + std::to_string(i);
    bool done = false;
    client_->AsyncPut(table, key, "value", true, [&](bool) { done = true; });
    RunIOService();
  }

  // GetKeys should gather from all shards
  bool get_keys_done = false;
  client_->AsyncGetKeys(table, "prefix_", [&](std::vector<std::string> keys) {
    EXPECT_EQ(keys.size(), 50);
    get_keys_done = true;
  });
  RunIOService();
  EXPECT_TRUE(get_keys_done);
}

TEST_F(ShardedStoreClientTest, ExistsRoutesToCorrectShard) {
  std::string table = "TEST_TABLE";
  std::string key = "test_key";

  // Check non-existent
  bool exists_done = false;
  client_->AsyncExists(table, key, [&](bool exists) {
    EXPECT_FALSE(exists);
    exists_done = true;
  });
  RunIOService();
  EXPECT_TRUE(exists_done);

  // Put
  bool put_done = false;
  client_->AsyncPut(table, key, "value", true, [&](bool) { put_done = true; });
  RunIOService();
  EXPECT_TRUE(put_done);

  // Check exists
  exists_done = false;
  client_->AsyncExists(table, key, [&](bool exists) {
    EXPECT_TRUE(exists);
    exists_done = true;
  });
  RunIOService();
  EXPECT_TRUE(exists_done);
}

TEST_F(ShardedStoreClientTest, ReplicatedTableWritesToAllShards) {
  std::string table = "NODE";  // Replicated table
  std::string key = "node_1";
  std::string value = "node_data";

  // Put to replicated table
  bool put_done = false;
  client_->AsyncPut(table, key, value, true, [&](bool) { put_done = true; });
  RunIOService();
  EXPECT_TRUE(put_done);

  // Verify data exists in all shards
  for (int shard = 0; shard < num_shards_; ++shard) {
    bool get_done = false;
    shard_clients_[shard]->AsyncGet(
        table, key,
        [&](const Status &status, const std::optional<std::string> &result) {
          EXPECT_TRUE(status.ok());
          EXPECT_TRUE(result.has_value());
          EXPECT_EQ(result.value(), value);
          get_done = true;
        });
    RunIOService();
    EXPECT_TRUE(get_done) << "Shard " << shard << " did not have the data";
  }
}

TEST_F(ShardedStoreClientTest, EmptyMultiGetReturnsEmpty) {
  std::string table = "TEST_TABLE";
  std::vector<std::string> empty_keys;

  bool done = false;
  client_->AsyncMultiGet(
      table, empty_keys, [&](absl::flat_hash_map<std::string, std::string> results) {
        EXPECT_TRUE(results.empty());
        done = true;
      });
  RunIOService();
  EXPECT_TRUE(done);
}

TEST_F(ShardedStoreClientTest, EmptyBatchDeleteReturnsZero) {
  std::string table = "TEST_TABLE";
  std::vector<std::string> empty_keys;

  bool done = false;
  client_->AsyncBatchDelete(table, empty_keys, [&](int64_t deleted) {
    EXPECT_EQ(deleted, 0);
    done = true;
  });
  RunIOService();
  EXPECT_TRUE(done);
}

}  // namespace gcs
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
