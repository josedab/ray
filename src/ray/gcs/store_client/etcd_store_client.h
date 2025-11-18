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

#include <memory>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/gcs/store_client/store_client.h"

namespace ray {
namespace gcs {

/// Configuration for etcd client connection.
struct EtcdClientOptions {
  /// List of etcd endpoints (e.g., "http://localhost:2379").
  std::vector<std::string> endpoints;
  /// Username for authentication (optional).
  std::string username;
  /// Password for authentication (optional).
  std::string password;
  /// Connection timeout in milliseconds.
  int connect_timeout_ms = 5000;
  /// Request timeout in milliseconds.
  int request_timeout_ms = 30000;
  /// Whether to use TLS.
  bool use_tls = false;
  /// CA certificate path for TLS.
  std::string ca_cert_path;
  /// Client certificate path for mutual TLS.
  std::string client_cert_path;
  /// Client key path for mutual TLS.
  std::string client_key_path;
  /// Key prefix for all operations (namespace).
  std::string key_prefix;
};

/// \class EtcdStoreClient
/// Store client implementation using etcd as the backend.
/// etcd provides strong consistency guarantees with its Raft-based consensus.
///
/// Key format: {prefix}/{table_name}/{key}
/// This differs from Redis which uses HASH data structures.
class EtcdStoreClient : public StoreClient {
 public:
  /// Create an etcd store client.
  ///
  /// \param options Configuration options for etcd connection.
  /// \param io_service IO service for async operations.
  explicit EtcdStoreClient(EtcdClientOptions options,
                            instrumented_io_context &io_service);

  ~EtcdStoreClient() override;

  /// Initialize the etcd connection.
  /// Must be called before any operations.
  Status Connect();

  /// Disconnect from etcd.
  void Disconnect();

  void AsyncPut(const std::string &table_name,
                const std::string &key,
                std::string data,
                bool overwrite,
                Postable<void(bool)> callback) override;

  void AsyncGet(const std::string &table_name,
                const std::string &key,
                ToPostable<OptionalItemCallback<std::string>> callback) override;

  void AsyncGetAll(
      const std::string &table_name,
      Postable<void(absl::flat_hash_map<std::string, std::string>)> callback) override;

  void AsyncMultiGet(
      const std::string &table_name,
      const std::vector<std::string> &keys,
      Postable<void(absl::flat_hash_map<std::string, std::string>)> callback) override;

  void AsyncDelete(const std::string &table_name,
                   const std::string &key,
                   Postable<void(bool)> callback) override;

  void AsyncBatchDelete(const std::string &table_name,
                        const std::vector<std::string> &keys,
                        Postable<void(int64_t)> callback) override;

  void AsyncGetNextJobID(Postable<void(int)> callback) override;

  void AsyncGetKeys(const std::string &table_name,
                    const std::string &prefix,
                    Postable<void(std::vector<std::string>)> callback) override;

  void AsyncExists(const std::string &table_name,
                   const std::string &key,
                   Postable<void(bool)> callback) override;

 private:
  /// Build the full key path for etcd.
  std::string BuildKeyPath(const std::string &table_name, const std::string &key) const;

  /// Build the prefix path for a table.
  std::string BuildTablePrefix(const std::string &table_name) const;

  /// Extract the key from a full etcd key path.
  std::string ExtractKey(const std::string &full_path,
                         const std::string &table_name) const;

  /// Configuration options.
  EtcdClientOptions options_;

  /// IO service for async operations.
  instrumented_io_context &io_service_;

  /// Whether the client is connected.
  bool connected_ = false;

  /// Job ID counter key.
  static constexpr char kJobCounterKey[] = "__job_counter__";
};

}  // namespace gcs
}  // namespace ray
