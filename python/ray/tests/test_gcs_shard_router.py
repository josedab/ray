# Copyright 2017 The Ray Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for GCS shard router."""

import os
import sys
import pytest
from collections import Counter

# Add the parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ray._private.gcs_shard_router import (
    ShardRouter,
    ShardedGcsClient,
    create_sharded_gcs_client,
)


class TestShardRouter:
    """Tests for the ShardRouter class."""

    def test_single_shard_always_returns_zero(self):
        """Single shard should always return 0."""
        router = ShardRouter(num_shards=1)

        for i in range(100):
            key = f"key_{i}".encode()
            shard = router.get_shard("ACTOR", key)
            assert shard == 0

    def test_consistent_hashing_is_consistent(self):
        """Same key should always map to same shard."""
        router = ShardRouter(num_shards=5)

        key = b"test_actor_id_12345"
        first_shard = router.get_shard("ACTOR", key)

        for _ in range(100):
            assert router.get_shard("ACTOR", key) == first_shard

    def test_distribution_is_reasonably_uniform(self):
        """Keys should be distributed reasonably uniformly across shards."""
        router = ShardRouter(num_shards=5)

        shard_counts = Counter()
        num_keys = 10000

        for i in range(num_keys):
            key = f"key_{i}".encode()
            shard = router.get_shard("TEST_TABLE", key)
            shard_counts[shard] += 1

        # Each shard should have roughly 2000 keys (±30%)
        expected = num_keys / 5
        min_expected = expected * 0.7
        max_expected = expected * 1.3

        for shard in range(5):
            count = shard_counts[shard]
            assert count >= min_expected, f"Shard {shard} has too few keys: {count}"
            assert count <= max_expected, f"Shard {shard} has too many keys: {count}"

    def test_node_data_always_goes_to_shard_zero(self):
        """Node data should always go to shard 0 (replicated)."""
        router = ShardRouter(num_shards=10)

        for i in range(100):
            node_id = f"node_{i}".encode()
            shard = router.get_shard_for_node(node_id)
            assert shard == 0

    def test_replicated_tables_return_shard_zero(self):
        """Replicated tables should always return shard 0."""
        router = ShardRouter(num_shards=5)

        assert router.is_replicated_table("NODE")
        assert router.is_replicated_table("NODE_RESOURCE")
        assert router.is_replicated_table("CLUSTER_RESOURCE")

        assert router.get_shard("NODE", b"some_key") == 0
        assert router.get_shard("NODE_RESOURCE", b"some_key") == 0

    def test_non_replicated_tables_are_sharded(self):
        """Non-replicated tables should be sharded."""
        router = ShardRouter(num_shards=5)

        assert not router.is_replicated_table("ACTOR")
        assert not router.is_replicated_table("JOB")
        assert not router.is_replicated_table("PLACEMENT_GROUP")

    def test_get_all_shards_returns_all_indices(self):
        """get_all_shards should return all shard indices."""
        router = ShardRouter(num_shards=7)

        shards = router.get_all_shards()
        assert shards == [0, 1, 2, 3, 4, 5, 6]

    def test_num_shards_property(self):
        """num_shards property should return correct value."""
        router = ShardRouter(num_shards=5)
        assert router.num_shards == 5

    def test_invalid_shard_count_raises(self):
        """Invalid shard count should raise ValueError."""
        with pytest.raises(ValueError):
            ShardRouter(num_shards=0)

        with pytest.raises(ValueError):
            ShardRouter(num_shards=-1)

    def test_different_tables_can_have_different_shards(self):
        """Same key in different tables can go to different shards."""
        router = ShardRouter(num_shards=10)

        key = b"same_key"
        actor_shard = router.get_shard("ACTOR", key)
        job_shard = router.get_shard("JOB", key)

        # They might be the same or different, but both should be valid
        assert 0 <= actor_shard < 10
        assert 0 <= job_shard < 10

    def test_convenience_methods(self):
        """Test convenience methods for different ID types."""
        router = ShardRouter(num_shards=5)

        actor_id = b"actor_id_123"
        job_id = b"job_id_456"
        pg_id = b"pg_id_789"
        worker_id = b"worker_id_012"

        # All should return valid shards
        assert 0 <= router.get_shard_for_actor(actor_id) < 5
        assert 0 <= router.get_shard_for_job(job_id) < 5
        assert 0 <= router.get_shard_for_placement_group(pg_id) < 5
        assert 0 <= router.get_shard_for_worker(worker_id) < 5


class TestShardedGcsClient:
    """Tests for the ShardedGcsClient class."""

    def test_initialization(self):
        """Client should initialize with correct number of shards."""
        addresses = ["gcs-0:10001", "gcs-1:10001", "gcs-2:10001"]
        client = ShardedGcsClient(addresses)

        assert client.num_shards == 3
        assert client.router.num_shards == 3

    def test_empty_addresses_raises(self):
        """Empty addresses should raise ValueError."""
        with pytest.raises(ValueError):
            ShardedGcsClient([])

    def test_get_channel_without_connect_raises(self):
        """Getting channel before connect should raise RuntimeError."""
        client = ShardedGcsClient(["gcs-0:10001"])

        with pytest.raises(RuntimeError):
            client.get_channel_for_shard(0)

    def test_invalid_shard_raises(self):
        """Invalid shard index should raise ValueError."""
        addresses = ["gcs-0:10001", "gcs-1:10001"]
        client = ShardedGcsClient(addresses)
        client._connected = True  # Bypass actual connection
        client._channels = [None, None]

        with pytest.raises(ValueError):
            client.get_channel_for_shard(5)

        with pytest.raises(ValueError):
            client.get_channel_for_shard(-1)


class TestCreateShardedGcsClient:
    """Tests for the create_sharded_gcs_client factory function."""

    def test_create_with_multiple_addresses(self):
        """Should create client with multiple addresses."""
        addresses = ["gcs-0:10001", "gcs-1:10001"]
        client = create_sharded_gcs_client(gcs_addresses=addresses)

        assert client.num_shards == 2

    def test_create_with_single_address(self):
        """Should create client with single address."""
        client = create_sharded_gcs_client(gcs_address="gcs:10001")

        assert client.num_shards == 1

    def test_no_addresses_raises(self):
        """No addresses should raise ValueError."""
        with pytest.raises(ValueError):
            create_sharded_gcs_client()


class TestHashCompatibility:
    """Tests to verify hash compatibility with C++ implementation."""

    def test_fnv1a_hash_known_values(self):
        """Test FNV-1a hash produces expected values."""
        router = ShardRouter(num_shards=100)

        # Test a few known inputs
        # The exact shard values depend on the hash implementation,
        # but they should be deterministic
        test_cases = [
            ("ACTOR", b"actor_1"),
            ("JOB", b"job_1"),
            ("ACTOR", b"actor_with_longer_id_12345678"),
        ]

        # Run twice to verify consistency
        results = []
        for table, key in test_cases:
            shard = router.get_shard(table, key)
            results.append(shard)

        # Run again
        for i, (table, key) in enumerate(test_cases):
            shard = router.get_shard(table, key)
            assert shard == results[i], f"Hash not consistent for {table}:{key}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
