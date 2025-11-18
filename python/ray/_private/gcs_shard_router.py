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

"""
GCS Shard Router for client-side routing in sharded GCS deployments.

This module provides consistent hashing functionality to route GCS operations
to the appropriate shard based on the key (e.g., ActorID, JobID).
"""

import hashlib
from typing import List, Optional


class ShardRouter:
    """Routes GCS operations to appropriate shards using jump consistent hashing.

    This router uses the Jump Consistent Hash algorithm for excellent distribution
    with minimal memory overhead. It's compatible with the C++ GcsShardRouter.

    Example usage:
        router = ShardRouter(num_shards=3)
        shard = router.get_shard("ACTOR", actor_id.binary())
    """

    # Tables that should be replicated to all shards
    REPLICATED_TABLES = {"NODE", "NODE_RESOURCE", "CLUSTER_RESOURCE"}

    def __init__(self, num_shards: int):
        """Initialize the shard router.

        Args:
            num_shards: Number of shards to distribute data across.
        """
        if num_shards <= 0:
            raise ValueError("Number of shards must be positive")
        self._num_shards = num_shards

    @property
    def num_shards(self) -> int:
        """Get the number of shards."""
        return self._num_shards

    def get_shard(self, table_name: str, key: bytes) -> int:
        """Get the shard for a given table and key.

        Args:
            table_name: Name of the table (e.g., "ACTOR", "JOB").
            key: Binary key (e.g., actor_id.binary()).

        Returns:
            Shard index (0 to num_shards - 1).
        """
        # Replicated tables always go to shard 0 for writes
        if self.is_replicated_table(table_name):
            return 0

        # Single shard setup
        if self._num_shards == 1:
            return 0

        # Compute hash and use jump consistent hash
        combined = f"{table_name}:".encode() + key
        hash_value = self._compute_hash(combined)
        return self._jump_consistent_hash(hash_value)

    def get_shard_for_actor(self, actor_id: bytes) -> int:
        """Get the shard for an ActorID."""
        return self.get_shard("ACTOR", actor_id)

    def get_shard_for_job(self, job_id: bytes) -> int:
        """Get the shard for a JobID."""
        return self.get_shard("JOB", job_id)

    def get_shard_for_placement_group(self, pg_id: bytes) -> int:
        """Get the shard for a PlacementGroupID."""
        return self.get_shard("PLACEMENT_GROUP", pg_id)

    def get_shard_for_worker(self, worker_id: bytes) -> int:
        """Get the shard for a WorkerID."""
        return self.get_shard("WORKER", worker_id)

    def get_shard_for_node(self, node_id: bytes) -> int:
        """Get the shard for a NodeID.

        Node data is replicated to all shards, but shard 0 is primary.
        """
        return 0

    def get_all_shards(self) -> List[int]:
        """Get all shard indices (for scatter-gather operations)."""
        return list(range(self._num_shards))

    def is_replicated_table(self, table_name: str) -> bool:
        """Check if a table should be replicated to all shards."""
        return table_name in self.REPLICATED_TABLES

    def _compute_hash(self, data: bytes) -> int:
        """Compute FNV-1a hash compatible with C++ implementation."""
        FNV_OFFSET_BASIS = 14695981039346656037
        FNV_PRIME = 1099511628211

        hash_value = FNV_OFFSET_BASIS
        for byte in data:
            hash_value ^= byte
            hash_value *= FNV_PRIME
            hash_value &= 0xFFFFFFFFFFFFFFFF  # Keep as 64-bit

        return hash_value

    def _jump_consistent_hash(self, key: int) -> int:
        """Jump consistent hash algorithm.

        Reference: https://arxiv.org/pdf/1406.2294.pdf
        """
        b = -1
        j = 0

        while j < self._num_shards:
            b = j
            key = (key * 2862933555777941757 + 1) & 0xFFFFFFFFFFFFFFFF
            j = int((b + 1) * (float(1 << 31) / float((key >> 33) + 1)))

        return b


class ShardedGcsClient:
    """GCS client that routes operations to appropriate shards.

    This client maintains connections to multiple GCS instances and
    routes operations based on the shard router.

    Example usage:
        client = ShardedGcsClient(
            gcs_addresses=["gcs-0:10001", "gcs-1:10001", "gcs-2:10001"]
        )
        await client.connect()
        actor_info = await client.get_actor(actor_id)
    """

    def __init__(
        self,
        gcs_addresses: List[str],
        aio: bool = False,
    ):
        """Initialize the sharded GCS client.

        Args:
            gcs_addresses: List of GCS addresses, one per shard.
            aio: Whether to use async gRPC channels.
        """
        if not gcs_addresses:
            raise ValueError("Must provide at least one GCS address")

        self._gcs_addresses = gcs_addresses
        self._aio = aio
        self._router = ShardRouter(len(gcs_addresses))
        self._channels = [None] * len(gcs_addresses)
        self._connected = False

    @property
    def num_shards(self) -> int:
        """Get the number of shards."""
        return len(self._gcs_addresses)

    @property
    def router(self) -> ShardRouter:
        """Get the shard router."""
        return self._router

    def connect(self):
        """Connect to all GCS shards."""
        from ray._private.gcs_utils import create_gcs_channel

        for i, address in enumerate(self._gcs_addresses):
            self._channels[i] = create_gcs_channel(address, self._aio)

        self._connected = True

    def disconnect(self):
        """Disconnect from all GCS shards."""
        for i in range(len(self._channels)):
            if self._channels[i] is not None:
                # Close channel if supported
                self._channels[i] = None
        self._connected = False

    def get_channel_for_shard(self, shard: int):
        """Get the gRPC channel for a specific shard."""
        if not self._connected:
            raise RuntimeError("Client not connected")
        if shard < 0 or shard >= len(self._channels):
            raise ValueError(f"Invalid shard index: {shard}")
        return self._channels[shard]

    def get_channel_for_actor(self, actor_id: bytes):
        """Get the gRPC channel for an actor operation."""
        shard = self._router.get_shard_for_actor(actor_id)
        return self.get_channel_for_shard(shard)

    def get_channel_for_job(self, job_id: bytes):
        """Get the gRPC channel for a job operation."""
        shard = self._router.get_shard_for_job(job_id)
        return self.get_channel_for_shard(shard)

    def get_channel_for_node(self, node_id: bytes):
        """Get the gRPC channel for a node operation."""
        shard = self._router.get_shard_for_node(node_id)
        return self.get_channel_for_shard(shard)

    def get_all_channels(self) -> List:
        """Get all channels (for scatter-gather operations)."""
        if not self._connected:
            raise RuntimeError("Client not connected")
        return self._channels.copy()


def create_sharded_gcs_client(
    gcs_addresses: Optional[List[str]] = None,
    gcs_address: Optional[str] = None,
    aio: bool = False,
) -> ShardedGcsClient:
    """Create a sharded or single GCS client based on configuration.

    Args:
        gcs_addresses: List of GCS addresses for sharded deployment.
        gcs_address: Single GCS address (for non-sharded deployment).
        aio: Whether to use async gRPC channels.

    Returns:
        ShardedGcsClient instance.
    """
    if gcs_addresses:
        return ShardedGcsClient(gcs_addresses, aio)
    elif gcs_address:
        return ShardedGcsClient([gcs_address], aio)
    else:
        raise ValueError("Must provide either gcs_addresses or gcs_address")
