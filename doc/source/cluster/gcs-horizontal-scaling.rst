.. _gcs-horizontal-scaling:

GCS Horizontal Scaling
======================

This document describes the GCS (Global Control Store) horizontal scaling feature,
which enables Ray clusters to scale beyond 10,000 nodes by distributing GCS data
across multiple shards.

.. note::
   This feature is currently in development and not recommended for production use.

Overview
--------

The GCS is Ray's centralized metadata store that manages:

- Actor registrations and lookups
- Job metadata
- Node membership and heartbeats
- Placement group scheduling
- Worker state

In single-node GCS deployments, the GCS can become a bottleneck for very large
clusters due to sequential processing of operations and single-master storage
limitations.

The horizontal scaling feature addresses this by:

1. Distributing data across multiple GCS shards using consistent hashing
2. Supporting alternative storage backends (etcd, TiKV)
3. Enabling client-side routing for optimal performance

Architecture
------------

Sharded GCS Architecture
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: text

                    Load Balancer
                         │
          ┌──────────────┼──────────────┐
          │              │              │
     ┌────▼────┐    ┌────▼────┐    ┌────▼────┐
     │  GCS 0  │    │  GCS 1  │    │  GCS 2  │
     │ (Shard) │    │ (Shard) │    │ (Shard) │
     └────┬────┘    └────┬────┘    └────┬────┘
          │              │              │
          └──────────────┼──────────────┘
                         │
                    Redis Cluster
                    (or etcd/TiKV)

Data Distribution
~~~~~~~~~~~~~~~~~

Data is distributed across shards using consistent hashing:

+------------------+---------------+-------------+
| Data Type        | Sharding Key  | Replication |
+==================+===============+=============+
| Actors           | ActorID       | Within shard|
+------------------+---------------+-------------+
| Jobs             | JobID         | Within shard|
+------------------+---------------+-------------+
| Placement Groups | PGID          | Within shard|
+------------------+---------------+-------------+
| Workers          | WorkerID      | Within shard|
+------------------+---------------+-------------+
| Nodes            | -             | All shards  |
+------------------+---------------+-------------+

Node data is replicated to all shards since it's needed for scheduling decisions
across all GCS instances.

Configuration
-------------

Enable sharded GCS by setting these configuration options:

.. code-block:: yaml

   # ray_config.yaml
   gcs_enable_sharding: true
   gcs_num_shards: 3
   gcs_storage: redis  # or "etcd"

Redis Cluster Backend
~~~~~~~~~~~~~~~~~~~~~

For Redis Cluster with multiple shards:

.. code-block:: yaml

   gcs_storage: redis
   gcs_num_shards: 3
   redis_address: "redis-lb:6379"

etcd Backend
~~~~~~~~~~~~

For etcd backend:

.. code-block:: yaml

   gcs_storage: etcd
   gcs_etcd_endpoints: "http://etcd-0:2379,http://etcd-1:2379,http://etcd-2:2379"
   gcs_etcd_key_prefix: "ray"
   gcs_etcd_connect_timeout_ms: 5000
   gcs_etcd_request_timeout_ms: 30000

For TLS-enabled etcd:

.. code-block:: yaml

   gcs_etcd_use_tls: true
   gcs_etcd_ca_cert_path: "/path/to/ca.crt"
   gcs_etcd_client_cert_path: "/path/to/client.crt"
   gcs_etcd_client_key_path: "/path/to/client.key"

Client-Side Routing
-------------------

Python Client
~~~~~~~~~~~~~

The Python client supports automatic routing to the appropriate shard:

.. code-block:: python

   from ray._private.gcs_shard_router import ShardedGcsClient

   # Connect to sharded GCS
   client = ShardedGcsClient(
       gcs_addresses=["gcs-0:10001", "gcs-1:10001", "gcs-2:10001"]
   )
   client.connect()

   # Get channel for actor operations (automatically routed)
   channel = client.get_channel_for_actor(actor_id.binary())

   # Get all channels for scatter-gather operations
   all_channels = client.get_all_channels()

Using ShardRouter Directly
~~~~~~~~~~~~~~~~~~~~~~~~~~

For custom routing logic:

.. code-block:: python

   from ray._private.gcs_shard_router import ShardRouter

   router = ShardRouter(num_shards=3)

   # Get shard for an actor
   shard = router.get_shard_for_actor(actor_id.binary())

   # Get shard for a job
   shard = router.get_shard_for_job(job_id.binary())

   # Get all shards for scatter-gather
   all_shards = router.get_all_shards()

C++ Client
~~~~~~~~~~

The C++ client uses ``GcsShardRouter`` for shard assignment:

.. code-block:: cpp

   #include "ray/gcs/store_client/gcs_shard_router.h"

   GcsShardRouter router(num_shards);

   // Get shard for an actor
   int shard = router.GetShard(actor_id);

   // Get shard for a job
   int shard = router.GetShard(job_id);

   // Get all shards
   std::vector<int> shards = router.GetAllShards();

Consistency Model
-----------------

The sharded GCS uses different consistency levels for different operations:

Eventually Consistent (Reads)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Most read operations can tolerate eventual consistency:

- Actor lookups can use local cache first
- Job metadata reads
- Worker state queries

Strongly Consistent (Writes)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Write operations require strong consistency within a shard:

- Actor registration
- Job creation
- Node membership changes

Cross-Shard Operations
~~~~~~~~~~~~~~~~~~~~~~

Operations that span multiple shards use scatter-gather:

- List all actors
- Get all jobs
- Prefix scans

These operations query all shards in parallel and aggregate results.

Migration Path
--------------

Phase 1: Storage Abstraction
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- Abstract storage backend interface
- Implement Redis Cluster support
- Add etcd/TiKV support

Phase 2: Sharding Infrastructure
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- Implement consistent hashing
- Add client-side routing
- Support cross-shard queries

Phase 3: High Availability
~~~~~~~~~~~~~~~~~~~~~~~~~~

- Per-shard replication
- Automatic failover
- Leader election

Phase 4: Operations
~~~~~~~~~~~~~~~~~~~

- Shard management CLI
- Monitoring dashboards
- Rebalancing tools

Backwards Compatibility
-----------------------

The sharded GCS is designed for non-breaking deployment:

.. code-block:: python

   # Single GCS (current) - no change needed
   ray.init(address="ray://head:10001")

   # Sharded GCS - use load balancer
   ray.init(address="ray://gcs-lb:10001")

Clients automatically detect whether they're connecting to a single GCS or
a sharded deployment.

Performance Considerations
--------------------------

Shard Count
~~~~~~~~~~~

Choose shard count based on:

- Expected cluster size
- Operation throughput requirements
- Storage backend capabilities

Recommended starting points:

- 1,000-5,000 nodes: 3 shards
- 5,000-20,000 nodes: 5 shards
- 20,000-50,000 nodes: 10 shards

Cross-Shard Operations
~~~~~~~~~~~~~~~~~~~~~~

Minimize cross-shard operations (GetAll, prefix scans) as they have higher
latency due to scatter-gather.

Caching
~~~~~~~

Enable client-side caching for frequently accessed data to reduce GCS load:

.. code-block:: yaml

   gcs_client_cache_enabled: true
   gcs_client_cache_ttl_seconds: 60

Monitoring
----------

Monitor sharded GCS using these metrics:

- ``gcs_shard_operation_count``: Operations per shard
- ``gcs_shard_latency_ms``: Latency per shard
- ``gcs_cross_shard_operation_count``: Cross-shard operations

Troubleshooting
---------------

Uneven Shard Distribution
~~~~~~~~~~~~~~~~~~~~~~~~~

If shards have uneven load:

1. Check key distribution using ``gcs_shard_operation_count`` metric
2. Consider increasing virtual nodes per shard
3. Rebalance data if needed

High Cross-Shard Latency
~~~~~~~~~~~~~~~~~~~~~~~~

If cross-shard operations are slow:

1. Reduce use of GetAll operations
2. Use more specific queries when possible
3. Enable client-side caching

Connection Issues
~~~~~~~~~~~~~~~~~

If clients can't connect to shards:

1. Verify all GCS shard addresses are reachable
2. Check firewall rules for GCS ports
3. Verify load balancer configuration

Limitations
-----------

Current limitations of the sharded GCS:

1. **PubSub**: Centralized PubSub not yet distributed
2. **State Migration**: No automatic state migration during rebalancing
3. **Manager State**: Some manager state still held in memory
4. **etcd/TiKV**: Storage backends are stub implementations

Future Work
-----------

Planned improvements:

- Distributed PubSub
- Automatic shard rebalancing
- State migration tools
- Full etcd/TiKV implementation
- Raft-based consensus per shard
