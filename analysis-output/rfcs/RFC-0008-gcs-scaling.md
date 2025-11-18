# RFC-0008: GCS Horizontal Scaling

**Status:** Draft
**Author:** Codebase Analysis
**Created:** 2025-01-18
**Commit Reference:** `d1cce8c9dc8411fad7cfbd619350bec6f19839a3`

## Summary

Enable GCS (Global Control Store) to scale horizontally, removing it as a bottleneck for very large Ray clusters (10,000+ nodes).

## Motivation

### Current State

GCS is a single-node service that can become a bottleneck:

- All actor lookups go through GCS
- Job metadata stored centrally
- Node heartbeats processed by single instance
- Redis backend has single-master limitation

### Evidence from Codebase

From performance analysis:
- GCS processes all actor registrations sequentially
- Node manager updates create GCS contention
- 10,000+ node clusters show GCS latency increase

### Impact

- Limits cluster size
- Increases actor creation latency at scale
- Single point of failure concern

## Detailed Design

### Sharded GCS Architecture

```
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
```

### Sharding Strategy

```cpp
// Consistent hashing for shard assignment
class GcsShardRouter {
 public:
  int GetShard(const ActorID& actor_id) {
    return ConsistentHash(actor_id) % num_shards_;
  }

  int GetShard(const JobID& job_id) {
    return ConsistentHash(job_id) % num_shards_;
  }

  // Node membership always goes to shard 0 (or special handling)
  int GetShardForNode(const NodeID& node_id) {
    return 0;  // Or use consensus-based membership
  }
};
```

### Data Distribution

| Data Type | Sharding Key | Replication |
|-----------|--------------|-------------|
| Actors | ActorID | Within shard |
| Jobs | JobID | Within shard |
| Nodes | - | All shards (replicated) |
| Placement Groups | PGID | Within shard |

### Client-Side Routing

```python
# Client maintains shard mapping
class GcsClient:
    def __init__(self, gcs_addresses: List[str]):
        self.shards = gcs_addresses
        self.router = ShardRouter(len(gcs_addresses))

    async def get_actor(self, actor_id: ActorID):
        shard = self.router.get_shard(actor_id)
        return await self.shards[shard].get_actor(actor_id)

    async def register_actor(self, actor_spec):
        shard = self.router.get_shard(actor_spec.actor_id)
        return await self.shards[shard].register_actor(actor_spec)
```

### Storage Backend Options

#### Option 1: Redis Cluster

```yaml
gcs:
  storage: redis-cluster
  nodes:
    - redis-0:6379
    - redis-1:6379
    - redis-2:6379
```

#### Option 2: etcd

```yaml
gcs:
  storage: etcd
  endpoints:
    - etcd-0:2379
    - etcd-1:2379
    - etcd-2:2379
```

#### Option 3: TiKV

```yaml
gcs:
  storage: tikv
  pd_endpoints:
    - pd-0:2379
```

### Consistency Model

Most operations can be eventually consistent:

```cpp
// Actor lookup - eventually consistent is fine
Status GetActor(const ActorID& id, ActorTableData* data) {
  // Read from local shard cache first
  if (cache_.Get(id, data)) {
    return Status::OK();
  }
  // Fall back to storage
  return storage_->Get(id, data);
}

// Actor registration - needs strong consistency within shard
Status RegisterActor(const ActorTableData& data) {
  // Write to storage with consistency guarantee
  return storage_->Put(data.actor_id(), data, WriteOptions::Consistent());
}
```

### Cross-Shard Operations

Some operations span shards:

```cpp
// List all actors (scatter-gather)
Status ListActors(std::vector<ActorTableData>* actors) {
  std::vector<std::future<std::vector<ActorTableData>>> futures;

  for (int shard = 0; shard < num_shards_; shard++) {
    futures.push_back(
      async([this, shard]() {
        return shards_[shard]->ListActors();
      })
    );
  }

  // Gather results
  for (auto& f : futures) {
    auto shard_actors = f.get();
    actors->insert(actors->end(), shard_actors.begin(), shard_actors.end());
  }

  return Status::OK();
}
```

### Shard Rebalancing

When adding/removing shards:

```cpp
class ShardRebalancer {
  void Rebalance(int old_shards, int new_shards) {
    // 1. Update routing table
    // 2. Migrate data in background
    // 3. Clients refresh routing

    for (auto& [key, value] : old_shard_data) {
      int new_shard = ConsistentHash(key) % new_shards;
      if (new_shard != old_shard) {
        MigrateKey(key, value, new_shard);
      }
    }
  }
};
```

### High Availability

Each shard can have replicas:

```yaml
gcs:
  shards: 3
  replicas_per_shard: 3
  consensus: raft  # or paxos
```

### Migration Path

1. **Phase 1:** Single GCS with Redis Cluster backend
2. **Phase 2:** Multiple GCS with routing
3. **Phase 3:** Automatic sharding and rebalancing

## Implementation Plan

### Month 1: Storage Abstraction
- Abstract storage backend
- Implement Redis Cluster support
- Add etcd support

### Month 2: Sharding Infrastructure
- Consistent hashing
- Client-side routing
- Cross-shard queries

### Month 3: High Availability
- Per-shard replication
- Failover handling
- Leader election

### Month 4: Operations
- Shard management CLI
- Monitoring dashboards
- Rebalancing tools

### Month 5-6: Testing & Hardening
- Scale testing (10,000+ nodes)
- Chaos testing
- Performance benchmarks

## Backwards Compatibility

**Non-breaking deployment:**

```python
# Single GCS (current)
ray.init(address="ray://head:10001")

# Sharded GCS
ray.init(address="ray://gcs-lb:10001")  # Load balancer routes
```

## Success Criteria

- [ ] Support 50,000 nodes
- [ ] Actor lookup < 10ms at p99
- [ ] Linear scaling with shard count
- [ ] Automatic failover < 30 seconds

## Effort Estimation

- **Development:** 24 dev-weeks
- **Testing:** 8 dev-weeks
- **Total:** 32 dev-weeks (8 months)

## References

- Current GCS: [`src/ray/gcs/`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/src/ray/gcs/)
- GCS server: [`src/ray/gcs/gcs_server/`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/src/ray/gcs/gcs_server/)
