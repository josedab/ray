# Part 5: Performance Analysis and Optimization

> **Series:** Ray Deep Dive | **Reading Time:** 17 minutes
> **Commit:** `d1cce8c9dc8411fad7cfbd619350bec6f19839a3`

## What You'll Learn

- Critical performance paths in Ray
- Memory management and garbage collection
- Scheduling optimizations
- Profiling and debugging techniques
- Configuration tuning for different workloads

## Introduction

Performance in distributed systems involves balancing many trade-offs: latency vs. throughput, memory vs. CPU, locality vs. load balancing. In this post, we'll explore Ray's performance characteristics, identify common bottlenecks, and learn how to optimize for your workload.

## Critical Performance Paths

### Task Submission Overhead

The minimum overhead for a Ray task includes:

1. **Serialization:** ~0.1-1ms for small arguments
2. **Scheduling:** ~0.1ms for local scheduling
3. **Worker dispatch:** ~0.1ms
4. **Deserialization:** ~0.1-1ms

**Total overhead:** ~1ms for trivial tasks

```python
import ray
import time

@ray.remote
def trivial():
    return 1

# Measure overhead
start = time.time()
refs = [trivial.remote() for _ in range(1000)]
ray.get(refs)
elapsed = time.time() - start

print(f"Per-task overhead: {elapsed / 1000 * 1000:.2f}ms")
# Typical output: Per-task overhead: ~1-2ms
```

### Object Transfer

Object transfer performance depends on:

1. **Chunk size:** Default 5MB chunks
2. **Network bandwidth:** Shared across all transfers
3. **Plasma capacity:** In-flight transfers limited to 2GB

```python
# python/ray/object_manager/push_manager.cc
// Transfer configuration
constexpr int64_t kDefaultChunkSize = 5 * 1024 * 1024;  // 5MB
constexpr int64_t kMaxBytesInFlight = 2ULL * 1024 * 1024 * 1024;  // 2GB
```

**Optimization:** For large transfers, increase chunk size:

```python
ray.init(_system_config={
    "object_manager_default_chunk_size": 10 * 1024 * 1024  # 10MB
})
```

### Scheduling Latency

Scheduling goes through several stages:

```cpp
// src/ray/raylet/scheduling/cluster_resource_scheduler.cc

// 1. Resource matching: O(n) where n = number of node types
// 2. Score calculation: O(m) where m = candidate nodes
// 3. Selection: O(m log m) for sorting
```

**Key factors:**
- Number of nodes in cluster
- Complexity of resource requirements
- Scheduling policy (hybrid vs spread vs pack)

## Memory Management

### Object Store Memory

Default allocation: 30% of system memory

```python
# Check object store usage
import ray

ray.init()
memory_info = ray.cluster_resources()
print(f"Object store: {memory_info.get('object_store_memory', 0) / 1e9:.2f} GB")
```

### Memory Thresholds

Ray uses several thresholds:

| Threshold | Default | Action |
|-----------|---------|--------|
| Eviction | 80% | Start LRU eviction |
| Spilling | Configurable | Write to disk |
| GC trigger | 70% | Aggressive garbage collection |
| OOM | 95% | Kill tasks/actors |

```cpp
// src/ray/common/ray_config_def.h

RAY_CONFIG(float, object_store_full_threshold, 0.95,
           "Threshold to consider object store full")

RAY_CONFIG(float, object_store_memory_mon_refresh_interval_ms, 100,
           "Memory monitor refresh interval")
```

### Garbage Collection

Ray's distributed GC uses reference counting:

```cpp
// src/ray/core_worker/reference_counter.cc

void ReferenceCounter::RemoveLocalReference(const ObjectID &object_id) {
  auto it = object_id_refs_.find(object_id);
  if (it == object_id_refs_.end()) {
    return;
  }

  it->second.local_ref_count--;

  if (it->second.RefCount() == 0) {
    // Object can be freed
    DeleteObject(object_id);
  }
}
```

**Common issue:** Reference cycles prevent GC

```python
# This creates a reference cycle
@ray.remote
class A:
    def set_b(self, b):
        self.b = b  # A -> B

@ray.remote
class B:
    def set_a(self, a):
        self.a = a  # B -> A

a = A.remote()
b = B.remote()
a.set_b.remote(b)
b.set_a.remote(a)
# Neither can be GC'd!
```

### Memory Optimization Patterns

#### 1. Avoid Large Return Values

```python
# Bad: Returns large data
@ray.remote
def process_large():
    result = compute()  # 1GB result
    return result

# Good: Store in object store, return reference
@ray.remote
def process_large():
    result = compute()
    return ray.put(result)  # Return ObjectRef
```

#### 2. Use Streaming for Large Datasets

```python
# Bad: Load all into memory
data = ray.get([load_chunk.remote(i) for i in range(1000)])

# Good: Stream processing
@ray.remote
class StreamProcessor:
    def __init__(self):
        self.result = 0

    def process_chunk(self, chunk_ref):
        chunk = ray.get(chunk_ref)
        self.result += process(chunk)
        # chunk is freed after this

    def get_result(self):
        return self.result
```

#### 3. Configure Spilling

```python
ray.init(_system_config={
    "object_spilling_config": {
        "type": "filesystem",
        "params": {
            "directory_path": "/fast/ssd/ray_spill",
            "buffer_size": 1048576  # 1MB buffer
        }
    },
    "max_io_workers": 4,
    "object_spilling_threshold": 0.8
})
```

## Scheduling Optimizations

### Spread Threshold Tuning

The hybrid policy's spread threshold balances load:

```python
# For CPU-bound tasks (prefer spreading)
ray.init(_system_config={
    "scheduler_spread_threshold": 0.8  # More spreading
})

# For data-intensive tasks (prefer locality)
ray.init(_system_config={
    "scheduler_spread_threshold": 0.2  # More packing/locality
})
```

### Scheduling Strategies

```python
# Spread across nodes
@ray.remote(scheduling_strategy="SPREAD")
def distributed_task():
    pass

# Pack onto fewer nodes
@ray.remote(scheduling_strategy="PACK")
def colocated_task():
    pass

# Node affinity
@ray.remote(scheduling_strategy=NodeAffinitySchedulingStrategy(
    node_id=ray.get_runtime_context().get_node_id(),
    soft=False
))
def local_task():
    pass
```

### Placement Groups for Co-location

```python
from ray.util.placement_group import placement_group

# Reserve resources together
pg = placement_group([
    {"CPU": 4, "GPU": 1},
    {"CPU": 4, "GPU": 1}
])

ray.get(pg.ready())

# Schedule on placement group
@ray.remote(num_cpus=4, num_gpus=1)
def task1():
    pass

@ray.remote(num_cpus=4, num_gpus=1)
def task2():
    pass

ray.get([
    task1.options(placement_group=pg).remote(),
    task2.options(placement_group=pg).remote()
])
```

## Profiling and Debugging

### Ray Timeline

```python
# Enable timeline
ray.init()
ray.timeline("timeline.json")

# Run workload
results = ray.get([task.remote(i) for i in range(100)])

# View in Chrome: chrome://tracing
```

### Ray Dashboard

Access at `http://<head-node>:8265`

Features:
- Live cluster view
- Task/actor status
- Memory usage
- Logs

### State API

```python
from ray.util.state import list_tasks, list_actors, get_task

# List running tasks
tasks = list_tasks(filters=[("state", "=", "RUNNING")])

# Get task details
for task in tasks:
    print(f"{task.task_id}: {task.name} - {task.state}")
    if task.error_message:
        print(f"  Error: {task.error_message}")

# List actors
actors = list_actors()
for actor in actors:
    print(f"{actor.actor_id}: {actor.class_name} - {actor.state}")
```

### Memory Debugging

```python
from ray._private.internal_api import memory_summary

# Get memory usage summary
print(memory_summary())

# Check for memory leaks
from ray.util.state import list_objects
objects = list_objects(filters=[("reference_type", "=", "LOCAL_REFERENCE")])
print(f"Objects with local refs: {len(objects)}")
```

### CPU Profiling

```python
# Enable profiling for actor
@ray.remote
class ProfiledActor:
    def expensive_method(self):
        with ray.profiling.profile("expensive_method"):
            # Code to profile
            result = compute()
        return result
```

Or use py-spy:

```bash
# Attach to running worker
py-spy record -o profile.svg --pid <worker_pid>
```

## Common Bottlenecks

### 1. GCS Bottleneck

**Symptoms:**
- High latency on actor creation
- Slow `ray.get_actor()`
- Dashboard timeouts

**Diagnosis:**

```python
from ray.util.state import list_cluster_events

events = list_cluster_events()
for event in events:
    if "GCS" in event.message:
        print(event)
```

**Solutions:**
- Reduce actor churn
- Use persistent actors
- Enable GCS HA for fault tolerance

### 2. Object Store Pressure

**Symptoms:**
- `RayOutOfMemoryError`
- Slow `ray.get()`
- High spilling activity

**Diagnosis:**

```python
# Check memory
from ray._private.internal_api import memory_summary
print(memory_summary())
```

**Solutions:**
- Increase object store size
- Enable spilling
- Reduce object sizes
- Delete references promptly

### 3. Serialization Overhead

**Symptoms:**
- High CPU on driver
- Slow task submission
- Large task specs

**Diagnosis:**

```python
import cloudpickle
import sys

# Measure serialization size
data = your_data
serialized = cloudpickle.dumps(data)
print(f"Serialized size: {sys.getsizeof(serialized)} bytes")
```

**Solutions:**
- Use `ray.put()` for large data
- Register custom serializers
- Avoid closures capturing large state

### 4. Network Saturation

**Symptoms:**
- Slow object transfers
- Timeout errors
- Uneven task distribution

**Diagnosis:**

```bash
# Check network usage
sar -n DEV 1
```

**Solutions:**
- Increase chunk size
- Use locality-aware scheduling
- Reduce data movement

## Performance Configuration Reference

### Task Performance

| Config | Default | Description |
|--------|---------|-------------|
| `task_retry_delay_ms` | 0 | Delay before retrying |
| `num_workers_soft_limit` | -1 | Soft cap on workers |
| `worker_cap_enabled` | true | Enable worker cap |

### Object Store

| Config | Default | Description |
|--------|---------|-------------|
| `object_store_memory` | 30% RAM | Object store size |
| `object_spilling_threshold` | 0.8 | When to start spilling |
| `max_io_workers` | 4 | Spilling parallelism |

### Scheduling

| Config | Default | Description |
|--------|---------|-------------|
| `scheduler_spread_threshold` | 0.5 | Hybrid policy balance |
| `worker_lease_timeout_ms` | 10000 | Lease timeout |

### Network

| Config | Default | Description |
|--------|---------|-------------|
| `object_manager_default_chunk_size` | 5MB | Transfer chunk size |
| `object_manager_pull_timeout_ms` | 10000 | Pull timeout |

## Benchmarking Approach

### Micro-benchmarks

```python
import ray
import time
import numpy as np

def benchmark_task_overhead():
    @ray.remote
    def noop():
        pass

    # Warmup
    ray.get([noop.remote() for _ in range(100)])

    # Benchmark
    start = time.time()
    refs = [noop.remote() for _ in range(10000)]
    ray.get(refs)
    elapsed = time.time() - start

    print(f"Task overhead: {elapsed / 10000 * 1000:.3f}ms")

def benchmark_object_transfer():
    @ray.remote
    def create_object(size_mb):
        return np.zeros(size_mb * 1024 * 1024 // 8)

    sizes = [1, 10, 100, 1000]

    for size in sizes:
        start = time.time()
        ref = create_object.remote(size)
        ray.get(ref)
        elapsed = time.time() - start

        throughput = size / elapsed
        print(f"{size}MB: {elapsed:.3f}s ({throughput:.1f} MB/s)")
```

### Application-level Benchmarks

```python
def benchmark_data_pipeline():
    # Create dataset
    ds = ray.data.range(10000000)

    start = time.time()

    result = (
        ds
        .map(lambda x: x * 2)
        .filter(lambda x: x % 3 == 0)
        .count()
    )

    elapsed = time.time() - start
    throughput = 10000000 / elapsed

    print(f"Pipeline throughput: {throughput:.0f} rows/s")
```

## Scaling Strategies

### Horizontal Scaling

Add nodes for:
- More CPU/GPU resources
- More memory
- Better fault isolation

```python
# Check cluster resources
print(ray.cluster_resources())

# Scale up
# (via autoscaler or manual node addition)
```

### Vertical Scaling

Larger instances for:
- Memory-bound workloads
- Reduced network overhead
- Simpler management

### Workload-Specific Tuning

#### High-Throughput Batch Processing

```python
ray.init(_system_config={
    "scheduler_spread_threshold": 0.3,  # Favor locality
    "object_manager_default_chunk_size": 10 * 1024 * 1024,  # Larger chunks
})
```

#### Low-Latency Serving

```python
ray.init(_system_config={
    "scheduler_spread_threshold": 0.8,  # Spread load
    "task_retry_delay_ms": 100,  # Quick retries
})
```

#### Memory-Intensive Analytics

```python
ray.init(
    _system_config={
        "object_spilling_threshold": 0.6,  # Early spilling
        "max_io_workers": 8,  # More spilling parallelism
    },
    object_store_memory=50 * 1024**3  # 50GB object store
)
```

## Key Takeaways

1. **Task overhead** is ~1ms minimum - batch small operations
2. **Memory management** requires attention to reference lifecycles
3. **Scheduling policy** should match workload characteristics
4. **Profiling tools** (timeline, dashboard, state API) are essential
5. **Configuration tuning** can significantly improve performance

## What's Next

In [Part 6](./06-observability-security.md), we'll cover:
- Ray's observability stack
- Security model and hardening
- Production deployment patterns

## Code References

- Configuration: [`src/ray/common/ray_config_def.h`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/src/ray/common/ray_config_def.h)
- Scheduling: [`src/ray/raylet/scheduling/`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/src/ray/raylet/scheduling/)
- Memory management: [`src/ray/object_manager/`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/src/ray/object_manager/)
- Reference counting: [`src/ray/core_worker/reference_counter.cc`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/src/ray/core_worker/reference_counter.cc)

---

*Next: [Part 6 - Observability, Security, and Production Deployment](./06-observability-security.md)*
