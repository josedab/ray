# Ray Performance Characteristics and Optimization Opportunities

## Executive Summary

Ray's performance is governed by multiple subsystems: scheduling, object management, serialization, network communication, and memory management. This report identifies key performance characteristics, bottlenecks, and tunable parameters across all these areas.

---

## 1. SCHEDULING PERFORMANCE

### Overview
Ray's scheduling system uses a **hybrid policy** that balances between locality, load balancing, and cold start penalties.

### Key Components

#### 1.1 Cluster Resource Scheduler
**File**: `/home/user/ray/src/ray/raylet/scheduling/cluster_resource_scheduler.h` (lines 42-100)
**File**: `/home/user/ray/src/ray/raylet/scheduling/scheduling_policy.h` (lines 25-100)

The scheduler implements three main policies:
- **Hybrid Policy**: Default policy combining node traversal with priority scheduling
- **Spread Policy**: Round-robin scheduling across available nodes
- **Random Policy**: Random node selection

**Key Features**:
- Local node prioritization to reduce cold start penalties
- Resource utilization thresholds for load balancing
- Handles infeasible task detection

#### 1.2 Hybrid Scheduling Policy Details
**File**: `/home/user/ray/src/ray/raylet/scheduling/scheduling_policy.h` (lines 33-71)

The policy design addresses three key performance concerns:
1. **Cold start penalty**: Scheduling tasks on new nodes requires warming the worker pool
2. **Noisy neighbor problem**: Caused by object spilling when nodes exceed utilization threshold
3. **Locality vs. Load Balancing**: Balanced through spread threshold and top-k random selection

**Configuration Parameters** (`RAY_*` environment variables):
- `RAY_scheduler_spread_threshold` (default: 0.5) - Node utilization threshold for spreading tasks
- `RAY_scheduler_top_k_fraction` (default: 0.2) - Fraction of top nodes to randomly pick from
- `RAY_scheduler_top_k_absolute` (default: 1) - Minimum number of top nodes to pick from

#### 1.3 Worker Pool Management & Capacity Control
**File**: `/home/user/ray/src/ray/raylet/worker_pool.h` (lines 45-150)

**Soft Cap Mechanism**:
- Prevents scheduling class deadlock by limiting concurrent workers of same type
- Exponential backoff strategy for tasks exceeding the cap

**Configuration Parameters**:
- `RAY_worker_cap_enabled` (default: true) - Enable soft cap on scheduling classes
- `RAY_worker_cap_initial_backoff_delay_ms` (default: 1000) - Initial backoff when cap is hit
- `RAY_worker_cap_max_backoff_delay_ms` (default: 10000) - Maximum backoff delay
- `RAY_worker_maximum_startup_concurrency` (default: 0/auto) - Max workers startable simultaneously
- `RAY_worker_register_timeout_seconds` (default: 60) - Timeout for worker registration

### Performance Bottlenecks & TODOs

**File**: `/home/user/ray/src/ray/object_manager/push_manager.cc` (lines 55-57)
```
// TODO(ekl) this isn't the best implementation of round robin, we should...
// TODO(dayshah): Does round-robin even make sense here? We should probably finish...
```
This indicates potential optimization opportunity in push manager's round-robin implementation.

### Optimization Opportunities

1. **Scheduling Policy Selection**: Use spread policy for CPU-bound workloads, hybrid for mixed workloads
2. **Worker Cap Tuning**: Adjust backoff delays based on workload characteristics
3. **Resource Utilization Monitoring**: Lower spread threshold for better load distribution

---

## 2. OBJECT STORE PERFORMANCE

### Overview
Ray uses **Plasma** as the distributed object store with support for spilling to external storage.

### 2.1 Plasma Store Architecture
**File**: `/home/user/ray/src/ray/object_manager/plasma/store.h` (lines 45-150)

**Key Features**:
- Shared memory-based storage for zero-copy access
- LRU eviction policy for memory management
- Spilling capability for out-of-core data
- Object lifecycle management

#### Memory Management
**File**: `/home/user/ray/src/ray/object_manager/plasma/malloc.h`
**File**: `/home/user/ray/src/ray/object_manager/plasma/dlmalloc.cc`

Uses Doug Lea's malloc (dlmalloc) for efficient memory allocation with:
- Contiguous memory regions
- Low fragmentation overhead
- Custom allocator support (fallback allocation)

### 2.2 Eviction Policy
**File**: `/home/user/ray/src/ray/object_manager/plasma/eviction_policy.h` (lines 39-100)

**LRU Implementation**:
- `ObjectCreated()`: Add object to LRU cache
- `BeginObjectAccess()`: Move object out of evictable set
- `EndObjectAccess()`: Return object to evictable set
- `ChooseObjectsToEvict()`: Select objects for eviction when space needed

**Performance Characteristics**:
- O(1) eviction selection
- Thread-safe through mutex protection
- Tracks both object count and byte size

### 2.3 Object Spilling System
**File**: `/home/user/ray/src/ray/raylet/local_object_manager.h` (lines 40-100)

**Spilling Configuration Parameters**:
- `RAY_min_spilling_size` (default: 100 MB) - Minimum size to trigger spilling
- `RAY_object_spilling_threshold` (default: 0.8) - Start spilling when 80% of store is used
- `RAY_automatic_object_spilling_enabled` (default: true) - Enable auto-spilling
- `RAY_object_spilling_directory` - External storage path (defaults to temp directory)
- `RAY_verbose_spill_logs` (default: 2 GB intervals) - Log spilling every N bytes

**Object Lifecycle**:
- **Pinning**: Objects are pinned when created/referenced
- **Unpinning**: Added to local cache for batch eviction
- **Batch Eviction**: Freed in batches via `free_objects_batch_size`

**Free Objects Configuration**:
- `RAY_free_objects_period_milliseconds` (default: 1000) - Batch flush interval
- `RAY_free_objects_batch_size` (default: 100) - Max objects per batch

**Performance Impact**: Spilling introduces I/O latency; tuning batch size trades memory consistency for throughput.

### 2.4 Lineage Pinning
**File**: `/home/user/ray/src/ray/common/ray_config_def.h` (lines 138-150)

**Configuration**:
- `RAY_lineage_pinning_enabled` (default: true) - Pin task lineage for reconstruction
- `RAY_max_lineage_bytes` (default: 1 GB) - Maximum lineage to keep

**Performance Implication**: Helps with fault tolerance but increases memory usage. When lineage limit is hit, 50% is evicted and lost objects can't be reconstructed.

### 2.5 Object Transfer & Chunking
**File**: `/home/user/ray/src/ray/object_manager/object_buffer_pool.h` (lines 31-80)

**Chunking Configuration**:
- `RAY_object_manager_default_chunk_size` (default: 5 MB) - Size for splitting large objects
- `RAY_object_manager_max_bytes_in_flight` (default: 2 GB) - Memory limit for concurrent transfers

**Performance Optimization**: Chunking allows:
- Parallel transfer of object segments
- Memory-bounded transfers
- Progressive receive capabilities

### 2.6 Push/Pull Manager - Rate Limiting & Deduplication
**File**: `/home/user/ray/src/ray/object_manager/push_manager.h` (lines 27-75)

**Push Manager**:
- Deduplicates concurrent pushes to same destination
- Rate-limits outbound transfers via `max_chunks_in_flight`
- Manages queue of pending push requests

**Key Functions**:
- `StartPush()`: Initiate push with deduplication
- `OnChunkComplete()`: Trigger next chunk send
- `HandleNodeRemoved()`: Cancel pending pushes to removed nodes

**File**: `/home/user/ray/src/ray/object_manager/pull_manager.h` (lines 40-100)

**Pull Manager**:
- Manages object fetch requests with priority-based batching
- Supports GET requests, WAIT requests, and task arguments
- Respects object store capacity constraints

**Priority Classes**:
```c
enum BundlePriority : uint8_t {
  GET_REQUEST,    // ray.get() requests
  WAIT_REQUEST,   // ray.wait() requests
  TASK_ARGS,      // Task argument fetching
}
```

**TODO Found** (line 133):
```
// TODO(ekl) this overestimates bytes needed if it's already available
```
Indicates potential memory estimation improvement in pull manager.

---

## 3. SERIALIZATION OVERHEAD

### 3.1 Serialization Infrastructure
**File**: `/home/user/ray/python/ray/_private/serialization.py` (lines 1-200)

**Serialization Flow**:
1. Custom cloudpickle reducers for Ray types
2. Object reference tracking
3. Tensor transport support (gloo, nccl)
4. Support for generator returns

**Key Performance Features**:
- Out-of-band object reference serialization (controlled by `RAY_allow_out_of_band_object_ref_serialization`)
- GPU object metadata handling
- Efficient function pickling through global function references

**TODO Found** (lines 83-85):
```python
# TODO(edoakes): we should be able to just capture a reference
# to 'self' here instead, but this function is itself pickled
# somewhere, which causes an error.
```
Optimization opportunity for reducing pickle overhead.

### 3.2 Arrow Serialization Optimization
**File**: `/home/user/ray/python/ray/_private/arrow_serialization.py` (lines 88-150)

**Issue**: Zero-copy array slicing pickling bug (Apache Arrow ARROW-10739)

**Optimization**:
- Custom reducer for Arrow Tables using IPC format
- Converts array-level slicing to buffer-level slicing
- Prevents full buffer serialization of slices

**Disabled Via**: `RAY_DISABLE_CUSTOM_ARROW_DATA_SERIALIZATION=1` (default: 0)

**Performance Impact**: 
- Handles chunked arrays and complex types
- Fallback to standard serialization for unsupported types
- Logs warnings for unsupported serialization paths

### 3.3 Serializer Selection
**Supported Serializers**:
1. **Cloudpickle** (Python objects, default)
2. **MessagePack** (lightweight, binary format)
3. **Pickle5** (protocol 5, buffer optimization)
4. **Arrow IPC** (columnar data, zero-copy)

**Configuration** (Ray core):
- `RAY_allow_out_of_band_object_ref_serialization` (default: true) - Out-of-band ref serialization

---

## 4. NETWORK COMMUNICATION & gRPC

### 4.1 gRPC Configuration
**File**: `/home/user/ray/src/ray/rpc/grpc_client.h` (lines 59-97)

**Message Size Configuration**:
- `RAY_max_grpc_message_size` (default: 512 MB) - Max message size (vs 4 MB default)
- `RAY_agent_max_grpc_message_size` (default: 20 MB) - Agent RPC limit
- `RAY_grpc_enable_http_proxy` (default: false) - HTTP proxy support

**gRPC Channel Configuration**:
```cpp
std::shared_ptr<grpc::Channel> BuildChannel(
    const std::string &address,
    int port,
    std::optional<grpc::ChannelArguments> arguments);
```

**Reconnection Configuration**:
- `RAY_gcs_grpc_max_reconnect_backoff_ms` (default: 2000)
- `RAY_gcs_grpc_min_reconnect_backoff_ms` (default: 1000) - Misused as connection timeout
- `RAY_gcs_grpc_initial_reconnect_backoff_ms` (default: 100)
- `RAY_grpc_client_check_connection_status_interval_milliseconds` (default: 1000)

**High-Latency Network Workaround**:
```
RAY_gcs_grpc_min_reconnect_backoff_ms should be > 4x the network latency
```

### 4.2 GCS (Global Control Store) RPC Configuration
**File**: `/home/user/ray/src/ray/common/ray_config_def.h` (lines 359-392)

**Threading Configuration**:
- `RAY_gcs_server_rpc_server_thread_num` (default: CPU cores / 4) - RPC server threads
- `RAY_gcs_server_rpc_client_thread_num` (default: CPU cores / 4) - RPC client threads

**Batching Configuration**:
- `RAY_maximum_gcs_deletion_batch_size` (default: 1000) - GCS deletion batch
- `RAY_maximum_gcs_storage_operation_batch_size` (default: 1000) - GCS operation batch

**Connection Pool**:
- `RAY_gcs_max_concurrent_resource_pulls` (default: 100) - Concurrent resource pulls
- `RAY_gcs_grpc_max_request_queued_max_bytes` (default: 5 GB) - Max queued request bytes

### 4.3 Object Transfer Configuration
**File**: `/home/user/ray/src/ray/common/ray_config_def.h` (lines 324-349)

**Timer & Timeout Configuration**:
- `RAY_object_manager_timer_freq_ms` (default: 100) - Global timer interval
- `RAY_object_manager_pull_timeout_ms` (default: 10000) - Pull retry timeout
- `RAY_object_manager_push_timeout_ms` (default: 10000) - Push timeout (negative = infinite)

**Chunk Configuration**:
- `RAY_object_manager_default_chunk_size` (default: 5 MB)
- `RAY_object_manager_max_bytes_in_flight` (default: 2 GB)

### 4.4 Object Resolution Configuration
**File**: `/home/user/ray/src/ray/common/ray_config_def.h` (lines 228-256)

**Fetch Request Configuration**:
- `RAY_object_timeout_milliseconds` (default: 100) - Initial owner check delay
- `RAY_raylet_fetch_timeout_milliseconds` (default: 1000) - Fetch retry interval
- `RAY_worker_fetch_request_size` (default: 10000) - Batch size for object fetches
- `RAY_get_check_signal_interval_milliseconds` (default: 1000) - Signal checking interval

**Timeout Configuration**:
- `RAY_fetch_warn_timeout_milliseconds` (default: 60 seconds) - Warning threshold
- `RAY_fetch_fail_timeout_milliseconds` (default: 600 seconds) - Failure threshold

### 4.5 Message Refresh Workaround
**File**: `/home/user/ray/src/ray/common/ray_config_def.h` (lines 451-454)

```
RAY_ray_syncer_message_refresh_interval_ms (default: 3000)
```
Protocol drawback workaround: Raylet refreshes messages periodically to prevent disconnection.

### 4.6 Batching for Telemetry
**File**: `/home/user/ray/src/ray/common/ray_config_def.h` (lines 456-468)

**Metrics & Events Batching**:
- `RAY_metrics_report_batch_size` (default: 10000) - Metrics per batch
- `RAY_task_events_report_interval_ms` (default: 1000) - Task event reporting
- `RAY_ray_events_report_interval_ms` (default: 1000) - Ray event reporting

---

## 5. MEMORY MANAGEMENT

### 5.1 Reference Counting System
**File**: `/home/user/ray/src/ray/core_worker/reference_counter.h` (lines 39-150)

**Core Functions**:
```cpp
// Track local references
void AddLocalReference(const ObjectID &object_id)
void RemoveLocalReference(const ObjectID &object_id, std::vector<ObjectID> *deleted)

// Track task references
void UpdateSubmittedTaskReferences(const std::vector<ObjectID> &return_ids, ...)
void UpdateFinishedTaskReferences(const std::vector<ObjectID> &return_ids, ...)

// Manage owned objects
void AddOwnedObject(const ObjectID &object_id, ...)
void UpdateObjectSize(const ObjectID &object_id, int64_t object_size)

// Borrowed objects
void AddBorrowedObject(const ObjectID &object_id, ...)
void GetOwner(const ObjectID &object_id, rpc::Address *owner_address)
```

**Reference Counting Configuration**:
- `RAY_record_ref_creation_sites` (default: false) - Track creation sites (5-10 μs overhead)
- `RAY_record_task_actor_creation_sites` (default: false) - Serialize creation stacktraces

### 5.2 Memory Monitoring & GC
**File**: `/home/user/ray/src/ray/common/ray_config_def.h` (lines 67-107)

**Memory Threshold Configuration**:
- `RAY_memory_usage_threshold` (default: 0.95) - OOM threshold
- `RAY_min_memory_free_bytes` (default: -1/disabled) - Minimum free space
- `RAY_memory_monitor_refresh_ms` (default: 250) - Memory monitor interval

**Garbage Collection**:
- `RAY_raylet_check_gc_period_milliseconds` (default: 100) - GC check interval
- `RAY_gcs_global_gc_interval_milliseconds` (default: 10000) - Global GC interval
- `RAY_local_gc_interval_s` (default: 90 * 60) - Local Python GC interval
- `RAY_local_gc_min_interval_s` (default: 10) - Minimum GC interval
- `RAY_global_gc_min_interval_s` (default: 30) - Global GC minimum interval

**Memory Pressure Response**:
- `RAY_plasma_store_usage_trigger_gc_threshold` (default: 0.7) - GC trigger at 70% usage
- `RAY_task_oom_retries` (default: -1/infinite) - Retry OOM-killed tasks

### 5.3 Plasma Memory Pre-allocation
**File**: `/home/user/ray/src/ray/common/ray_config_def.h` (lines 152-156)

```
RAY_preallocate_plasma_memory (default: false)
```
- Avoids SIGBUS errors during object creation
- Trade-off: Higher memory usage upfront, slower Ray startup
- Workaround for issue: https://github.com/ray-project/ray/issues/14182

### 5.4 Object Store Memory Reporting
**File**: `/home/user/ray/src/ray/common/ray_config_def.h` (lines 191-194)

```
RAY_scheduler_report_pinned_bytes_only (default: true)
```
- Only reports pinned object copies in `object_store_memory` resource
- Prevents autoscaler overestimation for secondary copies

---

## 6. PERFORMANCE BOTTLENECKS & KNOWN ISSUES

### 6.1 Object Manager Bottlenecks

**File**: `/home/user/ray/src/ray/object_manager/object_manager.h` (line 1)
```cpp
// TODO(hme): Add success/failure callbacks for push and pull.
```

### 6.2 Push Manager Round-Robin
**File**: `/home/user/ray/src/ray/object_manager/push_manager.cc` (lines 55-57)
```cpp
// TODO(ekl) this isn't the best implementation of round robin, we should...
// TODO(dayshah): Does round-robin even make sense here? We should probably finish...
```
**Impact**: Suboptimal load balancing in push operations.

### 6.3 Pull Manager Memory Estimation
**File**: `/home/user/ray/src/ray/object_manager/pull_manager.cc` (line 133)
```cpp
// TODO(ekl) this overestimates bytes needed if it's already available
```
**Impact**: Conservative but safe memory allocation for pulls.

### 6.4 Serialization Edge Cases
**File**: `/home/user/ray/python/ray/_private/serialization.py` (lines 83-85)
```python
# TODO(edoakes): we should be able to just capture a reference
# to 'self' here instead, but this function is itself pickled
# somewhere, which causes an error.
```
**Impact**: Extra pickle overhead for object references.

### 6.5 Random Scheduling Policy
**File**: `/home/user/ray/src/ray/raylet/scheduling/scheduling_policy.h` (lines 81-86)
```cpp
/// TODO(scv119): if there are a lot of nodes died or can't fulfill the resource
/// requirement, the distribution might not be even.
```

### 6.6 Plasma Lock Workaround
**File**: `/home/user/ray/src/ray/common/ray_config_def.h` (line 259)
```
RAY_yield_plasma_lock_workaround (default: true)
```
**Issue**: Temporary workaround for https://github.com/ray-project/ray/pull/16402
**Performance Impact**: Potential lock contention on plasma store access.

---

## 7. PERFORMANCE TUNING GUIDELINES

### 7.1 High-Throughput Workloads

**Recommended Settings**:
```
RAY_scheduler_spread_threshold=0.3          # Spread load across nodes
RAY_object_manager_default_chunk_size=10MB  # Larger chunks for throughput
RAY_object_manager_max_bytes_in_flight=5GB  # Increase concurrent transfers
RAY_free_objects_period_milliseconds=5000   # Less frequent eviction checks
RAY_free_objects_batch_size=500             # Larger batches
```

### 7.2 Low-Latency Workloads

**Recommended Settings**:
```
RAY_scheduler_spread_threshold=0.8          # Pack on available nodes
RAY_object_timeout_milliseconds=50          # Faster object detection
RAY_object_manager_pull_timeout_ms=5000     # Shorter pull retry
RAY_memory_monitor_refresh_ms=100           # Tighter memory monitoring
```

### 7.3 Memory-Constrained Clusters

**Recommended Settings**:
```
RAY_object_spilling_threshold=0.6           # Spill earlier
RAY_min_spilling_size=50MB                  # Smaller spill units
RAY_lineage_pinning_enabled=false           # Disable lineage pinning
RAY_max_lineage_bytes=100MB                 # Limit lineage size
RAY_preallocate_plasma_memory=false         # Don't pre-allocate
```

### 7.4 High-Latency Networks

**Recommended Settings**:
```
RAY_gcs_grpc_min_reconnect_backoff_ms=5000  # > 4x network latency
RAY_object_manager_pull_timeout_ms=30000    # Allow time for network
RAY_fetch_warn_timeout_milliseconds=120000  # Longer timeout threshold
```

---

## 8. CACHING & OPTIMIZATION MECHANISMS

### 8.1 Worker Pool Reuse
- Workers are cached and reused for same scheduling class
- Eliminates cold start penalties for subsequent tasks
- Soft cap prevents excessive pool growth

### 8.2 Serialization Caching (Future)
**File**: `/home/user/ray/src/ray/common/ray_config_def.h` (mentions in recent commits)
- Serialization results can be cached to avoid re-serialization
- Helpful for repeatedly serialized large objects

### 8.3 Object Location Caching
- Object locations cached in `ObjectDirectory`
- Reduces GCS queries for frequent objects

### 8.4 Metrics Batching
- Task events and Ray events batched before reporting
- Reduces GCS load from metrics

---

## 9. CONFIGURATION SUMMARY TABLE

| Component | Parameter | Default | Min | Max | Impact |
|-----------|-----------|---------|-----|-----|--------|
| **Scheduling** | `scheduler_spread_threshold` | 0.5 | 0.0 | 1.0 | Load distribution |
| | `worker_cap_enabled` | true | - | - | Deadlock prevention |
| | `worker_cap_initial_backoff_delay_ms` | 1000 | - | - | Backpressure |
| **Object Store** | `object_manager_default_chunk_size` | 5 MB | - | - | Transfer parallelism |
| | `object_manager_max_bytes_in_flight` | 2 GB | - | - | Memory usage |
| | `min_spilling_size` | 100 MB | - | - | Spill granularity |
| | `object_spilling_threshold` | 0.8 | - | - | When to spill |
| | `free_objects_period_milliseconds` | 1000 | - | - | Eviction batching |
| | `free_objects_batch_size` | 100 | - | - | Objects per batch |
| **Memory** | `memory_usage_threshold` | 0.95 | - | - | OOM threshold |
| | `memory_monitor_refresh_ms` | 250 | - | - | Monitor frequency |
| | `plasma_store_usage_trigger_gc_threshold` | 0.7 | - | - | GC trigger |
| **Network** | `max_grpc_message_size` | 512 MB | - | - | Max message size |
| | `object_manager_pull_timeout_ms` | 10000 | - | - | Pull retry |
| | `fetch_warn_timeout_milliseconds` | 60000 | - | - | Warn timeout |

---

## 10. RELATED DOCUMENTATION

- **Performance Tips**: `/home/user/ray/doc/source/data/performance-tips.rst`
- **Serve Performance**: `/home/user/ray/doc/source/serve/advanced-guides/performance.md`
- **Scheduling Tests**: `/home/user/ray/src/ray/raylet/scheduling/tests/`
- **Reference Counting**: `/home/user/ray/python/ray/tests/test_reference_counting.py`

---

## 11. RECOMMENDATION SUMMARY

1. **For Scheduling**: Adjust `scheduler_spread_threshold` based on workload type
2. **For Object Transfers**: Tune chunk size and in-flight bytes based on network bandwidth
3. **For Spilling**: Configure threshold and batch size based on available storage
4. **For Memory**: Adjust GC intervals and thresholds based on memory pressure
5. **For Networks**: Increase gRPC backoff times for high-latency links

