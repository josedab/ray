# Part 2: Deep Dive - Ray Core Task and Actor System

> **Series:** Ray Deep Dive | **Reading Time:** 18 minutes
> **Commit:** `d1cce8c9dc8411fad7cfbd619350bec6f19839a3`

## What You'll Learn

- Complete task lifecycle from submission to result retrieval
- Actor state management and method dispatch
- Object store internals and data transfer mechanisms
- Scheduling algorithms and resource allocation

## Introduction

In Part 1, we explored Ray's high-level architecture. Now we'll follow data through the system, examining exactly what happens when you call `.remote()` and `ray.get()`. Understanding these internals helps you write better Ray code and debug issues effectively.

## Task Submission Pipeline

When you call a remote function, a cascade of operations begins. Let's trace through each step.

### Step 1: The @ray.remote Decorator

The decorator transforms your function into a `RemoteFunction`:

```python
# What you write
@ray.remote
def process(data):
    return transform(data)

# What Ray creates (simplified)
# File: python/ray/remote_function.py
class RemoteFunction:
    def __init__(self, function, num_cpus=1, num_gpus=0, ...):
        self._function = function
        self._function_descriptor = self._make_descriptor()

    def remote(self, *args, **kwargs):
        return self._remote(args, kwargs)
```

The `RemoteFunction` stores metadata about resource requirements and creates a function descriptor for the cluster.

### Step 2: Argument Serialization

When you call `.remote(data)`, Ray serializes everything:

```python
# python/ray/_private/serialization.py

def serialize(obj):
    # 1. Check for special types (ObjectRef, ActorHandle)
    # 2. Use custom serializer if registered
    # 3. Fall back to cloudpickle

    if isinstance(obj, ObjectRef):
        return serialize_object_ref(obj)
    elif isinstance(obj, np.ndarray):
        # Zero-copy for numpy arrays
        return serialize_ndarray(obj)
    else:
        return cloudpickle.dumps(obj)
```

**Performance tip:** Large arguments are automatically placed in the object store. You can control this with:

```python
# Arguments > 100KB go to object store automatically
result = process.remote(large_data)  # Efficient

# Explicitly put for reuse
data_ref = ray.put(large_data)
results = [process.remote(data_ref) for _ in range(100)]  # Best
```

### Step 3: Function Export

The first time a function runs, Ray exports it to the GCS:

```python
# python/ray/_private/worker.py

def _export_function(self, function_descriptor, function):
    # Check if already exported
    if function_descriptor.function_id in self._exported_functions:
        return

    # Serialize function bytecode
    pickled = cloudpickle.dumps(function)

    # Send to GCS
    self._gcs_client.internal_kv_put(
        f"func:{function_descriptor.function_id}",
        pickled
    )
```

### Step 4: Task Submission to CoreWorker

The Python layer calls into C++ via Cython:

```cython
# python/ray/_raylet.pyx

def submit_task(self, function_descriptor, args, num_returns, resources):
    # Convert Python objects to C++ types
    cdef CTaskSpec task_spec = make_task_spec(...)

    # Submit to C++ CoreWorker
    with nogil:
        status = self.core_worker.get().SubmitTask(task_spec)
```

### Step 5: CoreWorker Processing

The C++ CoreWorker handles the actual submission:

```cpp
// src/ray/core_worker/core_worker.cc

Status CoreWorker::SubmitTask(const TaskSpecification &task_spec) {
  // 1. Track task dependencies
  for (const auto &arg : task_spec.Args()) {
    if (arg.IsObjectRef()) {
      reference_counter_->AddLocalReference(arg.GetObjectRef());
    }
  }

  // 2. Create ObjectRefs for return values
  std::vector<ObjectID> return_ids = task_spec.ReturnIds();

  // 3. Submit to task manager
  task_manager_->AddPendingTask(task_spec, return_ids);

  // 4. Request scheduling from Raylet
  direct_task_submitter_->SubmitTask(task_spec);

  return Status::OK();
}
```

### Step 6: Raylet Scheduling

The Raylet receives the task and schedules it:

```cpp
// src/ray/raylet/node_manager.cc

void NodeManager::HandleRequestWorkerLease(const TaskSpecification &task_spec) {
  // 1. Check local resources
  auto resources = task_spec.GetRequiredResources();

  // 2. Find best worker using scheduling policy
  auto worker = cluster_resource_scheduler_->GetBestSchedulableNode(resources);

  // 3. If local, dispatch to worker pool
  if (worker.is_local) {
    worker_pool_->DispatchTask(task_spec, worker);
  } else {
    // Forward to remote node
    ForwardTask(task_spec, worker.node_id);
  }
}
```

## Scheduling Deep Dive

Ray's scheduler balances multiple objectives. Let's examine how it works.

### Hybrid Scheduling Policy

The default policy considers:

1. **Resource availability:** Does the node have required CPU/GPU/memory?
2. **Load balancing:** Avoid overloading any single node
3. **Locality:** Prefer nodes where input data lives
4. **Spread threshold:** Configurable balance between packing and spreading

```cpp
// src/ray/raylet/scheduling/cluster_resource_scheduler.h

NodeID ClusterResourceScheduler::GetBestSchedulableNode(
    const ResourceRequest &resource_request) {

  std::vector<std::pair<NodeID, double>> candidates;

  for (const auto &node : cluster_resources_) {
    if (!node.HasAvailableResources(resource_request)) {
      continue;
    }

    // Calculate score based on:
    // - Available resources (higher = better)
    // - Number of pending tasks (lower = better)
    // - Data locality (local objects = bonus)
    double score = CalculateNodeScore(node, resource_request);
    candidates.push_back({node.id, score});
  }

  // Sort by score and apply spread threshold
  return SelectNode(candidates, spread_threshold_);
}
```

### Resource Types

Ray supports various resource types:

```python
# Built-in resources
@ray.remote(num_cpus=2, num_gpus=1, memory=4 * 1024**3)
def gpu_task():
    pass

# Custom resources
@ray.remote(resources={"TPU": 1, "special_hardware": 2})
def custom_task():
    pass

# Fractional resources (for GPU sharing)
@ray.remote(num_gpus=0.5)
def shared_gpu_task():
    pass
```

## Actor Method Dispatch

Actors add statefulness and ordering guarantees. Let's see how method calls work.

### Actor Creation

When you create an actor:

```python
@ray.remote
class Counter:
    def __init__(self, initial=0):
        self.value = initial

    def increment(self):
        self.value += 1
        return self.value

counter = Counter.remote(10)
```

1. **GCS registration:** Actor metadata stored in GCS
2. **Resource acquisition:** Raylet reserves resources
3. **Worker creation:** New process started (or existing reused)
4. **Constructor execution:** `__init__` runs on worker

### Method Call Pipeline

```python
result = counter.increment.remote()
```

```cpp
// src/ray/core_worker/core_worker.cc

Status CoreWorker::SubmitActorTask(
    const ActorID &actor_id,
    const TaskSpecification &task_spec) {

  // 1. Look up actor location from cache (or GCS)
  auto actor_handle = actor_manager_->GetActorHandle(actor_id);

  // 2. Get next sequence number for ordering
  uint64_t seq_no = actor_handle->NextTaskSeqNo();

  // 3. Send directly to actor's worker via gRPC
  auto &address = actor_handle->GetActorAddress();
  direct_actor_submitter_->SubmitTask(task_spec, address, seq_no);

  return Status::OK();
}
```

### Ordering Guarantees

Actor methods execute in submission order from each caller:

```python
# These execute in order: 1, 2, 3
counter.increment.remote()  # 1
counter.increment.remote()  # 2
counter.increment.remote()  # 3

# But different actors are independent
counter1.increment.remote()  # May run concurrently
counter2.increment.remote()  # with this
```

### Concurrency Control

By default, actors process one method at a time. You can enable concurrency:

```python
@ray.remote(max_concurrency=10)
class ConcurrentService:
    async def handle_request(self, request):
        # Up to 10 requests processed concurrently
        return await process(request)
```

**Key file:** [`src/ray/core_worker/actor_scheduling_queue.cc`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/src/ray/core_worker/actor_scheduling_queue.cc)

## Object Store Internals

The Plasma object store is central to Ray's performance. Let's explore how it works.

### Object Lifecycle

```python
# 1. Create object
ref = ray.put(data)

# 2. Object lives in Plasma (shared memory)
# 3. Other tasks/actors can access it

# 4. When all references are gone, object can be evicted
del ref
```

### Memory Layout

Plasma uses memory-mapped files for zero-copy access:

```cpp
// src/ray/object_manager/plasma/store.cc

Status PlasmaStore::CreateObject(const ObjectID &object_id,
                                  int64_t data_size,
                                  std::shared_ptr<Buffer> *buffer) {
  // Allocate from mmap'd region
  uint8_t *data = AllocateMemory(data_size);

  // Create buffer wrapper
  *buffer = std::make_shared<PlasmaBuffer>(data, data_size);

  // Track in object table
  object_table_[object_id] = ObjectEntry{data, data_size, CREATED};

  return Status::OK();
}
```

### Eviction and Spilling

When memory fills up:

```cpp
// src/ray/object_manager/plasma/eviction_policy.cc

std::vector<ObjectID> EvictionPolicy::ChooseObjectsToEvict(int64_t bytes_needed) {
  std::vector<ObjectID> to_evict;
  int64_t freed = 0;

  // LRU eviction
  while (freed < bytes_needed && !lru_queue_.empty()) {
    ObjectID oldest = lru_queue_.front();
    lru_queue_.pop_front();

    if (CanEvict(oldest)) {
      to_evict.push_back(oldest);
      freed += GetObjectSize(oldest);
    }
  }

  return to_evict;
}
```

Spilling writes to disk when memory is exhausted:

```python
# Configure spilling
ray.init(_system_config={
    "object_spilling_config": json.dumps({
        "type": "filesystem",
        "params": {"directory_path": "/tmp/ray_spill"}
    })
})
```

### Object Transfer

When a task needs an object from another node:

```cpp
// src/ray/object_manager/pull_manager.cc

void PullManager::Pull(const ObjectID &object_id) {
  // 1. Find object location from owner
  auto location = GetObjectLocation(object_id);

  // 2. Send pull request
  pull_requests_.push({object_id, location});

  // 3. Object is transferred and stored locally
}

// src/ray/object_manager/push_manager.cc

void PushManager::Push(const ObjectID &object_id, const NodeID &dest) {
  // Read from local Plasma
  auto buffer = plasma_store_->Get(object_id);

  // Send via gRPC
  SendObjectChunk(dest, object_id, buffer);
}
```

**Configuration for transfers:**
- `RAY_object_manager_pull_timeout_ms`: Pull timeout (default: 10000)
- Chunks: 5MB default, configurable

## Task Execution

When a worker receives a task:

```cpp
// src/ray/core_worker/task_execution/task_receiver.cc

void TaskReceiver::HandleTask(const TaskSpecification &task_spec) {
  // 1. Wait for dependencies
  std::vector<ObjectID> deps = task_spec.GetDependencies();
  WaitForObjects(deps);

  // 2. Load function
  auto function = LoadFunction(task_spec.FunctionDescriptor());

  // 3. Deserialize arguments
  auto args = DeserializeArgs(task_spec.Args());

  // 4. Execute (calls back into Python)
  auto result = Execute(function, args);

  // 5. Store result in Plasma
  auto return_id = task_spec.ReturnId();
  plasma_store_->Put(return_id, result);

  // 6. Notify task completion
  NotifyTaskComplete(task_spec.TaskId());
}
```

## Fault Tolerance

Ray provides automatic fault tolerance through lineage reconstruction.

### Object Reconstruction

If an object is lost (node failure), Ray recreates it:

```python
# Original task
ref = expensive_task.remote(input_data)

# If the node storing the result fails...
# Ray automatically re-executes expensive_task
result = ray.get(ref)  # Still works!
```

```cpp
// src/ray/core_worker/reference_counter.cc

void ReferenceCounter::HandleObjectLost(const ObjectID &object_id) {
  // 1. Find the task that created this object
  auto lineage = GetLineage(object_id);

  // 2. Re-submit the task
  ResubmitTask(lineage.task_spec);

  // 3. Recursively reconstruct dependencies if needed
  for (const auto &dep : lineage.dependencies) {
    if (IsObjectLost(dep)) {
      HandleObjectLost(dep);
    }
  }
}
```

### Actor Fault Tolerance

Actors can be restarted automatically:

```python
@ray.remote(max_restarts=3, max_task_retries=2)
class ResilientActor:
    def __init__(self):
        self.state = load_checkpoint()

    def process(self, data):
        result = compute(data)
        self.save_checkpoint()
        return result
```

**Actor restart flow:**
1. Actor process crashes
2. GCS detects failure (heartbeat timeout)
3. GCS transitions actor to RESTARTING
4. New worker created, constructor re-executed
5. Pending method calls retried

## Performance Patterns

### Batching for Efficiency

```python
# Inefficient: Many small tasks
results = [process.remote(item) for item in items]

# Better: Batch processing
@ray.remote
def process_batch(items):
    return [process(item) for item in items]

batches = [items[i:i+100] for i in range(0, len(items), 100)]
results = [process_batch.remote(batch) for batch in batches]
```

### Locality-Aware Scheduling

```python
# Put data on specific nodes
@ray.remote(resources={"node:worker1": 1})
def put_on_node1():
    return ray.put(large_data)

# Schedule task where data lives
data_ref = put_on_node1.remote()

@ray.remote
def process_local(data):
    # Will be scheduled on worker1 for locality
    return transform(data)

result = process_local.remote(data_ref)
```

### Pipelining

```python
# Sequential (slow)
a = step1.remote(input)
b = step2.remote(ray.get(a))
c = step3.remote(ray.get(b))

# Pipelined (fast)
a = step1.remote(input)
b = step2.remote(a)  # Don't call ray.get()
c = step3.remote(b)  # Ray handles dependencies
result = ray.get(c)  # Only block at the end
```

## Debugging Task Issues

### Using ray.timeline()

```python
ray.init()

# Enable timeline
ray.timeline("timeline.json")

# Run workload
results = ray.get([task.remote(i) for i in range(100)])

# View in chrome://tracing
```

### State API for Debugging

```python
from ray.util.state import list_tasks, get_task

# List all tasks
tasks = list_tasks(filters=[("state", "=", "RUNNING")])

# Get task details
task = get_task(task_id)
print(task.state, task.error_message)
```

## Key Takeaways

1. **Task submission** flows: Python → Cython → C++ CoreWorker → Raylet
2. **Scheduling** balances resources, load, and locality with configurable policy
3. **Actor methods** execute in order per caller with direct gRPC dispatch
4. **Object store** uses shared memory for zero-copy, with LRU eviction and disk spilling
5. **Fault tolerance** is automatic through lineage-based reconstruction

## What's Next

In [Part 3](./03-patterns-practices.md), we'll explore:
- Design patterns used throughout Ray
- Code organization strategies
- Testing approaches
- Error handling best practices

## Code References

- Task submission: [`src/ray/core_worker/task_submission/`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/src/ray/core_worker/task_submission/)
- Scheduling: [`src/ray/raylet/scheduling/`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/src/ray/raylet/scheduling/)
- Object store: [`src/ray/object_manager/`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/src/ray/object_manager/)
- Reference counting: [`src/ray/core_worker/reference_counter.cc`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/src/ray/core_worker/reference_counter.cc)

---

*Next: [Part 3 - Patterns and Practices in Ray](./03-patterns-practices.md)*
