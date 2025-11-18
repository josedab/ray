# Part 1: Understanding Ray - Architecture and Core Concepts

> **Series:** Ray Deep Dive | **Reading Time:** 15 minutes
> **Commit:** `d1cce8c9dc8411fad7cfbd619350bec6f19839a3`

## What You'll Learn

- Ray's layered architecture and why it's designed this way
- Core components: GCS, Raylet, Plasma, and CoreWorker
- Key design decisions and their trade-offs
- How tasks and actors work at a fundamental level

## Introduction

Ray has become the go-to framework for scaling Python applications, from machine learning training to data processing. But what makes Ray tick? In this post, we'll peel back the layers and explore how Ray transforms simple Python functions into distributed computations running across hundreds of machines.

We'll start with a high-level overview and progressively dive deeper into each component. By the end, you'll understand not just *what* Ray does, but *why* it's designed the way it is.

## The 30-Second Overview

At its core, Ray provides a simple programming model:

```python
import ray

@ray.remote
def process(data):
    return expensive_computation(data)

# Run on cluster
futures = [process.remote(chunk) for chunk in data_chunks]
results = ray.get(futures)
```

This simplicity hides a sophisticated distributed system. Let's explore how it works.

## Ray's Layered Architecture

Ray's architecture follows a layered design, each layer abstracting complexity from the one above:

```
┌─────────────────────────────────────────────────┐
│          User Application Layer                  │
│     (@ray.remote functions and classes)          │
└─────────────────────┬───────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────┐
│          Python API Layer                        │
│   ray.init(), ray.get(), ray.put(), actors      │
│   File: python/ray/__init__.py                   │
└─────────────────────┬───────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────┐
│          Cython Bindings Layer                   │
│   Bridge between Python and C++                  │
│   File: python/ray/_raylet.pyx (196KB)          │
└─────────────────────┬───────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────┐
│          C++ Core Layer                          │
│   CoreWorker, TaskManager, ReferenceCounter     │
│   Directory: src/ray/core_worker/               │
└─────────────────────┬───────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────┐
│      Distributed Services Layer                  │
│   GCS, Raylet, Plasma Object Store              │
└─────────────────────────────────────────────────┘
```

**Why this design?**

1. **Python API Layer** provides a Pythonic interface that feels natural
2. **Cython Bindings** enable zero-copy data transfer and efficient C++ interop
3. **C++ Core** handles performance-critical operations (scheduling, memory)
4. **Distributed Services** manage cluster-wide state and storage

This separation allows Ray to optimize each layer independently. The Python layer focuses on usability, while C++ handles the heavy lifting.

## Core Components Deep Dive

Let's explore each major component and understand its role.

### Global Control Store (GCS)

The GCS is Ray's central nervous system. It maintains cluster-wide metadata:

- **Actor registry:** Where actors live and their current state
- **Job information:** Active jobs and their configurations
- **Node membership:** Which nodes are in the cluster
- **Placement groups:** Resource reservations

**Implementation:** The GCS runs on the head node and uses Redis as its backend storage (configurable). All components communicate with GCS via gRPC.

**Key file:** [`src/ray/gcs/gcs_server/`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/src/ray/gcs/gcs_server/)

**Design decision:** Ray chose centralized metadata over distributed consensus (like Raft). This simplifies the consistency model significantly:

```cpp
// From src/ray/gcs/gcs_actor_manager.h
// Actor states are managed centrally
enum class ActorTableData_ActorState {
  DEPENDENCIES_UNREADY,
  PENDING_CREATION,
  ALIVE,
  RESTARTING,
  DEAD
};
```

**Trade-off:** The GCS can become a bottleneck at very large scale (10,000+ nodes). Ray mitigates this with:
- Caching at Raylets
- Pub/sub for state distribution
- GCS fault tolerance and HA options

### Raylet (Node Manager)

Each node runs a Raylet that handles local operations:

- **Task scheduling:** Deciding which worker runs each task
- **Resource management:** Tracking CPU, GPU, memory availability
- **Worker pool:** Starting and managing worker processes
- **Object transfer:** Coordinating data movement between nodes

**Key file:** [`src/ray/raylet/node_manager.cc`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/src/ray/raylet/node_manager.cc) (3,408 LOC)

The scheduling algorithm balances multiple concerns:

```cpp
// From src/ray/raylet/scheduling/cluster_resource_scheduler.h
// Hybrid scheduling policy
// - spread_threshold: 0.0 = pack (minimize nodes), 1.0 = spread (maximize distribution)
// - Considers: locality, load, resource availability
```

**Why per-node schedulers?** Distributed scheduling enables:
- Low-latency task dispatch (no round-trip to central scheduler)
- Better locality (tasks run where data lives when possible)
- Fault isolation (node failure doesn't stop other nodes)

### Plasma Object Store

Plasma is Ray's distributed in-memory object store. When you call `ray.put()`, data goes into Plasma.

**Key features:**
- **Shared memory (mmap):** Workers on the same node access objects without copying
- **Immutability:** Objects can't be modified after creation (enables sharing)
- **LRU eviction:** Old objects are evicted when memory is full
- **Disk spilling:** Overflow objects go to disk

**Key file:** [`src/ray/object_manager/plasma/`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/src/ray/object_manager/plasma/)

```python
# Example: Plasma zero-copy access
@ray.remote
def process_data():
    # This 1GB array is in shared memory
    # Multiple workers access it without copying
    big_array = np.zeros((1000, 1000, 1000))
    return ray.put(big_array)
```

**Memory management:**
- Default: 30% of system memory for object store
- Eviction threshold: 80% usage triggers LRU eviction
- Spilling threshold: Configurable, writes to local disk

### CoreWorker

The CoreWorker is the C++ engine inside every Ray worker process. It handles:

- **Task submission:** Sending tasks to the scheduler
- **Task execution:** Running function code and storing results
- **Reference counting:** Tracking object lifetimes for GC
- **gRPC communication:** All network operations

**Key file:** [`src/ray/core_worker/core_worker.cc`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/src/ray/core_worker/core_worker.cc) (4,660 LOC - largest C++ file)

The CoreWorker is the bridge between your Python code and Ray's distributed infrastructure:

```cpp
// Simplified view of task submission
Status CoreWorker::SubmitTask(const TaskSpecification &task_spec) {
  // 1. Serialize function and arguments
  // 2. Track dependencies (ObjectRefs)
  // 3. Send to Raylet for scheduling
  // 4. Return future (ObjectRef) to caller
}
```

## Key Design Decisions and Trade-offs

### 1. Centralized vs. Distributed Metadata

**Choice:** Centralized GCS with Redis backend

**Rationale:**
- Simpler programming model (strong consistency)
- Easier debugging (single source of truth)
- Reduced complexity vs. distributed consensus

**Trade-off:**
- Potential bottleneck at extreme scale
- Single point of failure (mitigated with HA)

**Alternative considered:** Distributed hash table with eventual consistency. Rejected because it complicates actor location lookups and state management.

### 2. Reference Counting vs. Tracing GC

**Choice:** Distributed reference counting with lineage reconstruction

**Rationale:**
- Deterministic cleanup (important for large objects)
- Enables fault tolerance through lineage
- Avoids stop-the-world pauses

**Trade-off:**
- Complexity in distributed reference tracking
- Potential memory leaks with reference cycles

```python
# Reference counting in action
ref = ray.put(data)  # ref count = 1
ref2 = ref           # ref count = 2
del ref              # ref count = 1
del ref2             # ref count = 0, object can be evicted
```

**Key file:** [`src/ray/core_worker/reference_counter.cc`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/src/ray/core_worker/reference_counter.cc)

### 3. Stateless Raylets

**Choice:** Raylets don't store task execution state

**Rationale:**
- Raylet failure doesn't lose task results
- Workers own their task state
- Simpler recovery model

**Trade-off:**
- More state tracking in workers
- Complex ownership model for objects

### 4. Hybrid Scheduling Policy

**Choice:** Balance locality vs. load spreading

**Rationale:**
- Pure locality causes hotspots
- Pure spreading hurts data locality
- Configurable threshold adapts to workloads

```python
# Scheduling hint
@ray.remote(scheduling_strategy="SPREAD")
def distributed_task():
    pass
```

## Task Lifecycle: Putting It All Together

Let's trace a task from submission to completion:

```python
@ray.remote
def add(a, b):
    return a + b

result_ref = add.remote(1, 2)
result = ray.get(result_ref)
```

### Step 1: Decoration
When you write `@ray.remote`, Ray wraps your function in a `RemoteFunction` object:

```python
# python/ray/remote_function.py:RemoteFunction
class RemoteFunction:
    def remote(self, *args, **kwargs):
        # Creates task specification
        # Serializes function and arguments
        # Returns ObjectRef (future)
```

### Step 2: Submission
The `.remote()` call triggers task submission:

1. **Serialize:** Function bytecode and arguments are pickled
2. **Export:** Function is sent to GCS (once per unique function)
3. **Submit:** Task spec is sent to local Raylet

### Step 3: Scheduling
The Raylet schedules the task:

1. **Resource check:** Does a worker have required resources?
2. **Locality check:** Is input data local?
3. **Worker assignment:** Lease a worker for execution

### Step 4: Execution
The worker executes the task:

1. **Import function:** Load serialized function
2. **Fetch inputs:** Get arguments from object store
3. **Execute:** Run the Python function
4. **Store result:** Put result in local Plasma store

### Step 5: Result Retrieval
`ray.get()` fetches the result:

1. **Check local:** Is result in local object store?
2. **Fetch remote:** If not, pull from owning node
3. **Deserialize:** Convert bytes back to Python object

## Actor State Machine

Actors have a more complex lifecycle than tasks:

```
DEPENDENCIES_UNREADY → PENDING_CREATION → ALIVE → RESTARTING → DEAD
                                           ↑          ↓
                                           └──────────┘
```

**Key states:**
- **DEPENDENCIES_UNREADY:** Waiting for creation arguments
- **PENDING_CREATION:** Resources acquired, starting process
- **ALIVE:** Ready to receive method calls
- **RESTARTING:** Crashed, being recreated
- **DEAD:** Permanently terminated

```python
@ray.remote
class Counter:
    def __init__(self):
        self.value = 0

    def increment(self):
        self.value += 1
        return self.value

# Actor lifecycle
counter = Counter.remote()  # PENDING_CREATION → ALIVE
result = counter.increment.remote()  # Method call
ray.kill(counter)  # ALIVE → DEAD
```

## Diagrams

### Component Communication

```
┌─────────────┐         gRPC          ┌─────────────┐
│   Driver    │◄─────────────────────►│     GCS     │
│ (Your Code) │                       │  (Head Node)│
└──────┬──────┘                       └──────┬──────┘
       │                                     │
       │ gRPC                           Pub/Sub
       ▼                                     │
┌─────────────┐         gRPC          ┌──────▼──────┐
│   Raylet    │◄─────────────────────►│   Raylet    │
│  (Node 1)   │                       │  (Node 2)   │
└──────┬──────┘                       └──────┬──────┘
       │                                     │
       │ Shared Memory                       │
       ▼                                     ▼
┌─────────────┐                       ┌─────────────┐
│   Plasma    │◄──Object Transfer────►│   Plasma    │
│  (Node 1)   │                       │  (Node 2)   │
└─────────────┘                       └─────────────┘
```

## Key Takeaways

1. **Layered architecture** separates concerns: Python for usability, C++ for performance
2. **Centralized GCS** simplifies consistency at the cost of potential scale bottleneck
3. **Per-node Raylets** enable low-latency scheduling and fault isolation
4. **Plasma's immutable objects** allow zero-copy sharing
5. **Reference counting** provides deterministic cleanup and fault tolerance

## What's Next

In [Part 2](./02-deep-dive-core.md), we'll dive deeper into the task and actor system, exploring:
- Complete task submission pipeline
- Actor method dispatch and ordering
- Object transfer optimizations
- Scheduling algorithm details

## Code References

- Main Python API: [`python/ray/__init__.py`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/python/ray/__init__.py)
- Remote function implementation: [`python/ray/remote_function.py`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/python/ray/remote_function.py)
- Core worker: [`src/ray/core_worker/core_worker.cc`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/src/ray/core_worker/core_worker.cc)
- Node manager: [`src/ray/raylet/node_manager.cc`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/src/ray/raylet/node_manager.cc)
- GCS actor manager: [`src/ray/gcs/gcs_actor_manager.cc`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/src/ray/gcs/gcs_actor_manager.cc)

---

*Next: [Part 2 - Deep Dive into Ray Core Task and Actor System](./02-deep-dive-core.md)*
