# Ray Core Architecture and Design Patterns Analysis

## Executive Summary

Ray is a distributed computing framework with a sophisticated, multi-layered architecture that combines Python user-facing APIs with high-performance C++ core components. The system uses a combination of architectural patterns: layered (Python → Cython → C++), event-driven (RPC-based), publish-subscribe for state distribution, and object-oriented (Manager/Scheduler patterns).

---

## 1. Core Abstractions

Ray's programming model revolves around three primary abstractions:

### 1.1 Tasks
**Definition**: A task is a stateless function invocation marked with `@ray.remote` decorator.

**File References**:
- `/home/user/ray/python/ray/remote_function.py` (Python API)
- `/home/user/ray/src/ray/common/task/task_spec.h:79-82` (Task specification wrapper)
- `/home/user/ray/src/ray/core_worker/task_submission/normal_task_submitter.h:81-100` (Task submission)

**Key Characteristics**:
```python
# From python/ray/__init__.py:128
remote()  # Core API for task creation
```

- **Lazy Evaluation**: Returns `ObjectRef[T]` immediately without execution
- **Serializable**: Functions are pickled for transport
- **Parameterizable**: Resource constraints (num_cpus, num_gpus, memory, custom_resources)
- **Retryable**: Can specify max_retries and retry_exceptions
- **Distributed**: Execute on any available worker

**Task Lifecycle**:
```
submission → scheduling (raylet) → execution (core_worker) → result storage (object store)
```

### 1.2 Actors
**Definition**: An actor is a stateful service (class instance) accessible via method calls.

**File References**:
- `/home/user/ray/python/ray/actor.py:1-100` (Python API - 102KB file)
- `/home/user/ray/src/ray/core_worker/actor_manager.h` (C++ actor lifecycle)
- `/home/user/ray/src/ray/gcs/gcs_actor_manager.h:48-100` (GCS actor state machine)

**Key Characteristics**:
- **Stateful**: Maintains state across method invocations
- **Serializable**: Actor class is pickled and instantiated on a specific node
- **Single-threaded by default**: But supports concurrency groups
- **Remote methods**: Actor methods return ObjectRefs
- **Lifecycle**: Creation, invocation, optional restart, destruction

**Actor State Machine** (from `/home/user/ray/src/ray/gcs/gcs_actor_manager.h:50-92`):
```
DEPENDENCIES_UNREADY 
    ↓ (0: register actor)
PENDING_CREATION 
    ↓ (1: create task pushed, 2: created successfully)
ALIVE 
    ├─→ (6: detect dead worker/node) RESTARTING → ALIVE (3-4)
    └─→ (5: max restarts exceeded) DEAD
DEAD ←─ (8-9: owner/creator dead, no restarts left)
```

### 1.3 Objects and References
**Definition**: Objects are immutable values stored in distributed object store (Plasma).

**File References**:
- `/home/user/ray/python/ray/__init__.py:87-104` (ID types)
- `/home/user/ray/src/ray/common/id.h` (ID definitions)
- `/home/user/ray/src/ray/core_worker/reference_counter.h:42-100` (Reference tracking)

**Key ID Types**:
```cpp
// From python/ray/__init__.py:87-104
ObjectID          // 20-byte identifier for objects
ObjectRef         // Reference to an object (type-generic wrapper)
TaskID            // Identifier for task execution
ActorID           // Identifier for actor instance
JobID             // Job/application identifier
NodeID            // Node identifier in cluster
WorkerID          // Worker process identifier
FunctionID        // Unique function descriptor
ActorClassID      // Actor class descriptor
```

**Object Lifecycle**:
```
Task execution → Return values (object_ids)
    ↓
Object stored in Plasma/Memory Store
    ↓
References tracked by ReferenceCounter
    ↓
Last reference dropped → Garbage collection (object deleted)
```

### 1.4 Key Abstractions Summary Table

| Abstraction | Scope | Lifetime | Storage | Concurrency |
|-------------|-------|----------|---------|-------------|
| Task | Function call | Execution time | Object Store (results) | Many tasks parallel |
| Actor | Class instance | Until destroyed | Node memory | Single executor by default |
| Object | Immutable value | Until GC | Plasma/Memory | Read-only (shareable) |
| Worker | Process | Lifetime of cluster | Memory | Executes tasks/actors |

---

## 2. Architecture Pattern

Ray employs a **multi-layered, distributed, event-driven architecture**:

### 2.1 Layered Architecture

**Layer 1: Python API** (`/home/user/ray/python/ray/`)
- User-facing interfaces: `@ray.remote`, `ray.put()`, `ray.get()`
- Type-safe wrappers and decorators
- Local client logic for RPC calls
- Files: `actor.py`, `remote_function.py`, `_private/worker.py`

**Layer 2: Cython/C++ Bindings** (`/home/user/ray/python/ray/_raylet.pyx`)
- Bridge between Python and C++ core
- Serialization/deserialization
- ID generation and management
- Direct C++ object access

**Layer 3: C++ Core** (`/home/user/ray/src/ray/`)
- Distributed system components
- RPC handlers and clients
- Task scheduling and execution
- Object storage management
- State management

### 2.2 Component Architecture Diagram

```
┌─────────────────────────────────────────────────────┐
│  Python API Layer (ray.remote, ray.get, ray.put)  │
└──────────────────┬──────────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────────┐
│  Cython Bindings (_raylet.pyx)                     │
│  - Serialization, ID generation, direct C++ API   │
└──────────────────┬──────────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────────┐
│        C++ Core (src/ray/)                          │
├─────────────────────────────────────────────────────┤
│  Distributed Components:                            │
│  - GCS (Global Control Store)                      │
│  - Raylet (Node Manager/Scheduler)                 │
│  - CoreWorker (Task Executor/RPC Handler)          │
│  - Object Store (Plasma)                           │
│  - Reference Counter (GC manager)                  │
└─────────────────────────────────────────────────────┘
```

### 2.3 Event-Driven Architecture

Ray uses **RPC-based event-driven communication** via gRPC:

**Key RPC Services** (from protobuf files):
- `CoreWorkerService`: Worker-to-worker communication (task submission, object transfer)
- `NodeManagerService`: Raylet RPC API (task scheduling, resource management)
- `GcsService`: Global state store (actor creation, job management)

**Event Flow Example** (Task Submission):
```
Client (CoreWorker) 
    ↓ (submit task via RPC)
Raylet (NodeManager::HandleSubmitTask)
    ↓ (allocate resources)
Worker Raylet (schedule on worker)
    ↓ (PushTask RPC)
Remote Worker (CoreWorker::HandlePushTask)
    ↓ (execute task)
Return Object Store
    ↓ (notify via pub/sub)
Caller (retrieve via ray.get())
```

### 2.4 Pub/Sub Pattern for State Distribution

**Publisher-Subscriber Architecture** (`/home/user/ray/src/ray/pubsub/`):

```cpp
// From gcs_publisher.h:53-65
void PublishActor(const ActorID &id, rpc::ActorTableData message);
void PublishJob(const JobID &id, rpc::JobTableData message);
void PublishNodeInfo(const NodeID &id, rpc::GcsNodeInfo message);
void PublishError(std::string id, rpc::ErrorTableData message);
```

**Use Cases**:
- Actor state changes (creation, restart, death)
- Node liveness updates
- Job completion notifications
- Error propagation

**Subscribers**:
- Raylets (receive actor placement decisions)
- CoreWorkers (receive actor address updates)
- Dashboard (UI updates)

### 2.5 Architecture Patterns Used

| Pattern | Usage | Location |
|---------|-------|----------|
| **Manager** | Lifecycle management | GcsActorManager, LocalObjectManager |
| **Scheduler** | Resource allocation | ClusterResourceScheduler, ActorScheduler |
| **Service Handler** | RPC handling | CoreWorkerServiceHandler |
| **Queue** | Task buffering | ActorSchedulingQueue, NormalSchedulingQueue |
| **Factory** | Component creation | CgroupManagerFactory |
| **Strategy** | Pluggable behavior | LeasePolicy, SchedulingStrategy |
| **Pub/Sub** | Event propagation | GcsPublisher, Subscriber |
| **RPC Proxy** | Remote calls | RayletClientPool |

---

## 3. Component Relationships

### 3.1 Component Dependency Graph

```
                        ┌─────────────┐
                        │     GCS     │ (Global Control Store)
                        │  Redis-backed State │
                        └────┬────┬───┘
                             │    │
                ┌────────────┘    └─────────────┐
                │                               │
        ┌──────▼─────────┐          ┌──────────▼──────────┐
        │   Actor        │          │    Job              │
        │   Manager      │          │    Manager          │
        └──────┬─────────┘          └─────────────────────┘
               │
        ┌──────▼──────────────────┐
        │   Raylet (on each node) │  (Node Manager)
        │  ┌────────────────────┐ │
        │  │ Scheduler (local)  │ │
        │  │ Resource Manager   │ │
        │  │ Worker Pool        │ │
        │  └────────┬───────────┘ │
        └───────────┼─────────────┘
                    │
        ┌───────────▼────────────┐
        │ CoreWorker (per proc)  │  (Task Executor)
        │ ┌────────────────────┐ │
        │ │ Task Submission    │ │
        │ │ Task Execution     │ │
        │ │ Object Management  │ │
        │ │ Reference Counter  │ │
        │ └────────────────────┘ │
        └────────┬────────────────┘
                 │
        ┌────────▼──────────┐
        │  Object Store     │
        │  (Plasma)         │
        └───────────────────┘
```

### 3.2 Key Component Interactions

#### 3.2.1 GCS (Global Control Store)
**Purpose**: Central state repository for distributed coordination

**File References**:
- `/home/user/ray/src/ray/gcs/` (implementation)
- `/home/user/ray/src/ray/gcs/gcs_actor_manager.h:1-100` (Actor lifecycle)
- `/home/user/ray/src/ray/gcs/gcs_job_manager.h` (Job tracking)

**Responsibilities**:
- Actor registration and state tracking
- Job initialization and termination
- Worker heartbeats and liveness
- Placement group management
- Error table (exception messages)

**Interaction Pattern**:
```
GcsClient (in Raylet/CoreWorker)
    ↓ (RPC)
GCS Service (gRPC endpoint)
    ↓ (read/write)
Backend Storage (Redis or in-memory)
    ↓ (publish changes)
Pub/Sub Channel
    ↓ (notify subscribers)
All interested parties (Raylets, CoreWorkers)
```

#### 3.2.2 Raylet (Node Manager)
**Purpose**: Per-node task scheduling and resource management

**File References**:
- `/home/user/ray/src/ray/raylet/node_manager.h:133-150` (Main class)
- `/home/user/ray/src/ray/raylet/scheduling/cluster_resource_scheduler.h` (Scheduler)
- `/home/user/ray/src/ray/raylet/worker_pool.h` (Worker lifecycle)

**Responsibilities**:
- **Scheduling**: Determine which worker executes tasks
- **Resource Management**: Track available/used resources
- **Worker Pool**: Spawn and manage worker processes
- **Object Management**: Coordinate object placement
- **Local Execution**: Execute actor creation tasks locally

**Key Data Structures**:
```cpp
// From node_manager.h:70-125 NodeManagerConfig
ResourceSet resource_config;           // Node capabilities
int num_workers_soft_limit;            // Worker limits
WorkerCommandMap worker_commands;      // Language-specific commands
std::string store_socket_name;         // Plasma store socket
std::string raylet_config;             // Configuration override
```

#### 3.2.3 CoreWorker (Task Executor)
**Purpose**: Execute tasks and actors, manage object references

**File References**:
- `/home/user/ray/src/ray/core_worker/core_worker.h:62-150` (Main class)
- `/home/user/ray/src/ray/core_worker/task_submission/` (Task submission logic)
- `/home/user/ray/src/ray/core_worker/task_execution/` (Task execution)

**Responsibilities**:
- **Task Submission**: Submit tasks and actor method calls
- **Task Execution**: Execute received tasks/methods
- **Object Reference**: Track local object references
- **RPC Handling**: Handle incoming task/method calls
- **Dependency Resolution**: Wait for object dependencies

**Key Subcomponents**:
```
CoreWorker
├── NormalTaskSubmitter (submit regular tasks)
├── ActorTaskSubmitter (submit actor method calls)
├── TaskReceiver (receive and execute tasks)
├── ReferenceCounter (track object lifetime)
├── CoreWorkerMemoryStore (local object cache)
└── TaskEventBuffer (for observability)
```

#### 3.2.4 Object Store (Plasma)
**Purpose**: Distributed in-memory storage for objects

**File References**:
- `/home/user/ray/src/ray/core_worker/store_provider/plasma_store_provider.h`
- `/home/user/ray/src/ray/raylet/local_object_manager.h`

**Features**:
- **Shared Memory**: Direct memory access via mmap
- **Object Replication**: Across multiple nodes
- **Eviction**: LRU-based when memory constrained
- **Locality-Aware Scheduling**: Raylet prefers workers with data

**Access Pattern**:
```
Task Result → Plasma Store
    ↓
ObjectID registered with ReferenceCounter
    ↓
Caller ray.get(object_ref)
    ↓
CoreWorker fetches from Plasma (local or remote)
    ↓
Returns deserialized value to caller
```

### 3.3 Information Flow Example: Task Execution

```
┌────────────────────────────────────────────────────────────┐
│ CLIENT SIDE                                                │
├────────────────────────────────────────────────────────────┤
│ 1. client.remote_fn(arg)                                   │
│    → CoreWorker::SubmitTask                               │
│    → Creates TaskSpec with args/resources/function        │
│    → Returns ObjectRef immediately                        │
│                                                            │
│ 2. ray.get(object_ref)                                    │
│    → CoreWorker::GetObjects                               │
│    → Blocks until object available                        │
└────────────────────────────────────────────────────────────┘
                        ↓ (RPC: SubmitTask)
┌────────────────────────────────────────────────────────────┐
│ RAYLET (Scheduler)                                         │
├────────────────────────────────────────────────────────────┤
│ 3. NodeManager::HandleSubmitTask                          │
│    → ClusterResourceScheduler::Schedule                   │
│    → Allocate resources, select worker node               │
│    → Create lease request                                 │
└────────────────────────────────────────────────────────────┘
                        ↓ (RPC: PushTask)
┌────────────────────────────────────────────────────────────┐
│ REMOTE WORKER                                              │
├────────────────────────────────────────────────────────────┤
│ 4. CoreWorker::HandlePushTask                             │
│    → TaskReceiver::Receive                                │
│    → Wait for object dependencies                         │
│    → Execute function with arguments                      │
│    → Serialize results                                    │
│    → Store in local Plasma                                │
│    → Notify via ObjectInfoPublisher                       │
└────────────────────────────────────────────────────────────┘
                        ↓ (Pub/Sub notification)
┌────────────────────────────────────────────────────────────┐
│ CLIENT SIDE (continued)                                    │
├────────────────────────────────────────────────────────────┤
│ 5. ray.get() unblocks (object available)                  │
│    → Fetch from local/remote Plasma                       │
│    → Deserialize                                          │
│    → Return value                                         │
└────────────────────────────────────────────────────────────┘
```

---

## 4. State Management

Ray manages distributed state across multiple tiers:

### 4.1 Global State (GCS)

**File Reference**: `/home/user/ray/src/ray/gcs/gcs_actor_manager.h:48-100`

**Actor State Transitions**:
```
┌─────────────────────────────────────────────────────────────┐
│ GcsActorManager maintains actor state in GCS               │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  DEPENDENCIES_UNREADY (0)                                  │
│    - Actor dependencies not satisfied                      │
│    - Blocking initialization dependencies                  │
│                                                             │
│  → PENDING_CREATION (1)                                    │
│    - Actor creation task scheduled to raylet               │
│    - Waiting for worker lease                              │
│                                                             │
│  → ALIVE (2)                                               │
│    - Actor successfully created on a worker                │
│    - Ready for method invocations                          │
│    - Can transition to RESTARTING if worker dies           │
│                                                             │
│  → RESTARTING (3) or DEAD (5)                              │
│    - Worker died (node down, process killed, etc.)         │
│    - Check restart count limits                            │
│    - If restarts available: lineage recovery               │
│    - If no restarts left: DEAD state                       │
│                                                             │
│  DEAD (final state)                                        │
│    - Max restarts exceeded                                 │
│    - Actor removed from created_actors                     │
│    - References to dead actor will fail                    │
└─────────────────────────────────────────────────────────────┘
```

**State Persistence**:
```
GCS → Backend Storage (Redis)
   ↓
Pub/Sub channels notify:
   - Raylets of actor placement decisions
   - Workers of actor address changes
   - Clients of actor death/restart
```

### 4.2 Local State (CoreWorker)

**File Reference**: `/home/user/ray/src/ray/core_worker/reference_counter.h:42-100`

**Reference Counting for GC**:
```cpp
// From reference_counter.h
class ReferenceCounter {
  // Track object reference counts by owner
  void AddLocalReference(const ObjectID &object_id, const std::string &call_site);
  void RemoveLocalReference(const ObjectID &object_id, std::vector<ObjectID> *deleted);
  
  // Track task return values
  void UpdateFinishedTaskReferences(
      const std::vector<ObjectID> &return_ids,
      const std::vector<ObjectID> &argument_ids,
      bool release_lineage,
      const rpc::Address &worker_addr,
      const ReferenceTableProto &borrowed_refs,
      std::vector<ObjectID> *deleted);
};
```

**Object Lifetime**:
```
Task execution produces ObjectID
    ↓
ReferenceCounter tracks ownership
    ↓
Local references in Python:
   ref = ray.get(object)  // Add reference
   del ref               // Remove reference
    ↓
If ref_count == 0:
   → Publish deletion to pub/sub
   → Plasma store evicts object (if needed)
   → Lineage manager can reconstruct if needed
```

### 4.3 Lineage-Based Recovery

**Concept**: Objects can be reconstructed from task logs if lost

**State Stored**:
- Task specification (function, arguments)
- Task dependencies (input object IDs)
- Task results (output object IDs)

**Recovery Trigger**:
- Object needed but lost from cluster
- Raylet detects missing object
- Initiates task resubmission with same args
- Generates new object with original ObjectID

### 4.4 Concurrency Control

**Task Execution Concurrency** (from `/home/user/ray/src/ray/core_worker/task_execution/fiber.h`):
```cpp
class FiberRateLimiter {
  // Control concurrent task execution
  // Default: 1 concurrent task (single-threaded)
  // Configurable via actor options
};
```

**Actor Concurrency Groups**:
```python
@ray.remote
class MyActor:
    def __init__(self):
        pass
    
    # Default group: max_concurrency=1
    def method_a(self): ...
    
    # Custom group: max_concurrency=5
    @ray.method(concurrency_group="io_group")
    def method_b(self): ...
```

---

## 5. Error Handling Patterns

### 5.1 Exception Hierarchy

**File Reference**: `/home/user/ray/python/ray/exceptions.py:26-100`

```python
# From exceptions.py
class RayError(Exception):
    """Super class of all ray exception types."""
    
    def to_bytes(self):
        # Serialize exception with traceback
        # Pickle the exception object
        # Store formatted traceback
        return RayException(
            language=PYTHON,
            serialized_exception=pickle.dumps(self),
            formatted_exception_string=formatted_exception_string,
        ).SerializeToString()
    
    @staticmethod
    def from_bytes(b):
        ray_exception = RayException()
        ray_exception.ParseFromString(b)
        return RayError.from_ray_exception(ray_exception)
```

**Exception Types**:

| Exception | Cause | Handling |
|-----------|-------|----------|
| **RayTaskError** | Task execution failed | Raised on `ray.get()` |
| **RaySystemError** | Ray system internal error | System level issue |
| **RayActorError** | Actor crashed/died | Actor reference becomes invalid |
| **TaskCancelledError** | Task cancelled via `ray.cancel()` | Task interrupted |
| **ObjectStoreFull Error** | Plasma store memory exhausted | Eviction + retry |
| **CrossLanguageError** | Error from non-Python task | Wrapped with context |

### 5.2 Error Propagation Flow

```
┌──────────────────────────────────────────────────────┐
│ Task Execution on Remote Worker                      │
├──────────────────────────────────────────────────────┤
│ try:                                                 │
│     result = execute_user_function(args)            │
│ except Exception as e:                               │
│     # Serialize exception with traceback             │
│     serialized = RayTaskError(e).to_bytes()         │
│     # Store as "error object" in Plasma              │
│     store.put(ObjectID, serialized)                  │
│     # Notify caller via pub/sub                      │
└──────────────────────────────────────────────────────┘
                        ↓
┌──────────────────────────────────────────────────────┐
│ Caller-side (ray.get(object_ref))                    │
├──────────────────────────────────────────────────────┤
│ object_bytes = fetch_from_store(object_ref)         │
│ if is_error(object_bytes):                           │
│     error = RayError.from_bytes(object_bytes)       │
│     raise error  # Re-raise with original traceback │
│ else:                                                │
│     return deserialize(object_bytes)                │
└──────────────────────────────────────────────────────┘
```

### 5.3 Specific Error Handling Examples

#### Task Retry on Transient Failure
```python
# From actor.py and remote_function.py
@ray.remote(max_retries=3, retry_exceptions=[TimeoutError])
def flaky_function():
    pass
```

**Behavior**:
- Task fails with TimeoutError
- Raylet retries task on different worker
- After 3 failures: RayTaskError raised to caller

#### Actor Restart on Death
**File Reference**: `/home/user/ray/src/ray/gcs/gcs_actor_manager.h:70-92`

```
Actor dies (worker crashed):
    ↓
GCS detects via heartbeat timeout
    ↓
If max_restarts > 0:
   - Resubmit actor creation task
   - Update actor state to RESTARTING
   - Replace ObjectID references (lineage recovery)
    ↓
Else:
   - Mark actor DEAD
   - Future method calls fail with ActorDeadError
```

#### Object Recovery from Lineage
```
ray.get(object_ref) but object lost:
    ↓
ReferenceCounter detects missing object
    ↓
If lineage available:
   - Resubmit creating task
   - Obtain new ObjectID
   - Eventually object becomes available
    ↓
Else:
   - Raise ObjectLostError
```

### 5.4 Crash Recovery

**Fault Tolerance Strategy**:

| Component | Failure | Recovery |
|-----------|---------|----------|
| Worker crash | Task interrupted | Retry task on different worker |
| Actor death | Actor lost | Restart if max_restarts > 0 |
| Node down | All processes lost | Evacuate workload to other nodes |
| GCS down | Central state unavailable | GCS HA (standby GCS) |
| Object lost | Needed data missing | Lineage-based reconstruction |

---

## 6. Cross-Cutting Concerns

### 6.1 Logging Architecture

**File References**:
- `/home/user/ray/python/ray/_private/ray_logging/logging_config.py`
- `/home/user/ray/python/ray/__init__.py:8` (log initialization)
- `/home/user/ray/python/ray/exceptions.py:1` (logging import)

**Logging Configuration**:
```python
# From __init__.py:8
log.generate_logging_config()
logger = logging.getLogger(__name__)
```

**Key Features**:
- **Centralized Config**: Single LoggingConfig for all Ray components
- **Level Control**: Environment variables (RAY_LOG_LEVEL)
- **Driver Logging**: RAY_LOG_TO_DRIVER enables streaming logs to driver
- **Event Filtering**: RAY_LOG_TO_DRIVER_EVENT_LEVEL filters event severity
- **Deduplication**: stderr_deduplicator, stdout_deduplicator prevent spam

**Driver-Side Event Logging**:
```python
# From ray_constants.py:55-58
RAY_LOG_TO_DRIVER = env_bool("RAY_LOG_TO_DRIVER", True)
RAY_LOG_TO_DRIVER_EVENT_LEVEL = os.environ.get(
    "RAY_LOG_TO_DRIVER_EVENT_LEVEL", "INFO"
)
```

### 6.2 Metrics and Observability

**File References**:
- `/home/user/ray/src/ray/observability/` (metrics implementation)
- `/home/user/ray/src/ray/core_worker/core_worker.h:64-76` (task counters)

**Metric Types**:
```cpp
// From core_worker.h
class TaskCounter {
  enum class TaskStatusType { kPending, kRunning, kFinished };
  
  void IncPending(const std::string &func_name, bool is_retry);
  void MovePendingToRunning(const std::string &func_name, bool is_retry);
  void MoveRunningToFinished(const std::string &func_name, bool is_retry);
  void RecordMetrics();
};
```

**Key Metrics**:
- **Task Metrics**: Pending, running, finished counts by function
- **Actor Metrics**: State counts by actor name and job
- **Object Metrics**: Object count and size by ownership state
- **Scheduling Metrics**: Placement time, wait time histograms

**Integration Points**:
- OpenTelemetry collector
- Ray Dashboard
- Custom Prometheus endpoints

### 6.3 Configuration Management

**File Reference**: `/home/user/ray/src/ray/common/ray_config.h:60-80`

**Configuration Pattern**:
```cpp
class RayConfig {
#define RAY_CONFIG(type, name, default_value) \
private:                                        \
  type name##_ = ReadEnv<type>(                \
      "RAY_" #name, #type, default_value);    \
public:                                        \
  inline type &name() { return name##_; }

#include "ray/common/ray_config_def.h"
```

**Configuration Sources** (priority order):
1. Environment variables: `RAY_VARIABLE_NAME=value`
2. Code defaults: Compiled into binary
3. Runtime configuration via `RayConfig::instance()`

**Python Configuration** (from `/home/user/ray/python/ray/_private/ray_constants.py:11-52`):
```python
def env_integer(key, default):
    # Parse RAY_KEY from environment
    # Return default if not set or invalid
    
def env_bool(key, default):
    # Parse RAY_KEY as boolean
    
def env_float(key, default):
    # Parse RAY_KEY as float

# Example configurations:
AUTOSCALER_EVENTS = env_integer("RAY_SCHEDULER_EVENTS", 1)
RAY_LOG_TO_DRIVER = env_bool("RAY_LOG_TO_DRIVER", True)
```

**Key Tunable Parameters**:
- `RAY_memory`: Max object store memory
- `RAY_object_store_memory`: Plasma store size
- `RAY_num_cpus`: CPUs for scheduling
- `RAY_log_level`: Logging verbosity
- `RAY_system_monitor_interval_ms`: Health check frequency

### 6.4 Tracing and Profiling

**File References**:
- `/home/user/ray/python/ray/util/tracing/tracing_helper.py`
- `/home/user/ray/python/ray/actor.py:59-63` (tracing injection)

**Tracing Features**:
```python
# From actor.py:59-63
from ray.util.tracing.tracing_helper import (
    _inject_tracing_into_class,
    _tracing_actor_creation,
    _tracing_actor_method_invocation,
)
```

**Instrumentation Points**:
- Task submission (timing to scheduling)
- Task execution (duration, resource usage)
- Actor method calls (RPC latency, serialization)
- Object transfers (network throughput)

**Profiling Integration**:
- Wall-clock timing
- CPU profiling (py-spy support)
- Memory profiling
- Dashboard visualization

### 6.5 Serialization

**File Reference**: `/home/user/ray/python/ray/cloudpickle/`

**Serialization Strategy**:

| Data | Method | Notes |
|------|--------|-------|
| **Function code** | Cloudpickle | Captures closures, globals |
| **Arguments** | Cloudpickle | Object graph serialization |
| **Return values** | Cloudpickle | With error handling |
| **Exceptions** | Pickle + formatted traceback | Preserves context |
| **ObjectIDs** | Binary (no serialization) | Direct reference |

**Serialization Flow**:
```python
# Task submission
task_args_serialized = cloudpickle.dumps(args)
task_spec = TaskSpec(function_id, task_args_serialized, ...)

# Object storage
value = calculate()
serialized = cloudpickle.dumps(value)
object_store.put(object_id, serialized)

# Task execution
args_deserialized = cloudpickle.loads(task_args_serialized)
return_value = function(*args_deserialized)
```

**Special Handling**:
- **ObjectRefs**: Not serialized, passed as references
- **Large objects**: Stored in Plasma, referenced by ID
- **Tensor Transport**: Special optimization for torch/numpy (TensorTransportEnum)

---

## 7. Advanced Architecture Patterns

### 7.1 Task Submission Pattern

**File References**:
- `/home/user/ray/src/ray/core_worker/task_submission/normal_task_submitter.h:39-100`
- `/home/user/ray/src/ray/core_worker/task_submission/actor_task_submitter.h:40-100`

**Normal Task Submission Flow**:
```
Input: Function, args, resource requirements
    ↓
1. Dependency Resolution
   - Identify which arguments are ObjectRefs
   - Check if objects are available locally
   - If not available: schedule waiter coroutine
    ↓
2. Serialization
   - Serialize function and arguments
   - Compute function descriptor
   - Create TaskSpec protobuf
    ↓
3. Lease Request
   - Contact local raylet for worker lease
   - Provide resource and runtime environment requirements
   - Raylet consults ClusterResourceScheduler
    ↓
4. Task Submission
   - Once lease granted, RPC to assigned worker
   - Worker receives PushTask message
   - Worker queues task for execution
    ↓
Output: ObjectRef (future reference)
```

**Rate Limiting** (from `/home/user/ray/src/ray/core_worker/task_submission/normal_task_submitter.h:49-78`):
```cpp
// Control max concurrent lease requests per scheduling class
class LeaseRequestRateLimiter {
  virtual size_t GetMaxPendingLeaseRequestsPerSchedulingCategory() = 0;
};

// Static limit (default)
class StaticLeaseRequestRateLimiter : public LeaseRequestRateLimiter {
  explicit StaticLeaseRequestRateLimiter(size_t limit) : kLimit(limit) {}
};

// Dynamic limit based on cluster size
class ClusterSizeBasedLeaseRequestRateLimiter : public LeaseRequestRateLimiter {
  size_t GetMaxPendingLeaseRequestsPerSchedulingCategory() override {
    return max(num_nodes_in_cluster, min_concurrent_lease_limit);
  }
};
```

### 7.2 Actor Method Call Pattern

**File Reference**: `/home/user/ray/src/ray/core_worker/task_submission/actor_task_submitter.h:68-100`

**Mechanism**:
```
Actor method call (e.g., actor.method(arg))
    ↓
1. Address Resolution
   - Look up actor in ActorID → (node_id, address) map
   - Check if actor is alive (GCS subscription)
    ↓
2. Queue Management
   - Maintain per-actor task queue
   - Options:
     a) SequentialActorSubmitQueue: In-order execution
     b) OutOfOrderActorSubmitQueue: Allow concurrent methods
    ↓
3. Method Call Serialization
   - Serialize method name, arguments
   - Attach actor generation number (for restarts)
    ↓
4. Direct RPC to Actor
   - Skip raylet scheduling (actor is pinned)
   - Send PushActorTask directly to remote CoreWorker
    ↓
5. Response Handling
   - Receive return value or exception
   - Update queue state
   - Backpressure if queue exceeds max_pending_calls
    ↓
Output: ObjectRef to method return value
```

**Queue Strategy Pattern**:
```cpp
// From actor_submit_queue.h
class ActorSubmitQueue {
  virtual void Push(const ActorID &actor_id, TaskSpec &spec) = 0;
  virtual std::vector<TaskSpec> GetBatch() = 0;
};

// Sequential: One-at-a-time
class SequentialActorSubmitQueue : public ActorSubmitQueue {
  // Method B waits for Method A to complete
  // Maintains order but serializes execution
};

// Out-of-order: Concurrent execution
class OutOfOrderActorSubmitQueue : public ActorSubmitQueue {
  // Multiple methods can execute concurrently
  // But return values may arrive out-of-order
  // Caller must handle via futures
};
```

### 7.3 Task Execution Pattern

**File Reference**: `/home/user/ray/src/ray/core_worker/task_execution/task_receiver.h:44-80`

**Execution Flow**:
```cpp
// From task_receiver.h
class TaskReceiver {
  using TaskHandler = std::function<Status(
      const TaskSpecification &task_spec,
      std::optional<ResourceMappingType> resource_ids,
      std::vector<std::pair<ObjectID, std::shared_ptr<RayObject>>> *return_objects,
      ...
  )>;
};
```

**Steps**:
```
1. Receive Task
   - CoreWorker::HandlePushTask (RPC handler)
   - Deserialize TaskSpec and arguments
    ↓
2. Wait for Dependencies
   - Identify argument ObjectIDs
   - Contact object store for each
   - Block until all available
   - Handle timeouts (task fails)
    ↓
3. Execute Function
   - Deserialize function code
   - Deserialize arguments
   - Call: function(*args, **kwargs)
   - Handle exceptions
    ↓
4. Serialize Results
   - Serialize return value(s)
   - Create RayObject (metadata + data)
    ↓
5. Store Results
   - Put in local Plasma/memory store
   - Get ObjectIDs
   - Publish ObjectInfo via pub/sub
    ↓
6. Cleanup
   - Remove from executing tasks queue
   - Update reference counts
   - Release resources
```

**Scheduling Queues** (from `/home/user/ray/src/ray/core_worker/task_execution/`):
```
Normal tasks:
├─ NormalSchedulingQueue: FIFO + priority levels

Actor tasks:
├─ ActorSchedulingQueue (sequential)
├─ OutOfOrderActorSchedulingQueue (concurrent per concurrency group)
└─ ConcurrencyGroupManager: Manages thread pools per group
```

---

## 8. Key Architectural Decisions

### 8.1 Why Central GCS?
- **Problem**: Distributed systems need central coordination
- **Solution**: GCS (Redis-backed) as single source of truth
- **Tradeoff**: Scalability vs. high availability (mitigated with active-standby HA)

### 8.2 Why Separate Object Store?
- **Problem**: Sharing large objects between tasks is expensive
- **Solution**: Plasma store with mmap for zero-copy access
- **Tradeoff**: Object lifetime management adds complexity

### 8.3 Why Stateless Raylets?
- **Problem**: Raylet failure would lose task state
- **Solution**: GCS stores task state; Raylet is stateless
- **Benefit**: Raylet can be restarted without data loss

### 8.4 Why Reference Counting + Lineage?
- **Problem**: When to delete objects? (distributed GC is hard)
- **Solution**: Reference counting + lineage-based recovery
- **Benefit**: Objects can be reconstructed if lost

### 8.5 Why Pub/Sub Instead of Polling?
- **Problem**: Workers need to know actor locations, node failures, etc.
- **Solution**: Pub/Sub channels for state change notifications
- **Benefit**: Immediate propagation without polling overhead

### 8.6 Why Lease-Based Scheduling?
- **Problem**: Assign task to worker, but worker might be busy
- **Solution**: Two-phase: (1) Get lease, (2) Submit task
- **Benefit**: Can pipeline multiple tasks to same worker

---

## 9. Code Navigation Reference

### 9.1 Python API Entry Points

| API | File | Line Range |
|-----|------|-----------|
| `ray.init()` | `_private/worker.py` | ~200-400 |
| `@ray.remote` | `remote_function.py` | ~40-150 |
| `@ray.remote class` | `actor.py` | ~150-300 |
| `ray.get()` | `_private/worker.py` | GetFunction ~150-200 |
| `ray.put()` | `_private/worker.py` | PutFunction ~100-150 |
| `ray.wait()` | `_private/worker.py` | ~500-600 |

### 9.2 C++ Core Component Files

| Component | Primary File | Key Methods |
|-----------|-------------|-----------|
| GCS Actor Lifecycle | `src/ray/gcs/gcs_actor_manager.cc` | CreateActor, OnActorDeath |
| Task Scheduling | `src/ray/raylet/scheduling/cluster_resource_scheduler.cc` | Schedule |
| Task Execution | `src/ray/core_worker/core_worker.cc` | HandlePushTask |
| Object Management | `src/ray/core_worker/reference_counter.cc` | AddLocalReference, RemoveLocalReference |
| RPC Handling | `src/ray/core_worker/grpc_service.cc` | Handler implementations |

### 9.3 Key Files Summary

```
Ray Architecture Files:
├── python/ray/
│   ├── __init__.py              # Public API exports
│   ├── actor.py                 # Actor abstraction (102KB)
│   ├── remote_function.py       # Task abstraction
│   ├── exceptions.py            # Error hierarchy
│   └── _private/
│       ├── worker.py            # Worker lifecycle & APIs
│       └── ray_constants.py     # Configuration
├── src/ray/
│   ├── core_worker/
│   │   ├── core_worker.h/cc     # Main worker class (91KB header)
│   │   ├── reference_counter.h/cc # GC management
│   │   ├── task_submission/     # Task submission logic
│   │   │   ├── normal_task_submitter.h
│   │   │   └── actor_task_submitter.h
│   │   └── task_execution/      # Task execution logic
│   │       ├── task_receiver.h
│   │       └── scheduling_queue.h
│   ├── raylet/
│   │   ├── node_manager.h       # Per-node scheduler (main component)
│   │   ├── scheduling/
│   │   │   └── cluster_resource_scheduler.h
│   │   └── worker_pool.h        # Worker lifecycle
│   ├── gcs/
│   │   ├── gcs_actor_manager.h  # Global actor state
│   │   ├── gcs_job_manager.h
│   │   └── gcs_function_manager.h
│   ├── common/
│   │   ├── task/task_spec.h     # Task representation
│   │   ├── id.h                 # ID definitions
│   │   └── ray_config.h         # Configuration
│   ├── object_manager/          # Object store
│   ├── pubsub/                  # Pub/Sub system
│   └── observability/           # Metrics and tracing
└── src/ray/protobuf/           # RPC definitions
    └── common.proto             # Message definitions
```

---

## 10. Summary Table: Architecture Patterns

| Pattern | Purpose | Implementation | File |
|---------|---------|---|---|
| **Manager** | Lifecycle mgmt | Actor/Job/Worker managers | `gcs_actor_manager.h` |
| **Scheduler** | Resource allocation | ClusterResourceScheduler | `cluster_resource_scheduler.h` |
| **Service Handler** | RPC dispatch | CoreWorkerServiceHandler | `grpc_service.h` |
| **Queue** | Task buffering | SchedulingQueue (multiple impls) | `scheduling_queue.h` |
| **Pub/Sub** | Event distribution | GcsPublisher + subscribers | `gcs_publisher.h` |
| **Factory** | Object creation | CgroupManagerFactory | `cgroup_manager_factory.h` |
| **Strategy** | Pluggable behavior | LeasePolicy, SchedulingStrategy | `lease_policy.h` |
| **RPC Proxy** | Remote calls | ClientPool, Client interfaces | `core_worker_client_pool.h` |
| **Reference Counting** | Memory mgmt | ReferenceCounter | `reference_counter.h` |
| **State Machine** | State transitions | Actor states (ALIVE→RESTARTING→DEAD) | `gcs_actor_manager.h` |

---

## 11. Key Takeaways

1. **Layered Design**: Python→Cython→C++ separation enables flexibility and performance
2. **Distributed Coordination**: GCS + Pub/Sub provide loose coupling between components
3. **Task-Oriented**: Everything reduces to task scheduling and execution
4. **Reference-Based**: Objects are referenced, not moved, optimizing throughput
5. **Failure Recovery**: Lineage + reference counting enable fault tolerance
6. **Extensibility**: Manager pattern allows custom scheduling, storage, etc.
7. **Observable**: Metrics, logging, and tracing built into core
8. **Configurable**: Environment-based configuration for cluster customization

