# Part 3: Patterns and Practices in Ray

> **Series:** Ray Deep Dive | **Reading Time:** 14 minutes
> **Commit:** `d1cce8c9dc8411fad7cfbd619350bec6f19839a3`

## What You'll Learn

- Design patterns employed throughout Ray
- Code organization strategies
- Testing approaches and best practices
- Error handling and resilience patterns

## Introduction

Ray's codebase demonstrates thoughtful software engineering across 1.1 million lines of code. In this post, we'll extract the patterns and practices that make Ray maintainable and reliable. Whether you're contributing to Ray or building distributed applications, these patterns provide valuable guidance.

## Design Patterns in Ray

### Manager Pattern

Ray uses the Manager pattern extensively to centralize lifecycle management for complex entities.

**Example: GcsActorManager**

```cpp
// src/ray/gcs/gcs_actor_manager.h

class GcsActorManager {
 public:
  // Create and register actor
  Status RegisterActor(const ActorTableData &actor);

  // Handle state transitions
  void OnActorCreationSuccess(const ActorID &actor_id);
  void OnActorCreationFailed(const ActorID &actor_id);
  void OnWorkerDead(const WorkerID &worker_id);

  // Query state
  std::optional<ActorTableData> GetActor(const ActorID &actor_id);

 private:
  // Centralized actor registry
  absl::flat_hash_map<ActorID, std::shared_ptr<GcsActor>> actors_;

  // State machine transitions
  void TransitionActorState(const ActorID &actor_id, ActorState new_state);
};
```

**Why this pattern?**
- Single source of truth for actor state
- Encapsulates complex state machine logic
- Easier testing through mocking

**Other examples:**
- `TaskManager` in CoreWorker
- `WorkerPool` in Raylet
- `PlacementGroupManager` in GCS

### Strategy Pattern

The Strategy pattern enables pluggable algorithms, particularly for scheduling.

```cpp
// src/ray/raylet/scheduling/policy/scheduling_policy.h

class ISchedulingPolicy {
 public:
  virtual NodeID Schedule(
      const ResourceRequest &request,
      const ClusterResourceManager &cluster_resources) = 0;
};

// Concrete strategies
class HybridSchedulingPolicy : public ISchedulingPolicy { ... };
class SpreadSchedulingPolicy : public ISchedulingPolicy { ... };
class PackSchedulingPolicy : public ISchedulingPolicy { ... };
```

**Usage:**

```cpp
// src/ray/raylet/scheduling/cluster_resource_scheduler.cc

NodeID ClusterResourceScheduler::GetBestSchedulableNode(
    const ResourceRequest &request) {
  // Policy is injected at construction
  return scheduling_policy_->Schedule(request, cluster_resources_);
}
```

**Benefits:**
- Easy to add new scheduling algorithms
- Testing different policies independently
- Runtime policy selection

### Publisher/Subscriber Pattern

Ray's pub/sub system distributes state updates efficiently.

```cpp
// src/ray/pubsub/publisher.h

class Publisher {
 public:
  // Publish to channel
  void Publish(const std::string &channel, const std::string &message);

  // Register subscriber
  void RegisterSubscription(
      const std::string &channel,
      const SubscriberID &subscriber_id,
      std::function<void(const std::string &)> callback);
};

// src/ray/pubsub/subscriber.h

class Subscriber {
 public:
  void Subscribe(
      const std::string &channel,
      std::function<void(const std::string &)> callback);
};
```

**Use cases in Ray:**
- Actor state changes
- Node membership updates
- Object location broadcasts

### Factory Pattern

Ray uses factories for creating complex objects with various configurations.

```python
# python/ray/train/trainer.py

class TrainerFactory:
    @staticmethod
    def create(
        backend: str,
        **kwargs
    ) -> Trainer:
        if backend == "torch":
            return TorchTrainer(**kwargs)
        elif backend == "tensorflow":
            return TensorflowTrainer(**kwargs)
        elif backend == "horovod":
            return HorovodTrainer(**kwargs)
        else:
            raise ValueError(f"Unknown backend: {backend}")
```

### Proxy Pattern

The Cython binding layer acts as a proxy between Python and C++:

```cython
# python/ray/_raylet.pyx

cdef class CoreWorker:
    cdef CCoreWorker *core_worker

    def submit_task(self, function_descriptor, args, ...):
        # Proxy: Convert Python to C++, call C++, convert back
        cdef CTaskSpec task_spec = self._create_task_spec(...)

        with nogil:
            status = self.core_worker.SubmitTask(task_spec)

        if not status.ok():
            raise RayError(status.message())

        return self._create_object_refs(task_spec.ReturnIds())
```

## Code Organization Strategies

### Module Structure

Ray organizes code by concern with clear boundaries:

```
python/ray/
├── __init__.py          # Public API (curated exports)
├── _private/            # Internal implementation
│   ├── worker.py        # Core worker logic
│   ├── serialization.py # Serialization utilities
│   └── ...
├── data/                # Ray Data library
├── train/               # Ray Train library
├── serve/               # Ray Serve library
└── tune/                # Ray Tune library
```

**Key principles:**
1. **Public vs. Private:** `_private/` for implementation details
2. **Library independence:** Each library can be used standalone
3. **Shared utilities:** Common code in `_private/`

### API Surface Control

Ray carefully controls its public API:

```python
# python/ray/__init__.py

# Explicit exports
from ray._raylet import ObjectRef
from ray.actor import ActorClass
from ray.remote_function import RemoteFunction

# Auto-init pattern
from ray._private.auto_init_hook import wrap_auto_init
get = wrap_auto_init(get)  # Auto-calls ray.init() if needed
put = wrap_auto_init(put)

# Version and compatibility
__version__ = "2.10.0"
__commit__ = "d1cce8c9dc8411fad7cfbd619350bec6f19839a3"
```

### Configuration Management

Ray uses environment variables with a central configuration system:

```cpp
// src/ray/common/ray_config_def.h

// Define all configuration with defaults and documentation
RAY_CONFIG(int64_t, object_store_memory,
           static_cast<int64_t>(0.3 * GetSystemMemory()),
           "Memory allocated to the object store")

RAY_CONFIG(int64_t, task_retry_delay_ms, 0,
           "Delay before retrying a failed task")

RAY_CONFIG(double, scheduler_spread_threshold, 0.5,
           "Threshold for hybrid scheduling policy")
```

**Accessing configuration:**

```cpp
// Usage in C++
int64_t memory = RayConfig::instance().object_store_memory();

// Set via environment variable
// RAY_object_store_memory=1000000000
```

```python
# Usage in Python
ray.init(_system_config={"object_store_memory": 10**9})
```

## Testing Strategies

Ray's test suite demonstrates mature testing practices.

### Test Organization

```
python/ray/tests/
├── conftest.py          # Shared fixtures
├── test_basic.py        # Core functionality
├── test_actor.py        # Actor tests
├── test_failure.py      # Fault tolerance
├── unit/                # Unit tests
└── ...

src/ray/core_worker/tests/
├── reference_counter_test.cc
├── task_manager_test.cc
└── ...
```

### Fixtures for Ray Cluster

```python
# python/ray/tests/conftest.py

@pytest.fixture
def ray_start_regular():
    """Start a small Ray cluster for testing."""
    ray.init(num_cpus=2)
    yield
    ray.shutdown()

@pytest.fixture
def ray_start_cluster():
    """Start a multi-node cluster."""
    cluster = Cluster()
    cluster.add_node(num_cpus=2)
    cluster.add_node(num_cpus=2)
    ray.init(address=cluster.address)
    yield cluster
    ray.shutdown()
    cluster.shutdown()
```

### Testing Distributed Behavior

```python
# python/ray/tests/test_failure.py

def test_actor_restart(ray_start_regular):
    """Test that actors restart on failure."""

    @ray.remote(max_restarts=1)
    class Counter:
        def __init__(self):
            self.value = 0

        def increment(self):
            self.value += 1
            return self.value

        def crash(self):
            os._exit(1)  # Simulate crash

    counter = Counter.remote()

    # Normal operation
    assert ray.get(counter.increment.remote()) == 1

    # Crash the actor
    counter.crash.remote()

    # Actor should restart and state should reset
    time.sleep(1)
    assert ray.get(counter.increment.remote()) == 1  # Reset to 0, then incremented
```

### Testing C++ Components

```cpp
// src/ray/core_worker/tests/reference_counter_test.cc

class ReferenceCounterTest : public ::testing::Test {
 protected:
  void SetUp() override {
    reference_counter_ = std::make_unique<ReferenceCounter>();
  }

  std::unique_ptr<ReferenceCounter> reference_counter_;
};

TEST_F(ReferenceCounterTest, AddRemoveReference) {
  ObjectID object_id = ObjectID::FromRandom();

  // Add reference
  reference_counter_->AddLocalReference(object_id);
  EXPECT_TRUE(reference_counter_->HasReference(object_id));

  // Remove reference
  reference_counter_->RemoveLocalReference(object_id);
  EXPECT_FALSE(reference_counter_->HasReference(object_id));
}
```

### Testing Best Practices in Ray

1. **Deterministic tests:** Avoid timing-dependent assertions when possible
2. **Isolation:** Each test starts with fresh Ray instance
3. **Tagging:** Tests tagged by team for parallel execution
4. **Retry flaky tests:** `pytest-rerunfailures` for network-dependent tests

```python
# pytest.ini
[pytest]
filterwarnings = error
timeout = 180

# Markers for categorization
markers =
    slow: marks tests as slow
    gpu: requires GPU
    client: Ray client tests
```

## Error Handling Patterns

### Exception Hierarchy

Ray defines a clear exception hierarchy:

```python
# python/ray/exceptions.py

class RayError(Exception):
    """Base class for Ray exceptions."""
    pass

class RayTaskError(RayError):
    """Error from task execution."""
    def __init__(self, function_name, traceback_str, cause):
        self.function_name = function_name
        self.traceback_str = traceback_str
        self.cause = cause

class RayActorError(RayError):
    """Error from actor operation."""
    pass

class ObjectLostError(RayError):
    """Object cannot be retrieved."""
    pass
```

### Error Propagation

Errors propagate through ObjectRefs:

```python
@ray.remote
def failing_task():
    raise ValueError("Something went wrong")

ref = failing_task.remote()

try:
    result = ray.get(ref)
except ray.exceptions.RayTaskError as e:
    print(f"Task failed: {e.cause}")
    print(f"Traceback:\n{e.traceback_str}")
```

**Implementation:**

```cpp
// src/ray/core_worker/core_worker.cc

void CoreWorker::HandleTaskFailure(
    const TaskID &task_id,
    const Status &error) {

  // Serialize error as special object
  auto error_object = CreateErrorObject(error);

  // Store in object store (replaces result)
  for (const auto &return_id : task_spec.ReturnIds()) {
    plasma_store_->Put(return_id, error_object);
  }
}
```

### Retry Patterns

```python
# Built-in retry
@ray.remote(max_retries=3, retry_exceptions=[ConnectionError])
def network_task():
    return fetch_from_api()

# Custom retry with backoff
def with_retry(func, max_attempts=3, backoff=1.0):
    for attempt in range(max_attempts):
        try:
            return ray.get(func.remote())
        except RayTaskError as e:
            if attempt == max_attempts - 1:
                raise
            time.sleep(backoff * (2 ** attempt))
```

### Graceful Degradation

```python
@ray.remote
class ResilientService:
    def __init__(self):
        self.cache = {}

    def get_data(self, key):
        try:
            # Try primary source
            return self.fetch_from_db(key)
        except Exception:
            # Fall back to cache
            if key in self.cache:
                return self.cache[key]
            raise
```

## Resilience Patterns

### Circuit Breaker

```python
@ray.remote
class CircuitBreaker:
    def __init__(self, failure_threshold=5, reset_timeout=60):
        self.failure_count = 0
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.last_failure_time = 0
        self.state = "CLOSED"

    def call(self, func, *args):
        if self.state == "OPEN":
            if time.time() - self.last_failure_time > self.reset_timeout:
                self.state = "HALF_OPEN"
            else:
                raise Exception("Circuit breaker is OPEN")

        try:
            result = func(*args)
            if self.state == "HALF_OPEN":
                self.state = "CLOSED"
                self.failure_count = 0
            return result
        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.failure_count >= self.failure_threshold:
                self.state = "OPEN"
            raise
```

### Bulkhead Pattern

Isolate failures using separate actors:

```python
# Each service is isolated
db_service = DatabaseService.remote()
cache_service = CacheService.remote()
api_service = APIService.remote()

# Failure in one doesn't affect others
try:
    db_result = ray.get(db_service.query.remote(q))
except Exception:
    # DB failed, but cache and API still work
    cache_result = ray.get(cache_service.get.remote(key))
```

### Timeout Patterns

```python
# Task-level timeout
@ray.remote
def long_task():
    time.sleep(100)
    return "done"

# Using ray.wait for timeout
ready, not_ready = ray.wait(
    [long_task.remote()],
    timeout=5.0
)

if not_ready:
    print("Task timed out")
    ray.cancel(not_ready[0])
```

## Documentation Patterns

### Docstring Standards

Ray uses Google-style docstrings:

```python
def ray_get(
    object_refs: Union[ObjectRef, List[ObjectRef]],
    timeout: Optional[float] = None
) -> Any:
    """Get the value of one or more object references.

    This method blocks until the objects are available locally
    or the timeout expires.

    Args:
        object_refs: Object reference(s) to retrieve.
        timeout: Maximum time to wait in seconds. None means
            wait indefinitely.

    Returns:
        The Python object(s) corresponding to the references.

    Raises:
        RayTaskError: If the task that created the object failed.
        ObjectLostError: If the object was lost and cannot be
            reconstructed.
        GetTimeoutError: If timeout is specified and exceeded.

    Examples:
        >>> ref = ray.put(42)
        >>> ray.get(ref)
        42

        >>> refs = [ray.put(i) for i in range(3)]
        >>> ray.get(refs)
        [0, 1, 2]
    """
```

### API Annotations

```python
# python/ray/_private/api_annotations.py

@PublicAPI
def init(address: Optional[str] = None, ...) -> None:
    """Initialize Ray."""
    pass

@DeveloperAPI
def get_actor_handle(actor_id: ActorID) -> ActorHandle:
    """Get handle to existing actor. For advanced use."""
    pass

@Deprecated("Use ray.train.Trainer instead")
def legacy_train(config):
    pass
```

## Key Takeaways

1. **Manager pattern** centralizes lifecycle and state management
2. **Strategy pattern** enables pluggable algorithms (especially scheduling)
3. **Pub/Sub** efficiently distributes state updates
4. **Clear module boundaries** with public/private separation
5. **Comprehensive testing** with fixtures for distributed behavior
6. **Rich exception hierarchy** with proper error propagation
7. **Resilience patterns** (retry, circuit breaker, bulkhead) for fault tolerance

## What's Next

In [Part 4](./04-extending-integrating.md), we'll explore:
- Ray's extension points and plugin architecture
- How Ray libraries (Data, Train, Serve, Tune) integrate
- Building custom serializers and schedulers

## Code References

- Exception hierarchy: [`python/ray/exceptions.py`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/python/ray/exceptions.py)
- Test fixtures: [`python/ray/tests/conftest.py`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/python/ray/tests/conftest.py)
- Scheduling policy: [`src/ray/raylet/scheduling/policy/`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/src/ray/raylet/scheduling/policy/)
- Configuration: [`src/ray/common/ray_config_def.h`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/src/ray/common/ray_config_def.h)

---

*Next: [Part 4 - Extending and Integrating Ray](./04-extending-integrating.md)*
