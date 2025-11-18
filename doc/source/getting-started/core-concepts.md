# Core Concepts

> Understand the fundamental building blocks of Ray applications.

## Overview

Ray provides a simple yet powerful set of primitives for building distributed applications. This guide explains the core concepts you need to understand: tasks, actors, objects, and the Ray runtime. Mastering these concepts will help you design efficient distributed applications.

## Prerequisites

- Ray installed (`pip install ray`)
- Completed the [Quick Start](quick-start.md)
- Basic understanding of concurrency concepts

## Quick Start

See all core concepts in action:

```python
import ray

ray.init()

# Tasks: Stateless parallel functions
@ray.remote
def task(x):
    return x * 2

# Actors: Stateful distributed objects
@ray.remote
class Actor:
    def __init__(self):
        self.state = 0

    def update(self, x):
        self.state += x
        return self.state

# Objects: Distributed data
data_ref = ray.put([1, 2, 3, 4, 5])

# Use them together
task_result = ray.get(task.remote(5))
actor = Actor.remote()
actor_result = ray.get(actor.update.remote(10))
data = ray.get(data_ref)

print(f"Task result: {task_result}")
print(f"Actor result: {actor_result}")
print(f"Data: {data}")

ray.shutdown()
```

## Detailed Guide

### Tasks

Tasks are stateless functions that run remotely and in parallel.

#### Creating Tasks

```python
@ray.remote
def my_task(x, y):
    return x + y

# Invoke with .remote()
future = my_task.remote(1, 2)
result = ray.get(future)  # 3
```

#### Task Options

Configure resources and behavior:

```python
@ray.remote(num_cpus=2, num_gpus=1, memory=1000 * 1024 * 1024)
def gpu_task():
    # Uses 2 CPUs, 1 GPU, 1GB memory
    pass

# Override at call time
future = my_task.options(num_cpus=4).remote(1, 2)
```

#### Task Dependencies

Pass ObjectRefs to create task dependencies:

```python
@ray.remote
def task_a():
    return 1

@ray.remote
def task_b(x):
    return x + 1

# task_b waits for task_a automatically
a_ref = task_a.remote()
b_ref = task_b.remote(a_ref)  # Pass the reference, not the value
result = ray.get(b_ref)  # 2
```

### Actors

Actors are stateful workers that run in their own process.

#### Creating Actors

```python
@ray.remote
class Counter:
    def __init__(self, start=0):
        self.value = start

    def increment(self):
        self.value += 1
        return self.value

    def get_value(self):
        return self.value

# Create an actor instance
counter = Counter.remote(start=10)

# Call methods
ray.get(counter.increment.remote())  # 11
ray.get(counter.increment.remote())  # 12
ray.get(counter.get_value.remote())  # 12
```

#### Actor Options

Configure actor resources and lifecycle:

```python
@ray.remote(num_cpus=1, num_gpus=0.5)
class GPUActor:
    pass

# Named actors (accessible from other processes)
actor = Counter.options(name="global-counter").remote()

# Get existing actor by name
counter = ray.get_actor("global-counter")
```

#### Async Actors

For I/O-bound workloads:

```python
@ray.remote
class AsyncActor:
    async def fetch(self, url):
        import aiohttp
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                return await response.text()
```

### Objects

Objects are immutable values stored in Ray's distributed object store.

#### Storing Objects

```python
# Store explicitly with ray.put
large_data = list(range(10000))
data_ref = ray.put(large_data)

# Task return values are automatically stored
@ray.remote
def generate_data():
    return list(range(10000))

data_ref = generate_data.remote()
```

#### Passing Objects

Pass object references to avoid copies:

```python
# Efficient: Data transferred once
data_ref = ray.put(large_data)
futures = [task.remote(data_ref) for _ in range(100)]

# Inefficient: Data serialized 100 times
futures = [task.remote(large_data) for _ in range(100)]
```

#### Object Ownership and Lifetime

Objects are garbage collected when no references remain:

```python
# Object stays in memory while ref exists
data_ref = ray.put(data)

# Reference goes out of scope, object can be freed
del data_ref
```

### Ray Runtime

The Ray runtime manages all resources and coordination.

#### Initialization

```python
# Local mode
ray.init()

# Connect to existing cluster
ray.init(address="ray://cluster:10001")

# Configure resources
ray.init(
    num_cpus=8,
    num_gpus=2,
    object_store_memory=10 * 1024**3  # 10GB
)
```

#### Cluster Resources

```python
# View available resources
print(ray.cluster_resources())
# {'CPU': 8.0, 'GPU': 2.0, 'memory': 17179869184.0, ...}

# View currently available (not in use)
print(ray.available_resources())
```

#### Shutdown

```python
# Clean shutdown
ray.shutdown()
```

## Common Patterns

### Pattern: Task vs Actor

**Use case:** Choosing between tasks and actors

```python
# Use TASKS for:
# - Stateless computations
# - Embarrassingly parallel workloads
# - Short-lived operations

@ray.remote
def process(x):
    return x * 2

# Use ACTORS for:
# - Maintaining state
# - Loading models/data once
# - Managing resources

@ray.remote
class ModelServer:
    def __init__(self):
        self.model = load_model()  # Load once

    def predict(self, x):
        return self.model(x)
```

### Pattern: Efficient Data Sharing

**Use case:** Share data across many tasks

```python
# Put data once, reference many times
shared_data = ray.put(large_dataset)

@ray.remote
def process(data_ref, index):
    data = ray.get(data_ref)
    return data[index] * 2

futures = [process.remote(shared_data, i) for i in range(1000)]
results = ray.get(futures)
```

### Pattern: Pipeline

**Use case:** Chain tasks with dependencies

```python
@ray.remote
def stage_1(x):
    return x + 1

@ray.remote
def stage_2(x):
    return x * 2

@ray.remote
def stage_3(x):
    return x - 1

# Create pipeline
ref = stage_1.remote(10)
ref = stage_2.remote(ref)
ref = stage_3.remote(ref)

result = ray.get(ref)  # ((10 + 1) * 2) - 1 = 21
```

## Troubleshooting

### Issue: Actor Method Returns None

**Symptoms:** Actor method returns `None` instead of expected value

**Cause:** Missing return statement or calling `.remote()` incorrectly

**Solution:** Ensure you return values and use `ray.get()`:

```python
@ray.remote
class Counter:
    def increment(self):
        self.value += 1
        return self.value  # Don't forget return!

counter = Counter.remote()
result = ray.get(counter.increment.remote())  # Use ray.get()
```

### Issue: Object Not Found

**Symptoms:** `ObjectLostError` or reference not found

**Cause:** Object was garbage collected or worker died

**Solution:** Keep references alive or use lineage reconstruction:

```python
# Keep reference alive
data_refs = []
for i in range(100):
    ref = process.remote(i)
    data_refs.append(ref)  # Keep refs alive

results = ray.get(data_refs)
```

### Issue: Task Not Running

**Symptoms:** Task stuck in pending state

**Cause:** Insufficient resources or resource deadlock

**Solution:** Check resource requirements:

```python
# Check what's needed vs available
print(ray.available_resources())

# Reduce resource requirements
@ray.remote(num_cpus=1)  # Instead of default
def task():
    pass
```

## What's Next

- [Tasks Deep Dive](../user-guide/ray-core/tasks.md) - Advanced task patterns
- [Actors Deep Dive](../user-guide/ray-core/actors.md) - Advanced actor patterns
- [Objects Deep Dive](../user-guide/ray-core/objects.md) - Memory management
- [Learning Paths](learning-paths/) - Guided learning for your role

> **Related:** Learn about [Ray Data](../user-guide/ray-data/basics.md) for data processing
> **Related:** Learn about [Ray Train](../user-guide/ray-train/basics.md) for ML training

## API Reference

- [`ray.remote`](../reference/api/ray-core.md#ray-remote) - Decorator for tasks and actors
- [`ray.get`](../reference/api/ray-core.md#ray-get) - Get object values
- [`ray.put`](../reference/api/ray-core.md#ray-put) - Store objects
- [`ray.wait`](../reference/api/ray-core.md#ray-wait) - Wait for tasks
