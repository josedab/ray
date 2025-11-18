# Quick Start

> Write your first distributed application with Ray in 5 minutes.

## Overview

This guide walks you through creating your first Ray application. You'll learn how to parallelize Python functions and see immediate speedup on your local machine. Ray makes it easy to scale from a laptop to a cluster without changing your code.

## Prerequisites

- Ray installed (`pip install ray`)
- Python 3.8+
- Basic Python knowledge

## Quick Start

Run this complete example to see Ray in action:

```python
import ray
import time

# Initialize Ray
ray.init()

# Define a remote function
@ray.remote
def slow_function(x):
    time.sleep(1)  # Simulate work
    return x * x

# Run in parallel
start = time.time()
futures = [slow_function.remote(i) for i in range(4)]
results = ray.get(futures)
print(f"Results: {results}")
print(f"Time: {time.time() - start:.2f}s")  # ~1s instead of 4s

ray.shutdown()
```

## Detailed Guide

### Step 1: Initialize Ray

Start the Ray runtime:

```python
import ray

# Local mode (default)
ray.init()

# Or connect to a cluster
# ray.init(address="ray://cluster-address:10001")
```

### Step 2: Create Remote Functions (Tasks)

Transform any Python function into a distributed task using the `@ray.remote` decorator:

```python
@ray.remote
def compute(x):
    return x ** 2

# Call the function remotely
future = compute.remote(5)  # Returns immediately with ObjectRef

# Get the result when needed
result = ray.get(future)  # Blocks until result is ready
print(result)  # 25
```

### Step 3: Create Actors (Stateful Workers)

Actors maintain state across method calls:

```python
@ray.remote
class Counter:
    def __init__(self):
        self.value = 0

    def increment(self):
        self.value += 1
        return self.value

    def get(self):
        return self.value

# Create an actor instance
counter = Counter.remote()

# Call methods on the actor
counter.increment.remote()
counter.increment.remote()
result = ray.get(counter.get.remote())
print(result)  # 2
```

### Step 4: Process Data in Parallel

Ray Data provides scalable data processing:

```python
import ray

# Create a dataset
ds = ray.data.range(1000)

# Transform in parallel
result = ds.map(lambda x: x * 2).take(10)
print(result)
```

### Step 5: Train ML Models

Ray Train scales your training code:

```python
from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer

def train_func():
    # Your training code here
    pass

trainer = TorchTrainer(
    train_func,
    scaling_config=ScalingConfig(num_workers=4)
)
result = trainer.fit()
```

## Common Patterns

### Pattern: Parallel Map

**Use case:** Apply a function to many inputs in parallel

```python
@ray.remote
def process_item(item):
    # Process single item
    return item * 2

items = list(range(100))
futures = [process_item.remote(item) for item in items]
results = ray.get(futures)
```

### Pattern: Actor Pool

**Use case:** Maintain stateful workers for repeated operations

```python
from ray.util import ActorPool

@ray.remote
class Worker:
    def process(self, x):
        return x * 2

workers = [Worker.remote() for _ in range(4)]
pool = ActorPool(workers)

results = list(pool.map(lambda w, x: w.process.remote(x), range(100)))
```

### Pattern: Nested Parallelism

**Use case:** Parallelize tasks that spawn other tasks

```python
@ray.remote
def leaf_task(x):
    return x * x

@ray.remote
def parent_task(start, end):
    futures = [leaf_task.remote(i) for i in range(start, end)]
    return sum(ray.get(futures))

futures = [parent_task.remote(i*10, (i+1)*10) for i in range(10)]
total = sum(ray.get(futures))
```

## Troubleshooting

### Issue: Ray is Slow on First Run

**Symptoms:** First task takes several seconds to complete

**Cause:** Ray serializing the function and dependencies

**Solution:** This is normal. Subsequent calls will be faster.

### Issue: Out of Memory

**Symptoms:** `RayOutOfMemoryError` or system becomes unresponsive

**Cause:** Too many objects in memory or tasks using too much RAM

**Solution:**
```python
# Delete objects when done
del large_object

# Or use ray.put for large objects
large_data_ref = ray.put(large_data)
futures = [task.remote(large_data_ref) for _ in range(100)]
```

### Issue: Tasks Not Running in Parallel

**Symptoms:** Tasks run sequentially instead of in parallel

**Cause:** Not enough resources or calling `ray.get()` in a loop

**Solution:** Launch all tasks first, then collect results:
```python
# Bad: Sequential
for i in range(10):
    result = ray.get(task.remote(i))  # Blocks each iteration

# Good: Parallel
futures = [task.remote(i) for i in range(10)]
results = ray.get(futures)  # Blocks once
```

## What's Next

- [Core Concepts](core-concepts.md) - Understand tasks, actors, and objects
- [Ray Core User Guide](../user-guide/ray-core/tasks.md) - Deep dive into Ray Core
- [Learning Paths](learning-paths/) - Follow a guided path for your role

> **Related:** Scale your training with [Ray Train](../user-guide/ray-train/basics.md)
> **Related:** Process data at scale with [Ray Data](../user-guide/ray-data/basics.md)

## API Reference

- [`ray.init`](../reference/api/ray-core.md#ray-init) - Initialize Ray
- [`ray.remote`](../reference/api/ray-core.md#ray-remote) - Create remote functions and actors
- [`ray.get`](../reference/api/ray-core.md#ray-get) - Retrieve results
- [`ray.put`](../reference/api/ray-core.md#ray-put) - Store objects in object store
