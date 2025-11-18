# Tasks

> Execute stateless functions in parallel across your cluster.

## Overview

Tasks are the simplest way to parallelize work in Ray. By adding the `@ray.remote` decorator, any Python function can run as a distributed task. Tasks are ideal for stateless computations, embarrassingly parallel workloads, and building complex distributed workflows.

## Prerequisites

- Ray installed (`pip install ray`)
- Basic understanding of [Core Concepts](../../getting-started/core-concepts.md)
- Familiarity with Python decorators

## Quick Start

```python
import ray

ray.init()

@ray.remote
def compute(x):
    return x ** 2

# Launch tasks in parallel
futures = [compute.remote(i) for i in range(10)]
results = ray.get(futures)
print(results)  # [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]
```

## Detailed Guide

### Creating Tasks

Transform any Python function into a task:

```python
@ray.remote
def my_function(a, b):
    return a + b

# Call with .remote() to execute remotely
future = my_function.remote(1, 2)

# Get the result
result = ray.get(future)  # 3
```

### Task Options

Configure task resource requirements and behavior:

```python
@ray.remote(
    num_cpus=2,
    num_gpus=1,
    memory=1000 * 1024 * 1024,  # 1GB
    max_retries=3
)
def gpu_task(data):
    import torch
    return torch.tensor(data).cuda()
```

Override options at call time:

```python
@ray.remote
def flexible_task():
    pass

# Override resources for this specific call
future = flexible_task.options(num_cpus=4, num_gpus=2).remote()
```

### Passing Arguments

Pass Python objects as arguments:

```python
@ray.remote
def process(data, multiplier):
    return [x * multiplier for x in data]

# Python objects are serialized automatically
result = ray.get(process.remote([1, 2, 3], 2))  # [2, 4, 6]
```

Pass object references for efficiency:

```python
# Put large data in object store once
large_data = list(range(1000000))
data_ref = ray.put(large_data)

# Pass reference instead of data (no copy)
futures = [process.remote(data_ref, i) for i in range(10)]
```

### Task Dependencies

Create task graphs by passing ObjectRefs:

```python
@ray.remote
def step1(x):
    return x + 1

@ray.remote
def step2(x):
    return x * 2

@ray.remote
def step3(a, b):
    return a + b

# Build a task graph
a = step1.remote(1)
b = step2.remote(a)  # Depends on step1
c = step2.remote(10)
result = step3.remote(b, c)  # Depends on step2 outputs

print(ray.get(result))  # ((1+1)*2) + (10*2) = 24
```

### Waiting for Tasks

Use `ray.wait` to process results as they complete:

```python
import time
import random

@ray.remote
def slow_task(x):
    time.sleep(random.uniform(0.5, 2))
    return x

# Launch many tasks
futures = [slow_task.remote(i) for i in range(10)]

# Process as they complete
while futures:
    ready, futures = ray.wait(futures, num_returns=1)
    result = ray.get(ready[0])
    print(f"Got result: {result}")
```

### Cancelling Tasks

Cancel tasks that are no longer needed:

```python
@ray.remote
def long_task():
    import time
    time.sleep(100)
    return "done"

future = long_task.remote()

# Cancel the task
ray.cancel(future, force=False)

try:
    ray.get(future)
except ray.exceptions.TaskCancelledError:
    print("Task was cancelled")
```

### Nested Tasks

Tasks can spawn other tasks:

```python
@ray.remote
def leaf(x):
    return x * x

@ray.remote
def parent(start, end):
    # Spawn child tasks
    children = [leaf.remote(i) for i in range(start, end)]
    return sum(ray.get(children))

# Nested parallelism
result = ray.get(parent.remote(0, 100))
```

## Common Patterns

### Pattern: Parallel Map

**Use case:** Apply a function to many inputs

```python
@ray.remote
def process_item(item):
    # Do work
    return item * 2

items = list(range(1000))
results = ray.get([process_item.remote(x) for x in items])
```

### Pattern: Tree Reduction

**Use case:** Aggregate results efficiently

```python
import numpy as np

@ray.remote
def aggregate(results):
    return sum(results)

@ray.remote
def process(x):
    return x ** 2

# First level: process
level1 = [process.remote(i) for i in range(1000)]

# Second level: aggregate in batches
batch_size = 100
level2 = []
for i in range(0, len(level1), batch_size):
    batch = level1[i:i+batch_size]
    level2.append(aggregate.remote(batch))

# Final aggregation
final = aggregate.remote(level2)
print(ray.get(final))
```

### Pattern: Pipeline

**Use case:** Multi-stage processing

```python
@ray.remote
def extract(url):
    # Fetch data
    return f"data from {url}"

@ray.remote
def transform(data):
    return data.upper()

@ray.remote
def load(data):
    return f"Loaded: {data}"

urls = ["url1", "url2", "url3"]

# Build pipeline
extracted = [extract.remote(url) for url in urls]
transformed = [transform.remote(e) for e in extracted]
loaded = [load.remote(t) for t in transformed]

results = ray.get(loaded)
```

### Pattern: Rate Limiting

**Use case:** Limit concurrent tasks

```python
from ray.util import ActorPool

@ray.remote
class Worker:
    def process(self, x):
        # Do work
        return x * 2

# Create limited pool
num_workers = 4
workers = [Worker.remote() for _ in range(num_workers)]
pool = ActorPool(workers)

# Process with limited concurrency
items = list(range(100))
results = list(pool.map(lambda w, x: w.process.remote(x), items))
```

## Troubleshooting

### Issue: Tasks Not Running in Parallel

**Symptoms:** Tasks execute sequentially instead of in parallel

**Cause:** Using `ray.get()` immediately after each `remote()` call

**Solution:** Launch all tasks first, then collect results:

```python
# Bad: Sequential execution
for i in range(10):
    result = ray.get(task.remote(i))  # Blocks!

# Good: Parallel execution
futures = [task.remote(i) for i in range(10)]  # Launch all
results = ray.get(futures)  # Block once
```

### Issue: Serialization Errors

**Symptoms:** `TypeError: cannot pickle 'X' object`

**Cause:** Trying to pass non-serializable objects

**Solution:** Use serializable types or custom serialization:

```python
# Bad: Lambda can't be pickled
ray.get(task.remote(lambda x: x * 2))

# Good: Define named function
def multiply(x):
    return x * 2

ray.get(task.remote(multiply))
```

### Issue: Memory Growing Unbounded

**Symptoms:** Worker memory keeps increasing

**Cause:** Holding onto many object references

**Solution:** Delete references when done:

```python
# Process in batches
for batch_start in range(0, 10000, 100):
    futures = [task.remote(i) for i in range(batch_start, batch_start + 100)]
    results = ray.get(futures)
    # Process results...
    # futures go out of scope, memory freed
```

### Issue: Task Not Using GPU

**Symptoms:** Task runs on CPU despite requesting GPU

**Cause:** Resources not properly specified or no GPUs available

**Solution:** Check GPU availability and specification:

```python
# Check available GPUs
print(ray.available_resources())

# Correctly request GPU
@ray.remote(num_gpus=1)
def gpu_task():
    import torch
    return torch.cuda.is_available()
```

## What's Next

- [Actors](actors.md) - Stateful distributed objects
- [Objects](objects.md) - Understanding the object store
- [Patterns](patterns/) - Advanced task patterns

> **Related:** Process large datasets with [Ray Data](../ray-data/basics.md)
> **Related:** Distribute training with [Ray Train](../ray-train/basics.md)

## API Reference

- [`ray.remote`](../../reference/api/ray-core.md#ray-remote) - Create tasks and actors
- [`ray.get`](../../reference/api/ray-core.md#ray-get) - Retrieve results
- [`ray.wait`](../../reference/api/ray-core.md#ray-wait) - Wait for tasks
- [`ray.cancel`](../../reference/api/ray-core.md#ray-cancel) - Cancel tasks
