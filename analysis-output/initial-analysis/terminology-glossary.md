# Ray Terminology Glossary

> **Commit:** `d1cce8c9dc8411fad7cfbd619350bec6f19839a3`

This glossary defines key terms used throughout the Ray codebase and documentation.

## Core Concepts

### Task
A stateless remote function invocation. Created by decorating a function with `@ray.remote` and calling it with `.remote()`.

```python
@ray.remote
def my_task(x):
    return x * 2

# Creates a task
result_ref = my_task.remote(5)
```

**Key files:** `python/ray/remote_function.py`

---

### Actor
A stateful computation unit. Created by decorating a class with `@ray.remote`. Maintains state between method calls.

```python
@ray.remote
class Counter:
    def __init__(self):
        self.value = 0

    def increment(self):
        self.value += 1
        return self.value
```

**Key files:** `python/ray/actor.py`

---

### ObjectRef
A reference to an object stored in Ray's distributed object store. Returned by `ray.put()` and task/actor method calls.

```python
ref = ray.put(data)  # Returns ObjectRef
result = ray.get(ref)  # Retrieves object
```

**Key files:** `python/ray/_raylet.pyx`

---

### Driver
The Python process that calls `ray.init()` and submits tasks. Typically the main program that orchestrates Ray workflows.

---

### Worker
A process that executes tasks and actor methods. Ray automatically starts workers on each node.

---

## Architecture Components

### GCS (Global Control Store)
Central metadata store for the Ray cluster. Stores:
- Actor locations and state
- Job information
- Node membership
- Placement group state

**Backed by:** Redis (default) or in-memory
**Key files:** `src/ray/gcs/`

---

### Raylet
Per-node daemon responsible for:
- Local task scheduling
- Resource management
- Worker process management
- Object transfer coordination

**Key files:** `src/ray/raylet/`

---

### Plasma (Object Store)
Distributed in-memory object store. Features:
- Shared memory (mmap) for zero-copy access
- LRU eviction
- Disk spilling for overflow

**Key files:** `src/ray/object_manager/plasma/`

---

### CoreWorker
C++ class that handles worker-side operations:
- Task submission and execution
- Object management
- Reference counting
- gRPC communication

**Key files:** `src/ray/core_worker/core_worker.cc`

---

### Head Node
The node running the GCS. In a cluster, one node is designated as the head node.

---

### Worker Node
Nodes that run Raylets and workers but not the GCS.

---

## Scheduling Concepts

### Resources
Logical or physical resources that tasks/actors can request:
- `num_cpus`: CPU cores
- `num_gpus`: GPU devices
- `memory`: Memory in bytes
- Custom resources: User-defined (e.g., `{"TPU": 1}`)

```python
@ray.remote(num_cpus=2, num_gpus=1)
def gpu_task():
    pass
```

---

### Placement Group
A way to reserve and co-locate resources across nodes. Used for gang scheduling.

```python
pg = ray.util.placement_group([{"CPU": 2}, {"CPU": 2}])
```

---

### Scheduling Policy
How Ray decides where to run tasks:
- **Hybrid:** Balance between locality and spreading (default)
- **Spread:** Distribute evenly across nodes
- **Pack:** Concentrate on fewer nodes

---

### Resource Lease
When a task runs, it "leases" resources from a Raylet until completion.

---

## Object Management

### Object Spilling
When object store memory is full, objects are written to disk to make room for new objects.

---

### Object Reconstruction (Lineage)
Ray can recreate lost objects by re-executing the tasks that produced them.

---

### Reference Counting
Ray tracks references to objects to determine when they can be garbage collected.

---

### Ownership
Each object has an owner (the worker that created it). The owner tracks the object's lifecycle.

---

## Ray Libraries

### Ray Data
Distributed data processing library. Key abstraction: `Dataset`.

```python
ds = ray.data.read_csv("s3://bucket/data.csv")
ds = ds.map(transform_fn)
```

---

### Ray Train
Distributed machine learning training. Key abstraction: `Trainer`.

```python
trainer = TorchTrainer(
    train_func,
    scaling_config=ScalingConfig(num_workers=4)
)
```

---

### Ray Serve
Model serving and inference. Key abstraction: `Deployment`.

```python
@serve.deployment
class MyModel:
    def __call__(self, request):
        return self.model.predict(request)
```

---

### Ray Tune
Hyperparameter tuning. Key abstraction: `Tuner`.

```python
tuner = Tuner(
    trainable,
    param_space={"lr": tune.loguniform(1e-4, 1e-1)}
)
```

---

### RLlib
Reinforcement learning library. Key abstraction: `Algorithm`.

```python
from ray.rllib.algorithms.ppo import PPOConfig
algo = PPOConfig().environment("CartPole-v1").build()
```

---

## Error Handling

### RayError
Base class for all Ray-specific exceptions.

---

### RayTaskError
Error that occurred during task execution. Contains the original exception and traceback.

---

### RayActorError
Error related to actor operations (creation, method calls, death).

---

### ObjectLostError
Raised when an object cannot be retrieved (lost and not reconstructable).

---

### RaySystemError
Internal Ray system error.

---

## Configuration

### RAY_* Environment Variables
Ray uses environment variables for configuration:
- `RAY_ADDRESS`: Cluster address
- `RAY_USE_TLS`: Enable TLS encryption
- `RAY_AUTH_MODE`: Authentication mode
- `RAY_OBJECT_STORE_MEMORY`: Object store size

---

### Runtime Environment
Package and environment configuration for tasks/actors:

```python
ray.init(runtime_env={
    "pip": ["pandas==1.3.0"],
    "env_vars": {"MY_VAR": "value"}
})
```

---

## Observability

### Ray Dashboard
Web UI for cluster monitoring at `http://<head-node>:8265`.

---

### State API
Programmatic access to cluster state:

```python
from ray.util.state import list_actors, list_tasks
actors = list_actors()
```

---

### Metrics
Prometheus-format metrics exposed at `/metrics` endpoint.

---

## Advanced Concepts

### Detached Actor
An actor that outlives its creator. Persists until explicitly killed.

```python
actor = MyActor.options(name="my_actor", lifetime="detached").remote()
```

---

### Actor Pool
A pool of actors for distributing work:

```python
from ray.util.actor_pool import ActorPool
pool = ActorPool([Actor.remote() for _ in range(4)])
```

---

### DAG (Directed Acyclic Graph)
Compiled task graphs for optimized execution:

```python
with ray.dag.InputNode() as inp:
    result = task.bind(inp)
dag = result.experimental_compile()
```

---

### Namespace
Isolation boundary for named actors and placement groups:

```python
ray.init(namespace="my_namespace")
```

---

### Job
A Ray application submission. Managed via Jobs API:

```bash
ray job submit --working-dir . -- python script.py
```

---

## Network & Communication

### gRPC
Google's RPC framework used for all Ray component communication.

---

### Object Transfer
Moving objects between nodes via Push (proactive) or Pull (on-demand) managers.

---

### Pub/Sub
Internal message distribution system for state updates.

---

## Abbreviations

| Abbreviation | Full Term |
|-------------|-----------|
| GCS | Global Control Store |
| OOM | Out of Memory |
| LRU | Least Recently Used |
| RPC | Remote Procedure Call |
| DAG | Directed Acyclic Graph |
| HA | High Availability |
| TLS | Transport Layer Security |

---

*This glossary provides definitions for terminology used throughout the Ray codebase. Refer to specific documentation for detailed usage.*
