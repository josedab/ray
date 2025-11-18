# Ray Data Flow and Public APIs Analysis

**Generated:** 2025-11-18  
**Repository:** Ray v2.x  
**Focus:** Python API Surface and Core Architecture

---

## Table of Contents
1. [Public API Surface](#public-api-surface)
2. [Data Flow & Task Submission](#data-flow--task-submission)
3. [Distributed Object Store](#distributed-object-store)
4. [Serialization Mechanisms](#serialization-mechanisms)
5. [Main Ray Libraries](#main-ray-libraries)
6. [Architecture Summary](#architecture-summary)

---

## 1. Public API Surface

### Main Entry Points

Ray exposes a clean, pythonic API with the following core functions and decorators:

#### **Core Decorators and Functions**

| API | Purpose | Location |
|-----|---------|----------|
| `@ray.remote` | Decorator to make functions or classes remote | `/home/user/ray/python/ray/_private/worker.py:3465+` |
| `ray.init()` | Initialize Ray cluster/connection | `/home/user/ray/python/ray/_private/worker.py:1432+` |
| `ray.get()` | Retrieve object from object store | `/home/user/ray/python/ray/_private/worker.py:2831+` |
| `ray.put()` | Store object in object store | `/home/user/ray/python/ray/_private/worker.py:3010+` |
| `ray.wait()` | Wait for one or more tasks to complete | `/home/user/ray/python/ray/_private/worker.py:3079+` |
| `ray.cancel()` | Cancel a running task | `/home/user/ray/python/ray/_private/worker.py` |
| `ray.shutdown()` | Disconnect from Ray cluster | `/home/user/ray/python/ray/_private/worker.py` |
| `ray.get_actor()` | Get reference to named actor | `/home/user/ray/python/ray/_private/worker.py:3224+` |
| `ray.method` | Decorator for actor methods | `/home/user/ray/python/ray/actor.py` |

**Source File:** `/home/user/ray/python/ray/__init__.py`

```python
# Define public API exports
__all__ = [
    "get",
    "put",
    "get_actor",
    "init",
    "is_initialized",
    "kill",
    "remote",
    "shutdown",
    "wait",
    "cancel",
    # ... plus 20+ others
]
```

### Auto-Init APIs

Ray automatically initializes the cluster if not already done:
```python
AUTO_INIT_APIS = {
    "cancel", "get", "get_actor", "get_gpu_ids",
    "kill", "put", "wait", "get_runtime_context",
}
```

### Non-Auto-Init APIs

These require explicit `ray.init()`:
```python
NON_AUTO_INIT_APIS = {
    "init", "shutdown", "remote", "is_initialized",
    "ClientBuilder", "Language", # ... etc
}
```

---

## 2. Data Flow & Task Submission

### High-Level Task Submission Pipeline

#### **Step 1: Decorator Application**

```python
@ray.remote(num_cpus=1, num_gpus=1, max_retries=3)
def process_data(x):
    return x * 2
```

**Result:** Creates a `RemoteFunction` object  
**File:** `/home/user/ray/python/ray/remote_function.py:41`

```python
@PublicAPI
class RemoteFunction:
    """A remote function.
    
    Attributes:
        _language: The target language (PYTHON, JAVA, etc.)
        _function: The original function
        _num_cpus, _num_gpus, _memory: Resource requirements
        _runtime_env: Runtime environment for task
        _max_retries: Retry configuration
        _scheduling_strategy: How to schedule task
    """
```

#### **Step 2: Remote Invocation**

```python
object_ref = process_data.remote(10)  # Returns ObjectRef[int]
```

**File:** `/home/user/ray/python/ray/remote_function.py:314+` (the `_remote` method)

Key sequence:
1. **Serialize the function** (if first invocation or cluster changed)
   ```python
   self._pickled_function = pickle_dumps(
       self._function,
       f"Could not serialize the function {self._descriptor}"
   )
   ```

2. **Export function to all workers**
   ```python
   worker.function_actor_manager.export(self)
   ```

3. **Flatten arguments** (handle ObjectRef dependencies)
   ```python
   list_args = ray._common.signature.flatten_args(
       self._function_signature, args, kwargs
   )
   ```

4. **Submit task to core worker**
   ```python
   object_refs = worker.core_worker.submit_task(
       self._language,                    # PYTHON
       self._function_descriptor,         # Function metadata
       list_args,                         # Serialized arguments
       name if name is not None else "",  # Task name
       num_returns,                       # Number of return values (1 by default)
       resources,                         # CPU/GPU/custom resources
       max_retries,                       # Retry count
       retry_exceptions,                  # Retry on which exceptions
       # ... scheduling, placement group, etc.
   )
   ```

### Submission Flow Diagram

```
User Code (@ray.remote decorator)
    ↓
RemoteFunction._remote() method
    ↓
[Serialize function + flatten args]
    ↓
worker.core_worker.submit_task()
    ↓
[C++/Cython RayletClient]
    ↓
Graylet (Scheduler)
    ↓
[Task assigned to worker/actor]
    ↓
Task executed, results stored in object store
    ↓
ObjectRef returned to user
```

### Actor Tasks

For actor methods, similar flow but with actor instantiation:

```python
@ray.remote
class Counter:
    def __init__(self):
        self.count = 0
    
    def increment(self):
        self.count += 1
        return self.count

# Actor creation
counter = Counter.remote()  # Returns ActorHandle

# Method invocation
ref = counter.increment.remote()  # Returns ObjectRef[int]
result = ray.get(ref)  # Get the result
```

**File:** `/home/user/ray/python/ray/actor.py:1+`

---

## 3. Distributed Object Store

### Object Storage Architecture

Ray uses **Plasma object store** for distributed data management:

- **Location:** `/home/user/ray/cpp/src/ray/runtime/object/object_store.*`
- **Python Interface:** `/home/user/ray/python/ray/_private/worker.py`

#### **How Objects Flow**

1. **Put an object**
   ```python
   obj = {"data": [1,2,3,4,5] * 1000}
   ref = ray.put(obj)  # Serializes and stores in local object store
   ```
   
   **File:** `/home/user/ray/python/ray/_private/worker.py:3010+`
   
   Implementation:
   ```python
   def put(
       value: Any,
       *,
       _owner: Optional["ray.actor.ActorHandle"] = None,
       _tensor_transport: str = "object_store",
   ) -> "ray.ObjectRef":
       """Store an object in the object store."""
       worker = global_worker
       worker.check_connected()
       
       with profiling.profile("ray.put"):
           try:
               object_ref = worker.put_object(
                   value,
                   owner_address=serialize_owner_address,
                   _tensor_transport=_tensor_transport,
               )
           except ObjectStoreFullError:
               logger.info("Put failed since the value was too large...")
               raise
           return object_ref
   ```

2. **Get an object**
   ```python
   result = ray.get(ref)  # Retrieves from object store
   results = ray.get([ref1, ref2, ref3])  # Batch get
   ```
   
   **File:** `/home/user/ray/python/ray/_private/worker.py:2861+`
   
   Features:
   - Blocks until object is available
   - Auto-fetches from remote object stores
   - Preserves ordering for list inputs
   - Optional timeout parameter
   
   ```python
   def get(
       object_refs: Union[ObjectRef[Any], Sequence[ObjectRef[Any]], ...],
       *,
       timeout: Optional[float] = None,
       _tensor_transport: Optional[str] = None,
   ) -> Union[Any, List[Any]]:
       """Get a remote object or list of remote objects from the object store.
       
       This method blocks until the object is available in the local object
       store. If this object is not in the local store, it will be shipped
       from an object store that has it.
       """
   ```

3. **Wait for completion**
   ```python
   ready, not_ready = ray.wait([ref1, ref2, ref3], num_returns=2, timeout=10)
   ```
   
   **File:** `/home/user/ray/python/ray/_private/worker.py:3079+`

### Object Metadata

Each object in the object store has:
- **ObjectRef (ID):** 20-byte binary identifier
- **Size:** bytes used
- **Metadata:** task that created it, dependencies
- **Status:** in-memory, spilled, lost, etc.

**ObjectRef Type:** `/home/user/ray/python/ray/types.py`

```python
@PublicAPI
class ObjectRef(Generic[T]):
    """Type hint wrapper for ray.ObjectRef (actual impl is in Cython)"""
    pass
```

Actual implementation in Cython: `/home/user/ray/python/ray/_raylet.pyx`

### Memory Management

- **Plasma Store:** C++ server managing shared memory
- **Eviction Policy:** LRU when store is full
- **Spilling:** To disk if memory limited
- **Reference Counting:** Pins objects while references exist

---

## 4. Serialization Mechanisms

### Serialization Stack

Ray uses a **multi-layer serialization approach**:

#### **Layer 1: cloudpickle (Default)**

```python
import ray.cloudpickle as pickle  # Ray's enhanced cloudpickle

# All function closures, class instances use cloudpickle
obj = {"closure_var": expensive_object}
serialized = pickle.dumps(obj)
```

**File:** `/home/user/ray/python/ray/cloudpickle/`

Features:
- Handles lambda functions
- Serializes closure variables
- Supports custom types

#### **Layer 2: Pickle5 with Buffer Protocol**

For large arrays, Ray uses **Pickle5 writer** for zero-copy:

```python
from ray._raylet import Pickle5SerializedObject, Pickle5Writer

# Efficiently serializes numpy arrays, torch tensors
serialized = Pickle5SerializedObject(...)
```

**File:** `/home/user/ray/python/ray/_private/serialization.py:1+`

#### **Layer 3: Custom Serializers**

```python
# Ray registers custom serializers for:
# - ObjectRef (object references)
# - ActorHandle (actor references)  
# - Tensors (PyTorch, TensorFlow)
# - GPU objects (CUDA memory)
```

### Serialization Context

**File:** `/home/user/ray/python/ray/_private/serialization.py:145+`

```python
class SerializationContext:
    """Initialize the serialization library.
    
    This defines a custom serializer for object refs and also tells ray to
    serialize several exception classes.
    """
    
    def add_default_serializer(self, cls, serializer, deserializer):
        """Register custom serializer for a type"""
        
    def add_default_deserializer(self, cls, deserializer):
        """Register custom deserializer for a type"""
```

### Object Reference Deserialization

When an ObjectRef is pickled and sent to a worker:

**File:** `/home/user/ray/python/ray/_private/serialization.py:68+`

```python
def _object_ref_deserializer(
    binary,              # ObjectRef bytes
    call_site,           # Where it was created
    owner_address,       # Owner worker address
    object_status,       # Current object status
    tensor_transport_val # Transport method (object_store, nixl, etc.)
):
    # 1. Deserialize ObjectRef
    obj_ref = ray.ObjectRef(
        binary, 
        owner_address, 
        call_site, 
        tensor_transport_val=tensor_transport_val
    )
    
    # 2. Register with core worker (tracks ref count)
    if owner_address:
        worker = ray._private.worker.global_worker
        worker.core_worker.deserialize_and_register_object_ref(
            obj_ref.binary(), 
            outer_id, 
            owner_address, 
            object_status
        )
    
    return obj_ref
```

### Serialization Options

```python
@ray.remote(
    _tensor_transport="object_store",  # or "nixl" for GPUs
)
def gpu_task():
    pass
```

Tensor transport methods:
- `"object_store"` - Standard Plasma store (default)
- `"nixl"` - Direct GPU-to-GPU transport (experimental)

---

## 5. Main Ray Libraries

### 5.1 Ray Data

**Purpose:** Distributed data processing  
**Entry Point:** `/home/user/ray/python/ray/data/__init__.py:1+`

```python
# Core Classes
from ray.data import (
    Dataset,           # Main data container
    DataIterator,      # Iterator for streaming
    Preprocessor,      # Data preprocessing
)

# Read APIs
ray.data.read_parquet("s3://bucket/data/*.parquet")
ray.data.read_csv("data.csv")
ray.data.read_json("data.json")
ray.data.read_lance("data.lance")
ray.data.from_pandas(df)
ray.data.from_arrow(table)
ray.data.from_torch(torch_dataloader)

# Write APIs
ds.write_parquet("output/")
ds.write_csv("output.csv")
ds.write_lance("output.lance")
```

Key classes:

```python
# File: /home/user/ray/python/ray/data/dataset.py:167
class Dataset:
    """Immutable, distributed dataset."""
    
    def map_batches(self, fn, batch_format="default", **options):
        """Apply function to batches"""
        
    def filter(self, predicate):
        """Filter rows based on condition"""
        
    def select_columns(self, columns):
        """Project specific columns"""
        
    def repartition(self, num_blocks):
        """Change number of partitions"""
        
    def random_shuffle(self):
        """Shuffle dataset"""
        
    def sort(self, key):
        """Sort dataset"""
        
    def groupby(self, key):
        """Group rows by key"""
        
    def to_torch(self):
        """Convert to PyTorch DataLoader"""
```

Architecture:
- **Blocks:** Arrow tables split across workers
- **Execution:** DAG of operators (map, filter, join, etc.)
- **Lazy evaluation:** Operations create DAG, execution on materialize/get

### 5.2 Ray Train

**Purpose:** Distributed model training  
**Entry Point:** `/home/user/ray/python/ray/train/__init__.py:1+`

Key APIs:

```python
from ray.train import Trainer, ScalingConfig

trainer = Trainer(
    trainable=train_fn,  # User training function
    scaling_config=ScalingConfig(
        num_workers=4,
        use_gpu=True,
        resources_per_worker={"GPU": 1}
    ),
    run_config=RunConfig(
        checkpoint_config=CheckpointConfig(num_to_keep=3),
        storage_path="s3://bucket/checkpoints",
    )
)

result = trainer.fit()  # Run distributed training
```

Features:
- **Distributed training:** DDP, Horovod, etc.
- **Fault tolerance:** Automatic checkpointing and recovery
- **Data integration:** Works with Ray Data for data loading
- **Hyperparameter tuning:** Integration with Ray Tune

### 5.3 Ray Serve

**Purpose:** Model serving and deployment  
**Entry Point:** `/home/user/ray/python/ray/serve/__init__.py:1+`

```python
from ray import serve

@serve.deployment
class Model:
    def __call__(self, request):
        return "prediction"

serve.run(Model.bind())
```

Features:
- **Autoscaling:** Scale replicas based on load
- **Versioning:** A/B testing, canary deployments
- **Batching:** Automatic request batching
- **Multiplexing:** Share GPU across models

### 5.4 Ray Tune

**Purpose:** Hyperparameter optimization  
**Entry Point:** `/home/user/ray/python/ray/tune/__init__.py:1+`

```python
from ray.tune import Tuner, TuneConfig
from ray.tune.search.optuna import OptunaSearch

tuner = Tuner(
    trainable=train_fn,
    param_space={
        "lr": tune.loguniform(1e-4, 1e-1),
        "batch_size": tune.choice([32, 64, 128]),
    },
    tune_config=TuneConfig(
        num_samples=10,
        search_alg=OptunaSearch(),
    ),
)

results = tuner.fit()
```

Features:
- **Search algorithms:** Grid, random, Bayesian, Optuna, etc.
- **Schedulers:** ASHA, PBT, population-based training
- **Callbacks:** Monitor and react to trial progress
- **Analysis:** Analyze results across trials

---

## 6. Architecture Summary

### Core Components

```
┌─────────────────────────────────────────────────────┐
│                   Ray Application                   │
├─────────────────────────────────────────────────────┤
│
│  User Code: @ray.remote, ray.get(), ray.put()
│
├─────────────────────────────────────────────────────┤
│              Ray Python API Layer                   │
├─────────────────────────────────────────────────────┤
│
│  remote_function.py (Task submission)
│  actor.py (Actor creation/method calls)
│  worker.py (Main worker process interface)
│  serialization.py (Cloudpickle + custom serializers)
│
├─────────────────────────────────────────────────────┤
│         Ray C++/Cython Core (_raylet.pyx)          │
├─────────────────────────────────────────────────────┤
│
│  ObjectRef, TaskID, WorkerID, etc. (ID types)
│  RayletClient (Submit tasks to scheduler)
│  Serialization helpers (Pickle5, MessagePack)
│
├─────────────────────────────────────────────────────┤
│            Ray Distributed System                   │
├─────────────────────────────────────────────────────┤
│
│  GCS (Global Control Store)
│    - Metadata management
│    - Service discovery
│    
│  Raylet (Scheduler)
│    - Task scheduling
│    - Resource management
│    - Worker management
│    
│  Plasma Object Store
│    - Distributed in-memory storage
│    - 64GB default, configurable
│    
│  Ray Worker Processes
│    - Execute tasks
│    - Manage local state
│    - Interact with object store
│
└─────────────────────────────────────────────────────┘
```

### Data Flow Summary

**Task Submission:**
```
@ray.remote def f(x): ... 
    ↓
f.remote(args)
    ↓
RemoteFunction._remote() 
    ↓
Serialize args + function
    ↓
worker.core_worker.submit_task()
    ↓
RayletClient (C++ binding)
    ↓
Raylet Scheduler
```

**Task Execution:**
```
Raylet assigns task to worker
    ↓
Worker deserializes args from object store
    ↓
Worker executes function
    ↓
Result serialized → Object Store
    ↓
ObjectRef returned to user
```

**Object Access:**
```
ray.get(ref)
    ↓
Check local object store (hit → return)
    ↓
Query GCS for location
    ↓
Fetch from remote object store
    ↓
Deserialize → return to user
```

---

## Key Design Principles

### 1. **Lazy Execution**
- Operations return ObjectRefs immediately
- No computation until `ray.get()` or explicit trigger
- Enables efficient DAG optimization

### 2. **Transparent Serialization**
- Users write normal Python code
- Ray handles all serialization/deserialization
- Custom serializers for specific types (ObjectRef, GPU tensors, etc.)

### 3. **Distributed Everywhere**
- Objects live in distributed object store
- Functions execute on remote workers
- Transparent data movement

### 4. **Fault Tolerance**
- Tasks can be retried (`max_retries` parameter)
- Lineage preserved for reconstruction
- Object pinning via reference counting

### 5. **Resource Isolation**
- Explicit resource requirements (CPU, GPU, custom)
- Placement groups for colocation
- Node affinity scheduling

---

## File Structure Reference

```
python/ray/
├── __init__.py                 # Public API exports
├── _private/
│   ├── worker.py              # Worker, init(), get(), put(), wait()
│   ├── serialization.py       # Serialization context, custom serializers
│   └── ...
├── remote_function.py         # RemoteFunction class, _remote() method
├── actor.py                   # ActorClass, ActorHandle
├── types.py                   # ObjectRef generic type
├── data/
│   ├── __init__.py            # Ray Data API
│   └── dataset.py             # Dataset class
├── train/
│   ├── __init__.py            # Ray Train API
│   └── ...
├── serve/
│   ├── __init__.py            # Ray Serve API
│   └── ...
├── tune/
│   ├── __init__.py            # Ray Tune API
│   └── ...
└── _raylet.pyx               # Cython/C++ bindings
```

---

## Example: Complete Data Flow

```python
import ray

# Initialize Ray
ray.init(num_cpus=4, num_gpus=1)

# Define a remote function
@ray.remote(num_cpus=1)
def process(data):
    return sum(data)

# Store data
data = [1, 2, 3, 4, 5]
ref = ray.put(data)  # → ObjectRef in object store

# Submit task (lazy)
result_ref = process.remote(ref)  # → ObjectRef (task not executed yet)

# Execute and get result
result = ray.get(result_ref)  # Block, fetch, deserialize → 15

# Shutdown
ray.shutdown()

# Data flow:
# 1. ray.put(data): Serializes data → Plasma store → ObjectRef
# 2. process.remote(ref): 
#    - Serializes function + args (including ObjectRef)
#    - Submits to Raylet scheduler
# 3. Raylet assigns to worker (resource-matched)
# 4. Worker deserializes from object store
# 5. Function executes
# 6. Result serialized → Object store
# 7. ray.get(result_ref): Fetches + deserializes → user code
```

---

