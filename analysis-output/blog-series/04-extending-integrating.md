# Part 4: Extending and Integrating Ray

> **Series:** Ray Deep Dive | **Reading Time:** 16 minutes
> **Commit:** `d1cce8c9dc8411fad7cfbd619350bec6f19839a3`

## What You'll Learn

- Ray's extension points and plugin architecture
- How Ray Data, Train, Serve, and Tune are built on Ray Core
- Creating custom serializers for your types
- Integration patterns for ML frameworks

## Introduction

Ray is designed to be extended. Whether you're integrating a new ML framework, building a custom scheduler, or creating specialized data pipelines, Ray provides clean extension points. In this post, we'll explore how Ray's high-level libraries are built and how you can extend Ray for your needs.

## Ray's Library Architecture

Ray's libraries share a common architecture pattern:

```
┌─────────────────────────────────────────────────┐
│          High-Level Library API                  │
│   (Ray Data, Train, Serve, Tune)                │
└─────────────────────┬───────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────┐
│       Library-Specific Abstractions             │
│  (Dataset, Trainer, Deployment, Tuner)          │
└─────────────────────┬───────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────┐
│              Ray Core Primitives                 │
│      (Tasks, Actors, ObjectRefs)                │
└─────────────────────────────────────────────────┘
```

Each library translates its domain concepts into Ray tasks and actors.

## Ray Data: Distributed Data Processing

Ray Data provides a `Dataset` abstraction for distributed data processing.

### Architecture

```python
# python/ray/data/dataset.py

class Dataset:
    """Distributed collection of data blocks."""

    def __init__(self, blocks: List[ObjectRef]):
        self._blocks = blocks  # ObjectRefs to data chunks

    def map(self, fn: Callable) -> "Dataset":
        """Apply function to each element."""
        # Creates tasks for each block
        new_blocks = [
            map_block.remote(block, fn)
            for block in self._blocks
        ]
        return Dataset(new_blocks)
```

**Key implementation:** [`python/ray/data/dataset.py`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/python/ray/data/dataset.py) (6,774 LOC)

### Building a Data Pipeline

```python
import ray

# Read data (creates Dataset)
ds = ray.data.read_parquet("s3://bucket/data/")

# Transform pipeline
result = (
    ds
    .filter(lambda row: row["value"] > 0)
    .map(lambda row: {"value": row["value"] * 2})
    .groupby("category")
    .mean("value")
)

# Execute and collect
df = result.to_pandas()
```

### Under the Hood

Each operation creates Ray tasks:

```python
# Simplified view of map implementation
@ray.remote
def map_block(block: Block, fn: Callable) -> Block:
    """Map function over a single block."""
    return block.map(fn)

def map(self, fn: Callable) -> "Dataset":
    # Schedule tasks for all blocks
    new_blocks = [
        map_block.remote(block, fn)
        for block in self._blocks
    ]
    return Dataset(new_blocks)
```

### Extending Ray Data

Create custom datasources:

```python
from ray.data.datasource import Datasource, ReadTask

class CustomDatasource(Datasource):
    def prepare_read(self, parallelism: int) -> List[ReadTask]:
        """Return read tasks for parallel loading."""
        tasks = []
        for partition in self.partitions:
            tasks.append(
                ReadTask(
                    lambda p=partition: self.read_partition(p),
                    metadata=BlockMetadata(num_rows=None, size_bytes=None)
                )
            )
        return tasks

# Use custom datasource
ds = ray.data.read_datasource(CustomDatasource(...))
```

## Ray Train: Distributed Training

Ray Train distributes ML training across workers.

### Architecture

```python
# python/ray/train/trainer.py

class DataParallelTrainer:
    """Base class for data-parallel training."""

    def __init__(self, train_loop_per_worker, scaling_config):
        self.train_loop = train_loop_per_worker
        self.scaling_config = scaling_config

    def fit(self):
        # Create training actors
        workers = [
            TrainingWorker.remote(self.train_loop)
            for _ in range(self.scaling_config.num_workers)
        ]

        # Coordinate training
        results = ray.get([
            worker.train.remote()
            for worker in workers
        ])

        return TrainingResult(results)
```

### PyTorch Integration

```python
from ray.train.torch import TorchTrainer
from ray.train import ScalingConfig

def train_func(config):
    # This runs on each worker
    model = create_model()
    model = ray.train.torch.prepare_model(model)

    dataset = ray.train.get_dataset_shard("train")

    for epoch in range(config["epochs"]):
        for batch in dataset.iter_torch_batches():
            loss = train_step(model, batch)

        # Report metrics
        ray.train.report({"loss": loss})

trainer = TorchTrainer(
    train_func,
    train_loop_config={"epochs": 10},
    scaling_config=ScalingConfig(num_workers=4, use_gpu=True),
    datasets={"train": train_dataset}
)

result = trainer.fit()
```

**Key files:**
- [`python/ray/train/torch/`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/python/ray/train/torch/)
- [`python/ray/train/trainer.py`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/python/ray/train/base_trainer.py)

### Creating a Custom Trainer

```python
from ray.train import BackendConfig, Backend

class CustomBackendConfig(BackendConfig):
    @property
    def backend_cls(self):
        return CustomBackend

class CustomBackend(Backend):
    def on_training_start(self, worker_group, backend_config):
        """Initialize custom distributed backend."""
        # Setup custom communication
        pass

    def on_training_end(self, worker_group, backend_config):
        """Cleanup."""
        pass

# Use custom backend
trainer = Trainer(
    train_func,
    backend=CustomBackendConfig()
)
```

## Ray Serve: Model Serving

Ray Serve provides model serving with autoscaling.

### Deployment Architecture

```python
# python/ray/serve/deployment.py

from ray import serve

@serve.deployment(
    num_replicas=3,
    ray_actor_options={"num_gpus": 1}
)
class ModelServer:
    def __init__(self):
        self.model = load_model()

    async def __call__(self, request):
        data = await request.json()
        return self.model.predict(data)

# Deploy
serve.run(ModelServer.bind())
```

### Under the Hood

Each deployment becomes Ray actors:

```python
# Simplified deployment management
class DeploymentState:
    def __init__(self, deployment_config):
        self.config = deployment_config
        self.replicas = []

    def scale_to(self, num_replicas):
        current = len(self.replicas)

        if num_replicas > current:
            # Create new replicas (actors)
            for _ in range(num_replicas - current):
                replica = ReplicaActor.remote(self.config)
                self.replicas.append(replica)

        elif num_replicas < current:
            # Remove excess replicas
            for replica in self.replicas[num_replicas:]:
                ray.kill(replica)
            self.replicas = self.replicas[:num_replicas]
```

**Key files:**
- [`python/ray/serve/_private/deployment_state.py`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/python/ray/serve/_private/deployment_state.py)
- [`python/ray/serve/deployment.py`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/python/ray/serve/deployment.py)

### Composition Patterns

```python
@serve.deployment
class Preprocessor:
    def process(self, data):
        return preprocess(data)

@serve.deployment
class Model:
    def predict(self, features):
        return self.model(features)

@serve.deployment
class Ensemble:
    def __init__(self, preprocessor, model):
        self.preprocessor = preprocessor
        self.model = model

    async def __call__(self, request):
        data = await request.json()
        features = await self.preprocessor.process.remote(data)
        return await self.model.predict.remote(features)

# Compose deployments
app = Ensemble.bind(
    Preprocessor.bind(),
    Model.bind()
)
serve.run(app)
```

## Ray Tune: Hyperparameter Tuning

Ray Tune manages distributed hyperparameter search.

### Architecture

```python
# python/ray/tune/tuner.py

class Tuner:
    def __init__(self, trainable, param_space, tune_config):
        self.trainable = trainable
        self.param_space = param_space
        self.tune_config = tune_config

    def fit(self):
        # Create trials (each is a Ray task or actor)
        trials = []
        for config in self.search_algorithm.suggest():
            trial = Trial(self.trainable, config)
            trials.append(trial)

        # Execute trials with scheduler
        return self.trial_executor.run(trials)
```

### Search and Scheduling

```python
from ray import tune
from ray.tune.schedulers import ASHAScheduler

def trainable(config):
    model = create_model(config)

    for epoch in range(100):
        loss = train_epoch(model)
        tune.report(loss=loss)

tuner = tune.Tuner(
    trainable,
    param_space={
        "lr": tune.loguniform(1e-4, 1e-1),
        "batch_size": tune.choice([16, 32, 64]),
        "hidden_size": tune.randint(64, 256)
    },
    tune_config=tune.TuneConfig(
        num_samples=100,
        scheduler=ASHAScheduler(
            metric="loss",
            mode="min"
        )
    )
)

results = tuner.fit()
best = results.get_best_result()
```

**Key files:**
- [`python/ray/tune/tuner.py`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/python/ray/tune/tuner.py)
- [`python/ray/tune/schedulers/`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/python/ray/tune/schedulers/)

## Custom Serialization

Ray allows you to register custom serializers for your types.

### Basic Custom Serializer

```python
import ray

class MyCustomClass:
    def __init__(self, data):
        self.data = data

def serialize_custom(obj):
    return {"data": obj.data}

def deserialize_custom(serialized):
    return MyCustomClass(serialized["data"])

# Register serializer
ray.util.register_serializer(
    MyCustomClass,
    serializer=serialize_custom,
    deserializer=deserialize_custom
)

# Now works with Ray
ref = ray.put(MyCustomClass([1, 2, 3]))
obj = ray.get(ref)
```

### Arrow Serialization for DataFrames

```python
# python/ray/_private/arrow_serialization.py

def _arrow_table_reducer(table):
    """Efficient Arrow table serialization."""
    # Use Arrow IPC format for zero-copy
    sink = pa.BufferOutputStream()
    writer = pa.ipc.new_stream(sink, table.schema)
    writer.write_table(table)
    writer.close()
    return sink.getvalue().to_pybytes()

# Register for pyarrow.Table
ray.util.register_serializer(
    pa.Table,
    serializer=_arrow_table_reducer,
    deserializer=_arrow_table_reconstructor
)
```

### GPU Tensor Serialization

```python
# Automatic handling for PyTorch tensors
import torch

# GPU tensors are automatically handled
@ray.remote(num_gpus=1)
def gpu_task():
    tensor = torch.randn(1000, 1000, device="cuda")
    return tensor  # Automatically serialized correctly
```

## Runtime Environments

Customize the execution environment for tasks and actors:

```python
# Package dependencies
ray.init(runtime_env={
    "pip": ["pandas==1.3.0", "numpy==1.21.0"],
    "conda": "environment.yml",
})

# Environment variables
@ray.remote(runtime_env={"env_vars": {"API_KEY": "secret"}})
def task_with_env():
    import os
    return os.environ["API_KEY"]

# Working directory
@ray.remote(runtime_env={"working_dir": "/path/to/code"})
def task_with_code():
    import my_module
    return my_module.run()

# Container image
@ray.remote(runtime_env={"container": {"image": "my-image:latest"}})
def containerized_task():
    pass
```

## Creating Ray Plugins

### Node Provider Plugin

For custom cluster management:

```python
from ray.autoscaler._private.node_provider import NodeProvider

class CustomNodeProvider(NodeProvider):
    def create_node(self, node_config, tags, count):
        """Provision nodes in your infrastructure."""
        instances = []
        for _ in range(count):
            instance = self.api.create_instance(node_config)
            instances.append(instance.id)
        return instances

    def terminate_node(self, node_id):
        """Terminate a node."""
        self.api.terminate_instance(node_id)

    def get_node_ips(self, nodes, use_private_ips):
        """Get IP addresses for nodes."""
        return [self.api.get_ip(node_id) for node_id in nodes]
```

**Register in cluster config:**

```yaml
cluster_name: my-cluster
provider:
    type: external
    module: my_provider.CustomNodeProvider
```

### Custom Autoscaler Policy

```python
from ray.autoscaler._private.resource_demand_scheduler import (
    ResourceDemandScheduler
)

class CustomScheduler(ResourceDemandScheduler):
    def get_nodes_to_launch(
        self,
        unfulfilled_demand,
        cluster_resources,
        node_types
    ):
        """Custom logic for scaling decisions."""
        # Your scaling algorithm here
        return nodes_to_launch
```

## Integration Patterns

### Wrapping External Libraries

```python
import ray
from external_lib import ExternalModel

@ray.remote
class ModelWrapper:
    def __init__(self, model_path):
        # Initialize external library
        self.model = ExternalModel.load(model_path)

    def predict(self, data):
        # Convert Ray types to library types
        array = data.numpy()

        # Call external library
        result = self.model.predict(array)

        # Convert back
        return result.tolist()
```

### Event-Driven Integration

```python
import ray
from kafka import KafkaConsumer

@ray.remote
class KafkaProcessor:
    def __init__(self, topic):
        self.consumer = KafkaConsumer(topic)

    def process_messages(self):
        for message in self.consumer:
            # Process with Ray tasks
            process_message.remote(message.value)

@ray.remote
def process_message(data):
    # Distributed processing
    return transform(data)

# Start processor
processor = KafkaProcessor.remote("events")
processor.process_messages.remote()
```

### Batch Inference Pattern

```python
from ray import serve
import numpy as np

@serve.deployment(
    max_concurrent_queries=100,
    batch_max_batch_size=32,
    batch_wait_timeout_s=0.1
)
class BatchPredictor:
    def __init__(self):
        self.model = load_model()

    @serve.batch
    async def __call__(self, requests):
        # Batch requests together
        inputs = np.stack([r.json()["input"] for r in requests])

        # Single batched prediction
        outputs = self.model.predict(inputs)

        # Return list of responses
        return [{"output": o.tolist()} for o in outputs]
```

## Best Practices for Extensions

### 1. Minimize Serialization Overhead

```python
# Bad: Serialize large data every call
@ray.remote
def process(large_data, param):
    return transform(large_data, param)

# Good: Put large data in object store once
large_ref = ray.put(large_data)

@ray.remote
def process(large_ref, param):
    return transform(large_ref, param)
```

### 2. Use Actors for Stateful Operations

```python
# Bad: Load model for each task
@ray.remote
def predict(data):
    model = load_model()  # Expensive!
    return model.predict(data)

# Good: Load once in actor
@ray.remote
class Predictor:
    def __init__(self):
        self.model = load_model()

    def predict(self, data):
        return self.model.predict(data)
```

### 3. Design for Fault Tolerance

```python
@ray.remote(max_restarts=3, max_task_retries=2)
class ResilientService:
    def __init__(self):
        # Load state from checkpoint
        self.state = self.load_checkpoint()

    def process(self, data):
        result = self.compute(data)
        self.checkpoint()  # Save periodically
        return result
```

## Key Takeaways

1. **Ray libraries** are built on tasks and actors - the same primitives you use
2. **Custom serializers** enable efficient handling of your data types
3. **Runtime environments** provide dependency isolation
4. **Node providers** extend cluster management to any infrastructure
5. **Composition patterns** let you build complex systems from simple pieces

## What's Next

In [Part 5](./05-performance-analysis.md), we'll explore:
- Performance characteristics of Ray
- Common bottlenecks and optimizations
- Profiling and debugging tools

## Code References

- Ray Data: [`python/ray/data/`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/python/ray/data/)
- Ray Train: [`python/ray/train/`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/python/ray/train/)
- Ray Serve: [`python/ray/serve/`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/python/ray/serve/)
- Ray Tune: [`python/ray/tune/`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/python/ray/tune/)
- Serialization: [`python/ray/_private/serialization.py`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/python/ray/_private/serialization.py)

---

*Next: [Part 5 - Performance Analysis and Optimization](./05-performance-analysis.md)*
