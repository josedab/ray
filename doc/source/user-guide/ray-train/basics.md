# Ray Train Basics

> Distribute your machine learning training across multiple workers.

## Overview

Ray Train simplifies distributed training by letting you scale existing training code with minimal changes. It supports PyTorch, TensorFlow, and other frameworks, handling the distributed setup, data loading, and checkpointing automatically.

## Prerequisites

- Ray installed (`pip install "ray[train]"`)
- PyTorch or TensorFlow
- Basic ML training experience
- Understanding of [Core Concepts](../../getting-started/core-concepts.md)

## Quick Start

```python
import ray
from ray import train
from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer

def train_func():
    for epoch in range(10):
        # Your training code here
        train.report({"loss": 1.0 / (epoch + 1)})

trainer = TorchTrainer(
    train_func,
    scaling_config=ScalingConfig(num_workers=4)
)

result = trainer.fit()
print(f"Final loss: {result.metrics['loss']}")
```

## Detailed Guide

### Creating a Trainer

Choose the trainer for your framework:

```python
from ray.train.torch import TorchTrainer
from ray.train.tensorflow import TensorflowTrainer
from ray.train.huggingface import HuggingFaceTrainer

# PyTorch
trainer = TorchTrainer(train_func, scaling_config=config)

# TensorFlow
trainer = TensorflowTrainer(train_func, scaling_config=config)

# Hugging Face Transformers
trainer = HuggingFaceTrainer(train_func, scaling_config=config)
```

### Configuring Scaling

Set the number of workers and resources:

```python
from ray.train import ScalingConfig

# CPU-only training
config = ScalingConfig(
    num_workers=4,
    use_gpu=False,
    resources_per_worker={"CPU": 2}
)

# GPU training
config = ScalingConfig(
    num_workers=4,
    use_gpu=True,
    resources_per_worker={"GPU": 1}
)
```

### Writing Training Functions

Your training function runs on each worker:

```python
import torch
import torch.nn as nn
from ray import train
from ray.train.torch import TorchTrainer

def train_func(config):
    # Create model
    model = nn.Linear(10, 1)

    # Prepare for distributed training
    model = train.torch.prepare_model(model)

    # Training loop
    optimizer = torch.optim.Adam(model.parameters(), lr=config["lr"])

    for epoch in range(config["epochs"]):
        loss = train_step(model, optimizer)

        # Report metrics
        train.report({"loss": loss, "epoch": epoch})

# Launch training
trainer = TorchTrainer(
    train_func,
    train_loop_config={"lr": 0.001, "epochs": 10},
    scaling_config=ScalingConfig(num_workers=4, use_gpu=True)
)

result = trainer.fit()
```

### Data Loading

Load data for distributed training:

```python
from ray import train
from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer
import ray.data

def train_func():
    # Get the data shard for this worker
    train_data = train.get_dataset_shard("train")

    for epoch in range(10):
        for batch in train_data.iter_torch_batches(batch_size=32):
            # Training step
            pass

# Create Ray Dataset
dataset = ray.data.read_parquet("s3://bucket/training-data/")

trainer = TorchTrainer(
    train_func,
    datasets={"train": dataset},
    scaling_config=ScalingConfig(num_workers=4)
)
```

### Checkpointing

Save and resume training:

```python
from ray import train
from ray.train import Checkpoint
import tempfile

def train_func():
    model = create_model()

    # Load from checkpoint if resuming
    checkpoint = train.get_checkpoint()
    if checkpoint:
        with checkpoint.as_directory() as dir:
            model.load_state_dict(torch.load(f"{dir}/model.pt"))

    for epoch in range(100):
        train_epoch(model)

        # Save checkpoint
        with tempfile.TemporaryDirectory() as tmpdir:
            torch.save(model.state_dict(), f"{tmpdir}/model.pt")
            train.report(
                {"epoch": epoch},
                checkpoint=Checkpoint.from_directory(tmpdir)
            )
```

### Accessing Results

Get training results and checkpoints:

```python
result = trainer.fit()

# Get final metrics
print(f"Loss: {result.metrics['loss']}")

# Get best checkpoint
checkpoint = result.best_checkpoints[0][0]

# Load model from checkpoint
with checkpoint.as_directory() as dir:
    model.load_state_dict(torch.load(f"{dir}/model.pt"))
```

## Common Patterns

### Pattern: PyTorch Distributed Training

**Use case:** Scale PyTorch training to multiple GPUs/nodes

```python
import torch
import torch.nn as nn
from torch.utils.data import DataLoader
from ray import train
from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer

def train_func(config):
    # Model
    model = nn.Sequential(
        nn.Linear(784, 128),
        nn.ReLU(),
        nn.Linear(128, 10)
    )

    # Prepare for distributed training
    model = train.torch.prepare_model(model)

    # Data
    dataset = get_dataset()
    dataloader = DataLoader(dataset, batch_size=config["batch_size"])
    dataloader = train.torch.prepare_data_loader(dataloader)

    # Training
    optimizer = torch.optim.Adam(model.parameters())
    criterion = nn.CrossEntropyLoss()

    for epoch in range(config["epochs"]):
        for batch in dataloader:
            optimizer.zero_grad()
            loss = criterion(model(batch["x"]), batch["y"])
            loss.backward()
            optimizer.step()

        train.report({"loss": loss.item()})

trainer = TorchTrainer(
    train_func,
    train_loop_config={"batch_size": 32, "epochs": 10},
    scaling_config=ScalingConfig(num_workers=4, use_gpu=True)
)
```

### Pattern: Fault Tolerant Training

**Use case:** Resume training after failures

```python
from ray.train import RunConfig, FailureConfig

trainer = TorchTrainer(
    train_func,
    scaling_config=ScalingConfig(num_workers=4),
    run_config=RunConfig(
        failure_config=FailureConfig(max_failures=3)
    )
)

# Training will automatically resume from checkpoint on failure
result = trainer.fit()
```

### Pattern: Experiment Tracking

**Use case:** Log metrics to external systems

```python
from ray import train
from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer
import mlflow

def train_func():
    mlflow.start_run()

    for epoch in range(10):
        loss = train_epoch()
        mlflow.log_metric("loss", loss, step=epoch)
        train.report({"loss": loss})

    mlflow.end_run()
```

## Troubleshooting

### Issue: Out of GPU Memory

**Symptoms:** `CUDA out of memory` error

**Cause:** Batch size too large or model too big

**Solution:** Reduce batch size or use gradient accumulation:

```python
def train_func(config):
    accumulation_steps = 4
    for batch_idx, batch in enumerate(dataloader):
        loss = compute_loss(batch) / accumulation_steps
        loss.backward()

        if (batch_idx + 1) % accumulation_steps == 0:
            optimizer.step()
            optimizer.zero_grad()
```

### Issue: Training Hangs

**Symptoms:** Training stops making progress

**Cause:** Deadlock in distributed communication

**Solution:** Ensure all workers execute the same operations:

```python
# Bad: Different code paths on different workers
if train.get_context().get_world_rank() == 0:
    train.report({"metric": value})  # Only rank 0 reports

# Good: All workers report
train.report({"metric": value})  # All workers report
```

### Issue: Slow Data Loading

**Symptoms:** GPU utilization is low

**Cause:** Data loading is bottleneck

**Solution:** Use Ray Data for efficient distributed loading:

```python
# Create dataset once
dataset = ray.data.read_parquet("data/")

# Automatically sharded across workers
trainer = TorchTrainer(
    train_func,
    datasets={"train": dataset},
    scaling_config=ScalingConfig(num_workers=4)
)
```

## What's Next

- [Advanced Training](advanced.md) - Custom training loops
- [Hyperparameter Tuning](../ray-tune/basics.md) - Tune your model

> **Related:** Load data efficiently with [Ray Data](../ray-data/basics.md)
> **Related:** Tune hyperparameters with [Ray Tune](../ray-tune/basics.md)
> **Related:** Deploy trained models with [Ray Serve](../ray-serve/basics.md)

## API Reference

- [`TorchTrainer`](../../reference/api/ray-train.md#torchtrainer)
- [`ScalingConfig`](../../reference/api/ray-train.md#scalingconfig)
- [`train.report`](../../reference/api/ray-train.md#report)
- [`train.get_checkpoint`](../../reference/api/ray-train.md#get-checkpoint)
