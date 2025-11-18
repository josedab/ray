# User Guide

> Comprehensive guides for all Ray libraries.

## Overview

The User Guide provides detailed documentation for each Ray library. Each section includes concepts, examples, best practices, and advanced patterns.

## Ray Libraries

### Core Computing

| Library | Description | Use Cases |
|---------|-------------|-----------|
| [Ray Core](ray-core/tasks.md) | Distributed primitives | Tasks, actors, objects |

### Machine Learning

| Library | Description | Use Cases |
|---------|-------------|-----------|
| [Ray Data](ray-data/basics.md) | Scalable data processing | ETL, ML preprocessing |
| [Ray Train](ray-train/basics.md) | Distributed training | Multi-GPU/node training |
| [Ray Tune](ray-tune/basics.md) | Hyperparameter tuning | HPO, experiment management |
| [Ray Serve](ray-serve/basics.md) | Model serving | Online inference APIs |

## How to Use This Guide

Each page follows a consistent structure:

1. **Overview** - What the feature does
2. **Quick Start** - Minimal working example
3. **Detailed Guide** - In-depth explanations
4. **Common Patterns** - Real-world usage
5. **Troubleshooting** - Common issues and solutions
6. **API Reference** - Links to relevant APIs

## Learning Progression

### Beginners

1. Start with [Core Concepts](../getting-started/core-concepts.md)
2. Learn [Tasks](ray-core/tasks.md) and [Actors](ray-core/actors.md)
3. Follow a [Learning Path](../getting-started/learning-paths/)

### ML Practitioners

1. [Ray Data](ray-data/basics.md) for data loading
2. [Ray Train](ray-train/basics.md) for distributed training
3. [Ray Tune](ray-tune/basics.md) for hyperparameter search
4. [Ray Serve](ray-serve/basics.md) for deployment

### Platform Engineers

1. [Cluster Setup](cluster/)
2. [Configuration](../reference/configuration/)
3. [Monitoring](../ray-observability/)

## Cross-Library Integration

Ray libraries work together seamlessly:

```python
import ray
from ray import train, tune
from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer

# Load data with Ray Data
dataset = ray.data.read_parquet("s3://bucket/data/")

# Train with Ray Train
def train_func(config):
    for epoch in range(10):
        train.report({"loss": 1.0 / (epoch + 1)})

trainer = TorchTrainer(
    train_func,
    datasets={"train": dataset},
    scaling_config=ScalingConfig(num_workers=4)
)

# Tune with Ray Tune
tuner = tune.Tuner(
    trainer,
    param_space={"train_loop_config": {"lr": tune.loguniform(1e-4, 1e-1)}}
)
results = tuner.fit()

# Serve with Ray Serve
# (deploy best model checkpoint)
```

## Related Resources

- [Getting Started](../getting-started/) - First steps with Ray
- [How-To Guides](../how-to/) - Task-oriented guides
- [API Reference](../reference/) - Complete API documentation
- [Architecture](../architecture/) - System internals
