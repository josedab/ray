# ML Engineer Learning Path

> A guided path to mastering Ray for machine learning workflows.

---

**Duration:** 2 hours
**Difficulty:** Intermediate

---

## Prerequisites

- Python familiarity (functions, classes, decorators)
- Basic ML knowledge (training, evaluation, hyperparameters)
- Familiarity with PyTorch or TensorFlow

## Overview

This learning path takes you from Ray basics to building complete ML pipelines. You'll learn how to distribute training, tune hyperparameters, and serve models—all using Ray's unified framework.

## Modules

### Module 1: Getting Started with Ray

**Duration:** 15 minutes

Learn the fundamentals of Ray's distributed computing model.

#### Topics Covered
- Installing Ray
- Tasks and actors
- The object store

#### Resources
- [Quick Start](../quick-start.md)
- [Core Concepts](../core-concepts.md)

#### Hands-on Exercise

Run your first distributed computation:

```python
import ray

ray.init()

@ray.remote
def square(x):
    return x ** 2

# Parallel computation
results = ray.get([square.remote(i) for i in range(10)])
print(f"Squares: {results}")
```

#### Checkpoint
You should be able to:
- [ ] Initialize Ray
- [ ] Create and call remote functions
- [ ] Understand ObjectRefs and `ray.get()`

---

### Module 2: Distributed Training with Ray Train

**Duration:** 30 minutes

Scale your training code from a single GPU to a cluster.

#### Topics Covered
- Ray Train architecture
- Scaling configurations
- Checkpointing and fault tolerance

#### Resources
- [Ray Train Basics](../../user-guide/ray-train/basics.md)
- [PyTorch Training Guide](../../user-guide/ray-train/pytorch.md)

#### Hands-on Exercise

Distribute a PyTorch training loop:

```python
import ray
from ray import train
from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer

def train_func():
    import torch
    import torch.nn as nn
    from ray.train import get_context

    # Get distributed context
    context = get_context()

    # Simple model
    model = nn.Linear(10, 1)

    # Prepare for distributed training
    model = train.torch.prepare_model(model)

    # Training loop
    for epoch in range(10):
        loss = torch.tensor(1.0 / (epoch + 1))
        train.report({"loss": loss.item()})

trainer = TorchTrainer(
    train_func,
    scaling_config=ScalingConfig(num_workers=2, use_gpu=False)
)

result = trainer.fit()
print(f"Final loss: {result.metrics['loss']}")
```

#### Checkpoint
You should be able to:
- [ ] Configure distributed training with ScalingConfig
- [ ] Report metrics during training
- [ ] Access training results

---

### Module 3: Hyperparameter Tuning with Ray Tune

**Duration:** 30 minutes

Efficiently search hyperparameter spaces at scale.

#### Topics Covered
- Search spaces and algorithms
- Schedulers for early stopping
- Analyzing results

#### Resources
- [Ray Tune Basics](../../user-guide/ray-tune/basics.md)
- [Tune Search Algorithms](../../user-guide/ray-tune/search-algorithms.md)

#### Hands-on Exercise

Run a hyperparameter search:

```python
from ray import tune
from ray.tune.schedulers import ASHAScheduler

def objective(config):
    for step in range(10):
        score = config["a"] ** 2 + config["b"]
        tune.report(score=score)

search_space = {
    "a": tune.uniform(-10, 10),
    "b": tune.uniform(-5, 5),
}

tuner = tune.Tuner(
    objective,
    param_space=search_space,
    tune_config=tune.TuneConfig(
        num_samples=20,
        scheduler=ASHAScheduler(metric="score", mode="min"),
    ),
)

results = tuner.fit()
best = results.get_best_result(metric="score", mode="min")
print(f"Best config: {best.config}")
```

#### Checkpoint
You should be able to:
- [ ] Define search spaces
- [ ] Configure trial schedulers
- [ ] Retrieve best results

---

### Module 4: Model Serving with Ray Serve

**Duration:** 30 minutes

Deploy models as scalable, production-ready services.

#### Topics Covered
- Deployments and replicas
- Request batching
- Model composition

#### Resources
- [Ray Serve Basics](../../user-guide/ray-serve/basics.md)
- [Serve Configuration](../../user-guide/ray-serve/configuration.md)

#### Hands-on Exercise

Deploy a simple model:

```python
from ray import serve
import ray

ray.init()
serve.start()

@serve.deployment(num_replicas=2)
class Predictor:
    def __init__(self):
        # Load model here
        self.model = lambda x: x * 2

    def __call__(self, request):
        data = request.query_params.get("input", 1)
        return {"result": self.model(float(data))}

# Deploy
app = Predictor.bind()
serve.run(app, route_prefix="/predict")

# Test locally
import requests
response = requests.get("http://localhost:8000/predict?input=5")
print(response.json())  # {"result": 10.0}
```

#### Checkpoint
You should be able to:
- [ ] Create a Serve deployment
- [ ] Configure replicas and resources
- [ ] Send requests to deployed models

---

### Module 5: Putting It All Together

**Duration:** 15 minutes

Build a complete ML pipeline with Ray.

#### Topics Covered
- Combining Ray libraries
- End-to-end workflows
- Best practices

#### Resources
- [ML Pipeline Tutorial](../../how-to/tutorials/ml-pipeline.md)

#### Hands-on Exercise

Create an end-to-end pipeline:

```python
import ray
from ray import train, tune
from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer

# Step 1: Define training function
def train_func(config):
    import torch
    import torch.nn as nn

    model = nn.Linear(10, 1)
    optimizer = torch.optim.SGD(model.parameters(), lr=config["lr"])

    for epoch in range(5):
        loss = torch.tensor(1.0 / (epoch + 1 + config["lr"]))
        train.report({"loss": loss.item()})

# Step 2: Create trainer with tuning
trainer = TorchTrainer(
    train_func,
    scaling_config=ScalingConfig(num_workers=2),
)

# Step 3: Run hyperparameter search
tuner = tune.Tuner(
    trainer,
    param_space={"train_loop_config": {"lr": tune.loguniform(1e-4, 1e-1)}},
    tune_config=tune.TuneConfig(num_samples=5),
)

results = tuner.fit()
best_result = results.get_best_result(metric="loss", mode="min")
print(f"Best learning rate: {best_result.config['train_loop_config']['lr']}")

# Step 4: Deploy best model (conceptual)
# checkpoint = best_result.checkpoint
# model = load_from_checkpoint(checkpoint)
# serve.run(model_deployment)
```

#### Checkpoint
You should be able to:
- [ ] Combine training and tuning
- [ ] Structure end-to-end workflows
- [ ] Understand Ray's ML ecosystem

---

## Next Steps

Congratulations! You've completed the ML Engineer learning path. Here's where to go next:

### Advanced Topics
- [Distributed Data Loading](../../user-guide/ray-data/basics.md) - Scale data preprocessing
- [Advanced Ray Train](../../user-guide/ray-train/advanced.md) - Custom training loops
- [Production Deployment](../../how-to/deployment/production.md) - Deploy to production

### Real-World Examples
- [LLM Fine-tuning](../../how-to/tutorials/llm-finetuning.md)
- [Computer Vision Pipeline](../../how-to/tutorials/cv-pipeline.md)
- [Recommendation System](../../how-to/tutorials/recommender.md)

### Community
- [Ray Forums](https://discuss.ray.io)
- [GitHub](https://github.com/ray-project/ray)
- [Slack Community](https://ray-distributed.slack.com)

## Summary

| Module | Duration | Key Skills |
|--------|----------|------------|
| Getting Started | 15 min | Tasks, actors, objects |
| Ray Train | 30 min | Distributed training |
| Ray Tune | 30 min | Hyperparameter tuning |
| Ray Serve | 30 min | Model serving |
| Full Pipeline | 15 min | End-to-end workflows |

**Total Time:** ~2 hours
