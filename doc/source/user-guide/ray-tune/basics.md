# Ray Tune Basics

> Optimize hyperparameters and run distributed experiments at scale.

## Overview

Ray Tune is a scalable hyperparameter tuning library that helps you find the best model configurations. It supports various search algorithms and schedulers, integrates with popular ML frameworks, and scales from a laptop to a cluster.

## Prerequisites

- Ray installed (`pip install "ray[tune]"`)
- Basic ML knowledge (training, validation, hyperparameters)
- Understanding of [Core Concepts](../../getting-started/core-concepts.md)

## Quick Start

```python
from ray import tune

def objective(config):
    score = config["a"] ** 2 + config["b"]
    return {"score": score}

search_space = {
    "a": tune.uniform(-10, 10),
    "b": tune.uniform(-5, 5),
}

tuner = tune.Tuner(
    objective,
    param_space=search_space,
    tune_config=tune.TuneConfig(num_samples=20),
)

results = tuner.fit()
best = results.get_best_result(metric="score", mode="min")
print(f"Best config: {best.config}")
```

## Detailed Guide

### Defining the Search Space

Specify hyperparameters to tune:

```python
from ray import tune

search_space = {
    # Uniform continuous
    "learning_rate": tune.uniform(0.001, 0.1),

    # Log uniform (for learning rates)
    "lr": tune.loguniform(1e-5, 1e-1),

    # Discrete choices
    "batch_size": tune.choice([16, 32, 64, 128]),

    # Integer range
    "num_layers": tune.randint(1, 10),

    # Grid search
    "optimizer": tune.grid_search(["adam", "sgd"]),

    # Conditional parameters
    "use_dropout": tune.choice([True, False]),
    "dropout_rate": tune.uniform(0.1, 0.5),  # Only used if use_dropout=True
}
```

### Writing Trainable Functions

Create functions that Tune will optimize:

```python
from ray import tune

def train_model(config):
    model = create_model(
        lr=config["learning_rate"],
        layers=config["num_layers"]
    )

    for epoch in range(100):
        loss, accuracy = train_epoch(model)

        # Report metrics to Tune
        tune.report(loss=loss, accuracy=accuracy)

# Run tuning
tuner = tune.Tuner(
    train_model,
    param_space=search_space,
    tune_config=tune.TuneConfig(
        num_samples=50,
        metric="loss",
        mode="min"
    )
)
```

### Search Algorithms

Choose how Tune explores the search space:

```python
from ray.tune.search.optuna import OptunaSearch
from ray.tune.search.hyperopt import HyperOptSearch
from ray.tune.search.bayesopt import BayesOptSearch

# Optuna (Bayesian optimization)
search_alg = OptunaSearch(metric="loss", mode="min")

# HyperOpt (TPE)
search_alg = HyperOptSearch(metric="loss", mode="min")

tuner = tune.Tuner(
    train_model,
    param_space=search_space,
    tune_config=tune.TuneConfig(
        num_samples=50,
        search_alg=search_alg
    )
)
```

### Schedulers

Terminate bad trials early to save resources:

```python
from ray.tune.schedulers import ASHAScheduler, PopulationBasedTraining

# ASHA: Aggressive early stopping
scheduler = ASHAScheduler(
    metric="loss",
    mode="min",
    max_t=100,  # Max training iterations
    grace_period=10,  # Min iterations before stopping
    reduction_factor=2
)

# Population Based Training
scheduler = PopulationBasedTraining(
    metric="loss",
    mode="min",
    perturbation_interval=5,
    hyperparam_mutations={
        "lr": tune.uniform(0.001, 0.1),
        "batch_size": [16, 32, 64]
    }
)

tuner = tune.Tuner(
    train_model,
    param_space=search_space,
    tune_config=tune.TuneConfig(
        num_samples=50,
        scheduler=scheduler
    )
)
```

### Analyzing Results

Examine tuning results:

```python
results = tuner.fit()

# Best trial
best_result = results.get_best_result(metric="loss", mode="min")
print(f"Best config: {best_result.config}")
print(f"Best loss: {best_result.metrics['loss']}")

# All trials as DataFrame
df = results.get_dataframe()
print(df[["config/lr", "loss", "accuracy"]])

# Get checkpoints
checkpoint = best_result.checkpoint
```

### Checkpointing

Save and restore trial state:

```python
from ray import tune
from ray.train import Checkpoint
import tempfile

def train_model(config):
    # Restore from checkpoint if resuming
    checkpoint = tune.get_checkpoint()
    if checkpoint:
        with checkpoint.as_directory() as dir:
            state = load_state(f"{dir}/state.pt")

    for epoch in range(100):
        loss = train_epoch()

        # Save checkpoint
        with tempfile.TemporaryDirectory() as tmpdir:
            save_state(f"{tmpdir}/state.pt")
            tune.report(
                loss=loss,
                checkpoint=Checkpoint.from_directory(tmpdir)
            )
```

## Common Patterns

### Pattern: Integration with Ray Train

**Use case:** Tune distributed training

```python
from ray import tune
from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer

def train_func(config):
    model = create_model(lr=config["lr"])
    for epoch in range(10):
        loss = train_epoch(model)
        ray.train.report({"loss": loss})

trainer = TorchTrainer(
    train_func,
    scaling_config=ScalingConfig(num_workers=4, use_gpu=True)
)

tuner = tune.Tuner(
    trainer,
    param_space={
        "train_loop_config": {
            "lr": tune.loguniform(1e-4, 1e-1)
        }
    },
    tune_config=tune.TuneConfig(num_samples=10)
)

results = tuner.fit()
```

### Pattern: Bayesian Optimization

**Use case:** Efficient search with few samples

```python
from ray.tune.search.optuna import OptunaSearch

search_alg = OptunaSearch(
    metric="accuracy",
    mode="max"
)

tuner = tune.Tuner(
    train_model,
    param_space=search_space,
    tune_config=tune.TuneConfig(
        num_samples=30,
        search_alg=search_alg
    )
)
```

### Pattern: Resume Tuning

**Use case:** Continue interrupted experiment

```python
# First run
tuner = tune.Tuner(
    train_model,
    param_space=search_space,
    run_config=tune.RunConfig(
        name="my_experiment",
        storage_path="/results"
    )
)
results = tuner.fit()

# Resume later
tuner = tune.Tuner.restore(
    "/results/my_experiment",
    trainable=train_model,
    resume_errored=True
)
results = tuner.fit()
```

## Troubleshooting

### Issue: Trials Keep Failing

**Symptoms:** All trials error out

**Cause:** Bug in training code or resource issues

**Solution:** Test locally first:

```python
# Test with specific config
result = train_model({"lr": 0.01, "batch_size": 32})

# Debug with 1 sample
tuner = tune.Tuner(
    train_model,
    param_space=search_space,
    tune_config=tune.TuneConfig(num_samples=1)
)
```

### Issue: Slow Tuning

**Symptoms:** Tuning takes too long

**Cause:** Each trial runs too long

**Solution:** Use early stopping:

```python
from ray.tune.schedulers import ASHAScheduler

scheduler = ASHAScheduler(
    metric="loss",
    mode="min",
    max_t=100,
    grace_period=5,  # Stop bad trials early
    reduction_factor=3
)
```

### Issue: Results Look Wrong

**Symptoms:** Best config has poor performance

**Cause:** Wrong metric or mode

**Solution:** Verify metric and mode:

```python
# For loss (minimize)
tuner = tune.Tuner(
    train_model,
    tune_config=tune.TuneConfig(
        metric="loss",
        mode="min"  # Lower is better
    )
)

# For accuracy (maximize)
tuner = tune.Tuner(
    train_model,
    tune_config=tune.TuneConfig(
        metric="accuracy",
        mode="max"  # Higher is better
    )
)
```

## What's Next

- [Search Algorithms](search-algorithms.md) - All search options
- [Schedulers](schedulers.md) - Early stopping strategies
- [Analysis](analysis.md) - Analyze results

> **Related:** Scale training with [Ray Train](../ray-train/basics.md)
> **Related:** Process data with [Ray Data](../ray-data/basics.md)
> **Related:** Deploy best model with [Ray Serve](../ray-serve/basics.md)

## API Reference

- [`tune.Tuner`](../../reference/api/ray-tune.md#tuner)
- [`tune.TuneConfig`](../../reference/api/ray-tune.md#tuneconfig)
- [`tune.report`](../../reference/api/ray-tune.md#report)
- [Search Space API](../../reference/api/ray-tune.md#search-space)
