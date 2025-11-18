# Getting Started

> Start your journey with Ray - from installation to building distributed applications.

## Overview

This section helps you get up and running with Ray quickly. Whether you're new to distributed computing or an experienced developer, you'll find the resources you need to start building scalable applications.

## Quick Navigation

### First Steps

1. **[Installation](installation.md)** - Install Ray and verify your setup
2. **[Quick Start](quick-start.md)** - Write your first Ray program in 5 minutes
3. **[Core Concepts](core-concepts.md)** - Understand tasks, actors, and objects

### Learning Paths

Choose a path based on your role:

| Path | Description | Duration |
|------|-------------|----------|
| [ML Engineer](learning-paths/ml-engineer.md) | Train, tune, and serve ML models | 2 hours |
| [Data Engineer](learning-paths/data-engineer.md) | Process large datasets with Ray Data | 2 hours |
| [Platform Engineer](learning-paths/platform-engineer.md) | Deploy and operate Ray clusters | 2.5 hours |

## What You'll Learn

- **Parallelization** - Turn any Python function into a distributed task
- **State Management** - Use actors for stateful computations
- **Data Processing** - Scale data pipelines with Ray Data
- **ML Training** - Distribute training across GPUs/nodes
- **Model Serving** - Deploy models as scalable APIs

## Sample Code

Here's a taste of what you can do with Ray:

```python
import ray

ray.init()

# Parallel tasks
@ray.remote
def process(x):
    return x ** 2

futures = [process.remote(i) for i in range(1000)]
results = ray.get(futures)

# Stateful actors
@ray.remote
class Counter:
    def __init__(self):
        self.count = 0

    def increment(self):
        self.count += 1
        return self.count

counter = Counter.remote()
ray.get([counter.increment.remote() for _ in range(100)])
```

## Next Steps

After completing the getting started guide, explore:

- [User Guide](../user-guide/) - Deep dive into each Ray library
- [How-To Guides](../how-to/) - Solve specific problems
- [API Reference](../reference/) - Complete API documentation

## Getting Help

- [Ray Documentation](https://docs.ray.io)
- [GitHub Discussions](https://github.com/ray-project/ray/discussions)
- [Slack Community](https://ray-distributed.slack.com)
- [Stack Overflow](https://stackoverflow.com/questions/tagged/ray)
