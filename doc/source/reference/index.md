# Reference

> Complete reference documentation for Ray APIs and configuration.

## Overview

The Reference section provides comprehensive documentation for all Ray APIs, configuration options, and command-line interfaces. Use this section when you need specific details about function signatures, parameters, and options.

## Contents

### API Reference

Complete API documentation for each library:

- [Ray Core API](api/ray-core.md) - Core primitives (ray.remote, ray.get, etc.)
- [Ray Data API](api/ray-data.md) - Data processing (read, transform, write)
- [Ray Train API](api/ray-train.md) - Distributed training
- [Ray Tune API](api/ray-tune.md) - Hyperparameter tuning
- [Ray Serve API](api/ray-serve.md) - Model serving

### Configuration

Configuration options and environment variables:

- [Configuration Reference](configuration/index.md) - All configuration options
- [Environment Variables](configuration/env-vars.md) - Environment variable reference
- [System Config](configuration/system-config.md) - Advanced system configuration

### Command Line

CLI tools and commands:

- [ray CLI](cli/ray.md) - Main Ray CLI
- [serve CLI](cli/serve.md) - Ray Serve CLI
- [tune CLI](cli/tune.md) - Ray Tune CLI

## Quick Links

### Most Used APIs

```python
# Core
ray.init()
ray.remote
ray.get()
ray.put()
ray.wait()

# Data
ray.data.read_parquet()
ray.data.read_csv()
Dataset.map_batches()

# Train
TorchTrainer
ScalingConfig
train.report()

# Tune
tune.Tuner
tune.TuneConfig
tune.report()

# Serve
@serve.deployment
serve.run()
```

### Most Used Config

```python
ray.init(
    num_cpus=8,
    num_gpus=2,
    object_store_memory=10e9
)
```

## How to Use

1. **Search** - Use Ctrl+F to find specific functions
2. **Navigate** - Use the table of contents
3. **Examples** - Every API includes usage examples
4. **Links** - Click through to related APIs

## Related Resources

- [User Guide](../user-guide/) - Conceptual documentation
- [How-To Guides](../how-to/) - Task-oriented guides
- [Getting Started](../getting-started/) - Tutorials for beginners
