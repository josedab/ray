# Installation

> Get Ray installed and ready to use on your system.

## Overview

Ray can be installed via pip or conda, with optional dependencies for specific use cases like machine learning or data processing. This guide covers all installation methods and helps you choose the right setup for your needs.

## Prerequisites

- Python 3.8, 3.9, 3.10, 3.11, or 3.12
- pip or conda package manager
- 64-bit system (Linux, macOS, or Windows)

## Quick Start

Install Ray with minimal dependencies:

```bash
pip install ray
```

Verify the installation:

```python
import ray
ray.init()
print(ray.cluster_resources())
ray.shutdown()
```

## Detailed Guide

### Installation Options

Ray offers several installation variants depending on your use case:

#### Default Installation

Basic Ray installation for distributed computing:

```bash
pip install ray
```

#### Ray with All Dependencies

Install Ray with all optional dependencies:

```bash
pip install "ray[all]"
```

#### Library-Specific Installations

Install only what you need:

```bash
# For machine learning workflows
pip install "ray[train]"

# For data processing
pip install "ray[data]"

# For model serving
pip install "ray[serve]"

# For hyperparameter tuning
pip install "ray[tune]"

# For reinforcement learning
pip install "ray[rllib]"
```

### Using Conda

Install Ray using conda:

```bash
conda install -c conda-forge ray
```

### Docker Installation

Use the official Ray Docker images:

```bash
# CPU-only
docker pull rayproject/ray:latest

# GPU support
docker pull rayproject/ray-ml:latest-gpu
```

Run a Ray container:

```bash
docker run -it rayproject/ray:latest python -c "import ray; ray.init(); print('Ray is working!')"
```

### Building from Source

For development or custom builds:

```bash
git clone https://github.com/ray-project/ray.git
cd ray/python
pip install -e . --verbose
```

## Common Patterns

### Pattern: Virtual Environment Setup

**Use case:** Isolate Ray installation from system Python

```bash
# Create virtual environment
python -m venv ray-env
source ray-env/bin/activate  # Linux/macOS
# ray-env\Scripts\activate  # Windows

# Install Ray
pip install "ray[all]"
```

### Pattern: GPU-Enabled Installation

**Use case:** Using Ray with CUDA for GPU workloads

```bash
# Ensure CUDA toolkit is installed
nvidia-smi

# Install Ray with GPU support
pip install "ray[all]"
```

## Troubleshooting

### Issue: Import Error After Installation

**Symptoms:** `ModuleNotFoundError: No module named 'ray'`

**Cause:** Ray not installed in the active Python environment

**Solution:** Verify your Python environment:

```bash
which python  # Check active Python
pip list | grep ray  # Check if Ray is installed
```

### Issue: Incompatible Python Version

**Symptoms:** Installation fails with version errors

**Cause:** Python version not supported

**Solution:** Check your Python version:

```bash
python --version  # Should be 3.8-3.12
```

### Issue: Missing Dependencies on Linux

**Symptoms:** Installation fails with missing system libraries

**Cause:** Missing system-level dependencies

**Solution:** Install required system packages:

```bash
# Ubuntu/Debian
sudo apt-get update
sudo apt-get install -y build-essential curl

# CentOS/RHEL
sudo yum groupinstall -y "Development Tools"
```

## What's Next

- [Quick Start](quick-start.md) - Write your first Ray program
- [Core Concepts](core-concepts.md) - Understand Ray's fundamental concepts
- [Learning Paths](learning-paths/) - Follow a guided path for your role

## API Reference

- [`ray.init`](../reference/api/ray-core.md#ray-init) - Initialize Ray runtime
- [Configuration Options](../reference/configuration/index.md) - Runtime configuration
