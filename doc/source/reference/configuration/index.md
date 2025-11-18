# Configuration Reference

> Comprehensive reference for all Ray configuration options.

## Overview

This page documents all configuration options for Ray runtime, organized by component. Each option includes its type, default value, environment variable equivalent, and usage examples.

## Quick Navigation

- [Ray Init Options](#ray-init-options)
- [Object Store](#object-store)
- [Memory Management](#memory-management)
- [Scheduling](#scheduling)
- [Networking](#networking)
- [Logging and Monitoring](#logging-and-monitoring)

---

## Ray Init Options

### address

- **Type:** `str`
- **Default:** `None` (local cluster)
- **Description:** Address of the Ray cluster to connect to. Use `"auto"` to connect to an existing cluster.

#### Example

```python
# Local cluster
ray.init()

# Connect to existing cluster
ray.init(address="auto")

# Connect to specific address
ray.init(address="ray://192.168.1.100:10001")
```

### num_cpus

- **Type:** `int`
- **Default:** Auto-detected
- **Description:** Number of CPUs available to this node. Overrides auto-detection.

#### Example

```python
ray.init(num_cpus=8)
```

#### Recommendations
- Leave as auto-detect for most cases
- Reduce to leave CPUs for system processes
- Useful in containerized environments

### num_gpus

- **Type:** `int`
- **Default:** Auto-detected
- **Description:** Number of GPUs available to this node.

#### Example

```python
ray.init(num_gpus=4)
```

### resources

- **Type:** `dict`
- **Default:** `{}`
- **Description:** Custom resources available on this node.

#### Example

```python
ray.init(resources={"special_hardware": 2, "TPU": 8})

@ray.remote(resources={"special_hardware": 1})
def use_special_hardware():
    pass
```

---

## Object Store

### object_store_memory

- **Type:** `int` (bytes)
- **Default:** 30% of system memory (min 1GB, max 200GB)
- **Environment:** `RAY_OBJECT_STORE_MEMORY`
- **Description:** Memory allocated to the object store (Plasma).

#### Example

```python
ray.init(object_store_memory=8 * 1024**3)  # 8GB
```

```bash
export RAY_OBJECT_STORE_MEMORY=8000000000
```

#### Recommendations
- Minimum 1GB for production workloads
- Increase for workloads with large objects
- Enable spilling if memory is limited
- Max is typically 200GB due to shared memory limits

### _plasma_directory

- **Type:** `str`
- **Default:** `/dev/shm` (Linux), `/tmp` (macOS)
- **Environment:** `RAY_PLASMA_DIRECTORY`
- **Description:** Directory for the object store memory-mapped files.

#### Example

```python
ray.init(_plasma_directory="/mnt/fast-storage")
```

#### Recommendations
- Use tmpfs or ramdisk for best performance
- Use SSD if shared memory is limited

### object_spilling_config

- **Type:** `dict`
- **Default:** Disabled
- **Description:** Configuration for spilling objects to disk when object store is full.

#### Example

```python
ray.init(
    _system_config={
        "object_spilling_config": {
            "type": "filesystem",
            "params": {
                "directory_path": "/tmp/ray-spill",
                "buffer_size": 1000000
            }
        }
    }
)
```

```bash
export RAY_object_spilling_config='{"type":"filesystem","params":{"directory_path":"/tmp/spill"}}'
```

#### Recommendations
- Enable for memory-constrained environments
- Use fast SSD for spill directory
- Set buffer_size based on object sizes

---

## Memory Management

### _memory

- **Type:** `int` (bytes)
- **Default:** Auto-detected
- **Description:** Total memory available to Ray on this node.

#### Example

```python
ray.init(_memory=16 * 1024**3)  # 16GB
```

### _redis_max_memory

- **Type:** `int` (bytes)
- **Default:** 10GB
- **Environment:** `RAY_REDIS_MAX_MEMORY`
- **Description:** Maximum memory for the GCS Redis instance.

#### Recommendations
- Increase for clusters with many objects/actors
- Monitor Redis memory usage

---

## Scheduling

### scheduling_strategy

Available scheduling strategies for tasks and actors.

### DEFAULT

The default scheduling strategy.

```python
@ray.remote
def task():
    pass
```

### SPREAD

Spread tasks across nodes.

```python
@ray.remote(scheduling_strategy="SPREAD")
def task():
    pass
```

### PlacementGroupSchedulingStrategy

Schedule within a placement group.

```python
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

pg = ray.util.placement_group([{"CPU": 1}])

@ray.remote
def task():
    pass

task.options(
    scheduling_strategy=PlacementGroupSchedulingStrategy(pg)
).remote()
```

### NodeAffinitySchedulingStrategy

Schedule on specific nodes.

```python
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

@ray.remote
def task():
    pass

task.options(
    scheduling_strategy=NodeAffinitySchedulingStrategy(
        node_id=ray.get_runtime_context().get_node_id(),
        soft=False
    )
).remote()
```

---

## Networking

### dashboard_host

- **Type:** `str`
- **Default:** `localhost`
- **Environment:** `RAY_DASHBOARD_HOST`
- **Description:** Host to bind the dashboard server.

#### Example

```python
ray.init(dashboard_host="0.0.0.0")  # Accessible externally
```

### dashboard_port

- **Type:** `int`
- **Default:** `8265`
- **Environment:** `RAY_DASHBOARD_PORT`
- **Description:** Port for the dashboard server.

#### Example

```python
ray.init(dashboard_port=8080)
```

### _node_ip_address

- **Type:** `str`
- **Default:** Auto-detected
- **Description:** IP address of the node.

#### Recommendations
- Use when auto-detection fails
- Required in some multi-NIC environments

### _raylet_ip_address

- **Type:** `str`
- **Default:** Same as node IP
- **Description:** IP address for raylet communication.

---

## Logging and Monitoring

### logging_level

- **Type:** `str` or `int`
- **Default:** `logging.INFO`
- **Environment:** `RAY_LOGGING_LEVEL`
- **Description:** Logging level for Ray.

#### Example

```python
import logging
ray.init(logging_level=logging.DEBUG)
```

```bash
export RAY_LOGGING_LEVEL=DEBUG
```

### log_to_driver

- **Type:** `bool`
- **Default:** `True`
- **Description:** Whether to push logs from workers to the driver.

#### Example

```python
ray.init(log_to_driver=False)  # Disable for cleaner output
```

### include_dashboard

- **Type:** `bool`
- **Default:** `True`
- **Description:** Whether to start the dashboard.

#### Example

```python
ray.init(include_dashboard=False)  # Disable dashboard
```

---

## Runtime Environment

### runtime_env

- **Type:** `dict`
- **Default:** `{}`
- **Description:** Runtime environment for the cluster.

#### Options

| Key | Description |
|-----|-------------|
| `pip` | Pip dependencies |
| `conda` | Conda environment |
| `env_vars` | Environment variables |
| `working_dir` | Working directory |
| `py_modules` | Python modules |
| `container` | Container image |

#### Example

```python
ray.init(runtime_env={
    "pip": ["pandas==1.5.0", "numpy"],
    "env_vars": {"MY_VAR": "value"},
    "working_dir": "./project"
})

# Per-task runtime environment
@ray.remote(runtime_env={"pip": ["scipy"]})
def task():
    import scipy
    return scipy.__version__
```

---

## Environment Variables Reference

| Variable | Description | Default |
|----------|-------------|---------|
| `RAY_ADDRESS` | Default cluster address | None |
| `RAY_OBJECT_STORE_MEMORY` | Object store memory | 30% of RAM |
| `RAY_memory_monitor_refresh_ms` | Memory monitor interval | 250 |
| `RAY_LOGGING_LEVEL` | Logging level | INFO |
| `RAY_DASHBOARD_HOST` | Dashboard host | localhost |
| `RAY_DASHBOARD_PORT` | Dashboard port | 8265 |
| `RAY_PLASMA_DIRECTORY` | Object store directory | /dev/shm |
| `RAY_ENABLE_RECORD_ACTOR_TASK_LOGGING` | Enable actor task logging | 0 |
| `RAY_DEDUP_LOGS` | Deduplicate logs | 1 |

---

## System Configuration

Advanced configuration options passed via `_system_config`:

```python
ray.init(
    _system_config={
        "automatic_object_spilling_enabled": True,
        "max_io_workers": 4,
        "min_spilling_size": 100 * 1024 * 1024,  # 100MB
        "object_spilling_threshold": 0.8,
    }
)
```

### Common System Config Options

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `automatic_object_spilling_enabled` | bool | True | Enable automatic spilling |
| `object_spilling_threshold` | float | 0.8 | Spilling threshold |
| `max_io_workers` | int | 4 | Workers for I/O operations |
| `min_spilling_size` | int | 50MB | Minimum size to spill |
| `raylet_heartbeat_period_milliseconds` | int | 1000 | Heartbeat interval |
| `num_heartbeats_timeout` | int | 30 | Heartbeats before timeout |

---

## See Also

- [Performance Tuning](../../how-to/performance/index.md)
- [Cluster Configuration](../../cluster/configuration.md)
- [Ray Core API](../api/ray-core.md)
