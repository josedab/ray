# Part 6: Observability, Security, and Production Deployment

> **Series:** Ray Deep Dive | **Reading Time:** 15 minutes
> **Commit:** `d1cce8c9dc8411fad7cfbd619350bec6f19839a3`

## What You'll Learn

- Ray's observability stack: metrics, tracing, logging
- Security model and hardening options
- Deployment patterns for production
- Operational best practices

## Introduction

Running Ray in production requires understanding its observability features, security model, and deployment patterns. In this final post, we'll cover everything you need to operate Ray reliably at scale.

## Observability Stack

Ray provides comprehensive observability through metrics, tracing, logging, and the dashboard.

### Metrics (Prometheus)

Ray exports Prometheus-format metrics at `/metrics`:

```python
# Enable metrics export
ray.init(
    _metrics_export_port=8080,
    _system_config={
        "metrics_report_interval_ms": 10000
    }
)
```

**Key metrics:**

| Metric | Description |
|--------|-------------|
| `ray_tasks_running` | Currently running tasks |
| `ray_actors_running` | Currently running actors |
| `ray_object_store_memory_used_bytes` | Object store usage |
| `ray_object_store_num_objects` | Number of objects stored |
| `ray_gcs_request_latency_seconds` | GCS request latency |

**Grafana dashboard example:**

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'ray'
    static_configs:
      - targets: ['<head-node>:8080']
```

**Key files:** [`python/ray/_private/metrics_agent.py`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/python/ray/_private/metrics_agent.py)

### Custom Metrics

```python
from ray.util.metrics import Counter, Gauge, Histogram

# Create metrics
requests = Counter("requests", description="Total requests")
active_connections = Gauge("active_connections", description="Active connections")
latency = Histogram("request_latency", description="Request latency")

@ray.remote
class Service:
    def handle(self, request):
        requests.inc()
        active_connections.set(self.connection_count)

        with latency.observe():
            result = process(request)

        return result
```

### Distributed Tracing (OpenTelemetry)

```python
# Enable tracing
ray.init(_tracing_startup_hook="ray.util.tracing.tracing_helper:setup_otel")

# Configure exporter
import os
os.environ["RAY_TRACING_ENABLED"] = "1"
os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"] = "http://jaeger:4317"
```

Traces show:
- Task submission and execution
- Actor method calls
- Object transfers
- GCS operations

**Key files:** [`python/ray/util/tracing/`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/python/ray/util/tracing/)

### Logging

Ray provides structured logging with aggregation:

```python
# Configure logging
ray.init(
    logging_level="INFO",
    log_to_driver=True  # Stream worker logs to driver
)

# In your code
import logging
logger = logging.getLogger(__name__)

@ray.remote
def task():
    logger.info("Processing started", extra={"task_id": "123"})
    return result
```

**Log locations:**
- Driver: stdout/stderr
- Workers: `/tmp/ray/session_*/logs/`
- Dashboard: Aggregated view

**Structured logging configuration:**

```python
from ray._private.ray_logging import LoggingConfig

ray.init(
    _system_config={
        "worker_slow_start_threshold_ms": 5000
    }
)
```

### Ray Dashboard

Access at `http://<head-node>:8265`

**Features:**
- **Cluster view:** Node status, resources
- **Jobs:** Active and historical jobs
- **Actors:** Actor list with state and logs
- **Tasks:** Task execution history
- **Logs:** Aggregated logs from all nodes
- **Metrics:** Built-in charts

**REST API:**

```python
import requests

# Get cluster status
response = requests.get("http://head-node:8265/api/cluster_status")
status = response.json()

# List actors
response = requests.get("http://head-node:8265/api/actors")
actors = response.json()
```

**Key files:** [`python/ray/dashboard/`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/python/ray/dashboard/)

### State API

Programmatic access to cluster state:

```python
from ray.util.state import (
    list_actors,
    list_tasks,
    list_objects,
    list_nodes,
    get_actor,
    get_task
)

# List all actors
actors = list_actors(
    filters=[("state", "=", "ALIVE")],
    detail=True
)

for actor in actors:
    print(f"{actor.actor_id}: {actor.class_name}")
    print(f"  Node: {actor.node_id}")
    print(f"  PID: {actor.pid}")

# Get task details
task = get_task(task_id)
print(f"State: {task.state}")
print(f"Duration: {task.end_time - task.start_time}ms")
```

## Security Model

### Design Philosophy

Ray's security model delegates to infrastructure:

> "Ray is designed for trusted environments. Enable TLS and authentication for production."

**Key points:**
- TLS disabled by default
- No built-in RBAC
- Code execution by design (no sandboxing)

### Enabling TLS

```python
# Generate certificates (example)
# openssl req -x509 -nodes -newkey rsa:4096 \
#   -keyout server.key -out server.crt -days 365

ray.init(
    _node_ip_address="0.0.0.0",
)

# Or via environment variables
# RAY_USE_TLS=1
# RAY_TLS_SERVER_CERT=/path/to/server.crt
# RAY_TLS_SERVER_KEY=/path/to/server.key
# RAY_TLS_CA_CERT=/path/to/ca.crt
```

**Key files:** [`python/ray/_private/tls_utils.py`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/python/ray/_private/tls_utils.py)

### Authentication

```python
# Enable token authentication
# RAY_AUTH_MODE=token
# RAY_AUTH_TOKEN=<your-secret-token>

ray.init(
    address="ray://head-node:10001",
    _credentials={"token": "your-secret-token"}
)
```

**Authentication flow:**
1. Client connects with token
2. GCS validates token
3. Connection established or rejected

**Key files:** [`python/ray/_private/authentication/`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/python/ray/_private/authentication/)

### Production Security Checklist

1. **Enable TLS** for all communication
2. **Enable authentication** (token or Kubernetes)
3. **Network isolation** (VPC, firewall rules)
4. **No public exposure** of Ray ports
5. **Trusted code only** - no untrusted user code
6. **Separate clusters** for different tenants

### Network Requirements

| Port | Service | Protocol |
|------|---------|----------|
| 6379 | GCS (Redis) | TCP |
| 8265 | Dashboard | HTTP |
| 10001 | Client | gRPC |
| 8076 | Object manager | TCP |
| 8077 | Node manager | gRPC |

**Firewall rules:**
- Allow internal cluster communication
- Restrict external access to dashboard (or disable)
- Use VPN/bastion for admin access

## Deployment Patterns

### Local Development

```python
# Single-node cluster
ray.init()

# With specific resources
ray.init(num_cpus=4, num_gpus=1)
```

### Cloud Deployment (AWS Example)

```yaml
# cluster.yaml
cluster_name: production

provider:
    type: aws
    region: us-west-2

auth:
    ssh_user: ubuntu

available_node_types:
    head:
        node_config:
            InstanceType: m5.2xlarge
        resources:
            CPU: 8
    worker:
        node_config:
            InstanceType: m5.4xlarge
        resources:
            CPU: 16
        min_workers: 2
        max_workers: 10

# Launch cluster
# ray up cluster.yaml
```

### Kubernetes (KubeRay)

```yaml
# raycluster.yaml
apiVersion: ray.io/v1
kind: RayCluster
metadata:
  name: production
spec:
  rayVersion: '2.10.0'
  headGroupSpec:
    rayStartParams:
      dashboard-host: '0.0.0.0'
    template:
      spec:
        containers:
          - name: ray-head
            image: rayproject/ray:2.10.0
            resources:
              limits:
                cpu: "4"
                memory: "8Gi"
  workerGroupSpecs:
    - replicas: 3
      minReplicas: 1
      maxReplicas: 10
      rayStartParams: {}
      template:
        spec:
          containers:
            - name: ray-worker
              image: rayproject/ray:2.10.0
              resources:
                limits:
                  cpu: "8"
                  memory: "16Gi"
```

**Key benefits of KubeRay:**
- Native Kubernetes integration
- Automatic pod management
- Rolling updates
- Resource quotas

**Key files:** [`python/ray/autoscaler/_private/kuberay/`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/python/ray/autoscaler/_private/kuberay/)

### High Availability

For GCS fault tolerance:

```yaml
# Enable GCS HA with Redis
provider:
  type: aws
  head_node_type: head

head_setup_commands:
  - ray start --head --redis-password="password" \
      --gcs-server-port=6379 \
      --object-manager-port=8076
```

Or use external Redis:

```python
ray.init(
    address="ray://head:10001",
    _redis_address="redis-cluster:6379",
    _redis_password="password"
)
```

## Operational Best Practices

### Health Monitoring

```python
import ray
from ray.util.state import list_nodes

def check_cluster_health():
    """Check cluster health and alert on issues."""

    # Check node status
    nodes = list_nodes()
    dead_nodes = [n for n in nodes if n.state == "DEAD"]

    if dead_nodes:
        alert(f"Dead nodes: {len(dead_nodes)}")

    # Check object store
    memory = ray.cluster_resources().get("object_store_memory", 0)
    used = ray._private.internal_api.get_store_stats().get("object_store_bytes_used", 0)

    if used / memory > 0.9:
        alert("Object store > 90% full")

    # Check GCS
    try:
        ray.get_actor("test", namespace="health")
    except:
        pass  # Expected

# Run periodically
```

### Graceful Shutdown

```python
# In your application
import signal
import ray

def shutdown_handler(signum, frame):
    print("Shutting down gracefully...")

    # Cancel running tasks
    # (application-specific)

    ray.shutdown()
    exit(0)

signal.signal(signal.SIGTERM, shutdown_handler)
```

### Log Aggregation

Integrate with your logging stack:

```yaml
# Fluent Bit configuration
[INPUT]
    Name tail
    Path /tmp/ray/session_*/logs/*.log
    Tag ray

[OUTPUT]
    Name elasticsearch
    Match ray
    Host elasticsearch
    Port 9200
    Index ray-logs
```

### Backup and Recovery

```python
# Checkpoint important state
@ray.remote
class StatefulService:
    def __init__(self):
        self.state = self.load_checkpoint()

    def checkpoint(self):
        # Save to persistent storage
        save_to_s3(self.state, "s3://bucket/checkpoint")

    def process(self, data):
        result = compute(data)
        self.state.update(result)

        # Periodic checkpoint
        if self.should_checkpoint():
            self.checkpoint()

        return result
```

### Capacity Planning

**Memory:**
- Object store: 30-50% of RAM
- Worker memory: Depends on workload
- Buffer: 20% for spikes

**CPU:**
- 1 CPU per worker (default)
- Account for system overhead

**Network:**
- Object transfers can saturate network
- Plan for peak data movement

### Autoscaling Configuration

```yaml
# Autoscaler settings
upscaling_speed: 1.0
idle_timeout_minutes: 5

available_node_types:
  worker:
    min_workers: 2
    max_workers: 100
    resources:
      CPU: 16
      GPU: 4
```

**Tuning tips:**
- Set `min_workers` for baseline capacity
- Adjust `idle_timeout_minutes` for cost/latency trade-off
- Use spot instances for cost savings

## Troubleshooting Guide

### Common Issues

#### 1. Tasks Stuck Pending

**Symptoms:** Tasks stay in PENDING state

**Causes:**
- Insufficient resources
- Dependencies unavailable
- Worker failures

**Resolution:**

```python
from ray.util.state import list_tasks

# Find pending tasks
pending = list_tasks(filters=[("state", "=", "PENDING_NODE_ASSIGNMENT")])

for task in pending:
    print(f"Task {task.task_id}:")
    print(f"  Required: {task.required_resources}")
    print(f"  Scheduling state: {task.scheduling_state}")
```

#### 2. Out of Memory

**Symptoms:** `RayOutOfMemoryError`

**Resolution:**

```python
# Check memory usage
from ray._private.internal_api import memory_summary
print(memory_summary())

# Enable spilling
ray.init(_system_config={
    "object_spilling_config": {
        "type": "filesystem",
        "params": {"directory_path": "/tmp/ray_spill"}
    }
})
```

#### 3. Slow Object Transfer

**Symptoms:** Long `ray.get()` times

**Resolution:**

```python
# Check object locations
from ray.util.state import list_objects

objects = list_objects(detail=True)
for obj in objects:
    print(f"{obj.object_id}: {obj.object_size} bytes")
    print(f"  Locations: {obj.node_id}")
```

### Debug Mode

```python
# Enable verbose logging
ray.init(
    logging_level="DEBUG",
    _system_config={
        "verbose_spill_logs": 1,
        "report_worker_backlog": True
    }
)
```

## Summary

We've covered Ray's full operational stack:

1. **Observability:** Metrics, tracing, logging, dashboard
2. **Security:** TLS, authentication, network isolation
3. **Deployment:** Cloud, Kubernetes, high availability
4. **Operations:** Monitoring, capacity planning, troubleshooting

Ray provides the tools needed for production operation, but requires careful configuration for security and observability.

## Series Conclusion

Over these six posts, we've explored Ray from its core architecture to production deployment:

1. **Architecture:** Layered design with GCS, Raylet, Plasma
2. **Deep Dive:** Task lifecycle, actors, object store
3. **Patterns:** Design patterns, testing, error handling
4. **Extensions:** Building on Ray Data, Train, Serve, Tune
5. **Performance:** Optimization, profiling, tuning
6. **Operations:** Observability, security, deployment

Ray's power comes from its simple programming model backed by sophisticated distributed systems engineering. We hope this series helps you build and operate Ray applications effectively.

## Code References

- Metrics: [`python/ray/_private/metrics_agent.py`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/python/ray/_private/metrics_agent.py)
- Tracing: [`python/ray/util/tracing/`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/python/ray/util/tracing/)
- Dashboard: [`python/ray/dashboard/`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/python/ray/dashboard/)
- Security: [`python/ray/_private/authentication/`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/python/ray/_private/authentication/)
- Autoscaler: [`python/ray/autoscaler/`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/python/ray/autoscaler/)

---

*Thank you for reading the Ray Deep Dive series!*
