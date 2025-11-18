# Platform Engineer Learning Path

> Master Ray cluster deployment, management, and operations.

---

**Duration:** 2.5 hours
**Difficulty:** Intermediate to Advanced

---

## Prerequisites

- Linux/Unix system administration
- Kubernetes basics (for K8s deployment)
- Cloud platform familiarity (AWS, GCP, or Azure)
- Basic understanding of distributed systems

## Overview

This learning path teaches you how to deploy, configure, and operate Ray clusters in production. You'll learn cluster management, monitoring, autoscaling, and troubleshooting—essential skills for supporting Ray workloads at scale.

## Modules

### Module 1: Ray Architecture Overview

**Duration:** 20 minutes

Understand Ray's distributed architecture.

#### Topics Covered
- Head node vs worker nodes
- Global Control Store (GCS)
- Object store and plasma
- Raylet and scheduling

#### Resources
- [Ray Architecture](../../architecture/internals/architecture.md)
- [Core Concepts](../core-concepts.md)

#### Key Concepts

```
┌─────────────────────────────────────────────┐
│                 Ray Cluster                 │
├─────────────────┬───────────────────────────┤
│   Head Node     │      Worker Nodes         │
│  ┌───────────┐  │  ┌──────┐  ┌──────┐      │
│  │    GCS    │  │  │Worker│  │Worker│ ...  │
│  │  Driver   │  │  │Raylet│  │Raylet│      │
│  │  Raylet   │  │  │ObjStr│  │ObjStr│      │
│  └───────────┘  │  └──────┘  └──────┘      │
└─────────────────┴───────────────────────────┘
```

#### Checkpoint
You should be able to:
- [ ] Explain the role of each Ray component
- [ ] Describe how tasks are scheduled
- [ ] Understand object transfer between nodes

---

### Module 2: Cluster Deployment Options

**Duration:** 30 minutes

Learn different ways to deploy Ray clusters.

#### Topics Covered
- Local clusters
- VM-based clusters
- Kubernetes deployment with KubeRay

#### Resources
- [Cluster Deployment](../../how-to/deployment/index.md)
- [Kubernetes Guide](../../user-guide/cluster/kubernetes.md)

#### Hands-on Exercise

Deploy a local cluster:

```bash
# Start a head node
ray start --head --port=6379 --dashboard-port=8265

# Start worker nodes (on different machines)
ray start --address='<head-node-ip>:6379'

# Verify cluster
ray status
```

Deploy on Kubernetes:

```yaml
# raycluster.yaml
apiVersion: ray.io/v1
kind: RayCluster
metadata:
  name: my-cluster
spec:
  rayVersion: '2.9.0'
  headGroupSpec:
    rayStartParams:
      dashboard-host: '0.0.0.0'
    template:
      spec:
        containers:
        - name: ray-head
          image: rayproject/ray:2.9.0
          resources:
            limits:
              cpu: "2"
              memory: "4Gi"
  workerGroupSpecs:
  - replicas: 3
    minReplicas: 1
    maxReplicas: 10
    groupName: workers
    rayStartParams: {}
    template:
      spec:
        containers:
        - name: ray-worker
          image: rayproject/ray:2.9.0
          resources:
            limits:
              cpu: "2"
              memory: "4Gi"
```

```bash
kubectl apply -f raycluster.yaml
```

#### Checkpoint
You should be able to:
- [ ] Start a multi-node cluster manually
- [ ] Deploy a cluster on Kubernetes
- [ ] Connect to a running cluster

---

### Module 3: Configuration and Resources

**Duration:** 30 minutes

Configure Ray for optimal performance.

#### Topics Covered
- Resource configuration
- Object store settings
- Environment variables
- Runtime environment

#### Resources
- [Configuration Reference](../../reference/configuration/index.md)
- [Resource Management](../../how-to/performance/resources.md)

#### Hands-on Exercise

Configure a production cluster:

```python
import ray

ray.init(
    address="auto",
    runtime_env={
        "pip": ["pandas", "numpy"],
        "env_vars": {"MY_VAR": "value"},
    },
    _system_config={
        "object_spilling_config": {
            "type": "filesystem",
            "params": {"directory_path": "/tmp/spill"}
        }
    }
)
```

Set resource limits:

```yaml
# Kubernetes resource configuration
resources:
  limits:
    cpu: "4"
    memory: "8Gi"
    nvidia.com/gpu: "1"
  requests:
    cpu: "2"
    memory: "4Gi"
```

Key environment variables:

```bash
export RAY_OBJECT_STORE_MEMORY=10000000000  # 10GB
export RAY_memory_monitor_refresh_ms=100
export RAY_ENABLE_RECORD_ACTOR_TASK_LOGGING=1
```

#### Checkpoint
You should be able to:
- [ ] Configure object store memory
- [ ] Set up runtime environments
- [ ] Manage GPU and custom resources

---

### Module 4: Monitoring and Observability

**Duration:** 30 minutes

Monitor cluster health and debug issues.

#### Topics Covered
- Ray Dashboard
- Prometheus metrics
- Logging configuration
- Profiling tools

#### Resources
- [Observability Guide](../../ray-observability/index.md)
- [Dashboard Guide](../../ray-observability/dashboard.md)

#### Hands-on Exercise

Set up Prometheus monitoring:

```yaml
# prometheus.yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'ray'
    static_configs:
      - targets: ['<head-node>:8080']
```

Export metrics:

```python
from ray.util.metrics import Counter, Gauge, Histogram

requests_counter = Counter(
    "requests_total",
    description="Total requests",
    tag_keys=("endpoint",)
)

latency_histogram = Histogram(
    "request_latency_seconds",
    description="Request latency",
    boundaries=[0.1, 0.5, 1.0, 5.0]
)

# Use in your code
requests_counter.inc(tags={"endpoint": "/predict"})
latency_histogram.observe(0.3)
```

Configure logging:

```python
import logging

# Set Ray logging level
logging.getLogger("ray").setLevel(logging.INFO)
logging.getLogger("ray.data").setLevel(logging.WARNING)
```

#### Checkpoint
You should be able to:
- [ ] Access and use the Ray Dashboard
- [ ] Set up Prometheus metrics
- [ ] Configure logging levels

---

### Module 5: Autoscaling

**Duration:** 20 minutes

Automatically scale clusters based on demand.

#### Topics Covered
- Autoscaler architecture
- Scaling policies
- Cloud provider integration

#### Resources
- [Autoscaling Guide](../../cluster/autoscaling.md)
- [KubeRay Autoscaler](../../cluster/kubernetes/autoscaling.md)

#### Hands-on Exercise

Configure autoscaling on Kubernetes:

```yaml
apiVersion: ray.io/v1
kind: RayCluster
metadata:
  name: autoscaling-cluster
spec:
  enableInTreeAutoscaling: true
  autoscalerOptions:
    upscalingMode: Default
    idleTimeoutSeconds: 60
  workerGroupSpecs:
  - replicas: 1
    minReplicas: 1
    maxReplicas: 10
    groupName: gpu-workers
    rayStartParams:
      num-gpus: "1"
```

Monitor autoscaling:

```bash
# Check autoscaler status
ray status

# View autoscaler logs
kubectl logs -l ray.io/node-type=head -c autoscaler
```

#### Checkpoint
You should be able to:
- [ ] Configure min/max replicas
- [ ] Understand scaling decisions
- [ ] Debug autoscaling issues

---

### Module 6: High Availability and Fault Tolerance

**Duration:** 20 minutes

Build resilient Ray deployments.

#### Topics Covered
- GCS fault tolerance
- Task and actor recovery
- Checkpointing strategies

#### Resources
- [Fault Tolerance](../../ray-core/fault-tolerance.md)
- [GCS HA](../../cluster/kubernetes/gcs-ha.md)

#### Hands-on Exercise

Enable GCS fault tolerance:

```yaml
apiVersion: ray.io/v1
kind: RayCluster
spec:
  headGroupSpec:
    rayStartParams:
      redis-password: 'password'
    template:
      spec:
        containers:
        - name: ray-head
          env:
          - name: RAY_REDIS_ADDRESS
            value: "redis:6379"
```

Configure actor recovery:

```python
@ray.remote(max_restarts=3, max_task_retries=2)
class ResilientActor:
    def __init__(self):
        self.state = self.load_checkpoint()

    def save_checkpoint(self):
        # Save state to persistent storage
        pass

    def load_checkpoint(self):
        # Load state from persistent storage
        return {}
```

#### Checkpoint
You should be able to:
- [ ] Set up GCS with external Redis
- [ ] Configure task and actor retries
- [ ] Implement checkpoint/recovery patterns

---

## Troubleshooting Guide

### Common Issues

#### Cluster won't start

```bash
# Check port availability
netstat -tlnp | grep 6379

# Check firewall rules
iptables -L -n

# Verify node connectivity
ping <head-node-ip>
```

#### Out of memory

```bash
# Check object store usage
ray memory

# Enable object spilling
export RAY_OBJECT_STORE_MEMORY=5000000000
export RAY_object_spilling_config='{"type":"filesystem","params":{"directory_path":"/tmp/spill"}}'
```

#### Tasks stuck pending

```bash
# Check resource availability
ray status

# Look for resource requests
ray list tasks --filter "state=PENDING_ARGS_AVAIL"
```

---

## Next Steps

### Advanced Topics
- [Multi-Cluster Management](../../how-to/deployment/multi-cluster.md)
- [Security Hardening](../../ray-security/index.md)
- [Performance Optimization](../../how-to/performance/index.md)

### Certifications and Training
- [Ray Certification Program](https://www.anyscale.com/certification)
- [Advanced Operations Workshop](https://www.anyscale.com/training)

### Community
- [Ray Slack #operations channel](https://ray-distributed.slack.com)
- [GitHub Discussions](https://github.com/ray-project/ray/discussions)

## Summary

| Module | Duration | Key Skills |
|--------|----------|------------|
| Architecture | 20 min | System understanding |
| Deployment | 30 min | Cluster setup |
| Configuration | 30 min | Performance tuning |
| Monitoring | 30 min | Observability |
| Autoscaling | 20 min | Resource management |
| Fault Tolerance | 20 min | Resilience |

**Total Time:** ~2.5 hours
