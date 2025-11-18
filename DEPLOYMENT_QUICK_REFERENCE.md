# Ray Deployment & Configuration - Quick Reference

## 1. Environment Variables - Most Important

```bash
# Connection
RAY_ADDRESS=head-node-ip:6379

# Cluster Behavior
RAY_ENABLE_CLUSTER_STATUS_LOG=1              # Enable autoscaler logs
RAY_SCHEDULER_EVENTS=1                        # Enable event logging
RAY_LOG_TO_DRIVER=true                        # Print logs to driver

# Autoscaler Tuning
AUTOSCALER_UPDATE_INTERVAL_S=5                # Check every 5s (lower = faster scaling)
AUTOSCALER_MAX_CONCURRENT_LAUNCHES=10         # Max 10 nodes in parallel
AUTOSCALER_MAX_LAUNCH_BATCH=5                 # Launch in batches of 5
AUTOSCALER_NODE_START_WAIT_S=900              # 15 min timeout for node startup
AUTOSCALER_HEARTBEAT_TIMEOUT_S=30             # 30s node heartbeat timeout
AUTOSCALER_CONSERVE_GPU_NODES=1               # Don't use GPU nodes for CPU tasks

# Memory Management  
RAY_DEFAULT_OBJECT_STORE_MEMORY_BYTES=209715200000  # 200GB object store
RAY_DEFAULT_OBJECT_STORE_MEMORY_PROPORTION=0.3      # Or 30% of RAM
```

## 2. Configuration File Structure

### Minimal AWS Cluster
```yaml
cluster_name: my-cluster
max_workers: 10
provider:
  type: aws
  region: us-west-2

available_node_types:
  ray.head.default:
    node_config:
      InstanceType: m5.large
  ray.worker.default:
    min_workers: 1
    max_workers: 10
    node_config:
      InstanceType: m5.large

head_node_type: ray.head.default
```

### Minimal Kubernetes (KubeRay) Cluster
```yaml
provider:
  type: kuberay
  namespace: default

available_node_types:
  workers:
    min_workers: 1
    max_workers: 10

autoscalerOptions:
  idleTimeoutSeconds: 300
  upscalingMode: Default
```

## 3. Build & Release Quick Facts

| Component | Technology | Config File |
|-----------|-----------|-------------|
| Python Build | setuptools + Bazel | `/python/setup.py` |
| Build Automation | Buildkite | `.buildkite/build.rayci.yml` |
| Release Automation | Buildkite | `.buildkite/release-automation/wheels.rayci.yml` |
| Version Management | setuptools_scm | `python/ray/_version.py` |
| Docker Base | Ubuntu 22.04 + miniforge | `docker/base-deps/Dockerfile` |
| Python Versions | 3.9, 3.10, 3.11, 3.12, 3.13 | Configurable at build time |

## 4. Node Lifecycle

```
Config loaded (YAML)
    ↓
Provider creates node (AWS EC2, GCP VM, etc.)
    ↓
NodeUpdater initializes
    ├── Waits for SSH (default 5s checks, max 900s)
    ├── Syncs file_mounts via rsync
    ├── Runs initialization_commands
    ├── Sets up Docker (optional)
    ├── Runs setup_commands
    ├── Starts Ray (ray_start_commands)
    └── Stores config hash in node tags
    ↓
Node ready for workload
    ↓
Autoscaler monitors liveness (heartbeat every 30s)
    ↓
On termination: graceful shutdown or force terminate
```

## 5. Cloud Provider Features

### AWS
- **Autoscaling**: Yes, via EC2 API
- **Spot Instances**: Yes (InstanceMarketOptions: {MarketType: spot})
- **Multi-region**: Yes (specify regions)
- **Default AMI**: Deep Learning AMI with CUDA pre-installed
- **Security**: Auto-creates VPC, security groups, IAM roles

### GCP
- **Autoscaling**: Yes, via Compute API
- **TPU Support**: Yes (v2, v3, v4, v5)
- **Service Accounts**: Auto-created with minimal required roles
- **Multiple zones**: Yes

### Azure
- **Autoscaling**: Yes, via Azure API
- **VM types**: Full Azure VM catalog support
- **Resource groups**: Organized by cluster

### Local/On-Premises
- **Autoscaling**: Manual only (user provides IPs)
- **Coordinator mode**: Can auto-provision from coordinator service
- **Manual mode**: Pre-provision all nodes, autoscaler manages allocation

### Kubernetes (KubeRay)
- **Autoscaling**: Yes, via replica count changes
- **Native**: Uses RayCluster CRD
- **No SSH**: Pod management via K8s API
- **Upscaling modes**: Conservative, Default, Aggressive

## 6. Autoscaler Configuration Parameters

```python
# File: python/ray/autoscaler/_private/constants.py

# How often autoscaler checks and scales (seconds)
AUTOSCALER_UPDATE_INTERVAL_S = 5

# Node liveness check interval (seconds)  
AUTOSCALER_HEARTBEAT_TIMEOUT_S = 30

# How long to wait for node to start (seconds)
AUTOSCALER_NODE_START_WAIT_S = 900  # 15 min

# Maximum nodes to start per batch
AUTOSCALER_MAX_LAUNCH_BATCH = 5

# Maximum concurrent node launches
AUTOSCALER_MAX_CONCURRENT_LAUNCHES = 10

# Failure count before aborting
AUTOSCALER_MAX_NUM_FAILURES = 5

# Maximum resource demand vectors to track
AUTOSCALER_MAX_RESOURCE_DEMAND_VECTOR_SIZE = 1000

# Prometheus metrics port
AUTOSCALER_METRIC_PORT = 44217
```

## 7. Docker Image Variants

**Available Types:**
- `ray:latest` / `ray:latest-cpu` - Minimal Ray + dependencies
- `ray:latest-cudaXX.X` - Ray + CUDA toolkit
- `ray-ml:latest` - Ray + ML libraries (PyTorch, TensorFlow, etc.)
- `ray-llm:latest` - Ray + LLM libraries (vLLM, etc.)

**Installed in all:**
- miniconda/miniforge (Python package manager)
- SSH client (for autoscaler)
- rsync (for file sync)
- Ray package (wheels)

## 8. Release Process Overview

1. **Build Phase** (.buildkite/build.rayci.yml)
   - Compiles C++ raylet and GCS
   - Builds wheels for 5 Python versions × 2 architectures
   - Builds Docker images for each Python/CUDA combo
   - Tests on Bazel cache

2. **Test Phase** (release-automation)
   - Upload to TestPyPI
   - Install and validate on each platform/Python
   - Verify functionality

3. **Release Phase**
   - Manual approval gate
   - Upload to Production PyPI
   - Build Docker images pushed to DockerHub

4. **Version Format**
   - Development: `3.0.0.dev0`
   - Release: `3.0.0`
   - Debug variants: `3.0.0.dev0+dbg|asan|tsan`

## 9. File Locations Reference

### Configuration Defaults
```
python/ray/autoscaler/aws/defaults.yaml
python/ray/autoscaler/gcp/defaults.yaml
python/ray/autoscaler/azure/defaults.yaml
python/ray/autoscaler/local/defaults.yaml
python/ray/autoscaler/*/example-*.yaml
```

### Autoscaler Core
```
python/ray/autoscaler/_private/autoscaler.py       # Main loop
python/ray/autoscaler/_private/updater.py          # Node setup
python/ray/autoscaler/_private/providers.py        # Provider registry
python/ray/autoscaler/_private/kuberay/            # KubeRay support
```

### Build & Release
```
python/setup.py                                     # Package definition
.buildkite/build.rayci.yml                         # Build pipeline
.buildkite/release-automation/wheels.rayci.yml     # Release pipeline
docker/base-deps/Dockerfile                        # Base image
python/ray/_version.py                             # Version info
```

## 10. Common Troubleshooting

**Node won't start:**
- Check SSH access: `ssh -i ~/.ssh/key.pem user@node-ip`
- Check node status: `ray exec cluster.yaml "echo working"`
- Increase timeout: `AUTOSCALER_NODE_START_WAIT_S=1800`

**Autoscaler not scaling:**
- Enable logs: `RAY_ENABLE_CLUSTER_STATUS_LOG=1`
- Check metrics: `curl localhost:44217` (port 44217)
- Monitor autoscaler: `ray dashboard cluster.yaml`
- Verify config: `ray validate-config cluster.yaml`

**Docker issues:**
- Rebuild image: `ray up -f cluster.yaml --no-config-cache`
- Check SSH keys in Docker setup
- Verify file mounts work with Docker volumes

**Memory issues:**
- Check object store: `ray memory`
- Adjust: `RAY_DEFAULT_OBJECT_STORE_MEMORY_BYTES=X`
- Monitor worker memory usage in dashboard

**GPU not detected:**
- Verify instance type has GPU
- Check CUDA installed on image
- Test: `ray exec cluster.yaml "python -c 'import torch; print(torch.cuda.is_available())'"` 

