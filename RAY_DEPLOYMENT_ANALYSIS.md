# Ray Deployment and Configuration Management Analysis

## Executive Summary

Ray is a distributed computing framework that supports multiple deployment models through a sophisticated architecture. This report analyzes Ray's deployment options, configuration patterns, and CI/CD infrastructure across cluster management, cloud provider support, build systems, and release automation.

---

## 1. Cluster Deployment Strategies

### 1.1 Deployment Models

Ray supports **multiple deployment models** through pluggable node providers:

#### Cloud Providers (Auto-managed)
- **AWS** (`AWSNodeProvider`): EC2-based clusters with auto-scaling
- **GCP** (`GCPNodeProvider`): Compute Engine with TPU support
- **Azure** (`AzureNodeProvider`): Virtual Machines
- **Alibaba Cloud** (Aliyun): ECS instances
- **vSphere**: On-premises VMware infrastructure

#### Container Orchestration
- **KubeRay** (`KubeRayNodeProvider`): Kubernetes-native clusters via RayCluster CRD
  - Integrates with Kubernetes API for pod management
  - Supports workersToDelete tracking for safe scaling
  - CRD-based configuration synchronization

#### On-Premises
- **Local/Manual** (`LocalNodeProvider`): Pre-provisioned machines
- **Coordinator-based**: Automatic on-premises node management
- **Read-only**: Monitoring existing clusters

#### Development/Testing
- **Fake Multi-Node**: Docker-based testing clusters
- **Fake Multi-Node Docker Provider**: Containerized node simulation

### 1.2 KubeRay Deployment Architecture

File: `/home/user/ray/python/ray/autoscaler/_private/kuberay/`

**Configuration Flow:**
```
RayCluster CR (Kubernetes Custom Resource)
    ↓
AutoscalingConfigProducer (reads from K8s API)
    ↓
Autoscaling Config (internal interface)
    ↓
KubeRay Node Provider
    ↓
Pod provisioning/termination
```

**Key Features:**
- Fetches RayCluster specifications from Kubernetes API server
- Transforms pod specs into autoscaler node types
- Manages worker group scaling via replica count modifications
- Supports upscaling modes: Conservative, Default, Aggressive
- Idle timeout configuration in seconds (`idleTimeoutSeconds`)
- Safe scaling with workersToDelete reconciliation

**Configuration Example (KubeRay):**
```yaml
autoscalerOptions:
  idleTimeoutSeconds: 300  # 5 minutes
  upscalingMode: Default   # Conservative, Default, or Aggressive
```

### 1.3 AWS Deployment Configuration

File: `/home/user/ray/autoscaler/aws/example-minimal.yaml`

**Structure:**
```yaml
cluster_name: aws-example
provider:
  type: aws
  region: us-west-2
  availability_zone: us-west-2a,us-west-2b
  cache_stopped_nodes: True

available_node_types:
  ray.head.default:
    node_config:
      InstanceType: m5.large
  ray.worker.default:
    min_workers: 0
    max_workers: 3
    node_config:
      InstanceType: m5.large
      InstanceMarketOptions:
        MarketType: spot

head_node_type: ray.head.default
max_workers: 3
```

**Key Capabilities:**
- Spot instance support for cost optimization
- Multi-availability zone configuration
- Custom resource mapping per node type
- Automatic IAM role/security group creation
- Instance-specific configuration via boto3 API

### 1.4 Local/On-Premises Deployment

File: `/home/user/ray/autoscaler/_private/local/config.py`

**Two Modes:**

1. **Manual Management:**
   - Pre-configured list of worker IPs
   - User manages node provision/lifecycle
   - Config provides list of available IPs to autoscaler

2. **Coordinator-based:**
   - Automatic node management via coordinator service
   - Requires explicit `max_workers` configuration
   - Scales workers automatically within specified bounds

**Configuration Pattern:**
```yaml
provider:
  type: local
  head_ip: "192.168.1.1"
  worker_ips:
    - "192.168.1.2"
    - "192.168.1.3"

max_workers: 10
min_workers: 0
```

---

## 2. Configuration System

### 2.1 Configuration Hierarchy

Ray uses a **three-tier configuration system**:

#### Tier 1: Environment Variables
File: `/home/user/ray/python/ray/_private/ray_constants.py`

**Configuration Helpers:**
```python
env_integer(key, default)   # Parse integer env vars
env_float(key, default)     # Parse float env vars
env_bool(key, default)      # Parse boolean env vars
env_set_by_user(key)        # Check if user set a var
```

**Key Environment Variables:**
| Variable | Default | Purpose |
|----------|---------|---------|
| `RAY_ADDRESS` | localhost:6379 | Head node connection |
| `RAY_NAMESPACE` | undefined | Job namespace isolation |
| `RAY_RUNTIME_ENV` | undefined | Worker environment setup |
| `RAY_LOG_TO_DRIVER` | True | Log output to driver |
| `RAY_DISABLE_MEMORY_MONITOR` | undefined | Disable memory tracking |
| `RAY_SCHEDULER_EVENTS` | 1 | Enable event logging |

#### Tier 2: Cluster Configuration (YAML)
Files: `/home/user/ray/python/ray/autoscaler/*/defaults.yaml`

**Standard Sections:**
```yaml
cluster_name: cluster-id
max_workers: number
upscaling_speed: float
idle_timeout_minutes: minutes

provider:
  type: aws|gcp|azure|local|kuberay|vsphere
  # Provider-specific config

auth:
  ssh_user: ubuntu
  ssh_private_key: /path/to/key

available_node_types:
  node_type_name:
    min_workers: number
    max_workers: number
    resources: {CPU: N, GPU: N}
    node_config: {...}

initialization_commands: [...]
setup_commands: [...]
head_start_ray_commands: [...]
worker_start_ray_commands: [...]

file_mounts: {remote: local}
docker: {}
```

#### Tier 3: Autoscaler Constants
File: `/home/user/ray/python/ray/autoscaler/_private/constants.py`

**Autoscaler Tuning Parameters:**
```python
# Timing
AUTOSCALER_UPDATE_INTERVAL_S = 5           # Update frequency
AUTOSCALER_HEARTBEAT_TIMEOUT_S = 30        # Node liveness check
AUTOSCALER_NODE_START_WAIT_S = 900         # Node startup timeout
AUTOSCALER_NODE_TERMINATE_WAIT_S = 900     # Node termination timeout

# Scaling Constraints
AUTOSCALER_MAX_LAUNCH_BATCH = 5            # Nodes per request
AUTOSCALER_MAX_CONCURRENT_LAUNCHES = 10    # Parallel launches
AUTOSCALER_MAX_NUM_FAILURES = 5            # Failure threshold
AUTOSCALER_CONSERVE_GPU_NODES = 1          # Avoid GPU for CPU tasks

# Monitoring
AUTOSCALER_STATUS_LOG = 1                  # Enable status logging
AUTOSCALER_METRIC_PORT = 44217             # Prometheus metrics
```

**All configurable via environment variables:**
```bash
export AUTOSCALER_UPDATE_INTERVAL_S=10
export AUTOSCALER_MAX_CONCURRENT_LAUNCHES=20
export RAY_ENABLE_CLUSTER_STATUS_LOG=0
```

### 2.2 Runtime Configuration Application

File: `/home/user/ray/autoscaler/_private/updater.py`

**Configuration Deployment Flow:**

```
Config loaded (YAML)
    ↓
validate_config() - Schema validation
    ↓
hash_launch_conf() - Track infrastructure changes
    ↓
hash_runtime_conf() - Track setup/runtime changes
    ↓
NodeUpdater
    ├── Sync file_mounts (rsync)
    ├── Run initialization_commands (outside container)
    ├── Setup docker (if configured)
    ├── Run setup_commands (inside container)
    ├── Run ray_start_commands
    └── Store runtime config hash in node tags
```

**Configuration Propagation:**
- Launch config → EC2 instance tags (AWS)
- Runtime config → Node metadata tags
- File mounts → Synchronized via rsync
- Docker config → Container environment

### 2.3 Provider Configuration Schema

Each provider implements `prepare_*()` function:

**AWS:** `/home/user/ray/autoscaler/_private/aws/config.py`
- Default AMI per region (Deep Learning AMI)
- Security group auto-creation
- IAM role templates
- Boto3 configuration

**GCP:** `/home/user/ray/autoscaler/_private/gcp/config.py`
- Service account management
- TPU configuration support
- Multi-region availability
- Resource quotas

**Local:** `/home/user/ray/autoscaler/_private/local/config.py`
- On-premise IP validation
- Manual vs. automatic mode selection
- Min/max worker enforcement

---

## 3. Build Process

### 3.1 Build System Architecture

**Technology Stack:**
- **Primary**: Bazel (build system)
- **Python Build**: setuptools
- **Python Version Support**: 3.9, 3.10, 3.11, 3.12, 3.13

File: `/home/user/ray/python/setup.py`

### 3.2 Build Configuration

**Build Types** (via `RAY_DEBUG_BUILD` env var):
```python
class BuildType(Enum):
    DEFAULT = 1      # Normal build
    DEBUG = 2        # Debug symbols
    ASAN = 3         # AddressSanitizer
    TSAN = 4         # ThreadSanitizer
    DEPS_ONLY = 5    # Dependencies only
```

**Build Flags:**
```python
BUILD_CORE = os.getenv("RAY_BUILD_CORE", "1") == "1"           # C++ core
BUILD_JAVA = os.getenv("RAY_INSTALL_JAVA", "0") == "1"        # Java bindings
BUILD_CPP = os.getenv("RAY_DISABLE_EXTRA_CPP") != "1"         # C++ extras
BUILD_REDIS = os.getenv("RAY_BUILD_REDIS", "1") == "1"        # Redis
SKIP_BAZEL_BUILD = os.getenv("SKIP_BAZEL_BUILD") == "1"       # Skip Bazel
```

### 3.3 Wheel Building Pipeline

File: `.buildkite/build.rayci.yml`

**Build Stages:**

1. **Manylinux Preparation** (base docker image)
   - Compiles C++ extensions
   - Builds native components
   - Creates intermediate artifacts

2. **Core Python Build**
   - Builds for Python 3.9, 3.10, 3.11, 3.12, 3.13
   - Uses Bazel: `bazel run //ci/ray_ci:build_in_docker -- wheel --python-version {{matrix}}`
   - Creates wheel packages (.whl)

3. **Docker Image Building**
   - CPU variants
   - CUDA variants (11.7, 11.8, 12.1, 12.3, 12.4, 12.5, 12.6, 12.8)
   - Multiple image types: ray, ray-extra, ray-llm

4. **Java Build**
   - Compiles Java bindings
   - Creates JAR packages
   - Optional (controlled by `RAY_INSTALL_JAVA`)

### 3.4 Docker Image Hierarchy

File: `/home/user/ray/docker/README.md`

**Layer Structure:**
```
ubuntu:22.04
└── base-deps:cpu
    ├── ray:cpu
    │   └── ray-extra:cpu
    │   └── ray-llm:cpu
    └── ray-ml:cpu

nvidia/cuda:XX.X
└── base-deps:cudaXXX
    └── ray:cudaXXX
        └── ray-llm:cudaXXX
```

**Key Base Image Details:**

File: `/home/user/ray/docker/base-deps/Dockerfile`

```dockerfile
# Base: ubuntu:22.04
# Python: 3.9+ via miniforge (conda)
# Package Manager: uv (fast Python package installer)

# Installed Tools:
- openssh-client (autoscaler SSH)
- tmux, screen (node management)
- rsync (file sync)
- cmake, g++ (build tools)
- jemalloc (memory allocator)
```

**Ray-specific Base:**
```dockerfile
# base-ray Dockerfile
FROM rayproject/ray-deps:nightly"$BASE_IMAGE"

# Installs:
- Ray wheel (${WHEEL_PATH})
- All dependencies
- Generates pip-freeze.txt for reproducibility
```

### 3.5 Package Structure

**Included Files** (from setup.py):
```
ray/
├── _raylet.so                          # C++ raylet
├── core/src/ray/gcs/gcs_server        # GCS (Global Control Store)
├── core/src/ray/raylet/raylet          # Local raylet
├── jars/ray_dist.jar                   # Java bindings
├── nightly-wheels.yaml                 # Version info

autoscaler/
├── aws/defaults.yaml
├── gcp/defaults.yaml
├── azure/defaults.yaml
├── local/defaults.yaml
├── ray-schema.json                     # Config validation schema

dashboard/
└── client/build/                       # React frontend
```

### 3.6 Package Extras (Optional Dependencies)

```python
setup_spec.extras = {
    "data": [numpy, pandas, pyarrow, fsspec],
    "serve": [uvicorn, fastapi, starlette],
    "tune": [tensorboardX, scikit-optimize],
    "rllib": [gymnasium, dm_tree, scipy],
    "train": [lightning, torchvision],
    "air": [all ML components],
    "llm": [vllm, nixl, meson],
}
```

---

## 4. Release Process

### 4.1 Release Automation

File: `.buildkite/release-automation/`

**Release Pipeline Stages:**

#### Stage 1: Build & Test
- Bazel cache management
- Build for multiple platforms:
  - Linux x86_64 (Python 3.9, 3.10, 3.11, 3.12, 3.13)
  - Linux arm64 (Python 3.9, 3.10, 3.11, 3.12, 3.13)
  - macOS arm64
  - Windows

#### Stage 2: Upload & Validate
Files: `.buildkite/release-automation/wheels.rayci.yml`

**Process:**
```
Build Wheels (all platforms & Python versions)
    ↓
Upload to TestPyPI (intermediate repository)
    ↓
Validate Installation (per platform & Python)
    ├── Linux x86_64 (4 Python versions)
    ├── Linux arm64 (4 Python versions)
    └── macOS arm64
    ↓
Manual Approval (block step)
    ↓
Upload to Production PyPI
```

**Validation Steps:**
```bash
# For each platform/Python combo:
export PYTHON_VERSION={{matrix}}
bash .buildkite/release-automation/verify-linux-wheels.sh
bash .buildkite/release-automation/verify-macos-wheels.sh
```

### 4.2 Version Management

File: `/home/user/ray/python/ray/_version.py`

**Version Format:**
```python
version = "3.0.0.dev0"
commit = "{{RAY_COMMIT_SHA}}"  # Replaced during build

# Build variant suffixes:
# - debug → "3.0.0.dev0+dbg"
# - asan  → "3.0.0.dev0+asan"
# - tsan  → "3.0.0.dev0+tsan"
```

**Version Setting Script:**
File: `.buildkite/release-automation/set-ray-version.sh`

- Extracts version from _version.py
- Sets RAY_VERSION environment variable
- Tracks commit hash for release

### 4.3 Release Configuration

File: `.buildkite/release-automation/config.yml`

**Infrastructure Setup:**
```yaml
name: ray-release-automation

# Storage for build artifacts
artifacts_bucket: ray-ci-artifact-pr-public
ci_temp: s3://ray-ci-artifact-pr-public/ci-temp/

# Container registry
ci_work_repo: ECR (AWS container registry)
forge_prefix: cr.ray.io/rayproject/

# Build queues (Buildkite agents)
builder_queues:
  builder: x86_64 queue
  builder-arm64: ARM64 queue
  builder-windows: Windows queue

runner_queues:
  default: Linux runners
  macos: macOS runners
  windows: Windows runners

# Cache configuration
env:
  BUILDKITE_BAZEL_CACHE_URL: https://bazel-cache-dev.s3.us-west-2.amazonaws.com
```

### 4.4 Build Matrix

**Platform Matrix:**
- 2 architectures: x86_64, arm64
- 5 Python versions: 3.9-3.13
- Multiple CUDA versions for GPU images
- CPU-only variants

**Parallel Builds:**
```
Linux wheels (5 Python × 2 arch) = 10 wheels
Docker images (5 Python × 8 CUDA variants) = 40 images
Java builds
Dashboard builds
```

---

## 5. Infrastructure Requirements

### 5.1 Cluster Resource Requirements

#### Minimum Resources (Single Node)
```
Head Node:
  - CPU: 1+ cores
  - Memory: 1GB+ (typically 2GB+)
  - Disk: 50GB+

Worker Nodes:
  - Configurable by user
  - Auto-scaling min/max bounds
```

#### Recommended (Production)
```
Head Node (m5.large on AWS):
  - 2 vCPU
  - 8GB RAM
  - 256GB storage (EBS)

Worker Nodes (m5.large):
  - 2 vCPU
  - 8GB RAM
  - Auto-scaling: 0-10+ nodes
```

### 5.2 Platform Support

**Supported Operating Systems:**
- Linux (primary)
  - Ubuntu 18.04, 20.04, 22.04
  - CentOS, RHEL, Rocky Linux
- macOS
  - 10.13+
  - ARM64 (Apple Silicon)
- Windows
  - Server 2019+
  - 10 (WSL2)

**Python Versions:**
- 3.9, 3.10, 3.11, 3.12, 3.13

**Hardware:**
- CPU: x86_64 (primary), arm64 (secondary)
- GPU: NVIDIA CUDA 11.x-12.x, AMD HIP
- TPU: Google Cloud TPU (v2, v3, v4, v5)

### 5.3 Network Requirements

**Ports (Default):**
```
6379    → Ray head node (object store, scheduler)
8076    → Object manager
44217   → Autoscaler Prometheus metrics
5020    → SSH (autoscaler)
```

**Network Features:**
- SSH access for node setup/management
- Kubernetes API access (KubeRay)
- Cloud provider API access (AWS, GCP, Azure)

### 5.4 Autoscaler Infrastructure

**Scaling Parameters:**

| Parameter | Default | Config Override |
|-----------|---------|-----------------|
| Update interval | 5s | `AUTOSCALER_UPDATE_INTERVAL_S` |
| Node startup timeout | 900s | `AUTOSCALER_NODE_START_WAIT_S` |
| Heartbeat timeout | 30s | `AUTOSCALER_HEARTBEAT_TIMEOUT_S` |
| Max concurrent launches | 10 | `AUTOSCALER_MAX_CONCURRENT_LAUNCHES` |
| Launch batch size | 5 | `AUTOSCALER_MAX_LAUNCH_BATCH` |
| Failure threshold | 5 | `AUTOSCALER_MAX_NUM_FAILURES` |
| GPU conservation | Enabled | `AUTOSCALER_CONSERVE_GPU_NODES` |

**Resource Demand Scheduling:**
- Bin-packing algorithm for optimal node allocation
- Placement group support
- Custom resource tracking
- Max demand vector size: 1000 (tunable)

### 5.5 Storage & State Management

**Data Locations:**

1. **Object Store** (on each node)
   - Default: 30% of available memory
   - Min: 75MB
   - Max: 200GB (default limit)
   - Configurable via `RAY_memory` parameter

2. **Cluster State** (on head node)
   - GCS (Global Control Store) - Redis/Persisted state
   - Configuration in node tags/metadata
   - Autoscaler bootstrap config: `~/ray_bootstrap_config.yaml`

3. **Logs** (distributed)
   - Head node: detailed logs
   - Worker nodes: per-task logs
   - Accessible via dashboard

### 5.6 CI/CD Infrastructure

**Build System:**
- Buildkite (primary CI/CD platform)
- S3 for artifact storage
- ECR for Docker images
- Bazel for distributed builds

**Caching:**
- Bazel cache: `bazel-cache-dev.s3.us-west-2.amazonaws.com`
- Docker layer caching
- Conda/pip cache in Docker images

**Testing Infrastructure:**
- Release tests: `release/`
- Benchmarks: `release/benchmarks/`
- Integration tests: Various

---

## 6. Configuration Patterns & Best Practices

### 6.1 Multi-Cloud Strategy

**Example: AWS with GPU Workers**
```yaml
available_node_types:
  cpu.worker:
    min_workers: 2
    node_config:
      InstanceType: m5.large
  gpu.worker:
    min_workers: 0
    max_workers: 4
    node_config:
      InstanceType: g4dn.xlarge  # GPU instance
    resources:
      GPU: 1
```

### 6.2 Kubernetes (KubeRay) Strategy

**Example: KubeRay with Autoscaling**
```yaml
cluster_name: ray-cluster
provider:
  type: kuberay
  namespace: ray-system

available_node_types:
  worker-group-1:
    min_workers: 1
    max_workers: 10
    
autoscalerOptions:
  idleTimeoutSeconds: 300
  upscalingMode: Default
```

### 6.3 Docker in Autoscaler

**Pattern:**
```yaml
docker:
  image: rayproject/ray:latest  # or custom image
  container_name: ray
  pull_before_run: True
  
# Ray commands run inside container
# File mounts work with Docker volumes
```

### 6.4 Custom Node Provisioning

**External Provider Pattern:**
```python
# Custom provider extends NodeProvider
class CustomNodeProvider(NodeProvider):
    def create_node(self, node_config, tags, count, ...):
        # Provision nodes via custom API
        pass
    
    def terminate_node(self, node_id):
        # Terminate nodes
        pass
```

---

## 7. Summary Table: Deployment Options

| Feature | AWS | GCP | Azure | Local | KubeRay |
|---------|-----|-----|-------|-------|---------|
| **Auto-scaling** | ✓ | ✓ | ✓ | ✗ | ✓ |
| **Spot Instances** | ✓ | ✓ | ✓ | ✗ | ✗ |
| **GPU Support** | ✓ | ✓ (TPU) | ✓ | ✓ | ✓ |
| **Multi-region** | ✓ | ✓ | ✓ | ✗ | ✗ |
| **Container Native** | ✗ | ✗ | ✗ | ✗ | ✓ |
| **On-premises** | ✗ | ✗ | ✗ | ✓ | ✓ |
| **SSH Management** | ✓ | ✓ | ✓ | ✓ | ✗ |
| **Config Type** | YAML | YAML | YAML | YAML | CRD |

---

## 8. Key Files Reference

### Configuration
- `/home/user/ray/python/ray/autoscaler/_private/constants.py` - Autoscaler tuning
- `/home/user/ray/python/ray/_private/ray_constants.py` - Ray global configuration
- `/home/user/ray/python/ray/autoscaler/*/defaults.yaml` - Provider defaults

### Deployment
- `/home/user/ray/python/ray/autoscaler/_private/autoscaler.py` - Main autoscaler logic
- `/home/user/ray/python/ray/autoscaler/_private/updater.py` - Node configuration application
- `/home/user/ray/python/ray/autoscaler/_private/providers.py` - Provider registry

### Build
- `/home/user/ray/python/setup.py` - Python package definition
- `/home/user/ray/.buildkite/build.rayci.yml` - Build pipeline
- `/home/user/ray/docker/base-deps/Dockerfile` - Base image

### Release
- `/home/user/ray/.buildkite/release-automation/` - Release automation scripts
- `/home/user/ray/.buildkite/release-automation/wheels.rayci.yml` - Wheel release pipeline
- `/home/user/ray/python/ray/_version.py` - Version management

