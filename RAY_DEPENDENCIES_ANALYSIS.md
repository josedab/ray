# Ray Project: Comprehensive Dependencies and Technology Stack Analysis

**Project Version**: 3.0.0.dev0  
**Analysis Date**: 2025-11-18  
**Repository**: https://github.com/ray-project/ray

---

## Executive Summary

Ray is a sophisticated distributed computing framework written in C++ with Python bindings. It uses a modern polyglot approach with:
- **Primary Build System**: Bazel 6.5.0
- **Core Language**: C++ (C++17)
- **Scripting Language**: Python 3.9-3.13
- **Key RPC Framework**: gRPC v1.57.1
- **Serialization**: Protocol Buffers (protobuf)
- **In-Memory Store**: Redis 7.2.3

---

## 1. Python Dependencies

### Core Runtime Dependencies (Minimal Install)

These are the essential dependencies required for Ray's basic functionality:

| Dependency | Version | Purpose |
|-----------|---------|---------|
| **click** | >= 7.0 | CLI framework for command-line interface |
| **filelock** | Any | File-based locking for concurrency control |
| **jsonschema** | Any | JSON schema validation |
| **msgpack** | >= 1.0.0, < 2.0.0 | Efficient binary serialization |
| **packaging** | Any | Package version parsing and comparison |
| **protobuf** | >= 3.20.3 | Protocol buffers serialization (matches C++ version) |
| **pyyaml** | Any | YAML configuration file parsing |
| **requests** | Any | HTTP client library |

### Optional Feature Dependencies (Extras)

#### Ray Dashboard & Default (`ray[default]`)
```python
aiohttp >= 3.7              # Async HTTP server
aiohttp_cors                # CORS support for HTTP
colorful                    # Colored terminal output
py-spy >= 0.2.0/0.4.0      # Profiling tool (version-dependent on Python)
grpcio >= 1.32.0/1.42.0    # gRPC client (version-dependent on Python)
opencensus                  # Distributed tracing (Google)
opentelemetry-sdk >= 1.30.0 # Observability framework
opentelemetry-exporter-prometheus  # Metrics exporter
opentelemetry-proto         # Observability protobuf definitions
pydantic != 2.0.*, < 3      # Data validation (excludes v2.0-2.4)
prometheus_client >= 0.7.1  # Prometheus metrics
smart_open                  # Cloud-aware file operations
virtualenv >= 20.0.24, != 20.21.1  # Virtual environment management
```

#### Ray Data (`ray[data]`)
```python
numpy >= 1.20
pandas >= 1.3
pyarrow >= 9.0.0
fsspec                      # Filesystem abstraction layer
```

#### Ray Serve (`ray[serve]`)
```python
uvicorn[standard]           # ASGI web server
requests
starlette                   # Lightweight ASGI framework
fastapi                     # Modern web framework
watchfiles                  # File monitoring
+ ray[default] dependencies
```

#### Ray Serve with gRPC (`ray[serve-grpc]`)
```python
grpcio >= 1.32.0/1.42.0
pyOpenSSL                   # SSL/TLS support
+ ray[serve] dependencies
```

#### Ray Tune (`ray[tune]`)
```python
pandas
pydantic (see above)
tensorboardX >= 1.9         # TensorBoard integration
requests
pyarrow >= 9.0.0
fsspec
```

#### Ray RLlib (`ray[rllib]`)
```python
dm_tree                     # Recursive tree utilities
gymnasium == 1.1.1          # Reinforcement learning environment API
lz4                         # Fast compression
ormsgpack == 1.7.0          # High-performance msgpack variant
pyyaml
scipy
+ ray[tune] dependencies
```

#### Ray Train (`ray[train]`)
```python
pydantic (see above)
+ ray[tune] dependencies
```

#### Ray LLM (`ray[llm]`) - *Not included in ray[all]*
```python
vllm[audio] >= 0.11.0       # Large language model inference engine
nixl >= 0.6.1               # Configuration library
jsonref >= 1.1.0            # JSON reference resolution
jsonschema                  # JSON validation
ninja                       # Build system
async-timeout (Python < 3.11)  # Asyncio timeout backport
typer                       # CLI framework
meson                       # Build system
pybind11                    # C++/Python bindings
hf_transfer                 # Fast HuggingFace Hub transfers
+ ray[data] + ray[serve] dependencies
```

#### Ray AI Runtime (`ray[air]`)
```python
# Composite extra combining Data, Tune, Serve, and Train
```

#### Ray All (`ray[all]`) - *Comprehensive but not recommended*
```python
# All of the above except [cpp] and [llm]
# Note: Users should specify needed extras rather than using [all]
```

### ML Framework Support

These are optional integrations for Ray's ML components:

**ML Training Integrations:**
- XGBoost == 2.1.0
- LightGBM == 4.6.0
- Transformers == 4.36.2 (HuggingFace)
- Accelerate == 0.28.0 (HuggingFace)

**ML Tracking/Experiment Management:**
- MLflow >= 2.22.0
- Weights & Biases (wandb) == 0.17.0
- Comet ML == 3.44.1

**Data Processing:**
- Dask/Distributed == 2023.6.1 (Python < 3.12) or 2025.5.0 (Python >= 3.12)
- Modin == 0.22.2 (Python < 3.12) or 0.31.0 (Python >= 3.12)
- Pandas == 1.5.3 (Python < 3.12) or 2.2.2 (Python >= 3.12)
- Daft == 0.4.3
- Bokeh == 2.4.3 (Python < 3.12)

**Reinforcement Learning:**
- Gymnasium == 1.1.1 (standard RL environment API)
- ONNX == 1.15.0 (ONNX model support, excluded on macOS ARM64)
- ONNX Runtime == 1.18.0 (ONNX inference, excluded on macOS ARM64)

---

## 2. C++ Dependencies

### Core C++ Libraries

| Dependency | Version | Purpose | Status |
|-----------|---------|---------|--------|
| **gRPC** | v1.57.1 | RPC communication framework | **Actively Maintained** |
| **Protocol Buffers** | v2c5fa078 (post-3.20) | Message serialization | **Actively Maintained** |
| **Redis** | 7.2.3 | In-memory data store/cache | **Actively Maintained** |
| **Hiredis** | 60e5075d4ac77424809f855ba3e398df7aacefe8 | Redis C client | **Actively Maintained** |
| **Boost** | (via rules_boost) | C++ standard library extensions | **Actively Maintained** |
| **Protobuf** (codegen) | v3.19.4 | Python/Java code generation | **Actively Maintained** |

### Serialization & Data Formats

| Dependency | Version | Purpose | Status |
|-----------|---------|---------|--------|
| **msgpack** | Latest | Efficient binary serialization | **Actively Maintained** |
| **FlatBuffers** | v25.2.10 | Binary serialization format | **Actively Maintained** |
| **nlohmann/json** | v3.9.1 | JSON library for C++ | **Actively Maintained** |
| **RapidJSON** | v1.1.0 | Fast JSON parser | **Actively Maintained** |

### Observability & Metrics

| Dependency | Version | Purpose | Status |
|-----------|---------|---------|--------|
| **OpenTelemetry C++** | v1.19.0 | Distributed tracing/metrics | **Actively Maintained** |
| **OpenTelemetry Proto** | v1.2.0 | OTLP protocol definitions | **Actively Maintained** |
| **OpenCensus C++** | 5e5f2632c84e2230fb7ccb8e336f603d2ec6aa1b | Google's tracing framework | **Maintained** |
| **Prometheus C++** | 60eaa4ea47b16751a8e8740b05fe70914c68a480 | Prometheus metrics | **Actively Maintained** |
| **OpenCensus Proto** | v0.3.0 | OpenCensus protocol buffers | **Maintained** |

### Utilities & Support Libraries

| Dependency | Version | Purpose | Status |
|-----------|---------|---------|--------|
| **spdlog** | v1.15.3 | Fast C++ logging library | **Actively Maintained** |
| **Abseil** | 20230802.1 | Google's C++ common libraries | **Actively Maintained** |
| **Google Test** | v1.14.0 | C++ testing framework | **Actively Maintained** |
| **gflags** | e171aa2d15ed9eb17054558e0b3a6a413bb01067 | Commandline flags library | **Maintained** |

### Build & Compilation Tools

| Dependency | Version | Purpose | Status |
|-----------|---------|---------|--------|
| **Cython** | 3.0.12 | Python/C language for bindings | **Actively Maintained** |
| **jemalloc** | 5.3.0 | High-performance memory allocator | **Actively Maintained** |

### Memory & Performance

| Dependency | Version | Purpose | Status |
|-----------|---------|---------|--------|
| **jemalloc** | 5.3.0 | Memory allocator (Linux only) | **Actively Maintained** |
| **OpenSSL** | 1.1.1f | SSL/TLS cryptography | **Maintained** (1.1.x is legacy, needs upgrade path) |

---

## 3. Build System Architecture

### Primary Build Tool: Bazel 6.5.0

Ray uses **Bazel** as its monolithic build system for all components:

```
WORKSPACE                 # Bazel workspace definition
BUILD.bazel               # Root build configuration
bazel/
├── ray_deps_setup.bzl    # Define C++ third-party dependencies
├── ray_deps_build_all.bzl # Build third-party dependencies
├── ray.bzl               # Ray-specific build rules
├── python.bzl            # Python-specific build rules
├── *.BUILD               # Individual library BUILD files
└── .bazelrc             # Bazel configuration & build flags
```

### Bazel Configuration Highlights

**C++ Standard**: C++17 (all platforms)
```
build:linux --cxxopt="-std=c++17"
build:macos --cxxopt="-std=c++17"
```

**Platform-Specific**:
- Linux: jemalloc enabled (memory optimization)
- Windows: MSVC/Clang-CL support
- macOS: ARM64 (M1/M2) support

**Build Modes**:
- Optimized (default): `-O2`
- Debug: `--config=debug` with symbols
- Address Sanitizer: `--config=asan-build`
- Thread Sanitizer: `--config=tsan`

### Secondary Build System: setuptools

Python packaging uses standard setuptools with custom build extension:
```
python/setup.py           # Primary installation script
pyproject.toml            # Modern Python project metadata
```

---

## 4. Key Frameworks & Architectures

### Remote Procedure Call (RPC)

**gRPC** (v1.57.1) - Primary communication layer
- Bidirectional streaming
- Protocol Buffer message definitions
- Multi-language support
- Load balancing integration
- Ray Client communication

**Important Note**: Ray Client requires specific gRPC versions:
- Python < 3.10: `grpcio >= 1.32.0`
- Python >= 3.10: `grpcio >= 1.42.0`
- macOS: Pinned to `grpcio == 1.54.2` (due to compatibility issues)

### Serialization Stack

1. **Protocol Buffers** (protobuf)
   - Core message definitions
   - Generated Python bindings in `ray/core/generated/`
   - Generated for Ray Serve in `ray/serve/generated/`
   
2. **msgpack** (1.0.0 - 1.x)
   - Task/object serialization
   - Lightweight and fast
   - C++ and Python support

3. **FlatBuffers** (v25.2.10)
   - Zero-copy binary serialization
   - Backward/forward compatibility

### In-Memory Storage

**Redis 7.2.3**
- Object store and metadata storage
- Built from source as part of Ray
- Cross-platform binaries (Linux x86_64, ARM64, macOS ARM64)
- Integrated Hiredis client library

### Observability Stack

**OpenTelemetry** (Modern standard)
- Core SDK v1.30.0+
- Prometheus exporter
- Protocol buffer definitions

**OpenCensus** (Legacy support)
- Distributed tracing
- Prometheus metrics export
- Google's original tracing framework

---

## 5. Version Requirements & Platform Support

### Python Version Support

**Supported Versions**: Python 3.9, 3.10, 3.11, 3.12, 3.13

```python
# From setup.py
SUPPORTED_PYTHONS = [(3, 9), (3, 10), (3, 11), (3, 12), (3, 13)]
python_requires = ">=3.9"
```

**Version-Specific Adjustments**:
- py-spy: >= 0.2.0 (Python < 3.12), >= 0.4.0 (Python >= 3.12)
- grpcio: >= 1.32.0 (Python < 3.10), >= 1.42.0 (Python >= 3.10)
- Data/ML packages: Different versions for Python < 3.12 vs >= 3.12

### Operating System Support

**Primary Platforms**:
- Linux (x86_64, ARM64) - Full support
- macOS (Intel, ARM64/M1/M2)
- Windows (native, not WSL)

**Build Requirements**:
- C++17 compatible compiler
- Python development headers
- Bazel 6.5.0 exactly

### Conditional Dependencies

**By Platform**:
- `cupy-cuda12x`: GPU array library (Linux/Windows only, excluded macOS)
- `memray`: Memory profiler (all platforms except Windows)
- `py-spy`: Performance profiler (version-dependent on Python)

**By Architecture**:
- `jemalloc`: Memory allocator (Linux only)
- ONNX/ONNX Runtime: Excluded on macOS ARM64

---

## 6. Development & Testing Dependencies

### Core Development Requirements

```
cython >= 3.0.12          # Python/C compiler
setuptools                # Package building
wheel                     # Wheel format support
pytest                    # Testing framework
```

### CI/Build Dependencies

```
psutil                    # System utilities (vendored)
colorama                  # Colored output
aiohttp                   # For runtime environment agents
bazelisk                  # Bazel version manager
```

---

## 7. Dependency Maintenance Status

### Actively Maintained (Regular Updates)
- **gRPC** - Latest v1.57.1, Google-backed
- **Protocol Buffers** - Regular releases
- **Redis** - Version 7.2.3, actively developed
- **OpenTelemetry** - CNCF project, production-ready
- **Boost** - Community-maintained build rules

### Regularly Updated
- **FlatBuffers** - v25.2.10, Google project
- **Abseil** - Google's C++ libs, actively used internally
- **Google Test** - Actively maintained

### Potentially Requires Attention
- **OpenSSL 1.1.x** - Legacy version, planned deprecation in 2024
  - Recommendation: Plan upgrade to OpenSSL 3.x
- **OpenCensus** - Superseded by OpenTelemetry
  - Note: Ray maintains both for compatibility

---

## 8. Dependency Interaction Map

```
┌─────────────────────────────────────────────────────────┐
│         Ray Core (C++17, C++ bindings)                 │
├─────────────────────────────────────────────────────────┤
│  ┌──────────────────────────────────────────────────┐  │
│  │ Communication: gRPC + Protocol Buffers + msgpack │  │
│  │ Storage: Redis (Hiredis client)                 │  │
│  │ Serialization: FlatBuffers, msgpack             │  │
│  └──────────────────────────────────────────────────┘  │
│                                                         │
│  ┌──────────────────────────────────────────────────┐  │
│  │ Observability:                                   │  │
│  │ - OpenTelemetry (SDK + Prometheus exporter)     │  │
│  │ - OpenCensus (Legacy support)                   │  │
│  │ - Prometheus C++ client                         │  │
│  └──────────────────────────────────────────────────┘  │
│                                                         │
│  ┌──────────────────────────────────────────────────┐  │
│  │ Core Libraries: Boost, Abseil, spdlog           │  │
│  │ Memory: jemalloc (Linux)                         │  │
│  │ Security: OpenSSL (TLS/SSL)                      │  │
│  └──────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
           │
           │ Python Bindings (Cython)
           ↓
┌─────────────────────────────────────────────────────────┐
│      Ray Python API (Python 3.9-3.13)                  │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  Core Modules:                                         │
│  - click (CLI), filelock, jsonschema, pyyaml          │
│  - requests (HTTP client)                              │
│                                                         │
│  Ray Serve (FastAPI + Uvicorn + Starlette)           │
│  Ray Data (pandas, numpy, pyarrow, fsspec)            │
│  Ray Tune (TensorBoard, pydantic)                      │
│  Ray RLlib (gymnasium, scipy, dm_tree)                │
│  Ray LLM (vLLM)                                        │
│                                                         │
│  Dashboard:                                            │
│  - aiohttp, prometheus_client                          │
│                                                         │
│  Observability:                                        │
│  - opentelemetry-sdk, opencensus                       │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

---

## 9. Dependency Statistics

| Category | Count | Total Dependencies |
|----------|-------|-------------------|
| Core Python Runtime | 8 | 8 |
| Optional Extras (all) | ~40+ | ~48+ |
| C++ Libraries (direct) | 20+ | 20+ |
| Transitive C++ (via gRPC, Boost, etc.) | 30+ | 50+ |
| ML Framework Integrations | 6 | 6 |
| **Total Direct Dependencies** | **28** | - |
| **Total with Transitive** | **150+** | - |

---

## 10. Risk Assessment & Recommendations

### Critical Dependencies
1. **gRPC** - Core communication, well-maintained ✓
2. **Redis** - Data storage, well-maintained ✓
3. **Protocol Buffers** - Message format, Google-backed ✓

### Potential Issues
1. **OpenSSL 1.1.x** - Legacy version
   - Timeline: OpenSSL 1.1 EOL was September 2023
   - Impact: Security updates limited
   - **Action**: Plan upgrade to OpenSSL 3.x
   
2. **Python 3.9 EOL** - October 2025
   - Impact: Deprecation approaching
   - **Action**: Consider dropping 3.9 support in future major version

3. **Pydantic Constraint** - Excludes 2.0-2.4
   - Reason: Known incompatibilities
   - Impact: Limits upgrade path
   - **Action**: Monitor Pydantic releases for resolution

### Advantages
- Well-chosen, actively-maintained core dependencies
- Minimal transitive dependency bloat
- Clear separation between core and optional ML dependencies
- Good use of modern standards (OpenTelemetry, Protobuf)

---

## Appendix: File Locations

**Configuration Files**:
- `/home/user/ray/python/setup.py` - Main installation
- `/home/user/ray/pyproject.toml` - Project metadata
- `/home/user/ray/python/requirements.txt` - Python deps
- `/home/user/ray/WORKSPACE` - Bazel workspace
- `/home/user/ray/BUILD.bazel` - Root build
- `/home/user/ray/.bazelrc` - Bazel config
- `/home/user/ray/bazel/ray_deps_setup.bzl` - C++ deps
- `/home/user/ray/bazel/ray_deps_build_all.bzl` - Dep build rules

**Version Files**:
- `/home/user/ray/python/ray/_version.py` - Current: 3.0.0.dev0
- `/home/user/ray/.bazelversion` - Bazel 6.5.0

