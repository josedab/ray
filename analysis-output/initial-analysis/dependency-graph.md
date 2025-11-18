# Ray Dependency Graph

> **Commit:** `d1cce8c9dc8411fad7cfbd619350bec6f19839a3`

## Visual Dependency Map

### High-Level Architecture Dependencies

```
┌─────────────────────────────────────────────────────────────┐
│                    USER APPLICATIONS                         │
│         (ML Training, Inference, Data Processing)            │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                   RAY LIBRARIES LAYER                        │
├──────────┬──────────┬──────────┬──────────┬─────────────────┤
│ Ray Data │Ray Train │Ray Serve │ Ray Tune │     RLlib       │
│ (pandas) │(PyTorch) │(FastAPI) │(optuna)  │  (gymnasium)    │
└────┬─────┴────┬─────┴────┬─────┴────┬─────┴────────┬────────┘
     │          │          │          │              │
┌────▼──────────▼──────────▼──────────▼──────────────▼────────┐
│                     RAY CORE PYTHON                          │
│         (ray.remote, ray.get, ray.put, actors)               │
│                                                              │
│  Dependencies: click, msgpack, pyyaml, requests, jsonschema  │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                  CYTHON BINDINGS                             │
│                    (_raylet.pyx)                             │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                     C++ CORE                                 │
├──────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │ CoreWorker  │  │   Raylet    │  │        GCS          │  │
│  │  (gRPC)     │  │  (gRPC)     │  │  (gRPC + Redis)     │  │
│  └──────┬──────┘  └──────┬──────┘  └──────────┬──────────┘  │
│         │                │                    │              │
│  ┌──────▼────────────────▼────────────────────▼──────────┐  │
│  │              SHARED INFRASTRUCTURE                     │  │
│  │  gRPC | Protobuf | Abseil | Boost | spdlog | jemalloc │  │
│  └───────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                   EXTERNAL SERVICES                          │
├────────────────────┬────────────────────┬────────────────────┤
│   Redis (GCS)      │   Object Store     │   Cloud Providers  │
│                    │   (Plasma/mmap)    │   (AWS/GCP/Azure)  │
└────────────────────┴────────────────────┴────────────────────┘
```

## Python Dependencies

### Core Dependencies (Required)

| Package | Version | Purpose |
|---------|---------|---------|
| `click` | >=7.0 | CLI framework |
| `msgpack` | >=1.0.0,<2.0.0 | Binary serialization |
| `protobuf` | >=3.15.3,!=3.19.5 | Protocol buffer support |
| `pyyaml` | - | YAML config parsing |
| `requests` | - | HTTP client |
| `jsonschema` | - | Config validation |
| `filelock` | - | File-based locking |
| `packaging` | - | Version parsing |

### Serialization & Data

| Package | Version | Purpose |
|---------|---------|---------|
| `cloudpickle` | bundled | Function/closure serialization |
| `pyarrow` | >=6.0.1 | Arrow columnar data, Plasma |
| `pandas` | - | DataFrame support |
| `numpy` | - | Array operations |

### Web & API

| Package | Version | Purpose |
|---------|---------|---------|
| `fastapi` | - | Ray Serve HTTP framework |
| `starlette` | - | ASGI framework |
| `uvicorn` | - | ASGI server |
| `aiohttp` | - | Async HTTP client |
| `grpcio` | >=1.32.0 | gRPC Python bindings |

### Observability

| Package | Version | Purpose |
|---------|---------|---------|
| `prometheus-client` | - | Metrics export |
| `opentelemetry-api` | - | Tracing API |
| `opentelemetry-sdk` | - | Tracing SDK |
| `tensorboardX` | - | Training visualization |

### ML Framework Integrations

| Package | Purpose | Library |
|---------|---------|---------|
| `torch` | PyTorch support | Ray Train |
| `tensorflow` | TensorFlow support | Ray Train |
| `xgboost` | XGBoost training | Ray Train |
| `lightgbm` | LightGBM training | Ray Train |
| `transformers` | HuggingFace models | Ray Train |
| `vllm` | LLM inference | Ray LLM |
| `gymnasium` | RL environments | RLlib |

## C++ Dependencies

### Communication Layer

| Library | Version | Purpose |
|---------|---------|---------|
| gRPC | 1.57.1 | RPC framework |
| Protocol Buffers | post-3.20 | Message serialization |
| Hiredis | - | Redis C client |

### Core Libraries

| Library | Version | Purpose |
|---------|---------|---------|
| Abseil | - | C++ utilities (from Google) |
| Boost | - | C++ standard extensions |
| nlohmann/json | - | JSON parsing |
| FlatBuffers | 25.2.10 | Zero-copy serialization |
| msgpack-c | - | MessagePack serialization |

### Observability

| Library | Version | Purpose |
|---------|---------|---------|
| OpenTelemetry | 1.19.0 | Distributed tracing |
| OpenCensus | - | Metrics (legacy) |
| spdlog | - | Fast logging |

### Memory & Performance

| Library | Purpose |
|---------|---------|
| jemalloc | Memory allocator (Linux) |
| mimalloc | Memory allocator (alternative) |

### Storage

| Library | Version | Purpose |
|---------|---------|---------|
| Redis | 7.2.3 | GCS backend storage |
| Plasma | bundled | Object store |

## Build System Dependencies

| Tool | Version | Purpose |
|------|---------|---------|
| Bazel | 6.5.0 (exact) | Primary build system |
| setuptools | - | Python packaging |
| Cython | - | Python-C++ bindings |
| cmake | - | Some native builds |

## Component Dependency Flow

### Task Execution Flow

```
User Code
    │
    ▼
ray.remote() ─────► RemoteFunction
    │                    │
    │                    ▼
    │              cloudpickle
    │                    │
    ▼                    ▼
CoreWorker ◄────── Serialized Task
    │
    ├─────► gRPC ─────► Raylet (scheduling)
    │
    ├─────► gRPC ─────► GCS (metadata)
    │
    └─────► Plasma ───► Object Store
```

### Data Flow Dependencies

```
ray.put(data)
    │
    ▼
Serialization Layer
    ├─── cloudpickle (general Python)
    ├─── pyarrow (DataFrames, Tables)
    └─── custom (tensors, special types)
    │
    ▼
Plasma Object Store
    │
    ▼
ray.get() ──► Deserialization ──► Python Object
```

## Dependency Compatibility Matrix

### Python Version Support

| Python | Status | Notes |
|--------|--------|-------|
| 3.9 | Supported | EOL Oct 2025 |
| 3.10 | Supported | |
| 3.11 | Supported | |
| 3.12 | Supported | |
| 3.13 | Supported | Latest |

### Platform Support

| Platform | Architecture | Status |
|----------|-------------|--------|
| Linux | x86_64 | Full support |
| Linux | arm64 | Full support |
| macOS | x86_64 | Full support |
| macOS | arm64 (M1/M2) | Full support |
| Windows | x86_64 | Limited support |

## Dependency Health Assessment

### Actively Maintained

- gRPC (Google)
- Protocol Buffers (Google)
- OpenTelemetry (CNCF)
- FastAPI
- PyArrow (Apache)

### Potential Concerns

| Dependency | Issue | Risk Level |
|------------|-------|------------|
| OpenSSL 1.1.x | EOL Sept 2023 | Medium |
| Python 3.9 | EOL Oct 2025 | Low |
| Pydantic constraints | Version conflicts | Low |

## License Compatibility

| Dependency | License | Compatible |
|------------|---------|------------|
| gRPC | Apache 2.0 | Yes |
| Protocol Buffers | BSD-3 | Yes |
| Redis | BSD-3 | Yes |
| Boost | BSL-1.0 | Yes |
| PyArrow | Apache 2.0 | Yes |

Ray itself is licensed under Apache 2.0, and all major dependencies are compatible.

## Version Pinning Strategy

### Strict Pinning
- Bazel: Exact version required (6.5.0)
- CI/CD tools: Pinned for reproducibility

### Range Pinning
- Python packages: Minimum versions with upper bounds
- Example: `msgpack>=1.0.0,<2.0.0`

### Unpinned
- Many optional dependencies
- User responsible for version management

## Recommendations

### Dependency Updates Needed

1. **OpenSSL migration** - Move to OpenSSL 3.x
2. **Python 3.9 deprecation planning** - Plan for EOL
3. **Pydantic v2** - Consider upgrade path

### Security Considerations

Run regular audits:
```bash
# Python
pip-audit

# npm (for dashboard)
npm audit
```

---

*This dependency graph provides a comprehensive view of Ray's technology stack and component interactions.*
