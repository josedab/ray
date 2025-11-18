# Ray Repository Structure

> **Commit:** `d1cce8c9dc8411fad7cfbd619350bec6f19839a3`

## Top-Level Directory Overview

```
ray/
├── python/           # Python implementation (2,505 files)
├── src/              # C++ core implementation (788 files)
├── java/             # Java API bindings (367 files)
├── cpp/              # C++ API wrappers (77 files)
├── doc/              # Documentation (610 files)
├── release/          # Release tests and automation
├── ci/               # CI/CD scripts and configuration
├── docker/           # Docker build configurations
├── .buildkite/       # Buildkite CI pipelines
├── .github/          # GitHub workflows and templates
└── rllib/            # Reinforcement learning library (913 files)
```

## Python Directory (`/python/ray/`)

The primary API and implementation layer.

### Core Modules

| Directory | Purpose | Key Files |
|-----------|---------|-----------|
| `_private/` | Internal implementation | `worker.py` (3,795 LOC), `serialization.py` |
| `_raylet.pyx` | Cython bindings to C++ | 196KB, main Python-C++ bridge |
| `actor.py` | Actor abstraction | 102KB, `ActorClass`, `ActorHandle` |
| `remote_function.py` | Task abstraction | 24KB, `RemoteFunction` |
| `exceptions.py` | Error hierarchy | `RayError`, `RayTaskError`, etc. |

### ML/AI Libraries

| Directory | Purpose | Size |
|-----------|---------|------|
| `data/` | Distributed data processing | `Dataset` class, read/write APIs |
| `train/` | Distributed ML training | `Trainer`, scaling configs |
| `serve/` | Model serving | `@serve.deployment`, autoscaling |
| `tune/` | Hyperparameter tuning | `Tuner`, schedulers, search algos |
| `llm/` | LLM serving | vLLM integration, batch inference |

### Infrastructure

| Directory | Purpose |
|-----------|---------|
| `autoscaler/` | Cluster autoscaling |
| `dashboard/` | Web UI and REST APIs |
| `job_submission/` | Job management APIs |
| `workflow/` | Workflow orchestration |

### File Counts

```
python/ray/
├── _private/          (156 files)
├── autoscaler/        (89 files)
├── dashboard/         (203 files)
├── data/              (178 files)
├── serve/             (142 files)
├── train/             (98 files)
├── tune/              (187 files)
├── tests/             (273 files)
└── ...
```

## C++ Source Directory (`/src/ray/`)

The core distributed systems implementation.

### Component Structure

```
src/ray/
├── core_worker/       # Worker process implementation
│   ├── core_worker.cc        (4,660 LOC - largest C++ file)
│   ├── task_manager.cc       (1,836 LOC)
│   ├── reference_counter.cc  (1,831 LOC)
│   └── task_submission/      # Task submission pipeline
├── raylet/            # Node manager and scheduler
│   ├── node_manager.cc       (3,408 LOC)
│   ├── worker_pool.cc        (1,890 LOC)
│   └── scheduling/           # Scheduling algorithms
├── gcs/               # Global Control Store
│   ├── gcs_actor_manager.cc  (2,033 LOC)
│   └── gcs_server/           # GCS server implementation
├── object_manager/    # Object store management
│   ├── plasma/               # Plasma store
│   └── push_manager.cc       # Object transfer
├── pubsub/            # Pub/sub for state distribution
├── common/            # Shared utilities
│   └── ray_config.h          # Configuration system
└── protobuf/          # Protocol buffer definitions
```

### Key C++ Files

| File | LOC | Purpose |
|------|-----|---------|
| `core_worker/core_worker.cc` | 4,660 | Main worker implementation |
| `raylet/node_manager.cc` | 3,408 | Node-level scheduling |
| `gcs/gcs_actor_manager.cc` | 2,033 | Actor lifecycle management |
| `core_worker/task_manager.cc` | 1,836 | Task state tracking |
| `core_worker/reference_counter.cc` | 1,831 | Distributed GC |

## Java Directory (`/java/`)

Java API bindings for Ray.

```
java/
├── api/               # Public API interfaces
│   └── RayCall.java          (2,089 LOC)
├── runtime/           # Runtime implementation
│   └── AbstractRayRuntime.java
├── serve/             # Ray Serve Java API
└── test/              # Integration tests
```

## Documentation (`/doc/`)

Sphinx-based documentation.

```
doc/
├── source/
│   ├── ray-core/             # Core Ray documentation
│   ├── ray-security/         # Security guide
│   ├── data/                 # Ray Data docs
│   ├── train/                # Ray Train docs
│   ├── serve/                # Ray Serve docs
│   ├── tune/                 # Ray Tune docs
│   ├── rllib/                # RLlib docs
│   └── cluster/              # Cluster management
├── Makefile                  # Doc build configuration
└── requirements-doc.txt      # Doc dependencies
```

## CI/CD and Build

### Build System

| File/Directory | Purpose |
|----------------|---------|
| `WORKSPACE` | Bazel workspace definition |
| `BUILD.bazel` | Root build file |
| `.bazelrc` | Bazel configuration |
| `setup.py` | Python package build |
| `pyproject.toml` | Python project config |

### CI Configuration

| Directory | Purpose |
|-----------|---------|
| `.buildkite/` | Buildkite pipelines (19 `.rayci.yml` files) |
| `.github/workflows/` | GitHub Actions |
| `ci/` | CI scripts and utilities |

### Key CI Files

```
.buildkite/
├── base.rayci.yml     # Base configuration
├── core.rayci.yml     # Core tests (55+ jobs)
├── lint.rayci.yml     # Linting pipeline
├── data.rayci.yml     # Ray Data tests
├── ml.rayci.yml       # ML module tests
├── serve.rayci.yml    # Ray Serve tests
└── ...
```

## Test Organization

Tests are co-located with source code and in dedicated directories.

### Test Locations

```
python/ray/tests/           # Core tests (273 files)
├── unit/                   # Unit tests
├── aws/                    # AWS-specific tests
├── gcp/                    # GCP-specific tests
└── kuberay/                # Kubernetes tests

python/ray/data/tests/      # Ray Data tests
python/ray/serve/tests/     # Ray Serve tests
python/ray/train/tests/     # Ray Train tests
python/ray/tune/tests/      # Ray Tune tests

release/                    # Release/nightly tests
├── serve_tests/
├── tune_tests/
├── long_running_tests/
└── ...
```

### Test Statistics

| Category | Count |
|----------|-------|
| Python test files | 1,261 |
| C++ test files | 180 |
| Java test files | 86 |
| Total test files | 1,527 |
| Test directories | 85 |

## Configuration Files

### Code Quality

| File | Purpose |
|------|---------|
| `.pre-commit-config.yaml` | Pre-commit hooks (271 lines) |
| `pyproject.toml` | Ruff, Black, mypy config |
| `.clang-format` | C++ formatting |
| `semgrep.yml` | Static analysis rules |

### Project Configuration

| File | Purpose |
|------|---------|
| `requirements*.txt` | Python dependencies |
| `dependencies.bzl` | Bazel dependencies |
| `.bazelversion` | Bazel version lock |

## Entry Points

### CLI Commands

Defined in `setup.py`:

```python
entry_points={
    "console_scripts": [
        "ray=ray.scripts.scripts:main",
        "tune=ray.tune.cli.scripts:cli",
        "serve=ray.serve.scripts:cli",
    ]
}
```

### Python API

Main entry: `python/ray/__init__.py`

```python
# Key exports
from ray._raylet import ObjectRef
from ray.actor import ActorClass
from ray.remote_function import RemoteFunction
from ray._private.worker import (
    init, shutdown, get, put, wait, cancel, ...
)
```

## File Statistics by Type

| Extension | Count | Description |
|-----------|-------|-------------|
| `.py` | 4,146 | Python source |
| `.h` | 434 | C++ headers |
| `.cc` | 431 | C++ source |
| `.rst` | 372 | Documentation |
| `.java` | 361 | Java source |
| `.md` | 238 | Markdown docs |
| `.txt` | 207 | Text/requirements |

## Navigation Tips

### Finding Code

1. **Core API:** `python/ray/__init__.py` exports all public APIs
2. **Implementation:** `python/ray/_private/worker.py` has core logic
3. **C++ core:** `src/ray/core_worker/core_worker.cc`
4. **Scheduling:** `src/ray/raylet/node_manager.cc`
5. **Configs:** `src/ray/common/ray_config_def.h` for all options

### Understanding Data Flow

1. Start at `@ray.remote` decorator in `remote_function.py`
2. Follow to `_remote()` method for task submission
3. Trace through `_raylet.pyx` to C++ bindings
4. End at `core_worker.cc` for execution

### Finding Tests

1. Look for `tests/` directory in each module
2. Test files follow `test_*.py` pattern
3. Check `conftest.py` for fixtures
4. Release tests in `/release/` directory

---

*This structure guide helps navigate the Ray codebase efficiently. Use the paths above to quickly locate specific functionality.*
