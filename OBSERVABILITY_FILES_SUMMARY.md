# Ray Observability - Complete File Structure

## Directory Layout & Key Files

### 1. Logging System
**Location:** `/home/user/ray/python/ray/_private/ray_logging/`

| File | Purpose | Size |
|------|---------|------|
| `__init__.py` | Log deduplication, WorkerStandardStreamDispatcher | Core dedup logic |
| `logging_config.py` | LoggingConfig API, TEXT/JSON formatters | Main config class |
| `constants.py` | Log enums (LogKey), standard attributes | Configuration |
| `default_impl.py` | DefaultLoggingConfigurator implementation | Initialization |

**Key Classes:**
- `LoggingConfig` - Configuration dataclass with encoding, log_level, attributes
- `LogDeduplicator` - Deduplicates repeated logs with time-window aggregation
- `DedupState` - Tracks duplicate log patterns
- `CoreContextFilter` - Injects job_id, worker_id, task_id into logs

---

### 2. Metrics Collection
**Location:** `/home/user/ray/python/ray/_private/`

#### Main Metrics Agent
**File:** `metrics_agent.py` (34KB)
- `Gauge` class - OpenCensus view wrapper
- `Record` namedtuple - Metric recording unit
- `fix_grpc_metric()` - Sanitizes gRPC metric names
- `MetricsAgent` - Core metrics aggregation

#### Prometheus Exporter
**File:** `prometheus_exporter.py`
- `Options` class - Configuration
- `Collector` class - Prometheus metric collection
- `PrometheusExporter` class - StatsExporter implementation
- Converts OpenCensus to Prometheus format

#### OpenTelemetry Integration
**File:** `telemetry/open_telemetry_metric_recorder.py`
- `OpenTelemetryMetricRecorder` - Main OTel recorder class
- `register_gauge_metric()` - Observable gauge registration
- `register_counter_metric()` - Counter registration
- `register_histogram_metric()` - Histogram registration
- Cardinality management with callbacks

#### Metric Cardinality
**File:** `telemetry/metric_cardinality.py`
- `MetricCardinality` - High-cardinality label detection
- `WORKER_ID_TAG_KEY` - Common high-cardinality label

---

### 3. Custom Metrics API
**Location:** `/home/user/ray/python/ray/util/`

**File:** `metrics.py`
- `Metric` base class - Parent of all metrics
- `Counter` - Monotonically increasing counter
- `Gauge` - Last recorded value
- `Histogram` - Value distribution
- `_is_invalid_metric_name()` - Prometheus validation

---

### 4. Distributed Tracing
**Location:** `/home/user/ray/python/ray/util/tracing/`

| File | Purpose |
|------|---------|
| `__init__.py` | Main tracing exports |
| `tracing_helper.py` | _OpenTelemetryProxy, tracing state management |
| `setup_tempo_tracing.py` | OTLP/Tempo exporter setup |
| `setup_local_tmp_tracing.py` | Local tracing for development |

**Key Components:**
- `_OpenTelemetryProxy` - Lazy OpenTelemetry import wrapper
- `_is_tracing_enabled()` - Check tracing state
- `_enable_tracing()` / `_disable_tracing()` - Control tracing

---

### 5. Dashboard System
**Location:** `/home/user/ray/python/ray/dashboard/`

#### Main Dashboard
**File:** `dashboard.py`
- `Dashboard` class - Main dashboard process
- Configuration: host, port, gcs_address
- Frontend serving and backend API aggregation

#### Dashboard Head
**File:** `head.py`
- `DashboardHead` - Head node dashboard component
- Aggregates data from all nodes
- Serves REST API

#### Dashboard Agent
**File:** `agent.py`
- `DashboardAgent` - Per-node agent
- Reports local metrics and state

#### Dashboard Metrics
**File:** `dashboard_metrics.py`
- `DashboardPrometheusMetrics` - Dashboard-specific metrics
- Defines Prometheus metric families
- Dashboard API duration, CPU, memory metrics

#### Dashboard Constants
**File:** `consts.py`
- `COMPONENT_METRICS_TAG_KEYS` - Standard metric tags
- `NODE_TAG_KEYS`, `GPU_TAG_KEYS`, `TPU_TAG_KEYS`
- `CLUSTER_TAG_KEYS`

---

### 6. Dashboard Metrics Module
**Location:** `/home/user/ray/python/ray/dashboard/modules/metrics/`

| File | Purpose |
|------|---------|
| `metrics_head.py` | Prometheus service discovery, Grafana setup |
| `grafana_dashboard_factory.py` | Dynamic dashboard generation |
| `install_and_start_prometheus.py` | Prometheus automation |
| `templates.py` | Prometheus/Grafana config templates |

#### Dashboard Panels
**Location:** `dashboards/`
- `common.py` - DashboardConfig, Panel definitions
- `default_dashboard_panels.py` - Cluster overview
- `serve_dashboard_panels.py` - Ray Serve specific
- `serve_deployment_dashboard_panels.py` - Serve deployments
- `serve_llm_dashboard_panels.py` - Serve LLM apps
- `train_dashboard_panels.py` - Ray Train specific
- `data_dashboard_panels.py` - Ray Data specific

---

### 7. Event Aggregation
**Location:** `/home/user/ray/python/ray/dashboard/modules/aggregator/`

#### Main Aggregator
**File:** `aggregator_agent.py` (125KB)
- `AggregatorAgent` - Main event aggregator
- gRPC service for event collection
- HTTP publishing to external services
- Event type filtering

#### Event Buffer
**File:** `multi_consumer_event_buffer.py`
- `MultiConsumerEventBuffer` - Multi-consumer queue
- Thread-safe event buffering

#### Publishers
**File:** `publisher/`
- `ray_event_publisher.py` - Ray event publisher
- `async_publisher_client.py` - Async HTTP publisher
- `metrics.py` - Publisher metrics

---

### 8. Event Handling
**Location:** `/home/user/ray/python/ray/dashboard/modules/event/`

| File | Purpose |
|------|---------|
| `event_head.py` | Event aggregation at head |
| `event_agent.py` | Per-node event reporting |
| `event_utils.py` | Event parsing and monitoring |
| `event_consts.py` | Event type constants |

---

### 9. State APIs
**Location:** `/home/user/ray/python/ray/util/state/`

| File | Purpose |
|------|---------|
| `api.py` | StateApiClient - main API class |
| `state_manager.py` | Backend state manager |
| `common.py` | Data models (ActorState, TaskState, etc.) |
| `state_cli.py` | Command-line interface |
| `exception.py` | Custom exceptions |
| `util.py` | Utility functions |

**Key Classes:**
- `StateApiClient` - HTTP REST client for state queries
- `StateResource` enum - NODES, ACTORS, TASKS, WORKERS, etc.
- `ActorState`, `TaskState`, `NodeState` - Data models
- `ListApiOptions`, `GetApiOptions` - Query options

---

### 10. Reporter & Profiling
**Location:** `/home/user/ray/python/ray/dashboard/modules/reporter/`

#### Main Reporter
**File:** `reporter_agent.py` (80KB+)
- `ReporterAgent` - Per-node metrics reporter
- Collects system metrics (CPU, memory, GPU)
- Reports to metrics server

#### Profiling Managers
**File:** `profile_manager.py`
- `CpuProfilingManager` - py-spy based CPU profiling
- `MemoryProfilingManager` - tracemalloc based memory profiling
- Root/non-root execution handling

**File:** `gpu_profile_manager.py`
- `GpuProfilingManager` - NVIDIA GPU metrics
- `TpuProfilingManager` - TPU metrics

#### GPU Providers
**File:** `gpu_providers.py`
- `GpuMetricProvider` - GPU metric collection
- `GpuUtilizationInfo` - GPU data model
- `TpuUtilizationInfo` - TPU data model

---

### 11. Debugging
**Location:** `/home/user/ray/python/ray/util/`

#### Debugpy Integration
**File:** `debugpy.py`
- `_try_import_debugpy()` - Safe debugpy import
- `_ensure_debugger_port_open_thread_safe()` - Port management
- `set_trace()` - Main breakpoint function
- `_override_breakpoint_hooks()` - Hook overrides
- `POST_MORTEM_ERROR_UUID` - Error debugging constant

#### Debug Utilities
**File:** `debug.py`
- `log_once()` - Log once per key
- `Suspect` namedtuple - Memory leak suspect
- `test_for_memory_leaks()` - Memory leak detection
- Uses tracemalloc + scipy regression

---

### 12. Log Management
**Location:** `/home/user/ray/python/ray/dashboard/modules/log/`

| File | Purpose |
|------|---------|
| `log_manager.py` | Log file handling |
| `log_agent.py` | Per-node log collection |
| `log_utils.py` | Log parsing utilities |
| `log_consts.py` | Log constants |

---

### 13. C++ Stats/Metrics
**Location:** `/home/user/ray/src/ray/stats/`

| File | Purpose | Language |
|------|---------|----------|
| `metric_defs.h` | Metric declarations (DECLARE_stats) | C++ |
| `metric.h` | Metric class definition | C++ |
| `metric.cc` | Metric implementation | C++ |
| `tag_defs.h` | Tag key definitions | C++ |
| `tag_defs.cc` | Tag implementation | C++ |
| `stats.h` | Stats configuration | C++ |
| `metric_exporter.h` | OTLP exporter interface | C++ |
| `metric_exporter.cc` | OTLP exporter implementation | C++ |

**Test Files:**
- `tests/metric_exporter_grpc_test.cc` - gRPC exporter tests
- `tests/metric_with_open_telemetry_test.cc` - OTel integration tests
- `tests/stats_test.cc` - Stats tests

---

### 14. Job Management
**Location:** `/home/user/ray/python/ray/dashboard/modules/job/`

| Component | Purpose |
|-----------|---------|
| `job_head.py` | Job state management |
| `job_agent.py` | Per-node job reporting |
| `job_manager.py` | Job lifecycle management |
| `job_supervisor.py` | Job supervision |
| `pydantic_models.py` | Job data models |

---

### 15. Node Information
**Location:** `/home/user/ray/python/ray/dashboard/modules/node/`

| File | Purpose |
|------|---------|
| `node_consts.py` | Node constants |
| `datacenter.py` | Datacenter information |

---

## Configuration Files

### HTTP Server Configuration
**Location:** `/home/user/ray/python/ray/dashboard/`
- `http_server_head.py` - Head node HTTP server
- `http_server_agent.py` - Agent HTTP server

### Constants & Configuration
**Location:** `/home/user/ray/python/ray/dashboard/`
- `consts.py` - Dashboard constants
- `optional_deps.py` - Optional dependencies

---

## Test Files Reference

### Logging Tests
- `python/ray/tests/test_log_*.py`

### Metrics Tests
- `python/ray/tests/test_metrics.py`
- `python/ray/tests/test_metrics_agent.py`
- `python/ray/tests/test_metrics_agent_2.py`
- `python/ray/tests/test_metric_cardinality.py`
- `python/ray/tests/test_open_telemetry_metric_recorder.py`
- `python/ray/tests/test_resource_metrics.py`
- `python/ray/tests/test_task_metrics.py`

### Tracing Tests
- `python/ray/tests/test_tracing.py`

### Dashboard Tests
- `python/ray/dashboard/modules/*/tests/`

### State API Tests
- `python/ray/tests/test_state_*.py`

### Debug Tests
- `python/ray/tests/test_debug_tools.py`
- `python/ray/tests/test_ray_debugger.py`

---

## Environment Configuration

### Logging Config
- `RAY_DEDUP_LOGS` - Enable deduplication
- `RAY_DEDUP_LOGS_AGG_WINDOW_S` - Aggregation window
- `RAY_DEDUP_LOGS_ALLOW_REGEX` - Allow patterns
- `RAY_DEDUP_LOGS_SKIP_REGEX` - Skip patterns

### Metrics Config
- `RAY_ENABLE_OPEN_TELEMETRY` - Enable OTel
- `RAY_PROMETHEUS_HOST` - Prometheus server
- `RAY_PROMETHEUS_HEADERS` - Custom headers
- `RAY_GRAFANA_HOST` - Grafana server

### Dashboard Config
- `RAY_DASHBOARD_AGGREGATOR_AGENT_*` - Aggregator settings
- `RAY_METRICS_GRAFANA_DASHBOARD_OUTPUT_DIR` - Dashboard output

### Profiling Config
- `RAY_DISABLE_PROFILING` - Disable profiling
- `RAY_WORKER_TIMEOUT_S` - Worker timeout

---

## Data Flow Architecture

```
Ray Tasks/Actors
       |
       ├─> Python Logging -> LogDeduplicator
       |
       ├─> Custom Metrics -> MetricsAgent -> PrometheusExporter
       |
       ├─> C++ Metrics -> OpenCensus -> PrometheusExporter
       |
       ├─> Events -> AggregatorAgent -> HTTP/External
       |
       └─> Tracing -> OpenTelemetry -> OTLP/Tempo

              |
              v
        ReporterAgent
       |       |       |
       |       |       └─> GPU/CPU Profiling
       |       |
       |       └─> Memory Profiling
       |
       └─> System Metrics (CPU, Memory, Disk)

              |
              v
        Prometheus (scrapes port 8000)
              |
              v
        Grafana Dashboards
```

---

## Summary Statistics

- **Total Observability Files:** 100+
- **Python Files:** 80+
- **C++ Files:** 12+
- **Test Files:** 30+
- **Total Lines:** 50,000+ lines of code
- **Core Modules:** 15 major modules

