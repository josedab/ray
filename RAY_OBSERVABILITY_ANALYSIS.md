# Ray Observability Features - Comprehensive Analysis

## Executive Summary

Ray provides a comprehensive observability stack including:
- **Logging**: Python-native logging with deduplication, structured JSON/TEXT formatting
- **Metrics**: Prometheus-based metrics collection with OpenTelemetry integration
- **Tracing**: Distributed tracing support via OpenTelemetry with OTLP exporters
- **Dashboard**: Web UI and REST API for cluster monitoring and job management
- **Profiling**: CPU and memory profiling with py-spy and native Python tools
- **Debugging**: Interactive distributed debugger via debugpy with breakpoint support
- **State APIs**: Comprehensive REST APIs for querying cluster state

---

## 1. LOGGING ARCHITECTURE

### Overview
Ray's logging system provides structured logging with support for both TEXT and JSON formats, automatic log deduplication, and context injection (job ID, worker ID, task ID, etc.).

### Key Components

#### 1.1 Logging Configuration (`python/ray/_private/ray_logging/`)

**File Structure:**
```
python/ray/_private/ray_logging/
├── __init__.py                    # Module initialization
├── logging_config.py              # LoggingConfig dataclass
├── constants.py                   # Log constants and enums
└── default_impl.py                # Default configurator implementation
```

#### 1.2 LoggingConfig API

**Location:** `/home/user/ray/python/ray/_private/ray_logging/logging_config.py`

**Usage:**
```python
import ray
from ray._private.ray_logging.logging_config import LoggingConfig

# Configure at cluster initialization
ray.init(
    logging_config=LoggingConfig(
        encoding="JSON",              # or "TEXT"
        log_level="INFO",              # INFO, DEBUG, WARNING, ERROR
        additional_log_standard_attrs=['name', 'funcName']
    )
)
```

**Configuration Options:**
- **encoding**: TEXT (default) or JSON format
- **log_level**: DEBUG, INFO, WARNING, ERROR
- **additional_log_standard_attrs**: Extra Python logging attributes to include

#### 1.3 Log Deduplication

**Location:** `/home/user/ray/python/ray/_private/ray_logging/__init__.py`

**Features:**
- Automatic deduplication of repeated log messages across the cluster
- Aggregation window: `RAY_DEDUP_LOGS_AGG_WINDOW_S` (default)
- Pattern-based filtering with regex
- Allow/skip regex patterns via environment variables

**Environment Variables:**
```bash
RAY_DEDUP_LOGS=1                           # Enable deduplication (default)
RAY_DEDUP_LOGS_AGG_WINDOW_S=5              # Aggregation window
RAY_DEDUP_LOGS_ALLOW_REGEX="pattern"       # Patterns to always allow
RAY_DEDUP_LOGS_SKIP_REGEX="pattern"        # Patterns to skip
```

**Implementation:**
- `LogDeduplicator` class: Deduplicates log lines based on canonicalization
- `DedupState` namedtuple: Tracks timestamp, count, sources of duplicates
- Separate deduplicators for stdout and stderr

#### 1.4 Log Formatters

**Location:** `/home/user/ray/_common/formatters.py` (implied from logging_config)

**Formats:**
1. **TEXT Format** (default):
   ```
   2025-02-12 12:25:16,836 INFO test.py:11 -- Message job_id=01000000 worker_id=xxx
   ```

2. **JSON Format**:
   ```json
   {
     "asctime": "2025-02-12 12:25:48,766",
     "levelname": "INFO",
     "message": "Message",
     "filename": "test.py",
     "job_id": "01000000",
     "worker_id": "xxx",
     "node_id": "xxx",
     "task_id": "xxx",
     "task_name": "f"
   }
   ```

**Context Injection (LogRecord Attributes):**
- job_id
- worker_id
- node_id
- task_id
- task_name
- task_func_name
- timestamp_ns
- actor_id
- actor_name

### 1.5 Component Logging

**Location:** `/home/user/ray/python/ray/_private/ray_logging/logging_config.py`

**setup_component_logger()** - Used for dashboard, monitor, log monitor:
- File-based logging with rotating file handler
- Configurable max bytes and backup count
- No stdout/stderr redirection to prevent log spam in multi-worker deployments

---

## 2. METRICS & MONITORING

### 2.1 Metrics Collection Architecture

**Key Components:**

| Component | File | Purpose |
|-----------|------|---------|
| MetricsAgent | `python/ray/_private/metrics_agent.py` | Collects and aggregates metrics |
| PrometheusExporter | `python/ray/_private/prometheus_exporter.py` | Exports metrics to Prometheus |
| OpenTelemetryRecorder | `python/ray/_private/telemetry/open_telemetry_metric_recorder.py` | OTel metric registration |
| DashboardMetrics | `python/ray/dashboard/dashboard_metrics.py` | Dashboard-specific Prometheus metrics |

### 2.2 Custom Metrics API

**Location:** `/home/user/ray/python/ray/util/metrics.py`

**Metric Types:**
```python
from ray.util.metrics import Counter, Gauge, Histogram

# Counter - monotonically increasing counter
counter = Counter("requests_total", description="Total requests")
counter.inc()
counter.inc(5)

# Gauge - last recorded value
gauge = Gauge("memory_usage_bytes", description="Memory usage")
gauge.set(1024000)

# Histogram - distribution of values
histogram = Histogram("request_latency_ms", description="Request latency")
histogram.observe(100)
```

**Tag Support:**
```python
counter = Counter(
    "api_requests",
    description="API requests",
    tag_keys=("endpoint", "method")
)
counter.inc(tags={"endpoint": "/api/jobs", "method": "GET"})
```

**Default Tags:**
```python
counter.set_default_tags({"service": "ray-dashboard"})
```

### 2.3 Prometheus Integration

**Metric Export:**
- Format: OpenCensus Stats -> Prometheus
- HTTP Port: 8000 (configurable)
- Namespace: `ray_` prefix
- View-based metrics registration

**Custom Prometheus Metrics:**
- `ray_dashboard_api_requests_duration_seconds` - API endpoint latencies (histogram)
- `ray_dashboard_api_requests_count` - Request counts (counter)
- `ray_dashboard_event_loop_tasks` - Event loop queue size (gauge)
- `ray_dashboard_event_loop_lag` - Event loop lag (gauge)
- `ray_component_cpu` - Component CPU usage (gauge)
- `ray_component_uss` - USS memory usage (gauge)
- `ray_component_rss` - RSS memory usage (gauge)

### 2.4 C++ Metrics (Stats)

**Location:** `/home/user/ray/src/ray/stats/`

**Key Files:**
- `metric_defs.h` - Metric declarations
- `metric.h/.cc` - Metric implementation
- `metric_exporter.h/.cc` - OTLP exporter

**Metric Convention (Prometheus):**
```
ray_[component]_[metric_name]_[unit]
Example: ray_pull_manager_spill_throughput_mb
```

**Core Metrics:**
- `io_context_event_loop_lag_ms` - ASIO event loop lag
- `operation_count` - Operation counts
- `operation_run_time_ms` - Operation execution time
- `scheduler_tasks` - Task scheduler metrics
- `spill_manager_throughput_mb` - Object spillage throughput
- `object_store_memory` - Object store memory usage
- `memory_manager_worker_eviction_total` - Eviction stats

### 2.5 OpenTelemetry Metrics

**Location:** `/home/user/ray/_private/telemetry/open_telemetry_metric_recorder.py`

**Features:**
```python
class OpenTelemetryMetricRecorder:
    - register_gauge_metric()      # ObservableGauge with callbacks
    - register_counter_metric()    # Counter
    - register_sum_metric()        # Up-down counter
    - register_histogram_metric()  # Histogram with custom buckets
```

**Cardinality Management:**
- High-cardinality labels are dropped/aggregated
- Metric: `ray_metric_cardinality_[name]_[type]`

**Configuration:**
```python
prometheus_reader = PrometheusMetricReader()
provider = MeterProvider(metric_readers=[prometheus_reader])
metrics.set_meter_provider(provider)
```

---

## 3. DISTRIBUTED TRACING

### 3.1 OpenTelemetry Integration

**Location:** `/home/user/ray/util/tracing/`

**Key Files:**
```
python/ray/util/tracing/
├── __init__.py                 # Main tracing exports
├── tracing_helper.py           # Tracing proxy and utilities
├── setup_tempo_tracing.py      # Tempo (Grafana) exporter setup
└── setup_local_tmp_tracing.py  # Local temporary setup
```

### 3.2 Tracing Helper

**Location:** `/home/user/ray/util/tracing/tracing_helper.py`

**OpenTelemetry Proxy:**
```python
class _OpenTelemetryProxy:
    allowed_functions = {"trace", "context", "propagate", "Context"}
    
    # Lazy imports to handle cases where OTel not installed everywhere
    def _trace()        # opentelemetry.trace
    def _context()      # opentelemetry.context
    def _propagate()    # opentelemetry.propagate
```

**Tracing State:**
```python
_global_is_tracing_enabled = False
_opentelemetry = None

def _is_tracing_enabled() -> bool
def _enable_tracing()
def _disable_tracing()
```

### 3.3 Setup Tempo Tracing

**Location:** `/home/user/ray/util/tracing/setup_tempo_tracing.py`

**Configuration:**
```python
def setup_tracing() -> None:
    # Sets TracerProvider with OTLP exporter
    trace.set_tracer_provider(TracerProvider())
    trace.get_tracer_provider().add_span_processor(
        SimpleSpanProcessor(
            OTLPSpanExporter(endpoint="http://localhost:4317", insecure=True)
        )
    )
```

**Supported Exporters:**
- OTLP gRPC exporter (Tempo/Jaeger)
- Console exporter (debugging)

### 3.4 Tracing Features

**Span Context Propagation:**
- W3C Trace Context support
- Context preservation across async boundaries
- Baggage propagation for metadata

**Installation:**
```bash
pip install opentelemetry-api==1.34.1 \
            opentelemetry-sdk==1.34.1 \
            opentelemetry-exporter-otlp==1.34.1
```

---

## 4. DASHBOARD & WEB UI

### 4.1 Dashboard Architecture

**Main Components:**

| Component | File | Purpose |
|-----------|------|---------|
| Dashboard | `python/ray/dashboard/dashboard.py` | Main dashboard process |
| DashboardHead | `python/ray/dashboard/head.py` | Dashboard head node |
| Agent | `python/ray/dashboard/agent.py` | Dashboard agent (per node) |
| MetricsHead | `python/ray/dashboard/modules/metrics/metrics_head.py` | Metrics aggregation |

### 4.2 Dashboard Modules

**Module Structure:**
```
python/ray/dashboard/modules/
├── job/              # Job management and monitoring
├── metrics/          # Prometheus/Grafana integration
├── event/            # Cluster event logging
├── node/             # Node information
├── log/              # Log file management
├── data/             # Data API integration
├── aggregator/       # Event aggregation and publishing
└── reporter/         # System metrics reporting
```

### 4.3 Metrics Module

**Location:** `/home/user/ray/python/ray/dashboard/modules/metrics/`

**Key Components:**
- `metrics_head.py` - Prometheus service discovery, Grafana configuration
- `grafana_dashboard_factory.py` - Dynamic Grafana dashboard generation
- `install_and_start_prometheus.py` - Prometheus setup automation

**Grafana Dashboards:**
- Default dashboard (cluster overview)
- Serve dashboard (Ray Serve specific)
- Serve deployment dashboard
- Serve LLM dashboard
- Train dashboard
- Data dashboard

**Service Discovery:**
```python
# Prometheus service discovery file generation
PROMETHEUS_SERVICE_DISCOVERY_FILE
# Updated with node addresses for dynamic scraping
```

**Configuration Environment Variables:**
```bash
RAY_PROMETHEUS_HOST=http://localhost:9090
RAY_PROMETHEUS_HEADERS={}
RAY_PROMETHEUS_NAME=Prometheus
RAY_GRAFANA_HOST=http://localhost:3000
RAY_GRAFANA_ORG_ID=1
RAY_METRICS_GRAFANA_DASHBOARD_OUTPUT_DIR=/path/to/dashboards
```

### 4.4 Event Aggregation

**Location:** `/home/user/ray/python/ray/dashboard/modules/aggregator/`

**AggregatorAgent:**
- Collects events via gRPC from ray components
- Buffers events in `MultiConsumerEventBuffer`
- Periodically publishes to external HTTP service

**Configuration:**
```bash
RAY_DASHBOARD_AGGREGATOR_AGENT_MAX_EVENT_BUFFER_SIZE=1000000
RAY_DASHBOARD_AGGREGATOR_AGENT_MAX_EVENT_SEND_BATCH_SIZE=10000
RAY_DASHBOARD_AGGREGATOR_AGENT_EVENTS_EXPORT_ADDR=http://<ip>:<port>
RAY_DASHBOARD_AGGREGATOR_AGENT_EXPOSABLE_EVENT_TYPES=TASK_DEFINITION_EVENT,...
```

**Event Types Supported:**
- TASK_DEFINITION_EVENT / TASK_LIFECYCLE_EVENT
- ACTOR_TASK_DEFINITION_EVENT
- DRIVER_JOB_DEFINITION_EVENT / DRIVER_JOB_LIFECYCLE_EVENT
- ACTOR_DEFINITION_EVENT / ACTOR_LIFECYCLE_EVENT
- NODE_DEFINITION_EVENT / NODE_LIFECYCLE_EVENT

### 4.5 Event Module

**Location:** `/home/user/ray/python/ray/dashboard/modules/event/`

**Features:**
- Cluster-wide event logging
- Event filtering and persistence
- Event export to external systems

**Event Constants:** `/home/user/ray/python/ray/dashboard/modules/event/event_consts.py`

---

## 5. STATE APIS

### 5.1 State API Overview

**Location:** `/home/user/ray/util/state/`

**Key Files:**
```
python/ray/util/state/
├── api.py            # StateApiClient - main API class
├── state_cli.py      # CLI interface
├── common.py         # Data models and enums
├── state_manager.py  # Backend state manager
└── util.py           # Utility functions
```

### 5.2 StateApiClient

**Usage:**
```python
from ray.util.state import StateApiClient, StateResource

client = StateApiClient(address="auto")

# List resources
nodes = client.list(StateResource.NODES)
actors = client.list(StateResource.ACTORS)
tasks = client.list(StateResource.TASKS)
jobs = client.list(StateResource.JOBS)

# Get specific resource
worker = client.get(StateResource.WORKERS, id="worker_id")

# Summarize resource
summary = client.summary(StateResource.ACTORS)
```

### 5.3 Supported Resources

```python
class StateResource:
    NODES              # Cluster nodes
    ACTORS             # Active actors
    TASKS              # Tasks (completed and pending)
    WORKERS            # Worker processes
    OBJECTS            # Objects in object store
    PLACEMENT_GROUPS   # Placement groups
    RUNTIME_ENVS       # Runtime environment info
    JOBS               # Ray jobs
    CLUSTER_EVENTS     # Cluster events
```

### 5.4 Filtering & Predicates

**Filtering:**
```python
# List with predicates
client.list(
    StateResource.ACTORS,
    filters=[("state", "==", "ALIVE")]
)

client.list(
    StateResource.TASKS,
    filters=[("runtime_env_id", "==", "env_id")]
)
```

**API Options:**
- `GetApiOptions` - Timeout, detailed output
- `ListApiOptions` - Limit, offset, filters
- `SummaryApiOptions` - Aggregation options

### 5.5 CLI Interface

**Usage:**
```bash
ray state nodes
ray state actors
ray state tasks
ray state workers
ray state objects
ray state placement_groups
ray state jobs
ray state cluster_events
```

---

## 6. PROFILING

### 6.1 CPU Profiling

**Location:** `/home/user/ray/dashboard/modules/reporter/profile_manager.py`

**CpuProfilingManager:**
- Uses py-spy for CPU profiling
- Supports both root and non-root execution
- Automatic permission handling

**Features:**
```python
async def start_profiling(
    duration: int,
    pid: int,
    output_file: str
)

async def stop_profiling(pid: int)
```

**Requirements:**
- py-spy installed with root permissions
- Either passwordless sudo or setuid on py-spy binary

**Permission Setup:**
```bash
# macOS
sudo chown root: `which py-spy`

# Linux
sudo chown root:root `which py-spy`
sudo chmod u+s `which py-spy`
```

### 6.2 Memory Profiling

**MemoryProfilingManager:**
- Uses Python's tracemalloc module
- Tracks memory allocations
- Identifies memory leaks

**Python API:**
```python
from ray.util.debug import Suspect, test_for_memory_leaks

# Detect memory leaks
suspects = test_for_memory_leaks(
    desc="Test case",
    init=init_func,
    code=test_func,
    repeats=10,
    max_num_trials=1
)

# Results include:
# - traceback: allocation stack trace
# - memory_increase: total memory increase
# - slope: linear regression slope
# - rvalue: correlation coefficient
# - hist: history of memory sizes
```

### 6.3 GPU Profiling

**Location:** `/home/user/ray/dashboard/modules/reporter/gpu_profile_manager.py`

**Features:**
- NVIDIA GPU metrics (cuda-smi)
- TPU metrics collection
- Per-GPU memory and utilization tracking

---

## 7. DISTRIBUTED DEBUGGING

### 7.1 Ray Debugger

**Location:** `/home/user/ray/util/debugpy.py`

**Features:**
- Interactive breakpoint support with `breakpoint()`
- Remote debugging via debugpy (v1.8.0+)
- Thread-safe debugger port management
- Post-mortem debugging support

**Installation:**
```bash
pip install debugpy==1.8.0
```

**Usage:**
```python
import ray
from ray.util.debugpy import set_trace

@ray.remote
def my_task():
    set_trace()  # Will drop into debugger
    # or
    breakpoint()  # Standard Python breakpoint
    return 42

ray.get(my_task.remote())
```

### 7.2 Debugger Features

**Port Management:**
```python
debugger_port = ray._private.worker.global_worker.debugger_port

# Port automatically assigned on first use
# Reused across subsequent breakpoints
```

**Hook Management:**
```python
_override_breakpoint_hooks()
# Ensures breakpoint() works correctly in multi-threaded context
```

**Error Handling:**
```python
# Post-mortem debugging on task failure
set_trace(breakpoint_uuid=POST_MORTEM_ERROR_UUID)
```

### 7.3 Debugger Workflow

1. Task execution triggers `breakpoint()` or `set_trace()`
2. Debugger port opened (one per worker process)
3. Debugger listens for client connection
4. Client attaches via VSCode Remote Debugger or debugpy CLI
5. Interactive debugging session begins

---

## 8. LOG AGGREGATION & MANAGEMENT

### 8.1 Log Module

**Location:** `/home/user/ray/dashboard/modules/log/`

**Components:**
- `log_manager.py` - Log file handling and streaming
- `log_utils.py` - Log parsing utilities
- `log_agent.py` - Per-node log collector
- `log_consts.py` - Log-related constants

### 8.2 Log File Organization

**Worker Log Files:**
```
logs/
├── worker-<worker_id>-<job_id>-<pid>
├── worker-<worker_id>-<job_id>-<pid>.err
└── worker-<worker_id>-<job_id>-<pid>.out
```

**Raylet Logs:**
```
logs/
├── raylet.out
└── raylet.err
```

**Dashboard Logs:**
```
logs/
├── dashboard.log
└── monitor.log
```

### 8.3 Log Streaming

**Features:**
- Stream logs from specific components
- Tail recent log lines
- Filter by job ID, node ID, etc.

---

## 9. KEY FILE PATHS REFERENCE

### Logging
- `/home/user/ray/python/ray/_private/ray_logging/logging_config.py` - LoggingConfig API
- `/home/user/ray/python/ray/_private/ray_logging/__init__.py` - Log deduplication
- `/home/user/ray/python/ray/_private/ray_logging/constants.py` - Log enums

### Metrics
- `/home/user/ray/python/ray/util/metrics.py` - Custom metrics API
- `/home/user/ray/python/ray/_private/metrics_agent.py` - Metrics collection
- `/home/user/ray/python/ray/_private/prometheus_exporter.py` - Prometheus export
- `/home/user/ray/python/ray/_private/telemetry/open_telemetry_metric_recorder.py` - OTel metrics

### Tracing
- `/home/user/ray/python/ray/util/tracing/tracing_helper.py` - Tracing utilities
- `/home/user/ray/python/ray/util/tracing/setup_tempo_tracing.py` - Tempo setup

### Dashboard
- `/home/user/ray/python/ray/dashboard/dashboard.py` - Main dashboard
- `/home/user/ray/python/ray/dashboard/modules/metrics/metrics_head.py` - Metrics aggregation
- `/home/user/ray/python/ray/dashboard/modules/aggregator/aggregator_agent.py` - Event aggregation
- `/home/user/ray/python/ray/dashboard/modules/event/` - Event handling

### State APIs
- `/home/user/ray/python/ray/util/state/api.py` - StateApiClient
- `/home/user/ray/python/ray/util/state/state_manager.py` - Backend state manager
- `/home/user/ray/python/ray/util/state/common.py` - Data models

### Debugging & Profiling
- `/home/user/ray/python/ray/util/debugpy.py` - Debugger integration
- `/home/user/ray/python/ray/util/debug.py` - Debug utilities (log_once, memory leak detection)
- `/home/user/ray/python/ray/dashboard/modules/reporter/profile_manager.py` - Profilers

### C++ Stats
- `/home/user/ray/src/ray/stats/metric_defs.h` - Metric declarations
- `/home/user/ray/src/ray/stats/metric.h/.cc` - Metric implementation
- `/home/user/ray/src/ray/stats/metric_exporter.h/.cc` - OTLP exporter

---

## 10. CONFIGURATION SUMMARY

### Environment Variables

**Logging:**
```bash
RAY_DEDUP_LOGS=1
RAY_DEDUP_LOGS_AGG_WINDOW_S=5
RAY_DEDUP_LOGS_ALLOW_REGEX=""
RAY_DEDUP_LOGS_SKIP_REGEX=""
```

**Metrics:**
```bash
RAY_ENABLE_OPEN_TELEMETRY=0
RAY_PROMETHEUS_HOST=http://localhost:9090
RAY_GRAFANA_HOST=http://localhost:3000
```

**Dashboard Aggregator:**
```bash
RAY_DASHBOARD_AGGREGATOR_AGENT_MAX_EVENT_BUFFER_SIZE=1000000
RAY_DASHBOARD_AGGREGATOR_AGENT_EVENTS_EXPORT_ADDR=""
RAY_DASHBOARD_AGGREGATOR_AGENT_EXPOSABLE_EVENT_TYPES=""
```

**Profiling:**
```bash
RAY_DISABLE_PROFILING=0
```

---

## 11. OBSERVABILITY BEST PRACTICES

### Logging
1. Use JSON encoding for log aggregation systems
2. Include relevant custom attributes for filtering
3. Enable log deduplication to reduce noise
4. Set appropriate log levels per environment

### Metrics
1. Use descriptive metric names following Prometheus convention
2. Add meaningful tags for filtering
3. Set reasonable cardinality limits
4. Monitor metric cardinality to prevent OOM

### Tracing
1. Enable OpenTelemetry only when needed (overhead)
2. Use sampling for high-throughput applications
3. Export to backends like Tempo/Jaeger for long-term storage

### Dashboard
1. Use provided dashboards as starting points
2. Set up Prometheus + Grafana for persistent metrics
3. Configure event aggregation for audit trails
4. Monitor dashboard health via its own metrics

### Debugging
1. Use debugpy for interactive debugging in development
2. Enable post-mortem debugging for production incidents
3. Use state APIs to query cluster state programmatically
4. Combine with logging for correlation

---

## 12. INTEGRATION EXAMPLES

### Complete Observability Stack

```python
import ray
from ray._private.ray_logging.logging_config import LoggingConfig
from ray.util.metrics import Counter

# 1. Configure logging
ray.init(
    logging_config=LoggingConfig(
        encoding="JSON",
        log_level="INFO",
        additional_log_standard_attrs=["name", "funcName"]
    )
)

# 2. Register custom metrics
request_counter = Counter(
    "api_requests_total",
    tag_keys=("endpoint",)
)

# 3. Use in tasks
@ray.remote
def process_request(path):
    request_counter.inc(tags={"endpoint": path})
    # Processing logic
    return result

# 4. Query state
from ray.util.state import StateApiClient, StateResource
client = StateApiClient()
actors = client.list(StateResource.ACTORS)

ray.shutdown()
```

### OpenTelemetry Setup

```python
from ray.util.tracing.setup_tempo_tracing import setup_tracing

# Enable distributed tracing
setup_tracing()

# Traces will be exported to http://localhost:4317
# Viewable in Jaeger/Tempo UI
```

---

## 13. TROUBLESHOOTING

### High Memory Usage
- Check metric cardinality with `MetricCardinality.get_high_cardinality_labels_to_drop()`
- Reduce event buffer sizes via environment variables
- Monitor dashboard component memory usage

### Missing Traces
- Verify OpenTelemetry is installed: `pip install opentelemetry-api opentelemetry-sdk opentelemetry-exporter-otlp`
- Check OTLP exporter endpoint is accessible
- Enable tracing with `_enable_tracing()` or environment variable

### Dashboard Unresponsive
- Check dashboard metrics via `ray_dashboard_event_loop_lag`
- Monitor dashboard process CPU/memory
- Reduce event aggregator batch sizes if needed

### Debugger Not Connecting
- Verify debugpy version ≥ 1.8.0
- Check firewall allows debugger port
- Review debugger port assignment in logs

---

## 14. OBSERVABILITY FLOW DIAGRAM

```
┌─────────────────────────────────────────────────────────────────┐
│                     Ray Cluster                                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ Raylet / Core Worker Process                             │   │
│  ├────────────────────────────────────────────────────────┤  │
│  │ • C++ Stats (metric_defs.h)                             │  │
│  │ • Task/Actor Events                                     │  │
│  │ • Custom Metrics                                        │  │
│  └──────────────────────────────────────────────────────────┘   │
│         │                    │                    │               │
│         ├─────OpenCensus────┤                    │               │
│         │                    │                    │               │
│  ┌──────▼─────────────────────▼──────────────────▼──────────┐   │
│  │ Reporter Agent (per node)                                │   │
│  ├──────────────────────────────────────────────────────────┤  │
│  │ • MetricsAgent (aggregates metrics)                      │  │
│  │ • PrometheusExporter                                     │  │
│  │ • CPU/Memory/GPU Profiling                              │  │
│  │ • Event collection                                       │  │
│  └──────────────────────────────────────────────────────────┘   │
│         │               │                  │                     │
│         │               │                  │                     │
└─────────┼───────────────┼──────────────────┼─────────────────────┘
          │               │                  │
          │               │                  ▼
          │               │        ┌──────────────────┐
          │               │        │ Aggregator Agent │
          │               │        │ (gRPC Events)    │
          │               │        └────────┬─────────┘
          │               │                 │
          ▼               ▼                 ▼
    ┌──────────────┐ ┌─────────────┐ ┌──────────────────┐
    │ Prometheus   │ │ OTLP/Tempo  │ │ External HTTP    │
    │ (Metrics)    │ │ (Traces)    │ │ (Event Export)   │
    └──────────────┘ └─────────────┘ └──────────────────┘
          │               │                 │
          ▼               ▼                 ▼
    ┌──────────────┐ ┌─────────────┐ ┌──────────────────┐
    │ Grafana      │ │ Jaeger/UI   │ │ Custom Analysis  │
    │ Dashboards   │ │ (Traces)    │ │ System           │
    └──────────────┘ └─────────────┘ └──────────────────┘
          ▲               ▲
          └───────┬───────┘
                  │
        ┌─────────▼─────────────┐
        │ Ray Dashboard         │
        │ (Web UI + REST API)   │
        ├─────────────────────┤
        │ • Cluster monitoring │
        │ • Job management     │
        │ • Event viewer       │
        │ • State APIs         │
        └─────────────────────┘
                  ▲
                  │
          ┌───────┴─────────┐
          │   Client SDK    │
          │ StateApiClient  │
          └─────────────────┘
```

---

## 15. CONCLUSION

Ray provides a **mature, multi-layered observability platform**:

1. **Structured Logging** - JSON/TEXT formats with context injection
2. **Metrics Collection** - Prometheus + OpenTelemetry integration
3. **Distributed Tracing** - Full W3C trace context support
4. **Web Dashboard** - Real-time cluster monitoring UI
5. **State APIs** - Programmatic cluster introspection
6. **Profiling Tools** - CPU, memory, GPU profiling
7. **Interactive Debugging** - debugpy-based remote debugging
8. **Event Aggregation** - Centralized event collection and export

The architecture is designed for **scalability, flexibility, and integration** with popular observability backends (Prometheus, Grafana, Jaeger, Tempo, etc.).

