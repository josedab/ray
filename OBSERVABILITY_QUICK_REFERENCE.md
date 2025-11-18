# Ray Observability - Quick Reference Guide

## Component Overview

| Feature | Location | Type | Key File |
|---------|----------|------|----------|
| **Logging** | `python/ray/_private/ray_logging/` | Python | `logging_config.py` |
| **Metrics** | `python/ray/_private/metrics_agent.py` | Python/C++ | `metrics_agent.py` |
| **Prometheus** | `python/ray/_private/prometheus_exporter.py` | Python | `prometheus_exporter.py` |
| **OpenTelemetry** | `python/ray/_private/telemetry/` | Python | `open_telemetry_metric_recorder.py` |
| **Tracing** | `python/ray/util/tracing/` | Python | `tracing_helper.py` |
| **Dashboard** | `python/ray/dashboard/` | Python | `dashboard.py` |
| **Metrics UI** | `python/ray/dashboard/modules/metrics/` | Python | `metrics_head.py` |
| **Events** | `python/ray/dashboard/modules/aggregator/` | Python | `aggregator_agent.py` |
| **State APIs** | `python/ray/util/state/` | Python | `api.py` |
| **Profiling** | `python/ray/dashboard/modules/reporter/` | Python | `profile_manager.py` |
| **Debugging** | `python/ray/util/debugpy.py` | Python | `debugpy.py` |
| **C++ Stats** | `src/ray/stats/` | C++ | `metric_defs.h` |

---

## Configuration Quick Commands

### Enable JSON Logging
```python
import ray
from ray._private.ray_logging.logging_config import LoggingConfig

ray.init(
    logging_config=LoggingConfig(
        encoding="JSON",
        log_level="INFO"
    )
)
```

### Custom Metrics
```python
from ray.util.metrics import Counter, Gauge

counter = Counter("my_metric", tag_keys=("label1",))
counter.inc(tags={"label1": "value"})
```

### Query Cluster State
```python
from ray.util.state import StateApiClient, StateResource

client = StateApiClient(address="auto")
actors = client.list(StateResource.ACTORS)
```

### Enable Distributed Tracing
```python
from ray.util.tracing.setup_tempo_tracing import setup_tracing
setup_tracing()
```

### Interactive Debugging
```python
from ray.util.debugpy import set_trace

@ray.remote
def task():
    set_trace()  # Breakpoint
    return 42
```

---

## Environment Variables Cheatsheet

```bash
# Logging
RAY_DEDUP_LOGS=1
RAY_DEDUP_LOGS_AGG_WINDOW_S=5

# Prometheus
RAY_PROMETHEUS_HOST=http://localhost:9090
RAY_GRAFANA_HOST=http://localhost:3000

# Events/Aggregator
RAY_DASHBOARD_AGGREGATOR_AGENT_MAX_EVENT_BUFFER_SIZE=1000000
RAY_DASHBOARD_AGGREGATOR_AGENT_EVENTS_EXPORT_ADDR=http://localhost:8000
```

---

## Metric Types

| Type | Usage | Example |
|------|-------|---------|
| Counter | Monotonically increasing values | Requests count, errors total |
| Gauge | Last recorded value | Memory usage, queue size |
| Histogram | Value distribution | Request latency, response size |

---

## Supported State Resources

```
NODES, ACTORS, TASKS, WORKERS, OBJECTS
PLACEMENT_GROUPS, RUNTIME_ENVS, JOBS, CLUSTER_EVENTS
```

---

## Dashboard Access

- Web UI: `http://localhost:8265`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000`

---

## Logging Formats

**TEXT (Default):**
```
2025-02-12 12:25:16,836 INFO test.py:11 -- Message job_id=xxx
```

**JSON:**
```json
{"asctime": "2025-02-12 12:25:48,766", "levelname": "INFO", "message": "Message", ...}
```

---

## C++ Core Metrics

```
ray_io_context_event_loop_lag_ms
ray_operation_count
ray_scheduler_tasks
ray_object_store_memory
ray_memory_manager_worker_eviction_total
```

---

## Profiling

**CPU:** Uses py-spy (requires root/setuid)
**Memory:** Uses Python tracemalloc
**GPU:** NVIDIA GPU metrics, TPU metrics

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| High memory | Check metric cardinality, reduce event buffer |
| Missing traces | Install OTel: `pip install opentelemetry-api opentelemetry-sdk opentelemetry-exporter-otlp` |
| Dashboard slow | Check `ray_dashboard_event_loop_lag` metric |
| Debugger not connecting | Verify debugpy 1.8.0+, check port/firewall |

