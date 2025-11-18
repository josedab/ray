# Ray Observability Analysis - Documentation Index

This directory contains a comprehensive analysis of Ray's observability features, architecture, and usage patterns.

## Documents Overview

### 1. RAY_OBSERVABILITY_ANALYSIS.md (958 lines, 29KB)
**Main comprehensive analysis document**

Complete deep-dive into Ray's observability stack covering:
- **Section 1:** Logging Architecture (configuration, deduplication, formats)
- **Section 2:** Metrics & Monitoring (collection, Prometheus, OpenTelemetry)
- **Section 3:** Distributed Tracing (OpenTelemetry integration, OTLP exporters)
- **Section 4:** Dashboard & Web UI (architecture, modules, Grafana integration)
- **Section 5:** State APIs (StateApiClient, resource queries, filtering)
- **Section 6:** Profiling (CPU, memory, GPU profiling)
- **Section 7:** Distributed Debugging (Ray Debugger, debugpy integration)
- **Section 8:** Log Aggregation & Management
- **Section 9:** Key File Paths Reference
- **Section 10:** Configuration Summary
- **Section 11:** Best Practices
- **Section 12:** Integration Examples
- **Section 13:** Troubleshooting Guide
- **Section 14:** Observability Flow Diagram
- **Section 15:** Conclusion

**Use this document when you need:**
- Deep understanding of how observability components work
- Implementation details and code patterns
- Configuration options and environment variables
- Best practices and integration examples
- Troubleshooting guides

---

### 2. OBSERVABILITY_QUICK_REFERENCE.md (158 lines, 3.8KB)
**Quick reference and cheat sheet**

Fast lookup guide with:
- Component overview table
- Configuration quick commands (Python code snippets)
- Environment variables cheatsheet
- Metric types summary
- Supported state resources
- Dashboard access URLs
- Logging format examples
- C++ core metrics list
- Profiling capabilities
- Troubleshooting table

**Use this document when you need:**
- Quick code examples for setup
- Environment variable references
- Fast troubleshooting tips
- Access URLs and ports
- Quick metric type reference

---

### 3. OBSERVABILITY_FILES_SUMMARY.md (408 lines, 12KB)
**Complete file structure and organization reference**

Detailed breakdown of all observability-related files including:
- **15 Major Modules** with complete file listings
  - Logging System (4 files)
  - Metrics Collection (4 components)
  - Custom Metrics API (1 file)
  - Distributed Tracing (4 files)
  - Dashboard System (5 files)
  - Dashboard Metrics Module (8 files)
  - Event Aggregation (3 files)
  - Event Handling (4 files)
  - State APIs (6 files)
  - Reporter & Profiling (5 files)
  - Debugging (2 files)
  - Log Management (4 files)
  - C++ Stats/Metrics (11 files)
  - Job Management (5 files)
  - Node Information (2 files)

- Key classes and methods for each module
- Test file references
- Environment configuration options
- Data flow architecture diagram
- Summary statistics (100+ files, 50,000+ lines of code)

**Use this document when you need:**
- To understand file organization
- To locate specific functionality
- To find test references
- To understand data flow
- To understand implementation details of each module

---

## Quick Navigation

### By Use Case

**"How do I configure logging?"**
- Quick Ref: Configuration Quick Commands
- Main Doc: Section 1 - Logging Architecture

**"How do I create custom metrics?"**
- Quick Ref: Custom Metrics section
- Main Doc: Section 2.2 - Custom Metrics API
- Files: `python/ray/util/metrics.py`

**"How do I enable distributed tracing?"**
- Quick Ref: Enable Distributed Tracing section
- Main Doc: Section 3 - Distributed Tracing
- Files: `python/ray/util/tracing/setup_tempo_tracing.py`

**"How do I query cluster state programmatically?"**
- Quick Ref: Query Cluster State section
- Main Doc: Section 5 - State APIs
- Files: `python/ray/util/state/api.py`

**"How do I debug a Ray task?"**
- Quick Ref: Interactive Debugging section
- Main Doc: Section 7 - Distributed Debugging
- Files: `python/ray/util/debugpy.py`

**"What metrics are available?"**
- Quick Ref: C++ Core Metrics section
- Main Doc: Section 2.4 - C++ Metrics
- Files: `src/ray/stats/metric_defs.h`

**"How do I access the dashboard?"**
- Quick Ref: Dashboard Access section
- Main Doc: Section 4 - Dashboard & Web UI
- Files: `python/ray/dashboard/dashboard.py`

**"What's the architecture of Ray's observability?"**
- Files: OBSERVABILITY_FLOW_DIAGRAM in Quick Ref
- Main Doc: Section 14 - Observability Flow Diagram
- Files: Complete file listing in Files Summary

---

## Key Concepts

### Logging
- **Format:** TEXT (default) or JSON
- **Features:** Deduplication, context injection (job_id, worker_id, task_id)
- **Config:** `LoggingConfig` dataclass at ray.init()
- **File:** `python/ray/_private/ray_logging/logging_config.py`

### Metrics
- **Types:** Counter, Gauge, Histogram
- **Export:** Prometheus (port 8000)
- **Integration:** OpenTelemetry, Prometheus
- **Backend:** Grafana dashboards
- **File:** `python/ray/util/metrics.py`

### Tracing
- **Protocol:** OTLP (gRPC)
- **Backends:** Tempo, Jaeger
- **Libraries:** OpenTelemetry
- **File:** `python/ray/util/tracing/setup_tempo_tracing.py`

### Dashboard
- **UI:** `http://localhost:8265`
- **Components:** Job management, metrics, events, logs
- **Grafana:** Automated dashboard generation
- **File:** `python/ray/dashboard/dashboard.py`

### State APIs
- **Type:** REST API
- **Resources:** Nodes, actors, tasks, workers, objects, jobs, etc.
- **Query:** StateApiClient with filtering
- **File:** `python/ray/util/state/api.py`

### Profiling
- **CPU:** py-spy (requires root)
- **Memory:** Python tracemalloc
- **GPU:** NVIDIA metrics, TPU metrics
- **File:** `python/ray/dashboard/modules/reporter/profile_manager.py`

### Debugging
- **Tool:** debugpy (v1.8.0+)
- **Features:** Breakpoints, interactive debugging, post-mortem
- **File:** `python/ray/util/debugpy.py`

---

## Architecture Overview

Ray's observability is built on a **modular, scalable architecture**:

```
┌─────────────────────────────────────────┐
│ Ray Tasks/Actors/Raylet                 │
└──────────────┬──────────────────────────┘
               │
        ┌──────┴──────┬──────────┬────────┐
        │             │          │        │
        v             v          v        v
     Logging      Metrics    Tracing   Events
        │             │          │        │
        ├─────────────┼──────────┼────────┤
        │             │          │        │
    Deduplicator  MetricsAgent  OTel  Aggregator
        │             │          │        │
        └──────────────┼──────────┼────────┘
                       │          │
        ┌──────────────┴──────────┴────────┐
        │   ReporterAgent (per node)       │
        │   ├─ CPU/Memory/GPU Profiling   │
        │   └─ System Metrics              │
        └──────┬───────────────────────────┘
               │
        ┌──────┴───────────┬──────────┬──────────┐
        │                  │          │          │
        v                  v          v          v
    Prometheus         OTLP/Tempo   HTTP    Dashboard
    (port 8000)       (port 4317)  Export   (port 8265)
        │                  │          │          │
        └──────────────────┼──────────┼──────────┘
                           │          │
        ┌──────────────────┼──────────┘
        │                  │
        v                  v
     Grafana            Jaeger/Tempo
   Dashboards        Trace Visualization
```

---

## File Statistics

- **Total Files:** 100+
- **Python Code:** 80+ files
- **C++ Code:** 12+ files
- **Test Files:** 30+ files
- **Total Lines:** 50,000+ lines
- **Core Modules:** 15 major modules

### By Component
| Component | Files | Type |
|-----------|-------|------|
| Logging | 4 | Python |
| Metrics | 8 | Python/C++ |
| Dashboard | 30+ | Python |
| Tracing | 4 | Python |
| State APIs | 6 | Python |
| Profiling | 5 | Python |
| Debugging | 2 | Python |
| C++ Stats | 11 | C++ |

---

## Getting Started

### 1. Basic Setup (5 minutes)
```python
import ray
from ray._private.ray_logging.logging_config import LoggingConfig

ray.init(
    logging_config=LoggingConfig(
        encoding="JSON",
        log_level="INFO"
    )
)

# Access dashboard at http://localhost:8265
```

### 2. Add Custom Metrics (10 minutes)
```python
from ray.util.metrics import Counter

counter = Counter("api_requests", tag_keys=("endpoint",))
counter.inc(tags={"endpoint": "/api/jobs"})
```

### 3. Query Cluster State (10 minutes)
```python
from ray.util.state import StateApiClient, StateResource

client = StateApiClient(address="auto")
actors = client.list(StateResource.ACTORS)
```

### 4. Enable Tracing (15 minutes)
```python
from ray.util.tracing.setup_tempo_tracing import setup_tracing
setup_tracing()
# Export traces to http://localhost:4317
```

### 5. Debug Interactive (10 minutes)
```python
from ray.util.debugpy import set_trace

@ray.remote
def my_task():
    set_trace()  # Drop into debugger
    return 42
```

---

## Configuration Checklist

- [ ] Choose logging format (TEXT or JSON)
- [ ] Configure log level (DEBUG, INFO, WARNING, ERROR)
- [ ] Enable/disable log deduplication
- [ ] Set up Prometheus for metrics collection
- [ ] Configure Grafana dashboards
- [ ] Enable distributed tracing if needed
- [ ] Set up profiling (CPU/memory/GPU)
- [ ] Configure event aggregation for audit
- [ ] Set dashboard access credentials if needed
- [ ] Configure log aggregation backend

---

## Common Tasks

### Monitor Cluster Health
1. Open Ray Dashboard at `http://localhost:8265`
2. Check node status, actor count, task rate
3. Review CPU/memory utilization graphs
4. Check event log for warnings/errors

### Debug Performance Issue
1. Enable CPU profiling via dashboard
2. Check memory metrics in Grafana
3. Review distributed traces in Jaeger
4. Query task/actor state via State APIs

### Analyze Logs
1. Set logging format to JSON
2. Aggregate logs with ELK or similar
3. Filter by job_id, worker_id, task_id
4. Use log deduplication to reduce noise

### Profile Memory Leak
1. Use `test_for_memory_leaks()` from `ray.util.debug`
2. Check traceback and memory growth pattern
3. Identify suspect allocation sites
4. Use memory profiling to narrow down

### Interactive Debugging
1. Add `set_trace()` or `breakpoint()` in Ray task
2. Run task with remote debugger connected
3. Use debugpy-compatible IDE (VSCode, PyCharm)
4. Inspect variables and step through code

---

## Resources

**Key Files to Explore:**
- `python/ray/_private/ray_logging/logging_config.py` - Logging setup
- `python/ray/util/metrics.py` - Custom metrics API
- `python/ray/dashboard/dashboard.py` - Dashboard main
- `python/ray/util/state/api.py` - State queries
- `python/ray/util/debugpy.py` - Debugging
- `src/ray/stats/metric_defs.h` - C++ metrics

**Documentation Files:**
- RAY_OBSERVABILITY_ANALYSIS.md - Comprehensive reference
- OBSERVABILITY_QUICK_REFERENCE.md - Quick lookup
- OBSERVABILITY_FILES_SUMMARY.md - File organization

**External Resources:**
- Ray Docs: https://docs.ray.io/
- OpenTelemetry: https://opentelemetry.io/
- Prometheus: https://prometheus.io/
- Grafana: https://grafana.com/

---

## Document Versions & Updates

- **Created:** 2025-11-18
- **Ray Version:** Latest from /home/user/ray
- **Coverage:** Logging, Metrics, Tracing, Dashboard, State APIs, Profiling, Debugging
- **Scope:** Core observability features and architecture

