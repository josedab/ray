# RFC-0005: Enhanced Memory Pressure Handling

**Status:** Draft
**Author:** Codebase Analysis
**Created:** 2025-01-18
**Commit Reference:** `d1cce8c9dc8411fad7cfbd619350bec6f19839a3`

## Summary

Improve Ray's memory pressure handling with proactive memory management, better spilling strategies, and graceful degradation under memory constraints.

## Motivation

### Current State

Ray's current memory management:
- Reactive: Waits until thresholds exceeded
- Binary: Either normal or OOM
- Limited visibility: Hard to predict pressure

Common issues:
1. Sudden OOM with no warning
2. Aggressive eviction causing re-computation
3. Spilling performance cliffs
4. No backpressure to task submission

### Evidence from Codebase

From [`src/ray/object_manager/plasma/eviction_policy.cc`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/src/ray/object_manager/plasma/eviction_policy.cc):

```cpp
// Simple LRU eviction
// No consideration of object importance or reconstruction cost
```

## Detailed Design

### Memory Pressure Levels

```cpp
// New pressure-aware system
enum class MemoryPressureLevel {
  NORMAL,    // < 60% - Normal operation
  ELEVATED,  // 60-75% - Start proactive measures
  HIGH,      // 75-90% - Aggressive measures
  CRITICAL   // > 90% - Emergency measures
};
```

### Proactive Memory Management

#### 1. Pressure Monitoring

```python
# ray/_private/memory_monitor.py

class MemoryMonitor:
    def __init__(self):
        self.pressure_callbacks = []

    async def monitor_loop(self):
        while True:
            pressure = self._calculate_pressure()
            level = self._classify_level(pressure)

            if level != self.current_level:
                await self._notify_pressure_change(level)

            await asyncio.sleep(0.1)  # 100ms interval

    def _calculate_pressure(self):
        store_used = get_object_store_used()
        store_total = get_object_store_total()
        system_mem = psutil.virtual_memory()

        return MemoryPressure(
            object_store_ratio=store_used / store_total,
            system_memory_ratio=system_mem.percent / 100,
            pending_tasks=get_pending_task_count()
        )
```

#### 2. Backpressure on Task Submission

```python
# When memory pressure is high, slow down task submission

class TaskSubmitter:
    async def submit(self, task_spec):
        pressure = memory_monitor.get_pressure()

        if pressure.level == MemoryPressureLevel.HIGH:
            # Exponential backoff
            delay = min(2 ** (pressure.consecutive_high_count), 10)
            await asyncio.sleep(delay)
            logger.warning(f"Delaying task submission due to memory pressure")

        if pressure.level == MemoryPressureLevel.CRITICAL:
            raise MemoryPressureError(
                "Cannot submit task: memory pressure is critical",
                remediation=[
                    "Wait for current tasks to complete",
                    "Reduce concurrent task count",
                    "Increase object store memory"
                ]
            )

        return await self._do_submit(task_spec)
```

### Intelligent Eviction

#### Cost-Aware Eviction

```cpp
// Consider reconstruction cost when evicting
struct EvictionCandidate {
  ObjectID object_id;
  int64_t size;
  int64_t reconstruction_cost;  // New: estimated recompute time
  int64_t last_access_time;
  int reference_count;
};

double EvictionPolicy::CalculateEvictionScore(const EvictionCandidate& c) {
  // Higher score = more likely to evict
  double age_score = CurrentTime() - c.last_access_time;
  double size_score = c.size;
  double cost_penalty = c.reconstruction_cost * kCostWeight;

  // Prefer evicting:
  // - Old objects
  // - Large objects
  // - Easy to reconstruct
  return (age_score * size_score) / (1 + cost_penalty);
}
```

#### Pinning Important Objects

```python
# User can pin objects that shouldn't be evicted
ref = ray.put(important_data)
ray.pin(ref)  # Won't be evicted

# Or with context manager
with ray.pinned(ref):
    # Object is pinned during this block
    process(ref)
```

### Enhanced Spilling

#### Tiered Spilling

```yaml
# Configuration
object_store:
  spilling:
    enabled: true
    tiers:
      - type: nvme
        path: /mnt/nvme/ray_spill
        priority: 1
        max_size: 100GB
      - type: ssd
        path: /mnt/ssd/ray_spill
        priority: 2
        max_size: 500GB
      - type: remote
        uri: s3://bucket/ray_spill
        priority: 3
```

#### Predictive Spilling

```python
# Spill before hitting threshold based on task requirements
class PredictiveSpiller:
    def should_spill_preemptively(self) -> bool:
        pending_tasks = get_pending_tasks()
        estimated_memory = sum(t.estimated_memory for t in pending_tasks)
        available = get_available_memory()

        return estimated_memory > available * 0.8
```

### Graceful Degradation

#### Memory Pressure Policies

```python
# Configure behavior at different pressure levels
ray.init(
    memory_pressure_policy={
        "elevated": {
            "actions": ["gc_aggressive", "spill_preemptive"],
            "task_submission_delay": 0.1
        },
        "high": {
            "actions": ["gc_aggressive", "spill_eager", "reject_new_puts"],
            "task_submission_delay": 1.0
        },
        "critical": {
            "actions": ["evict_all_spillable", "kill_lowest_priority"],
            "task_submission": "reject"
        }
    }
)
```

#### Priority-Based Task Management

```python
@ray.remote(priority=1)  # High priority
def critical_task():
    pass

@ray.remote(priority=10)  # Low priority
def background_task():
    pass

# Under critical pressure, low-priority tasks may be cancelled
```

### Observability Improvements

#### Memory Dashboard

```python
# New dashboard widgets
- Object store usage over time
- Pressure level indicator
- Spilling activity
- Eviction rate
- Top memory consumers
```

#### Alerts

```python
# Configurable alerts
ray.init(
    memory_alerts={
        "elevated": {"action": "log"},
        "high": {"action": "webhook", "url": "https://..."},
        "critical": {"action": "pagerduty", "key": "..."}
    }
)
```

## Implementation Plan

### Week 1: Pressure Monitoring
- Implement pressure levels
- Add monitoring loop
- Create metrics

### Week 2: Backpressure
- Task submission throttling
- Pressure-aware scheduling
- User-facing APIs

### Week 3: Intelligent Eviction
- Cost-aware eviction
- Object pinning
- Tiered spilling

### Week 4: Testing & Dashboard
- Comprehensive tests
- Dashboard widgets
- Documentation

## Backwards Compatibility

**Non-breaking changes:**
- New pressure levels are additive
- Existing configs continue to work
- New features are opt-in

## Success Criteria

- [ ] 50% reduction in unexpected OOM errors
- [ ] Predictable performance under memory pressure
- [ ] Clear visibility into memory state
- [ ] Graceful degradation vs. sudden failure

## Effort Estimation

- **Development:** 6 dev-weeks
- **Testing:** 2 dev-weeks
- **Total:** 8 dev-weeks

## References

- Eviction policy: [`src/ray/object_manager/plasma/eviction_policy.cc`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/src/ray/object_manager/plasma/eviction_policy.cc)
- Memory monitor: [`python/ray/_private/memory_monitor.py`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/python/ray/_private/memory_monitor.py)
