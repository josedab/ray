.. _memory-pressure:

Memory Pressure Handling
========================

Ray provides enhanced memory pressure handling features that enable proactive
memory management, better visibility into memory state, and graceful
degradation under memory constraints.

Memory Pressure Levels
----------------------

Ray classifies memory usage into four pressure levels:

- **NORMAL** (< 60%): Normal operation, no special actions needed.
- **ELEVATED** (60-75%): Proactive measures begin (e.g., aggressive GC).
- **HIGH** (75-90%): Aggressive measures (e.g., spilling, backpressure).
- **CRITICAL** (> 90%): Emergency measures (e.g., task rejection).

Checking Memory Pressure
------------------------

You can check the current memory pressure state at any time:

.. code-block:: python

    from ray._private.memory_pressure import (
        get_pressure,
        get_pressure_level,
        MemoryPressureLevel,
    )

    # Get full pressure state
    pressure = get_pressure()
    print(f"Memory usage: {pressure.system_memory_ratio:.1%}")
    print(f"Pressure level: {pressure.level.name}")

    # Get just the level
    level = get_pressure_level()
    if level == MemoryPressureLevel.CRITICAL:
        print("Warning: Memory pressure is critical!")

Object Pinning
--------------

Important objects can be pinned to prevent eviction from the object store:

.. code-block:: python

    import ray
    from ray._private.memory_pressure import pin, unpin, pinned

    # Create and pin an important object
    ref = ray.put(important_data)
    pin(ref)  # Object won't be evicted

    # Process the object...
    result = ray.get(ref)

    # Unpin when done
    unpin(ref)

Using a context manager for automatic unpinning:

.. code-block:: python

    ref = ray.put(important_data)
    with pinned(ref):
        # Object is protected from eviction during this block
        result = process(ref)
    # Object is automatically unpinned here

Task Submission Backpressure
----------------------------

When memory pressure is high, task submission can be automatically throttled
to prevent overwhelming the system:

.. code-block:: python

    from ray._private.task_submitter import configure_task_submitter

    # Configure backpressure behavior
    configure_task_submitter(
        max_delay=10.0,           # Maximum delay in seconds
        enable_backpressure=True,  # Enable backpressure
        reject_on_critical=True,   # Reject tasks during critical pressure
    )

The backpressure behavior by pressure level:

- **NORMAL**: No delay
- **ELEVATED**: 0.1 second delay
- **HIGH**: Exponential backoff (2^n seconds, capped at max_delay)
- **CRITICAL**: Task rejected with MemoryPressureError (if enabled)

Memory Pressure Policies
------------------------

Configure how Ray responds at different pressure levels:

.. code-block:: python

    from ray._private.memory_pressure import configure_memory_pressure_policy

    configure_memory_pressure_policy({
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
    })

Memory Alerts
-------------

Configure alerts to be triggered when memory pressure changes:

.. code-block:: python

    from ray._private.memory_pressure import configure_memory_alerts

    configure_memory_alerts({
        "elevated": {"action": "log"},
        "high": {"action": "webhook", "url": "https://your-monitoring-service/alert"},
        "critical": {"action": "pagerduty", "key": "your-pagerduty-key"}
    })

Monitoring Memory Pressure
--------------------------

You can register callbacks to be notified when memory pressure changes:

.. code-block:: python

    from ray._private.memory_monitor import get_memory_monitor

    def on_pressure_change(pressure):
        print(f"Pressure changed to {pressure.level.name}")
        if pressure.level.value >= 2:  # HIGH or CRITICAL
            # Take action
            pass

    monitor = get_memory_monitor()
    monitor.register_pressure_callback(on_pressure_change)

Cost-Aware Eviction
-------------------

Ray's eviction policy considers the reconstruction cost of objects when
deciding what to evict. Objects that are expensive to reconstruct are less
likely to be evicted.

The eviction score formula prefers evicting:

- Old objects (longer since last access)
- Large objects
- Objects that are easy to reconstruct (low reconstruction cost)
- Objects that are not pinned

Best Practices
--------------

1. **Pin critical objects**: Use ``pin()`` for objects that are expensive to
   recreate or needed for important computations.

2. **Monitor pressure levels**: Check pressure levels before submitting
   large batches of tasks.

3. **Configure alerts**: Set up alerts for HIGH and CRITICAL pressure levels
   to catch problems early.

4. **Use context managers**: Use ``with pinned(ref):`` to ensure objects are
   automatically unpinned.

5. **Handle MemoryPressureError**: Catch ``MemoryPressureError`` when
   submitting tasks and implement retry logic.

Example: Handling Memory Pressure
---------------------------------

.. code-block:: python

    import ray
    from ray._private.memory_pressure import (
        get_pressure_level,
        pinned,
        MemoryPressureError,
        MemoryPressureLevel,
    )
    from ray._private.task_submitter import get_task_submitter

    @ray.remote
    def process_data(data):
        # Process the data
        return result

    def submit_with_pressure_awareness(data_refs):
        submitter = get_task_submitter()
        results = []

        for ref in data_refs:
            # Check pressure before submitting
            level = get_pressure_level()
            if level == MemoryPressureLevel.CRITICAL:
                # Wait for pressure to decrease
                time.sleep(5)
                continue

            try:
                # Pin the input during processing
                with pinned(ref):
                    result = submitter.submit(
                        lambda: ray.get(process_data.remote(ref))
                    )
                    results.append(result)
            except MemoryPressureError as e:
                print(f"Task rejected due to memory pressure: {e}")
                # Implement retry logic or fallback

        return results

Troubleshooting
---------------

**Tasks being rejected unexpectedly**

Check the current pressure level:

.. code-block:: python

    pressure = get_pressure()
    print(f"Level: {pressure.level.name}")
    print(f"System memory: {pressure.system_memory_ratio:.1%}")
    print(f"Object store: {pressure.object_store_ratio:.1%}")

**Objects being evicted too aggressively**

Pin important objects to protect them from eviction:

.. code-block:: python

    critical_ref = ray.put(important_data)
    pin(critical_ref)

**High backpressure delays**

Reduce the max_delay or disable backpressure:

.. code-block:: python

    configure_task_submitter(
        max_delay=5.0,  # Reduce from default 10
        enable_backpressure=True
    )
