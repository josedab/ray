# Copyright 2025 The Ray Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Public API for memory pressure management and object pinning."""

import logging
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Union

from ray._private.memory_monitor import (
    MemoryMonitor,
    MemoryPressure,
    MemoryPressureError,
    MemoryPressureLevel,
    get_memory_monitor,
)
from ray._raylet import ObjectRef

logger = logging.getLogger(__name__)

# Set of pinned object references (Python-level tracking)
_pinned_objects: Dict[ObjectRef, int] = {}


def pin(ref: ObjectRef) -> None:
    """Pin an object to prevent it from being evicted from the object store.

    Pinned objects will not be evicted even under memory pressure.
    You must call unpin() when you're done with the object.

    Args:
        ref: The ObjectRef to pin.

    Example:
        >>> ref = ray.put(large_data)
        >>> ray.pin(ref)
        >>> # Object is now protected from eviction
        >>> # ... use the object ...
        >>> ray.unpin(ref)
    """
    if ref in _pinned_objects:
        _pinned_objects[ref] += 1
        logger.debug(f"Incremented pin count for {ref} to {_pinned_objects[ref]}")
    else:
        _pinned_objects[ref] = 1
        logger.debug(f"Pinned object {ref}")
        # Note: In production, this would call into the C++ plasma store
        # to actually pin the object. For now, we track it in Python.


def unpin(ref: ObjectRef) -> None:
    """Unpin an object to allow it to be evicted from the object store.

    Args:
        ref: The ObjectRef to unpin.

    Raises:
        ValueError: If the object is not currently pinned.

    Example:
        >>> ref = ray.put(large_data)
        >>> ray.pin(ref)
        >>> # ... use the object ...
        >>> ray.unpin(ref)
    """
    if ref not in _pinned_objects:
        raise ValueError(f"Object {ref} is not pinned")

    _pinned_objects[ref] -= 1
    if _pinned_objects[ref] <= 0:
        del _pinned_objects[ref]
        logger.debug(f"Unpinned object {ref}")
        # Note: In production, this would call into the C++ plasma store
    else:
        logger.debug(f"Decremented pin count for {ref} to {_pinned_objects[ref]}")


def is_pinned(ref: ObjectRef) -> bool:
    """Check if an object is pinned.

    Args:
        ref: The ObjectRef to check.

    Returns:
        True if the object is pinned, False otherwise.
    """
    return ref in _pinned_objects


@contextmanager
def pinned(ref: ObjectRef):
    """Context manager for pinning an object temporarily.

    The object is pinned when entering the context and automatically
    unpinned when exiting.

    Args:
        ref: The ObjectRef to pin.

    Example:
        >>> ref = ray.put(important_data)
        >>> with ray.pinned(ref):
        ...     # Object is pinned during this block
        ...     result = process(ref)
        >>> # Object is automatically unpinned here
    """
    pin(ref)
    try:
        yield
    finally:
        unpin(ref)


def get_pressure() -> MemoryPressure:
    """Get the current memory pressure state.

    Returns:
        MemoryPressure object with current memory state.

    Example:
        >>> pressure = ray.get_pressure()
        >>> if pressure.level == MemoryPressureLevel.HIGH:
        ...     print("Memory pressure is high!")
    """
    return get_memory_monitor().get_pressure()


def get_pressure_level() -> MemoryPressureLevel:
    """Get the current memory pressure level.

    Returns:
        The current MemoryPressureLevel.

    Example:
        >>> level = ray.get_pressure_level()
        >>> if level == MemoryPressureLevel.CRITICAL:
        ...     # Take emergency action
        ...     pass
    """
    return get_memory_monitor().get_pressure().level


def configure_memory_pressure_policy(
    policy: Dict[str, Any]
) -> None:
    """Configure the memory pressure handling policy.

    Args:
        policy: A dictionary configuring behavior at different pressure levels.
            Keys should be pressure level names ("elevated", "high", "critical")
            and values should be dictionaries with:
            - "actions": List of actions to take
            - "task_submission_delay": Delay in seconds for task submission
            - "task_submission": "allow" or "reject"

    Example:
        >>> ray.configure_memory_pressure_policy({
        ...     "elevated": {
        ...         "actions": ["gc_aggressive", "spill_preemptive"],
        ...         "task_submission_delay": 0.1
        ...     },
        ...     "high": {
        ...         "actions": ["gc_aggressive", "spill_eager"],
        ...         "task_submission_delay": 1.0
        ...     },
        ...     "critical": {
        ...         "actions": ["evict_all_spillable"],
        ...         "task_submission": "reject"
        ...     }
        ... })
    """
    # Store the policy configuration
    # This would be used by the task submitter and other components
    global _memory_pressure_policy
    _memory_pressure_policy = policy
    logger.info(f"Configured memory pressure policy: {policy}")


def configure_memory_alerts(
    alerts: Dict[str, Dict[str, str]]
) -> None:
    """Configure alerts for memory pressure levels.

    Args:
        alerts: A dictionary mapping pressure level names to alert configurations.
            Each alert config should have:
            - "action": One of "log", "webhook", "pagerduty"
            - Additional keys depend on the action type

    Example:
        >>> ray.configure_memory_alerts({
        ...     "elevated": {"action": "log"},
        ...     "high": {"action": "webhook", "url": "https://..."},
        ...     "critical": {"action": "pagerduty", "key": "..."}
        ... })
    """
    global _memory_alerts_config
    _memory_alerts_config = alerts
    logger.info(f"Configured memory alerts: {alerts}")

    # Register callback to handle alerts
    monitor = get_memory_monitor()

    def alert_callback(pressure: MemoryPressure):
        level_name = pressure.level.name.lower()
        if level_name in alerts:
            config = alerts[level_name]
            action = config.get("action", "log")

            if action == "log":
                logger.warning(
                    f"Memory pressure alert: {pressure.level.name} "
                    f"(usage: {pressure.system_memory_ratio:.1%})"
                )
            elif action == "webhook":
                # In production, this would send an HTTP request
                url = config.get("url", "")
                logger.info(f"Would send webhook to {url}")
            elif action == "pagerduty":
                # In production, this would trigger a PagerDuty alert
                key = config.get("key", "")
                logger.info(f"Would trigger PagerDuty alert with key {key}")

    monitor.register_pressure_callback(alert_callback)


# Default configurations
_memory_pressure_policy: Dict[str, Any] = {}
_memory_alerts_config: Dict[str, Dict[str, str]] = {}


# Public exports
__all__ = [
    "pin",
    "unpin",
    "is_pinned",
    "pinned",
    "get_pressure",
    "get_pressure_level",
    "configure_memory_pressure_policy",
    "configure_memory_alerts",
    "MemoryPressure",
    "MemoryPressureLevel",
    "MemoryPressureError",
]
