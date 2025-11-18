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

"""Task submitter with memory pressure backpressure support."""

import asyncio
import logging
import time
from typing import Any, Optional

from ray._private.memory_monitor import (
    MemoryPressureError,
    MemoryPressureLevel,
    get_memory_monitor,
)

logger = logging.getLogger(__name__)


class TaskSubmitter:
    """Task submitter that applies backpressure based on memory pressure.

    This class wraps task submission with memory pressure checks and
    implements exponential backoff when memory pressure is high.
    """

    def __init__(
        self,
        max_delay: float = 10.0,
        enable_backpressure: bool = True,
        reject_on_critical: bool = True,
    ):
        """Initialize the TaskSubmitter.

        Args:
            max_delay: Maximum delay in seconds for backpressure.
            enable_backpressure: Whether to enable backpressure delays.
            reject_on_critical: Whether to reject tasks during critical pressure.
        """
        self._max_delay = max_delay
        self._enable_backpressure = enable_backpressure
        self._reject_on_critical = reject_on_critical
        self._memory_monitor = get_memory_monitor()

    async def submit_async(self, submit_func, *args, **kwargs) -> Any:
        """Submit a task with memory pressure backpressure (async version).

        Args:
            submit_func: The function to call to submit the task.
            *args: Arguments for submit_func.
            **kwargs: Keyword arguments for submit_func.

        Returns:
            The result of submit_func.

        Raises:
            MemoryPressureError: If memory pressure is critical and
                reject_on_critical is True.
        """
        pressure = self._memory_monitor.get_pressure()

        if self._enable_backpressure:
            if pressure.level == MemoryPressureLevel.CRITICAL:
                if self._reject_on_critical:
                    raise MemoryPressureError(
                        "Cannot submit task: memory pressure is critical",
                        remediation=[
                            "Wait for current tasks to complete",
                            "Reduce concurrent task count",
                            "Increase object store memory",
                        ],
                    )
                else:
                    # Maximum delay
                    delay = self._max_delay
                    logger.warning(
                        f"Critical memory pressure, delaying task "
                        f"submission by {delay}s"
                    )
                    await asyncio.sleep(delay)

            elif pressure.level == MemoryPressureLevel.HIGH:
                # Exponential backoff based on consecutive high count
                delay = min(
                    2 ** pressure.consecutive_high_count,
                    self._max_delay
                )
                logger.warning(
                    f"High memory pressure (consecutive: "
                    f"{pressure.consecutive_high_count}), "
                    f"delaying task submission by {delay}s"
                )
                await asyncio.sleep(delay)

            elif pressure.level == MemoryPressureLevel.ELEVATED:
                # Small delay for elevated pressure
                delay = 0.1
                logger.debug(
                    f"Elevated memory pressure, delaying task "
                    f"submission by {delay}s"
                )
                await asyncio.sleep(delay)

        # Perform the actual submission
        if asyncio.iscoroutinefunction(submit_func):
            return await submit_func(*args, **kwargs)
        else:
            return submit_func(*args, **kwargs)

    def submit(self, submit_func, *args, **kwargs) -> Any:
        """Submit a task with memory pressure backpressure (sync version).

        Args:
            submit_func: The function to call to submit the task.
            *args: Arguments for submit_func.
            **kwargs: Keyword arguments for submit_func.

        Returns:
            The result of submit_func.

        Raises:
            MemoryPressureError: If memory pressure is critical and
                reject_on_critical is True.
        """
        pressure = self._memory_monitor.get_pressure()

        if self._enable_backpressure:
            if pressure.level == MemoryPressureLevel.CRITICAL:
                if self._reject_on_critical:
                    raise MemoryPressureError(
                        "Cannot submit task: memory pressure is critical",
                        remediation=[
                            "Wait for current tasks to complete",
                            "Reduce concurrent task count",
                            "Increase object store memory",
                        ],
                    )
                else:
                    # Maximum delay
                    delay = self._max_delay
                    logger.warning(
                        f"Critical memory pressure, delaying task "
                        f"submission by {delay}s"
                    )
                    time.sleep(delay)

            elif pressure.level == MemoryPressureLevel.HIGH:
                # Exponential backoff based on consecutive high count
                delay = min(
                    2 ** pressure.consecutive_high_count,
                    self._max_delay
                )
                logger.warning(
                    f"High memory pressure (consecutive: "
                    f"{pressure.consecutive_high_count}), "
                    f"delaying task submission by {delay}s"
                )
                time.sleep(delay)

            elif pressure.level == MemoryPressureLevel.ELEVATED:
                # Small delay for elevated pressure
                delay = 0.1
                logger.debug(
                    f"Elevated memory pressure, delaying task "
                    f"submission by {delay}s"
                )
                time.sleep(delay)

        # Perform the actual submission
        return submit_func(*args, **kwargs)


# Global task submitter instance
_global_task_submitter: Optional[TaskSubmitter] = None


def get_task_submitter() -> TaskSubmitter:
    """Get or create the global task submitter instance.

    Returns:
        The global TaskSubmitter instance.
    """
    global _global_task_submitter
    if _global_task_submitter is None:
        _global_task_submitter = TaskSubmitter()
    return _global_task_submitter


def configure_task_submitter(
    max_delay: float = 10.0,
    enable_backpressure: bool = True,
    reject_on_critical: bool = True,
) -> TaskSubmitter:
    """Configure the global task submitter with custom settings.

    Args:
        max_delay: Maximum delay in seconds for backpressure.
        enable_backpressure: Whether to enable backpressure delays.
        reject_on_critical: Whether to reject tasks during critical pressure.

    Returns:
        The configured TaskSubmitter instance.
    """
    global _global_task_submitter
    _global_task_submitter = TaskSubmitter(
        max_delay=max_delay,
        enable_backpressure=enable_backpressure,
        reject_on_critical=reject_on_critical,
    )
    return _global_task_submitter
