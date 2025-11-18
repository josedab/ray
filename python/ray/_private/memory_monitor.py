import asyncio
import logging
import os
import platform
import sys
import time
from dataclasses import dataclass
from enum import Enum
from typing import Callable, List, Optional

import ray  # noqa F401

# Import ray before psutil will make sure we use psutil's bundled version
from ray._common.utils import get_system_memory

import psutil  # noqa E402

logger = logging.getLogger(__name__)


class MemoryPressureLevel(Enum):
    """Memory pressure levels for proactive memory management."""
    NORMAL = 0    # < 60% - Normal operation
    ELEVATED = 1  # 60-75% - Start proactive measures
    HIGH = 2      # 75-90% - Aggressive measures
    CRITICAL = 3  # > 90% - Emergency measures


@dataclass
class MemoryPressure:
    """Represents the current memory pressure state."""
    object_store_ratio: float
    system_memory_ratio: float
    pending_tasks: int
    level: MemoryPressureLevel
    consecutive_high_count: int = 0


def classify_memory_pressure(usage_ratio: float) -> MemoryPressureLevel:
    """Classify memory usage ratio into a pressure level.

    Args:
        usage_ratio: Memory usage ratio between 0 and 1.

    Returns:
        The corresponding MemoryPressureLevel.
    """
    if usage_ratio >= 0.90:
        return MemoryPressureLevel.CRITICAL
    elif usage_ratio >= 0.75:
        return MemoryPressureLevel.HIGH
    elif usage_ratio >= 0.60:
        return MemoryPressureLevel.ELEVATED
    else:
        return MemoryPressureLevel.NORMAL


class MemoryPressureError(Exception):
    """Exception raised when memory pressure is too high to proceed."""

    def __init__(self, msg: str, remediation: Optional[List[str]] = None):
        self.remediation = remediation or []
        full_msg = msg
        if self.remediation:
            full_msg += "\n\nSuggested actions:\n"
            for i, action in enumerate(self.remediation, 1):
                full_msg += f"  {i}. {action}\n"
        super().__init__(full_msg)


def get_rss(memory_info):
    """Get the estimated non-shared memory usage from psutil memory_info."""
    mem = memory_info.rss
    # OSX doesn't have the shared attribute
    if hasattr(memory_info, "shared"):
        mem -= memory_info.shared
    return mem


def get_shared(virtual_memory):
    """Get the estimated shared memory usage from psutil virtual mem info."""
    # OSX doesn't have the shared attribute
    if hasattr(virtual_memory, "shared"):
        return virtual_memory.shared
    else:
        return 0


def get_top_n_memory_usage(n: int = 10):
    """Get the top n memory usage of the process

    Params:
        n: Number of top n process memory usage to return.
    Returns:
        (str) The formatted string of top n process memory usage.
    """
    proc_stats = []
    for proc in psutil.process_iter(["memory_info", "cmdline"]):
        try:
            proc_stats.append(
                (get_rss(proc.info["memory_info"]), proc.pid, proc.info["cmdline"])
            )
        except psutil.NoSuchProcess:
            # We should skip the process that has exited. Refer this
            # issue for more detail:
            # https://github.com/ray-project/ray/issues/14929
            continue
        except psutil.AccessDenied:
            # On MacOS, the proc_pidinfo call (used to get per-process
            # memory info) fails with a permission denied error when used
            # on a process that isn’t owned by the same user. For now, we
            # drop the memory info of any such process, assuming that
            # processes owned by other users (e.g. root) aren't Ray
            # processes and will be of less interest when an OOM happens
            # on a Ray node.
            # See issue for more detail:
            # https://github.com/ray-project/ray/issues/11845#issuecomment-849904019  # noqa: E501
            continue
    proc_str = "PID\tMEM\tCOMMAND"
    for rss, pid, cmdline in sorted(proc_stats, reverse=True)[:n]:
        proc_str += "\n{}\t{}GiB\t{}".format(
            pid, round(rss / (1024**3), 2), " ".join(cmdline)[:100].strip()
        )
    return proc_str


class RayOutOfMemoryError(Exception):
    def __init__(self, msg):
        Exception.__init__(self, msg)

    @staticmethod
    def get_message(used_gb, total_gb, threshold):
        proc_str = get_top_n_memory_usage(n=10)
        return (
            "More than {}% of the memory on ".format(int(100 * threshold))
            + "node {} is used ({} / {} GB). ".format(
                platform.node(), round(used_gb, 2), round(total_gb, 2)
            )
            + f"The top 10 memory consumers are:\n\n{proc_str}"
            + "\n\nIn addition, up to {} GiB of shared memory is ".format(
                round(get_shared(psutil.virtual_memory()) / (1024**3), 2)
            )
            + "currently being used by the Ray object store.\n---\n"
            "--- Tip: Use the `ray memory` command to list active "
            "objects in the cluster.\n"
            "--- To disable OOM exceptions, set "
            "RAY_DISABLE_MEMORY_MONITOR=1.\n---\n"
        )


class MemoryMonitor:
    """Helper class for raising errors on low memory.

    This presents a much cleaner error message to users than what would happen
    if we actually ran out of memory.

    The monitor tries to use the cgroup memory limit and usage if it is set
    and available so that it is more reasonable inside containers. Otherwise,
    it uses `psutil` to check the memory usage.

    The environment variable `RAY_MEMORY_MONITOR_ERROR_THRESHOLD` can be used
    to overwrite the default error_threshold setting.

    Used by test only. For production code use memory_monitor.cc
    """

    def __init__(self, error_threshold=0.95, check_interval=1):
        # Note: it takes ~50us to check the memory usage through psutil, so
        # throttle this check at most once a second or so.
        self.check_interval = check_interval
        self.last_checked = 0
        try:
            self.error_threshold = float(
                os.getenv("RAY_MEMORY_MONITOR_ERROR_THRESHOLD")
            )
        except (ValueError, TypeError):
            self.error_threshold = error_threshold
        # Try to read the cgroup memory limit if it is available.
        try:
            with open("/sys/fs/cgroup/memory/memory.limit_in_bytes", "rb") as f:
                self.cgroup_memory_limit_gb = int(f.read()) / (1024**3)
        except IOError:
            self.cgroup_memory_limit_gb = sys.maxsize / (1024**3)
        if not psutil:
            logger.warning(
                "WARNING: Not monitoring node memory since `psutil` "
                "is not installed. Install this with "
                "`pip install psutil` to enable "
                "debugging of memory-related crashes."
            )
        self.disabled = (
            "RAY_DEBUG_DISABLE_MEMORY_MONITOR" in os.environ
            or "RAY_DISABLE_MEMORY_MONITOR" in os.environ
        )

        # Memory pressure tracking
        self._pressure_callbacks: List[Callable[[MemoryPressure], None]] = []
        self._current_level = MemoryPressureLevel.NORMAL
        self._consecutive_high_count = 0
        self._running = False

    def get_memory_usage(self):
        from ray._private.utils import get_used_memory

        total_gb = get_system_memory() / (1024**3)
        used_gb = get_used_memory() / (1024**3)

        return used_gb, total_gb

    def get_pressure(self) -> MemoryPressure:
        """Calculate and return the current memory pressure state.

        Returns:
            MemoryPressure object with current memory state.
        """
        used_gb, total_gb = self.get_memory_usage()
        system_ratio = used_gb / total_gb if total_gb > 0 else 0

        # Get object store usage if available
        try:
            from ray._private.internal_api import memory_summary
            # This is a simplified approach - in production you'd get actual metrics
            object_store_ratio = system_ratio  # Simplified
        except Exception:
            object_store_ratio = system_ratio

        # Get pending task count if available
        try:
            pending_tasks = 0  # Would get from scheduler in production
        except Exception:
            pending_tasks = 0

        level = classify_memory_pressure(system_ratio)

        return MemoryPressure(
            object_store_ratio=object_store_ratio,
            system_memory_ratio=system_ratio,
            pending_tasks=pending_tasks,
            level=level,
            consecutive_high_count=self._consecutive_high_count
        )

    def register_pressure_callback(
        self, callback: Callable[[MemoryPressure], None]
    ) -> None:
        """Register a callback to be invoked when pressure level changes.

        Args:
            callback: Function that takes a MemoryPressure object.
        """
        self._pressure_callbacks.append(callback)

    def unregister_pressure_callback(
        self, callback: Callable[[MemoryPressure], None]
    ) -> None:
        """Unregister a previously registered callback.

        Args:
            callback: The callback to remove.
        """
        if callback in self._pressure_callbacks:
            self._pressure_callbacks.remove(callback)

    async def _notify_pressure_change(self, pressure: MemoryPressure) -> None:
        """Notify all registered callbacks of a pressure change.

        Args:
            pressure: The new pressure state.
        """
        for callback in self._pressure_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(pressure)
                else:
                    callback(pressure)
            except Exception as e:
                logger.error(f"Error in pressure callback: {e}")

    async def monitor_loop(self, interval: float = 0.1) -> None:
        """Async monitoring loop that checks memory pressure periodically.

        Args:
            interval: Time in seconds between checks (default 100ms).
        """
        self._running = True
        while self._running:
            pressure = self.get_pressure()

            # Track consecutive high pressure count
            if pressure.level in (MemoryPressureLevel.HIGH,
                                  MemoryPressureLevel.CRITICAL):
                self._consecutive_high_count += 1
            else:
                self._consecutive_high_count = 0

            pressure.consecutive_high_count = self._consecutive_high_count

            # Notify if level changed
            if pressure.level != self._current_level:
                logger.info(
                    f"Memory pressure level changed: "
                    f"{self._current_level.name} -> {pressure.level.name}"
                )
                await self._notify_pressure_change(pressure)
                self._current_level = pressure.level

            await asyncio.sleep(interval)

    def stop_monitor(self) -> None:
        """Stop the monitoring loop."""
        self._running = False

    def raise_if_low_memory(self):
        if self.disabled:
            return

        if time.time() - self.last_checked > self.check_interval:
            self.last_checked = time.time()
            used_gb, total_gb = self.get_memory_usage()

            if used_gb > total_gb * self.error_threshold:
                raise RayOutOfMemoryError(
                    RayOutOfMemoryError.get_message(
                        used_gb, total_gb, self.error_threshold
                    )
                )
            else:
                logger.debug(f"Memory usage is {used_gb} / {total_gb}")


# Global memory monitor instance
_global_memory_monitor: Optional[MemoryMonitor] = None


def get_memory_monitor() -> MemoryMonitor:
    """Get or create the global memory monitor instance.

    Returns:
        The global MemoryMonitor instance.
    """
    global _global_memory_monitor
    if _global_memory_monitor is None:
        _global_memory_monitor = MemoryMonitor()
    return _global_memory_monitor
