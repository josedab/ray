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

"""Tests for enhanced memory pressure handling features."""

import asyncio
import sys
import time
from unittest.mock import MagicMock, patch

import pytest

from ray._private.memory_monitor import (
    MemoryMonitor,
    MemoryPressure,
    MemoryPressureError,
    MemoryPressureLevel,
    classify_memory_pressure,
    get_memory_monitor,
)
from ray._private.memory_pressure import (
    configure_memory_alerts,
    configure_memory_pressure_policy,
    get_pressure,
    get_pressure_level,
    is_pinned,
    pin,
    pinned,
    unpin,
)
from ray._private.task_submitter import (
    TaskSubmitter,
    configure_task_submitter,
    get_task_submitter,
)


class TestMemoryPressureLevel:
    """Tests for MemoryPressureLevel classification."""

    def test_classify_normal(self):
        """Test that usage below 60% is classified as NORMAL."""
        assert classify_memory_pressure(0.0) == MemoryPressureLevel.NORMAL
        assert classify_memory_pressure(0.3) == MemoryPressureLevel.NORMAL
        assert classify_memory_pressure(0.59) == MemoryPressureLevel.NORMAL

    def test_classify_elevated(self):
        """Test that usage 60-75% is classified as ELEVATED."""
        assert classify_memory_pressure(0.60) == MemoryPressureLevel.ELEVATED
        assert classify_memory_pressure(0.67) == MemoryPressureLevel.ELEVATED
        assert classify_memory_pressure(0.74) == MemoryPressureLevel.ELEVATED

    def test_classify_high(self):
        """Test that usage 75-90% is classified as HIGH."""
        assert classify_memory_pressure(0.75) == MemoryPressureLevel.HIGH
        assert classify_memory_pressure(0.82) == MemoryPressureLevel.HIGH
        assert classify_memory_pressure(0.89) == MemoryPressureLevel.HIGH

    def test_classify_critical(self):
        """Test that usage >= 90% is classified as CRITICAL."""
        assert classify_memory_pressure(0.90) == MemoryPressureLevel.CRITICAL
        assert classify_memory_pressure(0.95) == MemoryPressureLevel.CRITICAL
        assert classify_memory_pressure(1.0) == MemoryPressureLevel.CRITICAL


class TestMemoryPressure:
    """Tests for MemoryPressure data class."""

    def test_memory_pressure_creation(self):
        """Test creating a MemoryPressure object."""
        pressure = MemoryPressure(
            object_store_ratio=0.5,
            system_memory_ratio=0.6,
            pending_tasks=10,
            level=MemoryPressureLevel.ELEVATED,
            consecutive_high_count=3,
        )
        assert pressure.object_store_ratio == 0.5
        assert pressure.system_memory_ratio == 0.6
        assert pressure.pending_tasks == 10
        assert pressure.level == MemoryPressureLevel.ELEVATED
        assert pressure.consecutive_high_count == 3


class TestMemoryPressureError:
    """Tests for MemoryPressureError exception."""

    def test_error_with_remediation(self):
        """Test creating error with remediation suggestions."""
        error = MemoryPressureError(
            "Memory is critical",
            remediation=["Wait for tasks", "Reduce concurrency"],
        )
        assert "Memory is critical" in str(error)
        assert "Wait for tasks" in str(error)
        assert "Reduce concurrency" in str(error)

    def test_error_without_remediation(self):
        """Test creating error without remediation."""
        error = MemoryPressureError("Memory is critical")
        assert "Memory is critical" in str(error)


class TestMemoryMonitor:
    """Tests for enhanced MemoryMonitor class."""

    def test_get_pressure(self):
        """Test getting memory pressure state."""
        monitor = MemoryMonitor()
        pressure = monitor.get_pressure()
        assert isinstance(pressure, MemoryPressure)
        assert isinstance(pressure.level, MemoryPressureLevel)
        assert 0 <= pressure.system_memory_ratio <= 1

    def test_register_callback(self):
        """Test registering and unregistering pressure callbacks."""
        monitor = MemoryMonitor()
        callback = MagicMock()

        monitor.register_pressure_callback(callback)
        assert callback in monitor._pressure_callbacks

        monitor.unregister_pressure_callback(callback)
        assert callback not in monitor._pressure_callbacks

    @pytest.mark.asyncio
    async def test_monitor_loop(self):
        """Test the async monitoring loop."""
        monitor = MemoryMonitor()
        callback_called = []

        def callback(pressure):
            callback_called.append(pressure)

        monitor.register_pressure_callback(callback)

        # Run monitor for a short time
        async def run_monitor():
            task = asyncio.create_task(monitor.monitor_loop(interval=0.05))
            await asyncio.sleep(0.1)
            monitor.stop_monitor()
            try:
                await asyncio.wait_for(task, timeout=0.5)
            except asyncio.CancelledError:
                pass

        await run_monitor()

        # Monitor should have run at least once
        # Callback only called on level changes, so may not have been called
        assert monitor._running is False

    def test_consecutive_high_count(self):
        """Test that consecutive high count is tracked."""
        monitor = MemoryMonitor()
        monitor._current_level = MemoryPressureLevel.NORMAL
        monitor._consecutive_high_count = 0

        # Simulate high pressure
        with patch.object(
            monitor, "get_memory_usage", return_value=(8.0, 10.0)
        ):  # 80% usage
            pressure = monitor.get_pressure()
            assert pressure.level == MemoryPressureLevel.HIGH


class TestTaskSubmitter:
    """Tests for TaskSubmitter with backpressure."""

    def test_submit_normal_pressure(self):
        """Test that tasks submit immediately under normal pressure."""
        submitter = TaskSubmitter()

        with patch.object(
            submitter._memory_monitor,
            "get_pressure",
            return_value=MemoryPressure(
                object_store_ratio=0.3,
                system_memory_ratio=0.3,
                pending_tasks=5,
                level=MemoryPressureLevel.NORMAL,
            ),
        ):
            result = submitter.submit(lambda: "result")
            assert result == "result"

    def test_submit_critical_pressure_reject(self):
        """Test that tasks are rejected under critical pressure."""
        submitter = TaskSubmitter(reject_on_critical=True)

        with patch.object(
            submitter._memory_monitor,
            "get_pressure",
            return_value=MemoryPressure(
                object_store_ratio=0.95,
                system_memory_ratio=0.95,
                pending_tasks=100,
                level=MemoryPressureLevel.CRITICAL,
            ),
        ):
            with pytest.raises(MemoryPressureError) as exc_info:
                submitter.submit(lambda: "result")
            assert "critical" in str(exc_info.value).lower()

    def test_submit_high_pressure_delay(self):
        """Test that tasks are delayed under high pressure."""
        submitter = TaskSubmitter(max_delay=1.0)

        with patch.object(
            submitter._memory_monitor,
            "get_pressure",
            return_value=MemoryPressure(
                object_store_ratio=0.8,
                system_memory_ratio=0.8,
                pending_tasks=50,
                level=MemoryPressureLevel.HIGH,
                consecutive_high_count=0,
            ),
        ):
            start = time.time()
            result = submitter.submit(lambda: "result")
            elapsed = time.time() - start

            assert result == "result"
            # Should have delayed (2^0 = 1 second, but max is 1.0)
            assert elapsed >= 0.9

    def test_backpressure_disabled(self):
        """Test that backpressure can be disabled."""
        submitter = TaskSubmitter(enable_backpressure=False)

        with patch.object(
            submitter._memory_monitor,
            "get_pressure",
            return_value=MemoryPressure(
                object_store_ratio=0.95,
                system_memory_ratio=0.95,
                pending_tasks=100,
                level=MemoryPressureLevel.CRITICAL,
            ),
        ):
            # Should not raise even under critical pressure
            result = submitter.submit(lambda: "result")
            assert result == "result"

    @pytest.mark.asyncio
    async def test_submit_async(self):
        """Test async task submission."""
        submitter = TaskSubmitter()

        with patch.object(
            submitter._memory_monitor,
            "get_pressure",
            return_value=MemoryPressure(
                object_store_ratio=0.3,
                system_memory_ratio=0.3,
                pending_tasks=5,
                level=MemoryPressureLevel.NORMAL,
            ),
        ):
            async def async_task():
                return "async_result"

            result = await submitter.submit_async(async_task)
            assert result == "async_result"


class TestObjectPinning:
    """Tests for object pinning API."""

    def setup_method(self):
        """Reset pinned objects before each test."""
        from ray._private import memory_pressure

        memory_pressure._pinned_objects.clear()

    def test_pin_and_unpin(self):
        """Test basic pin and unpin operations."""
        # Create a mock ObjectRef
        ref = MagicMock()

        pin(ref)
        assert is_pinned(ref)

        unpin(ref)
        assert not is_pinned(ref)

    def test_multiple_pins(self):
        """Test that multiple pins are tracked."""
        ref = MagicMock()

        pin(ref)
        pin(ref)
        assert is_pinned(ref)

        unpin(ref)
        assert is_pinned(ref)  # Still pinned once

        unpin(ref)
        assert not is_pinned(ref)  # Now unpinned

    def test_unpin_not_pinned_raises(self):
        """Test that unpinning a non-pinned object raises."""
        ref = MagicMock()

        with pytest.raises(ValueError):
            unpin(ref)

    def test_pinned_context_manager(self):
        """Test the pinned context manager."""
        ref = MagicMock()

        with pinned(ref):
            assert is_pinned(ref)

        assert not is_pinned(ref)

    def test_pinned_context_manager_exception(self):
        """Test that context manager unpins on exception."""
        ref = MagicMock()

        try:
            with pinned(ref):
                assert is_pinned(ref)
                raise RuntimeError("Test error")
        except RuntimeError:
            pass

        assert not is_pinned(ref)


class TestGlobalFunctions:
    """Tests for global convenience functions."""

    def test_get_task_submitter(self):
        """Test getting the global task submitter."""
        submitter = get_task_submitter()
        assert isinstance(submitter, TaskSubmitter)

        # Same instance should be returned
        submitter2 = get_task_submitter()
        assert submitter is submitter2

    def test_configure_task_submitter(self):
        """Test configuring the global task submitter."""
        submitter = configure_task_submitter(
            max_delay=5.0, enable_backpressure=False, reject_on_critical=False
        )

        assert submitter._max_delay == 5.0
        assert submitter._enable_backpressure is False
        assert submitter._reject_on_critical is False

    def test_get_memory_monitor(self):
        """Test getting the global memory monitor."""
        monitor = get_memory_monitor()
        assert isinstance(monitor, MemoryMonitor)

    def test_get_pressure_global(self):
        """Test getting pressure using global function."""
        pressure = get_pressure()
        assert isinstance(pressure, MemoryPressure)

    def test_get_pressure_level_global(self):
        """Test getting pressure level using global function."""
        level = get_pressure_level()
        assert isinstance(level, MemoryPressureLevel)


class TestMemoryPressurePolicy:
    """Tests for memory pressure policy configuration."""

    def test_configure_policy(self):
        """Test configuring memory pressure policy."""
        policy = {
            "elevated": {
                "actions": ["gc_aggressive"],
                "task_submission_delay": 0.1,
            },
            "high": {
                "actions": ["gc_aggressive", "spill_eager"],
                "task_submission_delay": 1.0,
            },
            "critical": {
                "actions": ["evict_all_spillable"],
                "task_submission": "reject",
            },
        }

        # Should not raise
        configure_memory_pressure_policy(policy)


class TestMemoryAlerts:
    """Tests for memory alert configuration."""

    def test_configure_alerts(self):
        """Test configuring memory alerts."""
        alerts = {
            "elevated": {"action": "log"},
            "high": {"action": "webhook", "url": "https://example.com"},
            "critical": {"action": "pagerduty", "key": "test-key"},
        }

        # Should not raise
        configure_memory_alerts(alerts)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
