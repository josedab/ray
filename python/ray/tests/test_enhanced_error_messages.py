"""Tests for enhanced error messages with remediation steps.

This module tests the EnhancedErrorMixin and error catalog functionality
that provides actionable error messages to users.
"""

import pytest
import sys

import ray
from ray.exceptions import (
    OutOfMemoryError,
    ObjectStoreFullError,
    OutOfDiskError,
    WorkerCrashedError,
    LocalRayletDiedError,
    TaskUnschedulableError,
    ActorUnschedulableError,
    RuntimeEnvSetupError,
)
from ray._private.error_catalog import (
    ERROR_CATALOG,
    ErrorInfo,
    get_error_info,
    get_all_error_codes,
    format_error_help,
    get_remediation_for_exception,
)


class TestErrorCatalog:
    """Tests for the error catalog module."""

    def test_error_catalog_not_empty(self):
        """Test that the error catalog has entries."""
        assert len(ERROR_CATALOG) > 0

    def test_get_error_info_returns_error_info(self):
        """Test that get_error_info returns ErrorInfo objects."""
        error_info = get_error_info("OUT_OF_MEMORY")
        assert error_info is not None
        assert isinstance(error_info, ErrorInfo)
        assert error_info.error_code == "OUT_OF_MEMORY"
        assert len(error_info.remediation) > 0
        assert error_info.doc_link is not None

    def test_get_error_info_returns_none_for_unknown(self):
        """Test that get_error_info returns None for unknown codes."""
        error_info = get_error_info("UNKNOWN_ERROR_CODE")
        assert error_info is None

    def test_get_all_error_codes(self):
        """Test that get_all_error_codes returns all codes."""
        codes = get_all_error_codes()
        assert len(codes) == len(ERROR_CATALOG)
        assert "OUT_OF_MEMORY" in codes
        assert "OBJECT_STORE_FULL" in codes

    def test_format_error_help(self):
        """Test that format_error_help produces formatted output."""
        help_text = format_error_help("OUT_OF_MEMORY")
        assert help_text is not None
        assert "OUT_OF_MEMORY" in help_text
        assert "How to fix:" in help_text
        assert "See:" in help_text

    def test_format_error_help_returns_none_for_unknown(self):
        """Test that format_error_help returns None for unknown codes."""
        help_text = format_error_help("UNKNOWN_ERROR_CODE")
        assert help_text is None

    def test_get_remediation_for_exception(self):
        """Test mapping from exception type names to error info."""
        error_info = get_remediation_for_exception("OutOfMemoryError")
        assert error_info is not None
        assert error_info.error_code == "OUT_OF_MEMORY"

    def test_error_info_format_help(self):
        """Test ErrorInfo.format_help method."""
        error_info = ErrorInfo(
            error_code="TEST_ERROR",
            message="Test error message",
            causes=["Cause 1", "Cause 2"],
            remediation=["Fix 1", "Fix 2"],
            doc_link="https://example.com/docs",
        )
        help_text = error_info.format_help()
        assert "TEST_ERROR" in help_text
        assert "Test error message" in help_text
        assert "Cause 1" in help_text
        assert "Fix 1" in help_text
        assert "https://example.com/docs" in help_text


class TestEnhancedExceptions:
    """Tests for enhanced exception classes."""

    def test_out_of_memory_error_basic(self):
        """Test OutOfMemoryError with basic message."""
        error = OutOfMemoryError("Test OOM message")
        error_str = str(error)
        assert "Test OOM message" in error_str
        assert "How to fix:" in error_str
        assert "Documentation:" in error_str

    def test_out_of_memory_error_with_context(self):
        """Test OutOfMemoryError with context information."""
        context = {
            "used_memory": "8GB",
            "total_memory": "16GB",
            "threshold": "90%",
            "node_id": "test-node-123",
        }
        error = OutOfMemoryError("OOM message", context=context)
        error_str = str(error)
        assert "OOM message" in error_str
        assert "8GB" in error_str
        assert "16GB" in error_str

    def test_out_of_memory_error_has_remediation(self):
        """Test OutOfMemoryError has remediation steps."""
        error = OutOfMemoryError("OOM message")
        assert len(error.remediation) > 0
        assert error.doc_link is not None

    def test_object_store_full_error_basic(self):
        """Test ObjectStoreFullError with basic message."""
        error = ObjectStoreFullError()
        error_str = str(error)
        assert "object store is full" in error_str.lower()
        assert "How to fix:" in error_str

    def test_object_store_full_error_with_context(self):
        """Test ObjectStoreFullError with context information."""
        context = {
            "object_store_size": "10GB",
            "used_memory": "9.5GB",
            "num_objects": 1234,
        }
        error = ObjectStoreFullError(context=context)
        error_str = str(error)
        assert "10GB" in error_str

    def test_out_of_disk_error_basic(self):
        """Test OutOfDiskError with basic message."""
        error = OutOfDiskError()
        error_str = str(error)
        assert "disk" in error_str.lower()
        assert "How to fix:" in error_str

    def test_worker_crashed_error_basic(self):
        """Test WorkerCrashedError with basic message."""
        error = WorkerCrashedError()
        error_str = str(error)
        assert "worker died unexpectedly" in error_str.lower()
        assert "How to fix:" in error_str

    def test_worker_crashed_error_with_context(self):
        """Test WorkerCrashedError with context information."""
        context = {
            "worker_id": "worker-123",
            "node_id": "node-456",
            "exit_code": -9,
        }
        error = WorkerCrashedError(context=context)
        error_str = str(error)
        assert "worker-123" in error_str

    def test_local_raylet_died_error_basic(self):
        """Test LocalRayletDiedError with basic message."""
        error = LocalRayletDiedError()
        error_str = str(error)
        assert "raylet died" in error_str.lower()
        assert "How to fix:" in error_str

    def test_task_unschedulable_error_basic(self):
        """Test TaskUnschedulableError with basic message."""
        error = TaskUnschedulableError("Node is dead")
        error_str = str(error)
        assert "not schedulable" in error_str.lower()
        assert "Node is dead" in error_str
        assert "How to fix:" in error_str

    def test_actor_unschedulable_error_basic(self):
        """Test ActorUnschedulableError with basic message."""
        error = ActorUnschedulableError("Insufficient resources")
        error_str = str(error)
        assert "not schedulable" in error_str.lower()
        assert "Insufficient resources" in error_str
        assert "How to fix:" in error_str

    def test_runtime_env_setup_error_basic(self):
        """Test RuntimeEnvSetupError with basic message."""
        error = RuntimeEnvSetupError("Package not found")
        error_str = str(error)
        assert "runtime environment" in error_str.lower()
        assert "Package not found" in error_str
        assert "How to fix:" in error_str


class TestEnhancedErrorMixin:
    """Tests for the EnhancedErrorMixin functionality."""

    def test_mixin_properties_default_to_empty(self):
        """Test that mixin properties have safe defaults."""
        error = OutOfMemoryError("Test")
        # These should not raise even if not initialized
        assert isinstance(error.remediation, list)
        assert isinstance(error.diagnostic_info, dict)

    def test_mixin_error_code_property(self):
        """Test that error_code property is set correctly."""
        error = OutOfMemoryError("Test")
        assert error.error_code == "OUT_OF_MEMORY"

    def test_mixin_doc_link_property(self):
        """Test that doc_link property is set correctly."""
        error = OutOfMemoryError("Test")
        assert error.doc_link is not None
        assert "docs.ray.io" in error.doc_link


class TestBackwardsCompatibility:
    """Tests to ensure backwards compatibility."""

    def test_out_of_memory_error_old_api(self):
        """Test OutOfMemoryError works with old API (message only)."""
        error = OutOfMemoryError("Simple message")
        # Old code that just converts to string should still work
        assert "Simple message" in str(error)

    def test_object_store_full_error_old_api(self):
        """Test ObjectStoreFullError works with old API (no args)."""
        error = ObjectStoreFullError()
        # Should not raise
        str(error)

    def test_task_unschedulable_error_old_api(self):
        """Test TaskUnschedulableError works with old API."""
        error = TaskUnschedulableError("Old message")
        assert error.error_message == "Old message"
        assert "Old message" in str(error)

    def test_runtime_env_setup_error_old_api(self):
        """Test RuntimeEnvSetupError works with old API."""
        error = RuntimeEnvSetupError("Old error")
        assert error.error_message == "Old error"
        assert "Old error" in str(error)


class TestCLIErrorCommand:
    """Tests for the CLI error command."""

    def test_error_command_format_help(self):
        """Test that the error command can format help for error codes."""
        from ray._private.error_catalog import format_error_help

        # Test that we can format help for all error codes
        for code in get_all_error_codes():
            help_text = format_error_help(code)
            assert help_text is not None
            assert code in help_text

    def test_all_catalog_entries_have_required_fields(self):
        """Test that all catalog entries have required fields."""
        for code, error_info in ERROR_CATALOG.items():
            assert error_info.error_code == code
            assert error_info.message
            assert len(error_info.remediation) > 0, f"{code} has no remediation steps"
            assert error_info.doc_link, f"{code} has no doc_link"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
