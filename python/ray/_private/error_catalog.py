"""Error catalog with actionable remediation steps for common Ray errors.

This module provides a centralized catalog of error information including
causes, remediation steps, and documentation links for common Ray errors.
"""

from typing import Any, Dict, List, Optional

# Base URLs for documentation
DOC_BASE_URL = "https://docs.ray.io/en/latest"


class ErrorInfo:
    """Container for error information including remediation steps."""

    def __init__(
        self,
        error_code: str,
        message: str,
        causes: Optional[List[str]] = None,
        remediation: Optional[List[str]] = None,
        doc_link: Optional[str] = None,
    ):
        self.error_code = error_code
        self.message = message
        self.causes = causes or []
        self.remediation = remediation or []
        self.doc_link = doc_link

    def format_help(self) -> str:
        """Format the error information as a help string."""
        parts = [f"{self.error_code}", "=" * len(self.error_code), self.message]

        if self.causes:
            parts.append("\nCommon causes:")
            for cause in self.causes:
                parts.append(f"  - {cause}")

        if self.remediation:
            parts.append("\nHow to fix:")
            for i, step in enumerate(self.remediation, 1):
                parts.append(f"  {i}. {step}")

        if self.doc_link:
            parts.append(f"\nSee: {self.doc_link}")

        return "\n".join(parts)


# Catalog of common errors with remediation steps
ERROR_CATALOG: Dict[str, ErrorInfo] = {
    "OUT_OF_MEMORY": ErrorInfo(
        error_code="OUT_OF_MEMORY",
        message="The node is running out of memory",
        causes=[
            "Tasks or actors consuming too much memory",
            "Memory leaks in user code",
            "Insufficient cluster memory for workload",
            "Large objects stored in object store",
        ],
        remediation=[
            "Increase object store size: ray.init(object_store_memory=...)",
            "Enable object spilling: ray.init(_system_config={'object_spilling_config': ...})",
            "Reduce object sizes or delete references sooner with del",
            "Check memory usage: ray memory",
            "Add more nodes to the cluster or increase node memory",
        ],
        doc_link=f"{DOC_BASE_URL}/ray-core/memory-management.html",
    ),
    "OBJECT_STORE_FULL": ErrorInfo(
        error_code="OBJECT_STORE_FULL",
        message="The local object store is full",
        causes=[
            "Too many objects in scope that cannot be evicted",
            "Object store memory limit too small for workload",
            "Objects not being freed after use",
        ],
        remediation=[
            "Use ray memory command to list active objects",
            "Delete unnecessary object references with del",
            "Increase object store memory: ray.init(object_store_memory=...)",
            "Enable object spilling to disk",
            "Process data in smaller batches",
        ],
        doc_link=f"{DOC_BASE_URL}/ray-core/memory-management.html#object-store-memory",
    ),
    "OUT_OF_DISK": ErrorInfo(
        error_code="OUT_OF_DISK",
        message="The local disk is full",
        causes=[
            "Too many spilled objects",
            "Large log files accumulating",
            "Insufficient disk space for workload",
        ],
        remediation=[
            "Check disk usage with df command",
            "Clean up old Ray sessions in /tmp/ray",
            "Reduce object spilling or add more disk space",
            "Configure spill directory to a larger disk",
        ],
        doc_link=f"{DOC_BASE_URL}/ray-core/memory-management.html#object-spilling",
    ),
    "TASK_SCHEDULING_TIMEOUT": ErrorInfo(
        error_code="TASK_SCHEDULING_TIMEOUT",
        message="Task could not be scheduled within timeout",
        causes=[
            "Insufficient cluster resources",
            "Resources reserved by other tasks",
            "Invalid resource requirements",
            "Resource deadlock",
        ],
        remediation=[
            "Check available resources: ray.available_resources()",
            "Check cluster resources: ray.cluster_resources()",
            "Review task resource requirements",
            "Add more nodes or increase resources",
            "Reduce concurrent task count",
        ],
        doc_link=f"{DOC_BASE_URL}/ray-core/scheduling/index.html",
    ),
    "ACTOR_DIED_UNEXPECTEDLY": ErrorInfo(
        error_code="ACTOR_DIED_UNEXPECTEDLY",
        message="Actor process died unexpectedly",
        causes=[
            "Segmentation fault in native code",
            "Out of memory killed by OS (OOM killer)",
            "Unhandled exception in actor",
            "Node failure",
        ],
        remediation=[
            "Check actor logs in /tmp/ray/session_latest/logs",
            "Enable core dumps for debugging: ulimit -c unlimited",
            "Review memory usage patterns",
            "Enable actor restarts: @ray.remote(max_restarts=3)",
            "Check for errors in actor __init__ method",
        ],
        doc_link=f"{DOC_BASE_URL}/ray-core/actors/fault-tolerance.html",
    ),
    "WORKER_CRASHED": ErrorInfo(
        error_code="WORKER_CRASHED",
        message="The worker died unexpectedly while executing a task",
        causes=[
            "Segmentation fault in native code or C extensions",
            "Memory corruption",
            "Out of memory killed by OS",
            "Unhandled signal",
        ],
        remediation=[
            "Check python-core-worker-*.log files for details",
            "Review recent code changes for memory issues",
            "Enable core dumps for debugging",
            "Reduce memory usage in the task",
            "Increase max_retries for the task",
        ],
        doc_link=f"{DOC_BASE_URL}/ray-core/fault-tolerance/tasks.html",
    ),
    "OBJECT_LOST": ErrorInfo(
        error_code="OBJECT_LOST",
        message="Object was lost from distributed memory",
        causes=[
            "Node failure where object was stored",
            "Object evicted and lineage lost",
            "System error during object transfer",
        ],
        remediation=[
            "Enable object reconstruction with lineage: @ray.remote(max_retries=3)",
            "Check cluster logs for node failures",
            "Increase lineage storage: RAY_max_lineage_bytes=...",
            "Use ray.put() with more replicas",
        ],
        doc_link=f"{DOC_BASE_URL}/ray-core/fault-tolerance/objects.html",
    ),
    "OWNER_DIED": ErrorInfo(
        error_code="OWNER_DIED",
        message="The object's owner has exited",
        causes=[
            "Driver process exited while tasks still running",
            "Worker that created object crashed",
            "Object reference passed to long-running task",
        ],
        remediation=[
            "Ensure driver stays alive until all tasks complete",
            "Use ray.get() to wait for results before exiting",
            "Check owner worker logs for crash details",
            "Consider using detached actors for long-running work",
        ],
        doc_link=f"{DOC_BASE_URL}/ray-core/fault-tolerance/objects.html#object-ownership",
    ),
    "TASK_UNSCHEDULABLE": ErrorInfo(
        error_code="TASK_UNSCHEDULABLE",
        message="The task cannot be scheduled",
        causes=[
            "Specified node is dead",
            "Required resources not available in cluster",
            "Placement group removed",
            "Invalid scheduling constraints",
        ],
        remediation=[
            "Check node status: ray status",
            "Review scheduling strategy configuration",
            "Verify resource requirements are satisfiable",
            "Check if placement group still exists",
        ],
        doc_link=f"{DOC_BASE_URL}/ray-core/scheduling/index.html",
    ),
    "ACTOR_UNSCHEDULABLE": ErrorInfo(
        error_code="ACTOR_UNSCHEDULABLE",
        message="The actor cannot be scheduled",
        causes=[
            "Specified node is dead",
            "Required resources not available",
            "Placement group removed",
            "Invalid scheduling constraints",
        ],
        remediation=[
            "Check node status: ray status",
            "Review actor resource requirements",
            "Verify placement constraints are satisfiable",
            "Check if placement group still exists",
        ],
        doc_link=f"{DOC_BASE_URL}/ray-core/actors/scheduling.html",
    ),
    "RUNTIME_ENV_SETUP_ERROR": ErrorInfo(
        error_code="RUNTIME_ENV_SETUP_ERROR",
        message="Failed to set up runtime environment",
        causes=[
            "Invalid pip/conda packages specified",
            "Network issues downloading packages",
            "Insufficient disk space",
            "Invalid working directory path",
        ],
        remediation=[
            "Check package names and versions are correct",
            "Verify network connectivity to package repositories",
            "Check available disk space on worker nodes",
            "Review runtime_env configuration syntax",
            "Check worker logs for detailed error messages",
        ],
        doc_link=f"{DOC_BASE_URL}/ray-core/handling-dependencies.html",
    ),
    "PENDING_CALLS_LIMIT_EXCEEDED": ErrorInfo(
        error_code="PENDING_CALLS_LIMIT_EXCEEDED",
        message="The pending actor calls exceeds max_pending_calls limit",
        causes=[
            "Caller sending requests faster than actor can process",
            "Actor blocking on slow operations",
            "Backpressure threshold too low",
        ],
        remediation=[
            "Increase max_pending_calls: @ray.remote(max_pending_calls=...)",
            "Add backpressure handling in caller",
            "Optimize actor method performance",
            "Use async methods in the actor",
            "Scale out with more actor replicas",
        ],
        doc_link=f"{DOC_BASE_URL}/ray-core/actors/async_api.html#limiting-pending-calls",
    ),
    "GET_TIMEOUT": ErrorInfo(
        error_code="GET_TIMEOUT",
        message="Call to ray.get() timed out",
        causes=[
            "Task taking longer than expected",
            "Worker crashed during task execution",
            "Resource starvation causing scheduling delays",
            "Network issues between nodes",
        ],
        remediation=[
            "Increase timeout: ray.get(ref, timeout=...)",
            "Check task is making progress",
            "Review task resource requirements",
            "Check cluster health: ray status",
            "Look for errors in worker logs",
        ],
        doc_link=f"{DOC_BASE_URL}/ray-core/api/doc/ray.get.html",
    ),
    "LOCAL_RAYLET_DIED": ErrorInfo(
        error_code="LOCAL_RAYLET_DIED",
        message="The task's local raylet died",
        causes=[
            "Node failure",
            "Raylet crashed due to bug or resource exhaustion",
            "Network partition",
        ],
        remediation=[
            "Check raylet.out log for error details",
            "Verify node is still in cluster: ray status",
            "Review system resources (memory, disk) on the node",
            "Check for recent cluster configuration changes",
        ],
        doc_link=f"{DOC_BASE_URL}/cluster/kubernetes/user-guides/logging.html",
    ),
    "NODE_DIED": ErrorInfo(
        error_code="NODE_DIED",
        message="The node is either dead or unreachable",
        causes=[
            "Node hardware failure",
            "Network connectivity issues",
            "Node removed by autoscaler",
            "Out of memory causing system crash",
        ],
        remediation=[
            "Check cluster status: ray status",
            "Review autoscaler logs for scaling decisions",
            "Check node health and network connectivity",
            "Review system logs on the affected node",
        ],
        doc_link=f"{DOC_BASE_URL}/cluster/kubernetes/user-guides/observability.html",
    ),
    "TASK_CANCELLED": ErrorInfo(
        error_code="TASK_CANCELLED",
        message="Task was cancelled",
        causes=[
            "Explicit call to ray.cancel()",
            "Parent task/actor cancelled",
            "Timeout triggered cancellation",
        ],
        remediation=[
            "Handle TaskCancelledError in your code",
            "Check if cancellation was intentional",
            "Review cancellation logic in caller",
        ],
        doc_link=f"{DOC_BASE_URL}/ray-core/api/doc/ray.cancel.html",
    ),
    "RPC_ERROR": ErrorInfo(
        error_code="RPC_ERROR",
        message="Error in the underlying RPC system",
        causes=[
            "Network connectivity issues",
            "Target process unavailable",
            "RPC timeout exceeded",
            "Serialization/deserialization failure",
        ],
        remediation=[
            "Check network connectivity between nodes",
            "Verify target worker/raylet is running",
            "Review RPC timeout configuration",
            "Check for serialization issues in arguments",
        ],
        doc_link=f"{DOC_BASE_URL}/ray-core/fault-tolerance/gcs.html",
    ),
}


def get_error_info(error_code: str) -> Optional[ErrorInfo]:
    """Get error information by error code.

    Args:
        error_code: The error code to look up.

    Returns:
        ErrorInfo object if found, None otherwise.
    """
    return ERROR_CATALOG.get(error_code)


def get_all_error_codes() -> List[str]:
    """Get all available error codes.

    Returns:
        List of all error codes in the catalog.
    """
    return list(ERROR_CATALOG.keys())


def format_error_help(error_code: str) -> Optional[str]:
    """Format help text for an error code.

    Args:
        error_code: The error code to get help for.

    Returns:
        Formatted help string if found, None otherwise.
    """
    error_info = get_error_info(error_code)
    if error_info:
        return error_info.format_help()
    return None


def get_remediation_for_exception(exception_type: str) -> Optional[ErrorInfo]:
    """Map exception type names to error catalog entries.

    Args:
        exception_type: The name of the exception class.

    Returns:
        ErrorInfo object if a mapping exists, None otherwise.
    """
    # Map exception type names to error codes
    exception_to_error_code = {
        "OutOfMemoryError": "OUT_OF_MEMORY",
        "ObjectStoreFullError": "OBJECT_STORE_FULL",
        "OutOfDiskError": "OUT_OF_DISK",
        "WorkerCrashedError": "WORKER_CRASHED",
        "ActorDiedError": "ACTOR_DIED_UNEXPECTEDLY",
        "ObjectLostError": "OBJECT_LOST",
        "OwnerDiedError": "OWNER_DIED",
        "TaskUnschedulableError": "TASK_UNSCHEDULABLE",
        "ActorUnschedulableError": "ACTOR_UNSCHEDULABLE",
        "RuntimeEnvSetupError": "RUNTIME_ENV_SETUP_ERROR",
        "PendingCallsLimitExceeded": "PENDING_CALLS_LIMIT_EXCEEDED",
        "GetTimeoutError": "GET_TIMEOUT",
        "LocalRayletDiedError": "LOCAL_RAYLET_DIED",
        "NodeDiedError": "NODE_DIED",
        "TaskCancelledError": "TASK_CANCELLED",
        "RpcError": "RPC_ERROR",
    }

    error_code = exception_to_error_code.get(exception_type)
    if error_code:
        return get_error_info(error_code)
    return None
