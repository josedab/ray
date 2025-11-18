"""
Usage tracking for multi-tenant isolation in Ray.
"""

import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from ray.multi_tenant.tenant import Tenant, ResourceQuota


@dataclass
class TenantUsage:
    """
    Tracks current resource usage for a tenant.

    Attributes:
        tenant_id: ID of the tenant.
        cpus_used: Number of CPUs currently in use.
        gpus_used: Number of GPUs currently in use.
        memory_gb_used: Memory in GB currently in use.
        object_store_gb_used: Object store memory in GB currently in use.
        running_tasks: Number of currently running tasks.
        pending_tasks: Number of pending tasks.
        actors: Number of active actors.
        placement_groups: Number of active placement groups.
        cpus_quota: CPU quota from tenant config.
        gpus_quota: GPU quota from tenant config.
        memory_gb_quota: Memory quota from tenant config.
        object_store_gb_quota: Object store quota from tenant config.
        timestamp: Timestamp when usage was recorded.
    """
    tenant_id: str
    cpus_used: float = 0.0
    gpus_used: float = 0.0
    memory_gb_used: float = 0.0
    object_store_gb_used: float = 0.0
    running_tasks: int = 0
    pending_tasks: int = 0
    actors: int = 0
    placement_groups: int = 0
    cpus_quota: Optional[int] = None
    gpus_quota: Optional[int] = None
    memory_gb_quota: Optional[float] = None
    object_store_gb_quota: Optional[float] = None
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "tenant_id": self.tenant_id,
            "cpus_used": self.cpus_used,
            "gpus_used": self.gpus_used,
            "memory_gb_used": self.memory_gb_used,
            "object_store_gb_used": self.object_store_gb_used,
            "running_tasks": self.running_tasks,
            "pending_tasks": self.pending_tasks,
            "actors": self.actors,
            "placement_groups": self.placement_groups,
            "cpus_quota": self.cpus_quota,
            "gpus_quota": self.gpus_quota,
            "memory_gb_quota": self.memory_gb_quota,
            "object_store_gb_quota": self.object_store_gb_quota,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TenantUsage":
        """Create from dictionary representation."""
        return cls(
            tenant_id=data["tenant_id"],
            cpus_used=data.get("cpus_used", 0.0),
            gpus_used=data.get("gpus_used", 0.0),
            memory_gb_used=data.get("memory_gb_used", 0.0),
            object_store_gb_used=data.get("object_store_gb_used", 0.0),
            running_tasks=data.get("running_tasks", 0),
            pending_tasks=data.get("pending_tasks", 0),
            actors=data.get("actors", 0),
            placement_groups=data.get("placement_groups", 0),
            cpus_quota=data.get("cpus_quota"),
            gpus_quota=data.get("gpus_quota"),
            memory_gb_quota=data.get("memory_gb_quota"),
            object_store_gb_quota=data.get("object_store_gb_quota"),
            timestamp=data.get("timestamp", time.time()),
        )

    def cpu_utilization(self) -> Optional[float]:
        """Get CPU utilization as a percentage of quota."""
        if self.cpus_quota is None or self.cpus_quota == 0:
            return None
        return (self.cpus_used / self.cpus_quota) * 100

    def gpu_utilization(self) -> Optional[float]:
        """Get GPU utilization as a percentage of quota."""
        if self.gpus_quota is None or self.gpus_quota == 0:
            return None
        return (self.gpus_used / self.gpus_quota) * 100

    def memory_utilization(self) -> Optional[float]:
        """Get memory utilization as a percentage of quota."""
        if self.memory_gb_quota is None or self.memory_gb_quota == 0:
            return None
        return (self.memory_gb_used / self.memory_gb_quota) * 100

    def is_over_quota(self) -> bool:
        """Check if tenant is over any quota."""
        if self.cpus_quota is not None and self.cpus_used > self.cpus_quota:
            return True
        if self.gpus_quota is not None and self.gpus_used > self.gpus_quota:
            return True
        if (
            self.memory_gb_quota is not None
            and self.memory_gb_used > self.memory_gb_quota
        ):
            return True
        if (
            self.object_store_gb_quota is not None
            and self.object_store_gb_used > self.object_store_gb_quota
        ):
            return True
        return False


class TenantUsageTracker:
    """
    Tracks resource usage for all tenants in a Ray cluster.
    """

    def __init__(self):
        """Initialize the usage tracker."""
        self._usage: Dict[str, TenantUsage] = {}
        self._tenants: Dict[str, Tenant] = {}
        self._lock = threading.RLock()
        self._history: Dict[str, List[TenantUsage]] = {}
        self._history_max_entries = 1000

    def register_tenant(self, tenant: Tenant) -> None:
        """
        Register a tenant for usage tracking.

        Args:
            tenant: The tenant to register.
        """
        with self._lock:
            self._tenants[tenant.id] = tenant
            self._usage[tenant.id] = TenantUsage(
                tenant_id=tenant.id,
                cpus_quota=tenant.resource_quota.max_cpus,
                gpus_quota=tenant.resource_quota.max_gpus,
                memory_gb_quota=tenant.resource_quota.max_memory_gb,
                object_store_gb_quota=tenant.resource_quota.max_object_store_gb,
            )
            self._history[tenant.id] = []

    def unregister_tenant(self, tenant_id: str) -> None:
        """
        Unregister a tenant from usage tracking.

        Args:
            tenant_id: ID of the tenant to unregister.
        """
        with self._lock:
            self._tenants.pop(tenant_id, None)
            self._usage.pop(tenant_id, None)
            self._history.pop(tenant_id, None)

    def update_usage(
        self,
        tenant_id: str,
        cpus_used: Optional[float] = None,
        gpus_used: Optional[float] = None,
        memory_gb_used: Optional[float] = None,
        object_store_gb_used: Optional[float] = None,
        running_tasks: Optional[int] = None,
        pending_tasks: Optional[int] = None,
        actors: Optional[int] = None,
        placement_groups: Optional[int] = None,
    ) -> None:
        """
        Update the usage for a tenant.

        Args:
            tenant_id: ID of the tenant.
            cpus_used: New CPU usage value (if provided).
            gpus_used: New GPU usage value (if provided).
            memory_gb_used: New memory usage value (if provided).
            object_store_gb_used: New object store usage value (if provided).
            running_tasks: New running tasks count (if provided).
            pending_tasks: New pending tasks count (if provided).
            actors: New actors count (if provided).
            placement_groups: New placement groups count (if provided).
        """
        with self._lock:
            if tenant_id not in self._usage:
                raise ValueError(f"Tenant {tenant_id} is not registered")

            usage = self._usage[tenant_id]

            # Update only provided values
            if cpus_used is not None:
                usage.cpus_used = cpus_used
            if gpus_used is not None:
                usage.gpus_used = gpus_used
            if memory_gb_used is not None:
                usage.memory_gb_used = memory_gb_used
            if object_store_gb_used is not None:
                usage.object_store_gb_used = object_store_gb_used
            if running_tasks is not None:
                usage.running_tasks = running_tasks
            if pending_tasks is not None:
                usage.pending_tasks = pending_tasks
            if actors is not None:
                usage.actors = actors
            if placement_groups is not None:
                usage.placement_groups = placement_groups

            usage.timestamp = time.time()

            # Record history
            self._record_history(tenant_id, usage)

    def increment_usage(
        self,
        tenant_id: str,
        cpus: float = 0,
        gpus: float = 0,
        memory_gb: float = 0,
        object_store_gb: float = 0,
        tasks: int = 0,
        actors: int = 0,
    ) -> None:
        """
        Increment the usage for a tenant.

        Args:
            tenant_id: ID of the tenant.
            cpus: CPUs to add.
            gpus: GPUs to add.
            memory_gb: Memory to add.
            object_store_gb: Object store memory to add.
            tasks: Tasks to add.
            actors: Actors to add.
        """
        with self._lock:
            if tenant_id not in self._usage:
                raise ValueError(f"Tenant {tenant_id} is not registered")

            usage = self._usage[tenant_id]
            usage.cpus_used += cpus
            usage.gpus_used += gpus
            usage.memory_gb_used += memory_gb
            usage.object_store_gb_used += object_store_gb
            usage.running_tasks += tasks
            usage.actors += actors
            usage.timestamp = time.time()

    def decrement_usage(
        self,
        tenant_id: str,
        cpus: float = 0,
        gpus: float = 0,
        memory_gb: float = 0,
        object_store_gb: float = 0,
        tasks: int = 0,
        actors: int = 0,
    ) -> None:
        """
        Decrement the usage for a tenant.

        Args:
            tenant_id: ID of the tenant.
            cpus: CPUs to remove.
            gpus: GPUs to remove.
            memory_gb: Memory to remove.
            object_store_gb: Object store memory to remove.
            tasks: Tasks to remove.
            actors: Actors to remove.
        """
        with self._lock:
            if tenant_id not in self._usage:
                raise ValueError(f"Tenant {tenant_id} is not registered")

            usage = self._usage[tenant_id]
            usage.cpus_used = max(0, usage.cpus_used - cpus)
            usage.gpus_used = max(0, usage.gpus_used - gpus)
            usage.memory_gb_used = max(0, usage.memory_gb_used - memory_gb)
            usage.object_store_gb_used = max(
                0, usage.object_store_gb_used - object_store_gb
            )
            usage.running_tasks = max(0, usage.running_tasks - tasks)
            usage.actors = max(0, usage.actors - actors)
            usage.timestamp = time.time()

    def get_usage(self, tenant_id: str) -> TenantUsage:
        """
        Get the current usage for a tenant.

        Args:
            tenant_id: ID of the tenant.

        Returns:
            Current usage for the tenant.
        """
        with self._lock:
            if tenant_id not in self._usage:
                raise ValueError(f"Tenant {tenant_id} is not registered")
            return self._usage[tenant_id]

    def get_all_usage(self) -> Dict[str, TenantUsage]:
        """
        Get the current usage for all tenants.

        Returns:
            Dictionary mapping tenant IDs to their usage.
        """
        with self._lock:
            return dict(self._usage)

    def check_quota(
        self,
        tenant_id: str,
        cpus: float = 0,
        gpus: float = 0,
        memory_gb: float = 0,
        object_store_gb: float = 0,
    ) -> bool:
        """
        Check if a tenant can allocate the requested resources.

        Args:
            tenant_id: ID of the tenant.
            cpus: CPUs requested.
            gpus: GPUs requested.
            memory_gb: Memory requested.
            object_store_gb: Object store memory requested.

        Returns:
            True if allocation is within quota, False otherwise.
        """
        with self._lock:
            if tenant_id not in self._usage:
                raise ValueError(f"Tenant {tenant_id} is not registered")

            usage = self._usage[tenant_id]

            # Check CPU quota
            if (
                usage.cpus_quota is not None
                and usage.cpus_used + cpus > usage.cpus_quota
            ):
                return False

            # Check GPU quota
            if (
                usage.gpus_quota is not None
                and usage.gpus_used + gpus > usage.gpus_quota
            ):
                return False

            # Check memory quota
            if (
                usage.memory_gb_quota is not None
                and usage.memory_gb_used + memory_gb > usage.memory_gb_quota
            ):
                return False

            # Check object store quota
            if (
                usage.object_store_gb_quota is not None
                and usage.object_store_gb_used + object_store_gb
                > usage.object_store_gb_quota
            ):
                return False

            return True

    def get_available_resources(self, tenant_id: str) -> ResourceQuota:
        """
        Get the available resources for a tenant.

        Args:
            tenant_id: ID of the tenant.

        Returns:
            ResourceQuota with available resources.
        """
        with self._lock:
            if tenant_id not in self._usage:
                raise ValueError(f"Tenant {tenant_id} is not registered")

            usage = self._usage[tenant_id]

            return ResourceQuota(
                max_cpus=(
                    int(usage.cpus_quota - usage.cpus_used)
                    if usage.cpus_quota is not None
                    else None
                ),
                max_gpus=(
                    int(usage.gpus_quota - usage.gpus_used)
                    if usage.gpus_quota is not None
                    else None
                ),
                max_memory_gb=(
                    usage.memory_gb_quota - usage.memory_gb_used
                    if usage.memory_gb_quota is not None
                    else None
                ),
                max_object_store_gb=(
                    usage.object_store_gb_quota - usage.object_store_gb_used
                    if usage.object_store_gb_quota is not None
                    else None
                ),
            )

    def get_history(
        self, tenant_id: str, limit: Optional[int] = None
    ) -> List[TenantUsage]:
        """
        Get the usage history for a tenant.

        Args:
            tenant_id: ID of the tenant.
            limit: Maximum number of history entries to return.

        Returns:
            List of historical usage records.
        """
        with self._lock:
            if tenant_id not in self._history:
                return []

            history = self._history[tenant_id]
            if limit is not None:
                return history[-limit:]
            return list(history)

    def _record_history(self, tenant_id: str, usage: TenantUsage) -> None:
        """Record a usage snapshot to history."""
        if tenant_id not in self._history:
            self._history[tenant_id] = []

        # Create a copy for history
        history_entry = TenantUsage(
            tenant_id=usage.tenant_id,
            cpus_used=usage.cpus_used,
            gpus_used=usage.gpus_used,
            memory_gb_used=usage.memory_gb_used,
            object_store_gb_used=usage.object_store_gb_used,
            running_tasks=usage.running_tasks,
            pending_tasks=usage.pending_tasks,
            actors=usage.actors,
            placement_groups=usage.placement_groups,
            cpus_quota=usage.cpus_quota,
            gpus_quota=usage.gpus_quota,
            memory_gb_quota=usage.memory_gb_quota,
            object_store_gb_quota=usage.object_store_gb_quota,
            timestamp=usage.timestamp,
        )

        self._history[tenant_id].append(history_entry)

        # Trim history if needed
        if len(self._history[tenant_id]) > self._history_max_entries:
            self._history[tenant_id] = self._history[tenant_id][
                -self._history_max_entries:
            ]
