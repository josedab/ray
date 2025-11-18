"""
Core data structures for multi-tenant isolation in Ray.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional


class IsolationLevel(Enum):
    """
    Defines the level of isolation between tenants.

    Attributes:
        NONE: Current behavior - no isolation between tenants.
        NAMESPACE: Separate namespaces for each tenant.
        RESOURCE: Resource quotas are enforced per tenant.
        PROCESS: Process-level isolation between tenants.
        CONTAINER: Container-level isolation for maximum security.
    """
    NONE = 0          # Current behavior
    NAMESPACE = 1     # Separate namespaces
    RESOURCE = 2      # Resource quotas enforced
    PROCESS = 3       # Process-level isolation
    CONTAINER = 4     # Container-level isolation


@dataclass
class ResourceQuota:
    """
    Defines resource limits for a tenant.

    Attributes:
        max_cpus: Maximum number of CPUs the tenant can use.
        max_gpus: Maximum number of GPUs the tenant can use.
        max_memory_gb: Maximum memory in GB the tenant can use.
        max_object_store_gb: Maximum object store memory in GB.
        max_running_tasks: Maximum number of concurrent running tasks.
        max_actors: Maximum number of actors the tenant can create.
        max_pending_tasks: Maximum number of pending tasks in queue.
        max_placement_groups: Maximum number of placement groups.
    """
    max_cpus: Optional[int] = None
    max_gpus: Optional[int] = None
    max_memory_gb: Optional[float] = None
    max_object_store_gb: Optional[float] = None
    max_running_tasks: Optional[int] = None
    max_actors: Optional[int] = None
    max_pending_tasks: Optional[int] = None
    max_placement_groups: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "max_cpus": self.max_cpus,
            "max_gpus": self.max_gpus,
            "max_memory_gb": self.max_memory_gb,
            "max_object_store_gb": self.max_object_store_gb,
            "max_running_tasks": self.max_running_tasks,
            "max_actors": self.max_actors,
            "max_pending_tasks": self.max_pending_tasks,
            "max_placement_groups": self.max_placement_groups,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ResourceQuota":
        """Create from dictionary representation."""
        return cls(
            max_cpus=data.get("max_cpus"),
            max_gpus=data.get("max_gpus"),
            max_memory_gb=data.get("max_memory_gb"),
            max_object_store_gb=data.get("max_object_store_gb"),
            max_running_tasks=data.get("max_running_tasks"),
            max_actors=data.get("max_actors"),
            max_pending_tasks=data.get("max_pending_tasks"),
            max_placement_groups=data.get("max_placement_groups"),
        )

    def is_unlimited(self) -> bool:
        """Check if all quotas are unlimited (None)."""
        return all(
            getattr(self, attr) is None
            for attr in [
                "max_cpus", "max_gpus", "max_memory_gb",
                "max_object_store_gb", "max_running_tasks",
                "max_actors", "max_pending_tasks", "max_placement_groups"
            ]
        )

    def merge(self, other: "ResourceQuota") -> "ResourceQuota":
        """
        Merge with another quota, taking the more restrictive limits.

        Args:
            other: Another ResourceQuota to merge with.

        Returns:
            A new ResourceQuota with the more restrictive limits.
        """
        def min_or_none(a, b):
            if a is None:
                return b
            if b is None:
                return a
            return min(a, b)

        return ResourceQuota(
            max_cpus=min_or_none(self.max_cpus, other.max_cpus),
            max_gpus=min_or_none(self.max_gpus, other.max_gpus),
            max_memory_gb=min_or_none(self.max_memory_gb, other.max_memory_gb),
            max_object_store_gb=min_or_none(
                self.max_object_store_gb, other.max_object_store_gb
            ),
            max_running_tasks=min_or_none(
                self.max_running_tasks, other.max_running_tasks
            ),
            max_actors=min_or_none(self.max_actors, other.max_actors),
            max_pending_tasks=min_or_none(
                self.max_pending_tasks, other.max_pending_tasks
            ),
            max_placement_groups=min_or_none(
                self.max_placement_groups, other.max_placement_groups
            ),
        )


@dataclass
class TenantConfig:
    """
    Configuration options for a tenant.

    Attributes:
        allow_burst: Allow temporary quota exceeds when resources are available.
        burst_duration_seconds: Maximum duration for burst usage.
        preemptible: Whether tasks from this tenant can be preempted.
        preemption_priority: Priority for preemption (higher = less likely to be preempted).
    """
    allow_burst: bool = False
    burst_duration_seconds: int = 300
    preemptible: bool = True
    preemption_priority: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "allow_burst": self.allow_burst,
            "burst_duration_seconds": self.burst_duration_seconds,
            "preemptible": self.preemptible,
            "preemption_priority": self.preemption_priority,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TenantConfig":
        """Create from dictionary representation."""
        return cls(
            allow_burst=data.get("allow_burst", False),
            burst_duration_seconds=data.get("burst_duration_seconds", 300),
            preemptible=data.get("preemptible", True),
            preemption_priority=data.get("preemption_priority", 0),
        )


@dataclass
class Tenant:
    """
    Represents a tenant in a multi-tenant Ray cluster.

    Attributes:
        id: Unique identifier for the tenant.
        name: Human-readable name for the tenant.
        resource_quota: Resource limits for this tenant.
        priority: Scheduling priority (higher = more priority).
        isolation_level: Level of isolation for this tenant.
        namespace: Namespace associated with this tenant.
        config: Additional configuration options.
        metadata: Arbitrary metadata associated with the tenant.
    """
    id: str
    name: str
    resource_quota: ResourceQuota = field(default_factory=ResourceQuota)
    priority: int = 0
    isolation_level: IsolationLevel = IsolationLevel.NAMESPACE
    namespace: Optional[str] = None
    config: TenantConfig = field(default_factory=TenantConfig)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Post-initialization processing."""
        # Default namespace to tenant id if not specified
        if self.namespace is None:
            self.namespace = f"tenant-{self.id}"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "id": self.id,
            "name": self.name,
            "resource_quota": self.resource_quota.to_dict(),
            "priority": self.priority,
            "isolation_level": self.isolation_level.value,
            "namespace": self.namespace,
            "config": self.config.to_dict(),
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Tenant":
        """Create from dictionary representation."""
        return cls(
            id=data["id"],
            name=data["name"],
            resource_quota=ResourceQuota.from_dict(data.get("resource_quota", {})),
            priority=data.get("priority", 0),
            isolation_level=IsolationLevel(data.get("isolation_level", 1)),
            namespace=data.get("namespace"),
            config=TenantConfig.from_dict(data.get("config", {})),
            metadata=data.get("metadata", {}),
        )

    def can_use_resource(
        self,
        resource_name: str,
        requested: float,
        current_usage: float
    ) -> bool:
        """
        Check if the tenant can use the requested amount of a resource.

        Args:
            resource_name: Name of the resource (e.g., "cpu", "gpu", "memory").
            requested: Amount of resource being requested.
            current_usage: Current usage of the resource.

        Returns:
            True if the resource usage is within quota, False otherwise.
        """
        quota_map = {
            "cpu": self.resource_quota.max_cpus,
            "cpus": self.resource_quota.max_cpus,
            "gpu": self.resource_quota.max_gpus,
            "gpus": self.resource_quota.max_gpus,
            "memory": self.resource_quota.max_memory_gb,
            "object_store": self.resource_quota.max_object_store_gb,
        }

        max_allowed = quota_map.get(resource_name.lower())
        if max_allowed is None:
            return True  # No limit set

        return (current_usage + requested) <= max_allowed
