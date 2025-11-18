"""
Fair scheduling for multi-tenant isolation in Ray.
"""

import logging
import threading
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from ray.multi_tenant.tenant import Tenant, ResourceQuota
from ray.multi_tenant.usage import TenantUsage, TenantUsageTracker

logger = logging.getLogger(__name__)


@dataclass
class ScheduleRequest:
    """
    Represents a scheduling request for a task or actor.

    Attributes:
        request_id: Unique identifier for the request.
        tenant_id: ID of the tenant making the request.
        cpus: CPUs requested.
        gpus: GPUs requested.
        memory_gb: Memory requested.
        object_store_gb: Object store memory requested.
        priority: Priority of this specific request.
        timestamp: When the request was made.
        metadata: Additional metadata.
    """
    request_id: str
    tenant_id: str
    cpus: float = 0
    gpus: float = 0
    memory_gb: float = 0
    object_store_gb: float = 0
    priority: int = 0
    timestamp: float = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ScheduleResult:
    """
    Result of a scheduling decision.

    Attributes:
        request_id: ID of the request.
        scheduled: Whether the request was scheduled.
        reason: Reason if not scheduled.
        node_id: ID of the node where scheduled (if applicable).
    """
    request_id: str
    scheduled: bool
    reason: Optional[str] = None
    node_id: Optional[str] = None


class TenantSchedulingPolicy:
    """
    Defines scheduling policies for tenants.
    """

    def __init__(
        self,
        enable_preemption: bool = False,
        enable_burst: bool = False,
        fair_share_tolerance: float = 0.1,
        priority_weight: float = 1.0,
    ):
        """
        Initialize the scheduling policy.

        Args:
            enable_preemption: Whether to enable task preemption.
            enable_burst: Whether to allow burst usage beyond quota.
            fair_share_tolerance: Tolerance for fair share deviation (0.0-1.0).
            priority_weight: Weight given to priority in scheduling decisions.
        """
        self.enable_preemption = enable_preemption
        self.enable_burst = enable_burst
        self.fair_share_tolerance = fair_share_tolerance
        self.priority_weight = priority_weight


class FairScheduler:
    """
    Fair scheduler for multi-tenant Ray clusters.

    Implements hierarchical fair share scheduling where each tenant
    receives resources proportional to their priority weight.
    """

    def __init__(
        self,
        usage_tracker: TenantUsageTracker,
        policy: Optional[TenantSchedulingPolicy] = None,
    ):
        """
        Initialize the fair scheduler.

        Args:
            usage_tracker: Tracker for tenant resource usage.
            policy: Scheduling policy to use.
        """
        self._usage_tracker = usage_tracker
        self._policy = policy or TenantSchedulingPolicy()
        self._tenants: Dict[str, Tenant] = {}
        self._pending_requests: Dict[str, List[ScheduleRequest]] = {}
        self._lock = threading.RLock()

        # Cluster resources
        self._total_cpus: float = 0
        self._total_gpus: float = 0
        self._total_memory_gb: float = 0
        self._total_object_store_gb: float = 0

    def register_tenant(self, tenant: Tenant) -> None:
        """
        Register a tenant with the scheduler.

        Args:
            tenant: The tenant to register.
        """
        with self._lock:
            self._tenants[tenant.id] = tenant
            self._pending_requests[tenant.id] = []

    def unregister_tenant(self, tenant_id: str) -> None:
        """
        Unregister a tenant from the scheduler.

        Args:
            tenant_id: ID of the tenant to unregister.
        """
        with self._lock:
            self._tenants.pop(tenant_id, None)
            self._pending_requests.pop(tenant_id, None)

    def update_cluster_resources(
        self,
        total_cpus: float,
        total_gpus: float,
        total_memory_gb: float,
        total_object_store_gb: float,
    ) -> None:
        """
        Update the total cluster resources.

        Args:
            total_cpus: Total CPUs in the cluster.
            total_gpus: Total GPUs in the cluster.
            total_memory_gb: Total memory in the cluster.
            total_object_store_gb: Total object store memory in the cluster.
        """
        with self._lock:
            self._total_cpus = total_cpus
            self._total_gpus = total_gpus
            self._total_memory_gb = total_memory_gb
            self._total_object_store_gb = total_object_store_gb

    def calculate_fair_shares(self) -> Dict[str, ResourceQuota]:
        """
        Calculate fair share of resources for each tenant.

        Returns:
            Dictionary mapping tenant IDs to their fair share ResourceQuota.
        """
        with self._lock:
            if not self._tenants:
                return {}

            # Calculate total priority weight
            total_priority = sum(
                max(1, tenant.priority + 1) for tenant in self._tenants.values()
            )

            fair_shares = {}
            for tenant_id, tenant in self._tenants.items():
                # Calculate share based on priority weight
                weight = max(1, tenant.priority + 1) / total_priority

                # Calculate fair share
                fair_cpus = self._total_cpus * weight
                fair_gpus = self._total_gpus * weight
                fair_memory = self._total_memory_gb * weight
                fair_object_store = self._total_object_store_gb * weight

                # Apply tenant quota limits
                quota = tenant.resource_quota
                if quota.max_cpus is not None:
                    fair_cpus = min(fair_cpus, quota.max_cpus)
                if quota.max_gpus is not None:
                    fair_gpus = min(fair_gpus, quota.max_gpus)
                if quota.max_memory_gb is not None:
                    fair_memory = min(fair_memory, quota.max_memory_gb)
                if quota.max_object_store_gb is not None:
                    fair_object_store = min(
                        fair_object_store, quota.max_object_store_gb
                    )

                fair_shares[tenant_id] = ResourceQuota(
                    max_cpus=int(fair_cpus),
                    max_gpus=int(fair_gpus),
                    max_memory_gb=fair_memory,
                    max_object_store_gb=fair_object_store,
                )

            return fair_shares

    def get_tenant_available_resources(
        self, tenant_id: str
    ) -> Tuple[float, float, float, float]:
        """
        Get the available resources for a tenant based on fair share.

        Args:
            tenant_id: ID of the tenant.

        Returns:
            Tuple of (available_cpus, available_gpus, available_memory,
                     available_object_store).
        """
        with self._lock:
            if tenant_id not in self._tenants:
                return (0, 0, 0, 0)

            fair_shares = self.calculate_fair_shares()
            fair_share = fair_shares.get(tenant_id)
            if fair_share is None:
                return (0, 0, 0, 0)

            try:
                usage = self._usage_tracker.get_usage(tenant_id)
            except ValueError:
                return (0, 0, 0, 0)

            available_cpus = (fair_share.max_cpus or 0) - usage.cpus_used
            available_gpus = (fair_share.max_gpus or 0) - usage.gpus_used
            available_memory = (
                (fair_share.max_memory_gb or 0) - usage.memory_gb_used
            )
            available_object_store = (
                (fair_share.max_object_store_gb or 0) - usage.object_store_gb_used
            )

            return (
                max(0, available_cpus),
                max(0, available_gpus),
                max(0, available_memory),
                max(0, available_object_store),
            )

    def schedule(
        self, requests: List[ScheduleRequest]
    ) -> List[ScheduleResult]:
        """
        Schedule a list of requests using fair share scheduling.

        Args:
            requests: List of scheduling requests.

        Returns:
            List of scheduling results.
        """
        with self._lock:
            results = []

            # Group requests by tenant
            tenant_requests: Dict[str, List[ScheduleRequest]] = {}
            for request in requests:
                if request.tenant_id not in tenant_requests:
                    tenant_requests[request.tenant_id] = []
                tenant_requests[request.tenant_id].append(request)

            # Calculate fair shares
            fair_shares = self.calculate_fair_shares()

            # Schedule within each tenant's fair share
            for tenant_id, tenant_reqs in tenant_requests.items():
                if tenant_id not in self._tenants:
                    for request in tenant_reqs:
                        results.append(ScheduleResult(
                            request_id=request.request_id,
                            scheduled=False,
                            reason="Tenant not found",
                        ))
                    continue

                tenant_results = self._schedule_tenant_requests(
                    tenant_id, tenant_reqs, fair_shares.get(tenant_id)
                )
                results.extend(tenant_results)

            return results

    def _schedule_tenant_requests(
        self,
        tenant_id: str,
        requests: List[ScheduleRequest],
        fair_share: Optional[ResourceQuota],
    ) -> List[ScheduleResult]:
        """
        Schedule requests for a specific tenant.

        Args:
            tenant_id: ID of the tenant.
            requests: List of requests from this tenant.
            fair_share: Fair share for this tenant.

        Returns:
            List of scheduling results.
        """
        results = []

        if fair_share is None:
            for request in requests:
                results.append(ScheduleResult(
                    request_id=request.request_id,
                    scheduled=False,
                    reason="No fair share allocated",
                ))
            return results

        # Sort by priority (higher first), then by timestamp (earlier first)
        sorted_requests = sorted(
            requests,
            key=lambda r: (-r.priority, r.timestamp),
        )

        # Track remaining resources
        try:
            usage = self._usage_tracker.get_usage(tenant_id)
        except ValueError:
            for request in requests:
                results.append(ScheduleResult(
                    request_id=request.request_id,
                    scheduled=False,
                    reason="Tenant usage not tracked",
                ))
            return results

        remaining_cpus = (fair_share.max_cpus or float('inf')) - usage.cpus_used
        remaining_gpus = (fair_share.max_gpus or float('inf')) - usage.gpus_used
        remaining_memory = (
            (fair_share.max_memory_gb or float('inf')) - usage.memory_gb_used
        )
        remaining_object_store = (
            (fair_share.max_object_store_gb or float('inf'))
            - usage.object_store_gb_used
        )

        for request in sorted_requests:
            # Check if request fits within remaining resources
            if (
                request.cpus <= remaining_cpus
                and request.gpus <= remaining_gpus
                and request.memory_gb <= remaining_memory
                and request.object_store_gb <= remaining_object_store
            ):
                # Schedule the request
                results.append(ScheduleResult(
                    request_id=request.request_id,
                    scheduled=True,
                ))

                # Update remaining resources
                remaining_cpus -= request.cpus
                remaining_gpus -= request.gpus
                remaining_memory -= request.memory_gb
                remaining_object_store -= request.object_store_gb

                # Update usage tracker
                self._usage_tracker.increment_usage(
                    tenant_id,
                    cpus=request.cpus,
                    gpus=request.gpus,
                    memory_gb=request.memory_gb,
                    object_store_gb=request.object_store_gb,
                    tasks=1,
                )
            else:
                # Queue the request
                reason = self._get_quota_exceeded_reason(
                    request,
                    remaining_cpus,
                    remaining_gpus,
                    remaining_memory,
                    remaining_object_store,
                )
                results.append(ScheduleResult(
                    request_id=request.request_id,
                    scheduled=False,
                    reason=reason,
                ))

                # Add to pending queue
                self._pending_requests[tenant_id].append(request)

        return results

    def _get_quota_exceeded_reason(
        self,
        request: ScheduleRequest,
        remaining_cpus: float,
        remaining_gpus: float,
        remaining_memory: float,
        remaining_object_store: float,
    ) -> str:
        """Get a reason string for why quota was exceeded."""
        reasons = []
        if request.cpus > remaining_cpus:
            reasons.append(
                f"CPU: {request.cpus} requested, {remaining_cpus:.1f} available"
            )
        if request.gpus > remaining_gpus:
            reasons.append(
                f"GPU: {request.gpus} requested, {remaining_gpus:.1f} available"
            )
        if request.memory_gb > remaining_memory:
            reasons.append(
                f"Memory: {request.memory_gb}GB requested, "
                f"{remaining_memory:.1f}GB available"
            )
        if request.object_store_gb > remaining_object_store:
            reasons.append(
                f"Object store: {request.object_store_gb}GB requested, "
                f"{remaining_object_store:.1f}GB available"
            )
        return "Quota exceeded: " + "; ".join(reasons)

    def get_pending_requests(
        self, tenant_id: Optional[str] = None
    ) -> Dict[str, List[ScheduleRequest]]:
        """
        Get pending requests.

        Args:
            tenant_id: If provided, get only requests for this tenant.

        Returns:
            Dictionary mapping tenant IDs to their pending requests.
        """
        with self._lock:
            if tenant_id is not None:
                return {tenant_id: self._pending_requests.get(tenant_id, [])}
            return dict(self._pending_requests)

    def clear_pending_request(self, tenant_id: str, request_id: str) -> bool:
        """
        Remove a request from the pending queue.

        Args:
            tenant_id: ID of the tenant.
            request_id: ID of the request to remove.

        Returns:
            True if request was found and removed, False otherwise.
        """
        with self._lock:
            if tenant_id not in self._pending_requests:
                return False

            for i, request in enumerate(self._pending_requests[tenant_id]):
                if request.request_id == request_id:
                    del self._pending_requests[tenant_id][i]
                    return True
            return False

    def get_fair_share_stats(self) -> Dict[str, Any]:
        """
        Get statistics about fair share allocation.

        Returns:
            Dictionary with fair share statistics.
        """
        with self._lock:
            fair_shares = self.calculate_fair_shares()
            stats = {
                "total_resources": {
                    "cpus": self._total_cpus,
                    "gpus": self._total_gpus,
                    "memory_gb": self._total_memory_gb,
                    "object_store_gb": self._total_object_store_gb,
                },
                "tenants": {},
            }

            for tenant_id, tenant in self._tenants.items():
                fair_share = fair_shares.get(tenant_id)
                try:
                    usage = self._usage_tracker.get_usage(tenant_id)
                except ValueError:
                    continue

                tenant_stats = {
                    "priority": tenant.priority,
                    "fair_share": {
                        "cpus": fair_share.max_cpus if fair_share else 0,
                        "gpus": fair_share.max_gpus if fair_share else 0,
                        "memory_gb": fair_share.max_memory_gb if fair_share else 0,
                        "object_store_gb": (
                            fair_share.max_object_store_gb if fair_share else 0
                        ),
                    },
                    "usage": {
                        "cpus": usage.cpus_used,
                        "gpus": usage.gpus_used,
                        "memory_gb": usage.memory_gb_used,
                        "object_store_gb": usage.object_store_gb_used,
                    },
                    "pending_requests": len(
                        self._pending_requests.get(tenant_id, [])
                    ),
                }
                stats["tenants"][tenant_id] = tenant_stats

            return stats

    def release_resources(
        self,
        tenant_id: str,
        cpus: float = 0,
        gpus: float = 0,
        memory_gb: float = 0,
        object_store_gb: float = 0,
    ) -> None:
        """
        Release resources for a tenant and try to schedule pending requests.

        Args:
            tenant_id: ID of the tenant.
            cpus: CPUs to release.
            gpus: GPUs to release.
            memory_gb: Memory to release.
            object_store_gb: Object store memory to release.
        """
        with self._lock:
            # Release resources
            self._usage_tracker.decrement_usage(
                tenant_id, cpus, gpus, memory_gb, object_store_gb, tasks=1
            )

            # Try to schedule pending requests
            self._try_schedule_pending()

    def _try_schedule_pending(self) -> None:
        """Try to schedule pending requests for all tenants."""
        for tenant_id in list(self._pending_requests.keys()):
            pending = self._pending_requests.get(tenant_id, [])
            if not pending:
                continue

            # Try to schedule pending requests
            results = self._schedule_tenant_requests(
                tenant_id,
                pending,
                self.calculate_fair_shares().get(tenant_id),
            )

            # Remove scheduled requests from pending
            scheduled_ids = {
                r.request_id for r in results if r.scheduled
            }
            self._pending_requests[tenant_id] = [
                r for r in pending if r.request_id not in scheduled_ids
            ]
