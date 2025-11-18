"""
Multi-tenant isolation support for Ray.

This module provides multi-tenant isolation capabilities enabling multiple
users/teams to share a cluster with resource isolation, security boundaries,
and fair scheduling.
"""

from ray.multi_tenant.tenant import (
    Tenant,
    ResourceQuota,
    IsolationLevel,
    TenantConfig,
)
from ray.multi_tenant.admin import TenantAdmin
from ray.multi_tenant.scheduler import (
    FairScheduler,
    TenantSchedulingPolicy,
    ScheduleRequest,
    ScheduleResult,
)
from ray.multi_tenant.usage import TenantUsage, TenantUsageTracker
from ray.multi_tenant.exceptions import (
    MultiTenantError,
    TenantNotFoundError,
    TenantAlreadyExistsError,
    QuotaExceededError,
    TenantIsolationError,
    TenantAuthenticationError,
    InvalidTenantConfigError,
)
from ray.multi_tenant.context import (
    get_current_tenant_id,
    get_current_tenant,
    set_current_tenant,
    clear_current_tenant,
    tenant_context,
    MultiTenantContext,
    get_tenant_runtime_context,
)

__all__ = [
    # Core data structures
    "Tenant",
    "ResourceQuota",
    "IsolationLevel",
    "TenantConfig",
    # Admin API
    "TenantAdmin",
    # Scheduler
    "FairScheduler",
    "TenantSchedulingPolicy",
    "ScheduleRequest",
    "ScheduleResult",
    # Usage tracking
    "TenantUsage",
    "TenantUsageTracker",
    # Exceptions
    "MultiTenantError",
    "TenantNotFoundError",
    "TenantAlreadyExistsError",
    "QuotaExceededError",
    "TenantIsolationError",
    "TenantAuthenticationError",
    "InvalidTenantConfigError",
    # Context
    "get_current_tenant_id",
    "get_current_tenant",
    "set_current_tenant",
    "clear_current_tenant",
    "tenant_context",
    "MultiTenantContext",
    "get_tenant_runtime_context",
]
