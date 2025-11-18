"""
Context management for multi-tenant operations in Ray.
"""

import threading
from contextlib import contextmanager
from typing import Any, Dict, Optional

import ray
from ray.multi_tenant.tenant import Tenant
from ray.multi_tenant.admin import TenantAdmin
from ray.multi_tenant.exceptions import (
    TenantNotFoundError,
    TenantIsolationError,
)


# Thread-local storage for tenant context
_tenant_context = threading.local()


def get_current_tenant_id() -> Optional[str]:
    """
    Get the current tenant ID for this thread.

    Returns:
        The current tenant ID, or None if not in a tenant context.
    """
    return getattr(_tenant_context, 'tenant_id', None)


def get_current_tenant() -> Optional[Tenant]:
    """
    Get the current tenant for this thread.

    Returns:
        The current Tenant object, or None if not in a tenant context.
    """
    tenant_id = get_current_tenant_id()
    if tenant_id is None:
        return None

    try:
        admin = TenantAdmin()
        return admin.get_tenant(tenant_id)
    except TenantNotFoundError:
        return None


def set_current_tenant(tenant_id: str) -> None:
    """
    Set the current tenant for this thread.

    Args:
        tenant_id: ID of the tenant to set as current.
    """
    _tenant_context.tenant_id = tenant_id


def clear_current_tenant() -> None:
    """
    Clear the current tenant for this thread.
    """
    _tenant_context.tenant_id = None


@contextmanager
def tenant_context(tenant_id: str):
    """
    Context manager for running code within a tenant context.

    Args:
        tenant_id: ID of the tenant.

    Yields:
        The Tenant object.

    Example:
        >>> with tenant_context("team-alpha") as tenant:
        ...     # All tasks/actors created here are associated with team-alpha
        ...     task.remote()
    """
    previous_tenant_id = get_current_tenant_id()
    try:
        set_current_tenant(tenant_id)
        admin = TenantAdmin()
        tenant = admin.get_tenant(tenant_id)
        yield tenant
    finally:
        if previous_tenant_id is not None:
            set_current_tenant(previous_tenant_id)
        else:
            clear_current_tenant()


class MultiTenantContext:
    """
    Context manager for multi-tenant Ray initialization.

    This class provides a convenient way to initialize Ray with
    multi-tenant support and associate operations with a specific tenant.
    """

    def __init__(
        self,
        tenant_id: str,
        multi_tenant: bool = True,
        **ray_init_kwargs: Any,
    ):
        """
        Initialize the multi-tenant context.

        Args:
            tenant_id: ID of the tenant.
            multi_tenant: Enable multi-tenant mode.
            **ray_init_kwargs: Additional arguments to pass to ray.init().
        """
        self.tenant_id = tenant_id
        self.multi_tenant = multi_tenant
        self.ray_init_kwargs = ray_init_kwargs
        self._previous_tenant_id = None

    def __enter__(self) -> "MultiTenantContext":
        """Enter the multi-tenant context."""
        # Initialize Ray if not already initialized
        if not ray.is_initialized():
            # Set namespace to tenant's namespace
            admin = TenantAdmin()
            try:
                tenant = admin.get_tenant(self.tenant_id)
                namespace = tenant.namespace
            except TenantNotFoundError:
                namespace = f"tenant-{self.tenant_id}"

            ray.init(namespace=namespace, **self.ray_init_kwargs)

        # Set current tenant
        self._previous_tenant_id = get_current_tenant_id()
        set_current_tenant(self.tenant_id)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the multi-tenant context."""
        if self._previous_tenant_id is not None:
            set_current_tenant(self._previous_tenant_id)
        else:
            clear_current_tenant()
        return False


def ensure_tenant_namespace(tenant_id: str) -> None:
    """
    Ensure the current Ray namespace matches the tenant's namespace.

    Args:
        tenant_id: ID of the tenant.

    Raises:
        TenantIsolationError: If the current namespace doesn't match.
    """
    if not ray.is_initialized():
        return

    admin = TenantAdmin()
    try:
        tenant = admin.get_tenant(tenant_id)
    except TenantNotFoundError:
        return

    current_namespace = ray.get_runtime_context().namespace
    if current_namespace != tenant.namespace:
        raise TenantIsolationError(
            f"Namespace mismatch: current={current_namespace}, "
            f"tenant={tenant.namespace}"
        )


def get_tenant_runtime_context() -> Dict[str, Any]:
    """
    Get the runtime context including tenant information.

    Returns:
        Dictionary with runtime context and tenant info.
    """
    context = {}

    if ray.is_initialized():
        rt_context = ray.get_runtime_context()
        context.update({
            "job_id": rt_context.get_job_id(),
            "node_id": rt_context.get_node_id(),
            "namespace": rt_context.namespace,
        })

        # Add worker/actor info if available
        try:
            context["worker_id"] = rt_context.get_worker_id()
        except Exception:
            pass
        try:
            context["actor_id"] = rt_context.get_actor_id()
        except Exception:
            pass
        try:
            context["task_id"] = rt_context.get_task_id()
        except Exception:
            pass

    # Add tenant info
    tenant_id = get_current_tenant_id()
    if tenant_id:
        context["tenant_id"] = tenant_id
        tenant = get_current_tenant()
        if tenant:
            context["tenant_name"] = tenant.name
            context["tenant_priority"] = tenant.priority

    return context
