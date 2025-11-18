"""
Administration API for multi-tenant isolation in Ray.
"""

import json
import logging
import threading
from typing import Any, Dict, List, Optional

import ray
from ray.multi_tenant.tenant import (
    Tenant,
    ResourceQuota,
    IsolationLevel,
    TenantConfig,
)
from ray.multi_tenant.usage import TenantUsage, TenantUsageTracker

logger = logging.getLogger(__name__)


class TenantAdminError(Exception):
    """Base exception for tenant administration errors."""
    pass


class TenantNotFoundError(TenantAdminError):
    """Raised when a tenant is not found."""
    pass


class TenantAlreadyExistsError(TenantAdminError):
    """Raised when attempting to create a tenant that already exists."""
    pass


class QuotaExceededError(TenantAdminError):
    """Raised when a tenant's quota would be exceeded."""
    pass


class TenantAdmin:
    """
    Administration interface for managing tenants in a multi-tenant Ray cluster.

    This class provides methods to create, update, delete, and query tenants,
    as well as view their resource usage.
    """

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        """Ensure singleton pattern for TenantAdmin."""
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self):
        """Initialize the tenant admin."""
        if self._initialized:
            return

        self._tenants: Dict[str, Tenant] = {}
        self._usage_tracker = TenantUsageTracker()
        self._tenant_lock = threading.RLock()
        self._gcs_client = None
        self._initialized = True

    def _ensure_connected(self):
        """Ensure Ray is initialized and get GCS client."""
        if not ray.is_initialized():
            raise TenantAdminError(
                "Ray must be initialized before using TenantAdmin"
            )
        if self._gcs_client is None:
            try:
                self._gcs_client = ray._private.worker.global_worker.gcs_client
            except Exception:
                # GCS client may not be available in all contexts
                pass

    def create_tenant(
        self,
        id: str,
        name: Optional[str] = None,
        quota: Optional[ResourceQuota] = None,
        priority: int = 0,
        isolation_level: IsolationLevel = IsolationLevel.NAMESPACE,
        namespace: Optional[str] = None,
        config: Optional[TenantConfig] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Tenant:
        """
        Create a new tenant.

        Args:
            id: Unique identifier for the tenant.
            name: Human-readable name (defaults to id).
            quota: Resource quota for the tenant.
            priority: Scheduling priority.
            isolation_level: Level of isolation.
            namespace: Namespace for the tenant.
            config: Additional configuration.
            metadata: Arbitrary metadata.

        Returns:
            The created Tenant object.

        Raises:
            TenantAlreadyExistsError: If a tenant with the same ID exists.
        """
        with self._tenant_lock:
            if id in self._tenants:
                raise TenantAlreadyExistsError(f"Tenant {id} already exists")

            tenant = Tenant(
                id=id,
                name=name or id,
                resource_quota=quota or ResourceQuota(),
                priority=priority,
                isolation_level=isolation_level,
                namespace=namespace,
                config=config or TenantConfig(),
                metadata=metadata or {},
            )

            self._tenants[id] = tenant
            self._usage_tracker.register_tenant(tenant)

            logger.info(f"Created tenant: {id}")

            # Store in GCS if available
            self._store_tenant_in_gcs(tenant)

            return tenant

    def update_tenant(
        self,
        id: str,
        name: Optional[str] = None,
        quota: Optional[ResourceQuota] = None,
        priority: Optional[int] = None,
        isolation_level: Optional[IsolationLevel] = None,
        config: Optional[TenantConfig] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Tenant:
        """
        Update an existing tenant.

        Args:
            id: ID of the tenant to update.
            name: New name (if provided).
            quota: New quota (if provided).
            priority: New priority (if provided).
            isolation_level: New isolation level (if provided).
            config: New configuration (if provided).
            metadata: New metadata (if provided).

        Returns:
            The updated Tenant object.

        Raises:
            TenantNotFoundError: If the tenant does not exist.
        """
        with self._tenant_lock:
            if id not in self._tenants:
                raise TenantNotFoundError(f"Tenant {id} not found")

            tenant = self._tenants[id]

            if name is not None:
                tenant.name = name
            if quota is not None:
                tenant.resource_quota = quota
                # Update quota in usage tracker
                usage = self._usage_tracker.get_usage(id)
                usage.cpus_quota = quota.max_cpus
                usage.gpus_quota = quota.max_gpus
                usage.memory_gb_quota = quota.max_memory_gb
                usage.object_store_gb_quota = quota.max_object_store_gb
            if priority is not None:
                tenant.priority = priority
            if isolation_level is not None:
                tenant.isolation_level = isolation_level
            if config is not None:
                tenant.config = config
            if metadata is not None:
                tenant.metadata = metadata

            logger.info(f"Updated tenant: {id}")

            # Update in GCS if available
            self._store_tenant_in_gcs(tenant)

            return tenant

    def delete_tenant(self, id: str) -> None:
        """
        Delete a tenant.

        Args:
            id: ID of the tenant to delete.

        Raises:
            TenantNotFoundError: If the tenant does not exist.
        """
        with self._tenant_lock:
            if id not in self._tenants:
                raise TenantNotFoundError(f"Tenant {id} not found")

            # Check for active resources
            usage = self._usage_tracker.get_usage(id)
            if usage.running_tasks > 0 or usage.actors > 0:
                logger.warning(
                    f"Deleting tenant {id} with active resources: "
                    f"{usage.running_tasks} tasks, {usage.actors} actors"
                )

            del self._tenants[id]
            self._usage_tracker.unregister_tenant(id)

            logger.info(f"Deleted tenant: {id}")

            # Remove from GCS if available
            self._delete_tenant_from_gcs(id)

    def get_tenant(self, id: str) -> Tenant:
        """
        Get a tenant by ID.

        Args:
            id: ID of the tenant.

        Returns:
            The Tenant object.

        Raises:
            TenantNotFoundError: If the tenant does not exist.
        """
        with self._tenant_lock:
            if id not in self._tenants:
                raise TenantNotFoundError(f"Tenant {id} not found")
            return self._tenants[id]

    def list_tenants(self) -> List[Tenant]:
        """
        List all tenants.

        Returns:
            List of all Tenant objects.
        """
        with self._tenant_lock:
            return list(self._tenants.values())

    def get_tenant_usage(self, id: str) -> TenantUsage:
        """
        Get the current resource usage for a tenant.

        Args:
            id: ID of the tenant.

        Returns:
            TenantUsage object with current usage.

        Raises:
            TenantNotFoundError: If the tenant does not exist.
        """
        with self._tenant_lock:
            if id not in self._tenants:
                raise TenantNotFoundError(f"Tenant {id} not found")
            return self._usage_tracker.get_usage(id)

    def get_all_tenant_usage(self) -> Dict[str, TenantUsage]:
        """
        Get the current resource usage for all tenants.

        Returns:
            Dictionary mapping tenant IDs to TenantUsage objects.
        """
        return self._usage_tracker.get_all_usage()

    def check_quota(
        self,
        tenant_id: str,
        cpus: float = 0,
        gpus: float = 0,
        memory_gb: float = 0,
        object_store_gb: float = 0,
    ) -> bool:
        """
        Check if a resource allocation is within tenant quota.

        Args:
            tenant_id: ID of the tenant.
            cpus: CPUs requested.
            gpus: GPUs requested.
            memory_gb: Memory requested.
            object_store_gb: Object store memory requested.

        Returns:
            True if allocation is within quota, False otherwise.

        Raises:
            TenantNotFoundError: If the tenant does not exist.
        """
        with self._tenant_lock:
            if tenant_id not in self._tenants:
                raise TenantNotFoundError(f"Tenant {tenant_id} not found")
            return self._usage_tracker.check_quota(
                tenant_id, cpus, gpus, memory_gb, object_store_gb
            )

    def allocate_resources(
        self,
        tenant_id: str,
        cpus: float = 0,
        gpus: float = 0,
        memory_gb: float = 0,
        object_store_gb: float = 0,
        tasks: int = 0,
        actors: int = 0,
    ) -> bool:
        """
        Allocate resources for a tenant.

        Args:
            tenant_id: ID of the tenant.
            cpus: CPUs to allocate.
            gpus: GPUs to allocate.
            memory_gb: Memory to allocate.
            object_store_gb: Object store memory to allocate.
            tasks: Tasks to allocate.
            actors: Actors to allocate.

        Returns:
            True if allocation succeeded, False if quota exceeded.

        Raises:
            TenantNotFoundError: If the tenant does not exist.
        """
        with self._tenant_lock:
            if tenant_id not in self._tenants:
                raise TenantNotFoundError(f"Tenant {tenant_id} not found")

            # Check quota first
            if not self._usage_tracker.check_quota(
                tenant_id, cpus, gpus, memory_gb, object_store_gb
            ):
                return False

            # Allocate
            self._usage_tracker.increment_usage(
                tenant_id, cpus, gpus, memory_gb, object_store_gb, tasks, actors
            )
            return True

    def release_resources(
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
        Release resources for a tenant.

        Args:
            tenant_id: ID of the tenant.
            cpus: CPUs to release.
            gpus: GPUs to release.
            memory_gb: Memory to release.
            object_store_gb: Object store memory to release.
            tasks: Tasks to release.
            actors: Actors to release.

        Raises:
            TenantNotFoundError: If the tenant does not exist.
        """
        with self._tenant_lock:
            if tenant_id not in self._tenants:
                raise TenantNotFoundError(f"Tenant {tenant_id} not found")

            self._usage_tracker.decrement_usage(
                tenant_id, cpus, gpus, memory_gb, object_store_gb, tasks, actors
            )

    def get_usage_history(
        self, tenant_id: str, limit: Optional[int] = None
    ) -> List[TenantUsage]:
        """
        Get usage history for a tenant.

        Args:
            tenant_id: ID of the tenant.
            limit: Maximum number of entries to return.

        Returns:
            List of historical TenantUsage objects.

        Raises:
            TenantNotFoundError: If the tenant does not exist.
        """
        with self._tenant_lock:
            if tenant_id not in self._tenants:
                raise TenantNotFoundError(f"Tenant {tenant_id} not found")
            return self._usage_tracker.get_history(tenant_id, limit)

    def get_tenant_by_namespace(self, namespace: str) -> Optional[Tenant]:
        """
        Get a tenant by its namespace.

        Args:
            namespace: Namespace to look up.

        Returns:
            The Tenant object if found, None otherwise.
        """
        with self._tenant_lock:
            for tenant in self._tenants.values():
                if tenant.namespace == namespace:
                    return tenant
            return None

    def _store_tenant_in_gcs(self, tenant: Tenant) -> None:
        """Store tenant data in GCS."""
        try:
            self._ensure_connected()
            if self._gcs_client is not None:
                key = f"TENANT:{tenant.id}".encode()
                value = json.dumps(tenant.to_dict()).encode()
                self._gcs_client.internal_kv_put(
                    key, value, True, b"multi_tenant"
                )
        except Exception as e:
            logger.debug(f"Failed to store tenant in GCS: {e}")

    def _delete_tenant_from_gcs(self, tenant_id: str) -> None:
        """Delete tenant data from GCS."""
        try:
            self._ensure_connected()
            if self._gcs_client is not None:
                key = f"TENANT:{tenant_id}".encode()
                self._gcs_client.internal_kv_del(key, False, b"multi_tenant")
        except Exception as e:
            logger.debug(f"Failed to delete tenant from GCS: {e}")

    def _load_tenants_from_gcs(self) -> None:
        """Load all tenants from GCS."""
        try:
            self._ensure_connected()
            if self._gcs_client is not None:
                keys = self._gcs_client.internal_kv_keys(
                    b"TENANT:", b"multi_tenant"
                )
                for key in keys:
                    value = self._gcs_client.internal_kv_get(
                        key, b"multi_tenant"
                    )
                    if value:
                        tenant_data = json.loads(value.decode())
                        tenant = Tenant.from_dict(tenant_data)
                        with self._tenant_lock:
                            self._tenants[tenant.id] = tenant
                            self._usage_tracker.register_tenant(tenant)
        except Exception as e:
            logger.debug(f"Failed to load tenants from GCS: {e}")

    @classmethod
    def reset(cls):
        """Reset the singleton instance (mainly for testing)."""
        with cls._lock:
            cls._instance = None
