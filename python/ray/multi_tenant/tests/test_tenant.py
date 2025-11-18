"""Tests for tenant data structures."""

import pytest

from ray.multi_tenant.tenant import (
    Tenant,
    ResourceQuota,
    IsolationLevel,
    TenantConfig,
)


class TestResourceQuota:
    """Tests for ResourceQuota class."""

    def test_create_default_quota(self):
        """Test creating a quota with default values."""
        quota = ResourceQuota()
        assert quota.max_cpus is None
        assert quota.max_gpus is None
        assert quota.max_memory_gb is None
        assert quota.max_object_store_gb is None
        assert quota.max_running_tasks is None
        assert quota.max_actors is None
        assert quota.is_unlimited()

    def test_create_quota_with_limits(self):
        """Test creating a quota with specific limits."""
        quota = ResourceQuota(
            max_cpus=100,
            max_gpus=10,
            max_memory_gb=256.0,
            max_object_store_gb=128.0,
            max_running_tasks=1000,
            max_actors=500,
        )
        assert quota.max_cpus == 100
        assert quota.max_gpus == 10
        assert quota.max_memory_gb == 256.0
        assert quota.max_object_store_gb == 128.0
        assert quota.max_running_tasks == 1000
        assert quota.max_actors == 500
        assert not quota.is_unlimited()

    def test_quota_to_dict(self):
        """Test converting quota to dictionary."""
        quota = ResourceQuota(max_cpus=100, max_gpus=10)
        data = quota.to_dict()
        assert data["max_cpus"] == 100
        assert data["max_gpus"] == 10
        assert data["max_memory_gb"] is None

    def test_quota_from_dict(self):
        """Test creating quota from dictionary."""
        data = {"max_cpus": 100, "max_gpus": 10, "max_memory_gb": 256.0}
        quota = ResourceQuota.from_dict(data)
        assert quota.max_cpus == 100
        assert quota.max_gpus == 10
        assert quota.max_memory_gb == 256.0
        assert quota.max_object_store_gb is None

    def test_quota_merge(self):
        """Test merging two quotas."""
        quota1 = ResourceQuota(max_cpus=100, max_gpus=10)
        quota2 = ResourceQuota(max_cpus=50, max_memory_gb=256.0)
        merged = quota1.merge(quota2)
        assert merged.max_cpus == 50  # More restrictive
        assert merged.max_gpus == 10  # From quota1
        assert merged.max_memory_gb == 256.0  # From quota2


class TestTenantConfig:
    """Tests for TenantConfig class."""

    def test_create_default_config(self):
        """Test creating config with default values."""
        config = TenantConfig()
        assert config.allow_burst is False
        assert config.burst_duration_seconds == 300
        assert config.preemptible is True
        assert config.preemption_priority == 0

    def test_create_custom_config(self):
        """Test creating config with custom values."""
        config = TenantConfig(
            allow_burst=True,
            burst_duration_seconds=600,
            preemptible=False,
            preemption_priority=5,
        )
        assert config.allow_burst is True
        assert config.burst_duration_seconds == 600
        assert config.preemptible is False
        assert config.preemption_priority == 5

    def test_config_to_dict(self):
        """Test converting config to dictionary."""
        config = TenantConfig(allow_burst=True)
        data = config.to_dict()
        assert data["allow_burst"] is True
        assert "burst_duration_seconds" in data

    def test_config_from_dict(self):
        """Test creating config from dictionary."""
        data = {"allow_burst": True, "preemption_priority": 10}
        config = TenantConfig.from_dict(data)
        assert config.allow_burst is True
        assert config.preemption_priority == 10


class TestIsolationLevel:
    """Tests for IsolationLevel enum."""

    def test_isolation_levels(self):
        """Test isolation level values."""
        assert IsolationLevel.NONE.value == 0
        assert IsolationLevel.NAMESPACE.value == 1
        assert IsolationLevel.RESOURCE.value == 2
        assert IsolationLevel.PROCESS.value == 3
        assert IsolationLevel.CONTAINER.value == 4


class TestTenant:
    """Tests for Tenant class."""

    def test_create_basic_tenant(self):
        """Test creating a basic tenant."""
        tenant = Tenant(id="team-alpha", name="Team Alpha")
        assert tenant.id == "team-alpha"
        assert tenant.name == "Team Alpha"
        assert tenant.priority == 0
        assert tenant.isolation_level == IsolationLevel.NAMESPACE
        assert tenant.namespace == "tenant-team-alpha"

    def test_create_tenant_with_custom_namespace(self):
        """Test creating tenant with custom namespace."""
        tenant = Tenant(
            id="team-alpha",
            name="Team Alpha",
            namespace="alpha-ns",
        )
        assert tenant.namespace == "alpha-ns"

    def test_create_tenant_with_quota(self):
        """Test creating tenant with resource quota."""
        quota = ResourceQuota(max_cpus=100, max_gpus=10)
        tenant = Tenant(
            id="team-alpha",
            name="Team Alpha",
            resource_quota=quota,
        )
        assert tenant.resource_quota.max_cpus == 100
        assert tenant.resource_quota.max_gpus == 10

    def test_create_tenant_with_all_options(self):
        """Test creating tenant with all options."""
        quota = ResourceQuota(max_cpus=100)
        config = TenantConfig(allow_burst=True)
        tenant = Tenant(
            id="team-alpha",
            name="Team Alpha",
            resource_quota=quota,
            priority=5,
            isolation_level=IsolationLevel.RESOURCE,
            namespace="alpha-ns",
            config=config,
            metadata={"department": "engineering"},
        )
        assert tenant.id == "team-alpha"
        assert tenant.priority == 5
        assert tenant.isolation_level == IsolationLevel.RESOURCE
        assert tenant.config.allow_burst is True
        assert tenant.metadata["department"] == "engineering"

    def test_tenant_to_dict(self):
        """Test converting tenant to dictionary."""
        tenant = Tenant(
            id="team-alpha",
            name="Team Alpha",
            priority=5,
        )
        data = tenant.to_dict()
        assert data["id"] == "team-alpha"
        assert data["name"] == "Team Alpha"
        assert data["priority"] == 5
        assert "resource_quota" in data
        assert "config" in data

    def test_tenant_from_dict(self):
        """Test creating tenant from dictionary."""
        data = {
            "id": "team-alpha",
            "name": "Team Alpha",
            "priority": 5,
            "resource_quota": {"max_cpus": 100},
            "isolation_level": 2,
        }
        tenant = Tenant.from_dict(data)
        assert tenant.id == "team-alpha"
        assert tenant.name == "Team Alpha"
        assert tenant.priority == 5
        assert tenant.resource_quota.max_cpus == 100
        assert tenant.isolation_level == IsolationLevel.RESOURCE

    def test_can_use_resource_within_quota(self):
        """Test checking resource usage within quota."""
        tenant = Tenant(
            id="team-alpha",
            name="Team Alpha",
            resource_quota=ResourceQuota(max_cpus=100),
        )
        assert tenant.can_use_resource("cpu", 10, 50)  # 50 + 10 = 60 < 100
        assert not tenant.can_use_resource("cpu", 60, 50)  # 50 + 60 = 110 > 100

    def test_can_use_resource_unlimited(self):
        """Test checking resource usage with unlimited quota."""
        tenant = Tenant(id="team-alpha", name="Team Alpha")
        # All should return True when no limit is set
        assert tenant.can_use_resource("cpu", 1000, 1000)
        assert tenant.can_use_resource("memory", 1000, 1000)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
