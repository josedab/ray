"""Tests for tenant administration API."""

import pytest

from ray.multi_tenant.admin import (
    TenantAdmin,
    TenantNotFoundError,
    TenantAlreadyExistsError,
)
from ray.multi_tenant.tenant import (
    Tenant,
    ResourceQuota,
    IsolationLevel,
    TenantConfig,
)


class TestTenantAdmin:
    """Tests for TenantAdmin class."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Reset TenantAdmin singleton before each test."""
        TenantAdmin.reset()
        yield
        TenantAdmin.reset()

    def test_create_tenant(self):
        """Test creating a tenant."""
        admin = TenantAdmin()
        tenant = admin.create_tenant(
            id="team-alpha",
            name="Team Alpha",
            quota=ResourceQuota(max_cpus=100),
            priority=5,
        )
        assert tenant.id == "team-alpha"
        assert tenant.name == "Team Alpha"
        assert tenant.resource_quota.max_cpus == 100
        assert tenant.priority == 5

    def test_create_tenant_default_name(self):
        """Test creating tenant with default name."""
        admin = TenantAdmin()
        tenant = admin.create_tenant(id="team-alpha")
        assert tenant.name == "team-alpha"

    def test_create_duplicate_tenant(self):
        """Test creating duplicate tenant raises error."""
        admin = TenantAdmin()
        admin.create_tenant(id="team-alpha")
        with pytest.raises(TenantAlreadyExistsError):
            admin.create_tenant(id="team-alpha")

    def test_get_tenant(self):
        """Test getting a tenant by ID."""
        admin = TenantAdmin()
        admin.create_tenant(id="team-alpha", name="Team Alpha")
        tenant = admin.get_tenant("team-alpha")
        assert tenant.id == "team-alpha"
        assert tenant.name == "Team Alpha"

    def test_get_nonexistent_tenant(self):
        """Test getting nonexistent tenant raises error."""
        admin = TenantAdmin()
        with pytest.raises(TenantNotFoundError):
            admin.get_tenant("nonexistent")

    def test_list_tenants(self):
        """Test listing all tenants."""
        admin = TenantAdmin()
        admin.create_tenant(id="team-alpha")
        admin.create_tenant(id="team-beta")
        admin.create_tenant(id="team-gamma")
        tenants = admin.list_tenants()
        assert len(tenants) == 3
        tenant_ids = {t.id for t in tenants}
        assert tenant_ids == {"team-alpha", "team-beta", "team-gamma"}

    def test_update_tenant(self):
        """Test updating a tenant."""
        admin = TenantAdmin()
        admin.create_tenant(id="team-alpha", name="Team Alpha", priority=0)
        updated = admin.update_tenant(
            id="team-alpha",
            name="Alpha Team",
            priority=10,
        )
        assert updated.name == "Alpha Team"
        assert updated.priority == 10

    def test_update_tenant_quota(self):
        """Test updating tenant quota."""
        admin = TenantAdmin()
        admin.create_tenant(
            id="team-alpha",
            quota=ResourceQuota(max_cpus=100),
        )
        new_quota = ResourceQuota(max_cpus=200, max_gpus=20)
        updated = admin.update_tenant(id="team-alpha", quota=new_quota)
        assert updated.resource_quota.max_cpus == 200
        assert updated.resource_quota.max_gpus == 20

    def test_update_nonexistent_tenant(self):
        """Test updating nonexistent tenant raises error."""
        admin = TenantAdmin()
        with pytest.raises(TenantNotFoundError):
            admin.update_tenant(id="nonexistent", name="New Name")

    def test_delete_tenant(self):
        """Test deleting a tenant."""
        admin = TenantAdmin()
        admin.create_tenant(id="team-alpha")
        admin.delete_tenant("team-alpha")
        with pytest.raises(TenantNotFoundError):
            admin.get_tenant("team-alpha")

    def test_delete_nonexistent_tenant(self):
        """Test deleting nonexistent tenant raises error."""
        admin = TenantAdmin()
        with pytest.raises(TenantNotFoundError):
            admin.delete_tenant("nonexistent")

    def test_get_tenant_usage(self):
        """Test getting tenant usage."""
        admin = TenantAdmin()
        admin.create_tenant(
            id="team-alpha",
            quota=ResourceQuota(max_cpus=100),
        )
        usage = admin.get_tenant_usage("team-alpha")
        assert usage.tenant_id == "team-alpha"
        assert usage.cpus_used == 0
        assert usage.cpus_quota == 100

    def test_allocate_resources(self):
        """Test allocating resources for a tenant."""
        admin = TenantAdmin()
        admin.create_tenant(
            id="team-alpha",
            quota=ResourceQuota(max_cpus=100),
        )
        result = admin.allocate_resources("team-alpha", cpus=50)
        assert result is True
        usage = admin.get_tenant_usage("team-alpha")
        assert usage.cpus_used == 50

    def test_allocate_resources_exceeds_quota(self):
        """Test allocating resources that exceed quota."""
        admin = TenantAdmin()
        admin.create_tenant(
            id="team-alpha",
            quota=ResourceQuota(max_cpus=100),
        )
        admin.allocate_resources("team-alpha", cpus=80)
        result = admin.allocate_resources("team-alpha", cpus=30)
        assert result is False  # Would exceed quota

    def test_release_resources(self):
        """Test releasing resources for a tenant."""
        admin = TenantAdmin()
        admin.create_tenant(
            id="team-alpha",
            quota=ResourceQuota(max_cpus=100),
        )
        admin.allocate_resources("team-alpha", cpus=50)
        admin.release_resources("team-alpha", cpus=30)
        usage = admin.get_tenant_usage("team-alpha")
        assert usage.cpus_used == 20

    def test_check_quota(self):
        """Test checking if allocation is within quota."""
        admin = TenantAdmin()
        admin.create_tenant(
            id="team-alpha",
            quota=ResourceQuota(max_cpus=100),
        )
        admin.allocate_resources("team-alpha", cpus=50)
        assert admin.check_quota("team-alpha", cpus=30) is True
        assert admin.check_quota("team-alpha", cpus=60) is False

    def test_get_all_tenant_usage(self):
        """Test getting usage for all tenants."""
        admin = TenantAdmin()
        admin.create_tenant(id="team-alpha")
        admin.create_tenant(id="team-beta")
        admin.allocate_resources("team-alpha", cpus=10)
        admin.allocate_resources("team-beta", cpus=20)
        all_usage = admin.get_all_tenant_usage()
        assert len(all_usage) == 2
        assert all_usage["team-alpha"].cpus_used == 10
        assert all_usage["team-beta"].cpus_used == 20

    def test_get_tenant_by_namespace(self):
        """Test getting tenant by namespace."""
        admin = TenantAdmin()
        admin.create_tenant(
            id="team-alpha",
            namespace="alpha-ns",
        )
        tenant = admin.get_tenant_by_namespace("alpha-ns")
        assert tenant is not None
        assert tenant.id == "team-alpha"

    def test_get_tenant_by_namespace_not_found(self):
        """Test getting tenant by nonexistent namespace."""
        admin = TenantAdmin()
        admin.create_tenant(id="team-alpha")
        tenant = admin.get_tenant_by_namespace("nonexistent-ns")
        assert tenant is None

    def test_singleton_behavior(self):
        """Test that TenantAdmin is a singleton."""
        admin1 = TenantAdmin()
        admin2 = TenantAdmin()
        assert admin1 is admin2
        admin1.create_tenant(id="team-alpha")
        tenant = admin2.get_tenant("team-alpha")
        assert tenant.id == "team-alpha"

    def test_get_usage_history(self):
        """Test getting usage history for a tenant."""
        admin = TenantAdmin()
        admin.create_tenant(id="team-alpha")
        admin.allocate_resources("team-alpha", cpus=10)
        admin.allocate_resources("team-alpha", cpus=20)
        history = admin.get_usage_history("team-alpha")
        assert len(history) >= 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
