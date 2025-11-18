"""Tests for usage tracking."""

import pytest
import time

from ray.multi_tenant.usage import TenantUsage, TenantUsageTracker
from ray.multi_tenant.tenant import Tenant, ResourceQuota


class TestTenantUsage:
    """Tests for TenantUsage class."""

    def test_create_default_usage(self):
        """Test creating usage with default values."""
        usage = TenantUsage(tenant_id="team-alpha")
        assert usage.tenant_id == "team-alpha"
        assert usage.cpus_used == 0.0
        assert usage.gpus_used == 0.0
        assert usage.memory_gb_used == 0.0
        assert usage.running_tasks == 0
        assert usage.actors == 0

    def test_create_usage_with_values(self):
        """Test creating usage with specific values."""
        usage = TenantUsage(
            tenant_id="team-alpha",
            cpus_used=50.0,
            gpus_used=5.0,
            memory_gb_used=128.0,
            running_tasks=10,
            actors=5,
            cpus_quota=100,
        )
        assert usage.cpus_used == 50.0
        assert usage.gpus_used == 5.0
        assert usage.memory_gb_used == 128.0
        assert usage.running_tasks == 10
        assert usage.actors == 5
        assert usage.cpus_quota == 100

    def test_cpu_utilization(self):
        """Test CPU utilization calculation."""
        usage = TenantUsage(
            tenant_id="team-alpha",
            cpus_used=50.0,
            cpus_quota=100,
        )
        assert usage.cpu_utilization() == 50.0

    def test_cpu_utilization_no_quota(self):
        """Test CPU utilization with no quota."""
        usage = TenantUsage(
            tenant_id="team-alpha",
            cpus_used=50.0,
        )
        assert usage.cpu_utilization() is None

    def test_is_over_quota(self):
        """Test over quota detection."""
        usage = TenantUsage(
            tenant_id="team-alpha",
            cpus_used=150.0,
            cpus_quota=100,
        )
        assert usage.is_over_quota() is True

    def test_is_not_over_quota(self):
        """Test not over quota."""
        usage = TenantUsage(
            tenant_id="team-alpha",
            cpus_used=50.0,
            cpus_quota=100,
        )
        assert usage.is_over_quota() is False

    def test_to_dict(self):
        """Test converting usage to dictionary."""
        usage = TenantUsage(
            tenant_id="team-alpha",
            cpus_used=50.0,
        )
        data = usage.to_dict()
        assert data["tenant_id"] == "team-alpha"
        assert data["cpus_used"] == 50.0

    def test_from_dict(self):
        """Test creating usage from dictionary."""
        data = {
            "tenant_id": "team-alpha",
            "cpus_used": 50.0,
            "gpus_used": 5.0,
        }
        usage = TenantUsage.from_dict(data)
        assert usage.tenant_id == "team-alpha"
        assert usage.cpus_used == 50.0
        assert usage.gpus_used == 5.0


class TestTenantUsageTracker:
    """Tests for TenantUsageTracker class."""

    @pytest.fixture
    def tracker(self):
        """Create a usage tracker."""
        return TenantUsageTracker()

    def test_register_tenant(self, tracker):
        """Test registering a tenant."""
        tenant = Tenant(
            id="team-alpha",
            name="Team Alpha",
            resource_quota=ResourceQuota(max_cpus=100),
        )
        tracker.register_tenant(tenant)
        usage = tracker.get_usage("team-alpha")
        assert usage.tenant_id == "team-alpha"
        assert usage.cpus_quota == 100

    def test_unregister_tenant(self, tracker):
        """Test unregistering a tenant."""
        tenant = Tenant(id="team-alpha", name="Team Alpha")
        tracker.register_tenant(tenant)
        tracker.unregister_tenant("team-alpha")
        with pytest.raises(ValueError):
            tracker.get_usage("team-alpha")

    def test_update_usage(self, tracker):
        """Test updating tenant usage."""
        tenant = Tenant(id="team-alpha", name="Team Alpha")
        tracker.register_tenant(tenant)
        tracker.update_usage(
            "team-alpha",
            cpus_used=50.0,
            gpus_used=5.0,
            running_tasks=10,
        )
        usage = tracker.get_usage("team-alpha")
        assert usage.cpus_used == 50.0
        assert usage.gpus_used == 5.0
        assert usage.running_tasks == 10

    def test_increment_usage(self, tracker):
        """Test incrementing tenant usage."""
        tenant = Tenant(id="team-alpha", name="Team Alpha")
        tracker.register_tenant(tenant)
        tracker.increment_usage("team-alpha", cpus=10, gpus=1, tasks=5)
        tracker.increment_usage("team-alpha", cpus=20, tasks=3)
        usage = tracker.get_usage("team-alpha")
        assert usage.cpus_used == 30.0
        assert usage.gpus_used == 1.0
        assert usage.running_tasks == 8

    def test_decrement_usage(self, tracker):
        """Test decrementing tenant usage."""
        tenant = Tenant(id="team-alpha", name="Team Alpha")
        tracker.register_tenant(tenant)
        tracker.increment_usage("team-alpha", cpus=50, tasks=10)
        tracker.decrement_usage("team-alpha", cpus=30, tasks=5)
        usage = tracker.get_usage("team-alpha")
        assert usage.cpus_used == 20.0
        assert usage.running_tasks == 5

    def test_decrement_usage_not_negative(self, tracker):
        """Test that decrement doesn't go below zero."""
        tenant = Tenant(id="team-alpha", name="Team Alpha")
        tracker.register_tenant(tenant)
        tracker.increment_usage("team-alpha", cpus=10)
        tracker.decrement_usage("team-alpha", cpus=50)
        usage = tracker.get_usage("team-alpha")
        assert usage.cpus_used == 0.0

    def test_check_quota(self, tracker):
        """Test quota checking."""
        tenant = Tenant(
            id="team-alpha",
            name="Team Alpha",
            resource_quota=ResourceQuota(max_cpus=100),
        )
        tracker.register_tenant(tenant)
        tracker.update_usage("team-alpha", cpus_used=50.0)
        assert tracker.check_quota("team-alpha", cpus=30) is True
        assert tracker.check_quota("team-alpha", cpus=60) is False

    def test_get_all_usage(self, tracker):
        """Test getting usage for all tenants."""
        tenant1 = Tenant(id="team-alpha", name="Team Alpha")
        tenant2 = Tenant(id="team-beta", name="Team Beta")
        tracker.register_tenant(tenant1)
        tracker.register_tenant(tenant2)
        tracker.update_usage("team-alpha", cpus_used=10)
        tracker.update_usage("team-beta", cpus_used=20)
        all_usage = tracker.get_all_usage()
        assert len(all_usage) == 2
        assert all_usage["team-alpha"].cpus_used == 10
        assert all_usage["team-beta"].cpus_used == 20

    def test_get_available_resources(self, tracker):
        """Test getting available resources."""
        tenant = Tenant(
            id="team-alpha",
            name="Team Alpha",
            resource_quota=ResourceQuota(max_cpus=100, max_gpus=10),
        )
        tracker.register_tenant(tenant)
        tracker.update_usage("team-alpha", cpus_used=30.0, gpus_used=3.0)
        available = tracker.get_available_resources("team-alpha")
        assert available.max_cpus == 70
        assert available.max_gpus == 7

    def test_get_history(self, tracker):
        """Test getting usage history."""
        tenant = Tenant(id="team-alpha", name="Team Alpha")
        tracker.register_tenant(tenant)
        tracker.update_usage("team-alpha", cpus_used=10)
        tracker.update_usage("team-alpha", cpus_used=20)
        tracker.update_usage("team-alpha", cpus_used=30)
        history = tracker.get_history("team-alpha")
        assert len(history) >= 3

    def test_get_history_with_limit(self, tracker):
        """Test getting limited usage history."""
        tenant = Tenant(id="team-alpha", name="Team Alpha")
        tracker.register_tenant(tenant)
        for i in range(10):
            tracker.update_usage("team-alpha", cpus_used=i * 10)
        history = tracker.get_history("team-alpha", limit=5)
        assert len(history) == 5

    def test_update_nonexistent_tenant(self, tracker):
        """Test updating nonexistent tenant raises error."""
        with pytest.raises(ValueError):
            tracker.update_usage("nonexistent", cpus_used=10)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
