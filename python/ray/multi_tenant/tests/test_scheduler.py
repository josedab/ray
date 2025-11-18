"""Tests for fair scheduling."""

import pytest
import time

from ray.multi_tenant.scheduler import (
    FairScheduler,
    TenantSchedulingPolicy,
    ScheduleRequest,
    ScheduleResult,
)
from ray.multi_tenant.tenant import Tenant, ResourceQuota
from ray.multi_tenant.usage import TenantUsageTracker


class TestScheduleRequest:
    """Tests for ScheduleRequest class."""

    def test_create_basic_request(self):
        """Test creating a basic schedule request."""
        request = ScheduleRequest(
            request_id="req-1",
            tenant_id="team-alpha",
            cpus=4,
            memory_gb=8.0,
        )
        assert request.request_id == "req-1"
        assert request.tenant_id == "team-alpha"
        assert request.cpus == 4
        assert request.memory_gb == 8.0

    def test_create_request_with_priority(self):
        """Test creating request with priority."""
        request = ScheduleRequest(
            request_id="req-1",
            tenant_id="team-alpha",
            cpus=4,
            priority=10,
        )
        assert request.priority == 10


class TestTenantSchedulingPolicy:
    """Tests for TenantSchedulingPolicy class."""

    def test_default_policy(self):
        """Test default scheduling policy."""
        policy = TenantSchedulingPolicy()
        assert policy.enable_preemption is False
        assert policy.enable_burst is False
        assert policy.fair_share_tolerance == 0.1
        assert policy.priority_weight == 1.0

    def test_custom_policy(self):
        """Test custom scheduling policy."""
        policy = TenantSchedulingPolicy(
            enable_preemption=True,
            enable_burst=True,
            fair_share_tolerance=0.2,
            priority_weight=2.0,
        )
        assert policy.enable_preemption is True
        assert policy.enable_burst is True
        assert policy.fair_share_tolerance == 0.2
        assert policy.priority_weight == 2.0


class TestFairScheduler:
    """Tests for FairScheduler class."""

    @pytest.fixture
    def usage_tracker(self):
        """Create a usage tracker."""
        return TenantUsageTracker()

    @pytest.fixture
    def scheduler(self, usage_tracker):
        """Create a scheduler with usage tracker."""
        scheduler = FairScheduler(usage_tracker)
        scheduler.update_cluster_resources(
            total_cpus=100,
            total_gpus=10,
            total_memory_gb=256.0,
            total_object_store_gb=128.0,
        )
        return scheduler

    def test_register_tenant(self, scheduler, usage_tracker):
        """Test registering a tenant."""
        tenant = Tenant(
            id="team-alpha",
            name="Team Alpha",
            resource_quota=ResourceQuota(max_cpus=50),
        )
        usage_tracker.register_tenant(tenant)
        scheduler.register_tenant(tenant)
        # Should not raise
        fair_shares = scheduler.calculate_fair_shares()
        assert "team-alpha" in fair_shares

    def test_calculate_fair_shares_single_tenant(self, scheduler, usage_tracker):
        """Test fair share calculation with single tenant."""
        tenant = Tenant(
            id="team-alpha",
            name="Team Alpha",
            priority=0,
        )
        usage_tracker.register_tenant(tenant)
        scheduler.register_tenant(tenant)
        fair_shares = scheduler.calculate_fair_shares()
        # Single tenant should get all resources
        assert fair_shares["team-alpha"].max_cpus == 100
        assert fair_shares["team-alpha"].max_gpus == 10

    def test_calculate_fair_shares_equal_priority(self, scheduler, usage_tracker):
        """Test fair share calculation with equal priority tenants."""
        tenant1 = Tenant(id="team-alpha", name="Team Alpha", priority=0)
        tenant2 = Tenant(id="team-beta", name="Team Beta", priority=0)
        usage_tracker.register_tenant(tenant1)
        usage_tracker.register_tenant(tenant2)
        scheduler.register_tenant(tenant1)
        scheduler.register_tenant(tenant2)
        fair_shares = scheduler.calculate_fair_shares()
        # Each should get 50% of resources
        assert fair_shares["team-alpha"].max_cpus == 50
        assert fair_shares["team-beta"].max_cpus == 50

    def test_calculate_fair_shares_different_priority(
        self, scheduler, usage_tracker
    ):
        """Test fair share calculation with different priorities."""
        tenant1 = Tenant(id="team-alpha", name="Team Alpha", priority=0)  # weight 1
        tenant2 = Tenant(id="team-beta", name="Team Beta", priority=2)  # weight 3
        usage_tracker.register_tenant(tenant1)
        usage_tracker.register_tenant(tenant2)
        scheduler.register_tenant(tenant1)
        scheduler.register_tenant(tenant2)
        fair_shares = scheduler.calculate_fair_shares()
        # Alpha: 1/4 = 25, Beta: 3/4 = 75
        assert fair_shares["team-alpha"].max_cpus == 25
        assert fair_shares["team-beta"].max_cpus == 75

    def test_calculate_fair_shares_with_quota_limit(
        self, scheduler, usage_tracker
    ):
        """Test fair share respects quota limits."""
        tenant = Tenant(
            id="team-alpha",
            name="Team Alpha",
            resource_quota=ResourceQuota(max_cpus=30),
        )
        usage_tracker.register_tenant(tenant)
        scheduler.register_tenant(tenant)
        fair_shares = scheduler.calculate_fair_shares()
        # Should be limited to 30 even though fair share would be 100
        assert fair_shares["team-alpha"].max_cpus == 30

    def test_schedule_single_request(self, scheduler, usage_tracker):
        """Test scheduling a single request."""
        tenant = Tenant(id="team-alpha", name="Team Alpha")
        usage_tracker.register_tenant(tenant)
        scheduler.register_tenant(tenant)
        request = ScheduleRequest(
            request_id="req-1",
            tenant_id="team-alpha",
            cpus=10,
        )
        results = scheduler.schedule([request])
        assert len(results) == 1
        assert results[0].scheduled is True
        assert results[0].request_id == "req-1"

    def test_schedule_multiple_requests(self, scheduler, usage_tracker):
        """Test scheduling multiple requests."""
        tenant = Tenant(id="team-alpha", name="Team Alpha")
        usage_tracker.register_tenant(tenant)
        scheduler.register_tenant(tenant)
        requests = [
            ScheduleRequest(
                request_id="req-1",
                tenant_id="team-alpha",
                cpus=10,
            ),
            ScheduleRequest(
                request_id="req-2",
                tenant_id="team-alpha",
                cpus=20,
            ),
        ]
        results = scheduler.schedule(requests)
        assert len(results) == 2
        assert all(r.scheduled for r in results)

    def test_schedule_exceeds_quota(self, scheduler, usage_tracker):
        """Test scheduling when request exceeds quota."""
        tenant = Tenant(
            id="team-alpha",
            name="Team Alpha",
            resource_quota=ResourceQuota(max_cpus=50),
        )
        usage_tracker.register_tenant(tenant)
        scheduler.register_tenant(tenant)
        request = ScheduleRequest(
            request_id="req-1",
            tenant_id="team-alpha",
            cpus=60,
        )
        results = scheduler.schedule([request])
        assert len(results) == 1
        assert results[0].scheduled is False
        assert "Quota exceeded" in results[0].reason

    def test_schedule_priority_ordering(self, scheduler, usage_tracker):
        """Test that higher priority requests are scheduled first."""
        tenant = Tenant(
            id="team-alpha",
            name="Team Alpha",
            resource_quota=ResourceQuota(max_cpus=50),
        )
        usage_tracker.register_tenant(tenant)
        scheduler.register_tenant(tenant)
        requests = [
            ScheduleRequest(
                request_id="req-low",
                tenant_id="team-alpha",
                cpus=40,
                priority=1,
                timestamp=1,
            ),
            ScheduleRequest(
                request_id="req-high",
                tenant_id="team-alpha",
                cpus=40,
                priority=10,
                timestamp=2,
            ),
        ]
        results = scheduler.schedule(requests)
        # High priority should be scheduled, low should be queued
        scheduled = [r for r in results if r.scheduled]
        assert len(scheduled) == 1
        assert scheduled[0].request_id == "req-high"

    def test_schedule_for_multiple_tenants(self, scheduler, usage_tracker):
        """Test scheduling for multiple tenants."""
        tenant1 = Tenant(id="team-alpha", name="Team Alpha")
        tenant2 = Tenant(id="team-beta", name="Team Beta")
        usage_tracker.register_tenant(tenant1)
        usage_tracker.register_tenant(tenant2)
        scheduler.register_tenant(tenant1)
        scheduler.register_tenant(tenant2)
        requests = [
            ScheduleRequest(
                request_id="req-alpha",
                tenant_id="team-alpha",
                cpus=30,
            ),
            ScheduleRequest(
                request_id="req-beta",
                tenant_id="team-beta",
                cpus=30,
            ),
        ]
        results = scheduler.schedule(requests)
        assert len(results) == 2
        assert all(r.scheduled for r in results)

    def test_get_pending_requests(self, scheduler, usage_tracker):
        """Test getting pending requests."""
        tenant = Tenant(
            id="team-alpha",
            name="Team Alpha",
            resource_quota=ResourceQuota(max_cpus=10),
        )
        usage_tracker.register_tenant(tenant)
        scheduler.register_tenant(tenant)
        request = ScheduleRequest(
            request_id="req-1",
            tenant_id="team-alpha",
            cpus=20,  # Exceeds quota
        )
        scheduler.schedule([request])
        pending = scheduler.get_pending_requests("team-alpha")
        assert len(pending["team-alpha"]) == 1
        assert pending["team-alpha"][0].request_id == "req-1"

    def test_clear_pending_request(self, scheduler, usage_tracker):
        """Test clearing a pending request."""
        tenant = Tenant(
            id="team-alpha",
            name="Team Alpha",
            resource_quota=ResourceQuota(max_cpus=10),
        )
        usage_tracker.register_tenant(tenant)
        scheduler.register_tenant(tenant)
        request = ScheduleRequest(
            request_id="req-1",
            tenant_id="team-alpha",
            cpus=20,
        )
        scheduler.schedule([request])
        result = scheduler.clear_pending_request("team-alpha", "req-1")
        assert result is True
        pending = scheduler.get_pending_requests("team-alpha")
        assert len(pending["team-alpha"]) == 0

    def test_release_resources(self, scheduler, usage_tracker):
        """Test releasing resources."""
        tenant = Tenant(id="team-alpha", name="Team Alpha")
        usage_tracker.register_tenant(tenant)
        scheduler.register_tenant(tenant)
        request = ScheduleRequest(
            request_id="req-1",
            tenant_id="team-alpha",
            cpus=10,
        )
        scheduler.schedule([request])
        usage = usage_tracker.get_usage("team-alpha")
        assert usage.cpus_used == 10
        scheduler.release_resources("team-alpha", cpus=10)
        usage = usage_tracker.get_usage("team-alpha")
        assert usage.cpus_used == 0

    def test_get_fair_share_stats(self, scheduler, usage_tracker):
        """Test getting fair share statistics."""
        tenant = Tenant(
            id="team-alpha",
            name="Team Alpha",
            priority=5,
        )
        usage_tracker.register_tenant(tenant)
        scheduler.register_tenant(tenant)
        usage_tracker.update_usage("team-alpha", cpus_used=20)
        stats = scheduler.get_fair_share_stats()
        assert "total_resources" in stats
        assert stats["total_resources"]["cpus"] == 100
        assert "team-alpha" in stats["tenants"]
        assert stats["tenants"]["team-alpha"]["priority"] == 5
        assert stats["tenants"]["team-alpha"]["usage"]["cpus"] == 20

    def test_get_tenant_available_resources(self, scheduler, usage_tracker):
        """Test getting available resources for a tenant."""
        tenant = Tenant(
            id="team-alpha",
            name="Team Alpha",
            resource_quota=ResourceQuota(max_cpus=50),
        )
        usage_tracker.register_tenant(tenant)
        scheduler.register_tenant(tenant)
        usage_tracker.update_usage("team-alpha", cpus_used=20)
        available = scheduler.get_tenant_available_resources("team-alpha")
        assert available[0] == 30  # 50 - 20

    def test_unregister_tenant(self, scheduler, usage_tracker):
        """Test unregistering a tenant."""
        tenant = Tenant(id="team-alpha", name="Team Alpha")
        usage_tracker.register_tenant(tenant)
        scheduler.register_tenant(tenant)
        scheduler.unregister_tenant("team-alpha")
        fair_shares = scheduler.calculate_fair_shares()
        assert "team-alpha" not in fair_shares


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
