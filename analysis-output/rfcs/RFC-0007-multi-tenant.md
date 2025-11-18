# RFC-0007: Multi-Tenant Isolation

**Status:** Draft
**Author:** Codebase Analysis
**Created:** 2025-01-18
**Commit Reference:** `d1cce8c9dc8411fad7cfbd619350bec6f19839a3`

## Summary

Add multi-tenant isolation capabilities to Ray, enabling multiple users/teams to share a cluster with resource isolation, security boundaries, and fair scheduling.

## Motivation

### Current State

Ray clusters are single-tenant:
- All jobs share all resources
- No isolation between jobs
- No resource quotas per user
- No priority/fairness scheduling

This limits Ray adoption in:
- Shared team environments
- Platform-as-a-service offerings
- Enterprise deployments

### Evidence from Codebase

From security analysis:
- No namespace-based resource isolation
- Jobs can access each other's actors by name
- No per-tenant resource quotas

## Detailed Design

### Tenant Model

```python
@dataclass
class Tenant:
    id: str
    name: str
    resource_quota: ResourceQuota
    priority: int
    isolation_level: IsolationLevel

@dataclass
class ResourceQuota:
    max_cpus: int
    max_gpus: int
    max_memory_gb: float
    max_object_store_gb: float
    max_running_tasks: int
    max_actors: int
```

### Isolation Levels

```python
class IsolationLevel(Enum):
    NONE = 0          # Current behavior
    NAMESPACE = 1     # Separate namespaces
    RESOURCE = 2      # Resource quotas enforced
    PROCESS = 3       # Process-level isolation
    CONTAINER = 4     # Container-level isolation
```

### Namespace Isolation

```python
# Each tenant gets a namespace
ray.init(namespace="team-alpha")

# Actors are only visible within namespace
@ray.remote
class Service:
    pass

service = Service.remote()

# Another tenant cannot access
ray.init(namespace="team-beta")
ray.get_actor("Service")  # Error: Actor not found
```

### Resource Quotas

```python
# Admin configures quotas
ray.admin.create_tenant(
    id="team-alpha",
    quota=ResourceQuota(
        max_cpus=100,
        max_gpus=10,
        max_memory_gb=256
    )
)

# Enforcement
@ray.remote(num_cpus=10)
def big_task():
    pass

# If team-alpha already using 95 CPUs
big_task.remote()  # Queued until quota available
```

### Fair Scheduling

```python
# Hierarchical fair scheduler
class FairScheduler:
    def schedule(self, tasks: List[Task]) -> List[ScheduleResult]:
        # Group by tenant
        tenant_tasks = group_by_tenant(tasks)

        # Calculate fair share per tenant
        fair_shares = self.calculate_fair_shares(tenant_tasks)

        # Schedule within fair share
        results = []
        for tenant, share in fair_shares.items():
            scheduled = self.schedule_tenant(
                tenant_tasks[tenant],
                max_resources=share
            )
            results.extend(scheduled)

        return results
```

### Tenant-Aware Components

#### GCS

```cpp
class TenantAwareGcsActorManager {
  // Track actors per tenant
  absl::flat_hash_map<TenantID, std::vector<ActorID>> tenant_actors_;

  // Enforce tenant quota on actor creation
  Status CreateActor(const ActorSpec& spec) {
    TenantID tenant = spec.tenant_id();
    if (ExceedsQuota(tenant, spec.resources())) {
      return Status::ResourceExhausted("Tenant quota exceeded");
    }
    // ...
  }
};
```

#### Raylet

```cpp
class TenantAwareScheduler {
  // Per-tenant resource accounting
  absl::flat_hash_map<TenantID, ResourceSet> tenant_usage_;

  // Schedule considering tenant quotas
  SchedulingResult Schedule(const TaskSpec& task) {
    TenantID tenant = task.tenant_id();
    ResourceSet available = GetTenantAvailable(tenant);

    if (!available.Contains(task.resources())) {
      return SchedulingResult::QuotaExceeded();
    }
    // ...
  }
};
```

### Administration API

```python
from ray.admin import TenantAdmin

admin = TenantAdmin()

# Create tenant
admin.create_tenant(
    id="team-alpha",
    quota=ResourceQuota(max_cpus=100),
    priority=1
)

# Update quota
admin.update_tenant(
    id="team-alpha",
    quota=ResourceQuota(max_cpus=200)
)

# View usage
usage = admin.get_tenant_usage("team-alpha")
print(f"CPUs: {usage.cpus_used}/{usage.cpus_quota}")

# List all tenants
for tenant in admin.list_tenants():
    print(f"{tenant.id}: {tenant.usage}")
```

### Dashboard Integration

New dashboard pages:
- Tenant list with usage
- Per-tenant resource graphs
- Quota utilization alerts
- Fair share visualization

## Implementation Plan

### Phase 1: Namespace Isolation (Month 1)
- Enhanced namespace support
- Namespace-scoped actors/PGs
- Namespace ACLs

### Phase 2: Resource Quotas (Month 2)
- Quota data structures
- GCS enforcement
- Raylet enforcement

### Phase 3: Fair Scheduling (Month 3)
- Fair share algorithm
- Priority support
- Preemption (optional)

### Phase 4: Operations (Month 4)
- Admin API
- Dashboard
- Monitoring/alerts

## Backwards Compatibility

**Default behavior unchanged:**
- Single-tenant mode remains default
- Existing code works without modification
- Multi-tenancy is opt-in

```python
# Single-tenant (current behavior)
ray.init()

# Multi-tenant mode
ray.init(
    multi_tenant=True,
    tenant_id="team-alpha"
)
```

## Security Considerations

- Tenants should not access other tenants' data
- Quotas prevent denial-of-service
- Authentication required for multi-tenant mode

## Alternatives Considered

### Alternative 1: Separate Clusters

Run separate Ray cluster per tenant.

**Rejected because:**
- Poor resource utilization
- High operational overhead
- No fine-grained sharing

### Alternative 2: Kubernetes-Only

Rely on Kubernetes for isolation.

**Rejected because:**
- Not all deployments use K8s
- Limited scheduling flexibility
- Ray-level quotas still needed

## Open Questions

1. **Preemption:** Should higher-priority tenants preempt lower?
2. **Burst:** Allow temporary quota exceeds?
3. **Chargeback:** Integration with cost tracking?

## Success Criteria

- [ ] Namespace isolation prevents cross-tenant access
- [ ] Quotas enforced within 5% accuracy
- [ ] Fair scheduler achieves < 10% deviation from fair share
- [ ] < 5% performance overhead for multi-tenant mode

## Effort Estimation

- **Development:** 16 dev-weeks
- **Testing:** 4 dev-weeks
- **Total:** 20 dev-weeks (5 months)

## References

- Current namespaces: [`python/ray/_private/namespace.py`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/python/ray/_private/)
- Scheduling: [`src/ray/raylet/scheduling/`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/src/ray/raylet/scheduling/)
