.. _multi-tenant:

Multi-Tenant Isolation
======================

Ray supports multi-tenant isolation, enabling multiple users/teams to share a cluster with resource isolation, security boundaries, and fair scheduling.

.. note::

   Multi-tenant isolation is an advanced feature designed for platform-as-a-service offerings and enterprise deployments.

Overview
--------

Multi-tenancy in Ray provides:

- **Namespace Isolation**: Each tenant gets a separate namespace for actors and placement groups
- **Resource Quotas**: Enforce resource limits per tenant (CPUs, GPUs, memory)
- **Fair Scheduling**: Distribute resources fairly among tenants based on priority
- **Usage Tracking**: Monitor resource usage per tenant

Basic Usage
-----------

Creating Tenants
~~~~~~~~~~~~~~~~

Use the ``TenantAdmin`` API to create and manage tenants:

.. code-block:: python

    from ray.multi_tenant import TenantAdmin, ResourceQuota

    # Initialize admin
    admin = TenantAdmin()

    # Create a tenant with resource quota
    tenant = admin.create_tenant(
        id="team-alpha",
        name="Team Alpha",
        quota=ResourceQuota(
            max_cpus=100,
            max_gpus=10,
            max_memory_gb=256.0
        ),
        priority=1
    )

Using Tenant Context
~~~~~~~~~~~~~~~~~~~~

Associate operations with a tenant using the context manager:

.. code-block:: python

    from ray.multi_tenant import tenant_context
    import ray

    with tenant_context("team-alpha"):
        # All operations here are associated with team-alpha
        @ray.remote
        def my_task():
            return "Hello from team-alpha"

        result = ray.get(my_task.remote())

Namespace Isolation
-------------------

Each tenant automatically gets a namespace. Actors and placement groups in one tenant's namespace are not visible to other tenants:

.. code-block:: python

    import ray
    from ray.multi_tenant import TenantAdmin

    admin = TenantAdmin()
    tenant = admin.get_tenant("team-alpha")

    # Initialize Ray with tenant's namespace
    ray.init(namespace=tenant.namespace)

    # Actors created here are only visible in this namespace
    @ray.remote
    class MyService:
        def get_data(self):
            return "Team Alpha data"

    service = MyService.remote()

Resource Quotas
---------------

Define resource limits for tenants:

.. code-block:: python

    from ray.multi_tenant import ResourceQuota, TenantAdmin

    admin = TenantAdmin()

    # Create quota
    quota = ResourceQuota(
        max_cpus=100,
        max_gpus=10,
        max_memory_gb=256.0,
        max_object_store_gb=128.0,
        max_running_tasks=1000,
        max_actors=500
    )

    admin.create_tenant(id="team-alpha", quota=quota)

    # Check if allocation is within quota
    if admin.check_quota("team-alpha", cpus=20, gpus=2):
        # Allocate resources
        admin.allocate_resources("team-alpha", cpus=20, gpus=2)
    else:
        print("Quota exceeded")

Monitoring Usage
~~~~~~~~~~~~~~~~

Track resource usage for tenants:

.. code-block:: python

    from ray.multi_tenant import TenantAdmin

    admin = TenantAdmin()

    # Get usage for a tenant
    usage = admin.get_tenant_usage("team-alpha")
    print(f"CPUs: {usage.cpus_used}/{usage.cpus_quota}")
    print(f"GPUs: {usage.gpus_used}/{usage.gpus_quota}")
    print(f"Memory: {usage.memory_gb_used}/{usage.memory_gb_quota} GB")
    print(f"Running tasks: {usage.running_tasks}")
    print(f"Actors: {usage.actors}")

    # Get usage for all tenants
    all_usage = admin.get_all_tenant_usage()
    for tenant_id, usage in all_usage.items():
        print(f"{tenant_id}: {usage.cpus_used} CPUs")

Fair Scheduling
---------------

The fair scheduler distributes resources among tenants based on their priority:

.. code-block:: python

    from ray.multi_tenant import FairScheduler, TenantUsageTracker, ScheduleRequest

    # Create tracker and scheduler
    tracker = TenantUsageTracker()
    scheduler = FairScheduler(tracker)

    # Configure cluster resources
    scheduler.update_cluster_resources(
        total_cpus=1000,
        total_gpus=100,
        total_memory_gb=2048.0,
        total_object_store_gb=1024.0
    )

    # Register tenants
    for tenant in admin.list_tenants():
        tracker.register_tenant(tenant)
        scheduler.register_tenant(tenant)

    # Calculate fair shares
    fair_shares = scheduler.calculate_fair_shares()
    for tenant_id, share in fair_shares.items():
        print(f"{tenant_id}: {share.max_cpus} CPUs")

    # Schedule requests
    requests = [
        ScheduleRequest(
            request_id="task-1",
            tenant_id="team-alpha",
            cpus=10,
            memory_gb=20.0
        ),
        ScheduleRequest(
            request_id="task-2",
            tenant_id="team-beta",
            cpus=5,
            memory_gb=10.0
        )
    ]

    results = scheduler.schedule(requests)
    for result in results:
        if result.scheduled:
            print(f"{result.request_id}: scheduled")
        else:
            print(f"{result.request_id}: queued - {result.reason}")

Isolation Levels
----------------

Ray supports multiple levels of isolation:

.. code-block:: python

    from ray.multi_tenant import IsolationLevel, TenantAdmin

    admin = TenantAdmin()

    # NAMESPACE: Separate namespaces for actors/PGs (default)
    admin.create_tenant(
        id="team-alpha",
        isolation_level=IsolationLevel.NAMESPACE
    )

    # RESOURCE: Namespace + resource quota enforcement
    admin.create_tenant(
        id="team-beta",
        isolation_level=IsolationLevel.RESOURCE
    )

    # PROCESS: Process-level isolation
    admin.create_tenant(
        id="team-gamma",
        isolation_level=IsolationLevel.PROCESS
    )

    # CONTAINER: Container-level isolation (maximum security)
    admin.create_tenant(
        id="team-delta",
        isolation_level=IsolationLevel.CONTAINER
    )

Administration
--------------

Managing Tenants
~~~~~~~~~~~~~~~~

.. code-block:: python

    from ray.multi_tenant import TenantAdmin

    admin = TenantAdmin()

    # Update tenant quota
    admin.update_tenant(
        id="team-alpha",
        quota=ResourceQuota(max_cpus=200)
    )

    # List all tenants
    for tenant in admin.list_tenants():
        print(f"{tenant.id}: priority={tenant.priority}")

    # Delete tenant
    admin.delete_tenant("team-alpha")

Handling Quota Exceeded
~~~~~~~~~~~~~~~~~~~~~~~

When a tenant exceeds their quota, tasks are queued:

.. code-block:: python

    from ray.multi_tenant import TenantAdmin, QuotaExceededError

    admin = TenantAdmin()

    # Check quota before scheduling
    if not admin.check_quota("team-alpha", cpus=100):
        # Wait or queue the request
        print("Quota exceeded, request will be queued")

Best Practices
--------------

1. **Set appropriate quotas**: Start with conservative quotas and adjust based on usage patterns.

2. **Use namespaces**: Always use tenant namespaces to prevent cross-tenant access to actors.

3. **Monitor usage**: Regularly check tenant usage to identify quota issues.

4. **Plan for bursts**: Consider enabling burst mode for tenants that need occasional extra resources.

5. **Priority assignment**: Assign priorities based on business criticality, not team size.

API Reference
-------------

See the :ref:`multi-tenant-api-reference` for complete API documentation.
