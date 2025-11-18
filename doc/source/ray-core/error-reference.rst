.. _error-reference:

Error Reference
===============

Ray provides enhanced error messages with actionable remediation steps to help you quickly diagnose and resolve issues. This page documents common Ray errors and how to fix them.

Using the Error Helper CLI
--------------------------

Ray includes a command-line tool to get help with error codes:

.. code-block:: bash

    # Get help for a specific error
    ray error OUT_OF_MEMORY

    # List all available error codes
    ray error --list

The error helper provides:

- Detailed explanation of the error
- Common causes
- Step-by-step remediation instructions
- Links to relevant documentation

Common Errors
-------------

Memory Errors
~~~~~~~~~~~~~

OUT_OF_MEMORY
^^^^^^^^^^^^^

**Message:** The node is running out of memory

**Common causes:**

- Tasks or actors consuming too much memory
- Memory leaks in user code
- Insufficient cluster memory for workload
- Large objects stored in object store

**How to fix:**

1. Increase object store size: ``ray.init(object_store_memory=...)``
2. Enable object spilling: ``ray.init(_system_config={'object_spilling_config': ...})``
3. Reduce object sizes or delete references sooner with ``del``
4. Check memory usage: ``ray memory``
5. Add more nodes to the cluster or increase node memory

**See:** :ref:`memory-management`

OBJECT_STORE_FULL
^^^^^^^^^^^^^^^^^

**Message:** The local object store is full

**Common causes:**

- Too many objects in scope that cannot be evicted
- Object store memory limit too small for workload
- Objects not being freed after use

**How to fix:**

1. Use ``ray memory`` command to list active objects
2. Delete unnecessary object references with ``del``
3. Increase object store memory: ``ray.init(object_store_memory=...)``
4. Enable object spilling to disk
5. Process data in smaller batches

**See:** :ref:`memory-management`

OUT_OF_DISK
^^^^^^^^^^^

**Message:** The local disk is full

**Common causes:**

- Too many spilled objects
- Large log files accumulating
- Insufficient disk space for workload

**How to fix:**

1. Check disk usage with ``df`` command
2. Clean up old Ray sessions in ``/tmp/ray``
3. Reduce object spilling or add more disk space
4. Configure spill directory to a larger disk

**See:** :ref:`memory-management`

Scheduling Errors
~~~~~~~~~~~~~~~~~

TASK_UNSCHEDULABLE
^^^^^^^^^^^^^^^^^^

**Message:** The task cannot be scheduled

**Common causes:**

- Specified node is dead
- Required resources not available in cluster
- Placement group removed
- Invalid scheduling constraints

**How to fix:**

1. Check node status: ``ray status``
2. Review scheduling strategy configuration
3. Verify resource requirements are satisfiable
4. Check if placement group still exists

**See:** :ref:`scheduling-index`

ACTOR_UNSCHEDULABLE
^^^^^^^^^^^^^^^^^^^

**Message:** The actor cannot be scheduled

**Common causes:**

- Specified node is dead
- Required resources not available
- Placement group removed
- Invalid scheduling constraints

**How to fix:**

1. Check node status: ``ray status``
2. Review actor resource requirements
3. Verify placement constraints are satisfiable
4. Check if placement group still exists

**See:** :ref:`scheduling-index`

Worker and Actor Errors
~~~~~~~~~~~~~~~~~~~~~~~

WORKER_CRASHED
^^^^^^^^^^^^^^

**Message:** The worker died unexpectedly while executing a task

**Common causes:**

- Segmentation fault in native code or C extensions
- Memory corruption
- Out of memory killed by OS
- Unhandled signal

**How to fix:**

1. Check ``python-core-worker-*.log`` files for details
2. Review recent code changes for memory issues
3. Enable core dumps for debugging
4. Reduce memory usage in the task
5. Increase ``max_retries`` for the task

**See:** :ref:`fault-tolerance-tasks`

ACTOR_DIED_UNEXPECTEDLY
^^^^^^^^^^^^^^^^^^^^^^^

**Message:** Actor process died unexpectedly

**Common causes:**

- Segmentation fault in native code
- Out of memory killed by OS (OOM killer)
- Unhandled exception in actor
- Node failure

**How to fix:**

1. Check actor logs in ``/tmp/ray/session_latest/logs``
2. Enable core dumps for debugging: ``ulimit -c unlimited``
3. Review memory usage patterns
4. Enable actor restarts: ``@ray.remote(max_restarts=3)``
5. Check for errors in actor ``__init__`` method

**See:** :ref:`fault-tolerance-actors`

Object Errors
~~~~~~~~~~~~~

OBJECT_LOST
^^^^^^^^^^^

**Message:** Object was lost from distributed memory

**Common causes:**

- Node failure where object was stored
- Object evicted and lineage lost
- System error during object transfer

**How to fix:**

1. Enable object reconstruction with lineage: ``@ray.remote(max_retries=3)``
2. Check cluster logs for node failures
3. Increase lineage storage: ``RAY_max_lineage_bytes=...``
4. Use ``ray.put()`` with more replicas

**See:** :ref:`fault-tolerance-objects`

OWNER_DIED
^^^^^^^^^^

**Message:** The object's owner has exited

**Common causes:**

- Driver process exited while tasks still running
- Worker that created object crashed
- Object reference passed to long-running task

**How to fix:**

1. Ensure driver stays alive until all tasks complete
2. Use ``ray.get()`` to wait for results before exiting
3. Check owner worker logs for crash details
4. Consider using detached actors for long-running work

**See:** :ref:`fault-tolerance-objects`

Runtime Environment Errors
~~~~~~~~~~~~~~~~~~~~~~~~~~

RUNTIME_ENV_SETUP_ERROR
^^^^^^^^^^^^^^^^^^^^^^^

**Message:** Failed to set up runtime environment

**Common causes:**

- Invalid pip/conda packages specified
- Network issues downloading packages
- Insufficient disk space
- Invalid working directory path

**How to fix:**

1. Check package names and versions are correct
2. Verify network connectivity to package repositories
3. Check available disk space on worker nodes
4. Review ``runtime_env`` configuration syntax
5. Check worker logs for detailed error messages

**See:** :ref:`handling-dependencies`

System Errors
~~~~~~~~~~~~~

LOCAL_RAYLET_DIED
^^^^^^^^^^^^^^^^^

**Message:** The task's local raylet died

**Common causes:**

- Node failure
- Raylet crashed due to bug or resource exhaustion
- Network partition

**How to fix:**

1. Check ``raylet.out`` log for error details
2. Verify node is still in cluster: ``ray status``
3. Review system resources (memory, disk) on the node
4. Check for recent cluster configuration changes

NODE_DIED
^^^^^^^^^

**Message:** The node is either dead or unreachable

**Common causes:**

- Node hardware failure
- Network connectivity issues
- Node removed by autoscaler
- Out of memory causing system crash

**How to fix:**

1. Check cluster status: ``ray status``
2. Review autoscaler logs for scaling decisions
3. Check node health and network connectivity
4. Review system logs on the affected node

Accessing Error Information Programmatically
--------------------------------------------

Enhanced error messages include additional attributes that can be accessed programmatically:

.. code-block:: python

    try:
        ray.get(ref)
    except ray.exceptions.OutOfMemoryError as e:
        # Access remediation steps
        for step in e.remediation:
            print(f"Fix: {step}")

        # Access documentation link
        if e.doc_link:
            print(f"Documentation: {e.doc_link}")

        # Access diagnostic information
        for key, value in e.diagnostic_info.items():
            print(f"{key}: {value}")

        # Access error code
        print(f"Error code: {e.error_code}")

All enhanced exceptions inherit from ``EnhancedErrorMixin`` and provide these attributes:

- ``remediation``: List of steps to fix the error
- ``doc_link``: URL to relevant documentation
- ``diagnostic_info``: Dictionary of diagnostic information
- ``error_code``: The error code for CLI lookup

Adding Custom Diagnostics
-------------------------

When raising enhanced exceptions in your code, you can include custom diagnostic information:

.. code-block:: python

    from ray.exceptions import OutOfMemoryError

    context = {
        "used_memory": "8GB",
        "total_memory": "16GB",
        "threshold": "90%",
    }
    raise OutOfMemoryError("Memory limit exceeded", context=context)

This will include the diagnostic information in the error output, helping users understand and debug the issue.
