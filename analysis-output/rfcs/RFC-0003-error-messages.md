# RFC-0003: Actionable Error Messages

**Status:** Draft
**Author:** Codebase Analysis
**Created:** 2025-01-18
**Commit Reference:** `d1cce8c9dc8411fad7cfbd619350bec6f19839a3`

## Summary

Enhance Ray error messages to include actionable remediation steps, relevant documentation links, and diagnostic information to help users resolve issues quickly.

## Motivation

### Current State

Many Ray errors provide minimal guidance:

```python
# Current
ray.exceptions.RayTaskError: Task failed with error

# Current OOM
ray.exceptions.OutOfMemoryError: Object store is out of memory
```

Users must search documentation or forums to understand:
- What caused the error
- How to fix it
- What information to gather for debugging

### Evidence from Codebase

From `python/ray/exceptions.py`:
- Exception classes exist but messages are basic
- No links to documentation
- No suggested remediation steps

### User Impact

- Increased time to resolution
- High support ticket volume for common issues
- Frustration with error messages

## Detailed Design

### Enhanced Exception Classes

```python
# ray/exceptions.py

class RayError(Exception):
    """Base class with enhanced error formatting."""

    def __init__(
        self,
        message: str,
        cause: Optional[Exception] = None,
        remediation: Optional[List[str]] = None,
        doc_link: Optional[str] = None,
        diagnostic_info: Optional[dict] = None
    ):
        self.message = message
        self.cause = cause
        self.remediation = remediation or []
        self.doc_link = doc_link
        self.diagnostic_info = diagnostic_info or {}

    def __str__(self):
        parts = [self.message]

        if self.remediation:
            parts.append("\nHow to fix:")
            for i, step in enumerate(self.remediation, 1):
                parts.append(f"  {i}. {step}")

        if self.doc_link:
            parts.append(f"\nDocumentation: {self.doc_link}")

        if self.diagnostic_info:
            parts.append("\nDiagnostic info:")
            for key, value in self.diagnostic_info.items():
                parts.append(f"  {key}: {value}")

        return "\n".join(parts)
```

### Example: OutOfMemoryError

```python
class OutOfMemoryError(RayError):
    """Enhanced OOM error with actionable guidance."""

    def __init__(self, context: dict):
        message = "Object store is out of memory"

        remediation = [
            "Increase object store size: ray.init(object_store_memory=...)",
            "Enable disk spilling in ray.init(_system_config={...})",
            "Reduce object sizes or delete references sooner",
            "Check for memory leaks with ray.util.state.list_objects()"
        ]

        doc_link = "https://docs.ray.io/en/latest/ray-core/memory-management.html"

        diagnostic_info = {
            "object_store_size": context.get("store_size"),
            "used_memory": context.get("used_memory"),
            "num_objects": context.get("num_objects"),
            "top_objects": context.get("top_objects", [])[:5]
        }

        super().__init__(
            message=message,
            remediation=remediation,
            doc_link=doc_link,
            diagnostic_info=diagnostic_info
        )
```

### Error Output Example

```
ray.exceptions.OutOfMemoryError: Object store is out of memory

How to fix:
  1. Increase object store size: ray.init(object_store_memory=...)
  2. Enable disk spilling in ray.init(_system_config={...})
  3. Reduce object sizes or delete references sooner
  4. Check for memory leaks with ray.util.state.list_objects()

Documentation: https://docs.ray.io/en/latest/ray-core/memory-management.html

Diagnostic info:
  object_store_size: 10.0 GB
  used_memory: 9.8 GB (98%)
  num_objects: 1,234
  top_objects: [('ObjectID(abc123)', '2.1 GB'), ('ObjectID(def456)', '1.8 GB')]
```

### Error Catalog

Create a catalog of common errors with remediation:

```python
# ray/_private/error_catalog.py

ERROR_CATALOG = {
    "TASK_SCHEDULING_TIMEOUT": {
        "message": "Task could not be scheduled within timeout",
        "causes": [
            "Insufficient cluster resources",
            "Resources reserved by other tasks",
            "Invalid resource requirements"
        ],
        "remediation": [
            "Check available resources: ray.cluster_resources()",
            "Add more nodes or increase resources",
            "Review task resource requirements"
        ],
        "doc_link": "https://docs.ray.io/en/latest/ray-core/scheduling.html"
    },
    "ACTOR_DIED_UNEXPECTEDLY": {
        "message": "Actor process died unexpectedly",
        "causes": [
            "Segmentation fault in native code",
            "Out of memory killed by OS",
            "Unhandled exception in actor"
        ],
        "remediation": [
            "Check actor logs: ray.util.state.get_actor(...).logs",
            "Enable core dumps for debugging",
            "Review memory usage patterns"
        ],
        "doc_link": "https://docs.ray.io/en/latest/ray-core/actors.html#fault-tolerance"
    }
}
```

### CLI Error Helper

```bash
# Get help for specific error
ray error TASK_SCHEDULING_TIMEOUT

# Output:
# TASK_SCHEDULING_TIMEOUT
# =======================
# Task could not be scheduled within timeout
#
# Common causes:
#   - Insufficient cluster resources
#   - Resources reserved by other tasks
#   ...
#
# How to fix:
#   1. Check available resources: ray.cluster_resources()
#   ...
#
# See: https://docs.ray.io/en/latest/ray-core/scheduling.html
```

## Implementation Plan

### Week 1: Framework
- Implement enhanced exception base class
- Create error catalog structure
- Add diagnostic info collection

### Week 2: Common Errors
- Enhance top 20 most common errors
- Add remediation steps
- Link to documentation

### Week 3: Integration
- Update error raising code throughout codebase
- Add CLI helper
- Create error telemetry (opt-in)

### Week 4: Documentation
- Update troubleshooting guide
- Add error reference page
- Create migration guide

## Backwards Compatibility

**Non-breaking change:**
- Exception types unchanged
- String representation enhanced
- New attributes are additive

```python
try:
    ray.get(ref)
except ray.exceptions.RayTaskError as e:
    # Old code still works
    print(str(e))

    # New attributes available
    if hasattr(e, 'remediation'):
        for step in e.remediation:
            print(step)
```

## Alternatives Considered

### Alternative 1: External Error Database

Maintain errors in separate service.

**Rejected because:**
- Requires network access
- Single point of failure
- Harder to keep in sync

### Alternative 2: Machine-Readable Errors Only

Return structured data, let tools format.

**Rejected because:**
- Poor CLI experience
- Requires tooling adoption

## Success Criteria

- [ ] Top 20 errors have remediation steps
- [ ] All errors link to relevant documentation
- [ ] 40% reduction in error-related support tickets
- [ ] User satisfaction survey improvement

## Effort Estimation

- **Development:** 3 dev-weeks
- **Documentation:** 1 dev-week
- **Testing:** 1 dev-week
- **Total:** 5 dev-weeks

## References

- Current exceptions: [`python/ray/exceptions.py`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/python/ray/exceptions.py)
