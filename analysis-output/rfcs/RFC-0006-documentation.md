# RFC-0006: Documentation Structure Overhaul

**Status:** Draft
**Author:** Codebase Analysis
**Created:** 2025-01-18
**Commit Reference:** `d1cce8c9dc8411fad7cfbd619350bec6f19839a3`

## Summary

Restructure Ray's documentation to improve discoverability, add learning paths, and ensure consistency across all Ray libraries.

## Motivation

### Current State

Ray has 610 documentation files, but users struggle to find information:

1. **Fragmented structure:** Each library has different organization
2. **Missing connections:** Hard to navigate between related topics
3. **Inconsistent depth:** Some areas well-documented, others sparse
4. **No learning paths:** Beginners don't know where to start

### Evidence

- Ray Core, Data, Train, Serve, Tune each have different doc structures
- Configuration options scattered across multiple pages
- Advanced topics mixed with beginner content

## Detailed Design

### Unified Documentation Structure

```
docs/
├── getting-started/
│   ├── installation.md
│   ├── quick-start.md
│   ├── core-concepts.md
│   └── learning-paths/
│       ├── ml-engineer.md
│       ├── platform-engineer.md
│       └── data-engineer.md
├── user-guide/
│   ├── ray-core/
│   │   ├── tasks.md
│   │   ├── actors.md
│   │   ├── objects.md
│   │   └── patterns/
│   ├── ray-data/
│   ├── ray-train/
│   ├── ray-serve/
│   └── ray-tune/
├── how-to/
│   ├── deployment/
│   ├── debugging/
│   ├── performance/
│   └── migration/
├── reference/
│   ├── api/
│   ├── configuration/
│   └── cli/
└── architecture/
    ├── design-docs/
    └── internals/
```

### Learning Paths

Define clear paths for different user types:

```yaml
# learning-paths/ml-engineer.md
title: ML Engineer Path
duration: 2 hours
prerequisites:
  - Python familiarity
  - Basic ML knowledge

modules:
  - title: "Getting Started with Ray"
    duration: 15 min
    content: getting-started/quick-start.md

  - title: "Distributed Training with Ray Train"
    duration: 30 min
    content: user-guide/ray-train/basics.md

  - title: "Hyperparameter Tuning"
    duration: 30 min
    content: user-guide/ray-tune/basics.md

  - title: "Model Serving"
    duration: 30 min
    content: user-guide/ray-serve/basics.md

  - title: "Putting It Together"
    duration: 15 min
    content: tutorials/ml-pipeline.md
```

### Consistent Page Structure

Every documentation page follows a template:

```markdown
# Page Title

> One-line description

## Overview
Brief introduction to the topic (2-3 sentences)

## Prerequisites
- Required knowledge
- Required setup

## Quick Start
Minimal working example

## Detailed Guide
In-depth explanation with examples

## Common Patterns
Real-world usage patterns

## Troubleshooting
Common issues and solutions

## What's Next
Links to related topics

## API Reference
Link to relevant API docs
```

### Cross-Library Navigation

Add navigation hints across libraries:

```markdown
<!-- In Ray Train docs -->
> **Related:** After training, deploy your model with [Ray Serve](../ray-serve/basics.md)
> **Related:** Optimize hyperparameters with [Ray Tune](../ray-tune/basics.md)
```

### Interactive Examples

Embed runnable examples:

```markdown
```python {run}
import ray

ray.init()

@ray.remote
def hello():
    return "Hello, Ray!"

print(ray.get(hello.remote()))
```
```

### Configuration Documentation

Centralized, searchable configuration reference:

```markdown
# Configuration Reference

## Object Store

### object_store.memory
- **Type:** int (bytes) or str ("8GB")
- **Default:** 30% of system memory
- **Environment:** `RAY_OBJECT_STORE_MEMORY`
- **Description:** Memory allocated to the object store

#### Example
```python
ray.init(object_store={"memory": "8GB"})
```

#### Recommendations
- Minimum 1GB for production
- Increase for workloads with large objects
- Enable spilling if memory limited
```

### Search Improvements

Enhance search with:
- Full-text search
- Faceted filtering (by library, by level)
- Popular searches
- "Did you mean" suggestions

## Implementation Plan

### Phase 1: Structure (Weeks 1-2)
- Define new structure
- Create templates
- Set up navigation

### Phase 2: Content Migration (Weeks 3-4)
- Migrate existing content
- Add cross-references
- Fill gaps

### Phase 3: Enhancements (Weeks 5-6)
- Learning paths
- Interactive examples
- Search improvements

### Phase 4: Maintenance (Ongoing)
- Review process
- Freshness checks
- Community contributions

## Success Criteria

- [ ] All pages follow consistent template
- [ ] Learning paths for 3+ user types
- [ ] Cross-references between all libraries
- [ ] Centralized configuration reference
- [ ] Documentation satisfaction score > 4/5

## Effort Estimation

- **Development:** 4 dev-weeks
- **Technical writing:** 6 weeks
- **Total:** 10 weeks

## References

- Current docs: [`doc/source/`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/doc/source/)
