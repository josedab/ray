# Architecture

> Understand Ray's internal architecture and design decisions.

## Overview

This section provides technical deep-dives into Ray's architecture. It's intended for contributors, advanced users who want to understand system behavior, and anyone curious about how Ray works under the hood.

## Contents

### Design Documents

Design documents explain the rationale behind major architectural decisions:

- [Task Scheduling](design-docs/scheduling.md) - How Ray schedules tasks
- [Object Management](design-docs/object-store.md) - The distributed object store
- [GCS Architecture](design-docs/gcs.md) - Global Control Store design
- [Actor Lifecycle](design-docs/actors.md) - Actor creation and management

### Internals

Detailed explanations of Ray components:

- [System Architecture](internals/architecture.md) - Overall system design
- [Raylet](internals/raylet.md) - Node manager component
- [Plasma Store](internals/plasma.md) - Object store implementation
- [Serialization](internals/serialization.md) - Object serialization

## Ray Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                       Ray Cluster                           │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────────┐         ┌──────────────────────────┐  │
│  │    Head Node     │         │     Worker Nodes         │  │
│  │                  │         │                          │  │
│  │  ┌────────────┐  │         │  ┌────────┐  ┌────────┐  │  │
│  │  │    GCS     │  │         │  │ Raylet │  │ Raylet │  │  │
│  │  └────────────┘  │         │  └────────┘  └────────┘  │  │
│  │                  │         │                          │  │
│  │  ┌────────────┐  │         │  ┌────────┐  ┌────────┐  │  │
│  │  │   Raylet   │  │         │  │  Obj   │  │  Obj   │  │  │
│  │  └────────────┘  │         │  │ Store  │  │ Store  │  │  │
│  │                  │         │  └────────┘  └────────┘  │  │
│  │  ┌────────────┐  │         │                          │  │
│  │  │  Obj Store │  │         │  ┌────────┐  ┌────────┐  │  │
│  │  └────────────┘  │         │  │Workers │  │Workers │  │  │
│  │                  │         │  └────────┘  └────────┘  │  │
│  └──────────────────┘         └──────────────────────────┘  │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Key Components

### Global Control Store (GCS)

The GCS is the central metadata store for the Ray cluster:

- Actor registry
- Placement group state
- Job information
- Node management

### Raylet

Each node runs a Raylet that handles:

- Local scheduling
- Object management
- Resource management
- Worker processes

### Object Store

The distributed object store (based on Plasma):

- Shared memory for fast access
- Zero-copy sharing between processes
- Distributed reference counting
- Automatic spilling to disk

### Workers

Python/Java/C++ processes that execute:

- Tasks
- Actor methods
- Driver programs

## Design Principles

1. **Simplicity** - Simple API for complex distributed computing
2. **Performance** - Low overhead, high throughput
3. **Flexibility** - Support diverse workloads
4. **Fault Tolerance** - Recover from failures gracefully

## Related Resources

- [Contributing Guide](../ray-contribute/)
- [Performance Tuning](../how-to/performance/)
- [Research Papers](https://www.ray.io/research)
