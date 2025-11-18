# How-To Guides

> Task-oriented guides for common Ray operations.

## Overview

How-To guides provide step-by-step instructions for specific tasks. Unlike tutorials that teach concepts, these guides help you accomplish concrete goals.

## Categories

### Deployment

Deploy Ray in different environments:

- [Local Development](deployment/local.md) - Run Ray on your laptop
- [Kubernetes Deployment](deployment/kubernetes.md) - Deploy on K8s
- [Cloud Deployment](deployment/cloud.md) - Deploy on AWS/GCP/Azure
- [Production Deployment](deployment/production.md) - Production best practices

### Debugging

Troubleshoot Ray applications:

- [Debugging Tasks](debugging/tasks.md) - Debug distributed tasks
- [Debugging Actors](debugging/actors.md) - Debug stateful actors
- [Memory Issues](debugging/memory.md) - Resolve OOM errors
- [Performance Issues](debugging/performance.md) - Find bottlenecks

### Performance

Optimize Ray applications:

- [Task Performance](performance/tasks.md) - Optimize task execution
- [Memory Management](performance/memory.md) - Reduce memory usage
- [Ray Data Performance](performance/ray-data.md) - Speed up data processing
- [GPU Utilization](performance/gpu.md) - Maximize GPU usage

### Migration

Migrate from other frameworks:

- [Spark to Ray](migration/spark-to-ray.md) - Migrate Spark pipelines
- [Dask to Ray](migration/dask-to-ray.md) - Migrate Dask workflows
- [Single Machine to Ray](migration/single-to-distributed.md) - Distribute local code

## Guide Format

Each how-to guide includes:

1. **Goal** - What you'll accomplish
2. **Prerequisites** - What you need
3. **Steps** - Detailed instructions
4. **Verification** - How to confirm success
5. **Common Issues** - Troubleshooting tips

## Quick Links

### Most Popular

- [Deploy to Kubernetes](deployment/kubernetes.md)
- [Debug OOM Errors](debugging/memory.md)
- [Optimize Ray Data](performance/ray-data.md)
- [Configure Autoscaling](deployment/autoscaling.md)

### Recently Updated

- [GPU Cluster Setup](deployment/gpu-cluster.md)
- [Multi-tenancy](deployment/multi-tenancy.md)
- [Cost Optimization](deployment/cost.md)

## Related Resources

- [User Guide](../user-guide/) - Comprehensive library documentation
- [Reference](../reference/) - API and configuration reference
- [Tutorials](tutorials/) - Step-by-step learning projects
