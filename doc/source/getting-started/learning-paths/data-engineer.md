# Data Engineer Learning Path

> Scale data processing pipelines with Ray Data.

---

**Duration:** 2 hours
**Difficulty:** Intermediate

---

## Prerequisites

- Python data processing experience (pandas, NumPy)
- SQL basics
- Understanding of ETL concepts
- Familiarity with file formats (Parquet, CSV, JSON)

## Overview

This learning path teaches you how to build scalable data pipelines with Ray Data. You'll learn to process large datasets, integrate with ML workflows, and optimize performance—all while using familiar pandas-like APIs.

## Modules

### Module 1: Introduction to Ray Data

**Duration:** 20 minutes

Understand Ray Data's architecture and use cases.

#### Topics Covered
- When to use Ray Data vs Spark/Dask
- Dataset abstraction
- Lazy execution model
- Block architecture

#### Resources
- [Ray Data Overview](../../user-guide/ray-data/basics.md)
- [Core Concepts](../core-concepts.md)

#### Key Concepts

Ray Data is designed for:
- ML preprocessing pipelines
- Batch inference
- Data loading for training
- ETL with Python UDFs

```python
import ray

# Create a dataset
ds = ray.data.range(10000)

# Lazy transformations
ds = ds.map(lambda x: x * 2)
ds = ds.filter(lambda x: x > 100)

# Execute and materialize
results = ds.take(10)
```

#### Checkpoint
You should be able to:
- [ ] Explain when to use Ray Data
- [ ] Understand lazy vs eager execution
- [ ] Create basic datasets

---

### Module 2: Reading and Writing Data

**Duration:** 25 minutes

Load data from various sources and write to different formats.

#### Topics Covered
- File formats (Parquet, CSV, JSON)
- Cloud storage (S3, GCS, Azure)
- Databases and custom sources
- Partitioned writes

#### Resources
- [Data Input/Output](../../user-guide/ray-data/io.md)
- [Format Reference](../../reference/api/ray-data.md#io)

#### Hands-on Exercise

Read from various sources:

```python
import ray

# Read Parquet files
ds = ray.data.read_parquet("s3://bucket/data/")

# Read CSV with schema
ds = ray.data.read_csv(
    "data/*.csv",
    schema={
        "id": int,
        "value": float,
        "name": str
    }
)

# Read from multiple sources
ds = ray.data.read_parquet([
    "s3://bucket/2023/",
    "s3://bucket/2024/"
])

# Read JSON lines
ds = ray.data.read_json("logs/*.jsonl")
```

Write to various formats:

```python
# Write Parquet
ds.write_parquet("output/parquet/")

# Write CSV
ds.write_csv("output/csv/")

# Write with partitioning
ds.write_parquet(
    "output/partitioned/",
    partition_cols=["year", "month"]
)
```

#### Checkpoint
You should be able to:
- [ ] Read data from files and cloud storage
- [ ] Write data in multiple formats
- [ ] Handle partitioned data

---

### Module 3: Transformations

**Duration:** 30 minutes

Apply transformations to datasets at scale.

#### Topics Covered
- Map and filter operations
- Batch transformations
- Aggregations
- Shuffling and groupby

#### Resources
- [Transformations Guide](../../user-guide/ray-data/transformations.md)
- [Performance Tips](../../how-to/performance/ray-data.md)

#### Hands-on Exercise

Apply transformations:

```python
import ray
import pandas as pd

ds = ray.data.read_parquet("data/")

# Row-wise transformation
ds = ds.map(lambda row: {
    **row,
    "normalized": row["value"] / 100
})

# Filter rows
ds = ds.filter(lambda row: row["value"] > 0)

# Batch transformation (more efficient)
def process_batch(batch: pd.DataFrame) -> pd.DataFrame:
    batch["doubled"] = batch["value"] * 2
    batch["log_value"] = np.log1p(batch["value"])
    return batch

ds = ds.map_batches(process_batch, batch_format="pandas")
```

Aggregations:

```python
# Global aggregation
count = ds.count()
mean = ds.mean("value")
stats = ds.aggregate(
    ray.data.aggregate.Mean("value"),
    ray.data.aggregate.Std("value"),
    ray.data.aggregate.Max("value")
)

# Grouped aggregation
grouped_stats = ds.groupby("category").mean("value")
```

Join datasets:

```python
# Read two datasets
users = ray.data.read_parquet("users/")
orders = ray.data.read_parquet("orders/")

# Note: For complex joins, consider using pandas UDFs
# Ray Data is optimized for ML preprocessing pipelines
```

#### Checkpoint
You should be able to:
- [ ] Apply map and filter operations
- [ ] Use batch transformations
- [ ] Perform aggregations

---

### Module 4: ML Data Preprocessing

**Duration:** 25 minutes

Prepare data for machine learning workflows.

#### Topics Covered
- Feature engineering
- Data splitting
- Batch inference
- Integration with Ray Train

#### Resources
- [ML Preprocessing](../../user-guide/ray-data/ml-preprocessing.md)
- [Ray Train Integration](../../user-guide/ray-train/data-loading.md)

#### Hands-on Exercise

Feature engineering:

```python
import ray
import pandas as pd
from sklearn.preprocessing import StandardScaler

# Read raw data
ds = ray.data.read_parquet("features/")

# Compute statistics
stats = ds.aggregate(
    ray.data.aggregate.Mean("feature_1"),
    ray.data.aggregate.Std("feature_1")
)

# Normalize features
class Preprocessor:
    def __init__(self):
        self.scaler = StandardScaler()
        self.fitted = False

    def __call__(self, batch: pd.DataFrame) -> pd.DataFrame:
        if not self.fitted:
            self.scaler.fit(batch[["feature_1", "feature_2"]])
            self.fitted = True
        batch[["feature_1", "feature_2"]] = self.scaler.transform(
            batch[["feature_1", "feature_2"]]
        )
        return batch

ds = ds.map_batches(Preprocessor, batch_format="pandas")
```

Split data for training:

```python
# Random split
train_ds, test_ds = ds.train_test_split(test_size=0.2)

# Stratified split (using random split per group)
# For true stratified split, implement custom logic
```

Batch inference:

```python
import torch

class TorchPredictor:
    def __init__(self):
        self.model = torch.load("model.pt")
        self.model.eval()

    def __call__(self, batch: pd.DataFrame) -> pd.DataFrame:
        with torch.no_grad():
            inputs = torch.tensor(batch[["f1", "f2"]].values)
            predictions = self.model(inputs).numpy()
        batch["prediction"] = predictions
        return batch

ds = ds.map_batches(
    TorchPredictor,
    batch_format="pandas",
    num_gpus=1,
    concurrency=4
)
```

#### Checkpoint
You should be able to:
- [ ] Build preprocessing pipelines
- [ ] Split datasets for ML
- [ ] Run batch inference

---

### Module 5: Performance Optimization

**Duration:** 20 minutes

Optimize Ray Data pipelines for production.

#### Topics Covered
- Parallelism tuning
- Memory management
- Caching and materialization
- Profiling

#### Resources
- [Performance Guide](../../how-to/performance/ray-data.md)
- [Memory Management](../../user-guide/ray-data/memory.md)

#### Hands-on Exercise

Tune parallelism:

```python
# Control read parallelism
ds = ray.data.read_parquet(
    "data/",
    parallelism=200  # Number of blocks
)

# Control map parallelism
ds = ds.map_batches(
    process_fn,
    num_cpus=2,
    concurrency=10  # Number of concurrent tasks
)
```

Memory management:

```python
# Limit memory usage
ctx = ray.data.DataContext.get_current()
ctx.execution_options.resource_limits.object_store_memory = 10e9

# Stream processing (avoid materializing entire dataset)
for batch in ds.iter_batches(batch_size=1000):
    process(batch)
```

Caching:

```python
# Cache intermediate results
ds = ray.data.read_parquet("data/")
ds = ds.map_batches(expensive_transform)

# Materialize to cache
ds = ds.materialize()

# Now can iterate multiple times efficiently
for epoch in range(10):
    for batch in ds.iter_batches():
        train(batch)
```

Profile execution:

```python
# Enable stats
ds = ray.data.read_parquet("data/")
ds = ds.map_batches(transform)
ds.materialize()

# Print stats
print(ds.stats())
```

#### Checkpoint
You should be able to:
- [ ] Tune parallelism for your workload
- [ ] Manage memory effectively
- [ ] Profile and optimize pipelines

---

## Real-World Pipeline Example

Complete ETL pipeline:

```python
import ray
import pandas as pd
from datetime import datetime

# Initialize
ray.init()

# Extract: Read from multiple sources
events = ray.data.read_parquet("s3://bucket/events/")
users = ray.data.read_parquet("s3://bucket/users/")

# Transform: Clean and enrich data
def clean_events(batch: pd.DataFrame) -> pd.DataFrame:
    # Remove nulls
    batch = batch.dropna(subset=["user_id", "timestamp"])
    # Parse timestamps
    batch["date"] = pd.to_datetime(batch["timestamp"]).dt.date
    # Add processing metadata
    batch["processed_at"] = datetime.utcnow()
    return batch

events = events.map_batches(clean_events, batch_format="pandas")

# Aggregate: Compute daily metrics
daily_metrics = (
    events
    .groupby(["date", "event_type"])
    .count()
)

# Load: Write to data warehouse
daily_metrics.write_parquet(
    "s3://bucket/processed/daily_metrics/",
    partition_cols=["date"]
)

print(f"Processed {events.count()} events")
print(daily_metrics.stats())
```

---

## Next Steps

### Advanced Topics
- [Custom Data Sources](../../user-guide/ray-data/custom-sources.md)
- [Streaming Ingestion](../../user-guide/ray-data/streaming.md)
- [Integration with Spark](../../how-to/migration/spark-to-ray.md)

### Related Learning Paths
- [ML Engineer Path](ml-engineer.md) - Use processed data for ML
- [Platform Engineer Path](platform-engineer.md) - Deploy data pipelines

### Community
- [Ray Slack #data channel](https://ray-distributed.slack.com)
- [GitHub Discussions](https://github.com/ray-project/ray/discussions)

## Summary

| Module | Duration | Key Skills |
|--------|----------|------------|
| Introduction | 20 min | Ray Data fundamentals |
| I/O | 25 min | Reading/writing data |
| Transformations | 30 min | Data processing |
| ML Preprocessing | 25 min | Feature engineering |
| Optimization | 20 min | Performance tuning |

**Total Time:** ~2 hours
