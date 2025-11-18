# Ray Data Basics

> Process and transform large datasets at scale.

## Overview

Ray Data is a scalable data processing library designed for ML workloads. It provides a familiar pandas-like API while automatically handling distributed execution, memory management, and integration with other Ray libraries.

## Prerequisites

- Ray installed (`pip install "ray[data]"`)
- Familiarity with pandas
- Understanding of [Core Concepts](../../getting-started/core-concepts.md)

## Quick Start

```python
import ray

# Create a dataset
ds = ray.data.range(10000)

# Transform
ds = ds.map(lambda x: x * 2)
ds = ds.filter(lambda x: x > 100)

# Consume
results = ds.take(10)
print(results)
```

## Detailed Guide

### Creating Datasets

Create from various sources:

```python
import ray

# From Python objects
ds = ray.data.from_items([1, 2, 3, 4, 5])

# From range
ds = ray.data.range(1000)

# From pandas
import pandas as pd
df = pd.DataFrame({"col": [1, 2, 3]})
ds = ray.data.from_pandas(df)

# From files
ds = ray.data.read_parquet("s3://bucket/data/")
ds = ray.data.read_csv("data/*.csv")
ds = ray.data.read_json("logs/*.jsonl")
```

### Transformations

Apply transformations to your data:

```python
# Map: Transform each row
ds = ds.map(lambda row: {"value": row["x"] * 2})

# Filter: Keep matching rows
ds = ds.filter(lambda row: row["value"] > 0)

# Flat map: One-to-many transformation
ds = ds.flat_map(lambda row: [row, row])
```

### Batch Transformations

Process data in batches for efficiency:

```python
import pandas as pd

def process_batch(batch: pd.DataFrame) -> pd.DataFrame:
    batch["normalized"] = (batch["value"] - batch["value"].mean()) / batch["value"].std()
    return batch

# Apply to batches
ds = ds.map_batches(process_batch, batch_format="pandas")

# Control batch size
ds = ds.map_batches(process_batch, batch_size=1000)
```

### Aggregations

Compute aggregate statistics:

```python
# Global aggregations
count = ds.count()
mean = ds.mean("value")
stats = ds.aggregate(
    ray.data.aggregate.Mean("value"),
    ray.data.aggregate.Max("value"),
    ray.data.aggregate.Min("value")
)

# Grouped aggregations
grouped = ds.groupby("category").mean("value")
```

### Writing Data

Write to various formats:

```python
# Write Parquet
ds.write_parquet("output/")

# Write with partitioning
ds.write_parquet(
    "output/",
    partition_cols=["year", "month"]
)

# Write CSV
ds.write_csv("output/")

# Write JSON
ds.write_json("output/")
```

### Consuming Data

Multiple ways to consume data:

```python
# Take N rows
rows = ds.take(10)

# Iterate all rows
for row in ds.iter_rows():
    process(row)

# Iterate batches
for batch in ds.iter_batches(batch_size=100):
    process_batch(batch)

# Convert to pandas
df = ds.to_pandas()

# Convert to Arrow
table = ds.to_arrow()
```

## Common Patterns

### Pattern: ML Preprocessing Pipeline

**Use case:** Prepare data for model training

```python
import ray
import pandas as pd
from sklearn.preprocessing import StandardScaler

# Read raw data
ds = ray.data.read_parquet("raw_features/")

# Clean
ds = ds.filter(lambda row: row["value"] is not None)

# Feature engineering
def add_features(batch: pd.DataFrame) -> pd.DataFrame:
    batch["log_value"] = np.log1p(batch["value"])
    batch["value_squared"] = batch["value"] ** 2
    return batch

ds = ds.map_batches(add_features, batch_format="pandas")

# Normalize
scaler = StandardScaler()

def normalize(batch: pd.DataFrame) -> pd.DataFrame:
    batch[["value", "log_value"]] = scaler.fit_transform(
        batch[["value", "log_value"]]
    )
    return batch

ds = ds.map_batches(normalize, batch_format="pandas")

# Split for training
train_ds, test_ds = ds.train_test_split(test_size=0.2)
```

### Pattern: Batch Inference

**Use case:** Run predictions on large dataset

```python
import torch

class TorchPredictor:
    def __init__(self):
        self.model = torch.load("model.pt")
        self.model.eval()

    def __call__(self, batch: pd.DataFrame) -> pd.DataFrame:
        with torch.no_grad():
            inputs = torch.tensor(batch[["f1", "f2"]].values).float()
            predictions = self.model(inputs).numpy()
        batch["prediction"] = predictions
        return batch

# Run inference
ds = ray.data.read_parquet("data/")
ds = ds.map_batches(
    TorchPredictor,
    batch_format="pandas",
    num_gpus=1,
    concurrency=4
)
ds.write_parquet("predictions/")
```

### Pattern: ETL Pipeline

**Use case:** Extract, transform, load workflow

```python
# Extract
events = ray.data.read_parquet("s3://bucket/events/")
users = ray.data.read_parquet("s3://bucket/users/")

# Transform
def clean_events(batch: pd.DataFrame) -> pd.DataFrame:
    batch = batch.dropna(subset=["user_id"])
    batch["date"] = pd.to_datetime(batch["timestamp"]).dt.date
    return batch

events = events.map_batches(clean_events, batch_format="pandas")

# Aggregate
daily = events.groupby(["date", "event_type"]).count()

# Load
daily.write_parquet("s3://bucket/processed/")
```

## Troubleshooting

### Issue: Out of Memory

**Symptoms:** `OutOfMemoryError` or OOM killed

**Cause:** Dataset too large for memory

**Solution:** Use streaming or limit memory:

```python
# Stream processing
for batch in ds.iter_batches(batch_size=1000):
    process(batch)

# Limit object store memory
ctx = ray.data.DataContext.get_current()
ctx.execution_options.resource_limits.object_store_memory = 10e9
```

### Issue: Slow Performance

**Symptoms:** Transformations take too long

**Cause:** Using row-wise operations instead of batch

**Solution:** Use `map_batches` instead of `map`:

```python
# Slow: Row-wise
ds = ds.map(lambda row: {"x": row["x"] * 2})

# Fast: Batch-wise
def batch_transform(batch):
    batch["x"] = batch["x"] * 2
    return batch

ds = ds.map_batches(batch_transform, batch_format="pandas")
```

### Issue: Schema Mismatch

**Symptoms:** `TypeError` or unexpected columns

**Cause:** Transformation returns different schema

**Solution:** Ensure consistent schema:

```python
def transform(batch: pd.DataFrame) -> pd.DataFrame:
    # Always return same columns
    result = batch.copy()
    result["new_col"] = compute(batch)
    return result[["col1", "col2", "new_col"]]
```

## What's Next

- [Transformations Guide](transformations.md) - All transformation operations
- [I/O Guide](io.md) - Reading and writing data
- [Performance](../../how-to/performance/ray-data.md) - Optimization tips

> **Related:** Train models on your data with [Ray Train](../ray-train/basics.md)
> **Related:** Serve models for inference with [Ray Serve](../ray-serve/basics.md)
> **Related:** Tune preprocessing with [Ray Tune](../ray-tune/basics.md)

## API Reference

- [`ray.data.read_parquet`](../../reference/api/ray-data.md#read-parquet)
- [`Dataset.map_batches`](../../reference/api/ray-data.md#map-batches)
- [`Dataset.write_parquet`](../../reference/api/ray-data.md#write-parquet)
