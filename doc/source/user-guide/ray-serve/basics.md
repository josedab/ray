# Ray Serve Basics

> Deploy ML models and business logic as scalable web services.

## Overview

Ray Serve is a scalable model serving library for building online inference APIs. It allows you to deploy any Python code as a web service, with features like autoscaling, batching, and model composition built-in.

## Prerequisites

- Ray installed (`pip install "ray[serve]"`)
- Basic understanding of HTTP/REST APIs
- Understanding of [Core Concepts](../../getting-started/core-concepts.md)

## Quick Start

```python
from ray import serve
import ray

ray.init()
serve.start()

@serve.deployment
class Greeter:
    def __call__(self, request):
        name = request.query_params.get("name", "World")
        return f"Hello, {name}!"

app = Greeter.bind()
serve.run(app)

# Test: curl http://localhost:8000/?name=Ray
```

## Detailed Guide

### Creating Deployments

A deployment is a class or function served over HTTP:

```python
from ray import serve

@serve.deployment
class MyDeployment:
    def __init__(self, config_value):
        self.config = config_value

    def __call__(self, request):
        return {"config": self.config, "data": "result"}

# Bind with arguments
app = MyDeployment.bind(config_value="production")
serve.run(app)
```

### Configuring Deployments

Set replicas, resources, and other options:

```python
@serve.deployment(
    num_replicas=4,
    ray_actor_options={"num_cpus": 2, "num_gpus": 0.5},
    max_ongoing_requests=100
)
class MLModel:
    def __init__(self):
        self.model = load_model()

    def __call__(self, request):
        data = request.json()
        return self.model.predict(data)
```

### Handling Requests

Access request data in different formats:

```python
@serve.deployment
class RequestHandler:
    async def __call__(self, request):
        # Query parameters
        param = request.query_params.get("key")

        # JSON body
        data = await request.json()

        # Form data
        form = await request.form()

        # Headers
        header = request.headers.get("X-Custom")

        return {"received": data}
```

### Autoscaling

Configure automatic scaling based on load:

```python
from ray.serve.config import AutoscalingConfig

@serve.deployment(
    autoscaling_config=AutoscalingConfig(
        min_replicas=1,
        max_replicas=10,
        target_ongoing_requests=5,
        upscale_delay_s=10,
        downscale_delay_s=60
    )
)
class ScalableModel:
    pass
```

### Request Batching

Batch requests for improved throughput:

```python
@serve.deployment
class BatchPredictor:
    @serve.batch(max_batch_size=32, batch_wait_timeout_s=0.1)
    async def __call__(self, requests):
        # requests is a list of requests
        inputs = [r.query_params.get("input") for r in requests]

        # Process as batch
        results = self.model.predict_batch(inputs)

        return results  # Return list matching input size
```

### Model Composition

Chain multiple deployments together:

```python
@serve.deployment
class Preprocessor:
    def process(self, data):
        return data.lower().strip()

@serve.deployment
class Model:
    def predict(self, text):
        return {"score": 0.9, "text": text}

@serve.deployment
class Pipeline:
    def __init__(self, preprocessor, model):
        self.preprocessor = preprocessor
        self.model = model

    async def __call__(self, request):
        text = await request.body()
        text = text.decode()

        # Call other deployments
        cleaned = await self.preprocessor.process.remote(text)
        result = await self.model.predict.remote(cleaned)

        return result

# Compose the pipeline
preprocessor = Preprocessor.bind()
model = Model.bind()
app = Pipeline.bind(preprocessor, model)

serve.run(app)
```

### Configuration Files

Deploy using config files:

```yaml
# serve_config.yaml
applications:
  - name: my_app
    route_prefix: /
    import_path: my_module:app
    deployments:
      - name: Model
        num_replicas: 2
        ray_actor_options:
          num_gpus: 1
```

```bash
serve deploy serve_config.yaml
```

## Common Patterns

### Pattern: GPU Model Serving

**Use case:** Serve a PyTorch model on GPU

```python
import torch
from ray import serve

@serve.deployment(
    ray_actor_options={"num_gpus": 1},
    num_replicas=2
)
class TorchModel:
    def __init__(self, model_path):
        self.model = torch.load(model_path)
        self.model.cuda()
        self.model.eval()

    async def __call__(self, request):
        data = await request.json()
        with torch.no_grad():
            input_tensor = torch.tensor(data["input"]).cuda()
            output = self.model(input_tensor)
        return {"prediction": output.cpu().tolist()}

app = TorchModel.bind("model.pt")
```

### Pattern: A/B Testing

**Use case:** Route traffic between model versions

```python
import random
from ray import serve

@serve.deployment
class ModelA:
    def predict(self, x):
        return {"model": "A", "result": x * 2}

@serve.deployment
class ModelB:
    def predict(self, x):
        return {"model": "B", "result": x * 3}

@serve.deployment
class Router:
    def __init__(self, model_a, model_b):
        self.model_a = model_a
        self.model_b = model_b

    async def __call__(self, request):
        data = await request.json()

        # Route 80% to A, 20% to B
        if random.random() < 0.8:
            return await self.model_a.predict.remote(data["x"])
        else:
            return await self.model_b.predict.remote(data["x"])

app = Router.bind(ModelA.bind(), ModelB.bind())
```

### Pattern: Streaming Responses

**Use case:** Stream results back to client

```python
from ray import serve
from starlette.responses import StreamingResponse

@serve.deployment
class StreamingModel:
    async def generate(self):
        for i in range(10):
            yield f"token_{i} "
            await asyncio.sleep(0.1)

    async def __call__(self, request):
        return StreamingResponse(
            self.generate(),
            media_type="text/plain"
        )
```

## Troubleshooting

### Issue: High Latency

**Symptoms:** Slow response times

**Cause:** Too few replicas or resources

**Solution:** Scale up or enable batching:

```python
# Increase replicas
@serve.deployment(num_replicas=4)
class Model:
    pass

# Enable batching
@serve.deployment
class Model:
    @serve.batch(max_batch_size=32)
    async def __call__(self, requests):
        pass
```

### Issue: Memory Growth

**Symptoms:** Replica memory keeps increasing

**Cause:** Memory leak in deployment code

**Solution:** Use health checks and soft restarts:

```python
@serve.deployment(
    health_check_period_s=10,
    health_check_timeout_s=30
)
class Model:
    def __init__(self):
        self.request_count = 0

    def __call__(self, request):
        self.request_count += 1
        return "ok"

    def check_health(self):
        # Fail health check to trigger restart
        if self.request_count > 10000:
            raise RuntimeError("Need restart")
```

### Issue: Request Timeouts

**Symptoms:** Client receives timeout errors

**Cause:** Processing taking too long

**Solution:** Increase timeout or optimize:

```python
# Client-side timeout
import requests
response = requests.get(url, timeout=60)

# Server-side: Use async for I/O
@serve.deployment
class Model:
    async def __call__(self, request):
        # Async I/O won't block
        result = await async_operation()
        return result
```

## What's Next

- [Configuration Guide](configuration.md) - Advanced configuration
- [Production Deployment](../../how-to/deployment/serve-production.md) - Deploy to production
- [Monitoring](monitoring.md) - Monitor deployments

> **Related:** Train models to deploy with [Ray Train](../ray-train/basics.md)
> **Related:** Tune serving performance with [Ray Tune](../ray-tune/basics.md)
> **Related:** Process batch data with [Ray Data](../ray-data/basics.md)

## API Reference

- [`@serve.deployment`](../../reference/api/ray-serve.md#deployment)
- [`serve.run`](../../reference/api/ray-serve.md#run)
- [`AutoscalingConfig`](../../reference/api/ray-serve.md#autoscaling-config)
- [`@serve.batch`](../../reference/api/ray-serve.md#batch)
