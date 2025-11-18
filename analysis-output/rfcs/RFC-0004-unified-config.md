# RFC-0004: Unified Configuration System

**Status:** Draft
**Author:** Codebase Analysis
**Created:** 2025-01-18
**Commit Reference:** `d1cce8c9dc8411fad7cfbd619350bec6f19839a3`

## Summary

Create a unified configuration system that consolidates Ray's 100+ configuration options into a single, well-documented, hierarchical configuration model with clear precedence rules.

## Motivation

### Current State

Ray configuration is fragmented across:

1. **Environment variables:** `RAY_*` (100+ options)
2. **ray.init() parameters:** Mixed documented/undocumented
3. **_system_config dictionary:** Internal options exposed
4. **Cluster YAML:** Autoscaler configuration
5. **C++ RayConfig:** Compile-time defaults

Problems:
- No single source of truth
- Inconsistent naming (snake_case vs camelCase)
- Unclear precedence rules
- Hard to audit current configuration

### Evidence from Codebase

From [`src/ray/common/ray_config_def.h`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/src/ray/common/ray_config_def.h):

```cpp
// 280+ configs defined with various naming conventions
RAY_CONFIG(int64_t, object_store_memory, ...)
RAY_CONFIG(int, numCpus, ...)  // Inconsistent naming
```

## Detailed Design

### Hierarchical Configuration Model

```python
# ray/_private/config.py

@dataclass
class RayConfig:
    """Unified Ray configuration."""

    # Cluster resources
    resources: ResourceConfig = field(default_factory=ResourceConfig)

    # Object store
    object_store: ObjectStoreConfig = field(default_factory=ObjectStoreConfig)

    # Scheduling
    scheduling: SchedulingConfig = field(default_factory=SchedulingConfig)

    # Security
    security: SecurityConfig = field(default_factory=SecurityConfig)

    # Observability
    observability: ObservabilityConfig = field(default_factory=ObservabilityConfig)

@dataclass
class ObjectStoreConfig:
    """Object store configuration."""
    memory_bytes: int = field(
        default_factory=lambda: int(0.3 * psutil.virtual_memory().total)
    )
    spilling_enabled: bool = True
    spilling_directory: str = "/tmp/ray_spill"
    eviction_threshold: float = 0.8
```

### Configuration Sources (Priority Order)

1. **Programmatic** (`ray.init()`) - Highest priority
2. **Environment variables** (`RAY_*`)
3. **Configuration file** (`~/.ray/config.yaml` or `RAY_CONFIG_FILE`)
4. **Defaults**

```python
# Configuration loading
class ConfigLoader:
    def load(self) -> RayConfig:
        config = RayConfig()  # Defaults

        # Load from file
        config_file = os.environ.get("RAY_CONFIG_FILE", "~/.ray/config.yaml")
        if os.path.exists(config_file):
            file_config = yaml.safe_load(open(config_file))
            config = self._merge(config, file_config)

        # Load from environment
        env_config = self._load_from_env()
        config = self._merge(config, env_config)

        return config
```

### Configuration File Format

```yaml
# ~/.ray/config.yaml

resources:
  num_cpus: 4
  num_gpus: 1
  memory: 16GB

object_store:
  memory: 8GB
  spilling:
    enabled: true
    directory: /mnt/ssd/ray_spill
  eviction_threshold: 0.8

scheduling:
  spread_threshold: 0.5
  worker_lease_timeout: 10s

security:
  tls:
    enabled: true
    cert_path: /etc/ray/certs/server.crt
    key_path: /etc/ray/certs/server.key
  authentication:
    mode: token
    token_path: /etc/ray/auth_token

observability:
  metrics:
    enabled: true
    export_port: 8080
  logging:
    level: INFO
    format: json
```

### Environment Variable Mapping

```python
# Structured environment variable names
RAY_RESOURCES_NUM_CPUS=4
RAY_OBJECT_STORE_MEMORY=8GB
RAY_SCHEDULING_SPREAD_THRESHOLD=0.5
RAY_SECURITY_TLS_ENABLED=true
```

### Programmatic API

```python
from ray.config import RayConfig, ObjectStoreConfig

# Full configuration object
config = RayConfig(
    object_store=ObjectStoreConfig(
        memory_bytes=8 * 1024**3,
        spilling_enabled=True
    )
)

ray.init(config=config)

# Or partial override
ray.init(
    object_store={"memory": "8GB"}
)
```

### Configuration Introspection

```python
# Get current configuration
current = ray.get_config()
print(current.object_store.memory_bytes)

# Dump full configuration
print(ray.get_config().to_yaml())

# Compare with defaults
diff = ray.get_config().diff_from_defaults()
for path, (default, current) in diff.items():
    print(f"{path}: {default} -> {current}")
```

### CLI Tools

```bash
# Show current configuration
ray config show

# Show specific section
ray config show object_store

# Validate configuration file
ray config validate ~/.ray/config.yaml

# Generate default config
ray config generate > config.yaml

# Show environment variable mappings
ray config env-vars
```

## Example Usage

### Development Setup

```yaml
# dev-config.yaml
resources:
  num_cpus: 4

object_store:
  memory: 2GB

security:
  development_mode: true  # Disables TLS/auth

observability:
  logging:
    level: DEBUG
```

```python
ray.init(config_file="dev-config.yaml")
```

### Production Setup

```yaml
# prod-config.yaml
object_store:
  memory: 50GB
  spilling:
    enabled: true
    directory: /mnt/nvme/ray_spill

security:
  tls:
    enabled: true
    cert_path: /etc/ray/certs/server.crt
  authentication:
    mode: token

observability:
  metrics:
    enabled: true
    labels:
      environment: production
      cluster: us-west-2
```

## Implementation Plan

### Phase 1: Schema & Loading (Weeks 1-2)
- Define configuration dataclasses
- Implement YAML loading
- Environment variable mapping

### Phase 2: Integration (Weeks 3-4)
- Wire into ray.init()
- Replace _system_config usage
- Update C++ config access

### Phase 3: Tooling (Week 5)
- CLI tools
- Documentation generation
- Migration utilities

### Phase 4: Migration (Week 6)
- Deprecation warnings for old style
- Migration guide
- Update examples

## Backwards Compatibility

### Transition Period

```python
# Old style (deprecated with warning)
ray.init(
    num_cpus=4,
    _system_config={"object_store_memory": 8e9}
)
# WARNING: Deprecated configuration style. See migration guide.

# New style
ray.init(
    resources={"num_cpus": 4},
    object_store={"memory": "8GB"}
)
```

### Compatibility Layer

```python
def init(
    # Old parameters (deprecated)
    num_cpus=None,
    num_gpus=None,
    _system_config=None,

    # New configuration
    config=None,
    **config_overrides
):
    if num_cpus or num_gpus or _system_config:
        warnings.warn("Deprecated config style", DeprecationWarning)
        config = _convert_legacy_config(...)
```

## Alternatives Considered

### Alternative 1: Keep Current System

Status quo with better documentation.

**Rejected because:**
- Doesn't solve fragmentation
- Can't enforce consistency

### Alternative 2: Single Flat Namespace

All configs at one level.

**Rejected because:**
- 100+ options at one level is unwieldy
- Hard to organize documentation

## Open Questions

1. **Format:** Should we support TOML in addition to YAML?
2. **Secrets:** How to handle sensitive configuration?
3. **Cluster vs Node:** Which configs are cluster-wide vs per-node?

## Success Criteria

- [ ] Single configuration file can configure Ray completely
- [ ] All configs documented with schema
- [ ] Clear precedence rules
- [ ] Migration tools for existing configs
- [ ] IDE autocomplete support

## Effort Estimation

- **Development:** 6 dev-weeks
- **Testing:** 2 dev-weeks
- **Documentation:** 2 dev-weeks
- **Total:** 10 dev-weeks

## References

- Current config: [`src/ray/common/ray_config_def.h`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/src/ray/common/ray_config_def.h)
- Init parameters: [`python/ray/_private/worker.py`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/python/ray/_private/worker.py)
