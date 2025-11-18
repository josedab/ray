# RFC-0002: Configuration Validation and Warnings

**Status:** Draft
**Author:** Codebase Analysis
**Created:** 2025-01-18
**Commit Reference:** `d1cce8c9dc8411fad7cfbd619350bec6f19839a3`

## Summary

Add comprehensive validation for Ray configuration options with helpful warnings for common misconfigurations, deprecated options, and performance anti-patterns.

## Motivation

### Current State

Ray has 100+ configuration options spread across:
- Environment variables (`RAY_*`)
- `ray.init()` parameters
- `_system_config` dictionary
- YAML cluster configurations

Current problems:
1. **Silent failures:** Invalid configs are often ignored
2. **No deprecation warnings:** Old configs work until they don't
3. **Performance foot-guns:** Easy to misconfigure for poor performance
4. **Type mismatches:** String "true" vs boolean True

### Evidence from Codebase

From `src/ray/common/ray_config_def.h`:

```cpp
// 280+ configuration options defined
// No validation of values
// No deprecation mechanism
```

### User Impact

- Hours debugging configuration issues
- Performance problems from misconfigurations
- Upgrade failures from deprecated options

## Detailed Design

### Configuration Schema

Define a schema for all configuration options:

```python
# ray/_private/config_schema.py

CONFIG_SCHEMA = {
    "object_store_memory": {
        "type": "int",
        "min": 0,
        "max": lambda: psutil.virtual_memory().total,
        "default": lambda: int(0.3 * psutil.virtual_memory().total),
        "description": "Bytes allocated to object store",
        "deprecated_names": ["plasma_store_memory"],
        "performance_hints": [
            {
                "condition": lambda v: v < 1e9,
                "message": "Object store < 1GB may cause frequent spilling"
            }
        ]
    },
    "num_cpus": {
        "type": "int",
        "min": 0,
        "default": lambda: os.cpu_count(),
        "description": "Number of CPUs available to Ray",
        "conflicts_with": ["resources.CPU"]
    },
    "scheduler_spread_threshold": {
        "type": "float",
        "min": 0.0,
        "max": 1.0,
        "default": 0.5,
        "description": "Threshold for hybrid scheduling policy"
    }
}
```

### Validation Engine

```python
# ray/_private/config_validator.py

class ConfigValidator:
    def __init__(self, schema: dict):
        self.schema = schema
        self.warnings = []
        self.errors = []

    def validate(self, config: dict) -> ValidationResult:
        for key, value in config.items():
            if key not in self.schema:
                self._handle_unknown(key, value)
                continue

            spec = self.schema[key]
            self._validate_type(key, value, spec)
            self._validate_range(key, value, spec)
            self._check_deprecation(key, spec)
            self._check_conflicts(key, config, spec)
            self._check_performance(key, value, spec)

        return ValidationResult(
            errors=self.errors,
            warnings=self.warnings
        )

    def _handle_unknown(self, key, value):
        # Suggest similar options
        similar = self._find_similar(key)
        if similar:
            self.warnings.append(
                f"Unknown config '{key}'. Did you mean '{similar}'?"
            )
        else:
            self.warnings.append(f"Unknown config '{key}' will be ignored")
```

### Integration Points

#### ray.init() Validation

```python
def init(
    address: Optional[str] = None,
    num_cpus: Optional[int] = None,
    **kwargs
):
    # Validate all parameters
    validator = ConfigValidator(CONFIG_SCHEMA)
    result = validator.validate(kwargs)

    # Show warnings
    for warning in result.warnings:
        logger.warning(warning)

    # Fail on errors (with helpful messages)
    if result.errors:
        error_msg = "Configuration errors:\n"
        for error in result.errors:
            error_msg += f"  - {error}\n"
        raise ValueError(error_msg)
```

#### Environment Variable Validation

```python
# On import
def _validate_environment():
    """Validate RAY_* environment variables."""
    for key, value in os.environ.items():
        if key.startswith("RAY_"):
            config_key = key[4:].lower()
            if config_key in CONFIG_SCHEMA:
                _validate_env_value(key, value, CONFIG_SCHEMA[config_key])
            else:
                logger.warning(f"Unknown environment variable: {key}")
```

### Warning Categories

#### 1. Deprecation Warnings

```python
# Old config name
ray.init(plasma_store_memory=1e9)
# WARNING: 'plasma_store_memory' is deprecated, use 'object_store_memory'
```

#### 2. Type Warnings

```python
# String instead of boolean
os.environ["RAY_USE_TLS"] = "true"
# WARNING: RAY_USE_TLS should be "1" or "0", got "true". Interpreting as True.
```

#### 3. Performance Warnings

```python
# Small object store
ray.init(object_store_memory=100_000_000)
# WARNING: object_store_memory=100MB is small. Consider at least 1GB to avoid
# frequent spilling. Current system memory: 32GB
```

#### 4. Conflict Warnings

```python
# Conflicting settings
ray.init(num_cpus=4, resources={"CPU": 8})
# WARNING: num_cpus=4 conflicts with resources.CPU=8. Using resources.CPU.
```

### CLI Validation Tool

```bash
# Validate cluster config
ray config validate cluster.yaml

# Output:
# ✓ provider: aws (valid)
# ✓ head_node_type: m5.xlarge (valid)
# ⚠ worker_node_type: t2.micro (warning: t2.micro has only 1 CPU)
# ✗ min_workers: -1 (error: must be >= 0)
```

## Example Usage

### Basic Validation

```python
import ray

# This will show helpful warnings
ray.init(
    num_cpus=4,
    object_store_memory=100_000_000,  # Warning: too small
    plasma_directory="/tmp"  # Warning: deprecated
)

# Output:
# WARNING: object_store_memory=100MB is below recommended minimum (1GB)
# WARNING: 'plasma_directory' is deprecated, use 'object_spilling_directory'
```

### Strict Mode

```python
import ray

# Fail on any warning
ray.init(
    num_cpus=4,
    _config_validation="strict"  # Warnings become errors
)
```

### Programmatic Access

```python
from ray._private.config_validator import validate_config

result = validate_config({
    "num_cpus": 4,
    "object_store_memory": 100_000_000
})

if result.has_warnings:
    for w in result.warnings:
        print(f"Warning: {w}")
```

## Implementation Plan

### Week 1: Schema Definition
- Define schema for top 50 most-used configs
- Add type information and ranges
- Document defaults

### Week 2: Validation Engine
- Implement core validation logic
- Add deprecation checking
- Add similarity matching

### Week 3: Integration
- Integrate with `ray.init()`
- Add environment variable validation
- Add CLI tool

### Week 4: Testing & Documentation
- Comprehensive tests
- Update documentation
- Migration guide

## Backwards Compatibility

This RFC is **non-breaking**:
- Invalid configs that were silently ignored now produce warnings
- Deprecated configs continue to work with warnings
- No behavior changes for valid configs

### Opt-out

```python
# Disable validation warnings
ray.init(_config_validation="off")
```

## Alternatives Considered

### Alternative 1: Hard Failures for Invalid Config

Reject invalid configurations entirely.

**Rejected because:**
- Breaks existing code
- Too disruptive for users

### Alternative 2: Configuration File Only

Move all config to files with schema validation.

**Rejected because:**
- Loses flexibility of programmatic config
- Major breaking change

## Open Questions

1. **Warning Verbosity:** How verbose should warnings be by default?
2. **Deprecation Timeline:** How long to keep deprecated configs?
3. **Telemetry:** Should we collect anonymized config patterns?

## Success Criteria

- [ ] Schema covers 100% of documented configs
- [ ] All deprecated configs produce warnings
- [ ] Performance anti-patterns detected
- [ ] 50% reduction in config-related support tickets

## Effort Estimation

- **Development:** 3-4 dev-weeks
- **Testing:** 1 dev-week
- **Documentation:** 0.5 dev-weeks
- **Total:** 4.5-5.5 dev-weeks

## References

- Configuration definitions: [`src/ray/common/ray_config_def.h`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/src/ray/common/ray_config_def.h)
- Init parameters: [`python/ray/_private/worker.py`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/python/ray/_private/worker.py)
