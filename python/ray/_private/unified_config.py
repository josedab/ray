# Copyright 2017 The Ray Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Unified Configuration System for Ray.

This module provides a hierarchical configuration model that consolidates
Ray's configuration options into a single, well-documented system with
clear precedence rules.

Configuration Sources (Priority Order):
1. Programmatic (ray.init()) - Highest priority
2. Environment variables (RAY_*)
3. Configuration file (~/.ray/config.yaml or RAY_CONFIG_FILE)
4. Defaults

Example usage:
    from ray._private.unified_config import RayConfig, ConfigLoader

    # Load configuration from all sources
    config = ConfigLoader().load()

    # Or create a custom configuration
    config = RayConfig(
        object_store=ObjectStoreConfig(
            memory_bytes=8 * 1024**3,
            spilling_enabled=True
        )
    )

    ray.init(config=config)
"""

from dataclasses import dataclass, field, fields, asdict
from typing import Any, Dict, List, Optional, Tuple, Union
import os
import warnings
import copy

import yaml

try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False


def _default_object_store_memory():
    """Calculate default object store memory (30% of total memory)."""
    if HAS_PSUTIL:
        return int(0.3 * psutil.virtual_memory().total)
    return 2 * 1024**3  # Default to 2GB if psutil not available


def _parse_memory_string(value: Union[str, int, float]) -> int:
    """Parse memory string like '8GB' to bytes."""
    if isinstance(value, (int, float)):
        return int(value)

    value = value.strip().upper()
    units = {
        'B': 1,
        'KB': 1024,
        'MB': 1024**2,
        'GB': 1024**3,
        'TB': 1024**4,
    }

    for unit, multiplier in units.items():
        if value.endswith(unit):
            number = value[:-len(unit)].strip()
            return int(float(number) * multiplier)

    # Try parsing as plain number
    return int(float(value))


def _parse_duration_string(value: Union[str, int, float]) -> float:
    """Parse duration string like '10s' or '5m' to seconds."""
    if isinstance(value, (int, float)):
        return float(value)

    value = value.strip().lower()
    units = {
        'ms': 0.001,
        's': 1,
        'm': 60,
        'h': 3600,
    }

    for unit, multiplier in units.items():
        if value.endswith(unit):
            number = value[:-len(unit)].strip()
            return float(number) * multiplier

    return float(value)


@dataclass
class TLSConfig:
    """TLS/SSL configuration for secure communication."""
    enabled: bool = False
    cert_path: Optional[str] = None
    key_path: Optional[str] = None
    ca_cert_path: Optional[str] = None

    def validate(self) -> List[str]:
        """Validate the TLS configuration."""
        errors = []
        if self.enabled:
            if not self.cert_path:
                errors.append("TLS enabled but cert_path not specified")
            elif not os.path.exists(os.path.expanduser(self.cert_path)):
                errors.append(f"TLS cert_path does not exist: {self.cert_path}")

            if not self.key_path:
                errors.append("TLS enabled but key_path not specified")
            elif not os.path.exists(os.path.expanduser(self.key_path)):
                errors.append(f"TLS key_path does not exist: {self.key_path}")
        return errors


@dataclass
class AuthenticationConfig:
    """Authentication configuration."""
    mode: str = "disabled"  # "disabled", "token"
    token_path: Optional[str] = None

    def validate(self) -> List[str]:
        """Validate the authentication configuration."""
        errors = []
        valid_modes = ["disabled", "token"]
        if self.mode not in valid_modes:
            errors.append(f"Invalid authentication mode: {self.mode}. "
                         f"Must be one of: {valid_modes}")

        if self.mode == "token" and self.token_path:
            if not os.path.exists(os.path.expanduser(self.token_path)):
                errors.append(f"Token path does not exist: {self.token_path}")
        return errors


@dataclass
class SecurityConfig:
    """Security configuration for Ray cluster."""
    tls: TLSConfig = field(default_factory=TLSConfig)
    authentication: AuthenticationConfig = field(default_factory=AuthenticationConfig)
    development_mode: bool = False  # Disables TLS/auth for development

    def validate(self) -> List[str]:
        """Validate the security configuration."""
        errors = []
        if not self.development_mode:
            errors.extend(self.tls.validate())
            errors.extend(self.authentication.validate())
        return errors


@dataclass
class SpillingConfig:
    """Object spilling configuration."""
    enabled: bool = True
    directory: str = "/tmp/ray_spill"
    max_objects: Optional[int] = None
    max_bytes: Optional[int] = None


@dataclass
class ObjectStoreConfig:
    """Object store configuration."""
    memory_bytes: int = field(default_factory=_default_object_store_memory)
    spilling: SpillingConfig = field(default_factory=SpillingConfig)
    eviction_threshold: float = 0.8

    def __post_init__(self):
        """Handle memory string conversion."""
        if isinstance(self.memory_bytes, str):
            self.memory_bytes = _parse_memory_string(self.memory_bytes)

    def validate(self) -> List[str]:
        """Validate the object store configuration."""
        errors = []
        if self.memory_bytes <= 0:
            errors.append(f"object_store.memory_bytes must be positive, "
                         f"got {self.memory_bytes}")
        if not 0 < self.eviction_threshold <= 1:
            errors.append(f"object_store.eviction_threshold must be in (0, 1], "
                         f"got {self.eviction_threshold}")
        return errors


@dataclass
class ResourceConfig:
    """Resource configuration for Ray nodes."""
    num_cpus: Optional[int] = None  # None means auto-detect
    num_gpus: Optional[int] = None  # None means auto-detect
    memory: Optional[int] = None  # Memory in bytes
    custom: Dict[str, float] = field(default_factory=dict)

    def __post_init__(self):
        """Handle memory string conversion."""
        if isinstance(self.memory, str):
            self.memory = _parse_memory_string(self.memory)

    def validate(self) -> List[str]:
        """Validate the resource configuration."""
        errors = []
        if self.num_cpus is not None and self.num_cpus < 0:
            errors.append(f"resources.num_cpus must be non-negative, "
                         f"got {self.num_cpus}")
        if self.num_gpus is not None and self.num_gpus < 0:
            errors.append(f"resources.num_gpus must be non-negative, "
                         f"got {self.num_gpus}")
        if self.memory is not None and self.memory < 0:
            errors.append(f"resources.memory must be non-negative, "
                         f"got {self.memory}")
        return errors


@dataclass
class SchedulingConfig:
    """Scheduling configuration."""
    spread_threshold: float = 0.5
    worker_lease_timeout_seconds: float = 10.0
    top_k_fraction: float = 0.2
    top_k_absolute: int = 1

    def __post_init__(self):
        """Handle duration string conversion."""
        if isinstance(self.worker_lease_timeout_seconds, str):
            self.worker_lease_timeout_seconds = _parse_duration_string(
                self.worker_lease_timeout_seconds
            )

    def validate(self) -> List[str]:
        """Validate the scheduling configuration."""
        errors = []
        if not 0 <= self.spread_threshold <= 1:
            errors.append(f"scheduling.spread_threshold must be in [0, 1], "
                         f"got {self.spread_threshold}")
        if self.worker_lease_timeout_seconds <= 0:
            errors.append(f"scheduling.worker_lease_timeout_seconds must be positive, "
                         f"got {self.worker_lease_timeout_seconds}")
        return errors


@dataclass
class MetricsConfig:
    """Metrics export configuration."""
    enabled: bool = True
    export_port: int = 8080
    labels: Dict[str, str] = field(default_factory=dict)


@dataclass
class LoggingConfig:
    """Logging configuration."""
    level: str = "INFO"
    format: str = "text"  # "text" or "json"
    to_driver: bool = True

    def validate(self) -> List[str]:
        """Validate the logging configuration."""
        errors = []
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if self.level.upper() not in valid_levels:
            errors.append(f"Invalid logging level: {self.level}. "
                         f"Must be one of: {valid_levels}")
        valid_formats = ["text", "json"]
        if self.format not in valid_formats:
            errors.append(f"Invalid logging format: {self.format}. "
                         f"Must be one of: {valid_formats}")
        return errors


@dataclass
class ObservabilityConfig:
    """Observability configuration (metrics, logging, tracing)."""
    metrics: MetricsConfig = field(default_factory=MetricsConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)

    def validate(self) -> List[str]:
        """Validate the observability configuration."""
        return self.logging.validate()


@dataclass
class DashboardConfig:
    """Dashboard configuration."""
    enabled: Optional[bool] = None  # None means auto-detect
    host: str = "127.0.0.1"
    port: Optional[int] = None  # None means auto-assign


@dataclass
class RayConfig:
    """
    Unified Ray configuration.

    This is the main configuration class that contains all Ray configuration
    options organized into logical sections.

    Example:
        config = RayConfig(
            resources=ResourceConfig(num_cpus=4, num_gpus=1),
            object_store=ObjectStoreConfig(memory_bytes=8*1024**3)
        )
        ray.init(config=config)
    """

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

    # Dashboard
    dashboard: DashboardConfig = field(default_factory=DashboardConfig)

    # Address configuration
    address: Optional[str] = None
    namespace: Optional[str] = None

    def validate(self) -> List[str]:
        """
        Validate the entire configuration.

        Returns:
            List of error messages. Empty list if valid.
        """
        errors = []
        errors.extend(self.resources.validate())
        errors.extend(self.object_store.validate())
        errors.extend(self.scheduling.validate())
        errors.extend(self.security.validate())
        errors.extend(self.observability.validate())
        return errors

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to nested dictionary."""
        return _dataclass_to_dict(self)

    def to_yaml(self) -> str:
        """Convert configuration to YAML string."""
        return yaml.dump(self.to_dict(), default_flow_style=False, sort_keys=False)

    def diff_from_defaults(self) -> Dict[str, Tuple[Any, Any]]:
        """
        Compare this configuration with defaults.

        Returns:
            Dictionary mapping dotted paths to (default_value, current_value) tuples
            for all values that differ from defaults.
        """
        defaults = RayConfig()
        return _compare_configs(defaults.to_dict(), self.to_dict())

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RayConfig":
        """Create a RayConfig from a dictionary."""
        return _dict_to_dataclass(cls, data)

    @classmethod
    def from_yaml(cls, yaml_str: str) -> "RayConfig":
        """Create a RayConfig from a YAML string."""
        data = yaml.safe_load(yaml_str)
        return cls.from_dict(data) if data else cls()

    @classmethod
    def from_yaml_file(cls, filepath: str) -> "RayConfig":
        """Create a RayConfig from a YAML file."""
        filepath = os.path.expanduser(filepath)
        with open(filepath, 'r') as f:
            return cls.from_yaml(f.read())


def _dataclass_to_dict(obj: Any) -> Any:
    """Recursively convert a dataclass to a dictionary."""
    if hasattr(obj, '__dataclass_fields__'):
        result = {}
        for f in fields(obj):
            value = getattr(obj, f.name)
            result[f.name] = _dataclass_to_dict(value)
        return result
    elif isinstance(obj, dict):
        return {k: _dataclass_to_dict(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [_dataclass_to_dict(v) for v in obj]
    else:
        return obj


def _dict_to_dataclass(cls, data: Dict[str, Any]) -> Any:
    """Recursively convert a dictionary to a dataclass."""
    if data is None:
        return cls()

    field_types = {f.name: f.type for f in fields(cls)}
    kwargs = {}

    for field_name, value in data.items():
        if field_name not in field_types:
            warnings.warn(f"Unknown configuration field: {field_name}")
            continue

        field_type = field_types[field_name]

        # Handle nested dataclasses
        if hasattr(field_type, '__dataclass_fields__'):
            if isinstance(value, dict):
                kwargs[field_name] = _dict_to_dataclass(field_type, value)
            else:
                kwargs[field_name] = value
        else:
            kwargs[field_name] = value

    return cls(**kwargs)


def _compare_configs(
    default: Dict[str, Any],
    current: Dict[str, Any],
    prefix: str = ""
) -> Dict[str, Tuple[Any, Any]]:
    """Compare two config dictionaries and return differences."""
    diff = {}

    all_keys = set(default.keys()) | set(current.keys())

    for key in all_keys:
        path = f"{prefix}.{key}" if prefix else key
        default_val = default.get(key)
        current_val = current.get(key)

        if isinstance(default_val, dict) and isinstance(current_val, dict):
            diff.update(_compare_configs(default_val, current_val, path))
        elif default_val != current_val:
            diff[path] = (default_val, current_val)

    return diff


class ConfigLoader:
    """
    Load Ray configuration from multiple sources.

    Sources are loaded in order of increasing priority:
    1. Defaults
    2. Configuration file
    3. Environment variables
    4. Programmatic overrides
    """

    # Mapping of environment variables to config paths
    ENV_VAR_MAPPINGS = {
        'RAY_RESOURCES_NUM_CPUS': 'resources.num_cpus',
        'RAY_RESOURCES_NUM_GPUS': 'resources.num_gpus',
        'RAY_RESOURCES_MEMORY': 'resources.memory',
        'RAY_OBJECT_STORE_MEMORY': 'object_store.memory_bytes',
        'RAY_OBJECT_STORE_SPILLING_ENABLED': 'object_store.spilling.enabled',
        'RAY_OBJECT_STORE_SPILLING_DIRECTORY': 'object_store.spilling.directory',
        'RAY_OBJECT_STORE_EVICTION_THRESHOLD': 'object_store.eviction_threshold',
        'RAY_SCHEDULING_SPREAD_THRESHOLD': 'scheduling.spread_threshold',
        'RAY_SCHEDULING_WORKER_LEASE_TIMEOUT': 'scheduling.worker_lease_timeout_seconds',
        'RAY_SECURITY_TLS_ENABLED': 'security.tls.enabled',
        'RAY_SECURITY_TLS_CERT_PATH': 'security.tls.cert_path',
        'RAY_SECURITY_TLS_KEY_PATH': 'security.tls.key_path',
        'RAY_SECURITY_AUTHENTICATION_MODE': 'security.authentication.mode',
        'RAY_SECURITY_AUTHENTICATION_TOKEN_PATH': 'security.authentication.token_path',
        'RAY_SECURITY_DEVELOPMENT_MODE': 'security.development_mode',
        'RAY_OBSERVABILITY_METRICS_ENABLED': 'observability.metrics.enabled',
        'RAY_OBSERVABILITY_METRICS_EXPORT_PORT': 'observability.metrics.export_port',
        'RAY_OBSERVABILITY_LOGGING_LEVEL': 'observability.logging.level',
        'RAY_OBSERVABILITY_LOGGING_FORMAT': 'observability.logging.format',
        'RAY_DASHBOARD_ENABLED': 'dashboard.enabled',
        'RAY_DASHBOARD_HOST': 'dashboard.host',
        'RAY_DASHBOARD_PORT': 'dashboard.port',
        'RAY_ADDRESS': 'address',
        'RAY_NAMESPACE': 'namespace',
    }

    def __init__(self, config_file: Optional[str] = None):
        """
        Initialize the ConfigLoader.

        Args:
            config_file: Path to the configuration file. If None, will check
                        RAY_CONFIG_FILE environment variable, then fall back
                        to ~/.ray/config.yaml.
        """
        self.config_file = config_file

    def load(self, overrides: Optional[Dict[str, Any]] = None) -> RayConfig:
        """
        Load configuration from all sources.

        Args:
            overrides: Programmatic overrides (highest priority).

        Returns:
            Merged RayConfig object.
        """
        # Start with defaults
        config_dict = RayConfig().to_dict()

        # Load from file
        file_config = self._load_from_file()
        if file_config:
            config_dict = self._merge_dicts(config_dict, file_config)

        # Load from environment variables
        env_config = self._load_from_env()
        if env_config:
            config_dict = self._merge_dicts(config_dict, env_config)

        # Apply programmatic overrides
        if overrides:
            config_dict = self._merge_dicts(config_dict, overrides)

        return RayConfig.from_dict(config_dict)

    def _load_from_file(self) -> Optional[Dict[str, Any]]:
        """Load configuration from file."""
        config_path = self.config_file

        if config_path is None:
            config_path = os.environ.get('RAY_CONFIG_FILE')

        if config_path is None:
            config_path = os.path.expanduser('~/.ray/config.yaml')
        else:
            config_path = os.path.expanduser(config_path)

        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)

        return None

    def _load_from_env(self) -> Dict[str, Any]:
        """Load configuration from environment variables."""
        config = {}

        for env_var, config_path in self.ENV_VAR_MAPPINGS.items():
            value = os.environ.get(env_var)
            if value is not None:
                # Parse the value
                parsed_value = self._parse_env_value(value)
                # Set the value at the config path
                self._set_nested_value(config, config_path, parsed_value)

        return config

    def _parse_env_value(self, value: str) -> Any:
        """Parse an environment variable value to the appropriate type."""
        # Boolean
        if value.lower() in ('true', 'false'):
            return value.lower() == 'true'

        # Integer
        try:
            return int(value)
        except ValueError:
            pass

        # Float
        try:
            return float(value)
        except ValueError:
            pass

        # String
        return value

    def _set_nested_value(
        self,
        config: Dict[str, Any],
        path: str,
        value: Any
    ) -> None:
        """Set a value in a nested dictionary using a dotted path."""
        parts = path.split('.')
        current = config

        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]

        current[parts[-1]] = value

    def _merge_dicts(
        self,
        base: Dict[str, Any],
        override: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Deep merge two dictionaries."""
        result = copy.deepcopy(base)

        for key, value in override.items():
            if (key in result and
                isinstance(result[key], dict) and
                isinstance(value, dict)):
                result[key] = self._merge_dicts(result[key], value)
            else:
                result[key] = copy.deepcopy(value)

        return result

    @classmethod
    def get_env_var_mappings(cls) -> Dict[str, str]:
        """Get the environment variable to config path mappings."""
        return cls.ENV_VAR_MAPPINGS.copy()


def convert_legacy_config(
    num_cpus: Optional[int] = None,
    num_gpus: Optional[int] = None,
    resources: Optional[Dict[str, float]] = None,
    object_store_memory: Optional[int] = None,
    _system_config: Optional[Dict[str, Any]] = None,
    include_dashboard: Optional[bool] = None,
    dashboard_host: Optional[str] = None,
    dashboard_port: Optional[int] = None,
    logging_level: Optional[int] = None,
    namespace: Optional[str] = None,
    address: Optional[str] = None,
    **kwargs
) -> RayConfig:
    """
    Convert legacy ray.init() parameters to a RayConfig object.

    This function provides backwards compatibility with the old configuration style.

    Args:
        num_cpus: Number of CPUs
        num_gpus: Number of GPUs
        resources: Custom resources dictionary
        object_store_memory: Object store memory in bytes
        _system_config: Legacy system config dictionary
        include_dashboard: Whether to include dashboard
        dashboard_host: Dashboard host
        dashboard_port: Dashboard port
        logging_level: Logging level as integer
        namespace: Ray namespace
        address: Ray cluster address
        **kwargs: Additional legacy parameters (ignored with warning)

    Returns:
        RayConfig object with the legacy parameters converted.
    """
    config = RayConfig()

    # Resources
    if num_cpus is not None:
        config.resources.num_cpus = num_cpus
    if num_gpus is not None:
        config.resources.num_gpus = num_gpus
    if resources:
        config.resources.custom = resources

    # Object store
    if object_store_memory is not None:
        config.object_store.memory_bytes = object_store_memory

    # Dashboard
    if include_dashboard is not None:
        config.dashboard.enabled = include_dashboard
    if dashboard_host is not None:
        config.dashboard.host = dashboard_host
    if dashboard_port is not None:
        config.dashboard.port = dashboard_port

    # Logging
    if logging_level is not None:
        import logging
        level_map = {
            logging.DEBUG: "DEBUG",
            logging.INFO: "INFO",
            logging.WARNING: "WARNING",
            logging.ERROR: "ERROR",
            logging.CRITICAL: "CRITICAL",
        }
        config.observability.logging.level = level_map.get(
            logging_level, "INFO"
        )

    # Namespace and address
    if namespace is not None:
        config.namespace = namespace
    if address is not None:
        config.address = address

    # Handle _system_config
    if _system_config:
        # Map known system config options
        if 'object_store_memory' in _system_config:
            config.object_store.memory_bytes = _system_config['object_store_memory']
        if 'scheduler_spread_threshold' in _system_config:
            config.scheduling.spread_threshold = _system_config['scheduler_spread_threshold']

    # Warn about unused kwargs
    if kwargs:
        for key in kwargs:
            warnings.warn(
                f"Legacy parameter '{key}' is not mapped to the new config system",
                DeprecationWarning
            )

    return config


def get_default_config() -> RayConfig:
    """Get a RayConfig with all default values."""
    return RayConfig()


def generate_config_template() -> str:
    """
    Generate a YAML configuration template with comments.

    Returns:
        YAML string with default configuration and documentation comments.
    """
    template = '''# Ray Configuration File
# This file configures Ray's behavior. All values shown are defaults.
# Environment variables can override these settings using RAY_* prefix.
# See: https://docs.ray.io/en/latest/ray-core/configure.html

# Resource configuration for Ray nodes
resources:
  # Number of CPUs (null = auto-detect)
  num_cpus: null
  # Number of GPUs (null = auto-detect)
  num_gpus: null
  # Memory in bytes (null = auto-detect)
  memory: null
  # Custom resources (e.g., {"special_hardware": 1})
  custom: {}

# Object store configuration
object_store:
  # Memory allocated to object store in bytes
  # Default is 30% of system memory
  memory_bytes: {memory_bytes}
  # Object spilling configuration
  spilling:
    # Enable object spilling to disk
    enabled: true
    # Directory for spilled objects
    directory: /tmp/ray_spill
  # Fraction of object store memory that triggers eviction
  eviction_threshold: 0.8

# Scheduling configuration
scheduling:
  # Threshold for spreading tasks across nodes (0-1)
  # Lower values encourage more load spreading
  spread_threshold: 0.5
  # Timeout for worker lease in seconds
  worker_lease_timeout_seconds: 10.0
  # Fraction of top nodes to consider for scheduling
  top_k_fraction: 0.2
  # Minimum number of top nodes to consider
  top_k_absolute: 1

# Security configuration
security:
  # TLS/SSL configuration
  tls:
    enabled: false
    cert_path: null
    key_path: null
    ca_cert_path: null
  # Authentication configuration
  authentication:
    # Authentication mode: "disabled" or "token"
    mode: disabled
    token_path: null
  # Development mode disables TLS and auth
  development_mode: false

# Observability configuration
observability:
  # Metrics export configuration
  metrics:
    enabled: true
    export_port: 8080
    # Custom labels for metrics
    labels: {}
  # Logging configuration
  logging:
    # Log level: DEBUG, INFO, WARNING, ERROR, CRITICAL
    level: INFO
    # Log format: "text" or "json"
    format: text
    # Send logs to driver
    to_driver: true

# Dashboard configuration
dashboard:
  # Enable dashboard (null = auto-detect)
  enabled: null
  # Dashboard host
  host: 127.0.0.1
  # Dashboard port (null = auto-assign)
  port: null

# Cluster address (e.g., "auto", "local", or "ray://host:port")
address: null

# Ray namespace for job isolation
namespace: null
'''

    return template.format(
        memory_bytes=_default_object_store_memory()
    )
