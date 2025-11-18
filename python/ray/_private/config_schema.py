# Copyright 2025 The Ray Authors.
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

"""Configuration schema for Ray configuration validation.

This module defines schemas for Ray configuration options including
type information, valid ranges, defaults, deprecation info, and
performance hints.
"""

import os
from typing import Any, Callable, Dict, List, Optional, Union

try:
    import psutil

    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False


def _get_system_memory() -> int:
    """Get total system memory in bytes."""
    if HAS_PSUTIL:
        return psutil.virtual_memory().total
    # Fallback: try to read from /proc/meminfo on Linux
    try:
        with open("/proc/meminfo", "r") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    # Value is in kB
                    return int(line.split()[1]) * 1024
    except Exception:
        pass
    # Default fallback: 8GB
    return 8 * 1024 * 1024 * 1024


def _get_cpu_count() -> int:
    """Get number of CPUs."""
    return os.cpu_count() or 1


# Schema entry type definition
# Each config entry can have:
#   - type: str, int, float, bool, dict, list
#   - min/max: numeric bounds (can be callable)
#   - default: default value (can be callable)
#   - description: human-readable description
#   - deprecated: bool indicating if deprecated
#   - deprecated_names: list of old names for this config
#   - replacement: name of the config that replaces this one
#   - conflicts_with: list of configs that conflict with this one
#   - performance_hints: list of dicts with condition and message
#   - env_var: corresponding environment variable name
#   - internal: bool indicating if this is an internal config

CONFIG_SCHEMA: Dict[str, Dict[str, Any]] = {
    # Core resource configuration
    "num_cpus": {
        "type": "int",
        "min": 0,
        "default": _get_cpu_count,
        "description": "Number of CPUs available to Ray",
        "conflicts_with": ["resources.CPU"],
        "performance_hints": [
            {
                "condition": lambda v, ctx: v == 0,
                "message": "num_cpus=0 means no tasks can be scheduled on this node",
            },
            {
                "condition": lambda v, ctx: v > _get_cpu_count() * 2,
                "message": (
                    "num_cpus={value} is more than 2x the detected CPU count "
                    f"({_get_cpu_count()}). This may cause oversubscription."
                ),
            },
        ],
    },
    "num_gpus": {
        "type": "int",
        "min": 0,
        "default": 0,
        "description": "Number of GPUs available to Ray",
        "conflicts_with": ["resources.GPU"],
    },
    "object_store_memory": {
        "type": "int",
        "min": 78643200,  # 75MB minimum
        "max": lambda: _get_system_memory(),
        "default": lambda: int(0.3 * _get_system_memory()),
        "description": "Bytes allocated to object store",
        "deprecated_names": ["plasma_store_memory"],
        "env_var": "RAY_OBJECT_STORE_MEMORY",
        "performance_hints": [
            {
                "condition": lambda v, ctx: v < 1_000_000_000,
                "message": (
                    "object_store_memory={value} ({value_hr}) is below 1GB. "
                    "This may cause frequent object spilling for workloads with "
                    "large objects. Consider at least 1GB for better performance."
                ),
            },
            {
                "condition": lambda v, ctx: v > 0.8 * _get_system_memory(),
                "message": (
                    "object_store_memory={value} ({value_hr}) is more than 80% of "
                    "system memory. This may cause out-of-memory issues for worker processes."
                ),
            },
        ],
    },
    "_memory": {
        "type": "int",
        "min": 0,
        "description": "Amount of reservable memory resource in bytes",
        "internal": True,
    },
    "resources": {
        "type": "dict",
        "description": "Custom resources available on this node",
        "performance_hints": [
            {
                "condition": lambda v, ctx: "CPU" in v and ctx.get("num_cpus") is not None,
                "message": (
                    "Both 'resources.CPU' and 'num_cpus' are specified. "
                    "'resources.CPU' will take precedence."
                ),
            },
            {
                "condition": lambda v, ctx: "GPU" in v and ctx.get("num_gpus") is not None,
                "message": (
                    "Both 'resources.GPU' and 'num_gpus' are specified. "
                    "'resources.GPU' will take precedence."
                ),
            },
        ],
    },
    "labels": {
        "type": "dict",
        "description": "Key-value labels for the node",
    },
    # Networking and dashboard
    "address": {
        "type": "str",
        "description": "Address of Ray cluster to connect to",
        "performance_hints": [
            {
                "condition": lambda v, ctx: v and "localhost" in v,
                "message": (
                    "Using 'localhost' in address may cause issues with distributed "
                    "workloads. Consider using the actual IP address."
                ),
            },
        ],
    },
    "dashboard_host": {
        "type": "str",
        "default": "127.0.0.1",
        "description": "Host to bind dashboard server to",
        "performance_hints": [
            {
                "condition": lambda v, ctx: v == "0.0.0.0",
                "message": (
                    "dashboard_host='0.0.0.0' exposes the dashboard to all interfaces. "
                    "Ensure this is intended for security reasons."
                ),
            },
        ],
    },
    "dashboard_port": {
        "type": "int",
        "min": 1,
        "max": 65535,
        "default": 8265,
        "description": "Port for the dashboard server",
    },
    "include_dashboard": {
        "type": "bool",
        "description": "Whether to start the Ray dashboard",
    },
    # Logging configuration
    "configure_logging": {
        "type": "bool",
        "default": True,
        "description": "Whether to configure logging",
    },
    "logging_level": {
        "type": "int",
        "min": 0,
        "description": "Logging level for the ray logger",
    },
    "logging_format": {
        "type": "str",
        "description": "Logging format string",
    },
    "log_to_driver": {
        "type": "bool",
        "description": "Whether to redirect worker output to driver",
    },
    # Runtime configuration
    "namespace": {
        "type": "str",
        "description": "Namespace for jobs and named actors",
    },
    "runtime_env": {
        "type": "dict",
        "description": "Runtime environment for the job",
    },
    "local_mode": {
        "type": "bool",
        "default": False,
        "description": "Run Ray in local mode for debugging",
        "deprecated": True,
        "replacement": "Ray Distributed Debugger",
    },
    "ignore_reinit_error": {
        "type": "bool",
        "default": False,
        "description": "Suppress errors from calling ray.init() twice",
    },
    # Resource isolation
    "enable_resource_isolation": {
        "type": "bool",
        "default": False,
        "description": "Enable resource isolation through cgroupv2",
    },
    "system_reserved_cpu": {
        "type": "float",
        "min": 0,
        "description": "CPU cores to reserve for Ray system processes",
    },
    "system_reserved_memory": {
        "type": "int",
        "min": 0,
        "description": "Memory in bytes to reserve for Ray system processes",
    },
    # Internal/hidden options
    "_enable_object_reconstruction": {
        "type": "bool",
        "default": False,
        "description": "Enable object reconstruction on failure",
        "internal": True,
    },
    "_plasma_directory": {
        "type": "str",
        "description": "Override plasma mmap file directory",
        "internal": True,
        "deprecated": True,
        "replacement": "object_spilling_directory",
    },
    "object_spilling_directory": {
        "type": "str",
        "description": "Path to spill objects to",
    },
    "_node_ip_address": {
        "type": "str",
        "description": "IP address of the current node",
        "internal": True,
    },
    "_driver_object_store_memory": {
        "type": "int",
        "description": "Object store memory for driver",
        "internal": True,
        "deprecated": True,
    },
    "_redis_username": {
        "type": "str",
        "description": "Redis username for authentication",
        "internal": True,
    },
    "_redis_password": {
        "type": "str",
        "description": "Redis password for authentication",
        "internal": True,
    },
    "_temp_dir": {
        "type": "str",
        "description": "Root temporary directory for Ray",
        "internal": True,
    },
    "_metrics_export_port": {
        "type": "int",
        "min": 1,
        "max": 65535,
        "description": "Port for Prometheus metrics endpoint",
        "internal": True,
    },
    "_system_config": {
        "type": "dict",
        "description": "Override RayConfig defaults (testing only)",
        "internal": True,
    },
    "_tracing_startup_hook": {
        "type": "callable",
        "description": "Function to set up tracing",
        "internal": True,
    },
    "_node_name": {
        "type": "str",
        "description": "User-provided node name",
        "internal": True,
    },
    "_skip_env_hook": {
        "type": "bool",
        "default": False,
        "description": "Skip environment hook",
        "internal": True,
    },
    "_cgroup_path": {
        "type": "str",
        "description": "Cgroup path for resource isolation",
        "internal": True,
    },
    "_config_validation": {
        "type": "str",
        "description": "Config validation mode: 'on', 'off', or 'strict'",
        "internal": True,
        "default": "on",
    },
    # Deprecated options (for backward compatibility warnings)
    "plasma_store_memory": {
        "type": "int",
        "deprecated": True,
        "replacement": "object_store_memory",
        "description": "Deprecated: use object_store_memory instead",
    },
    "plasma_directory": {
        "type": "str",
        "deprecated": True,
        "replacement": "object_spilling_directory",
        "description": "Deprecated: use object_spilling_directory instead",
    },
}

# Environment variable schema for RAY_* variables
ENV_VAR_SCHEMA: Dict[str, Dict[str, Any]] = {
    "RAY_ADDRESS": {
        "type": "str",
        "config_key": "address",
        "description": "Address of Ray cluster to connect to",
    },
    "RAY_OBJECT_STORE_MEMORY": {
        "type": "int",
        "config_key": "object_store_memory",
        "description": "Bytes allocated to object store",
    },
    "RAY_NUM_CPUS": {
        "type": "int",
        "config_key": "num_cpus",
        "description": "Number of CPUs available to Ray",
    },
    "RAY_NUM_GPUS": {
        "type": "int",
        "config_key": "num_gpus",
        "description": "Number of GPUs available to Ray",
    },
    "RAY_NAMESPACE": {
        "type": "str",
        "config_key": "namespace",
        "description": "Namespace for jobs and named actors",
    },
    "RAY_RUNTIME_ENV": {
        "type": "json",
        "config_key": "runtime_env",
        "description": "Runtime environment as JSON",
    },
    "RAY_USE_TLS": {
        "type": "bool",
        "description": "Enable TLS for Ray connections",
        "bool_values": {"true": True, "false": False, "1": True, "0": False},
    },
    "RAY_TLS_CA_CERT": {
        "type": "str",
        "description": "Path to TLS CA certificate",
    },
    "RAY_TLS_CERT": {
        "type": "str",
        "description": "Path to TLS certificate",
    },
    "RAY_TLS_KEY": {
        "type": "str",
        "description": "Path to TLS private key",
    },
    "RAY_ENABLE_RECORD_ACTOR_TASK_LOGGING": {
        "type": "bool",
        "description": "Enable actor task logging",
        "bool_values": {"true": True, "false": False, "1": True, "0": False},
    },
    "RAY_LOG_TO_DRIVER": {
        "type": "bool",
        "config_key": "log_to_driver",
        "description": "Redirect worker output to driver",
        "bool_values": {"true": True, "false": False, "1": True, "0": False},
    },
    "RAY_IGNORE_UNHANDLED_ERRORS": {
        "type": "bool",
        "description": "Suppress unhandled error messages",
        "bool_values": {"true": True, "false": False, "1": True, "0": False},
    },
    "RAY_memory_monitor_refresh_ms": {
        "type": "int",
        "min": 0,
        "description": "Memory monitor refresh interval in ms",
    },
    "RAY_memory_usage_threshold": {
        "type": "float",
        "min": 0.0,
        "max": 1.0,
        "description": "Memory usage threshold for killing processes",
    },
    "RAY_scheduler_spread_threshold": {
        "type": "float",
        "min": 0.0,
        "max": 1.0,
        "description": "Threshold for hybrid scheduling policy",
    },
    "RAY_DEBUG": {
        "type": "bool",
        "description": "Enable Ray debug mode",
        "bool_values": {"true": True, "false": False, "1": True, "0": False},
    },
}


def get_config_schema() -> Dict[str, Dict[str, Any]]:
    """Get the configuration schema dictionary."""
    return CONFIG_SCHEMA.copy()


def get_env_var_schema() -> Dict[str, Dict[str, Any]]:
    """Get the environment variable schema dictionary."""
    return ENV_VAR_SCHEMA.copy()


def get_all_config_names() -> List[str]:
    """Get list of all valid configuration names."""
    return list(CONFIG_SCHEMA.keys())


def get_deprecated_configs() -> Dict[str, str]:
    """Get mapping of deprecated config names to their replacements."""
    result = {}
    for name, spec in CONFIG_SCHEMA.items():
        if spec.get("deprecated"):
            replacement = spec.get("replacement", "")
            result[name] = replacement
        # Also add deprecated_names
        for old_name in spec.get("deprecated_names", []):
            result[old_name] = name
    return result
