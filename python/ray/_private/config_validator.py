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

"""Configuration validator for Ray.

This module provides validation for Ray configuration options,
including type checking, range validation, deprecation warnings,
and performance hints.
"""

import difflib
import json
import logging
import os
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from ray._private.config_schema import (
    CONFIG_SCHEMA,
    ENV_VAR_SCHEMA,
    get_all_config_names,
    get_deprecated_configs,
)

logger = logging.getLogger(__name__)


def _format_bytes(num_bytes: int) -> str:
    """Format bytes as human-readable string."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(num_bytes) < 1024.0:
            return f"{num_bytes:.1f}{unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:.1f}PB"


@dataclass
class ValidationResult:
    """Result of configuration validation.

    Attributes:
        errors: List of error messages (validation failures).
        warnings: List of warning messages (deprecations, performance hints).
        info: List of informational messages.
    """

    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    info: List[str] = field(default_factory=list)

    @property
    def has_errors(self) -> bool:
        """Check if there are any errors."""
        return len(self.errors) > 0

    @property
    def has_warnings(self) -> bool:
        """Check if there are any warnings."""
        return len(self.warnings) > 0

    @property
    def is_valid(self) -> bool:
        """Check if validation passed (no errors)."""
        return not self.has_errors

    def merge(self, other: "ValidationResult") -> "ValidationResult":
        """Merge another validation result into this one."""
        self.errors.extend(other.errors)
        self.warnings.extend(other.warnings)
        self.info.extend(other.info)
        return self

    def __str__(self) -> str:
        """Format validation result as string."""
        lines = []
        if self.errors:
            lines.append("Errors:")
            for error in self.errors:
                lines.append(f"  - {error}")
        if self.warnings:
            lines.append("Warnings:")
            for warning in self.warnings:
                lines.append(f"  - {warning}")
        if self.info:
            lines.append("Info:")
            for info in self.info:
                lines.append(f"  - {info}")
        return "\n".join(lines) if lines else "Validation passed"


class ConfigValidator:
    """Validator for Ray configuration options.

    This class validates configuration dictionaries against the
    defined schema, checking types, ranges, deprecations, and
    providing performance hints.
    """

    def __init__(self, schema: Optional[Dict[str, Dict[str, Any]]] = None):
        """Initialize the validator.

        Args:
            schema: Configuration schema to use. Defaults to CONFIG_SCHEMA.
        """
        self.schema = schema or CONFIG_SCHEMA
        self._all_names = get_all_config_names()
        self._deprecated = get_deprecated_configs()

    def validate(self, config: Dict[str, Any]) -> ValidationResult:
        """Validate a configuration dictionary.

        Args:
            config: Configuration dictionary to validate.

        Returns:
            ValidationResult containing any errors and warnings.
        """
        result = ValidationResult()

        for key, value in config.items():
            if key not in self.schema:
                self._handle_unknown(key, value, result)
                continue

            spec = self.schema[key]

            # Check for deprecated configs
            self._check_deprecation(key, spec, result)

            # Skip further validation for None values (use default)
            if value is None:
                continue

            # Validate type
            self._validate_type(key, value, spec, result)

            # Validate range
            self._validate_range(key, value, spec, result)

            # Check conflicts
            self._check_conflicts(key, config, spec, result)

            # Check performance hints
            self._check_performance(key, value, spec, config, result)

        return result

    def _handle_unknown(
        self, key: str, value: Any, result: ValidationResult
    ) -> None:
        """Handle unknown configuration keys.

        Args:
            key: Unknown configuration key.
            value: Value provided for the key.
            result: ValidationResult to update.
        """
        # Check if it's a deprecated name
        if key in self._deprecated:
            replacement = self._deprecated[key]
            result.warnings.append(
                f"'{key}' is deprecated. Use '{replacement}' instead."
            )
            return

        # Try to find similar config names
        similar = self._find_similar(key)
        if similar:
            result.warnings.append(
                f"Unknown configuration '{key}'. Did you mean '{similar}'?"
            )
        else:
            result.warnings.append(
                f"Unknown configuration '{key}' will be ignored."
            )

    def _find_similar(self, key: str) -> Optional[str]:
        """Find similar configuration names using fuzzy matching.

        Args:
            key: Configuration key to match.

        Returns:
            Most similar configuration name, or None if no good match.
        """
        matches = difflib.get_close_matches(
            key, self._all_names, n=1, cutoff=0.6
        )
        return matches[0] if matches else None

    def _validate_type(
        self,
        key: str,
        value: Any,
        spec: Dict[str, Any],
        result: ValidationResult,
    ) -> None:
        """Validate the type of a configuration value.

        Args:
            key: Configuration key.
            value: Value to validate.
            spec: Schema specification for this key.
            result: ValidationResult to update.
        """
        expected_type = spec.get("type")
        if not expected_type:
            return

        type_map = {
            "int": (int,),
            "float": (int, float),
            "str": (str,),
            "bool": (bool,),
            "dict": (dict,),
            "list": (list,),
            "callable": (Callable,),
        }

        expected_types = type_map.get(expected_type)
        if not expected_types:
            return

        # Special handling for callable
        if expected_type == "callable":
            if not callable(value):
                result.errors.append(
                    f"'{key}' must be callable, got {type(value).__name__}"
                )
            return

        if not isinstance(value, expected_types):
            # Try to provide helpful conversion hints
            actual_type = type(value).__name__

            # Check for common string-to-bool issues
            if expected_type == "bool" and isinstance(value, str):
                result.warnings.append(
                    f"'{key}' should be a bool, got string '{value}'. "
                    f"Use True/False instead of '{value}'."
                )
                return

            # Check for string-to-int issues
            if expected_type == "int" and isinstance(value, str):
                try:
                    int(value)
                    result.warnings.append(
                        f"'{key}' should be an int, got string '{value}'. "
                        f"Consider using int({value}) = {int(value)}."
                    )
                    return
                except ValueError:
                    pass

            result.errors.append(
                f"'{key}' must be of type {expected_type}, got {actual_type}"
            )

    def _validate_range(
        self,
        key: str,
        value: Any,
        spec: Dict[str, Any],
        result: ValidationResult,
    ) -> None:
        """Validate numeric range constraints.

        Args:
            key: Configuration key.
            value: Value to validate.
            spec: Schema specification for this key.
            result: ValidationResult to update.
        """
        if not isinstance(value, (int, float)):
            return

        min_val = spec.get("min")
        max_val = spec.get("max")

        # Resolve callable bounds
        if callable(min_val):
            min_val = min_val()
        if callable(max_val):
            max_val = max_val()

        if min_val is not None and value < min_val:
            result.errors.append(
                f"'{key}' value {value} is below minimum {min_val}"
            )

        if max_val is not None and value > max_val:
            result.errors.append(
                f"'{key}' value {value} is above maximum {max_val}"
            )

    def _check_deprecation(
        self, key: str, spec: Dict[str, Any], result: ValidationResult
    ) -> None:
        """Check if a configuration is deprecated.

        Args:
            key: Configuration key.
            spec: Schema specification for this key.
            result: ValidationResult to update.
        """
        if spec.get("deprecated"):
            replacement = spec.get("replacement", "")
            if replacement:
                result.warnings.append(
                    f"'{key}' is deprecated. Use '{replacement}' instead."
                )
            else:
                result.warnings.append(f"'{key}' is deprecated.")

    def _check_conflicts(
        self,
        key: str,
        config: Dict[str, Any],
        spec: Dict[str, Any],
        result: ValidationResult,
    ) -> None:
        """Check for conflicting configuration options.

        Args:
            key: Configuration key.
            config: Full configuration dictionary.
            spec: Schema specification for this key.
            result: ValidationResult to update.
        """
        conflicts = spec.get("conflicts_with", [])
        for conflict_key in conflicts:
            # Handle nested keys like "resources.CPU"
            parts = conflict_key.split(".")
            conflict_value = config
            try:
                for part in parts:
                    conflict_value = conflict_value[part]
                # Found conflicting value
                result.warnings.append(
                    f"'{key}' conflicts with '{conflict_key}'. "
                    f"'{conflict_key}' will take precedence."
                )
            except (KeyError, TypeError):
                # Conflict key not present
                pass

    def _check_performance(
        self,
        key: str,
        value: Any,
        spec: Dict[str, Any],
        config: Dict[str, Any],
        result: ValidationResult,
    ) -> None:
        """Check performance hints for a configuration.

        Args:
            key: Configuration key.
            value: Configuration value.
            spec: Schema specification for this key.
            config: Full configuration dictionary for context.
            result: ValidationResult to update.
        """
        hints = spec.get("performance_hints", [])
        for hint in hints:
            condition = hint.get("condition")
            message = hint.get("message", "")

            if condition and callable(condition):
                try:
                    if condition(value, config):
                        # Format message with value
                        formatted_msg = message.format(
                            value=value,
                            value_hr=_format_bytes(value) if isinstance(value, int) else value,
                        )
                        result.warnings.append(formatted_msg)
                except Exception as e:
                    # Don't fail validation due to hint errors
                    logger.debug(f"Error checking performance hint for '{key}': {e}")


def validate_config(config: Dict[str, Any]) -> ValidationResult:
    """Validate a Ray configuration dictionary.

    This is the main entry point for programmatic config validation.

    Args:
        config: Configuration dictionary to validate.

    Returns:
        ValidationResult containing errors and warnings.

    Example:
        >>> from ray._private.config_validator import validate_config
        >>> result = validate_config({
        ...     "num_cpus": 4,
        ...     "object_store_memory": 100_000_000
        ... })
        >>> if result.has_warnings:
        ...     for w in result.warnings:
        ...         print(f"Warning: {w}")
    """
    validator = ConfigValidator()
    return validator.validate(config)


def validate_environment_variables() -> ValidationResult:
    """Validate RAY_* environment variables.

    Returns:
        ValidationResult containing errors and warnings.
    """
    result = ValidationResult()

    for key, value in os.environ.items():
        if not key.startswith("RAY_"):
            continue

        if key in ENV_VAR_SCHEMA:
            spec = ENV_VAR_SCHEMA[key]
            _validate_env_var(key, value, spec, result)
        else:
            # Check for unknown RAY_ variables
            # Allow RAY_ prefix for system configs
            if not any(
                key.startswith(prefix)
                for prefix in ["RAY_BACKEND_", "RAY_USAGE_", "RAY_SCHEDULER_"]
            ):
                # Find similar env vars
                similar = difflib.get_close_matches(
                    key, list(ENV_VAR_SCHEMA.keys()), n=1, cutoff=0.6
                )
                if similar:
                    result.warnings.append(
                        f"Unknown environment variable '{key}'. "
                        f"Did you mean '{similar[0]}'?"
                    )

    return result


def _validate_env_var(
    key: str, value: str, spec: Dict[str, Any], result: ValidationResult
) -> None:
    """Validate a single environment variable.

    Args:
        key: Environment variable name.
        value: Environment variable value (always string).
        spec: Schema specification for this variable.
        result: ValidationResult to update.
    """
    expected_type = spec.get("type")

    if expected_type == "int":
        try:
            int_value = int(value)
            # Check range
            min_val = spec.get("min")
            max_val = spec.get("max")
            if min_val is not None and int_value < min_val:
                result.errors.append(
                    f"{key}={value} is below minimum {min_val}"
                )
            if max_val is not None and int_value > max_val:
                result.errors.append(
                    f"{key}={value} is above maximum {max_val}"
                )
        except ValueError:
            result.errors.append(
                f"{key} should be an integer, got '{value}'"
            )

    elif expected_type == "float":
        try:
            float_value = float(value)
            # Check range
            min_val = spec.get("min")
            max_val = spec.get("max")
            if min_val is not None and float_value < min_val:
                result.errors.append(
                    f"{key}={value} is below minimum {min_val}"
                )
            if max_val is not None and float_value > max_val:
                result.errors.append(
                    f"{key}={value} is above maximum {max_val}"
                )
        except ValueError:
            result.errors.append(
                f"{key} should be a float, got '{value}'"
            )

    elif expected_type == "bool":
        bool_values = spec.get("bool_values", {})
        lower_value = value.lower()
        if lower_value not in bool_values and value not in bool_values:
            valid_values = list(bool_values.keys())
            result.warnings.append(
                f"{key} should be one of {valid_values}, got '{value}'. "
                f"Interpreting as {'True' if value else 'False'}."
            )

    elif expected_type == "json":
        try:
            json.loads(value)
        except json.JSONDecodeError as e:
            result.errors.append(
                f"{key} should be valid JSON: {e}"
            )


def validate_init_config(
    config: Dict[str, Any],
    validation_mode: str = "on",
) -> Tuple[ValidationResult, Dict[str, Any]]:
    """Validate configuration for ray.init() and apply transformations.

    This function validates the configuration and applies any necessary
    transformations (e.g., handling deprecated names).

    Args:
        config: Configuration dictionary from ray.init() kwargs.
        validation_mode: 'on', 'off', or 'strict'.

    Returns:
        Tuple of (ValidationResult, transformed_config).
    """
    if validation_mode == "off":
        return ValidationResult(), config

    result = ValidationResult()
    transformed_config = config.copy()

    # Handle deprecated names by mapping to new names
    deprecated_configs = get_deprecated_configs()
    for old_name, new_name in deprecated_configs.items():
        if old_name in transformed_config:
            value = transformed_config.pop(old_name)
            if new_name and new_name not in transformed_config:
                transformed_config[new_name] = value
                result.warnings.append(
                    f"'{old_name}' is deprecated. "
                    f"Automatically mapped to '{new_name}'."
                )
            elif new_name:
                result.warnings.append(
                    f"'{old_name}' is deprecated and ignored because "
                    f"'{new_name}' is already specified."
                )

    # Validate the transformed config
    validator = ConfigValidator()
    validation_result = validator.validate(transformed_config)
    result.merge(validation_result)

    return result, transformed_config
