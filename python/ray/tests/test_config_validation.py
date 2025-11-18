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

"""Tests for Ray configuration validation.

This module tests the config validation functionality including:
- Type validation
- Range validation
- Deprecation warnings
- Performance hints
- Conflict detection
- Environment variable validation
"""

import os
import pytest
import sys
import tempfile

import yaml


class TestConfigSchema:
    """Tests for configuration schema."""

    def test_schema_has_required_configs(self):
        """Test that schema contains common configuration options."""
        from ray._private.config_schema import CONFIG_SCHEMA

        required_configs = [
            "num_cpus",
            "num_gpus",
            "object_store_memory",
            "address",
            "dashboard_host",
            "dashboard_port",
        ]

        for config in required_configs:
            assert config in CONFIG_SCHEMA, f"Missing required config: {config}"

    def test_schema_entries_have_type(self):
        """Test that all schema entries have a type definition."""
        from ray._private.config_schema import CONFIG_SCHEMA

        for name, spec in CONFIG_SCHEMA.items():
            assert "type" in spec, f"Config '{name}' missing type definition"

    def test_get_deprecated_configs(self):
        """Test getting deprecated configuration mapping."""
        from ray._private.config_schema import get_deprecated_configs

        deprecated = get_deprecated_configs()
        assert isinstance(deprecated, dict)
        # plasma_store_memory should map to object_store_memory
        assert "plasma_store_memory" in deprecated
        assert deprecated["plasma_store_memory"] == "object_store_memory"


class TestValidationResult:
    """Tests for ValidationResult class."""

    def test_empty_result(self):
        """Test empty validation result."""
        from ray._private.config_validator import ValidationResult

        result = ValidationResult()
        assert result.is_valid
        assert not result.has_errors
        assert not result.has_warnings

    def test_result_with_errors(self):
        """Test validation result with errors."""
        from ray._private.config_validator import ValidationResult

        result = ValidationResult(errors=["error1", "error2"])
        assert not result.is_valid
        assert result.has_errors
        assert len(result.errors) == 2

    def test_result_with_warnings(self):
        """Test validation result with warnings."""
        from ray._private.config_validator import ValidationResult

        result = ValidationResult(warnings=["warning1"])
        assert result.is_valid
        assert result.has_warnings
        assert len(result.warnings) == 1

    def test_result_merge(self):
        """Test merging validation results."""
        from ray._private.config_validator import ValidationResult

        result1 = ValidationResult(errors=["e1"], warnings=["w1"])
        result2 = ValidationResult(errors=["e2"], warnings=["w2"])
        result1.merge(result2)

        assert len(result1.errors) == 2
        assert len(result1.warnings) == 2
        assert "e1" in result1.errors
        assert "e2" in result1.errors


class TestConfigValidator:
    """Tests for ConfigValidator class."""

    def test_validate_empty_config(self):
        """Test validating empty configuration."""
        from ray._private.config_validator import ConfigValidator

        validator = ConfigValidator()
        result = validator.validate({})
        assert result.is_valid

    def test_validate_valid_config(self):
        """Test validating a valid configuration."""
        from ray._private.config_validator import ConfigValidator

        validator = ConfigValidator()
        result = validator.validate({
            "num_cpus": 4,
            "num_gpus": 1,
        })
        assert result.is_valid

    def test_validate_type_int(self):
        """Test type validation for integers."""
        from ray._private.config_validator import ConfigValidator

        validator = ConfigValidator()

        # Valid int
        result = validator.validate({"num_cpus": 4})
        assert not any("type" in e.lower() for e in result.errors)

        # Invalid type (string instead of int)
        result = validator.validate({"num_cpus": "four"})
        assert result.has_errors or result.has_warnings

    def test_validate_type_bool(self):
        """Test type validation for booleans."""
        from ray._private.config_validator import ConfigValidator

        validator = ConfigValidator()

        # String "true" should trigger warning
        result = validator.validate({"local_mode": "true"})
        assert result.has_warnings
        assert any("bool" in w.lower() for w in result.warnings)

    def test_validate_range_min(self):
        """Test minimum range validation."""
        from ray._private.config_validator import ConfigValidator

        validator = ConfigValidator()

        # Below minimum
        result = validator.validate({"num_cpus": -1})
        assert result.has_errors
        assert any("below minimum" in e.lower() for e in result.errors)

    def test_validate_range_max(self):
        """Test maximum range validation."""
        from ray._private.config_validator import ConfigValidator

        validator = ConfigValidator()

        # Above maximum (port > 65535)
        result = validator.validate({"dashboard_port": 70000})
        assert result.has_errors
        assert any("above maximum" in e.lower() for e in result.errors)

    def test_deprecation_warning(self):
        """Test deprecation warnings for old config names."""
        from ray._private.config_validator import ConfigValidator

        validator = ConfigValidator()
        result = validator.validate({"local_mode": True})
        assert result.has_warnings
        assert any("deprecated" in w.lower() for w in result.warnings)

    def test_unknown_config_warning(self):
        """Test warning for unknown configuration keys."""
        from ray._private.config_validator import ConfigValidator

        validator = ConfigValidator()
        result = validator.validate({"unknown_config_key": "value"})
        assert result.has_warnings
        assert any("unknown" in w.lower() for w in result.warnings)

    def test_similar_config_suggestion(self):
        """Test that similar config names are suggested."""
        from ray._private.config_validator import ConfigValidator

        validator = ConfigValidator()
        # "num_cpu" is close to "num_cpus"
        result = validator.validate({"num_cpu": 4})
        assert result.has_warnings
        assert any("num_cpus" in w for w in result.warnings)

    def test_performance_hint_small_object_store(self):
        """Test performance hint for small object store."""
        from ray._private.config_validator import ConfigValidator

        validator = ConfigValidator()
        # 100MB is below the 1GB recommendation
        result = validator.validate({"object_store_memory": 100_000_000})
        assert result.has_warnings
        assert any("1GB" in w or "spilling" in w.lower() for w in result.warnings)


class TestValidateConfig:
    """Tests for the validate_config function."""

    def test_validate_config_function(self):
        """Test the main validate_config function."""
        from ray._private.config_validator import validate_config

        result = validate_config({"num_cpus": 4})
        assert result.is_valid

    def test_validate_deprecated_config_mapping(self):
        """Test that deprecated configs are properly handled."""
        from ray._private.config_validator import validate_init_config

        result, transformed = validate_init_config(
            {"plasma_store_memory": 1_000_000_000},
            validation_mode="on"
        )

        # Should have deprecation warning
        assert result.has_warnings
        assert any("deprecated" in w.lower() for w in result.warnings)

        # Should be mapped to new name
        assert "object_store_memory" in transformed or "plasma_store_memory" not in transformed


class TestEnvironmentVariableValidation:
    """Tests for environment variable validation."""

    def test_validate_env_vars_empty(self):
        """Test validating with no RAY_ environment variables."""
        from ray._private.config_validator import validate_environment_variables

        # Clear RAY_ vars for this test
        ray_vars = [k for k in os.environ if k.startswith("RAY_")]
        original_values = {k: os.environ.pop(k) for k in ray_vars}

        try:
            result = validate_environment_variables()
            # Should not fail with no vars
            assert isinstance(result.errors, list)
            assert isinstance(result.warnings, list)
        finally:
            # Restore original values
            for k, v in original_values.items():
                os.environ[k] = v

    def test_validate_env_var_invalid_int(self):
        """Test validation of invalid integer environment variable."""
        from ray._private.config_validator import validate_environment_variables

        original = os.environ.get("RAY_NUM_CPUS")
        try:
            os.environ["RAY_NUM_CPUS"] = "not_a_number"
            result = validate_environment_variables()
            assert result.has_errors
            assert any("RAY_NUM_CPUS" in e for e in result.errors)
        finally:
            if original:
                os.environ["RAY_NUM_CPUS"] = original
            else:
                os.environ.pop("RAY_NUM_CPUS", None)

    def test_validate_env_var_bool_string(self):
        """Test validation of boolean environment variable with string value."""
        from ray._private.config_validator import validate_environment_variables

        original = os.environ.get("RAY_USE_TLS")
        try:
            # "yes" is not a standard boolean value
            os.environ["RAY_USE_TLS"] = "yes"
            result = validate_environment_variables()
            assert result.has_warnings
        finally:
            if original:
                os.environ["RAY_USE_TLS"] = original
            else:
                os.environ.pop("RAY_USE_TLS", None)


class TestValidationModes:
    """Tests for different validation modes."""

    def test_validation_mode_off(self):
        """Test that validation can be disabled."""
        from ray._private.config_validator import validate_init_config

        # Invalid config that would normally fail
        result, _ = validate_init_config(
            {"num_cpus": -100},
            validation_mode="off"
        )
        # Should pass because validation is off
        assert not result.has_errors
        assert not result.has_warnings

    def test_validation_mode_on(self):
        """Test normal validation mode."""
        from ray._private.config_validator import validate_init_config

        result, _ = validate_init_config(
            {"num_cpus": -100},
            validation_mode="on"
        )
        # Should have errors
        assert result.has_errors

    def test_validation_mode_strict(self):
        """Test strict validation mode treats warnings as actionable."""
        from ray._private.config_validator import validate_init_config

        # This should produce a warning (deprecated config)
        result, _ = validate_init_config(
            {"local_mode": True},
            validation_mode="strict"
        )
        # Should have warnings
        assert result.has_warnings


class TestCLIValidation:
    """Tests for CLI validation tool."""

    def test_validate_yaml_config(self):
        """Test validating a YAML cluster config file."""
        from click.testing import CliRunner
        from ray.scripts.scripts import validate_config_cli

        runner = CliRunner()

        # Create a temporary config file
        config = {
            "cluster_name": "test-cluster",
            "min_workers": 1,
            "max_workers": 10,
            "provider": {"type": "aws"},
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(config, f)
            config_path = f.name

        try:
            result = runner.invoke(validate_config_cli, [config_path])
            # Should succeed (exit code 0)
            assert result.exit_code == 0
        finally:
            os.unlink(config_path)

    def test_validate_invalid_yaml(self):
        """Test validating an invalid YAML file."""
        from click.testing import CliRunner
        from ray.scripts.scripts import validate_config_cli

        runner = CliRunner()

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write("invalid: yaml: content: [")
            config_path = f.name

        try:
            result = runner.invoke(validate_config_cli, [config_path])
            # Should fail (exit code 1)
            assert result.exit_code == 1
            assert "YAML" in result.output or "error" in result.output.lower()
        finally:
            os.unlink(config_path)

    def test_validate_config_with_errors(self):
        """Test validating a config with errors."""
        from click.testing import CliRunner
        from ray.scripts.scripts import validate_config_cli

        runner = CliRunner()

        # Config with invalid min_workers
        config = {
            "cluster_name": "test-cluster",
            "min_workers": -1,  # Invalid
            "max_workers": 10,
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(config, f)
            config_path = f.name

        try:
            result = runner.invoke(validate_config_cli, [config_path])
            # Should fail
            assert result.exit_code == 1
            assert "min_workers" in result.output
        finally:
            os.unlink(config_path)

    def test_validate_env_vars_flag(self):
        """Test the --env-vars flag."""
        from click.testing import CliRunner
        from ray.scripts.scripts import validate_config_cli

        runner = CliRunner()
        result = runner.invoke(validate_config_cli, ["--env-vars"])
        # Should complete (may have warnings but shouldn't crash)
        assert result.exit_code in [0, 1]

    def test_no_args_shows_usage(self):
        """Test that no arguments shows usage information."""
        from click.testing import CliRunner
        from ray.scripts.scripts import validate_config_cli

        runner = CliRunner()
        result = runner.invoke(validate_config_cli, [])
        assert result.exit_code == 1
        assert "Usage" in result.output


class TestIntegrationWithRayInit:
    """Integration tests for ray.init() validation."""

    def test_ray_init_with_validation_off(self):
        """Test ray.init with validation disabled."""
        import ray

        # This test just checks that the parameter is accepted
        # We don't actually call ray.init() to avoid side effects
        # The parameter should be in the valid kwargs
        from ray._private.config_schema import CONFIG_SCHEMA
        assert "_config_validation" in CONFIG_SCHEMA

    def test_deprecation_in_schema(self):
        """Test that deprecated configs are marked in schema."""
        from ray._private.config_schema import CONFIG_SCHEMA

        # local_mode should be deprecated
        assert CONFIG_SCHEMA["local_mode"].get("deprecated") is True

        # plasma_store_memory should be deprecated
        assert CONFIG_SCHEMA["plasma_store_memory"].get("deprecated") is True


class TestEdgeCases:
    """Tests for edge cases and corner scenarios."""

    def test_none_values_skip_validation(self):
        """Test that None values are skipped during validation."""
        from ray._private.config_validator import ConfigValidator

        validator = ConfigValidator()
        result = validator.validate({
            "num_cpus": None,
            "num_gpus": None,
        })
        # None values should not cause errors
        assert not result.has_errors

    def test_callable_bounds(self):
        """Test that callable bounds are resolved."""
        from ray._private.config_validator import ConfigValidator
        from ray._private.config_schema import _get_system_memory

        validator = ConfigValidator()
        # Object store memory with callable max
        system_mem = _get_system_memory()
        result = validator.validate({
            "object_store_memory": system_mem + 1000
        })
        # Should fail because it exceeds system memory
        assert result.has_errors

    def test_empty_string_handling(self):
        """Test handling of empty strings."""
        from ray._private.config_validator import ConfigValidator

        validator = ConfigValidator()
        result = validator.validate({"namespace": ""})
        # Empty string should be valid for string type
        assert not result.has_errors

    def test_conflict_detection(self):
        """Test detection of conflicting configurations."""
        from ray._private.config_validator import ConfigValidator

        validator = ConfigValidator()
        result = validator.validate({
            "num_cpus": 4,
            "resources": {"CPU": 8},
        })
        # Should warn about conflict
        assert result.has_warnings
        assert any("conflict" in w.lower() for w in result.warnings)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
