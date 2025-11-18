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

"""Tests for the unified configuration system."""

import os
import tempfile
import pytest
import yaml

from ray._private.unified_config import (
    RayConfig,
    ResourceConfig,
    ObjectStoreConfig,
    SchedulingConfig,
    SecurityConfig,
    ObservabilityConfig,
    SpillingConfig,
    TLSConfig,
    AuthenticationConfig,
    MetricsConfig,
    LoggingConfig,
    DashboardConfig,
    ConfigLoader,
    convert_legacy_config,
    generate_config_template,
    _parse_memory_string,
    _parse_duration_string,
)


class TestMemoryParsing:
    """Tests for memory string parsing."""

    def test_parse_bytes(self):
        assert _parse_memory_string(1024) == 1024
        assert _parse_memory_string(1024.5) == 1024

    def test_parse_kb(self):
        assert _parse_memory_string("1KB") == 1024
        assert _parse_memory_string("1kb") == 1024

    def test_parse_mb(self):
        assert _parse_memory_string("1MB") == 1024**2
        assert _parse_memory_string("1 MB") == 1024**2

    def test_parse_gb(self):
        assert _parse_memory_string("1GB") == 1024**3
        assert _parse_memory_string("8GB") == 8 * 1024**3

    def test_parse_tb(self):
        assert _parse_memory_string("1TB") == 1024**4

    def test_parse_plain_number(self):
        assert _parse_memory_string("1024") == 1024


class TestDurationParsing:
    """Tests for duration string parsing."""

    def test_parse_seconds(self):
        assert _parse_duration_string(10) == 10.0
        assert _parse_duration_string(10.5) == 10.5

    def test_parse_milliseconds(self):
        assert _parse_duration_string("100ms") == 0.1
        assert _parse_duration_string("1000ms") == 1.0

    def test_parse_seconds_string(self):
        assert _parse_duration_string("10s") == 10.0
        assert _parse_duration_string("30s") == 30.0

    def test_parse_minutes(self):
        assert _parse_duration_string("5m") == 300.0
        assert _parse_duration_string("1m") == 60.0

    def test_parse_hours(self):
        assert _parse_duration_string("1h") == 3600.0


class TestResourceConfig:
    """Tests for ResourceConfig."""

    def test_defaults(self):
        config = ResourceConfig()
        assert config.num_cpus is None
        assert config.num_gpus is None
        assert config.memory is None
        assert config.custom == {}

    def test_custom_values(self):
        config = ResourceConfig(num_cpus=4, num_gpus=2)
        assert config.num_cpus == 4
        assert config.num_gpus == 2

    def test_memory_string_conversion(self):
        config = ResourceConfig(memory="8GB")
        assert config.memory == 8 * 1024**3

    def test_validation_negative_cpus(self):
        config = ResourceConfig(num_cpus=-1)
        errors = config.validate()
        assert len(errors) == 1
        assert "num_cpus" in errors[0]


class TestObjectStoreConfig:
    """Tests for ObjectStoreConfig."""

    def test_defaults(self):
        config = ObjectStoreConfig()
        assert config.memory_bytes > 0
        assert config.eviction_threshold == 0.8
        assert config.spilling.enabled is True

    def test_memory_string_conversion(self):
        config = ObjectStoreConfig(memory_bytes="8GB")
        assert config.memory_bytes == 8 * 1024**3

    def test_validation_invalid_threshold(self):
        config = ObjectStoreConfig(eviction_threshold=1.5)
        errors = config.validate()
        assert len(errors) == 1
        assert "eviction_threshold" in errors[0]


class TestSchedulingConfig:
    """Tests for SchedulingConfig."""

    def test_defaults(self):
        config = SchedulingConfig()
        assert config.spread_threshold == 0.5
        assert config.worker_lease_timeout_seconds == 10.0

    def test_duration_string_conversion(self):
        config = SchedulingConfig(worker_lease_timeout_seconds="30s")
        assert config.worker_lease_timeout_seconds == 30.0

    def test_validation_invalid_spread_threshold(self):
        config = SchedulingConfig(spread_threshold=2.0)
        errors = config.validate()
        assert len(errors) == 1
        assert "spread_threshold" in errors[0]


class TestSecurityConfig:
    """Tests for SecurityConfig."""

    def test_defaults(self):
        config = SecurityConfig()
        assert config.tls.enabled is False
        assert config.authentication.mode == "disabled"
        assert config.development_mode is False

    def test_development_mode_skips_validation(self):
        # With development_mode, TLS errors should be ignored
        config = SecurityConfig(
            development_mode=True,
            tls=TLSConfig(enabled=True)  # No paths specified
        )
        errors = config.validate()
        assert len(errors) == 0


class TestObservabilityConfig:
    """Tests for ObservabilityConfig."""

    def test_defaults(self):
        config = ObservabilityConfig()
        assert config.metrics.enabled is True
        assert config.logging.level == "INFO"
        assert config.logging.format == "text"

    def test_logging_validation(self):
        config = ObservabilityConfig(
            logging=LoggingConfig(level="INVALID")
        )
        errors = config.validate()
        assert len(errors) == 1
        assert "logging level" in errors[0]


class TestRayConfig:
    """Tests for RayConfig."""

    def test_defaults(self):
        config = RayConfig()
        assert config.resources is not None
        assert config.object_store is not None
        assert config.scheduling is not None
        assert config.security is not None
        assert config.observability is not None

    def test_to_dict(self):
        config = RayConfig()
        config_dict = config.to_dict()
        assert "resources" in config_dict
        assert "object_store" in config_dict
        assert "scheduling" in config_dict

    def test_to_yaml(self):
        config = RayConfig()
        yaml_str = config.to_yaml()
        parsed = yaml.safe_load(yaml_str)
        assert "resources" in parsed

    def test_from_dict(self):
        data = {
            "resources": {"num_cpus": 4},
            "object_store": {"memory_bytes": 1000000}
        }
        config = RayConfig.from_dict(data)
        assert config.resources.num_cpus == 4
        assert config.object_store.memory_bytes == 1000000

    def test_from_yaml(self):
        yaml_str = """
        resources:
          num_cpus: 8
        scheduling:
          spread_threshold: 0.7
        """
        config = RayConfig.from_yaml(yaml_str)
        assert config.resources.num_cpus == 8
        assert config.scheduling.spread_threshold == 0.7

    def test_diff_from_defaults(self):
        config = RayConfig(
            resources=ResourceConfig(num_cpus=4)
        )
        diff = config.diff_from_defaults()
        assert "resources.num_cpus" in diff
        assert diff["resources.num_cpus"] == (None, 4)

    def test_validate(self):
        config = RayConfig(
            resources=ResourceConfig(num_cpus=-1),
            scheduling=SchedulingConfig(spread_threshold=2.0)
        )
        errors = config.validate()
        assert len(errors) == 2


class TestConfigLoader:
    """Tests for ConfigLoader."""

    def test_load_defaults(self):
        loader = ConfigLoader()
        config = loader.load()
        assert isinstance(config, RayConfig)

    def test_load_from_file(self):
        config_content = """
        resources:
          num_cpus: 16
        object_store:
          memory_bytes: 10000000000
        """
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.yaml', delete=False
        ) as f:
            f.write(config_content)
            f.flush()
            filepath = f.name

        try:
            loader = ConfigLoader(config_file=filepath)
            config = loader.load()
            assert config.resources.num_cpus == 16
            assert config.object_store.memory_bytes == 10000000000
        finally:
            os.unlink(filepath)

    def test_load_from_env(self):
        # Set environment variables
        env_vars = {
            'RAY_RESOURCES_NUM_CPUS': '8',
            'RAY_OBJECT_STORE_MEMORY': '5000000000',
            'RAY_SCHEDULING_SPREAD_THRESHOLD': '0.3',
        }

        # Save original values and set new ones
        original = {}
        for key, value in env_vars.items():
            original[key] = os.environ.get(key)
            os.environ[key] = value

        try:
            loader = ConfigLoader()
            config = loader.load()
            assert config.resources.num_cpus == 8
            assert config.object_store.memory_bytes == 5000000000
            assert config.scheduling.spread_threshold == 0.3
        finally:
            # Restore original values
            for key, value in original.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    def test_load_with_overrides(self):
        overrides = {
            "resources": {"num_cpus": 32},
            "scheduling": {"spread_threshold": 0.9}
        }
        loader = ConfigLoader()
        config = loader.load(overrides=overrides)
        assert config.resources.num_cpus == 32
        assert config.scheduling.spread_threshold == 0.9

    def test_env_overrides_file(self):
        """Test that environment variables override file config."""
        config_content = """
        resources:
          num_cpus: 4
        """
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.yaml', delete=False
        ) as f:
            f.write(config_content)
            f.flush()
            filepath = f.name

        original = os.environ.get('RAY_RESOURCES_NUM_CPUS')
        os.environ['RAY_RESOURCES_NUM_CPUS'] = '16'

        try:
            loader = ConfigLoader(config_file=filepath)
            config = loader.load()
            # Environment should override file
            assert config.resources.num_cpus == 16
        finally:
            os.unlink(filepath)
            if original is None:
                os.environ.pop('RAY_RESOURCES_NUM_CPUS', None)
            else:
                os.environ['RAY_RESOURCES_NUM_CPUS'] = original

    def test_get_env_var_mappings(self):
        mappings = ConfigLoader.get_env_var_mappings()
        assert 'RAY_RESOURCES_NUM_CPUS' in mappings
        assert mappings['RAY_RESOURCES_NUM_CPUS'] == 'resources.num_cpus'


class TestLegacyConfigConversion:
    """Tests for converting legacy config to new format."""

    def test_convert_basic_params(self):
        config = convert_legacy_config(
            num_cpus=4,
            num_gpus=2,
            object_store_memory=1000000
        )
        assert config.resources.num_cpus == 4
        assert config.resources.num_gpus == 2
        assert config.object_store.memory_bytes == 1000000

    def test_convert_dashboard_params(self):
        config = convert_legacy_config(
            include_dashboard=True,
            dashboard_host="0.0.0.0",
            dashboard_port=8080
        )
        assert config.dashboard.enabled is True
        assert config.dashboard.host == "0.0.0.0"
        assert config.dashboard.port == 8080

    def test_convert_address_and_namespace(self):
        config = convert_legacy_config(
            address="auto",
            namespace="my_namespace"
        )
        assert config.address == "auto"
        assert config.namespace == "my_namespace"


class TestConfigTemplate:
    """Tests for configuration template generation."""

    def test_generate_template(self):
        template = generate_config_template()
        assert "resources:" in template
        assert "object_store:" in template
        assert "scheduling:" in template
        assert "security:" in template
        assert "observability:" in template

    def test_template_is_valid_yaml(self):
        template = generate_config_template()
        # Should parse without errors
        parsed = yaml.safe_load(template)
        assert parsed is not None
        assert "resources" in parsed


class TestYamlRoundTrip:
    """Test that configs can be serialized and deserialized."""

    def test_roundtrip(self):
        original = RayConfig(
            resources=ResourceConfig(num_cpus=4, num_gpus=2),
            object_store=ObjectStoreConfig(
                memory_bytes=8*1024**3,
                eviction_threshold=0.7
            ),
            scheduling=SchedulingConfig(spread_threshold=0.6)
        )

        yaml_str = original.to_yaml()
        restored = RayConfig.from_yaml(yaml_str)

        assert restored.resources.num_cpus == 4
        assert restored.resources.num_gpus == 2
        assert restored.object_store.memory_bytes == 8*1024**3
        assert restored.object_store.eviction_threshold == 0.7
        assert restored.scheduling.spread_threshold == 0.6


class TestFileOperations:
    """Test file-based configuration operations."""

    def test_from_yaml_file(self):
        config_content = """
        resources:
          num_cpus: 4
          custom:
            special_hardware: 2
        """
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.yaml', delete=False
        ) as f:
            f.write(config_content)
            f.flush()
            filepath = f.name

        try:
            config = RayConfig.from_yaml_file(filepath)
            assert config.resources.num_cpus == 4
            assert config.resources.custom == {"special_hardware": 2}
        finally:
            os.unlink(filepath)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
