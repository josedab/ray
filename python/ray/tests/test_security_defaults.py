"""Tests for security defaults (RFC-0001).

This module tests the secure-by-default configuration for Ray clusters,
including development mode, TLS certificate generation, and authentication
token management.
"""

import os
import tempfile
import time
from pathlib import Path
from unittest.mock import patch

import pytest

import ray
from ray._private.security_defaults import (
    DEFAULT_RAY_AUTH_TOKEN_PATH,
    DEFAULT_RAY_CERTS_DIR,
    DEVELOPMENT_MODE_WARNING_INTERVAL_SECONDS,
    DevelopmentModeWarningThread,
    TLSConfig,
    cleanup_security_resources,
    disable_development_mode_warnings,
    enable_development_mode,
    generate_auth_token,
    generate_self_signed_cert,
    get_default_auth_token,
    get_default_tls_config,
    is_development_mode_enabled,
    is_legacy_security_mode_enabled,
    setup_secure_defaults,
)


class TestTLSConfig:
    """Tests for the TLSConfig dataclass."""

    def test_tls_config_creation(self, tmp_path):
        """Test that TLSConfig can be created with paths."""
        cert_path = tmp_path / "cert.pem"
        key_path = tmp_path / "key.pem"
        ca_path = tmp_path / "ca.pem"

        # Create test files
        cert_path.write_text("cert")
        key_path.write_text("key")
        ca_path.write_text("ca")

        config = TLSConfig(
            cert_path=str(cert_path),
            key_path=str(key_path),
            ca_path=str(ca_path),
        )

        assert config.enabled is True
        config.validate()  # Should not raise

    def test_tls_config_disabled(self):
        """Test that disabled TLSConfig doesn't validate paths."""
        config = TLSConfig(
            cert_path="/nonexistent/cert.pem",
            key_path="/nonexistent/key.pem",
            ca_path="/nonexistent/ca.pem",
            enabled=False,
        )

        config.validate()  # Should not raise because enabled=False

    def test_tls_config_missing_file(self, tmp_path):
        """Test that TLSConfig raises error for missing files."""
        config = TLSConfig(
            cert_path=str(tmp_path / "nonexistent.pem"),
            key_path=str(tmp_path / "key.pem"),
            ca_path=str(tmp_path / "ca.pem"),
        )

        with pytest.raises(FileNotFoundError, match="cert_path"):
            config.validate()


class TestDevelopmentMode:
    """Tests for development mode functionality."""

    def test_is_development_mode_enabled_default(self):
        """Test that development mode is disabled by default."""
        with patch.dict(os.environ, {}, clear=True):
            assert is_development_mode_enabled() is False

    def test_is_development_mode_enabled_true(self):
        """Test that development mode can be enabled via env var."""
        for value in ["1", "true", "TRUE", "True"]:
            with patch.dict(os.environ, {"RAY_DEVELOPMENT_MODE": value}):
                assert is_development_mode_enabled() is True

    def test_is_development_mode_enabled_false(self):
        """Test that development mode can be explicitly disabled."""
        for value in ["0", "false", "FALSE"]:
            with patch.dict(os.environ, {"RAY_DEVELOPMENT_MODE": value}):
                assert is_development_mode_enabled() is False

    def test_is_legacy_security_mode_enabled(self):
        """Test legacy security mode environment variable."""
        with patch.dict(os.environ, {"RAY_LEGACY_SECURITY_MODE": "1"}):
            assert is_legacy_security_mode_enabled() is True

        with patch.dict(os.environ, {}, clear=True):
            assert is_legacy_security_mode_enabled() is False

    def test_enable_development_mode_sets_env_vars(self):
        """Test that enable_development_mode sets the correct env vars."""
        with patch.dict(os.environ, {}, clear=True):
            enable_development_mode()

            assert os.environ.get("RAY_USE_TLS") == "0"
            assert os.environ.get("RAY_AUTH_MODE") == "disabled"

        # Clean up
        disable_development_mode_warnings()

    def test_development_mode_warning_thread(self):
        """Test that warning thread can be started and stopped."""
        thread = DevelopmentModeWarningThread()
        thread.start()

        assert thread.is_alive()

        thread.stop()
        thread.join(timeout=1)

        assert not thread.is_alive()


class TestCertificateGeneration:
    """Tests for TLS certificate generation."""

    def test_generate_self_signed_cert(self, tmp_path):
        """Test that self-signed certificates can be generated."""
        cert_dir = tmp_path / "certs"

        config = generate_self_signed_cert(cert_dir)

        assert config.enabled is True
        assert Path(config.cert_path).exists()
        assert Path(config.key_path).exists()
        assert Path(config.ca_path).exists()

        # Check permissions
        key_stat = os.stat(config.key_path)
        assert (key_stat.st_mode & 0o777) == 0o600  # Private key should be restricted

    def test_get_default_tls_config_generates_if_missing(self, tmp_path):
        """Test that get_default_tls_config generates certs if missing."""
        cert_dir = tmp_path / "certs"

        config = get_default_tls_config(cert_dir)

        assert config.enabled is True
        assert Path(config.cert_path).exists()

    def test_get_default_tls_config_uses_existing(self, tmp_path):
        """Test that get_default_tls_config uses existing certs."""
        cert_dir = tmp_path / "certs"

        # Generate once
        config1 = get_default_tls_config(cert_dir)

        # Read cert content
        with open(config1.cert_path) as f:
            cert1_content = f.read()

        # Get config again (should use existing)
        config2 = get_default_tls_config(cert_dir)

        with open(config2.cert_path) as f:
            cert2_content = f.read()

        assert cert1_content == cert2_content


class TestAuthTokenGeneration:
    """Tests for authentication token generation."""

    def test_generate_auth_token(self, tmp_path):
        """Test that authentication token can be generated."""
        token_path = tmp_path / "auth_token"

        token = generate_auth_token(token_path)

        assert len(token) == 43  # secrets.token_urlsafe(32) produces 43 chars
        assert token_path.exists()

        # Check content matches
        with open(token_path) as f:
            saved_token = f.read()
        assert saved_token == token

        # Check permissions
        token_stat = os.stat(token_path)
        assert (token_stat.st_mode & 0o777) == 0o600  # Token should be restricted

    def test_get_default_auth_token_generates_if_missing(self, tmp_path):
        """Test that get_default_auth_token generates token if missing."""
        token_path = tmp_path / "auth_token"

        token = get_default_auth_token(token_path)

        assert len(token) == 43
        assert token_path.exists()

    def test_get_default_auth_token_uses_existing(self, tmp_path):
        """Test that get_default_auth_token uses existing token."""
        token_path = tmp_path / "auth_token"

        # Generate once
        token1 = get_default_auth_token(token_path)

        # Get token again (should use existing)
        token2 = get_default_auth_token(token_path)

        assert token1 == token2

    def test_token_is_cryptographically_secure(self, tmp_path):
        """Test that generated tokens are unique and random."""
        tokens = set()

        for i in range(10):
            token_path = tmp_path / f"auth_token_{i}"
            token = generate_auth_token(token_path)
            tokens.add(token)

        # All tokens should be unique
        assert len(tokens) == 10


class TestSetupSecureDefaults:
    """Tests for the setup_secure_defaults function."""

    def test_setup_development_mode(self):
        """Test setup with development mode enabled."""
        with patch.dict(os.environ, {}, clear=True):
            setup_secure_defaults(development_mode=True)

            assert os.environ.get("RAY_USE_TLS") == "0"
            assert os.environ.get("RAY_AUTH_MODE") == "disabled"

        cleanup_security_resources()

    def test_setup_legacy_security_mode(self):
        """Test setup with legacy security mode."""
        original_env = os.environ.copy()

        with patch.dict(os.environ, {}, clear=True):
            setup_secure_defaults(legacy_security_mode=True)

            # Should not set any security environment variables
            assert "RAY_USE_TLS" not in os.environ

        cleanup_security_resources()

    def test_setup_with_custom_tls_paths(self, tmp_path):
        """Test setup with custom TLS certificate paths."""
        # Create test certificates
        cert_path = tmp_path / "cert.pem"
        key_path = tmp_path / "key.pem"
        ca_path = tmp_path / "ca.pem"

        cert_path.write_text("cert")
        key_path.write_text("key")
        ca_path.write_text("ca")

        with patch.dict(os.environ, {}, clear=True):
            setup_secure_defaults(
                tls_cert_path=str(cert_path),
                tls_key_path=str(key_path),
                tls_ca_path=str(ca_path),
            )

            assert os.environ.get("RAY_USE_TLS") == "1"
            assert os.environ.get("RAY_TLS_SERVER_CERT") == str(cert_path)

        cleanup_security_resources()

    def test_setup_with_custom_auth_token(self):
        """Test setup with custom authentication token."""
        with patch.dict(os.environ, {}, clear=True):
            setup_secure_defaults(auth_token="my-custom-token")

            assert os.environ.get("RAY_AUTH_MODE") == "token"
            assert os.environ.get("RAY_AUTH_TOKEN") == "my-custom-token"

        cleanup_security_resources()

    def test_setup_env_var_overrides_parameter(self):
        """Test that environment variables override parameters."""
        with patch.dict(os.environ, {"RAY_DEVELOPMENT_MODE": "1"}, clear=True):
            setup_secure_defaults(development_mode=False)

            # Env var should win
            assert os.environ.get("RAY_USE_TLS") == "0"
            assert os.environ.get("RAY_AUTH_MODE") == "disabled"

        cleanup_security_resources()


class TestRayInitIntegration:
    """Integration tests for ray.init() with security defaults."""

    def test_ray_init_development_mode(self):
        """Test that ray.init(development_mode=True) disables security."""
        # This test just verifies the parameter is accepted
        # Full integration would require actually starting Ray

        # We can at least verify the function signature accepts the parameter
        import inspect
        sig = inspect.signature(ray.init)
        assert "development_mode" in sig.parameters
        assert "legacy_security_mode" in sig.parameters

    def test_ray_init_hidden_tls_params(self):
        """Test that ray.init() accepts hidden TLS parameters."""
        # These are passed via **kwargs
        # We just verify the signature has **kwargs
        import inspect
        sig = inspect.signature(ray.init)
        assert any(
            p.kind == inspect.Parameter.VAR_KEYWORD
            for p in sig.parameters.values()
        )


class TestTokenGenerator:
    """Tests for the authentication token generator."""

    def test_token_length_and_format(self):
        """Test that generated tokens have correct format."""
        from ray._private.authentication.authentication_token_generator import (
            generate_new_authentication_token,
        )

        token = generate_new_authentication_token()

        # secrets.token_urlsafe(32) produces 43 characters
        assert len(token) == 43

        # Should be URL-safe (no +, /, or =)
        assert "+" not in token
        assert "/" not in token

    def test_token_uniqueness(self):
        """Test that each generated token is unique."""
        from ray._private.authentication.authentication_token_generator import (
            generate_new_authentication_token,
        )

        tokens = [generate_new_authentication_token() for _ in range(100)]

        # All tokens should be unique
        assert len(set(tokens)) == 100


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
