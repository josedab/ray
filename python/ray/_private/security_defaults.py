"""Security defaults for Ray.

This module provides secure-by-default configuration for Ray clusters,
including automatic TLS certificate generation, authentication token management,
and development mode support.

RFC: RFC-0001-security-defaults
"""

import logging
import os
import secrets
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Environment variable for development mode
RAY_DEVELOPMENT_MODE_ENV_VAR = "RAY_DEVELOPMENT_MODE"

# Environment variable for legacy security mode (v2.x behavior)
RAY_LEGACY_SECURITY_MODE_ENV_VAR = "RAY_LEGACY_SECURITY_MODE"

# Default paths for security credentials
DEFAULT_RAY_CERTS_DIR = Path.home() / ".ray" / "certs"
DEFAULT_RAY_AUTH_TOKEN_PATH = Path.home() / ".ray" / "auth_token"

# Warning interval for development mode (5 minutes)
DEVELOPMENT_MODE_WARNING_INTERVAL_SECONDS = 300


@dataclass
class TLSConfig:
    """TLS configuration for Ray cluster communication."""

    cert_path: str
    key_path: str
    ca_path: str
    enabled: bool = True

    def validate(self) -> None:
        """Validate that all TLS files exist."""
        if not self.enabled:
            return

        for path_name, path_value in [
            ("cert_path", self.cert_path),
            ("key_path", self.key_path),
            ("ca_path", self.ca_path),
        ]:
            if not os.path.exists(path_value):
                raise FileNotFoundError(
                    f"TLS {path_name} not found: {path_value}"
                )


class DevelopmentModeWarningThread(threading.Thread):
    """Background thread that periodically warns about insecure development mode."""

    def __init__(self):
        super().__init__(daemon=True, name="DevelopmentModeWarning")
        self._stop_event = threading.Event()

    def run(self):
        """Run the warning thread."""
        while not self._stop_event.is_set():
            self._stop_event.wait(DEVELOPMENT_MODE_WARNING_INTERVAL_SECONDS)
            if not self._stop_event.is_set():
                print(
                    "\n"
                    + "=" * 70 + "\n"
                    "WARNING: Running in development mode. TLS and authentication\n"
                    "are disabled. Do not use in production.\n"
                    + "=" * 70 + "\n",
                    file=sys.stderr,
                )

    def stop(self):
        """Stop the warning thread."""
        self._stop_event.set()


# Global warning thread instance
_development_mode_warning_thread: Optional[DevelopmentModeWarningThread] = None


def is_development_mode_enabled() -> bool:
    """Check if development mode is enabled via environment variable.

    Returns:
        bool: True if RAY_DEVELOPMENT_MODE is set to "1" or "true"
    """
    return os.environ.get(RAY_DEVELOPMENT_MODE_ENV_VAR, "0").lower() in ("1", "true")


def is_legacy_security_mode_enabled() -> bool:
    """Check if legacy security mode is enabled via environment variable.

    Legacy security mode preserves v2.x behavior where security is disabled
    by default.

    Returns:
        bool: True if RAY_LEGACY_SECURITY_MODE is set to "1" or "true"
    """
    return os.environ.get(RAY_LEGACY_SECURITY_MODE_ENV_VAR, "0").lower() in (
        "1",
        "true",
    )


def enable_development_mode() -> None:
    """Enable development mode with security disabled.

    This function:
    1. Prints a warning to stderr
    2. Starts a background thread that periodically warns about insecure mode
    3. Sets environment variables to disable TLS and authentication
    """
    global _development_mode_warning_thread

    # Print initial warning
    print(
        "\n"
        + "=" * 70 + "\n"
        "WARNING: Running in development mode. TLS and authentication are\n"
        "disabled. Do not use in production.\n"
        + "=" * 70 + "\n",
        file=sys.stderr,
    )

    # Start the periodic warning thread if not already running
    if _development_mode_warning_thread is None or not _development_mode_warning_thread.is_alive():
        _development_mode_warning_thread = DevelopmentModeWarningThread()
        _development_mode_warning_thread.start()

    # Disable TLS and authentication
    os.environ["RAY_USE_TLS"] = "0"
    os.environ["RAY_AUTH_MODE"] = "disabled"

    logger.info("Development mode enabled - TLS and authentication disabled")


def disable_development_mode_warnings() -> None:
    """Stop the development mode warning thread."""
    global _development_mode_warning_thread

    if _development_mode_warning_thread is not None:
        _development_mode_warning_thread.stop()
        _development_mode_warning_thread = None


def generate_self_signed_cert(cert_dir: Optional[Path] = None) -> TLSConfig:
    """Generate self-signed TLS certificates for Ray cluster.

    Args:
        cert_dir: Directory to store certificates. Defaults to ~/.ray/certs/

    Returns:
        TLSConfig with paths to the generated certificates

    Raises:
        ImportError: If cryptography library is not installed
    """
    from ray._private.tls_utils import generate_self_signed_tls_certs

    if cert_dir is None:
        cert_dir = DEFAULT_RAY_CERTS_DIR

    cert_dir = Path(cert_dir)
    cert_dir.mkdir(parents=True, exist_ok=True)

    cert_path = cert_dir / "server.crt"
    key_path = cert_dir / "server.key"
    ca_path = cert_dir / "ca.crt"

    # Generate certificates
    cert_contents, key_contents = generate_self_signed_tls_certs()

    # Write certificate
    with open(cert_path, "w") as f:
        f.write(cert_contents)
    os.chmod(cert_path, 0o644)

    # Write private key (restricted permissions)
    with open(key_path, "w") as f:
        f.write(key_contents)
    os.chmod(key_path, 0o600)

    # For self-signed certs, CA cert is the same as server cert
    with open(ca_path, "w") as f:
        f.write(cert_contents)
    os.chmod(ca_path, 0o644)

    logger.info(f"Generated TLS certificates in {cert_dir}")

    return TLSConfig(
        cert_path=str(cert_path),
        key_path=str(key_path),
        ca_path=str(ca_path),
        enabled=True,
    )


def get_default_tls_config(cert_dir: Optional[Path] = None) -> TLSConfig:
    """Get or generate default TLS configuration.

    If certificates don't exist in the cert_dir, they will be generated.

    Args:
        cert_dir: Directory for certificates. Defaults to ~/.ray/certs/

    Returns:
        TLSConfig with paths to the certificates
    """
    if cert_dir is None:
        cert_dir = DEFAULT_RAY_CERTS_DIR

    cert_dir = Path(cert_dir)
    cert_path = cert_dir / "server.crt"
    key_path = cert_dir / "server.key"
    ca_path = cert_dir / "ca.crt"

    # Check if all certificates exist
    if not all(p.exists() for p in [cert_path, key_path, ca_path]):
        return generate_self_signed_cert(cert_dir)

    logger.info(f"Using existing TLS certificates from {cert_dir}")

    return TLSConfig(
        cert_path=str(cert_path),
        key_path=str(key_path),
        ca_path=str(ca_path),
        enabled=True,
    )


def generate_auth_token(token_path: Optional[Path] = None) -> str:
    """Generate a secure authentication token and save it to disk.

    Args:
        token_path: Path to save the token. Defaults to ~/.ray/auth_token

    Returns:
        The generated authentication token
    """
    if token_path is None:
        token_path = DEFAULT_RAY_AUTH_TOKEN_PATH

    token_path = Path(token_path)

    # Generate a cryptographically secure token
    token = secrets.token_urlsafe(32)

    # Create directory if needed
    token_path.parent.mkdir(parents=True, exist_ok=True)

    # Write token with restricted permissions
    with open(token_path, "w") as f:
        f.write(token)
    os.chmod(token_path, 0o600)

    logger.info(f"Generated authentication token in {token_path}")

    return token


def get_default_auth_token(token_path: Optional[Path] = None) -> str:
    """Get or generate default authentication token.

    If the token file doesn't exist, a new token will be generated.

    Args:
        token_path: Path to the token file. Defaults to ~/.ray/auth_token

    Returns:
        The authentication token
    """
    if token_path is None:
        token_path = DEFAULT_RAY_AUTH_TOKEN_PATH

    token_path = Path(token_path)

    if not token_path.exists():
        return generate_auth_token(token_path)

    with open(token_path) as f:
        token = f.read().strip()

    logger.info(f"Using existing authentication token from {token_path}")

    return token


def setup_secure_defaults(
    development_mode: bool = False,
    legacy_security_mode: bool = False,
    tls_cert_path: Optional[str] = None,
    tls_key_path: Optional[str] = None,
    tls_ca_path: Optional[str] = None,
    auth_token: Optional[str] = None,
    auth_token_path: Optional[str] = None,
) -> None:
    """Set up secure defaults for Ray.

    This function configures TLS and authentication based on the provided
    parameters and environment variables.

    Args:
        development_mode: If True, disables all security with warnings
        legacy_security_mode: If True, uses v2.x behavior (security disabled)
        tls_cert_path: Custom path to TLS certificate
        tls_key_path: Custom path to TLS private key
        tls_ca_path: Custom path to TLS CA certificate
        auth_token: Custom authentication token
        auth_token_path: Custom path to authentication token file
    """
    # Check environment variables for mode overrides
    if is_development_mode_enabled():
        development_mode = True
    if is_legacy_security_mode_enabled():
        legacy_security_mode = True

    # Development mode takes precedence - disable everything
    if development_mode:
        enable_development_mode()
        return

    # Legacy mode - preserve v2.x behavior
    if legacy_security_mode:
        logger.info("Legacy security mode enabled - using v2.x defaults")
        return

    # Set up TLS
    if tls_cert_path and tls_key_path and tls_ca_path:
        # Use custom certificates
        tls_config = TLSConfig(
            cert_path=tls_cert_path,
            key_path=tls_key_path,
            ca_path=tls_ca_path,
            enabled=True,
        )
        tls_config.validate()
    else:
        # Generate or load default certificates
        try:
            tls_config = get_default_tls_config()
        except ImportError as e:
            logger.warning(
                f"Could not set up TLS (cryptography library not available): {e}. "
                "Install cryptography with: pip install cryptography"
            )
            tls_config = None

    # Set TLS environment variables
    if tls_config and tls_config.enabled:
        os.environ["RAY_USE_TLS"] = "1"
        os.environ["RAY_TLS_SERVER_CERT"] = tls_config.cert_path
        os.environ["RAY_TLS_SERVER_KEY"] = tls_config.key_path
        os.environ["RAY_TLS_CA_CERT"] = tls_config.ca_path
        logger.info(f"TLS enabled with certificates from {Path(tls_config.cert_path).parent}")

    # Set up authentication
    if auth_token:
        # Use provided token
        token = auth_token
    elif auth_token_path:
        # Load token from custom path
        with open(auth_token_path) as f:
            token = f.read().strip()
    else:
        # Generate or load default token
        token = get_default_auth_token()

    # Set authentication environment variables
    os.environ["RAY_AUTH_MODE"] = "token"
    os.environ["RAY_AUTH_TOKEN"] = token
    logger.info(f"Authentication enabled with token from {DEFAULT_RAY_AUTH_TOKEN_PATH}")


def cleanup_security_resources() -> None:
    """Clean up security-related resources.

    This function should be called during shutdown to clean up any
    background threads or resources.
    """
    disable_development_mode_warnings()
