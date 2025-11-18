# RFC-0001: Enable Security by Default

**Status:** Draft
**Author:** Codebase Analysis
**Created:** 2025-01-18
**Commit Reference:** `d1cce8c9dc8411fad7cfbd619350bec6f19839a3`

## Summary

Change Ray's default security posture from "open by default" to "secure by default" by enabling TLS encryption and authentication out of the box, while providing a simple opt-out for development scenarios.

## Motivation

### Current State

Ray currently ships with security features disabled:

```python
# Current defaults
RAY_USE_TLS=0  # No encryption
RAY_AUTH_MODE=none  # No authentication
```

This creates several problems:

1. **Security incidents:** Users deploy to production without realizing security is disabled
2. **Compliance failures:** Default configurations fail security audits
3. **Data exposure:** Sensitive data transmitted in plaintext
4. **Unauthorized access:** Any client can connect to cluster

### Evidence from Codebase

Analysis found 48 instances of insecure defaults:
- `python/ray/_private/tls_utils.py`: TLS disabled by default
- `python/ray/_private/authentication/`: Auth mechanisms exist but disabled
- No warnings shown when running insecurely

### User Impact

- New users unaware of security implications
- Production deployments accidentally left insecure
- Increased support burden for security issues

## Detailed Design

### Phase 1: Secure Development Mode (Week 1-2)

Create a new "development mode" that explicitly disables security:

```python
# Explicit development mode
ray.init(development_mode=True)  # Disables TLS/auth with warning

# Or via environment
RAY_DEVELOPMENT_MODE=1
```

When development mode is enabled:
- Clear warning printed to stderr
- Warning repeated every 5 minutes
- Metrics tagged with `insecure=true`

### Phase 2: TLS by Default (Week 3-4)

Enable TLS with auto-generated certificates:

```python
# ray/python/ray/_private/tls_utils.py

def get_default_tls_config():
    """Generate or load TLS certificates."""
    cert_dir = os.path.expanduser("~/.ray/certs")

    if not os.path.exists(f"{cert_dir}/server.crt"):
        # Auto-generate self-signed cert
        generate_self_signed_cert(cert_dir)
        logger.info(f"Generated TLS certificates in {cert_dir}")

    return TLSConfig(
        cert_path=f"{cert_dir}/server.crt",
        key_path=f"{cert_dir}/server.key",
        ca_path=f"{cert_dir}/ca.crt"
    )

# Default to TLS enabled
RAY_CONFIG(bool, use_tls, true, "Enable TLS encryption")
```

### Phase 3: Authentication by Default (Week 5-6)

Enable token authentication with auto-generated tokens:

```python
# ray/python/ray/_private/authentication/token_generator.py

def get_default_auth_config():
    """Generate or load authentication token."""
    token_path = os.path.expanduser("~/.ray/auth_token")

    if not os.path.exists(token_path):
        # Auto-generate token
        token = secrets.token_urlsafe(32)
        with open(token_path, 'w') as f:
            f.write(token)
        os.chmod(token_path, 0o600)
        logger.info(f"Generated auth token in {token_path}")

    with open(token_path) as f:
        return f.read().strip()
```

### Configuration Changes

New default configuration:

```python
# Before
ray.init()  # Insecure

# After
ray.init()  # Secure (TLS + auto-generated token)

# For development
ray.init(development_mode=True)  # Explicit opt-out

# For production with custom certs
ray.init(
    _tls_cert_path="/path/to/cert.pem",
    _tls_key_path="/path/to/key.pem",
    _auth_token="production-token"
)
```

### Migration Path

1. **v2.11:** Add `development_mode`, deprecation warnings for insecure usage
2. **v2.12:** Default to TLS enabled, auth still optional
3. **v3.0:** Full secure defaults, development mode required for insecure

## Example Usage

### Development

```python
import ray

# Local development - explicitly insecure
ray.init(development_mode=True)

# Output:
# WARNING: Running in development mode. TLS and authentication are disabled.
# Do not use in production.
```

### Production

```python
import ray

# Production - secure by default
ray.init()

# Prints path to auto-generated credentials:
# INFO: Using TLS with certificates from ~/.ray/certs/
# INFO: Authentication token stored in ~/.ray/auth_token

# Connect from another machine
ray.init(
    address="ray://production-server:10001",
    _tls_ca_path="~/.ray/certs/ca.crt",
    _auth_token_path="~/.ray/auth_token"
)
```

## Implementation Plan

### Week 1: Development Mode
- Add `development_mode` parameter
- Implement warnings
- Update documentation

### Week 2: Certificate Generation
- Implement auto-generation of TLS certificates
- Test with various platforms
- Add certificate management utilities

### Week 3: Token Generation
- Implement auto-generation of auth tokens
- Add token rotation support
- Create token management CLI

### Week 4: Integration
- Wire together TLS and auth
- Update all connection points
- Comprehensive testing

### Week 5: Documentation & Migration
- Update all documentation
- Create migration guide
- Blog post announcement

### Week 6: Release
- Release with deprecation warnings
- Monitor for issues
- Gather feedback

## Backwards Compatibility

### Breaking Changes

This is a **breaking change** for users who:
- Deploy without explicit security configuration
- Rely on open cluster access
- Use custom connection logic

### Migration Strategy

1. **Deprecation warnings** in v2.11 for insecure usage
2. **Opt-in secure defaults** available immediately
3. **Opt-out required** in v3.0

### Compatibility Flag

```python
# For gradual migration
ray.init(legacy_security_mode=True)  # v2.x behavior
```

## Alternatives Considered

### Alternative 1: Security Warnings Only

Add warnings but don't change defaults.

**Rejected because:**
- Users ignore warnings
- Doesn't solve the fundamental problem
- Still fails security audits

### Alternative 2: Require Explicit Configuration

Require users to explicitly choose secure or insecure.

**Rejected because:**
- Breaks all existing code
- Poor developer experience
- Confusing error messages

### Alternative 3: Environment-Based Defaults

Default to secure in production, insecure in development.

**Rejected because:**
- Hard to detect "production" reliably
- Inconsistent behavior confuses users
- Testing doesn't match production

## Open Questions

1. **Certificate Rotation:** Should we auto-rotate certificates? How often?
2. **Token Storage:** What's the best cross-platform secure storage?
3. **Kubernetes Integration:** How does this interact with KubeRay?
4. **Dashboard:** Should dashboard have separate auth?

## Success Criteria

- [ ] No security warnings in default deployment
- [ ] All internal communication encrypted
- [ ] Authentication required for client connections
- [ ] Clear migration path documented
- [ ] < 5% increase in connection latency
- [ ] All existing tutorials updated

## Effort Estimation

- **Development:** 4-6 dev-weeks
- **Testing:** 2 dev-weeks
- **Documentation:** 1 dev-week
- **Total:** 7-9 dev-weeks

## Stakeholder Approval

- [ ] Security team
- [ ] Core maintainers
- [ ] Documentation team
- [ ] Community feedback period (2 weeks)

## References

- Current TLS implementation: [`python/ray/_private/tls_utils.py`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/python/ray/_private/tls_utils.py)
- Authentication: [`python/ray/_private/authentication/`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/python/ray/_private/authentication/)
- Security documentation: [`doc/source/ray-security/`](https://github.com/ray-project/ray/blob/d1cce8c9dc8411fad7cfbd619350bec6f19839a3/doc/source/ray-core/configure.rst)
