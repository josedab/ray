# Ray Security Quick Reference

## Key File Locations
| Component | Location | Key Files |
|-----------|----------|-----------|
| **Authentication** | `python/ray/_private/authentication/` | `authentication_utils.py`, `grpc_authentication_*.py`, `http_token_authentication.py` |
| **TLS/Encryption** | `python/ray/_private/` | `tls_utils.py`, `grpc_utils.py` |
| **Dashboard Auth** | `python/ray/dashboard/` | `http_server_head.py` (middleware setup), `routes.py` |
| **Documentation** | `doc/source/` | `ray-security/index.md`, `runtime_env_auth.md`, `kuberay-auth.md` |

## Environment Variables for Security

### Authentication
```bash
RAY_AUTH_MODE=token          # Enable token auth (or 'k8s', 'disabled')
RAY_AUTH_TOKEN=<token>       # Direct token value (prefer file)
RAY_AUTH_TOKEN_PATH=<path>   # Path to token file (~/.ray/auth_token default)
```

### TLS/Encryption
```bash
RAY_USE_TLS=1                           # Enable TLS (default: 0)
RAY_TLS_SERVER_CERT=/path/to/cert.pem  # Server certificate
RAY_TLS_SERVER_KEY=/path/to/key.pem    # Server private key
RAY_TLS_CA_CERT=/path/to/ca.pem        # CA certificate (for client auth)
```

### Other
```bash
RAY_DASHBOARD_BUILD_FOLLOW_SYMLINKS=1  # SECURITY RISK - Allow symlinks
```

## Security Checklist

### Minimal Security (Development)
- [ ] Local network only
- [ ] Authentication disabled
- [ ] TLS disabled

### Basic Security (Single-node)
- [ ] VPN or controlled network
- [ ] `RAY_AUTH_MODE=token` enabled
- [ ] `RAY_USE_TLS=1` enabled
- [ ] Token stored securely (file with `chmod 600`)

### Production (Single Cluster)
- [ ] Isolated VPC/network
- [ ] `RAY_AUTH_MODE=token` or `k8s`
- [ ] `RAY_USE_TLS=1` with valid certificates
- [ ] External proxy for authentication
- [ ] Audit logging configured
- [ ] Network policies restrict access
- [ ] No credential exposure in logs

### Production (Multi-Tenant)
- [ ] **SEPARATE RAY CLUSTERS** per tenant
- [ ] All above + cluster isolation
- [ ] External authorization proxy
- [ ] Namespace/RBAC at cluster level
- [ ] Audit trail with tenant isolation

## Authentication Flow

### Token-Based (gRPC)
```
Client                      Server
  |                           |
  +-- Bearer <token> -------> |
  |    (in metadata)          |
  |                           | validate_request_token()
  |                           | (equality check in C++)
  | <------ OK/FAIL --------- +
```

### HTTP Dashboard
```
Browser                                Server
  |                                       |
  +-- POST /api/authenticate ----------> |
  |    {"Authorization": "Bearer <token>"} 
  |                                       | validate_request_token()
  |                                       | set HttpOnly cookie
  | <-- 200 + Set-Cookie: auth_token -- +
  |                                       |
  +-- GET /api/jobs -------- (cookie) --> |
  |                                       | token_auth_middleware
  | <------ Protected data ------------- +
```

## Vulnerability Quick Summary

### CRITICAL (Must Fix Before Production)
1. **No Sandboxing** - Code runs with full Ray privileges
2. **TLS Optional** - Disabled by default
3. **No Multi-tenant Isolation** - Jobs can interfere in same cluster
4. **Pickle RCE** - Deserialization allows arbitrary code execution

### HIGH (Strong Recommendation)
5. **No RBAC** - All users have equal permissions
6. **Token in Plaintext** - Without TLS, visible on network
7. **Resource Exhaustion** - One job can starve cluster
8. **Environment Variable Leakage** - Credentials visible in ps/logs

### MEDIUM (Best Practice)
9. **No Token Expiration** - 30-day validity indefinite
10. **No Rate Limiting** - Brute force not prevented
11. **Plaintext Token Storage** - Files readable by any user
12. **No Key Rotation** - Manual rotation required

## Recommended Deployment Architecture

```
┌─────────────────────────────────────────────────┐
│           Internet / Untrusted Network          │
└────────────────────┬────────────────────────────┘
                     │
                     │ HTTPS/TLS
                     ▼
┌──────────────────────────────────────────────┐
│         Authentication Proxy                  │
│  (OAuth2-Proxy, kube-rbac-proxy, etc.)       │
│  - Validates tokens                          │
│  - Enforces authorization                    │
└─────────────────┬──────────────────────────┘
                  │
                  │ Internal TLS (RAY_USE_TLS=1)
                  ▼
         ┌────────────────────┐
         │   Ray Dashboard    │
         │  (port 8265)       │
         └────────┬───────────┘
                  │
                  │ gRPC (TLS + Auth)
                  ▼
         ┌────────────────────┐
         │    Ray Cluster     │
         │  (Head + Workers)  │
         └────────────────────┘
```

## Code Examples

### Enabling Token Auth
```python
import os
os.environ["RAY_AUTH_MODE"] = "token"
os.environ["RAY_AUTH_TOKEN"] = "my_secret_token_here"
# Or from file:
os.environ["RAY_AUTH_TOKEN_PATH"] = "/path/to/token/file"

import ray
ray.init()
```

### Enabling TLS
```python
import os
os.environ["RAY_USE_TLS"] = "1"
os.environ["RAY_TLS_SERVER_CERT"] = "/path/to/cert.pem"
os.environ["RAY_TLS_SERVER_KEY"] = "/path/to/key.pem"
os.environ["RAY_TLS_CA_CERT"] = "/path/to/ca.pem"

import ray
ray.init()
```

### Multi-cluster Isolation (Recommended)
```python
import ray
import os

# Each tenant gets their own cluster
def init_tenant_cluster(tenant_id):
    os.environ["RAY_HEAD_SERVICE_HOST"] = f"ray-{tenant_id}-head"
    os.environ["RAY_HEAD_SERVICE_PORT"] = "6379"
    os.environ["RAY_AUTH_TOKEN"] = get_tenant_token(tenant_id)
    ray.init()

# Tenant A
init_tenant_cluster("tenant-a")
# Runs on ray-tenant-a-head:6379

# Tenant B (separate cluster)
ray.shutdown()  # Always disconnect from previous
init_tenant_cluster("tenant-b")
# Runs on ray-tenant-b-head:6379
```

## Testing Security

### Test Token Auth
```bash
# Without token - should fail
curl http://localhost:8265/api/version

# With token - should succeed
curl -H "Authorization: Bearer $(cat ~/.ray/auth_token)" \
     http://localhost:8265/api/version
```

### Test TLS
```bash
# Insecure (fails if TLS required)
openssl s_client -connect localhost:6379

# With certificate
openssl s_client -connect localhost:6379 \
  -cert client.pem \
  -key client-key.pem \
  -CAfile ca.pem
```

### Check Running Security Config
```python
import ray
from ray._raylet import get_authentication_mode, AuthenticationMode

ray.init()
auth_mode = get_authentication_mode()
print(f"Auth Mode: {auth_mode}")
# Output: AuthenticationMode.TOKEN or AuthenticationMode.DISABLED
```

## References

- **Ray Security Docs:** `/doc/source/ray-security/index.md`
- **Runtime Env Auth:** `/doc/source/ray-core/runtime_env_auth.md`
- **KubeRay Auth:** `/doc/source/cluster/kubernetes/user-guides/kuberay-auth.md`
- **SECURITY.md:** `security@anyscale.com` for vulnerability reports
- **Source Code:**
  - `python/ray/_private/authentication/` - Auth implementation
  - `python/ray/_private/tls_utils.py` - TLS setup
  - `src/ray/rpc/authentication/` - C++ auth layer

## Key Takeaways

1. **Ray assumes trusted network** - Deploy in controlled environments
2. **Enforce security outside Ray** - Use proxies, firewalls, isolation
3. **TLS is opt-in** - Always enable with valid certificates
4. **Token auth is basic** - Combine with network security
5. **No multi-tenant isolation** - Use separate clusters per tenant
6. **Code execution is unrestricted** - Only allow trusted submitters
7. **Audit externally** - Log at infrastructure level, not Ray

## Additional Resources

### External Authentication Solutions
- `kube-rbac-proxy` - Kubernetes RBAC enforcement
- `oauth2-proxy` - OAuth/OIDC proxy
- HashiCorp Vault - Secrets management
- AWS Secrets Manager - Cloud secrets

### Monitoring & Logging
- Prometheus metrics on Ray dashboard
- Application Performance Monitoring (APM)
- Centralized logging (ELK, Splunk, etc.)
- Network monitoring (TCPDump, ntopng)

