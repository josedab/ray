# Ray Security Patterns and Vulnerabilities Analysis

**Date:** November 18, 2025  
**Scope:** Ray Core security architecture, authentication, encryption, and multi-tenancy isolation

---

## Executive Summary

Ray is a distributed computing framework designed for high-performance workloads with **intentional design philosophy of operating in controlled network environments**. The framework provides **token-based authentication, TLS encryption, and gRPC security** but **explicitly delegates responsibility for isolation and access control to the platform/infrastructure layer**.

### Key Finding
Ray is **not designed for untrusted networks** or multi-tenant isolation within a single cluster. Security is **enforced outside Ray** through network controls, external proxies, and multiple Ray clusters for isolation.

---

## 1. Authentication/Authorization

### Current Implementation

#### Token-Based Authentication
- **Location:** `/python/ray/_private/authentication/`
- **Mechanism:** Stateless bearer token validation
- **Token Generation:** UUID-based (see `authentication_token_generator.py`)
  ```python
  def generate_new_authentication_token() -> str:
      return uuid.uuid4().hex  # 32-character hex string
  ```

#### Token Setup
- **Environment Variables:**
  - `RAY_AUTH_MODE`: Set to `token` or `k8s` to enable (defaults to disabled)
  - `RAY_AUTH_TOKEN`: Direct token value
  - `RAY_AUTH_TOKEN_PATH`: Path to token file (default: `~/.ray/auth_token`)

- **Token Loading:** Handled by C++ `AuthenticationTokenLoader` singleton with caching

#### gRPC Authentication
- **Server Interceptor:** `SyncAuthenticationServerInterceptor` and `AsyncAuthenticationServerInterceptor`
- **Client Interceptor:** `SyncAuthenticationMetadataClientInterceptor` and `AsyncAuthenticationMetadataClientInterceptor`
- **Validation:** Tokens validated via C++ layer with simple equality check

#### Dashboard/HTTP Authentication
- **Middleware:** `get_token_auth_middleware()` in `http_token_authentication.py`
- **Cookie Support:** HttpOnly, Secure, SameSite=Strict cookies set after successful authentication
- **Endpoints:** 
  - Public: `/`, `/static/*`, `/api/authenticate`, `/api/authentication_mode`
  - Protected: All other API endpoints

### Vulnerabilities & Concerns

| Issue | Severity | Details |
|-------|----------|---------|
| **Token Generation Strength** | MEDIUM | UUID-based tokens (128 bits of entropy) may be sufficient but no documented cryptographic guarantees. No HMAC or signing. |
| **Token Storage** | MEDIUM | Tokens stored in plaintext in files with default umask. No file permission validation before reading. |
| **Token Transmission** | MEDIUM | TLS encryption is optional via `RAY_USE_TLS`. Without TLS, tokens are sent in plaintext over HTTP. |
| **No Token Expiration** | MEDIUM | Token cookies set for 30 days without expiration field. No server-side revocation mechanism. |
| **No Rate Limiting** | MEDIUM | Authentication endpoints have no rate limiting on token validation attempts. |
| **K8s Mode Limited** | MEDIUM | K8s authentication mode documented but integration details unclear. Relies on external proxy. |
| **Bearer Token Validation** | LOW | Validation expects full "Bearer <token>" format; case-insensitive prefix matching could be more robust. |

---

## 2. Network Security

### TLS/SSL Configuration

#### Implementation Details
- **Location:** `/python/ray/_private/tls_utils.py`
- **Certificate Loading:** Environment variables only
  ```
  RAY_USE_TLS=1  (or "true")
  RAY_TLS_SERVER_CERT=<path to cert>
  RAY_TLS_SERVER_KEY=<path to key>
  RAY_TLS_CA_CERT=<path to CA cert>
  ```

#### TLS Setup
- **Server:** `grpc.ssl_server_credentials()` with optional client auth
  ```python
  credentials = grpc.ssl_server_credentials(
      [(private_key, server_cert_chain)],
      root_certificates=ca_cert,
      require_client_auth=ca_cert is not None  # mTLS if CA provided
  )
  ```
- **Client:** `grpc.ssl_channel_credentials()` for secure channels
- **Self-signed Cert Generation:** Uses `cryptography` library with SHA256, 2048-bit RSA
  ```python
  key = rsa.generate_private_key(
      public_exponent=65537, key_size=2048, backend=default_backend()
  )
  ```

#### gRPC Keepalive
- Configurable keepalive settings from Ray config
- Prevents long-lived idle connections

### Vulnerabilities & Concerns

| Issue | Severity | Details |
|-------|----------|---------|
| **TLS is Optional** | HIGH | Default is insecure communication. `RAY_USE_TLS` must be explicitly set. 48 instances of `insecure_channel` in codebase. |
| **No Certificate Pinning** | MEDIUM | No mechanism to pin expected certificates. Vulnerable to MITM in compromised networks. |
| **Self-signed Cert Validation** | MEDIUM | Self-signed certs have 1-year expiration hardcoded. No renewal mechanism. |
| **mTLS Requires CA** | LOW | Client authentication only enabled if `RAY_TLS_CA_CERT` provided. Optional feature. |
| **No TLS Version Enforcement** | MEDIUM | Code doesn't specify minimum TLS version. Depends on gRPC/Python defaults. |
| **Cert File Permissions** | MEDIUM | No validation of certificate file permissions before loading. Readable by any user on system. |

---

## 3. Code Execution Safety

### Arbitrary Code Execution Model

Ray **intentionally executes arbitrary Python code** as its core function:

> "Ray faithfully executes code that is passed to it – Ray doesn't differentiate between a tuning experiment, a rootkit install, or an S3 bucket inspection."

#### Code Submission Paths
1. **Ray Jobs API:** `/api/jobs/submit` → `JobSubmitRequest` → `job_head.py`
2. **Ray Client:** gRPC-based client submitting functions/actors
3. **Direct Driver:** Python code calling `ray.remote()`, `ray.put()`, etc.

#### Serialization Mechanism
- **Framework:** Cloudpickle (Python's pickle with extended functionality)
- **Files:** 
  - `/python/ray/_private/serialization.py`
  - `/python/ray/_private/function_manager.py`
- **Usage:** All function arguments, return values, and actor state serialized via cloudpickle

#### No Sandboxing
- Code executes in worker processes with **same privileges as Ray**
- No language-level sandboxing
- No resource quotas enforced in Python layer (relies on cgroups if configured)

### Vulnerabilities & Concerns

| Issue | Severity | Details |
|-------|----------|---------|
| **Pickle/Cloudpickle Vulnerabilities** | CRITICAL | Pickle allows arbitrary code execution during deserialization. Historical CVEs in pickle libraries. No input validation before deserialization. |
| **No Sandboxing** | CRITICAL | All code runs with Ray process privileges. Malicious code can: access filesystem, modify memory, escape via RCE. |
| **No Code Review** | CRITICAL | No mechanism to review/approve code before execution. Assumes all submitters are trusted. |
| **Implicit Execution** | HIGH | Code in function bodies executes during definition/import time, not just at call time. |
| **Resource Exhaustion** | MEDIUM | Code can consume unlimited CPU, memory, disk I/O. Depends on external cgroup/limits. |
| **Dependency Injection** | MEDIUM | Runtime environments allow arbitrary package installation via `runtime_env` parameter. |

### Security Model
Ray's security model is **explicitly based on trust:**
- Only run Ray in networks where you trust all potential code submitters
- Use external controls (authentication, network isolation, process isolation) to restrict access
- Multiple Ray clusters for workload isolation

---

## 4. Secrets Management

### Current Approach

#### Environment-Based Configuration
- Credentials passed via environment variables
- Files referenced by paths in environment variables
- No built-in secrets management

#### Runtime Environment Authentication
- **netrc Files:** For remote URI authentication (GitHub, S3, etc.)
  - Location: `$HOME/.netrc` or configured via `NETRC` env var
  - Format: GNU netrc with `machine`, `login`, `password`
  - Security: Requires `chmod 600` (user-only read/write)
  - Usage: Automatically used by urllib for authentication

#### Token Files
- Default location: `~/.ray/auth_token`
- User-readable plaintext
- No encryption at rest

#### TLS Certificates
- Paths specified via environment variables:
  - `RAY_TLS_SERVER_CERT`, `RAY_TLS_SERVER_KEY`, `RAY_TLS_CA_CERT`
- Files must be readable by Ray process

### Vulnerabilities & Concerns

| Issue | Severity | Details |
|-------|----------|---------|
| **Plaintext Secrets in Environment** | HIGH | Environment variables often logged, exposed in process listings, core dumps. |
| **No Secrets Management Integration** | MEDIUM | No support for HashiCorp Vault, AWS Secrets Manager, etc. Must manage manually. |
| **netrc Plaintext** | MEDIUM | Credentials stored in plaintext despite umask requirements. Vulnerable to filesystem access. |
| **Credential Exposure in Logs** | MEDIUM | Runtime environment URIs with credentials can be logged. Users must use netrc to avoid this. |
| **No Key Rotation** | MEDIUM | No mechanism for automatic token/credential rotation. Manual updates required. |
| **File Permissions Not Validated** | LOW | Code reads cert/token files without checking permissions, though OS enforces at read time. |

#### Recommendations from Docs
The documentation explicitly warns against embedding credentials in URIs:
```python
# WRONG:
runtime_env = {"working_dir": "https://user:token@github.com/repo/archive.zip"}

# RIGHT: Use ~/.netrc file
```

---

## 5. Input Validation & Deserialization Safety

### Validation Points

#### Job Submission Validation
- `parse_and_validate_request()` in `job/utils.py`
- Validates `JobSubmitRequest` fields (entrypoint, runtime_env, etc.)
- Type checking via Pydantic models

#### HTTP Request Validation
- Query/path parameter validation minimal
- JSON body validation present via Pydantic
- No explicit SQL injection/command injection checks (not applicable - no SQL)

#### Path Traversal Protection
```python
# From http_server_head.py
request_path = pathlib.PurePosixPath(posixpath.realpath(request.path))
if request_path != parent and parent not in request_path.parents:
    raise aiohttp.web.HTTPForbidden()  # Prevents ../../../ attacks
```

#### Dashboard Input
- XSS Protection: HttpOnly cookies, SameSite=Strict
- CSRF Protection: Form validation, same-site cookies
- Browser POST/PUT Blocking: Middleware blocks mutating requests from browsers

### Vulnerabilities & Concerns

| Issue | Severity | Details |
|-------|----------|---------|
| **Cloudpickle Deserialization** | CRITICAL | Pickle inherently unsafe - allows arbitrary code during `loads()`. No validation possible. |
| **Job Config Not Sanitized** | MEDIUM | Job configuration (runtime_env, entrypoint) passed to subprocess without escaping. |
| **String Interpolation in Shell Commands** | MEDIUM | Runtime environment setup may use string interpolation in shell execution (depends on context). |
| **Missing Input Limits** | LOW | Dashboard client max size set to 100MiB but no per-field limits. |

---

## 6. Multi-Tenancy & Isolation

### Ray's Isolation Model

**Ray does NOT provide multi-tenancy isolation within a cluster.** From official documentation:

> "If workloads require isolation from each other, use separate, isolated Ray Clusters. Ray can schedule multiple distinct Jobs in a single Cluster, but doesn't attempt to enforce isolation between them."

#### Namespace Support
- **Job Namespaces:** Jobs can specify a namespace for isolation of named actors
  ```python
  ray.get_actor(name="my_actor", namespace="job1")
  ```
- **Internal Namespace Prefix:** `__ray_internal__` for system actors
- **Limitations:** Namespaces are logical only, not enforced at resource/permission level

#### Job Isolation Mechanisms
- **Process Isolation:** Each job's driver runs in separate process
- **Memory Isolation:** Object store separation via reference counting
- **No Network Isolation:** All workers can communicate directly

#### Resource Constraints
- **Cgroup Integration:** Optional cgroup-based resource limits
  - CPU reservation: `DEFAULT_SYSTEM_RESERVED_CPU_PROPORTION` (default 5%)
  - Memory reservation: `DEFAULT_SYSTEM_RESERVED_MEMORY_PROPORTION` (default 10%)
- **Limitations:** 
  - Disabled by default
  - Only CPU/memory isolation, not I/O or network

### Vulnerabilities & Concerns

| Issue | Severity | Details |
|-------|----------|---------|
| **No Enforcement of Job Isolation** | CRITICAL | Jobs in same cluster can access each other's data via Object Store, KV store, environment variables. |
| **Shared Memory Space** | CRITICAL | Multiple jobs run in same Python workers (if not using separate worker processes). Can modify each other's globals. |
| **No Network Isolation** | HIGH | All workers can communicate with each other. No firewall rules between jobs. |
| **Resource Exhaustion Attacks** | HIGH | One job can starve others: allocate all CPU, fill object store, exhaust memory. |
| **Shared KV Store** | MEDIUM | Jobs can read/write same KV namespace without restrictions. Default namespace is global. |
| **Actor Namespace Bypass** | MEDIUM | Namespace enforced only by naming convention, not enforced at API level. |
| **No RBAC** | HIGH | Ray has no role-based access control. All authenticated users have equal permissions. |
| **Dashboard Observability** | HIGH | Dashboard exposes internal state of all jobs/actors/tasks. No job-specific view restriction. |

#### Isolation Strategy (Per Documentation)
For true multi-tenancy:
1. **Deploy separate Ray clusters** for different tenants
2. **Use network isolation** (VPCs, firewalls) between clusters
3. **Implement external access control** (load balancers with auth)
4. **Audit and log** at infrastructure level

---

## 7. Configuration Security

### Environment Variables

#### Security-Critical Variables
```bash
RAY_AUTH_MODE              # Enable token/k8s auth
RAY_AUTH_TOKEN            # Plaintext token
RAY_AUTH_TOKEN_PATH       # Token file path
RAY_USE_TLS               # Enable TLS (0/1)
RAY_TLS_SERVER_CERT       # TLS certificate path
RAY_TLS_SERVER_KEY        # TLS private key path
RAY_TLS_CA_CERT          # CA certificate path
RAY_DASHBOARD_BUILD_FOLLOW_SYMLINKS  # Path traversal risk
```

#### Validation
- Environment variable values parsed with type checking (int, float, bool)
- Boolean parsing: `"1"` or `"true"` (case-insensitive) for true
- No validation of paths before use (OS enforces at file access)

### Vulnerabilities & Concerns

| Issue | Severity | Details |
|-------|----------|---------|
| **Environment Variable Leakage** | HIGH | Credentials/tokens visible via `ps`, `cat /proc/*/environ`, system logs. |
| **No Configuration Encryption** | MEDIUM | Ray config files stored plaintext. No built-in encryption. |
| **Symlink Following Risk** | MEDIUM | `RAY_DASHBOARD_BUILD_FOLLOW_SYMLINKS` can expose files outside build directory. Disabled by default. |
| **No Config Validation Schema** | LOW | Configuration passed as dicts without comprehensive schema validation. |

---

## 8. Communication Security

### gRPC Communication
- Uses gRPC by default for inter-component communication
- TLS encryption optional (see section 2)
- Authentication interceptors on every RPC call when enabled

### Dashboard/HTTP
- Uses aiohttp for HTTP/WebSocket
- Same port as job submission (default 8265)
- Middleware-based authentication

### Performance vs Security Trade-off
```python
# From grpc_utils.py
options_dict["grpc.keepalive_time_ms"] = ray._config.grpc_client_keepalive_time_ms()
```
- Keepalive settings configurable but not security-focused

---

## 9. Deployment Patterns

### KubeRay Integration
- **Proxy Pattern:** `kube-rbac-proxy` sidecar for Kubernetes RBAC enforcement
- **Token Integration:** Kubernetes service account tokens as bearer tokens
- **Cloud IAM:** Support for cloud provider IAM (GCP, AWS)

### Recommended Deployment
```
External Load Balancer/Proxy
    ↓
  (Auth enforced here)
    ↓
Ray Dashboard (port 8265)
    ↓
Ray Cluster (gRPC, insecure by default)
```

---

## Summary of Vulnerabilities

### CRITICAL Severity
1. **Arbitrary Code Execution via Cloudpickle** - No sandboxing, pickle allows RCE
2. **No Multi-tenant Isolation** - Jobs can interfere with each other within cluster
3. **No Network Security by Default** - All communication unencrypted by default
4. **Unrestricted Code Submission** - Any authenticated user can execute arbitrary code

### HIGH Severity
5. **TLS Disabled by Default** - Requires explicit configuration
6. **No Sandboxing for User Code** - Code runs with full Ray process privileges
7. **Resource Exhaustion Possible** - One job can starve others
8. **Token Transmission in Plaintext** - Without TLS, tokens exposed on network
9. **No Role-Based Access Control** - All authenticated users have equal privileges
10. **Dashboard Exposes All Internal State** - No job-specific view restrictions

### MEDIUM Severity
11. **Token Generation Strength** - UUID-based (128 bits) without cryptographic claims
12. **Token Storage in Plaintext** - Files readable by any user on system
13. **No Token Expiration/Revocation** - Tokens valid indefinitely
14. **Credentials in Environment Variables** - Can leak via logs, process listing
15. **No Rate Limiting on Auth** - Brute force token validation possible
16. **Certificate Management** - Self-signed certs, no pinning, no rotation

### LOW Severity
17. **Path Traversal Protection** - Implemented correctly but only for specific paths
18. **Bearer Token Validation** - Works correctly but could be more strict

---

## Recommendations

### Immediate (Critical Path)
1. **Always Enable TLS** - Make `RAY_USE_TLS=1` default in production
2. **Require Authentication** - Set `RAY_AUTH_MODE=token` or `k8s` before exposing services
3. **Network Isolation** - Deploy Ray in protected network, use VPC/firewall rules
4. **Restrict Code Access** - Only allow trusted developers to submit jobs
5. **Use External Proxy** - Deploy authentication/authorization proxy (kube-rbac-proxy, OAuth2-proxy) in front of Ray

### Short Term (1-2 releases)
1. **Implement Rate Limiting** - Add rate limiting to authentication endpoints
2. **Token Expiration** - Add configurable token TTL and refresh mechanism
3. **Server-side Token Storage** - Don't rely on client-provided tokens; maintain whitelist
4. **Configuration Validation** - Validate all TLS file paths and permissions on startup
5. **Audit Logging** - Log all authentication attempts, code submissions, API calls
6. **Certificate Pinning** - Support certificate pinning for client connections

### Medium Term (2-3 releases)
1. **Secrets Management Integration** - Support HashiCorp Vault, AWS Secrets Manager
2. **RBAC System** - Implement Ray-native role-based access control
3. **Job Isolation Enforcement** - Isolate job object stores, prevent cross-job access
4. **Code Signing** - Support signing functions/actors to prevent tampering
5. **Sandboxing** - Investigate user-space sandboxing (seccomp, gVisor) for code execution
6. **Credentials Rotation** - Automatic rotation for tokens, certificates

### Long Term (3+ releases)
1. **Multi-tenancy Isolation** - Implement hard isolation between jobs (separate processes, cgroups, SELinux)
2. **Zero-trust Architecture** - Eliminate implicit trust assumptions
3. **Formal Security Model** - Document and prove security guarantees
4. **Attestation** - Support work attestation/compliance requirements

---

## Compliance Considerations

### Security Model Alignment
- **Development/Testing:** Ray suitable with local clusters or dev networks
- **Production (Single Tenant):** Acceptable with TLS, token auth, network isolation, audit logging
- **Production (Multi-Tenant):** **NOT RECOMMENDED** - Use separate clusters per tenant
- **Regulated Environments:** Requires additional controls beyond Ray (network FW, SIEM, DLP)

### Standards Alignment
- **OWASP Top 10:** Ray violates #6 (Broken Access Control) - no RBAC, #8 (Software/Data Integrity) - no code signing
- **CIS Benchmarks:** Lacks encryption defaults, audit logging, secrets management
- **Zero Trust:** Ray assumes network trust - deploy with external zero-trust proxy

---

## Conclusion

Ray is a **high-performance distributed computing framework designed for trusted environments**. Its security model explicitly delegates isolation and access control to the infrastructure layer rather than implementing them within Ray itself.

**Key Principle:** "Security and isolation must be enforced outside of the Ray Cluster."

For production deployments, treat Ray as a trusted internal service and deploy it:
- In controlled network environments
- Behind authentication/authorization proxies
- In separate clusters per security domain
- With comprehensive audit logging
- With external secrets management
- With TLS encryption enabled

The framework is **not suitable for exposing directly to untrusted networks** or for enforcing multi-tenant isolation without substantial additional infrastructure.

