"""
Exceptions for multi-tenant isolation in Ray.
"""


class MultiTenantError(Exception):
    """Base exception for multi-tenant errors."""
    pass


class TenantNotFoundError(MultiTenantError):
    """Raised when a tenant is not found."""
    pass


class TenantAlreadyExistsError(MultiTenantError):
    """Raised when attempting to create a tenant that already exists."""
    pass


class QuotaExceededError(MultiTenantError):
    """Raised when a tenant's quota would be exceeded."""
    pass


class TenantIsolationError(MultiTenantError):
    """Raised when tenant isolation is violated."""
    pass


class TenantAuthenticationError(MultiTenantError):
    """Raised when tenant authentication fails."""
    pass


class InvalidTenantConfigError(MultiTenantError):
    """Raised when tenant configuration is invalid."""
    pass
