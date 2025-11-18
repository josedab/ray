import secrets


def generate_new_authentication_token() -> str:
    """Generate a cryptographically secure authentication token.

    Uses secrets.token_urlsafe() for better security than UUID-based tokens.
    The token is URL-safe base64 encoded and contains 32 bytes of randomness.

    Returns:
        A 43-character URL-safe base64-encoded token.
    """
    return secrets.token_urlsafe(32)
