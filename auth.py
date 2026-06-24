"""
API key authentication for the event gateway.

Your friend sends his requests with a header:
    X-API-Key: <the key you gave him>

This is checked against GATEWAY_API_KEY in your .env before anything
else happens. If it doesn't match, the request is rejected immediately
- no schema parsing, no RabbitMQ connection, nothing.
"""

import os
import secrets

from fastapi import Header, HTTPException, status

GATEWAY_API_KEY = os.environ["GATEWAY_API_KEY"]


def verify_api_key(x_api_key: str = Header(..., alias="X-API-Key")) -> None:
    """FastAPI dependency. Raises 401 if the key is missing or wrong.

    Uses secrets.compare_digest instead of `==` to avoid leaking timing
    information about how many characters matched.
    """
    if not secrets.compare_digest(x_api_key, GATEWAY_API_KEY):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
        )