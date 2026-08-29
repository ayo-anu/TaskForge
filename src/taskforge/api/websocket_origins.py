"""Canonical WebSocket browser-origin validation."""

from __future__ import annotations

import ipaddress
import re
from urllib.parse import urlsplit

_DNS_HOST = re.compile(
    r"(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)(?:\.(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?))*\Z"
)


class InvalidWebSocketOrigin(ValueError):
    pass


class OpaqueWebSocketOrigin(InvalidWebSocketOrigin):
    pass


def canonical_websocket_origin(value: str) -> str:
    """Return one canonical HTTP(S) origin or reject ambiguous input."""
    if value == "null":
        raise OpaqueWebSocketOrigin("opaque origins are not accepted")
    if (
        not value
        or not value.isascii()
        or any(
            character.isspace() or ord(character) < 32 or ord(character) == 127
            for character in value
        )
    ):
        raise InvalidWebSocketOrigin("origin contains invalid characters")
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError as error:
        raise InvalidWebSocketOrigin("origin is malformed") from error
    scheme = parsed.scheme.lower()
    if (
        scheme not in {"http", "https"}
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise InvalidWebSocketOrigin("origin is not an HTTP(S) origin")
    hostname = parsed.hostname
    if hostname is None or "*" in hostname or "%" in hostname:
        raise InvalidWebSocketOrigin("origin host is invalid")
    host = _canonical_host(hostname)
    if port is not None and not 1 <= port <= 65535:
        raise InvalidWebSocketOrigin("origin port is invalid")
    if port == (80 if scheme == "http" else 443):
        port = None
    authority = host if port is None else f"{host}:{port}"
    return f"{scheme}://{authority}"


def _canonical_host(value: str) -> str:
    try:
        address = ipaddress.ip_address(value)
    except ValueError:
        host = value.lower()
        if len(host) > 253 or _DNS_HOST.fullmatch(host) is None:
            raise InvalidWebSocketOrigin("origin host is invalid") from None
        return host
    if isinstance(address, ipaddress.IPv6Address):
        return f"[{address.compressed}]"
    return address.compressed
