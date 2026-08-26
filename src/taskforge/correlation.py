"""Neutral correlation-identity validation shared across transport and domains."""

from __future__ import annotations

MAX_CORRELATION_ID_LENGTH = 128


def is_valid_correlation_id(value: object) -> bool:
    """Return whether an opaque optional correlation identity is canonical."""
    return value is None or (
        isinstance(value, str)
        and 1 <= len(value) <= MAX_CORRELATION_ID_LENGTH
        and all(32 <= ord(character) <= 126 for character in value)
    )
