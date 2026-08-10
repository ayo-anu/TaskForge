"""Canonical lexical contract for Taskforge capability names."""

from __future__ import annotations

import re

CAPABILITY_NAME_PATTERN = re.compile(r"\A[a-z][a-z0-9_.-]{0,127}\Z")


def is_valid_capability_name(value: str) -> bool:
    """Return whether a string satisfies the shared capability-name contract."""
    return CAPABILITY_NAME_PATTERN.fullmatch(value) is not None
