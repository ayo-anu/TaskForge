"""Executable entry point for the production Taskforge worker process."""

from __future__ import annotations

import asyncio
import sys

from pydantic import ValidationError

from taskforge.settings import WorkerSettings
from taskforge.worker.application import WorkerApplication


def main() -> int:
    """Run one supervised worker process and return a stable exit status."""
    try:
        settings = WorkerSettings()
    except ValidationError:
        sys.stderr.write("taskforge worker configuration is invalid\n")
        return 2
    try:
        asyncio.run(WorkerApplication(settings).run())
    except KeyboardInterrupt:
        return 0
    except Exception:
        sys.stderr.write("taskforge worker runtime failed\n")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
