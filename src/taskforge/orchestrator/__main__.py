"""Executable entry point for the production Taskforge orchestrator process."""

from __future__ import annotations

import asyncio
import sys

from pydantic import ValidationError

from taskforge.orchestrator.application import OrchestratorApplication
from taskforge.settings import OrchestratorSettings


def main() -> int:
    """Run one supervised orchestrator process and return a stable exit status."""
    try:
        settings = OrchestratorSettings()
    except ValidationError:
        sys.stderr.write("taskforge orchestrator configuration is invalid\n")
        return 2
    try:
        asyncio.run(OrchestratorApplication(settings).run())
    except KeyboardInterrupt:
        return 0
    except Exception:
        sys.stderr.write("taskforge orchestrator runtime failed\n")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
