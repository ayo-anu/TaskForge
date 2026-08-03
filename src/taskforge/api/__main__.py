"""Minimal executable entry point for the Taskforge API process."""

import uvicorn

from taskforge.settings import Settings


def main() -> int:
    """Run the API using only typed runtime configuration."""
    settings = Settings()
    uvicorn.run(
        "taskforge.api.application:create_app",
        factory=True,
        host=settings.api_host,
        port=settings.api_port,
        log_level=settings.log_level.lower(),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
