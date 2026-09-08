"""Process-wide worker runtime failure classifications."""


class WorkerProcessFailure(RuntimeError):
    """A required process-wide authority or runtime dependency failed."""
