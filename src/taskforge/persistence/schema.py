"""Register every relational schema with Taskforge's shared metadata."""

from taskforge.identity import schema as identity_schema
from taskforge.persistence.metadata import metadata
from taskforge.workflows import schema as workflow_schema

__all__ = ["metadata"]

# These imports intentionally register their tables with the shared metadata.
assert identity_schema.api_principals is not None
assert workflow_schema.workflow_definitions is not None
