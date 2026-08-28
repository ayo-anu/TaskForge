"""Register every relational schema with Taskforge's shared metadata."""

from taskforge import rate_limits_schema
from taskforge.audit import schema as audit_schema
from taskforge.dead_letters import schema as dead_letter_schema
from taskforge.identity import schema as identity_schema
from taskforge.persistence.metadata import metadata
from taskforge.runs import schema as run_schema
from taskforge.worker import schema as worker_schema
from taskforge.workflows import schema as workflow_schema

__all__ = ["metadata"]

# These imports intentionally register their tables with the shared metadata.
assert dead_letter_schema.dead_letter_items is not None
assert audit_schema.audit_records is not None
assert identity_schema.api_principals is not None
assert run_schema.workflow_runs is not None
assert worker_schema.worker_sessions is not None
assert workflow_schema.workflow_definitions is not None
assert rate_limits_schema.rate_limit_counters is not None
