"""Workflow run persistence and target-resolution domain."""

from taskforge.runs.domain import (
    CreatedWorkflowRun,
    ExplicitWorkflowVersion,
    LatestWorkflowVersion,
    ResolvedWorkflowVersion,
    WorkflowRunInput,
    create_workflow_run_input,
)

__all__ = (
    "CreatedWorkflowRun",
    "ExplicitWorkflowVersion",
    "LatestWorkflowVersion",
    "ResolvedWorkflowVersion",
    "WorkflowRunInput",
    "create_workflow_run_input",
)
