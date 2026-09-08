"""Trusted installed task catalogs and worker-handler profiles."""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from importlib import metadata
from typing import Protocol, cast

from taskforge.worker.handlers import (
    TaskHandler,
    TaskHandlerDefinition,
    TaskHandlerRegistry,
)
from taskforge.workflows.task_types import TaskTypeDefinition, TaskTypeRegistry

TASK_CATALOG_ENTRY_POINT_GROUP = "taskforge.task_catalog"
WORKER_PROFILE_ENTRY_POINT_GROUP = "taskforge.worker_profile"
_PROVIDER_NAME = re.compile(r"\A[a-z][a-z0-9_.-]{0,127}\Z")


class RuntimeProviderError(RuntimeError):
    """Installed trusted runtime providers are absent, ambiguous, or invalid."""


class InstalledEntryPoint(Protocol):
    name: str
    group: str
    value: str

    def load(self) -> object: ...


type EntryPointDiscovery = Callable[[], Iterable[InstalledEntryPoint]]


@dataclass(frozen=True)
class WorkerHandlerBinding:
    """A deployment-controlled task type to executable handler binding."""

    task_type: str
    handler: TaskHandler


@dataclass(frozen=True)
class ResolvedWorkerProfile:
    """A validated immutable profile whose capabilities come from its handlers."""

    name: str
    handlers: TaskHandlerRegistry
    capabilities: tuple[str, ...]


type TaskCatalogProvider = Callable[[], tuple[TaskTypeDefinition, ...]]
type WorkerProfileProvider = Callable[[], tuple[WorkerHandlerBinding, ...]]


def load_installed_task_catalog(
    *, discovery: EntryPointDiscovery = metadata.entry_points
) -> TaskTypeRegistry:
    """Load exactly one installed metadata-only trusted task catalog."""
    matches = _entry_points(discovery, TASK_CATALOG_ENTRY_POINT_GROUP)
    if len(matches) != 1:
        raise RuntimeProviderError("exactly one installed task catalog is required")
    loaded = matches[0].load()
    if not callable(loaded):
        raise RuntimeProviderError("installed task catalog provider is not callable")
    try:
        definitions = cast(TaskCatalogProvider, loaded)()
        if not isinstance(definitions, tuple) or not definitions:
            raise RuntimeProviderError("installed task catalog must be non-empty")
        if any(not isinstance(item, TaskTypeDefinition) for item in definitions):
            raise RuntimeProviderError(
                "installed task catalog returned invalid entries"
            )
        return TaskTypeRegistry(definitions)
    except RuntimeProviderError:
        raise
    except Exception as error:
        raise RuntimeProviderError("installed task catalog is invalid") from error


def load_installed_worker_profile(
    profile_name: str,
    catalog: TaskTypeRegistry,
    *,
    discovery: EntryPointDiscovery = metadata.entry_points,
) -> ResolvedWorkerProfile:
    """Resolve one exact installed profile without accepting an import expression."""
    if _PROVIDER_NAME.fullmatch(profile_name) is None:
        raise RuntimeProviderError("worker profile name is invalid")
    matches = tuple(
        entry
        for entry in _entry_points(discovery, WORKER_PROFILE_ENTRY_POINT_GROUP)
        if entry.name == profile_name
    )
    if len(matches) != 1:
        raise RuntimeProviderError(
            "exactly one installed worker profile must match the configured name"
        )
    loaded = matches[0].load()
    if not callable(loaded):
        raise RuntimeProviderError("installed worker profile provider is not callable")
    try:
        bindings = cast(WorkerProfileProvider, loaded)()
        if not isinstance(bindings, tuple) or not bindings:
            raise RuntimeProviderError("installed worker profile must be non-empty")
        if any(not isinstance(item, WorkerHandlerBinding) for item in bindings):
            raise RuntimeProviderError(
                "installed worker profile returned invalid entries"
            )
        definitions: list[TaskHandlerDefinition] = []
        for binding in bindings:
            task_type = catalog.definition(binding.task_type)
            if task_type is None:
                raise RuntimeProviderError(
                    "worker profile references an unregistered task type"
                )
            definitions.append(
                TaskHandlerDefinition(
                    binding.task_type,
                    task_type.required_capability,
                    binding.handler,
                )
            )
        handlers = TaskHandlerRegistry(tuple(definitions), catalog)
    except RuntimeProviderError:
        raise
    except Exception as error:
        raise RuntimeProviderError("installed worker profile is invalid") from error
    return ResolvedWorkerProfile(
        profile_name,
        handlers,
        tuple(sorted(handlers.required_capabilities)),
    )


def _entry_points(
    discovery: EntryPointDiscovery, group: str
) -> tuple[InstalledEntryPoint, ...]:
    try:
        return tuple(entry for entry in discovery() if entry.group == group)
    except Exception as error:
        raise RuntimeProviderError(
            "installed runtime providers cannot be discovered"
        ) from error
