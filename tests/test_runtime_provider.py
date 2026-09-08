"""Tests for trusted installed catalog and worker-profile resolution."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from taskforge.api.application import create_production_app
from taskforge.runtime_provider import (
    RuntimeProviderError,
    WorkerHandlerBinding,
    load_installed_task_catalog,
    load_installed_worker_profile,
)
from taskforge.workflows.task_types import (
    JSONMapping,
    TaskTypeDefinition,
    WorkflowValidationIssue,
)


class Validator:
    def validate(self, parameters: JSONMapping) -> tuple[WorkflowValidationIssue, ...]:
        return ()


async def left_handler(context: Any) -> object:
    return {"handled": context.task_type}


async def right_handler(context: Any) -> object:
    return {"handled": context.task_type}


def catalog_provider() -> tuple[TaskTypeDefinition, ...]:
    return (
        TaskTypeDefinition("left.task", "left", Validator()),
        TaskTypeDefinition("right.task", "right", Validator()),
    )


@dataclass
class EntryPoint:
    name: str
    group: str
    value: str
    loaded: object
    loads: int = 0

    def load(self) -> object:
        self.loads += 1
        return self.loaded


def discovered(*entries: EntryPoint) -> Any:
    return lambda: entries


def catalog_entry(provider: object = catalog_provider) -> EntryPoint:
    return EntryPoint(
        "application", "taskforge.task_catalog", "tests:catalog", provider
    )


def test_catalog_requires_exactly_one_installed_provider() -> None:
    with pytest.raises(RuntimeProviderError):
        load_installed_task_catalog(discovery=discovered())
    first = catalog_entry()
    second = catalog_entry()
    with pytest.raises(RuntimeProviderError):
        load_installed_task_catalog(discovery=discovered(first, second))
    assert first.loads == second.loads == 0


def test_catalog_builds_metadata_registry_without_loading_worker_profile() -> None:
    profile = EntryPoint(
        "left", "taskforge.worker_profile", "tests:left_profile", object()
    )
    catalog = load_installed_task_catalog(
        discovery=discovered(catalog_entry(), profile)
    )
    assert catalog.names == {"left.task", "right.task"}
    assert profile.loads == 0


def test_api_production_factory_loads_only_catalog_surface(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog = load_installed_task_catalog(discovery=discovered(catalog_entry()))
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        "taskforge.api.application.load_installed_task_catalog", lambda: catalog
    )

    def create(**kwargs: object) -> object:
        captured.update(kwargs)
        return object()

    monkeypatch.setattr("taskforge.api.application.create_app", create)

    assert create_production_app() is not None
    assert captured == {"task_types": catalog}


def test_named_profile_derives_subset_capabilities_from_catalog() -> None:
    profile = EntryPoint(
        "left",
        "taskforge.worker_profile",
        "tests:left_profile",
        lambda: (WorkerHandlerBinding("left.task", left_handler),),
    )
    catalog = load_installed_task_catalog(discovery=discovered(catalog_entry()))
    resolved = load_installed_worker_profile(
        "left", catalog, discovery=discovered(profile)
    )
    assert resolved.name == "left"
    assert resolved.handlers.task_types == {"left.task"}
    assert resolved.capabilities == ("left",)


def test_heterogeneous_profiles_resolve_independent_trusted_subsets() -> None:
    left = EntryPoint(
        "left",
        "taskforge.worker_profile",
        "tests:left_profile",
        lambda: (WorkerHandlerBinding("left.task", left_handler),),
    )
    right = EntryPoint(
        "right",
        "taskforge.worker_profile",
        "tests:right_profile",
        lambda: (WorkerHandlerBinding("right.task", right_handler),),
    )
    catalog = load_installed_task_catalog(discovery=discovered(catalog_entry()))
    entries = discovered(left, right)
    assert load_installed_worker_profile(
        "left", catalog, discovery=entries
    ).capabilities == ("left",)
    assert load_installed_worker_profile(
        "right", catalog, discovery=entries
    ).capabilities == ("right",)


def test_profile_zero_match_fails_before_import() -> None:
    other = EntryPoint("other", "taskforge.worker_profile", "tests:other", object())
    catalog = load_installed_task_catalog(discovery=discovered(catalog_entry()))
    with pytest.raises(RuntimeProviderError):
        load_installed_worker_profile("missing", catalog, discovery=discovered(other))
    assert other.loads == 0


def test_duplicate_profile_name_fails_before_either_import() -> None:
    first = EntryPoint("same", "taskforge.worker_profile", "one:profile", object())
    second = EntryPoint("same", "taskforge.worker_profile", "two:profile", object())
    catalog = load_installed_task_catalog(discovery=discovered(catalog_entry()))
    with pytest.raises(RuntimeProviderError):
        load_installed_worker_profile(
            "same", catalog, discovery=discovered(second, first)
        )
    assert first.loads == second.loads == 0


@pytest.mark.parametrize(
    "name", ("module:callable", "../profile", "/tmp/profile", "Profile", "")
)
def test_profile_name_rejects_import_and_filesystem_expressions(name: str) -> None:
    catalog = load_installed_task_catalog(discovery=discovered(catalog_entry()))
    with pytest.raises(RuntimeProviderError):
        load_installed_worker_profile(name, catalog, discovery=discovered())


def test_profile_rejects_empty_unknown_duplicate_and_non_callable_bindings() -> None:
    catalog = load_installed_task_catalog(discovery=discovered(catalog_entry()))
    providers: tuple[object, ...] = (
        lambda: (),
        lambda: (WorkerHandlerBinding("unknown", left_handler),),
        lambda: (
            WorkerHandlerBinding("left.task", left_handler),
            WorkerHandlerBinding("left.task", left_handler),
        ),
        lambda: (WorkerHandlerBinding("left.task", object()),),  # type: ignore[arg-type]
    )
    for provider in providers:
        entry = EntryPoint("selected", "taskforge.worker_profile", "test:p", provider)
        with pytest.raises(RuntimeProviderError):
            load_installed_worker_profile(
                "selected", catalog, discovery=discovered(entry)
            )
