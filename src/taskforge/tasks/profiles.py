"""Deployment-controlled worker profiles for the production task catalog."""

from __future__ import annotations

from taskforge.runtime_provider import WorkerHandlerBinding
from taskforge.tasks.catalog import (
    INGEST_TASK_TYPE,
    NOTIFY_TASK_TYPE,
    TRANSFORM_TASK_TYPE,
    VALIDATE_TASK_TYPE,
)
from taskforge.tasks.handlers import ingest, notify, transform, validate


def provide_pipeline_profile() -> tuple[WorkerHandlerBinding, ...]:
    return (
        WorkerHandlerBinding(INGEST_TASK_TYPE, ingest),
        WorkerHandlerBinding(VALIDATE_TASK_TYPE, validate),
        WorkerHandlerBinding(TRANSFORM_TASK_TYPE, transform),
        WorkerHandlerBinding(NOTIFY_TASK_TYPE, notify),
    )


def provide_ingestion_profile() -> tuple[WorkerHandlerBinding, ...]:
    return (WorkerHandlerBinding(INGEST_TASK_TYPE, ingest),)


def provide_processing_profile() -> tuple[WorkerHandlerBinding, ...]:
    return (
        WorkerHandlerBinding(VALIDATE_TASK_TYPE, validate),
        WorkerHandlerBinding(TRANSFORM_TASK_TYPE, transform),
    )


def provide_notification_profile() -> tuple[WorkerHandlerBinding, ...]:
    return (WorkerHandlerBinding(NOTIFY_TASK_TYPE, notify),)
