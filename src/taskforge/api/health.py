"""API-owned liveness, readiness, and bounded dependency state."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from enum import StrEnum
from typing import Literal, Protocol

from pydantic import BaseModel

from taskforge.logging import log_event
from taskforge.metrics import add as add_metric

logger = logging.getLogger(__name__)


class LivenessResponse(BaseModel):
    """Safe response body for the unversioned operational liveness endpoint."""

    alive: Literal[True] = True


class ReadinessResponse(BaseModel):
    """Safe response body for the unversioned operational readiness endpoint."""

    ready: bool
    status: Literal["ready", "degraded", "not_ready"]


class DependencyObservation(StrEnum):
    UNKNOWN = "unknown"
    AVAILABLE = "available"
    UNAVAILABLE = "unavailable"


class APIPhase(StrEnum):
    STARTING = "starting"
    SERVING = "serving"
    STOPPING = "stopping"


class PostgreSQLReadinessProbe(Protocol):
    async def is_ready(self) -> bool:
        """Return whether the authoritative API database runtime is usable."""


@dataclass(frozen=True)
class APIReadinessSnapshot:
    ready: bool
    status: Literal["ready", "degraded", "not_ready"]


class ReadinessCoordinator:
    """Own readiness for the current API process and its two real dependencies."""

    def __init__(
        self,
        postgresql: PostgreSQLReadinessProbe,
        timeout_seconds: float,
    ) -> None:
        self._postgresql = postgresql
        self._timeout_seconds = timeout_seconds
        self._phase = APIPhase.STARTING
        self._postgresql_state = DependencyObservation.UNKNOWN
        self._execution_stream_state = DependencyObservation.UNKNOWN
        self._reported_status: str | None = None

    async def start(self) -> APIReadinessSnapshot:
        """Establish a real initial database observation before serving telemetry."""
        await self._probe_postgresql()
        self._phase = APIPhase.SERVING
        snapshot = self._derive()
        self._report_readiness_transition(snapshot.status, "starting")
        return snapshot

    async def snapshot(self) -> APIReadinessSnapshot:
        """Return readiness after one bounded live authoritative database probe."""
        if self._phase is APIPhase.SERVING:
            await self._probe_postgresql()
        snapshot = self._derive()
        self._report_readiness_transition(snapshot.status, None)
        return snapshot

    def observe_execution_stream(self, available: bool) -> None:
        """Accept one failure-isolated observation from the listener supervisor."""
        state = (
            DependencyObservation.AVAILABLE
            if available
            else DependencyObservation.UNAVAILABLE
        )
        if self._phase is APIPhase.STOPPING:
            self._execution_stream_state = state
            return
        self._set_dependency("execution_stream", state, "connection_lost")
        if self._phase is APIPhase.SERVING:
            self._report_readiness_transition(self._derive().status, None)

    def withdraw(self) -> None:
        """Withdraw readiness before useful API resources are torn down."""
        if self._phase is APIPhase.STOPPING:
            return
        self._phase = APIPhase.STOPPING
        self._report_readiness_transition("not_ready", "stopping")

    async def close(self) -> None:
        """Retain the lifecycle surface used by application composition."""
        self.withdraw()

    async def _probe_postgresql(self) -> None:
        try:
            async with asyncio.timeout(self._timeout_seconds):
                available = await self._postgresql.is_ready()
        except asyncio.CancelledError:
            raise
        except TimeoutError:
            self._set_dependency(
                "postgresql", DependencyObservation.UNAVAILABLE, "probe_timeout"
            )
            return
        except Exception:
            self._set_dependency(
                "postgresql", DependencyObservation.UNAVAILABLE, "probe_failed"
            )
            return
        self._set_dependency(
            "postgresql",
            DependencyObservation.AVAILABLE
            if available
            else DependencyObservation.UNAVAILABLE,
            "probe_failed",
        )

    def _set_dependency(
        self,
        name: Literal["postgresql", "execution_stream"],
        state: DependencyObservation,
        unavailable_reason: str,
    ) -> None:
        attribute = (
            "_postgresql_state" if name == "postgresql" else "_execution_stream_state"
        )
        if getattr(self, attribute) is state:
            return
        setattr(self, attribute, state)
        try:
            log_event(
                logger,
                logging.INFO
                if state is DependencyObservation.AVAILABLE
                else logging.WARNING,
                "dependency.state.changed",
                {
                    "dependency.name": name,
                    "dependency.state": state.value,
                    "reason.code": (
                        "available"
                        if state is DependencyObservation.AVAILABLE
                        else unavailable_reason
                    ),
                },
            )
        except Exception:
            pass
        try:
            add_metric(
                "taskforge.dependency.state.transitions",
                attributes={
                    "taskforge.dependency": name,
                    "taskforge.dependency.state": state.value,
                },
            )
        except Exception:
            pass

    def _derive(self) -> APIReadinessSnapshot:
        if (
            self._phase is not APIPhase.SERVING
            or self._postgresql_state is not DependencyObservation.AVAILABLE
        ):
            return APIReadinessSnapshot(False, "not_ready")
        if self._execution_stream_state is not DependencyObservation.AVAILABLE:
            return APIReadinessSnapshot(True, "degraded")
        return APIReadinessSnapshot(True, "ready")

    def _report_readiness_transition(
        self, status: Literal["ready", "degraded", "not_ready"], reason: str | None
    ) -> None:
        if self._reported_status == status:
            return
        self._reported_status = status
        fields: dict[str, object] = {"readiness.status": status}
        if reason is not None:
            fields["reason.code"] = reason
        try:
            log_event(
                logger,
                logging.INFO if status != "not_ready" else logging.WARNING,
                "process.readiness.changed",
                fields,
            )
        except Exception:
            pass
        try:
            add_metric(
                "taskforge.process.readiness.transitions",
                attributes={"taskforge.readiness.status": status},
            )
        except Exception:
            pass
