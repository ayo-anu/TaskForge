"""Shared canonical M21 Task 1 workload runner."""

from __future__ import annotations

import asyncio
import json
import secrets
import socket
from collections import Counter
from dataclasses import dataclass
from itertools import pairwise
from pathlib import Path
from typing import Any, Protocol, cast
from uuid import UUID, uuid4

import aio_pika
import asyncpg
import httpx2
import uvicorn
from sqlalchemy import insert

from taskforge.api.application import create_app
from taskforge.api.health import ReadinessCoordinator
from taskforge.broker.consumer import RabbitMQDispatchConsumer
from taskforge.broker.rabbitmq import RabbitMQDispatchPublisher
from taskforge.broker.topology import (
    RabbitMQTopologyConfiguration,
    declare_dispatch_topology,
)
from taskforge.claims.authority import TaskClaimResultAuthorityIssuer
from taskforge.claims.service import TaskClaimService
from taskforge.dispatch.publisher import TaskDispatchPublisher
from taskforge.dispatch.service import TaskDispatchService
from taskforge.identity.authorization import OwnerFilter
from taskforge.identity.credentials import DEFAULT_VERIFIER_ALGORITHM, DEFAULT_VERIFIERS
from taskforge.identity.schema import (
    api_credentials,
    api_principal_roles,
    api_principals,
)
from taskforge.persistence.claims import SQLAlchemyTaskClaimRepository
from taskforge.persistence.database import build_async_engine, build_session_factory
from taskforge.persistence.dispatch import (
    SQLAlchemyDispatchOutboxRepository,
    SQLAlchemyTaskDispatchRepository,
)
from taskforge.persistence.runs import SQLAlchemyWorkflowRunRepository
from taskforge.persistence.task_results import SQLAlchemyTaskResultRepository
from taskforge.persistence.task_start import SQLAlchemyTaskStartRepository
from taskforge.rate_limits import AllowAllRateLimiter
from taskforge.runs.domain import ExplicitWorkflowVersion, create_workflow_run_input
from taskforge.runs.service import WorkflowRunService
from taskforge.worker.consumer_ports import DispatchDeliveryControl
from taskforge.worker.execution import WorkerExecutionConsumer
from taskforge.worker.handlers import (
    TaskContext,
    TaskHandlerDefinition,
    TaskHandlerRegistry,
)
from taskforge.worker.lifecycle import WorkerDispatchRuntime
from taskforge.worker.result_submission import TaskResultSubmissionService
from taskforge.worker.start import TaskStartService
from taskforge.workflows.schema import (
    workflow_definitions,
    workflow_version_steps,
    workflow_versions,
)
from taskforge.workflows.task_types import (
    JSONMapping,
    TaskTypeDefinition,
    TaskTypeRegistry,
    WorkflowValidationIssue,
)
from tests.integration.postgresql import asyncpg_dsn
from tests.integration.test_authentication_persistence import settings_for
from tests.integration.test_protected_principal_route import AlwaysReady
from tests.integration.test_task_claim_acquisition import add_worker
from tests.integration.test_workflow_routes import credential_value
from tests.performance.m21_workload import (
    M21Evidence,
    M21WorkloadConfiguration,
    WorkerDistribution,
)

_AUTHORITY_SECRET = b"m21-task1-result-authority-secret"


@dataclass(frozen=True)
class AcceptParameters:
    def validate(self, parameters: JSONMapping) -> tuple[WorkflowValidationIssue, ...]:
        del parameters
        return ()


@dataclass(frozen=True)
class OwnerFacts:
    principal_id: UUID
    workflow_id: UUID
    credential: str


class M21MeasurementObserver(Protocol):
    async def start(
        self,
        *,
        api_port: int,
        runs_with_owners: list[tuple[Any, OwnerFacts]],
        database_url: Any,
        broker_metadata: dict[str, str],
        setup: Any,
        runtimes: list[WorkerDispatchRuntime],
    ) -> None: ...

    async def finish(
        self,
        *,
        setup: Any,
        run_ids: list[UUID],
        workers: list[Any],
        runtimes: list[WorkerDispatchRuntime],
    ) -> None: ...

    async def close(self) -> None: ...


class RequeueOnceControl:
    def __init__(
        self,
        delegate: DispatchDeliveryControl,
        dispatch_id: UUID,
        selected: set[UUID],
        requeued: set[UUID],
    ) -> None:
        self._delegate = delegate
        self._dispatch_id = dispatch_id
        self._selected = selected
        self._requeued = requeued

    @property
    def delivery(self) -> Any:
        return self._delegate.delivery

    async def acknowledge(self) -> None:
        if (
            self._dispatch_id in self._selected
            and self._dispatch_id not in self._requeued
        ):
            self._requeued.add(self._dispatch_id)
            await self._delegate.reject(requeue=True)
            return
        await self._delegate.acknowledge()

    async def reject(self, *, requeue: bool) -> None:
        await self._delegate.reject(requeue=requeue)


async def _seed_workflows(sessions: Any, count: int) -> list[OwnerFacts]:
    owners: list[OwnerFacts] = []
    async with sessions.begin() as session:
        for owner_ordinal in range(count):
            owner_id, workflow_id, version_id, credential_id = (
                uuid4(),
                uuid4(),
                uuid4(),
                uuid4(),
            )
            secret = secrets.token_bytes(32)
            verifier = DEFAULT_VERIFIERS.encode(
                secret, algorithm=DEFAULT_VERIFIER_ALGORITHM
            )
            await session.execute(
                insert(api_principals).values(
                    id=owner_id, name=f"m21-owner-{owner_ordinal}-{uuid4().hex}"
                )
            )
            await session.execute(
                insert(api_principal_roles).values(
                    principal_id=owner_id, role="workflow_operator"
                )
            )
            await session.execute(
                insert(api_credentials).values(
                    id=credential_id,
                    principal_id=owner_id,
                    credential_verifier=verifier,
                )
            )
            await session.execute(
                insert(workflow_definitions).values(
                    id=workflow_id,
                    owner_principal_id=owner_id,
                    name=f"M21 four-root workload {owner_ordinal}",
                    status="enabled",
                )
            )
            await session.execute(
                insert(workflow_versions).values(
                    id=version_id,
                    workflow_definition_id=workflow_id,
                    version_number=1,
                    name="M21 four-root workload",
                    execution_policy=None,
                )
            )
            await session.execute(
                insert(workflow_version_steps),
                [
                    {
                        "workflow_version_id": version_id,
                        "step_identifier": f"root-{ordinal:02d}",
                        "task_type": (
                            "workload.redelivery"
                            if ordinal == 0
                            else "workload.success"
                        ),
                        "parameters": {},
                        "execution_policy": None,
                    }
                    for ordinal in range(4)
                ],
            )
            owners.append(
                OwnerFacts(
                    owner_id,
                    workflow_id,
                    credential_value(credential_id, secret),
                )
            )
    return owners


async def run_m21_workload(
    database_url: Any,
    amqp_url: str,
    output: Path,
    *,
    observer: M21MeasurementObserver | None = None,
    emit_evidence: bool = True,
) -> M21Evidence:
    configuration = M21WorkloadConfiguration()
    evidence = M21Evidence()
    gate = asyncio.Event()
    concurrent = asyncio.Event()
    active_workers: set[int] = set()
    handler_counts: Counter[UUID] = Counter()
    distributions = [WorkerDistribution(index) for index in range(8)]
    selected_dispatches: set[UUID] = set()
    requeued: set[UUID] = set()
    delivery_counts: Counter[UUID] = Counter()
    redelivered_counts: Counter[UUID] = Counter()
    runtimes: list[WorkerDispatchRuntime] = []
    subscriber_messages: dict[UUID, list[dict[str, Any]]] = {}
    subscriber_tasks: list[asyncio.Task[None]] = []
    subscriber_ready = asyncio.Event()
    subscriber_ready_count = 0
    issuer = TaskClaimResultAuthorityIssuer(_AUTHORITY_SECRET)
    current_phase = "infrastructure_setup"
    run_ids: list[UUID] = []
    publication_counts = {"durable_dispatches": 0, "published_messages": 0}
    engine: Any = None
    setup: Any = None
    broker: Any = None
    queue: Any = None
    topology: Any = None
    topology_configuration: Any = None
    listener: socket.socket | None = None
    server: uvicorn.Server | None = None
    server_task: asyncio.Task[None] | None = None
    overall_expired = False
    primary_error: BaseException | None = None
    cleanup_errors: list[dict[str, str]] = []
    current_task = asyncio.current_task()
    assert current_task is not None

    def expire_overall() -> None:
        nonlocal overall_expired
        overall_expired = True
        current_task.cancel()

    overall_deadline = asyncio.get_running_loop().call_later(
        configuration.overall_timeout_seconds, expire_overall
    )

    def write_evidence() -> None:
        output.write_text(
            json.dumps(evidence.to_mapping(configuration), indent=2, sort_keys=True),
            encoding="utf-8",
        )

    try:
        settings = settings_for(database_url).model_copy(
            update={"database_pool_size": 20, "execution_stream_queue_size": 100}
        )
        engine = build_async_engine(settings)
        sessions = build_session_factory(engine)
        setup = await asyncpg.connect(asyncpg_dsn(database_url))
        broker = await aio_pika.connect(amqp_url)
        channel = await broker.channel(publisher_confirms=True, on_return_raises=True)
        suffix = uuid4().hex
        topology_configuration = RabbitMQTopologyConfiguration(
            f"taskforge.m21.dispatch.{suffix}",
            f"taskforge.m21.malformed.{suffix}",
            5.0,
        )
        task_types = TaskTypeRegistry(
            (
                TaskTypeDefinition(
                    "workload.success", "workload.execute", AcceptParameters()
                ),
                TaskTypeDefinition(
                    "workload.redelivery", "workload.execute", AcceptParameters()
                ),
            )
        )
        topology = await declare_dispatch_topology(
            channel, task_types, topology_configuration
        )
        queue = topology.capability_queues["workload.execute"]
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        listener.listen(128)
        api_port = cast(tuple[str, int], listener.getsockname())[1]
        app = create_app(
            settings,
            ReadinessCoordinator(AlwaysReady(), timeout_seconds=1),
            task_types=task_types,
        )
        server = uvicorn.Server(
            uvicorn.Config(app, access_log=False, log_config=None, lifespan="on")
        )
        server_task = asyncio.create_task(server.serve(sockets=[listener]))
        current_phase = "api_start"
        async with asyncio.timeout(configuration.phase_timeout_seconds):
            while not server.started:
                await asyncio.sleep(0)
        current_phase = "seed_and_create_runs"
        owners = await _seed_workflows(sessions, configuration.owner_count)
        run_service = WorkflowRunService(SQLAlchemyWorkflowRunRepository(sessions))
        runs_with_owners = []
        for owner in owners:
            for _ in range(configuration.runs_per_owner):
                run = await run_service.create_run(
                    owner.workflow_id,
                    owner_filter=OwnerFilter.only(owner.principal_id),
                    requested_by_principal_id=owner.principal_id,
                    selection=ExplicitWorkflowVersion(1),
                    input_snapshot=create_workflow_run_input({}, {}),
                )
                runs_with_owners.append((run, owner))
        runs = [run for run, _ in runs_with_owners]
        run_ids = [run.id for run in runs]
        current_phase = "runnable_checkpoint"
        runnable = await setup.fetch(
            "SELECT workflow_run_id, id, step_identifier, status::text "
            "FROM task_runs WHERE workflow_run_id = ANY($1::uuid[]) "
            "ORDER BY workflow_run_id, step_identifier",
            [run.id for run in runs],
        )
        assert len(runs) == 25
        assert len(runnable) == 100
        assert {row["status"] for row in runnable} == {"runnable"}
        assert await setup.fetchval("SELECT count(*) FROM task_attempts") == 0
        assert await setup.fetchval("SELECT count(*) FROM task_dispatch_outbox") == 0
        evidence.checkpoints["runnable"] = {
            "active_runs": 25,
            "task_runs": 100,
            "runnable_tasks": 100,
        }

        current_phase = "consumer_registration"
        workers = [
            await add_worker(setup, capability="workload.execute")
            for _ in range(configuration.worker_count)
        ]

        def handler_for(worker_ordinal: int) -> Any:
            async def handler(context: TaskContext) -> object:
                handler_counts[context.dispatch_id] += 1
                distributions[worker_ordinal].handler_invocations += 1
                active_workers.add(worker_ordinal)
                if len(active_workers) >= 2:
                    concurrent.set()
                await gate.wait()
                return {"ok": True}

            return handler

        for ordinal, worker in enumerate(workers):
            worker_channel = await broker.channel()
            await worker_channel.set_qos(prefetch_count=configuration.worker_prefetch)
            worker_queue = await worker_channel.get_queue(queue.name)
            registry = TaskHandlerRegistry(
                (
                    TaskHandlerDefinition(
                        "workload.success", "workload.execute", handler_for(ordinal)
                    ),
                    TaskHandlerDefinition(
                        "workload.redelivery", "workload.execute", handler_for(ordinal)
                    ),
                ),
                task_types,
            )
            consumer = WorkerExecutionConsumer(
                TaskClaimService(
                    SQLAlchemyTaskClaimRepository(
                        sessions, worker_stale_after_seconds=30
                    ),
                    issuer,
                    lease_seconds=60,
                ),
                TaskStartService(SQLAlchemyTaskStartRepository(sessions)),
                TaskResultSubmissionService(
                    SQLAlchemyTaskResultRepository(sessions),
                    issuer,
                    rate_limiter=AllowAllRateLimiter(),
                ),
                registry,
                worker.authenticated,
                worker.session_id,
            )

            async def delivery(
                control: DispatchDeliveryControl,
                *,
                worker_ordinal: int = ordinal,
                coordinator: WorkerExecutionConsumer = consumer,
            ) -> None:
                message_id = control.delivery.metadata.message_id
                assert message_id is not None
                dispatch_id = UUID(message_id)
                distributions[worker_ordinal].deliveries += 1
                delivery_counts[dispatch_id] += 1
                if control.delivery.redelivered:
                    distributions[worker_ordinal].redelivered += 1
                    redelivered_counts[dispatch_id] += 1
                await coordinator.consume(
                    RequeueOnceControl(
                        control, dispatch_id, selected_dispatches, requeued
                    )
                )

            runtime = WorkerDispatchRuntime(
                RabbitMQDispatchConsumer(worker_queue), delivery
            )
            await runtime.start()
            runtimes.append(runtime)
        assert len(runtimes) == 8

        async def subscribe(run_id: UUID, credential: str) -> None:
            nonlocal subscriber_ready_count
            messages: list[dict[str, Any]] = []
            subscriber_messages[run_id] = messages
            async with httpx2.AsyncClient(
                timeout=configuration.phase_timeout_seconds
            ) as client:
                async with client.websocket(
                    f"ws://127.0.0.1:{api_port}/api/v1/workflow-runs/{run_id}/stream",
                    params={"cursor": "0"},
                    headers={"Authorization": f"Bearer {credential}"},
                ) as websocket:
                    subscriber_ready_count += 1
                    if subscriber_ready_count == configuration.subscriber_count:
                        subscriber_ready.set()
                    while True:
                        message = cast(
                            dict[str, Any],
                            await websocket.receive_json(
                                timeout=configuration.overall_timeout_seconds
                            ),
                        )
                        messages.append(message)
                        event = message.get("event")
                        if (
                            isinstance(event, dict)
                            and event.get("event_type") == "workflow_run.status_changed"
                            and event.get("payload", {}).get("status") == "succeeded"
                        ):
                            return

        current_phase = "websocket_handshakes"
        subscriber_tasks = [
            asyncio.create_task(subscribe(run.id, owner.credential))
            for run, owner in runs_with_owners
        ]
        try:
            async with asyncio.timeout(configuration.phase_timeout_seconds):
                await subscriber_ready.wait()
        except TimeoutError as error:
            failures = [
                repr(task.exception())
                for task in subscriber_tasks
                if task.done() and not task.cancelled()
            ]
            raise AssertionError(
                f"only {subscriber_ready_count} authenticated subscribers became "
                f"ready; completed failures={failures}"
            ) from error
        assert subscriber_ready_count == 25
        evidence.checkpoints["websocket"] = {
            "authenticated_handshakes": 25,
            "authorized_run_subscriptions": 25,
            "owner_principals": 5,
            "connections_per_principal": 5,
        }

        if observer is not None:
            current_phase = "measurement_start"
            assert broker.transport is not None
            server_properties = broker.transport.connection.server_properties
            await observer.start(
                api_port=api_port,
                runs_with_owners=runs_with_owners,
                database_url=database_url,
                broker_metadata={
                    "product": str(server_properties.get("product", "unavailable")),
                    "version": str(server_properties.get("version", "unavailable")),
                },
                setup=setup,
                runtimes=runtimes,
            )

        current_phase = "dispatch_creation"
        dispatch_service = TaskDispatchService(
            SQLAlchemyTaskDispatchRepository(sessions), task_types
        )
        redelivery_runs = {
            runs[index].id for index in configuration.redelivery_run_ordinals
        }
        dispatched = []
        for row in runnable:
            item = await dispatch_service.dispatch_task(
                row["workflow_run_id"], row["id"]
            )
            dispatched.append(item)
            if (
                row["workflow_run_id"] in redelivery_runs
                and row["step_identifier"] == "root-00"
            ):
                selected_dispatches.add(item.dispatch_id)
        assert len(dispatched) == 100
        publication_counts["durable_dispatches"] = len(dispatched)
        current_phase = "broker_publication"
        publisher = TaskDispatchPublisher(
            SQLAlchemyDispatchOutboxRepository(sessions),
            RabbitMQDispatchPublisher(topology.exchange, timeout_seconds=5),
        )
        publication = await publisher.reconcile_unpublished(
            page_size=100, pass_limit=100
        )
        assert publication.acknowledged == 100
        publication_counts["published_messages"] = publication.acknowledged
        declaration = await queue.declare()
        evidence.checkpoints["publication"] = {
            "durable_dispatches": 100,
            "published_messages": 100,
            "ready_messages": declaration.message_count or 0,
            "unacknowledged_messages": sum(runtime.in_flight for runtime in runtimes),
            "admitted_deliveries": sum(runtime.in_flight for runtime in runtimes),
            "handler_entries": sum(item.handler_invocations for item in distributions),
        }
        current_phase = "worker_contention"
        async with asyncio.timeout(configuration.phase_timeout_seconds):
            await concurrent.wait()
        evidence.checkpoints["contention"] = {
            "registered_consumers": 8,
            "distinct_executing_workers": len(active_workers),
        }
        gate.set()

        current_phase = "execution_completion"
        async with asyncio.timeout(configuration.phase_timeout_seconds):
            while True:
                succeeded_tasks = await setup.fetchval(
                    "SELECT count(*) FROM task_runs WHERE workflow_run_id = ANY($1::uuid[]) "
                    "AND status::text = 'succeeded'",
                    [run.id for run in runs],
                )
                if succeeded_tasks == 100 and selected_dispatches <= requeued:
                    break
                await asyncio.sleep(0.05)
        current_phase = "run_reconciliation"
        for run in runs:
            reconciled = await run_service.reconcile_workflow_run(run.id)
            assert reconciled.final_status is not None
            assert reconciled.final_status.value == "succeeded"
        async with asyncio.timeout(configuration.phase_timeout_seconds):
            await asyncio.gather(*subscriber_tasks)

        current_phase = "authoritative_invariants"
        assert (
            await setup.fetchval(
                "SELECT count(*) FROM task_runs WHERE workflow_run_id = ANY($1::uuid[]) "
                "AND status::text = 'succeeded'",
                [run.id for run in runs],
            )
            == 100
        )
        assert await setup.fetchval("SELECT count(*) FROM task_attempts") == 100
        assert await setup.fetchval("SELECT count(*) FROM task_attempt_results") == 100
        assert set(handler_counts.values()) == {1}
        assert selected_dispatches == requeued
        assert all(delivery_counts[item] >= 2 for item in selected_dispatches)
        assert all(redelivered_counts[item] >= 1 for item in selected_dispatches)
        for run in runs:
            messages = subscriber_messages[run.id]
            events = [message["event"] for message in messages if "event" in message]
            cursors = [event["cursor"] for event in events]
            assert all(left < right for left, right in pairwise(cursors))
            persisted = await setup.fetch(
                "SELECT id, cursor, task_run_id, event_type, payload "
                "FROM workflow_run_execution_events WHERE workflow_run_id = $1 "
                "ORDER BY cursor",
                run.id,
            )
            persisted_by_id = {str(row["id"]): row for row in persisted}
            observed_order = [(event["id"], event["cursor"]) for event in events]
            persisted_order = [(str(row["id"]), row["cursor"]) for row in persisted]
            assert observed_order == persisted_order
            for event in events:
                assert event["workflow_run_id"] == str(run.id)
                row = persisted_by_id[event["id"]]
                assert row["cursor"] == event["cursor"]
                assert (
                    str(row["task_run_id"]) if row["task_run_id"] is not None else None
                ) == event["task_run_id"]
                assert row["event_type"] == event["event_type"]
                payload = (
                    json.loads(row["payload"])
                    if isinstance(row["payload"], str)
                    else row["payload"]
                )
                assert payload == event["payload"]
            assert events[-1]["cursor"] == persisted[-1]["cursor"]
            assert events[-1]["payload"]["status"] == "succeeded"

        current_phase = "broker_drain"
        async with asyncio.timeout(configuration.phase_timeout_seconds):
            while True:
                declaration = await queue.declare()
                ready_messages = declaration.message_count or 0
                worker_in_flight = sum(runtime.in_flight for runtime in runtimes)
                if ready_messages == 0 and worker_in_flight == 0:
                    break
                await asyncio.sleep(0.05)
        assert ready_messages == 0
        assert worker_in_flight == 0
        evidence.checkpoints["broker_drain"] = {
            "ready_messages": ready_messages,
            "worker_in_flight": worker_in_flight,
        }

        if observer is not None:
            current_phase = "measurement_finish"
            await observer.finish(
                setup=setup,
                run_ids=run_ids,
                workers=workers,
                runtimes=runtimes,
            )

        current_phase = "write_success_evidence"
        evidence.status = "passed"
        evidence.worker_distribution = distributions
        evidence.redelivery_dispatches = [
            {
                "dispatch_id": str(dispatch_id),
                "deliveries": delivery_counts[dispatch_id],
                "redelivered": redelivered_counts[dispatch_id],
                "handler_invocations": handler_counts[dispatch_id],
            }
            for dispatch_id in sorted(selected_dispatches, key=str)
        ]
        evidence.subscriber_summaries = [
            {
                "authenticated": True,
                "workflow_run_id": str(run.id),
                "event_count": len(subscriber_messages[run.id]),
                "last_cursor": max(
                    message["event"]["cursor"]
                    for message in subscriber_messages[run.id]
                    if "event" in message
                ),
            }
            for run in runs
        ]
        write_evidence()
        if emit_evidence:
            print(output.read_text(encoding="utf-8"))
    except BaseException as error:
        evidence.status = "failed"
        diagnostics: dict[str, Any] = {
            "publication": publication_counts,
            "subscriber_ready_count": subscriber_ready_count,
            "subscriber_last_cursors": {
                str(run_id): max(
                    (
                        message["event"]["cursor"]
                        for message in messages
                        if "event" in message
                    ),
                    default=0,
                )
                for run_id, messages in list(subscriber_messages.items())[:25]
            },
            "worker_in_flight": [runtime.in_flight for runtime in runtimes[:8]],
        }
        if run_ids and setup is not None:
            try:
                rows = await setup.fetch(
                    "SELECT status::text, count(*) FROM task_runs "
                    "WHERE workflow_run_id = ANY($1::uuid[]) GROUP BY status::text",
                    run_ids,
                    timeout=2,
                )
                diagnostics["task_status_counts"] = {
                    row["status"]: row["count"] for row in rows
                }
            except Exception as diagnostic_error:
                diagnostics["task_status_error"] = type(diagnostic_error).__name__
        try:
            if queue is None:
                raise RuntimeError("workload queue was not declared")
            declaration = await asyncio.wait_for(queue.declare(), timeout=2)
            diagnostics["broker_ready_messages"] = declaration.message_count or 0
        except Exception as diagnostic_error:
            diagnostics["broker_observation_error"] = type(diagnostic_error).__name__
        evidence.failure = {
            "phase": current_phase,
            "error_type": (
                "TimeoutError"
                if overall_expired and isinstance(error, asyncio.CancelledError)
                else type(error).__name__
            ),
            "diagnostics": diagnostics,
        }
        write_evidence()
        if overall_expired and isinstance(error, asyncio.CancelledError):
            primary_error = TimeoutError("overall workload deadline exceeded")
            primary_error.__cause__ = error
        else:
            primary_error = error
    finally:
        overall_deadline.cancel()
        gate.set()

        async def cleanup_step(name: str, operation: Any) -> Any:
            try:
                return await asyncio.wait_for(
                    operation, timeout=configuration.phase_timeout_seconds
                )
            except BaseException as cleanup_error:
                cleanup_errors.append(
                    {"operation": name, "error_type": type(cleanup_error).__name__}
                )
                return None

        for task in subscriber_tasks:
            if not task.done():
                task.cancel()
        subscriber_results = await cleanup_step(
            "websocket_subscribers",
            asyncio.gather(*subscriber_tasks, return_exceptions=True),
        )
        if subscriber_results is not None:
            cleanup_errors.extend(
                {
                    "operation": "websocket_subscriber",
                    "error_type": type(result).__name__,
                }
                for result in subscriber_results
                if isinstance(result, BaseException)
                and not isinstance(result, asyncio.CancelledError)
            )
        worker_results = await cleanup_step(
            "worker_runtimes",
            asyncio.gather(
                *(runtime.shutdown() for runtime in runtimes), return_exceptions=True
            ),
        )
        if worker_results is not None:
            cleanup_errors.extend(
                {
                    "operation": "worker_runtime",
                    "error_type": type(result).__name__,
                }
                for result in worker_results
                if isinstance(result, BaseException)
            )
        if observer is not None:
            await cleanup_step("measurement_observer", observer.close())
        if server is not None and server_task is not None:
            server.should_exit = True
            await cleanup_step("uvicorn_server", server_task)
            if not server_task.done():
                server_task.cancel()
                await cleanup_step(
                    "uvicorn_server_cancellation",
                    asyncio.gather(server_task, return_exceptions=True),
                )
        if listener is not None:
            try:
                listener.close()
            except BaseException as cleanup_error:
                cleanup_errors.append(
                    {
                        "operation": "api_listener",
                        "error_type": type(cleanup_error).__name__,
                    }
                )
        if broker is not None and not broker.is_closed:
            cleanup = await cleanup_step("rabbitmq_cleanup_channel", broker.channel())
            if cleanup is not None:
                if queue is not None:
                    await cleanup_step(
                        "rabbitmq_workload_queue",
                        cleanup.queue_delete(
                            queue.name, if_unused=False, if_empty=False
                        ),
                    )
                if topology is not None:
                    await cleanup_step(
                        "rabbitmq_quarantine_queue",
                        cleanup.queue_delete(
                            topology.quarantine_queue.name,
                            if_unused=False,
                            if_empty=False,
                        ),
                    )
                if topology_configuration is not None:
                    await cleanup_step(
                        "rabbitmq_dispatch_exchange",
                        cleanup.exchange_delete(
                            topology_configuration.dispatch_exchange_name
                        ),
                    )
                    await cleanup_step(
                        "rabbitmq_malformed_exchange",
                        cleanup.exchange_delete(
                            topology_configuration.malformed_exchange_name
                        ),
                    )
            await cleanup_step("rabbitmq_connection", broker.close())
        if setup is not None:
            await cleanup_step("postgresql_diagnostic_connection", setup.close())
        if engine is not None:
            await cleanup_step("sqlalchemy_engine", engine.dispose())

    if cleanup_errors:
        evidence.status = "failed"
        if evidence.failure is None:
            evidence.failure = {
                "phase": "cleanup",
                "error_type": "CleanupError",
                "diagnostics": {},
            }
        evidence.failure.setdefault("diagnostics", {})["cleanup_errors"] = (
            cleanup_errors
        )
        write_evidence()
    if primary_error is not None:
        raise primary_error.with_traceback(primary_error.__traceback__)
    if cleanup_errors:
        raise RuntimeError("M21 workload cleanup failed")
    return evidence
