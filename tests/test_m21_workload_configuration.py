"""Fast contract tests for the opt-in M21 Task 1 workload."""

import pytest

from tests.performance.m21_workload import M21Evidence, M21WorkloadConfiguration


def test_canonical_workload_counts_and_redelivery_selection() -> None:
    workload = M21WorkloadConfiguration()

    assert workload.run_count == 25
    assert workload.task_count == 100
    assert workload.subscriber_count == 25
    assert workload.nominal_in_flight_limit == 32
    assert workload.redelivery_run_ordinals == (0, 5, 10, 15, 20)


@pytest.mark.parametrize(
    "factory",
    [
        lambda: M21WorkloadConfiguration(owner_count=4),
        lambda: M21WorkloadConfiguration(roots_per_run=3),
        lambda: M21WorkloadConfiguration(worker_count=1),
        lambda: M21WorkloadConfiguration(worker_prefetch=0),
        lambda: M21WorkloadConfiguration(phase_timeout_seconds=121.0),
    ],
)
def test_workload_rejects_contract_weakening(factory: object) -> None:
    with pytest.raises(ValueError):
        assert callable(factory)
        factory()


def test_evidence_keeps_publication_and_delivery_dimensions_separate() -> None:
    evidence = M21Evidence(
        checkpoints={
            "publication": {
                "durable_dispatches": 100,
                "published_messages": 100,
                "ready_messages": 68,
                "unacknowledged_messages": 32,
                "admitted_deliveries": 32,
                "handler_entries": 2,
            }
        }
    ).to_mapping(M21WorkloadConfiguration())

    publication = evidence["checkpoints"]["publication"]
    assert publication["published_messages"] == 100
    assert publication["unacknowledged_messages"] == 32
    assert publication["published_messages"] != publication["admitted_deliveries"]


def test_failure_evidence_is_serialized_without_unbounded_exception_text() -> None:
    evidence = M21Evidence(status="failed")
    evidence.failure = {
        "phase": "broker_drain",
        "error_type": "TimeoutError",
        "diagnostics": {
            "task_status_counts": {"succeeded": 100},
            "worker_in_flight": [0] * 8,
        },
    }

    mapping = evidence.to_mapping(M21WorkloadConfiguration())

    assert mapping["status"] == "failed"
    assert mapping["failure"] == evidence.failure
