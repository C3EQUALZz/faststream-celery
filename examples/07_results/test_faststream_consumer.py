"""Testing a backend-configured broker with no Redis running.

`faststream_consumer.py` is built with `result_backend=`, and its handlers'
outcomes are recorded there rather than returned over a reply queue. Under
`TestCeleryBroker` that backend is swapped for an in-memory one, so the whole
path — handler, validation, result envelope — runs offline.

Run:
    pytest test_faststream_consumer.py
"""

import pytest
from pydantic import ValidationError

from faststream_celery import CeleryTask, TestCeleryBroker
from faststream_celery.backend import InMemoryResultBackend

from faststream_consumer import QUEUE, broker

REQUEST_TIMEOUT = 5.0


def recorded_results() -> InMemoryResultBackend:
    """The in-memory backend `TestCeleryBroker` put in place of the Redis one."""
    backend = broker.config.broker_config.result_backend

    assert isinstance(backend, InMemoryResultBackend)
    return backend


@pytest.mark.asyncio()
async def test_a_result_is_recorded_under_its_task_id() -> None:
    async with TestCeleryBroker(broker):
        await broker.publish(
            CeleryTask("examples.build_report", kwargs={"month": "2026-08"}),
            queue=QUEUE,
            correlation_id="task-id-1",
        )

        recorded = recorded_results().results["task-id-1"]

        assert recorded["status"] == "SUCCESS"
        assert recorded["result"] == {
            "status": "built",
            "month": "2026-08",
            "rows": 128,
        }


@pytest.mark.asyncio()
async def test_a_failure_is_recorded_as_an_envelope() -> None:
    async with TestCeleryBroker(broker):
        with pytest.raises(RuntimeError, match="nope"):
            await broker.publish(
                CeleryTask("examples.fail", kwargs={"message": "nope"}),
                queue=QUEUE,
                correlation_id="task-id-2",
            )

        recorded = recorded_results().results["task-id-2"]

        assert recorded["status"] == "FAILURE"
        assert recorded["result"]["exc_type"] == "RuntimeError"


@pytest.mark.asyncio()
async def test_request_reads_the_envelope_back() -> None:
    """With a backend, `request()` returns the Celery envelope, not the value."""
    async with TestCeleryBroker(broker):
        response = await broker.request(
            CeleryTask("examples.build_report", kwargs={"month": "2026-09"}),
            queue=QUEUE,
            timeout=REQUEST_TIMEOUT,
        )
        envelope = await response.decode()

    assert envelope["status"] == "SUCCESS"
    assert envelope["result"]["month"] == "2026-09"


@pytest.mark.asyncio()
async def test_an_invalid_payload_is_recorded_as_a_failure() -> None:
    """Validation runs inside the task, so it fails the task like anything else.

    A Celery client polling this task id reads `FAILURE` with the Pydantic
    error in the traceback — not a timeout.
    """
    async with TestCeleryBroker(broker):
        with pytest.raises(ValidationError):
            await broker.publish(
                CeleryTask("examples.build_report", kwargs={}),
                queue=QUEUE,
                correlation_id="task-id-3",
            )

        recorded = recorded_results().results["task-id-3"]

        assert recorded["status"] == "FAILURE"
        assert recorded["result"]["exc_type"] == "ValidationError"
