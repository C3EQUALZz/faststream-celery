"""no_reply controls automatic replies independently of results and canvas."""

import pytest

from faststream_celery import CeleryBroker, CeleryTask, TestCeleryBroker, signature
from faststream_celery.backend import InMemoryResultBackend


@pytest.mark.asyncio()
@pytest.mark.parametrize("no_reply", (False, True))
@pytest.mark.parametrize("fail", (False, True))
@pytest.mark.parametrize("backend", (None, "redis://localhost"))
async def test_no_reply_preserves_results_and_continuations(
    queue: str,
    no_reply: bool,
    fail: bool,
    backend: str | None,
) -> None:
    broker = CeleryBroker(result_backend=backend)
    observer = CeleryBroker()
    replies = observer.publisher(f"{queue}.replies")
    continuations = observer.publisher(f"{queue}.canvas")
    output = observer.publisher(f"{queue}.output")

    @observer.subscriber(f"{queue}.output")
    async def published() -> None: ...

    @broker.subscriber(queue, no_reply=no_reply)
    @broker.publisher(f"{queue}.output")
    async def handler() -> int:
        if fail:
            error = "task failed"
            raise ValueError(error)
        return 7

    callback = signature("tests.following", options={"queue": f"{queue}.canvas"})
    task = CeleryTask(
        "tests.no_reply",
        link=[] if fail else [callback],
        link_error=[callback] if fail else [],
    )

    async with TestCeleryBroker(broker, observer):
        if fail:
            with pytest.raises(ValueError, match="task failed"):
                await broker.publish(
                    task,
                    queue=queue,
                    correlation_id="task-id",
                    reply_to=f"{queue}.replies",
                )
        else:
            await broker.publish(
                task,
                queue=queue,
                correlation_id="task-id",
                reply_to=f"{queue}.replies",
            )

        if no_reply:
            replies.mock.assert_not_called()
        else:
            replies.mock.assert_called_once()
            result = replies.mock.call_args.args[0]
            assert result["task_id"] == "task-id"
            assert result["status"] == ("FAILURE" if fail else "SUCCESS")

        continuations.mock.assert_called_once_with({
            "args": ["task-id" if fail else 7],
            "kwargs": {},
        })
        if fail:
            output.mock.assert_not_called()
        else:
            output.mock.assert_called_once_with(7)

        result_backend = broker.config.broker_config.result_backend
        if backend is None:
            assert result_backend is None
        else:
            assert isinstance(result_backend, InMemoryResultBackend)
            result = result_backend.results["task-id"]
            assert result["status"] == ("FAILURE" if fail else "SUCCESS")
            if fail:
                error = result["result"]
                assert isinstance(error, dict)
                assert error["exc_type"] == "ValueError"
                assert error["exc_message"] == ["task failed"]
            else:
                assert result["result"] == 7


@pytest.mark.asyncio()
@pytest.mark.parametrize("backend", (None, "redis://localhost"))
@pytest.mark.parametrize("fail", (False, True))
async def test_no_reply_requests_can_still_use_a_result_backend(
    queue: str,
    backend: str | None,
    fail: bool,
) -> None:
    broker = CeleryBroker(result_backend=backend)

    @broker.subscriber(queue, no_reply=True)
    async def handler() -> int:
        if fail:
            error = "task failed"
            raise ValueError(error)
        return 7

    async with TestCeleryBroker(broker):
        if backend is None:
            with pytest.raises(TimeoutError, match="No Celery reply"):
                await broker.request(
                    CeleryTask("tests.no_reply"),
                    queue=queue,
                    timeout=0.02,
                )
        else:
            response = await broker.request(
                CeleryTask("tests.no_reply"),
                queue=queue,
                timeout=1,
            )
            envelope = await response.decode()
            assert isinstance(envelope, dict)
            assert envelope["status"] == ("FAILURE" if fail else "SUCCESS")
