"""A failed chain step completes frozen downstream results without executing them."""

import pytest
from faststream.middlewares import AckPolicy

from faststream_celery import CeleryBroker, CeleryTask, TestCeleryBroker, signature
from faststream_celery.annotations import CeleryMessage
from faststream_celery.backend import InMemoryResultBackend


@pytest.mark.asyncio()
@pytest.mark.parametrize("backend_url", (None, "redis://localhost"))
@pytest.mark.parametrize("no_reply", (False, True))
@pytest.mark.parametrize("ignore_result", (False, True))
async def test_failure_completes_frozen_chain_results(
    queue: str,
    backend_url: str | None,
    *,
    no_reply: bool,
    ignore_result: bool,
) -> None:
    broker = CeleryBroker(result_backend=backend_url)
    observer = CeleryBroker()
    root_replies = observer.publisher(f"{queue}.root-replies")
    chain_replies = observer.publisher(f"{queue}.chain-replies")
    executed: list[str] = []
    error = ValueError("middle failed")

    @broker.subscriber(queue, task="tests.middle", no_reply=no_reply)
    async def middle() -> None:
        raise error

    @broker.subscriber(queue)
    async def following(message: CeleryMessage) -> None:
        executed.append(str(message.headers.get("task")))

    task = CeleryTask(
        "tests.middle",
        chain=[
            signature(
                "tests.last",
                options={
                    "task_id": "last-id",
                    "reply_to": f"{queue}.chain-replies",
                },
            ),
            signature("tests.unfrozen"),
            signature("tests.next", options={"task_id": "next-id"}),
        ],
    )

    async with TestCeleryBroker(broker, observer):
        with pytest.raises(ValueError, match="middle failed") as caught:
            await broker.publish(
                task,
                queue=queue,
                correlation_id="middle-id",
                reply_to=f"{queue}.root-replies",
                headers={"ignore_result": ignore_result},
            )
        assert caught.value is error
        assert executed == []

        backend = broker.config.broker_config.result_backend
        if backend_url is not None:
            assert isinstance(backend, InMemoryResultBackend)
            if ignore_result:
                assert backend.results == {}
            else:
                last_result = await backend.wait("last-id", timeout=0.02)
                assert last_result["status"] == "FAILURE"
                assert set(backend.results) == {"middle-id", "next-id", "last-id"}
                for task_id, envelope in backend.results.items():
                    assert envelope["task_id"] == task_id
                    assert envelope["result"] == {
                        "exc_type": "ValueError",
                        "exc_message": ["middle failed"],
                        "exc_module": "builtins",
                    }
                    assert envelope["traceback"] == last_result["traceback"]

        if no_reply or ignore_result:
            root_replies.mock.assert_not_called()
            chain_replies.mock.assert_not_called()
        else:
            root_replies.mock.assert_called_once()
            assert root_replies.mock.call_args.args[0]["task_id"] == "middle-id"
            chain_replies.mock.assert_called_once()
            result = chain_replies.mock.call_args.args[0]
            assert result["task_id"] == "last-id"
            assert result["status"] == "FAILURE"


@pytest.mark.asyncio()
@pytest.mark.parametrize("disposition", ("exception", "nack", "reject"))
async def test_unfinished_header_chain_does_not_notify_chord(
    queue: str,
    disposition: str,
) -> None:
    broker = CeleryBroker(
        result_backend="redis://localhost",
        ack_policy=AckPolicy.NACK_ON_ERROR,
    )

    @broker.subscriber(queue, task="tests.middle")
    async def middle(message: CeleryMessage) -> None:
        if disposition == "exception":
            error = "retry processing"
            raise ValueError(error)
        if disposition == "nack":
            await message.nack()
        else:
            await message.reject()

    task = CeleryTask(
        "tests.middle",
        chain=[
            signature(
                "tests.tail",
                options={
                    "task_id": "tail-id",
                    "group_id": "group-id",
                    "group_index": 1,
                    "chord": signature("tests.body", options={"task_id": "body-id"}),
                },
            ),
        ],
    )
    async with TestCeleryBroker(broker):
        if disposition == "exception":
            with pytest.raises(ValueError, match="retry processing"):
                await broker.publish(task, queue=queue)
        else:
            await broker.publish(task, queue=queue)

        backend = broker.config.broker_config.result_backend
        assert isinstance(backend, InMemoryResultBackend)
        assert backend.results == {}
