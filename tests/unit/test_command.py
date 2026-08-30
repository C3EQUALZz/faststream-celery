import pytest
from faststream.exceptions import SetupError
from faststream.response import PublishCommand, PublishType

from faststream_celery.response import CeleryPublishCommand
from faststream_celery.task import CeleryTask


def test_command_requires_queue() -> None:
    with pytest.raises(SetupError, match="queue"):
        CeleryPublishCommand(
            CeleryTask("proj.tasks.add"),
            _publish_type=PublishType.PUBLISH,
        )


def test_command_queue_property() -> None:
    cmd = CeleryPublishCommand(
        CeleryTask("proj.tasks.add"),
        queue="celery",
        _publish_type=PublishType.PUBLISH,
    )

    assert cmd.queue == "celery"
    assert cmd.destination == "celery"
    assert cmd.exchange is None
    assert cmd.declare


def test_from_cmd_converts_plain_publish_command() -> None:
    plain = PublishCommand(
        "payload",
        correlation_id="cor-1",
        reply_to="reply-queue",
        headers={"h": "v"},
        _publish_type=PublishType.REPLY,
    )

    cmd = CeleryPublishCommand.from_cmd(plain, queue="celery")

    assert cmd.body == "payload"
    assert cmd.queue == "celery"
    assert cmd.correlation_id == "cor-1"
    assert cmd.reply_to == "reply-queue"
    assert cmd.headers == {"h": "v"}
    assert cmd.publish_type is PublishType.REPLY


def test_from_cmd_passes_through_celery_command() -> None:
    original = CeleryPublishCommand(
        CeleryTask("proj.tasks.add"),
        queue="celery",
        _publish_type=PublishType.PUBLISH,
    )

    assert CeleryPublishCommand.from_cmd(original, queue="other") is original
