from typing import TYPE_CHECKING, Union

from typing_extensions import override

from faststream_celery._internal import FakePublisher
from faststream_celery.response import CeleryPublishCommand
from faststream_celery.schemas.result import build_success

if TYPE_CHECKING:
    from faststream.response import PublishCommand

    from faststream_celery._internal import ProducerProto


class CeleryFakePublisher(FakePublisher):
    """Publisher answering the ``reply_to`` of an incoming Celery task.

    The handler's return value is wrapped in a Celery result envelope so a
    Celery client's ``AsyncResult.get()`` understands it. Replies go to the
    default exchange with the reply queue name as the routing key; the reply
    queue is exclusive to the client and must not be declared by us.
    """

    def __init__(
        self,
        producer: "ProducerProto[CeleryPublishCommand]",
        queue: str,
        task_id: str,
    ) -> None:
        super().__init__(producer=producer)
        self.queue = queue
        self.task_id = task_id

    @override
    def patch_command(
        self,
        cmd: Union["PublishCommand", "CeleryPublishCommand"],
    ) -> "CeleryPublishCommand":
        cmd = super().patch_command(cmd)
        real_cmd = CeleryPublishCommand.from_cmd(cmd, queue=self.queue)

        real_cmd.body = build_success(self.task_id, real_cmd.body)
        real_cmd.correlation_id = self.task_id
        real_cmd.exchange = ""
        real_cmd.declare = False

        return real_cmd
