from typing import TYPE_CHECKING, Union

from faststream_celery._internal import FakePublisher
from faststream_celery.response import CeleryPublishCommand

if TYPE_CHECKING:
    from faststream.response import PublishCommand

    from faststream_celery._internal import ProducerProto


class CeleryFakePublisher(FakePublisher):
    """Publisher to answer ``reply_to`` of an incoming Celery task message.

    Celery AMQP RPC replies go to the default exchange with the reply queue
    name as the routing key; the reply queue is exclusive to the client and
    must not be declared by us.
    """

    def __init__(
        self,
        producer: "ProducerProto[CeleryPublishCommand]",
        queue: str,
    ) -> None:
        super().__init__(producer=producer)
        self.queue = queue

    def patch_command(
        self,
        cmd: Union["PublishCommand", "CeleryPublishCommand"],
    ) -> "CeleryPublishCommand":
        cmd = super().patch_command(cmd)
        real_cmd = CeleryPublishCommand.from_cmd(cmd, queue=self.queue)
        real_cmd.exchange = ""
        real_cmd.declare = False
        return real_cmd
