from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Any, Optional, cast

from faststream.exceptions import SetupError
from faststream.middlewares import AckPolicy
from typing_extensions import override

from faststream_celery._internal import (
    EMPTY,
    Registrator,
)
from faststream_celery.configs import CeleryBrokerConfig
from faststream_celery.message import ConsumerMessage
from faststream_celery.publisher.factory import create_publisher
from faststream_celery.subscriber.factory import create_subscriber

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant

    from faststream_celery._internal import (
        BrokerMiddleware,
        CodecProto,
        CustomCallable,
    )
    from faststream_celery.publisher import CeleryPublisher
    from faststream_celery.subscriber import CelerySubscriber


class CeleryRegistrator(Registrator[ConsumerMessage, CeleryBrokerConfig]):
    """Includable to CeleryBroker router."""

    @override
    def subscriber(  # type: ignore[override]
        self,
        queue: str,
        *,
        task: str | None = None,
        prefetch_count: int | None = None,
        ack_policy: AckPolicy = EMPTY,
        # broker arguments
        dependencies: Iterable["Dependant"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        codec: Optional["CodecProto"] = None,
        no_reply: bool = False,
        persistent: bool = True,
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
        max_workers: int | None = None,
    ) -> "CelerySubscriber":
        """Subscribe a handler to a Celery queue.

        Args:
            queue: Celery queue name to consume tasks from.
            task: Celery task name (`headers["task"]`) to filter messages by.
                Without it, the handler accepts every task in the queue.
            prefetch_count: kombu QoS prefetch count (broker connection-level
                `prefetch_count` or `max_workers` by default).
            ack_policy: Acknowledgement policy for message processing.
            dependencies: Dependencies list (`[Depends(),]`) to apply to the subscriber.
            parser: Parser to map the original kombu Message to a FastStream one.
            decoder: Function to decode the FastStream msg body to python objects.
            codec: Custom codec object.
            no_reply: Whether to disable **FastStream** RPC and Reply To auto
                responses or not.
            persistent: Whether to make the subscriber persistent or not.
            title: AsyncAPI subscriber object title.
            description: AsyncAPI subscriber object description.
                Uses decorated docstring as default.
            include_in_schema: Whether to include operation in AsyncAPI schema or not.
            max_workers: Number of workers to process messages concurrently.

        Returns:
            CelerySubscriber: The subscriber object.
        """
        subscriber = create_subscriber(
            queue=queue,
            task=task,
            # subscriber args
            max_workers=max_workers,
            prefetch_count=prefetch_count,
            no_reply=no_reply,
            ack_policy=ack_policy,
            config=cast("CeleryBrokerConfig", self.config),
            # AsyncAPI
            title_=title,
            description_=description,
            include_in_schema=include_in_schema,
        )

        super().subscriber(subscriber, persistent=persistent)

        return subscriber.add_call(
            parser_=parser or self._parser,
            decoder_=decoder or self._decoder,
            codec_=codec,
            dependencies_=dependencies,
        )

    @override
    def publisher(  # type: ignore[override]
        self,
        queue: str,
        *,
        exchange: str | None = None,
        routing_key: str | None = None,
        headers: dict[str, Any] | None = None,
        reply_to: str = "",
        persistent: bool = True,
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        schema: Any | None = None,
        include_in_schema: bool = True,
    ) -> "CeleryPublisher":
        """Creates long-living and AsyncAPI-documented publisher object.

        You can use it as a handler decorator (handler should be decorated by
        `@broker.subscriber(...)` too) - `@broker.publisher(...)`.
        In such case publisher will publish your handler return value.

        Or you can create a publisher object to call it lately -
        `broker.publisher(...).publish(...)`.

        Args:
            queue: Celery queue name to publish tasks to.
            exchange: Exchange name to publish to (the queue name by default,
                mirroring Celery's default queue declaration).
            routing_key: Routing key to publish with (the queue name by default).
            headers: Message headers to store meta-information. Can be overridden
                by `publish.headers` if specified.
            reply_to: Reply message destination queue name.
            persistent: Whether to make the publisher persistent or not.
            title: AsyncAPI publisher object title.
            description: AsyncAPI publisher object description.
            schema: AsyncAPI publishing message type. Should be any python-native
                object annotation or `pydantic.BaseModel`.
            include_in_schema: Whether to include operation in AsyncAPI schema or not.
        """
        publisher = create_publisher(
            queue=queue,
            exchange=exchange,
            routing_key=routing_key,
            headers=headers,
            reply_to=reply_to,
            config=cast("CeleryBrokerConfig", self.config),
            # AsyncAPI
            title_=title,
            description_=description,
            schema_=schema,
            include_in_schema=include_in_schema,
        )

        super().publisher(publisher, persistent=persistent)

        return publisher

    @override
    def include_router(
        self,
        router: "CeleryRegistrator",  # type: ignore[override]
        *,
        prefix: str = "",
        dependencies: Iterable["Dependant"] = (),
        middlewares: Sequence["BrokerMiddleware[Any, Any]"] = (),
        include_in_schema: bool | None = None,
    ) -> None:
        if not isinstance(router, CeleryRegistrator):
            msg = (
                f"Router must be an instance of CeleryRegistrator, "
                f"got {type(router).__name__} instead"
            )
            raise SetupError(msg)

        super().include_router(
            router,
            prefix=prefix,
            dependencies=dependencies,
            middlewares=middlewares,
            include_in_schema=include_in_schema,
        )
