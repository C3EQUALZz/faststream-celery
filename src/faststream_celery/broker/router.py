from collections.abc import Awaitable, Callable, Iterable, Sequence
from typing import TYPE_CHECKING, Any, Optional

from faststream.middlewares import AckPolicy

from faststream_celery._internal import (
    EMPTY,
    ArgsContainer,
    BrokerRouter,
    SubscriberRoute,
)
from faststream_celery.configs import CeleryRouterConfig
from faststream_celery.message import ConsumerMessage

from .registrator import CeleryRegistrator

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant

    from faststream_celery._internal import (
        BrokerMiddleware,
        CustomCallable,
        SendableMessage,
    )


class CeleryPublisherArgs(ArgsContainer):
    """Delayed CeleryPublisher registration object.

    Just a copy of CeleryRegistrator.publisher(...) arguments.
    """

    def __init__(  # ruff: ignore[too-many-arguments]
        self,
        queue: str,
        *,
        exchange: str | None = None,
        routing_key: str | None = None,
        headers: dict[str, Any] | None = None,
        reply_to: str = "",
        title: str | None = None,
        description: str | None = None,
        schema: Any | None = None,
        include_in_schema: bool = True,
    ) -> None:
        super().__init__(
            queue=queue,
            exchange=exchange,
            routing_key=routing_key,
            headers=headers,
            reply_to=reply_to,
            title=title,
            description=description,
            schema=schema,
            include_in_schema=include_in_schema,
        )


class CeleryRoute(SubscriberRoute):
    """Class to store delayed CeleryBroker subscriber registration."""

    def __init__(  # ruff: ignore[too-many-arguments]
        self,
        call: Callable[..., "SendableMessage"]
        | Callable[..., Awaitable["SendableMessage"]],
        queue: str,
        *,
        publishers: Iterable[CeleryPublisherArgs] = (),
        task: str | None = None,
        prefetch_count: int | None = None,
        dependencies: Iterable["Dependant"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        ack_policy: AckPolicy = EMPTY,
        no_reply: bool = False,
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
        max_workers: int | None = None,
    ) -> None:
        """Initialize the CeleryRoute.

        Args:
            call: Message handler function to wrap the same with
                `@broker.subscriber(...)` way.
            queue: Celery queue name to consume tasks from.
            publishers: Celery publishers to broadcast the handler result.
            task: Celery task name (`headers["task"]`) to filter messages by.
            prefetch_count: kombu QoS prefetch count.
            dependencies: Dependencies list (`[Dependant(),]`) to apply to the subscriber.
            parser: Parser to map the original kombu Message to a FastStream one.
            decoder: Function to decode the FastStream msg body to python objects.
            ack_policy: Acknowledgement policy of the handler.
            no_reply: Whether to disable **FastStream** RPC and Reply To auto
                responses or not.
            title: AsyncAPI subscriber object title.
            description: AsyncAPI subscriber object description.
                Uses decorated docstring as default.
            include_in_schema: Whether to include operation in AsyncAPI schema or not.
            max_workers: Number of workers to process messages concurrently.
        """
        super().__init__(
            call,
            queue=queue,
            publishers=publishers,
            task=task,
            prefetch_count=prefetch_count,
            dependencies=dependencies,
            parser=parser,
            decoder=decoder,
            ack_policy=ack_policy,
            no_reply=no_reply,
            title=title,
            description=description,
            include_in_schema=include_in_schema,
            max_workers=max_workers,
        )


class CeleryRouter(
    CeleryRegistrator,
    BrokerRouter[ConsumerMessage],
):
    """Includable to CeleryBroker router."""

    def __init__(  # ruff: ignore[too-many-arguments]
        self,
        prefix: str = "",
        handlers: Iterable[CeleryRoute] = (),
        *,
        dependencies: Iterable["Dependant"] = (),
        middlewares: Sequence["BrokerMiddleware[Any, Any]"] = (),
        routers: Iterable[CeleryRegistrator] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        include_in_schema: bool | None = None,
        ack_policy: AckPolicy = EMPTY,
    ) -> None:
        """Initialize the CeleryRouter.

        Args:
            prefix: String prefix to add to all subscribers queues.
            handlers: Route objects to include.
            dependencies: Dependencies list (`[Dependant(),]`) to apply to all
                routers' publishers/subscribers.
            middlewares: Router middlewares to apply to all routers'
                publishers/subscribers.
            routers: Routers to apply to broker.
            parser: Parser to map the original kombu Message to a FastStream one.
            decoder: Function to decode the FastStream msg body to python objects.
            include_in_schema: Whether to include operation in AsyncAPI schema or not.
            ack_policy: Default acknowledgement policy for all subscribers in this router.
                Can be overridden at the subscriber level.
        """
        super().__init__(
            handlers=handlers,
            config=CeleryRouterConfig(
                prefix=prefix,
                ack_policy=ack_policy,
                broker_dependencies=dependencies,
                broker_middlewares=middlewares,
                broker_parser=parser,
                broker_decoder=decoder,
                include_in_schema=include_in_schema,
            ),
            routers=routers,
        )
