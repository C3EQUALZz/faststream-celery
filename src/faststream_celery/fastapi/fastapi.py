import logging
from collections.abc import Callable, Iterable, Sequence
from typing import TYPE_CHECKING, Any, Final, Optional, Union, cast

from fastapi.datastructures import Default
from fastapi.routing import APIRoute
from fastapi.utils import generate_unique_id
from faststream.message import gen_cor_id
from faststream.middlewares import AckPolicy
from starlette.responses import JSONResponse
from typing_extensions import override

from faststream_celery._internal import EMPTY, ContextRepo, IdGenerator
from faststream_celery.broker import CeleryBroker
from faststream_celery.fastapi._internal import StreamRouter
from faststream_celery.message import ConsumerMessage
from faststream_celery.types import MutableHeaders

# FastAPI sentinels, built once instead of on every call.
NO_RESPONSE_MODEL: Final[Any] = Default(None)
DEFAULT_RESPONSE_CLASS: Final[Any] = Default(JSONResponse)
DEFAULT_UNIQUE_ID_FUNCTION: Final[Any] = Default(generate_unique_id)

if TYPE_CHECKING:
    from enum import Enum

    from fast_depends.library.serializer import SerializerProto
    from fastapi import params
    from fastapi.types import IncEx
    from faststream.security import BaseSecurity
    from faststream.specification.base import SpecificationFactory
    from faststream.specification.schema.extra import Tag, TagDict
    from starlette.responses import Response
    from starlette.routing import BaseRoute
    from starlette.types import ASGIApp, Lifespan

    from faststream_celery._internal import (
        BrokerMiddleware,
        CodecProto,
        CustomCallable,
    )
    from faststream_celery.publisher import CeleryPublisher
    from faststream_celery.subscriber import CelerySubscriber


class CeleryRouter(StreamRouter[ConsumerMessage]):
    """A Celery router that plugs into a FastAPI application.

    Example:
        ```python
        router = CeleryRouter("amqp://guest:guest@localhost:5672//")


        @router.subscriber("celery", task="proj.tasks.add")
        async def add(args: list[int], kwargs: dict) -> int:
            return sum(args)


        app = FastAPI(lifespan=router.lifespan_context)
        app.include_router(router)
        ```
    """

    broker_class = CeleryBroker
    broker: CeleryBroker

    def __init__(  # ruff: ignore[too-many-arguments]
        self,
        url: str = "amqp://guest:guest@localhost:5672//",
        *,
        # CeleryBroker options
        transport_options: dict[str, Any] | None = None,
        ssl: bool | dict[str, Any] | None = None,
        security: Optional["BaseSecurity"] = None,
        max_workers: int = 1,
        prefetch_count: int | None = None,
        graceful_timeout: float | None = 15.0,
        ack_policy: AckPolicy = EMPTY,
        middlewares: Sequence["BrokerMiddleware[Any, Any]"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        codec: Optional["CodecProto"] = None,
        serializer: Optional["SerializerProto"] = None,
        logger: Any = EMPTY,
        log_level: int = logging.INFO,
        id_generator: "IdGenerator" = gen_cor_id,
        # FastAPI options
        context: Optional["ContextRepo"] = None,
        prefix: str = "",
        tags: list[Union[str, "Enum"]] | None = None,
        dependencies: Sequence["params.Depends"] | None = None,
        default_response_class: type["Response"] = DEFAULT_RESPONSE_CLASS,
        responses: dict[int | str, dict[str, Any]] | None = None,
        callbacks: list["BaseRoute"] | None = None,
        routes: list["BaseRoute"] | None = None,
        redirect_slashes: bool = True,
        default: Optional["ASGIApp"] = None,
        dependency_overrides_provider: Any | None = None,
        route_class: type["APIRoute"] = APIRoute,
        on_startup: Sequence[Callable[[], Any]] | None = None,
        on_shutdown: Sequence[Callable[[], Any]] | None = None,
        deprecated: bool | None = None,
        include_in_schema: bool = True,
        setup_state: bool = True,
        lifespan: Optional["Lifespan[Any]"] = None,
        generate_unique_id_function: Callable[["APIRoute"], str] = (
            DEFAULT_UNIQUE_ID_FUNCTION
        ),
        # AsyncAPI options
        specification: Optional["SpecificationFactory"] = None,
        specification_tags: Iterable[Union["Tag", "TagDict"]] = (),
        schema_url: str | None = "/asyncapi",
    ) -> None:
        """Initialize the router.

        Args:
            url: kombu connection url.
            transport_options: kombu transport options.
            ssl: kombu `ssl` connection option.
            security: FastStream security object.
            max_workers: Default number of workers per subscriber.
            prefetch_count: kombu QoS prefetch count.
            graceful_timeout: Graceful shutdown timeout.
            ack_policy: Default acknowledgement policy for all subscribers.
            middlewares: Broker middlewares.
            parser: Parser to map the original kombu Message to a FastStream one.
            decoder: Function to decode the FastStream msg body.
            codec: Custom codec object.
            serializer: Custom serializer object.
            logger: Custom logger object.
            log_level: Log level for the broker logger.
            id_generator: Custom message id generator.
            context: FastStream context repository.
            prefix: HTTP route prefix.
            tags: HTTP route tags.
            dependencies: FastAPI dependencies applied to the HTTP routes.
            default_response_class: FastAPI default response class.
            responses: FastAPI additional responses.
            callbacks: FastAPI OpenAPI callbacks.
            routes: Pre-built routes to include.
            redirect_slashes: Whether to redirect on a missing trailing slash.
            default: Fallback ASGI application.
            dependency_overrides_provider: FastAPI dependency override provider.
            route_class: FastAPI route class.
            on_startup: Startup callbacks.
            on_shutdown: Shutdown callbacks.
            deprecated: Whether the routes are deprecated.
            include_in_schema: Whether to include the routes in the OpenAPI schema.
            setup_state: Whether to put the broker into the application state.
            lifespan: Application lifespan.
            generate_unique_id_function: FastAPI operation id factory.
            specification: AsyncAPI specification factory.
            specification_tags: AsyncAPI broker tags.
            schema_url: Path the AsyncAPI schema is served at.
        """
        super().__init__(
            url,
            transport_options=transport_options,
            ssl=ssl,
            security=security,
            max_workers=max_workers,
            prefetch_count=prefetch_count,
            graceful_timeout=graceful_timeout,
            ack_policy=ack_policy,
            middlewares=middlewares,
            parser=parser,
            decoder=decoder,
            codec=codec,
            serializer=serializer,
            logger=logger,
            log_level=log_level,
            id_generator=id_generator,
            context=context,
            prefix=prefix,
            tags=tags,
            dependencies=dependencies,
            default_response_class=default_response_class,
            responses=responses,
            callbacks=callbacks,
            routes=routes,
            redirect_slashes=redirect_slashes,
            default=default,
            dependency_overrides_provider=dependency_overrides_provider,
            route_class=route_class,
            on_startup=on_startup,
            on_shutdown=on_shutdown,
            deprecated=deprecated,
            include_in_schema=include_in_schema,
            setup_state=setup_state,
            lifespan=lifespan,
            generate_unique_id_function=generate_unique_id_function,
            specification=specification,
            specification_tags=specification_tags,
            schema_url=schema_url,
        )

    @override
    def subscriber(  # type: ignore[override]
        self,
        queue: str,
        *,
        task: str | None = None,
        prefetch_count: int | None = None,
        ack_policy: AckPolicy = EMPTY,
        max_workers: int | None = None,
        # broker arguments
        dependencies: Iterable["params.Depends"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        codec: Optional["CodecProto"] = None,
        no_reply: bool = False,
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
        # FastAPI args
        response_model: Any = NO_RESPONSE_MODEL,
        response_model_include: Optional["IncEx"] = None,
        response_model_exclude: Optional["IncEx"] = None,
        response_model_by_alias: bool = True,
        response_model_exclude_unset: bool = False,
        response_model_exclude_defaults: bool = False,
        response_model_exclude_none: bool = False,
    ) -> "CelerySubscriber":
        """Subscribe a FastAPI-style handler to a Celery queue."""
        return cast(
            "CelerySubscriber",
            super().subscriber(
                queue=queue,
                task=task,
                prefetch_count=prefetch_count,
                ack_policy=ack_policy,
                max_workers=max_workers,
                dependencies=dependencies,
                parser=parser,
                decoder=decoder,
                codec=codec,
                no_reply=no_reply,
                title=title,
                description=description,
                include_in_schema=include_in_schema,
                # FastAPI args
                response_model=response_model,
                response_model_include=response_model_include,
                response_model_exclude=response_model_exclude,
                response_model_by_alias=response_model_by_alias,
                response_model_exclude_unset=response_model_exclude_unset,
                response_model_exclude_defaults=response_model_exclude_defaults,
                response_model_exclude_none=response_model_exclude_none,
            ),
        )

    @override
    def publisher(  # type: ignore[override]
        self,
        queue: str,
        *,
        exchange: str | None = None,
        routing_key: str | None = None,
        headers: MutableHeaders | None = None,
        reply_to: str = "",
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        schema: Any | None = None,
        include_in_schema: bool = True,
    ) -> "CeleryPublisher":
        """Create a publisher the handler's return value is sent through."""
        return self.broker.publisher(
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
