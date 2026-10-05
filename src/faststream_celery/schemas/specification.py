"""Construct AsyncAPI channel specs across supported FastStream releases."""

from dataclasses import fields
from typing import Any, TypeVar

from faststream.specification.schema import Operation, PublisherSpec, SubscriberSpec
from faststream.specification.schema.bindings import ChannelBinding

ChannelSpec = TypeVar("ChannelSpec", SubscriberSpec, PublisherSpec)


def channel_spec(
    spec_type: type[ChannelSpec],
    *,
    address: str,
    description: str | None,
    operation: Operation,
    bindings: ChannelBinding,
) -> ChannelSpec:
    """Pass the channel address when the installed FastStream accepts it."""
    kwargs: dict[str, Any] = {
        "description": description,
        "operation": operation,
        "bindings": bindings,
    }
    if any(field.name == "address" for field in fields(spec_type)):
        kwargs["address"] = address

    return spec_type(**kwargs)
