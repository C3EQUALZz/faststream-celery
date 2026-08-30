from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, NamedTuple


@dataclass
class CeleryTask:
    """Wrapper to publish a Celery task via ``broker.publish(...)``.

    Example:
        ```python
        await broker.publish(
            CeleryTask("proj.tasks.send_email", args=[user_id], kwargs={"urgent": True}),
            queue="celery",
        )
        ```
    """

    task: str
    args: Sequence[Any] = ()
    kwargs: Mapping[str, Any] = field(default_factory=dict)


class TaskEnvelope(NamedTuple):
    """A Celery protocol v2 envelope: kombu body plus headers."""

    body: tuple[Any, ...]
    headers: dict[str, Any]


def build_task_envelope(task: CeleryTask, *, task_id: str) -> TaskEnvelope:
    """Assemble a Celery protocol v2 envelope.

    Mirrors ``celery.app.amqp.as_task_v2``: metadata lives in the headers,
    the body is the ``(args, kwargs, embed)`` triple.
    """
    args = list(task.args)
    kwargs = dict(task.kwargs)

    headers: dict[str, Any] = {
        "lang": "py",
        "task": task.task,
        "id": task_id,
        "shadow": None,
        "eta": None,
        "expires": None,
        "group": None,
        "group_index": None,
        "retries": 0,
        "timelimit": [None, None],
        "root_id": None,
        "parent_id": None,
        "argsrepr": repr(args),
        "kwargsrepr": repr(kwargs),
        "origin": None,
        "ignore_result": False,
        "replaced_task_nesting": 0,
        "stamped_headers": None,
        "stamps": {},
    }

    body = (
        args,
        kwargs,
        {
            "callbacks": None,
            "errbacks": None,
            "chain": None,
            "chord": None,
        },
    )

    return TaskEnvelope(body=body, headers=headers)
