# 4. Validating a task payload with Pydantic

A Celery task body is `(args, kwargs, embed)`. This broker normalizes it to
`{"args": [...], "kwargs": {...}}`, and FastStream fills the handler's
parameters from that dict and validates each one with Pydantic — so a handler
never has to reach into a raw dict.

| Form | Handler | Best for |
|---|---|---|
| 1 | `args: tuple[int, int], kwargs: EmailOptions` | most tasks; closest to the wire |
| 2 | `body: TransferBody` | a rule spanning both halves of the body |
| 3 | `send_email(user_id, urgent)` + `decoder=` | mirroring the Celery task signature you are replacing |

## The one gotcha

The body's keys are mapped onto parameters only when a handler declares **two
or more** body parameters. A handler with a single body parameter is handed the
whole body — which is what form 2 wants, and what form 1 avoids by always
declaring both `args` and `kwargs`, even when the task uses only one of them.

Declaring the unused half is not wasted: `args: tuple[()]` rejects a caller
that sends positional arguments to a keyword-only task.

## Run

```bash
docker compose -f ../docker-compose.yaml up -d rabbitmq

# terminal 1
faststream run faststream_consumer.py:app     # or: python faststream_consumer.py

# terminal 2
python celery_client.py
```

## Expected output

```
form 1: examples.send_email -> {'status': 'sent', 'user_id': 42}
form 2: examples.transfer -> {'from': 1, 'to': 2, 'amount_cents': 4999}
form 3: examples.resize_image -> {'upload_id': 'upload-7', 'size': [800, 600]}
unknown keyword: examples.send_email -> FAILURE: Exception: <class 'pydantic_core._pydantic_core.ValidationError'>([])
negative id: examples.send_email -> FAILURE: Exception: <class 'pydantic_core._pydantic_core.ValidationError'>([])
unexpected args: examples.send_email -> FAILURE: Exception: <class 'pydantic_core._pydantic_core.ValidationError'>([])
same account: examples.transfer -> FAILURE: Exception: <class 'pydantic_core._pydantic_core.ValidationError'>([])
oversized: examples.resize_image -> FAILURE: Exception: <class 'pydantic_core._pydantic_core.ValidationError'>([])
```

Every rejection happens before the handler body runs, and reaches the caller as
a Celery `FAILURE` with the Pydantic error in the traceback. Celery cannot
reconstruct a `ValidationError` on the client side (it takes constructor
arguments Celery has no way to supply), so `get()` raises a plain `Exception`
naming the type — the consumer's log has the full message:

```
ValidationError: 1 validation error for send_email
kwargs.user_id
  Input should be greater than 0 [type=greater_than, input_value=-1, ...]
```

## Sharing the schema

`celery_client.py` builds its payload from the same `EmailOptions` model the
consumer validates with. In a real system that model lives in a package both
services depend on — which is how a renamed field becomes a failed import at
deploy time instead of a `KeyError` in production.
