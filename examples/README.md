# faststream-celery examples

Each directory is one scenario, with **both sides present** — a FastStream app
and the stock Celery process it talks to — so you can start them and watch the
tasks cross. Every payload is validated with a Pydantic model on both ends.

## Setup

```bash
pip install "faststream-celery[fastapi,redis,otel,prometheus]"
pip install "faststream[cli]" celery       # `faststream run`, and the Celery side
docker compose -f examples/docker-compose.yaml up -d
```

The FastStream apps run either way:

```bash
faststream run faststream_consumer.py:app     # with the CLI, reload and workers
python faststream_consumer.py                 # without it
```

Run every command from inside the example's own directory — the Celery worker
is started as `-A celery_worker`, which resolves relative to the working
directory.

## The scenarios

|    | Directory                                              | Who publishes                      | Who consumes                                         |
|----|--------------------------------------------------------|------------------------------------|------------------------------------------------------|
| 1  | [`01_celery_to_faststream/`](01_celery_to_faststream/) | stock Celery client                | FastStream handlers                                  |
| 2  | [`02_faststream_to_celery/`](02_faststream_to_celery/) | FastStream                         | stock `celery worker`                                |
| 3  | [`03_both_ways/`](03_both_ways/)                       | both, in one loop                  | both                                                 |
| 4  | [`04_payload_validation/`](04_payload_validation/)     | stock Celery client                | three ways to validate with Pydantic                 |
| 5  | [`05_fastapi/`](05_fastapi/)                           | HTTP endpoints and a Celery client | FastAPI app + `celery worker`                        |
| 6  | [`06_canvas/`](06_canvas/)                             | Celery `chain`, and ours           | a FastStream handler mid-chain                       |
| 7  | [`07_results/`](07_results/)                           | both                               | Redis transport, result backend, tests without Redis |
| 8  | [`08_scheduling/`](08_scheduling/)                     | both                               | `countdown`, `eta`, `expires`                        |
| 9  | [`09_router/`](09_router/)                             | stock Celery client                | routers with queue prefixes                          |
| 10 | [`10_observability/`](10_observability/)               | stock Celery client                | OTel spans, Prometheus, AsyncAPI                     |
| 11 | [`11_middlewares_and_di/`](11_middlewares_and_di/)     | stock Celery client                | middlewares, `Depends`, annotations                  |
| 12 | [`12_testing/`](12_testing/)                           | the test itself                    | `TestCeleryBroker`, no services                      |
| 13 | [`13_security/`](13_security/)                         | —                                  | credentials and TLS (read-only)                      |

Start with 1 and 2 — one direction each. 3 is both at once. 4 is the one to
read before writing handlers of your own.

## Handler payloads at a glance

A Celery task body is `(args, kwargs, embed)`. It is normalized to
`{"args": [...], "kwargs": {...}}`, and FastStream fills the handler's
parameters from that dict, validating each with Pydantic:

```python
class EmailOptions(BaseModel):
    user_id: PositiveInt
    urgent: bool = False


@broker.subscriber("celery", task="examples.send_email")
async def send_email(args: tuple[()], kwargs: EmailOptions) -> str:
    return f"sent to {kwargs.user_id}"
```

Both parameters are declared even when the task uses only one: a handler with a
**single** body parameter is handed the whole body instead of one key — which
is what a whole-body model wants, and what this form must avoid.
[`04_payload_validation/`](04_payload_validation/) covers all three shapes,
including a `decoder` that lets a handler keep the signature the Celery task
had.

## What is not here

A chord's body is carried but not dispatched — see
[`06_canvas/`](06_canvas/#not-yet). Publishing protocol v1 is not supported
(reading it is). Batch publishing is not a Celery concept and raises.
