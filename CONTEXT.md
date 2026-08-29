# FastStream ↔ Celery Bridge

A FastStream broker that is wire-compatible with Celery: it consumes tasks sent by Celery clients and publishes tasks that real Celery workers execute. It exists to enable gradual migration from Celery to FastStream without stopping the bus.

## Language

### Roles

**Client side (we are the client)**:
Our FastStream application publishes tasks that real Celery workers execute. The analogue of `celery.send_task`.
_Avoid_: producer side, sender

**Worker side (we are the worker)**:
Our FastStream application consumes tasks sent by Celery clients and executes them with handlers. The analogue of `celery worker`.
_Avoid_: consumer side, receiver

**Migration**:
The gradual-transition mode in which a FastStream application and Celery workers listen to the same queues simultaneously. The primary scenario this project exists for.
_Avoid_: rewrite, replacement

### Protocol

**Task**:
A named unit of Celery work. Routed by the name in `headers["task"]`, not by queue: one queue mixes tasks with different names.
_Avoid_: message, job

**CeleryTask**:
The message wrapper type used to publish a task via the stock `broker.publish(...)`: carries the task name, args/kwargs and Celery-specific parameters (countdown/eta, link/link_error).
_Avoid_: TaskMessage, CeleryMessage (that's the incoming message type)

**Envelope**:
The Celery message envelope on top of kombu: `headers` (task metadata) + `body` `(args, kwargs, embed)` + `properties` (`correlation_id`, content-type).
_Avoid_: frame, payload

**Protocol v2 / v1**:
Envelope format versions. v2 keeps metadata in headers and is detected by the presence of the `task` header; v1 keeps everything flat in the body. We publish v2 only; we read both.
_Avoid_: new/old format

**Foreign task**:
A queue message that did not pass the subscriber's task-name filter. Handled by the stock FastStream filter rules.
_Avoid_: unknown message

### Delayed execution

**ETA**:
The absolute execution time of a task (ISO8601 in headers). A message with a future ETA is published to the queue immediately; holding it until due is the executor's job.
_Avoid_: delay, scheduled time

**countdown**:
A relative delay in seconds given at publish time; converted into an ETA on the client.
_Avoid_: delay, timeout

### Results

**AMQP RPC**:
Getting a result via `reply_to` + `correlation_id` (a temporary reply queue). Maps onto FastStream's stock `broker.request()`.
_Avoid_: RPC backend, reply queue

**celery-task-meta**:
The result format of the Redis result backend: a `celery-task-meta-<task_id>` key holding status/result/traceback. Lives outside the kombu transport.
_Avoid_: result key, redis result

### Canvas

**Canvas**:
Celery's workflow primitives, carried in the v2 body `embed`: chain, callbacks/errbacks, group, chord.
_Avoid_: workflow, pipeline

**Chain**:
A linear task chain: after the current task succeeds, the next signature is published with the result as its first argument (unless the signature is immutable).
_Avoid_: pipeline, sequence

**Chord**:
Fan-in: a callback that runs after every task of a group completes, receiving their results. Requires coordination through the result backend (an atomic group counter).
_Avoid_: barrier, join
