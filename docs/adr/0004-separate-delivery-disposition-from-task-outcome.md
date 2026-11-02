# Separate delivery disposition from task outcome

Status: proposed for review with task 3.

## Context

FastStream instructions control message delivery. Reporting them as Celery
FAILURE lets a client finish before a requeued task succeeds. Treating an
explicit nack followed by a normal return as SUCCESS has the same problem.
ADR-0002 keeps FastStream acknowledgement semantics rather than Celery retry APIs.

## Decision

`HandlerException` instructions produce no result envelope or canvas
continuation. Explicit `nack()` and `reject()` likewise suppress automatic
task results and canvas continuations for that processing attempt.

An ordinary exception under the active subscriber's `NACK_ON_ERROR` policy
produces no terminal failure while the message remains unsettled and eligible
for requeue. An already acknowledged message still reports an ordinary
success or failure. A normal `ack()` call alone does not suppress results.

## Consequences

A later processing attempt may complete a requeued task's result. Redelivery
does not synthesize Celery RETRY state, increment retries or change ETA.
Ending processing with AckMessage, RejectMessage or SkipMessage may leave a
client waiting until timeout; these instructions have no implicit task value.
Applications needing an outcome must complete processing or report it explicitly.

Ordinary errors under REJECT_ON_ERROR still produce FAILURE and errbacks.
This rule does not change FastStream's delivery settlement or make result
publication, continuation publication and acknowledgement atomic.
