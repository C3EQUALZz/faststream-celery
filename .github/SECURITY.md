# Security Policy

## Supported versions

`faststream-celery` is pre-1.0. Only the latest release and the `main` branch
receive security fixes.

## Reporting a vulnerability

Please **do not open a public issue** for a security problem.

Report it privately through GitHub's
[private vulnerability reporting](https://github.com/C3EQUALZz/faststream-celery/security/advisories/new),
or by email to <dan.kovalev2013@gmail.com>.

Include, as far as you can:

- what an attacker can do, and what access they need to do it
- the affected component (broker, publisher, subscriber, parser, testing utils)
- a reproduction — a message payload, a script, or a failing test

You can expect an acknowledgement within 7 days and an assessment within 30.

## Scope

Especially relevant for this library:

- **Message parsing.** `parser.py` decodes kombu/Celery protocol v1/v2
  payloads. Anything that turns a crafted message into code execution (unsafe
  deserialization), an unbounded allocation, or a consumer crash is in scope.
- **Credential exposure.** Broker URLs carry credentials; a code path that
  logs or leaks one — including through exceptions and tracebacks — is in
  scope.
- **The kombu thread boundary.** The sync kombu consumer runs in its own
  thread; a flaw that lets a remote message corrupt asyncio state or bypass
  ack/nack/reject semantics is in scope.

Out of scope: findings that require an already-compromised broker or host,
denial of service by volume alone, and reports produced by a scanner without a
demonstrated impact.
