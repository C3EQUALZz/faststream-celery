# 13. Credentials and TLS

Unlike the other examples, these two do not run against the local
docker-compose — they point at a TLS broker that needs credentials. Read them,
then adapt the URL and the certificate paths.

## The mapping

| FastStream                                          | kombu / Celery                                                     |
|-----------------------------------------------------|--------------------------------------------------------------------|
| `SASLPlaintext(username=..., password=...)`         | `userid` / `password` on the connection, or credentials in the URL |
| `BaseSecurity(use_ssl=True)`                        | `ssl=True`                                                         |
| `CeleryBroker(ssl={...})`                           | Celery's `broker_use_ssl={...}`                                    |
| `SASLScram256/512`, `SASLGSSAPI`, `SASLOAuthBearer` | nothing — raises `NotImplementedError`                             |

Two rules behind that table:

* **`ssl=` wins.** `security=BaseSecurity(use_ssl=True)` turns TLS on;
  `CeleryBroker(ssl={...})` says how. When both are given, the explicit dict is
  what kombu gets.
* **An `ssl_context` is refused.** kombu wants its own dict of options, not a
  Python `SSLContext`, so passing one raises rather than being quietly ignored.
  Translate it into `ssl={"ca_certs": ..., "cert_reqs": ...}`.

The unsupported SASL mechanisms raise instead of falling back to the TLS
settings: falling back would drop the credentials and fail at the broker with a
message about something else entirely.

## Why bother with the security object

`CeleryBroker("amqp://user:secret@host//")` works and is one line shorter. But
the URL ends up in logs, in AsyncAPI output and in tracebacks. A
`SASLPlaintext` built from the environment keeps the password out of all three.
