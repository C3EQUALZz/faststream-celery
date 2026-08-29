# faststream-celery Development Guidelines

Canonical guidelines for AI agents and contributors. `CLAUDE.md` and `GEMINI.md` are pointers to this file.

## Agent skills

### Project skills

Project-scope skills live in `.agents/skills/` (`code-architecture`, `dev-workflow`, `documentation-writing`, `testing-patterns` — mirrored from `ag2ai/faststream`). Follow them when working in the matching area: source conventions, test layout, docs, and workflow commands.

### Issue tracker

Issues and PRDs live as local markdown files under `.scratch/<feature-slug>/` (`SPEC.md`, `PLAN.md`, `tickets/`). See `docs/agents/issue-tracker.md`.

### Triage labels

Default vocabulary (`needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`), recorded as a `Status:` line in each ticket file. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: `CONTEXT.md` + `docs/adr/` at the repo root. The agreed broker design is `docs/design.md`. See `docs/agents/domain.md`.

## AI-assisted contribution policy

Before opening a PR, read and follow `.github/AI_POLICY.md`.

- Do not open PRs with unverified AI-generated code or text.
- Ensure the PR description explains the real problem or use case and accurately reflects the diff.
- Include validation and testing information in the PR body.
- Be prepared to explain and revise the contribution in response to reviewer questions.
- Write the PR description using `.github/pull_request_template.md`. Keep its section headings (`## Why are these changes needed?`, `## Related issue number`, `## Checks`, `## AI assistance`), fill each one in, and only check a checklist box once it is actually true.

## Architecture Decision Records (ADR)

Cross-cutting and hard-to-reverse design decisions are recorded in `docs/adr/`, sequentially numbered (`0001-*.md`, `0002-*.md`, …) with a short Context / Decision / Consequences body. Existing records: `0001` (kombu as the transport layer), `0002` (broker API stays within FastStream idioms).

- **Consult them before changing established public API or architecture.** They explain *why* something is the way it is. If a change contradicts an ADR, supersede it rather than silently reverting the code.
- **Add one when a decision qualifies**: it is hard to reverse, surprising without context (a reader would assume the opposite), and the result of a real trade-off. Scan `docs/adr/` for the highest number and increment. Keep it short — recording *that* a decision was made and *why* is the value.

The current architectural baseline is `docs/design.md`.

## Code Style Guidelines

- Do not use `from __future__ import annotations`.
- With `@contextmanager` / `@asynccontextmanager`, annotate the return type as `Generator[T]` / `AsyncGenerator[T]`, never `Iterator[T]` / `AsyncIterator[T]`. The decorator needs a real generator — it calls `throw()` / `athrow()` on it. Import from `collections.abc` (not `typing`) and omit the default send type: `AsyncGenerator[None]`, not `AsyncGenerator[None, None]`.
- Do not use global variables or top-level side-effect function calls unless the user explicitly allows it.
- For filesystem paths, use `pathlib.Path` internally. Public signatures should accept `str | os.PathLike[str]`.
- User-facing code imports only from public packages (`faststream_celery`, `faststream`). Imports from `faststream._internal` are confined to the adapter layer of this package (see `docs/design.md` §12) — never scattered across modules.
- Do not use function-level imports unless the user explicitly allows it.
  ```python
  # === BAD - import inside function ===
  def execute_tool():
      from .tool import Tool

      ...


  # === GOOD - top-level import ===
  from .tool import Tool


  def execute_tool(): ...
  ```
- Do not create nested functions inside runtime execution paths.
  ```python
  # === BAD - function will be created each call ===
  def execute_tool():
      def _inner_function():
          pass

      _inner_function()


  # === GOOD - function created once, executed each call ===
  def execute_tool():
      _inner_function()


  def _inner_function():
      pass


  # === GOOD - decorator executed at import time, so closure functions are fine here ===
  def decorator(func):
      def wrapper():
          return func()

      return wrapper
  ```
- Do not perform side effects in initialization methods. Apply side effects only at runtime.
  ```python
  # === BAD - create directory in initial method ===
  class KnowledgeStore:
      def __init__(self, path: str | os.PathLike[str]) -> None:
          self.path = Path(path)
          # side effect - directory creation
          self.path.parent.mkdir(parents=True, exist_ok=True)

      def run(self) -> None: ...


  # === GOOD - create directory in runtime method ===
  class KnowledgeStore:
      def __init__(self, path: str | os.PathLike[str]) -> None:
          self.path = Path(path)

      def run(self) -> None:
          self.path.parent.mkdir(parents=True, exist_ok=True)
          ...
  ```
- Line length 90, double quotes, Google-style docstrings. `ruff` and `mypy` must pass before a change is proposed (see `ruff.toml`, `mypy.ini`).

## Package Structure

`src/faststream_celery/` is a FastStream broker package; it mirrors the anatomy of the built-in brokers (`faststream/redis/` and `faststream/kafka/` are the reference). See `docs/design.md` §17.

```
src/faststream_celery/
├── __init__.py        # public exports with explicit __all__
├── annotations.py     # broker-specific Annotated type aliases
├── broker/            # broker.py (BrokerUsecase subclass), router.py, registrator.py, logging.py
├── configs/           # @dataclass(kw_only=True) configs inheriting BrokerConfig
├── message.py         # StreamMessage subclass (CeleryMessage: ack/nack/reject → kombu)
├── parser.py          # message parser (kombu Message → StreamMessage, protocol v1/v2 detection)
├── publisher/         # publisher endpoint + producer.py (kombu.Producer wrapper)
├── subscriber/        # subscriber endpoint (kombu consumer thread + asyncio.Queue bridge,
│                      #  ETA scheduler, canvas; split into usecases/ if it grows)
├── response.py        # PublishCommand subclasses (CeleryPublishCommand)
├── security.py        # auth/security helpers
├── testing.py         # in-memory TestBroker (TestCeleryBroker)
└── exceptions.py      # broker-specific exceptions
```

### Public API (`faststream_celery`)

Top-level exports: `CeleryBroker`, `CeleryRouter`, `TestCeleryBroker`, `CeleryTask`.

### Re-export rules

All public implementations must be re-exported from `faststream_celery/__init__.py` and listed in `__all__`. If an import requires an optional third-party dependency (e.g. `redis` for the Redis transport), wrap it in a `try/except ImportError` block that raises an `ImportError` telling the user which extra to install.

## Design principles

- **FastStream idioms first**: mirror the built-in broker packages (shape, naming, configuration). No Celery-specific API extensions — see ADR-0002.
- **kombu is the transport** (ADR-0001): all kombu objects are confined to the subscriber's consumer thread; the async side communicates via `asyncio.Queue` and `loop.call_soon_threadsafe`. Never touch kombu objects from the event loop thread.
- **Async throughout** on the FastStream side: handlers, publishing, and lifecycle are async; the sync kombu boundary is the only exception and stays isolated.
- **Wire compatibility is verified against live Celery**: behavior changes ship with integration tests running a real Celery client/worker (testcontainers).
