# Contributing

Thanks for taking the time. This document covers the mechanics; the
architecture, package layout and style rules live in
[AGENTS.md](../AGENTS.md) — read that before writing code. The agreed broker
design is [docs/design.md](../docs/design.md); cross-cutting decisions are
recorded in [docs/adr/](../docs/adr/).

## Setup

Requires Python 3.10+ (3.14 is the pinned dev version),
[uv](https://docs.astral.sh/uv/), [just](https://just.systems/) and Docker
(integration tests start RabbitMQ/Redis via testcontainers).

```sh
uv sync
uv run prek install --install-hooks   # lint + conventional commit-msg hooks
```

## The loop

```sh
just linter                          # ruff format + ruff check + codespell
just static-analysis                 # mypy + bandit + semgrep
uv run pytest tests/unit -q          # fast, no Docker
uv run pytest tests/integration -q   # needs Docker (testcontainers)
```

All of these must be clean before a PR. Lint and type-check configuration lives
in `ruff.toml` and `mypy.ini` — line length 90, double quotes, Google-style
docstrings.

## Conventions worth knowing up front

- **No `from __future__ import annotations`.**
- **FastStream idioms first.** The package mirrors the built-in FastStream
  brokers (`faststream/redis/` is the reference); Celery-specific API
  extensions are out of scope (ADR-0002).
- **kombu stays in the consumer thread.** The async side talks to it through
  `asyncio.Queue` and `loop.call_soon_threadsafe` — never touch kombu objects
  from the event loop thread (ADR-0001).
- **No function-level imports, no nested functions on runtime paths, no side
  effects in `__init__`.** See AGENTS.md for the full list.
- **Re-exports.** Public implementations are re-exported from
  `faststream_celery/__init__.py` and listed in `__all__`.
- **Accept ruff's autofixes; do not silence a rule.** If a rule is genuinely
  wrong for a file, add a scoped `per-file-ignores` entry with a comment
  saying why.

## Commits and pull requests

Commit messages and PR titles follow
[Conventional Commits](https://www.conventionalcommits.org/) — enforced by the
`conventional-pre-commit` hook locally and by the PR Title workflow on GitHub.

```
feat: adding ETA scheduler for countdown tasks
fix: mapping kombu reject to nack in CeleryMessage
test: adding parser tests for protocol v1 envelopes
```

Fill in the pull request template, including the "What I did not verify"
section. Reporting that something is untested is useful; implying it works is
not.

## Issues

Bugs and feature requests go through the issue templates. Larger pieces of work
are tracked as markdown tickets under `.scratch/<feature-slug>/` — see
[docs/agents/issue-tracker.md](../docs/agents/issue-tracker.md).
