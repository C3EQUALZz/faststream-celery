# AI Policy

AI assistants are used on this repository, and that is fine. The rules exist so
that a reviewer can trust a diff without having to guess how it was produced.

## For contributors

- **You own the diff.** Whether you typed it or generated it, you are the author
  and you answer for it in review.
- **Run it.** `just linter`, `just static-analysis` and the test suite are the
  minimum bar. A change that touches the wire format also needs the integration
  tests against a live Celery client/worker (testcontainers) — code that types
  clean can still break message interop, and the kombu consumer thread fails in
  ways the event loop never sees in a unit test.
- **Report what actually happened.** If tests fail, say so and show the output.
  If a part is unverified — no Docker, no live broker — say which part. The
  pull request template has a section for exactly this.
- **Do not paste secrets into a model.** Broker URLs carry credentials; `.env`
  files, connection strings and API keys stay out of prompts.
- Disclosure of AI assistance is welcome but not required. Unrunnable or
  unreviewed generated code is not, regardless of disclosure.

## For agents

Agent configuration is checked in and is the source of truth:

- [`AGENTS.md`](../AGENTS.md) — architecture, package layout, style rules,
  mandatory commands. Shared by every agent.
- [`CLAUDE.md`](../CLAUDE.md) — pointer to `AGENTS.md`.
- `.agents/skills/` — project skills: source conventions, test layout, docs,
  workflow commands.
- `.claude/` — permissions, skills and subagents.
- [`docs/design.md`](../docs/design.md) and [`docs/adr/`](../docs/adr/) — the
  agreed broker design and the decisions behind it. A change that contradicts
  an ADR must supersede the ADR, not silently revert the code.

An agent that changes behaviour should update the relevant document in the same
pull request.
