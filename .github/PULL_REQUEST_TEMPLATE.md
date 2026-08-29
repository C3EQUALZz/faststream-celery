## Why are these changes needed?

<!-- What does this PR do, and which problem does it solve? Explain the design
     choice if it is not obvious from the diff. -->

## Related issue number

<!-- For example: "Closes #1234". Larger pieces of work are tracked as tickets
     under .scratch/<feature-slug>/ — link the ticket if there is one. -->

## Checks

- [ ] `just linter` is clean (ruff format, ruff check, codespell)
- [ ] `just static-analysis` is clean (mypy, bandit, semgrep)
- [ ] `uv run pytest tests/unit -q` is green — state how many tests ran
- [ ] New behaviour is tested; changes that affect the wire format ship with
      integration tests against a live Celery client/worker (testcontainers)
- [ ] Public API changes are re-exported from `faststream_celery/__init__.py`
      and comply with the ADRs in `docs/adr/`
- [ ] PR title follows Conventional Commits

## What I did not verify

<!-- No Docker, no live broker, no Celery worker — say which part is
     unverified rather than implying it works. -->

## AI assistance

<!-- AI-assisted contributions are welcome; you remain responsible for
     everything you submit. See .github/AI_POLICY.md for details. -->

- [ ] I understand the changes in this PR and can explain them in my own words.
- [ ] I have verified that the PR description accurately reflects the actual diff.
- [ ] If AI assistance was used, I reviewed, tested, and validated the generated code/text before submitting.
