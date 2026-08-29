# Issue tracker: Local Markdown

Issues and PRDs for this repo live as markdown files in `.scratch/`.

## Conventions

- One feature per directory: `.scratch/<feature-slug>/`
- The feature specification is `.scratch/<feature-slug>/SPEC.md`
- The execution plan (ticket list + dependency graph) is `.scratch/<feature-slug>/PLAN.md`
- Implementation tickets are `.scratch/<feature-slug>/tickets/ticket-<N>.md`, numbered from `1`
- Triage state is recorded as a `Status:` line near the top of each issue file (see `triage-labels.md` for the role strings)
- Comments and conversation history append to the bottom of the file under a `## Comments` heading

## When a skill says "publish to the issue tracker"

Create a new file under `.scratch/<feature-slug>/` (creating the directory if needed).

## When a skill says "fetch the relevant ticket"

Read the file at the referenced path. The user will normally pass the path or the issue number directly.
