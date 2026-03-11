# Contributing

## Project Goal

Build-a-Query is intended to become a Python counterpart to `knex.js`: a query builder with broad SQL dialect support and a consistent developer experience.

## Supported Dialects

This project currently focuses on open-source or otherwise free database backends that can run locally, either in the filesystem or in containers. Hosted or paid-only database offerings are outside the current contribution scope.

## CI Expectations

For a pull request to be considered ready to merge:

- `poetry run all-tests` should pass in your local environment.
- GitHub Actions CI must pass for the branch.

At the moment, GitHub Actions runs unit checks only. Integration coverage is still expected locally, so contributors should treat local `poetry run all-tests` as the required validation path before opening or updating a PR.

## Questions And Issues

If you have a question, leave a comment on [Issue #1](https://github.com/AnirudhB3000/buildaquery/issues/1).

If you find a bug or want to work on an improvement, open a new issue in the repository before or alongside your change.

## AI Agents

This repository includes `AGENTS.md` as a working instruction file for AI coding agents. Contributors using agent-assisted workflows should treat it as the canonical source for repository-specific expectations, including architecture constraints, testing workflow, security requirements, and edit approval rules.

The same guidance can usually be reused or adapted for other agent systems:

- Claude-based agents can consume the file directly or through a pasted project instruction block.
- Gemini-based agents can use the same content as repository guidance, workspace instructions, or prompt context.
- IDE-based agents can reference the file as local project instructions when the editor supports repository-scoped agent configuration.

If your agent does not support `AGENTS.md` natively, replicate the important sections manually in that tool's project instructions. Keep the replicated guidance aligned with this repository's current `AGENTS.md` so contributors and agents follow the same rules.
