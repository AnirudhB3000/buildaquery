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
