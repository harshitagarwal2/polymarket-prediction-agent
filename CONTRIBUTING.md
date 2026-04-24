# Contributing

Thanks for considering a contribution.

## Before you start

- Read the project posture in [`README.md`](README.md): this repo is a supervised, fail-closed trading and research workspace, not an unattended live-trading system.
- Read [`SECURITY.md`](SECURITY.md) before reporting vulnerabilities or handling credentials.
- Follow [`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md) in issues, pull requests, and review threads.

## Local setup

Base environment:

```bash
uv sync --locked
```

Developer tooling and local hooks:

```bash
uv sync --locked --extra dev
pre-commit install
```

Research extras:

```bash
uv sync --locked --extra research
```

Runtime-related extras:

```bash
uv sync --locked --extra postgres
uv sync --locked --extra polymarket
uv sync --locked --extra kalshi
```

## Development workflow

1. Create a focused branch.
2. Keep changes scoped and explain the user-facing or operator-facing reason for the change.
3. Update docs and tests when behavior, commands, or repo contracts change.
4. Never commit live credentials, `.env` files, DSN markers, generated runtime state, or logs.

## Required local checks

Run the checks that match your change. For most repo changes, use:

```bash
make check
make coverage
make audit
```

If you touch the Compose or Postgres-backed substrate paths, also run:

```bash
make smoke-service-stack
make smoke-compose
```

## Pull request expectations

Please include:

- a concise summary of what changed and why
- linked issues or context when available
- the commands you ran locally
- any operator-facing or docs changes that reviewers should look at

Pull requests that change runtime behavior, safety controls, policy interpretation, or operator workflows should update the relevant docs under `docs/`.

## Documentation expectations

The repository treats docs and automation as contracts. If you change CI, entrypoints, smoke paths, or operator expectations, update the matching documentation and keep the docs sync tests passing.

## Dependency updates

This repo uses `uv.lock` as the dependency source of truth. If you change dependency declarations, refresh the lockfile with:

```bash
uv lock
```

## Security and sensitive changes

- Do not post secrets, tokens, account identifiers, or unsafe live-run details in public issues.
- Route sensitive reports through [`SECURITY.md`](SECURITY.md).
- Keep development defaults clearly separated from live credentials.
