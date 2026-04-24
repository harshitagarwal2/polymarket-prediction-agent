# Security Policy

## Supported security posture

This repository is maintained as a supervised, fail-closed trading and research workspace.
It is **not** positioned as an unattended live trading system, and security fixes should preserve that boundary rather than broaden it implicitly.

## Reporting a vulnerability

Please do **not** open a public issue for suspected credential leaks, unsafe live-trading behavior, or exploitable vulnerabilities.

Instead, use one of these private paths:

1. Open a private GitHub security advisory for this repository, if you have access.
2. If that is not available, open a regular GitHub issue only for low-risk disclosure-safe concerns and avoid posting credentials, tokens, DSNs, or reproduction data that contains account information.

When reporting, include:

- affected file paths and commands
- reproduction steps using placeholder credentials only
- impact assessment
- whether the issue affects preview-only, supervised live operation, or offline research flows

## Secret handling expectations

- Keep live credentials in environment variables or local secret stores only.
- Do not commit `.env`, DSN marker files, private keys, logs, or runtime-generated data.
- Treat `.env.example` as placeholders only; it must never contain live credentials.

## Operational boundaries

- Docker and Compose paths in this repo validate local benchmark or substrate smoke paths.
- They do **not** prove unattended production readiness by themselves.
- Any deployment target, secret manager, or on-call process remains provider-specific and out of scope unless it is explicitly added and documented in-repo.
