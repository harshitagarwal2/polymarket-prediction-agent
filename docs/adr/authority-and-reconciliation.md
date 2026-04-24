# Authority and Reconciliation ADR

## Status

Accepted for the current productionization wave.

## Decision

For supervised, staging, and live operation, this repository treats **Postgres-backed raw capture plus projected current-state tables** as the authoritative data plane when a Postgres DSN marker is resolvable.

`runtime/data/current/*.json` remains useful, but only as compatibility export material. It is not the primary source of truth for supervised operation.

## Ownership table

| Domain | Authoritative writer | Read authority in serious modes | Notes |
|---|---|---|---|
| Raw capture events | dedicated capture workers | Postgres | append-only ingress |
| Capture checkpoints | dedicated capture workers / projector | Postgres | restart and replay boundary |
| Source health | capture workers + deterministic builders | Postgres | compatibility JSON may mirror this |
| Capture-owned current state | projector | Postgres projected tables | compatibility JSON is derived only |
| Market mappings | deterministic builders | projected/Postgres-backed reads | compatibility JSON is export only |
| Fair values | deterministic builders | projected/Postgres-backed reads | manifests are compatibility/export products |
| Opportunities | deterministic builders | projected/Postgres-backed reads | runtime preview may consume exports, not authority |
| Runtime decisions | `run-agent-loop` | adapter truth + authoritative projected inputs | must fail closed on stale truth |
| Venue/account reconciliation | `run-agent-loop` + `operator-cli` | venue truth + persisted safety state | operator-supervised only |

## Polymarket user-channel decision

For this productionization wave, the Polymarket user channel is owned by the dedicated `run-polymarket-capture user` worker as raw/user-truth ingress only.

- It remains part of the authoritative capture substrate.
- It is **not** elevated to a selector-facing projected compatibility export in this wave.
- Runtime and operator reconciliation may consume its downstream effects through authoritative truth and persisted safety state, but the user-channel itself does not become a new compatibility-current table during PR-00/01.

## Sanctioned production entrypoints

These are the supported supervised productionization-wave entrypoints:

- `bootstrap-postgres`
- `run-sportsbook-capture`
- `run-polymarket-capture`
- `run-current-projection`
- `python -m scripts.ingest_live_data build-mappings`
- `python -m scripts.ingest_live_data build-fair-values`
- `python -m scripts.ingest_live_data build-opportunities`
- `run-agent-loop`
- `operator-cli`

## Manual or compatibility utilities

These are still useful, but they are **not** the sanctioned continuous production entrypoints:

- `python -m scripts.ingest_live_data build-inference-dataset`
- `python -m scripts.ingest_live_data build-training-dataset`
- legacy offline `ingest-live-data --layer ... --output ...`

They may remain available for one-shot, offline, or compatibility work, but they must not be treated as the steady-state supervised ingestion boundary.

## Deprecated entrypoints

- `ingest-live-data polymarket-markets` — retired; use `run-polymarket-capture market` and `run-current-projection` for the capture-owned market catalog path.
- `ingest-live-data sportsbook-odds` — retired; use `run-sportsbook-capture` and `run-current-projection` for the sanctioned sportsbook capture path.
- `ingest-live-data polymarket-bbo` — retired; use `run-polymarket-capture market` for live Polymarket capture.

## Reconciliation invariants

1. A supervised runtime cycle must not mix file-backed compatibility data with authoritative Postgres/projected reads.
2. Capture workers own raw ingress, checkpoints, and source-health writes only.
3. The projector owns compatibility exports for capture-owned tables.
4. Deterministic builders own mappings, fair values, opportunities, and dataset artifacts.
5. If authoritative projected inputs are stale or unavailable, supervised runtime must fail closed.

## Rollback rule

If a cutover step fails, roll back to an earlier supervised/read-only posture. Do **not** reintroduce split-brain authority by making compatibility JSON authoritative again.
