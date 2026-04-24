# Production Readiness Checklist

This repository is designed for **supervised**, fail-closed operation. Completion of the production-readiness wave means the substrate, truth path, execution shell, and operator verification surfaces are in place and reproducibly testable. It does **not** mean unattended live trading.

The authority and sanctioned-entrypoint contract for this wave is defined in [`docs/adr/authority-and-reconciliation.md`](adr/authority-and-reconciliation.md).

Replay, attribution, and advisory freeze-lift criteria are defined in [`docs/REPLAY_FREEZE_LIFT.md`](REPLAY_FREEZE_LIFT.md).

## Minimum release gate

Before calling a branch production-ready, all of the following must be true:

1. `uv.lock` matches the dependency declarations.
2. Full unittest discovery passes under the required extras.
3. `make smoke-service-stack` passes.
4. `make smoke-compose` passes.
5. `docker build -t prediction-market-agent .` passes.
6. `operator-cli status` reflects the expected runtime safety state for the supervised target configuration.
7. If local watchdog automation is enabled, `operator-cli status --output runtime/data/current/runtime_status.json` writes the expected machine-readable payload.
8. Verification artifacts under:
   - `docs/VERIFICATION_SPORTS_POLYMARKET.md`
   - `docs/verification_sports_polymarket.json`
   are refreshed from the exact branch state being claimed as ready.

## Observability expectations

Operators should be able to inspect:

- capture source health via projected `source_health`
- projector lane status (`projection_sportsbook_odds`, `projection_polymarket_market_catalog`, `projection_polymarket_market_channel`, `projection_polymarket_user_channel`)
- fair value artifact freshness through `runtime/data/current/fair_value_manifest.json`
- projected account truth through `runtime/data/current/polymarket_orders.json`, `polymarket_fills.json`, `polymarket_positions.json`, and `polymarket_balance.json`
- runtime preview/state via `operator-cli status`
- pending cancels, pending submissions, recovery items, and refresh requests through the persisted safety state

These checks are the current repo-backed substitute for external dashboards/alerts.

## Operations checklist

### Postgres bootstrap / migration rollout

```bash
uv run bootstrap-postgres --root runtime/data
```

Or via compose:

```bash
docker compose up -d postgres
docker compose run --rm bootstrap-postgres
```

### DSN rotation

The runtime resolves Postgres authority from:

- `PREDICTION_MARKET_POSTGRES_DSN`
- `POSTGRES_DSN`
- `DATABASE_URL`
- `runtime/data/postgres/postgres.dsn`

To rotate safely:

1. update the environment or `postgres.dsn` marker,
2. rerun `bootstrap-postgres`,
3. rerun `make smoke-service-stack`,
4. verify `operator-cli status` and the current verification artifacts.

### Worker restart order

1. `postgres`
2. `bootstrap-postgres`
3. capture workers (`run-sportsbook-capture`, `run-polymarket-capture`)
4. `run-current-projection`
5. deterministic builders if needed
6. supervised `run-agent-loop`

### Supervised runtime check

```bash
operator-cli status --state-file runtime/safety-state.json
```

Confirm:

- expected runtime health state
- no unresolved pending cancels/submissions unless intentionally testing recovery
- kill-switch state consistent with source health

## What is intentionally out of scope

- unattended live trading
- autonomous LLM-driven execution
- unreviewed strategy/model promotion
- queue-accurate full microstructure simulation beyond the currently implemented replay label substrate
