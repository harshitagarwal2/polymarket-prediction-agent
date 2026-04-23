# Operator Runbook

This runbook explains how to operate the repo in its current intended posture: supervised automation, mainly on Polymarket.

## What this system is today

Treat this repo as a supervised trading agent lab.

It can:

- persist safety state across restarts
- reload authoritative account truth and fail closed when truth is incomplete
- rank opportunities and pair opportunities
- apply deterministic sizing and execution-policy vetoes
- run preview or supervised live cycles
- pause, hold new orders, force refresh, resume, cancel all, and cancel stale orders
- write JSONL event journals and summarize them

It should still be run with an operator watching it. The docs do not claim unattended live trading is solved.

## Venue posture

- **Primary supported live path:** Polymarket
- **Secondary thin path:** Kalshi adapter support only, with less operational maturity

If you want the lowest-risk way to use the repo, stay in Polymarket preview mode first and use the benchmark toolkit for offline work.

## Quick reference

### Continue supervising normally

Continue only when:

- account truth is complete
- the engine is not halted
- there is no unresolved recovery item that should block new risk
- recent policy rejections are routine quality filters, not a sign of broken state

### Pause

Pause stops new decisions but still allows housekeeping.

```bash
operator-cli pause --state-file runtime/safety-state.json --reason "manual maintenance"
```

### Hold new orders

Use this when you want status and recovery work to continue, but you do not want any new order submissions.

```bash
operator-cli hold-new-orders --state-file runtime/safety-state.json --reason "hold while checking venue state"
```

Clear it with:

```bash
operator-cli clear-hold-new-orders --state-file runtime/safety-state.json
```

### Halt

Halt is the safety posture when trust is broken.

Treat the system as halted when:

- reconciliation drift is unresolved
- account truth is incomplete
- pending cancel or pending submission recovery is unclear
- restart state still does not match venue state

### Force refresh

Queue an authoritative refresh request if you want the next runtime cycle or a later investigation to refresh truth deliberately.

```bash
operator-cli force-refresh --state-file runtime/safety-state.json --reason "operator requested refresh"
```

You can scope it to a contract when needed:

```bash
operator-cli force-refresh --state-file runtime/safety-state.json --venue polymarket --symbol <token-id> --outcome yes --reason "refresh this contract"
```

### Resume

Resume is contract-scoped and supervised.

```bash
operator-cli resume --venue polymarket --symbol <token-id> --outcome yes --state-file runtime/safety-state.json
```

Resume only when venue truth looks healthy again and the clean resume check has enough fresh evidence.

## Recommended operating stages

### Stage 1, preview only

```bash
run-agent-loop \
  --venue polymarket \
  --mode preview \
  --fair-values-file runtime/data/current/fair_value_manifest.json \
  --max-fair-value-age-seconds 900 \
  --max-cycles 1
```

Use this until the following are boring and explainable:

- discovery and ranking
- policy-gate vetoes
- sizing decisions
- journaling
- operator status output

### Stage 2, repeated supervised preview

```bash
run-agent-loop \
  --venue polymarket \
  --mode preview \
  --fair-values-file runtime/data/current/fair_value_manifest.json \
  --max-fair-value-age-seconds 900 \
  --interval-seconds 15 \
  --max-cycles 100
```

### Stage 3, small supervised live run

Only after stage 2 is stable and well understood:

```bash
run-agent-loop \
  --venue polymarket \
  --mode run \
  --fair-values-file runtime/data/current/fair_value_manifest.json \
  --max-fair-value-age-seconds 900 \
  --interval-seconds 15 \
  --max-cycles 10
```

Use small quantities and tight risk caps.

## Runtime policy in operations

If you want repeatable runtime behavior, pass `--policy-file`.

```bash
run-agent-loop \
  --venue polymarket \
  --mode preview \
  --fair-values-file runtime/data/current/fair_value_manifest.json \
  --policy-file runtime/policy.json
```

The policy file can control:

- fair-value field selection, `raw` or `calibrated`
- strategy quantity and edge threshold
- ranker and pair-ranker thresholds
- shared risk caps, including event-level caps
- deterministic execution gate settings
- engine timing and overlay recovery knobs
- lifecycle cleanup policy
- Polymarket depth admission

## Preflight checklist

Before any continuous run:

- `uv sync --locked`
- `uv sync --locked --extra polymarket` for Polymarket operation
- `uv sync --locked --extra kalshi` for Kalshi operation
- `uv sync --locked --extra postgres` for the dedicated sportsbook capture worker or any other Postgres-backed storage flow
- `runtime/data/current/fair_value_manifest.json` exists
- if you just ran `build-mappings`, `runtime/data/current/market_mappings.json` should exist for runtime selection and `runtime/data/current/market_mapping_manifest.json` should exist for structured mapping review/debugging
- if you are using the research dataset builders, `runtime/data/processed/inference/joined_inference_dataset.jsonl` and/or `runtime/data/processed/training/historical_training_dataset.jsonl` should exist, with matching versioned snapshots under `runtime/data/datasets`
- chosen state file path exists or can be created
- chosen journal path exists or can be created
- required venue credentials are present
- if using `--policy-file`, the file exists and matches schema version 1

If you are using `run-sportsbook-capture`, make sure Postgres is bootstrapped first:

- export `PREDICTION_MARKET_POSTGRES_DSN` / `POSTGRES_DSN` / `DATABASE_URL`, or write a `postgres.dsn` marker under `runtime/data/postgres`
- if needed, run `bootstrap-postgres --root runtime/data` once to apply the shipped storage migrations before the continuous worker starts

For Polymarket run mode:

- `POLYMARKET_PRIVATE_KEY` must be present
- optional `POLYMARKET_FUNDER` and `POLYMARKET_ACCOUNT_ADDRESS` can be set when needed
- live user-stream condition IDs are derived from the configured fair-value manifest when available
- optional `POLYMARKET_LIVE_USER_MARKETS` can override those derived condition IDs when you need to pin the user stream manually
- optional `POLYMARKET_USER_WS_HOST` can override the default user websocket endpoint

`run-agent-loop` fails fast when the fair-values file, policy file, or required credentials are missing.

## Fair-value manifest guidance

The runtime accepts a legacy flat map, but the richer manifest is the safer path.

```json
{
  "generated_at": "2026-04-07T12:00:00Z",
  "source": "sports-model-v1",
  "max_age_seconds": 900,
  "values": {
    "<market_key>": {
      "fair_value": 0.61,
      "calibrated_fair_value": 0.63,
      "condition_id": "<condition-id>",
      "event_key": "<event-key>"
    }
  }
}
```

Operationally important points:

- stale manifests can be blocked with `--max-fair-value-age-seconds`
- runtime policy can choose `raw` or `calibrated`
- `condition_id` and `event_key` help fail closed on identity mismatch
- event metadata can seed per-event exposure tracking

If another supervised process is refreshing the manifest, `run-agent-loop` can reload it without restart through `--fair-values-reload-seconds`.

## Core operator commands

### Status

```bash
operator-cli status --state-file runtime/safety-state.json
operator-cli status --state-file runtime/safety-state.json --journal runtime/events.jsonl
operator-cli status --state-file runtime/safety-state.json --venue polymarket --symbol <token-id> --outcome yes
```

Status can show:

- persisted safety state
- pending cancels, pending submissions, pending refresh requests, and recovery items
- recent journal summaries, including runtime-state counts, gate-stage totals, and lifecycle action counts
- last persisted truth summary
- latest runtime summary for the most recent cycle, skip, truth block, or lifecycle batch
- venue snapshot and truth drift when `--venue` is supplied
- Polymarket live-state and market-state overlay health

### Advisory sidecar

If you have offline contract-review rows, build the advisory artifact first:

```bash
operator-cli build-llm-advisory \
  --llm-input runtime/llm_contract_rows.json \
  --opportunity-root runtime/data \
  --output runtime/data/current/llm_advisory.json
```

Inspect it in structured or human-readable form:

```bash
operator-cli show-llm-advisory \
  --llm-advisory-file runtime/data/current/llm_advisory.json

operator-cli show-llm-advisory \
  --llm-advisory-file runtime/data/current/llm_advisory.json \
  --format markdown

operator-cli status --state-file runtime/safety-state.json --llm-advisory-file runtime/data/current/llm_advisory.json
```

`runtime/data/current/llm_advisory.json` is a sidecar artifact for operator review and dashboards. It does not drive `run-agent-loop`, risk checks, or execution-policy decisions.

If the runtime is using a non-default policy file, pass the same `--policy-file` to `build-llm-advisory` so the preview proposal/blocking context matches the live thresholds and freeze rules.

### Pause and unpause

```bash
operator-cli pause --state-file runtime/safety-state.json --reason "manual maintenance"
operator-cli unpause --state-file runtime/safety-state.json
```

### Hold and clear hold

```bash
operator-cli hold-new-orders --state-file runtime/safety-state.json --reason "hold for review"
operator-cli clear-hold-new-orders --state-file runtime/safety-state.json
```

### Force refresh

```bash
operator-cli force-refresh --state-file runtime/safety-state.json --reason "operator requested refresh"
```

### Resume after halt

```bash
operator-cli resume --venue polymarket --symbol <token-id> --outcome yes --state-file runtime/safety-state.json
```

### Emergency cleanup

```bash
operator-cli cancel-all --venue polymarket --symbol <token-id> --outcome yes
operator-cli cancel-stale --venue polymarket --symbol <token-id> --outcome yes --max-order-age-seconds 30
```

## What blocks new trading decisions

The runtime can refuse new decisions when:

- the engine is paused
- the engine is halted
- new orders are held by the operator
- authoritative account truth is incomplete
- pending recovery work is still open
- the deterministic execution gate rejects the candidate

The execution gate can reject for reasons such as:

- stale book
- thin liquidity
- wide spreads
- unhealthy reconciliation
- duplicate exposure
- open-order count pressure
- open-order notional pressure
- contract capital-at-risk limits
- unresolved partial fills
- cooldown violations

Separate from that, risk limits can still reject for:

- per-market exposure caps
- global exposure caps
- per-event exposure caps when event identity is known
- max daily loss

## What can still run while paused or held

Pause and hold-new-orders do not shut off all runtime work.

The system may still:

- refresh account truth
- observe overlay state
- cancel stale orders
- write journal events

That is intentional. The design tries to stop new risk without blinding the operator.

## Polymarket-specific notes

Polymarket is the main supported venue path, and it has extra runtime behaviors.

- heartbeat support can be required for live trading health
- a phase-1 live user-state overlay tracks open-order and fill freshness
- a market-state overlay can improve book freshness
- runtime policy can apply depth-admission rules before order placement

Important caveat:

These overlays improve freshness, but they do not replace authoritative REST-based reconciliation for balances, positions, and final account truth.

## Suggested 24/7 posture

The safest current posture is:

1. preview mode first
2. persisted safety state enabled
3. journal enabled
4. operator CLI available in another terminal or session
5. frequent status and journal review
6. only short supervised live windows

## Missing before unattended trading

- deeper engine-native live-state integration
- more trustworthy restart rebuild from venue plus journal history
- stronger partial-fill and cancel-race handling
- richer portfolio-level exposure modeling
- more trustworthy fair-value generation and monitoring

Until then, treat the repo as supervised automation, not unattended production trading.
