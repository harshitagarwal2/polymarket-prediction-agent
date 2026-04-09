# Prediction Market Agent Architecture

This document describes the current verified shape of the repo, not an aspirational future state.

## System summary

The repo has two main layers:

1. a supervised Polymarket runtime for discovery, preview, live order handling, reconciliation, and operator recovery
2. an offline research and benchmark layer for fair-value generation, calibration, replay, and dataset snapshots

Polymarket is the main supported venue path. Kalshi remains present behind the same adapter interface, but the runtime, docs, and tests are much more complete on the Polymarket side.

## Current runtime flow

```text
external sportsbook rows or model output
                |
                v
   fair-value manifest with identity metadata
                |
                v
   ManifestFairValueProvider or StaticFairValueProvider
                |
                v
  market discovery and ranking in engine.discovery
                |
                v
 deterministic execution policy gate and sizing
                |
                v
        TradingEngine in engine.runner
                |
                v
        venue adapter in adapters/
                |
                v
 safety state, journal, operator controls
```

## Main modules and responsibilities

### `adapters/`

This is the venue boundary.

- `adapters/polymarket.py` is the richest adapter today
- `adapters/kalshi.py` provides a thinner path with normalized types
- `adapters/types.py` holds the shared domain model used across runtime and research code

Polymarket-specific runtime details that matter today:

- authenticated CLOB access
- open-order and fill normalization
- account snapshot recovery
- heartbeat management
- phase-1 live-state overlays for user order and fill freshness
- market-state overlay support
- depth-admission settings applied from runtime policy

### `engine/`

This is the runtime orchestration layer.

Important components:

- `engine/runner.py` - trading engine, reconciliation flow, safety state updates, pending cancel and pending submission recovery
- `engine/discovery.py` - opportunity ranking, pair ranking, deterministic execution gate, polling loop orchestration
- `engine/runtime_policy.py` - schema-validated runtime policy loader
- `engine/order_state.py` and `engine/reconciliation.py` - order lifecycle and truth comparison helpers
- `engine/safety_state.py` and `engine/safety_store.py` - persisted operator and recovery state

The engine is designed to fail closed when truth is incomplete or recovery work is still open.

### `risk/`

This layer owns deterministic trading constraints.

- `risk/limits.py` enforces per-market, global, and optional per-event exposure caps
- `risk/cleanup.py` supports stale-order cleanup and verification flows used by operator tooling

Event-level exposure caps are real runtime behavior. `RiskEngine` tracks market-to-event mappings and rejects orders when `max_contracts_per_event` would be exceeded.

In the default `run-agent-loop` path, event identity is seeded from fair-value manifest metadata such as `event_key`, `sport`, `series`, and `game_id`.

### `research/`

This layer supports offline work.

- `research/fair_values.py` builds fair-value manifests from sportsbook-style rows
- `research/calibration.py` fits and loads histogram calibration artifacts
- `research/paper.py` and `research/replay.py` drive replay and paper execution
- `research/benchmark_runner.py` and `research/benchmark_suite.py` run single-case and suite benchmarks
- `research/datasets.py` stores local dataset snapshots and walk-forward splits

## Runtime policy architecture

`engine/runtime_policy.py` is the runtime configuration contract.

It currently validates and builds these sections:

- `fair_value`
- `strategy`
- `risk_limits`
- `opportunity_ranker`
- `pair_opportunity_ranker`
- `execution_policy_gate`
- `trading_engine`
- `order_lifecycle_policy`
- `venues.polymarket`

Current properties of the policy system:

- `schema_version` must be `1`
- unknown keys are rejected
- wrong types are rejected
- `fair_value.field` can be `raw` or `calibrated`
- Polymarket depth-admission settings are part of policy, not hidden adapter constants

This makes runtime behavior easier to reproduce across preview and supervised live runs.

## Fair-value manifest architecture

Runtime fair values can come from a simple flat JSON map, but the richer manifest format is the intended path.

Each manifest value can include:

- `fair_value`
- optional `calibrated_fair_value`
- `generated_at`
- `source`
- `condition_id`
- `event_key`
- `sport`
- `series`
- `game_id`
- `sports_market_type`

Why that matters:

- runtime can reject stale fair values
- runtime can choose raw or calibrated values
- identity mismatches can fail closed instead of silently matching the wrong contract
- event metadata can seed event-level exposure tracking

## Execution and safety flow

For each candidate market, the runtime roughly does this:

1. load account truth and market state
2. rank or pair-rank opportunities
3. look up fair value from the provider
4. generate intents from the strategy
5. run deterministic execution-policy checks
6. size or shrink the order
7. run shared risk checks
8. place, preview, or reject
9. reconcile against authoritative truth
10. persist safety state and journal output

The deterministic gate can reject trades for reasons such as:

- stale books
- thin liquidity
- wide spreads
- unhealthy reconciliation
- duplicate same-side exposure
- open-order pressure
- capital-at-risk limits
- unresolved partial fills
- cooldown violations

On Polymarket, the adapter also performs order-book depth admission based on policy-configured depth levels, visible-liquidity fraction, and optional expected-slippage caps.

## Operator control plane

`scripts/operator_cli.py` is the operator entrypoint.

Current commands:

- `status`
- `pause`
- `unpause`
- `hold-new-orders`
- `clear-hold-new-orders`
- `force-refresh`
- `resume`
- `cancel-all`
- `cancel-stale`

The CLI reads persisted safety state, can compare it against venue truth, and can drive recovery actions without starting the polling loop.

## Benchmark architecture

The benchmark layer is intentionally offline.

It supports:

- case-driven fair-value builds from normalized sportsbook rows
- best-line and independent bookmaker aggregation
- multiplicative and power de-vig methods
- optional calibration overlays from case-provided calibration samples
- replay scoring and baseline comparisons
- suite-level edge-ledger aggregation
- dataset snapshots and walk-forward splits for research iteration

The benchmark layer does not claim live execution realism. It is useful for relative comparison and regression tracking.

## CI architecture

`.github/workflows/python-ci.yml` is the current repository-level verification path.

It:

- installs the package on Python 3.10
- compiles key runtime and research modules
- runs the unittest suite under `tests/`

## Current maturity boundaries

### Production-safer today

- offline benchmark and fixture flows
- fair-value manifest generation
- schema-validated runtime policy files
- supervised Polymarket preview runs
- operator state inspection and controlled cleanup

### Approximate or incomplete today

- queue-position and latency realism in replay
- unattended live trading
- deep Kalshi runtime support
- portfolio-level correlation modeling
- full engine-native live-state architecture beyond the current Polymarket overlay phase
