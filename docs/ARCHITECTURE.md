# Prediction Market Agent Architecture

This document describes the current verified shape of the repo, not an aspirational future state.

## System summary

The repo now has first-class product-facing layers on top of the original runtime/research split:

1. `contracts/` for deterministic contract identity, mapping confidence, and resolution-rule parsing
2. `forecasting/` for fair-value, calibration, consensus, dashboard, pipeline, and model-registry surfaces
3. `opportunity/` for executable edge, fillability, and ranking
4. `storage/` for raw capture envelopes, normalized row builders, parquet writers, and runtime journaling
5. a supervised Polymarket runtime for discovery, preview, live order handling, reconciliation, and operator recovery
6. an offline research and benchmark layer for replay, benchmarking, and dataset snapshots

Polymarket is the main supported venue path. Kalshi remains present behind the same adapter interface, but the runtime, docs, and tests are much more complete on the Polymarket side.

## Authority and ownership boundaries

This repo currently treats Postgres plus projected current-state tables as the authoritative live data plane when a Postgres DSN marker is resolvable.

- capture workers own raw ingress, checkpoints, and source-health writes only
- `services/projection/` plus `scripts/run_current_projection.py` own the compatibility `runtime/data/current/*.json` exports for capture-owned tables
- deterministic builders in `scripts/ingest_live_data.py`, `storage/current_selection.py`, `execution/planner.py`, and `opportunity/` own mappings, fair values, opportunities, and other derived outputs

That means selector-facing current JSON is not capture-worker authority. When a DSN marker exists, runtime and ingest readers treat the projected Postgres-backed read boundary as authoritative and treat `runtime/data/current/*.json` as compatibility exports.

The legacy live `polymarket-bbo` path is deprecated and should not be treated as the supported production capture path. The supported live capture path is the dedicated `run_polymarket_capture` worker.

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
  market discovery orchestration in engine.discovery
                |
                v
 executable edge and ranking in opportunity/
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

- `adapters/polymarket/` is the richest adapter today, with `__init__.py` as the stable facade and extracted modules such as `gamma_client.py`, `clob_client.py`, `ws_market.py`, `ws_user.py`, `ws_sports.py`, and `normalize.py`
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

The dedicated Phase-1 worker path now surfaces through `scripts/run_polymarket_capture.py`, which uses the Polymarket websocket/catalog adapters plus the Postgres-backed capture stores to append raw/normalized market or user events without going through the monolithic ingest CLI.

`adapters/polymarket/ws_sports.py` is currently a real websocket transport boundary helper, but not yet a full live ingestion orchestrator on its own.

### `engine/`

This is the runtime orchestration layer.

Important components:

- `engine/runner.py` - trading engine, reconciliation flow, safety state updates, pending cancel and pending submission recovery
- `engine/discovery.py` - polling loop orchestration, scan-cycle logging, pair-preview/live orchestration, and compatibility exports while ranking migrates into `opportunity/`
- `engine/runtime_policy.py` - schema-validated runtime policy loader
- `engine/order_state.py` and `engine/reconciliation.py` - order lifecycle and truth comparison helpers
- `engine/safety_state.py` and `engine/safety_store.py` - persisted operator and recovery state

The engine is designed to fail closed when truth is incomplete or recovery work is still open.

### `contracts/`

This layer owns contract meaning.

- `contracts/ontology.py` builds normalized contract identities and grouping keys
- `contracts/confidence.py` scores deterministic contract-match confidence
- `contracts/resolution_rules.py` parses resolution metadata and freeze conditions
- `contracts/mapper.py` assembles normalized contract objects for downstream forecasting and risk work

### `forecasting/`

This layer owns fair value and forecast evaluation.

- `forecasting/fair_value_engine.py` holds fair-value providers plus deterministic consensus fair-value combination
- `forecasting/calibrator.py` and `forecasting/calibration.py` hold calibration loaders and adapters
- `forecasting/model_registry.py` provides registry surfaces for reusable model loaders
- `forecasting/dashboards.py`, `forecasting/contracts.py`, and `forecasting/pipeline.py` cover model-vs-market review, optional LLM evidence, and non-sports pipeline scaffolding

`research/calibration.py` and forecast-scoring pieces of `research/scoring.py` remain compatibility facades for the older sports benchmark path.

### `llm/`

This layer owns operator-facing advisory helpers and sidecar artifacts.

- `llm/advisory_artifact.py` validates and writes the structured advisory sidecar at `runtime/data/current/llm_advisory.json`
- `llm/evidence_summarizer.py` and `llm/operator_memo.py` keep summary/memo rendering deterministic
- `contracts/llm_parser.py` still owns the nested rule/ambiguity parser for contract-specific LLM outputs

That `runtime/data/current/llm_advisory.json` artifact is for operator review and dashboards only. It does not participate in order placement, sizing, risk limits, or execution-policy gating.

### `opportunity/`

This layer sits between fair value and execution.

- `opportunity/executable_edge.py` computes net edge from executable quotes, fees, and slippage assumptions
- `opportunity/fillability.py` estimates visible fillability from market snapshots and books
- `opportunity/ranker.py` owns single-market and paired ranking

### `storage/`

This layer makes runtime and capture storage explicit.

- `storage/raw/` writes immutable capture envelopes
- `storage/postgres/` builds normalized market/order-book row payloads for relational persistence
- `storage/parquet/` owns local parquet writers and partition helpers
- `storage/journal.py` holds runtime JSONL journaling and operator summary helpers

The current-state projection seam is now explicit: `services/projection/` and `scripts/run_current_projection.py` replay raw Postgres capture events into projected current-state tables and the compatibility snapshots under `runtime/data/current/` that older runtime/research selectors still read.

The sportsbook capture split now has two explicit storage modes:

- the dedicated `run-sportsbook-capture` worker is Postgres-backed and expects the optional `postgres` dependency set plus a resolvable DSN / `postgres.dsn` marker
- generic temp-root research and ingest helper commands can still fall back to local JSON persistence when no DSN is configured
- dedicated `run-polymarket-capture` workers append authoritative raw market/user capture events plus checkpoints/source-health into Postgres-backed storage
- the dedicated `run-current-projection` worker replays raw Postgres capture events into selector-facing current tables and compatibility snapshots under `runtime/data/current/`

With a resolvable Postgres marker, JSON under `runtime/data/current/` is now treated as a compatibility export rather than the source of truth. Runtime and ingest read boundaries prefer the projected Postgres-backed current-state adapter when that marker exists.

The checked-in Docker and CI surfaces now prove repo-level quality and security gates plus the benchmark and substrate smoke paths. The CI contract includes compile checks, `ruff` format/lint gates, `mypy`, coverage-backed unittest discovery, a deterministic service-stack smoke path, and a Compose substrate smoke path. It still does not claim unattended live deployment readiness.

### `risk/`

This layer owns deterministic trading constraints.

- `risk/limits.py` enforces per-market, global, and optional per-event exposure caps
- `risk/cleanup.py` supports stale-order cleanup and verification flows used by operator tooling

Event-level exposure caps are real runtime behavior. `RiskEngine` tracks market-to-event mappings and rejects orders when `max_contracts_per_event` would be exceeded.

In the default `run-agent-loop` path, event identity is seeded from fair-value manifest metadata such as `event_key`, `sport`, `series`, and `game_id`.

### `research/`

This layer supports offline work.

- `research/fair_values.py` builds fair-value manifests from sportsbook-style rows
- `research/calibration.py` is the sports-facing compatibility facade for histogram calibration artifacts that now live in `forecasting/calibration.py`
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
- `build-llm-advisory`
- `show-llm-advisory`
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

- checks that `uv.lock` matches the dependency declarations
- installs the package plus locked dev tooling on Python 3.10
- compiles key runtime and research modules with `python -m compileall -q`
- runs a scoped `ruff format --check` contract for maintained repo-policy files
- runs the repo-wide serious `ruff check` lint gate
- runs the configured `mypy` contract
- runs the focused **Run advisory and docs contract regressions** unittest step
- runs the unittest suite under `tests/` with Coverage.py reporting
- runs the dedicated Postgres smoke, deterministic service-stack smoke, and Compose substrate smoke jobs
- runs the separate reproducibility job

The separate security workflow audits the locked dependency graph and scans changed commit ranges with checksum-verified `gitleaks`. The build-only release workflow produces Python distributions from the locked environment without isolated backend resolution and uploads them plus a Docker image archive as GitHub artifacts without publishing them to any registry or deployment target.

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
