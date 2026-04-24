# Prediction Market Agent Architecture

This document describes the current verified shape of the repo, not an aspirational future state.

## System summary

The current repository is best understood as four connected architecture lanes:

1. **Capture** — dedicated workers append raw ingress, checkpoints, and `source_health` into Postgres-backed capture storage.
2. **Projection** — a dedicated projector replays raw capture lanes into projected current-state tables and compatibility JSON under `runtime/data/current/`.
3. **Deterministic builders** — mapping, fair-value, opportunity, and dataset commands consume the current-state read boundary and materialize derived artifacts.
4. **Supervised runtime and operator control** — `run-agent-loop` remains the venue-facing runtime shell, while `operator-cli` remains the pause/resume/status/advisory control plane.

Polymarket is the main supported venue path. Kalshi remains present behind the same adapter interface, but the runtime, capture, docs, and tests are much more complete on the Polymarket side.

## Authority and ownership boundaries

This repo currently treats Postgres plus projected current-state tables as the authoritative live data plane when a Postgres DSN marker is resolvable.

- capture workers own raw ingress, checkpoints, and source-health writes only
- `services/projection/` plus `scripts/run_current_projection.py` own the compatibility `runtime/data/current/*.json` exports for capture-owned tables
- deterministic builders in `scripts/ingest_live_data.py`, `storage/current_projection.py`, `execution/planner.py`, and `opportunity/` own mappings, fair values, opportunities, and other derived outputs
- `scripts/run_agent_loop.py` owns the supervised trading loop, reconciliation, safety state, and journaled execution behavior

That means selector-facing current JSON is not capture-worker authority. When a DSN marker exists, runtime and ingest readers treat the projected Postgres-backed read boundary as authoritative and treat `runtime/data/current/*.json` as compatibility exports.

See [`docs/adr/authority-and-reconciliation.md`](adr/authority-and-reconciliation.md) for the sanctioned-entrypoint and reconciliation contract used by the current productionization wave.

The legacy live `polymarket-bbo` path is deprecated and should not be treated as the supported production capture path. The supported live capture path is the dedicated `run-polymarket-capture` worker.

## Primary system flows

### 1. Capture -> projection -> current-state tables

The data-plane substrate is explicit now.

- `run-sportsbook-capture` appends sportsbook raw envelopes through `services/capture/sportsbook.py`
- `run-polymarket-capture market` appends Polymarket market-channel envelopes through `services/capture/polymarket.py` and `services/capture/polymarket_worker.py`
- `run-polymarket-capture user` appends Polymarket user-channel envelopes through the same capture substrate
- `run-current-projection` replays raw rows from `raw_capture_events` and advances projection checkpoints in `capture_checkpoints`
- `services/projection/current_state.py` projects capture-owned tables such as sportsbook events, sportsbook odds, Polymarket market catalog, Polymarket BBO rows, and `source_health`

This lane is what keeps Postgres-backed reads and `runtime/data/current/*.json` compatibility exports aligned.

### 2. Deterministic builder lane

The deterministic builder path hangs off the projected current-state read boundary rather than the raw capture workers.

- `python -m scripts.ingest_live_data build-mappings` builds deterministic contract mappings and writes `runtime/data/current/market_mapping_manifest.json`
- `python -m scripts.ingest_live_data build-fair-values` builds live/current-state fair values and writes `runtime/data/current/fair_value_manifest.json`
- `python -m scripts.ingest_live_data build-opportunities` materializes executable opportunities from mappings, fair values, and executable BBO state
- `python -m scripts.ingest_live_data build-inference-dataset` and `build-training-dataset` materialize versioned datasets and keep `source_health` in sync

This lane is deterministic by design: it uses explicit manifests, thresholds, and persisted rows rather than hidden runtime state.

### 3. Supervised runtime lane

`run-agent-loop` remains the supervised runtime entrypoint. It does **not** simply replay the projected opportunity table as its live trading loop.

Instead it:

1. loads config and runtime policy
2. builds the venue adapter and fair-value provider
3. optionally resolves projected current-state authority for kill-switch and preview context support
4. lists live venue markets through the adapter
5. ranks opportunities against the fair-value provider
6. previews candidate execution in `TradingEngine`
7. sizes, policy-gates, and risk-checks intents
8. previews or places orders depending on mode
9. reconciles against authoritative truth
10. persists safety state, runtime metrics, and journal output

That distinction matters: the projected current-state lane feeds deterministic builders and operator-side preview context, while the supervised runtime still operates as a live adapter-driven decision loop.

### 4. Operator and advisory lane

`scripts/operator_cli.py` is the operator entrypoint.

It can:

- inspect `runtime/safety-state.json`
- inspect `runtime/events.jsonl`
- compare persisted truth against venue truth
- pause, unpause, hold new orders, clear hold, force refresh, resume, cancel all, cancel stale, and sync quotes
- build and inspect the operator-side advisory artifact with `build-llm-advisory` and `show-llm-advisory`

`runtime/data/current/llm_advisory.json` is operator-side and dashboard-facing only. It does not participate in order placement, sizing, risk limits, or execution-policy gating.

## Main modules and responsibilities

### `adapters/`

This is the venue boundary.

- `adapters/polymarket/` is the richest adapter today, with the package facade in `__init__.py` and extracted modules such as `gamma_client.py`, `clob_client.py`, `market_catalog.py`, websocket helpers, and normalization helpers
- `adapters/kalshi.py` provides a thinner path with normalized types
- `adapters/types.py` holds the shared domain model used across runtime and research code

Polymarket-specific runtime details that matter today:

- authenticated CLOB access
- open-order and fill normalization
- account snapshot recovery
- heartbeat management
- live user-state overlay support
- market-state overlay support
- depth-admission settings applied from runtime policy

### `services/capture/`

This layer owns dedicated ingestion workers.

- `services/capture/sportsbook.py` handles sportsbook raw ingress, checkpoint writes, and `source_health`
- `services/capture/worker.py` runs the continuous sportsbook worker loop
- `services/capture/polymarket.py` owns Polymarket catalog, market-channel, and user-channel persistence helpers
- `services/capture/polymarket_worker.py` runs dedicated Polymarket market/user capture workers

The dedicated worker path is the supported live capture boundary. It is intentionally separate from the monolithic legacy ingest shape.

### `services/projection/`

This layer owns replaying raw capture events into current-state compatibility tables.

- `services/projection/current_state.py` projects sportsbook, Polymarket market-catalog, Polymarket BBO, and `source_health` lanes
- `services/projection/worker.py` runs the projection loop and advances projection checkpoints

This is the layer that keeps `runtime/data/current/*.json` synchronized with authoritative projected reads.

### `storage/`

This layer makes runtime and capture storage explicit.

- `storage/current_read_adapter.py` chooses between `ProjectedCurrentStateReadAdapter` and `FileCurrentStateReadAdapter`
- `storage/current_projection.py` builds preview-order context from current-state tables
- `storage/journal.py` holds runtime JSONL journaling and summaries
- `storage/raw/`, `storage/parquet/`, and `storage/postgres/` define the explicit storage backends

The important architectural point is that current-state reads are now a first-class adapter boundary rather than ad hoc JSON file access.

### `contracts/`

This layer owns contract meaning and mapping semantics.

- `contracts/ontology.py` builds normalized contract identities and grouping keys
- `contracts/confidence.py` scores deterministic contract-match confidence
- `contracts/resolution_rules.py` parses resolution metadata and freeze conditions
- `contracts/mapping*.py` and `contracts/models.py` drive structured mapping manifests and validation

### `forecasting/`

This layer owns fair value and forecast evaluation.

- `forecasting/fair_value_engine.py` holds fair-value providers plus deterministic consensus fair-value combination
- `forecasting/calibrator.py` and `forecasting/calibration.py` hold calibration loaders and adapters
- `forecasting/model_registry.py`, `ml_train.py`, `ml_infer.py`, and `ml_features.py` expose reusable model-facing surfaces
- `forecasting/dashboards.py`, `forecasting/contracts.py`, and `forecasting/pipeline.py` cover model-vs-market review, optional LLM evidence, and non-sports pipeline scaffolding

`research/calibration.py` and forecast-scoring pieces of `research/scoring.py` remain compatibility facades for the older sports benchmark path.

### `opportunity/`

This layer sits between fair value and execution.

- `opportunity/executable_edge.py` computes net edge from executable quotes, fees, and slippage assumptions
- `opportunity/fillability.py` estimates visible fillability from market snapshots and books
- `opportunity/ranker.py` owns single-market and paired ranking
- `opportunity/models.py` defines the deterministic opportunity snapshot shape used by builder flows

### `execution/`

This layer owns deterministic execution proposals and supervised quote-shell helpers.

- `execution/planner.py` evaluates opportunity rows into `OrderProposal` payloads with freeze windows and source-health gating
- `execution/quote_manager.py` drives the supervised `operator-cli sync-quote` shell
- `execution/cancel_replace.py` and `execution/models.py` hold the quote-shell lifecycle model

### `engine/`

This is the runtime orchestration layer.

Important components:

- `engine/discovery.py` - polling loop orchestration, scan-cycle logging, ranker/pair-ranker bridging, deterministic sizer, and policy gating
- `engine/runner.py` - trading engine, reconciliation flow, safety state updates, pending cancel and pending submission recovery
- `engine/runtime_policy.py` - schema-validated runtime policy loader
- `engine/runtime_bootstrap.py` - adapter building plus projected-current-state authority selection
- `engine/safety_state.py` and `engine/safety_store.py` - persisted operator and recovery state

The engine is designed to fail closed when truth is incomplete or recovery work is still open.

### `risk/`

This layer owns deterministic trading constraints.

- `risk/limits.py` enforces per-market, global, and optional per-event exposure caps
- `risk/cleanup.py` supports stale-order cleanup and verification flows used by operator tooling
- `risk/kill_switch.py` derives a supervised hard gate from projected `source_health` and other future kill-switch inputs

Event-level exposure caps are real runtime behavior. `RiskEngine` tracks market-to-event mappings and rejects orders when `max_contracts_per_event` would be exceeded.

### `llm/`

This layer owns operator-facing advisory helpers and sidecar artifacts.

- `llm/advisory_artifact.py` validates and writes the structured advisory sidecar at `runtime/data/current/llm_advisory.json`
- `llm/evidence_summarizer.py` and `llm/operator_memo.py` keep summary/memo rendering deterministic
- `llm/advisory_context.py` builds the same preview context shape used by operator advisory flows

### `research/`

This layer supports offline work.

- `research/fair_values.py` builds sportsbook-style fair-value manifests for the benchmark lane
- `research/paper.py` and `research/replay.py` drive replay and paper execution
- `research/benchmark_runner.py` and `research/benchmark_suite.py` run single-case and suite benchmarks
- `research/data/` and `research/datasets.py` store local dataset snapshots, derived datasets, and walk-forward splits

## Current-state artifacts and compatibility exports

The current architecture makes these artifact families important:

- raw capture events and checkpoints in Postgres-backed capture storage
- projected current-state tables exposed through `ProjectedCurrentStateReadAdapter`
- compatibility JSON under `runtime/data/current/`
- runtime safety and journal artifacts (`runtime/safety-state.json`, `runtime/events.jsonl`)
- operator-side artifacts such as `runtime/data/current/llm_advisory.json`, `runtime/data/current/preview_order_context.json`, and `runtime/data/current/runtime_metrics.json`
- versioned dataset snapshots under `runtime/data/datasets`

The compatibility files are still important for human inspection and backwards compatibility, but the repo now treats the projected current-state adapter as the primary read boundary when Postgres authority is configured.

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

## Operator control plane

`scripts/operator_cli.py` is the operator entrypoint.

Current commands include:

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
- `sync-quote`

The CLI reads persisted safety state, can compare it against venue truth, and can drive recovery actions without starting the polling loop.

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
- dedicated capture-worker plus projector substrate
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
