# Prediction Market Agent

This repository is a Polymarket-first prediction market trading and research workspace.

Today it has two mature user-facing paths:

- a supervised Polymarket runtime for preview, operator-controlled live runs, and runtime-state recovery
- an offline sports fair-value and replay benchmark toolkit for reproducible research work

It is not an unattended live trading system. The live path is still supervised, fail-closed, and conservative by design.

## Current state, in plain terms

- **Polymarket is the primary mature venue path.** The Polymarket adapter, polling loop, operator CLI, runtime policy loader, and benchmark-to-runtime fair-value flow are the main supported workflow.
- **Kalshi support exists, but it is thinner.** The adapter is useful for interface compatibility and early experiments, but the repo does not claim Polymarket and Kalshi parity.
- **Runtime policy is real and schema-validated.** `engine/runtime_policy.py` loads a versioned JSON policy file and rejects unknown keys or wrong types.
- **Fair-value manifests can carry raw and calibrated values.** Runtime can choose which field to trade from through policy.
- **Risk caps include event-level exposure.** `risk/limits.py` enforces per-market, global, and optional per-event caps when event identity is available.
- **Replay is approximate.** The paper broker models resting orders, reserved capital, fill caps, delay, slippage, wait-time drift, and stale-snapshot flags, but it is still not a claim of venue-true queue or latency realism.

## What the repo does

- wraps venue APIs behind normalized adapters in `adapters/`
- runs preview, live, and pair-mode polling loops from `scripts/run_agent_loop.py`
- persists safety state, operator pause state, pending recovery items, and recent truth summaries
- exposes operator actions through `scripts/operator_cli.py`
- builds Polymarket-facing fair-value manifests from normalized sportsbook rows
- supports optional benchmark calibration overlays and suite-level edge-ledger artifacts
- snapshots local research datasets and generates chronological walk-forward splits
- runs unit tests in CI and compiles the key runtime and research modules on every push and pull request

## Repository map

- `adapters/` - venue wrappers and normalized types
- `engine/` - polling loop, orchestration, runtime policy, reconciliation, safety state
- `risk/` - exposure caps, cleanup helpers, deterministic risk checks
- `research/` - fair-value builder, calibration, replay, benchmark runner, dataset registry
- `scripts/` - user-facing CLI entrypoints
- `configs/` - sample league and runtime-policy configuration files
- `docs/` - onboarding, architecture, runbook, and benchmark docs
- `tests/` - unit coverage for runtime policy, risk, replay, benchmark, and CLI behavior

## Install

The canonical local setup is a locked `uv` environment, verified on Python 3.10.

From the repo root, sync the base environment:

```bash
uv sync --locked
```

For the reproducible offline benchmark path:

```bash
uv sync --locked --extra research
```

For venue-specific runtime integrations:

```bash
uv sync --locked --extra polymarket
uv sync --locked --extra kalshi
```

If you want the console entrypoints on your shell `PATH`, activate the virtualenv after syncing:

```bash
source .venv/bin/activate
```

The committed `uv.lock` file is the source of truth for dependency resolution. If dependency declarations change, refresh it with:

```bash
uv lock
```

The package installs these console entrypoints from `pyproject.toml`:

- `run-agent-loop`
- `operator-cli`
- `ingest-live-data`
- `train-models`
- `build-fair-values`
- `build-sports-fair-values`
- `refresh-sports-fair-values`
- `run-sports-benchmark`
- `run-sports-benchmark-suite`
- `prediction-market-sports-benchmark`
- `prediction-market-sports-benchmark-suite`
- `render-model-vs-market-dashboard`
- `scaffold-forecasting-pipeline`

## Fast start paths

### 1. Run the offline benchmark first

```bash
make sync-research
make reproduce

# or run the suite command directly
uv run --locked --extra research prediction-market-sports-benchmark --fixture sports_benchmark_tiny.json
uv run --locked --extra research prediction-market-sports-benchmark-suite --output-dir runtime/benchmark-suite
```

Containerized reproduction path:

```bash
docker build -t polymarket-prediction-agent:v0.1.0 .
docker run --rm polymarket-prediction-agent:v0.1.0
```

Packaged fixtures live in `research/fixtures/`:

- `sports_benchmark_tiny.json`
- `sports_benchmark_best_line.json`
- `sports_benchmark_round_trip.json`

The suite writes:

- `benchmark_suite_summary.json`
- `benchmark_suite_summary.md`
- `benchmark_suite_edge_ledger.json`
- `benchmark_suite_execution_ledger.json`
- `cases/<case-name>.json`

There is also a checked-in example suite output under `runtime/benchmark-suite-e2e-check/`.

### 2. Build a Polymarket fair-value manifest

```bash
python3 scripts/export_polymarket_markets.py --output runtime/polymarket_markets.json --limit 200
python3 scripts/fetch_the_odds_api_rows.py --sport-key basketball_nba --event-map-file runtime/odds_event_map.json --output runtime/sportsbook_odds.json
python3 scripts/build_sports_fair_values.py --input runtime/sportsbook_odds.json --markets-file runtime/polymarket_markets.json --output runtime/fair_values.json --book-aggregation best-line --devig-method multiplicative --max-age-seconds 900
```

The emitted manifest can include:

- `fair_value`
- optional `calibrated_fair_value`
- `condition_id`
- `event_key`
- sport metadata such as `sport`, `series`, `game_id`, and `sports_market_type`

Additional architecture-aligned helper entrypoints now exist for the split research tree:

- `ingest-live-data` - write normalized offline capture envelopes for Gamma, CLOB, Data API, or sports-input payloads
- `run-sportsbook-capture` - continuously poll sportsbook odds into runtime/current and postgres-layer capture stores
- `train-models` - write lightweight Elo, Bradley–Terry, or blend artifacts from benchmark cases
- `build-fair-values` - thin wrapper around the existing sports fair-value manifest builder

For the supervised live/current-state path, use the `ingest-live-data` subcommands directly:

```bash
python -m scripts.train_models --model consensus --output runtime/consensus_artifact.json

python -m scripts.ingest_live_data sportsbook-odds \
  --sport basketball_nba \
  --market h2h \
  --event-map-file runtime/odds_event_map.json \
  --root runtime/data

python -m scripts.ingest_live_data build-mappings \
  --market h2h \
  --root runtime/data

python -m scripts.ingest_live_data build-fair-values \
  --root runtime/data \
  --consensus-artifact runtime/consensus_artifact.json

python -m scripts.ingest_live_data build-fair-values \
  --root runtime/data \
  --consensus-artifact runtime/consensus_artifact.json \
  --calibration-artifact runtime/calibration_artifact.json

python -m scripts.ingest_live_data build-inference-dataset \
  --root runtime/data

python -m scripts.ingest_live_data build-training-dataset \
  --input runtime/sports_inputs_labeled.json \
  --polymarket-input runtime/polymarket_markets.json \
  --root runtime/data

python -m scripts.train_models \
  --model elo \
  --training-dataset historical-training-dataset \
  --dataset-root runtime/data/datasets \
  --output runtime/elo_artifact.json
```

For a continuous sportsbook capture worker that no longer depends on the monolithic end-to-end script loop, run:

```bash
uv sync --extra postgres

# Either export a DSN directly...
export PREDICTION_MARKET_POSTGRES_DSN=postgresql://...

# ...or write one to runtime/data/postgres/postgres.dsn

python -m scripts.run_sportsbook_capture \
  --sport basketball_nba \
  --market h2h \
  --event-map-file runtime/odds_event_map.json \
  --root runtime/data \
  --refresh-interval-seconds 60
```

That worker keeps publishing the same downstream-compatible current-state artifacts under `runtime/data/current/`, while also appending sportsbook quote events and mirrored source-health rows through the postgres-layer storage contract. The dedicated worker uses the Postgres repository layer directly, so it requires the optional `postgres` extra and either `PREDICTION_MARKET_POSTGRES_DSN` / `POSTGRES_DSN` / `DATABASE_URL` or a `postgres.dsn` marker file under the configured `runtime/data/postgres` root.

That live path keeps sportsbook event identity (`event_key` / `game_id`) in the current-state mapping flow and lets the consensus artifact configure the deterministic fair-value snapshot builder. `build-mappings` still writes the flat selector-facing snapshot to `runtime/data/current/market_mappings.json`, and now also emits a structured sidecar schema at `runtime/data/current/market_mapping_manifest.json` with `mapping_status`, structured `mapping_confidence`, structured `blocked_reason`, identity metadata, and rule semantics for each mapped Polymarket market. If you also pass a calibration artifact, the live snapshot keeps raw `fair_yes_prob` in `runtime/data/current/fair_values.json`, adds sibling `calibrated_fair_yes_prob`, and projects `calibrated_fair_value` plus calibration metadata into `runtime/data/current/fair_value_manifest.json`. `build-inference-dataset` materializes the latest joined inference rows at `runtime/data/processed/inference/joined_inference_dataset.jsonl` and also registers a versioned `joined-inference-dataset` snapshot under `runtime/data/datasets`. `build-training-dataset` does the same for labeled training rows at `runtime/data/processed/training/historical_training_dataset.jsonl` and the `historical-training-dataset` snapshot that `train-models --training-dataset ...` can now consume directly.

The checked-in sample league configs now carry those live defaults too, so the same flow can be driven with fewer flags:

```bash
python -m scripts.ingest_live_data sportsbook-odds \
  --config-file configs/sports_nba.yaml \
  --root runtime/data

python -m scripts.ingest_live_data build-mappings \
  --config-file configs/sports_nba.yaml \
  --root runtime/data

python -m scripts.ingest_live_data build-fair-values \
  --config-file configs/sports_nba.yaml \
  --root runtime/data
```

The sample league configs now include an optional `runtime.calibration_artifact` key alongside `runtime.consensus_artifact`. Leave it empty for the raw-only path, or point it at a histogram calibration payload when you want the live manifest to carry calibrated values too.

The sample league configs can drive the new helper entrypoints directly:

```bash
ingest-live-data \
  --layer sports-inputs \
  --config-file configs/sports_nba.yaml \
  --event-map-file runtime/odds_event_map.json \
  --output runtime/sports_inputs.json

train-models \
  --config-file configs/sports_nba.yaml \
  --training-data runtime/sports_inputs_labeled.json \
  --output runtime/elo_artifact.json

python -m scripts.ingest_live_data build-training-dataset \
  --input runtime/sports_inputs_labeled.json \
  --polymarket-input runtime/polymarket_markets.json \
  --root runtime/data

train-models \
  --model elo \
  --training-dataset historical-training-dataset \
  --dataset-root runtime/data/datasets \
  --output runtime/elo_artifact.json

build-fair-values \
  --input runtime/sportsbook_odds.json \
  --markets-file runtime/polymarket_markets.json \
  --output runtime/fair_values.json \
  --config-file configs/sports_nba.yaml
```

### 3. Run one supervised Polymarket preview cycle

```bash
run-agent-loop \
  --venue polymarket \
  --mode preview \
  --fair-values-file runtime/data/current/fair_value_manifest.json \
  --max-cycles 1
```

You can also run pair ranking modes:

- `--mode pair-preview`
- `--mode pair-run`

### 4. Put trading thresholds in a runtime policy file

```bash
run-agent-loop \
  --venue polymarket \
  --mode preview \
  --fair-values-file runtime/data/current/fair_value_manifest.json \
  --policy-file runtime/policy.json
```

The policy file is the source of truth for:

- fair-value field selection, `raw` or `calibrated`
- base quantity and edge threshold
- shared risk limits, including `max_contracts_per_event`
- ranker and pair-ranker filters
- deterministic execution gate settings
- engine timing and overlay recovery settings
- lifecycle cleanup policy
- Polymarket depth admission settings

The sample sports configs can also set runtime defaults:

```bash
run-agent-loop \
  --config-file configs/sports_nba.yaml
```

With the current sample config, that path supplies `configs/runtime_policy.preview.json`, keeps the loop in preview mode, points `run-agent-loop` at `runtime/data/current/fair_value_manifest.json`, and sets `opportunity_root` to `runtime/data`.

It also supplies the sample safety/polling defaults, so you do not need to restate `--max-fair-value-age-seconds`, `--interval-seconds`, or `--max-cycles` for the normal preview loop.

### 5. Inspect runtime state

```bash
operator-cli status --state-file runtime/safety-state.json
operator-cli status --state-file runtime/safety-state.json --journal runtime/events.jsonl
```

### 6. Build an operator-side advisory artifact

```bash
operator-cli build-llm-advisory \
  --llm-input runtime/llm_contract_rows.json \
  --policy-file runtime/policy.json \
  --opportunity-root runtime/data \
  --output runtime/data/current/llm_advisory.json

operator-cli show-llm-advisory \
  --llm-advisory-file runtime/data/current/llm_advisory.json \
  --format markdown

operator-cli status --state-file runtime/safety-state.json --llm-advisory-file runtime/data/current/llm_advisory.json
```

`runtime/data/current/llm_advisory.json` is an operator-side sidecar artifact. It stays off the deterministic runtime path and can also be passed to `render-model-vs-market-dashboard --llm-contract-evidence ...` for contract review.

Pass `--policy-file` when you want the advisory preview proposals and blocked reasons to use the same thresholds and freeze rules as `run-agent-loop`.

## Runtime safety model

The supervised runtime is built to fail closed.

- New trading can be blocked by pause, halt, hold-new-orders, incomplete truth, pending recovery work, or policy-gate rejection.
- Polymarket has a phase-1 live-state overlay for open-order and fill freshness, but REST truth is still the final source for balances, positions, and reconciliation.
- The operator CLI exposes `pause`, `unpause`, `hold-new-orders`, `clear-hold-new-orders`, `force-refresh`, `resume`, `cancel-all`, and `cancel-stale`.

This is meant for supervised operation, not for fire-and-forget deployment.

## Benchmark and research artifacts

The benchmark stack is useful when you want a reproducible offline slice of the repo.

- `research/benchmark_cli.py` runs a single case
- `research/benchmark_suite_cli.py` runs a multi-case suite
- `research/benchmark_runner.py` reports raw and calibrated forecast metrics when calibration samples are present
- `research/benchmark_suite.py` writes a suite-level edge ledger for later attribution work
- `research/datasets.py` snapshots row datasets or benchmark cases and generates walk-forward splits

## CI

`.github/workflows/python-ci.yml` currently runs a unittest job plus a separate reproducibility job on pushes and pull requests. The unittest job does five things:

- checks that `uv.lock` matches the dependency declarations
- installs the package from the committed lockfile
- compiles the key runtime and research modules with `python -m compileall -q`
- runs the focused **Run advisory and docs contract regressions** unittest step
- runs `python -m unittest discover -s tests -p "test_*.py"`

The reproducibility job separately exercises the benchmark and fixture workflows on Ubuntu.

## Citation and release metadata

- `CITATION.cff` at the repo root provides software citation metadata for GitHub's **Cite this repository** surface.
- `v0.1.0` is the first intended reproducible software release tag for this benchmark toolkit.
- Zenodo DOI minting still requires Zenodo-side GitHub linking and repository enablement in addition to the repo metadata in this project.

## Read next

- `docs/GETTING_STARTED.md` for the new-engineer setup path
- `docs/ARCHITECTURE.md` for the current system shape
- `docs/architecture/sports_polymarket_architecture.md` for the target sports + Polymarket architecture
- `docs/VERIFICATION_SPORTS_POLYMARKET.md` for the current verification record of the added sports + Polymarket paths
- `docs/OPERATOR_RUNBOOK.md` for supervised runtime operation
- `docs/BENCHMARK_TOOLKIT.md` for the offline benchmark flow
- `docs/BENCHMARK_PROTOCOL.md` for suite artifacts and evaluation rules
- `docs/BENCHMARK_CASE_SCHEMA.md` for case file structure
