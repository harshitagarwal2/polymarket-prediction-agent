# Getting Started

This guide is for a new engineer who wants to understand what this repo does today and start using the supported paths without guessing.

## Start with the right mental model

This repo is not just a pile of upstream references anymore.

It currently provides:

- a supervised Polymarket runtime in `adapters/`, `engine/`, `risk/`, and `scripts/`
- an offline sports fair-value and replay benchmark toolkit in `research/`
- runtime state persistence, operator controls, and JSONL journaling
- a schema-validated runtime policy file for repeatable runtime behavior
- a local dataset registry and walk-forward split helpers for research snapshots

The main supported venue path is Polymarket. Kalshi support exists behind the same interface, but it is thinner and should be treated as scaffolding, not parity.

There is also a reusable `forecasting/` package for domain-agnostic calibration, scoring, model-vs-market dashboard generation, and non-sports pipeline scaffolding. See `docs/FORECASTING_PLATFORM.md` for that surface.

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

The committed `uv.lock` file is the source of truth for dependency resolution. If dependency declarations change, refresh it with `uv lock`.

## Know the entrypoints

You can call the Python scripts directly, or use the installed console scripts from `pyproject.toml`.

Common entrypoints:

- `run-agent-loop`
- `operator-cli`
- `ingest-live-data`
- `run-polymarket-capture`
- `run-current-projection`
- `run-replay-attribution`
- `train-models`
- `build-fair-values`
- `build-sports-fair-values`
- `refresh-sports-fair-values`
- `render-model-vs-market-dashboard`
- `scaffold-forecasting-pipeline`
- `prediction-market-sports-benchmark`
- `prediction-market-sports-benchmark-suite`

## Path A, fastest safe path: run the offline benchmark

This is the easiest way to touch the repo without live credentials.

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

What this gives you:

- de-vigged fair values from sportsbook-style rows
- optional calibration overlay when a case includes `calibration_samples`
- replay metrics from the paper execution model
- suite summary artifacts and a suite-level edge ledger

Packaged fixtures live in `research/fixtures/`:

- `sports_benchmark_tiny.json`
- `sports_benchmark_best_line.json`
- `sports_benchmark_round_trip.json`

## Path B, Polymarket preview flow

### 1. Export market metadata

```bash
python3 scripts/export_polymarket_markets.py --output runtime/polymarket_markets.json --limit 200
```

### 2. Collect normalized sportsbook rows

```bash
python3 scripts/fetch_the_odds_api_rows.py \
  --sport-key basketball_nba \
  --event-map-file runtime/odds_event_map.json \
  --output runtime/sportsbook_odds.json
```

### 3. Build a runtime fair-value manifest

```bash
python3 scripts/build_sports_fair_values.py \
  --input runtime/sportsbook_odds.json \
  --markets-file runtime/polymarket_markets.json \
  --output runtime/fair_values.json \
  --book-aggregation best-line \
  --devig-method multiplicative \
  --max-age-seconds 900
```

The manifest format is more than a flat market-to-probability map. Each value can carry:

- `fair_value`
- optional `calibrated_fair_value`
- `generated_at`
- `source`
- `condition_id`
- `event_key`
- sports metadata used for identity matching and event-level risk grouping

You can also let the sample sports config set the fair-value build defaults:

```bash
build-fair-values \
  --input runtime/sportsbook_odds.json \
  --markets-file runtime/polymarket_markets.json \
  --output runtime/fair_values.json \
  --config-file configs/sports_nba.yaml
```

With the checked-in sample config, that currently resolves to `best-line` aggregation and `multiplicative` devigging.

For the live/current-state path, the fair-value builder is a different command surface. The intended sequence is:

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

If you want continuous sportsbook polling without keeping the whole end-to-end ingest script in the loop, run the dedicated capture worker instead:

Equivalent console entrypoint: `run-sportsbook-capture`

```bash
uv sync --extra postgres

# Export a DSN or write one to <runtime_root>/postgres/postgres.dsn first.
export PREDICTION_MARKET_POSTGRES_DSN=postgresql://...

python -m scripts.run_sportsbook_capture \
  --sport basketball_nba \
  --market h2h \
  --event-map-file runtime/odds_event_map.json \
  --root runtime/data \
  --refresh-interval-seconds 60
```

That command keeps replacing `runtime/data/current/sportsbook_events.json` and `runtime/data/current/sportsbook_odds.json` with the latest snapshot for downstream selectors, appends sportsbook quote events through the postgres-layer capture store, and mirrors `source_health` into `runtime/data/current/source_health.json` plus the relational `source_health` / `source_health_events` tables under the configured Postgres DSN. Because it uses the Postgres repository layer directly, the worker needs the optional `postgres` dependency set plus a resolvable DSN from `PREDICTION_MARKET_POSTGRES_DSN` / `POSTGRES_DSN` / `DATABASE_URL` or a `postgres.dsn` marker file under the configured runtime root.

For dedicated Polymarket capture and projection, the equivalent script entrypoints are:

```bash
python -m scripts.run_polymarket_capture market \
  --asset-id <asset-id> \
  --root runtime/data \
  --max-sessions 1

python -m scripts.run_current_projection \
  --root runtime/data \
  --max-cycles 1
```

`run_polymarket_capture` appends Polymarket market/user channel events into the Postgres-backed capture substrate and keeps `source_health` current for the dedicated capture lanes. `run_current_projection` then projects those raw capture rows back into `runtime/data/current/*.json` compatibility snapshots plus the projected current-state tables used by runtime readers.

The `--event-map-file` input enriches live sportsbook events with stable identity fields such as `event_key` and `game_id`. `build-mappings` now fails closed if that upstream identity is missing, keeps the flat runtime selector snapshot in `runtime/data/current/market_mappings.json`, and also emits a structured sidecar schema at `runtime/data/current/market_mapping_manifest.json` with `mapping_status`, structured `mapping_confidence`, structured `blocked_reason`, event identity, and rule-semantics details for each mapping decision. `build-fair-values --consensus-artifact ...` uses the consensus artifact as deterministic inference configuration for the current-state fair-value snapshot builder, and the optional `--calibration-artifact ...` overlay adds sibling `calibrated_fair_yes_prob` / `calibrated_fair_value` outputs without changing the raw baseline fields. `build-inference-dataset` then writes the latest joined inference rows to `runtime/data/processed/inference/joined_inference_dataset.jsonl` and registers a versioned `joined-inference-dataset` snapshot. `build-training-dataset` writes `runtime/data/processed/training/historical_training_dataset.jsonl`, registers a versioned `historical-training-dataset` snapshot, and enables `train-models --training-dataset historical-training-dataset --dataset-root runtime/data/datasets` for downstream model fitting.

The checked-in sample configs now include `capture.sport_key`, `runtime.sportsbook_market`, `runtime.event_map_file`, `runtime.consensus_artifact`, and an optional `runtime.calibration_artifact` key, so the live/current-state flow can also be driven from config defaults:

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

If the config also points `runtime.calibration_artifact` at a histogram calibration payload, the same command writes raw live fair values to `runtime/data/current/fair_values.json`, includes `calibrated_fair_yes_prob` beside them, and emits `metadata.calibration` in `runtime/data/current/fair_value_manifest.json` for runtime policy selection.

### 4. Run a preview cycle

```bash
run-agent-loop \
  --venue polymarket \
  --mode preview \
  --fair-values-file runtime/data/current/fair_value_manifest.json \
  --max-cycles 1
```

Useful modes:

- `preview`
- `run`
- `pair-preview`
- `pair-run`

If you want the sample runtime defaults from `configs/sports_nba.yaml`, you can run:

```bash
run-agent-loop \
  --config-file configs/sports_nba.yaml
```

That path currently provides `configs/runtime_policy.preview.json`, keeps the loop in preview mode, points `run-agent-loop` at `runtime/data/current/fair_value_manifest.json`, and sets `opportunity_root` to `runtime/data`.

The sample config also carries the normal preview-loop defaults for `max_fair_value_age_seconds`, `interval_seconds`, and `max_cycles`, so you only need extra CLI flags when you want to override them.

For Polymarket, the runtime now also derives live user-stream condition IDs from that configured fair-value manifest by default. You only need `POLYMARKET_LIVE_USER_MARKETS` when you want to override the derived subscription set manually.

For a long-running supervised preview process, add `--interval-seconds` and a larger `--max-cycles`.

### 5. Inspect state and journal output

```bash
operator-cli status --state-file runtime/safety-state.json
operator-cli status --state-file runtime/safety-state.json --journal runtime/events.jsonl
```

If you have offline contract-evidence rows, materialize the advisory sidecar before reviewing them:

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

`runtime/data/current/llm_advisory.json` is a review artifact for operators and dashboards. It is not an execution input.

Pass `--policy-file` when you want the advisory preview proposals and blocked reasons to match the same runtime policy that `run-agent-loop` is using.

## Runtime policy files

If you want repeatable runtime behavior, use `--policy-file`.

```bash
run-agent-loop \
  --venue polymarket \
  --mode preview \
  --fair-values-file runtime/data/current/fair_value_manifest.json \
  --policy-file runtime/policy.json
```

The policy file is versioned and schema-validated in `engine/runtime_policy.py`. Unknown keys fail fast.

Current top-level sections are:

- `schema_version`
- `fair_value`
- `strategy`
- `risk_limits`
- `opportunity_ranker`
- `pair_opportunity_ranker`
- `execution_policy_gate`
- `trading_engine`
- `order_lifecycle_policy`
- `venues.polymarket`

Minimal example:

```json
{
  "schema_version": 1,
  "fair_value": {
    "field": "calibrated"
  },
  "strategy": {
    "base_quantity": 1.0,
    "edge_threshold": 0.03
  },
  "risk_limits": {
    "max_global_contracts": 20,
    "max_contracts_per_market": 5,
    "max_contracts_per_event": 10
  },
  "venues": {
    "polymarket": {
      "depth_admission_levels": 3,
      "depth_admission_liquidity_fraction": 0.5,
      "depth_admission_max_expected_slippage_bps": 50
    }
  }
}
```

Important behavior:

- `fair_value.field` can be `raw` or `calibrated`
- `max_contracts_per_event` is supported in runtime risk limits
- `venues.polymarket` controls the Polymarket depth admission settings applied at adapter build time

## What calibration means here

Calibration in this repo is a benchmark and manifest overlay, not a magic forecasting layer.

- `research/calibration.py` fits a histogram calibrator from binary outcome samples
- benchmark cases can include `calibration_samples` and `calibration_bin_count`
- the fair-value builder can write `calibrated_fair_value` alongside raw `fair_value`
- runtime can choose which field to read by policy

That means calibration is explicit, inspectable, and reversible.

## Paper execution realism knobs

Replay and benchmark runs use `PaperExecutionConfig` in `research/paper.py`.

Current knobs are:

- `max_fill_ratio_per_step`
- `slippage_bps`
- `resting_max_fill_ratio_per_step`
- `resting_fill_delay_steps`
- `stale_after_steps`
- `price_move_bps_per_step`

These help make offline replay less naive, but they still do not make replay venue-true.

## Research dataset artifacts

`research/datasets.py` provides a local dataset registry for versioned snapshots.

It currently supports:

- `rows_jsonl` snapshots for dated row data
- `benchmark_cases` snapshots for benchmark-case collections
- manifest files per snapshot version
- chronological walk-forward split generation with `generate_walk_forward_splits(...)`

Snapshot outputs are written under `research/datasets/<dataset-name>/<version>/` when you use `DatasetRegistry`.

## Captured-data helpers

The new sports + Polymarket research tree also supports config-driven helper paths:

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
```

`ingest-live-data` writes typed capture artifacts (`markets` for Polymarket, `rows` for sports inputs). The checked-in league configs currently drive the `sports-inputs` path by supplying league-to-sport-key defaults; Gamma/CLOB capture still needs its layer selected explicitly. `train-models` can train from captured sports-input rows when they include labels.

Note that this config-driven helper path is for offline research captures. The live supervised path uses the `ingest-live-data` subcommands shown above rather than the standalone `build-fair-values` manifest builder.

## Environment variables

### Polymarket

- `POLYMARKET_PRIVATE_KEY`
- `POLYMARKET_FUNDER`, when needed
- `POLYMARKET_ACCOUNT_ADDRESS`, when needed for targeted recovery
- optional `POLYMARKET_LIVE_USER_MARKETS`
- optional `POLYMARKET_USER_WS_HOST`

### Kalshi

- `KALSHI_API_KEY_ID`
- `KALSHI_PRIVATE_KEY_PATH`

## CI and local validation expectations

GitHub Actions currently checks that `uv.lock` matches the dependency declarations, installs the package from the lockfile, compiles key modules with `python -m compileall -q`, runs the focused **Run advisory and docs contract regressions** unittest step, runs `python -m unittest discover -s tests -p "test_*.py"`, and also runs the separate reproducibility job.

If you are changing runtime or research behavior, that test suite is the baseline contract.

## What is still approximate or incomplete

- Live trading is still supervised, not unattended.
- Polymarket live-state support is a partial overlay, not a full engine-native live-state architecture.
- Replay is still an approximation of queue position, latency, and venue fill behavior.
- Kalshi support is thinner than Polymarket support.
- The benchmark toolkit is sports-focused and binary-market-only today.

## Read next

- `docs/ARCHITECTURE.md`
- `docs/OPERATOR_RUNBOOK.md`
- `docs/BENCHMARK_TOOLKIT.md`
