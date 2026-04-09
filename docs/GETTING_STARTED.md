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

## Install

From the repo root:

```bash
pip install -e .
pip install -e upstreams/py-clob-client
pip install -e upstreams/pykalshi
```

Optional research helpers:

```bash
pip install pandas pyarrow duckdb
```

## Know the entrypoints

You can call the Python scripts directly, or use the installed console scripts from `pyproject.toml`.

Common entrypoints:

- `run-agent-loop`
- `operator-cli`
- `build-sports-fair-values`
- `refresh-sports-fair-values`
- `prediction-market-sports-benchmark`
- `prediction-market-sports-benchmark-suite`

## Path A, fastest safe path: run the offline benchmark

This is the easiest way to touch the repo without live credentials.

```bash
prediction-market-sports-benchmark --fixture sports_benchmark_tiny.json
prediction-market-sports-benchmark-suite --output-dir runtime/benchmark-suite
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

### 4. Run a preview cycle

```bash
run-agent-loop \
  --venue polymarket \
  --mode preview \
  --fair-values-file runtime/fair_values.json \
  --max-cycles 1
```

Useful modes:

- `preview`
- `run`
- `pair-preview`
- `pair-run`

For a long-running supervised preview process, add `--interval-seconds` and a larger `--max-cycles`.

### 5. Inspect state and journal output

```bash
operator-cli status --state-file runtime/safety-state.json
operator-cli status --state-file runtime/safety-state.json --journal runtime/events.jsonl
```

## Runtime policy files

If you want repeatable runtime behavior, use `--policy-file`.

```bash
run-agent-loop \
  --venue polymarket \
  --mode preview \
  --fair-values-file runtime/fair_values.json \
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

These help make offline replay less naive, but they still do not make replay venue-true.

## Research dataset artifacts

`research/datasets.py` provides a local dataset registry for versioned snapshots.

It currently supports:

- `rows_jsonl` snapshots for dated row data
- `benchmark_cases` snapshots for benchmark-case collections
- manifest files per snapshot version
- chronological walk-forward split generation with `generate_walk_forward_splits(...)`

Snapshot outputs are written under `research/datasets/<dataset-name>/<version>/` when you use `DatasetRegistry`.

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

GitHub Actions currently installs the package, compiles key modules with `py_compile`, and runs `python -m unittest discover -s tests -p "test_*.py"`.

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
