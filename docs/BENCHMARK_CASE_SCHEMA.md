# Benchmark Case Schema

Each benchmark case is a JSON object with this top-level shape:

```json
{
  "name": "sports-benchmark-example",
  "description": "Optional human-readable description.",
  "fair_value_case": { ... },
  "replay_case": { ... }
}
```

At least one of `fair_value_case` or `replay_case` should be present.

## `fair_value_case`

Current fields:

- `rows` - normalized sportsbook-style rows
- `markets` - optional market snapshot rows used for sportsbook-to-market resolution
- `devig_method` - `multiplicative` or `power`
- `book_aggregation` - `independent` or `best-line`
- `max_age_seconds` - optional manifest freshness hint
- `source` - optional source label
- `expected_market_keys` - optional fail-closed list of market keys that must resolve
- `outcome_labels` - optional binary labels keyed by market key
- `calibration_samples` - optional list of `{ "prediction": ..., "outcome": ... }`
- `calibration_bin_count` - optional positive integer, default `5`

### Identity and resolution rules

Rows must resolve through at least one identity field:

- `event_key`
- `condition_id`
- `game_id`

Only binary outcome groups are supported by the current fair-value builder.

### Fail-closed fields

- `expected_market_keys`
  - if any expected key is missing after resolution, the benchmark raises an error
- `outcome_labels`
  - if any labeled key is missing after resolution, the benchmark raises an error instead of scoring a partial subset

### Calibration fields

When `calibration_samples` are present:

- the runner fits a histogram calibrator
- the report includes raw and calibrated forecast metrics
- the report includes calibrated market probabilities
- per-market evaluation rows include both raw and calibrated error columns

## `replay_case`

Current fields:

- `strategy`
  - `quantity`
  - `edge_threshold`
  - `aggressive`
- `broker`
  - `cash`
  - `max_fill_ratio_per_step`
  - `slippage_bps`
  - `resting_max_fill_ratio_per_step`
  - `resting_fill_delay_steps`
- `risk_limits`
  - `max_global_contracts`
  - `max_contracts_per_market`
  - `reserve_contracts_buffer`
  - `max_order_notional`
  - `min_price`
  - `max_price`
  - `max_daily_loss`
  - `daily_realized_pnl`
  - `enforce_atomic_batches`
- `steps`
  - replay steps containing `book`, optional `fair_value`, and optional `metadata`

### Important limitation

The replay schema mirrors the current `ReplayRiskConfig`, not the full live runtime risk surface. For example, event-level exposure caps are a live runtime feature, but they are not part of the replay case schema today.

## Packaged examples

- `sports_benchmark_tiny.json` - tiny fair-value plus replay case
- `sports_benchmark_best_line.json` - fair-value case with best-line aggregation and midpoint baseline
- `sports_benchmark_round_trip.json` - replay-only round-trip case with no-trade baseline

## CLI contract

- `prediction-market-sports-benchmark --fixture ...` accepts packaged fixture names shipped with the project
- `prediction-market-sports-benchmark-suite --output-dir ...` runs the packaged multi-case suite
- `python3 scripts/run_sports_benchmark.py --case /path/to/case.json` runs an explicit case file
- `python3 scripts/run_sports_benchmark_suite.py --fixtures-dir /path/to/cases --output-dir ...` runs a directory of case files

## Suite artifact contract

Suite artifacts are written as:

- `benchmark_suite_summary.json`
- `benchmark_suite_summary.md`
- `benchmark_suite_edge_ledger.json`
- `cases/<safe-case-name>.json`

The suite summary JSON currently exposes:

- `aggregate`
- `case_results`
- `failures`
- `edge_ledger`

`edge_ledger.rows` contains the per-market fair-value evaluation rows, enriched with suite context such as `case_name` and `case_path`.
