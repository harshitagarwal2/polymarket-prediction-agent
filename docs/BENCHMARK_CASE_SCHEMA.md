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

Both `fair_value_case` and `replay_case` are optional, but at least one should be present for a useful benchmark case.

## `fair_value_case`

Fields:

- `rows`: normalized sportsbook-style rows
- `markets`: optional market snapshot rows for sportsbook-to-market resolution
- `devig_method`: `multiplicative` or `power`
- `book_aggregation`: `independent` or `best-line`
- `max_age_seconds`: optional manifest freshness hint
- `source`: optional source label
- `expected_market_keys`: optional fail-closed list of keys that must appear in the manifest
- `outcome_labels`: optional binary labels keyed by market key

Additional constraints inherited from the fair-value builder:

- rows must resolve through at least one identity field: `event_key`, `condition_id`, or `game_id`
- only binary outcome groups are supported in the current fair-value benchmark flow

### Fail-closed fields

- `expected_market_keys`
  - if any expected key is missing after resolution, the benchmark raises an error
- `outcome_labels`
  - if any labeled key is missing after resolution, the benchmark raises an error instead of scoring a partial subset

## `replay_case`

Fields:

- `strategy`
  - `quantity`
  - `edge_threshold`
  - `aggressive`
- `broker`
  - `cash`
  - `max_fill_ratio_per_step`
  - `slippage_bps`
- `risk_limits`
  - mirrors `RiskLimits`
- `steps`
  - replay steps containing `book`, optional `fair_value`, and optional `metadata`

## Packaged examples

- `sports_benchmark_tiny.json` — tiny fair-value + replay case
- `sports_benchmark_best_line.json` — fair-value case with best-line aggregation and midpoint baseline
- `sports_benchmark_round_trip.json` — replay-only round-trip case with no-trade baseline

## Packaged CLI contract

- `prediction-market-sports-benchmark --fixture ...` accepts only packaged fixture names shipped with the project
- `prediction-market-sports-benchmark-suite --output-dir ...` runs the packaged multi-case suite
- suite artifacts are written as:
  - `benchmark_suite_summary.json`
  - `benchmark_suite_summary.md`
  - `cases/<safe-case-name>.json`

## Suite summary report shape

`benchmark_suite_summary.json` is emitted with these top-level keys:

- `aggregate`
- `case_results`
- `failures`

`aggregate` includes cross-case metrics such as average fair-value scores, average replay scores, and baseline delta summaries.
