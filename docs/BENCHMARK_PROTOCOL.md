# Benchmark Protocol

This benchmark is designed as an **offline, public-safe evaluation workflow** for sports fair values and replay behavior.

## End-to-end flow

1. **Case inputs**
   - normalized sportsbook-style rows
   - optional market snapshots
   - optional replay steps
   - optional labels keyed by market key
2. **Fair-value lane**
   - resolve sportsbook rows to market keys
   - de-vig into fair probabilities
   - require identity via `event_key`, `condition_id`, or `game_id`
   - support binary groups only in the current fair-value builder
   - validate `expected_market_keys`
   - score against `outcome_labels`
3. **Replay lane**
   - run `FairValueBandStrategy` over replay steps
   - simulate fills with `PaperBroker`
   - compare against a no-trade replay baseline
4. **Suite layer**
   - run all packaged or directory-provided cases
   - emit per-case JSON
   - emit aggregate JSON and Markdown summaries

## Baselines

The toolkit now reports several comparison points:

- `bookmaker_multiplicative_independent`
- `bookmaker_power_independent`
- `bookmaker_multiplicative_best_line`
- `market_midpoint`
- `noop_strategy` for replay-only comparison

These are meant to make the benchmark more useful as a research artifact, not to claim production-quality market modeling.

## Public-safe scope

- offline only
- fixture/case driven
- no proprietary sportsbook history shipped in the repo
- no live venue execution in the benchmark path

## Running a single case

```bash
python3 scripts/run_sports_benchmark.py --fixture sports_benchmark_tiny.json
```

## Running the packaged suite

```bash
python3 scripts/run_sports_benchmark_suite.py --output-dir runtime/benchmark-suite
```

Installed console entrypoint:

```bash
prediction-market-sports-benchmark-suite --output-dir runtime/benchmark-suite
```

This writes:

- `benchmark_suite_summary.json`
- `benchmark_suite_summary.md`
- `cases/<case-name>.json`

The suite summary JSON has three top-level sections:

- `aggregate` — cross-case metrics and baseline deltas
- `case_results` — per-case benchmark reports
- `failures` — case-level execution failures captured by the suite runner

Suite case artifact names are normalized to filesystem-safe names before write. If normalization would produce an empty name, the fallback filename stem is `benchmark-case`.

The fair-value artifacts intentionally preserve the repo's runtime manifest contract: top-level `generated_at`, `source`, optional `max_age_seconds`, `values`, and optional `skipped_groups`, plus per-record provenance fields such as `condition_id`, `event_key`, and sport metadata.

## Interpretation caveat

Replay results use the repo's paper-execution model. They are useful for relative offline comparison, but they are still an approximation of real queue position, latency, and venue behavior.
