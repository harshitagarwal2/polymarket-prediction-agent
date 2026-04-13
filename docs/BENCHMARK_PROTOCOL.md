# Benchmark Protocol

This protocol defines the current offline evaluation flow used by the sports benchmark toolkit.

## Scope

The protocol is designed for:

- offline execution only
- public-safe fixtures or local private inputs
- binary fair-value evaluation
- replay comparison with deterministic paper-execution assumptions

It is not a live trading protocol.

## End-to-end flow

1. **Prepare case inputs**
   - normalized sportsbook-style rows
   - optional market snapshots for row-to-market resolution
   - optional replay steps
   - optional binary outcome labels keyed by market key
   - optional model fair values keyed by market key
   - optional model blend weight for sportsbook/model combination
   - optional calibration samples

Walk-forward dataset runs can also generate `model_fair_value` internally with `--model-generator elo`, fitting Elo ratings on prior-window benchmark cases and applying them only to future-window test cases.
2. **Run the fair-value lane**
   - resolve row identity through `event_key`, `condition_id`, or `game_id`
   - aggregate books as `independent` or `best-line`
   - de-vig with `multiplicative` or `power`
   - fail closed if `expected_market_keys` are missing
   - fail closed if `outcome_labels` point at unresolved keys
3. **Apply optional calibration overlay**
   - fit a histogram calibrator from the case's calibration samples
   - report raw and calibrated forecast metrics
   - include calibrated market probabilities in the report payload
4. **Run the replay lane**
   - replay `FairValueBandStrategy` over the case steps
   - simulate fills with `PaperBroker`
   - compare against replay baselines such as `noop_strategy`
5. **Aggregate suite artifacts**
   - write per-case reports
   - write aggregate JSON and Markdown summaries
   - write a suite-level edge ledger across fair-value evaluation rows

## Baselines

The current benchmark layer reports several comparison points.

Fair-value baselines:

- `bookmaker_multiplicative_independent`
- `bookmaker_power_independent`
- `bookmaker_multiplicative_best_line`
- `market_midpoint`
- `model_fair_value`
- `blended_fair_value`

Replay baselines:

- `noop_strategy`

These are research comparisons, not claims of production model quality.

## Running the protocol

### Single case

```bash
uv run --locked --extra research python3 scripts/run_sports_benchmark.py --fixture sports_benchmark_tiny.json
```

### Packaged suite

```bash
uv run --locked --extra research python3 scripts/run_sports_benchmark_suite.py --output-dir runtime/benchmark-suite
```

### Dataset snapshot suite

```bash
uv run --locked --extra research python3 scripts/run_sports_benchmark_suite.py \
  --dataset-root research/datasets \
  --dataset-name benchmark-cases \
  --dataset-version v1 \
  --output-dir runtime/benchmark-suite
```

### Walk-forward dataset suite

```bash
uv run --locked --extra research python3 scripts/run_sports_benchmark_suite.py \
  --dataset-root research/datasets \
  --dataset-name benchmark-cases \
  --dataset-version v1 \
  --walk-forward \
  --min-train-size 10 \
  --test-size 5 \
  --step-size 5 \
  --output-dir runtime/benchmark-suite-walk-forward
```

Installed console entrypoint:

```bash
uv run --locked --extra research prediction-market-sports-benchmark-suite --output-dir runtime/benchmark-suite
```

## Output contract

The suite currently writes:

- `benchmark_suite_summary.json`
- `benchmark_suite_summary.md`
- `benchmark_suite_edge_ledger.json`
- `cases/<safe-case-name>.json`

Walk-forward runs additionally write:

- `walk_forward_benchmark_summary.json`
- `splits/<split-id>/benchmark_suite_summary.json`
- `splits/<split-id>/benchmark_suite_summary.md`
- `splits/<split-id>/benchmark_suite_edge_ledger.json`
- `splits/<split-id>/cases/<safe-case-name>.json`

`walk_forward_benchmark_summary.json` includes:

- dataset provenance
- walk-forward split settings and counts
- a pooled out-of-fold `aggregate` across all split test reports
- split-level calibration provenance and artifact paths

`benchmark_suite_summary.json` has four top-level sections:

- `aggregate`
- `case_results`
- `failures`
- `edge_ledger`

The aggregate section includes:

- fair-value case counts and averages
- replay case counts and averages
- calibrated metric aggregates when calibration is present
- fair-value baseline deltas
- replay baseline deltas
- edge-ledger row count

## Manifest contract

The fair-value artifacts preserve the runtime-oriented manifest shape.

Top-level fields can include:

- `generated_at`
- `source`
- `max_age_seconds`
- `values`
- `skipped_groups`
- `metadata`

Per-value records can include:

- `fair_value`
- optional `model_fair_value`
- optional `blended_fair_value`
- optional `calibrated_fair_value`
- `condition_id`
- `event_key`
- sport metadata
- provenance fields such as source bookmaker and match strategy

This matters because the benchmark path can produce artifacts that the runtime path can read without inventing a separate format.

## Dataset and walk-forward use

For repeated research work, you can snapshot inputs and case collections with `DatasetRegistry` in `research/datasets.py`.

That gives you:

- versioned row snapshots
- versioned benchmark-case snapshots
- manifest metadata per snapshot
- chronological walk-forward splits through `generate_walk_forward_splits(...)`

The suite CLI can now consume benchmark-case snapshots directly, and in walk-forward mode it fits calibration only from prior-window training edge-ledger rows before applying that prefit calibrator to future-window test cases. The root walk-forward summary pools the split test reports into one out-of-fold aggregate so the top-level metrics reflect evaluated future windows, not just split counts.

This protocol does not require dataset snapshots, but it supports them cleanly.

## Interpretation caveats

- Replay metrics are relative offline signals, not live-fill predictions.
- Calibration overlays are only as good as the samples that fit them.
- The benchmark path is sports-focused and binary-market-only today.
- The protocol does not imply Polymarket and Kalshi parity.
