# Sports Benchmark Toolkit

This repo now includes an **offline sports fair-value and replay benchmark/toolkit** built from the existing research primitives.

The goal is to make one small, reproducible public slice easy to run:

1. load normalized sportsbook-style rows and market snapshots,
2. build a de-vigged fair-value manifest,
3. replay a simple strategy over synthetic order-book steps,
4. score the result with deterministic forecast and replay metrics.

## Scope of v1

This toolkit is intentionally narrow:

- **offline only**
- **binary sports markets only**
- **synthetic/public-safe fixtures only**
- **replay and fair-value evaluation only**

It is **not** a packaged live trading bot and it does **not** redistribute proprietary sportsbook odds history.

The replay lane reuses the repo's existing paper-execution model, so it is still a **controlled approximation** of real queue position, latency, and venue behavior rather than a claim of live execution realism.

## Core modules

- `research/fair_values.py` — sportsbook odds parsing, matching, de-vigging, manifest build
- `research/paper.py` — paper broker with slippage and partial-fill realism knobs
- `research/replay.py` — replay runner
- `research/schemas.py` — benchmark case/fixture schema and loaders
- `research/scoring.py` — forecast and replay scoring
- `research/benchmark_runner.py` — orchestrated benchmark execution
- `research/benchmark_cli.py` — package-friendly CLI entry point

## Packaged fixture

The repo ships with one small fixture:

- `research/fixtures/sports_benchmark_tiny.json`

and additional packaged benchmark cases for suite runs:

- `research/fixtures/sports_benchmark_best_line.json`
- `research/fixtures/sports_benchmark_round_trip.json`

It exercises both sides of the toolkit:

- fair-value construction from normalized moneyline rows
- replay of `FairValueBandStrategy` over two deterministic order-book steps

## Benchmark case semantics

`fair_value_case` supports two fields that make the benchmark **fail closed** instead of silently accepting partial resolution:

- `expected_market_keys` — market keys that must be present in the resolved fair-value manifest. If any are missing, the runner raises an explicit benchmark error.
- `outcome_labels` — binary labels keyed by resolved market key. If a labeled key is missing from the manifest, the runner also raises an explicit benchmark error instead of scoring a partial subset.

Use these fields when you want fixture validity to be part of the benchmark contract, not just the reported metrics.

## Quick start

After `pip install -e .`, run either:

```bash
prediction-market-sports-benchmark --fixture sports_benchmark_tiny.json
```

or:

```bash
python3 scripts/run_sports_benchmark.py --fixture sports_benchmark_tiny.json
```

To save the full report and the fair-value manifest separately:

```bash
python3 scripts/run_sports_benchmark.py \
  --fixture sports_benchmark_tiny.json \
  --output runtime/sports_benchmark_report.json \
  --write-manifest runtime/sports_benchmark_manifest.json
```

To run the packaged benchmark suite and write aggregated artifacts:

```bash
python3 scripts/run_sports_benchmark_suite.py --output-dir runtime/benchmark-suite
```

or, after installation:

```bash
prediction-market-sports-benchmark-suite --output-dir runtime/benchmark-suite
```

## Report shape

The benchmark report includes two sections when both lanes are present in the case file:

- `fair_value`
  - row counts
  - resolved market keys
  - skipped-group count
  - generated manifest payload
  - optional forecast metrics: **Brier**, **log loss**, **accuracy**, **ECE**
- `replay`
  - trade counts
  - rejection count
  - ending cash
  - ending portfolio value
  - net PnL
  - return percentage

The suite runner also writes:

- `benchmark_suite_summary.json`
- `benchmark_suite_summary.md`
- `cases/<case-name>.json`

See `docs/BENCHMARK_PROTOCOL.md` and `docs/BENCHMARK_CASE_SCHEMA.md` for the broader protocol and case format.

## What belongs in the public benchmark slice

Public now:

- fair-value math and manifest generation
- replay and paper execution primitives
- benchmark schema, scoring, runner, and fixture-driven CLI

Private/internal for now:

- live venue adapters
- account-truth recovery and live runtime orchestration
- operator control plane and incident-time workflows

## Recommended next extensions

1. add more synthetic/public-safe benchmark cases,
2. add richer calibration and attribution reports,
3. add optional event/context inputs for event-informed fair-value updates,
4. only then consider a separate public repo extraction.
