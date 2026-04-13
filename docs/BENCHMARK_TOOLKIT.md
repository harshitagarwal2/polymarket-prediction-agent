# Sports Benchmark Toolkit

This repo includes an offline sports fair-value and replay benchmark toolkit built from the research layer that also feeds the Polymarket runtime workflow.

## What it is

The toolkit is a reproducible offline slice of the repo.

It lets you:

1. load normalized sportsbook-style rows
2. resolve those rows to market identity when market snapshots are present
3. de-vig binary books into fair-value manifests
4. optionally fit and apply a calibration overlay
5. replay a simple strategy with the paper execution model
6. score forecasts and replay results, then aggregate suite artifacts

## What it is not

- not live trading
- not a queue-accurate venue simulator
- not a distributor of proprietary sportsbook history
- not a claim that replay results equal live execution results

Replay uses the repo's paper broker, which models slippage, fill caps, resting-order delay, and reserved capital, but it is still approximate.

## Core modules

- `research/fair_values.py` - sportsbook row parsing, market matching, de-vig, manifest build
- `research/calibration.py` - histogram calibration artifacts and fitting
- `research/paper.py` - paper broker and fill realism knobs
- `research/replay.py` - replay runner
- `research/scoring.py` - forecast and replay scoring
- `research/benchmark_runner.py` - single benchmark-case execution
- `research/benchmark_suite.py` - multi-case suite aggregation and edge ledger
- `research/datasets.py` - versioned dataset snapshots and walk-forward splits
- `research/benchmark_cli.py` - single-case CLI
- `research/benchmark_suite_cli.py` - suite CLI

## Packaged fixtures and example artifacts

Packaged fixtures in `research/fixtures/`:

- `sports_benchmark_tiny.json`
- `sports_benchmark_best_line.json`
- `sports_benchmark_round_trip.json`

Checked-in example suite output:

- `runtime/benchmark-suite-e2e-check/benchmark_suite_summary.json`
- `runtime/benchmark-suite-e2e-check/benchmark_suite_summary.md`
- `runtime/benchmark-suite-e2e-check/cases/`

## Quick start

After `uv sync --locked --extra research`, run either form.

If you do not activate `.venv`, prefix direct console entrypoints with `uv run --locked --extra research ...`.

Canonical local reproduction path:

```bash
make sync-research
make reproduce
```

Containerized reproduction path:

```bash
docker build -t polymarket-prediction-agent:v0.1.0 .
docker run --rm polymarket-prediction-agent:v0.1.0
```

Single packaged fixture:

```bash
uv run --locked --extra research prediction-market-sports-benchmark --fixture sports_benchmark_tiny.json
```

Equivalent script form:

```bash
uv run --locked --extra research python3 scripts/run_sports_benchmark.py --fixture sports_benchmark_tiny.json
```

Write both report and manifest:

```bash
uv run --locked --extra research python3 scripts/run_sports_benchmark.py \
  --fixture sports_benchmark_tiny.json \
  --output runtime/sports_benchmark_report.json \
  --write-manifest runtime/sports_benchmark_manifest.json
```

Run the packaged suite:

```bash
uv run --locked --extra research prediction-market-sports-benchmark-suite --output-dir runtime/benchmark-suite
```

## Fair-value lane

The fair-value lane currently supports:

- binary groups only
- `multiplicative` and `power` de-vig methods
- `independent` and `best-line` bookmaker aggregation
- fail-closed expected market-key checks
- fail-closed labeled-outcome checks
- optional `model_fair_values` inputs for model-only fair-value baselines
- optional `model_blend_weight` for logit-combined blended fair values
- optional calibration samples that produce calibrated probabilities and before-versus-after metrics

Walk-forward suites also support `--model-generator elo`, which fits Elo ratings from prior benchmark cases in each training window and injects generated `model_fair_value` probabilities into future-window test cases.

When calibration is present, the benchmark report includes:

- raw forecast metrics
- calibrated forecast metrics
- calibration artifact payload
- calibrated market probabilities
- per-market raw and calibrated evaluation rows

When model inputs are present, the fair-value report can also include:

- `model_fair_value` and `blended_fair_value` baseline scores
- per-market `model_fair_value` / `blended_fair_value` values in the manifest payload
- per-market model/blended evaluation columns in the fair-value report and suite edge ledger

That same calibrated value can be carried into runtime manifests through `calibrated_fair_value`, while preserving the original raw `fair_value`.

## Replay lane

The replay lane runs `FairValueBandStrategy` over benchmark steps and uses `PaperBroker` for simulated execution.

Current paper realism knobs in `PaperExecutionConfig`:

- `max_fill_ratio_per_step`
- `slippage_bps`
- `resting_max_fill_ratio_per_step`
- `resting_fill_delay_steps`

Those knobs make replay more useful for relative comparison, but not production-safe on their own.

## Report and suite artifacts

Single-case reports can include two sections:

- `fair_value`
- `replay`

The fair-value section can include:

- row counts
- resolved market keys
- missing market keys
- skipped-group count
- manifest payload
- raw forecast metrics
- calibrated forecast metrics, when calibration exists
- per-market `evaluation_rows`
- fair-value baseline comparisons

The replay section can include:

- replay score
- ending positions
- mark prices
- ending cash
- ending portfolio value
- net PnL
- replay baselines such as `noop_strategy`

Suite output writes:

- `benchmark_suite_summary.json`
- `benchmark_suite_summary.md`
- `benchmark_suite_edge_ledger.json`
- `cases/<safe-case-name>.json`

The edge ledger is useful for later attribution, calibration fitting, and error inspection across cases.

## Dataset snapshots and walk-forward splits

The benchmark toolkit is not limited to hand-authored fixtures.

`research/datasets.py` supports:

- `rows_jsonl` snapshots for dated row collections
- `benchmark_cases` snapshots for case collections
- manifest files per snapshot version
- chronological walk-forward split generation

This lets you turn ad hoc benchmark work into versioned local research artifacts.

## Connection to the runtime path

The benchmark toolkit is offline, but it is not isolated from the runtime design.

- the fair-value manifest format matches the runtime manifest contract
- manifests can carry both raw and calibrated values
- runtime policy can choose which field, `raw` or `calibrated`, to read
- suite edge-ledger rows can seed later calibration work

## Honest limitations

- sports-focused benchmark scope only
- binary markets only in the fair-value builder
- approximate replay fills
- no claim of Polymarket live-fill realism
- no claim of Kalshi parity in this benchmark path

For the broader evaluation protocol and case format, see `docs/BENCHMARK_PROTOCOL.md` and `docs/BENCHMARK_CASE_SCHEMA.md`.
