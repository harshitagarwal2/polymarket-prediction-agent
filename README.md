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

## Architecture at a glance

The current repo is organized around four connected lanes:

1. **Capture workers** append authoritative raw envelopes, checkpoints, and `source_health` into the Postgres-backed capture substrate.
   - `run-sportsbook-capture` owns sportsbook raw ingress.
   - `run-polymarket-capture` owns Polymarket market-channel and user-channel raw ingress.
2. **Projection** replays raw capture lanes into projected current-state tables and compatibility JSON under `runtime/data/current/`.
   - `run-current-projection` is the explicit projector worker.
3. **Deterministic builders** consume the current-state read boundary to build mappings, fair values, executable opportunities, and dataset artifacts.
   - `python -m scripts.ingest_live_data build-mappings`
   - `python -m scripts.ingest_live_data build-fair-values`
   - `python -m scripts.ingest_live_data build-opportunities`
   - `python -m scripts.ingest_live_data build-inference-dataset`
   - `python -m scripts.ingest_live_data build-training-dataset`
4. **Supervised runtime and operator control** run on top of the adapter, runtime policy, safety state, and journaling shell.
   - `run-agent-loop` is the supervised preview/run loop.
   - `operator-cli` is the pause/resume/status/advisory control plane.

When a Postgres DSN marker is resolvable, projected Postgres-backed reads are authoritative. The JSON files under `runtime/data/current/` remain in sync as compatibility exports rather than the primary source of truth.

The authority and sanctioned-entrypoint contract for this wave is captured in [`docs/adr/authority-and-reconciliation.md`](docs/adr/authority-and-reconciliation.md).

## Community and contribution

- Read [`CONTRIBUTING.md`](CONTRIBUTING.md) before opening pull requests.
- Follow [`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md) in issues, reviews, and discussions.
- Use [`SECURITY.md`](SECURITY.md) for vulnerabilities or sensitive reports.
- The project is released under the [`MIT License`](LICENSE).

## Repository map

- `adapters/` - venue wrappers and normalized types
- `contracts/` - contract identity, mapping confidence, and resolution-rule parsing
- `forecasting/` - fair-value engines, calibration, consensus, scoring, dashboards, and ML-facing helpers
- `opportunity/` - executable edge, fillability, and ranking
- `execution/` - deterministic order proposals and quote-management helpers
- `engine/` - polling loop, orchestration, runtime policy, reconciliation, safety state, and runtime bootstrap
- `risk/` - exposure caps, cleanup helpers, deterministic risk checks
- `services/` - dedicated capture workers and current-state projection workers
- `storage/` - current-state adapters, journaling, raw/parquet stores, and Postgres-backed repositories
- `research/` - replay, benchmarking, datasets, and offline model/training flows
- `llm/` - operator-side advisory artifact generation and deterministic evidence rendering
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

For repo maintenance and CI-equivalent local quality gates:

```bash
uv sync --locked --extra dev
pre-commit install
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
- `bootstrap-postgres`
- `run-sportsbook-capture`
- `run-polymarket-capture`
- `run-current-projection`
- `run-replay-attribution`

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

This Docker path proves the benchmark and reproducibility workflow only. It is not the worker, projector, and runtime service stack.

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
- `run-sportsbook-capture` - continuously append sportsbook raw ingress, checkpoints, and source-health rows into the Postgres capture substrate
- `train-models` - write lightweight Elo, Bradley–Terry, or blend artifacts from benchmark cases
- `build-fair-values` - thin wrapper around the existing sports fair-value manifest builder

For the sanctioned supervised current-state path, use the dedicated capture workers first, then the projector, then the sanctioned builder commands:

```bash
python -m scripts.train_models --model consensus --output runtime/consensus_artifact.json

python -m scripts.run_sportsbook_capture \
  --sport basketball_nba \
  --market h2h \
  --event-map-file runtime/odds_event_map.json \
  --root runtime/data

python -m scripts.run_polymarket_capture market \
  --asset-id <asset-id> \
  --root runtime/data \
  --max-sessions 1

python -m scripts.run_current_projection \
  --root runtime/data \
  --max-cycles 1

python -m scripts.ingest_live_data build-mappings \
  --market h2h \
  --root runtime/data

python -m scripts.ingest_live_data build-fair-values \
  --root runtime/data \
  --consensus-artifact runtime/consensus_artifact.json

python -m scripts.ingest_live_data build-opportunities \
  --root runtime/data

python -m scripts.ingest_live_data build-fair-values \
  --root runtime/data \
  --consensus-artifact runtime/consensus_artifact.json \
  --calibration-artifact runtime/calibration_artifact.json

python -m scripts.ingest_live_data build-opportunities \
  --root runtime/data
```

The following `ingest-live-data` commands remain useful as manual or compatibility utilities, but they are not the sanctioned continuous production entrypoints for this wave:

```bash
python -m scripts.ingest_live_data build-inference-dataset --root runtime/data

python -m scripts.ingest_live_data build-training-dataset \
  --input runtime/sports_inputs_labeled.json \
  --polymarket-input runtime/polymarket_markets.json \
  --root runtime/data
```

The retired `ingest-live-data polymarket-markets`, `ingest-live-data sportsbook-odds`, and `ingest-live-data polymarket-bbo` paths are deprecated and should not be revived.

For a continuous sportsbook capture worker that no longer depends on the monolithic end-to-end script loop, run:

```bash
uv sync --extra postgres

# Either export a DSN directly...
export PREDICTION_MARKET_POSTGRES_DSN=postgresql://...

# ...or write one to runtime/data/postgres/postgres.dsn

python -m scripts.run_sportsbook_capture \
  --provider theoddsapi \
  --sport basketball_nba \
  --market h2h \
  --event-map-file runtime/odds_event_map.json \
  --root runtime/data \
  --refresh-interval-seconds 60

python -m scripts.run_sportsbook_capture \
  --provider json_feed \
  --provider-url https://example.com/sportsbook-feed.json \
  --sport basketball_nba \
  --market h2h \
  --root runtime/data \
  --refresh-interval-seconds 60

python -m scripts.run_sportsbook_capture \
  --provider sportsgameodds \
  --event-map-file runtime/odds_event_map.json \
  --sport basketball_nba \
  --market h2h \
  --root runtime/data \
  --refresh-interval-seconds 60

python -m scripts.run_polymarket_capture market \
  --asset-id pm-1 \
  --root runtime/data \
  --max-sessions 1 \
  --max-messages-per-session 1

python -m scripts.run_current_projection \
  --root runtime/data \
  --max-cycles 1
```

Use the dedicated workers for the live capture boundary. The legacy live `polymarket-bbo` path is deprecated and is not the supported production path.

The current ownership split is:

- capture workers own raw ingress, checkpoints, and source-health writes
- `run-current-projection` owns compatibility exports under `runtime/data/current/` for capture-owned tables
- deterministic builders such as `build-mappings`, `build-fair-values`, and opportunity builders own mappings, fair values, and other derived outputs

That split is intentional: the capture/projector substrate materializes the read boundary for deterministic builders and operator-facing preview context, while `run-agent-loop` remains the supervised venue-facing runtime that lists live markets, applies runtime policy, and journals execution decisions.

To replace the manual `runtime/odds_event_map.json` step, the repo now ships `build_event_map_from_schedule_feed.py`, which can build an event map from a schedule/status feed or directly from the public MLB StatsAPI schedule endpoint.

```bash
export SPORTSGAMEODDS_API_KEY=...
```

The `sportsgameodds` provider still needs the same `event_key` / `game_id` enrichment path as other sportsbook sources, so pair it with a generated or maintained `runtime/odds_event_map.json`.

When a Postgres DSN marker is present, either through `PREDICTION_MARKET_POSTGRES_DSN` / `POSTGRES_DSN` / `DATABASE_URL` or a `postgres.dsn` marker file under `runtime/data/postgres`, projected reads are authoritative. The JSON files under `runtime/data/current/` stay as compatibility exports, not the source of truth. The dedicated workers use the Postgres repository layer directly, so they require the optional `postgres` extra and one of those DSN resolution paths.

For a local service-stack smoke path, copy `.env.example` to `.env`, then use the checked-in Compose stack:

```bash
docker compose up -d postgres
docker compose run --rm bootstrap-postgres
docker compose --profile projection run --rm run-current-projection
docker compose down -v
```

That stack is intended to validate the Postgres/bootstrap/projector substrate. The bundled Postgres service is bound to `127.0.0.1` for local-only access, and the sample credentials are development defaults only. It does not by itself prove unattended live trading readiness.

For non-preview runtime config surfaces, the repo now also ships:

- `configs/runtime_policy.staging.json`
- `configs/runtime_policy.production_supervised.json`
- `configs/sports_nba.staging.yaml`
- `configs/sports_nfl.staging.yaml`

The preview configs remain useful for low-risk local preview cycles, but the staging/supervised files are the checked-in direction for production-readiness verification.

That live path keeps sportsbook event identity (`event_key` / `game_id`) in the current-state mapping flow and lets the consensus artifact configure the deterministic fair-value snapshot builder. `build-mappings` still writes the flat selector-facing snapshot to `runtime/data/current/market_mappings.json`, and now also emits a structured sidecar schema at `runtime/data/current/market_mapping_manifest.json` with `mapping_status`, structured `mapping_confidence`, structured `blocked_reason`, identity metadata, and rule semantics for each mapped Polymarket market. If you also pass a calibration artifact, the live snapshot keeps raw `fair_yes_prob` in `runtime/data/current/fair_values.json`, adds sibling `calibrated_fair_yes_prob`, and projects `calibrated_fair_value` plus calibration metadata into `runtime/data/current/fair_value_manifest.json`. `build-inference-dataset` materializes the latest joined inference rows at `runtime/data/processed/inference/joined_inference_dataset.jsonl` and also registers a versioned `joined-inference-dataset` snapshot under `runtime/data/datasets`. `build-training-dataset` does the same for labeled training rows at `runtime/data/processed/training/historical_training_dataset.jsonl` and the `historical-training-dataset` snapshot that `train-models --training-dataset ...` can now consume directly.

The checked-in sample league configs now carry those live defaults too, so the same flow can be driven with fewer flags:

```bash
python -m scripts.run_sportsbook_capture \
  --config-file configs/sports_nba.yaml \
  --root runtime/data \
  --max-cycles 1

python -m scripts.run_current_projection \
  --root runtime/data \
  --max-cycles 1

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

`.github/workflows/python-ci.yml` currently runs a quality job, a Postgres smoke job, a deterministic service-stack smoke job, a Compose substrate smoke job, and a separate reproducibility job on pushes and pull requests. The quality job does eight things:

- checks that `uv.lock` matches the dependency declarations
- installs the package plus dev tooling from the committed lockfile
- compiles the key runtime and research modules with `python -m compileall -q`
- checks formatting with a scoped `ruff format --check` contract for the maintained repo-policy files
- runs the repo-wide serious `ruff check` lint gate
- runs the configured `mypy` contract
- runs the focused **Run advisory and docs contract regressions** unittest step
- runs `python -m unittest discover -s tests -p "test_*.py"` under Coverage.py and uploads `coverage.xml`

The Postgres smoke job exercises the dedicated Postgres-backed worker tests, the deterministic service-stack smoke path validates the scriptable substrate flow, the Compose smoke job validates the local Postgres/bootstrap/projector substrate, and the reproducibility job separately exercises the benchmark and fixture workflows on Ubuntu.

`.github/workflows/security.yml` separately audits the locked dependency graph with `pip-audit` and scans pull-request or push ranges with checksum-verified `gitleaks`.

`.github/workflows/release-artifacts.yml` is intentionally build-only: on version tags it builds Python distributions from the locked environment without isolated backend resolution, builds a Docker image archive, uploads both as GitHub artifacts, and stops short of publishing or deploying anywhere.

## Citation and release metadata

- `CITATION.cff` at the repo root provides software citation metadata for GitHub's **Cite this repository** surface.
- `v0.1.0` is the first intended reproducible software release tag for this benchmark toolkit.
- Zenodo DOI minting still requires Zenodo-side GitHub linking and repository enablement in addition to the repo metadata in this project.

## Read next

- `docs/GETTING_STARTED.md` for the new-engineer setup path
- `docs/ARCHITECTURE.md` for the current system shape
- `docs/architecture/sports_polymarket_architecture.md` for the target sports + Polymarket architecture
- `docs/VERIFICATION_SPORTS_POLYMARKET.md` for the current verification record of the added sports + Polymarket paths
- `docs/PRODUCTION_READINESS.md` for the supervised production-readiness checklist and rollout gate
- `docs/OPERATOR_RUNBOOK.md` for supervised runtime operation
- `docs/BENCHMARK_TOOLKIT.md` for the offline benchmark flow
- `docs/BENCHMARK_PROTOCOL.md` for suite artifacts and evaluation rules
- `docs/BENCHMARK_CASE_SCHEMA.md` for case file structure
