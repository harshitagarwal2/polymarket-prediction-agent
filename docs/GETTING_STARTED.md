# Getting Started

This guide is for a new engineer who wants to understand what this repo does today and start using the supported paths without guessing.

## Start with the right mental model

This repo is not just a pile of upstream references anymore.

It currently provides:

- a supervised Polymarket runtime in `adapters/`, `engine/`, `risk/`, and `scripts/`
- a dedicated capture and projection substrate in `services/` and `storage/`
- deterministic contract, forecasting, opportunity, and execution layers in `contracts/`, `forecasting/`, `opportunity/`, and `execution/`
- an offline sports fair-value and replay benchmark toolkit in `research/`
- runtime state persistence, operator controls, and JSONL journaling
- a schema-validated runtime policy file for repeatable runtime behavior
- a local dataset registry and walk-forward split helpers for research snapshots

The main supported venue path is Polymarket. Kalshi support exists behind the same interface, but it is thinner and should be treated as scaffolding, not parity.

There is also a reusable `forecasting/` package for domain-agnostic calibration, scoring, model-vs-market dashboard generation, and non-sports pipeline scaffolding. See `docs/FORECASTING_PLATFORM.md` for that surface.

## Current architecture in one page

The easiest way to reason about the repo today is to keep these lanes separate:

1. **Capture workers** append raw ingress plus checkpoints and `source_health` into the Postgres-backed capture substrate.
   - `run-sportsbook-capture`
   - `run-polymarket-capture`
2. **Projection** replays those raw lanes into projected current-state tables and compatibility JSON under `runtime/data/current/`.
   - `run-current-projection`
3. **Deterministic builders** consume the current-state read boundary to materialize mappings, fair values, opportunities, and datasets.
   - `python -m scripts.ingest_live_data build-mappings`
   - `python -m scripts.ingest_live_data build-fair-values`
   - `python -m scripts.ingest_live_data build-opportunities`
   - `python -m scripts.ingest_live_data build-inference-dataset`
   - `python -m scripts.ingest_live_data build-training-dataset`
4. **Supervised runtime** still runs through `run-agent-loop`, the venue adapters, runtime policy, reconciliation, and risk gates.
5. **Operator control and review** happen through `operator-cli`, `runtime/safety-state.json`, `runtime/events.jsonl`, and optional sidecar artifacts such as `runtime/data/current/llm_advisory.json`.

When a Postgres DSN marker exists, projected Postgres-backed reads are authoritative. `runtime/data/current/*.json` stays useful, but it is a compatibility export rather than the primary authority boundary.

See [`docs/adr/authority-and-reconciliation.md`](adr/authority-and-reconciliation.md) for the authority and sanctioned-entrypoint contract.

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
- `run-sportsbook-capture`
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

That Docker path proves the benchmark and reproducibility workflow only. It is not the supported live worker stack.

For the supervised service-stack substrate, the repo now also ships `.env.example` plus `docker-compose.yml`. The smallest local infrastructure smoke path is:

```bash
cp .env.example .env
docker compose up -d postgres
docker compose run --rm bootstrap-postgres
docker compose --profile projection run --rm run-current-projection
docker compose down -v
```

That Compose path validates the Postgres/bootstrap/projector substrate and DSN marker wiring. The bundled Postgres service is bound to `127.0.0.1` for local-only access, and the checked-in credentials are development defaults only. It does not replace the offline benchmark path above, and it is not by itself evidence that the full live worker/runtime stack is production-ready.

For repo maintenance and CI-equivalent local quality gates, also install the dev tooling and hooks:

```bash
uv sync --locked --extra dev
pre-commit install
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

For the sanctioned live/current-state path, use the dedicated capture workers first, then projection, then the sanctioned builder commands. The intended sequence is:

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

The following `ingest-live-data` commands remain available as manual/offline compatibility utilities, but they are not the sanctioned continuous production entrypoints for this wave:

```bash
python -m scripts.ingest_live_data build-inference-dataset --root runtime/data

python -m scripts.ingest_live_data build-training-dataset \
  --input runtime/sports_inputs_labeled.json \
  --polymarket-input runtime/polymarket_markets.json \
  --root runtime/data
```

The retired `ingest-live-data polymarket-markets`, `ingest-live-data sportsbook-odds`, and `ingest-live-data polymarket-bbo` paths are deprecated and should stay retired.

If you want continuous sportsbook polling without keeping the whole end-to-end ingest script in the loop, run the dedicated capture worker instead:

Equivalent console entrypoint: `run-sportsbook-capture`

```bash
uv sync --extra postgres

# Export a DSN or write one to <runtime_root>/postgres/postgres.dsn first.
export PREDICTION_MARKET_POSTGRES_DSN=postgresql://...

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
```

That command owns raw sportsbook ingress only. It appends sportsbook quote events, checkpoints, and `source_health` rows through the Postgres capture store, and it does not own selector-facing `runtime/data/current/*.json`. `run_current_projection` owns the compatibility exports for capture-owned tables, and when a Postgres DSN marker is present the projected Postgres-backed reads are authoritative. Because the worker uses the Postgres repository layer directly, it needs the optional `postgres` dependency set plus a resolvable DSN from `PREDICTION_MARKET_POSTGRES_DSN` / `POSTGRES_DSN` / `DATABASE_URL` or a `postgres.dsn` marker file under the configured runtime root.

If you want to stop maintaining `runtime/odds_event_map.json` by hand, use the new schedule-feed helper:

```bash
export SPORTSGAMEODDS_API_KEY=...
```

The `sportsgameodds` source still depends on `event_key` / `game_id` enrichment for downstream mapping and fair-value builders, so use the helper (or another trusted identity feed) to keep `runtime/odds_event_map.json` current.

```bash
python -m scripts.build_event_map_from_schedule_feed \
  --provider file \
  --schedule-file runtime/schedule_feed.json \
  --sport nba \
  --series playoffs \
  --output runtime/odds_event_map.json
```

For MLB, the same helper can call the public StatsAPI directly:

```bash
python -m scripts.build_event_map_from_schedule_feed \
  --provider mlb-statsapi \
  --date 2026-04-24 \
  --output runtime/odds_event_map.json
```

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

`run_polymarket_capture` appends Polymarket market/user channel events into the Postgres-backed capture substrate and keeps `source_health` current for the dedicated capture lanes. In this wave, `run_current_projection` now projects the capture-owned market catalog, market-channel, and account-truth lanes back into `runtime/data/current/*.json` compatibility snapshots plus the projected current-state tables used by runtime readers.

For live Polymarket capture, use `run_polymarket_capture`. The legacy live `polymarket-bbo` path is deprecated and is not the supported production path.

The `--event-map-file` input enriches live sportsbook events with stable identity fields such as `event_key` and `game_id`. `build-mappings` now fails closed if that upstream identity is missing, keeps the flat runtime selector snapshot in `runtime/data/current/market_mappings.json`, and also emits a structured sidecar schema at `runtime/data/current/market_mapping_manifest.json` with `mapping_status`, structured `mapping_confidence`, structured `blocked_reason`, event identity, and rule-semantics details for each mapping decision. `build-fair-values --consensus-artifact ...` uses the consensus artifact as deterministic inference configuration for the current-state fair-value snapshot builder, and the optional `--calibration-artifact ...` overlay adds sibling `calibrated_fair_yes_prob` / `calibrated_fair_value` outputs without changing the raw baseline fields. `build-inference-dataset` then writes the latest joined inference rows to `runtime/data/processed/inference/joined_inference_dataset.jsonl` and registers a versioned `joined-inference-dataset` snapshot. `build-training-dataset` writes `runtime/data/processed/training/historical_training_dataset.jsonl`, registers a versioned `historical-training-dataset` snapshot, and enables `train-models --training-dataset historical-training-dataset --dataset-root runtime/data/datasets` for downstream model fitting.

The checked-in sample configs now include `capture.sport_key`, `runtime.sportsbook_market`, `runtime.event_map_file`, `runtime.consensus_artifact`, and an optional `runtime.calibration_artifact` key, so the live/current-state flow can also be driven from config defaults:

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

If the config also points `runtime.calibration_artifact` at a histogram calibration payload, the same command writes raw live fair values to `runtime/data/current/fair_values.json`, includes `calibrated_fair_yes_prob` beside them, and emits `metadata.calibration` in `runtime/data/current/fair_value_manifest.json` for runtime policy selection.

The ownership split for the live/current-state path is simple:

- capture workers own raw ingress, checkpoints, and source-health
- projector owns compatibility current-state exports for capture-owned tables
- deterministic builders own mappings, fair values, opportunities, and dataset artifacts

`run-agent-loop` then sits beside that substrate rather than inside it: it still lists live venue markets, applies the runtime policy gate stack, and journals supervised execution decisions, while the current-state pipeline powers deterministic builders, operator preview context, and advisory flows.

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

For non-preview configuration surfaces, use the new staging/supervised artifacts instead:

- `configs/runtime_policy.staging.json`
- `configs/runtime_policy.production_supervised.json`
- `configs/sports_nba.staging.yaml`
- `configs/sports_nfl.staging.yaml`

The sample config also carries the normal preview-loop defaults for `max_fair_value_age_seconds`, `interval_seconds`, and `max_cycles`, so you only need extra CLI flags when you want to override them.

For Polymarket, the runtime now also derives live user-stream condition IDs from that configured fair-value manifest by default. The dedicated `run-polymarket-capture user` worker follows the same contract in supervised/live flows: it derives condition IDs from projected fair-value coverage plus current market metadata when Postgres authority is present, and only needs `POLYMARKET_LIVE_USER_MARKETS` when you want to override the subscription set manually.

Serious Polymarket live flows are now also expected to carry:

- one of `POLYMARKET_PRIVATE_KEY`, `POLYMARKET_PRIVATE_KEY_FILE`, or `POLYMARKET_PRIVATE_KEY_COMMAND`
- `POLYMARKET_ROUTE_LABEL`
- `POLYMARKET_GEO_COMPLIANCE_ACK=true`

Those variables make the active route and compliance posture explicit instead of leaving private/proxy routing as an invisible assumption.

If you want a simple process-local compliance throttle on live HTTP calls, set:

```bash
export PREDICTION_MARKET_HTTP_MIN_INTERVAL_SECONDS=0.25
```

That env var applies a per-host minimum interval inside `engine.http_client.get_json`, which is used by the shared HTTP paths for sportsbook feeds and Polymarket HTTP surfaces.

If you already have an external secret helper on the machine, you can inject the key through:

```bash
export POLYMARKET_PRIVATE_KEY_COMMAND="python -c \"print('your-private-key')\""
```

If you want serious live modes to require private order flow as well, set:

```bash
export POLYMARKET_PRIVATE_ORDER_FLOW_REQUIRED=true
export POLYMARKET_CLOB_HOST=https://private-clob.example.invalid
```

For a long-running supervised preview process, add `--interval-seconds` and a larger `--max-cycles`.

To exercise the supervised-live account-truth contract locally, run:

```bash
make smoke-supervised-live-account-truth
```

That gate runs the focused runtime/bootstrap/projection/Polymarket capture suites that protect the user-channel -> projection -> runtime truth path.

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

To build and dry-run the webhook alerting baseline from the machine-readable status payload, run:

```bash
operator-cli status \
  --state-file runtime/safety-state.json \
  --output runtime/data/current/runtime_status.json

operator-cli build-alerts \
  --runtime-status-file runtime/data/current/runtime_status.json \
  --output runtime/data/current/runtime_alerts.json

operator-cli send-alerts \
  --alerts-file runtime/data/current/runtime_alerts.json \
  --webhook-url https://example.invalid/hooks/runtime \
  --dry-run
```

For the deterministic baseline smoke gate, run:

```bash
make smoke-alerting
```

To exercise the reverse-alerting heartbeat baseline, run:

```bash
operator-cli build-heartbeat \
  --runtime-status-file runtime/data/current/runtime_status.json \
  --output runtime/data/current/runtime_heartbeat.json

operator-cli send-heartbeat \
  --heartbeat-file runtime/data/current/runtime_heartbeat.json \
  --webhook-url https://example.invalid/heartbeat \
  --dry-run

make smoke-heartbeat
```

To export a baseline tax/audit CSV from authoritative projected fill rows, run:

```bash
operator-cli export-tax-audit \
  --opportunity-root runtime/data \
  --output runtime/data/current/tax_audit.csv

make smoke-tax-audit
```

To build a model-drift report from a benchmark JSON and exercise the fail-closed baseline, run:

```bash
operator-cli build-model-drift \
  --benchmark-report-file runtime/benchmark_report.json \
  --output runtime/data/current/model_drift.json \
  --max-brier-score 0.20 \
  --max-expected-calibration-error 0.10

make smoke-model-drift
```

To run the current unattended guardrail baselines together, use:

```bash
make smoke-unattended-guardrails
```

If you want serious live modes to hold when the latest report is unhealthy, pass the report back into the loop:

```bash
run-agent-loop \
  --venue polymarket \
  --mode run \
  --opportunity-root runtime/data \
  --drift-report-file runtime/data/current/model_drift.json
```

If you want the loop to treat itself as explicitly autonomous rather than just supervised live, add:

```bash
run-agent-loop \
  --venue polymarket \
  --mode run \
  --opportunity-root runtime/data \
  --policy-file runtime/policy.json \
  --execution-lock-name primary-loop \
  --drift-report-file runtime/data/current/model_drift.json \
  --autonomous-mode
```

You can also set the same posture in the runtime policy under `trading_engine.autonomous_mode = true`.

If you want the loop to treat itself as explicitly autonomous rather than just supervised live, add:

```bash
run-agent-loop \
  --venue polymarket \
  --mode run \
  --opportunity-root runtime/data \
  --policy-file runtime/policy.json \
  --execution-lock-name primary-loop \
  --drift-report-file runtime/data/current/model_drift.json \
  --autonomous-mode
```

That flag is intentionally fail-closed: the loop refuses to start in autonomous mode unless the required guardrail contract is already in place.

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

GitHub Actions currently checks that `uv.lock` matches the dependency declarations, installs the package plus dev tooling from the lockfile, compiles key modules with `python -m compileall -q`, checks formatting for the maintained repo-policy files with a scoped `ruff format --check`, runs the repo-wide serious `ruff check` gate, runs the configured `mypy` contract, runs the focused **Run advisory and docs contract regressions** unittest step, runs `python -m unittest discover -s tests -p "test_*.py"` under Coverage.py, runs deterministic service-stack and Compose substrate smoke paths, and also runs the separate reproducibility job.

The separate security workflow audits the locked dependency graph with `pip-audit` and scans changed commit ranges with checksum-verified `gitleaks`.

For a local approximation of the non-provider-specific repo gates, use:

```bash
make check
make coverage
make audit
make smoke-service-stack
make smoke-compose
```

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
