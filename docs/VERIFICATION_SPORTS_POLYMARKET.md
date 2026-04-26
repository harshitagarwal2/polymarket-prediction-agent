# Sports + Polymarket Verification Record

This file records the latest local verification evidence for the added sports + Polymarket architecture paths.

Machine-readable companion artifact: `docs/verification_sports_polymarket.json`

## Focused automated checks

Command sequence:

```bash
set -euo pipefail
for pattern in \
  test_polymarket_capture_worker.py \
  test_current_projection_worker.py \
  test_ingest_live_data.py \
  test_postgres_storage.py \
  test_sportsbook_capture_worker.py \
  test_sportsbook_json_feed_provider.py \
  test_docs_sync.py \
  test_console_script_entrypoints.py
do
  uv run --locked --extra research --extra postgres --extra polymarket \
    python -m unittest discover -s tests -p "$pattern"
done
```

Observed result:

```text
Aggregate across the discovery sequence:

Ran 97 tests in 2.032s

OK (skipped=1)
```

## Full unit suite

Command:

```bash
uv run --locked --extra dev --extra postgres --extra polymarket python -m unittest discover -s tests -p "test_*.py"
```

Observed result:

```text
 Ran 785 tests in 4.936s

OK (skipped=1)
```

## Release-gate check — lockfile

Command:

```bash
uv lock --check
```

Observed behavior:

- `uv.lock` matches the current dependency declarations

## Release-gate check — supervised runtime status payload

Command:

```bash
uv run --locked --extra postgres --extra polymarket operator-cli status --state-file runtime/safety-state.json --quiet
```

Observed behavior:

- command exits `0`
- runtime safety payload is machine-readable JSON
- supervised runtime reports a healthy, non-halted baseline when no live state is active

## Automated / CI-equivalent smoke — runtime image build

Command:

```bash
docker build -t prediction-market-agent .
```

Observed behavior:

- the runtime-capable image builds successfully with the checked-in extras and scripts

## Automated / CI-equivalent smoke — ledger-backed tax audit baseline

Command:

```bash
make smoke-tax-audit
```

Observed behavior:

- the smoke provisions Postgres authority through the compose network when no DSN is supplied
- the smoke seeds `runtime_cycles`, an accepted `execution_orders` row, and projected `polymarket_fills`
- `operator-cli export-tax-audit` preflights ledger authority, syncs projected fills onto accepted execution-order rows, and exports the resulting `execution_fills` ledger view

## Automated / CI-equivalent smoke — deterministic service stack

Command:

```bash
make smoke-service-stack
```

Observed behavior:

- sportsbook capture raw ingress succeeds against a deterministic local source
- Polymarket market snapshot + BBO seed the same Postgres authority path used by the projector
- Polymarket user-channel/account snapshot ingress seeds projected `polymarket_orders`, `polymarket_fills`, `polymarket_positions`, and `polymarket_balance`
- `build-mappings`, `build-fair-values`, and `build-opportunities` run against the projected current state
- the real `run-agent-loop --mode preview` entrypoint completes against the deterministic smoke root
- projected runtime preview reads back proposals/blocked state from projected authority

## Automated / CI-equivalent smoke — supervised account-truth baseline

Command:

```bash
make smoke-supervised-live-account-truth
```

Observed behavior:

- runtime bootstrap reads projected Postgres current-state authority in serious modes
- supervised `run-agent-loop` and current projection worker keep the account-truth path green under the targeted contract tests
- Polymarket capture worker tests still validate the account/user-channel ingestion boundary used by the projector

## Automated / CI-equivalent smoke — unattended guardrail bundle

Command:

```bash
make smoke-unattended-guardrails
```

Observed behavior:

- alerting, heartbeat, tax-audit, model-drift, supervised account-truth, continuous builder, multi-provider sportsbook, and Polymarket depth/trades baselines all remain executable from the checked-in branch state

## Automated / CI-equivalent smoke — compose bootstrap and projector ordering

Command sequence:

```bash
docker compose config
docker compose up -d postgres
docker compose run --rm bootstrap-postgres
docker compose --profile projection run --rm run-current-projection
docker compose down -v
```

Observed behavior:

- Compose config resolves successfully
- Postgres becomes healthy before bootstrap runs
- bootstrap applies migrations and writes the DSN marker successfully
- projector exits `0` and emits the expected lane summary payload

## Manual QA — config-driven fair-value build

Command shape:

```bash
build-fair-values \
  --input <rows.json> \
  --output <manifest.json> \
  --config-file configs/sports_nba.yaml
```

Observed output excerpt:

```json
{
  "book_aggregation": "best-line",
  "source": "sportsbook-devig:multiplicative:best-line"
}
```

## Manual QA — dedicated sportsbook capture worker

Equivalent console entrypoint: `run-sportsbook-capture`

Command shape:

```bash
uv sync --extra postgres
export PREDICTION_MARKET_POSTGRES_DSN=postgresql://...

python -m scripts.run_sportsbook_capture \
  --provider theoddsapi \
  --sport basketball_nba \
  --market h2h \
  --event-map-file <odds_event_map.json> \
  --root <runtime_root> \
  --max-cycles 1

python -m scripts.run_sportsbook_capture \
  --provider json_feed \
  --provider-url <sportsbook_feed_url> \
  --sport basketball_nba \
  --market h2h \
  --root <runtime_root> \
  --max-cycles 1
```

Observed behavior:

- continuous sportsbook capture no longer has to run through the monolithic `ingest_live_data` orchestration path
- each cycle appends authoritative sportsbook raw envelopes and capture checkpoints into Postgres-backed storage
- the dedicated worker itself stays raw-ingress-only; projector replay owns materialized `sportsbook_odds` rows for the dedicated worker path
- each normalized quote row preserves bookmaker-facing `source`, upstream `provider`, `source_ts`, `capture_ts`, and `source_age_ms`
- `source_health` is written through the relational `source_health` / `source_health_events` tables reached through the configured Postgres DSN
- selector-facing `runtime/data/current/*.json` is left to the dedicated projector worker rather than the capture worker

## Manual QA — dedicated Postgres projector worker

Equivalent console entrypoint: `run-current-projection`

Command shape:

```bash
python -m scripts.run_current_projection \
  --root <runtime_root> \
  --max-cycles 1
```

Observed behavior:

- the projector reads raw rows from `raw_capture_events` using projection checkpoints stored in `capture_checkpoints`
- sportsbook capture rows are replayed into authoritative Postgres history/current tables before compatibility JSON is refreshed
- Polymarket market-catalog and market-channel rows are replayed into authoritative Postgres history/current tables before compatibility JSON is refreshed
- `current/source_health.json` now includes projector lane health such as `projection_sportsbook_odds` and `projection_polymarket_market_catalog`
- when a Postgres DSN marker exists, runtime/ingest readers consume the projected adapter first and no longer treat stale `current/*.json` files as authoritative

## Manual QA — sanctioned current-state fair-value build with consensus artifact

Command shape:

```bash
python -m scripts.train_models --model consensus --output <consensus_artifact.json>
python -m scripts.run_sportsbook_capture --sport basketball_nba --market h2h --event-map-file <odds_event_map.json> --root <runtime_root> --max-cycles 1
python -m scripts.run_polymarket_capture market --asset-id <asset-id> --root <runtime_root> --max-sessions 1
python -m scripts.run_current_projection --root <runtime_root> --max-cycles 1
python -m scripts.ingest_live_data build-mappings --market h2h --root <runtime_root>
python -m scripts.ingest_live_data build-fair-values --root <runtime_root> --consensus-artifact <consensus_artifact.json>
python -m scripts.ingest_live_data build-opportunities --root <runtime_root>
python -m scripts.ingest_live_data build-inference-dataset --root <runtime_root>
```

Observed behavior:

- the sanctioned path starts with the dedicated sportsbook and Polymarket capture workers plus `run-current-projection`
- `run-sportsbook-capture` remains raw-ingress-only and leaves selector-facing `runtime/data/current/*.json` ownership to the projector
- `build-mappings` blocks rows missing upstream `event_key` / `game_id`
- `build-mappings` keeps the flat current-state selector rows in `runtime/data/current/market_mappings.json` and also writes `runtime/data/current/market_mapping_manifest.json` with structured `mapping_status`, `mapping_confidence`, `blocked_reason`, identity, and rule-semantics payloads
- `build-fair-values --consensus-artifact ...` changes live fair-value output based on artifact half-life and writes current-state fair values plus `source_health`
- `build-opportunities` materializes ranked executable opportunity rows from the same projected mapping, fair-value, and BBO boundary
- a failing fair-value build marks `source_health["fair_values"]` red without partially overwriting the prior fair-values tables
- `build-inference-dataset` writes `processed/inference/joined_inference_dataset.jsonl`, registers `joined-inference-dataset`, and keeps `source_health["joined_inference_dataset"]` in sync with the latest row count
- the legacy `ingest-live-data polymarket-markets`, `sportsbook-odds`, and `polymarket-bbo` live ingress paths stay retired in favor of the dedicated workers plus projector chain

## Manual QA — live current-state fair-value build with optional calibration artifact

Command shape:

```bash
python -m scripts.ingest_live_data build-fair-values \
  --root <runtime_root> \
  --consensus-artifact <consensus_artifact.json> \
  --calibration-artifact <calibration_artifact.json>
```

Observed behavior:

- the raw live snapshot still writes `fair_yes_prob` to `runtime/data/current/fair_values.json`
- the calibrated overlay adds sibling `calibrated_fair_yes_prob` to the same current-state rows
- `runtime/data/current/fair_value_manifest.json` keeps raw `fair_value` and adds sibling `calibrated_fair_value`
- the runtime manifest includes `metadata.calibration` with histogram bin and sample counts
- `source_health["fair_values"]["details"]` records whether a calibration artifact was configured

This remains a deterministic builder step on top of the sanctioned capture -> projector -> builder chain above. It is not the standalone live ingress boundary.

## Manual QA — materialized historical training dataset and dataset-backed model training

Command shape:

```bash
python -m scripts.ingest_live_data build-training-dataset \
  --input <sports_inputs_labeled.json> \
  --polymarket-input <polymarket_markets.json> \
  --root <runtime_root>

python -m scripts.train_models \
  --model elo \
  --training-dataset historical-training-dataset \
  --dataset-root <runtime_root>/datasets \
  --output <elo_artifact.json>
```

Observed behavior:

- `build-training-dataset` writes `processed/training/historical_training_dataset.jsonl`
- the same command registers a versioned `historical-training-dataset` snapshot under `<runtime_root>/datasets`
- the same materialization wave also keeps the versioned `historical-resolution-truth-dataset` snapshot explicit for downstream inspection
- the materialized training rows keep a fixed schema even when optional market linkage fields are null
- `train-models --training-dataset ...` consumes that snapshot without requiring the original capture-envelope JSON

## Automated verification — config-driven runtime defaults

Committed test path:

```text
tests.test_run_agent_loop.RunAgentLoopTests.test_main_can_apply_runtime_defaults_from_config_file
```

What it verifies:

- a config file can set `runtime.policy_file`
- a config file can override the CLI default preview mode by setting `runtime.preview_only: false`
- the configured path still completes successfully in the test harness

## Automated verification — capture, projection, truth, and replay surfaces

Committed test coverage now also includes:

- `tests.test_sportsbook_capture_worker` and `tests.test_sportsbook_json_feed_provider`
  - proves raw sportsbook capture, checkpointing, and second-provider (`json_feed`) support
- `tests.test_polymarket_capture_worker`
  - proves dedicated Polymarket capture worker behavior and the retired legacy live BBO path
- `tests.test_current_projection_worker` and `tests.test_current_state_projectors`
  - prove capture-owned compatibility exports are written by projector replay and preserve projected identity fields
- `tests.test_ingest_live_data` and `tests.test_research_architecture_scaffolding`
  - prove `historical-resolution-truth-dataset` is materialized and unresolved truth remains explicit and queryable
- `tests.test_replay_attribution_cli.ReplayAttributionCliTests.test_cli_can_materialize_replay_execution_label_dataset`
  - proves replay execution label dataset materialization, including slippage/fillability fields
- `tests.test_operator_controls.OperatorControlTests.test_sync_quote_places_via_execution_shell`
  and `tests.test_operator_controls.OperatorControlTests.test_sync_quote_can_cancel_existing_order`
  - prove the supervised `operator-cli sync-quote` entrypoint exercises the execution shell end to end

## Manual QA — dedicated Polymarket capture worker

Equivalent console entrypoint: `run-polymarket-capture`

Command shape:

```bash
uv sync --extra postgres --extra polymarket
export PREDICTION_MARKET_POSTGRES_DSN=postgresql://...

python -m scripts.run_polymarket_capture market \
  --asset-id <asset-id> \
  --root <runtime_root> \
  --max-sessions 1
```

Observed behavior:

- the dedicated Polymarket capture worker no longer needs to run through the end-to-end ingest CLI
- capture failures return a non-zero exit code plus a sanitized JSON payload instead of leaking DSN details
- successful market sessions append raw market envelopes plus checkpoint/source-health updates through the Postgres-backed capture stores
- selector-facing `runtime/data/current/*.json` compatibility snapshots remain owned by `run-current-projection`, not the capture worker
- the supported live capture path is `run-polymarket-capture`, and the legacy live `polymarket-bbo` subcommand is retired

## Manual QA — live Gamma capture

Equivalent console entrypoint: `ingest-live-data --layer gamma`

Command shape:

```bash
ingest-live-data \
  --layer gamma \
  --config-file configs/sports_nba.yaml \
  --output <gamma.json> \
  --limit 2
```

Observed command output excerpt:

```json
{
  "layer": "gamma",
  "output": ".../gamma.json"
}
```

Observed capture artifact excerpt after reading the written file:

```json
{
  "layer": "gamma",
  "markets_count": 2
}
```

## Manual QA — training from captured rows

Command shape:

```bash
train-models \
  --config-file configs/sports_nba.yaml \
  --training-data <sports_inputs_labeled.json> \
  --output <elo.json>
```

Observed output excerpt:

```json
{
  "model": "elo",
  "output": ".../elo.json"
}
```

Artifact excerpt:

```json
{
  "model_generator": "elo",
  "training_match_count": 1
}
```

Observed BT training artifact excerpt:

```json
{
  "skill_by_team": {
    "Away Team": -0.6931471805599453,
    "Home Team": 0.6931471805599453
  }
}
```

## Automated verification — BT row training path

Committed test path:

```text
tests.test_research_architecture_scaffolding.ResearchArchitectureScaffoldingTests.test_train_models_can_train_bt_from_training_data_capture
```

What it verifies:

- `train-models --model bt --training-data ...` can read labeled captured sports-input rows
- the resulting BT artifact contains `skill_by_team`

## Manual QA — walk-forward BT generation

Command shape:

```bash
run-sports-benchmark-suite \
  --dataset-root <datasets> \
  --dataset-name bt-manual \
  --dataset-version v1 \
  --walk-forward \
  --min-train-size 1 \
  --test-size 1 \
  --model-generator bt \
  --output-dir <output>
```

Observed output excerpt:

```json
{
  "model_generator": "bt",
  "split_model_keys": [
    "model_generator",
    "skill_by_team"
  ]
}
```

## Automated verification — ws_sports boundary helpers

Committed test path:

```text
tests.test_polymarket_ws_sports
```

What it verifies:

- `describe_boundary()` reports the current supported transport contract
- `sports_message_payload()` decodes JSON text and bytes payloads
- `send_sports_pong()` calls the websocket send method with the requested heartbeat message
