# Sports + Polymarket Verification Record

This file records the latest local verification evidence for the added sports + Polymarket architecture paths.

Machine-readable companion artifact: `docs/verification_sports_polymarket.json`

## Focused automated checks

Command:

```bash
uv run --locked python -m unittest \
  tests.test_build_sports_fair_values \
  tests.test_run_agent_loop \
  tests.test_research_architecture_scaffolding \
  tests.test_ingest_live_data \
  tests.test_polymarket_ws_sports
```

Observed result:

```text
Ran 82 tests in 1.107s

OK
```

## Full unit suite

Command:

```bash
uv run --locked python -m unittest discover -s tests -p "test_*.py"
```

Observed result:

```text
Ran 511 tests in 3.181s

OK
```

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

## Manual QA — live current-state fair-value build with consensus artifact

## Manual QA — dedicated sportsbook capture worker

Command shape:

```bash
uv sync --extra postgres
export PREDICTION_MARKET_POSTGRES_DSN=postgresql://...

python -m scripts.run_sportsbook_capture \
  --sport basketball_nba \
  --market h2h \
  --event-map-file <odds_event_map.json> \
  --root <runtime_root> \
  --max-cycles 1
```

Observed behavior:

- continuous sportsbook capture no longer has to run through the monolithic `ingest_live_data` orchestration path
- each cycle replaces `current/sportsbook_events.json` and `current/sportsbook_odds.json` with the latest snapshot rather than accumulating stale rows forever
- append-only sportsbook quote rows are still written to the postgres-layer `sportsbook_odds` store
- each normalized quote row preserves bookmaker-facing `source`, upstream `provider`, `source_ts`, `capture_ts`, and `source_age_ms`
- `source_health` is mirrored into both `current/source_health.json` and `postgres/source_health.json`

## Manual QA — live current-state fair-value build with consensus artifact

Command shape:

```bash
python -m scripts.train_models --model consensus --output <consensus_artifact.json>
python -m scripts.ingest_live_data sportsbook-odds --sport basketball_nba --market h2h --event-map-file <odds_event_map.json> --root <runtime_root>
python -m scripts.ingest_live_data build-mappings --market h2h --root <runtime_root>
python -m scripts.ingest_live_data build-fair-values --root <runtime_root> --consensus-artifact <consensus_artifact.json>
python -m scripts.ingest_live_data build-inference-dataset --root <runtime_root>
```

Observed behavior:

- the live sportsbook ingest path accepts `--event-map-file` and persists enriched sportsbook event identity for mapping
- `build-mappings` blocks rows missing upstream `event_key` / `game_id`
- `build-mappings` keeps the flat current-state selector rows in `runtime/data/current/market_mappings.json` and also writes `runtime/data/current/market_mapping_manifest.json` with structured `mapping_status`, `mapping_confidence`, `blocked_reason`, identity, and rule-semantics payloads
- `build-fair-values --consensus-artifact ...` changes live fair-value output based on artifact half-life and writes current-state fair values plus `source_health`
- a failing fair-value build marks `source_health["fair_values"]` red without partially overwriting the prior fair-values tables
- `build-inference-dataset` writes `processed/inference/joined_inference_dataset.jsonl`, registers `joined-inference-dataset`, and keeps `source_health["joined_inference_dataset"]` in sync with the latest row count

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

## Manual QA — live Gamma capture

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
