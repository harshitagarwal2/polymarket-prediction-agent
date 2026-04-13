# Sports + Polymarket Verification Record

This file records the latest local verification evidence for the added sports + Polymarket architecture paths.

Current HEAD for this record: `b37d5871bcbeb79c5075ff204bc139aced0376ab`

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
Ran 38 tests in 0.056s

OK
```

## Full unit suite

Command:

```bash
uv run --locked python -m unittest discover -s tests -p "test_*.py"
```

Observed result:

```text
Ran 329 tests in 1.611s

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

Observed output excerpt:

```json
{
  "layer": "gamma",
  "market_count": 2
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
