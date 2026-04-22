# Forecasting Platform Surfaces

The repo now has a reusable `forecasting/` package for domain-agnostic forecast tooling that does not need to live under `research/`.

## What moved into `forecasting/`

- `forecasting/calibration.py` - reusable histogram calibration artifacts and loaders
- `forecasting/calibrator.py` - production-facing calibration adapter around reusable calibration artifacts
- `forecasting/consensus.py` - weighted consensus and dispersion helpers for deterministic fair-value combination
- `forecasting/fair_value_engine.py` - fair-value providers plus deterministic consensus fair-value engine
- `forecasting/model_registry.py` - lightweight registry for named forecast-model loaders
- `forecasting/scoring.py` - forecast metrics, calibration bins, bootstrap intervals, and paired comparison stats
- `forecasting/contracts.py` - optional LLM contract-evidence parsing plus deterministic consistency fallbacks
- `forecasting/dashboards.py` - model-vs-market JSON/Markdown dashboard artifacts
- `forecasting/pipeline.py` - non-sports forecasting pipeline scaffolds

`research/calibration.py` and the forecast-oriented parts of `research/scoring.py` remain compatibility facades so the existing sports benchmark flow keeps working.

## Model-vs-market dashboard artifacts

Use the new script to publish dashboard artifacts from contract-level rows:

```bash
python -m scripts.render_model_vs_market_dashboard   --input runtime/forecasting/politics/forecast_output.json   --output-dir runtime/forecasting/politics
```

Expected row shape:

- `contract_id` or `market_key`
- `model_probability` (or `fair_value` / `prediction`)
- `market_probability` (or `market_midpoint`)
- optional `outcome_label`
- optional `domain`, `segment`, `market_source`

If outcome labels are present, the dashboard includes model-vs-market calibration deltas for Brier score, log loss, accuracy, and expected calibration error.

## Optional LLM contract evidence

If you have offline contract evidence, pass it as a JSON file:

```bash
python -m scripts.render_model_vs_market_dashboard   --input runtime/forecasting/politics/forecast_output.json   --output-dir runtime/forecasting/politics   --llm-contract-evidence runtime/forecasting/politics/llm_contract_evidence.json
```

Evidence is optional. When the file is omitted or a contract has no LLM probability, the dashboard records a deterministic fallback consistency surface instead of failing closed. That keeps market-vs-model review available even when LLM support is absent or incomplete.

## Non-sports pipeline scaffolding

To scaffold a politics, macro, crypto, or custom-event forecasting lane:

```bash
python -m scripts.scaffold_forecasting_pipeline   --domain politics   --output runtime/forecasting/politics/pipeline_scaffold.json
```

The scaffold writes domain-neutral stage definitions for capture, training/model loading, forecasting, and dashboard publication. Optional LLM contract evidence is explicitly documented so the deterministic path remains the source of truth.
