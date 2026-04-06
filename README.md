# Prediction Market Agent Workspace

This workspace is a monorepo-style seed for building a single evolving Kalshi/Polymarket trading agent.

It is organized around one core idea:

- **execution and risk live in your codebase**
- **venue connectivity comes from official or battle-tested clients**
- **research and model training stay separate from the live trading loop**

## What is here

- `upstreams/poly-market-maker/` — execution-kernel reference and fork-first base
- `upstreams/py-clob-client/` — official Polymarket CLOB client
- `upstreams/KalshiMarketMaker/` — Kalshi risk/ops and cleanup patterns
- `upstreams/prediction-market-analysis/` — historical data, indexing, and analysis backbone
- `upstreams/pykalshi/` — Kalshi client dependency/reference layer
- `upstreams/TradingAgents/` — optional research/orchestration patterns for analyst-style agents
- `upstreams/OpenBB/` — optional data platform layer for broader market/news ingestion
- `upstreams/qlib/` — optional model/research workflow layer for training, evaluation, and experiment management

## Workspace layout

- `adapters/` — your venue adapters and normalization layer
- `engine/` — strategy loop, order intent generation, orchestration
- `risk/` — authoritative risk engine, kill switch, caps, stale-market checks
- `research/` — replay, labeling, backtests, feature generation, calibration
- `infra/` — deployment, monitoring, environment wiring
- `scripts/` — operational and bootstrap scripts
- `docs/` — architecture and integration notes
- `upstreams/` — reference repos cloned locally for selective reuse

## Recommended build order

1. Fork `poly-market-maker` concepts into `engine/` as your live execution shell.
2. Wrap `py-clob-client` behind your own Polymarket adapter in `adapters/`.
3. Add a Kalshi adapter using `pykalshi` and patterns from `KalshiMarketMaker`.
4. Build a shared `risk/` module before adding more strategies.
5. Use `prediction-market-analysis` patterns inside `research/` for replay, labeling, and model evaluation.
6. Pull from `TradingAgents`, `OpenBB`, and `qlib` only as optional support layers after the execution/risk core is stable.

## Important rule

Do **not** turn `upstreams/` into production code by importing everything directly. Use it as one of:

- a pinned dependency,
- a reference implementation,
- or a source for small selective ports.

The product should converge into _your_ `adapters/`, `engine/`, `risk/`, and `research/` layers.

See `docs/ARCHITECTURE.md` for the full system design.
