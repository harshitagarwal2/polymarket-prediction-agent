# Upstream Repos in This Workspace

This directory exists so you can move fast without losing track of what each upstream repo is meant to do.

## Cloned upstreams

### `upstreams/poly-market-maker`

- Source: `Polymarket/poly-market-maker`
- Why it is here: fastest live execution shell to start from
- Keep from it: execution loop, cancel/replace lifecycle, config shape
- Avoid: hard-coding Polymarket-specific assumptions into your shared core

### `upstreams/py-clob-client`

- Source: `Polymarket/py-clob-client`
- Why it is here: official Polymarket client for CLOB connectivity
- Keep from it: auth, order posting, orderbook access, cancel flow
- Best usage: wrap it in `adapters/polymarket/`

### `upstreams/KalshiMarketMaker`

- Source: `rodlaf/KalshiMarketMaker`
- Why it is here: strong runtime safety and cleanup patterns
- Keep from it: risk caps, cleanup invariants, retry/backoff, liquidation workflow
- Avoid: treating its Kalshi-specific structure as your cross-venue domain model

### `upstreams/prediction-market-analysis`

- Source: `Jon-Becker/prediction-market-analysis`
- Why it is here: strongest public Kalshi/Polymarket data and analysis backbone
- Keep from it: indexers, schemas, Parquet storage, analysis/replay ideas
- Best usage: feed `research/`, not `engine/`

### `upstreams/pykalshi`

- Source: `ArshKA/pykalshi`
- Why it is here: convenient Kalshi client and websocket reference
- Keep from it: request signing, ws auth, async feed patterns, rate-limit handling
- Best usage: wrap it in `adapters/kalshi/`

### `upstreams/TradingAgents`

- Source: `TauricResearch/TradingAgents`
- Why it is here: optional analyst/research-agent orchestration reference
- Keep from it: research workflow ideas, multi-analyst decomposition, report generation patterns
- Avoid: using LLM-agent debate as the core trading engine or replacing deterministic execution/risk

### `upstreams/OpenBB`

- Source: `OpenBB-finance/OpenBB`
- Why it is here: optional data platform and ingestion layer
- Keep from it: data connectors, research workflows, broader market/news data plumbing
- Avoid: letting a broad analytics platform sprawl into the execution core

### `upstreams/qlib`

- Source: `microsoft/qlib`
- Why it is here: optional research/model pipeline reference
- Keep from it: experiment management, feature workflows, model evaluation ideas, backtest discipline
- Avoid: forcing a large quant framework into the core bot before the bot's own execution/reconciliation loop is mature

## Rule of thumb

If a file touches:

- live order posting,
- account balances,
- open orders,
- positions,
- or risk decisions,

it should gradually move into your own `adapters/`, `engine/`, and `risk/` code instead of staying scattered across upstream repos.

Optional research/data/orchestration repos should usually influence `research/` and offline workflows first, not the live execution path.
