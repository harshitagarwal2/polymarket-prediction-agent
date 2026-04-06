# Getting Started

## What this workspace is right now

This is now a **real scaffold**, not just a folder of upstream repos.

It contains:

- normalized trading types in `adapters/types.py`
- a common adapter contract in `adapters/base.py`
- thin wrappers for Polymarket and Kalshi in `adapters/polymarket.py` and `adapters/kalshi.py`
- a reusable execution shell in `engine/runner.py`
- an offline-first discovery and ranking layer in `engine/discovery.py`
- an offline-first scan/rank/act orchestrator in `engine/discovery.py`
- a JSONL event journal in `research/storage.py`
- a cached venue-account state in `engine/accounting.py`
- order lifecycle utilities in `engine/order_state.py`
- reconciliation reports in `engine/reconciliation.py`
- a first fair-value strategy in `engine/strategies.py`
- a shared risk gate in `risk/limits.py`
- a fail-closed cleanup helper in `risk/cleanup.py`
- a stronger paper broker with resting orders, reserved capital, and configurable fill realism in `research/paper.py`
- a replay runner in `research/replay.py`

## Installation

From the workspace root:

```bash
pip install -e .
pip install -e upstreams/py-clob-client
pip install -e upstreams/pykalshi
```

Optional research helpers:

```bash
pip install pandas pyarrow duckdb
```

Quick demos:

```bash
python3 scripts/demo_preview.py
python3 scripts/demo_replay.py
python3 scripts/operator_cli.py status --state-file runtime/safety-state.json
python3 scripts/operator_cli.py status --state-file runtime/safety-state.json --journal runtime/events.jsonl
python3 scripts/run_agent_loop.py --venue polymarket --mode preview --fair-values-file runtime/fair_values.json --max-cycles 1
python3 scripts/summarize_events.py --journal runtime/events.jsonl
```

`summarize_events.py` now shows both aggregate counts and a compact recent-runtime summary.
That recent-runtime summary now includes the last execution-attempt context as well (selected market, policy outcome, placement count, accepted placements, and order IDs when present).

Paper execution realism knobs live in `PaperExecutionConfig` and currently support:

- `max_fill_ratio_per_step` — cap how much displayed depth you can actually consume per replay step
- `slippage_bps` — deterministic price penalty applied to simulated fills

## Environment setup

### Polymarket

- private key
- funder address if using a proxy wallet
- `account_address` if you want Data API position recovery to target a specific wallet explicitly
- optional phase-1 live-state env vars:
  - `POLYMARKET_LIVE_USER_MARKETS` = comma-separated condition IDs for the user stream subscription
  - `POLYMARKET_USER_WS_HOST` = override websocket endpoint if needed

### Kalshi

- `KALSHI_API_KEY_ID`
- `KALSHI_PRIVATE_KEY_PATH`

## How you start using it

### 1. Read-only market inspection

Use the adapters in read-only mode first.

### 2. Strategy preview

Build a `StrategyContext`, restore from venue truth, generate intents, and run `preview_once()` through the risk engine.

If you want halt state to survive restarts, instantiate `TradingEngine(..., safety_state_path="runtime/safety-state.json")`.

### 3. Paper-trade the same intents

Feed the intents into `PaperBroker.execute()` so you can observe behavior without live money.

### 4. Replay many snapshots

Run the same strategy through `ReplayRunner` over stored or synthetic snapshots.

### 5. Add one narrow strategy

Start with one event family or market family, not everything.

### 6. Discover and rank opportunities offline first

Use adapter market discovery plus `OpportunityRanker` before building an always-on scanner.

### 7. Orchestrate one scan cycle

Use `AgentOrchestrator` to run a single `scan -> rank -> preview` or `scan -> rank -> execute` cycle.
If you pass an `EventJournal`, each cycle is written as a durable JSONL event.
`run_top()` is now guarded by a deterministic policy gate, so the top-ranked candidate can still be rejected before order placement.
That gate can veto trades for stale state, unhealthy reconciliation, thin liquidity, wide spreads, duplicate exposure, open-order count, cooldowns, and contract-level capital-at-risk limits.
It also now sees global outstanding order pressure through total open-order count and total open-order notional.
Before execution, a deterministic sizing layer can also override the raw strategy quantity based on edge and available top-level liquidity.
The gate can also veto new risk when unresolved partial fills still exist on the candidate contract or globally.

### 8. Poll continuously

Use `PollingAgentLoop` or `scripts/run_agent_loop.py` to repeat those cycles on a timer.
When the engine is manually paused, the polling loop skips new scan/rank cycles instead of continuing to generate fresh decisions.
If a lifecycle manager is configured, the loop can still cancel stale orders as housekeeping before it skips new decisions.
The loop now also refreshes global account truth before each scan. If account truth is incomplete, it blocks new decisions fail-closed and only performs housekeeping/operator actions.

For Polymarket run mode, the adapter also has a phase-1 websocket overlay for **open-order freshness**. It only serves cached order state when the stream is initialized and fresh, and otherwise falls back to the existing REST path. Balances, positions, fills, and reconciliation still remain REST-driven in this phase.

### 9. Clean up stale live orders

Use `OrderLifecycleManager` with `OrderLifecyclePolicy` to detect and cancel stale resting orders deterministically.
The same cleanup path can be invoked automatically from the polling loop or manually via the operator CLI.

## Example flow

```python
from adapters.polymarket import PolymarketAdapter, PolymarketConfig
from adapters.types import Contract, OutcomeSide, Venue
from engine.strategies import FairValueBandStrategy
from engine.runner import TradingEngine
from risk.limits import RiskEngine, RiskLimits

adapter = PolymarketAdapter(PolymarketConfig())
contract = Contract(venue=Venue.POLYMARKET, symbol="<token-id>", outcome=OutcomeSide.YES)

engine = TradingEngine(
    adapter=adapter,
    strategy=FairValueBandStrategy(quantity=1, edge_threshold=0.03),
    risk_engine=RiskEngine(RiskLimits()),
)

result = engine.preview_once(contract, fair_value=0.61)
print(result.context.book.best_bid, result.context.book.best_ask)
print(result.reconciliation_before.healthy)
print(result.reconciliation_before.balance_drift, result.reconciliation_before.position_drift)
print(result.risk.approved, result.risk.rejected)
```

## Replay example

```python
from adapters.types import OrderBookSnapshot, PriceLevel
from research.paper import PaperBroker
from research.replay import ReplayRunner, ReplayStep

steps = [
    ReplayStep(
        book=OrderBookSnapshot(
            contract=contract,
            bids=[PriceLevel(0.45, 10)],
            asks=[PriceLevel(0.50, 5)],
        ),
        fair_value=0.60,
    ),
    ReplayStep(
        book=OrderBookSnapshot(
            contract=contract,
            bids=[PriceLevel(0.48, 10)],
            asks=[PriceLevel(0.52, 10)],
        ),
        fair_value=0.60,
    ),
]

runner = ReplayRunner(
    strategy=FairValueBandStrategy(quantity=5, edge_threshold=0.03),
    risk_engine=RiskEngine(RiskLimits(max_contracts_per_market=10)),
    broker=PaperBroker(cash=100),
)
result = runner.run(steps)
print(result.ending_cash, result.ending_positions)
print(result.ending_portfolio_value, result.net_pnl)
```

## Discovery / ranking example

```python
from engine.discovery import OpportunityRanker, StaticFairValueProvider

markets = adapter.list_markets(limit=50)
provider = StaticFairValueProvider(
    fair_values={market.contract.market_key: 0.60 for market in markets}
)
ranker = OpportunityRanker(edge_threshold=0.03, limit=10)
candidates = ranker.rank(markets, provider)

for candidate in candidates[:5]:
    print(candidate.contract.market_key, candidate.action, candidate.edge, candidate.rationale)
```

## Orchestration example

```python
from engine.discovery import AgentOrchestrator
from research.storage import EventJournal

orchestrator = AgentOrchestrator(
    adapter=adapter,
    engine=engine,
    fair_value_provider=provider,
    ranker=ranker,
    journal=EventJournal("runtime/events.jsonl"),
)

cycle = orchestrator.preview_top(market_limit=50)
print(cycle.selected)
print(cycle.execution.risk.approved if cycle.execution else [])
```

## What makes the bot better

1. better reconciliation and venue-state truth
2. stronger risk limits and cleanup
3. better paper execution assumptions (latency, queue position, partial fills)
4. replay and backtesting discipline
5. better fair-value models
6. autonomous discovery and ranking over the market universe
7. better postmortems on losing trades

## Current limitations

- Polymarket wrapper is strongest for orderbook/order placement, but positions are still a thin placeholder.
- Kalshi wrapper depends on `pykalshi` field shapes and should be validated live in demo mode first.
- The paper broker is better than the first slice, but still optimistic compared to real queue/latency conditions.
- Resting paper orders now reserve cash or inventory, so later simulated trades cannot reuse the same capital twice.
- There is no portfolio-level cross-market correlation model yet.
- Reconciliation currently focuses on open-order divergence; it still needs richer balance/fill/position truth.
- Polymarket websocket support is still only a phase-1 adapter-local open-order overlay, not an engine-native live-state architecture.

## Account-truth model

The runner now treats venue snapshots as the source of truth:

1. `adapter.get_account_snapshot(contract)` fetches balance, positions, open orders, and fills.
2. `engine.restore_from_venue(contract)` rebuilds local cached state before strategy evaluation.
3. After placements, reconciliation compares local cached state against a fresh venue snapshot.
4. If the snapshot is marked incomplete, `run_once()` blocks trading instead of placing orders.
5. If reconciliation sees severe drift after trading, the engine enters a halted state until you explicitly resume it.

This is still a scaffold, but it is a much safer shape than relying only on locally remembered intents.

## Halt and resume workflow

The engine now distinguishes between:

- `ok` — safe to continue
- `resync` — order-state mismatch, needs attention/resync but not automatically latched as a hard stop
- `halt` — dangerous drift like fill, position, or balance mismatch

When a `halt` policy is triggered after a run:

1. the engine latches `safety_state.halted = True`
2. the halt reason is stored in `safety_state.reason`
3. the halt is scoped to the offending contract key
4. future `run_once()` calls reject trading
5. you must call `engine.try_resume(contract)` after venue truth is healthy again

If `try_resume(contract)` sees a complete snapshot and clean reconciliation, it increments a clean-resume streak.
Only **newer observed snapshots** count toward that streak.
The halt only clears after the required number of consecutive clean resume checks.
If the snapshot is still incomplete, or if you try to resume the wrong contract, the halt remains latched.
If you configured `safety_state_path`, that halt also survives process restart.

## Manual operator controls

The engine now also supports an operator pause separate from automatic halts:

- `engine.pause("reason")`
- `engine.clear_pause()`
- `engine.status_snapshot()`

CLI examples:

```bash
python3 scripts/operator_cli.py status --state-file runtime/safety-state.json
python3 scripts/operator_cli.py status --state-file runtime/safety-state.json --journal runtime/events.jsonl
python3 scripts/operator_cli.py status --state-file runtime/safety-state.json --venue polymarket --symbol <token-id> --outcome yes
python3 scripts/operator_cli.py pause --state-file runtime/safety-state.json --reason "manual maintenance"
python3 scripts/operator_cli.py unpause --state-file runtime/safety-state.json
python3 scripts/operator_cli.py cancel-all --venue polymarket --symbol <token-id> --outcome yes
python3 scripts/operator_cli.py cancel-stale --venue polymarket --symbol <token-id> --outcome yes --max-order-age-seconds 30
```

If a venue dependency or credential path is missing, the operator CLI now exits with a clear operator-facing error instead of a Python traceback.

Manual pause and automatic halt are different:

- **pause** = operator intent to stop trading temporarily
- **halt** = engine safety latch caused by unsafe reconciliation / recovery state

`status` now also shows the **last persisted runtime truth summary** (counts of open orders, positions, fills, partial fills, balance, open-order notional, reserved buy notional, marked position notional, and whether the last observed truth was complete), even before it can reach the venue again.
When `--venue ...` is provided, `status` also shows a drift report between that persisted truth summary and the current live venue snapshot.
For Polymarket, venue-backed `status` now also reports whether the live-state overlay is active, initialized, fresh, and which condition IDs it is subscribed to.

For incident-time operation, see the quick reference in `docs/OPERATOR_RUNBOOK.md` under **pause vs halt vs resume**.

For Polymarket specifically, the adapter now attempts a best-effort snapshot from:

- CLOB open orders
- authenticated CLOB trades/fills
- Data API positions
- collateral balance/allowance

If those sources cannot be recovered consistently, the snapshot stays incomplete and live `run_once()` remains fail-closed.

## Recommended next steps

1. add demo-mode integration checks
2. add richer venue reconciliation for positions/fills
3. replay against stored parquet snapshots
4. add PnL attribution and calibration metrics
5. only then consider live money
