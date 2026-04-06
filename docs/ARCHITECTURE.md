# Prediction Market Agent Architecture

## Goal

Build one codebase that can keep compounding over time instead of becoming a pile of disconnected bots.

The architecture should optimize for:

1. **fast iteration**
2. **safe live execution**
3. **clean venue abstraction**
4. **research/live parity**
5. **learning from resolved markets and bad trades**

## Core design decision

The live system should be built around an **execution-and-risk core**, not around an LLM or around research notebooks.

- **Execution core**: takes market state + strategy intent and manages orders/fills
- **Risk core**: can reject or flatten independently of strategy
- **Research core**: produces fair values, model outputs, labels, and replay results
- **LLM layer**: optional helper for news triage, contract understanding, and postmortems — not the source of final probability

## Proposed target architecture

```text
                        +----------------------+
                        |  External Inputs     |
                        |  - News / RSS        |
                        |  - Event data        |
                        |  - Research signals  |
                        +----------+-----------+
                                   |
                                   v
 +-------------------+    +----------------------+    +-------------------+
 | Polymarket        |    |  Adapters            |    | Kalshi            |
 | py-clob-client    +--->|  - polymarket/       +<---+ pykalshi / SDK    |
 | CLOB / ws / auth  |    |  - kalshi/           |    | REST / ws / auth  |
 +-------------------+    |  - normalized types  |    +-------------------+
                          +----------+-----------+
                                     |
                                     v
                          +----------------------+
                          | Engine               |
                          | - market snapshot    |
                          | - strategy hooks     |
                          | - order intents      |
                          | - execution loop     |
                          +----------+-----------+
                                     |
                    +----------------+----------------+
                    |                                 |
                    v                                 v
          +-------------------+             +-------------------+
          | Risk              |             | Research/Models   |
          | - caps            |             | - replay          |
          | - exposure        |             | - features        |
          | - stale checks    |             | - calibration     |
          | - kill switch     |             | - fair value      |
          +---------+---------+             +---------+---------+
                    |                                 |
                    +----------------+----------------+
                                     |
                                     v
                          +----------------------+
                          | Monitoring / Ops     |
                          | - logs               |
                          | - alerts             |
                          | - dashboards         |
                          | - paper trading      |
                          +----------------------+
```

## Directory responsibilities

### `adapters/`

Your venue-agnostic boundary.

Suggested sublayout:

```text
adapters/
  polymarket/
    client.py
    market_data.py
    execution.py
    mapper.py
  kalshi/
    client.py
    market_data.py
    execution.py
    mapper.py
  types.py
```

Responsibilities:

- hide venue-specific auth, signing, websocket, and order payload details
- map both venues into one internal model:
  - `Market`
  - `Contract`
  - `OrderBook`
  - `Order`
  - `Fill`
  - `Position`
  - `Balance`

### `engine/`

The live trading brain, but not the statistical brain.

Responsibilities:

- poll/subscribe to market state
- build a normalized snapshot
- ask strategy modules for intents
- pass intents through risk checks
- submit/cancel/replace orders through adapters
- reconcile fills and open orders

Suggested sublayout:

```text
engine/
  runner.py
  market_snapshot.py
  intents.py
  strategy_api.py
  order_manager.py
  reconciliation.py
```

### `risk/`

This should be authoritative and independent.

Responsibilities:

- max contracts per market
- max global exposure
- daily loss limit
- concentration limits by category/event
- stale orderbook protection
- spread/slippage constraints
- disconnect or heartbeat fail-close behavior
- kill switch / flatten logic

Borrow most of the operational attitude from `KalshiMarketMaker`, not just the math.

### `research/`

This is where you become better than random over time.

Responsibilities:

- historical ingestion and replay
- feature generation
- labeling from resolved outcomes
- backtesting and paper trading
- model calibration
- error attribution on bad trades

Suggested sublayout:

```text
research/
  data/
  features/
  labels/
  replay/
  models/
  evaluation/
```

## Upstream repo roles

## 1. `upstreams/poly-market-maker`

**Role:** fork-first execution shell.

Use it for:

- sync loop structure
- cancel/replace lifecycle
- strategy plug points
- config and local runner shape
- Docker/bootstrap ideas

Do not keep it as your permanent internal truth. Extract the useful structure into `engine/`.

## 2. `upstreams/py-clob-client`

**Role:** Polymarket dependency/reference.

Use it for:

- auth and API credentials
- order construction and posting
- orderbook reads
- order cancellation

Best practice: keep this as a dependency or a thin wrapped client, not copied wholesale into core logic.

## 3. `upstreams/KalshiMarketMaker`

**Role:** Kalshi risk and runtime pattern source.

Use it for:

- cleanup invariants
- worker lifecycle
- retry/backoff
- portfolio caps
- liquidation controls
- dynamic market selection ideas

Best use: port concepts into shared risk/runtime code rather than copy venue-specific logic directly.

## 4. `upstreams/prediction-market-analysis`

**Role:** research and data backbone.

Use it for:

- market/trade indexing
- Parquet storage patterns
- cursor resume / checkpointing
- analysis scripts
- outcome labeling and calibration studies

This repo should heavily influence `research/`, not `engine/`.

## 5. `upstreams/pykalshi`

**Role:** Kalshi client dependency/reference.

Use it for:

- signed request flow
- websocket auth/header patterns
- async feed handling
- rate limiting and retry patterns

## 6. `upstreams/TradingAgents`

**Role:** optional research-agent orchestration reference.

Use it for:

- multi-analyst decomposition ideas
- research report / debate workflows
- optional news or evidence synthesis layers

Do not let it become the live trading core. Its best fit is upstream of fair-value estimation, not downstream in execution.

## 7. `upstreams/OpenBB`

**Role:** optional data platform and connector layer.

Use it for:

- pulling broader market/news/macroeconomic data
- normalizing external datasets for research
- enriching offline analysis and scanners

Best use: feed `research/` and future scanner/ranker modules, not `engine/` directly.

## 8. `upstreams/qlib`

**Role:** optional quant research / model workflow reference.

Use it for:

- feature pipelines
- experiment tracking ideas
- model evaluation discipline
- backtest/research organization

Best use: support future ML-assisted layers only after the execution/risk/account-truth stack is stable.

## Copy vs dependency vs inspiration

### Keep as dependency or wrapped client

- `py-clob-client`
- `pykalshi`

### Port selected concepts and some code carefully

- `poly-market-maker`
- `KalshiMarketMaker`
- `prediction-market-analysis`

### Optional supporting systems (usually inspiration or offline tooling first)

- `TradingAgents`
- `OpenBB`
- `qlib`

### Do not let become the product core

- notebooks
- ad hoc scripts
- venue-specific configuration blobs as your internal domain model
- direct imports from `upstreams/` into production code unless intentionally pinned and wrapped

## How the live loop should work

1. **adapter** fetches market state/orderbook/trades
2. **engine** builds normalized snapshot
3. **research/model output** provides fair value or probability estimate
4. **strategy** proposes action:
   - no trade
   - quote both sides
   - buy yes / buy no
   - reduce / flatten
5. **risk** approves, shrinks, or rejects
6. **adapter** sends order
7. **reconciliation** updates fills and positions
8. **logging** stores everything needed for future postmortems

## How learning from losses should work

Every resolved trade should produce a record with:

- market and contract text
- resolution rules snapshot
- orderbook state at decision time
- your predicted probability/fair value
- size and risk settings
- actual fill details and slippage
- final resolution outcome
- postmortem category

Then evaluate by:

- Brier score
- log loss
- calibration bucket accuracy
- realized PnL net of fees/slippage
- hit rate by market type
- error class:
  - wrong forecast
  - wrong interpretation
  - wrong sizing
  - bad execution
  - liquidity trap
  - stale data

An LLM can help summarize the postmortem, but should not rewrite the label or invent the lesson.

## Recommended implementation order

### Phase 1 — unify execution

- create normalized domain models
- wrap Polymarket behind adapter interface
- wrap Kalshi behind adapter interface
- build shared order manager and reconciliation

### Phase 2 — shared risk shell

- implement global caps
- per-market caps
- stale-book checks
- kill switch
- flatten routine

### Phase 3 — research bridge

- bring in historical datasets
- build replay runner
- build paper trading loop
- compare against market-implied baseline

### Phase 4 — forecasting

- start with simple calibrated models
- feed only fair values/probabilities into engine
- keep models out of order transport and risk logic

### Phase 5 — optional LLM augmentation

- contract understanding
- news summarization
- evidence extraction
- postmortem summaries

## What not to do

- do not let an LLM directly assign tradable confidence without calibration
- do not mix research notebooks into the live bot process
- do not let venue-specific payloads leak above `adapters/`
- do not make strategy the owner of risk
- do not assume a high win rate means good economics

## Practical recommendation

If you want the fastest path:

- **use `poly-market-maker` as the conceptual base**
- **use `py-clob-client` and `pykalshi` as transport dependencies**
- **use `KalshiMarketMaker` to harden risk/runtime behavior**
- **use `prediction-market-analysis` to build your research/replay system**

That gives you one codebase that can grow in a disciplined way instead of becoming a Frankenstack.
