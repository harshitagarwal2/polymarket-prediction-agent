# Architecture Diagrams

This folder contains a **small, high-signal diagram set** for understanding the prediction-market agent without drowning in detail.

## Recommended reading order

1. `01-system-context.md` — what the bot talks to and what the major boundaries are
2. `02-container-view.md` — what lives inside the workspace
3. `03-live-runtime-sequence.md` — how one live decision runs
4. `04-account-truth-and-safety.md` — how state truth, reconciliation, halt, and resume work
5. `05-research-paper-replay-loop.md` — how strategy development and replay fit in
6. `06-discovery-to-execution-decision-flow.md` — how the bot turns a market universe into a concrete trade decision
7. `07-operator-control-plane.md` — how the operator, scripts, runtime state, and journal fit together
8. `08-runtime-state-and-artifacts.md` — what persists across restart and why it matters

## Why this set exists

Based on external architecture-diagram best practices, this set stays intentionally small:

- **system context** for orientation
- **container/component view** for ownership boundaries
- **runtime sequence** for the critical live path
- **state / safety view** for the most important failure contract
- **research loop** for understanding how improvement happens

That is enough to understand the bot quickly without creating a giant stale diagram set.

## Source-of-truth files

The diagrams are grounded in these implementation files:

- `README.md`
- `docs/ARCHITECTURE.md`
- `docs/GETTING_STARTED.md`
- `adapters/base.py`
- `adapters/types.py`
- `adapters/polymarket.py`
- `adapters/kalshi.py`
- `engine/accounting.py`
- `engine/order_state.py`
- `engine/reconciliation.py`
- `engine/runner.py`
- `engine/strategies.py`
- `risk/limits.py`
- `research/paper.py`
- `research/replay.py`

When behavior and diagrams disagree, **the code and tests win**.

For live operator decisions under pressure, pair this diagram set with the quick reference in `docs/OPERATOR_RUNBOOK.md`.
