# Architecture Diagrams

This folder contains a **small, high-signal diagram set** for understanding the current prediction-market agent without drowning in detail.

## Recommended reading order

1. `01-system-context.md` — the major external systems and repo-owned lanes
2. `02-container-view.md` — the current package boundaries inside the workspace
3. `03-live-runtime-sequence.md` — one supervised `run-agent-loop` cycle
4. `04-account-truth-and-safety.md` — truth, recovery, kill-switch, and resume state
5. `05-research-paper-replay-loop.md` — the offline benchmark and replay improvement loop
6. `06-discovery-to-execution-decision-flow.md` — the projected current-state builder lane and deterministic proposal path
7. `07-operator-control-plane.md` — how workers, runtime, artifacts, and operator commands fit together
8. `08-runtime-state-and-artifacts.md` — what persists across restart and incident review

## Why this set exists

The repo has grown beyond a simple runtime/research split, but the right answer is still **a small diagram set that stays current**.

This set focuses on:

- **system context** for orientation
- **container/component view** for ownership boundaries
- **supervised runtime sequence** for the live decision path
- **state / safety view** for the main failure contract
- **builder / projection flow** for the newer current-state architecture
- **research loop** for improvement without live capital

That is enough to understand the workspace quickly without creating a giant stale diagram set.

## Source-of-truth files

The diagrams are grounded in these implementation files:

- `README.md`
- `docs/ARCHITECTURE.md`
- `docs/GETTING_STARTED.md`
- `docs/architecture/sports_polymarket_architecture.md`
- `scripts/run_agent_loop.py`
- `scripts/operator_cli.py`
- `scripts/ingest_live_data.py`
- `scripts/run_sportsbook_capture.py`
- `scripts/run_polymarket_capture.py`
- `scripts/run_current_projection.py`
- `services/capture/sportsbook.py`
- `services/capture/polymarket.py`
- `services/projection/current_state.py`
- `engine/discovery.py`
- `engine/runner.py`
- `engine/runtime_bootstrap.py`
- `execution/planner.py`
- `forecasting/fair_value_engine.py`
- `opportunity/ranker.py`
- `risk/kill_switch.py`
- `risk/limits.py`
- `storage/current_projection.py`
- `storage/current_read_adapter.py`
- `storage/journal.py`
- `research/paper.py`
- `research/replay.py`

When behavior and diagrams disagree, **the code and tests win**.

For live operator decisions under pressure, pair this diagram set with the quick reference in `docs/OPERATOR_RUNBOOK.md`.

## Rendering workflow

The canonical regeneration command is:

```bash
python3 scripts/render_diagrams.py
```

That script uses a pinned Mermaid CLI version and rewrites the checked-in generated artifacts in this folder:

- `docs/diagrams/rendered/*.svg`
- `docs/diagrams/RENDERED.md`

Those files are versioned documentation artifacts in this repository, not throwaway local build output. When a diagram source changes, rerender and commit the generated artifacts together so reviewers can compare the Markdown source and the rendered output in the same change.
