# Replay and Freeze-Lift Checklist

This document defines what must be true before replay, attribution, residual ML, or advisory/LLM scope is allowed to expand beyond the current preparatory surfaces.

## Current posture

The repository already has:

- deterministic replay coverage in `tests/test_strategy_and_replay.py`
- replay attribution CLI coverage in `tests/test_replay_attribution_cli.py`
- replay execution label dataset materialization through `run-replay-attribution`
- non-authoritative advisory artifacts under `llm/`

Those are **not** a green light to expand replay or advisory authority yet.

## Freeze-lift prerequisites

All of the following must be true before replay/ML/advisory scope grows:

1. The authority ADR remains current and accurate.
2. Capture workers are the sole live ingress owners.
3. Projector checkpoints and compatibility materialization are atomic.
4. Deterministic builders consume projected/current-state authority only.
5. `run-agent-loop` serious modes consume projected fair-value authority without requiring a manifest file.
6. `make smoke-service-stack` passes.
7. `make smoke-compose` passes.
8. The execution shell is the only sanctioned supervised placement seam.
9. Replay artifacts can be tied back to authoritative truth inputs, not compatibility fallbacks.

## What counts as “replay ready”

Replay is ready to expand only when:

- strategy replay still passes (`tests/test_strategy_and_replay.py`)
- replay attribution CLI still passes (`tests/test_replay_attribution_cli.py`)
- replay execution labels are materialized from authoritative truth and carry a versioned dataset snapshot
- advisory artifacts remain explicitly non-authoritative and operator-review only

## What still stays frozen

Until the checklist above is green, do **not**:

- expand residual ML training loops
- promote advisory output into an execution authority
- make replay-derived labels the primary authority for runtime decisions
- add new non-reviewed strategy promotion shortcuts

## Verification commands

```bash
uv run --locked python -m unittest discover -s tests -p "test_strategy_and_replay.py"
uv run --locked python -m unittest discover -s tests -p "test_replay_attribution_cli.py"
uv run --locked python -m unittest discover -s tests -p "test_llm_advisory.py"
```
