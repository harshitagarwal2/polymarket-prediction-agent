# 06 — Current-State Builder to Proposal Flow

This diagram answers: **how does the projected current-state lane turn captured data into deterministic execution proposals and blocked reasons?**

```mermaid
flowchart TD
    raw[Raw capture lanes in Postgres] --> project[run-current-projection]
    project --> current[Projected current-state tables\ncompatibility JSON]

    current --> mappings[build-mappings]
    current --> fvs[build-fair-values]
    current --> opps[build-opportunities]

    mappings --> opps
    fvs --> opps
    current --> plannerctx[build_preview_runtime_context]
    opps --> plannerctx
    plannerctx --> planner[ExecutionPlanner]

    planner --> allowed{proposal allowed?}
    allowed -- yes --> previewctx[preview_order_context.json\npreview proposals]
    allowed -- no --> blocked[blocked proposals\nreason sets]

    previewctx --> advisory[build-llm-advisory\noperator review]
    blocked --> advisory
```

## Why this flow exists

This is the lane that makes the newer architecture understandable:

- raw capture is separated from current-state projection
- deterministic builders consume projected state rather than raw envelopes directly
- operator-side preview/advisory context is materialized from the same deterministic substrate
- this lane supports the runtime and operator workflows without becoming the runtime itself
