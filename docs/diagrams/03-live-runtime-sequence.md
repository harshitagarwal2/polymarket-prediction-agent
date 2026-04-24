# 03 — Live Runtime Sequence

This diagram answers: **what happens during one supervised `run-agent-loop` cycle?**

```mermaid
sequenceDiagram
    participant O as Operator
    participant CLI as run-agent-loop
    participant B as runtime_bootstrap
    participant A as Venue adapter
    participant F as FairValueProvider
    participant D as AgentOrchestrator
    participant E as TradingEngine
    participant G as PolicyGate + RiskEngine
    participant J as Safety state / journal

    O->>CLI: start preview/run cycle
    CLI->>B: load config, policy, adapter, current-state authority
    B-->>CLI: adapter + fair value provider + kill-switch context
    CLI->>A: list_markets()
    A-->>CLI: live market summaries
    CLI->>F: fair_value_for(market)
    F-->>CLI: raw or calibrated fair values
    CLI->>D: rank candidates
    D->>E: preview_once(candidate)
    E-->>D: preview context + reconciliation_before
    D->>D: deterministic size()
    D->>E: review_precomputed(preview)
    E-->>D: approved / rejected intents
    D->>G: execution policy + shared risk checks
    G-->>D: allow or reject

    alt preview mode
        D-->>O: journal preview outcome only
    else run mode
        D->>E: run_precomputed()
        E->>A: place/cancel/refresh as needed
        A-->>E: venue truth and placement results
        E->>J: persist safety state + events + runtime metrics
        E-->>O: cycle result
    end
```

## Key design point

The runtime still makes decisions from **live adapter state plus a fair-value provider**. The projected current-state substrate supports kill-switch context, builder workflows, and operator-side preview artifacts, but it does not replace the supervised runtime loop itself.
