# 04 — Account Truth, Recovery, and Safety State

This file contains the two most important safety views:

1. **how runtime health and truth are assembled and checked**
2. **how pause / hold / recover / halt work**

## 4A. Runtime health and truth inputs

```mermaid
flowchart TD
    start[Need tradable state] --> venue[Adapter account snapshot + order book]
    start --> persisted[Persisted safety state]
    start --> projected[Projected source_health]

    subgraph truth[Truth and safety inputs]
        acct[balance • positions • open orders • fills]
        overlay[live overlay freshness\nheartbeat • user stream • market stream]
        pending[pending cancels • pending submissions • refresh requests]
        recovery[recovery items]
        kill[kill switch from source_health]
    end

    venue --> acct
    venue --> overlay
    persisted --> pending
    persisted --> recovery
    projected --> kill

    acct --> reconcile[Reconcile local vs observed truth]
    overlay --> reconcile
    pending --> runtimehealth[Build runtime summary]
    recovery --> runtimehealth
    kill --> runtimehealth
    reconcile --> runtimehealth

    runtimehealth --> ok[healthy / paused / hold / recovering / halted]
```

## 4B. Runtime state machine

```mermaid
stateDiagram-v2
    [*] --> Healthy

    Healthy --> Paused: operator pause
    Healthy --> HoldNewOrders: operator hold-new-orders
    Healthy --> Recovering: pending cancels/submissions\nrecovery items\noverlay degradation
    Healthy --> Halted: reconciliation halt\nkill switch\nresume check failure

    Paused --> Healthy: operator unpause
    HoldNewOrders --> Healthy: clear-hold-new-orders
    Recovering --> Healthy: recovery items cleared\ntruth healthy again
    Recovering --> Halted: trust broken / severe drift
    Halted --> Healthy: supervised resume with clean evidence
```

## What matters most

- the system can **stop itself** when truth, source health, or recovery state becomes unsafe
- kill-switch state is now derived from projected `source_health`, not only from local runtime memory
- a resume is not a blind toggle; it requires a clean supervised check against current truth
