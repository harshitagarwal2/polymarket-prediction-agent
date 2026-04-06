# 04 — Account Truth, Reconciliation, and Safety Halt

This file contains the two most important safety views:

1. **how account truth is assembled and checked**
2. **how halt and resume work**

## 4A. Account Truth and Reconciliation Flow

```mermaid
flowchart TD
    start[Need tradable state] --> snap[adapter.get_account_snapshot]

    subgraph truth[Snapshot inputs]
        orders[Open orders\nCLOB / venue API]
        fills[Fills / trades\nauthenticated history]
        positions[Positions\nvenue or data API]
        balance[Balance / allowance]
    end

    orders --> snap
    fills --> snap
    positions --> snap
    balance --> snap

    snap --> complete{snapshot.complete?}
    complete -- no --> block[Fail closed\nblock run_once trading]
    complete -- yes --> sync[Sync AccountStateCache + OrderState]
    sync --> compare[Reconcile local vs observed]
    compare --> diff1[Order drift]
    compare --> diff2[Fill drift]
    compare --> diff3[Position drift]
    compare --> diff4[Balance drift]
    diff1 --> policy[Reconciliation policy]
    diff2 --> policy
    diff3 --> policy
    diff4 --> policy
    policy --> ok[ok]
    policy --> resync[resync]
    policy --> halt[halt]
```

## 4B. Halt / Resume State Machine

```mermaid
stateDiagram-v2
    [*] --> Running

    Running --> Running: policy = ok
    Running --> Running: policy = resync
    Running --> Halted: policy = halt

    state Halted {
        [*] --> Latched
        Latched --> Latched: run_once() while halted
        Latched --> Latched: try_resume(wrong contract)
        Latched --> Latched: try_resume(incomplete snapshot)
        Latched --> Running: try_resume(clean snapshot + clean reconciliation)
    }
```

## What matters most

- the bot can **stop itself** when trust is broken
- recovery is **contract-scoped**
- a halt is not cleared by wishful thinking; it needs a clean resume signal
