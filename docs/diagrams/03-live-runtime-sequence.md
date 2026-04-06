# 03 — Live Runtime Sequence

This diagram answers: **what happens during one live decision cycle?**

```mermaid
sequenceDiagram
    participant O as Operator
    participant E as TradingEngine
    participant A as Adapter
    participant C as AccountStateCache / OrderState
    participant S as Strategy
    participant R as RiskEngine
    participant Q as ReconciliationEngine

    O->>E: run_once(contract, fair_value)
    E->>A: get_account_snapshot(contract)
    A-->>E: AccountSnapshot
    E->>C: sync snapshot into local caches
    E->>A: get_order_book(contract)
    A-->>E: OrderBookSnapshot
    E->>Q: reconcile(contract)
    Q-->>E: reconciliation_before
    E->>S: generate_intents(context)
    S-->>E: proposed intents
    E->>R: evaluate(intents, position, open_orders)
    R-->>E: approved / rejected

    alt engine halted
        E-->>O: reject all trading
    else snapshot incomplete
        E-->>O: fail closed, no order placement
    else safe to trade
        loop approved intents
            E->>A: place_limit_order(intent)
            A-->>E: placement result
            E->>C: record submitted order locally
        end
        E->>A: get_account_snapshot(contract)
        A-->>E: fresh observed snapshot
        E->>Q: reconcile(contract, observed_snapshot)
        Q-->>E: reconciliation_after + policy
        E->>C: sync observed snapshot
        alt severe drift
            E-->>O: latch safety halt
        else ok/resync only
            E-->>O: return result
        end
    end
```

## Key design point

The engine does **not** trust local memory first. It restores from venue truth first, then decides.
