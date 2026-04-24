# 01 — System Context

This diagram answers: **what does the current workspace talk to, and what are the major owned boundaries?**

```mermaid
flowchart LR
    operator[Operator / Developer]

    subgraph external[External systems]
        sportsbook[Sportsbook providers / feeds]
        gamma[Polymarket Gamma / catalog]
        marketws[Polymarket market websocket]
        userws[Polymarket user websocket]
        kalshi[Kalshi API / WebSocket]
    end

    subgraph workspace[Prediction Market Agent workspace]
        capture[Capture workers\nrun-sportsbook-capture\nrun-polymarket-capture]
        projection[Projection worker\nrun-current-projection]
        builders[Deterministic builders\nmappings • fair values • opportunities • datasets]
        runtime[Supervised runtime\nrun-agent-loop]
        control[Operator control\noperator-cli • advisory]
        research[Offline research\nbenchmark • replay • training]
    end

    subgraph substrate[Storage and artifacts]
        postgres[Postgres capture substrate\nraw_capture_events • checkpoints • source_health]
        current[Projected current-state tables\ncompatibility JSON in runtime/data/current]
        state[Safety / journal artifacts\nsafety-state.json • events.jsonl]
    end

    sportsbook --> capture
    gamma --> capture
    marketws --> capture
    userws --> capture
    kalshi --> runtime

    capture --> postgres
    postgres --> projection
    projection --> current
    current --> builders
    builders --> runtime
    current --> control
    runtime --> state
    control --> state
    research --> builders
    operator --> control
    operator --> runtime
```

## Read this as

- **capture and projection own the live data substrate**
- **deterministic builders turn projected state into mapping, fair-value, opportunity, and dataset artifacts**
- **`run-agent-loop` stays the supervised venue-facing runtime rather than becoming a generic replay of the projected opportunity table**
