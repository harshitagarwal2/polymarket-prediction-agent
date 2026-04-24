# 07 — Operator Control Plane

This diagram answers: **how does a human supervise the system while workers, builders, and runtime are active?**

```mermaid
flowchart LR
    operator[Operator]

    subgraph entrypoints[Entry points]
        runtime[run-agent-loop]
        cli[operator-cli]
        capture[run-sportsbook-capture\nrun-polymarket-capture]
        projector[run-current-projection]
    end

    subgraph artifacts[Persisted artifacts]
        safety[safety-state.json]
        journal[events.jsonl]
        advisory[llm_advisory.json]
        preview[preview_order_context.json]
        metrics[runtime_metrics.json]
        current[current/*.json compatibility exports]
    end

    subgraph services[Underlying services]
        captureSvc[Capture substrate]
        projectionSvc[Projection lanes]
        builderSvc[Deterministic builders]
        runtimeSvc[TradingEngine + AgentOrchestrator]
    end

    operator --> cli
    operator --> runtime
    operator --> capture
    operator --> projector

    capture --> captureSvc
    projector --> projectionSvc
    projectionSvc --> current
    builderSvc --> preview
    cli --> advisory
    runtime --> runtimeSvc
    runtimeSvc --> safety
    runtimeSvc --> journal
    runtimeSvc --> metrics
    cli --> safety
    cli --> journal
    current --> cli
    current --> builderSvc
```

## Main idea

- the operator supervises **more than one process now**: workers, projector, runtime, and CLI tooling
- the operator-facing control plane is built from persisted artifacts, not only from the live process memory
- advisory and preview artifacts stay operator-side and deterministic
