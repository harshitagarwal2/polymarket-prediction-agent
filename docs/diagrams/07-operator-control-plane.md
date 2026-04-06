# 07 — Operator Control Plane

This diagram answers: **how does a human supervise the agent while it runs?**

```mermaid
flowchart LR
    operator[Operator]

    subgraph scripts[Scripts]
        loop[run_agent_loop.py]
        cli[operator_cli.py]
        summary[summarize_events.py]
    end

    subgraph runtime[Runtime Artifacts]
        safety[safety-state.json]
        journal[events.jsonl]
    end

    subgraph engine[Engine]
        poll[PollingAgentLoop]
        orch[AgentOrchestrator]
        run[TradingEngine]
    end

    operator --> loop
    operator --> cli
    operator --> summary

    loop --> poll
    poll --> orch
    orch --> run

    cli --> safety
    run --> safety
    orch --> journal
    cli --> journal
    summary --> journal
    cli --> run
```

## Main idea

- `run_agent_loop.py` drives repeated cycles
- `operator_cli.py` changes state and inspects runtime
- `safety-state.json` persists halt/pause/truth summary
- `events.jsonl` persists cycle-level decisions and operator actions
