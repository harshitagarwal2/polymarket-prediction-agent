# 02 — Container / Component View

This diagram answers: **what major modules exist inside the workspace, and what each one is responsible for?**

```mermaid
flowchart TB
    subgraph entrypoints[scripts/]
        loop[run_agent_loop.py]
        opcli[operator_cli.py]
        ingest[ingest_live_data.py]
        sbworker[run_sportsbook_capture.py]
        pmworker[run_polymarket_capture.py]
        projector[run_current_projection.py]
    end

    subgraph venue[adapters/]
        poly[adapters/polymarket/]
        kal[adapters/kalshi.py]
        types[adapters/types.py]
    end

    subgraph capture[services/capture/]
        sbsvc[sportsbook.py + worker.py]
        pmsvc[polymarket.py + polymarket_worker.py]
    end

    subgraph projectionSvc[services/projection/]
        proj[current_state.py + worker.py]
    end

    subgraph storage[storage/]
        read[current_read_adapter.py]
        preview[current_projection.py]
        journal[journal.py]
        stores[raw/ • parquet/ • postgres/]
    end

    subgraph domain[Domain layers]
        contracts[contracts/]
        forecasting[forecasting/]
        opportunity[opportunity/]
        execution[execution/]
        engine[engine/]
        risk[risk/]
        llm[llm/]
        research[research/]
    end

    entrypoints --> capture
    entrypoints --> projectionSvc
    entrypoints --> engine
    entrypoints --> llm
    entrypoints --> research
    venue --> engine
    capture --> stores
    projectionSvc --> stores
    projectionSvc --> read
    read --> execution
    read --> llm
    read --> ingest
    contracts --> forecasting
    forecasting --> opportunity
    opportunity --> execution
    execution --> llm
    engine --> risk
    engine --> journal
    engine --> read
    research --> forecasting
```

## Main idea

- `services/` + `storage/` now make the capture/projection/current-state substrate explicit
- `contracts/`, `forecasting/`, `opportunity/`, and `execution/` are first-class product layers, not just helpers under `research/`
- `engine/` remains the supervised runtime shell around adapters, policy, reconciliation, and persisted safety state
