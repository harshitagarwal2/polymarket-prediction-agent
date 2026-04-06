# 02 — Container / Component View

This diagram answers: **what major modules exist inside the workspace, and what each one is responsible for?**

```mermaid
flowchart TB
    subgraph adapters[adapters/]
        types[types.py\nContract • OrderBookSnapshot • AccountSnapshot • FillSnapshot]
        base[base.py\nTradingAdapter protocol]
        poly[polymarket.py\nCLOB + Data API wrapper]
        kal[kalshi.py\nKalshi wrapper]
    end

    subgraph engine[engine/]
        runner[runner.py\nTradingEngine + EngineSafetyState]
        acct[accounting.py\nAccountStateCache]
        orders[order_state.py\nlocal projected orders]
        recon[reconciliation.py\ndrift detection + policy]
        strat[strategies.py\nFairValueBandStrategy]
        iface[interfaces.py\nStrategyContext]
    end

    subgraph risk[risk/]
        limits[limits.py\nRiskEngine + RiskLimits]
        cleanup[cleanup.py\ncancel/verify helper]
    end

    subgraph research[research/]
        paper[paper.py\nPaperBroker + fill realism]
        replay[replay.py\nReplayRunner]
        storage[storage.py\nParquet persistence]
        analysis[analysis.py / indexer.py\nresearch scaffolding]
    end

    base --> poly
    base --> kal
    types --> poly
    types --> kal
    types --> runner
    runner --> acct
    runner --> orders
    runner --> recon
    runner --> strat
    runner --> limits
    recon --> acct
    recon --> orders
    recon --> base
    replay --> paper
    replay --> strat
    replay --> limits
```

## Main idea

- `adapters/` = venue-specific truth gathering and order transport
- `engine/` = the live orchestrator and safety boundary
- `risk/` = independent gatekeeper
- `research/` = how you improve the bot without risking live capital
