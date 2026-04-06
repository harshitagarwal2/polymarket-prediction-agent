# 01 — System Context

This diagram answers: **what is this bot, what does it talk to, and what does it own?**

```mermaid
flowchart LR
    operator[Operator / Developer]

    subgraph external[External Systems]
        polyclob[Polymarket CLOB API\norders • trades • orderbooks]
        polydata[Polymarket Data API\npositions • value]
        kalshi[Kalshi API / WebSocket\nmarkets • portfolio • orders]
        signals[External Research Inputs\nnews • event data • model signals]
    end

    subgraph workspace[Prediction Market Agent Workspace]
        adapters[Adapters\nnormalize venue APIs]
        engine[Engine\nrun_once • preview_once • try_resume]
        risk[Risk\nlimits • fail-closed guards]
        research[Research\npaper broker • replay • storage]
        docs[Docs / Scripts\ndemos • diagrams • runbooks]
    end

    subgraph upstreams[Reference Upstreams - not product core]
        up1[poly-market-maker]
        up2[py-clob-client]
        up3[KalshiMarketMaker]
        up4[prediction-market-analysis]
        up5[pykalshi]
        up6[TradingAgents]
        up7[OpenBB]
        up8[qlib]
    end

    operator --> docs
    operator --> engine
    signals --> research
    research --> engine
    engine --> risk
    risk --> engine
    engine --> adapters
    adapters --> polyclob
    adapters --> polydata
    adapters --> kalshi
    upstreams -. reference / dependency .-> workspace
```

## Read this as

- **the workspace owns execution, risk, research, and docs**
- **venue connectivity is delegated to adapters**
- **upstream repos are sources of ideas or wrapped dependencies, not your permanent product core**
