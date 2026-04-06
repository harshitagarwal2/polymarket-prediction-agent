# 08 — Runtime State and Artifacts

This diagram answers: **what persisted artifacts exist, and what do they tell you after restart or incident review?**

```mermaid
flowchart TD
    subgraph live[Live runtime]
        adapter[Adapter account snapshot]
        account[AccountStateCache]
        orders[OrderState]
        engine[TradingEngine / Polling loop]
    end

    subgraph persisted[Persisted artifacts]
        safety[safety-state.json\n- halt/pause state\n- clean resume streak\n- last truth summary]
        journal[events.jsonl\n- scan cycles\n- veto reasons\n- lifecycle actions\n- operator actions]
    end

    adapter --> account
    adapter --> orders
    account --> engine
    orders --> engine
    engine --> safety
    engine --> journal

    safety --> restart[Restart / status inspection]
    journal --> restart
```

## Why this matters

If the process restarts, the bot should not come back “forgetful.”

- `safety-state.json` carries forward the last known safety/truth posture
- `events.jsonl` carries forward the recent decision trail

Together they give the operator a much better starting point than raw memory.
