# 08 — Runtime State and Artifacts

This diagram answers: **what persisted artifacts exist now, and what do they tell you after restart or incident review?**

```mermaid
flowchart TD
    subgraph capture[Capture substrate]
        raw[raw_capture_events]
        checkpoints[capture_checkpoints]
        health[source_health / source_health_events]
    end

    subgraph current[Projected current-state]
        compat[current/*.json]
        mappingmanifest[market_mapping_manifest.json]
        fvmanifest[fair_value_manifest.json]
        preview[preview_order_context.json]
        advisory[llm_advisory.json]
        metrics[runtime_metrics.json]
    end

    subgraph runtime[Supervised runtime]
        safety[safety-state.json]
        journal[events.jsonl]
    end

    subgraph research[Research artifacts]
        datasets[datasets/*]
        benchmark[benchmark-suite outputs]
        attribution[replay attribution outputs]
    end

    raw --> compat
    checkpoints --> compat
    health --> compat
    compat --> mappingmanifest
    compat --> fvmanifest
    compat --> preview
    compat --> advisory
    compat --> metrics
    compat --> safety
    fvmanifest --> journal
    preview --> advisory
    compat --> datasets
    datasets --> benchmark
    benchmark --> attribution
```

## Why this matters

If the process restarts, the system should not come back forgetful.

- the capture substrate preserves **what was seen and when**
- projected current-state artifacts preserve **what the deterministic builders and operator saw**
- safety state and journal preserve **what the runtime decided and why**
- dataset and benchmark artifacts preserve **how the offline evaluation path was produced**

Together these artifacts make the current architecture far easier to inspect than the earlier single-loop mental model.
