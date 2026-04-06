# Operator Runbook

This runbook explains how to operate the prediction-market agent in a **supervised 24/7 posture**.

## What this bot is today

This workspace is a **supervised trading agent lab**, not a fire-and-forget profit machine.

It can:

- restore/persist safety state
- refresh account truth and fail closed when truth is incomplete
- discover and rank opportunities
- apply deterministic sizing and policy vetoes
- run preview or live cycles
- pause, halt, resume, cancel-all, and cancel stale orders
- journal cycles and summarize them
- correlate related events within a polling cycle via `cycle_id`
- trace individual events via `event_id`

It should still be treated as **operator-supervised**.

## Quick reference — pause vs halt vs resume

Use this when you do not have time to reread the whole runbook.

### Continue supervising normally

Use normal supervision when:

- account truth is complete
- engine is not halted
- policy gate is rejecting only routine low-quality candidates
- no unexplained position/fill drift exists

### Pause

Pause when:

- you want to stop new decisions temporarily
- you are doing manual maintenance
- you want housekeeping to continue but do not want new entries

Command:

```bash
python3 scripts/operator_cli.py pause --state-file runtime/safety-state.json --reason "manual maintenance"
```

### Halt

Halt is not a command you choose casually; it is the safety posture when trust is broken.

Treat the bot as halted when:

- reconciliation drift exists
- account truth is incomplete
- partial-fill / cancel-race state is unclear
- restart state does not match venue reality

In halted state, do **not** attempt to push new live runs.

### Resume

Resume only when:

- venue truth looks healthy again
- last truth summary is consistent
- journal shows no unresolved confusion about the contract you care about
- the clean-resume checks have enough fresh evidence

Resume is contract-scoped and should be supervised.

### If unsure

If you are unsure whether to continue or resume:

1. inspect `status`
2. inspect journal summary
3. pause if needed
4. cancel stale orders if needed
5. prefer no new risk over optimistic continuation

## Recommended operating stages

### Stage 1 — preview only

Use this first:

```bash
python3 scripts/run_agent_loop.py \
  --venue polymarket \
  --mode preview \
  --fair-values-file runtime/fair_values.json \
  --max-cycles 1
```

Do this until you trust:

- market discovery
- candidate ranking
- sizing
- policy vetoes
- journaling
- operator status output

### Stage 2 — supervised repeated preview

```bash
python3 scripts/run_agent_loop.py \
  --venue polymarket \
  --mode preview \
  --fair-values-file runtime/fair_values.json \
  --interval-seconds 15 \
  --max-cycles 100
```

Use this to validate 24/7 behavior without placing orders.

### Stage 3 — small supervised live run

Only after stage 2 is boring and explainable:

```bash
python3 scripts/run_agent_loop.py \
  --venue polymarket \
  --mode run \
  --fair-values-file runtime/fair_values.json \
  --interval-seconds 15 \
  --max-cycles 10
```

Use very small quantities and tight risk caps.

If you want the phase-1 Polymarket live user-state overlay during run mode, export the condition IDs before starting:

```bash
export POLYMARKET_LIVE_USER_MARKETS="<condition-id-1>,<condition-id-2>"
```

This overlay only accelerates open-order freshness. It does not replace REST reconciliation for balances, positions, or fills.

## Preflight checklist

Before any continuous run:

- `pip install -e .`
- venue deps installed if needed:
  - `pip install -e upstreams/py-clob-client`
  - `pip install -e upstreams/pykalshi`
- fair values file exists: `runtime/fair_values.json`
- safety state path chosen: `runtime/safety-state.json`
- journal path chosen: `runtime/events.jsonl`
- env vars present for the venue you intend to use
- continuous run entrypoint now fails fast if required credentials or the fair-values file are missing
- for Polymarket live user-state overlay, set `POLYMARKET_LIVE_USER_MARKETS` to the condition IDs you want the user stream to follow
- operator knows how to run:
  - `status`
  - `pause`
  - `unpause`
  - `resume`
  - `cancel-all`
  - `cancel-stale`

## Fair values file

The polling loop requires a JSON file:

```json
{
  "<market_key>": 0.61
}
```

Where `market_key` is typically:

- `symbol:yes`
- `symbol:no`

The current system does **not** generate fair values by itself yet. You must provide them.

## Core operator commands

### Inspect status

```bash
python3 scripts/operator_cli.py status --state-file runtime/safety-state.json
python3 scripts/operator_cli.py status --state-file runtime/safety-state.json --journal runtime/events.jsonl
python3 scripts/operator_cli.py status --state-file runtime/safety-state.json --venue polymarket --symbol <token-id> --outcome yes
```

For Polymarket, venue-backed `status` now also shows websocket live-state freshness/activity, subscribed condition IDs, and the deeper persisted-vs-venue reconciliation detail.

### Pause and unpause

```bash
python3 scripts/operator_cli.py pause --state-file runtime/safety-state.json --reason "manual maintenance"
python3 scripts/operator_cli.py unpause --state-file runtime/safety-state.json
```

### Resume after halt

```bash
python3 scripts/operator_cli.py resume --venue polymarket --symbol <token-id> --outcome yes --state-file runtime/safety-state.json
```

Resume remains contract-scoped and supervised. The command performs the engine-side clean-resume check and reports whether the halt can be cleared yet.

### Emergency cleanup

```bash
python3 scripts/operator_cli.py cancel-all --venue polymarket --symbol <token-id> --outcome yes
python3 scripts/operator_cli.py cancel-stale --venue polymarket --symbol <token-id> --outcome yes --max-order-age-seconds 30
```

### Review journal summary

```bash
python3 scripts/summarize_events.py --journal runtime/events.jsonl
```

## What blocks new trading decisions

The system will refuse new decisions when:

- engine is manually paused
- engine is safety-halted
- pre-scan global account truth is incomplete
- per-candidate preview fails policy gate

The policy gate can veto for:

- stale book
- unhealthy reconciliation
- thin liquidity
- wide spreads
- duplicate exposure
- too many open orders
- too much capital at risk
- unresolved partial fills
- cooldown violations

## What housekeeping can still run while paused

Paused means **no new scan/rank decisions**, but the loop may still:

- refresh account truth
- cancel stale orders
- write journal events

This is intentional.

## Halt vs pause

- **pause** = operator intent
- **halt** = system safety response to drift / unsafe truth

Do not treat them as the same thing.

## Resume procedure

If halted:

1. inspect `status`
2. inspect last truth summary
3. inspect journal summary
4. verify venue truth is healthy again
5. run the supervised operator resume command for the affected contract
6. require repeated fresh clean evidence before trusting it again

## Suggested 24/7 posture

For now, the safest 24/7 setup is:

1. **preview mode first**
2. event journal enabled
3. safety state persisted
4. operator CLI available in another terminal/session
5. frequent journal/status review
6. only short supervised live windows

## Missing before true unattended trading

- richer reconciled runtime truth from Polymarket
- better restart rebuild from exchange + journal history
- stronger partial-fill / cancel-race handling
- deeper live-state integration into the engine instead of adapter-local caching
- portfolio-level exposure model
- more trustworthy fair-value generation

Until those exist, treat this as **supervised automation**, not unattended trading.
