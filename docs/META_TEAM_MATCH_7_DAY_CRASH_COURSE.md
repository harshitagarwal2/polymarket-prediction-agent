# Meta Team Match 7-Day Crash Course

This is the shortest practical ramp for understanding a ranking feature infra team before talking to the hiring manager.

---

## Goal

By the end of 7 days, you should be able to explain:

- what this team likely does
- what the hard problems are
- what metrics matter
- what to ask the HM
- whether the role is closer to feature infra, model design, or inference/perf

---

## Day 1: Build the mental model

Learn:

- retrieval -> ranking -> serving
- where features sit in that pipeline
- why ads ranking is constrained by latency and scale

Output:

- draw one simple diagram: events -> features -> model -> serving -> logs/feedback

---

## Day 2: Learn feature infra basics

Learn:

- offline vs online features
- what a feature store does
- materialization
- point-in-time correctness

Output:

- explain in your own words why training-serving skew is bad

---

## Day 3: Learn freshness and realtime aggregation

Learn:

- freshness vs staleness
- TTL
- rolling windows / realtime aggregations
- late events / duplicate events / missing events

Output:

- describe one feature that must stay fresh and what happens if it goes stale

---

## Day 4: Learn serving constraints

Learn:

- p95/p99 latency
- fallbacks
- timeout behavior
- cost vs quality tradeoffs
- why average latency can hide bad tail behavior

Output:

- explain why a slightly simpler feature may be better than a richer but slow one

---

## Day 5: Learn the advanced feature types

Learn:

- sequence features
- graph features
- embeddings at a high level

Output:

- explain sequence learning here as “ordered user/ad history features at scale,” not just model theory

---

## Day 6: Prepare for the HM discussion

Learn:

- team influence vs seat influence
- platform ownership vs support work
- what good E5 / E6 scope sounds like

Output:

- write your top 8 HM questions

---

## Day 7: Synthesize and rehearse

Do:

- read `docs/META_TEAM_MATCH_HM_ONE_PAGER.md`
- read `docs/META_TEAM_MATCH_HM_SCRIPT.md`
- skim `docs/META_TEAM_MATCH_GUIDE.md`
- practice your 60-second framing out loud

Suggested framing:

> I’m newer to ML infra specifically, but my understanding is that this team’s hardest problems are around freshness, consistency, latency, observability, and cost for ranking features at scale. That’s the layer I’ve been ramping on.

---

## Meta Engineering posts to read first

1. Sequence learning for personalized ads recommendations  
   https://engineering.fb.com/2024/11/19/data-infrastructure/sequence-learning-personalized-ads-recommendations/

2. Machine learning prediction robustness at Meta  
   https://engineering.fb.com/2024/07/10/data-infrastructure/machine-learning-ml-prediction-robustness-meta/

3. Tail utilization in ads inference at Meta  
   https://engineering.fb.com/2024/07/10/production-engineering/tail-utilization-ads-inference-meta/

4. Meta Andromeda: next-gen personalized ads retrieval engine  
   https://engineering.fb.com/2024/12/02/production-engineering/meta-andromeda-advantage-automation-next-gen-personalized-ads-retrieval-engine/

5. Adaptive Ranking Model: bending the inference scaling curve  
   https://engineering.fb.com/2026/03/31/ml-applications/meta-adaptive-ranking-model-bending-the-inference-scaling-curve-to-serve-llm-scale-models-for-ads/

---

## What to ignore at first

Unless the HM says the role is model-heavy, do **not** start with:

- advanced ranking loss theory
- deep recommender papers
- low-level kernel optimization details

Start with:

- feature lifecycle
- correctness
- latency
- reliability
- cost

---

## Minimum bar to sound credible

You should be able to explain:

- offline vs online features
- training-serving skew
- freshness / staleness
- p99 latency
- fallback behavior
- why sequence features matter
