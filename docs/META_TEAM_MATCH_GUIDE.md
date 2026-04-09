# Meta Team Match Guide: Ranking & Foundational AI / Feature Scalability

This note captures the main guidance from our discussion so you can refer back to it before talking with the hiring manager.

---

## 1. Bottom line

This team looks like a **good E5 team with real E6 upside** **if** the role gives you ownership of a clear platform surface with visible metrics.

The highest-upside version of this role is:

- central to Meta monetization / ads ranking
- platform-oriented, not just support work
- measured by clear infra and business-adjacent metrics
- broad enough to create cross-team influence

The lower-upside version is:

- support-heavy
- diffuse ownership
- partner teams get the visible credit
- you help many teams but do not control roadmap or standards

---

## 2. What this team likely does

This is best understood as **ML feature infrastructure for ads ranking**, not pure model research.

In plain English, the team likely sits between:

- raw user/ad/content events
- feature generation and storage
- model training and model serving
- ranking quality / latency / cost / reliability goals

Core areas mentioned in the description:

- **Feature generation**: turning logs, user actions, content, and context into model inputs
- **Feature scalability**: making those features usable across many model teams and large traffic volumes
- **Sequence learning**: using ordered history, not just aggregate counters
- **Realtime aggregations**: rolling windows like clicks in the last X minutes/hours
- **Feature freshness**: ensuring features are recent enough to matter
- **Hybrid online/offline generation**: some features are batch-produced, others computed in realtime
- **Graph learning**: relationship-based features between users, ads, content, entities
- **Adaptive inference**: managing quality vs latency vs compute cost
- **Agentic ecosystem**: internal AI-assisted tools and infra workflows

Good mental model:

> This is a distributed systems + data correctness + low-latency serving job with ML customers.

---

## 3. Why it can be strong for E5 -> E6

This kind of team can be especially strong because it has:

- **business centrality**: ads/monetization is close to Meta's core revenue engine
- **platform leverage**: improvements can help many ranking models and partner teams at once
- **cross-functional reach**: ML, Product, Data Science, and Core Infra all depend on the platform
- **promotable scope**: migrations, standards, cost wins, reliability improvements, platform adoption

The key condition is that your role must have:

- a named area of ownership
- metrics leadership cares about
- visible cross-team influence
- ability to set or shape direction, not just support others

---

## 4. What makes the role weaker

The risk pattern is:

- high-influence org
- low-influence seat

That usually means:

- you mostly support researchers or model teams
- you help integrate features but do not own the platform roadmap
- you debug and unblock but do not get durable attribution
- impact is real but hard to translate into promotion evidence

Simple rule:

- **High-influence seat** = ownership, metrics, decisions, migrations, standards
- **Low-influence seat** = collaboration, support, integration, hidden plumbing

---

## 5. What the role is **not**

It is important to separate three related but different roles:

### A. Feature infra / platform role

Works **before and around the model**.

Main problems:

- feature generation
- freshness
- training/serving consistency
- storage and serving
- reliability and latency
- platform adoption

Closest to:

- distributed systems
- data infrastructure
- backend platform engineering

### B. Ranking-model design / research-heavy role

Works **on the model itself**.

Main problems:

- model architecture
- loss/objective design
- embeddings and ranking quality
- offline/online evaluation

Closest to:

- applied ML / recommender systems research

### C. Low-level C++ inference role

Works **under the model runtime**.

Main problems:

- memory layout
- kernel optimization
- quantization
- CPU/GPU/accelerator efficiency
- hot-path latency

Closest to:

- performance engineering
- systems/runtime/kernel work

Why this matters:

- **Feature infra** is usually a better ramp for a strong backend engineer
- **Research-heavy ranking** needs more ML intuition from day one
- **Low-level inference** needs stronger C++/perf expertise from day one

---

## 6. Major problems this team likely works on

If this is truly a feature scalability / ranking foundations team, the hard problems are usually:

1. **Feature freshness**  
   Stale signals reduce ranking quality.

2. **Training-serving skew**  
   Features used in training differ from what production serves.

3. **Online/offline consistency**  
   Batch, streaming, and request-time feature paths diverge.

4. **Realtime aggregation correctness**  
   Late events, duplicates, missing events, windows, and event-time complexity.

5. **Tail latency (p95/p99)**  
   Slow requests matter more than average latency in ranking systems.

6. **Cost vs quality tradeoffs**  
   Better/richer features can increase compute/storage/serving cost.

7. **Stateful sequence features**  
   Keeping ordered user/ad history queryable at large scale is hard.

8. **Reliability / fallback design**  
   What happens when features are stale, missing, corrupt, or too slow?

9. **Observability**  
   You need to diagnose whether quality drops come from models, features, infra, or experiments.

10. **Platform adoption**  
    Even good infrastructure fails if model teams cannot use it safely and easily.

---

## 7. Likely PSC / success criteria and metrics

Your likely personal success criteria at E5 would be:

> Own a real platform area and move visible technical and business-adjacent metrics.

Important metric buckets:

### Business / model impact

- CTR
- CVR
- revenue lift
- ranking quality
- experiment win rate

### Latency / reliability

- p50 / p95 / p99 latency
- timeout rate
- error rate
- SLO attainment

### Feature quality

- freshness lag
- stale feature rate
- missing feature rate
- training-serving skew
- coverage / correctness

### Efficiency / infra health

- throughput
- cost per request / inference / feature generation
- CPU/GPU utilization
- storage/compute savings

### Platform leverage

- adoption by model teams
- onboarding speed
- launch velocity
- time to production for new features

For E6, the bar usually becomes:

- wider platform ownership
- more cross-team influence
- clearer technical direction-setting
- visible tradeoff ownership across cost / quality / latency / reliability

---

## 8. How LLMs might be used here

LLMs are probably **not** primarily used as direct chat-style ranking engines.

More likely modes:

### A. Offline or nearline feature enrichment

Examples:

- embeddings
- semantic content understanding
- topic/entity extraction
- quality/safety signals

This is the most natural fit for a feature platform team.

### B. Online ranking-time inference

The LLM or foundation model contributes in the serving path.

This creates harder problems around:

- p99 latency
- caching
- fallbacks
- serving capacity
- GPU/accelerator costs

### C. Internal agentic tooling

LLMs may be used for:

- experiment assistance
- debugging and analysis
- optimization workflows
- developer productivity

Good question for the HM:

> Are LLMs here mostly used for offline feature enrichment, online ranking-time inference, or internal agentic tooling?

Interpretation:

- **Mostly offline enrichment** -> likely core feature infra
- **Mostly online inference** -> more serving/perf/C++ heavy
- **Mostly agentic tooling** -> more internal AI tooling than core ranking platform

---

## 9. How to think about cross-functional collaboration

The line:

> Collaborate daily with ML, Product, Data Science, and core Infrastructure partners

usually means the team sits at the center of platform execution, but you need to determine whether the team has **decision rights** or is just a glue layer.

### ML partners

They want:

- better features
- more experimentation speed
- better model quality
- support for new paradigms like sequence or graph features

Good sign:

- ML teams are customers of your platform.

Bad sign:

- infra is just a ticket queue for researchers.

### Product partners

They care about:

- ad quality
- user experience
- advertiser outcomes
- monetization performance

Good sign:

- Product pressure influences priorities, but the team still owns the platform roadmap.

Bad sign:

- constant reactive fire drills and no strategic control.

### Data Science partners

They care about:

- experiment results
- lift measurement
- guardrails
- quality validation

Good sign:

- DS helps prove your team’s impact with metrics.

Bad sign:

- DS owns the impact story and infra contributions get buried.

### Core Infrastructure partners

They provide lower-level systems:

- compute
- storage
- serving infra
- networking
- hardware platforms
- observability

Good sign:

- your team shapes requirements upward and is not just blocked by other infra groups.

Bad sign:

- your team is mostly glue between other orgs.

Best interpretation question:

> In these daily collaborations, who is this team serving, and where does it actually make the final technical call?

---

## 10. How to assess team influence

For an infra/platform team, influence means:

> Other important teams cannot ship, scale, or hit efficiency goals without you, and leadership tracks your metrics and decisions.

But there is a big difference between:

- **high-influence team**
- **high-influence seat on that team**

### Signs of a high-influence team

- multiple important orgs depend on them
- leadership tracks their metrics regularly
- they own a control point in the stack
- they can force migrations or define standards
- they shape platform defaults

### Signs of a low-influence seat on a high-influence team

- role is support-heavy
- ownership is diffuse
- work is mostly integration and unblocker tasks
- partner teams present the impact upward
- the team is central, but your seat is not

---

## 11. How to test influence in the HM conversation

Use these checks:

### Ask who depends on them

> If this team slipped by a quarter, which orgs or launches would feel it?

Strong answer:

- specific teams, launches, dependencies, and business impact

Weak answer:

- “lots of teams use us” with no examples

### Ask what they actually control

> What hard part of the stack does this team own?

Strong answer:

- feature freshness, serving path, launch gate, cost efficiency, consistency, core platform surface

Weak answer:

- tooling/support with no real control point

### Ask if they set standards or just support others

> Can the team drive migrations, deprecate old patterns, or set platform defaults?

Strong answer:

- yes, they can shape how others build

Weak answer:

- mostly reactive to partner requests

### Ask how leadership sees them

> What metrics does leadership track for this team?

Strong answer:

- clear recurring metrics tied to cost, latency, freshness, adoption, launch speed, or business outcomes

Weak answer:

- only “helping others” metrics or no concrete KPI

### Ask for promotion examples

> What did a recent E5 project and recent E6 promotion from this team look like?

Strong answer:

- concrete examples with platform ownership, cross-team influence, and visible metrics

Weak answer:

- vague examples, old stories, or “people often switch teams for E6”

---

## 12. Answers to listen for when asking about recognition

Question:

> How do you make sure infra work gets recognized, not hidden behind partner teams?

### Strong answers sound like

- “We tie infra work to concrete metrics.”
- “Engineers own named platform areas.”
- “We make impact visible through reviews, dashboards, launch docs, and postmortems.”
- “Cross-team work has explicit ownership attribution.”
- “Here’s a recent example where an infra engineer got visible credit.”

### Medium answers

- “Our work is naturally visible because it is important.”
- “Good work gets noticed.”

### Red flags

- “Recognition just happens.”
- “Researchers or product teams usually present impact, but people know who helped.”
- “We do not really separate ownership because everything is collaborative.”

Great follow-up:

> Can you walk me through a recent infra project and how the owning engineer’s impact was documented upward?

---

## 13. Best wording to understand your role specifically

These questions help separate team influence from seat influence:

1. **What exact area would I own in the first 6–12 months?**
2. **What metrics would I personally be expected to move?**
3. **Can you share a recent E5 project from this team?**
4. **Who presents the impact upward?**
5. **If a partner team disagrees, can this team set the default?**
6. **How much of the roadmap is proactive platform strategy vs partner-driven requests?**

Simple decision rule:

- if the HM keeps talking about **ownership, metrics, decisions, migrations, standards**, that is a good sign
- if the HM keeps talking mostly about **collaboration, support, and helping many teams**, be cautious

---

## 14. Beginner-friendly mental model for ramping

If you do not come from ML infra, the best way to ramp is:

> Learn the feature lifecycle and serving constraints before trying to learn all of recommender-system theory.

What to focus on first:

1. retrieval -> ranking -> serving architecture
2. offline vs online features
3. feature stores and point-in-time correctness
4. freshness, staleness, TTLs, and realtime windows
5. training-serving skew
6. p95/p99 latency and fallback behavior
7. cost vs quality tradeoffs
8. sequence and graph features

What to **ignore first** unless the HM says the role is model-heavy:

- deep ranking-loss theory
- advanced recommender papers
- deep model architecture details

Minimum bar to sound credible:

- explain offline vs online feature generation
- explain why skew is dangerous
- explain why fresher but simpler features can beat richer stale features
- explain why p99 and fallbacks matter
- explain sequence learning as ordered history features at scale

Useful honest framing for the HM:

> I’m newer to ML infra specifically, but my read is that this role is about building a trustworthy feature data plane for ranking — freshness, consistency, observability, latency, and cost. That’s the lens I’m using to ramp.

---

## 15. Likely tech stack / concepts to be ready for

### Likely languages

- Python
- C++
- possibly accelerator-adjacent work in hot paths

### Likely systems concepts

- PyTorch / TorchRec-style recsys thinking
- distributed training/serving
- online feature stores / KV-like stores
- offline historical stores / warehouses
- event pipelines and stream processing
- backfills and replays
- observability, SLOs, rollout safety

### Likely operational concerns

- p95/p99 latency
- timeouts and fallbacks
- throughput and scaling
- hardware efficiency
- cost per request / cost per model path
- train/serve consistency
- feature quality / missingness / staleness

### What to know for the HM chat

- how the ranking funnel works
- why feature correctness and freshness matter
- why tail latency matters
- why richer features can raise cost
- how platform teams create leverage for many model teams

---

## 16. Hiring Manager matrix

Use this as a live scorecard. Score each row:

- **2 = strong**
- **1 = mixed**
- **0 = weak / vague**

| Area | Ask this | Strong answer (2) | Weak / red flag (0-1) | Why it matters |
|---|---|---|---|---|
| Team mission | What is the core problem this team exists to solve? | Clear charter: freshness, sequence feature infra, consistency, efficiency | Buzzwords only | Tells you if the team has a real mission |
| Your ownership | What exact area would I own in the first 6–12 months? | Named surface area with boundaries | “You’ll help across a bunch of things” | Biggest filter for seat quality |
| Metrics / PSC | What are the top metrics this team is judged on, and what would mine be? | Freshness, p99, cost, adoption, launch speed, quality lift | “Mostly unblocking others” | Good teams have legible impact |
| Team influence | If this team slipped by a quarter, who would feel it? | Named orgs, launches, concrete consequences | Vague dependency claims | Measures real centrality |
| Seat influence | Where does this role make decisions versus supporting partners? | Ownership of tradeoffs, standards, migrations | Mostly support/integration | Distinguishes strong seat vs hidden plumbing |
| Roadmap control | How much of the roadmap is proactive strategy vs incoming partner requests? | Team sets direction | Mostly reactive | Hard for E6 growth if reactive |
| Cross-functional collaboration | How do ML, Product, DS, and Core Infra each interact with this team? | Clear partner roles and clear team ownership | “We collaborate with everyone” | Collaboration only helps if ownership is clear |
| Credit / visibility | How is infra work made visible and recognized? | Dashboards, reviews, docs, launch artifacts, attribution | “People know who did it” | Infra can be invisible without explicit recognition |
| E5 -> E6 growth | What did a recent E5 project and recent E6 promotion from this team look like? | Concrete examples | No examples or only team-switch examples | Best reality check for promotion path |
| Day-to-day work | What does a normal week look like for this role? | Mix of build/design/debug/partner work | Mostly meetings/firefighting/support | Shows whether the role is strategic |
| Tech stack / ramp | How much Python vs C++? How much ML depth is needed day one? | Clear split and realistic ramp expectations | Immediate deep expertise expected with no ramp | Helps you assess fit honestly |
| Infra pain points | What are the main incidents or failure modes today? | Clear issues like skew, freshness, latency, cost | “Nothing major” / vague | Good managers know the team’s pain |
| LLM usage | Are LLMs used mainly for offline enrichment, online inference, or internal tooling? | Clear primary mode tied to real systems | “All of the above” with no detail | Tells you what the job really is |
| Manager quality | How do you help people grow and get scope? | Intentional staffing, examples, sponsorship | “Good work gets noticed” | Strong HM matters a lot |

### How to interpret the score

- **24–28**: very strong team + strong seat
- **18–23**: promising; likely good E5 scope, inspect weak spots
- **12–17**: mixed; could be support-heavy or vague
- **<12**: likely weak scope / reactive role / unclear growth path

### Fast red flags

- “You’ll work across many things.”
- “We collaborate with everyone.”
- “Impact is mostly helping partner teams.”
- “Good work naturally gets recognized.”
- “E6 usually depends on the right opportunity.”

### Best closing question

> If I joined and did really well, what specific kind of project here would make you say I’m on an E6 trajectory?

This usually reveals:

- scope
- ownership
- metrics
- visibility
- promotion realism

---

## 17. Meta Engineering blog posts to read

Start here:

1. **Sequence learning for personalized ads recommendations**  
   https://engineering.fb.com/2024/11/19/data-infrastructure/sequence-learning-personalized-ads-recommendations/

2. **Machine learning prediction robustness at Meta**  
   https://engineering.fb.com/2024/07/10/data-infrastructure/machine-learning-ml-prediction-robustness-meta/

3. **Tail utilization in ads inference at Meta**  
   https://engineering.fb.com/2024/07/10/production-engineering/tail-utilization-ads-inference-meta/

4. **Meta Andromeda: next-gen personalized ads retrieval engine**  
   https://engineering.fb.com/2024/12/02/production-engineering/meta-andromeda-advantage-automation-next-gen-personalized-ads-retrieval-engine/

5. **Adaptive Ranking Model: bending the inference scaling curve**  
   https://engineering.fb.com/2026/03/31/ml-applications/meta-adaptive-ranking-model-bending-the-inference-scaling-curve-to-serve-llm-scale-models-for-ads/

Nice extras:

6. **Journey to 1000 models: scaling Instagram’s recommendation system**  
   https://engineering.fb.com/2025/05/21/production-engineering/journey-to-1000-models-scaling-instagrams-recommendation-system/

7. **REA / KernelEvolve** for the newer AI-native / agentic angle

---

## 18. Simple 3-week ramp plan

### Week 1: system mental model

Learn:

- retrieval -> ranking -> serving
- what a feature store is
- offline vs online features

Goal:

- be able to explain the end-to-end flow in 5 minutes

### Week 2: correctness and freshness

Learn:

- point-in-time correctness
- training-serving skew
- freshness / staleness / TTLs
- materialization and realtime windows

Goal:

- explain why training/serving consistency is hard and important

### Week 3: reliability and advanced signals

Learn:

- p95/p99 latency
- fallbacks
- cost vs quality tradeoffs
- sequence features
- graph features

Goal:

- talk confidently about production ranking infra constraints

---

## 19. High-value questions to ask the HM

If you only ask a few, ask these:

1. **What exact area would I own in the first 6–12 months?**
2. **What are the top 3 metrics this team is judged on?**
3. **What kind of work on this team puts someone on an E6 trajectory?**
4. **How do you make sure infra work gets recognized and not hidden behind partner teams?**
5. **What are the biggest technical pain points today: freshness, skew, latency, cost, reliability, or platform adoption?**
6. **Are LLMs here mostly used for offline feature enrichment, online ranking-time inference, or internal agentic tooling?**
7. **How much is Python vs C++ in practice?**
8. **How much of the roadmap is proactive platform strategy vs reactive partner requests?**

---

## 20. Personal reminder before the HM call

You do **not** need to pretend to already be an ML infra specialist.

A strong honest framing is:

> I’m newer to ML infra specifically, but I’m mapping this role as a large-scale systems and feature-platform problem: freshness, consistency, observability, latency, and cost. That’s what I’m ramping on first.

That framing is credible, grounded, and aligned with the likely shape of the role.
