# Meta Team Match: Ownership vs Support Check

Use this to answer one question:

> Is this a durable 2026 ML infra seat with real platform ownership, or a support-heavy slice on an important team?

The domain itself can be strong in 2026. The bigger risk is role shape.

---

## 1. Bottom line

Treat this as a strong opportunity only if the seat owns a real platform surface with clear metrics, decision rights, and reusable leverage.

Be much more careful if the role sounds like:

- helping many teams but owning little,
- reacting to partner requests,
- doing migrations and unblocker work without roadmap control,
- or contributing impact that partner teams present upward.

---

## 2. Why this matters in 2026

The current shift toward LLMs and agents does **not** make ranking, retrieval, freshness, or serving infrastructure less relevant.

If anything, it makes the systems layer more important:

- retrieval and reranking still matter,
- fresh context and state still matter,
- online/offline consistency still matters,
- latency and cost matter even more,
- and platform teams that own reusable infra still have strong leverage.

The decision is usually **not** “good domain or bad domain.”

The real decision is:

> Will this seat let me own a real platform problem with visible metrics, or will I mostly support other teams?

---

## 3. Fast heuristic

### High-upside seat

- named platform surface or subsystem
- direct metrics like freshness, p99, cost, adoption, launch speed, or reliability
- ability to drive standards, interfaces, or migrations
- multiple teams depend on the work
- clear examples of new hires owning something meaningful
- infra impact is visible and attributable

### Lower-upside seat

- vague ownership
- “you’ll help across several areas” language
- mostly partner support or ticket intake
- roadmap shaped mainly by incoming asks
- lots of coordination, little technical control
- impact described as “enabling others” with no named metrics

---

## 4. The 6 categories to test

Use these six categories as your decision rubric.

1. **Ownership** - What exact surface would you own?
2. **Metrics** - What would success be measured by?
3. **Boundaries** - Where does this team decide versus support?
4. **Roadmap control** - Is the work proactive platform building or reactive intake?
5. **Platform leverage** - How many teams or launches depend on this work?
6. **Growth / attribution** - Does this kind of work turn into visible E5/E6 scope?

---

## 5. Recruiter questions

These should be lighter, faster, and designed to reveal whether the role is worth deeper evaluation.

### Ownership

**Ask:** If I joined, what kind of platform area would someone at my level usually own in the first 6–12 months?

### Metrics

**Ask:** What are the main metrics this team is judged on?

### Boundaries

**Ask:** Is this team mainly building core platform capabilities, or more often supporting model teams with integrations and unblockers?

### Roadmap control

**Ask:** Does the team mostly set its own platform roadmap, or is it mainly driven by incoming partner requests?

### Platform leverage

**Ask:** Are there multiple model teams or launches that depend on this team’s work?

### Growth / attribution

**Ask:** Do people on this team usually get clear ownership and visible scope, or is the work more collaborative/support-heavy?

---

## 6. Hiring manager questions

These are sharper. Ask for recent, concrete examples.

### 1. Ownership

**Ask:** What exact platform surface would you want me to own end to end by around month 6 or month 12?

**Strong answer:** Named system, service, feature path, serving path, or platform area with clear boundaries.  
**Weak answer:** “You’d help across a few things depending on priorities.”

### 2. Metrics

**Ask:** What are the top metrics for this team, and what would mine likely be if I joined?

**Strong answer:** freshness, p95/p99 latency, cost efficiency, launch speed, reliability, adoption, model lift.  
**Weak answer:** mostly soft success language or only “helping other teams move faster.”

### 3. Boundaries / decision rights

**Ask:** Where does this team make the final technical call, and where does it mainly support partner teams?

**Strong answer:** clear decision rights on architecture, standards, migrations, interfaces, or platform policy.  
**Weak answer:** “It depends” or “ownership is shared across everyone.”

### 4. Roadmap control

**Ask:** How did the team’s time split last quarter between proactive platform work, maintenance/oncall, migrations, and partner-request work?

**Strong answer:** concrete percentages or at least a clear split with examples of proactive work.  
**Weak answer:** vague answer, or mostly reactive intake and support.

### 5. Platform leverage

**Ask:** If this team slipped by a quarter, which teams, launches, or metrics would feel it?

**Strong answer:** names specific orgs, launches, dependencies, and consequences.  
**Weak answer:** “Lots of teams use us” with no specifics.

### 6. Growth / attribution

**Ask:** What did a recent E5 project and a recent E6-level project from this team look like?

**Strong answer:** concrete projects with ownership, metrics, cross-team influence, and visible outcomes.  
**Weak answer:** no clear examples, or growth depends on switching teams.

---

## 7. Extra questions if the role sounds promising

Use these only if the first pass sounds good.

- What part of the stack does this team directly pager for?
- What services, APIs, stores, or pipelines does this team fully own?
- Can the team deprecate old patterns and drive migrations, or does it mostly recommend?
- How is infra work made visible in reviews and promotion packets?
- What are the hardest open problems right now: freshness, skew, latency, cost, reliability, or platform adoption?
- When you say “LLM” or “agentic,” do you mean core serving/context plumbing or mostly internal productivity tooling?

---

## 8. Red flags to watch for

Be cautious if you hear repeated versions of these:

- “We work with everyone.”
- “Ownership is shared.”
- “The roadmap depends on partner asks.”
- “New hires usually start by helping across a few projects.”
- “We don’t really track metrics at the individual level.”
- “Impact is obvious internally” without examples of attribution.
- “A lot of the job is coordination.”
- “You’ll learn the space by taking whatever comes in.”

None of these alone kills the role, but several together usually mean the seat is weaker than the team’s reputation.

---

## 9. Quick scorecard

Score each category:

- **2 = strong**
- **1 = mixed**
- **0 = weak / vague**

### Score sheet

- Ownership: __ / 2
- Metrics: __ / 2
- Boundaries: __ / 2
- Roadmap control: __ / 2
- Platform leverage: __ / 2
- Growth / attribution: __ / 2

**Total:** __ / 12

### Interpretation

- **10-12:** strong seat; likely worth leaning in
- **7-9:** promising, but inspect weak spots carefully
- **4-6:** mixed; meaningful support-heavy risk
- **0-3:** likely weak seat even if the team sounds important

---

## 10. Final decision rule

You should feel good about this role if, after the conversations, you can say all of the following with confidence:

- I know what platform surface I would likely own.
- I know what metrics would prove success.
- I know where this team has real decision rights.
- I believe the team can shape the roadmap, not just react to it.
- I can see how this work creates reusable leverage across multiple teams.
- I can see how strong work here would translate into visible growth.

If you cannot say those things clearly, the domain may still be good, but the **seat** is probably not good enough.

---

## 11. Best way to use this doc

- **Recruiter call:** use Sections 3-5
- **HM call:** use Sections 6-9
- **Post-call decision:** use Section 10

If you want a broader live script, use `docs/META_TEAM_MATCH_HM_SCRIPT.md`.
If you want a detailed note-taking template, use `docs/META_TEAM_MATCH_CALL_NOTES_TEMPLATE.md`.
