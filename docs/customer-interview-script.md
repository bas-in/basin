---
title: "Phase 0 customer interview script — wedge validation"
nav_section: meta
sidebar_position: 40
summary: "5-10 founder interviews to validate the Basin wedge (multi-project Postgres-compat HTAP) before committing 3-6 months to a hosted product. Question bank, facilitation guide, scoring rubric."
---

# Phase 0 customer interview script — wedge validation

## 1. Why we're doing this

The canonical failure mode for a platform-shift product is shipping into a market that doesn't exist. Every year, technically excellent databases fail not because the engineering was wrong but because the founders mistook "this is clever" for "this is what people will pay to use."

Basin has strong technical evidence: Vortex storage is 2–30x faster than Parquet on typical SaaS analytical shapes; the multi-project isolation primitives (per-project shard owners, eviction, memory budgets) are architecturally sound; ADR 0016's HTAP hot tier closes the write-latency gap that used to make columnar engines awkward for transactional workloads. The substrate is real.

What we do not have is strong customer evidence. We don't know which segment feels the pain most acutely, which specific workload justifies a DB migration, which switching costs are prohibitive, or what willingness-to-pay looks like when the ask is real money rather than a hobbyist side project. Five to ten 45-minute interviews with the right people will either confirm the wedge or surface the pivot before we spend three to six months building a hosted product on a faulty premise. Running these interviews is the cheapest insurance available.

---

## 2. Target persona

**Primary:** VP Engineering or technical co-founder at a B2B SaaS company.

Qualifying criteria:

- **Product type:** Per-project data is core to the product — CRM, billing platform, project management, AI tooling, audit/compliance tools. The data model is naturally per-customer, not a shared blob.
- **Current stack:** Postgres, Aurora, RDS, or Supabase. (Conversations with teams on MySQL or MongoDB are useful background but don't validate the pgwire compatibility moat.)
- **Scale:** 1,000–100,000 projects. Below 1k the pain is theoretical; above 100k the architecture is likely purpose-built (Planetscale-style shard orchestration, Vitess, etc.) and the wedge fit weakens.
- **Pain signal:** Has personally dealt with at least one of: slow project onboarding, noisy-neighbor incidents, unexpected DB cost spikes, or a shard migration project.

**Secondary (useful but not primary):** Staff or Principal Engineers who own the data layer at a qualifying company. They describe pain more precisely but lack purchasing authority; treat their input as corroboration.

**Out of scope for Phase 0:** Consumer apps, gaming, pure analytics warehouses, teams already on a sharded solution they're happy with. Don't waste slots on these — they will produce false-negative signal.

---

## 3. Sourcing — finding the 5–10

Start sourcing immediately; scheduling drag means 10 targets → 5–7 completed interviews over 2–3 weeks.

**Warm channels (highest conversion, start here):**

- Personal network. Ask in engineering Slack groups (Rands Leadership Slack, SaaS founders Slack, local engineering communities). A warm intro converts at 3–5x a cold message.
- Founder communities. YC alumni network, On Deck, Lenny's Slack, Indie Hackers (filter for B2B SaaS threads mentioning "database" or "scaling").

**Cold outreach (fill remaining slots):**

- LinkedIn Sales Navigator: filter Series A–C B2B SaaS, headcount 20–200, "VP Engineering" or "CTO" title. Personalize by referencing a publicly known scale story.
- YC W/S 2022–2025 batch directory: filter by SaaS tag. Many founders list their email; a short, honest cold note ("I'm building a multi-project DB and want to understand your pain before we build the wrong thing") converts better than a product pitch.
- Case studies published by Supabase, Neon, PlanetScale, Vercel. Companies whose engineering blog describes scaling pain are self-selecting signal — their VP Eng is literally on record saying the problem exists.

**Incentive:** Offer either $200 Amazon gift card or $500 of Basin Cloud credit on launch. Mention it in the ask. Founders are time-constrained; the incentive signals you value their time without making the conversation transactional.

---

## 4. Interview format

**Length:** 45 minutes. Hard stop — it respects their time and keeps scheduling friction low.

**Medium:** Video call (Zoom, Google Meet, or equivalent). Video matters — you read the moment someone lights up or goes flat.

**Recording:** Always ask for consent at the start. Say: "Do you mind if I record this? It's only for my own notes — I won't share the recording." Most engineers say yes. If they decline, take notes faster. Never record without consent.

**Three phases:**

| Phase | Time | Purpose |
|---|---|---|
| Intro + consent | 5 min | Establish rapport, confirm recording consent, let them describe their role |
| Their workload + pain | 30 min | Open-ended discovery — you listen 80%, speak 20% |
| Basin reaction | 10 min | Show one-paragraph description, collect honest reaction |

**Intro script (verbatim, not improvised):**

> "Thanks for making time. I'm building a database product aimed at multi-project SaaS — but before I build the wrong thing I want to understand how people are actually running their data infrastructure today. There's no product pitch in this call; I genuinely want to hear about your stack and where the pain is. Do you mind if I record for my own notes?"

---

## 5. Question bank — the 30-minute "their workload and pain" section

Run these as a conversation, not a checklist. Follow the energy. If they go deep on one question, let them run — that depth is signal. You don't need every question answered; you need to understand their world.

**Opening (set the stage):**

1. "Walk me through how your customers' data is stored today. One database everyone shares? Schema-per-project? Database per project? Something else?"

2. "How many projects are you at now? What's the largest project by row count or query volume? The smallest? Is there a lot of variance or are they roughly similar?"

**Operations and incidents:**

3. "What's a recent incident that woke you up at an inconvenient time? What was the root cause — was the database involved?"

4. "What does project onboarding look like operationally? Walk me through the steps. How long does it take from 'new customer signed the contract' to 'their data is live'? What can go wrong in that process?"

5. "If a project suddenly 10x'd their query volume — say, they ran a big report or exported everything — what happens to your other projects? Have you seen that in practice?"

**Cost and economics:**

6. "What does your database bill look like as a percentage of revenue? Is it a line item that comes up in planning discussions?"

7. "What's the biggest scale-related pain you would pay to remove if there were a product that solved it cleanly? Give me a number if you can."

**Switching and inertia:**

8. "Have you seriously considered switching databases — even just exploring alternatives? What prompted it? What stopped you?"

9. "If you could wave a wand and change one property of your database tomorrow — not replace it, just change one thing — what would it be?"

**Bar for a new tool:**

10. "What would you need to see in a new database to consider even running it on a side project or a non-critical workload?"

**Closing:**

11. "What am I not asking that I should be asking about your database situation?"

---

## 6. The 10-minute "Basin reaction" section

After the open-ended section, say:

> "Thanks — that's really helpful. I want to show you one paragraph about what we're building and get your honest reaction. I'm not pitching; I want to know what feels right and what feels wrong."

Then share your screen (or paste into chat) this description:

---

> "Basin is a Postgres-compatible database designed multi-project from day one. Every project gets per-project memory budgets, isolated quotas, and a columnar+row hybrid storage that handles both fast point queries and big analytical scans in one engine. Today it is 2–30x faster than Parquet on typical SaaS workloads, costs less to run because storage is bucket-native, and speaks pgwire so existing apps drop in. We are 8 weeks from beta."

---

Then ask, in this order:

- **"What's your reaction in one sentence?"** — Wait for them to answer before saying anything. The first unguarded sentence is the most valuable data point in the interview.
- **"What would you need to believe to try this on a side project or a non-critical service?"** — Surfaces their personal bar, not their company's procurement bar.
- **"What part of that description feels too good to be true?"** — Gives them permission to poke holes. Healthy skepticism is information; you want it surfaced here, not after they've soft-committed.
- **"Who else should I talk to? Do you know anyone else running multi-project Postgres at scale who might have opinions?"** — Never skip this. It turns 10 interviews into 20 leads.

---

## 7. What to listen for

Strong signal — increase conviction in current wedge:

- They mention multi-project pain **unprompted** in the open-ended section, before you've said anything about Basin.
- They ask mid-interview "could I try this?" or "when can I get access?"
- Their incident story maps precisely to noisy-neighbor degradation or project onboarding complexity.
- They describe DB cost as a top-3 operational concern without prompting.

Weak signal — the wedge may be right but you haven't found the right cohort yet:

- They describe a plausible multi-project architecture but say "our DB is fine."
- They acknowledge the pain exists but describe it as "a quarterly nuisance, not a daily fire."
- They react positively to the Basin description but pivot immediately to "we couldn't switch because of migrations."

Negative signal — reconsider the wedge:

- Multi-project pain is described as their #1 cost driver, but they say flatly they would not switch databases under any circumstances. (Switching cost is a real moat; if it's absolute, the wedge doesn't break through it.)
- Three or more interviewees independently name a completely different pain as the thing they'd pay to fix — see pivot triggers below.

**Pivot triggers:** If three or more interviewees independently name the same alternative pain as their primary — e.g., "actually our problem is vector search at scale," or "our real issue is real-time fan-out to millions of subscribers," or "we'd pay anything for affordable global replication" — that is a statistically meaningful signal. Flag it immediately, pause further interviews in the current script, and re-evaluate the wedge before booking more calls.

---

## 8. Scoring rubric

Score each interview immediately after it ends, while memory is fresh. Use whole numbers.

| Dimension | 1 | 3 | 5 |
|---|---|---|---|
| **Multi-project pain present?** | Not mentioned; they're happy with their stack | Acknowledged but described as low-priority | Unprompted, central to their narrative |
| **Postgres-compat valuable?** | Irrelevant; they'd switch parsers happily | Nice-to-have; reduces friction | Hard requirement; migration from pgwire is a non-starter |
| **Cost a major concern?** | DB cost is immaterial to planning | On the radar; comes up quarterly | Top-3 cost driver; actively trying to reduce it |
| **Willingness to try beta?** | No interest; not even curious | Would watch from the sidelines | Directly asked how to get access |

**Total: 4–20.** Record the four component scores plus a key quote and a one-line summary in your tracking sheet.

---

## 9. Decision criteria after 5–10 interviews

Evaluate after every five interviews; don't wait until all ten are done.

| Outcome | Action |
|---|---|
| 6 or more interviews scoring **15 or higher** | Wedge validated. Proceed with the current plan — hosted product, beta access, ICP targeting multi-project SaaS at 1k–100k projects. |
| 3–5 interviews scoring **15 or higher** | Wedge plausible but ICP is too broad or sourcing missed the right cohort. Narrow the ICP (e.g., restrict to 5k–50k projects, or focus on a specific vertical like billing platforms) and run a second round of 5 interviews before committing to a hosted product. |
| Fewer than 3 scoring **15 or higher** | Wedge invalid in current form. Pivot triggered. Review the pattern in low scores — is it missing pain, low switching willingness, or Postgres-compat irrelevance? That pattern determines the direction of the pivot. |

A score of 15 is not arbitrary: it requires pain to be genuinely present (≥3), Postgres-compat to matter (≥3), cost to be a concern (≥4), and some expressed willingness to try (≥5) — or similar combinations that add up to a real buyer, not a polite nod.

---

## 10. Anti-patterns — things that invalidate your signal

**Do not pitch in the open-ended section.** If you mention Basin, multi-project architecture, or bucket-native storage before the 30-minute mark, you prime the interviewee. They will tell you what you want to hear. The open-ended section must be 100% about their world.

**Do not ask leading questions.** "Don't you find multi-project databases frustrating?" is not a question; it is a suggestion. Use neutral language: "Walk me through what that looks like" rather than "So I imagine that must cause noisy-neighbor problems?"

**Do not skip the incentive mention.** Founders have competing priorities. Offering $200 or equivalent cloud credit is not bribery — it is acknowledging that 45 minutes of their time has real value. Leaving it out signals you don't understand how time-constrained founders are.

**Do not forget "who else?"** at the end of every interview. One warm introduction is worth three cold outreach attempts. You are building a referral chain, not a one-shot survey.

**Do not run more than two interviews per day.** By the third interview in a day you stop listening and start confirming. Earlier answers will color how you hear later ones. One per day is ideal; two is the ceiling.

**Do not aggregate qualitatively before you have five data points.** Pattern-matching on two interviews is confabulation. Hold your conclusions loosely until you have at least five complete scores.

---

## 11. Logistics and tracking

**Recording and notes:**

- Recording: Granola (recommended — auto-transcripts, meeting-native), Loom, or native Zoom + transcript export. If none are available, a shared Google Doc where a second person takes live notes is acceptable.
- Notes template: one Google Doc or Notion page per interview, with the scoring rubric pre-filled at the top so you complete it while closing the tab.

**Aggregate tracking sheet** — one row per interview, columns:

| Column | Notes |
|---|---|
| Date | ISO format |
| Company | |
| Persona | Title + whether VP/CTO/Staff |
| Source | How you got the intro |
| Multi-project pain (1–5) | |
| Postgres-compat (1–5) | |
| Cost concern (1–5) | |
| Beta willingness (1–5) | |
| Total (4–20) | |
| Key quote | Verbatim, one sentence |
| Pivot flag? | Y/N — did they name a different primary pain? |
| Follow-up | Promised access, referral contact, etc. |

**Anonymization:** Before sharing the aggregate sheet with advisors or investors, replace company names and personal names with anonymous codes (Interviewee A, Company 1, etc.). Founders talk to each other; a leaked "Company X said their DB cost is 22% of revenue" creates trust problems.

**Recording retention:** Keep recordings for 90 days, then delete unless the interviewee has explicitly consented to longer retention.

---

## 12. Timeline

| Week | Activity |
|---|---|
| Week 1, days 1–3 | Draft outreach messages (personalized per channel). Send 20–30 outreach messages / DMs. Book first 2–3 slots. |
| Week 1, days 4–7 | First 2–3 interviews. Review notes same day. Adjust question emphasis if early signal is weak. |
| Week 2 | Remaining 4–6 interviews, one per day. Follow-up on "who else?" referrals from week 1. |
| Week 3 (if needed) | Make-up interviews for no-shows; additional interviews if 3–5 scored 15+ and you're in the "narrow ICP" outcome. |
| End of week 3 | Score all interviews. Apply decision criteria. Write a one-page synthesis (key quotes, score distribution, ICP hypothesis, go/no-go recommendation). |

Scheduling drag is real — assume 40–50% of initial outreach results in a booked call, and 20% of booked calls reschedule at least once. Send more outreach than you think you need on day 1.

---

*This document is a living facilitation guide. Update it after the first two interviews if any question consistently produces low-signal responses.*
