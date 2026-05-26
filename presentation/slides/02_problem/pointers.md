# Slide 2 — The Problem (target: 45s)

## Core message
Existing observability is **infrastructure-shaped**. It answers
"is the cluster up?" — not "did the data move correctly?"

## Talking points
1. Datadog / CloudWatch / Splunk = great at CPU/memory/latency.
2. Data teams need **business-level** signals: lineage, volumes, success rate.
3. Today: those signals live in scattered notebooks, ad-hoc print statements, or nowhere.
4. The quote ("I can prove my cluster was healthy…") is the bumper sticker — pause after it.

## Concrete examples to mention if asked
- A silent silver-table join that drops 30% of rows — no monitor catches it.
- A gold dashboard that hasn't refreshed in 12 hours — no freshness SLA.
- A failed job at 3 AM — root cause buried in driver logs.

## Transition
*"So we built a small library that fills exactly that gap…"* → slide 3.
