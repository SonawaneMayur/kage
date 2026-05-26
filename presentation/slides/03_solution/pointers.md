# Slide 3 — How KAGE Works (target: 60s)

## Core message
Tiny surface area, lakehouse-shaped output.

## Talking points
1. **3 event types** — that's the entire data model. Easy to remember.
2. **2 APIs** — `KageLogger` for explicit control, `@` decorators for
   zero-boilerplate. Use either, mix freely.
3. **JSONL on disk** — no daemon, no agent, no separate database.
4. **Partitioned path** = direct SQL: `SELECT * FROM json.\`…/event_type=job_run/\``.
5. **Medallion-aware** — every event carries `layer`, so lineage and
   survival-rate metrics fall out for free.

## If asked about architecture
Point to `docs/architecture.html` — six-layer stack:
User Code → API → Core → Transports → Storage → Analytics.

## Transition
*"Let's see the declarative version — this is where it gets fun."* → slide 4.
