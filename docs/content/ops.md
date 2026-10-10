---
sidebar:
  order: 3
---

# Two Tiers of Ops

This split is the whole reason the manager is reusable — see `PROTOCOL.md` for the authoritative table.

- **Structural ops** — `REGISTER`, `HEARTBEAT`, `QUIESCE`, `QUIESCED`, `DEPLOY`. The only ops the manager acts on. It `switch`es on these to drive registration, liveness, the generation handoff, and rollouts.
- **Business ops** — everything else (`SETUP_TICKETS`, `GET_FORUM_TAGS`, …). **Opaque** to the manager. It checks the op name against an allow-list, then forwards the frame to the target cluster **without ever decoding the payload**. A different bot supplies a completely different business-op vocabulary with zero manager code changes.

## Validation split

Two places validation happens, two places it must not:

- The **manager** checks each inbound op against the allowed-op list (structural always allowed; business ops from config). Unknown/misspelled op → rejected here with `{ok:false, error}` instead of being silently forwarded. This list is **flat op-name strings only**.
- The **cluster** validates the _payload_ at its dispatch boundary (Zod), because that's where the payload is consumed and where the bot's types live.
- The manager **never** validates, parses, or learns payload shapes. Putting payload schemas in the manager would re-couple it to one bot's domain and defeat the reuse design.
