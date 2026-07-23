# AMP Native — Vision, Boundaries, and Plan

This document supersedes `docs/AMP_HARNESSES_TASK.md` as the source of truth for
what we build and, just as important, what we explicitly do **not** build yet.

It is grounded in a substrate investigation (`.polly/substrate_report_part2.md`)
that mapped how every existing native agent actually works. The guiding
principle: **amp-native is a peer of the other native agents, built on top of
the same substrate. It is not a research project and it does not need to solve
problems the rest of the platform deliberately leaves unsolved.**

---

## 1. Vision

A **working, imperfect** `amp-native` that is a credible peer of `pi-native`
and `antigravity-native`: the user can launch Amp in the runner-owned terminal,
drive turns through Omnigent, see completed transcript and lifecycle status,
cancel, and resume a persisted thread — with the same reliability guarantees
those peers offer and no more.

"Working" is the bar. "Perfect" is explicitly not the bar.

---

## 2. Hard boundaries

### IN SCOPE — required for MVP parity

Grounded in the substrate report's calibration section:

1. **Runner-owned launch + resume** via persisted external `T-*` thread
   (`amp threads continue`).
2. **Prompt delivery** via the established bracketed-paste + separate `Enter`
   path.
3. **Executor seam** — registered harness + `Executor.run_turn()` + control
   hooks.
4. **Transcript + status reporting** — echoed user message, completed assistant
   text, and `running`/`idle`/`failed` lifecycle events through the shared
   session-event endpoint.
5. **Cancel** — interrupt routed to the resident thread, `thread.cancel()`,
   retained until cancellation takes effect.
6. **Basic MCP relay** — the shared relay infrastructure + per-agent config that
   pi-native and antigravity-native use.

### EXPLICITLY DEFERRED — "not yet," with rationale

These are **decisions, not omissions.** Each is parked until someone makes a
deliberate call to promote it. None blocks MVP parity.

| Deferred item | Why deferred (evidence) |
|---|---|
| Durable `DeliveryJournal` state machine + crash reconciliation (old 001) | **Substrate gap.** No peer has one. pi-native intentionally drops stale payloads on restart; antigravity-native has an intentional no-durable-cursor comment. |
| Exactly-once delivery, idempotency keys, server dedupe, durable outbox (old 008) | **Substrate gap.** The server itself has no dedupe; shared POST retry deliberately avoids retrying ambiguous writes. |
| Cross-process dedupe / send claims | No peer provides this. |
| Composer-readiness, paste, and post-submit verification | Hardening, not parity. |
| Cancel acknowledgements, grace periods, fallback termination, indeterminate state | Hardening, not parity. |
| Authoritative thread-ID conflict detection + recovery | Hardening, not parity. |
| Token-level streaming | Amp plugin API has no delta events (platform limit). Completed output only. |
| Structured tool / permission mirroring beyond basic relay | Hardening, not parity. |
| Wrapper-transport extraction (old 010) | Pure refactor; Pi-reuse works today. Not a precondition. |
| Crash / restart / duplicate / cancel-race E2E coverage | Hardening; deferred with the durability work it would test. |
| Authenticated qualification (old 007) | Requires real Amp credentials + Linux/macOS; manual-only, cannot be fully automated in CI. |

**Rule:** if a reviewer raises one of these during an MVP-parity review, it is a
`CONTRACT-GAP` for the backlog, never a blocking fix. See
`.polly/AGENT_PROTOCOL.md` §5.

---

## 3. Decision on the durability work (old AMP-NATIVE-001..010)

The prior task list was durability-heavy. Under the parity bar, almost all of
it lands in the DEFERRED table above. In particular:

- **AMP-NATIVE-001 (`DeliveryJournal`)** — recommend **revert / discard.** It is
  new substrate infrastructure for a property the platform deliberately does
  not provide, and its exactly-once invariant surface is the single largest
  source of review churn. Keeping it maintains unneeded complexity and a
  permanent scope-creep foothold.
- **AMP-NATIVE-010 (wrapper extraction)** — defer; refactor only.
- **AMP-NATIVE-007 (authenticated qualification)** — keep as a manual,
  creds-gated activity; not an automated task.

The human decides on the revert at the plan gate.

---

## 4. Phasing

- **Phase 0 — MVP parity.** Confirm the existing implementation actually works
  end-to-end, then close only the parity gaps found. Each task is small,
  single-concern, opens its own PR. Test-first for anything stateful
  (`.polly/AGENT_PROTOCOL.md` §2).
- **Phase 1+ — hardening menu (parked).** Items in the DEFERRED table above.
  Each requires an explicit promotion decision (and some require a *substrate*
  decision, not just an amp-native one) before any work starts. They are not
  sequenced; they are a menu.

No phase chases perfect. A Phase 0 task is "done" when it meets its stated
contract, not when the reviewer stops finding new edge cases.

---

## 5. Phase 0 atomic tasks

> These are derived from the documented gaps. **Task 0-Q runs first** and its
> findings confirm or refute the rest — we do not spec implementation in detail
> before we have ground truth on what is actually broken (that is what caused
> the prior drift).

| ID | Task | Non-goals |
|---|---|---|
| **0-Q** | **Qualify the existing MVP end-to-end.** Drive launch → turn → transcript → cancel → resume; record exactly what works and what breaks. Output: findings report that seeds/refutes 0-2..0-6. | Hardening; durability; anything in the DEFERRED table. |
| 0-1 | (Reserved for whatever 0-Q surfaces as the highest-impact parity gap.) | — |
| 0-2 | Delivery submit-retry parity: bounded retry on the `Enter` keystroke, matching antigravity-native's approach. | Exactly-once; durable journal; paste verification. |
| 0-3 | Transcript + lifecycle reporting completeness: echoed user msg, completed text, `running`/`idle`/`failed` events all reach the session endpoint. | Token streaming; idempotent delivery. |
| 0-4 | Cancel path: interrupt → `thread.cancel()`, retained until effect. | Cancel acks; grace; fallback termination; indeterminate state. |
| 0-5 | External thread resume correctness: persist `T-*`, `amp threads continue`. | Conflict detection; authoritative recovery. |
| 0-6 | Basic MCP relay: shared relay infra + per-agent config, matching pi/antigravity. | Structured tool/permission mirroring. |

Each implementer reads `.polly/AGENT_SETUP.md` (tooling) and
`.polly/AGENT_PROTOCOL.md` (contract-first + review rules) before starting.

---

## 6. Process guardrails (why this won't drift again)

- **Contract-first** for anything stateful: numbered invariants with
  falsification clauses, a state machine with per-transition atomicity, a test
  list written before code, and an explicit non-goals fence.
- **Test-first two-pass** for invariant-heavy work; direct implement for
  trivial/non-invariant work, still gated.
- **Review budget:** max 2 rounds on the same class of issue. A 3rd round that
  finds a *new* invariant stops the loop — the invariant goes into the
  contract, then one clean re-pass against the full contract. No more
  point-fix ping-pong.
- **Reviewer anti-drift:** a problem not covered by a stated invariant is
  reported as `CONTRACT-GAP`, not a blocking code fix.

See `.polly/AGENT_PROTOCOL.md` for the full text.
