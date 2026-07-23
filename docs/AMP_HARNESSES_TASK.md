# Amp-native hardening tasks

This task list covers reliability hardening for the currently implemented
`amp-native` interactive harness. It is ordered by priority and informed by
qualification of the current implementation and comparison with amux's
thread-bound interactive worker lifecycle.

Priority meanings:

- **P0:** prevent duplicate, lost, or misbound user work;
- **P1:** make lifecycle control and failures deterministic and diagnosable;
- **P2:** close durability and maintainability gaps before production use.

Feature expansion such as MCP relay, structured tool rendering, attachments,
and mode switching remains in `AMP_HARNESSES.md` and is not part of this
hardening list.

## Verification status (2026-07-23)

A read-only verification pass against the `feat/amp-native` code produced the
following baseline, updated as implementation work lands. Tasks must be
re-checked against code at the start of each implementation pass.

| Task | Status | Notes |
| --- | --- | --- |
| AMP-NATIVE-001 | Implemented | Durable versioned delivery journal (`amp_native_delivery.py`) with monotonic CAS transitions (per-record `fcntl` lock, reject regressions) and fail-closed fsync; `submission_started` is fsynced before paste; post-mutation failures become `recovery_required` (never replayable `failed`); executor reconciles non-terminal records to `recovery_required` on restart (no blind replay); confirmation is wired through the real plugin-mirror event path (`confirm_outstanding`, validated by matching thread + `response_id`); durable cross-process set-once dedupe (`external_item_dedupe.py`) keyed by the `agent.start` `response_id`, committed after append and recovered against the store on a crashed-mid-append; runner recreate reconciles and publishes a typed recovery event. Covered by `tests/test_amp_native_delivery.py`. |
| AMP-NATIVE-002 | Partial | Multiline buffer paste works; no readiness signal or paste verification |
| AMP-NATIVE-003 | Partial | Plugin binds first ID and ignores conflicts; no authoritative-ID comparison or recovery state |
| AMP-NATIVE-004 | Partial | Only `socket_path`/`tmux_target` persisted; mutations check `has-session` only |
| AMP-NATIVE-005 | Partial | Ordered interrupt + `thread.cancel()` exists; no ack, grace, fallback, or uncertainty state |
| AMP-NATIVE-006 | Partial | Generic exit diagnostics exist; no Amp-specific diagnostic envelope or redaction |
| AMP-NATIVE-007 | Not started (manual) | Mechanisms exist; no authenticated E2E qualification. Requires real Amp credentials |
| AMP-NATIVE-008 | Not started | No idempotency keys, server dedupe, or durable outbox |
| AMP-NATIVE-009 | Partial | Marker rejection + atomic replace already done (baseline); no version/lock/uninstall |
| AMP-NATIVE-010 | Not started | Amp mutates Pi module globals around the synchronous call; concurrent cross-talk risk |

## Dependencies and sequencing

Execute in numeric priority order, honoring these dependencies:

```diagram
001 (delivery journal) ──▶ 003 (thread binding reconciliation uses delivery IDs)
004 (target validator) ──▶ 005 (fallback cancellation reuses the validator)
001 ──▶ 008 (delivery confirmation keys feed idempotency keys)
010 (wrapper extraction) ──▶ 007 (concurrency qualification needs no cross-talk)
001..006, 008, 009, 010 ──▶ 007 (final credentialed qualification)
```

- **AMP-NATIVE-007 is last and manual/credentialed.** It cannot be fully
  automated in CI because it requires an authenticated Amp installation on
  Linux and macOS. Its deterministic, fake-Amp portions may be implemented
  earlier; the authenticated matrix is run by a human and recorded.
- **AMP-NATIVE-010 is filed under P2 by altitude, but it is a real
  correctness race** (concurrent Pi + Amp sessions share module-global
  transport identity). Treat it as P0-grade for concurrency safety even though
  it lands after the higher-numbered lifecycle work; it must be done before
  AMP-NATIVE-007's two-concurrency check is meaningful.
- Each implementation pass must re-verify the relevant status row above before
  starting, and update it in the implementation PR.

## P0 — prompt delivery and thread safety

### AMP-NATIVE-001 — Persist prompt delivery state

**Short description:** Record delivery intent before tmux injection so a crash
or retry cannot silently submit the same browser prompt twice.

**Implementation status:** Not started. Browser turns call
`omnigent/inner/amp_native_executor.py:inject_user_message` directly and report
completion when tmux submission returns; no delivery journal participates.

**Definition of done / acceptance criteria:**

- A durable delivery journal record is written per browser prompt with at least
  `{delivery_id, conversation_id, response_id, normalized_content_hash, state,
  created_at, updated_at}`, where `state` ∈ `{pending, submission_started,
  confirmed, recovery_required, failed}`. The schema is versioned.
- `submission_started` is made durable (atomic write + `fsync`) **before** any
  paste or Enter byte can reach Amp (`amp_native_bridge.py` injection path).
- On restart or retry, a record left in `submission_started` is never
  automatically resubmitted; it transitions to `recovery_required` and surfaces
  an actionable, typed result (not a bare `False` / generic executor error).
- A `confirmed` record is reconciled with the plugin-mirrored user message
  (correlated by `response_id` and the `agent.start` of the matching thread) and
  cannot produce a duplicate durable Omnigent conversation item. This requires a
  server-side set-once/dedupe path for the browser optimistic item keyed by
  `delivery_id`.
- Terminal recreation (`omnigent/runner/app.py` recreate path) must not clear
  delivery state; pending journal records survive runner restart.
- Fault-injection tests simulate termination (a) before paste, (b) after paste,
  (c) after Enter, (d) after plugin confirmation, and assert the resulting
  state and that no blind replay occurs.

**Named tests to add:** `tests/test_amp_native_delivery.py` covering the five
termination points, restart reconciliation, and duplicate-suppression.

### AMP-NATIVE-002 — Verify composer readiness and paste visibility

**Short description:** Do not press Enter until the Amp composer is ready and
the complete pasted prompt is visible in the target pane.

**Implementation status:** Partial. Multiline input is normalized and delivered
via file-backed `tmux load-buffer`/`paste-buffer -p` (good). Readiness checks
only `tmux has-session`; Enter follows a fixed 100 ms sleep with no
`capture-pane` verification.

**Definition of done / acceptance criteria:**

- Define a versioned readiness predicate (named, e.g. `composer_ready()`) with
  an explicit bounded deadline and injectable clock/tmux runner, replacing the
  sole `has-session` + fixed sleep.
- Multiline prompts continue to use buffer paste without shell interpolation or
  command-line size limits.
- A capture-and-verify step confirms the complete normalized prompt is visible
  in the target pane before Enter. Define the capture normalization rules
  explicitly (handle wrapping, scrollback, tabs, CRLF, Unicode, hidden composer
  content).
- If readiness or paste verification fails, Enter is **not** sent and delivery
  is left in an explicit `recovery_required`/indeterminate state (wired to
  AMP-NATIVE-001's journal).
- Deterministic tests cover: delayed startup, single-line, multiline, Unicode,
  CRLF, partial/incomplete paste, and verification timeout. Existing
  `test_inject_user_message_reaches_tmux_without_vendor_readiness_delay` must be
  reframed or replaced (its name is stale relative to this task).

**Named tests to add:** mocked command-order tests in
`tests/test_amp_native.py` plus real-tmux multiline/Unicode/delay/timeout cases.

### AMP-NATIVE-003 — Detect unexpected thread binding

**Short description:** Prove that a browser prompt is handled by the Amp thread
bound to the Omnigent conversation and fail closed on conflicting identity.

**Implementation status:** Partial. The plugin binds the first `session.start`
ID and ignores later conflicting starts; runner cold-resume validates only the
`T-` prefix. No authoritative persisted expected-ID comparison, no PATCH
conflict handling, no typed recovery event.

**Definition of done / acceptance criteria:**

- The authoritative expected `T-*` ID is loaded (from the persisted
  conversation/launch snapshot) and compared against **every** observed
  `session.start`, `agent.start`, and `agent.end` identity — not just the
  plugin-local first ID.
- Exactly one atomic bind is allowed when no authoritative ID exists yet; after
  binding, the authoritative ID is set-once and immutable.
- A conflicting thread ID (a) cannot PATCH the conversation, (b) cannot mirror
  transcript/status events, and (c) cannot be accepted as successful prompt
  delivery. A PATCH 409 / non-2xx / ID mismatch produces a typed
  `recovery_required` event surfaced to Omnigent.
- Prompt confirmation (AMP-NATIVE-001) is correlated with a matching-thread
  event; a prompt confirmed by a non-matching thread is `recovery_required`.
- Qualification determines whether Amp can create/select an unexpected recipient
  thread during first-prompt tmux injection; any ambiguity produces
  `recovery_required` with no guessing, silent new thread, or replay.
- `amp threads new` pre-provisioning is not introduced unless qualification
  proves it necessary; if introduced, empty-residue cleanup and alternate
  recipient reconciliation are specified and tested first.
- Static TypeScript source-string assertions are replaced with **executable**
  plugin contract tests (run the plugin logic against fixture events).

**Named tests to add:** executable plugin tests for bind-once, conflict
rejection, PATCH 409 handling, and mismatched-recipient confirmation;
`tests/test_amp_native.py` argv test keeps asserting plain `amp` for fresh and
`threads continue` for resume.

## P1 — deterministic lifecycle and diagnostics

### AMP-NATIVE-004 — Verify exact tmux pane and Amp process ownership

**Short description:** Confirm that launch, reattach, injection, interrupt, and
teardown target the exact pane running the expected Amp process.

**Implementation status:** Partial. Runner persists `socket_path` and a generic
`tmux_target`. Mutations check only `has-session`. No pane ID/PID, process tree,
argv, cwd, or creation token is revalidated.

**Definition of done / acceptance criteria:**

- Define an immutable target fingerprint containing socket, session/window/pane
  IDs, pane PID (or process start token), executable realpath, argv digest, cwd,
  and a creation token.
- A single `validate_amp_target()` runs immediately before each **mutating**
  operation: paste, Enter, fallback cancel/kill, recreate/attach, and teardown
  (plugin-native `thread.cancel()` through the inbox is not a tmux mutation and
  is exempt). A race-recheck re-validates between validation and mutation.
- Startup and recreation verify the target pane owns a live Amp process with the
  expected cwd and launch form.
- Stale pane IDs, replaced windows, extra panes, or identity drift fail closed
  without injecting input or killing an unrelated process.
- Tests cover stale tmux metadata, pane replacement, process exit during
  validation, and two concurrent Amp-native sessions (no cross-talk).

**Named tests to add:** fake-tmux/process tests in `tests/test_amp_native.py`
plus a two-real-session isolation test.

### AMP-NATIVE-005 — Add bounded cancellation escalation

**Short description:** Keep plugin-native cancellation as the primary path but
add a bounded terminal/process fallback when the plugin cannot complete it.

**Implementation status:** Partial. Interrupt endpoint enqueues an ordered
record and returns 204; plugin drains in order, calls `thread.cancel()`, deletes
on success. No acknowledgement, grace timer, matching-turn wait, process
fallback, or uncertainty state. **Depends on AMP-NATIVE-004** for safe fallback
targeting.

**Definition of done / acceptance criteria:**

- Interrupt carries an interrupt ID and acknowledgement lifecycle tied to the
  expected `response_id`/thread ID. Define whether HTTP 204 means "accepted" vs
  "cancelled" and document it.
- After enqueue, the harness waits a documented grace period (named constant)
  for the matching turn to become idle/failed.
- If plugin cancellation is unavailable or times out, fallback targets **only**
  the AMP-NATIVE-004-revalidated pane/process and follows a documented
  escalation sequence (e.g. grace → SIGTERM tree → SIGKILL tree after a
  named second grace).
- Outcomes are explicit and typed: `accepted`, `cancelled`,
  `recovery_required`/`indeterminate`. Cancellation never reports idle success
  merely because the local pane was destroyed; uncertain Amp state is
  represented explicitly.
- Tests cover: successful plugin cancellation, unavailable plugin, timeout,
  stale pane identity, and forced teardown; plus late acknowledgement vs
  terminal-event races.

**Named tests to add:** `tests/test_amp_native_cancel.py` for each escalation
boundary and the ack/terminal race.

### AMP-NATIVE-006 — Capture bounded startup and recovery diagnostics

**Short description:** Preserve enough local evidence to explain terminal
startup, recreation, injection, and cancellation failures without leaking
secrets.

**Implementation status:** Partial. Generic terminal-exit diagnostics capture
bounded pane output, command shape, and cwd; argv values are omitted; pane
output is not redacted; no Amp-specific structured envelope.

**Definition of done / acceptance criteria:**

- Define an `AmpNativeDiagnostic` envelope with enumerated lifecycle phases
  (startup, recreate, inject, cancel, teardown), an allowlisted field set, exact
  size limits per field and total, and pane-output redaction.
- Diagnostic fields include sanitized tmux identity, expected launch form, cwd,
  process state, and lifecycle phase. Credential-bearing env/config values are
  never included; pane output is redacted against a known secret pattern set.
- Diagnostic size is capped; the original failure remains primary when
  diagnostic capture also fails.
- Tests cover startup exit, malformed tmux metadata, missing process, capture
  failure, truncation, and representative-secret redaction.

**Named tests to add:** `tests/test_amp_native_diagnostics.py`.

### AMP-NATIVE-007 — Complete authenticated lifecycle qualification

**Short description:** Exercise the full interactive lifecycle against a real
authenticated Amp installation, including concurrency and crash boundaries.

**Implementation status:** Not started. Mechanisms exist; no authenticated E2E
qualification. **This task is last, manual, and credentialed** — it requires
real Amp credentials and Linux/macOS hosts and cannot be fully automated in CI.
Depends on 001–006, 008, 009, 010.

**Definition of done / acceptance criteria:**

Deterministic (automatable) portion, using a fake/local Amp where possible:

- Fake-Amp lifecycle tests cover fresh launch, first browser prompt, follow-up
  prompt, terminal-entered prompt, assistant completion, interrupt,
  detach/reattach, and cold resume.
- Browser and terminal prompts each appear exactly once in durable Omnigent
  history under the fake-Amp harness.

Authenticated (manual) matrix, recorded with OS, Amp/tmux versions, Omnigent
commit, thread/conversation IDs, timestamps, and sanitized evidence:

- The full lifecycle (fresh launch … cold resume) passes on Linux and macOS.
- Two simultaneous conversations preserve distinct bridge directories, tmux
  panes, response IDs, and Amp thread IDs.
- Runner restart during prompt delivery exercises the AMP-NATIVE-001 states
  without blind replay.
- Unsupported platforms are blocked explicitly and documented.

Exactly-once outage/retry semantics are validated here but depend on
AMP-NATIVE-001 and AMP-NATIVE-008.

**Deliverable:** a recorded qualification matrix (markdown) committed under
`docs/` plus the deterministic fake-Amp tests; the manual matrix is run by a
human and checked off with evidence.

## P2 — durable events and maintainable ownership

### AMP-NATIVE-008 — Add idempotent external event delivery

**Short description:** Give plugin transcript and status delivery a server-side
idempotency contract, then retry through a durable ordered outbox.

**Implementation status:** Not started. Plugin POSTs carry no idempotency keys
and their results are ignored; `response_id` is correlation only and can fall
back to `"event"`. No durable outbox. Includes **server-side** work.

**Definition of done / acceptance criteria:**

- External native events carry stable, fully-scoped idempotency keys
  (conversation + Amp thread + response + event kind + item where applicable).
  Define the versioned event ID format.
- The server enforces a uniqueness constraint on the key and accepts repeated
  delivery without duplicating conversation items or regressing terminal status
  (terminal-status precedence is explicit and monotonic).
- The plugin persists an ordered, durable outbox and removes entries only after
  an acknowledged idempotent write. Define outbox format, atomicity,
  acknowledgement semantics, retry/backoff, and corruption handling.
- Outbox retention is bounded by documented count/byte/age limits.
- Tests cover: duplicate delivery, post-commit timeout (server committed, client
  times out), restart, temporary server outage recovery, and concurrent
  sessions — for **both** server and plugin sides.

**Named tests to add:** server dedupe tests in the runner test suite and plugin
outbox tests in `tests/test_amp_native.py`.

### AMP-NATIVE-009 — Harden managed plugin lifecycle

**Short description:** Qualify installation, upgrade, collision, and removal of
the inert global Amp plugin.

**Implementation status:** Partial. **Baseline already done:** managed-marker
validation prevents overwriting an unmanaged plugin, and config/plugin writes
use temp files + `os.replace` (atomic). Missing: compatibility version, install
locking, uninstall workflow, live-session lease handling. Invalid config
silently disables the plugin.

**Definition of done / acceptance criteria:**

- (Baseline, already met) Managed-marker validation prevents overwriting an
  unmanaged plugin at the destination path. Keep regression tests.
- (Baseline, already met) Installation and file replacement are atomic via
  `os.replace`. Keep regression tests.
- Add protocol versions with explicit compatibility ranges; version skew between
  Omnigent, bridge config, and plugin produces an actionable compatibility
  error (not silent disable).
- Define concurrent-install safety via either a lock or a proven lock-free
  protocol; installation/upgrade are safe while other Amp-native sessions are
  active.
- Define explicit uninstall rules: normal session cleanup must **not** remove
  the global plugin (the design intentionally retains it); document when, if
  ever, the global plugin is removed, and never remove an unmanaged or
  actively-required plugin.
- Permission, collision, interrupted-update, downgrade, and concurrent-install
  tests pass.

**Named tests to add:** extend `tests/test_amp_native.py` with downgrade,
interrupted-update, concurrent-install, and version-skew cases.

### AMP-NATIVE-010 — Extract the vendor-neutral native terminal wrapper

**Short description:** Remove Amp's dependency on temporary Pi module identity
substitution while preserving existing native terminal behavior.

**Implementation status:** Not started. Amp mutates four Pi module globals,
invokes Pi's private transport, then restores them in `finally`. Restoration
does not prevent concurrent Pi/Amp cross-talk. **This is a correctness race
under concurrent sessions, not merely maintainability** — treat as P0-grade
for concurrency safety. Must land before AMP-NATIVE-007's concurrency check.

**Definition of done / acceptance criteria:**

- Define an immutable wrapper configuration containing agent/terminal identity,
  labels, command/resume behavior, arguments, progress strings, resource role,
  picker filter, and spec metadata.
- The common create, bind, ensure, wait, attach, resume-picker, and teardown
  flow is owned by a vendor-neutral helper that consumes the immutable config.
  No module-global identity mutation.
- Pi and Amp both use the helper. Transport-global mutation is prohibited.
- Launch arguments, progress labels, wrapper metadata, terminal roles, and
  resume behavior remain unchanged for both integrations (exact payload-parity
  tests).
- Existing Pi and Amp focused tests pass; add barrier-based concurrency coverage
  that launches both native wrappers simultaneously without cross-talk.

**Named tests to add:** `tests/test_native_wrapper.py` for payload parity and
simultaneous Pi/Amp concurrency.
