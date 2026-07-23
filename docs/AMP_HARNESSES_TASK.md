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

## P0 — prompt delivery and thread safety

### AMP-NATIVE-001 — Persist prompt delivery state

**Short description:** Record delivery intent before tmux injection so a crash
or retry cannot silently submit the same browser prompt twice.

**Definition of done / acceptance criteria:**

- Each browser prompt has a stable delivery ID and persisted state covering at
  least `pending`, `submission_started`, and `confirmed`.
- The `submission_started` state is durable before paste or Enter can reach
  Amp.
- Restart or retry never automatically resubmits a prompt left in an uncertain
  `submission_started` state.
- The UI and session state expose an actionable recovery-required error instead
  of reporting uncertain delivery as success or failure.
- A confirmed prompt is reconciled with the plugin-mirrored user message and
  cannot produce a duplicate durable Omnigent conversation item.
- Tests simulate termination before paste, after paste, after Enter, and after
  plugin confirmation.

### AMP-NATIVE-002 — Verify composer readiness and paste visibility

**Short description:** Do not press Enter until the Amp composer is ready and
the complete pasted prompt is visible in the target pane.

**Definition of done / acceptance criteria:**

- Injection waits for a bounded, Amp-specific composer readiness signal rather
  than relying only on pane existence.
- Multiline prompts use tmux buffer paste without shell interpolation or
  command-line size limits.
- Pane capture confirms the complete normalized prompt before Enter is sent.
- Missing, partial, or unverifiable pasted content fails without sending Enter
  and leaves delivery in an explicit recoverable or indeterminate state.
- Readiness, paste verification, and timeout behavior have deterministic tmux
  tests, including delayed startup and multiline input.

### AMP-NATIVE-003 — Detect unexpected thread binding

**Short description:** Prove that a browser prompt is handled by the Amp thread
bound to the Omnigent conversation and fail closed on conflicting identity.

**Definition of done / acceptance criteria:**

- Every observed `session.start`, `agent.start`, and `agent.end` identity is
  validated against the conversation's set-once Amp `T-*` ID.
- A conflicting thread ID cannot patch the conversation, mirror transcript
  events, or be accepted as successful prompt delivery.
- Fresh first-prompt qualification tests determine whether Amp can create or
  select an unexpected recipient thread during tmux injection.
- Any ambiguous or conflicting recipient produces a recovery-required state;
  the harness does not guess, silently open another thread, or replay the
  prompt.
- `amp threads new` pre-provisioning is not introduced unless qualification
  proves it necessary; if introduced, empty-residue cleanup and alternate
  recipient reconciliation are specified and tested first.

## P1 — deterministic lifecycle and diagnostics

### AMP-NATIVE-004 — Verify exact tmux pane and Amp process ownership

**Short description:** Confirm that launch, reattach, injection, interrupt, and
teardown target the exact pane running the expected Amp process.

**Definition of done / acceptance criteria:**

- The bridge persists and validates tmux socket, session, window, and pane
  identity before every mutating operation.
- Startup and recreation verify that the target pane owns a live Amp process
  with the expected working directory and launch form.
- Stale pane IDs, replaced windows, extra panes, or identity drift fail closed
  without injecting input or killing an unrelated process.
- Tests cover stale tmux metadata, pane replacement, process exit during
  validation, and concurrent Amp-native sessions.

### AMP-NATIVE-005 — Add bounded cancellation escalation

**Short description:** Keep plugin-native cancellation as the primary path but
add a bounded terminal/process fallback when the plugin cannot complete it.

**Definition of done / acceptance criteria:**

- Interrupt first calls `PluginThread.cancel()` through the ordered inbox.
- The harness waits a documented grace period for the matching turn to become
  idle or failed.
- If plugin cancellation is unavailable or times out, fallback targets only
  the revalidated Amp pane/process and follows a documented escalation policy.
- Cancellation never reports idle success merely because the local pane was
  destroyed; uncertain Amp state is represented explicitly.
- Tests cover successful plugin cancellation, unavailable plugin, timeout,
  stale pane identity, and forced teardown.

### AMP-NATIVE-006 — Capture bounded startup and recovery diagnostics

**Short description:** Preserve enough local evidence to explain terminal
startup, recreation, injection, and cancellation failures without leaking
secrets.

**Definition of done / acceptance criteria:**

- Failures include bounded pane history plus sanitized tmux identity, expected
  launch form, working directory, process state, and lifecycle phase.
- Diagnostic size is capped and credential-bearing environment/config values
  are never included.
- The original failure remains primary when diagnostic capture also fails.
- Tests cover startup exit, malformed tmux metadata, missing process, capture
  failure, truncation, and redaction.

### AMP-NATIVE-007 — Complete authenticated lifecycle qualification

**Short description:** Exercise the full interactive lifecycle against a real
authenticated Amp installation, including concurrency and crash boundaries.

**Definition of done / acceptance criteria:**

- Fresh launch, first browser prompt, follow-up prompt, terminal-entered prompt,
  assistant completion, interrupt, detach/reattach, and cold resume pass.
- Browser and terminal prompts each appear exactly once in durable Omnigent
  history.
- Two simultaneous conversations preserve distinct bridge directories, tmux
  panes, response IDs, and Amp thread IDs.
- Runner restart during prompt delivery exercises the states from
  `AMP-NATIVE-001` without blind replay.
- Qualification runs on Linux and macOS, or unsupported platforms are blocked
  explicitly and documented.

## P2 — durable events and maintainable ownership

### AMP-NATIVE-008 — Add idempotent external event delivery

**Short description:** Give plugin transcript and status delivery a server-side
idempotency contract, then retry through a durable ordered outbox.

**Definition of done / acceptance criteria:**

- External native events carry stable idempotency keys scoped to conversation,
  Amp thread, response, event kind, and item where applicable.
- The server accepts repeated delivery without duplicating conversation items
  or regressing terminal status.
- The plugin persists ordered undelivered events and removes them only after an
  acknowledged idempotent write.
- Restart, network timeout after server commit, temporary server outage, and
  concurrent sessions are covered by tests.
- Outbox retention and bounded-growth behavior are documented and tested.

### AMP-NATIVE-009 — Harden managed plugin lifecycle

**Short description:** Qualify installation, upgrade, collision, and removal of
the inert global Amp plugin.

**Definition of done / acceptance criteria:**

- Managed-marker validation prevents overwriting an unmanaged plugin at the
  destination path.
- Installation and upgrade are atomic and safe while other Amp-native sessions
  are active.
- Version skew between Omnigent, bridge config, and plugin produces an
  actionable compatibility error.
- Uninstall behavior is documented and cannot remove an unmanaged or actively
  required plugin.
- Permission, collision, interrupted update, downgrade, and concurrent install
  tests pass.

### AMP-NATIVE-010 — Extract the vendor-neutral native terminal wrapper

**Short description:** Remove Amp's dependency on temporary Pi module identity
substitution while preserving existing native terminal behavior.

**Definition of done / acceptance criteria:**

- Common create, bind, ensure, wait, attach, resume-picker, and teardown flow is
  owned by a vendor-neutral helper with explicit metadata inputs.
- Pi and Amp use the helper without module-global identity mutation.
- Launch arguments, progress labels, wrapper metadata, terminal roles, and
  resume behavior remain unchanged for both integrations.
- Existing Pi and Amp focused tests pass, with added concurrency coverage that
  launches both native wrappers without cross-talk.
