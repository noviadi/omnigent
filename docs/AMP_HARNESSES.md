# Amp harness integrations

**Status:** interactive MVP implemented on `feat/amp-native`; direct harness
planned, not implemented

**Implemented harness:** `amp-native`

**Implemented alias:** `native-amp`

**Planned direct harness:** `amp` or `amp-direct` (name to be confirmed)

**CLI:** `omnigent amp`

**Native agent:** `amp-native-ui`

## Executive summary

`amp-native` runs the real interactive [Amp](https://ampcode.com/) TUI inside an
Omnigent-owned tmux terminal. The same terminal is attachable from the local
CLI and visible in Omnigent's browser UI. Browser prompts are pasted into the
resident TUI, while an Amp TypeScript plugin mirrors structured lifecycle and
completed transcript events back to Omnigent.

This is deliberately a native TUI integration rather than an ACP adapter:

- ACP runs Amp headlessly through `--execute --stream-json`; it cannot expose
  Amp's interactive UI.
- `amp-native` keeps Amp itself in control of the interactive thread, built-in
  tools, modes, authentication, and terminal experience.
- Omnigent owns process lifecycle, tmux attachment, browser input delivery,
  transcript mirroring, status, interrupt, and cold resume.

The MVP is suitable for local/manual evaluation. MCP relay integration,
structured tool-call mirroring, token-level streaming, and exactly-once event
delivery are documented follow-ups rather than implied capabilities.

A second, non-interactive Amp surface is planned. It will run Amp's supported
`--execute --stream-json` interface directly as an Omnigent harness. It will
not open tmux, depend on the interactive plugin bridge, or require the
third-party ACP adapter. The two surfaces solve different use cases and should
coexist rather than forcing one lifecycle model onto both.

## Goals and non-goals

### MVP goals

1. Launch the real Amp TUI in the project working directory.
2. Attach the same TUI from `omnigent amp` and the Omnigent browser.
3. Deliver the first and subsequent browser prompts into the resident TUI.
4. Mirror terminal-entered and browser-entered user prompts into durable
   Omnigent conversation history.
5. Mirror completed assistant text and running/idle/failed status.
6. Persist Amp's `T-*` thread ID and use it for cold resume.
7. Interrupt a running Amp turn from Omnigent.
8. Avoid modifying the user's project `.amp/` directory.
9. Leave ordinary, non-Omnigent Amp invocations unaffected.

### Deliberate non-goals for the MVP

- Replacing Amp's TUI with a headless JSON renderer.
- Token-by-token assistant or thinking mirroring. Amp's current plugin API has
  no message-delta events.
- Mirroring structured tool calls/results into browser chat.
- Routing Omnigent's native MCP relay into Amp.
- Browser-driven model switching or compaction.
- Reconstructing Amp thread history for cross-harness forks.
- Exactly-once delivery across an Omnigent server outage.

## Why ACP was not sufficient

The ACP experiment used Amp's headless execution path:

```text
amp --execute --stream-json
```

That path can provide request/response interoperability, but it is a different
runtime from Amp's interactive terminal UI. ACP does not wrap an arbitrary CLI
inside tmux and does not mirror its rendered terminal. As a result, it cannot
provide the user experience required here:

```diagram
ACP
┌──────────┐   protocol messages   ┌────────────────────┐
│ Omnigent │──────────────────────▶│ Amp execute mode   │
└──────────┘                       │ no interactive TUI │
                                   └────────────────────┘

amp-native
┌──────────┐   tmux paste/attach   ┌────────────────────┐
│ Omnigent │◀─────────────────────▶│ Real Amp TUI       │
└────┬─────┘                       └─────────┬──────────┘
     │ structured lifecycle/transcript      │
     └──────────────────────────────────────┘
```

The native harness is therefore a core Omnigent integration, not a community
harness plugin. Omnigent's current harness plugin interface does not allow
community packages to register native terminal metadata or runner terminal
lifecycle branches.

ACP remains useful as a prototype of the headless path. A patched `amp-acp`
adapter successfully completed a direct ACP handshake and prompt, proving that
Amp's JSON execution mode is viable. It is not a durable dependency for the
planned direct harness: the adapter is a separately versioned Node package,
has already required local patches for Amp executable discovery and removed
CLI options, and adds a protocol translation layer Omnigent does not need.

## Two complementary Amp surfaces

The intended end state has two first-class harnesses:

| Concern | `amp-native` (implemented) | Direct Amp (planned) |
| --- | --- | --- |
| Primary use | Human-operated, persistent Amp TUI | Browser/API-driven agent turns |
| Amp mode | Interactive | `--execute --stream-json` |
| Process lifetime | Resident process in runner-owned tmux | One subprocess per Omnigent turn |
| Prompt transport | Multiline-safe tmux paste | stdin to the Amp subprocess |
| Output transport | Amp plugin, completed turn events | Incrementally parsed JSON Lines |
| Transcript authority | Resident Amp thread plus plugin observations | Amp thread plus normalized JSON stream |
| Resume | Warm pane reattach or cold `threads continue` | New execute process using `threads continue T-*` |
| Terminal access | Yes | No |
| Live message queue | Yes, through the TUI | No for the initial direct implementation |
| Structured output | Turn-complete text/status | Stream records as exposed by Amp |
| Cancellation | `PluginThread.cancel()` | Terminate and reap the execute process tree |
| Omnigent MCP relay | Follow-up | Planned after the direct text MVP |

`--stream-json` means records are available while Amp runs; it does not by
itself guarantee token-level text deltas. The implementation must characterize
the actual record granularity and advertise `supports_streaming` only for the
events Amp emits incrementally.

## Architecture

### Components

| Component | Responsibility |
| --- | --- |
| `omnigent/amp_native.py` | CLI wrapper, executable resolution, launch argv, native agent spec |
| `omnigent/amp_native_bridge.py` | Per-session bridge, tmux prompt injection, interrupt queue, managed plugin installation |
| `omnigent/inner/amp_native_executor.py` | Converts Omnigent turns into terminal prompt injection |
| `omnigent/inner/amp_native_harness.py` | Exposes the executor through the standard harness adapter |
| `omnigent/resources/amp_native/omnigent-native.ts` | Amp plugin for thread identity, transcript, status, and cancellation |
| `omnigent/runner/app.py` | Creates/recreates the Amp terminal and wires runner lifecycle |
| Harness/CLI/onboarding registries | Discovery, aliases, readiness, default agent, and resume dispatch |

### Runtime data flow

```diagram
                              runner-owned tmux
┌───────────────┐  prompt   ┌─────────────────────┐
│ Omnigent web  │──────────▶│ Amp interactive TUI │
│ conversation  │  paste    │                     │
└───────▲───────┘           └──────────┬──────────┘
        │                              │
        │ external_* events            │ Amp plugin hooks
        │                              ▼
        │                   ┌─────────────────────┐
        └───────────────────│ omnigent-native.ts │
                            └──────────┬──────────┘
                                       │
                         interrupt JSON│
                            ┌──────────▼──────────┐
                            │ per-session bridge │
                            │ ~/.omnigent/       │
                            │ amp-native/<hash>/ │
                            └─────────────────────┘
```

#### Browser to Amp

Omnigent persists a pending optimistic browser input and invokes the
`amp-native` executor. The executor performs a bracketed tmux paste into the
Amp composer and sends Enter. This path is required for the first prompt:
a newly opened blank Amp TUI has no thread and emits no `session.start`, so a
plugin cannot address a thread with `appendUserMessage` yet.

The tmux transport:

- waits up to five seconds for the runner to advertise `tmux.json`;
- verifies the pane still exists;
- normalizes multiline input for bracketed paste;
- uses `tmux load-buffer`/`paste-buffer`, avoiding command-line size limits;
- submits with Enter without applying another vendor's composer-clearing keys.

#### Amp to Omnigent

The Amp plugin consumes these documented events:

- `session.start`
- `agent.start`
- `agent.end`

It posts Omnigent-native event types:

- `external_conversation_item` for user messages;
- `external_assistant_message` for completed assistant text;
- `external_session_status` for running and terminal status.

Browser-originated user messages are intentionally mirrored back too. The
server reconciles that terminal-observed event with its pending optimistic
input, persists the authoritative item, and restores any file blocks that the
text-only TUI transcript cannot represent.

The plugin binds to the first Amp thread it sees and ignores lifecycle events
from any other thread, preventing a TUI thread switch from mixing unrelated
history into the Omnigent conversation.

#### Interrupt

The runner writes an ordered `interrupt` record into the bridge inbox. The Amp
plugin resolves the managed thread and calls:

```ts
thread.cancel()
```

Inbox records are deleted only after successful handling, so a temporarily
unavailable thread does not silently lose the interrupt.

#### Resume

On `session.start`, the plugin patches the Omnigent conversation with Amp's
native `T-*` thread ID. The idempotent patch retries briefly on failure.

The runner reads the authoritative session launch snapshot. A fresh session
launches:

```text
amp [persisted Amp arguments]
```

A resumed session launches:

```text
amp threads continue T-... [persisted Amp arguments]
```

Invalid external IDs and session-snapshot failures fail terminal creation
instead of silently opening an unrelated fresh Amp thread.

## Plugin installation and isolation

Amp does not offer a `--plugin <path>` CLI option. It discovers plugins only
from project and user plugin directories. Writing a plugin into the user's
project would mutate the repository being worked on, so the runner installs a
single managed plugin at:

```text
~/.config/amp/plugins/omnigent-native.ts
```

The plugin is inert unless the Amp process has:

```text
OMNIGENT_AMP_NATIVE_CONFIG=/path/to/session/config.json
```

This allows multiple Amp-native sessions to share one stable plugin source
while using isolated per-process configurations. Ordinary Amp sessions load
the file but register no handlers.

Safety properties:

- the source contains a stable Omnigent-managed marker;
- an existing file without that marker is never overwritten;
- plugin updates use atomic replacement;
- bridge directories and the plugin directory use mode `0700`;
- credential-bearing per-session config uses mode `0600`;
- project `.amp/` files are never created or changed;
- deleting an Omnigent session removes its bridge directory but leaves the
  inert global plugin, avoiding races with other active sessions.

## Registration and lifecycle integration

The implementation registers:

| Property | Value |
| --- | --- |
| Canonical harness | `amp-native` |
| Alias | `native-amp` |
| Native agent key | `amp` |
| Agent name | `amp-native-ui` |
| Terminal | `amp:main` |
| Wrapper label | `amp-native-ui` |
| Authentication | Amp-owned |
| Resume | Warm reattach and cold `T-*` resume |
| Interrupt | Supported |
| Live queue | Supported through tmux input |
| Structured streaming | No; final assistant message only |

Integration points include:

- harness registry, compatibility allowlist, aliasing, and capabilities;
- native CLI command and generic `run --harness` dispatch;
- default built-in agent seeding;
- runner spawn environment, terminal creation, recreation locks, and teardown;
- agentless resume dispatch;
- executable readiness and install guidance;
- packaged TypeScript resource metadata.

## Usage

### Prerequisites

1. Install and authenticate Amp.
2. Install tmux.
3. Install this Omnigent branch or run it from the checkout.

Amp can be installed using its official installer:

```bash
curl -fsSL https://ampcode.com/install.sh | bash
amp login
```

### Launch from a project

Run from the project directory Amp should operate on:

```bash
omnigent amp
```

Pass Amp arguments after `--` when needed:

```bash
omnigent amp -- --mode high
```

Resume an Omnigent conversation explicitly:

```bash
omnigent amp --resume <conversation-id>
```

Open the Amp-native conversation picker:

```bash
omnigent amp --resume
```

The generic native-harness route is also registered:

```bash
omnigent run --harness amp-native <agent.yaml>
```

Override the Amp executable with either configuration or the environment:

```bash
OMNIGENT_AMP_PATH=/path/to/amp omnigent amp
```

```yaml
harness:
  amp-native:
    command: /path/to/amp
    args:
      - --mode
      - high
```

## Validation completed

The implementation has been checked with:

- focused Amp-native unit tests;
- a real tmux paste test using an isolated tmux socket;
- harness alias/native-agent registry suites;
- relevant native CLI dispatch tests;
- Ruff on all touched Python files;
- Python bytecode compilation;
- `git diff --check`;
- `amp plugins list` with the packaged TypeScript plugin loaded from a test
  project.

The focused tests cover:

- fresh and resumed Amp argv;
- input block normalization;
- bridge permissions, ordering, and stale inbox clearing;
- tmux input delivery without vendor-specific readiness delays;
- managed plugin installation and unmanaged-file conflict protection;
- harness aliases, native metadata, capabilities, and install metadata;
- required structured event contracts in the plugin resource.

## Current limitations and risks

### Completed output rather than token streaming

Amp's plugin API exposes completed `agent.end.messages`, but no assistant-text,
thinking, or tool-output delta events. The terminal remains visually live; the
structured browser assistant message appears when the turn completes.

### Best-effort transcript event delivery

The Omnigent external transcript endpoint is append-only and currently has no
idempotency-key contract. Retrying after an ambiguous network failure could
duplicate history, while not retrying can lose an event. The MVP therefore
keeps transcript/status posts best-effort and retries only the idempotent Amp
thread-ID patch. Exactly-once behavior requires a server-side idempotency
contract before a durable plugin outbox is safe.

### No Omnigent MCP relay yet

Amp can accept `--mcp-config`, but this branch does not yet generate and pass a
session-local Omnigent relay configuration. Amp runs with its native built-in
and user-configured tools.

### No structured tool visualization or policy bridge

Amp exposes `tool.call` and `tool.result`, including a pre-execution policy
interception result. The MVP does not post these events into Omnigent or route
them through Omnigent's policy/elicitation UI.

### Shared wrapper transport

The CLI wrapper currently reuses the established Pi daemon/bind/attach
transport while temporarily substituting Amp's native identity and terminal
metadata around the synchronous call. This keeps the MVP small and functional,
but a follow-up should extract a vendor-neutral native terminal wrapper helper
instead of sharing transport through Pi's module-level identifiers.

## Planned direct/non-interactive Amp harness

### Recommendation

Build a built-in Omnigent executor around Amp's supported CLI contract instead
of productizing the ACP prototype. Use `amp` as the likely canonical harness ID
and retain `amp-native` for the interactive TUI, following the existing pattern
where a vendor can have direct and `-native` surfaces. Confirm the name before
registration because `omnigent amp` already launches the native TUI; if that
CLI/harness distinction is considered too surprising, use `amp-direct` and
reserve `amp` until a compatibility plan exists.

The recommended initial lifecycle is one Amp process per Omnigent turn:

```text
# New conversation
amp --execute --stream-json --no-archive-after-execute \
  --no-ide --no-remote-control-terminal

# Subsequent turn
amp threads continue T-... \
  --execute --stream-json --no-archive-after-execute \
  --no-ide --no-remote-control-terminal
```

Use `--stream-json-thinking` instead of `--stream-json` only when reasoning
output is requested, qualified, and safe to expose.

Send the user message over stdin, run the process in the project working
directory, and parse stdout as JSON Lines while separately capturing bounded
stderr diagnostics. This gives each turn a clear timeout, exit code, and
cancellation boundary. Amp remains responsible for durable context through its
thread ID, so Omnigent does not replay prior messages on every turn.

Use the repository's cross-platform isolated-process helpers rather than
assuming POSIX process groups: `spawn_kwargs()` for launch, then
`terminate_tree()` and, after a grace period, `kill_tree()`. This is required to
reap Amp descendants consistently on Linux, macOS, and Windows.

Do not start with a long-lived `--stream-json-input` process. It may become a
later optimization for live input, but it introduces ambiguous turn
boundaries, recovery after process loss, backpressure, and cancellation
semantics before the basic execute/resume contract is qualified.

### Proposed runtime data flow

```diagram
┌────────────────────┐   latest user turn   ┌────────────────────────┐
│ Omnigent session   │─────────────────────▶│ Direct Amp executor    │
│                    │                      │ project cwd            │
│ external_session_id│◀──── thread ID ──────│ one process per turn   │
└─────────▲──────────┘                      └───────────┬────────────┘
          │                                             │ stdin / NDJSON
          │ normalized executor events                  ▼
          │                                  ┌────────────────────────┐
          └──────────────────────────────────│ amp --execute          │
                                             │ --stream-json          │
                                             └────────────────────────┘
```

For a fresh conversation, the executor launches Amp without a continuation ID,
captures the first valid Amp `session_id`, validates it as a `T-*` thread, and
persists it as `external_session_id`. Every later turn starts a new process with
that exact ID. Missing or conflicting IDs are protocol failures; silently
starting a new thread would split one Omnigent conversation across multiple Amp
histories.

The executor cannot persist this identity by itself: the current generic
`ExecutorEvent`/harness SSE contract has no external-session identity event.
Before direct Amp can support multiple turns, add a vendor-neutral identity
path from executor to harness adapter to runner. The runner should perform the
server's set-once/idempotent external-session patch, and a newly spawned harness
must receive the persisted ID from the authoritative session launch snapshot.
A conflicting ID or failure to bind a newly created Amp thread durably is a
non-retryable recovery error, never permission to continue on a fresh thread.

The direct executor should consume only the newest user turn after Amp identity
has been established. Transcript replay is a separate fork/import feature and
must not be mixed into ordinary resume. Fresh conversations created from an
existing non-Amp transcript need an explicit import policy before they are
supported.

### Stream normalization contract

Amp describes the output as Claude Code-compatible stream JSON, with an Amp
extension for thinking blocks. Before implementation, capture the exact schema
from the minimum supported Amp version and normalize it behind an Amp-owned
parser rather than allowing raw records into session logic.

The documented baseline uses complete message records rather than stable
content-block delta IDs. The initial parser should use these explicit rules,
then revise them only when captured minimum-version fixtures require it:

| Amp stream observation | Omnigent behavior |
| --- | --- |
| `system`/`init` | Validate every present `session_id`; bind the first and reject conflicts |
| Echoed `user` record | Ignore for transcript output, including `tool_result` content |
| Top-level `assistant` with no `parent_tool_use_id` | Emit each text block once as a coarse `TextChunk` |
| Top-level thinking block | Emit one coarse `ReasoningChunk` only when explicitly enabled |
| Subagent record with `parent_tool_use_id` | Ignore in the text prototype; later use a non-executable observation path |
| `tool_use` or `tool_result` | Never emit an executable `ToolCallRequest` |
| Successful `result` | Supply terminal status and one canonical usage source; do not emit `result.result` as text |
| Unknown well-formed record | Log at debug level and continue when safe |
| Malformed or truncated record | Fail as a protocol error; prior live chunks may not be durable |

Assistant text, reasoning, result summaries, and tool observations can overlap
across stream records. The parser must not concatenate every text-looking field
or emit a second final response from the `result` record. Result-level usage
should be authoritative unless captured fixtures prove that call-level records
must be aggregated; the same usage must never be counted twice.

The current runner accumulates live chunks in memory and persists the assistant
response only on successful completion. Therefore a protocol failure may leave
already emitted text visible to the connected client but not durable. Durable
partial-response recovery is out of scope for the first release and must not be
claimed as a property of the parser.

Amp executes its built-in tools itself. A streamed `tool_use` observation must
therefore **not** become an Omnigent `ToolCallRequest`, which would execute the
same operation twice. The text MVP should ignore tool lifecycle records except
for diagnostics. Structured visualization requires an observational event path
that cannot trigger tool execution. Omnigent-owned tools should later be
exposed to Amp through a per-session MCP relay, while Amp remains the tool
orchestrator.

Terminal records, parse errors, process exits, signals, timeout, and user
cancellation can race. The executor should collect those observations, reap the
child, and commit exactly one terminal event through a tested precedence rule.
Cancellation latches: a late successful result cannot overwrite it. Cleanup
runs in `finally`. If local termination cannot prove that the remote Amp turn
stopped, mark the thread recovery-required and block automatic continuation
until its outcome is reconciled.

### Configuration and security contract

The direct harness can share Amp executable resolution and authentication with
`amp-native`, including `OMNIGENT_AMP_PATH`, normal `PATH` lookup, install
guidance, and Amp-owned login. It should otherwise use an explicit allowlist of
forwarded settings:

- project working directory;
- Amp mode (`low`, `medium`, `high`, or `ultra`), not an arbitrary model ID;
- visibility and labels where Omnigent exposes them;
- a generated per-session `--mcp-config` file when relay support is added;
- an additive Omnigent framework prompt, only after the supported Amp settings
  key and merge behavior are verified;
- bounded timeout and cancellation grace period owned by Omnigent.

Do not pass arbitrary `ExecutorConfig.extra` keys as CLI flags or copy all host
environment variables into a generated settings file. Preserve the user's Amp
configuration by default. If framework prompt, permissions, enabled tools, or
skills require a temporary settings file, merge only documented keys into a
private per-session copy, atomically create it, and delete it after the turn.
Never overwrite the user's settings.

The child environment must also be deterministic:

- pass `--no-ide` so unrelated editor selection is not injected into a
  headless prompt;
- pass `--no-remote-control-terminal` because the direct harness advertises no
  terminal surface;
- remove `OMNIGENT_AMP_NATIVE_CONFIG` even if the parent inherited it;
- write settings and MCP configuration containing secrets to unique `0600`
  files and pass paths, not inline secret-bearing JSON in process arguments;
- derive the child environment from the runner-approved `OSEnvSpec` rather than
  an accidental harness-process environment, while preserving required Amp
  auth, proxy, certificate, project-tool, and user-specified variables without
  logging their values.

Headless policy behavior must be chosen deliberately. The executor must not
silently add a force-allow flag. The default should inherit Amp's configured
permissions; any Omnigent policy integration needs an explicit fail-open or
fail-closed contract and tests for disconnected clients. Secrets in MCP config,
stderr, and raw JSON records must be redacted before logging or persistence.

Amp may execute Bash and filesystem tools during an ordinary textual prompt;
"text-only" describes Omnigent's output mapping, not a tool-free Amp run. Until
a policy bridge exists, Amp owns those tools and Omnigent tool policies do not
intercept them. Before release, qualify allow/ask/reject behavior in execute
mode without a TTY and state that limitation in user-facing capability text.
Apply the agent's `OSEnvSpec` sandbox to the entire Amp process tree, or fail
closed when the requested sandbox cannot be honored. Never silently downgrade
to unsandboxed execution.

### Reuse boundary

Share the vendor-level pieces that have identical contracts:

- Amp executable resolution and `OMNIGENT_AMP_PATH` precedence;
- binary presence, version, login/readiness, and install metadata;
- `T-*` thread-ID validation;
- supported mode validation;
- eventually, MCP relay configuration construction and cleanup;
- authentication prerequisites and user-facing documentation.

Keep the lifecycle-specific pieces separate:

- `amp-native` tmux creation, prompt injection, and terminal attachment;
- the global Amp plugin and per-session interactive bridge;
- direct subprocess ownership, stdin, stdout parser, and stderr capture;
- interactive plugin events versus direct JSON stream as transcript source;
- `PluginThread.cancel()` versus cross-platform process-tree cancellation;
- native live-queue capability versus direct turn-at-a-time capability;
- fixtures and end-to-end tests for each transport.

The managed interactive plugin is inert without
`OMNIGENT_AMP_NATIVE_CONFIG`; the direct executor must remove that variable
from its child environment.
This prevents duplicate transcript/status events when both harnesses are used
on the same machine.

### Gap analysis from the current branch

| Area | Current `amp-native` implementation | Work needed for direct Amp |
| --- | --- | --- |
| Registration | `amp-native` plus `native-amp` alias | Choose and register `amp` or `amp-direct` with non-native capabilities |
| Launch | Persistent TUI in runner-owned tmux | Async headless subprocess in the project cwd |
| Input | tmux bracketed paste | Exact stdin framing for one prompt; qualify JSON input separately |
| Output | Plugin posts completed external events | Incremental JSON Lines reader and normalized event parser |
| Thread identity | Plugin `session.start` patches `T-*` ID | Add generic identity event, runner persistence, and snapshot restore |
| Resume | Recreate interactive TUI with `threads continue` | Execute one resumed headless turn without replaying history |
| Streaming | Completed assistant message only | Measure record granularity; map text/thinking without duplication |
| Tool events | Deferred plugin observation | Observe without re-executing; add a safe visualization contract |
| Omnigent tools | Not relayed | Generate isolated MCP config and verify tool-result correlation |
| System prompt | Amp owns interactive prompt | Verify additive settings-file mechanism and cleanup |
| Permissions | Amp-owned interactive behavior | Define safe non-interactive default and policy behavior |
| Sandbox | Interactive process uses native runner lifecycle | Apply `OSEnvSpec` to the complete direct process tree or fail closed |
| Environment | Native bridge activation is injected | Sanitize IDE, remote-terminal, plugin, auth, proxy, and secret handling |
| Cancellation | Plugin calls `thread.cancel()` | Cross-platform tree termination, terminal-event arbitration, recovery gate |
| Errors | Bridge/plugin status | Reconcile result records, exit code, signal, malformed JSON, and stderr |
| Timeouts | Long-lived terminal lifecycle | Per-turn startup, inactivity, and total-runtime behavior |
| Retry | Interactive thread remains resident | Prevent automatic replay after partial output or side effects |
| Schema compatibility | Plugin event API | Minimum Amp version plus tolerant stream-schema parser |
| Tests | Interactive bridge, argv, tmux, plugin contract | Recorded fixtures, fake executable, lifecycle, authenticated smoke test |
| ACP coexistence | External experimental route | Decide whether ACP remains an optional compatibility path |

### Delivery plan

#### Phase 0: capture the supported contract

1. Record `amp --help` and `amp threads continue --help` for the proposed
   minimum supported Amp version.
2. Capture sanitized JSON Lines for successful text-only, thinking, tool-using,
   failed, interrupted, and resumed turns.
3. Confirm complete-message, echoed-user, subagent, tool, usage, session-ID, and
   terminal-result behavior across those captures.
4. Verify signal handling, child-process cleanup, and whether cancelling the
   local process also stops the remote Amp turn.
5. Write the normalized internal event contract before writing the executor.

#### Phase 1: internal text-only prototype

1. Spawn fresh and resumed Amp execute processes in the requested project cwd.
2. Send one textual prompt over stdin and close stdin.
3. Parse stdout incrementally with line and total-output bounds; capture bounded
   stderr separately.
4. Emit text, final status, usage, and errors with exactly one terminal event.
5. Reap the complete process tree on success, cancellation, timeout, or parse
   failure using the shared cross-platform lifecycle helpers.
6. Keep this prototype unregistered and internal until Phase 2's continuity,
   prompt, input, and execution-safety gates are complete.

#### Phase 2: first releasable direct harness

1. Add the vendor-neutral external-session identity event, runner persistence,
   launch-snapshot restore, and cold harness-process restart coverage.
2. Resume subsequent turns with `threads continue T-*`, validate every emitted
   ID, and reject identity changes.
3. Deliver the agent and framework prompt through a verified additive settings
   mechanism without overwriting user configuration.
4. Support textual input only and reject attachments or unsupported content
   blocks explicitly rather than dropping them.
5. Apply `OSEnvSpec`, qualify Amp native-tool permissions in headless mode, and
   publish the fact that Amp-owned tools bypass Omnigent policy until bridged.
6. Disable automatic retries once the prompt has been handed to Amp because it
   may already have performed side effects or committed the turn remotely.
7. Register the chosen harness ID with accurate streaming, live-queue, model,
   tool, attachment, terminal, and policy capability metadata.
8. Define explicit fresh, resume, fork, and cross-harness import semantics.

#### Phase 3: Omnigent tools and policy parity

1. Generate a private per-session MCP relay config and pass it with
   `--mcp-config`.
2. Correlate streamed MCP tool observations without converting them into
   executable `ToolCallRequest` events.
3. Add structured observational tool rendering if Omnigent's event contract
   can represent already-executed tools safely.
4. Define permissions, elicitation, disconnected-client behavior, and secret
   redaction for headless runs.

#### Phase 4: hardening and qualification

1. Add fixture tests for every captured record shape, including unknown,
   malformed, oversized, and truncated records.
2. Use a fake Amp executable to test argv, cwd, stdin, stdout/stderr ordering,
   timeout, signal escalation, child cleanup, and nonzero exits
   deterministically.
3. Run authenticated smoke tests for fresh/resumed turns, thinking, native tool
   use, MCP tool use, cancellation, and concurrent conversations.
4. Test direct and interactive Amp sessions concurrently to prove there is no
   plugin, environment, settings, or thread-ID cross-talk.
5. Document the minimum Amp version and add a diagnostic for incompatible
   stream schemas rather than accepting silently corrupted output.

### Direct harness acceptance criteria

The direct harness is ready for initial release when:

1. it never creates or attaches a tmux session and removes the interactive
   plugin activation variable from its child environment;
2. it runs Amp in the requested project directory and sends multiline input
   without shell interpolation, while unsupported content fails explicitly;
3. each recognized JSON record maps deterministically to Omnigent output, with
   echoed user and subagent records filtered and no duplicated assistant text,
   reasoning, usage, or final response;
4. exactly one of `TurnComplete`, `TurnCancelled`, or `ExecutorError` terminates
   every run after the complete child tree has been reaped;
5. a validated Amp thread ID survives both Amp child exit and harness-process
   restart, then is used for the next turn without replaying prior history;
6. cancellation and timeout use cross-platform tree termination; an
   inconclusive remote cancellation blocks continuation as recovery-required;
7. malformed, truncated, or oversized output fails clearly without exposing
   secrets, and documentation does not claim failed partial text is durable;
8. the complete agent/framework prompt reaches Amp through a verified additive
   settings mechanism without changing the user's settings;
9. the requested `OSEnvSpec` applies to the whole process tree or launch fails
   closed, with Amp-owned tool and policy limitations advertised accurately;
10. Amp-native tool observations are never executed a second time by Omnigent;
11. `--no-ide`, remote-terminal disabling, private config files, and an
    explicit child-environment policy prevent ambient context leakage;
12. direct and `amp-native` conversations run concurrently without plugin,
   config, process, or thread identity cross-talk;
13. executable overrides, authentication failures, unsupported versions,
    nonzero exits, and stderr diagnostics produce actionable messages;
14. fixture/fake-process tests pass without Amp credentials, and an
    authenticated end-to-end qualification passes against the minimum and
    current supported Amp versions;
15. existing `amp-native` behavior and tests remain unchanged.

## Interactive `amp-native` follow-up plan

### Priority 1: manual end-to-end qualification

Run the following against a local Omnigent server and authenticated Amp:

1. `omnigent amp` opens the actual Amp TUI.
2. A browser prompt appears once in Amp and once in durable browser history.
3. A terminal-entered prompt appears once in browser history.
4. Completed assistant text and running/idle state are mirrored.
5. Browser interrupt cancels the active Amp turn.
6. Detach/reattach preserves the same live pane.
7. Runner restart resumes the same `T-*` thread.
8. Two simultaneous conversations remain isolated.

### Priority 2: vendor-neutral wrapper extraction

Extract the common create/bind/ensure/wait/attach flow now embedded in native
wrapper modules. Parameterize it with native-agent metadata, terminal name,
launch arguments, progress labels, and resume picker identity. Migrate Pi and
Amp to the helper without changing behavior.

### Priority 3: Omnigent MCP relay

Generate a per-session MCP configuration in the bridge directory, pass it via
Amp's `--mcp-config`, and start the existing native comment/tool relay. Verify
that concurrent sessions do not share relay credentials or tool state.

### Priority 4: tool and permission mirroring

Use Amp's `tool.call` and `tool.result` events to:

- render structured tool state in browser chat;
- map terminal tool approvals to Omnigent elicitation where appropriate;
- apply Omnigent policy without scraping the rendered TUI;
- fail open or closed according to an explicit policy when the browser is
  disconnected.

### Priority 5: reliable event delivery

Add server-side idempotency keys for external native events. Then implement a
durable ordered plugin outbox keyed by session, event kind, and response ID.

### Priority 6: parity features

- usage and cost reporting;
- attachment-aware Amp input beyond path references;
- browser mode switching where supported;
- fork/history reconstruction;
- harness-bench and authenticated end-to-end coverage.

## Acceptance criteria for graduating the MVP

The harness can be considered production-ready when:

1. the manual qualification matrix passes on Linux and macOS;
2. external event delivery has server-backed idempotency;
3. MCP relay and policy behavior are explicitly tested or explicitly excluded
   from the advertised capability set;
4. the shared wrapper transport is vendor-neutral;
5. an authenticated end-to-end test covers fresh launch, browser prompt,
   terminal prompt, interrupt, detach/reattach, and cold resume;
6. install, upgrade, collision, and uninstall behavior for the managed global
   Amp plugin is documented and tested.
