# Amp native harness

**Status:** MVP implemented on `feat/amp-native`

**Harness:** `amp-native`

**Alias:** `native-amp`

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

## Follow-up plan

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
