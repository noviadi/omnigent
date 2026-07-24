"""Focused contracts for the Amp native integration."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import stat
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace

import pytest

from omnigent import amp_native, amp_native_bridge
from omnigent.harness_aliases import (
    canonicalize_harness,
    is_native_harness,
    native_terminal_name,
)
from omnigent.harness_plugins import AMP_NATIVE_CODING_AGENT, harness_capabilities
from omnigent.inner import amp_native_executor as amp_native_executor_module
from omnigent.inner.amp_native_executor import AmpNativeExecutor, _content_to_text
from omnigent.inner.executor import ExecutorError, TextChunk, TurnComplete
from omnigent.native_coding_agents import native_coding_agent_for_harness
from omnigent.onboarding.harness_install import AMP_KEY, required_cli_for_harness


def test_build_amp_launch_fresh_and_resume() -> None:
    which = lambda _command: "/opt/bin/amp"  # noqa: E731
    assert amp_native.build_amp_launch(["--mode", "smart"], which=which) == [
        "/opt/bin/amp",
        "--mode",
        "smart",
    ]
    assert amp_native.build_amp_launch(
        ["--mode", "smart"], external_session_id="T-123", which=which
    ) == ["/opt/bin/amp", "threads", "continue", "T-123", "--mode", "smart"]


def test_executor_normalizes_input_text_blocks() -> None:
    assert _content_to_text("hello") == "hello"
    assert (
        _content_to_text(
            [
                {"type": "input_text", "text": "one"},
                {"type": "attachment", "text": "ignored"},
                {"type": "input_text", "text": "two"},
            ]
        )
        == "one\ntwo"
    )
    assert _content_to_text([]) == ""


def test_amp_native_registry_and_install_metadata() -> None:
    assert canonicalize_harness("native-amp") == "amp-native"
    assert is_native_harness("amp-native")
    assert native_terminal_name("native-amp") == "amp"
    assert native_coding_agent_for_harness("native-amp") is AMP_NATIVE_CODING_AGENT
    capabilities = harness_capabilities()["amp-native"]
    assert capabilities.interrupt is True
    assert capabilities.streaming is False
    install = required_cli_for_harness("amp-native")
    assert install is not None
    assert install.binary == AMP_KEY


def test_bridge_permissions_order_and_clear(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(amp_native_bridge, "_ROOT", tmp_path / "amp")
    bridge = amp_native_bridge.prepare_bridge_dir("conv_1")
    first = amp_native_bridge.enqueue_user_message(bridge, "first")
    second = amp_native_bridge.enqueue_interrupt(bridge)
    names = sorted(item.name for item in (bridge / "inbox").glob("*.json"))
    assert first in names[0] and second in names[1]
    assert stat.S_IMODE(bridge.stat().st_mode) == 0o700
    amp_native_bridge.clear_inbox(bridge)
    assert not list((bridge / "inbox").iterdir())


def test_inject_user_message_reaches_tmux_without_vendor_readiness_delay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if shutil.which("tmux") is None:
        pytest.skip("tmux is not installed")
    socket = tmp_path / "tmux.sock"
    bridge = tmp_path / "bridge"
    bridge.mkdir()
    subprocess.run(
        ["tmux", "-S", str(socket), "new-session", "-d", "-s", "amp-test", "cat"],
        check=True,
    )
    try:
        target = subprocess.run(
            ["tmux", "-S", str(socket), "list-panes", "-t", "amp-test", "-F", "#{pane_id}"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        (bridge / "tmux.json").write_text(
            json.dumps({"socket_path": str(socket), "tmux_target": target}),
            encoding="utf-8",
        )

        # The real `cat` pane has no resident plugin, so simulate the plugin's
        # turn-started marker arriving immediately after the submit Enter.
        monkeypatch.setattr(
            amp_native_bridge,
            "_wait_for_turn_started",
            lambda path, token, *, timeout_s: True,
        )
        started = time.monotonic()
        amp_native_bridge.inject_user_message(bridge, "hello from browser")
        elapsed = time.monotonic() - started
        pane = subprocess.run(
            ["tmux", "-S", str(socket), "capture-pane", "-p", "-t", target],
            check=True,
            capture_output=True,
            text=True,
        ).stdout

        assert elapsed < 2.0
        assert "hello from browser" in pane
    finally:
        subprocess.run(["tmux", "-S", str(socket), "kill-server"], check=False)


def test_managed_install_permissions_and_conflict(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    bridge = tmp_path / "bridge"
    target, config = amp_native_bridge.install_plugin_and_config(
        bridge, session_id="conv_1", server_url="http://server/", auth_headers={}
    )
    assert amp_native_bridge.MANAGED_MARKER in target.read_text(encoding="utf-8")
    assert stat.S_IMODE(config.stat().st_mode) == 0o600
    assert json.loads(config.read_text())["serverUrl"] == "http://server"
    target.write_text("// user plugin", encoding="utf-8")
    with pytest.raises(RuntimeError, match="Refusing to overwrite unmanaged"):
        amp_native_bridge.install_plugin_and_config(
            bridge, session_id="conv_1", server_url="http://server", auth_headers={}
        )


def test_plugin_resource_uses_typed_thread_and_external_event_contracts() -> None:
    source = (
        Path(amp_native_bridge.__file__).parent / "resources" / "amp_native" / "omnigent-native.ts"
    ).read_text(encoding="utf-8")
    assert 'import type { PluginAPI, ThreadID } from "@ampcode/plugin"' in source
    assert "amp.threads.get(managedThreadID)" in source
    # First-turn adoption: agent.start adopts the event thread when no managed
    # thread is attributed yet (session.start may fire late or not at all in
    # some Amp runtimes) instead of bailing and dropping the turn-started
    # signal. Later turns still reject other threads.
    assert "if (!managedThreadID) managedThreadID = event.thread.id" in source
    assert "else if (event.thread.id !== managedThreadID) return" in source
    assert 'type: "external_conversation_item", data:' in source
    assert 'type: "external_assistant_message", data:' in source
    assert 'type: "external_session_status", data:' in source
    # The plugin writes a local turn-started marker on agent.start, stamped with
    # the current delivery's token read from pending_delivery.json, so the
    # delivery path can verify the submit Enter took effect for THIS delivery
    # (mirrors the interrupt inbox file-IPC, no new event type).
    assert "turn_started.json" in source
    assert "pending_delivery.json" in source
    assert "signalTurnStarted" in source
    assert "readPendingToken" in source
    # The token is captured and the marker written BEFORE any awaited POST.
    assert "signalTurnStarted(readPendingToken())" in source


# Drives the real TypeScript handler. Node >= 23 strips types from .ts imports
# out of the box, so this exercises the actual adoption logic rather than a
# string check or a fake that stamps the marker inside the Enter path. The
# harness reads everything but the plugin path from the environment, so its
# source needs no Python brace-escaping.
_FIRST_TURN_HARNESS = """\
import fs from "node:fs";
import path from "node:path";
import plugin from "file://__PLUGIN__";
const bridgeDir = process.env.BR;
const tsFile = path.join(bridgeDir, process.env.TS_FILE);
const pdFile = path.join(bridgeDir, process.env.PD_FILE);
const handlers = {};
const amp = {
  on: (event, cb) => { handlers[event] = cb; },
  threads: { get: async () => ({ cancel: async () => {} }) },
};
plugin(amp);
// FIRST-TURN ORDERING: agent.start arrives with NO prior session.start, so the
// managed thread is unattributed when it runs. The handler must adopt this
// thread and stamp the marker with THIS delivery's token rather than bail.
await handlers["agent.start"]({ id: "resp-1", thread: { id: "T-managed" }, message: "hello" });
let marker = null;
try { marker = JSON.parse(fs.readFileSync(tsFile, "utf8")); } catch {}
console.log("MARKER=" + JSON.stringify(marker));
// A later agent.start from a different thread must be rejected and must not
// re-stamp the marker with a newer token.
fs.writeFileSync(pdFile, JSON.stringify({ token: "other-token" }));
await handlers["agent.start"]({ id: "resp-2", thread: { id: "T-other" }, message: "ignored" });
console.log("AFTER=" + JSON.stringify(JSON.parse(fs.readFileSync(tsFile, "utf8"))));
process.exit(0);
"""


def test_plugin_first_agent_start_writes_marker_without_session_start(
    tmp_path: Path,
) -> None:
    """The first turn's agent.start must signal turn-started even when it fires
    before (or without) session.start attributing a managed thread.

    Reproduces the live regression: on the prior code the guard bailed because
    ``managedThreadID`` was unset, so no marker was written and the bridge
    exhausted its submit retry budget. Runs the real handler under Node's type
    stripping with the real event payload shape.
    """
    node = shutil.which("node")
    if node is None:
        pytest.skip("node is not installed")

    plugin = (
        Path(amp_native_bridge.__file__).parent / "resources" / "amp_native" / "omnigent-native.ts"
    )
    bridge = tmp_path / "bridge"
    inbox = bridge / "inbox"
    inbox.mkdir(parents=True)
    token = "first-turn-token"
    (bridge / amp_native_bridge._PENDING_DELIVERY_FILE).write_text(
        json.dumps({"token": token}), encoding="utf-8"
    )
    config = tmp_path / "config.json"
    config.write_text(
        json.dumps(
            {
                "sessionId": "s",
                # Refused connection: the plugin fails open on POST, but stamps
                # the marker synchronously before any awaited POST.
                "serverUrl": "http://127.0.0.1:1",
                "authHeaders": {},
                "inboxDir": str(inbox),
            }
        ),
        encoding="utf-8",
    )
    harness = tmp_path / "harness.mjs"
    harness.write_text(_FIRST_TURN_HARNESS.replace("__PLUGIN__", str(plugin)), encoding="utf-8")

    result = subprocess.run(
        [node, str(harness)],
        env={
            **os.environ,
            "BR": str(bridge),
            "TS_FILE": amp_native_bridge._TURN_STARTED_FILE,
            "PD_FILE": amp_native_bridge._PENDING_DELIVERY_FILE,
            "OMNIGENT_AMP_NATIVE_CONFIG": str(config),
        },
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert result.returncode == 0, result.stderr

    def _payload(prefix: str) -> dict[str, object]:
        line = next(line for line in result.stdout.splitlines() if line.startswith(prefix))
        return json.loads(line.split("=", 1)[1])

    marker = _payload("MARKER=")
    after = _payload("AFTER=")

    # The first-turn marker was written with THIS delivery's token despite no
    # session.start — the regression dropped it entirely.
    assert marker.get("token") == token
    # Cross-thread guard still holds: a later agent.start from another thread
    # did not adopt it or re-stamp a newer token.
    assert after.get("token") == token


# ── Verified bounded submit (AMP-NATIVE-0-2) ───────────────────────────────
#
# Token-correlated: the marker is confirmed only when it carries THIS delivery's
# nonce, so a stale marker, a delayed previous-turn write, or a clear failure
# can never false-confirm. Delivery is serialized per session.


class _FakeCompletedProcess:
    def __init__(self, returncode: int = 0) -> None:
        self.returncode = returncode
        self.stdout = ""
        self.stderr = ""


def _advertise_tmux(bridge: Path, tmp_path: Path) -> None:
    (bridge / amp_native_bridge._TMUX_FILE).write_text(
        json.dumps({"socket_path": str(tmp_path / "tmux.sock"), "tmux_target": "amp"}),
        encoding="utf-8",
    )


_STALE_TOKEN = "prior-delivery-token"


class _FakeTmux:
    """Records tmux send-keys calls and simulates the plugin's turn-started signal.

    The fake mirrors the real plugin: on Enter it reads the current
    ``pending_delivery.json`` and stamps ``turn_started.json`` with that token.

    - ``match_on``: 1-based Enter indices that fire ``agent.start`` for THIS
      delivery (marker stamped with the current token).
    - ``stale_on``: 1-based Enter indices whose ``agent.start`` is a delayed
      previous-turn write landing mid-flow (marker stamped with a stale token).
    """

    def __init__(
        self,
        bridge: Path,
        *,
        match_on: set[int] | None = None,
        stale_on: set[int] | None = None,
    ) -> None:
        self.bridge = bridge
        self.match_on = set(match_on or ())
        self.stale_on = set(stale_on or ())
        self.calls: list[tuple[str, ...]] = []

    def _stamp(self, token: str) -> None:
        (self.bridge / amp_native_bridge._TURN_STARTED_FILE).write_text(
            json.dumps({"token": token}), encoding="utf-8"
        )

    def _pending_token(self) -> str | None:
        try:
            return json.loads(
                (self.bridge / amp_native_bridge._PENDING_DELIVERY_FILE).read_text(
                    encoding="utf-8"
                )
            )["token"]
        except (OSError, ValueError, KeyError):
            return None

    def __call__(self, _socket_path: str, *args: str) -> None:
        self.calls.append(tuple(args))
        if not (args and args[0] == "send-keys" and args[-1] == "Enter"):
            return
        enter_index = sum(1 for c in self.calls if c and c[0] == "send-keys" and c[-1] == "Enter")
        if enter_index in self.stale_on:
            self._stamp(_STALE_TOKEN)  # delayed previous-turn write (wrong token)
        elif enter_index in self.match_on:
            token = self._pending_token()
            if token is not None:
                self._stamp(token)  # THIS delivery's agent.start

    @property
    def enters(self) -> int:
        return sum(1 for c in self.calls if c and c[0] == "send-keys" and c[-1] == "Enter")

    @property
    def loads(self) -> int:
        return sum(1 for c in self.calls if c and c[0] == "load-buffer")

    @property
    def pastes(self) -> int:
        return sum(1 for c in self.calls if c and c[0] == "paste-buffer")


def _wire_offline_submit(
    bridge: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    match_on: set[int] | None = None,
    stale_on: set[int] | None = None,
) -> _FakeTmux:
    _advertise_tmux(bridge, tmp_path)
    monkeypatch.setattr(
        amp_native_bridge.subprocess,
        "run",
        lambda *a, **k: _FakeCompletedProcess(0),
    )
    # Tighten the windows so a budget-exhaustion case resolves in milliseconds.
    monkeypatch.setattr(amp_native_bridge, "_SUBMIT_VERIFY_TIMEOUT_S", 0.04)
    monkeypatch.setattr(amp_native_bridge, "_SUBMIT_POLL_INTERVAL_S", 0.005)
    fake = _FakeTmux(bridge, match_on=match_on, stale_on=stale_on)
    monkeypatch.setattr(amp_native_bridge, "_run_tmux", fake)
    return fake


def _bridge(tmp_path: Path) -> Path:
    bridge = tmp_path / "bridge"
    bridge.mkdir()
    return bridge


def test_submit_stale_marker_from_prior_turn_is_not_mistaken_for_confirmation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge = _bridge(tmp_path)
    # A marker from a previous turn (wrong token) is present before this delivery.
    (bridge / amp_native_bridge._TURN_STARTED_FILE).write_text(
        json.dumps({"token": _STALE_TOKEN}), encoding="utf-8"
    )
    # Defeat the best-effort clear so the stale marker survives the whole loop —
    # the token check alone must prevent the false confirmation.
    monkeypatch.setattr(amp_native_bridge, "_clear_turn_started", lambda _path: None)
    fake = _wire_offline_submit(bridge, tmp_path, monkeypatch, match_on={1})

    amp_native_bridge.inject_user_message(bridge, "hello from browser")

    # The stale marker did not short-circuit: the prompt was submitted, and the
    # matching signal on the first Enter confirmed it (no re-send).
    assert fake.enters == 1
    assert fake.pastes == 1


def test_submit_delayed_previous_turn_write_is_ignored_via_token_mismatch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge = _bridge(tmp_path)
    # Attempt 1's "agent.start" is a delayed previous-turn write (stale token);
    # attempt 2 is the genuine signal for THIS delivery.
    fake = _wire_offline_submit(bridge, tmp_path, monkeypatch, stale_on={1}, match_on={2})

    amp_native_bridge.inject_user_message(bridge, "hello from browser")

    # The stale marker landing in attempt 1's window was ignored (token
    # mismatch); the submit was re-sent and confirmed on attempt 2.
    assert fake.enters == 2


def test_submit_marker_clear_failure_does_not_false_confirm(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge = _bridge(tmp_path)
    # Stale marker present, and its unlink fails (IO error during clear).
    (bridge / amp_native_bridge._TURN_STARTED_FILE).write_text(
        json.dumps({"token": _STALE_TOKEN}), encoding="utf-8"
    )

    def _raise_unlink(self: Path, *args: object, **kwargs: object) -> None:
        raise OSError("unlink denied")

    monkeypatch.setattr(amp_native_bridge.Path, "unlink", _raise_unlink)
    fake = _wire_offline_submit(bridge, tmp_path, monkeypatch, match_on={1})

    amp_native_bridge.inject_user_message(bridge, "hello from browser")

    # The failed clear left the stale marker in place, but its token mismatch
    # meant it could not confirm — the prompt was still submitted, not silently
    # dropped (which would re-introduce the original bug).
    assert fake.enters == 1
    assert fake.pastes == 1


def test_submit_pre_send_check_prevents_enter_after_turn_started(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge = _bridge(tmp_path)
    fake = _wire_offline_submit(bridge, tmp_path, monkeypatch)  # no signal via Enter

    # The matching signal arrives just as attempt 1's verify window closes —
    # written from outside the Enter path (the pre-send check on the next
    # iteration must catch it and skip the next Enter).
    def late_signal(path: Path, token: str, *, timeout_s: float) -> bool:
        (bridge / amp_native_bridge._TURN_STARTED_FILE).write_text(
            json.dumps({"token": token}), encoding="utf-8"
        )
        return False  # too late for attempt 1's window

    monkeypatch.setattr(amp_native_bridge, "_wait_for_turn_started", late_signal)

    amp_native_bridge.inject_user_message(bridge, "hello from browser")

    # Exactly one Enter: the re-check before the second Enter saw the matching
    # marker and returned, so no Enter landed after the turn had started.
    assert fake.enters == 1


def test_submit_matching_marker_arrives_normally_sends_one_enter(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge = _bridge(tmp_path)
    fake = _wire_offline_submit(bridge, tmp_path, monkeypatch, match_on={1})

    amp_native_bridge.inject_user_message(bridge, "hello from browser")

    assert fake.enters == 1  # confirmed, no re-send
    assert fake.loads == 1
    assert fake.pastes == 1


def test_submit_pastes_prompt_exactly_once_across_resends(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge = _bridge(tmp_path)
    # Matching signal arrives only on the last allowed attempt.
    fake = _wire_offline_submit(bridge, tmp_path, monkeypatch, match_on={3})

    amp_native_bridge.inject_user_message(bridge, "multi\nline\nprompt")

    assert fake.loads == 1  # invariant 1: paste never repeated
    assert fake.pastes == 1
    assert fake.enters == 3


def test_submit_enter_attempts_bounded_on_persistent_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge = _bridge(tmp_path)
    fake = _wire_offline_submit(bridge, tmp_path, monkeypatch)  # never signals

    confirmed = amp_native_bridge.inject_user_message(bridge, "hello from browser")

    # Invariant #2 (bounded) + #4 (non-fatal): no exception, returns False so the
    # caller can surface a warning instead of failing the turn.
    assert confirmed is False
    assert fake.enters == 3  # bounded, never an infinite loop


def _run_turn_events(executor: AmpNativeExecutor, text: str) -> list[object]:
    async def collect() -> list[object]:
        return [
            event
            async for event in executor.run_turn(
                [{"role": "user", "content": [{"type": "input_text", "text": text}]}],
                [],
                "",
            )
        ]

    return asyncio.run(collect())


def test_run_turn_surfaces_warning_when_submit_unconfirmed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Amp's first turn emits no agent.start, so the resident plugin never stamps
    # the turn-started marker and the submit-confirm loop exhausts non-fatally.
    bridge = tmp_path / "bridge"
    bridge.mkdir()
    monkeypatch.setattr(
        amp_native_executor_module, "inject_user_message", lambda _path, _content: False
    )
    executor = AmpNativeExecutor(bridge_dir=bridge)

    events = _run_turn_events(executor, "hello from browser")

    # Invariant #4 (amended): exhaustion is non-fatal — the turn completes, a
    # user-visible warning is surfaced, and no ExecutorError fails the session.
    assert isinstance(events[-1], TurnComplete)
    assert not any(isinstance(event, ExecutorError) for event in events)
    chunks = [event for event in events if isinstance(event, TextChunk)]
    assert len(chunks) == 1
    assert "Couldn't auto-confirm" in chunks[0].text
    assert "press Enter" in chunks[0].text


def test_run_turn_no_warning_when_submit_confirmed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge = tmp_path / "bridge"
    bridge.mkdir()
    monkeypatch.setattr(
        amp_native_executor_module, "inject_user_message", lambda _path, _content: True
    )
    executor = AmpNativeExecutor(bridge_dir=bridge)

    events = _run_turn_events(executor, "hello from browser")

    # A confirmed submit surfaces no warning and no error — just completion.
    assert len(events) == 1
    assert isinstance(events[0], TurnComplete)
    assert not any(isinstance(event, (TextChunk, ExecutorError)) for event in events)


def test_executor_serializes_concurrent_deliveries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Concurrent deliveries (e.g. a run_turn + an enqueue_session_message) share
    # the TUI paste and the pending_delivery.json token channel, so per-executor
    # delivery must be serialized — mirroring AntigravityNativeExecutor._send_lock.
    bridge = tmp_path / "bridge"
    bridge.mkdir()
    state: dict[str, int] = {"in_flight": 0, "max_in_flight": 0}
    guard = threading.Lock()

    def fake_inject(_path: Path, content: str) -> None:
        with guard:
            state["in_flight"] += 1
            state["max_in_flight"] = max(state["max_in_flight"], state["in_flight"])
        time.sleep(0.05)  # widen the window so any overlap would be observable
        with guard:
            state["in_flight"] -= 1

    monkeypatch.setattr(amp_native_executor_module, "inject_user_message", fake_inject)
    executor = AmpNativeExecutor(bridge_dir=bridge)

    async def main() -> None:
        await asyncio.gather(
            executor.enqueue_session_message("s", "first"),
            executor.enqueue_session_message("s", "second"),
        )

    asyncio.run(main())

    # The two deliveries never ran concurrently (no interleaving on the TUI /
    # token channel). Without the per-executor send lock this would be 2.
    assert state["max_in_flight"] == 1


# ── AMP-NATIVE-0-6: basic MCP relay (registerTool + shared relay) ──────────


_RELAY_TOOLS = [
    {
        "name": "sys_os_read",
        "description": "Read a file from the OS workspace.",
        "parameters": {
            "type": "object",
            "properties": {"path": {"type": "string"}},
            "required": ["path"],
        },
    },
    {
        "name": "manager_status",
        "description": "Report the live runner status.",
        "parameters": {"type": "object", "properties": {}},
    },
]


class _RelayRecorder:
    """Captures the last relay POST the plugin made (auth + body)."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.last_auth: str | None = None
        self.last_body: object | None = None


def _start_relay(
    token: str, *, status: int = 200, payload: object | None = None
) -> tuple[ThreadingHTTPServer, int, _RelayRecorder]:
    """Stand up the localhost shared-relay endpoint the plugin POSTs to."""
    recorder = _RelayRecorder()
    expected_auth = f"Bearer {token}"

    def build_handler() -> type[BaseHTTPRequestHandler]:
        class _Handler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: object) -> None:
                del format, args

            def do_POST(self) -> None:
                length = int(self.headers.get("Content-Length", "0") or "0")
                raw = self.rfile.read(length) if length else b""
                try:
                    body = json.loads(raw or b"{}")
                except ValueError:
                    body = None
                with recorder.lock:
                    recorder.last_auth = self.headers.get("Authorization")
                    recorder.last_body = body
                if self.headers.get("Authorization") != expected_auth:
                    self._send(401, b"unauthorized")
                    return
                if status != 200:
                    self._send(status, b"relay-failure")
                    return
                body_payload = payload
                if body_payload is None:
                    name = body.get("name") if isinstance(body, dict) else None
                    body_payload = {
                        "content": [
                            {"type": "text", "text": json.dumps({"ok": True, "name": name})}
                        ]
                    }
                self._send(200, json.dumps(body_payload).encode("utf-8"))

            def _send(self, code: int, data: bytes) -> None:
                self.send_response(code)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        return _Handler

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), build_handler())
    thread = threading.Thread(target=httpd.serve_forever, name="relay-stub", daemon=True)
    thread.start()
    return httpd, httpd.server_address[1], recorder


_PLUGIN_HARNESS_TEMPLATE = """\
import plugin from "file://__PLUGIN__";
const registrations = [];
const amp = {
  on: () => {},
  threads: { get: async () => ({ cancel: async () => {} }) },
  registerTool: (def) => { registrations.push(def); return { unsubscribe: () => {} }; },
};
plugin(amp);
const summary = registrations.map((r) => ({
  name: r.name,
  description: r.description,
  inputSchemaType: r.inputSchema && r.inputSchema.type,
  required: r.inputSchema && r.inputSchema.required,
}));
console.log("REGISTERED=" + JSON.stringify(summary));
const target = registrations.find((r) => r.name === "__TOOL__") || registrations[0];
if (target) {
  try {
    const res = await target.execute(__INPUT__);
    console.log("RESULT=" + JSON.stringify(res));
    console.log("TYPE=" + typeof res);
  } catch (e) {
    console.log("THREW=" + String((e && e.message) ? e.message : e));
  }
}
process.exit(0);
"""


def _write_relay_config(
    bridge: Path, *, server_url: str, token: str, tools: list[dict[str, object]]
) -> tuple[Path, Path]:
    """Write the per-session config.json + tool_relay.json the plugin reads."""
    relay_path = bridge / "tool_relay.json"
    relay_path.write_text(
        json.dumps(
            {
                "url": server_url,
                "token": token,
                "tools": tools,
                "pid": 1,
                "updated_at": 1.0,
            }
        ),
        encoding="utf-8",
    )
    config = bridge / "config.json"
    config.write_text(
        json.dumps(
            {
                "sessionId": "s",
                "serverUrl": "http://127.0.0.1:1",
                "authHeaders": {},
                "inboxDir": str(bridge / "inbox"),
                "toolRelayPath": str(relay_path),
            }
        ),
        encoding="utf-8",
    )
    return config, relay_path


def _run_plugin_harness(
    bridge: Path, config: Path, *, tool: str, input_obj: dict[str, object]
) -> subprocess.CompletedProcess[str]:
    plugin_src = (
        Path(amp_native_bridge.__file__).parent / "resources" / "amp_native" / "omnigent-native.ts"
    )
    harness = bridge / "harness.mjs"
    harness.write_text(
        _PLUGIN_HARNESS_TEMPLATE.replace("__PLUGIN__", str(plugin_src))
        .replace("__TOOL__", tool)
        .replace("__INPUT__", json.dumps(input_obj)),
        encoding="utf-8",
    )
    env = {**os.environ, "OMNIGENT_AMP_NATIVE_CONFIG": str(config)}
    return subprocess.run(
        ["node", str(harness)],
        env=env,
        capture_output=True,
        text=True,
        timeout=20,
    )


def _harness_line(out: str, prefix: str) -> str:
    return next(line for line in out.splitlines() if line.startswith(prefix)).split("=", 1)[1]


def test_config_json_includes_relay_path_field(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """config.json carries toolRelayPath alongside the existing fields."""
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    bridge = tmp_path / "bridge"
    _target, config = amp_native_bridge.install_plugin_and_config(
        bridge, session_id="conv_1", server_url="http://server/", auth_headers={"x": "y"}
    )
    payload = json.loads(config.read_text())
    # Existing fields preserved.
    assert payload["sessionId"] == "conv_1"
    assert payload["serverUrl"] == "http://server"
    assert payload["authHeaders"] == {"x": "y"}
    assert payload["inboxDir"] == str(bridge / "inbox")
    # New relay-path field points at the session bridge dir's descriptor.
    assert payload["toolRelayPath"] == str(bridge / "tool_relay.json")


def test_plugin_registers_each_relay_tool_schema(tmp_path: Path) -> None:
    """Every tool_relay.json schema is registered via amp.registerTool."""
    if shutil.which("node") is None:
        pytest.skip("node is not installed")
    bridge = tmp_path / "bridge"
    (bridge / "inbox").mkdir(parents=True)
    config, _relay = _write_relay_config(
        bridge,
        server_url="http://127.0.0.1:1",  # unreachable; registration needs no call
        token="irrelevant",
        tools=_RELAY_TOOLS,
    )
    result = _run_plugin_harness(bridge, config, tool="sys_os_read", input_obj={"path": "/x"})
    assert result.returncode == 0, result.stderr
    registered = json.loads(_harness_line(result.stdout, "REGISTERED="))
    assert [t["name"] for t in registered] == ["sys_os_read", "manager_status"]
    by_name = {t["name"]: t for t in registered}
    assert by_name["sys_os_read"]["description"] == _RELAY_TOOLS[0]["description"]
    assert by_name["sys_os_read"]["inputSchemaType"] == "object"
    assert by_name["sys_os_read"]["required"] == ["path"]
    assert by_name["manager_status"]["description"] == _RELAY_TOOLS[1]["description"]


def test_plugin_execute_routes_through_relay_with_bearer(tmp_path: Path) -> None:
    """A registered tool's execute POSTs {name, arguments} to the shared relay
    url with the bearer token and returns a PluginToolResult from the response."""
    if shutil.which("node") is None:
        pytest.skip("node is not installed")
    bridge = tmp_path / "bridge"
    (bridge / "inbox").mkdir(parents=True)
    token = "relay-bearer-token"
    httpd, port, recorder = _start_relay(token)
    try:
        config, _relay = _write_relay_config(
            bridge,
            server_url=f"http://127.0.0.1:{port}",
            token=token,
            tools=_RELAY_TOOLS,
        )
        result = _run_plugin_harness(
            bridge, config, tool="sys_os_read", input_obj={"path": "/etc/hosts"}
        )
    finally:
        httpd.shutdown()
    assert result.returncode == 0, result.stderr
    assert "THREW=" not in result.stdout
    # POST routed to the shared relay with the bearer token + arguments body.
    with recorder.lock:
        assert recorder.last_auth == f"Bearer {token}"
        assert recorder.last_body == {"name": "sys_os_read", "arguments": {"path": "/etc/hosts"}}
    # Result is a PluginToolResult string carrying the relay's content text.
    value = json.loads(_harness_line(result.stdout, "RESULT="))
    assert _harness_line(result.stdout, "TYPE=") == "string"
    assert json.loads(value)["ok"] is True


def test_plugin_execute_relay_error_becomes_tool_result_not_throw(tmp_path: Path) -> None:
    """A relay error (non-ok) becomes a tool result, not an uncaught throw."""
    if shutil.which("node") is None:
        pytest.skip("node is not installed")
    bridge = tmp_path / "bridge"
    (bridge / "inbox").mkdir(parents=True)
    token = "relay-bearer-token"
    httpd, port, _recorder = _start_relay(token, status=500)
    try:
        config, _relay = _write_relay_config(
            bridge,
            server_url=f"http://127.0.0.1:{port}",
            token=token,
            tools=_RELAY_TOOLS,
        )
        result = _run_plugin_harness(bridge, config, tool="sys_os_read", input_obj={"path": "/x"})
    finally:
        httpd.shutdown()
    assert result.returncode == 0, result.stderr
    assert "THREW=" not in result.stdout
    value = json.loads(_harness_line(result.stdout, "RESULT="))
    assert isinstance(value, str) and value


@pytest.mark.asyncio
async def test_auto_create_amp_terminal_starts_relay_after_prepare_before_launch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """ensure_comment_relay fires after prepare_bridge_dir and before launch."""
    from omnigent.runner import app as runner_app
    from omnigent.runner.app import _auto_create_amp_terminal

    events: list[str] = []

    def fake_prepare(session_id: str) -> Path:
        events.append("prepare")
        bridge = tmp_path / "bridge"
        (bridge / "inbox").mkdir(parents=True, exist_ok=True)
        return bridge

    def fake_install(bridge: Path, **_kwargs: object) -> tuple[Path, Path]:
        events.append("config")
        return bridge / "plugin.ts", bridge / "config.json"

    def fake_clear(_path: Path) -> None:
        events.append("clear")

    async def fake_launch_config(*, session_id: str, server_client: object) -> SimpleNamespace:
        return SimpleNamespace(
            server_url="http://server",
            external_session_id=None,
            terminal_launch_args=[],
            workspace=str(tmp_path),
        )

    async def ensure_relay(session_id: str, **_kwargs: object) -> None:
        events.append("relay")

    class _FakeRegistry:
        terminal_registry = None

        async def launch_required_terminal(self, **_kwargs: object) -> SimpleNamespace:
            events.append("launch")
            return SimpleNamespace(
                id="res-1",
                type="terminal",
                session_id="conv_1",
                name="amp",
                metadata=None,
                environment=None,
            )

    import omnigent.amp_native as amp_native_mod
    import omnigent.cli_auth as cli_auth_mod
    import omnigent.runner._entry as runner_entry

    monkeypatch.setattr(amp_native_bridge, "prepare_bridge_dir", fake_prepare)
    monkeypatch.setattr(amp_native_bridge, "clear_inbox", fake_clear)
    monkeypatch.setattr(amp_native_bridge, "install_plugin_and_config", fake_install)
    # Module-level imports in app.py — patchable on the app namespace.
    monkeypatch.setattr(runner_app, "_pi_native_launch_config", fake_launch_config)
    monkeypatch.setattr(runner_app, "_agent_os_env_from_spec", lambda _spec: None)
    # Locally-imported helpers inside _auto_create_amp_terminal — patch source.
    monkeypatch.setattr(runner_entry, "_make_auth_token_factory", lambda: lambda: "tok")
    monkeypatch.setattr(amp_native_mod, "build_amp_launch", lambda *a, **k: ["amp"])
    monkeypatch.setattr(cli_auth_mod, "databricks_request_headers", lambda *a, **k: {})

    await _auto_create_amp_terminal(
        "conv_1",
        _FakeRegistry(),  # type: ignore[arg-type]
        lambda *_a, **_k: None,
        server_client=object(),  # truthy so the relay guard fires
        agent_spec=None,
        ensure_comment_relay=ensure_relay,  # type: ignore[call-arg]
    )

    relay_at = events.index("relay")
    prepare_at = events.index("prepare")
    launch_at = events.index("launch")
    assert prepare_at < relay_at < launch_at
