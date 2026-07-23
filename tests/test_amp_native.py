"""Focused contracts for the Amp native integration."""

from __future__ import annotations

import json
import shutil
import stat
import subprocess
import time
from pathlib import Path

import pytest

from omnigent import amp_native, amp_native_bridge
from omnigent.harness_aliases import (
    canonicalize_harness,
    is_native_harness,
    native_terminal_name,
)
from omnigent.harness_plugins import AMP_NATIVE_CODING_AGENT, harness_capabilities
from omnigent.inner.amp_native_executor import _content_to_text
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
            amp_native_bridge, "_wait_for_turn_started", lambda path, *, timeout_s: True
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
    assert "event.thread?.id !== managedThreadID" in source
    assert 'type: "external_conversation_item", data:' in source
    assert 'type: "external_assistant_message", data:' in source
    assert 'type: "external_session_status", data:' in source
    # The plugin writes a local turn-started marker on agent.start so the
    # delivery path can verify the submit Enter took effect (mirrors the
    # interrupt inbox file-IPC, no new event type).
    assert "turn_started.json" in source
    assert "signalTurnStarted" in source


# ── Verified bounded submit (AMP-NATIVE-0-2) ───────────────────────────────


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


class _FakeTmux:
    """Records tmux send-keys calls and simulates the plugin's turn-started signal.

    ``start_on`` is the set of 1-based Enter indices after which the plugin
    would observe ``agent.start`` and write the turn-started marker.
    """

    def __init__(self, bridge: Path, *, start_on: set[int] | None = None) -> None:
        self.bridge = bridge
        self.start_on = set(start_on or ())
        self.calls: list[tuple[str, ...]] = []

    def __call__(self, _socket_path: str, *args: str) -> None:
        self.calls.append(tuple(args))
        is_enter = bool(args) and args[0] == "send-keys" and args[-1] == "Enter"
        if not is_enter:
            return
        enter_index = sum(1 for c in self.calls if c and c[0] == "send-keys" and c[-1] == "Enter")
        if enter_index in self.start_on:
            (self.bridge / amp_native_bridge._TURN_STARTED_FILE).write_text("{}", encoding="utf-8")

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
    start_on: set[int] | None = None,
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
    fake = _FakeTmux(bridge, start_on=start_on)
    monkeypatch.setattr(amp_native_bridge, "_run_tmux", fake)
    return fake


def test_submit_happy_path_sends_one_enter_and_observes_turn_started(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge = tmp_path / "bridge"
    bridge.mkdir()
    fake = _wire_offline_submit(bridge, tmp_path, monkeypatch, start_on={1})

    amp_native_bridge.inject_user_message(bridge, "hello from browser")

    assert fake.enters == 1  # invariant 2: confirmed, no re-send
    assert fake.loads == 1
    assert fake.pastes == 1


def test_submit_resends_enter_when_turn_start_not_signalled_within_window(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge = tmp_path / "bridge"
    bridge.mkdir()
    fake = _wire_offline_submit(bridge, tmp_path, monkeypatch, start_on={2})

    amp_native_bridge.inject_user_message(bridge, "hello from browser")

    assert fake.enters == 2  # first Enter's signal missed, re-sent within bound


def test_submit_pastes_prompt_exactly_once_across_resends(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge = tmp_path / "bridge"
    bridge.mkdir()
    # Signal arrives only on the last allowed attempt.
    fake = _wire_offline_submit(bridge, tmp_path, monkeypatch, start_on={3})

    amp_native_bridge.inject_user_message(bridge, "multi\nline\nprompt")

    assert fake.loads == 1  # invariant 1: paste never repeated
    assert fake.pastes == 1
    assert fake.enters == 3


def test_submit_enter_attempts_bounded_on_persistent_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge = tmp_path / "bridge"
    bridge.mkdir()
    # Plugin never writes the marker.
    fake = _wire_offline_submit(bridge, tmp_path, monkeypatch, start_on=set())

    with pytest.raises(RuntimeError):
        amp_native_bridge.inject_user_message(bridge, "hello from browser")

    assert fake.enters == 3  # invariant 2: bounded, never an infinite loop


def test_submit_sends_no_enter_after_turn_has_started(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge = tmp_path / "bridge"
    bridge.mkdir()
    # Signal arrives on the 2nd Enter; the loop must stop there, not press a 3rd.
    fake = _wire_offline_submit(bridge, tmp_path, monkeypatch, start_on={2})

    amp_native_bridge.inject_user_message(bridge, "hello from browser")

    assert fake.enters == 2  # invariant 3: no Enter after the turn started


def test_submit_budget_exhausted_raises_runtime_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge = tmp_path / "bridge"
    bridge.mkdir()
    fake = _wire_offline_submit(bridge, tmp_path, monkeypatch, start_on=set())

    with pytest.raises(RuntimeError, match="did not confirm the submitted turn"):
        amp_native_bridge.inject_user_message(bridge, "hello from browser")

    assert fake.enters == 3  # invariant 4: loud failure, never silent
