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

        started = time.monotonic()
        from omnigent.amp_native_delivery import DeliveryJournal

        journal = DeliveryJournal(bridge)
        record = journal.create(content="hello from browser", conversation_id="conv_1")
        amp_native_bridge.inject_user_message(
            bridge,
            "hello from browser",
            journal=journal,
            delivery_id=record.delivery_id,
        )
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
