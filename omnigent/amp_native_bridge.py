"""Ordered file bridge used by the resident Amp plugin."""

from __future__ import annotations

import contextlib
import hashlib
import itertools
import json
import os
import subprocess
import tempfile
import time
import uuid
from importlib.resources import files
from pathlib import Path
from typing import Any

AMP_NATIVE_BRIDGE_DIR_ENV_VAR = "HARNESS_AMP_NATIVE_BRIDGE_DIR"
AMP_NATIVE_REQUEST_SESSION_ID_ENV_VAR = "HARNESS_AMP_NATIVE_REQUEST_SESSION_ID"
AMP_NATIVE_CONFIG_ENV_VAR = "OMNIGENT_AMP_NATIVE_CONFIG"
_ROOT = Path.home() / ".omnigent" / "amp-native"
_SEQUENCE = itertools.count()
MANAGED_MARKER = "// omnigent-managed-amp-native-plugin"
_TMUX_FILE = "tmux.json"
_TMUX_BUFFER = "omnigent_amp_native_paste"


def bridge_dir_for_session_id(session_id: str) -> Path:
    return _ROOT / hashlib.sha256(session_id.encode()).hexdigest()[:32]


def prepare_bridge_dir(session_id: str) -> Path:
    path = bridge_dir_for_session_id(session_id)
    (path / "inbox").mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(path, 0o700)
    return path


def clear_inbox(path: Path) -> None:
    """Remove controls left by a previous process."""
    inbox = path / "inbox"
    if inbox.is_dir():
        for item in inbox.iterdir():
            if item.is_file():
                with contextlib.suppress(OSError):
                    item.unlink()


def build_amp_native_spawn_env(session_id: str) -> dict[str, str]:
    return {
        AMP_NATIVE_BRIDGE_DIR_ENV_VAR: str(bridge_dir_for_session_id(session_id)),
        AMP_NATIVE_REQUEST_SESSION_ID_ENV_VAR: session_id,
    }


def _enqueue(path: Path, payload: dict[str, Any]) -> str:
    item_id = str(payload["id"])
    inbox = path / "inbox"
    inbox.mkdir(parents=True, exist_ok=True, mode=0o700)
    ordinal = f"{time.time_ns():020d}_{next(_SEQUENCE):08d}_{item_id}"
    fd, temporary = tempfile.mkstemp(prefix=f".{ordinal}.", suffix=".tmp", dir=inbox)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle)
        os.replace(temporary, inbox / f"{ordinal}.json")
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return item_id


def enqueue_user_message(path: Path, content: str) -> str:
    item_id = f"msg_{uuid.uuid4().hex}"
    return _enqueue(path, {"id": item_id, "type": "user_message", "content": content})


def enqueue_interrupt(path: Path) -> str:
    item_id = f"interrupt_{uuid.uuid4().hex}"
    return _enqueue(path, {"id": item_id, "type": "interrupt"})


def inject_user_message(path: Path, content: str) -> None:
    """Paste a browser prompt into the resident Amp TUI.

    A blank Amp TUI has no thread yet, so its plugin cannot address the first
    browser turn with ``appendUserMessage``. Tmux injection creates that first
    thread naturally and also keeps every browser turn visible in the real TUI.
    The plugin observes the resulting ``agent.start`` and mirrors the user item
    back through Omnigent's pending-input reconciliation path.
    """
    if not content:
        raise RuntimeError("amp-native injection requires non-empty content")
    deadline = time.monotonic() + 5.0
    info: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        try:
            candidate = json.loads((path / _TMUX_FILE).read_text(encoding="utf-8"))
        except (OSError, ValueError):
            candidate = None
        if (
            isinstance(candidate, dict)
            and isinstance(candidate.get("socket_path"), str)
            and isinstance(candidate.get("tmux_target"), str)
        ):
            info = candidate
            break
        time.sleep(0.05)
    if info is None:
        raise RuntimeError("amp-native tmux target was not advertised within 5s")

    socket_path = info["socket_path"]
    tmux_target = info["tmux_target"]
    try:
        alive = subprocess.run(
            ["tmux", "-S", socket_path, "has-session", "-t", tmux_target],
            check=False,
            capture_output=True,
            text=True,
            timeout=5.0,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError(f"amp-native could not inspect tmux session: {exc}") from exc
    if alive.returncode != 0:
        raise RuntimeError("amp terminal is no longer running; restart the session")

    normalized = content.replace("\r\n", "\n").replace("\r", "\n")
    payload = bytearray()
    for character in normalized:
        if character == "\n":
            payload.append(0x0D)
        elif character == "\t":
            payload.append(0x09)
        elif ord(character) >= 0x20:
            payload.extend(character.encode("utf-8"))
    with tempfile.NamedTemporaryFile(dir=path, prefix="paste_", delete=False) as paste:
        paste.write(bytes(payload))
        paste_path = paste.name
    try:
        _run_tmux(socket_path, "load-buffer", "-b", _TMUX_BUFFER, paste_path)
        _run_tmux(
            socket_path,
            "paste-buffer",
            "-p",
            "-d",
            "-b",
            _TMUX_BUFFER,
            "-t",
            tmux_target,
        )
        time.sleep(0.1)
        _run_tmux(socket_path, "send-keys", "-t", tmux_target, "Enter")
    finally:
        with contextlib.suppress(OSError):
            os.unlink(paste_path)


def install_plugin_and_config(
    path: Path, *, session_id: str, server_url: str, auth_headers: dict[str, str]
) -> tuple[Path, Path]:
    """Atomically install the inert global plugin and per-session config."""
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(path, 0o700)
    config = path / "config.json"
    config_payload = json.dumps(
        {
            "sessionId": session_id,
            "serverUrl": server_url.rstrip("/"),
            "authHeaders": auth_headers,
            "inboxDir": str(path / "inbox"),
        }
    )
    fd, temporary_config = tempfile.mkstemp(prefix=".config.", suffix=".tmp", dir=path)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(config_payload)
        os.chmod(temporary_config, 0o600)
        os.replace(temporary_config, config)
    finally:
        if os.path.exists(temporary_config):
            os.unlink(temporary_config)
    target = Path.home() / ".config" / "amp" / "plugins" / "omnigent-native.ts"
    target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(target.parent, 0o700)
    source = files("omnigent.resources.amp_native").joinpath("omnigent-native.ts").read_bytes()
    if target.exists() and MANAGED_MARKER.encode() not in target.read_bytes():
        raise RuntimeError(
            f"Refusing to overwrite unmanaged Amp plugin at {target}. "
            "Move or rename that file, then retry."
        )
    fd, temporary = tempfile.mkstemp(prefix=".omnigent-native.", suffix=".tmp", dir=target.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(source)
        os.replace(temporary, target)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return target, config
def _run_tmux(socket_path: str, *args: str) -> None:
    try:
        result = subprocess.run(
            ["tmux", "-S", socket_path, *args],
            check=False,
            capture_output=True,
            text=True,
            timeout=5.0,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError(f"amp-native tmux command failed: {exc}") from exc
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip() or "<no output>"
        raise RuntimeError(f"amp-native tmux command failed: {detail}")
