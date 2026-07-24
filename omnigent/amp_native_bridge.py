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
# Verified bounded submit (mirrors antigravity's submit-verify loop, adapted to
# amp-native's turn-started signal). Only ``Enter`` is re-sent, never the paste.
# The marker is correlated to a specific delivery by a per-delivery nonce so a
# stale marker or a delayed previous-turn write cannot false-confirm.
_TURN_STARTED_FILE = "turn_started.json"
# Bridge -> plugin channel for the current delivery's token. The plugin reads
# this at ``agent.start`` and stamps the marker with it (mirrors the interrupt
# inbox's file-IPC; no new plugin event type, no config field).
_PENDING_DELIVERY_FILE = "pending_delivery.json"
_MAX_SUBMIT_ATTEMPTS = 3
# How long one submit Enter is given for the resident plugin to signal that the
# turn started before it is re-sent. Generous: the signal is local file-IPC and
# latency is not a correctness concern.
_SUBMIT_VERIFY_TIMEOUT_S = 5.0
_SUBMIT_POLL_INTERVAL_S = 0.1


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


def inject_user_message(path: Path, content: str) -> bool:
    """Paste a browser prompt into the resident Amp TUI.

    A blank Amp TUI has no thread yet, so its plugin cannot address the first
    browser turn with ``appendUserMessage``. Tmux injection creates that first
    thread naturally and also keeps every browser turn visible in the real TUI.
    The plugin observes the resulting ``agent.start`` and mirrors the user item
    back through Omnigent's pending-input reconciliation path.

    Returns True when the submit was confirmed within the retry budget, or
    False when it was not (non-fatal: the prompt was pasted and Enter pressed,
    but the turn-started signal never arrived — e.g. Amp's first turn emits no
    ``agent.start``). Raises ``RuntimeError`` only for delivery infra failures
    (no tmux target advertised, the Amp terminal died, or a tmux command
    errored) — those mean the prompt did not reach the TUI.
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
        return _submit_and_verify(path, socket_path, tmux_target)
    finally:
        with contextlib.suppress(OSError):
            os.unlink(paste_path)


def _turn_started_marker(path: Path) -> Path:
    return path / _TURN_STARTED_FILE


def _pending_delivery_marker(path: Path) -> Path:
    return path / _PENDING_DELIVERY_FILE


def _write_pending_delivery(path: Path, token: str) -> None:
    """Publish this delivery's token so the plugin can stamp the marker with it.

    Atomic (temp + replace) like the interrupt inbox. Delivery is serialized per
    session, so this is the only token in flight when the plugin reads it.
    """
    payload = json.dumps({"token": token})
    fd, temporary = tempfile.mkstemp(prefix=".pending.", suffix=".tmp", dir=path)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(payload)
        os.replace(temporary, _pending_delivery_marker(path))
    finally:
        if os.path.exists(temporary):
            with contextlib.suppress(OSError):
                os.unlink(temporary)


def _confirm_turn_started(path: Path, expected_token: str) -> bool:
    """True iff the resident plugin signalled THIS delivery's turn started.

    Confirmation requires a marker stamped with ``expected_token``. A stale
    marker, a delayed previous-turn write, or a half-written/unreadable marker
    has the wrong (or no) token and is ignored — never treated as confirmation.
    """
    try:
        raw = _turn_started_marker(path).read_text(encoding="utf-8")
    except (OSError, ValueError):
        return False
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, ValueError):
        return False
    return isinstance(data, dict) and data.get("token") == expected_token


def _clear_turn_started(path: Path) -> None:
    """Best-effort hygiene: drop a marker left by a previous turn.

    Non-load-bearing: confirmation is keyed on the delivery token, so a marker
    that survives a clear failure (or lands after it) cannot false-confirm — it
    simply carries the wrong token and is ignored. A failed clear is therefore
    tolerated rather than aborting a delivery whose paste already landed.
    """
    marker = _turn_started_marker(path)
    try:
        marker.unlink()
    except FileNotFoundError:
        return
    except OSError:
        # Token matching guards confirmation; leave the stale marker in place.
        return


def _wait_for_turn_started(path: Path, expected_token: str, *, timeout_s: float) -> bool:
    deadline = time.monotonic() + timeout_s
    while True:
        if _confirm_turn_started(path, expected_token):
            return True
        if time.monotonic() >= deadline:
            return _confirm_turn_started(path, expected_token)
        time.sleep(_SUBMIT_POLL_INTERVAL_S)


def _submit_and_verify(path: Path, socket_path: str, tmux_target: str) -> bool:
    """Press ``Enter`` to submit the pasted prompt, verifying the turn started.

    Mirrors antigravity's bounded submit-verify loop, keyed on amp-native's
    turn-started signal correlated to THIS delivery by a per-delivery token: the
    bridge mints a nonce and publishes it (:func:`_write_pending_delivery`); the
    plugin reads it at ``agent.start`` and stamps ``turn_started.json`` with it.
    Only a marker whose token matches this delivery confirms the submit, so a
    stale marker or a delayed previous-turn write is ignored.

    The paste is done once before this call; only ``Enter`` is re-sent, bounded
    by :data:`_MAX_SUBMIT_ATTEMPTS`. The matching marker is re-checked
    immediately before every Enter, so an Enter whose signal lagged past the
    prior window never produces an Enter after this delivery's turn already
    started. Returns True when the signal confirms this delivery within the
    budget; returns False (non-fatal) when the budget is exhausted — the prompt
    was pasted and Enter pressed up to :data:`_MAX_SUBMIT_ATTEMPTS` times, but
    the resident plugin never signalled (e.g. Amp's first turn emits no
    ``agent.start``). The caller surfaces an in-session warning rather than
    failing the turn; see invariant #4.
    """
    token = uuid.uuid4().hex
    _write_pending_delivery(path, token)
    _clear_turn_started(path)
    for _ in range(_MAX_SUBMIT_ATTEMPTS):
        # Re-check before each Enter so a matching signal that lagged past the
        # prior window short-circuits instead of pressing another Enter. NOTE:
        # the check->send window between this confirm and send-keys is an
        # accepted parity-level residual (mirrors antigravity's check-then-act
        # residual; see AMP-NATIVE-0-2 non-goals).
        if _confirm_turn_started(path, token):
            return True
        _run_tmux(socket_path, "send-keys", "-t", tmux_target, "Enter")
        if _wait_for_turn_started(path, token, timeout_s=_SUBMIT_VERIFY_TIMEOUT_S):
            return True
    return False


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
