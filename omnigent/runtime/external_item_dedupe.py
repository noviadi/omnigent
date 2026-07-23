"""Durable, cross-process set-once dedupe for terminal-mirrored user items.

The Amp plugin mirrors a browser prompt back to Omnigent as an
``external_conversation_item`` user message once the resident TUI accepts it.
The post carries a ``response_id`` derived from the Amp ``agent.start`` of the
matching thread (``<thread_id>:<event_id>``) — stable across plugin retries.
Without a server-side guard, a retried post (network blip, plugin/runner
restart) would both append a SECOND durable conversation item and, because the
reconciliation path drains the optimistic pending-input queue FIFO, consume the
NEXT queued browser prompt's entry. Both are the "misbound / duplicated user
work" failure mode AMP-NATIVE-001 exists to prevent.

This module is the durable set-once guard. A claim is a marker file under the
amp-native bridge directory, created atomically with ``O_CREAT | O_EXCL``
(cross-process safe) and committed with the durable item id only AFTER a
successful append. A claim is therefore never consumed by a persistence that
did not happen, and it survives any restart — unlike a volatile in-memory set.
Tentative markers left by a crash mid-append are recovered against the store
(see :func:`recover_tentative`).

The key is the plugin ``response_id``, which already binds the matching thread
plus event id; full delivery_id echo (AMP-NATIVE-002/003) will widen it. Scope:
only amp-native user-message external items are deduped here; non-amp routes
are untouched (dedupe is gated by the caller on the wrapper label).
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path

from omnigent.amp_native_bridge import bridge_dir_for_session_id

_DEDUPE_SUBDIR = "dedupe"


@dataclass(frozen=True)
class ClaimDecision:
    """Outcome of acquiring a dedupe claim before persistence.

    ``persist``: the caller owns the claim and must append, then
    :func:`commit` with the new item id (or :func:`release` on failure).
    ``skip``: the item is already durable; ``existing_item_id`` is the id to
    return to the caller without appending.
    """

    persist: bool
    existing_item_id: str | None = None


def _marker_dir(session_id: str) -> Path:
    directory = bridge_dir_for_session_id(session_id) / "delivery" / _DEDUPE_SUBDIR
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(directory, 0o700)
    return directory


def _marker_path(session_id: str, response_id: str) -> Path:
    # Hash so any byte in response_id is filename-safe.
    digest = hashlib.sha256(response_id.encode("utf-8")).hexdigest()
    return _marker_dir(session_id) / f"{digest}.json"


def _read_item_id(path: Path) -> str | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if isinstance(payload, dict):
        value = payload.get("item_id")
        if isinstance(value, str) and value:
            return value
    return None


def _write_item_id(path: Path, item_id: str) -> None:
    """Atomically commit the durable item id into an existing claim marker."""
    directory = path.parent
    fd, temporary = tempfile_mkstemp(directory, prefix=".claim.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump({"item_id": item_id}, handle)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        temporary = None
        _fsync_dir(directory)
    finally:
        if temporary is not None and os.path.exists(temporary):
            os.unlink(temporary)


def acquire(session_id: str, response_id: str) -> ClaimDecision:
    """Atomically acquire a durable claim for one (session, response_id).

    First caller wins via ``O_CREAT | O_EXCL``. A marker that already carries
    a committed item id means the post is a durable duplicate → skip. An empty
    marker means a prior process crashed mid-append → the caller must
    :func:`recover_tentative` against the store before deciding.
    """
    path = _marker_path(session_id, response_id)
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        existing = _read_item_id(path)
        if existing is not None:
            return ClaimDecision(persist=False, existing_item_id=existing)
        return ClaimDecision(persist=False, existing_item_id=None)  # tentative
    os.close(fd)
    return ClaimDecision(persist=True, existing_item_id=None)


def recover_tentative(
    session_id: str,
    response_id: str,
    existing_item_id: str | None,
) -> ClaimDecision:
    """Resolve a tentative (crashed-mid-append) marker against the store.

    If the prior attempt actually appended, commit the found id and skip. If
    nothing durable exists, release the stale marker so the caller can
    re-acquire and append (a retry whose first attempt failed before
    persistence is never silently discarded).
    """
    path = _marker_path(session_id, response_id)
    if existing_item_id is not None:
        _write_item_id(path, existing_item_id)
        return ClaimDecision(persist=False, existing_item_id=existing_item_id)
    release(session_id, response_id)
    return acquire(session_id, response_id)


def commit(session_id: str, response_id: str, item_id: str) -> None:
    """Record the durable item id for a claim after a successful append."""
    _write_item_id(_marker_path(session_id, response_id), item_id)


def release(session_id: str, response_id: str) -> None:
    """Drop an uncommitted claim so a retry can re-acquire it."""
    with contextlib_suppress_oserror():
        _marker_path(session_id, response_id).unlink()


def is_claimed(session_id: str, response_id: str) -> bool:
    """Read-only check for whether a committed claim exists (test/diagnostic)."""
    return _read_item_id(_marker_path(session_id, response_id)) is not None


# --- process-affine serialization of acquire/append/commit per session --------

import asyncio  # noqa: E402
import threading  # noqa: E402

_async_locks: dict[str, asyncio.Lock] = {}
_session_locks: dict[str, threading.Lock] = {}
_session_locks_guard = threading.Lock()


def async_lock(session_id: str) -> asyncio.Lock:
    """Per-session asyncio lock making acquire→append→commit atomic.

    The durable marker (O_EXCL) is cross-process safe; this closes the
    in-process TOCTOU between the existence check and the append under
    concurrent requests on the server event loop.
    """
    lock = _async_locks.get(session_id)
    if lock is None:
        lock = asyncio.Lock()
        _async_locks[session_id] = lock
    return lock


def session_lock(session_id: str) -> threading.Lock:
    """Synchronous per-session lock variant for non-async callers/tests."""
    with _session_locks_guard:
        lock = _session_locks.get(session_id)
        if lock is None:
            lock = threading.Lock()
            _session_locks[session_id] = lock
        return lock


def reset_for_tests() -> None:
    """Clear in-process locks. Test isolation only (markers live on disk)."""
    with _session_locks_guard:
        _session_locks.clear()
    _async_locks.clear()


# --- small helpers kept local to avoid broad imports at module top ------------


def tempfile_mkstemp(directory: Path, *, prefix: str, suffix: str) -> tuple[int, str]:
    import tempfile

    return tempfile.mkstemp(prefix=prefix, suffix=suffix, dir=directory)


def _fsync_dir(directory: Path) -> None:
    fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def contextlib_suppress_oserror():  # pragma: no cover - trivial wrapper
    import contextlib

    return contextlib.suppress(OSError)
