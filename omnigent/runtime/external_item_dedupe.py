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
amp-native bridge directory. The whole acquire → (store-recovery) → append →
commit critical section for one ``(session, response_id)`` is serialized by a
cross-process ``fcntl`` flock on a per-key lock file, so two callers (processes
or threads) for the same key can NEVER interleave: the second blocks on the
flock until the first commits or aborts. A claim therefore carries an explicit
state and is only ever removed by its OWNER on a known append failure — a
concurrent caller never sees or deletes an active claim. The marker is written
with fail-closed ``fsync`` (file AND directory) before the append proceeds, so a
crash after the marker is created but before commit cannot lose the claim. A
tentative marker left by a crashed prior attempt is recovered against the store
on the next claim.

The key is the plugin ``response_id``, which already binds the matching thread
plus event id. Scope: only amp-native user-message external items are deduped
here; non-amp routes are untouched (dedupe is gated by the caller on the wrapper
label).
"""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

from omnigent.amp_native_bridge import bridge_dir_for_session_id

_DEDUPE_SUBDIR = "dedupe"


@dataclass(frozen=True)
class ClaimView:
    """Outcome of :meth:`Claim.enter` for one (session, response_id).

    ``duplicate``: a committed item already exists for this key; ``item_id`` is
    the durable id to return without appending.

    ``orphan``: a tentative marker from a crashed prior attempt exists (no
    committed id). The owner MUST first check the store: if an item exists,
    :meth:`Claim.commit` it and return it; otherwise treat the claim as owned
    and append.

    Neither flag set: a fresh claim the owner appends to, then commits.
    """

    duplicate: bool
    item_id: str | None = None
    orphan: bool = False


def _marker_dir(session_id: str) -> Path:
    directory = bridge_dir_for_session_id(session_id) / "delivery" / _DEDUPE_SUBDIR
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(directory, 0o700)
    return directory


def _marker_path(session_id: str, response_id: str) -> Path:
    # Hash so any byte in response_id is filename-safe.
    digest = hashlib.sha256(response_id.encode("utf-8")).hexdigest()
    return _marker_dir(session_id) / f"{digest}.json"


def _read_marker(path: Path) -> dict | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return payload if isinstance(payload, dict) else None


def _write_marker(path: Path, payload: dict) -> None:
    """Atomically write a marker with fail-closed fsync (file AND directory).

    The durability guarantee depends on the marker surviving a crash that kills
    the append in flight, so both the file and its directory are fsynced before
    this returns. A write failure raises (fail closed) so the caller never
    proceeds to append against a claim that is not durably recorded.
    """
    directory = path.parent
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(directory, 0o700)
    fd, temporary = tempfile.mkstemp(prefix=".claim.", suffix=".tmp", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        temporary = None
        _fsync_dir(directory)
    finally:
        if temporary is not None and os.path.exists(temporary):
            with contextlib.suppress(OSError):
                os.unlink(temporary)


def _fsync_path_and_dir(fd: int, directory: Path) -> None:
    """fsync an open fd and its directory (durable claim creation)."""
    os.fsync(fd)
    _fsync_dir(directory)


def _fsync_dir(directory: Path) -> None:
    fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class Claim:
    """A cross-process flock-guarded claim for one ``(session, response_id)``.

    The flock serializes the entire acquire → append → commit critical section
    across processes and threads. ``enter`` takes the flock and resolves the
    marker state; the caller then appends (or returns an existing id) and calls
    ``commit``; ``close`` releases the flock. On an exception between ``enter``
    and ``commit`` the OWNER's own tentative marker is removed (``abort``) so a
    retry can re-acquire — a concurrent caller, blocked on the flock, never
    observes or deletes an active claim.

    Use as a context manager around the append, or call ``enter``/``commit``/
    ``close`` explicitly (the server path holds the flock across an awaited
    append, so it cannot use the blocking context form on the event loop).
    """

    def __init__(self, session_id: str, response_id: str) -> None:
        self._session_id = session_id
        self._response_id = response_id
        self._path = _marker_path(session_id, response_id)
        self._lock_path = self._path.with_suffix(".lock")
        self._lock_fd: int | None = None
        self._entered = False
        self._committed = False

    def _ensure_lock_open(self) -> int:
        if self._lock_fd is not None:
            return self._lock_fd
        # Create the lock file durably so a crash cannot lose the flock anchor.
        directory = self._path.parent
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(directory, 0o700)
        fd = os.open(self._lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            _fsync_path_and_dir(fd, directory)
        except OSError:
            os.close(fd)
            raise
        self._lock_fd = fd
        return fd

    def enter(self) -> ClaimView:
        """Take the flock and resolve the marker; must pair with ``close``."""
        fd = self._ensure_lock_open()
        fcntl.flock(fd, fcntl.LOCK_EX)
        self._entered = True
        existing = _read_marker(self._path)
        if existing and isinstance(existing.get("item_id"), str) and existing["item_id"]:
            # A committed item already exists: this post is a durable duplicate.
            return ClaimView(duplicate=True, item_id=existing["item_id"])
        orphan = existing is not None
        if not orphan:
            # Fresh claim: record the tentative marker durably BEFORE the
            # append proceeds, so a crash after this point is recoverable.
            _write_marker(self._path, {})
        return ClaimView(duplicate=False, item_id=None, orphan=orphan)

    def commit(self, item_id: str) -> None:
        """Record the durable item id for this claim after a successful append."""
        _write_marker(self._path, {"item_id": item_id})
        self._committed = True

    def abort(self) -> None:
        """Remove this owner's own tentative claim so a retry can re-acquire.

        Only safe to call by the claim owner while it holds the flock. A
        concurrent caller never reaches here (it is blocked on the flock).
        """
        with contextlib.suppress(OSError):
            self._path.unlink()

    def close(self) -> None:
        """Release the flock and close the lock fd. Idempotent."""
        if self._lock_fd is None:
            return
        if self._entered:
            with contextlib.suppress(OSError):
                fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
            self._entered = False
        os.close(self._lock_fd)
        self._lock_fd = None

    def __enter__(self) -> ClaimView:
        return self.enter()

    def __exit__(self, exc_type, exc, tb) -> None:
        try:
            if exc_type is not None and not self._committed:
                # The owner's append failed: drop its tentative claim so a retry
                # can re-acquire. A concurrent caller is blocked on the flock
                # and never sees this claim, so this never deletes active work.
                self.abort()
        finally:
            self.close()


def commit(session_id: str, response_id: str, item_id: str) -> None:
    """Record the durable item id for a claim after a successful append.

    Convenience for callers that already hold the claim via :class:`Claim`;
    prefer ``Claim.commit`` inside the ``with Claim(...)`` block so the flock is
    held across the append.
    """
    _write_marker(_marker_path(session_id, response_id), {"item_id": item_id})


def is_claimed(session_id: str, response_id: str) -> bool:
    """Read-only check for whether a committed claim exists (test/diagnostic)."""
    marker = _read_marker(_marker_path(session_id, response_id))
    if marker is None:
        return False
    value = marker.get("item_id")
    return isinstance(value, str) and bool(value)


def reset_for_tests() -> None:
    """No-op retained for test compatibility.

    Claim serialization is a cross-process flock (no in-process lock dict), so
    there is nothing to reset in memory; marker files live on disk and are
    cleaned by test tmp directories.
    """
