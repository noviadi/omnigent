"""Durable, versioned prompt-delivery journal for ``amp-native``.

A browser prompt reaches the resident Amp TUI through tmux paste + Enter
(:func:`omnigent.amp_native_bridge.inject_user_message`). Tmux injection is
not transactional: a crash or runner restart between the paste and Amp's
plugin confirmation leaves an ambiguous outcome. Amp may have already
committed the prompt (or executed tools) even though the local process
reported nothing. Blindly replaying the prompt on retry can therefore
submit the same browser work twice.

This module closes that gap with a per-bridge delivery journal. The bridge
writes a versioned record for every browser prompt and advances its state
machine atomically (write + ``fsync``) so a restart can reconcile each
record deterministically:

::

    pending
       │ mark_submission_started (durable BEFORE paste)
       ▼
    submission_started
       │ plugin confirmation (correlated by response_id)
       ▼
    confirmed | failed

A record left in ``pending`` or ``submission_started`` across a restart is
NEVER automatically resubmitted: :func:`reconcile_on_start` transitions it
to ``recovery_required`` so the harness surfaces an actionable, typed
result instead of silently re-injecting the prompt.

Records live under ``<bridge_dir>/delivery/<delivery_id>.json`` — outside
``inbox/`` — so :func:`omnigent.amp_native_bridge.clear_inbox` (called on
terminal recreation) cannot wipe delivery state.
"""

from __future__ import annotations

import contextlib
import enum
import hashlib
import json
import os
import tempfile
import time
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1
_DELIVERY_DIR = "delivery"


class DeliveryState(str, enum.Enum):
    """Lifecycle of one browser prompt's delivery into Amp."""

    PENDING = "pending"
    SUBMISSION_STARTED = "submission_started"
    CONFIRMED = "confirmed"
    RECOVERY_REQUIRED = "recovery_required"
    FAILED = "failed"


# Records in these states have no proven terminal outcome; a restart must
# never replay them automatically. See :func:`reconcile_on_start`.
_NON_TERMINAL_STATES = frozenset({DeliveryState.PENDING, DeliveryState.SUBMISSION_STARTED})


@dataclass
class DeliveryRecord:
    """One persisted prompt-delivery journal entry (schema v1)."""

    schema_version: int
    delivery_id: str
    conversation_id: str | None
    response_id: str | None
    normalized_content_hash: str
    state: str
    created_at: float
    updated_at: float
    attempts: int = 0
    # Free-form reason captured on the last state transition into a
    # recovery/failed state, so the surfaced typed result is actionable.
    reason: str | None = None

    def state_enum(self) -> DeliveryState:
        return DeliveryState(self.state)


def _clock() -> float:
    """Indirection point so tests can advance the clock deterministically."""
    return time.time()


def normalized_content_hash(content: str) -> str:
    """Hash the paste-normalized prompt text (no raw prompt is stored).

    The journal deliberately stores a digest rather than prompt contents:
    a prompt may carry secrets, and durability is about delivery outcome,
    not transcript replay. Normalization mirrors the injection path's
    newline handling so two calls with equivalent prompts share a digest.
    """
    normalized = content.replace("\r\n", "\n").replace("\r", "\n")
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


class DeliveryJournal:
    """File-backed, versioned prompt-delivery journal for one bridge.

    Each record is one JSON file written via temp-file + ``os.replace`` +
    ``fsync``, so a crash mid-write cannot leave a partially persisted
    state transition. The journal is append/transition-only: a record is
    created once and then advanced through its state machine.
    """

    def __init__(self, bridge_dir: Path) -> None:
        self._bridge_dir = bridge_dir
        self._dir = bridge_dir / _DELIVERY_DIR

    @property
    def root(self) -> Path:
        return self._dir

    def _ensure_dir(self) -> None:
        self._dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(self._dir, 0o700)

    def _path(self, delivery_id: str) -> Path:
        return self._dir / f"{delivery_id}.json"

    def _write_atomic(self, record: DeliveryRecord) -> None:
        """Persist a record with temp-file + ``os.replace`` + ``fsync``."""
        self._ensure_dir()
        payload = json.dumps(asdict(record), sort_keys=True)
        fd, temporary = tempfile.mkstemp(
            prefix=f".{record.delivery_id}.", suffix=".tmp", dir=self._dir
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            # fsync the directory so the rename itself is durable.
            os.replace(temporary, self._path(record.delivery_id))
            self._fsync_dir()
        finally:
            if os.path.exists(temporary):
                with contextlib.suppress(OSError):
                    os.unlink(temporary)

    def _fsync_dir(self) -> None:
        try:
            dir_fd = os.open(self._dir, os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(dir_fd)
        except OSError:
            pass
        finally:
            os.close(dir_fd)

    def create(
        self,
        *,
        content: str,
        conversation_id: str | None = None,
        response_id: str | None = None,
        delivery_id: str | None = None,
    ) -> DeliveryRecord:
        """Create and persist a ``pending`` delivery record."""
        now = _clock()
        record = DeliveryRecord(
            schema_version=SCHEMA_VERSION,
            delivery_id=delivery_id or f"delivery_{uuid.uuid4().hex}",
            conversation_id=conversation_id,
            response_id=response_id,
            normalized_content_hash=normalized_content_hash(content),
            state=DeliveryState.PENDING.value,
            created_at=now,
            updated_at=now,
        )
        self._write_atomic(record)
        return record

    def get(self, delivery_id: str) -> DeliveryRecord | None:
        path = self._path(delivery_id)
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None
        return _record_from_dict(data)

    def load_all(self) -> list[DeliveryRecord]:
        if not self._dir.is_dir():
            return []
        records: list[DeliveryRecord] = []
        for entry in self._dir.glob("*.json"):
            try:
                data = json.loads(entry.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                continue
            record = _record_from_dict(data)
            if record is not None:
                records.append(record)
        records.sort(key=lambda r: r.created_at)
        return records

    def _transition(
        self,
        delivery_id: str,
        target: DeliveryState,
        *,
        reason: str | None = None,
        response_id: str | None = None,
        bump_attempt: bool = False,
    ) -> DeliveryRecord | None:
        record = self.get(delivery_id)
        if record is None:
            return None
        record.state = target.value
        record.updated_at = _clock()
        if reason is not None:
            record.reason = reason
        if response_id is not None:
            record.response_id = response_id
        if bump_attempt:
            record.attempts += 1
        self._write_atomic(record)
        return record

    def mark_submission_started(self, delivery_id: str) -> DeliveryRecord | None:
        """Advance a record to ``submission_started`` (durable BEFORE paste)."""
        return self._transition(delivery_id, DeliveryState.SUBMISSION_STARTED)

    def confirm(
        self,
        delivery_id: str,
        *,
        response_id: str | None = None,
    ) -> DeliveryRecord | None:
        """Advance a record to ``confirmed`` once the plugin mirrors it."""
        return self._transition(delivery_id, DeliveryState.CONFIRMED, response_id=response_id)

    def mark_recovery_required(
        self, delivery_id: str, *, reason: str | None = None
    ) -> DeliveryRecord | None:
        return self._transition(delivery_id, DeliveryState.RECOVERY_REQUIRED, reason=reason)

    def mark_failed(self, delivery_id: str, *, reason: str | None = None) -> DeliveryRecord | None:
        return self._transition(delivery_id, DeliveryState.FAILED, reason=reason)

    def reconcile_on_start(self) -> list[DeliveryRecord]:
        """Transition any non-terminal record to ``recovery_required``.

        A record in ``pending`` or ``submission_started`` has no proven
        outcome after a restart — Amp may already have committed the
        prompt or run tools. It must NEVER be automatically resubmitted.
        Each such record is durably marked ``recovery_required`` (with a
        reason naming the prior state) so the harness surfaces an
        actionable typed result and a human/explicit path decides replay.
        """
        recovered: list[DeliveryRecord] = []
        for record in self.load_all():
            try:
                state = DeliveryState(record.state)
            except ValueError:
                continue
            if state in _NON_TERMINAL_STATES:
                updated = self.mark_recovery_required(
                    record.delivery_id,
                    reason=f"restart interrupted delivery in {state.value}",
                )
                if updated is not None:
                    recovered.append(updated)
        return recovered

    def has_outstanding_delivery(self) -> bool:
        """Whether any record is still pending or submission_started."""
        return any(_safe_state(record) in _NON_TERMINAL_STATES for record in self.load_all())


def _safe_state(record: DeliveryRecord) -> DeliveryState | None:
    try:
        return DeliveryState(record.state)
    except ValueError:
        return None


def _record_from_dict(data: dict[str, Any]) -> DeliveryRecord | None:
    if not isinstance(data, dict):
        return None
    try:
        return DeliveryRecord(
            schema_version=int(data.get("schema_version", SCHEMA_VERSION)),
            delivery_id=str(data["delivery_id"]),
            conversation_id=data.get("conversation_id"),
            response_id=data.get("response_id"),
            normalized_content_hash=str(data["normalized_content_hash"]),
            state=str(data["state"]),
            created_at=float(data["created_at"]),
            updated_at=float(data["updated_at"]),
            attempts=int(data.get("attempts", 0)),
            reason=data.get("reason"),
        )
    except (KeyError, TypeError, ValueError):
        return None


# Local import kept tiny and lazy to avoid pulling ``contextlib`` overhead at
# module import for callers that only need the state enum.
def contextlib_suppress() -> Any:
    import contextlib

    return contextlib.suppress(OSError)
