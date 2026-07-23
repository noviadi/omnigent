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
machine atomically (temp-file + ``os.replace`` + ``fsync`` of file AND
directory) so a restart can reconcile each record deterministically::

    pending
       │ mark_submission_started (durable BEFORE paste)
       ▼
    submission_started
       │ plugin confirmation (correlated by the matching thread + response_id)
       ▼
    confirmed

A failure AFTER the first mutating tmux operation is treated as ambiguous
(Amp may have received the prompt), so it becomes ``recovery_required`` —
never ``failed``. Only a pre-paste failure (no byte could have reached Amp)
may become ``failed``. A record left non-terminal across a restart is NEVER
automatically resubmitted: :func:`DeliveryJournal.reconcile_on_start`
transitions it to ``recovery_required`` so the harness surfaces an
actionable, typed result instead of silently re-injecting the prompt.

Concurrency: every transition is a compare-and-swap guarded by a per-record
``fcntl`` lock, and only monotonic state transitions are accepted (a
``confirmed`` record can never be regressed to ``recovery_required`` by a
late reconcile). Durability is fail-closed: if the file or directory
``fsync`` cannot be established, the write raises and injection does not
proceed to paste.

Records live under ``<bridge_dir>/delivery/<delivery_id>.json`` — outside
``inbox/`` — so :func:`omnigent.amp_native_bridge.clear_inbox` (called on
terminal recreation) cannot wipe delivery state.
"""

from __future__ import annotations

import enum
import fcntl
import hashlib
import json
import os
import tempfile
import time
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1
_DELIVERY_DIR = "delivery"
_REASON_MAX = 200


class DeliveryState(str, enum.Enum):
    """Lifecycle of one browser prompt's delivery into Amp."""

    PENDING = "pending"
    SUBMISSION_STARTED = "submission_started"
    CONFIRMED = "confirmed"
    RECOVERY_REQUIRED = "recovery_required"
    FAILED = "failed"


# States with no proven terminal outcome; a restart must never replay them.
_NON_TERMINAL_STATES = frozenset({DeliveryState.PENDING, DeliveryState.SUBMISSION_STARTED})


# Monotonic transition table: the set of source states from which each target
# is reachable. Anything not listed is rejected as a regression. ``confirm`` is
# allowed from a recovery state so a late plugin confirmation can resolve an
# uncertainty that a restart reconcile had already flagged.
_ALLOWED_SOURCES: dict[DeliveryState, frozenset[DeliveryState]] = {
    DeliveryState.SUBMISSION_STARTED: frozenset({DeliveryState.PENDING}),
    DeliveryState.CONFIRMED: frozenset(
        {DeliveryState.PENDING, DeliveryState.SUBMISSION_STARTED, DeliveryState.RECOVERY_REQUIRED}
    ),
    DeliveryState.RECOVERY_REQUIRED: frozenset(
        {DeliveryState.PENDING, DeliveryState.SUBMISSION_STARTED}
    ),
    # Only a pre-paste failure (no byte reached Amp) may become failed.
    DeliveryState.FAILED: frozenset({DeliveryState.PENDING}),
    DeliveryState.PENDING: frozenset(),
}


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
    # Sanitized reason captured on the last transition into recovery/failed.
    reason: str | None = None
    # Durable Omnigent conversation item id stamped at confirmation, so a
    # duplicate plugin post can return the existing item without re-appending.
    confirmed_item_id: str | None = field(default=None, repr=False)

    def state_enum(self) -> DeliveryState:
        return DeliveryState(self.state)


def _clock() -> float:
    """Indirection point so tests can advance the clock deterministically."""
    return time.time()


def canonical_prompt(content: str) -> str:
    """Normalize prompt text the same way injection delivers it.

    Injection converts CRLF/CR to ``\\n``, keeps ``\\t`` and printable chars,
    and drops every other control byte. The delivery hash must use the SAME
    canonical form so two calls with paste-equivalent prompts share a digest
    (a hash collision then means the prompts are genuinely the same delivery).
    """
    text = content.replace("\r\n", "\n").replace("\r", "\n")
    out: list[str] = []
    for character in text:
        if character == "\n" or character == "\t" or ord(character) >= 0x20:
            out.append(character)
    return "".join(out)


def normalized_content_hash(content: str) -> str:
    """Hash the paste-canonical prompt text (no raw prompt is stored).

    A prompt may carry secrets, and durability is about delivery outcome, not
    transcript replay — so only a digest is persisted.
    """
    return hashlib.sha256(canonical_prompt(content).encode("utf-8")).hexdigest()


def sanitize_reason(text: str | None) -> str | None:
    """Bound and de-control a free-form reason before it is persisted.

    Exception text and tmux diagnostics can echo pane content or secrets; keep
    the surfaced reason short, printable, and free of control bytes.
    """
    if not text:
        return None
    cleaned = "".join(ch if (ch == " " or ord(ch) >= 0x20) else " " for ch in str(text))
    cleaned = " ".join(cleaned.split())
    return cleaned[:_REASON_MAX] or None


class DurabilityError(RuntimeError):
    """Raised when an fsync cannot establish on-disk durability (fail closed)."""


@dataclass(frozen=True)
class ConfirmationOutcome:
    """Result of correlating a plugin mirror with an outstanding delivery."""

    confirmed: bool
    delivery_id: str | None
    already_confirmed: bool


class DeliveryJournal:
    """File-backed, versioned prompt-delivery journal for one bridge.

    Each record is one JSON file written via temp-file + ``os.replace`` +
    ``fsync`` (file and directory), so a crash mid-write cannot leave a
    partially persisted state transition. Every transition is a per-record
    compare-and-swap under a ``fcntl`` lock and only monotonic transitions are
    accepted.
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

    def _lock_path(self, delivery_id: str) -> Path:
        return self._dir / f".{delivery_id}.lock"

    @contextmanager
    def _record_lock(self, delivery_id: str):
        """Exclusive cross-process lock serializing one record's transitions."""
        self._ensure_dir()
        lock_path = self._lock_path(delivery_id)
        fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def _fsync_dir(self) -> None:
        """fsync the directory so a rename is durable — fail closed on error."""
        dir_fd = os.open(self._dir, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)

    def _write_atomic(self, record: DeliveryRecord) -> None:
        """Persist a record with temp-file + ``os.replace`` + fail-closed fsync."""
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
            os.replace(temporary, self._path(record.delivery_id))
            temporary = None  # rename consumed the temp file
            self._fsync_dir()
        finally:
            if temporary is not None and os.path.exists(temporary):
                with contextlib_suppress_oserror():
                    os.unlink(temporary)

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
        confirmed_item_id: str | None = None,
    ) -> DeliveryRecord | None:
        """Compare-and-swap a monotonic state transition under a record lock.

        Returns the updated record, the current record if the transition is
        already satisfied (idempotent), or ``None`` if the record is missing or
        the transition would regress the state machine.
        """
        with self._record_lock(delivery_id):
            record = self.get(delivery_id)
            if record is None:
                return None
            current: DeliveryState
            try:
                current = DeliveryState(record.state)
            except ValueError:
                return None
            if current == target:
                return record  # idempotent re-transition
            if current not in _ALLOWED_SOURCES[target]:
                return None  # regression rejected
            updated = replace(
                record,
                state=target.value,
                updated_at=_clock(),
                reason=sanitize_reason(reason) if reason is not None else record.reason,
                response_id=response_id if response_id is not None else record.response_id,
                confirmed_item_id=(
                    confirmed_item_id
                    if confirmed_item_id is not None
                    else record.confirmed_item_id
                ),
            )
            self._write_atomic(updated)
            return updated

    def mark_submission_started(self, delivery_id: str) -> DeliveryRecord | None:
        """Advance a record to ``submission_started`` (durable BEFORE paste)."""
        return self._transition(delivery_id, DeliveryState.SUBMISSION_STARTED)

    def confirm(
        self,
        delivery_id: str,
        *,
        response_id: str | None = None,
        confirmed_item_id: str | None = None,
    ) -> DeliveryRecord | None:
        """Advance a record to ``confirmed`` once the plugin mirrors it."""
        return self._transition(
            delivery_id,
            DeliveryState.CONFIRMED,
            response_id=response_id,
            confirmed_item_id=confirmed_item_id,
        )

    def confirm_outstanding(
        self,
        *,
        response_id: str,
        expected_thread_id: str | None = None,
        confirmed_item_id: str | None = None,
    ) -> ConfirmationOutcome:
        """Confirm the single outstanding delivery for a plugin mirror.

        Correlates the plugin-mirrored user message (carrying a ``response_id``
        of ``<thread_id>:<event_id>`` from the matching ``agent.start``) with
        the one record still in ``submission_started``. Validates the
        ``response_id`` thread component against the conversation's bound
        ``external_session_id`` when known, and stamps the durable Omnigent
        item id so a duplicate post can return it. Safe under concurrency: the
        transition is a locked CAS, and zero/multiple outstanding records are a
        no-op (ambiguous → no guessing).
        """
        if expected_thread_id and not _response_id_matches_thread(response_id, expected_thread_id):
            return ConfirmationOutcome(confirmed=False, delivery_id=None, already_confirmed=False)
        outstanding = [
            r for r in self.load_all() if r.state == DeliveryState.SUBMISSION_STARTED.value
        ]
        if len(outstanding) != 1:
            return ConfirmationOutcome(confirmed=False, delivery_id=None, already_confirmed=False)
        target = outstanding[0]
        updated = self.confirm(
            target.delivery_id,
            response_id=response_id,
            confirmed_item_id=confirmed_item_id,
        )
        if updated is None:
            # Lost a race to a terminal state (e.g. reconcile). If it landed in
            # confirmed this mirror is a harmless duplicate; otherwise surface
            # non-confirmation.
            current = self.get(target.delivery_id)
            already = current is not None and current.state == DeliveryState.CONFIRMED.value
            return ConfirmationOutcome(
                confirmed=False,
                delivery_id=target.delivery_id,
                already_confirmed=already,
            )
        return ConfirmationOutcome(
            confirmed=updated.state == DeliveryState.CONFIRMED.value,
            delivery_id=updated.delivery_id,
            already_confirmed=False,
        )

    def mark_recovery_required(
        self, delivery_id: str, *, reason: str | None = None
    ) -> DeliveryRecord | None:
        return self._transition(delivery_id, DeliveryState.RECOVERY_REQUIRED, reason=reason)

    def mark_failed(self, delivery_id: str, *, reason: str | None = None) -> DeliveryRecord | None:
        return self._transition(delivery_id, DeliveryState.FAILED, reason=reason)

    def reconcile_on_start(self) -> list[DeliveryRecord]:
        """Transition any non-terminal record to ``recovery_required``.

        A record in ``pending``/``submission_started`` has no proven outcome
        after a restart — Amp may already have committed the prompt. It must
        NEVER be automatically resubmitted. Each such record is durably marked
        ``recovery_required`` so the harness surfaces an actionable typed
        result and a human/explicit path decides replay. Terminal records
        (confirmed/failed/recovery_required) are never regressed.
        """
        recovered: list[DeliveryRecord] = []
        for record in self.load_all():
            try:
                state = DeliveryState(record.state)
            except ValueError:
                continue
            if state in _NON_TERMINAL_STATES:
                # Lock per record to avoid clobbering a concurrent confirm.
                with self._record_lock(record.delivery_id):
                    current = self.get(record.delivery_id)
                    if current is None:
                        continue
                    try:
                        cur_state = DeliveryState(current.state)
                    except ValueError:
                        continue
                    if cur_state not in _NON_TERMINAL_STATES:
                        continue  # raced to terminal; do not regress
                    updated = replace(
                        current,
                        state=DeliveryState.RECOVERY_REQUIRED.value,
                        updated_at=_clock(),
                        reason=f"restart interrupted delivery in {cur_state.value}",
                    )
                    self._write_atomic(updated)
                    recovered.append(updated)
        return recovered

    def has_outstanding_delivery(self) -> bool:
        """Whether any record is still pending or submission_started."""
        return any(_safe_state(record) in _NON_TERMINAL_STATES for record in self.load_all())


def _response_id_matches_thread(response_id: str, expected_thread_id: str) -> bool:
    """A response_id ``<thread>:<event>`` belongs to the expected thread."""
    if not expected_thread_id:
        return True
    prefix = response_id.split(":", 1)[0]
    return prefix == expected_thread_id


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
            confirmed_item_id=data.get("confirmed_item_id"),
        )
    except (KeyError, TypeError, ValueError):
        return None


def contextlib_suppress_oserror() -> Any:
    """Lazy ``contextlib.suppress(OSError)`` for temp-file cleanup only."""
    import contextlib

    return contextlib.suppress(OSError)
