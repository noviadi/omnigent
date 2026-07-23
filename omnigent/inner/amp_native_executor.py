"""Executor that queues browser controls for an interactive Amp process.

Browser prompts reach the resident Amp TUI through tmux paste + Enter
(:func:`omnigent.amp_native_bridge.inject_user_message`). Each injection is
tracked by a durable, versioned delivery journal
(:class:`omnigent.amp_native_delivery.DeliveryJournal`) so a crash or retry can
never silently resubmit the same prompt. On (re)start the journal is
reconciled: any prompt left ``pending``/``submission_started`` transitions to
``recovery_required`` and a retry of that exact prompt surfaces an actionable,
typed result instead of a blind replay.
"""

from __future__ import annotations

import asyncio
import fcntl
import logging
import os
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

from omnigent.amp_native_bridge import (
    AMP_NATIVE_BRIDGE_DIR_ENV_VAR,
    AMP_NATIVE_REQUEST_SESSION_ID_ENV_VAR,
    InjectionHooks,
    InjectionPreconditionError,
    inject_user_message,
)
from omnigent.amp_native_delivery import (
    DeliveryJournal,
    DeliveryRecord,
    DurabilityError,
)
from omnigent.inner.executor import (
    Executor,
    ExecutorConfig,
    ExecutorError,
    ExecutorEvent,
    LiveQueueResult,
    Message,
    ToolSpec,
    TurnComplete,
)

_logger = logging.getLogger(__name__)

# Prefix stamped on recovery/failed delivery errors so callers can distinguish
# an actionable delivery-state result from a bare/generic executor failure.
RECOVERY_ERROR_CODE = "amp_native_delivery_recovery_required"
FAILED_ERROR_CODE = "amp_native_delivery_failed"


def _content_to_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "\n".join(
            block["text"]
            for block in content
            if isinstance(block, dict)
            and block.get("type") == "input_text"
            and isinstance(block.get("text"), str)
        )
    return ""


class AmpNativeExecutor(Executor):
    def __init__(self, bridge_dir: Path | None = None) -> None:
        raw = os.environ.get(AMP_NATIVE_BRIDGE_DIR_ENV_VAR, "")
        self._bridge_dir = bridge_dir or (Path(raw) if raw else None)
        if self._bridge_dir is None:
            raise RuntimeError(f"{AMP_NATIVE_BRIDGE_DIR_ENV_VAR} is required")
        self._conversation_id = os.environ.get(AMP_NATIVE_REQUEST_SESSION_ID_ENV_VAR)
        self._journal = DeliveryJournal(self._bridge_dir)
        # Corrupt or unsupported-schema journal entries are NOT absent: the
        # loader cannot tell whether such a record represents an in-flight
        # delivery, so any unreadable state blocks new injection (see the guard
        # in enqueue_session_message / run_turn). Surface it at startup so a
        # recovery refusal is traceable rather than silent.
        for note in self._journal.unreadable_records():
            _logger.warning("amp-native journal holds an unreadable record: %s", note)
        # Reconcile any delivery left mid-flight by a prior process: a record
        # in pending/submission_started has no proven outcome and must NEVER be
        # automatically resubmitted. It becomes recovery_required so a retry
        # surfaces an actionable typed result instead of a blind replay.
        recovered = self._journal.reconcile_on_start()
        for record in recovered:
            _logger.warning(
                "amp-native delivery %s requires recovery: %s",
                record.delivery_id,
                record.reason,
            )

    @property
    def journal(self) -> DeliveryJournal:
        """Exposed for tests and future confirmation bridges."""
        return self._journal

    def supports_streaming(self) -> bool:
        return False

    def supports_live_message_queue(self) -> bool:
        return True

    def _recovery_block_reason(self) -> str | None:
        """Typed recovery reason if the journal cannot establish clean state.

        Two fail-closed conditions block new injection: unreadable
        (corrupt/unsupported-schema) entries that may hide an in-flight delivery,
        and a durability failure while reconciling non-terminal records at
        startup. Neither may be treated as resolved/absent — pasting against
        uncertain state risks replaying a delivery.
        """
        unreadable = self._journal.unreadable_records()
        if unreadable:
            return (
                f"[{RECOVERY_ERROR_CODE}] journal holds {len(unreadable)} unreadable "
                "record(s) that may represent an in-flight delivery; new injection is "
                "blocked until the journal is reconciled."
            )
        failures = self._journal.reconcile_failures()
        if failures:
            return (
                f"[{RECOVERY_ERROR_CODE}] startup reconciliation could not durably "
                f"recover {len(failures)} non-terminal record(s); new injection is "
                "blocked until the journal is reconciled."
            )
        return None

    def _mark_failed_safe(self, delivery_id: str, reason: str) -> None:
        """Persist a failed transition without letting its fsync escape raw.

        A DurabilityError while writing the failure transition must not propagate
        as a stack trace: the caller has already classified the outcome (safe
        pre-mutation failure) and will emit the typed FAILED result regardless.
        """
        try:
            self._journal.mark_failed(delivery_id, reason=reason)
        except DurabilityError as exc:
            _logger.error(
                "amp-native delivery %s failed and the failure transition could "
                "not be persisted: %s",
                delivery_id,
                exc,
            )

    def _mark_recovery_safe(self, delivery_id: str, reason: str) -> None:
        """Persist a recovery transition without letting its fsync escape raw.

        A DurabilityError while writing the recovery transition must not
        propagate: the caller has already classified the outcome
        (post-mutation, replayable) and will emit the typed RECOVERY result.
        """
        try:
            self._journal.mark_recovery_required(delivery_id, reason=reason)
        except DurabilityError as exc:
            _logger.error(
                "amp-native delivery %s needs recovery and the recovery "
                "transition could not be persisted: %s",
                delivery_id,
                exc,
            )

    def _blocking_reason_for(self, text: str) -> str | None:
        """Typed recovery reason if a prior delivery of this prompt is unresolved.

        A retry must NEVER replay a prompt whose delivery is non-terminal
        (pending/submission_started — the paste may already have happened while
        the process stayed alive) or already recovery_required. Matching is by
        the paste-canonical content digest the journal stores and the owning
        conversation, so only a confirmed/failed delivery (proven outcome / never
        reached Amp) allows a new delivery for the same prompt.
        """
        from omnigent.amp_native_delivery import (
            _RETRY_BLOCKING_STATES,
            normalized_content_hash,
        )

        digest = normalized_content_hash(text)
        blocking = {state.value for state in _RETRY_BLOCKING_STATES}
        for record in reversed(self._journal.load_all()):
            if record.normalized_content_hash != digest:
                continue
            if record.conversation_id not in (None, self._conversation_id):
                continue
            if record.state in blocking:
                return (
                    f"[{RECOVERY_ERROR_CODE}] delivery {record.delivery_id} is "
                    f"{record.state} for this prompt; resubmission is blocked to "
                    "avoid replay. Reconcile the Amp thread outcome or start a new "
                    "conversation."
                )
        return None

    async def enqueue_session_message(
        self, session_key: str, content: Any
    ) -> bool | LiveQueueResult:
        """Inject a mid-session message, gated by delivery recovery state.

        Returns a typed :class:`LiveQueueResult` (never a bare ``False``):
        ``accepted=False`` with a ``[amp_native_delivery_recovery_required]`` /
        ``[amp_native_delivery_failed]`` reason when the message is blocked, so
        the caller can distinguish a recovery refusal from an unsupported queue.
        """
        del session_key
        text = _content_to_text(content)
        if not text:
            return LiveQueueResult(accepted=False, reason="empty message")
        blocked = self._recovery_block_reason()
        if blocked is not None:
            return LiveQueueResult(accepted=False, reason=blocked)
        # Hold an exclusive cross-process lock for this (conversation, prompt)
        # across scan -> create -> submission_started -> paste so two processes
        # cannot both observe no existing delivery and both create one. The flock
        # is acquired in a worker thread so a contended acquire cannot block the
        # event loop (or deadlock within one process).
        lock_fd = self._journal.open_prompt_lock(self._conversation_id, text)
        try:
            await asyncio.to_thread(fcntl.flock, lock_fd, fcntl.LOCK_EX)
            in_flight = self._blocking_reason_for(text)
            if in_flight is not None:
                return LiveQueueResult(accepted=False, reason=in_flight)
            record: DeliveryRecord | None = None
            try:
                record = self._journal.create(content=text, conversation_id=self._conversation_id)
                await asyncio.to_thread(
                    inject_user_message,
                    self._bridge_dir,
                    text,
                    journal=self._journal,
                    delivery_id=record.delivery_id,
                )
            except InjectionPreconditionError as exc:
                # No byte reached Amp: safe to mark failed and let a retry proceed.
                self._mark_failed_safe(record.delivery_id, reason=str(exc))
                return LiveQueueResult(accepted=False, reason=f"[{FAILED_ERROR_CODE}] {exc}")
            except DurabilityError as exc:
                # A durable write (create or submission_started) failed before any
                # byte could reach Amp: a safe pre-mutation failure. The record
                # may not exist (create failed), so mark_failed only when it does.
                if record is not None:
                    self._mark_failed_safe(record.delivery_id, reason=str(exc))
                return LiveQueueResult(accepted=False, reason=f"[{FAILED_ERROR_CODE}] {exc}")
            except RuntimeError as exc:
                # A mutation may have reached Amp: never ``failed`` (replayable).
                self._mark_recovery_safe(record.delivery_id, reason=str(exc))
                return LiveQueueResult(
                    accepted=False,
                    reason=(
                        f"[{RECOVERY_ERROR_CODE}] delivery {record.delivery_id} was "
                        "interrupted after submission started; resubmission is blocked."
                    ),
                )
        finally:
            self._journal.close_prompt_lock(lock_fd)
        return LiveQueueResult(accepted=True)

    async def run_turn(
        self,
        messages: list[Message],
        tools: list[ToolSpec],
        system_prompt: str,
        config: ExecutorConfig | None = None,
    ) -> AsyncIterator[ExecutorEvent]:
        del tools, system_prompt, config
        text = next(
            (
                _content_to_text(m.get("content"))
                for m in reversed(messages)
                if m.get("role") == "user"
            ),
            "",
        )
        if not text:
            yield ExecutorError(message="Amp native turn had no user text to send")
            return
        blocked = self._recovery_block_reason()
        if blocked is not None:
            yield ExecutorError(message=blocked)
            return
        # Hold an exclusive cross-process lock for this (conversation, prompt)
        # across scan -> create -> submission_started -> paste so two processes
        # cannot both observe no existing delivery and both create one. The flock
        # is acquired in a worker thread so a contended acquire cannot block the
        # event loop (or deadlock within one process).
        lock_fd = self._journal.open_prompt_lock(self._conversation_id, text)
        try:
            await asyncio.to_thread(fcntl.flock, lock_fd, fcntl.LOCK_EX)
            # A retry of an in-flight or interrupted delivery must surface an
            # actionable, typed result and never be automatically resubmitted.
            in_flight = self._blocking_reason_for(text)
            if in_flight is not None:
                yield ExecutorError(message=in_flight)
                return
            record: DeliveryRecord | None = None
            try:
                record = self._journal.create(content=text, conversation_id=self._conversation_id)
                await asyncio.to_thread(
                    inject_user_message,
                    self._bridge_dir,
                    text,
                    journal=self._journal,
                    delivery_id=record.delivery_id,
                    hooks=self._injection_hooks(),
                )
            except InjectionPreconditionError as exc:
                # Pre-paste failure: no byte could have reached Amp, so the record
                # may safely become ``failed`` (a retry can create a new delivery).
                self._mark_failed_safe(record.delivery_id, reason=str(exc))
                yield ExecutorError(message=f"[{FAILED_ERROR_CODE}] {exc}")
                return
            except DurabilityError as exc:
                # A durable write failed before any byte could reach Amp: a safe
                # pre-mutation failure. The record may not exist (create failed),
                # so only mark_failed when it was persisted.
                if record is not None:
                    self._mark_failed_safe(record.delivery_id, reason=str(exc))
                yield ExecutorError(message=f"[{FAILED_ERROR_CODE}] {exc}")
                return
            except RuntimeError as exc:
                # Post-mutation failure: Amp may have received the prompt, so this
                # must become ``recovery_required`` — never ``failed`` (replayable).
                self._mark_recovery_safe(record.delivery_id, reason=str(exc))
                yield ExecutorError(
                    message=(
                        f"[{RECOVERY_ERROR_CODE}] delivery {record.delivery_id} was "
                        "interrupted after submission started; resubmission is blocked. "
                        "Reconcile the Amp thread outcome or start a new conversation."
                    )
                )
                return
        finally:
            self._journal.close_prompt_lock(lock_fd)
        yield TurnComplete(response=None)

    def _injection_hooks(self) -> InjectionHooks | None:
        """Override hook in tests to inject faults at paste/Enter boundaries."""
        return None
