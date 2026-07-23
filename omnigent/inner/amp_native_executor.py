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
import logging
import os
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

from omnigent.amp_native_bridge import (
    AMP_NATIVE_BRIDGE_DIR_ENV_VAR,
    AMP_NATIVE_REQUEST_SESSION_ID_ENV_VAR,
    InjectionHooks,
    inject_user_message,
)
from omnigent.amp_native_delivery import DeliveryJournal, DeliveryState
from omnigent.inner.executor import (
    Executor,
    ExecutorConfig,
    ExecutorError,
    ExecutorEvent,
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

    def _recovery_record_for(self, text: str) -> str | None:
        """Return the delivery_id of the most recent recovery record matching text.

        A retry of a prompt whose prior delivery was interrupted (now
        ``recovery_required``) must not be replayed. Matching is by the
        paste-normalized content digest the journal stores, so whitespace/CRLF
        variants of the same prompt are recognized as the same delivery.
        """
        from omnigent.amp_native_delivery import normalized_content_hash

        digest = normalized_content_hash(text)
        for record in reversed(self._journal.load_all()):
            if (
                record.state == DeliveryState.RECOVERY_REQUIRED.value
                and record.normalized_content_hash == digest
            ):
                return record.delivery_id
        return None

    async def enqueue_session_message(self, session_key: str, content: Any) -> bool:
        """Inject a mid-session message, gated by delivery recovery state.

        Returns ``False`` (rather than injecting) when the message is a retry of
        a prompt already marked recovery_required, so a live-queue retry cannot
        blind-replay an interrupted delivery.
        """
        del session_key
        text = _content_to_text(content)
        if not text:
            return False
        if self._recovery_record_for(text) is not None:
            _logger.warning("amp-native live-queue message refused: delivery in recovery_required")
            return False
        try:
            record = self._journal.create(content=text, conversation_id=self._conversation_id)
            await asyncio.to_thread(
                inject_user_message,
                self._bridge_dir,
                text,
                journal=self._journal,
                delivery_id=record.delivery_id,
            )
        except RuntimeError:
            return False
        return True

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
        # A retry of an interrupted delivery must surface an actionable, typed
        # result and never be automatically resubmitted.
        recovery_id = self._recovery_record_for(text)
        if recovery_id is not None:
            yield ExecutorError(
                message=(
                    f"[{RECOVERY_ERROR_CODE}] delivery {recovery_id} for this prompt "
                    "was interrupted before confirmation; resubmission is blocked. "
                    "Reconcile the Amp thread outcome or start a new conversation."
                )
            )
            return
        record = None
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
        except RuntimeError as exc:
            if record is not None:
                self._journal.mark_failed(record.delivery_id, reason=f"injection error: {exc}")
            yield ExecutorError(message=f"[{FAILED_ERROR_CODE}] {exc}")
            return
        yield TurnComplete(response=None)

    def _injection_hooks(self) -> InjectionHooks | None:
        """Override hook in tests to inject faults at paste/Enter boundaries."""
        return None
