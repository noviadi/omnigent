"""Executor that queues browser controls for an interactive Amp process."""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

from omnigent.amp_native_bridge import AMP_NATIVE_BRIDGE_DIR_ENV_VAR, inject_user_message
from omnigent.inner.executor import (
    Executor,
    ExecutorConfig,
    ExecutorError,
    ExecutorEvent,
    Message,
    TextChunk,
    ToolSpec,
    TurnComplete,
)


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


# Non-fatal warning surfaced in-session when the submit-confirm loop could not
# verify the prompt reached Amp (invariant #4: budget-exhausted is non-fatal;
# the session stays usable). Amp's first turn emits no agent.start, so the
# turn-started signal never arrives for it — the prompt is still pasted and
# Enter pressed; the user may just need to press Enter manually.
_AMP_NATIVE_SUBMIT_UNCONFIRMED_WARNING = (
    "Couldn't auto-confirm that Amp received the prompt on this first turn "
    "(Amp emits no start signal for it). If it didn't send, press Enter in the "
    "Amp terminal — the session stays usable for follow-ups."
)


class AmpNativeExecutor(Executor):
    def __init__(self, bridge_dir: Path | None = None) -> None:
        raw = os.environ.get(AMP_NATIVE_BRIDGE_DIR_ENV_VAR, "")
        self._bridge_dir = bridge_dir or (Path(raw) if raw else None)
        if self._bridge_dir is None:
            raise RuntimeError(f"{AMP_NATIVE_BRIDGE_DIR_ENV_VAR} is required")
        # Serializes delivery so a concurrent run_turn (initiating message) and
        # enqueue_session_message (mid-turn steer, live message queue) don't
        # paste into the shared TUI at once or interleave on the shared
        # pending_delivery.json token channel.
        self._send_lock = asyncio.Lock()

    def supports_streaming(self) -> bool:
        return False

    def supports_live_message_queue(self) -> bool:
        return True

    async def enqueue_session_message(self, session_key: str, content: Any) -> bool:
        del session_key
        text = _content_to_text(content)
        if not text:
            return False
        try:
            async with self._send_lock:
                await asyncio.to_thread(inject_user_message, self._bridge_dir, text)
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
        confirmed = True
        try:
            async with self._send_lock:
                confirmed = await asyncio.to_thread(inject_user_message, self._bridge_dir, text)
        except RuntimeError as exc:
            yield ExecutorError(message=str(exc))
            return
        # Submit-confirm exhaustion is non-fatal (invariant #4): the prompt was
        # pasted and Enter pressed, but Amp's first turn emits no start signal to
        # confirm against. Surface a heads-up rather than fail the turn.
        if not confirmed:
            yield TextChunk(text=_AMP_NATIVE_SUBMIT_UNCONFIRMED_WARNING)
        yield TurnComplete(response=None)
