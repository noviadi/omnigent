"""Set-once dedupe for terminal-mirrored user conversation items.

The Amp plugin mirrors a browser prompt back to Omnigent as an
``external_conversation_item`` user message once the resident TUI accepts it.
The post carries a ``response_id`` derived from the Amp ``agent.start`` of the
matching thread (``<thread_id>:<event_id>``) — stable across plugin retries.
Without a server-side guard, a retried post (network blip, plugin restart)
would both append a SECOND durable conversation item and, because the
reconciliation path drains the optimistic pending-input queue FIFO, consume
the NEXT queued browser prompt's entry. Both are the
"misbound / duplicated user work" failure mode this task (AMP-NATIVE-001)
exists to prevent.

This module is the set-once guard keyed by that stable correlation id. It is
deliberately process-affine and in-memory, matching the established transient
recovery-state modules (:mod:`pending_inputs`, :mod:`pending_elicitations`,
:mod:`inflight_text`) that ride the same single-process session affinity.

Scope: only user-message external items are deduped here. Assistant text and
status events are not user work and are not guarded by this path.
"""

from __future__ import annotations

import threading

# Per-conversation set of response_ids that have already produced a durable
# user-message conversation item. Once claimed, a repeat post for the same
# (conversation, response_id) is a no-op: no second durable item, no FIFO
# drain of a different prompt's pending entry.
_claimed: dict[str, set[str]] = {}
_lock = threading.Lock()


def claim(conversation_id: str, response_id: str) -> bool:
    """Record a user-item response_id, set-once per conversation.

    :param conversation_id: Omnigent conversation id, e.g. ``"conv_abc123"``.
    :param response_id: Stable correlation id from the Amp ``agent.start``
        of the matching thread, e.g. ``"T-42:ev_7"``.
    :returns: ``True`` the first time the key is claimed for this
        conversation (the caller should persist + drain); ``False`` on any
        repeat (the caller must skip persist and drain — the item is already
        durable and the optimistic bubble already cleared).
    """
    with _lock:
        seen = _claimed.setdefault(conversation_id, set())
        if response_id in seen:
            return False
        seen.add(response_id)
        return True


def is_claimed(conversation_id: str, response_id: str) -> bool:
    """Read-only check for whether a key has been claimed (test/diagnostic)."""
    with _lock:
        return response_id in _claimed.get(conversation_id, ())


def reset_for_tests() -> None:
    """Clear all claims. Test isolation only."""
    with _lock:
        _claimed.clear()
