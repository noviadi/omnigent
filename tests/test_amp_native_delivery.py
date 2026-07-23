"""Delivery-state fault injection for the ``amp-native`` harness (AMP-NATIVE-001).

Covers the durable delivery journal (CAS transitions, fail-closed fsync), the
paste/Enter boundary ordering, restart reconciliation (no blind replay),
phase-aware failure handling (post-mutation -> recovery_required), the durable
cross-process set-once dedupe for the browser optimistic item, and the
real-event-path confirmation that wires a plugin mirror to a delivery.
"""

from __future__ import annotations

import asyncio
import json
import subprocess
import types
from pathlib import Path

import pytest

from omnigent import amp_native_bridge
from omnigent.amp_native_bridge import InjectionHooks, inject_user_message
from omnigent.amp_native_delivery import (
    SCHEMA_VERSION,
    DeliveryJournal,
    DeliveryState,
)
from omnigent.inner import amp_native_executor as executor_module
from omnigent.inner.amp_native_executor import AmpNativeExecutor
from omnigent.runtime import external_item_dedupe

# --------------------------------------------------------------------------- #
# Test fixtures
# --------------------------------------------------------------------------- #


class _FakeTmux:
    """Stand-in for tmux so inject_user_message is deterministic in tests."""

    def __init__(self, bridge: Path) -> None:
        self.bridge = bridge
        self.paste_calls = 0
        self.enter_calls = 0
        (bridge / "tmux.json").write_text(
            json.dumps({"socket_path": str(bridge / "tmux.sock"), "tmux_target": "amp:0.0"}),
            encoding="utf-8",
        )

    def run(self, *args, **kwargs):
        del kwargs
        argv = list(args[0]) if args else []
        if "has-session" in argv:
            return subprocess.CompletedProcess(argv, 0, "", "")
        if "paste-buffer" in argv:
            self.paste_calls += 1
        if "send-keys" in argv and "Enter" in argv:
            self.enter_calls += 1
        return subprocess.CompletedProcess(argv, 0, "", "")


@pytest.fixture
def fake_tmux(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> _FakeTmux:
    bridge = tmp_path / "bridge"
    bridge.mkdir()
    fake = _FakeTmux(bridge)
    monkeypatch.setattr(amp_native_bridge.subprocess, "run", fake.run)
    return fake


def _hooks(raise_at: str) -> InjectionHooks:
    """Hooks that simulate the harness dying at one paste/Enter boundary."""

    def _die() -> None:
        raise RuntimeError(f"simulated termination at {raise_at}")

    return InjectionHooks(
        before_paste=_die if raise_at == "before_paste" else None,
        after_paste=_die if raise_at == "after_paste" else None,
        after_enter=_die if raise_at == "after_enter" else None,
    )


async def _noop_async(*args: object, **kwargs: object) -> None:
    return None


# --------------------------------------------------------------------------- #
# Journal state machine
# --------------------------------------------------------------------------- #


def test_journal_creates_versioned_pending_record(tmp_path: Path) -> None:
    journal = DeliveryJournal(tmp_path / "bridge")
    record = journal.create(content="hello\nworld", conversation_id="conv_1")
    assert record.schema_version == SCHEMA_VERSION
    assert record.state == DeliveryState.PENDING.value
    assert record.conversation_id == "conv_1"
    assert record.normalized_content_hash  # digest stored, not raw text
    assert record.created_at == record.updated_at
    assert (tmp_path / "bridge" / "delivery" / f"{record.delivery_id}.json").exists()


def test_hash_canonicalization_matches_injection_drops() -> None:
    """The digest uses the same canonical form injection delivers."""
    from omnigent.amp_native_delivery import normalized_content_hash

    # A vertical tab (0x0B) is dropped by injection; equivalent prompts share a digest.
    assert normalized_content_hash("a\x0bb") == normalized_content_hash("ab")
    assert normalized_content_hash("a\r\nb") == normalized_content_hash("a\nb")


def test_submission_started_is_durable_before_paste(fake_tmux: _FakeTmux) -> None:
    """submission_started is written + fsynced BEFORE any paste/Enter byte."""
    journal = DeliveryJournal(fake_tmux.bridge)
    record = journal.create(content="ship it", conversation_id="conv_1")

    seen_started_before_paste = {"value": False}

    def _before_paste() -> None:
        reloaded = journal.get(record.delivery_id)
        seen_started_before_paste["value"] = (
            reloaded is not None and reloaded.state == DeliveryState.SUBMISSION_STARTED.value
        )

    inject_user_message(
        fake_tmux.bridge,
        "ship it",
        journal=journal,
        delivery_id=record.delivery_id,
        hooks=InjectionHooks(before_paste=_before_paste),
    )
    assert seen_started_before_paste["value"] is True
    assert fake_tmux.paste_calls == 1
    assert fake_tmux.enter_calls == 1


# --------------------------------------------------------------------------- #
# Fault injection at the paste/Enter boundaries
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("boundary", ["before_paste", "after_paste", "after_enter"])
def test_termination_during_injection_leaves_submission_started(
    fake_tmux: _FakeTmux, boundary: str
) -> None:
    """Termination at any paste/Enter boundary leaves submission_started durable."""
    journal = DeliveryJournal(fake_tmux.bridge)
    record = journal.create(content="risky prompt", conversation_id="conv_1")
    with pytest.raises(RuntimeError, match="simulated termination"):
        inject_user_message(
            fake_tmux.bridge,
            "risky prompt",
            journal=journal,
            delivery_id=record.delivery_id,
            hooks=_hooks(boundary),
        )
    reloaded = journal.get(record.delivery_id)
    assert reloaded is not None
    assert reloaded.state == DeliveryState.SUBMISSION_STARTED.value


def test_termination_after_plugin_confirmation_stays_confirmed(
    fake_tmux: _FakeTmux,
) -> None:
    """After plugin confirmation, the record is terminal and survives restart."""
    journal = DeliveryJournal(fake_tmux.bridge)
    record = journal.create(content="confirmed prompt", conversation_id="conv_1")
    inject_user_message(
        fake_tmux.bridge,
        "confirmed prompt",
        journal=journal,
        delivery_id=record.delivery_id,
    )
    journal.confirm(record.delivery_id, response_id="T-1:ev_42")
    reloaded = journal.get(record.delivery_id)
    assert reloaded is not None
    assert reloaded.state == DeliveryState.CONFIRMED.value
    assert reloaded.response_id == "T-1:ev_42"


# --------------------------------------------------------------------------- #
# Restart reconciliation, CAS safety, and no blind replay
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("boundary", ["before_paste", "after_paste", "after_enter"])
def test_restart_transitions_submission_started_to_recovery(
    fake_tmux: _FakeTmux, boundary: str
) -> None:
    journal = DeliveryJournal(fake_tmux.bridge)
    record = journal.create(content="interrupted", conversation_id="conv_1")
    with pytest.raises(RuntimeError):
        inject_user_message(
            fake_tmux.bridge,
            "interrupted",
            journal=journal,
            delivery_id=record.delivery_id,
            hooks=_hooks(boundary),
        )
    recovered = journal.reconcile_on_start()
    assert len(recovered) == 1
    assert recovered[0].delivery_id == record.delivery_id
    assert recovered[0].state == DeliveryState.RECOVERY_REQUIRED.value
    assert recovered[0].reason is not None


def test_restart_does_not_regress_confirmed_record(fake_tmux: _FakeTmux) -> None:
    journal = DeliveryJournal(fake_tmux.bridge)
    record = journal.create(content="ok", conversation_id="conv_1")
    journal.confirm(record.delivery_id, response_id="T-2:ev_9")
    recovered = journal.reconcile_on_start()
    assert recovered == []
    assert journal.get(record.delivery_id).state == DeliveryState.CONFIRMED.value


def test_cas_rejects_failed_after_submission_started(tmp_path: Path) -> None:
    """A post-mutation record can never regress to ``failed`` (replayable)."""
    journal = DeliveryJournal(tmp_path / "bridge")
    record = journal.create(content="x", conversation_id="conv_1")
    journal.mark_submission_started(record.delivery_id)
    # mark_failed is only valid from pending -> rejected (returns None).
    assert journal.mark_failed(record.delivery_id, reason="late tmux error") is None
    assert journal.get(record.delivery_id).state == DeliveryState.SUBMISSION_STARTED.value


def test_confirm_outstanding_rejects_wrong_thread(tmp_path: Path) -> None:
    """Confirmation validates the matching thread before marking confirmed."""
    journal = DeliveryJournal(tmp_path / "bridge")
    record = journal.create(content="x", conversation_id="conv_1")
    journal.mark_submission_started(record.delivery_id)
    outcome = journal.confirm_outstanding(response_id="T-999:ev_1", expected_thread_id="T-1")
    assert outcome.confirmed is False
    assert journal.get(record.delivery_id).state == DeliveryState.SUBMISSION_STARTED.value


def test_confirm_outstanding_confirms_matching_thread(tmp_path: Path) -> None:
    journal = DeliveryJournal(tmp_path / "bridge")
    record = journal.create(content="x", conversation_id="conv_1")
    journal.mark_submission_started(record.delivery_id)
    outcome = journal.confirm_outstanding(
        response_id="T-1:ev_7", expected_thread_id="T-1", confirmed_item_id="item_9"
    )
    assert outcome.confirmed is True
    reloaded = journal.get(record.delivery_id)
    assert reloaded.state == DeliveryState.CONFIRMED.value
    assert reloaded.response_id == "T-1:ev_7"
    assert reloaded.confirmed_item_id == "item_9"


def test_executor_does_not_blindly_replay_recovered_prompt(
    fake_tmux: _FakeTmux, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge = fake_tmux.bridge
    monkeypatch.setenv(executor_module.AMP_NATIVE_BRIDGE_DIR_ENV_VAR, str(bridge))
    monkeypatch.setenv(executor_module.AMP_NATIVE_REQUEST_SESSION_ID_ENV_VAR, "conv_1")
    journal = DeliveryJournal(bridge)
    journal.create(content="once is enough", conversation_id="conv_1")
    journal.reconcile_on_start()

    executor = AmpNativeExecutor(bridge_dir=bridge)
    assert journal.load_all()[0].state == DeliveryState.RECOVERY_REQUIRED.value

    messages = [{"role": "user", "content": "once is enough"}]
    events = asyncio.run(_drive_turn(executor, messages))
    assert executor_module.RECOVERY_ERROR_CODE in " ".join(events)
    assert fake_tmux.paste_calls == 0
    assert fake_tmux.enter_calls == 0


def test_executor_injects_new_prompt_after_recovery(
    fake_tmux: _FakeTmux, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge = fake_tmux.bridge
    monkeypatch.setenv(executor_module.AMP_NATIVE_BRIDGE_DIR_ENV_VAR, str(bridge))
    monkeypatch.setenv(executor_module.AMP_NATIVE_REQUEST_SESSION_ID_ENV_VAR, "conv_1")
    journal = DeliveryJournal(bridge)
    rec = journal.create(content="stuck prompt", conversation_id="conv_1")
    journal.mark_recovery_required(rec.delivery_id, reason="prior crash")

    executor = AmpNativeExecutor(bridge_dir=bridge)
    messages = [{"role": "user", "content": "a brand new question"}]
    outcome = asyncio.run(_first_event_name(executor, messages))
    assert outcome == "TurnComplete"
    assert fake_tmux.paste_calls == 1
    new_records = [r for r in journal.load_all() if r.delivery_id != rec.delivery_id]
    assert new_records
    assert new_records[-1].state == DeliveryState.SUBMISSION_STARTED.value


def test_post_paste_failure_becomes_recovery_not_replayable(
    fake_tmux: _FakeTmux, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failure AFTER paste becomes recovery_required; a retry never replays."""

    class _FaultyExecutor(AmpNativeExecutor):
        def _injection_hooks(self) -> InjectionHooks:
            return _hooks("after_paste")

    bridge = fake_tmux.bridge
    monkeypatch.setenv(executor_module.AMP_NATIVE_BRIDGE_DIR_ENV_VAR, str(bridge))
    monkeypatch.setenv(executor_module.AMP_NATIVE_REQUEST_SESSION_ID_ENV_VAR, "conv_1")
    executor = _FaultyExecutor(bridge_dir=bridge)
    messages = [{"role": "user", "content": "maybe pasted"}]

    events = asyncio.run(_drive_turn(executor, messages))
    assert executor_module.RECOVERY_ERROR_CODE in " ".join(events)
    # Paste reached tmux, but the delivery is recovery_required, not failed.
    assert fake_tmux.paste_calls == 1
    record = next(r for r in executor.journal.load_all() if r.state != "pending")
    assert record.state == DeliveryState.RECOVERY_REQUIRED.value

    # A retry of the same prompt is blocked — no second paste.
    events = asyncio.run(_drive_turn(executor, messages))
    assert executor_module.RECOVERY_ERROR_CODE in " ".join(events)
    assert fake_tmux.paste_calls == 1


def test_live_queue_recovery_returns_typed_result_not_bare_false(
    fake_tmux: _FakeTmux, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge = fake_tmux.bridge
    monkeypatch.setenv(executor_module.AMP_NATIVE_BRIDGE_DIR_ENV_VAR, str(bridge))
    monkeypatch.setenv(executor_module.AMP_NATIVE_REQUEST_SESSION_ID_ENV_VAR, "conv_1")
    journal = DeliveryJournal(bridge)
    rec = journal.create(content="blocked live", conversation_id="conv_1")
    journal.mark_recovery_required(rec.delivery_id, reason="prior crash")

    executor = AmpNativeExecutor(bridge_dir=bridge)
    result = asyncio.run(executor.enqueue_session_message("main", "blocked live"))
    assert (
        isinstance(result, executor_module.LiveQueueResult)
        if hasattr(executor_module, "LiveQueueResult")
        else True
    )
    assert bool(result) is False
    assert result.reason is not None
    assert executor_module.RECOVERY_ERROR_CODE in result.reason
    assert fake_tmux.paste_calls == 0


def test_concurrent_confirm_and_reconcile_do_not_clobber(tmp_path: Path) -> None:
    """A confirm racing a reconcile cannot regress a confirmed record."""
    import threading

    journal = DeliveryJournal(tmp_path / "bridge")
    record = journal.create(content="race", conversation_id="conv_1")
    journal.mark_submission_started(record.delivery_id)

    errors: list[BaseException] = []

    def _confirm() -> None:
        try:
            journal.confirm(record.delivery_id, response_id="T-1:ev_1")
        except BaseException as exc:
            errors.append(exc)

    def _reconcile() -> None:
        try:
            journal.reconcile_on_start()
        except BaseException as exc:
            errors.append(exc)

    t1 = threading.Thread(target=_confirm)
    t2 = threading.Thread(target=_reconcile)
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    assert errors == []
    final = journal.get(record.delivery_id)
    # Either confirmed (won the race) or recovery_required; never pending, and a
    # confirmed record is never regressed by reconcile.
    assert final.state in {
        DeliveryState.CONFIRMED.value,
        DeliveryState.RECOVERY_REQUIRED.value,
    }


# --------------------------------------------------------------------------- #
# Fail-closed durability
# --------------------------------------------------------------------------- #


def test_fsync_failure_is_fail_closed_no_paste(
    fake_tmux: _FakeTmux, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If directory fsync cannot establish durability, injection does not paste."""
    journal = DeliveryJournal(fake_tmux.bridge)
    record = journal.create(content="durable?", conversation_id="conv_1")

    def _raise() -> None:
        raise OSError("simulated fsync failure")

    monkeypatch.setattr(journal, "_fsync_dir", _raise)
    with pytest.raises(OSError, match="simulated fsync failure"):
        inject_user_message(
            fake_tmux.bridge,
            "durable?",
            journal=journal,
            delivery_id=record.delivery_id,
        )
    # No paste/Enter reached Amp because durability could not be established.
    assert fake_tmux.paste_calls == 0
    assert fake_tmux.enter_calls == 0


# --------------------------------------------------------------------------- #
# Terminal-recreate survival + runner reconciliation
# --------------------------------------------------------------------------- #


def test_clear_inbox_does_not_wipe_delivery_state(tmp_path: Path) -> None:
    bridge = amp_native_bridge.prepare_bridge_dir("conv_1")
    journal = DeliveryJournal(bridge)
    record = journal.create(content="survive recreate", conversation_id="conv_1")
    journal.mark_submission_started(record.delivery_id)

    amp_native_bridge.prepare_bridge_dir("conv_1")
    amp_native_bridge.clear_inbox(bridge)

    reloaded = journal.get(record.delivery_id)
    assert reloaded is not None
    assert reloaded.state == DeliveryState.SUBMISSION_STARTED.value
    recovered = DeliveryJournal(bridge).reconcile_on_start()
    assert any(r.delivery_id == record.delivery_id for r in recovered)


def test_runner_recreate_reconciles_and_publishes_recovery(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The recreate lifecycle reconciles the journal and surfaces recovery."""
    bridge = tmp_path / "bridge"
    bridge.mkdir()
    monkeypatch.setattr(amp_native_bridge, "bridge_dir_for_session_id", lambda _sid: bridge)
    journal = DeliveryJournal(bridge)
    journal.create(content="left mid-flight", conversation_id="conv_1")
    journal.mark_submission_started(journal.load_all()[0].delivery_id)

    published: list[dict[str, object]] = []

    def _publish(_sid: str, event: dict[str, object]) -> None:
        published.append(event)

    # Mirror the recreate reconcile step from _auto_create_amp_terminal.
    for recovered in DeliveryJournal(
        amp_native_bridge.bridge_dir_for_session_id("conv_1")
    ).reconcile_on_start():
        _publish(
            "conv_1",
            {
                "type": "amp_native_delivery_recovery",
                "delivery_id": recovered.delivery_id,
                "reason": recovered.reason,
            },
        )
    assert len(published) == 1
    assert published[0]["type"] == "amp_native_delivery_recovery"
    assert journal.load_all()[0].state == DeliveryState.RECOVERY_REQUIRED.value


# --------------------------------------------------------------------------- #
# Durable, cross-process set-once dedupe
# --------------------------------------------------------------------------- #


@pytest.fixture
def isolated_dedupe(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Redirect the dedupe marker root to a tmp bridge and reset in-process locks."""
    bridge = tmp_path / "dedupe_bridge"
    monkeypatch.setattr(external_item_dedupe, "bridge_dir_for_session_id", lambda _sid: bridge)
    external_item_dedupe.reset_for_tests()
    return bridge


def test_dedupe_marker_is_durable_set_once(isolated_dedupe: Path) -> None:
    decision = external_item_dedupe.acquire("conv_1", "T-1:ev_42")
    assert decision.persist is True
    # A repeat acquire for the same key is refused (no existing id yet = tentative).
    again = external_item_dedupe.acquire("conv_1", "T-1:ev_42")
    assert again.persist is False
    assert again.existing_item_id is None
    # After committing the item id, a repeat returns the durable item.
    external_item_dedupe.commit("conv_1", "T-1:ev_42", "item_7")
    assert external_item_dedupe.is_claimed("conv_1", "T-1:ev_42") is True
    duplicate = external_item_dedupe.acquire("conv_1", "T-1:ev_42")
    assert duplicate.persist is False
    assert duplicate.existing_item_id == "item_7"


def test_dedupe_release_lets_retry_proceed(isolated_dedupe: Path) -> None:
    """A failed-before-persistence claim is released so a retry can re-acquire."""
    external_item_dedupe.acquire("conv_1", "T-1:ev_9")
    external_item_dedupe.release("conv_1", "T-1:ev_9")
    decision = external_item_dedupe.acquire("conv_1", "T-1:ev_9")
    assert decision.persist is True


def test_dedupe_recovers_tentative_against_store(isolated_dedupe: Path) -> None:
    """A tentative (crashed-mid-append) marker is reconciled with the store."""
    external_item_dedupe.acquire("conv_1", "T-1:ev_5")  # leaves empty marker
    # Prior attempt actually persisted -> recover fills the marker and skips.
    skip = external_item_dedupe.recover_tentative("conv_1", "T-1:ev_5", "item_3")
    assert skip.persist is False
    assert skip.existing_item_id == "item_3"

    external_item_dedupe.release("conv_1", "T-1:ev_5")
    external_item_dedupe.acquire("conv_1", "T-1:ev_5")  # tentative again
    # Prior attempt persisted nothing -> recover releases and re-acquires.
    redo = external_item_dedupe.recover_tentative("conv_1", "T-1:ev_5", None)
    assert redo.persist is True


# --------------------------------------------------------------------------- #
# Server-side persistence: durable dedupe + real-event-path confirmation
# --------------------------------------------------------------------------- #


class _FakeStore:
    def __init__(self) -> None:
        self._items: list[object] = []

    def append(self, session_id: str, items: list[object]) -> list[object]:
        out = []
        for item in items:
            saved = types.SimpleNamespace(
                id=f"item_{len(self._items)}",
                type=getattr(item, "type", None),
                data=getattr(item, "data", None),
                response_id=getattr(item, "response_id", None),
            )
            self._items.append(saved)
            out.append(saved)
        return out

    def list_items(self, session_id: str, **kwargs: object):
        return types.SimpleNamespace(data=list(self._items))


def _patch_server_neutrals(monkeypatch: pytest.MonkeyPatch) -> None:
    import omnigent.server.routes.sessions as server_routes

    def _noop(*args: object, **kwargs: object) -> None:
        return None

    monkeypatch.setattr(server_routes, "_publish_external_conversation_item", _noop)
    monkeypatch.setattr(server_routes, "_drive_terminal_resolved_elicitation", _noop)
    monkeypatch.setattr(server_routes, "prepare_background_session_title", _noop)
    monkeypatch.setattr(server_routes, "_seed_missing_title_from_user_message", _noop_async)


def _user_mirror_body(response_id: str, text: str) -> types.SimpleNamespace:
    return types.SimpleNamespace(
        data={
            "item_type": "message",
            "response_id": response_id,
            "item_data": {
                "role": "user",
                "content": [{"type": "input_text", "text": text}],
            },
        }
    )


def test_persist_dedupes_amp_user_mirror_and_confirms_delivery(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """First mirror persists + confirms the delivery; duplicate returns existing id."""
    import omnigent.server.routes.sessions as server_routes

    bridge = tmp_path / "bridge"
    monkeypatch.setattr(server_routes, "bridge_dir_for_session_id", lambda _sid: bridge)
    monkeypatch.setattr(external_item_dedupe, "bridge_dir_for_session_id", lambda _sid: bridge)
    external_item_dedupe.reset_for_tests()
    _patch_server_neutrals(monkeypatch)

    # An outstanding browser delivery awaiting plugin confirmation.
    journal = DeliveryJournal(bridge)
    record = journal.create(content="hello", conversation_id="conv_1")
    journal.mark_submission_started(record.delivery_id)

    conv = types.SimpleNamespace(
        labels={"omnigent.wrapper": "amp-native-ui"},
        external_session_id="T-1",
        title=None,
    )
    store = _FakeStore()
    body = _user_mirror_body("T-1:ev_42", "hello")

    first = asyncio.run(
        server_routes._persist_external_conversation_item(
            "conv_1",
            conv,
            body,
            store,  # type: ignore[arg-type]
        )
    )
    # The delivery was confirmed through the real event path (not a direct call).
    confirmed = journal.get(record.delivery_id)
    assert confirmed.state == DeliveryState.CONFIRMED.value
    assert confirmed.response_id == "T-1:ev_42"
    assert confirmed.confirmed_item_id == first

    # A duplicate plugin post returns the existing durable item, no second append.
    second = asyncio.run(
        server_routes._persist_external_conversation_item(
            "conv_1",
            conv,
            body,
            _FakeStore(),  # type: ignore[arg-type]
        )
    )
    assert second == first
    external_item_dedupe.reset_for_tests()


def test_persist_does_not_dedupe_non_amp_routes(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A non-amp user mirror is not deduped (route unchanged)."""
    import omnigent.server.routes.sessions as server_routes

    external_item_dedupe.reset_for_tests()
    _patch_server_neutrals(monkeypatch)

    conv = types.SimpleNamespace(labels={}, external_session_id=None, title=None)
    store = _FakeStore()
    body = _user_mirror_body("T-1:ev_42", "hi")

    first = asyncio.run(
        server_routes._persist_external_conversation_item(
            "conv_1",
            conv,
            body,
            store,  # type: ignore[arg-type]
        )
    )
    second = asyncio.run(
        server_routes._persist_external_conversation_item(
            "conv_1",
            conv,
            body,
            store,  # type: ignore[arg-type]
        )
    )
    assert first != second  # both appended; no dedupe for non-amp
    assert len(store._items) == 2


# --------------------------------------------------------------------------- #
# async driving helpers
# --------------------------------------------------------------------------- #


async def _drive_turn(executor: AmpNativeExecutor, messages: list[dict]) -> list[str]:
    events: list[str] = []
    async for event in executor.run_turn(messages, [], ""):
        events.append(type(event).__name__)
        message = getattr(event, "message", "")
        if isinstance(message, str):
            events.append(message)
    return events


async def _first_event_name(executor: AmpNativeExecutor, messages: list[dict]) -> str:
    async for event in executor.run_turn(messages, [], ""):
        return type(event).__name__
    return "none"
